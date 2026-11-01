/*
 * Copyright (c) 2026.
 * seekdb native file scan core - independent of SQL query planning.
 * A bounded, restartable, projected reader with schema discovery.
 * The native SQL physical operator must still call this class.
 */
#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_CORE_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SCAN_CORE_H_

#include "sql/engine/basic/ob_file_stream_reader.h"
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

namespace oceanbase { namespace sql { namespace file_stream {

enum ScanType { NULL_VALUE, BOOLEAN_VALUE, INTEGER_VALUE, DOUBLE_VALUE,
                DATE_VALUE, DATETIME_VALUE, STRING_VALUE };
struct ScanCell {
  std::string value;
  bool is_null;
  ScanCell():value(),is_null(true){}
  explicit ScanCell(const std::string &v):value(v),is_null(false){}
};
struct ScanSchema {
  std::vector<std::string> names;
  std::vector<ScanType> types;
};
struct ScanBatch {
  std::vector<std::vector<ScanCell> > rows;
  void clear() { rows.clear(); }
};

// A valid JSON literal used as a transport envelope in JSON_TABLE's existing
// value expression. The executor MUST recheck file access on the SQL session.
// Hex encoding makes filenames unambiguous and prevents SQL/JSON injection.
inline bool make_stream_token(const std::string &absolute_path,
                              FileFormat format, bool header,
                              std::string &token) {
  if(absolute_path.empty() || absolute_path.size()>4096 ||
     absolute_path.find('\0')!=std::string::npos) return false;
  static const char hexdigits[]="0123456789abcdef";
  std::string hex;
  hex.reserve(absolute_path.size()*2);
  for(size_t i=0;i<absolute_path.size();++i) {
    const unsigned char byte=static_cast<unsigned char>(absolute_path[i]);
    hex.push_back(hexdigits[byte>>4]);
    hex.push_back(hexdigits[byte&15]);
  }
  token="[{\"__seekdb_native_file_scan_v1__\":\""+hex+
        "\",\"format\":\""+(format==CSV?"csv":format==JSONL?"jsonl":"json_array")+
        "\",\"header\":"+(header?"1":"0")+"}]";
  return true;
}

inline bool is_stream_token(const std::string &value) {
  static const std::string prefix=
      "[{\"__seekdb_native_file_scan_v1__\":\"";
  return value.compare(0,prefix.size(),prefix)==0;
}

inline bool parse_stream_token(const std::string &token,
                               std::string &path,FileFormat &format,
                               bool &header) {
  static const std::string prefix=
      "[{\"__seekdb_native_file_scan_v1__\":\"";
  if(!is_stream_token(token))return false;
  const size_t stop=token.find('"',prefix.size());
  if(stop==std::string::npos || stop-prefix.size()>8192 ||
     (stop-prefix.size())%2!=0)return false;
  const std::string suffix=token.substr(stop);
  if(suffix=="\",\"format\":\"csv\",\"header\":1}]") {
    format=CSV;header=true;
  }else if(suffix=="\",\"format\":\"csv\",\"header\":0}]") {
    format=CSV;header=false;
  }else if(suffix=="\",\"format\":\"jsonl\",\"header\":1}]") {
    format=JSONL;header=true;
  }else if(suffix=="\",\"format\":\"jsonl\",\"header\":0}]") {
    format=JSONL;header=false;
  }else if(suffix=="\",\"format\":\"json_array\",\"header\":1}]") {
    format=JSON_ARRAY;header=true;
  }else if(suffix=="\",\"format\":\"json_array\",\"header\":0}]") {
    format=JSON_ARRAY;header=false;
  }else return false;
  path.clear();path.reserve((stop-prefix.size())/2);
  for(size_t i=prefix.size();i<stop;i+=2) {
    const char hi=token[i],lo=token[i+1];
    auto digit=[](char c)->int {
      if(c>='0'&&c<='9')return c-'0';
      if(c>='a'&&c<='f')return c-'a'+10;
      return -1;
    };
    const int h=digit(hi),l=digit(lo);
    if(h<0||l<0)return false;
    path.push_back(static_cast<char>((h<<4)|l));
  }
  return !path.empty() && path[0]=='/' &&
         path.find('\0')==std::string::npos;
}

inline void append_json_escaped(const std::string &value,std::string &out) {
  static const char hex[]="0123456789abcdef";
  out.push_back('"');
  for(size_t i=0;i<value.size();++i) {
    const unsigned char c=static_cast<unsigned char>(value[i]);
    switch(c) {
      case '"':out+="\\\"";break;
      case '\\':out+="\\\\";break;
      case '\b':out+="\\b";break;
      case '\f':out+="\\f";break;
      case '\n':out+="\\n";break;
      case '\r':out+="\\r";break;
      case '\t':out+="\\t";break;
      default:
        if(c<0x20) {
          out+="\\u00";
          out.push_back(hex[c>>4]);out.push_back(hex[c&15]);
        }else out.push_back(static_cast<char>(c));
    }
  }
  out.push_back('"');
}

// One row batch at a time; never contains the complete source file.
inline bool scan_batch_as_json(const ScanBatch &batch,
                               const std::vector<std::string> &columns,
                               std::string &json,std::string &err,
                               size_t max_bytes=8u*1024u*1024u) {
  json="[";err.clear();
  for(size_t i=0;i<batch.rows.size();++i) {
    if(batch.rows[i].size()!=columns.size()) {
      err="file scan row/column count mismatch";return false;
    }
    if(i)json.push_back(',');
    json.push_back('{');
    for(size_t j=0;j<columns.size();++j) {
      if(j)json.push_back(',');
      append_json_escaped(columns[j],json);
      json.push_back(':');
      if(batch.rows[i][j].is_null)json+="null";
      else append_json_escaped(batch.rows[i][j].value,json);
      if(json.size()>max_bytes) {
        err="file scan JSON batch exceeds bounded memory budget";
        return false;
      }
    }
    json.push_back('}');
  }
  json.push_back(']');
  if(json.size()>max_bytes) {
    err="file scan JSON batch exceeds bounded memory budget";
    return false;
  }
  return true;
}

class NativeFileScanCursor {
public:
  NativeFileScanCursor():opened_(false),options_(),reader_(),schema_(),
    projected_names_(),projected_indexes_(),scanned_rows_(0){}

  bool open(const std::string &root,const std::string &path,
            const StreamOptions &options,
            const std::vector<std::string> &projected_columns,
            std::string &err) {
    close();err.clear();
    options_=options;
    if (!reader_.open(root,path,options_,err))return false;
    if (options_.format==CSV) {
      schema_.names=reader_.columns();
      schema_.types.assign(schema_.names.size(),NULL_VALUE);
      if (schema_.names.empty()) {
        err="CSV source has no columns";close();return false;
      }
    }
    FileBatch batch;
    do {
      if (!reader_.next_batch(batch,std::min<size_t>(128,options_.max_batch_rows),err)) {close();return false;}
      for(size_t i=0;i<batch.rows.size();++i) {
        if (options_.format==CSV) {
          for(size_t j=0;j<schema_.names.size();++j) {
            const std::string &v=batch.rows[i][j];
            schema_.types[j]=merge_type(schema_.types[j],
                                        v.empty()?NULL_VALUE:detect_type(v));
          }
        } else {
          std::vector<Field> fields;
          if (!parse_object(batch.rows[i][0],fields,err)) {close();return false;}
          for(size_t j=0;j<fields.size();++j) {
            size_t col=0;
            while(col<schema_.names.size() && schema_.names[col]!=fields[j].name)++col;
            if(col==schema_.names.size()) {
              if(schema_.names.size()>=options_.max_columns) {
                err="JSONL schema exceeds column limit";close();return false;
              }
              schema_.names.push_back(fields[j].name);
              schema_.types.push_back(NULL_VALUE);
            }
            schema_.types[col]=merge_type(schema_.types[col],
              fields[j].cell.is_null?NULL_VALUE:
              (fields[j].was_quoted?detect_quoted_type(fields[j].cell.value):
               detect_type(fields[j].cell.value)));
          }
        }
      }
    }while(!batch.rows.empty());
    if(schema_.names.empty()) {err="file schema has no columns";close();return false;}
    if(!reader_.rescan(err)) {close();return false;}
    projected_names_=projected_columns.empty()?schema_.names:projected_columns;
    for(size_t i=0;i<projected_names_.size();++i) {
      const std::vector<std::string>::const_iterator found=
        std::find(schema_.names.begin(),schema_.names.end(),projected_names_[i]);
      if(found==schema_.names.end()) {
        err="unknown projected column: "+projected_names_[i];
        close();return false;
      }
      projected_indexes_.push_back(static_cast<size_t>(found-schema_.names.begin()));
    }
    opened_=true;
    return true;
  }

  bool next_batch(ScanBatch &out,size_t wanted,std::string &err) {
    out.clear();err.clear();
    if(!opened_){err="scan is not open";return false;}
    FileBatch input;
    if(!reader_.next_batch(input,wanted,err))return false;
    for(size_t i=0;i<input.rows.size();++i) {
      std::vector<ScanCell> row;
      row.reserve(projected_indexes_.size());
      if(options_.format==CSV) {
        for(size_t j=0;j<projected_indexes_.size();++j) {
          const std::string &v=input.rows[i][projected_indexes_[j]];
          row.push_back(v.empty()?ScanCell():ScanCell(v));
        }
      } else {
        std::vector<Field> fields;
        if(!parse_object(input.rows[i][0],fields,err)) {
          out.clear();return false;
        }
        for(size_t j=0;j<projected_names_.size();++j) {
          ScanCell cell;
          for(size_t k=0;k<fields.size();++k) {
            if(fields[k].name==projected_names_[j])cell=fields[k].cell;
          }
          row.push_back(std::move(cell));
        }
      }
      out.rows.push_back(std::move(row));
    }
    scanned_rows_+=out.rows.size();
    return true;
  }

  bool rescan(std::string &err) {
    if(!opened_){err="scan is not open";return false;}
    if(!reader_.rescan(err))return false;
    scanned_rows_=0;return true;
  }
  void close() {
    reader_.close();opened_=false;scanned_rows_=0;
    schema_.names.clear();schema_.types.clear();
    projected_names_.clear();projected_indexes_.clear();
  }
  const ScanSchema &schema() const {return schema_;}
  const std::vector<std::string> &projected_names() const {return projected_names_;}
  uint64_t scanned_rows() const {return scanned_rows_;}
  uint64_t bytes_read() const {return reader_.bytes_returned();}
  const std::string &canonical_path() const {return reader_.canonical_path();}
  bool is_open() const {return opened_;}

  static std::string sql_type(ScanType t) {
    switch(t) {
      case BOOLEAN_VALUE:return "TINYINT";
      case INTEGER_VALUE:return "BIGINT";
      case DOUBLE_VALUE:return "DOUBLE";
      case DATE_VALUE:return "DATE";
      case DATETIME_VALUE:return "DATETIME";
      default:return "VARCHAR(4096)";
    }
  }

private:
  struct Field {
    std::string name;
    ScanCell cell;
    bool was_quoted;
    Field():name(),cell(),was_quoted(false){}
  };
  static bool whitespace(char c) {
    return c==' ' || c=='\t' || c=='\n' || c=='\r';
  }
  static void skip_space(const std::string &text,size_t &p) {
    while(p<text.size() && whitespace(text[p]))++p;
  }
  static int hex(char c) {
    if(c>='0' && c<='9')return c-'0';
    if(c>='A' && c<='F')return c-'A'+10;
    if(c>='a' && c<='f')return c-'a'+10;
    return -1;
  }
  static bool utf8_append(uint32_t v,std::string &out) {
    if(v==0 || v>0x10ffff || (v>=0xd800 && v<=0xdfff))return false;
    if(v<0x80)out.push_back(static_cast<char>(v));
    else if(v<0x800) {
      out.push_back(static_cast<char>(0xc0|(v>>6)));
      out.push_back(static_cast<char>(0x80|(v&63)));
    }else if(v<0x10000){
      out.push_back(static_cast<char>(0xe0|(v>>12)));
      out.push_back(static_cast<char>(0x80|((v>>6)&63)));
      out.push_back(static_cast<char>(0x80|(v&63)));
    }else{
      out.push_back(static_cast<char>(0xf0|(v>>18)));
      out.push_back(static_cast<char>(0x80|((v>>12)&63)));
      out.push_back(static_cast<char>(0x80|((v>>6)&63)));
      out.push_back(static_cast<char>(0x80|(v&63)));
    }
    return true;
  }
  static bool hex4(const std::string &text,size_t &p,uint32_t &cp) {
    if(text.size()-p<4)return false;
    cp=0;
    for(int i=0;i<4;++i) {
      const int digit=hex(text[p++]);
      if(digit<0)return false;
      cp=(cp<<4)|static_cast<uint32_t>(digit);
    }
    return true;
  }
  static bool json_string(const std::string &text,size_t &p,
                          std::string &out) {
    if(p>=text.size() || text[p++]!='"')return false;
    while(p<text.size()) {
      const unsigned char c=static_cast<unsigned char>(text[p++]);
      if(c=='"')return true;
      if(c<32)return false;
      if(c!='\\'){out.push_back(static_cast<char>(c));continue;}
      if(p==text.size())return false;
      const char esc=text[p++];
      switch(esc) {
        case '"':out.push_back('"');break;
        case '\\':out.push_back('\\');break;
        case '/':out.push_back('/');break;
        case 'b':out.push_back('\b');break;
        case 'f':out.push_back('\f');break;
        case 'n':out.push_back('\n');break;
        case 'r':out.push_back('\r');break;
        case 't':out.push_back('\t');break;
        case 'u': {
          uint32_t cp=0;
          if(!hex4(text,p,cp))return false;
          if(cp>=0xd800 && cp<=0xdbff) {
            if(p+6>text.size() || text[p++]!='\\' || text[p++]!='u')return false;
            uint32_t low=0;
            if(!hex4(text,p,low) || low<0xdc00 || low>0xdfff)return false;
            cp=0x10000+((cp-0xd800)<<10)+(low-0xdc00);
          }else if(cp>=0xdc00 && cp<=0xdfff)return false;
          if(!utf8_append(cp,out))return false;
          break;
        }
        default:return false;
      }
    }
    return false;
  }
  static bool parse_object(const std::string &text,
                           std::vector<Field> &fields,std::string &err) {
    fields.clear();size_t p=0;skip_space(text,p);
    if(p>=text.size() || text[p++]!='{'){
      err="JSONL row is not an object";return false;
    }
    skip_space(text,p);
    if(p<text.size() && text[p]=='}'){
      ++p;skip_space(text,p);
      if(p==text.size())return true;
    }
    while(p<text.size()) {
      Field field;
      if(!json_string(text,p,field.name) ||
         field.name.empty() || field.name.size()>256) {
        err="JSONL has invalid field name";return false;
      }
      skip_space(text,p);
      if(p>=text.size() || text[p++]!=':'){
        err="JSONL field missing colon";return false;
      }
      skip_space(text,p);
      if(p>=text.size()){
        err="JSONL field missing value";return false;
      }
      field.was_quoted=text[p]=='"';
      if(field.was_quoted) {
        std::string value;
        if(!json_string(text,p,value)){
          err="JSONL field has invalid quoted scalar";return false;
        }
        if(value.size()>4096){
          err="JSONL text exceeds VARCHAR(4096) limit";return false;
        }
        field.cell=ScanCell(value);
      }else{
        const size_t start=p;
        while(p<text.size() && !whitespace(text[p]) &&
              text[p]!=',' && text[p]!='}')++p;
        const std::string value=text.substr(start,p-start);
        if(value=="null")field.cell=ScanCell();
        else if(value=="true" || value=="false" ||
                detect_type(value)==INTEGER_VALUE ||
                detect_type(value)==DOUBLE_VALUE)field.cell=ScanCell(value);
        else {err="JSONL has unsupported scalar";return false;}
      }
      for(size_t index=0;index<fields.size();++index) {
        if(fields[index].name==field.name) {
          err="duplicate JSONL object key: "+field.name;return false;
        }
      }
      fields.push_back(std::move(field));
      skip_space(text,p);
      if(p>=text.size()){err="JSONL row ended unexpectedly";return false;}
      if(text[p]=='}'){
        ++p;skip_space(text,p);
        if(p!=text.size()){
          err="JSONL row has trailing text";return false;
        }
        return true;
      }
      if(text[p++]!=','){
        err="JSONL row missing comma";return false;
      }
      skip_space(text,p);
    }
    err="JSONL row missing closing brace";return false;
  }
  static bool digits(const std::string &v,size_t start,size_t n) {
    if(start+n>v.size())return false;
    for(size_t i=start;i<start+n;++i)if(v[i]<'0'||v[i]>'9')return false;
    return true;
  }
  static bool valid_date(const std::string &v) {
    if(v.size()!=10 || v[4]!='-' || v[7]!='-' ||
       !digits(v,0,4)||!digits(v,5,2)||!digits(v,8,2))return false;
    const int mm=(v[5]-'0')*10+v[6]-'0';
    const int dd=(v[8]-'0')*10+v[9]-'0';
    if(mm<1||mm>12||dd<1)return false;
    int limit=31;
    if(mm==4||mm==6||mm==9||mm==11)limit=30;
    if(mm==2){
      const int year=(v[0]-'0')*1000+(v[1]-'0')*100+(v[2]-'0')*10+(v[3]-'0');
      limit=((year%4==0 && year%100!=0)||year%400==0)?29:28;
    }
    return dd<=limit;
  }
  static ScanType detect_quoted_type(const std::string &value) {
    if(valid_date(value))return DATE_VALUE;
    if(value.size()>=19 && valid_date(value.substr(0,10)) &&
       (value[10]=='T'||value[10]==' ') &&
       digits(value,11,2) && value[13]==':' &&
       digits(value,14,2) && value[16]==':' && digits(value,17,2))return DATETIME_VALUE;
    return STRING_VALUE;
  }
  static ScanType detect_type(const std::string &value) {
    if(value.empty())return NULL_VALUE;
    std::string lowered=value;
    std::transform(lowered.begin(),lowered.end(),lowered.begin(),
      [](unsigned char c){return (c>='A' && c<='Z')?static_cast<char>(c-'A'+'a'):
                                   static_cast<char>(c);});
    if(lowered=="true" || lowered=="false")return BOOLEAN_VALUE;
    if(valid_date(value))return DATE_VALUE;
    if(value.size()>=19 && valid_date(value.substr(0,10)) &&
       (value[10]=='T'||value[10]==' ') &&
       digits(value,11,2) && value[13]==':' &&
       digits(value,14,2) && value[16]==':' && digits(value,17,2))return DATETIME_VALUE;
    char *end=nullptr;errno=0;
    (void)std::strtoll(value.c_str(),&end,10);
    if(errno==0 && end==value.c_str()+value.size())return INTEGER_VALUE;
    end=nullptr;errno=0;
    const double number=std::strtod(value.c_str(),&end);
    if(errno==0 && end==value.c_str()+value.size() &&
       std::isfinite(number) &&
       (value.find('.')!=std::string::npos ||
        value.find('e')!=std::string::npos ||
        value.find('E')!=std::string::npos))return DOUBLE_VALUE;
    return STRING_VALUE;
  }
  static ScanType merge_type(ScanType a,ScanType b) {
    if(a==NULL_VALUE)return b;
    if(b==NULL_VALUE)return a;
    if(a==b)return a;
    if((a==INTEGER_VALUE && b==DOUBLE_VALUE) ||
       (b==INTEGER_VALUE && a==DOUBLE_VALUE))return DOUBLE_VALUE;
    if((a==DATE_VALUE && b==DATETIME_VALUE) ||
       (a==DATETIME_VALUE && b==DATE_VALUE))return DATETIME_VALUE;
    return STRING_VALUE;
  }

  bool opened_;
  StreamOptions options_;
  FileStreamReader reader_;
  ScanSchema schema_;
  std::vector<std::string> projected_names_;
  std::vector<size_t> projected_indexes_;
  uint64_t scanned_rows_;
  NativeFileScanCursor(const NativeFileScanCursor &)=delete;
  NativeFileScanCursor &operator=(const NativeFileScanCursor &)=delete;
};

}}} // oceanbase::sql::file_stream
#endif
