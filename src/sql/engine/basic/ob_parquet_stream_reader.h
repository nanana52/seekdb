#pragma once
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <fcntl.h>
#include <limits.h>
#include <limits>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>
namespace oceanbase { namespace sql { namespace parquet_stream {
static const size_t MAX_FILE_BYTES=32u*1024u*1024u;
static const size_t MAX_FILE_ROWS=16u*1024u*1024u;
inline std::string json_quote(const std::string &x) {
  static const char *hex="0123456789abcdef";
  std::string out="\"";
  for (size_t i=0; i<x.size(); ++i) {
    unsigned char c=static_cast<unsigned char>(x[i]);
    switch(c) {
      case '"': out+="\\\""; break;
      case '\\': out+="\\\\"; break;
      case '\n': out+="\\n"; break;
      case '\r': out+="\\r"; break;
      case '\t': out+="\\t"; break;
      default:
        if (c<32) {
          out+="\\u00";
          out.push_back(hex[c>>4]);
          out.push_back(hex[c&15]);
        } else {
          out.push_back(static_cast<char>(c));
        }
    }
  }
  out.push_back('"');
  return out;
}


struct Reader {
  const unsigned char *data;
  size_t len;
  size_t pos;
  bool ok;
  Reader(const char *src,size_t length)
      :data(reinterpret_cast<const unsigned char *>(src)),
       len(length),pos(0),ok(true){}
  bool can(size_t count) const {
    return count<=len && pos<=len-count;
  }
  unsigned char byte() {
    if(!can(1)){ok=false;return 0;}
    return data[pos++];
  }
  uint64_t uvar() {
    uint64_t v=0;
    for(int shift=0;shift<70;shift+=7) {
      if(!can(1)){ok=false;return 0;}
      const unsigned char c=byte();
      if(shift==63 && (c&0x7e)!=0){ok=false;return 0;}
      if(shift>63 && (c&0x7f)!=0){ok=false;return 0;}
      v|=uint64_t(c&0x7f)<<shift;
      if(!(c&0x80))return v;
    }
    ok=false;return 0;
  }
  int64_t zig() {
    uint64_t n=uvar();
    return static_cast<int64_t>((n>>1) ^ (~(n&1)+1));
  }
  uint64_t le(size_t width) {
    if(width>8 || !can(width)){ok=false;return 0;}
    uint64_t result=0;
    for(size_t i=0;i<width;++i) result|=uint64_t(byte())<<(i*8);
    return result;
  }
  std::string binary() {
    const uint64_t n=uvar();
    if(n>MAX_FILE_BYTES || !can(static_cast<size_t>(n))){
      ok=false;return std::string();
    }
    const std::string result(reinterpret_cast<const char*>(data+pos),
                             static_cast<size_t>(n));
    pos+=static_cast<size_t>(n);
    return result;
  }
  bool field(int &last,int &id,int &type) {
    if(!ok || !can(1)) {ok=false;return false;}
    const int c=byte();
    type=c&0x0f;
    if(type==0)return false;
    const int delta=c>>4;
    const int64_t v=delta?last+delta:zig();
    if(v<1 || v>32767){ok=false;return false;}
    id=static_cast<int>(v);last=id;
    return true;
  }
  bool list(int &type,size_t &count) {
    if(!can(1)){ok=false;return false;}
    const int v=byte();
    type=v&15;
    uint64_t n=(v>>4)==15?uvar():static_cast<uint64_t>(v>>4);
    if(n>MAX_FILE_ROWS*128){ok=false;return false;}
    count=static_cast<size_t>(n);
    return ok;
  }
  bool skip(int type,int depth=0) {
    if(depth>32){ok=false;return false;}
    switch(type) {
      case 0: case 1: case 2: break;
      case 3: if(!can(1))ok=false;else ++pos;break;
      case 4:case 5:case 6: uvar();break;
      case 7: if(!can(8))ok=false;else pos+=8;break;
      case 8: binary();break;
      case 9:case 10: {
        int subtype;size_t count;
        if(!list(subtype,count))return false;
        for(size_t i=0;i<count && ok;++i)skip(subtype,depth+1);
        break;
      }
      case 11: {
        const uint64_t n=uvar();
        if(n>MAX_FILE_ROWS*128){ok=false;return false;}
        if(n>0){
          const int types=byte();
          for(size_t i=0;i<n && ok;++i){
            skip(types>>4,depth+1);
            skip(types&0xf,depth+1);
          }
        }
        break;
      }
      case 12: {
        int last=0,id=0,subtype=0;
        while(ok && field(last,id,subtype))skip(subtype,depth+1);
        break;
      }
      default: ok=false;
    }
    return ok;
  }
};

struct Schema {
  std::string name;
  int type;
  int repetition;
  int children;
  int converted;
  Schema():type(-1),repetition(0),children(0),converted(-1){}
};
struct Column {
  int type;
  int codec;
  uint64_t num_values;
  uint64_t data_offset;
  uint64_t dictionary_offset;
  bool has_dictionary;
  Column():type(-1),codec(-1),num_values(0),data_offset(0),
           dictionary_offset(0),has_dictionary(false){}
};
struct RowGroup {
  uint64_t rows;
  std::vector<Column> columns;
  RowGroup():rows(0){}
};
struct FileMeta {
  std::vector<Schema> schema;
  std::vector<RowGroup> groups;
  uint64_t num_rows;
  FileMeta():num_rows(0){}
};
struct Page {
  int type;
  uint64_t compressed_size,raw_size;
  int encoding,level_encoding,rep_encoding;
  uint64_t values;
  uint64_t def_length,rep_length;
  bool values_compressed;
  Page():type(-1),compressed_size(0),raw_size(0),
         encoding(-1),level_encoding(3),rep_encoding(3),
         values(0),def_length(0),rep_length(0),values_compressed(true){}
};

inline bool parse_schema(Reader &r,Schema &s) {
  int last=0,id=0,type=0;
  while(r.ok && r.field(last,id,type)) {
    if(id==1 && type==5)s.type=static_cast<int>(r.zig());
    else if(id==3 && type==5)s.repetition=static_cast<int>(r.zig());
    else if(id==4 && type==8)s.name=r.binary();
    else if(id==5 && type==5)s.children=static_cast<int>(r.zig());
    else if(id==6 && type==5)s.converted=static_cast<int>(r.zig());
    else r.skip(type);
  }
  return r.ok && !s.name.empty();
}
inline bool parse_colmeta(Reader &r,Column &c) {
  int last=0,id=0,type=0;
  while(r.ok && r.field(last,id,type)) {
    if(id==1 && type==5)c.type=static_cast<int>(r.zig());
    else if(id==4 && type==5)c.codec=static_cast<int>(r.zig());
    else if(id==5 && type==6)c.num_values=static_cast<uint64_t>(r.zig());
    else if(id==9 && type==6)c.data_offset=static_cast<uint64_t>(r.zig());
    else if(id==11 && type==6) {
      c.dictionary_offset=static_cast<uint64_t>(r.zig());
      c.has_dictionary=true;
    } else r.skip(type);
  }
  return r.ok && c.type>=0 && c.codec>=0;
}
inline bool parse_column(Reader &r,Column &column) {
  int last=0,id=0,type=0;
  bool found=false;
  while(r.ok && r.field(last,id,type)) {
    if(id==1 && type==8) {
      // External column chunks are not supported: paths must stay inside
      // the single resolved file, not be followed as auxiliary inputs.
      const std::string other=r.binary();
      if(!other.empty())return false;
    } else if(id==3 && type==12) {
      found=parse_colmeta(r,column);
      if(!found)return false;
    }else r.skip(type);
  }
  return r.ok && found;
}
inline bool parse_rowgroup(Reader &r,RowGroup &group) {
  int last=0,id=0,type=0;
  while(r.ok && r.field(last,id,type)) {
    if(id==1 && type==9) {
      int child=0;size_t count=0;
      if(!r.list(child,count) || child!=12 || count>256)return false;
      for(size_t i=0;i<count;++i) {
        Column column;
        if(!parse_column(r,column))return false;
        group.columns.push_back(column);
      }
    } else if(id==3 && type==6)group.rows=static_cast<uint64_t>(r.zig());
    else r.skip(type);
  }
  return r.ok && group.columns.size()>0 && group.rows<=MAX_FILE_ROWS;
}
inline bool parse_filemeta(Reader &r,FileMeta &meta) {
  int last=0,id=0,type=0;
  while(r.ok && r.field(last,id,type)) {
    if(id==2 && type==9) {
      int subtype=0;size_t count=0;
      if(!r.list(subtype,count) || subtype!=12 || count>257)return false;
      for(size_t i=0;i<count;++i) {
        Schema s;
        if(!parse_schema(r,s))return false;
        meta.schema.push_back(s);
      }
    } else if(id==3 && type==6)meta.num_rows=static_cast<uint64_t>(r.zig());
    else if(id==4 && type==9) {
      int subtype=0;size_t count=0;
      if(!r.list(subtype,count) || subtype!=12 || count>MAX_FILE_ROWS)return false;
      for(size_t i=0;i<count;++i) {
        RowGroup g;
        if(!parse_rowgroup(r,g))return false;
        meta.groups.push_back(g);
      }
    } else r.skip(type);
  }
  return r.ok && meta.schema.size()>=2 &&
         meta.num_rows<=MAX_FILE_ROWS && !meta.groups.empty();
}
inline bool parse_page(Reader &r,Page &page) {
  int last=0,id=0,type=0;
  while(r.ok && r.field(last,id,type)) {
    if(id==1 && type==5)page.type=static_cast<int>(r.zig());
    else if(id==2 && type==5)page.raw_size=static_cast<uint64_t>(r.zig());
    else if(id==3 && type==5)page.compressed_size=static_cast<uint64_t>(r.zig());
    else if(id==5 && type==12) {
      int inner_last=0,inner_id=0,inner_type=0;
      while(r.ok && r.field(inner_last,inner_id,inner_type)) {
        if(inner_id==1 && inner_type==5)page.values=static_cast<uint64_t>(r.zig());
        else if(inner_id==2 && inner_type==5)page.encoding=static_cast<int>(r.zig());
        else if(inner_id==3 && inner_type==5)page.level_encoding=static_cast<int>(r.zig());
        else if(inner_id==4 && inner_type==5)page.rep_encoding=static_cast<int>(r.zig());
        else r.skip(inner_type);
      }
    } else if(id==7 && type==12) {
      int inner_last=0,inner_id=0,inner_type=0;
      while(r.ok && r.field(inner_last,inner_id,inner_type)) {
        if(inner_id==1 && inner_type==5)page.values=static_cast<uint64_t>(r.zig());
        else if(inner_id==2 && inner_type==5)page.encoding=static_cast<int>(r.zig());
        else r.skip(inner_type);
      }
    } else if(id==8 && type==12) {
      int inner_last=0,inner_id=0,inner_type=0;
      while(r.ok && r.field(inner_last,inner_id,inner_type)) {
        if(inner_id==1 && inner_type==5)page.values=static_cast<uint64_t>(r.zig());
        else if(inner_id==4 && inner_type==5)page.encoding=static_cast<int>(r.zig());
        else if(inner_id==5 && inner_type==5)page.def_length=static_cast<uint64_t>(r.zig());
        else if(inner_id==6 && inner_type==5)page.rep_length=static_cast<uint64_t>(r.zig());
        else if(inner_id==7 && (inner_type==1 || inner_type==2))
          page.values_compressed=inner_type==1;
        else r.skip(inner_type);
      }
    } else r.skip(type);
  }
  return r.ok && page.type>=0 &&
         page.compressed_size<=MAX_FILE_BYTES &&
         page.raw_size<=MAX_FILE_BYTES && page.values<=MAX_FILE_ROWS;
}

inline bool snappy_decode(const std::string &src,
                          size_t limit,std::string &out) {
  Reader r(src.data(),src.size());
  const uint64_t declared=r.uvar();
  if(!r.ok || declared>limit)return false;
  out.clear();
  out.reserve(static_cast<size_t>(declared));
  while(r.ok && r.pos<r.len && out.size()<declared) {
    const unsigned char tag=r.byte();
    const unsigned kind=tag&3;
    if(kind==0) {
      size_t n=(tag>>2)+1;
      if((tag>>2)>=60) {
        const size_t width=(tag>>2)-59;
        n=static_cast<size_t>(r.le(width))+1;
      }
      if(!r.ok || !r.can(n) || n>declared-out.size())return false;
      out.append(reinterpret_cast<const char*>(r.data+r.pos),n);
      r.pos+=n;
    } else {
      size_t n=0;
      size_t offset=0;
      if(kind==1) {
        n=4+((tag>>2)&7);
        offset=(static_cast<size_t>(tag&0xe0)<<3)|r.byte();
      } else if(kind==2) {
        n=1+(tag>>2);
        offset=static_cast<size_t>(r.le(2));
      } else {
        n=1+(tag>>2);
        offset=static_cast<size_t>(r.le(4));
      }
      if(!r.ok || offset==0 || offset>out.size() ||
         n>declared-out.size())return false;
      for(size_t i=0;i<n;++i)
        out.push_back(out[out.size()-offset]);
    }
  }
  return r.ok && r.pos==r.len && out.size()==declared;
}
inline bool decompressed(const std::string &src,int codec,size_t raw,
                          std::string &out) {
  if(raw>MAX_FILE_BYTES || src.size()>MAX_FILE_BYTES)return false;
  if(codec==0)out=src;
  else if(codec==1) {
    if(!snappy_decode(src,MAX_FILE_BYTES,out))return false;
  }else return false;
  return out.size()==raw;
}
inline bool hybrid_decode(const std::string &src,unsigned width,
                          size_t count,std::vector<uint32_t> &out) {
  if(width>32 || count>MAX_FILE_ROWS)return false;
  Reader r(src.data(),src.size());
  out.clear();
  while(r.ok && r.pos<r.len && out.size()<count) {
    const uint64_t header=r.uvar();
    if(!r.ok || header==0)return false;
    if((header&1)==0) {
      const uint64_t n=header>>1;
      if(n==0 || n>MAX_FILE_ROWS || n>count-out.size())return false;
      const uint32_t value=static_cast<uint32_t>(r.le((width+7)/8));
      if(!r.ok || (width<32 && value >= (uint64_t(1)<<width)))return false;
      out.insert(out.end(),static_cast<size_t>(n),value);
    }else {
      const uint64_t groups=header>>1;
      if(groups==0 || groups>MAX_FILE_ROWS)return false;
      const size_t bytes=static_cast<size_t>(groups)*width;
      if(!r.can(bytes))return false;
      const size_t n=std::min(static_cast<size_t>(groups)*8,count-out.size());
      for(size_t k=0;k<n;++k) {
        const size_t bit=k*width;
        uint32_t value=0;
        for(unsigned j=0;j<width;++j) {
          if((r.data[r.pos+(bit+j)/8]>>((bit+j)%8))&1)
            value|=(uint32_t(1)<<j);
        }
        out.push_back(value);
      }
      r.pos+=bytes;
    }
  }
  return r.ok && out.size()==count;
}
inline std::string iso_date(int64_t days) {
  days+=719468;
  const int64_t era=days>=0?days/146097:(days-146096)/146097;
  const uint64_t doe=static_cast<uint64_t>(days-era*146097);
  const uint64_t yoe=(doe-doe/1460+doe/36524-doe/146096)/365;
  int64_t year=static_cast<int64_t>(yoe)+era*400;
  const uint64_t doy=doe-(365*yoe+yoe/4-yoe/100);
  const uint64_t mp=(5*doy+2)/153;
  const uint64_t day=doy-(153*mp+2)/5+1;
  const uint64_t month=mp+(mp<10?3:-9);
  year+=(month<=2);
  char buffer[50];
  std::snprintf(buffer,sizeof(buffer),"%04lld-%02llu-%02llu",
                static_cast<long long>(year),
                static_cast<unsigned long long>(month),
                static_cast<unsigned long long>(day));
  return buffer;
}
inline std::string iso_datetime(int64_t millis) {
  int64_t days=millis/86400000LL;
  int64_t rem=millis%86400000LL;
  if(rem<0){rem+=86400000LL;--days;}
  char buffer[64];
  std::snprintf(buffer,sizeof(buffer),"%02lld:%02lld:%02lld",
    static_cast<long long>(rem/3600000),
    static_cast<long long>((rem/60000)%60),
    static_cast<long long>((rem/1000)%60));
  return iso_date(days)+" "+buffer;
}
inline std::string parquet_sql_type(const Schema &s) {
  if(s.converted==6)return "DATE";
  if(s.converted==9)return "DATETIME";
  if(s.type==0)return "TINYINT";
  if(s.type==1 || s.type==2)return "BIGINT";
  if(s.type==4 || s.type==5)return "DOUBLE";
  return "VARCHAR(4096)";
}
inline bool plain_read(Reader &r,int type,int converted,
                       uint64_t &bool_position,std::string &value) {
  if(type==0) {
    // Plain BOOLEAN uses one bit/value, 8 values per byte.
    // Bool state is tracked by bit index; byte must be consumed only for
    // first bit of each octet.
    if((bool_position%8)==0) {
      if(!r.can(1))return false;
    }
    if(r.pos==0)return false;
    // Use the next data byte without advancing until all 8 bits consumed.
    const unsigned char byte=r.data[r.pos];
    value=((byte>>(bool_position%8))&1)?"1":"0";
    ++bool_position;
    if((bool_position%8)==0)++r.pos;
    return true;
  } else if(type==1) {
    value=std::to_string(static_cast<int32_t>(r.le(4)));
    if(converted==6)value=json_quote(iso_date(static_cast<int32_t>(
                              std::strtol(value.c_str(),nullptr,10))));
    return r.ok;
  } else if(type==2) {
    const int64_t n=static_cast<int64_t>(r.le(8));
    value=converted==9?json_quote(iso_datetime(n)):std::to_string(n);
    return r.ok;
  } else if(type==4 || type==5) {
    const uint64_t bits=r.le(type==4?4:8);
    if(!r.ok)return false;
    double f=0;
    if(type==4) {
      const uint32_t b=static_cast<uint32_t>(bits);
      float f32=0;std::memcpy(&f32,&b,sizeof(f32));
      f=f32;
    } else std::memcpy(&f,&bits,sizeof(f));
    if(!std::isfinite(f))value="null";
    else {
      char buffer[64];
      std::snprintf(buffer,sizeof(buffer),type==4?"%.9g":"%.17g",f);
      value=buffer;
    }
    return true;
  } else if(type==6) {
    const uint64_t len=r.le(4);
    if(!r.ok || len>MAX_FILE_BYTES || !r.can(static_cast<size_t>(len)))
      return false;
    std::string str(reinterpret_cast<const char*>(r.data+r.pos),
                    static_cast<size_t>(len));
    r.pos+=static_cast<size_t>(len);
    value=json_quote(str);
    return true;
  }
  return false;
}
inline bool decode_values(const std::string &data,int physical,int converted,
                          size_t count,std::vector<std::string> &out) {
  Reader r(data.data(),data.size());
  uint64_t bool_index=0;
  for(size_t i=0;i<count;++i) {
    std::string value;
    if(physical==0) {
      // Boolean bit-packed values need per-byte offset handling.
      if((i%8)==0 && !r.can(1))return false;
      value=((r.data[r.pos]>>(i%8))&1)?"1":"0";
      if(i%8==7 || i+1==count)++r.pos;
    } else if(!plain_read(r,physical,converted,bool_index,value))
      return false;
    out.push_back(value);
  }
  return r.ok;
}
inline bool page_values(const std::string &bytes,const Page &page,
                        const Column &column,const Schema &schema,
                        const std::vector<std::string> &dictionary,
                        std::vector<std::string> &values,
                        std::string &err) {
  std::string raw;
  const bool v2=page.type==3;
  if(v2 && page.def_length+page.rep_length<=bytes.size()) {
    const size_t levels_length=static_cast<size_t>(page.def_length+page.rep_length);
    const std::string initial=bytes.substr(0,levels_length);
    const std::string encoded=bytes.substr(levels_length);
    std::string decoded;
    if(page.values_compressed) {
      if(!decompressed(encoded,column.codec,
                       static_cast<size_t>(page.raw_size-levels_length),
                       decoded)) {
        err="Parquet v2 page decompression failed";return false;
      }
    } else decoded=encoded;
    raw=initial+decoded;
  } else if(!decompressed(bytes,column.codec,
                          static_cast<size_t>(page.raw_size),raw)) {
    err="Parquet page decompression failed";return false;
  }
  if(raw.size()!=page.raw_size){err="Parquet page length mismatch";return false;}
  size_t offset=0;
  std::vector<uint32_t> levels;
  if(schema.repetition==1) {
    std::string encoded;
    if(v2) {
      if(page.rep_length!=0 || page.def_length>raw.size()) {
        err="unsupported Parquet nested/repeated levels";return false;
      }
      encoded=raw.substr(0,static_cast<size_t>(page.def_length));
      offset=static_cast<size_t>(page.def_length);
    } else {
      Reader r(raw.data(),raw.size());
      const uint64_t length=r.le(4);
      if(!r.ok || length>raw.size()-r.pos) {
        err="malformed Parquet definition levels";return false;
      }
      encoded=raw.substr(r.pos,static_cast<size_t>(length));
      offset=r.pos+static_cast<size_t>(length);
    }
    if(!hybrid_decode(encoded,1,static_cast<size_t>(page.values),levels)){
      err="invalid Parquet definition levels";return false;
    }
  } else if(v2)offset=static_cast<size_t>(page.def_length+page.rep_length);
  size_t defined=static_cast<size_t>(page.values);
  if(schema.repetition==1) {
    defined=0;
    for(size_t i=0;i<levels.size();++i)defined+=levels[i]==1;
  }
  const std::string body=raw.substr(offset);
  std::vector<std::string> decoded;
  if(page.encoding==0) {
    if(!decode_values(body,column.type,schema.converted,defined,decoded)){
      err="unsupported or malformed Parquet PLAIN values";return false;
    }
  } else if(page.encoding==2 || page.encoding==8) {
    if(dictionary.empty() && defined>0) {
      err="Parquet dictionary is missing";return false;
    }
    if(body.empty() && defined>0)return false;
    std::vector<uint32_t> ids;
    if(defined>0 && !hybrid_decode(body.substr(1),
           static_cast<unsigned char>(body[0]),defined,ids)){
      err="Parquet dictionary indexes invalid";return false;
    }
    for(size_t i=0;i<ids.size();++i) {
      if(ids[i]>=dictionary.size()){err="Parquet bad dictionary index";return false;}
      decoded.push_back(dictionary[ids[i]]);
    }
  } else {
    err="unsupported Parquet page encoding";return false;
  }
  size_t j=0;
  for(size_t i=0;i<page.values;++i){
    if(schema.repetition==1 && levels[i]==0)values.push_back("null");
    else {
      if(j>=decoded.size()){err="Parquet value count mismatch";return false;}
      values.push_back(decoded[j++]);
    }
  }
  return true;
}

inline std::string make_token(const std::string &path,const std::vector<std::string> &columns={}) {
  static const char hex[]="0123456789abcdef";
  std::string out="[{\"__seekdb_parquet_stream_v1__\":\"";
  for(unsigned char c:path){out.push_back(hex[c>>4]);out.push_back(hex[c&15]);}
  out+="\",\"projection\":\"";
  for(size_t i=0;i<columns.size();++i) {
    if(i)out+=':';
    for(unsigned char c:columns[i]){out.push_back(hex[c>>4]);out.push_back(hex[c&15]);}
  }
  return out+"\"}]";
}
inline bool is_token(const std::string &value) {
  return value.compare(0,std::string("[{\"__seekdb_parquet_stream_v1__\":\"").size(),"[{\"__seekdb_parquet_stream_v1__\":\"")==0;
}
inline bool parse_token(const std::string &value,std::string &path,std::vector<std::string> &columns) {
  const std::string prefix="[{\"__seekdb_parquet_stream_v1__\":\"";
  const std::string middle="\",\"projection\":\"";
  const size_t split=value.find(middle,prefix.size());
  if(value.compare(0,prefix.size(),prefix)||value.size()<prefix.size()+3||
     value.substr(value.size()-3)!="\"}]"||split==std::string::npos||(split-prefix.size())%2)return false;
  path.clear();
  const auto digit=[](char c)->int{return c>='0'&&c<='9'?c-'0':c>='a'&&c<='f'?c-'a'+10:-1;};
  for(size_t i=prefix.size();i<split;i+=2) {
    int a=digit(value[i]),b=digit(value[i+1]);if(a<0||b<0)return false;
    path.push_back(static_cast<char>((a<<4)|b));
  }
  columns.clear();
  size_t at=split+middle.size(),end=value.size()-3;
  while(at<end) {
    const size_t separator=value.find(':',at);
    const size_t stop=separator==std::string::npos?end:separator;
    if(stop>end||(stop-at)%2||stop==at)return false;
    std::string column;
    for(size_t i=at;i<stop;i+=2) {
      int a=digit(value[i]),b=digit(value[i+1]);if(a<0||b<0)return false;
      column.push_back(static_cast<char>((a<<4)|b));
    }
    columns.push_back(column);at=stop+1;
  }
  return !path.empty()&&path[0]=='/'&&path.find(char(0))==std::string::npos;
}

class Cursor {
public:
  Cursor():fd_(-1),footer_start_(0),group_(0),row_(0),meta_(),identity_(){}
  ~Cursor(){close();}
  Cursor(const Cursor&)=delete;
  Cursor& operator=(const Cursor&)=delete;
  bool open(const std::string &root,const std::string &path,std::string &error) {
    close();
    char allowed[PATH_MAX]={},resolved[PATH_MAX]={};
    if(root.empty()||path.empty()||path.find('\0')!=std::string::npos||
       path.find("://")!=std::string::npos||!realpath(root.c_str(),allowed)) {
      error="invalid local Parquet path";return false;
    }
    const std::string request=path[0]=='/'?path:std::string(allowed)+"/"+path;
    if(!realpath(request.c_str(),resolved)){error="Parquet file does not exist";return false;}
    const std::string base=allowed;
    path_=resolved;
    if(base!="/" && !(path_.size()>base.size() && path_.compare(0,base.size(),base)==0 &&
                      path_[base.size()]=='/')) {
      error="Parquet path escapes allowed root";return false;
    }
    fd_=::open(path_.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
    if(fd_<0||fstat(fd_,&identity_)!=0||!S_ISREG(identity_.st_mode)||identity_.st_size<12) {
      error="invalid Parquet input";close();return false;
    }
    std::string magic,trailer,footer;
    if(!read(0,4,magic,error)||magic!="PAR1"||
       !read(identity_.st_size-8,8,trailer,error)||trailer.substr(4)!="PAR1") {
      error="invalid Parquet magic";close();return false;
    }
    Reader length(trailer.data(),4);
    const uint64_t bytes=length.le(4);
    if(!length.ok||bytes>MAX_FILE_BYTES||bytes>static_cast<uint64_t>(identity_.st_size-12)) {
      error="invalid or oversized Parquet footer";close();return false;
    }
    footer_start_=identity_.st_size-8-bytes;
    if(!read(footer_start_,bytes,footer,error)){close();return false;}
    Reader reader(footer.data(),footer.size());
    if(!parse_filemeta(reader,meta_)||meta_.schema.empty()||
       meta_.schema[0].children!=static_cast<int>(meta_.schema.size()-1)) {
      error="unsupported Parquet footer";close();return false;
    }
    uint64_t rows=0;
    for(size_t g=0;g<meta_.groups.size();++g) {
      const RowGroup &group=meta_.groups[g];
      if(group.columns.size()+1!=meta_.schema.size()){error="Parquet schema mismatch";close();return false;}
      rows+=group.rows;
    }
    if(rows!=meta_.num_rows){error="Parquet row count mismatch";close();return false;}
    for(size_t i=1;i<meta_.schema.size();++i) {
      const Schema &column=meta_.schema[i];
      if(column.children||column.repetition>1||column.type<0||
         column.type==3||column.type==7||column.type>6) {
        error="unsupported nested or specialized Parquet column";close();return false;
      }
      names_.push_back(column.name);
      types_.push_back(parquet_sql_type(column));
    }
    selected_.clear();
    for(size_t i=0;i<names_.size();++i)selected_.push_back(i);
    return rescan(error);
  }
  bool project(const std::vector<std::string>& columns,std::string &error) {
    if(columns.empty())return true;
    selected_.clear();
    for(const std::string &name:columns) {
      auto found=std::find(names_.begin(),names_.end(),name);
      if(found==names_.end()){error="unknown projected Parquet column";return false;}
      selected_.push_back(found-names_.begin());
    }
    return true;
  }
  bool rescan(std::string &error) {
    if(!unchanged(error))return false;
    group_=0;row_=0;states_.clear();
    return true;
  }
  bool next_json(std::string &json,size_t wanted,std::string &error) {
    if(!wanted||wanted>4096){error="invalid Parquet batch size";return false;}
    if(!unchanged(error))return false;
    json="[";
    size_t count=0;
    while(count<wanted && group_<meta_.groups.size()) {
      const RowGroup &group=meta_.groups[group_];
      if(row_>=group.rows){++group_;row_=0;states_.clear();continue;}
      if(states_.empty()) {
        states_.resize(names_.size());
        for(size_t c=0;c<names_.size();++c) {
          const Column &column=group.columns[c];
          if(column.type!=meta_.schema[c+1].type||column.num_values!=group.rows) {
            error="Parquet physical type or value count mismatch";return false;
          }
          states_[c].offset=column.has_dictionary?
              std::min(column.dictionary_offset,column.data_offset):column.data_offset;
        }
      }
      if(count++)json+=",";
      json+="{";
      for(size_t projection=0;projection<selected_.size();++projection) {
        const size_t c=selected_[projection];
        State &state=states_[c];
        while(state.position>=state.values.size()) {
          state.values.clear();state.position=0;
          if(!page(c,state,error))return false;
        }
        if(projection)json+=",";
        json+=json_quote(names_[c])+":"+state.values[state.position++];
        if(json.size()>MAX_FILE_BYTES){error="Parquet batch memory limit exceeded";return false;}
      }
      json+="}";++row_;
    }
    json+="]";
    return unchanged(error);
  }
  const std::vector<std::string>& names()const{return names_;}
  const std::vector<std::string>& types()const{return types_;}
  const std::string& canonical_path()const{return path_;}
  bool nullable(size_t i)const{return i+1<meta_.schema.size()&&meta_.schema[i+1].repetition==1;}
  void close() {
    if(fd_>=0)::close(fd_);
    fd_=-1;path_.clear();meta_=FileMeta();names_.clear();types_.clear();states_.clear();selected_.clear();
    group_=0;row_=0;footer_start_=0;
  }
private:
  struct State {
    uint64_t offset=0;
    uint64_t decoded=0;
    size_t position=0;
    std::vector<std::string> dictionary,values;
  };
  bool read(uint64_t offset,size_t bytes,std::string &out,std::string &error) {
    if(bytes>MAX_FILE_BYTES || offset>static_cast<uint64_t>(identity_.st_size) ||
       bytes>static_cast<uint64_t>(identity_.st_size)-offset) {
      error="Parquet range exceeds file or memory limit";return false;
    }
    out.resize(bytes);
    size_t done=0;
    while(done<bytes) {
      const ssize_t n=pread(fd_,&out[done],bytes-done,offset+done);
      if(n<0 && errno==EINTR)continue;
      if(n<=0){error="Parquet range read failed";return false;}
      done+=static_cast<size_t>(n);
    }
    return true;
  }
  bool unchanged(std::string &error)const {
    struct stat now,path;
    if(fd_<0||fstat(fd_,&now)||stat(path_.c_str(),&path)||
       now.st_dev!=identity_.st_dev||now.st_ino!=identity_.st_ino||
       now.st_size!=identity_.st_size||now.st_mtim.tv_sec!=identity_.st_mtim.tv_sec||
       now.st_mtim.tv_nsec!=identity_.st_mtim.tv_nsec||
       now.st_ctim.tv_sec!=identity_.st_ctim.tv_sec||now.st_ctim.tv_nsec!=identity_.st_ctim.tv_nsec||
       path.st_dev!=identity_.st_dev||path.st_ino!=identity_.st_ino) {
      error="Parquet input changed during scan";return false;
    }
    return true;
  }
  bool page(size_t col,State &state,std::string &error) {
    const RowGroup &group=meta_.groups[group_];
    const Column &column=group.columns[col];
    if(state.decoded>=group.rows){error="Parquet column ended before row group";return false;}
    for(size_t skipped=0;skipped<10000;++skipped) {
      if(state.offset<4||state.offset>=footer_start_){error="invalid Parquet page offset";return false;}
      std::string header,bytes,raw;
      Page parsed;size_t length=0;
      size_t probe=std::min<uint64_t>(65536,footer_start_-state.offset);
      if(!read(state.offset,probe,header,error))return false;
      Reader r(header.data(),header.size());
      if(!parse_page(r,parsed)){error="invalid or oversized Parquet page header";return false;}
      length=r.pos;
      if(parsed.compressed_size>footer_start_-state.offset-length) {
        error="Parquet page crosses footer";return false;
      }
      if(!read(state.offset+length,static_cast<size_t>(parsed.compressed_size),bytes,error))return false;
      state.offset+=length+parsed.compressed_size;
      if(parsed.type==2) {
        if(parsed.encoding!=0||!decompressed(bytes,column.codec,parsed.raw_size,raw)||
           !decode_values(raw,column.type,meta_.schema[col+1].converted,parsed.values,state.dictionary)) {
          error="invalid Parquet dictionary page";return false;
        }
      } else if(parsed.type==0||parsed.type==3) {
        if(parsed.values>group.rows-state.decoded){error="Parquet page has excess rows";return false;}
        if(!page_values(bytes,parsed,column,meta_.schema[col+1],state.dictionary,state.values,error))return false;
        state.decoded+=parsed.values;
        if(!bounded(error))return false;
        if(!state.values.empty())return true;
      }
    }
    error="too many non-data Parquet pages";return false;
  }
  bool bounded(std::string &error)const {
    size_t bytes=0;
    for(const State &state:states_) {
      for(const std::string &value:state.dictionary)bytes+=sizeof(std::string)+value.capacity();
      for(const std::string &value:state.values)bytes+=sizeof(std::string)+value.capacity();
      if(bytes>64u*1024u*1024u){error="Parquet page working set exceeds 64 MiB";return false;}
    }
    return true;
  }
  int fd_;
  uint64_t footer_start_;
  size_t group_;
  uint64_t row_;
  FileMeta meta_;
  struct stat identity_;
  std::string path_;
  std::vector<std::string> names_,types_;
  std::vector<size_t> selected_;
  std::vector<State> states_;
};

}}}
