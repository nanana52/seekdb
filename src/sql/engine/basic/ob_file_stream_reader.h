/*
 * Copyright (c) 2026.
 * Bounded native seekdb file reader. This is an execution-layer primitive:
 * no complete-file strings, no JSON_TABLE rewriting, no SQL implementation.
 * Integration into a physical FileScan operator is still required.
 */
#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_STREAM_READER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_STREAM_READER_H_
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace oceanbase { namespace sql { namespace file_stream {

struct FileBatch {
  std::vector<std::vector<std::string> > rows;
  void clear() { rows.clear(); }
};
enum FileFormat { CSV, JSONL, JSON_ARRAY };

struct StreamOptions {
  FileFormat format;
  bool header;
  size_t max_columns;
  size_t max_field_bytes;
  size_t max_row_bytes;
  size_t max_batch_rows;
  StreamOptions():format(CSV),header(true),max_columns(256),
      max_field_bytes(4096),max_row_bytes(65536),max_batch_rows(512){}
};

class FileStreamReader {
public:
  FileStreamReader():fd_(-1),first_(0),last_(0),eof_(false),consumed_(false),
     bytes_returned_(0),first_row_pending_(false),array_after_item_(false),
     array_closed_(false),identity_(),opts_() {}
  ~FileStreamReader() { close(); }
  FileStreamReader(const FileStreamReader &)=delete;
  FileStreamReader &operator=(const FileStreamReader &)=delete;

  bool open(const std::string &allowed_root,const std::string &filename,
            const StreamOptions &options,std::string &err) {
    close();
    if((options.format!=CSV && options.format!=JSONL &&
        options.format!=JSON_ARRAY) ||
       options.max_columns==0 || options.max_columns>1024 ||
       options.max_field_bytes==0 || options.max_field_bytes>(1u<<20) ||
       options.max_row_bytes<options.max_field_bytes ||
       options.max_row_bytes>(16u<<20) ||
       options.max_batch_rows==0 || options.max_batch_rows>4096) {
      err="invalid stream resource limits";return false;
    }
    if(allowed_root.empty() || filename.empty() ||
       filename.find('\0')!=std::string::npos ||
       filename.find("://")!=std::string::npos) {
      err="a local filename and a nonempty allowed root are required";
      return false;
    }
    char resolved_root[PATH_MAX]={};
    char resolved_file[PATH_MAX]={};
    if(realpath(allowed_root.c_str(),resolved_root)==nullptr) {
      err="invalid file root";return false;
    }
    std::string requested=filename[0]=='/'?filename:
      std::string(resolved_root)+"/"+filename;
    if(realpath(requested.c_str(),resolved_file)==nullptr) {
      err="file does not exist";return false;
    }
    root_=resolved_root;
    path_=resolved_file;
    if(root_!="/" &&
       !(path_.size()>root_.size() &&
         path_.compare(0,root_.size(),root_)==0 &&
         path_[root_.size()]=='/')) {
      err="file path escapes allowed root";close();return false;
    }
    fd_=::open(path_.c_str(),O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if(fd_<0){err=std::string("cannot open file: ")+std::strerror(errno);
      close();return false;}
    if(::fstat(fd_,&identity_)!=0 || !S_ISREG(identity_.st_mode)) {
      err="file is not regular";close();return false;
    }
    opts_=options;
    if(!rewind_reader(err)) {
      close();return false;
    }
    return true;
  }

  bool next_batch(FileBatch &batch,size_t wanted,std::string &err) {
    batch.clear();
    if(fd_<0){err="scan not open";return false;}
    if(wanted==0 || wanted>opts_.max_batch_rows) {
      err="batch size out of range";return false;
    }
    if(!check_same_file(err))return false;
    while(batch.rows.size()<wanted) {
      std::vector<std::string> row;
      if(first_row_pending_) {
        row.swap(pending_row_);
        first_row_pending_=false;
      } else {
        bool has_row=false;
        if(opts_.format==CSV) {
          if(!csv_row(row,has_row,err))return false;
        } else if(opts_.format==JSON_ARRAY) {
          if(!json_array_row(row,has_row,err))return false;
        } else {
          if(!jsonl_row(row,has_row,err))return false;
        }
        if(!has_row)break;
      }
      if(opts_.format==CSV && row.size()!=column_names_.size()) {
        err="CSV row has different column count";return false;
      }
      batch.rows.push_back(std::move(row));
    }
    return check_same_file(err);
  }

  bool rescan(std::string &err) {
    if(fd_<0){err="scan not open";return false;}
    if(!check_same_file(err))return false;
    return rewind_reader(err);
  }

  bool end_of_file() const {return consumed_ && !first_row_pending_;}
  const std::vector<std::string> &columns() const {return column_names_;}
  uint64_t bytes_returned() const {return bytes_returned_;}
  const std::string &canonical_path() const {return path_;}

  void close() {
    if(fd_>=0)::close(fd_);
    fd_=-1;first_=last_=0;eof_=consumed_=false;
    array_after_item_=array_closed_=false;
    first_row_pending_=false;
    bytes_returned_=0;
    root_.clear();path_.clear();
    column_names_.clear();pending_row_.clear();
  }

private:
  static bool is_space(char c) {
    return c==' ' || c=='\t' || c=='\n' || c=='\r';
  }
  static bool valid_utf8(const std::string &value) {
    const unsigned char *s=reinterpret_cast<const unsigned char *>(value.data());
    size_t i=0,n=value.size();
    while(i<n) {
      const unsigned c=s[i++];
      if(c<0x80)continue;
      uint32_t cp=0,minimum=0;
      size_t needed=0;
      if((c&0xe0)==0xc0){needed=1;minimum=0x80;cp=c&0x1f;}
      else if((c&0xf0)==0xe0){needed=2;minimum=0x800;cp=c&0x0f;}
      else if((c&0xf8)==0xf0){needed=3;minimum=0x10000;cp=c&0x07;}
      else return false;
      if(needed>n-i)return false;
      while(needed--) {
        if((s[i]&0xc0)!=0x80)return false;
        cp=(cp<<6)|(s[i++]&0x3f);
      }
      if(cp<minimum || cp>0x10ffff || (cp>=0xd800 && cp<=0xdfff))
        return false;
    }
    return true;
  }
  static bool same_identity(const struct stat &a,const struct stat &b) {
    return a.st_dev==b.st_dev &&
      a.st_ino==b.st_ino &&
      a.st_size==b.st_size &&
      a.st_mtim.tv_sec==b.st_mtim.tv_sec &&
      a.st_mtim.tv_nsec==b.st_mtim.tv_nsec &&
      a.st_ctim.tv_sec==b.st_ctim.tv_sec &&
      a.st_ctim.tv_nsec==b.st_ctim.tv_nsec;
  }
  bool check_same_file(std::string &err) const {
    struct stat now;
    struct stat linked;
    if(fd_<0 || ::fstat(fd_,&now)!=0 ||
       ::stat(path_.c_str(),&linked)!=0 ||
       !same_identity(identity_,now) || !same_identity(identity_,linked)) {
      err="file changed during scan or was replaced";
      return false;
    }
    return true;
  }
  int peek(std::string &err) {
    if(first_<last_)return static_cast<unsigned char>(buffer_[first_]);
    if(eof_)return -1;
    for(;;) {
      const ssize_t count=::read(fd_,buffer_,sizeof(buffer_));
      if(count<0 && errno==EINTR)continue;
      if(count<0) {
        err=std::string("file read failed: ")+std::strerror(errno);
        return -2;
      }
      if(count==0){eof_=true;return -1;}
      first_=0;last_=static_cast<size_t>(count);
      return static_cast<unsigned char>(buffer_[first_]);
    }
  }
  int consume(std::string &err) {
    const int c=peek(err);
    if(c>=0){++first_;++bytes_returned_;}
    return c;
  }
  bool rewind_reader(std::string &err) {
    if(::lseek(fd_,0,SEEK_SET)<0){
      err="file cannot be rewound";return false;
    }
    first_=last_=0;eof_=consumed_=false;
    array_after_item_=array_closed_=false;
    first_row_pending_=false;pending_row_.clear();column_names_.clear();
    bytes_returned_=0;
    if(opts_.format==CSV) {
      std::vector<std::string> header_row;
      bool has_row=false;
      if(!csv_row(header_row,has_row,err))return false;
      if(!has_row){err="CSV is empty";return false;}
      if(header_row.size()>opts_.max_columns){
        err="CSV has too many columns";return false;
      }
      if(!header_row.empty() && header_row[0].size()>=3 &&
         static_cast<unsigned char>(header_row[0][0])==0xef &&
         static_cast<unsigned char>(header_row[0][1])==0xbb &&
         static_cast<unsigned char>(header_row[0][2])==0xbf)
        header_row[0].erase(0,3);
      if(opts_.header) {
        column_names_=header_row;
        for(size_t i=0;i<column_names_.size();++i) {
          const std::string base=column_names_[i].empty()?
            "column_"+std::to_string(i+1):column_names_[i];
          std::string candidate=base;
          for(size_t n=2;;++n) {
            if(std::find(column_names_.begin(),
                         column_names_.begin()+static_cast<std::ptrdiff_t>(i),
                         candidate)==
               column_names_.begin()+static_cast<std::ptrdiff_t>(i))break;
            candidate=base+"__"+std::to_string(n);
          }
          column_names_[i]=candidate;
        }
      } else {
        pending_row_=header_row;
        first_row_pending_=true;
        for(size_t i=0;i<header_row.size();++i)
          column_names_.push_back("column"+std::to_string(i));
      }
    }
    if(opts_.format==JSON_ARRAY) {
      // The array envelope is read incrementally; never materialize the file.
      int c=-1;
      if(!array_skip_ws(c,err))return false;
      if(c!='[') {err="JSON array must start with '['";return false;}
      (void)consume(err);
    }
    return check_same_file(err);
  }

  bool array_skip_ws(int &c,std::string &err) {
    for(;;) {
      c=peek(err);
      if(c==-2)return false;
      if(c<0 || !is_space(static_cast<char>(c)))return true;
      (void)consume(err);
    }
  }

  bool json_array_row(std::vector<std::string> &row,bool &has_row,
                      std::string &err) {
    row.clear();has_row=false;
    if(array_closed_)return true;
    int c=-1;
    if(!array_skip_ws(c,err))return false;
    if(array_after_item_) {
      if(c==']') {
        (void)consume(err);
        array_closed_=consumed_=true;
        if(!array_skip_ws(c,err))return false;
        if(c!=-1) {err="JSON array has trailing content";return false;}
        return true;
      }
      if(c!=',') {err="JSON array requires a comma";return false;}
      (void)consume(err);
      if(!array_skip_ws(c,err))return false;
    } else if(c==']') {
      (void)consume(err);
      array_closed_=consumed_=true;
      if(!array_skip_ws(c,err))return false;
      if(c!=-1) {err="JSON array has trailing content";return false;}
      return true;
    }
    if(c!='{') {err="JSON array requires flat JSON objects";return false;}
    std::string object;
    bool quoted=false,escaped=false;
    int depth=0;
    for(;;) {
      c=consume(err);
      if(c==-2)return false;
      if(c==-1) {err="unterminated JSON array object";return false;}
      if(object.size()>=opts_.max_row_bytes) {
        err="JSON array object exceeds row limit";return false;
      }
      const char v=static_cast<char>(c);
      object.push_back(v);
      if(quoted) {
        if(escaped)escaped=false;
        else if(v=='\\')escaped=true;
        else if(v=='"')quoted=false;
      } else if(v=='"')quoted=true;
      else if(v=='{')++depth;
      else if(v=='}') {
        if(--depth==0)break;
        if(depth<0) {err="JSON array has unmatched brace";return false;}
      }
    }
    if(!valid_utf8(object) || !json_flat_object(object)) {
      err="JSON array must contain flat UTF-8 JSON objects";return false;
    }
    row.push_back(std::move(object));
    array_after_item_=true;
    has_row=true;
    return true;
  }

  bool csv_row(std::vector<std::string> &row,bool &has_row,
               std::string &err) {
    row.clear();has_row=false;
    std::string field;
    bool quoting=false,closed=false,started=false;
    size_t row_bytes=0;
    for(;;) {
      const int cp=consume(err);
      if(cp==-2)return false;
      if(cp==-1) {
        if(quoting){err="CSV ended inside quoted field";return false;}
        consumed_=true;
        if(started || !field.empty() || !row.empty()) {
          if(field.size()>opts_.max_field_bytes || !valid_utf8(field)){
            err="CSV final field exceeds limits or is invalid UTF-8";
            return false;
          }
          row.push_back(field);
          has_row=true;
          return true;
        }
        return true;
      }
      char c=static_cast<char>(cp);
      if(++row_bytes>opts_.max_row_bytes){
        err="CSV record exceeds row limit";return false;
      }
      if(quoting) {
        if(c=='"') {
          const int next=peek(err);
          if(next==-2)return false;
          if(next=='"') {
            (void)consume(err);
            if(++row_bytes>opts_.max_row_bytes){
              err="CSV record exceeds row limit";return false;
            }
            field.push_back('"');
          } else {
            quoting=false;closed=true;
          }
        } else field.push_back(c);
      } else if(c=='"' && !started && field.empty() && !closed) {
        quoting=true;started=true;
      } else if(c==',' || c=='\n' || c=='\r') {
        if(field.size()>opts_.max_field_bytes || !valid_utf8(field)){
          err="CSV field exceeds limits or is invalid UTF-8";return false;
        }
        if(c=='\r') {
          const int next=peek(err);
          if(next==-2)return false;
          if(next=='\n') (void)consume(err);
        }
        row.push_back(field);
        field.clear();
        closed=false;started=false;
        if(row.size()>opts_.max_columns){
          err="CSV column count exceeds limit";return false;
        }
        if(c==',' )continue;
        has_row=true;
        return true;
      } else if(c=='"' || closed) {
        err="CSV has invalid quote escaping";return false;
      } else {
        started=true;
        field.push_back(c);
      }
      if(field.size()>opts_.max_field_bytes) {
        err="CSV field exceeds configured limit";return false;
      }
    }
  }

  static bool json_string(const std::string &line,size_t &pos) {
    if(pos==line.size() || line[pos]!='"')return false;
    ++pos;
    while(pos<line.size()) {
      const unsigned char c=static_cast<unsigned char>(line[pos++]);
      if(c=='"')return true;
      if(c<32)return false;
      if(c!='\\')continue;
      if(pos==line.size())return false;
      const char esc=line[pos++];
      if(esc=='u'){
        if(pos+4>line.size())return false;
        for(int i=0;i<4;++i){
          const char v=line[pos++];
          if(!((v>='0' && v<='9') ||
               (v>='A' && v<='F') ||
               (v>='a' && v<='f')))return false;
        }
      }else if(esc!='"' && esc!='\\' && esc!='/' && esc!='b' &&
               esc!='f' && esc!='n' && esc!='r' && esc!='t')return false;
    }
    return false;
  }
  static void json_ws(const std::string &s,size_t &p) {
    while(p<s.size() && is_space(s[p]))++p;
  }
  static bool json_number(const std::string &s,size_t &p) {
    const size_t start=p;
    if(p<s.size() && s[p]=='-')++p;
    if(p==s.size())return false;
    if(s[p]=='0')++p;
    else if(s[p]>='1' && s[p]<='9') {
      while(p<s.size() && s[p]>='0' && s[p]<='9')++p;
    }else return false;
    if(p<s.size() && s[p]=='.') {
      ++p;
      const size_t n=p;
      while(p<s.size() && s[p]>='0' && s[p]<='9')++p;
      if(n==p)return false;
    }
    if(p<s.size() && (s[p]=='e' || s[p]=='E')) {
      ++p;
      if(p<s.size() && (s[p]=='+' || s[p]=='-'))++p;
      const size_t n=p;
      while(p<s.size() && s[p]>='0' && s[p]<='9')++p;
      if(n==p)return false;
    }
    return p>start;
  }
  static bool json_flat_object(const std::string &line) {
    size_t p=0;json_ws(line,p);
    if(p>=line.size() || line[p++]!='{')return false;
    json_ws(line,p);
    if(p<line.size() && line[p]=='}') {
      ++p;json_ws(line,p);return p==line.size();
    }
    while(p<line.size()){
      if(!json_string(line,p))return false;
      json_ws(line,p);
      if(p>=line.size() || line[p++]!=':')return false;
      json_ws(line,p);
      if(p>=line.size())return false;
      if(line[p]=='"') {
        if(!json_string(line,p))return false;
      } else if(line[p]=='t' && line.compare(p,4,"true")==0) p+=4;
      else if(line[p]=='f' && line.compare(p,5,"false")==0) p+=5;
      else if(line[p]=='n' && line.compare(p,4,"null")==0) p+=4;
      else if(!json_number(line,p))return false;
      json_ws(line,p);
      if(p==line.size())return false;
      if(line[p]=='}'){
        ++p;json_ws(line,p);return p==line.size();
      }
      if(line[p++]!=',')return false;
      json_ws(line,p);
    }
    return false;
  }
  bool jsonl_row(std::vector<std::string> &row,bool &has_row,
                 std::string &err) {
    row.clear();has_row=false;
    for(;;) {
      std::string line;
      for(;;) {
        const int cp=consume(err);
        if(cp==-2)return false;
        if(cp==-1) {
          consumed_=true;break;
        }
        if(cp=='\n')break;
        line.push_back(static_cast<char>(cp));
        if(line.size()>opts_.max_row_bytes){
          err="JSONL record exceeds row limit";return false;
        }
      }
      if(!line.empty() && line.back()=='\r')line.pop_back();
      if(line.empty()){
        if(consumed_)return true;
        continue;
      }
      if((bytes_returned_==line.size()+1 || bytes_returned_==line.size() ||
          bytes_returned_==line.size()+2) &&
         line.size()>=3 &&
         static_cast<unsigned char>(line[0])==0xef &&
         static_cast<unsigned char>(line[1])==0xbb &&
         static_cast<unsigned char>(line[2])==0xbf)
        line.erase(0,3);
      if(!valid_utf8(line) || !json_flat_object(line)){
        err="JSONL must contain flat UTF-8 JSON objects";return false;
      }
      row.push_back(std::move(line));
      has_row=true;
      return true;
    }
  }

  int fd_;
  size_t first_,last_;
  bool eof_,consumed_;
  uint64_t bytes_returned_;
  bool first_row_pending_;
  bool array_after_item_;
  bool array_closed_;
  struct stat identity_;
  StreamOptions opts_;
  std::string root_,path_;
  std::vector<std::string> column_names_,pending_row_;
  char buffer_[65536];
};

}}} // oceanbase::sql::file_stream
#endif
