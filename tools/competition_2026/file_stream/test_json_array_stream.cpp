#include "sql/engine/basic/ob_file_scan_core.h"
#include "sql/parser/ob_file_sql_rewrite.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

using namespace oceanbase::sql::file_stream;
static void must(bool ok,const std::string &why) {if(!ok){std::cerr<<why<<"\n";std::exit(3);}}
static std::string path(const std::string &root,const char *name) {return root+"/"+name;}
static void write(const std::string &f,const std::string &s) {std::ofstream out(f.c_str());out<<s;out.close();}
int main() {
 char temp[]="/tmp/seekdb-json-array-XXXXXX";
 char *rootp=mkdtemp(temp);must(rootp!=nullptr,"mkdtemp");
 const std::string root(rootp),large=path(root,"large.json");
 {
   std::ofstream f(large.c_str());f<<"[\n";
   for(int i=1;i<=80000;++i){
     if(i>1)f<<",\n";
     f<<"{\"id\":"<<i<<",\"grp\":"<<(i%10)<<",\"s\":\""<<std::string(50,'x')<<"\"}";
   }
   f<<"\n]  \n";
 }
 struct stat st;must(::stat(large.c_str(),&st)==0,"stat");
 must(st.st_size>2*1024*1024,"size gt 2MiB");
 must(::setenv("SEEKDB_FILE_SQL_ROOT",root.c_str(),1)==0,"setenv");
 std::string rewritten,sqlerr;
 must(oceanbase::sql::native_file_sql::rewrite(
     "SELECT COUNT(*) FROM read_json('large.json')",rewritten,sqlerr),
     "SQL rewrite: "+sqlerr);
 must(rewritten.find("__seekdb_native_file_scan_v1__")!=std::string::npos,
     "SQL rewrite must stream");
 must(rewritten.find("json_array")!=std::string::npos,"stream token format");
 must(rewritten.size()<8192,"bounded SQL rewrite");
 StreamOptions opts;opts.format=JSON_ARRAY;opts.max_batch_rows=16;
 NativeFileScanCursor cursor;std::string err;
 must(cursor.open(root,large,opts,{},err),"open: "+err);
 must(cursor.schema().names.size()==3,"schema count");
 must(cursor.schema().names[0]=="id","schema id");
 must(cursor.schema().types[0]==INTEGER_VALUE,"type int");
 ScanBatch batch;size_t rows=0,batches=0;
 std::string json;
 while(true){
   must(cursor.next_batch(batch,16,err),"next: "+err);
   if(batch.rows.empty())break;
   ++batches; rows+=batch.rows.size();
   must(batch.rows.size()<=16,"batch row cap");
   must(scan_batch_as_json(batch,cursor.projected_names(),json,err),"json: "+err);
   must(json.size()<16*1024,"batch byte cap");
 }
 must(rows==80000,"row count");
 must(batches==5000,"batch count");
 must(cursor.rescan(err),"rescan: "+err);
 must(cursor.next_batch(batch,1,err),"rescan first batch: "+err);
 must(batch.rows.size()==1 && batch.rows[0][0].value=="1","rescan first row");
 std::string token,p;FileFormat fmt=CSV;bool header=false;
 must(make_stream_token(large,JSON_ARRAY,false,token),"make token");
 must(parse_stream_token(token,p,fmt,header),"parse token");
 must(p==large && fmt==JSON_ARRAY && !header,"token roundtrip");
 auto invalid=[&](const char *name,const std::string &content) {
   const std::string f=path(root,name);write(f,content);
   FileStreamReader rd;StreamOptions o;o.format=JSON_ARRAY;
   std::string e;must(rd.open(root,f,o,e),std::string("open invalid ")+name+": "+e);
   FileBatch b;must(!rd.next_batch(b,16,e),std::string("accepted invalid ")+name);
   ::unlink(f.c_str());
 };
 invalid("trailing_comma.json","[{\"x\":1},]");
 invalid("missing_comma.json","[{\"x\":1} {\"x\":2}]");
 invalid("trailing_text.json","[{\"x\":1}]garbage");
 invalid("nested.json","[{\"x\":{\"v\":2}}]");
 invalid("huge.json","[{\"x\":\""+std::string(70000,'z')+"\"}]");
 cursor.close();::unlink(large.c_str());::rmdir(root.c_str());
 std::cout<<"JSON_ARRAY_STREAM_CORE_PASS rows="<<rows<<" batches="<<batches<<" file_bytes="<<st.st_size<<" malformed=5 token=PASS rescan=PASS\n";
}
