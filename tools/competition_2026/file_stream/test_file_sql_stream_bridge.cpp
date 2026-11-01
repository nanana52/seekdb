#include "sql/engine/basic/ob_file_scan_core.h"
#include "sql/parser/ob_file_sql_rewrite.h"
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
using namespace oceanbase::sql;
using namespace oceanbase::sql::file_stream;
static void write_csv(const std::string &path) {
  std::ofstream f(path.c_str());
  f<<"id,note,metric\n";
  for(int i=0;i<80000;++i)
    f<<i<<",\"row "<<i<<" quoted \"\"word\"\"\","<<i*2<<"\n";
}
int main(int argc,char **argv) {
  assert(argc==2);
  const std::string root=argv[1];
  assert(::setenv("SEEKDB_FILE_SQL_ROOT",root.c_str(),1)==0);
  std::string token,path,err,rewritten;
  FileFormat fmt=CSV;
  bool header=false;
  assert(make_stream_token(root+"/a'odd.csv",CSV,true,token));
  assert(parse_stream_token(token,path,fmt,header) && path==root+"/a'odd.csv");
  assert(fmt==CSV && header);
  assert(!parse_stream_token(token+"tail",path,fmt,header));
  assert(!parse_stream_token("no token",path,fmt,header));
  const std::string csv_path=root+"/large.csv";
  write_csv(csv_path);
  struct stat st;
  assert(::stat(csv_path.c_str(),&st)==0 && st.st_size>2*1024*1024);
  assert(native_file_sql::rewrite("SELECT COUNT(*) FROM read_csv('large.csv')",rewritten,err));
  assert(rewritten.find("__seekdb_native_file_scan_v1__")!=std::string::npos);
  assert(rewritten.find("JSON_TABLE(")!=std::string::npos);
  assert(rewritten.size()<8192);
  StreamOptions opts;
  opts.format=CSV;opts.max_batch_rows=16;
  NativeFileScanCursor cursor;
  assert(cursor.open(root,csv_path,opts,{},err));
  ScanBatch batch;
  uint64_t count=0,batches=0;
  size_t max_chunk=0;
  std::ofstream sample((root+"/samples.jsonl").c_str());
  while(true) {
    assert(cursor.next_batch(batch,16,err));
    if(batch.rows.empty())break;
    std::string chunk;
    assert(scan_batch_as_json(batch,cursor.projected_names(),chunk,err));
    if(batches<3) sample<<chunk<<"\n";
    assert(chunk.size()<8u*1024u*1024u);
    if(chunk.size()>max_chunk)max_chunk=chunk.size();
    count+=batch.rows.size();++batches;
  }
  assert(count==80000 && batches==5000);
  assert(cursor.rescan(err) && cursor.next_batch(batch,16,err));
  assert(batch.rows[0][0].value=="0");
  cursor.close();
  const std::string jsonl_path=root+"/docs.jsonl";
  {
    std::ofstream f(jsonl_path.c_str());
    f<<"{\"id\":1,\"title\":\"first\",\"optional\":null}\n";
    f<<"{\"id\":2,\"title\":\"quote \\\"hi\\\"\",\"optional\":\"x\"}\n";
  }
  assert(native_file_sql::rewrite("SELECT * FROM read_ndjson('docs.jsonl')",rewritten,err));
  assert(rewritten.find("__seekdb_native_file_scan_v1__")!=std::string::npos);
  opts.format=JSONL;
  assert(cursor.open(root,jsonl_path,opts,{},err));
  assert(cursor.next_batch(batch,16,err) && batch.rows.size()==2);
  std::string json;
  assert(scan_batch_as_json(batch,cursor.projected_names(),json,err));
  std::ofstream f((root+"/jsonl.json").c_str());f<<json;f.close();
  assert(cursor.next_batch(batch,16,err) && batch.rows.empty());
  assert(!cursor.open(root,"/etc/passwd",opts,{},err));
  std::cout<<"FILESCAN_SQL_BRIDGE_CORE_PASS rows="<<count<<" batches="<<batches
           <<" max_chunk_bytes="<<max_chunk<<" file_bytes="<<st.st_size
           <<" rewrite_gt_2MiB=PASS"<<std::endl;
}
