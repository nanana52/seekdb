#include "ob_file_stream_reader.h"
#include <cassert>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
using namespace oceanbase::sql::file_stream;
static void write(const std::string &p,const std::string &s) {
  std::ofstream f(p,std::ios::binary);f.write(s.data(),s.size());
}
int main(int argc,char **argv) {
  if(argc!=2)return 2;
  const std::string root=argv[1];
  std::string error;
  StreamOptions options;
  FileStreamReader scan;
  FileBatch batch;
  write(root+"/simple.csv", "\357\273\277id,id,note\r\n"
        "1,101,\"hello,\r\nworld\"\r\n"
        "2,102,\"say \"\"hi\"\"\"\r\n"
        "3,103,\"unicode: \346\265\213\350\257\225\"\n");
  assert(scan.open(root,"simple.csv",options,error));
  assert(scan.columns().size()==3 && scan.columns()[0]=="id" &&
         scan.columns()[1]=="id__2");
  assert(scan.next_batch(batch,2,error));
  assert(batch.rows.size()==2);
  assert(batch.rows[0][2]=="hello,\r\nworld");
  assert(batch.rows[1][2]=="say \"hi\"");
  assert(scan.next_batch(batch,2,error));
  assert(batch.rows.size()==1 && batch.rows[0][0]=="3");
  assert(scan.next_batch(batch,2,error) && batch.rows.empty());
  assert(scan.rescan(error));
  assert(scan.next_batch(batch,1,error) && batch.rows[0][0]=="1");

  options.header=false;
  assert(scan.open(root,"simple.csv",options,error));
  assert(scan.columns().size()==3 && scan.columns()[0]=="column0");
  assert(scan.next_batch(batch,1,error));
  assert(batch.rows[0][0]=="id" && batch.rows[0][1]=="id");
  assert(scan.next_batch(batch,1,error) && batch.rows[0][0]=="1");
  options.header=true;
  write(root+"/bad.csv","a,b\n1,\"unterminated\n");
  assert(scan.open(root,"bad.csv",options,error));
  assert(!scan.next_batch(batch,1,error));
  assert(error.find("quoted")!=std::string::npos);

  write(root+"/badquote.csv","a,b\n1,b\"ad\n");
  error.clear();
  assert(scan.open(root,"badquote.csv",options,error));
  assert(!scan.next_batch(batch,1,error));
  assert(error.find("quote")!=std::string::npos);

  write(root+"/utf8.csv",std::string("a\n")+std::string("\303\050")+"\n");
  error.clear();
  assert(scan.open(root,"utf8.csv",options,error));
  assert(!scan.next_batch(batch,1,error));

  write(root+"/utf8_no_newline.csv",
        std::string("a\n")+std::string("\303\050"));
  assert(scan.open(root,"utf8_no_newline.csv",options,error));
  assert(!scan.next_batch(batch,1,error));
  StreamOptions json_opts;
  json_opts.format=JSONL;
  write(root+"/good.jsonl",
    "{\"id\":1,\"name\":\"A\",\"active\":true}\n"
    "{\"id\":2,\"name\":\"B\",\"active\":false}\r\n"
    "{\"id\":3,\"score\":2.1e-4,\"note\":null}\n");
  error.clear();
  assert(scan.open(root,"good.jsonl",json_opts,error));
  assert(scan.next_batch(batch,2,error) && batch.rows.size()==2);
  assert(batch.rows[0][0].find("\"id\":1")!=std::string::npos);
  assert(scan.next_batch(batch,2,error) && batch.rows.size()==1);
  assert(scan.rescan(error));
  assert(scan.next_batch(batch,3,error) && batch.rows.size()==3);

  write(root+"/bom.jsonl","\357\273\277{\"id\":1}\n");
  assert(scan.open(root,"bom.jsonl",json_opts,error));
  assert(scan.next_batch(batch,1,error) && batch.rows.size()==1);
  write(root+"/nested.jsonl","{\"a\":[1,2]}\n");
  assert(scan.open(root,"nested.jsonl",json_opts,error));
  assert(!scan.next_batch(batch,1,error));
  write(root+"/badnum.jsonl","{\"a\":0123}\n");
  assert(scan.open(root,"badnum.jsonl",json_opts,error));
  assert(!scan.next_batch(batch,1,error));

  error.clear();
  assert(!scan.open(root,"/etc/passwd",options,error));
  assert(!scan.open(root,"../outside-file.csv",options,error));
  assert(::symlink("/etc/passwd",(root+"/escape.csv").c_str()) == 0);
  assert(!scan.open(root,"escape.csv",options,error));

  write(root+"/change.csv","id\n1\n2\n");
  assert(scan.open(root,"change.csv",options,error));
  assert(scan.next_batch(batch,1,error));
  write(root+"/change.csv","id\n1\n2\n3\n");
  assert(!scan.rescan(error));
  assert(error.find("changed")!=std::string::npos);
  write(root+"/replacement.csv","id\n7\n");
  assert(scan.open(root,"replacement.csv",options,error));
  write(root+"/replacement2.csv","id\n8\n");
  assert(::rename((root+"/replacement2.csv").c_str(),
                  (root+"/replacement.csv").c_str())==0);
  assert(!scan.next_batch(batch,1,error));

  StreamOptions large_opts;
  large_opts.header=true;
  large_opts.max_batch_rows=128;
  {
    std::ofstream out(root+"/huge.csv",std::ios::binary);
    out<<"id,payload\n";
    for(int i=0;i<80000;++i)
      out<<i<<",123456789012345678901234567890123456789012345678901234567890\n";
  }
  assert(scan.open(root,"huge.csv",large_opts,error));
  struct stat size_info;
  assert(::stat((root+"/huge.csv").c_str(),&size_info)==0);
  assert(size_info.st_size>2*1024*1024);
  size_t rows=0;
  do {
    assert(scan.next_batch(batch,128,error));
    assert(batch.rows.size()<=128);
    rows+=batch.rows.size();
  }while(!batch.rows.empty());
  assert(rows==80000);
  assert(scan.rescan(error));
  assert(scan.next_batch(batch,128,error) && batch.rows.size()==128);
  assert(batch.rows[0][0]=="0" && batch.rows[127][0]=="127");
  assert(!scan.next_batch(batch,129,error));
  std::cout<<"NATIVE_STREAM_CSV_JSONL_UNIT_PASS"
           <<" rows="<<rows<<" bytes="<<size_info.st_size<<"\n";
}
