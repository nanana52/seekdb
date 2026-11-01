#include "sql/engine/basic/ob_file_scan_core.h"
#include "sql/parser/ob_file_sql_rewrite.h"
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
using oceanbase::sql::file_stream::NativeFileScanCursor;
using oceanbase::sql::file_stream::StreamOptions;
using oceanbase::sql::file_stream::ScanBatch;
using oceanbase::sql::file_stream::JSONL;
using oceanbase::sql::file_stream::CSV;
namespace fs=oceanbase::sql::native_file_sql;
static void write_file(const std::string &name,const std::string &value) {
  std::ofstream file(name,std::ios::binary);
  file.write(value.data(),value.size());
  assert(file.good());
}
int main(int argc,char **argv) {
  if(argc!=2)return 2;
  const std::string root=argv[1];
  assert(::setenv("SEEKDB_FILE_SQL_ROOT",root.c_str(),1)==0);
  write_file(root+"/short.csv", "id,id,note\n1,101,a\n2,,b\n3,103,c\n");
  StreamOptions opts;
  NativeFileScanCursor reader;
  ScanBatch rows;
  std::string error;
  assert(reader.open(root,"short.csv",opts,{"id__2","note"},error));
  assert(reader.schema().names.size()==3);
  assert(reader.schema().names[1]=="id__2");
  assert(reader.schema().types[0]==oceanbase::sql::file_stream::INTEGER_VALUE);
  assert(reader.next_batch(rows,2,error) && rows.rows.size()==2);
  assert(rows.rows[0][0].value=="101" && rows.rows[0][1].value=="a");
  assert(rows.rows[1][0].is_null && rows.rows[1][1].value=="b");
  assert(reader.rescan(error));
  assert(reader.next_batch(rows,1,error) && rows.rows[0][0].value=="101");
  assert(!reader.open(root,"short.csv",opts,{"missing"},error));
  assert(error.find("unknown projected column")!=std::string::npos);

  write_file(root+"/data.jsonl",
    "{\"id\":1,\"text\":\"A\",\"city\":\"\\u5317\\u4eac\"}\n"
    "{\"id\":2,\"text\":null,\"active\":true}\n"
    "{\"id\":3,\"text\":\"\",\"city\":\"\\uD83C\\uDF0F\"}\n");
  opts.format=JSONL;
  assert(reader.open(root,"data.jsonl",opts,{"city","id","text","active"},error));
  assert(reader.schema().names.size()==4);
  assert(reader.next_batch(rows,3,error) && rows.rows.size()==3);
  assert(rows.rows[0][0].value=="\345\214\227\344\272\254");
  assert(rows.rows[0][1].value=="1");
  assert(rows.rows[0][3].is_null);
  assert(rows.rows[1][2].is_null && rows.rows[1][3].value=="true");
  assert(!rows.rows[2][2].is_null && rows.rows[2][2].value.empty());
  assert(rows.rows[2][0].value=="\360\237\214\217");
  assert(reader.next_batch(rows,3,error) && rows.rows.empty());
  assert(reader.rescan(error));
  assert(reader.next_batch(rows,1,error) && rows.rows[0][1].value=="1");

  // BOM without trailing newline must be accepted by both the scanner
  // and the streaming describe rewrite.
  write_file(root+"/bom.jsonl","\357\273\277{\"one\":1}");
  assert(reader.open(root,"bom.jsonl",opts,{},error));
  assert(reader.next_batch(rows,1,error) && rows.rows[0][0].value=="1");
  write_file(root+"/bom_crlf.jsonl","\357\273\277{\"one\":2}\r\n");
  assert(reader.open(root,"bom_crlf.jsonl",opts,{},error));
  assert(reader.next_batch(rows,1,error) && rows.rows[0][0].value=="2");

  write_file(root+"/bad.jsonl", "{\"a\":\"\\uD800\"}\n");
  assert(!reader.open(root,"bad.jsonl",opts,{},error));
  write_file(root+"/badquote.csv","x\n1,2\n");
  opts.format=CSV;
  assert(!reader.open(root,"badquote.csv",opts,{},error));
  assert(!reader.open(root,"/etc/passwd",opts,{},error));

  // >2 MiB stream: can infer schema, project, rescan, and DESCRIBE via
  // native SQL rewrite, while SELECT must still reject materialization.
  const std::string huge=root+"/big.csv";
  {
    std::ofstream file(huge,std::ios::binary);
    file<<"id,blob\n";
    for(int i=0;i<80000;++i)
      file<<i<<",123456789012345678901234567890123456789012345678901234567890\n";
  }
  assert(reader.open(root,"big.csv",opts,{"id"},error));
  size_t count=0;
  do {
    assert(reader.next_batch(rows,128,error));
    assert(rows.rows.size()<=128);
    for(const auto &row:rows.rows)assert(row.size()==1);
    count+=rows.rows.size();
  }while(!rows.rows.empty());
  assert(count==80000);
  assert(reader.rescan(error));
  assert(reader.next_batch(rows,2,error) && rows.rows.size()==2);
  assert(rows.rows[0][0].value=="0" && rows.rows[1][0].value=="1");

  // JSONL large-file discovery is also streaming and compatible with the
  // read_json(.jsonl) and read_ndjson aliases used in the parser.
  {
    std::ofstream file(root+"/big.jsonl",std::ios::binary);
    for(int i=0;i<80000;++i)
      file<<"{\"key\":"<<i<<",\"label\":\"long-value-for-schema\"}\n";
  }
  opts.format=JSONL;
  assert(reader.open(root,"big.jsonl",opts,{"key"},error));
  assert(reader.schema().names.size()==2);
  assert(reader.next_batch(rows,1,error) && rows.rows[0][0].value=="0");

  std::string rewritten;
  assert(fs::rewrite("DESCRIBE SELECT * FROM read_json('big.jsonl')",rewritten,error));
  assert(rewritten.find("'label'")!=std::string::npos);
  assert(fs::rewrite("DESCRIBE SELECT * FROM read_csv('big.csv')",rewritten,error));
  assert(rewritten.find("column_name")!=std::string::npos);
  assert(rewritten.find("'id'")!=std::string::npos);
  rewritten.clear();error.clear();
  assert(!fs::rewrite("SELECT COUNT(*) FROM read_csv('big.csv')",rewritten,error));
  assert(error.find("2 MiB")!=std::string::npos);
  std::cout<<"FILE_SCAN_CORE_PASS rows="<<count
           <<" projected_columns=1 streaming_describe=PASS"
           <<" SELECT_gt_2MiB=NOT_YET_SUPPORTED\n";
  return 0;
}
