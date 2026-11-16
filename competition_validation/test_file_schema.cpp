#include "sql/engine/basic/ob_file_scan_core.h"
#include <cassert>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
using namespace oceanbase::sql::file_stream;
int main() {
  char dir[]="/tmp/seekdb-file-schema-XXXXXX";
  assert(mkdtemp(dir)!=nullptr);
  const std::string root=dir;
  const std::string csv=root+"/rows.csv";
  {std::ofstream f(csv);f<<"id,active,name,optional\n1,true,A,\n2,false,B,x\n";}
  NativeFileScanCursor cursor;StreamOptions options;std::string error;
  assert(cursor.open(root,csv,options,{},error));
  assert(cursor.schema().nullable.size()==4);
  assert(!cursor.schema().nullable[0] && !cursor.schema().nullable[1]);
  assert(!cursor.schema().nullable[2] && cursor.schema().nullable[3]);
  ScanBatch batch;assert(cursor.next_batch(batch,2,error));
  assert(batch.rows[0][1].value=="1" && batch.rows[1][1].value=="0");
  cursor.close();
  const std::string json=root+"/rows.jsonl";
  {std::ofstream f(json);f<<"{\"id\":1,\"active\":true,\"old\":null}\n"
                            "{\"id\":2,\"active\":false,\"late\":\"x\"}\n";}
  options.format=JSONL;
  assert(cursor.open(root,json,options,{},error));
  assert(!cursor.schema().nullable[0] && !cursor.schema().nullable[1]);
  assert(cursor.schema().nullable[2] && cursor.schema().nullable[3]);
  assert(cursor.next_batch(batch,2,error));
  assert(batch.rows[0][1].value=="1" && batch.rows[1][1].value=="0");
  unlink(csv.c_str());unlink(json.c_str());rmdir(root.c_str());
  std::cout<<"FILE_SCHEMA_BOOL_NULLABILITY_REGRESSION_PASS\n";
}
