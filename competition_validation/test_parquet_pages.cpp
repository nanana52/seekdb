#include "sql/engine/basic/ob_parquet_stream_reader.h"
#include <cassert>
#include <iostream>
int main(int argc,char **argv) {
  assert(argc==3);
  oceanbase::sql::parquet_stream::Cursor cursor;
  std::string error,json;
  if(!cursor.open(argv[1],argv[2],error)){std::cerr<<error;return 1;}
  size_t rows=0;
  for(;;){
    if(!cursor.next_json(json,2,error)){std::cerr<<error;return 2;}
    if(json=="[]")break;
    std::cout<<json<<std::endl;
  }
  if(!cursor.rescan(error)||!cursor.next_json(json,2,error))return 3;
  std::cout<<"PARQUET_PAGE_STREAM_RESCAN_PASS"<<std::endl;
}
