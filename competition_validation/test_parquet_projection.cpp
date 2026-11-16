
#include "sql/parser/ob_file_sql_rewrite.h"
#include <cassert>
#include <iostream>
int main(int argc,char**argv){
 using namespace oceanbase::sql;
 native_file_sql::ScopedFileSqlSecurity security(argv[1]);
 std::string output,error;
 assert(native_file_sql::rewrite(std::string("SELECT c1 FROM read_parquet('")+argv[2]+"')",output,error));
 assert(output.find("projection")!=std::string::npos);
 parquet_stream::Cursor cursor;
 assert(cursor.open(argv[1],argv[2],error));
 assert(cursor.project({"c1"},error));
 std::string token=parquet_stream::make_token(argv[2],{"c1"}),path;
 std::vector<std::string> cols;
 assert(parquet_stream::is_token(token)&&parquet_stream::parse_token(token,path,cols));
 assert(path==argv[2]&&cols==std::vector<std::string>{"c1"});
 assert(cursor.next_json(output,2,error));
 std::cout<<"PARQUET_COLUMN_PROJECTION_TOKEN_PASS"<<std::endl;
}
