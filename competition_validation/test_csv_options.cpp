#include "sql/engine/basic/ob_file_scan_core.h"
#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>
using namespace oceanbase::sql::file_stream;
int main(){
 char tmp[]="/tmp/file-options-XXXXXX";assert(mkdtemp(tmp));
 std::string root=tmp,path=root+"/a.csv",error,token,decoded;
 {std::ofstream f(path);f<<"id;name;flag;optional\n1;'O\\'Brien';true;NA\n2;'X;Y';false;ok\n";}
 StreamOptions opts;opts.delimiter=';';opts.quote='\'';opts.escape='\\';opts.null_text="NA";
 assert(make_stream_token(path,opts,token));StreamOptions restored;
 assert(parse_stream_token(token,decoded,restored));
 assert(decoded==path&&restored.delimiter==';'&&restored.quote=='\''&&restored.escape=='\\'&&restored.null_text=="NA");
 NativeFileScanCursor cursor;assert(cursor.open(root,path,restored,{},error));
 ScanBatch batch;assert(cursor.next_batch(batch,2,error));
 assert(batch.rows.size()==2&&batch.rows[0][1].value=="O'Brien"&&batch.rows[1][1].value=="X;Y");
 assert(batch.rows[0][3].is_null&&batch.rows[0][2].value=="1"&&batch.rows[1][2].value=="0");
 unlink(path.c_str());rmdir(root.c_str());
 std::cout<<"CSV_NAMED_OPTIONS_TOKEN_SCAN_PASS"<<std::endl;
}
