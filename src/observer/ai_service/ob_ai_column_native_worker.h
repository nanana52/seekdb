// Copyright (c) 2026 OceanBase competition contributors.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "share/ai_service/ob_ai_column_schema_adapter.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_thread_pool.h"
#include "share/ob_server_struct.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include <atomic>
#include <cmath>
#include <mutex>
#include <thread>

namespace oceanbase { namespace observer { namespace ai_column {
using share::ai_column::quote_identifier;
using share::ai_column::quote_literal;
using share::ai_column::NativeColumnDefinition;
using common::OB_SUCCESS;
using common::sqlclient::ObISQLConnection;
using common::sqlclient::ObISQLConnectionGuard;

inline common::ObString native_sql_string(const std::string &text) {
  return common::ObString(text.size(),text.data());
}
inline int native_write(ObISQLConnection &connection,const std::string &sql,int64_t *rows=nullptr) {
  int64_t affected=0;const int ret=connection.execute_write(native_sql_string(sql),affected,true);
  if (rows) { *rows=affected; }
  return ret;
}
inline int read_text(common::sqlclient::ObMySQLResult &row,const char *name,std::string &text) {
  common::ObString value;const int ret=row.get_varchar(name,value);
  if (ret==common::OB_ERR_NULL_VALUE) { text.clear();return OB_SUCCESS; }
  if (ret==OB_SUCCESS) { text.assign(value.ptr(),value.length()); }
  return ret;
}
struct TaskGroup {
  uint64_t table_id=0;
  std::string request,rowkey,owner;
  std::string predicate() const {
    return "table_id="+std::to_string(table_id)+" AND request_id="+quote_literal(request)+
      " AND rowkey_predicate="+quote_literal(rowkey);
  }
};
struct TaskResult {
  uint64_t column_id=0;
  int64_t state=0,retries=0,max_retries=0,version=1;
  std::string input,model,type,function,instruction,database,table,column,state_name,request_name,spec_name,result;
  int64_t dimension=0;
};

inline int read_tasks(ObISQLConnection &connection,const TaskGroup &group,std::vector<TaskResult> &tasks,bool lock_rows=false) {
  common::ObISQLClient::ReadResult result;
  const std::string query="SELECT task.column_id,task.state,task.retry_count,task.max_retries,task.definition_version,task.input_text,task.result_sql,"
    "def.model_name,def.result_type,def.ai_function,def.database_name,def.table_name,def.column_name,def.state_column_name,def.request_column_name,def.result_spec_column_name,def.vector_dimension,def.normalized_expr "
    "FROM oceanbase.__all_ai_column_task task JOIN oceanbase.__all_ai_column def ON task.table_id=def.table_id AND task.column_id=def.column_id WHERE task."+
    group.predicate()+" AND task.owner_token="+quote_literal(group.owner)+" ORDER BY task.column_id"+(lock_rows?" FOR UPDATE":"");
  int ret=connection.execute_read(native_sql_string(query),result,true);
  if (ret!=OB_SUCCESS) { return ret; }
  auto *rows=result.get_result();if (!rows) { return common::OB_ERR_UNEXPECTED; }
  while ((ret=rows->next())==OB_SUCCESS) {
    TaskResult task;int64_t id=0;
    if ((ret=rows->get_int("column_id",id))!=OB_SUCCESS) { break; }task.column_id=id;
    if ((ret=rows->get_int("state",task.state))!=OB_SUCCESS ||
        (ret=rows->get_int("retry_count",task.retries))!=OB_SUCCESS ||
        (ret=rows->get_int("max_retries",task.max_retries))!=OB_SUCCESS ||
        (ret=rows->get_int("definition_version",task.version))!=OB_SUCCESS ||
        (ret=rows->get_int("vector_dimension",task.dimension))!=OB_SUCCESS) { break; }
    for (auto field:std::vector<std::pair<const char *,std::string *>>{
      {"input_text",&task.input},{"result_sql",&task.result},{"model_name",&task.model},{"result_type",&task.type},
      {"ai_function",&task.function},{"database_name",&task.database},{"table_name",&task.table},{"column_name",&task.column},
      {"state_column_name",&task.state_name},{"request_column_name",&task.request_name},{"result_spec_column_name",&task.spec_name},
      {"normalized_expr",&task.instruction}}) {
      if ((ret=read_text(*rows,field.first,*field.second))!=OB_SUCCESS) { break; }
    }
    if (ret!=OB_SUCCESS) { break; }
    tasks.push_back(std::move(task));
  }
  return ret==common::OB_ITER_END?OB_SUCCESS:ret;
}
inline int generate_task_value(ObISQLConnection &connection,TaskResult &task) {
  const std::string type=share::ai_column::lower_identifier(task.type);
  std::string text;
  int ret=OB_SUCCESS;
  if (task.function=="AI_COMPLETE") {
    std::string instruction;
    if (type.find("json")!=std::string::npos) { instruction="Return exactly one valid JSON value"; }
    else if (type.find("int")!=std::string::npos) { instruction="Return exactly one base-10 integer and no other text"; }
    else if (type.find("double")!=std::string::npos||type.find("float")!=std::string::npos||type.find("decimal")!=std::string::npos) { instruction="Return exactly one valid finite number"; }
    common::ObArenaAllocator allocator("AIColModel");
    common::ObAIFuncExprInfo *info=nullptr;
    share::ObAiModelEndpointInfo endpoint;
    auto *resolver=share::server_service<query::ObIAiEndpointResolver>();
    common::ObString name(task.model.size(),task.model.data());
    if ((ret=common::ObAIFuncUtils::get_ai_func_info(allocator,name,info))!=OB_SUCCESS) { return ret; }
    if (!resolver) { return common::OB_NOT_INIT; }
    if ((ret=resolver->resolve_by_model_name(name,allocator,endpoint))!=OB_SUCCESS) { return ret; }
    common::ObAIFuncModel model(allocator,*info,endpoint);
    common::ObString input(task.input.size(),task.input.data()),system(instruction.size(),instruction.data()),output;
    if ((ret=model.call_completion_with_system(system,input,nullptr,output))!=OB_SUCCESS) { return ret; }
    text.assign(output.ptr(),output.length());
  } else {
    const std::string sql="SELECT AI_EMBED("+quote_literal(task.model)+","+quote_literal(task.input)+","+std::to_string(task.dimension)+") AS ai_value";
    common::ObISQLClient::ReadResult result;
    if ((ret=connection.execute_read(native_sql_string(sql),result,true))!=OB_SUCCESS) { return ret; }
    auto *row=result.get_result();if (!row||row->next()!=OB_SUCCESS) { return common::OB_ERR_UNEXPECTED; }
    if ((ret=read_text(*row,"ai_value",text))!=OB_SUCCESS) { return ret; }
  }
  try {
    if (type.find("int")!=std::string::npos) {
      size_t end=0;std::stoll(text,&end);
      while(end<text.size() && std::isspace(static_cast<unsigned char>(text[end]))) { ++end; }
      if (end!=text.size()) { return common::OB_INVALID_ARGUMENT; }
      task.result=text;
    } else if (type.find("double")!=std::string::npos||type.find("float")!=std::string::npos||type.find("decimal")!=std::string::npos) {
      size_t end=0;const double value=std::stod(text,&end);
      while(end<text.size() && std::isspace(static_cast<unsigned char>(text[end]))) { ++end; }
      if (end!=text.size()||!std::isfinite(value)) { return common::OB_INVALID_ARGUMENT; }
      task.result=text;
    } else if (type.find("json")!=std::string::npos) {
      common::ObISQLClient::ReadResult check;
      const auto query="SELECT JSON_VALID("+quote_literal(text)+") AS valid";
      if ((ret=connection.execute_read(native_sql_string(query),check,true))!=OB_SUCCESS) { return ret; }
      auto *value=check.get_result();int64_t valid=0;
      if (!value||value->next()!=OB_SUCCESS||value->get_int("valid",valid)!=OB_SUCCESS||!valid) { return common::OB_INVALID_ARGUMENT; }
      task.result="CAST("+quote_literal(text)+" AS JSON)";
    } else { task.result=quote_literal(text); }
  } catch (const std::exception &) { return common::OB_INVALID_ARGUMENT; }
  return ret;
}

class NativeColumnWorker final : public share::ObThreadPool {
public:
  NativeColumnWorker():share::ObThreadPool(4) {}
  ~NativeColumnWorker() { stop();wait(); }
  void run1() override {
    lib::set_thread_name("AIColumnWorker");
    while (!has_set_stop()) {
      try { process_one_group(); } catch (...) {}
      for (int n=0;n<5&&!has_set_stop();++n) { ob_usleep(20*1000); }
    }
  }
  void process_one_group() {
    ObISQLConnectionGuard guard;
    if (ObInnerSQLConnection::create_connection_with_owned_session(false,0,guard)!=OB_SUCCESS) { return; }
    auto &connection=*guard.get_ptr();TaskGroup group;
    {
      common::ObISQLClient::ReadResult result;
      const std::string query="SELECT table_id,request_id,rowkey_predicate,UUID() AS owner FROM oceanbase.__all_ai_column_task "
        "WHERE state IN (1,2,3,4,5) AND eligible_ts<=NOW(6) AND (owner_token IS NULL OR lease_deadline<NOW(6)) ORDER BY eligible_ts LIMIT 1";
      if (connection.execute_read(native_sql_string(query),result,true)!=OB_SUCCESS) { return; }
      auto *row=result.get_result();int64_t id=0;
      if (!row||row->next()!=OB_SUCCESS||row->get_int("table_id",id)!=OB_SUCCESS) { return; }
      group.table_id=id;
      if(read_text(*row,"request_id",group.request)!=OB_SUCCESS||read_text(*row,"rowkey_predicate",group.rowkey)!=OB_SUCCESS||read_text(*row,"owner",group.owner)!=OB_SUCCESS) { return; }
    }
    if (connection.start_transaction()!=OB_SUCCESS) { return; }
    int64_t claimed=0;
    int ret=native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET owner_token="+quote_literal(group.owner)+
      ",lease_deadline=DATE_ADD(NOW(6),INTERVAL 60 SECOND),state=IF(state IN(1,2,3),2,state) WHERE "+group.predicate()+
      " AND (owner_token IS NULL OR lease_deadline<NOW(6))",&claimed);
    if(ret!=OB_SUCCESS||claimed==0) { connection.rollback();return; }
    if(connection.commit()!=OB_SUCCESS) { return; }
    std::vector<TaskResult> tasks;
    if (read_tasks(connection,group,tasks)!=OB_SUCCESS||tasks.empty()) { return; }
    bool pending=false;
    for(auto &task:tasks) {
      if(task.state==4||task.state==5) { continue; }
      const int generated=generate_task_value(connection,task);
      const std::string key=group.predicate()+" AND column_id="+std::to_string(task.column_id)+" AND owner_token="+quote_literal(group.owner);
      if(generated==OB_SUCCESS) {
        native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET state=4,result_sql="+quote_literal(task.result)+" WHERE "+key);
        task.state=4;
      } else if(task.retries<task.max_retries) {
        native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET state=3,retry_count=retry_count+1,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE "+key);
        pending=true;
      } else {
        native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET state=5,last_error="+quote_literal(std::to_string(generated))+" WHERE "+key);
        task.state=5;
      }
    }
    if(pending) {
      native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET owner_token=NULL,lease_deadline=NULL WHERE "+group.predicate()+" AND owner_token="+quote_literal(group.owner));
      return;
    }
    if(connection.start_transaction()!=OB_SUCCESS) { return; }
    tasks.clear();
    if (read_tasks(connection,group,tasks,true)!=OB_SUCCESS || tasks.empty()) {
      connection.rollback();return;
    }
    for (const auto &task:tasks) {
      if (task.state!=4 && task.state!=5) { connection.rollback();return; }
    }
    std::string statement="UPDATE "+quote_identifier(tasks.front().database)+"."+quote_identifier(tasks.front().table)+" SET ";
    std::string fence=" WHERE ("+group.rowkey+")";
    for(size_t i=0;i<tasks.size();++i) {
      const auto &task=tasks[i];if(i)statement+=",";
      statement+=quote_identifier(task.column)+"="+(task.state==4?task.result:"NULL")+","+quote_identifier(task.state_name)+"="+(task.state==4?"1":"2");
      fence+=" AND "+quote_identifier(task.request_name)+"="+quote_literal(group.request)+" AND "+quote_identifier(task.spec_name)+"="+std::to_string(task.version);
    }
    ret=native_write(connection,statement+fence);
    if(ret==OB_SUCCESS)ret=native_write(connection,"DELETE FROM oceanbase.__all_ai_column_task WHERE "+group.predicate()+" AND owner_token="+quote_literal(group.owner));
    if (ret==OB_SUCCESS) {
      ret=connection.commit();
    } else {
      connection.rollback();
    }
    if (ret!=OB_SUCCESS) {
      // Keep staged output when publication fails, without a second model call.
      native_write(connection,"UPDATE oceanbase.__all_ai_column_task SET owner_token=NULL,lease_deadline=NULL,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE "+group.predicate()+" AND owner_token="+quote_literal(group.owner));
    }
  }
};
inline NativeColumnWorker &native_column_worker() {
  static NativeColumnWorker worker;
  return worker;
}
inline int start_native_column_worker() {
  static std::once_flag once;static int status=OB_SUCCESS;
  std::call_once(once,[&]{status=native_column_worker().start();});
  return status;
}
inline void stop_native_column_worker() {
  native_column_worker().stop();
  native_column_worker().wait();
}


}}} // namespace oceanbase::observer::ai_column
