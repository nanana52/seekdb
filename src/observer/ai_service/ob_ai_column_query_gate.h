// Copyright (c) 2026 OceanBase competition contributors.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "observer/ai_service/ob_ai_column_native_worker.h"
#include "share/ob_errno.h"
#include "share/system_variable/ob_sys_var_class_type.h"

namespace oceanbase { namespace observer { namespace ai_column {
struct RuntimeColumn {
  uint64_t table_id,column_id;
  int64_t version;
  std::string database,table,column,state_name,request_name,spec_name;
};
inline int list_runtime_columns(ObISQLConnection &connection,std::vector<RuntimeColumn> &columns) {
  common::ObISQLClient::ReadResult result;
  const auto sql=std::string("SELECT a.table_id,a.column_id,a.definition_version,a.database_name,a.table_name,a.column_name,a.state_column_name,a.request_column_name,a.result_spec_column_name FROM oceanbase.__all_ai_column a JOIN oceanbase.__all_table t ON a.table_id=t.table_id");
  int ret=connection.execute_read(native_sql_string(sql),result,true);
  if(ret!=OB_SUCCESS)return ret;
  auto *row=result.get_result();if(!row)return common::OB_ERR_UNEXPECTED;
  while((ret=row->next())==OB_SUCCESS) {
    RuntimeColumn c;int64_t table=0,column=0;
    if(row->get_int("table_id",table)!=OB_SUCCESS||row->get_int("column_id",column)!=OB_SUCCESS||row->get_int("definition_version",c.version)!=OB_SUCCESS)return common::OB_ERR_UNEXPECTED;
    c.table_id=table;c.column_id=column;
    for(auto field:std::vector<std::pair<const char *,std::string *>>{{"database_name",&c.database},{"table_name",&c.table},{"column_name",&c.column},{"state_column_name",&c.state_name},{"request_column_name",&c.request_name},{"result_spec_column_name",&c.spec_name}})if((ret=read_text(*row,field.first,*field.second))!=OB_SUCCESS)return ret;
    columns.push_back(std::move(c));
  }
  return ret==common::OB_ITER_END?OB_SUCCESS:ret;
}
inline int prepare_native_column_query(sql::ObSQLSessionInfo &session,const common::ObString &input,std::string &rewritten) {
  static thread_local bool nested=false;
  if(nested)return OB_SUCCESS;
  const std::string text(input.ptr(),input.length());
  std::vector<share::ai_column::SqlToken> tokens;
  try {tokens=share::ai_column::tokenize_definition(text);}catch(...){return OB_SUCCESS;}
  if(tokens.empty())return OB_SUCCESS;
  bool strict=false;
  int ret=session.get_sys_variable(share::SYS_VAR_AI_COLUMN_STRICT_READ,strict);
  if(ret!=OB_SUCCESS)return ret;
  const bool retry=tokens[0].is("ALTER")&&tokens.size()>6&&tokens[1].is("TABLE");
  const bool read=tokens[0].is("SELECT")||tokens[0].is("UPDATE")||tokens[0].is("DELETE");
  if(!strict&&!retry)return OB_SUCCESS;
  if(!read&&!retry)return OB_SUCCESS;
  struct NestedGuard {bool &flag;NestedGuard(bool &f):flag(f){flag=true;}~NestedGuard(){flag=false;}} scope(nested);
  ObISQLConnectionGuard connection;
  if((ret=ObInnerSQLConnection::create_connection_with_external_session(&session,connection))!=OB_SUCCESS)return ret;
  std::vector<RuntimeColumn> columns;
  if((ret=list_runtime_columns(*connection.get_ptr(),columns))!=OB_SUCCESS)return ret;
  const auto db=session.get_database_name();const std::string current(db.ptr(),db.length());
  const auto same=[](const std::string &a,const std::string &b){return share::ai_column::lower_identifier(a)==share::ai_column::lower_identifier(b);};
  const auto parse_table=[&](size_t &at,std::string &database,std::string &table)->bool {
    if(at>=tokens.size()||tokens[at].kind!=share::ai_column::SqlToken::IDENTIFIER)return false;
    database=current;table=tokens[at++].value;
    if(at+1<tokens.size()&&tokens[at].value==".") {database=table;table=tokens[at+1].value;at+=2;}
    return true;
  };
  if(retry) {
    size_t at=2;std::string database,table;
    if(!parse_table(at,database,table)||at+3>=tokens.size()||!tokens[at].is("RETRY")||!tokens[at+1].is("AI")||!tokens[at+2].is("COLUMN"))return OB_SUCCESS;
    const std::string target=tokens[at+3].value;at+=4;
    const RuntimeColumn *column=nullptr;
    for(const auto &c:columns)if(same(c.database,database)&&same(c.table,table)&&same(c.column,target))column=&c;
    if(!column) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED,"retrying a column that is not an AI column");
      return common::OB_NOT_SUPPORTED;
    }
    if(at<tokens.size()&&!tokens[at].is("WHERE")&&tokens[at].value!=";") {
      LOG_USER_ERROR(OB_NOT_SUPPORTED,"combining AI COLUMN RETRY with another ALTER action");
      return common::OB_NOT_SUPPORTED;
    }
    std::string predicate="1";
    if(at<tokens.size()&&tokens[at].is("WHERE")) {
      ++at;if(at>=tokens.size())return common::OB_INVALID_ARGUMENT;
      const size_t start=tokens[at].first;size_t consumed=start;predicate.clear();int depth=0;
      for(size_t i=at;i<tokens.size();++i) {
        if(tokens[i].kind==share::ai_column::SqlToken::SYMBOL) {
          if(tokens[i].value=="(")++depth;else if(tokens[i].value==")")--depth;
          else if(tokens[i].value==","&&depth==0) {
            LOG_USER_ERROR(OB_NOT_SUPPORTED,"combining AI COLUMN RETRY with another ALTER action");
            return common::OB_NOT_SUPPORTED;
          }
        }
        if(tokens[i].is("STATUS") && !(i>0 && tokens[i-1].value==".")) {
          predicate.append(text,consumed,tokens[i].first-consumed);
          predicate+="(CASE "+quote_identifier(column->state_name)+" WHEN 1 THEN 'READY' WHEN 2 THEN 'FAILED' ELSE 'PENDING' END)";
          consumed=tokens[i].last;
        }
      }
      predicate.append(text,consumed,std::string::npos);
      while(!predicate.empty()&&(std::isspace(static_cast<unsigned char>(predicate.back()))||predicate.back()==';'))predicate.pop_back();
    }
    rewritten="UPDATE "+quote_identifier(database)+"."+quote_identifier(table)+" SET "+quote_identifier(column->column)+"=NULL,"+quote_identifier(column->state_name)+"=0,"+quote_identifier(column->request_name)+"=UUID(),"+quote_identifier(column->spec_name)+"="+std::to_string(column->version)+" WHERE "+predicate;
    return OB_SUCCESS;
  }
  if(!strict)return OB_SUCCESS;
  std::vector<std::pair<std::string,std::string>> tables;
  for(size_t i=0;i<tokens.size();++i)if(tokens[i].is("FROM")||tokens[i].is("JOIN")||(i==0&&tokens[i].is("UPDATE"))) {
    size_t at=i+1;std::string database,table;
    if(parse_table(at,database,table))tables.push_back({database,table});
  }
  for(const auto &column:columns) {
    bool table_used=false;
    for(const auto &table:tables)table_used=table_used||(same(column.database,table.first)&&same(column.table,table.second));
    if(!table_used)continue;
    bool referenced=false;
    for(size_t i=0;i<tokens.size();++i) {
      referenced=referenced||(tokens[i].kind==share::ai_column::SqlToken::IDENTIFIER&&same(tokens[i].value,column.column));
      referenced=referenced||(tokens[i].kind==share::ai_column::SqlToken::SYMBOL&&tokens[i].value=="*"&&i>0&&(tokens[i-1].is("SELECT")||tokens[i-1].value=="."));
    }
    if(!referenced)continue;
    common::ObISQLClient::ReadResult result;
    const auto query="SELECT 1 AS pending FROM "+quote_identifier(column.database)+"."+quote_identifier(column.table)+" WHERE "+quote_identifier(column.state_name)+" IS NULL OR "+quote_identifier(column.state_name)+"<>1 LIMIT 1";
    if((ret=connection->execute_read(native_sql_string(query),result,true))!=OB_SUCCESS)return ret;
    auto *row=result.get_result();if(!row)return common::OB_ERR_UNEXPECTED;
    if(row->next()==OB_SUCCESS) {
      OB_LOGGER.log_user_message(common::ObLogger::USER_ERROR,OB_AI_COLUMN_NOT_READY,
          "AI column has unfinished tasks for table_id=%lu, column_id=%lu",column.table_id,column.column_id);
      return common::OB_AI_COLUMN_NOT_READY;
    }
  }
  return OB_SUCCESS;
}

}}} // namespace oceanbase::observer::ai_column
