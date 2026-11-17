// Copyright (c) 2026 OceanBase competition contributors.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ob_ai_column_definition.h"
#include <sstream>
#include <utility>

namespace oceanbase { namespace share { namespace ai_column {

enum class NativeColumnState : int64_t { PENDING = 0, READY = 1, FAILED = 2 };
enum class NativeTaskState : int64_t { WAITING = 1, RUNNING = 2, RETRY_WAIT = 3, READY_STAGED = 4, FAILED_STAGED = 5 };
struct BoundAIColumn {
  NativeColumnDefinition definition;
  uint64_t column_id, state_column_id, request_column_id, result_spec_column_id;
  std::string state_name, request_name, spec_name;
  uint64_t definition_version = 1;
};
struct BoundPrimaryKey { std::string name;bool numeric; };
struct BoundAITable {
  uint64_t table_id;
  std::string database, name;
  std::vector<BoundPrimaryKey> primary_key;
  std::vector<BoundAIColumn> columns;
  std::string qualified_name() const { return quote_identifier(database)+"."+quote_identifier(name); }
};

inline std::string expression_for_row(const NativeColumnDefinition &column, const std::string &row) {
  const auto tokens=tokenize_definition(column.input_expression);
  std::string out;size_t consumed=0;
  for (size_t i=0;i<tokens.size();++i) {
    const auto &token=tokens[i];bool dependency=false;
    if (token.kind==SqlToken::IDENTIFIER && !(i+1<tokens.size() && tokens[i+1].value=="(")) {
      for (const auto &name:column.dependencies) {
        dependency=dependency || lower_identifier(token.value)==lower_identifier(name);
      }
    }
    if (!dependency) { continue; }
    size_t first=token.first;
    if (i>=2 && tokens[i-1].value=="." && tokens[i-2].kind==SqlToken::IDENTIFIER) { first=tokens[i-2].first; }
    if (first<consumed) { throw std::invalid_argument("Invalid qualified dependency"); }
    out.append(column.input_expression,consumed,first-consumed);
    out+=row+"."+quote_identifier(token.value);consumed=token.last;
  }
  out.append(column.input_expression,consumed,std::string::npos);
  return out;
}
inline std::string rowkey_as_sql_expression(const BoundAITable &table,const std::string &row) {
  if (table.primary_key.empty()) { throw std::invalid_argument("AI columns require an explicit primary key"); }
  std::string expression="CONCAT(";
  bool first=true;
  for (const auto &key:table.primary_key) {
    if (!first) { expression+=","+quote_literal(" AND ")+","; }
    expression+=quote_literal(quote_identifier(key.name)+" = ")+",";
    const auto reference=row+"."+quote_identifier(key.name);
    expression+=key.numeric?"CAST("+reference+" AS CHAR)":"QUOTE("+reference+")";
    first=false;
  }
  return expression+")";
}
inline std::string enqueue_row_sql(const BoundAITable &table,const BoundAIColumn &column) {
  return "INSERT INTO oceanbase.__all_ai_column_task (table_id,column_id,request_id,rowkey_predicate,"
      "input_text,state,retry_count,max_retries,eligible_ts,definition_version) VALUES ("+
      std::to_string(table.table_id)+","+std::to_string(column.column_id)+",NEW."+
      quote_identifier(column.request_name)+","+rowkey_as_sql_expression(table,"NEW")+","+
      expression_for_row(column.definition,"NEW")+",1,0,"+std::to_string(column.definition.max_retries)+
      ",NOW(6),"+std::to_string(column.definition_version)+");";
}

struct NativeTriggerPlan { std::vector<std::string> create_statements; };
inline NativeTriggerPlan build_transactional_trigger_plan(const BoundAITable &table) {
  if (table.table_id==0 || table.columns.empty() || table.primary_key.empty()) {
    throw std::invalid_argument("Incomplete AI table binding");
  }
  NativeTriggerPlan plan;
  const auto table_name=table.qualified_name();
  const auto prefix=quote_identifier(table.database)+".";
  const auto trigger=[&](const std::string &kind,const std::string &timing,const std::string &body) {
    return "CREATE TRIGGER "+prefix+quote_identifier("__aic_"+std::to_string(table.table_id)+"_"+kind)+
      " "+timing+" ON "+table_name+" FOR EACH ROW BEGIN "+body+" END";
  };
  std::string before_insert="DECLARE request_group VARCHAR(36);SET request_group=UUID();";
  std::string after_insert;
  std::string before_update="DECLARE request_group VARCHAR(36);SET request_group=UUID();";
  std::string after_update;
  std::string key_changed;
  for (const auto &key:table.primary_key) {
    if (!key_changed.empty()) { key_changed+=" OR "; }
    key_changed+="NOT(OLD."+quote_identifier(key.name)+" <=> NEW."+quote_identifier(key.name)+")";
  }
  for (const auto &column:table.columns) {
    const auto result=quote_identifier(column.definition.name);
    const auto state=quote_identifier(column.state_name);
    const auto request=quote_identifier(column.request_name);
    const auto spec=quote_identifier(column.spec_name);
    before_insert+="SET NEW."+result+"=NULL,NEW."+state+"=0,NEW."+request+
        "=request_group,NEW."+spec+"="+std::to_string(column.definition_version)+";";
    after_insert+=enqueue_row_sql(table,column);
    std::string changed=key_changed;
    for (const auto &name:column.definition.dependencies) {
      changed+=" OR NOT(OLD."+quote_identifier(name)+" <=> NEW."+quote_identifier(name)+")";
    }
    before_update+="IF "+changed+" THEN SET NEW."+result+"=NULL,NEW."+state+
        "=0,NEW."+request+"=request_group,NEW."+spec+"="+
        std::to_string(column.definition_version)+";ELSEIF NOT(OLD."+result+" <=> NEW."+result+") AND (OLD."+
        state+" <=> NEW."+state+") AND (OLD."+request+" <=> NEW."+request+") THEN "
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='AI column values are generated';END IF;";
    after_update+="IF NOT(OLD."+request+" <=> NEW."+request+") THEN DELETE FROM oceanbase.__all_ai_column_task WHERE table_id="+
        std::to_string(table.table_id)+" AND column_id="+std::to_string(column.column_id)+
        " AND rowkey_predicate="+rowkey_as_sql_expression(table,"OLD")+";"+
        enqueue_row_sql(table,column)+"END IF;";
  }
  plan.create_statements.push_back(trigger("bi","BEFORE INSERT",before_insert));
  plan.create_statements.push_back(trigger("ai","AFTER INSERT",after_insert));
  plan.create_statements.push_back(trigger("bu","BEFORE UPDATE",before_update));
  plan.create_statements.push_back(trigger("au","AFTER UPDATE",after_update));
  plan.create_statements.push_back(trigger("ad","AFTER DELETE",
      "DELETE FROM oceanbase.__all_ai_column_task WHERE table_id="+std::to_string(table.table_id)+
      " AND rowkey_predicate="+rowkey_as_sql_expression(table,"OLD")+";"));
  return plan;
}

struct StagedColumnResult { BoundAIColumn column;std::string sql_value;bool success; };
// This plan must execute under a row lock in one caller-owned native SQL
// transaction. A zero-row UPDATE is a stale/deleted row, never an INSERT.
inline std::pair<std::string,std::string> build_group_publication(
    const BoundAITable &table,const std::string &rowkey_predicate,
    const std::string &request_id,const std::vector<StagedColumnResult> &results) {
  if (results.empty() || rowkey_predicate.empty() || request_id.empty()) {
    throw std::invalid_argument("Empty AI publication group");
  }
  std::string update="UPDATE "+table.qualified_name()+" SET ";
  std::string fence=" WHERE ("+rowkey_predicate+")";
  std::set<uint64_t> columns;
  for (const auto &result:results) {
    const auto &column=result.column;
    if (!columns.insert(column.column_id).second) { throw std::invalid_argument("Duplicate publication column"); }
    if (columns.size()>1) { update+=","; }
    update+=quote_identifier(column.definition.name)+"="+(result.success?result.sql_value:"NULL")+
      ","+quote_identifier(column.state_name)+"="+(result.success?"1":"2");
    fence+=" AND "+quote_identifier(column.request_name)+"="+quote_literal(request_id)+
      " AND "+quote_identifier(column.spec_name)+"="+std::to_string(column.definition_version);
  }
  const auto cleanup="DELETE FROM oceanbase.__all_ai_column_task WHERE table_id="+
      std::to_string(table.table_id)+" AND request_id="+quote_literal(request_id)+
      " AND rowkey_predicate="+quote_literal(rowkey_predicate);
  return {update+fence,cleanup};
}

}}} // namespace oceanbase::share::ai_column
