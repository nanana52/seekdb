// Copyright (c) 2026 OceanBase competition contributors.
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstring>
#include "share/ai_service/ob_ai_column_runtime_plan.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase { namespace share { namespace ai_column {
constexpr const char *COLUMN_METADATA_PREFIX = "__seekdb_ai_column_v1:";
inline bool has_native_definition(const schema::ObColumnSchemaV2 &column) {
  const auto comment=column.get_comment_str();
  return comment.length()>static_cast<int64_t>(std::strlen(COLUMN_METADATA_PREFIX)) &&
      std::memcmp(comment.ptr(),COLUMN_METADATA_PREFIX,std::strlen(COLUMN_METADATA_PREFIX))==0;
}
inline NativeColumnDefinition native_definition(const schema::ObColumnSchemaV2 &column) {
  const auto comment=column.get_comment_str();
  const size_t prefix=std::strlen(COLUMN_METADATA_PREFIX);
  const std::string text(comment.ptr()+prefix,comment.length()-prefix);
  const auto tokens=tokenize_definition(text);
  return parse_native_column(text,tokens,0,tokens.size());
}
inline int validate_native_models(const schema::ObTableSchema &table, schema::ObSchemaGetterGuard &guard) {
  try {
    for (auto it=table.column_begin();it!=table.column_end();++it) {
      if (!has_native_definition(**it)) { continue; }
      const auto definition=native_definition(**it);
      const schema::ObAiModelSchema *model=nullptr;
      const common::ObString name(definition.model_name.size(),definition.model_name.data());
      const int status=guard.get_ai_model_schema(name,model);
      if (status!=common::OB_SUCCESS) { return status; }
      if (!model) { return common::OB_AI_FUNC_MODEL_NOT_FOUND; }
      const bool embedding=model->get_type()==share::EndpointType::DENSE_EMBEDDING;
      if (embedding!=definition.embedding || (!embedding && model->get_type()!=share::EndpointType::COMPLETION)) { return common::OB_NOT_SUPPORTED; }
    }
  } catch (...) { return common::OB_INVALID_ARGUMENT; }
  return common::OB_SUCCESS;
}
inline std::string hidden_state_name(uint64_t id) { return "__aic_state_"+std::to_string(id); }
inline std::string hidden_request_name(uint64_t id) { return "__aic_request_"+std::to_string(id); }
inline std::string hidden_spec_name(uint64_t id) { return "__aic_spec_"+std::to_string(id); }
inline bool is_native_hidden_name(const common::ObString &name) {
  return name.length()>6 && std::memcmp(name.ptr(),"__aic_",6)==0;
}
inline int add_native_hidden_columns(schema::ObTableSchema &table) {
  int ret=common::OB_SUCCESS;
  try {
    std::vector<NativeColumnDefinition> definitions;
    std::vector<uint64_t> ids;
    for (auto it=table.column_begin();it!=table.column_end();++it) {
      if (has_native_definition(**it)) { definitions.push_back(native_definition(**it));ids.push_back((*it)->get_column_id()); }
      else { NativeColumnDefinition ordinary;const auto name=(*it)->get_column_name_str();ordinary.name.assign(name.ptr(),name.length());definitions.push_back(std::move(ordinary)); }
    }
    if (ids.empty()) { return ret; }
    bind_native_dependencies(definitions);
    if (table.get_rowkey_column_num()==0) { return common::OB_NOT_SUPPORTED; }
    for (const auto id:ids) {
      for (int kind=0;kind<3 && ret==common::OB_SUCCESS;++kind) {
        const auto name=kind==0?hidden_state_name(id):kind==1?hidden_request_name(id):hidden_spec_name(id);
        if (table.get_column_schema(common::ObString(name.size(),name.data()))!=nullptr) { continue; }
        schema::ObColumnSchemaV2 hidden;
        hidden.set_column_id(table.get_max_used_column_id()+1);
        hidden.set_table_id(table.get_table_id());
        hidden.set_nullable(true);hidden.set_is_hidden(true);
        hidden.add_column_flag(INVISIBLE_COLUMN_FLAG);
        if (kind==1) { hidden.set_data_type(common::ObVarcharType);hidden.set_data_length(36);hidden.set_collation_type(common::CS_TYPE_UTF8MB4_BIN); }
        else { hidden.set_data_type(common::ObIntType);hidden.set_data_precision(20);hidden.set_data_scale(0); }
        if ((ret=hidden.set_column_name(common::ObString(name.size(),name.data())))==common::OB_SUCCESS) {
          ret=table.add_column(hidden);
        }
      }
    }
  } catch (const std::exception &) { ret=common::OB_NOT_SUPPORTED; }
  return ret;
}
inline BoundAITable bind_native_table(const schema::ObTableSchema &table,const std::string &database) {
  BoundAITable bound;bound.table_id=table.get_table_id();bound.database=database;
  const auto name=table.get_table_name_str();bound.name.assign(name.ptr(),name.length());
  std::vector<NativeColumnDefinition> definitions;
  for (auto it=table.column_begin();it!=table.column_end();++it) {
    if ((*it)->is_hidden()) { continue; }
    if (has_native_definition(**it)) { definitions.push_back(native_definition(**it)); }
    else { NativeColumnDefinition ordinary;const auto n=(*it)->get_column_name_str();ordinary.name.assign(n.ptr(),n.length());definitions.push_back(std::move(ordinary)); }
  }
  bind_native_dependencies(definitions);
  for (const auto &definition:definitions) {
    if (!definition.ai) { continue; }
    const auto *column=table.get_column_schema(common::ObString(definition.name.size(),definition.name.data()));
    BoundAIColumn result;result.definition=definition;result.column_id=column->get_column_id();
    result.state_name=hidden_state_name(result.column_id);result.request_name=hidden_request_name(result.column_id);result.spec_name=hidden_spec_name(result.column_id);
    const auto lookup=[&](const std::string &n) { return table.get_column_schema(common::ObString(n.size(),n.data())); };
    const auto *state=lookup(result.state_name),*request=lookup(result.request_name),*spec=lookup(result.spec_name);
    if (!state||!request||!spec) { throw std::invalid_argument("AI hidden columns absent"); }
    result.state_column_id=state->get_column_id();result.request_column_id=request->get_column_id();result.result_spec_column_id=spec->get_column_id();
    bound.columns.push_back(std::move(result));
  }
  for (int64_t at=0;at<table.get_rowkey_column_num();++at) {
    uint64_t id=0;
    if (table.get_rowkey_info().get_column_id(at,id)!=common::OB_SUCCESS) { throw std::invalid_argument("Invalid AI row key"); }
    const auto *column=table.get_column_schema(id);
    const auto n=column->get_column_name_str();bound.primary_key.push_back({std::string(n.ptr(),n.length()),common::ob_is_integer_type(column->get_data_type())});
  }
  return bound;
}
inline std::vector<std::string> native_catalog_insert_statements(const BoundAITable &table) {
  std::vector<std::string> statements;
  for (const auto &column:table.columns) {
    std::string dependency_names;
    for (const auto &name:column.definition.dependencies) { if(!dependency_names.empty())dependency_names+='\n';dependency_names+=name; }
    const auto normalized=column.definition.expression_sql+
      (column.definition.typed_instruction.empty()?std::string():" /* "+column.definition.typed_instruction+" */");
    statements.push_back("INSERT INTO oceanbase.__all_ai_column (table_id,column_id,column_name,database_name,table_name,normalized_expr,model_name,ai_function,result_type,vector_dimension,max_retries,definition_version,dependency_column_names,state_column_id,request_column_id,result_spec_column_id,state_column_name,request_column_name,result_spec_column_name) VALUES ("+
      std::to_string(table.table_id)+","+std::to_string(column.column_id)+","+quote_literal(column.definition.name)+","+
      quote_literal(table.database)+","+quote_literal(table.name)+","+quote_literal(normalized)+","+quote_literal(column.definition.model_name)+","+
      quote_literal(column.definition.embedding?"AI_EMBED":"AI_COMPLETE")+","+quote_literal(column.definition.type_sql)+","+
      std::to_string(column.definition.dimension)+","+std::to_string(column.definition.max_retries)+",1,"+quote_literal(dependency_names)+","+
      std::to_string(column.state_column_id)+","+std::to_string(column.request_column_id)+","+std::to_string(column.result_spec_column_id)+","+
      quote_literal(column.state_name)+","+quote_literal(column.request_name)+","+quote_literal(column.spec_name)+")");
  }
  return statements;
}

}}} // namespace oceanbase::share::ai_column
