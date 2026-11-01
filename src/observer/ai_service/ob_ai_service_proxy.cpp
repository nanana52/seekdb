/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "rust/config.h"
#include "observer/ai_service/ob_ai_service_proxy.h"
#include "share/ob_share_util.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_server_struct.h"
#include "share/ob_max_id_fetcher.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "common/ob_timeout_ctx.h"
#include "lib/utility/ob_utility.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::sqlclient;
using namespace oceanbase::transaction;

namespace
{
// ObHexEscapeSqlStr serializes a null pointer as SQL NULL even when length is
// zero. Local keyless providers need an actual empty string in the system table.
ObString nonnull_empty_string(const ObString &value)
{
  return value.empty() && OB_ISNULL(value.ptr()) ? ObString(0, "") : value;
}
}

// These use the two generated native system tables. They deliberately do not
// acquire or commit transactions: the schema/DML writer owns atomicity.
int ObAiServiceProxy::insert_ai_column_definition_in_trans(
    ObMySQLTransaction &trans, const ObAiColumnDefinitionRow &row)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  if (!trans.is_started() || row.table_id_ == OB_INVALID_ID ||
      row.column_id_ == OB_INVALID_ID || row.table_id_ == 0 || row.column_id_ == 0 ||
      row.definition_version_ <= 0 || row.schema_version_ <= 0 ||
      row.expression_sql_.empty() || row.model_name_.empty() ||
      row.ai_function_.empty() || row.sync_mode_.compare("async") != 0 ||
      row.vector_dimension_ < 0 || row.max_retries_ < 0 ||
      row.definition_status_ != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sql.add_pk_column("table_id", row.table_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("column_id", row.column_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("definition_version", row.definition_version_))) {
  } else if (OB_FAIL(sql.add_column("schema_version", row.schema_version_))) {
  } else if (OB_FAIL(sql.add_column("expression_sql", ObHexEscapeSqlStr(row.expression_sql_)))) {
  } else if (OB_FAIL(sql.add_column("dependency_column_ids", ObHexEscapeSqlStr(nonnull_empty_string(row.dependency_column_ids_))))) {
  } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(row.model_name_)))) {
  } else if (OB_FAIL(sql.add_column("ai_function", ObHexEscapeSqlStr(row.ai_function_)))) {
  } else if (OB_FAIL(sql.add_column("return_type", row.return_type_))) {
  } else if (OB_FAIL(sql.add_column("vector_dimension", row.vector_dimension_))) {
  } else if (OB_FAIL(sql.add_column("sync_mode", ObHexEscapeSqlStr(row.sync_mode_)))) {
  } else if (OB_FAIL(sql.add_column("max_retries", row.max_retries_))) {
  } else if (OB_FAIL(sql.add_column("definition_status", row.definition_status_))) {
  } else if (OB_FAIL(sql.splice_insert_sql("__all_ai_column_definition", buffer))) {
  } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObAiServiceProxy::enqueue_ai_column_task_in_trans(
    ObMySQLTransaction &trans, const ObAiColumnTaskRow &row)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  if (!trans.is_started() || row.table_id_ == OB_INVALID_ID ||
      row.column_id_ == OB_INVALID_ID || row.table_id_ == 0 || row.column_id_ == 0 ||
      row.definition_version_ <= 0 || row.schema_version_ <= 0 ||
      row.request_id_.empty() || row.request_id_.length() > 64 ||
      row.row_identity_.empty() || row.row_incarnation_.empty() ||
      row.row_incarnation_.length() > 64 || row.group_id_.empty() ||
      row.group_id_.length() > 64 || row.dependency_revision_ <= 0 ||
      row.group_size_ <= 0 || row.max_retries_ < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sql.add_pk_column("table_id", row.table_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("column_id", row.column_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("request_id", ObHexEscapeSqlStr(row.request_id_)))) {
  } else if (OB_FAIL(sql.add_column("definition_version", row.definition_version_))) {
  } else if (OB_FAIL(sql.add_column("schema_version", row.schema_version_))) {
  } else if (OB_FAIL(sql.add_column("row_identity", ObHexEscapeSqlStr(row.row_identity_)))) {
  } else if (OB_FAIL(sql.add_column("row_incarnation", ObHexEscapeSqlStr(row.row_incarnation_)))) {
  } else if (OB_FAIL(sql.add_column("dependency_revision", row.dependency_revision_))) {
  } else if (OB_FAIL(sql.add_column("group_id", ObHexEscapeSqlStr(row.group_id_)))) {
  } else if (OB_FAIL(sql.add_column("group_size", row.group_size_))) {
  } else if (OB_FAIL(sql.add_column("max_retries", row.max_retries_))) {
  // status, attempts, next_due and lease fields use the catalog's defaults.
  } else if (OB_FAIL(sql.splice_insert_sql("__all_ai_column_task", buffer))) {
  } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObAiServiceProxy::insert_ai_endpoint(ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  
  if (OB_FAIL(sql.add_pk_column("endpoint_id", endpoint.endpoint_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("scope", ObHexEscapeSqlStr(endpoint.scope_)))) {
  } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
  } else if (OB_FAIL(sql.add_column("endpoint_name", ObHexEscapeSqlStr(endpoint.name_)))) {
  } else if (OB_FAIL(sql.add_column("ai_model_name", ObHexEscapeSqlStr(endpoint.ai_model_name_)))) {
  } else if (OB_FAIL(sql.add_column("url", ObHexEscapeSqlStr(endpoint.url_)))) {
  } else if (OB_FAIL(sql.add_column("access_key", ObHexEscapeSqlStr(nonnull_empty_string(endpoint.access_key_))))) {
  } else if (OB_FAIL(sql.add_column("request_model_name", ObHexEscapeSqlStr(endpoint.request_model_name_)))) {
  } else if (OB_FAIL(sql.add_column("provider", ObHexEscapeSqlStr(endpoint.provider_)))) {
  } else if (OB_FAIL(sql.add_column("parameters", ObHexEscapeSqlStr(endpoint.parameters_)))) {
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ObHexEscapeSqlStr(endpoint.request_transform_fn_)))) {
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ObHexEscapeSqlStr(endpoint.response_transform_fn_)))) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
  } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    ret = OB_ENTRY_EXIST;
    LOG_USER_ERROR(OB_ENTRY_EXIST, "endpoint already exists");
  }
  return ret;
}

int ObAiServiceProxy::update_ai_endpoint(ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  
  if (OB_FAIL(sql.add_pk_column("endpoint_id", endpoint.endpoint_id_))) {
  } else if (OB_FAIL(sql.add_pk_column("scope", ObHexEscapeSqlStr(endpoint.scope_)))) {
  } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
  } else if (OB_FAIL(sql.add_column("endpoint_name", ObHexEscapeSqlStr(endpoint.name_)))) {
  } else if (OB_FAIL(sql.add_column("ai_model_name", ObHexEscapeSqlStr(endpoint.ai_model_name_)))) {
  } else if (OB_FAIL(sql.add_column("url", ObHexEscapeSqlStr(endpoint.url_)))) {
  } else if (OB_FAIL(sql.add_column("access_key", ObHexEscapeSqlStr(nonnull_empty_string(endpoint.access_key_))))) {
  } else if (OB_FAIL(sql.add_column("request_model_name", ObHexEscapeSqlStr(endpoint.request_model_name_)))) {
  } else if (OB_FAIL(sql.add_column("provider", ObHexEscapeSqlStr(endpoint.provider_)))) {
  } else if (OB_FAIL(sql.add_column("parameters", ObHexEscapeSqlStr(endpoint.parameters_)))) {
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ObHexEscapeSqlStr(endpoint.request_transform_fn_)))) {
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ObHexEscapeSqlStr(endpoint.response_transform_fn_)))) {
  } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
  } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObAiServiceProxy::select_ai_endpoint(ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
                                         const ObString &name, ObAiModelEndpointInfo &endpoint, bool for_update)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = config::internal_sql_execute_timeout();
  endpoint.reset();
  
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME))) {
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
  } else if (for_update && (OB_FAIL(sql.append_fmt(" FOR UPDATE")))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
          LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_NOT_FOUND, name.length(), name.ptr());
        } else {
        }
      } else if (OB_FAIL(build_ai_endpoint_(allocator, *result, endpoint))) {
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObAiServiceProxy::select_ai_endpoint_by_ai_model_name(ObArenaAllocator &allocator, ObISQLClient &sql_proxy,
  const ObString &ai_model_name, ObNameCaseMode name_case_mode, ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = config::internal_sql_execute_timeout();
  endpoint.reset();
  
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s ", OB_ALL_AI_MODEL_ENDPOINT_TNAME))) {
  } else {
    if (OB_ORIGIN_AND_INSENSITIVE == name_case_mode || OB_LOWERCASE_AND_INSENSITIVE == name_case_mode) {
      if (OB_FAIL(sql.append(" WHERE ai_model_name = "))) {
      } else if (OB_FAIL(sql_append_hex_escape_str(ai_model_name, sql))) {
      }
    } else if (OB_ORIGIN_AND_SENSITIVE == name_case_mode) {
      if (OB_FAIL(sql.append(" WHERE ai_model_name COLLATE utf8mb4_bin = "))) {
      } else if (OB_FAIL(sql_append_hex_escape_str(ai_model_name, sql))) {
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid name case mode");
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
        } else {
        }
      } else if (OB_FAIL(build_ai_endpoint_(allocator, *result, endpoint))) {
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObAiServiceProxy::build_ai_endpoint_(ObArenaAllocator &allocator, ObMySQLResult &result, ObAiModelEndpointInfo &endpoint)
{
  int ret = OB_SUCCESS;
  endpoint.reset();
  ObString scope;
  ObString name;
  ObString ai_model_name;
  ObString url;
  ObString access_key;
  ObString provider;
  ObString request_model_name;
  ObString arguments;
  ObString request_transform_fn;
  ObString response_transform_fn;
  int64_t type = 0;
  EXTRACT_INT_FIELD_MYSQL(result, "endpoint_id", endpoint.endpoint_id_, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "scope", scope);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "endpoint_name", name);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "ai_model_name", ai_model_name);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "url", url);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "access_key", access_key, true, false, ""/*keyless provider*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "provider", provider, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "request_model_name", request_model_name, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "parameters", arguments, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "request_transform_fn", request_transform_fn, true, false, ""/*wont be used*/);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "response_transform_fn", response_transform_fn, true, false, ""/*wont be used*/);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, scope, endpoint.scope_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, name, endpoint.name_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, ai_model_name, endpoint.ai_model_name_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, url, endpoint.url_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, access_key, endpoint.access_key_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, provider, endpoint.provider_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, request_model_name, endpoint.request_model_name_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, arguments, endpoint.parameters_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, request_transform_fn, endpoint.request_transform_fn_))) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator, response_transform_fn, endpoint.response_transform_fn_))) {
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(endpoint.check_valid())) {
    }
  }

  return ret;
}

int ObAiServiceProxy::drop_ai_model_endpoint(ObMySQLTransaction &trans, const ObString &name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME))) {
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
  } else if (0 == affected_rows) {
    ret = OB_AI_FUNC_ENDPOINT_NOT_FOUND;
    LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_NOT_FOUND, name.length(), name.ptr());
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObAiServiceProxy::check_ai_endpoint_exists(ObArenaAllocator &allocator, ObISQLClient &sql_proxy, const ObString &name, bool &is_exists)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t default_timeout = config::internal_sql_execute_timeout();
  
  int64_t count = 0;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT count(*) FROM %s WHERE endpoint_name = ",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME))) {
  } else if (OB_FAIL(sql_append_hex_escape_str(name, sql))) {
  } else if (OB_FAIL(sql.append(" FOR UPDATE"))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      const int64_t idx = 0;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int(idx, count))) {
      } else if (count > 1) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_ITER_END != (tmp_ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_exists = count == 1;
  }
  return ret;
}
