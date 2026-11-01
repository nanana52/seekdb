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

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_

#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{

// Transient catalog write records. These do not change ObColumnSchemaV2 or
// pretend that CREATE TABLE / DML is wired. The caller owns all ObString data.
struct ObAiColumnDefinitionRow
{
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t definition_version_;
  int64_t schema_version_;
  ObString expression_sql_;
  ObString dependency_column_ids_;
  ObString model_name_;
  ObString ai_function_;
  int64_t return_type_;
  int64_t vector_dimension_;
  ObString sync_mode_;
  int64_t max_retries_;
  int64_t definition_status_;
};

struct ObAiColumnTaskRow
{
  uint64_t table_id_;
  uint64_t column_id_;
  ObString request_id_;
  int64_t definition_version_;
  int64_t schema_version_;
  ObString row_identity_;
  ObString row_incarnation_;
  int64_t dependency_revision_;
  ObString group_id_;
  int64_t group_size_;
  int64_t max_retries_;
};

class ObAiServiceProxy
{
public:
  ObAiServiceProxy() = default;
  ~ObAiServiceProxy() = default;
  // Must be called with the CALLER'S already started transaction.
  // Never start, end or commit a transaction in these methods. A DDL caller
  // must use the same transaction as its schema update; a DML caller must
  // use the same transaction as the base-row modification and invalidation.
  static int insert_ai_column_definition_in_trans(
      common::ObMySQLTransaction &trans, const ObAiColumnDefinitionRow &row);
  static int enqueue_ai_column_task_in_trans(
      common::ObMySQLTransaction &trans, const ObAiColumnTaskRow &row);
  // ai endpoint
  static int insert_ai_endpoint(common::ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint);
  static int select_ai_endpoint(common::ObArenaAllocator &allocator, common::ObISQLClient &sql_proxy,
                                const ObString &name, ObAiModelEndpointInfo &endpoint, bool for_update = false);
  static int drop_ai_model_endpoint(common::ObMySQLTransaction &trans, const ObString &name);
  static int update_ai_endpoint(common::ObMySQLTransaction &trans, const int64_t new_endpoint_version, const ObAiModelEndpointInfo &endpoint);
  static int select_ai_endpoint_by_ai_model_name(common::ObArenaAllocator &allocator, common::ObISQLClient &sql_proxy,
                                                 const ObString &ai_model_name, common::ObNameCaseMode name_case_mode, ObAiModelEndpointInfo &endpoint);
  static int check_ai_endpoint_exists(common::ObArenaAllocator &allocator, common::ObISQLClient &sql_proxy, const ObString &name, bool &is_exists);

private:
  // ai endpoint
  static int build_ai_endpoint_(common::ObArenaAllocator &allocator, common::sqlclient::ObMySQLResult &result, ObAiModelEndpointInfo &endpoint);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_PROXY_H_
