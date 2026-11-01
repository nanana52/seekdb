#!/usr/bin/env python3
"""Compile and exercise the actual catalog writer method bodies against strict C++ mocks.

This is a fast isolated contract test, NOT a seekdb native compilation or an SQL
end-to-end test. It checks emitted columns, hex escaping, transaction ownership,
input rejection and affected-row fencing against the shipped C++ sources.
"""
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
CPP = (ROOT / "src/observer/ai_service/ob_ai_service_proxy.cpp").read_text()
HDR = (ROOT / "src/observer/ai_service/ob_ai_service_proxy.h").read_text()

A = "int ObAiServiceProxy::insert_ai_column_definition_in_trans("
B = "int ObAiServiceProxy::enqueue_ai_column_task_in_trans("
C = "int ObAiServiceProxy::try_claim_ai_column_task_in_trans("
assert CPP.count(A) == CPP.count(B) == CPP.count(C) == 1
methods = CPP[CPP.index(A):CPP.index(C)]
structs = HDR[HDR.index("struct ObAiColumnDefinitionRow"):HDR.index("class ObAiServiceProxy")]
for name in ["insert_ai_column_definition_in_trans", "enqueue_ai_column_task_in_trans"]:
    assert ("static int " + name + "(") in HDR
    body = methods.split("int ObAiServiceProxy::" + name + "(", 1)[1].split("\n}\n", 1)[0]
    assert "trans.write(buffer.ptr(), affected_rows)" in body
    assert "trans.start(" not in body and "trans.end(" not in body

stub = r'''
#include <cstdint>
#include <climits>
#include <cassert>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <iostream>
#include <algorithm>
using int64_t = std::int64_t;
using uint64_t = std::uint64_t;
static constexpr int OB_SUCCESS = 0;
static constexpr int OB_INVALID_ARGUMENT = -2;
static constexpr int OB_ERR_UNEXPECTED = -3;
static constexpr uint64_t OB_INVALID_ID = UINT64_MAX;
#define OB_FAIL(expr) ((ret = (expr)) != OB_SUCCESS)
struct ObString {
  std::string s;
  ObString() = default;
  ObString(const char *v) : s(v) {}
  ObString(const std::string &v) : s(v) {}
  bool empty() const { return s.empty(); }
  int64_t length() const { return static_cast<int64_t>(s.size()); }
  int compare(const char *v) const { return s.compare(v); }
};
struct ObHexEscapeSqlStr {
  const ObString &s;
  explicit ObHexEscapeSqlStr(const ObString &v) : s(v) {}
};
ObString nonnull_empty_string(const ObString &s) { return s; }
struct ObSqlString {
  std::string value;
  const char *ptr() const { return value.c_str(); }
};
struct ObDMLSqlSplicer {
  std::vector<std::pair<std::string,std::string>> fields;
  static std::string encode(const ObHexEscapeSqlStr &v) {
    const char *hexdigits="0123456789ABCDEF";
    std::string out="X'";
    for (unsigned char c : v.s.s) {
      out.push_back(hexdigits[c >> 4]);
      out.push_back(hexdigits[c & 15]);
    }
    return out + "'";
  }
  template <class T> static std::string encode(const T &v) {
    return std::to_string(v);
  }
  template <class T> int add_column(const char *name, const T &v) {
    fields.emplace_back(name, encode(v)); return OB_SUCCESS;
  }
  template <class T> int add_pk_column(const char *name, const T &v) {
    return add_column(name, v);
  }
  int splice_insert_sql(const char *name, ObSqlString &out) {
    out.value="INSERT INTO " + std::string(name) + " (";
    for (size_t i=0;i<fields.size();++i) {
      if (i) out.value+=",";
      out.value+=fields[i].first;
    }
    out.value+=") VALUES (";
    for (size_t i=0;i<fields.size();++i) {
      if (i) out.value+=",";
      out.value+=fields[i].second;
    }
    out.value+=")";
    return OB_SUCCESS;
  }
};
struct ObMySQLTransaction {
  bool started = true;
  int64_t affected = 1;
  std::vector<std::string> writes;
  bool is_started() const { return started; }
  int write(const char *sql, int64_t &count) {
    writes.emplace_back(sql);
    count = affected;
    return OB_SUCCESS;
  }
};
'''
cls = r'''
class ObAiServiceProxy {
public:
  static int insert_ai_column_definition_in_trans(
      ObMySQLTransaction &trans, const ObAiColumnDefinitionRow &row);
  static int enqueue_ai_column_task_in_trans(
      ObMySQLTransaction &trans, const ObAiColumnTaskRow &row);
};
'''
tests = r'''
int main() {
  ObMySQLTransaction tx;
  ObAiColumnDefinitionRow d{};
  d.table_id_ = 90000; d.column_id_ = 21;
  d.definition_version_ = 1; d.schema_version_ = 2;
  d.expression_sql_ = ObString("AI_COMPLETE('model', note); DROP TABLE users;");
  d.dependency_column_ids_ = ObString("");
  d.model_name_ = ObString("model");
  d.ai_function_ = ObString("AI_COMPLETE");
  d.return_type_ = 5; d.vector_dimension_ = 0;
  d.sync_mode_ = ObString("async"); d.max_retries_ = 2;
  d.definition_status_ = 0;
  assert(OB_SUCCESS == ObAiServiceProxy::insert_ai_column_definition_in_trans(tx, d));
  assert(tx.writes.size() == 1);
  assert(tx.writes.back().find("__all_ai_column_definition") != std::string::npos);
  assert(tx.writes.back().find("DROP TABLE users") == std::string::npos);
  assert(tx.writes.back().find("expression_sql") != std::string::npos);
  assert(tx.writes.back().find("dependency_column_ids") != std::string::npos);
  assert(tx.writes.back().find("X''") != std::string::npos);
  d.sync_mode_ = ObString("sync");
  assert(OB_INVALID_ARGUMENT == ObAiServiceProxy::insert_ai_column_definition_in_trans(tx, d));
  assert(tx.writes.size() == 1);
  d.sync_mode_ = ObString("async");
  tx.started = false;
  assert(OB_INVALID_ARGUMENT == ObAiServiceProxy::insert_ai_column_definition_in_trans(tx, d));
  tx.started = true;
  tx.affected = 0;
  assert(OB_ERR_UNEXPECTED == ObAiServiceProxy::insert_ai_column_definition_in_trans(tx, d));
  tx.affected = 1;
  ObAiColumnTaskRow t{};
  t.table_id_ = 90000; t.column_id_ = 21;
  t.request_id_ = ObString("7a");
  t.definition_version_ = 1; t.schema_version_ = 2;
  t.row_identity_ = ObString("pk:'quote'\\\\");
  t.row_incarnation_ = ObString("incarnation-1");
  t.dependency_revision_ = 1; t.group_id_ = ObString("group-1");
  t.group_size_ = 1; t.max_retries_ = 2;
  assert(OB_SUCCESS == ObAiServiceProxy::enqueue_ai_column_task_in_trans(tx, t));
  assert(tx.writes.back().find("__all_ai_column_task") != std::string::npos);
  assert(tx.writes.back().find("pk:'quote'") == std::string::npos);
  assert(tx.writes.back().find("dependency_revision") != std::string::npos);
  assert(tx.writes.back().find("request_id") != std::string::npos);
  const auto done = tx.writes.size();
  t.request_id_ = ObString(std::string(65, 'x'));
  assert(OB_INVALID_ARGUMENT == ObAiServiceProxy::enqueue_ai_column_task_in_trans(tx, t));
  assert(tx.writes.size() == done);
  t.request_id_ = ObString("request-2");
  t.dependency_revision_ = 0;
  assert(OB_INVALID_ARGUMENT == ObAiServiceProxy::enqueue_ai_column_task_in_trans(tx, t));
  t.dependency_revision_ = 1;
  tx.started = false;
  assert(OB_INVALID_ARGUMENT == ObAiServiceProxy::enqueue_ai_column_task_in_trans(tx, t));
  std::cout << "AI_COLUMN_CATALOG_WRITER_MOCK_PASS checks=hex_encoding,tx_guard,affected_rows,defaults,invalid_inputs\n";
}
'''
with tempfile.TemporaryDirectory(prefix="ai-writer-test-") as td:
    src = pathlib.Path(td) / "test.cpp"
    exe = pathlib.Path(td) / "test"
    src.write_text(stub + structs + cls + methods + tests)
    subprocess.run(["g++", "-std=c++14", "-Wall", "-Wextra", "-Werror", "-pedantic",
                    str(src), "-o", str(exe)], check=True)
    subprocess.run([str(exe)], check=True)
