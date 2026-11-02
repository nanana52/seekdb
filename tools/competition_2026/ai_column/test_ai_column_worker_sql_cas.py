#!/usr/bin/env python3
"""Compile actual C++ catalog CAS method bodies with small mocks, and exercise emitted SQL in SQLite.

This verifies SQL transitions and race/fence predicates only. This is NOT a
seekdb native build, runtime worker, user-DML integration or SQL E2E test.
"""
from pathlib import Path
import sqlite3
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
cpp = (ROOT / 'src/observer/ai_service/ob_ai_service_proxy.cpp').read_text()
hdr = (ROOT / 'src/observer/ai_service/ob_ai_service_proxy.h').read_text()
from re import search, findall
canonical = (ROOT / 'src/share/ai_service/ob_ai_column_task_state.h').read_text()
a = search(r'enum class TaskState\s*:\s*uint8_t\s*\{(.*?)\};', canonical, flags=16)
b = search(r'enum class AiCatalogSqlTaskStatus\s*:\s*int\s*\{(.*?)\};', cpp, flags=16)
assert a and b
values = lambda x: dict((k,int(v)) for k,v in findall(r'([A-Z_]+)\s*=\s*(\d+)', x.group(1)))
assert all(values(a).get(k) == v for k,v in values(b).items())
start = 'int ObAiServiceProxy::try_claim_ai_column_task_in_trans('
end = 'int ObAiServiceProxy::insert_ai_endpoint('
assert cpp.count(start) == 1 and cpp.count(end) == 1
methods = cpp[cpp.index(start):cpp.index(end)]
struct_start = 'struct ObAiColumnTaskLease'
assert hdr.count(struct_start) == 1
struct = hdr[hdr.index(struct_start):hdr.index('class ObAiServiceProxy')]
for name in ('try_claim_ai_column_task_in_trans',
             'stage_ai_column_success_in_trans',
             'renew_ai_column_task_lease_in_trans',
             'recover_expired_ai_column_task_in_trans',
             'stage_ai_column_failure_in_trans'):
    assert ('static int ' + name + '(') in hdr
    method = methods.split('int ObAiServiceProxy::' + name + '(', 1)[1].split('\n}\n', 1)[0]
    assert 'trans.write(sql.ptr(), affected_rows)' in method
    assert 'trans.start(' not in method and 'trans.end(' not in method
    assert 'lease_token' in methods

stub = r'''
#include <cassert>
#include <cinttypes>
#include <cstdarg>
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
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
  ObString(const char *v):s(v){}
  bool empty() const { return s.empty(); }
  int64_t length() const { return (int64_t)s.size(); }
};
ObString nonnull_empty_string(const ObString &v) { return v; }
struct ObSqlString {
  std::string value;
  const char *ptr() const { return value.c_str(); }
  int append_fmt(const char *fmt, ...) {
    char buffer[4096]; va_list args; va_start(args, fmt);
    const int n = vsnprintf(buffer, sizeof(buffer), fmt, args); va_end(args);
    if (n < 0 || n >= (int)sizeof(buffer)) return -8;
    value.append(buffer, (size_t)n); return OB_SUCCESS;
  }
  int assign_fmt(const char *fmt, ...) {
    char buffer[4096]; va_list args; va_start(args, fmt);
    const int n = vsnprintf(buffer, sizeof(buffer), fmt, args); va_end(args);
    if (n < 0 || n >= (int)sizeof(buffer)) return -8;
    value.assign(buffer, (size_t)n); return OB_SUCCESS;
  }
};
int sql_append_hex_escape_str(const ObString &s, ObSqlString &out) {
  static const char hexdigits[] = "0123456789ABCDEF";
  out.value.append("X'");
  for (unsigned char c: s.s) {
    out.value.push_back(hexdigits[c >> 4]);
    out.value.push_back(hexdigits[c & 15]);
  }
  out.value.push_back('\''); return OB_SUCCESS;
}
struct ObMySQLTransaction {
  bool started = true;
  int64_t affected = 1;
  std::vector<std::string> writes;
  bool is_started() const { return started; }
  int write(const char *sql, int64_t &count) {
    writes.emplace_back(sql); count=affected; return OB_SUCCESS;
  }
};
#include "share/ai_service/ob_ai_column_task_state.h"
namespace ai_column = oceanbase::share::ai_column;
using AiCatalogSqlTaskStatus = oceanbase::share::ai_column::TaskState;
'''
cls = r'''
class ObAiServiceProxy {
public:
  static int try_claim_ai_column_task_in_trans(ObMySQLTransaction &, const ObAiColumnTaskLease &, bool &);
  static int renew_ai_column_task_lease_in_trans(ObMySQLTransaction &, const ObAiColumnTaskLease &, bool &);
  static int stage_ai_column_success_in_trans(ObMySQLTransaction &, const ObAiColumnTaskLease &, const ObString &, int64_t, bool &);
  static int recover_expired_ai_column_task_in_trans(ObMySQLTransaction &, const ObAiColumnTaskLease &, int64_t, bool &);
  static int stage_ai_column_failure_in_trans(ObMySQLTransaction &, const ObAiColumnTaskLease &, bool, int64_t, int64_t, const ObString &, bool &);
};
'''
tests = r'''
int main() {
  ObMySQLTransaction tx;
  ObAiColumnTaskLease lease{};
  lease.table_id_=1234; lease.column_id_=17; lease.request_id_="req'\\x";
  lease.definition_version_=1; lease.dependency_revision_=8;
  lease.row_incarnation_="g'42"; lease.lease_owner_="worker1";
  lease.lease_token_="token1"; lease.now_us_=100; lease.lease_deadline_us_=200;
  bool ok=false;
  int rc=ObAiServiceProxy::try_claim_ai_column_task_in_trans(tx, lease, ok);
  assert(rc==OB_SUCCESS && ok && tx.writes.size()==1);
  std::cout << "CLAIM|" << tx.writes.back() << '\n';
  lease.now_us_=125; lease.lease_deadline_us_=300;
  rc=ObAiServiceProxy::renew_ai_column_task_lease_in_trans(tx,lease,ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "RENEW|" << tx.writes.back() << '\n';
  lease.now_us_=100; lease.lease_deadline_us_=200;
  rc=ObAiServiceProxy::stage_ai_column_success_in_trans(tx,lease,ObString("answer'"),1,ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "STAGE|" << tx.writes.back() << '\n';
  lease.now_us_=201;
  rc=ObAiServiceProxy::recover_expired_ai_column_task_in_trans(tx,lease,211,ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "RECOVER|" << tx.writes.back() << '\n';
  lease.now_us_=120;
  rc=ObAiServiceProxy::stage_ai_column_failure_in_trans(tx,lease,true,135,499,ObString("temporary'error"),ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "FAIL_RETRY|" << tx.writes.back() << '\n';
  rc=ObAiServiceProxy::stage_ai_column_failure_in_trans(tx,lease,false,0,400,ObString("terminal"),ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "FAIL_FINAL|" << tx.writes.back() << '\n';
  lease.now_us_=201;

  lease.now_us_=220;lease.lease_deadline_us_=300;lease.lease_token_="token2";
  rc=ObAiServiceProxy::try_claim_ai_column_task_in_trans(tx,lease,ok);
  assert(rc==OB_SUCCESS && ok);
  std::cout << "RECLAIM|" << tx.writes.back() << '\n';
  lease.now_us_=100;lease.lease_deadline_us_=200;lease.lease_token_="token1";
  tx.affected=0;
  rc=ObAiServiceProxy::try_claim_ai_column_task_in_trans(tx,lease,ok);
  assert(rc==OB_SUCCESS && !ok);
  tx.affected=2;
  rc=ObAiServiceProxy::try_claim_ai_column_task_in_trans(tx,lease,ok);
  assert(rc==OB_ERR_UNEXPECTED && !ok);
  tx.affected=1;
  tx.started=false;
  size_t before=tx.writes.size();
  rc=ObAiServiceProxy::stage_ai_column_success_in_trans(tx,lease,ObString("x"),1,ok);
  assert(rc==OB_INVALID_ARGUMENT && !ok && tx.writes.size()==before);
  tx.started=true;
  lease.lease_token_="";
  rc=ObAiServiceProxy::try_claim_ai_column_task_in_trans(tx,lease,ok);
  assert(rc==OB_INVALID_ARGUMENT && !ok && tx.writes.size()==before);
  std::cout << "CPP_CONTRACT_PASS" << '\n';
}
'''

with tempfile.TemporaryDirectory() as temp:
    src = Path(temp) / 'worker_cas_mock.cpp'
    exe = Path(temp) / 'worker_cas_mock'
    src.write_text(stub + struct + cls + methods + tests)
    subprocess.run(['g++', '-std=c++14', '-Wall', '-Wextra', '-Werror', '-pedantic', '-I', str(ROOT / 'src'), str(src), '-o', str(exe)], check=True)
    out = subprocess.run([str(exe)], capture_output=True, text=True, check=True).stdout
assert 'CPP_CONTRACT_PASS' in out
sqls = dict(line.split('|',1) for line in out.splitlines() if '|' in line)
assert set(sqls) == {'CLAIM','RENEW','STAGE','RECOVER','RECLAIM','FAIL_RETRY','FAIL_FINAL'}
assert all('req\'' not in sql for sql in sqls.values())
assert all('g\'42' not in sql for sql in sqls.values())

def connection(state=0, attempts=0, retries=2, due=0, deadline=0, token=None):
    db = sqlite3.connect(':memory:')
    db.execute('''CREATE TABLE __all_ai_column_task (
      table_id INTEGER, column_id INTEGER, request_id BLOB,
      definition_version INTEGER, dependency_revision INTEGER, row_incarnation BLOB,
      task_status INTEGER, attempt_count INTEGER, max_retries INTEGER,
      next_due_us INTEGER, lease_owner BLOB, lease_token BLOB,
      lease_deadline_us INTEGER, staged_result BLOB, result_type INTEGER,
      error_code INTEGER, error_message BLOB,
      PRIMARY KEY(table_id,column_id,request_id))''')
    db.execute('''INSERT INTO __all_ai_column_task
      (table_id,column_id,request_id,definition_version,dependency_revision,row_incarnation,
      task_status,attempt_count,max_retries,next_due_us,lease_owner,lease_token,lease_deadline_us)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
      (1234,17,b"req'\\x",1,8,b"g'42",state,attempts,retries,due,
       b'worker1' if state==1 else None,token,deadline))
    return db

def run(db, label):
    before=db.total_changes
    db.execute(sqls[label])
    return db.total_changes-before

def row(db):
    return db.execute('SELECT task_status, attempt_count, lease_token, lease_deadline_us, staged_result FROM __all_ai_column_task').fetchone()

checks=0
# CAS claim is atomic and cannot claim again while RUNNING.
db=connection()
assert run(db,'CLAIM')==1 and row(db)[:2]==(1,1)
assert run(db,'CLAIM')==0
checks+=2
# Current lease can stage, then never stage twice.
assert run(db,'STAGE')==1 and row(db)[0]==3 and row(db)[4]==b"answer'"
assert run(db,'STAGE')==0
checks+=2
# Expired worker must not change already staged result.
assert run(db,'RECOVER')==0
checks+=1
# max_retries counts *additional* attempts: 0 still permits the first call.
db=connection(retries=0)
assert run(db,'CLAIM')==1 and row(db)[:2]==(1,1)
assert run(db,'FAIL_RETRY')==1 and row(db)[0]==4
assert run(db,'RECLAIM')==0
checks+=3
# Retry waiting not due, no claim; once due, can claim.
db=connection(state=2,due=150)
assert run(db,'CLAIM')==0
checks+=1
db=connection(state=2,due=50)
assert run(db,'CLAIM')==1
checks+=1
# Expired lease is recovered; old stage cannot publish; new claim is fenced.
db=connection(state=1,attempts=1,deadline=170,token=b'token1')
assert run(db,'RECOVER')==1 and row(db)[0]==2 and row(db)[2]==b'token1'
assert run(db,'STAGE')==0
assert run(db,'RECLAIM')==1 and row(db)[0]==1 and row(db)[2]==b'token2'
assert run(db,'STAGE')==0
checks+=4
# Two retries means three total attempts. The third failure is terminal.
db=connection(state=1,attempts=2,retries=2,deadline=170,token=b'token1')
assert run(db,'RECOVER')==1 and row(db)[0]==2
assert run(db,'RECLAIM')==1 and row(db)[:2]==(1,3)
checks+=2
db=connection(state=1,attempts=3,retries=2,deadline=170,token=b'token1')
assert run(db,'RECOVER')==1 and row(db)[0]==4
assert db.execute('SELECT next_due_us FROM __all_ai_column_task').fetchone()[0]==0
assert run(db,'RECLAIM')==0
checks+=3
# Failed model execution honors retry budget while retaining token fencing.
db=connection(state=1,attempts=1,deadline=200,token=b'token1')
assert run(db,'FAIL_RETRY')==1 and row(db)[0]==2
assert run(db,'FAIL_RETRY')==0
checks+=2
db=connection(state=1,attempts=2,retries=2,deadline=200,token=b'token1')
assert run(db,'FAIL_RETRY')==1 and row(db)[0]==2
checks+=1
db=connection(state=1,attempts=3,retries=2,deadline=200,token=b'token1')
assert run(db,'FAIL_RETRY')==1 and row(db)[0]==4
assert db.execute('SELECT next_due_us FROM __all_ai_column_task').fetchone()[0]==0
checks+=2
db=connection(state=1,attempts=1,deadline=200,token=b'token1')
assert run(db,'FAIL_FINAL')==1 and row(db)[0]==4
checks+=1
# Only the current worker can extend an active lease; no shrinking or
# same-deadline renewal, no extending after expiry, and no stale identity.
db=connection(state=1,attempts=1,deadline=200,token=b'token1')
assert run(db,'RENEW')==1 and row(db)[3]==300
assert run(db,'RENEW')==0 and row(db)[3]==300
checks+=2
db=connection(state=1,attempts=1,deadline=125,token=b'token1')
assert run(db,'RENEW')==0
checks+=1
db=connection(state=1,attempts=1,deadline=350,token=b'token1')
assert run(db,'RENEW')==0
checks+=1
db=connection(state=1,attempts=1,deadline=200,token=b'old-token')
assert run(db,'RENEW')==0
checks+=1
db=connection(state=1,attempts=1,deadline=200,token=b'token1')
db.execute('UPDATE __all_ai_column_task SET row_incarnation=?', (b"different",))
assert run(db,'RENEW')==0
checks+=1
db=connection(state=3,attempts=1,deadline=200,token=b'token1')
assert run(db,'RENEW')==0
checks+=1
# Revisions and row incarnations must match exactly.
db=connection()
db.execute('UPDATE __all_ai_column_task SET dependency_revision=9')
assert run(db,'CLAIM')==0
checks+=1
print('AI_COLUMN_WORKER_SQL_CAS_MOCK_PASS checks=%d (compiled C++ SQL, SQLite race/fence simulation)' % checks)
print('NATIVE_SEEKDB_BUILD=NOT_RUN WORKER_LOOP=NOT_IMPLEMENTED DML_OUTBOX=NOT_WIRED')
