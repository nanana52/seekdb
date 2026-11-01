/*
 * Copyright (c) 2026 OceanBase competition contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>
#include <cstddef>
#include <limits>
#include <set>
#include <vector>

namespace oceanbase {
namespace share {
namespace ai_column {

// Pure, deterministic transition rules for a future transactionally persisted
// outbox. This header does NOT create an outbox, DDL, SQL hook or worker.
enum class TaskState : uint8_t {
  WAITING = 0,
  RUNNING = 1,
  RETRY_WAIT = 2,
  SUCCESS_STAGED = 3,
  FAILURE_STAGED = 4,
  READY_PUBLISHED = 5,
  FAILED_PUBLISHED = 6,
  CANCELLED = 7
};

enum class ColumnStatus : uint8_t { PENDING = 0, READY = 1, FAILED = 2 };

// row_generation must distinguish delete/reinsert of the same primary key.
// dependency_version increments on relevant input or primary-key change, not
// on unrelated updates. definition_version increments with AI DDL changes.
// request_id and group_id are persisted tokens generated in the business tx.
struct Fence {
  uint64_t tenant_id;
  uint64_t table_id;
  uint64_t row_generation;
  uint64_t dependency_version;
  uint64_t definition_version;
  uint64_t request_id;
  uint64_t group_id;
  uint64_t column_id;

  bool valid() const
  {
    return tenant_id && table_id && row_generation && dependency_version &&
           definition_version && request_id && group_id && column_id;
  }
};

struct RowCheckpoint {
  uint64_t tenant_id;
  uint64_t table_id;
  uint64_t row_generation;
  uint64_t dependency_version;
  uint64_t definition_version;
  uint64_t request_id;
  uint64_t group_id;
  bool exists;
};

inline bool matches_row(const Fence &task, const RowCheckpoint &row)
{
  return task.valid() && row.exists &&
         task.tenant_id == row.tenant_id &&
         task.table_id == row.table_id &&
         task.row_generation == row.row_generation &&
         task.dependency_version == row.dependency_version &&
         task.definition_version == row.definition_version &&
         task.request_id == row.request_id &&
         task.group_id == row.group_id;
}

inline bool matches_group(const Fence &lhs, const Fence &rhs)
{
  return lhs.valid() && rhs.valid() &&
         lhs.tenant_id == rhs.tenant_id && lhs.table_id == rhs.table_id &&
         lhs.row_generation == rhs.row_generation &&
         lhs.dependency_version == rhs.dependency_version &&
         lhs.definition_version == rhs.definition_version &&
         lhs.request_id == rhs.request_id && lhs.group_id == rhs.group_id;
}

inline uint64_t saturating_add(uint64_t x, uint64_t y)
{
  const uint64_t limit = std::numeric_limits<uint64_t>::max();
  return y > limit - x ? limit : x + y;
}

struct Task {
  Fence fence;
  TaskState state;
  uint32_t attempts;
  uint32_t max_attempts;
  uint64_t lease_token;
  uint64_t lease_deadline_ms;
  uint64_t next_attempt_ms;
};

inline bool claimable(const Task &task, uint64_t now_ms)
{
  if (!task.fence.valid() || task.max_attempts == 0 ||
      task.attempts >= task.max_attempts) {
    return false;
  }
  return task.state == TaskState::WAITING ||
         (task.state == TaskState::RETRY_WAIT &&
          now_ms >= task.next_attempt_ms);
}

// Call this under a row lock or conditional UPDATE in the same database;
// otherwise competing workers can both observe claimable() == true.
inline bool claim(Task &task, uint64_t now_ms, uint64_t lease_duration_ms,
                  uint64_t new_lease_token)
{
  if (!claimable(task, now_ms) || lease_duration_ms == 0 ||
      new_lease_token == 0 || new_lease_token == task.lease_token) {
    return false;
  }
  ++task.attempts;
  task.state = TaskState::RUNNING;
  task.lease_token = new_lease_token;
  task.lease_deadline_ms = saturating_add(now_ms, lease_duration_ms);
  task.next_attempt_ms = 0;
  return true;
}

inline bool has_active_lease(const Task &task, uint64_t lease_token,
                             uint64_t now_ms)
{
  return task.state == TaskState::RUNNING && lease_token != 0 &&
         lease_token == task.lease_token && now_ms < task.lease_deadline_ms;
}

// Completing AI HTTP is NOT publication. Group publication must validate
// the row fence again and atomically publish all staged columns in SQL.
inline bool stage_success(Task &task, const RowCheckpoint &current_row,
                          uint64_t lease_token, uint64_t now_ms)
{
  if (!has_active_lease(task, lease_token, now_ms) ||
      !matches_row(task.fence, current_row)) {
    return false;
  }
  task.state = TaskState::SUCCESS_STAGED;
  // Preserve the last lease token to reject immediate token reuse.
  return true;
}

inline bool stage_failure(Task &task, const RowCheckpoint &current_row,
                          uint64_t lease_token, uint64_t now_ms,
                          bool retryable, uint64_t backoff_ms)
{
  if (!has_active_lease(task, lease_token, now_ms) ||
      !matches_row(task.fence, current_row)) {
    return false;
  }
  // Preserve the last lease token to reject immediate token reuse.
  if (retryable && task.attempts < task.max_attempts) {
    task.state = TaskState::RETRY_WAIT;
    task.next_attempt_ms = saturating_add(now_ms, backoff_ms);
  } else {
    task.state = TaskState::FAILURE_STAGED;
  }
  return true;
}

// Expired RUNNING tasks can be reclaimed after process restart. An expired
// attempt at the retry cap becomes terminal so a group cannot hang forever.
inline bool recover_expired(Task &task, uint64_t now_ms, uint64_t backoff_ms)
{
  if (task.state != TaskState::RUNNING ||
      now_ms < task.lease_deadline_ms) {
    return false;
  }
  // Preserve the last lease token to reject immediate token reuse.
  if (task.attempts < task.max_attempts) {
    task.state = TaskState::RETRY_WAIT;
    task.next_attempt_ms = saturating_add(now_ms, backoff_ms);
  } else {
    task.state = TaskState::FAILURE_STAGED;
  }
  return true;
}

inline bool is_staged_terminal(TaskState s)
{
  return s == TaskState::SUCCESS_STAGED || s == TaskState::FAILURE_STAGED;
}

// All members from a single dependency change must publish together. A
// failure is terminal only after its configured retry budget is exhausted.
inline bool group_publishable(const std::vector<Task> &members,
                              const RowCheckpoint &row, std::size_t expected_count)
{
  if (expected_count == 0 || members.size() != expected_count ||
      members.empty()) {
    return false;
  }
  const Fence &first = members[0].fence;
  if (!matches_row(first, row)) {
    return false;
  }
  std::set<uint64_t> columns;
  for (const Task &member : members) {
    if (!matches_group(first, member.fence) ||
        !matches_row(member.fence, row) ||
        !is_staged_terminal(member.state) ||
        !columns.insert(member.fence.column_id).second) {
      return false;
    }
  }
  return true;
}

// Publication must take place in one real database transaction with a
// conditional fence check; this helper only enforces in-memory preconditions.
inline bool mark_published(std::vector<Task> &members,
                           const RowCheckpoint &row, std::size_t expected_count)
{
  if (!group_publishable(members, row, expected_count)) {
    return false;
  }
  for (Task &member : members) {
    member.state = member.state == TaskState::SUCCESS_STAGED
                       ? TaskState::READY_PUBLISHED
                       : TaskState::FAILED_PUBLISHED;
  }
  return true;
}

inline ColumnStatus visible_status(TaskState state)
{
  if (state == TaskState::READY_PUBLISHED) {
    return ColumnStatus::READY;
  } else if (state == TaskState::FAILED_PUBLISHED) {
    return ColumnStatus::FAILED;
  } else {
    return ColumnStatus::PENDING;
  }
}

// This is a column-level readiness gate, deliberately independent of WHERE.
// The caller must count all live rows for each referenced AI column.
inline bool strict_read_rejects(bool referenced_ai_column,
                                 uint64_t live_row_count,
                                 uint64_t ready_row_count)
{
  return referenced_ai_column && ready_row_count != live_row_count;
}

// A manual retry creates a new group/request version in the same business
// transaction. Reusing an old task or request_id is never permitted.
inline bool manual_retry_allowed(TaskState published_state)
{
  return published_state == TaskState::READY_PUBLISHED ||
         published_state == TaskState::FAILED_PUBLISHED;
}

}  // namespace ai_column
}  // namespace share
}  // namespace oceanbase
