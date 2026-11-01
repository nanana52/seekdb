#include "src/share/ai_service/ob_ai_column_task_state.h"
#include <iostream>
#include <limits>
#include <vector>

using namespace oceanbase::share::ai_column;
static int checks = 0;
#define CHECK(x) do { ++checks; if (!(x)) { \
  std::cerr << "FAIL line " << __LINE__ << ": " << #x << std::endl; \
  return 1; } } while (0)

static Fence make_fence(uint64_t column_id)
{
  return Fence{1, 42, 1001, 5, 3, 22, 11, column_id};
}
static RowCheckpoint make_row()
{
  return RowCheckpoint{1, 42, 1001, 5, 3, 22, 11, true};
}
static Task make_task(uint64_t col, uint32_t max_attempts = 3)
{
  return Task{make_fence(col), TaskState::WAITING, 0, max_attempts, 0, 0, 0};
}

int main()
{
  Task a = make_task(20), b = make_task(21);
  RowCheckpoint row = make_row();
  CHECK(a.fence.valid());
  CHECK(matches_row(a.fence, row));
  CHECK(claimable(a, 100));
  CHECK(!claim(a, 100, 0, 1));
  CHECK(!claim(a, 100, 100, 0));
  CHECK(claim(a, 100, 50, 10001));
  CHECK(a.attempts == 1);
  CHECK(!claim(a, 100, 50, 10002));
  CHECK(!stage_success(a, row, 10002, 110));
  CHECK(!stage_success(a, row, 10001, 150));
  CHECK(!recover_expired(a, 149, 10));
  CHECK(recover_expired(a, 150, 10));
  CHECK(a.state == TaskState::RETRY_WAIT);
  CHECK(!claimable(a, 159));
  CHECK(!claim(a, 160, 25, 10001)); // token reuse forbidden
  CHECK(claim(a, 160, 25, 10003));
  CHECK(!stage_success(a, row, 10001, 170));
  CHECK(stage_success(a, row, 10003, 170));
  CHECK(a.state == TaskState::SUCCESS_STAGED);
  CHECK(visible_status(a.state) == ColumnStatus::PENDING);
  CHECK(claim(b, 100, 80, 20001));
  CHECK(stage_failure(b, row, 20001, 120, true, 10));
  CHECK(b.state == TaskState::RETRY_WAIT);
  std::vector<Task> group{a, b};
  CHECK(!group_publishable(group, row, 2));
  CHECK(claim(b, 130, 80, 20002));
  CHECK(stage_failure(b, row, 20002, 135, false, 0));
  group[1] = b;
  CHECK(group_publishable(group, row, 2));
  CHECK(mark_published(group, row, 2));
  CHECK(visible_status(group[0].state) == ColumnStatus::READY);
  CHECK(visible_status(group[1].state) == ColumnStatus::FAILED);
  CHECK(manual_retry_allowed(group[0].state));
  CHECK(manual_retry_allowed(group[1].state));
  CHECK(!manual_retry_allowed(TaskState::WAITING));
  CHECK(!group_publishable(group, row, 2));
  CHECK(!strict_read_rejects(false, 10, 0));
  CHECK(!strict_read_rejects(true, 10, 10));
  CHECK(strict_read_rejects(true, 10, 9));
  CHECK(strict_read_rejects(true, 10, 11));
  CHECK(!strict_read_rejects(true, 0, 0));

  // Old definition, old request, old dependency input, deletion and reinsert
  // each invalidate an old task, even if a slow HTTP response arrives later.
  Task old = make_task(25);
  CHECK(claim(old, 100, 100, 500));
  RowCheckpoint updated = row;
  updated.dependency_version = 6;
  CHECK(!stage_success(old, updated, 500, 101));
  updated = row; updated.definition_version = 4;
  CHECK(!stage_success(old, updated, 500, 101));
  updated = row; updated.request_id = 23;
  CHECK(!stage_success(old, updated, 500, 101));
  updated = row; updated.row_generation = 1002;
  CHECK(!stage_success(old, updated, 500, 101));
  updated = row; updated.exists = false;
  CHECK(!stage_success(old, updated, 500, 101));
  CHECK(stage_success(old, row, 500, 102));
  CHECK(!group_publishable(std::vector<Task>{old}, updated, 1));

  // A timed-out final attempt becomes a staged failure after restart.
  Task exhausted = make_task(30, 1);
  CHECK(claim(exhausted, 50, 20, 100));
  CHECK(recover_expired(exhausted, 70, 10));
  CHECK(exhausted.state == TaskState::FAILURE_STAGED);
  CHECK(!claimable(exhausted, 80));

  // A lost response after the last permitted attempt terminates the group.
  Task flaky = make_task(31, 2);
  CHECK(claim(flaky, 0, 10, 100));
  CHECK(stage_failure(flaky, row, 100, 5, true, 7));
  CHECK(!claimable(flaky, 11));
  CHECK(claim(flaky, 12, 10, 101));
  CHECK(stage_failure(flaky, row, 101, 15, true, 10));
  CHECK(flaky.state == TaskState::FAILURE_STAGED);
  CHECK(!claimable(flaky, 25));
  CHECK(visible_status(flaky.state) == ColumnStatus::PENDING);

  // Groups are required to contain unique columns from the same change.
  Task x = make_task(40), y = make_task(40);
  CHECK(claim(x, 0, 20, 1));
  CHECK(claim(y, 0, 20, 2));
  CHECK(stage_success(x, row, 1, 5));
  CHECK(stage_success(y, row, 2, 5));
  CHECK(!group_publishable(std::vector<Task>{x, y}, row, 2));
  y.fence.column_id = 41;
  CHECK(group_publishable(std::vector<Task>{x, y}, row, 2));
  y.fence.group_id = 12;
  CHECK(!group_publishable(std::vector<Task>{x, y}, row, 2));
  y.fence.group_id = 11;
  y.fence.request_id = 23;
  CHECK(!group_publishable(std::vector<Task>{x, y}, row, 2));
  CHECK(!group_publishable(std::vector<Task>{x}, row, 2));

  // Saturate timing arithmetic instead of wrapping into the past.
  const uint64_t top = std::numeric_limits<uint64_t>::max();
  CHECK(saturating_add(top - 2, 100) == top);
  CHECK(saturating_add(100, 20) == 120);
  Task late = make_task(50);
  CHECK(claim(late, top - 10, 100, 500));
  CHECK(late.lease_deadline_ms == top);
  CHECK(has_active_lease(late, 500, top - 1));
  CHECK(!has_active_lease(late, 500, top));

  // All required fence tokens are nonzero and belong to a single tenant.
  Task invalid = make_task(60);
  invalid.fence.request_id = 0;
  CHECK(!claimable(invalid, 100));
  CHECK(!claim(invalid, 100, 100, 1));
  RowCheckpoint other_tenant = row;
  other_tenant.tenant_id = 2;
  CHECK(!matches_row(make_fence(20), other_tenant));

  std::cout << "AI_COLUMN_STATE_CORE_PASS assertions=" << checks
            << " checks=leases,retry,stale_results,groups,strict_read_gating"
            << std::endl;
  return 0;
}
