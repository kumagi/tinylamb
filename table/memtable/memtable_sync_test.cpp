/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// T4191: Put/Delete with sync=true drain through MemTable::Sync and fire
// durability_fn_, but the Sync run outputs (blob / key-run / row-run /
// generation) are deliberately discarded at those two sites. These tests
// pin the half of the contract callers actually rely on: sync means the
// durability hook ran with a non-empty run before Put/Delete returned;
// and because the run is DISCARDED there, the drained entry is no longer
// visible through the table afterwards. A future sync-to-SortedRun design
// must plumb the outputs through the signature -- it may NOT assume they
// survive here.

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/test_util.hpp"
#include "common/version.hpp"
#include "table/memtable/memtable.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Capture {
  std::vector<char> blob;
  int calls = 0;
};
void CaptureDurability(const char* data, size_t len, void* ctx) {
  auto& capture = *static_cast<Capture*>(ctx);
  capture.blob.assign(data, data + len);
  ++capture.calls;
}

Schema MakeSyncSchema() {
  std::vector<Column> cols;
  cols.emplace_back(ColumnName{"", "id"}, ValueType::kInt64,
                    Constraint{Constraint::kPrimaryKey});
  cols.emplace_back(ColumnName{"", "v"}, ValueType::kInt64);
  return Schema{"syncprobe", std::move(cols), 0, 1};
}

std::string KeyOf(const Schema& sc, int64_t id) {
  return Row{Value(id), Value(int64_t{0})}.EncodeKeyMemcomparableFormat(sc);
}

class MemTableSyncTest : public ::testing::Test {
 protected:
  Capture capture_;
};

TEST_F(MemTableSyncTest, PutSyncFiresDurabilityHookAndDrainsEntryIntoRun) {
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  mt.SetDurabilityCallback(&CaptureDurability, &capture_);
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{42})},
                        /*sync=*/true));
  EXPECT_EQ(capture_.calls, 1);
  EXPECT_FALSE(capture_.blob.empty());
  // T4191's visible edge: the drain REMOVES the entry from the skiplists
  // and Put/Delete discard the run outputs, so post-sync Lookup sees the
  // entry only through the hook's copy of the blob -- never through the
  // table. Pin that (surprising but current) contract.
  Row out;
  EXPECT_EQ(mt.Lookup(KeyOf(sc, 7), &out), Status::kNotExists);
}

TEST_F(MemTableSyncTest, DeleteSyncFiresDurabilityHook) {
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{9}), Value(int64_t{1})}));
  mt.SetDurabilityCallback(&CaptureDurability, &capture_);
  ASSERT_SUCCESS(mt.Delete(KeyOf(sc, 9), /*sync=*/true));
  EXPECT_EQ(capture_.calls, 1);
  // Tombstone drained along with the run (Lookup can no longer observe it,
  // neither as row nor as kDeleted marker): persistence of the delete is
  // entirely the hook's job on this path.
  Row out;
  EXPECT_EQ(mt.Lookup(KeyOf(sc, 9), &out), Status::kNotExists);
}

TEST_F(MemTableSyncTest, SyncFailurePropagatesInsteadOfFalseSuccess) {
  // A durability hook that throws must surface as the Put error, not be
  // masked by the discarded run outputs (the historical shape of T4191).
  struct Thrower {
    static void Boom(const char* /*key*/, size_t /*size*/, void* /*context*/) {
      throw std::runtime_error("disk on fire");
    }
  };
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  mt.SetDurabilityCallback(&Thrower::Boom, nullptr);
  const Status st = mt.Put(Row{Value(int64_t{3}), Value(int64_t{3})},
                           /*sync=*/true);
  EXPECT_NE(st, Status::kSuccess);
}

TEST_F(MemTableSyncTest, FailedDurabilityHookKeepsDataForRetry) {
  // If the durability hook throws, the sealed entry must survive so the next
  // Sync can hand it to the hook again. Releasing the arenas first made the
  // failed run unrecoverable (the bytes existed only in the discarded blob).
  struct Thrower {
    static void Boom(const char*, size_t, void*) {
      throw std::runtime_error("disk on fire");
    }
  };
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{5}), Value(int64_t{55})}));

  mt.SetDurabilityCallback(&Thrower::Boom, nullptr);
  std::vector<char> blob;
  std::vector<uint32_t> key_run, row_run;
  size_t generation = 0;
  EXPECT_EQ(mt.Sync(&blob, &key_run, &row_run, &generation), Status::kIOError);

  mt.SetDurabilityCallback(&CaptureDurability, &capture_);
  blob.clear();
  key_run.clear();
  row_run.clear();
  ASSERT_SUCCESS(mt.Sync(&blob, &key_run, &row_run, &generation));
  EXPECT_EQ(capture_.calls, 1);
  EXPECT_FALSE(capture_.blob.empty());
  EXPECT_FALSE(blob.empty());
}

// Issue 2525: Snapshot(V) must resolve the version that was current at the
// pinned read version even when the key was rewritten several times within
// one pending generation.  Before the fix the second Put overwrote the first
// in place, so a reader pinned at the intermediate version reported the key
// missing.
TEST(MemTableVersionTest, SnapshotResolvesIntermediateVersions) {
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  mt.SetGlobalVersionMode(true);
  // Reserve the current counter so the two Puts land on v1 and v2 exactly.
  const uint64_t v0 = NextGlobalVersion();
  const uint64_t v1 = v0 + 1;
  const uint64_t v2 = v0 + 2;
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{100})}));
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{200})}));

  const auto at_v2 = mt.Snapshot(v2);
  ASSERT_EQ(at_v2.size(), 1U);
  EXPECT_FALSE(at_v2[0].second.is_deleted);
  EXPECT_EQ(at_v2[0].second.row[1], Value(int64_t{200}));

  // The regression itself: pinned at v1 the intermediate write must be
  // served, not dropped behind the newer invisible one.
  const auto at_v1 = mt.Snapshot(v1);
  ASSERT_EQ(at_v1.size(), 1U);
  EXPECT_FALSE(at_v1[0].second.is_deleted);
  EXPECT_EQ(at_v1[0].second.row[1], Value(int64_t{100}));

  // Pinned before both writes: nothing visible.
  EXPECT_TRUE(mt.Snapshot(v0).empty());

  // A tombstone is a version too: pinned before the delete the row must
  // still come back, at the delete version the tombstone collapses.
  const uint64_t v3 = v2 + 1;
  ASSERT_SUCCESS(mt.Delete(KeyOf(sc, 7)));
  const auto before_delete = mt.Snapshot(v2);
  ASSERT_EQ(before_delete.size(), 1U);
  EXPECT_FALSE(before_delete[0].second.is_deleted);
  EXPECT_EQ(before_delete[0].second.row[1], Value(int64_t{200}));
  const auto at_delete = mt.Snapshot(v3);
  ASSERT_EQ(at_delete.size(), 1U);
  EXPECT_TRUE(at_delete[0].second.is_deleted);

  // Sync collapses history to the newest state (the chain is pending-only).
  std::vector<char> blob;
  std::vector<uint32_t> key_run;
  std::vector<uint32_t> row_run;
  size_t generation = 0;
  ASSERT_SUCCESS(mt.Sync(&blob, &key_run, &row_run, &generation));
  EXPECT_TRUE(mt.Snapshot().empty());
}

TEST(MemTableVersionTest, UnversionedModeStillReplacesInPlace) {
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);  // global-version mode off: versions stay 0
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{100})}));
  ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{200})}));
  const auto snap = mt.Snapshot();
  ASSERT_EQ(snap.size(), 1U);
  EXPECT_EQ(snap[0].second.row[1], Value(int64_t{200}));
}

TEST(MemTableVersionTest, PendingHistoryIsCappedPerKey) {
  Schema sc = MakeSyncSchema();
  MemTable mt(sc);
  mt.SetGlobalVersionMode(true);
  const uint64_t v0 = NextGlobalVersion();
  constexpr int kPuts = 20;
  for (int i = 0; i < kPuts; ++i) {
    ASSERT_SUCCESS(mt.Put(Row{Value(int64_t{7}), Value(int64_t{i})}));
  }
  // The newest write plus the 16 retained older versions = v20 down to v4.
  const auto oldest_kept = mt.Snapshot(v0 + 4);
  ASSERT_EQ(oldest_kept.size(), 1U);
  EXPECT_EQ(oldest_kept[0].second.row[1], Value(int64_t{3}));
  // v3 fell off the cap: pinned there, the key is no longer answerable.
  EXPECT_TRUE(mt.Snapshot(v0 + 3).empty());
}

}  // namespace
}  // namespace tinylamb
