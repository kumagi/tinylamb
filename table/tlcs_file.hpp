/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_TABLE_TLCS_FILE_HPP
#define TINYLAMB_TABLE_TLCS_FILE_HPP

#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"
#include "common/status_or.hpp"
#include "table/lstable_bridge.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class RuntimeFilterGroup;
class Table;
class Transaction;
struct IntegerPeekCompare;

enum class TlcsColumnEncoding : uint8_t {
  kConstNull = 0,
  kNarrowDict = 1,
  kFor = 2,
  kRawDouble = 3,
  kVarChar = 4,
};

enum class TlcsDictKind : uint8_t {
  kNone = 0,
  kCompleteNarrow = 1,
  kPartialFrozen = 2,
};

// Persistent columnar file reader/directory (`<db>.tlcs.<first_pid>`).
// Keeps only block zone maps and small (<=254 entry) dictionaries in memory
// (~tens of MB even on SF=100 tables). When the expanded file fits inside
// `min(ColumnarPoolLimit() / 2, kDefaultResidentMmapLimitBytes)`, it maps the
// file and exposes a resident `LSTableStore` (`is_persistent_tlcs = true`).
// Larger tables run in paged mode using projected `platform::Pread` calls
// with a bounded LRU cache for point lookups.
class TlcsFile {
 public:
  static constexpr uint64_t kDefaultResidentMmapLimitBytes = uint64_t{1}
                                                             << 30;  // 1 GiB
  static constexpr uint32_t kFormatVersion = 2;
  static constexpr uint32_t kDirGroupSize = 128;

  struct DirGroupMeta {
    uint64_t group_offset{0};
    uint32_t block_count{0};
    uint32_t pid_crc32c{0};
    uint64_t body_size{0};
    uint32_t body_crc32c{0};
    page_id_t first_pid{0};
    page_id_t last_pid{0};
    page_id_t min_pid{0};
    page_id_t max_pid{0};
    bool pids_monotonic{false};
  };

  struct ColumnMeta {
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};
    TlcsColumnEncoding encoding{TlcsColumnEncoding::kConstNull};
    uint8_t for_width{0};
    bool has_null{false};
    bool has_min_max{false};
    bool has_double_min_max{false};
    bool has_nan{false};
    bool all_single_byte_varchar{false};
    bool is_const_code{false};
    bool has_code_min_max{false};
    int64_t for_base{0};
    int64_t min_int{std::numeric_limits<int64_t>::max()};
    int64_t max_int{std::numeric_limits<int64_t>::min()};
    double min_double{std::numeric_limits<double>::infinity()};
    double max_double{-std::numeric_limits<double>::infinity()};
    uint32_t const_code{UINT32_MAX};
    uint32_t min_code{UINT32_MAX};
    uint32_t max_code{0};
    std::array<uint64_t, 4> dict_mask{};
    uint64_t nulls_offset{0};
    uint64_t nulls_size{0};
    uint64_t data_offset{0};
    uint64_t data_size{0};
    uint64_t aux_offset{0};
    uint64_t aux_size{0};
  };

  struct BlockDirEntry {
    page_id_t pid{0};
    uint32_t row_count{0};
    bool has_any_null{false};
    std::vector<ColumnMeta> columns;
  };

  struct DecodedColumnChunk {
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};
    bool has_null{false};
    bool has_min_max{false};
    bool has_double_min_max{false};
    bool has_nan{false};
    uint32_t row_count{0};
    int64_t min_int{std::numeric_limits<int64_t>::max()};
    int64_t max_int{std::numeric_limits<int64_t>::min()};
    double min_double{std::numeric_limits<double>::infinity()};
    double max_double{-std::numeric_limits<double>::infinity()};
    const LSTableColumnDictionary* dict{nullptr};
    bool dict_encoded{false};
    std::vector<uint8_t> nulls;
    std::vector<int64_t> int_cells;
    std::vector<uint8_t> dict_codes;
    std::vector<uint8_t> varchar_bytes;
    std::vector<uint32_t> varchar_ends;

    [[nodiscard]] bool IsNull(size_t row) const noexcept {
      return has_null && row < nulls.size() && nulls[row] != 0;
    }

    [[nodiscard]] std::string_view VarcharAt(size_t row) const noexcept {
      if (dict_encoded && dict != nullptr) {
        if (row >= dict_codes.size()) {
          return {};
        }
        const uint8_t code = dict_codes[row];
        return code < dict->StringEntryCount() ? dict->StringEntryAt(code)
                                               : std::string_view{};
      }
      if (row >= varchar_ends.size()) {
        return {};
      }
      const uint32_t begin = (row == 0) ? 0U : varchar_ends[row - 1];
      const uint32_t end = varchar_ends[row];
      if (end < begin || end > varchar_bytes.size()) {
        return {};
      }
      return {reinterpret_cast<const char*>(varchar_bytes.data()) + begin,
              end - begin};
    }

    [[nodiscard]] size_t VarcharLength(size_t row) const noexcept {
      if (dict_encoded && dict != nullptr) {
        if (row >= dict_codes.size()) {
          return 0;
        }
        const uint8_t code = dict_codes[row];
        return code < dict->StringEntryCount()
                   ? dict->StringEntryAt(code).size()
                   : 0;
      }
      if (row >= varchar_ends.size()) {
        return 0;
      }
      const uint32_t begin = (row == 0) ? 0U : varchar_ends[row - 1];
      const uint32_t end = varchar_ends[row];
      return end >= begin ? static_cast<size_t>(end - begin) : 0;
    }
  };

  struct DecodedBlock {
    uint32_t block_index{0};
    page_id_t pid{0};
    uint32_t row_count{0};
    std::vector<slot_t> column_ids;
    std::vector<DecodedColumnChunk> columns;
  };

  TlcsFile() = default;
  TlcsFile(const TlcsFile&) = delete;
  TlcsFile& operator=(const TlcsFile&) = delete;
  TlcsFile(TlcsFile&&) = delete;
  TlcsFile& operator=(TlcsFile&&) = delete;
  ~TlcsFile();

  [[nodiscard]] static StatusOr<std::shared_ptr<TlcsFile>> Open(
      const std::string& path, const Schema& schema);
  [[nodiscard]] static StatusOr<std::shared_ptr<TlcsFile>> OpenWithStoredSchema(
      const std::string& path, Schema* schema_out);

  [[nodiscard]] const std::string& Path() const noexcept { return path_; }
  [[nodiscard]] uint64_t FileSize() const noexcept { return file_size_; }
  [[nodiscard]] uint64_t LayoutGeneration() const noexcept {
    return layout_generation_;
  }
  void BindLayoutGeneration(uint64_t generation) noexcept {
    layout_generation_ = generation;
    std::scoped_lock lk(resident_mu_);
    if (resident_store_ != nullptr) {
      resident_store_->layout_generation = generation;
    }
  }
  [[nodiscard]] page_id_t FirstPageId() const noexcept { return first_pid_; }
  [[nodiscard]] uint64_t TotalRows() const noexcept { return total_rows_; }
  [[nodiscard]] uint32_t BlockCount() const noexcept { return block_count_; }

  // #1111 test pins: column-decode executions are counted so tests can
  // assert the encoded-image cache serves repeat reads, and the reclaim
  // eviction loop is reachable without driving global memory pressure.
  static void ResetDecodeCallsForTest();
  [[nodiscard]] static uint64_t DecodeCallsForTest();
  [[nodiscard]] uint64_t CachedEncodedBlocksForTest() const;
  size_t EvictDecodedCacheForTest(size_t target_bytes) const noexcept {
    return EvictDecodedCacheForReclaim(target_bytes);
  }
  [[nodiscard]] slot_t ColumnCount() const noexcept { return column_count_; }
  [[nodiscard]] bool IsPaged() const noexcept {
    std::scoped_lock lk(resident_mu_);
    return resident_store_ == nullptr;
  }
  [[nodiscard]] bool IsPagedDirectory() const noexcept {
    std::scoped_lock lk(dir_cache_mu_);
    return block_count_ > 0 && eager_dir_groups_.empty();
  }
  [[nodiscard]] std::shared_ptr<const LSTableStore> ResidentStore()
      const noexcept {
    std::scoped_lock lk(resident_mu_);
    return resident_store_;
  }
  [[nodiscard]] std::string_view SerializedColumnStats() const noexcept {
    return serialized_column_stats_;
  }
  [[nodiscard]] StatusOr<uint32_t> BlockRowCount(uint32_t idx) const;
  [[nodiscard]] bool BlockAllRowsMatchPeek(
      uint32_t block_idx,
      const std::vector<IntegerPeekCompare>& peek_compares) const noexcept;
  [[nodiscard]] const LSTableColumnDictionary& Dictionary(
      slot_t col) const noexcept {
    return dicts_[col];
  }
  [[nodiscard]] TlcsDictKind DictionaryKind(slot_t col) const noexcept {
    return dict_kinds_[col];
  }

  [[nodiscard]] uint32_t FindBlockIndex(page_id_t pid) const noexcept;

  [[nodiscard]] bool BlockMatchesZoneMaps(
      uint32_t block_idx, const std::vector<IntegerPeekCompare>* peek_compares,
      const RuntimeFilterGroup* runtime_filter,
      std::optional<slot_t> runtime_filter_column) const noexcept;

  // Streaming decode of only `column_ids` for `block_idx` into caller-owned
  // reusable buffers (`out`, optional `io_scratch`). Safe to call concurrently
  // from multiple scan threads with distinct `out`/`io_scratch` instances.
  [[nodiscard]] Status DecodeBlockColumns(
      uint32_t block_idx, std::span<const slot_t> column_ids,
      const Schema& schema, DecodedBlock* out,
      bool materialize_dict_ints = true,
      std::vector<uint8_t>* io_scratch = nullptr) const;
  [[nodiscard]] Status DecodeBlockColumnsWithOptions(
      uint32_t block_idx, std::span<const slot_t> column_ids,
      const Schema& schema, DecodedBlock* out, bool materialize_dict_ints,
      std::vector<uint8_t>* io_scratch,
      std::span<const uint8_t> varchar_lengths_only) const;

  [[nodiscard]] uint32_t DirGroupCount() const noexcept {
    return static_cast<uint32_t>(dir_groups_.size());
  }
  [[nodiscard]] uint32_t DirGroupSize() const noexcept {
    return dir_group_size_;
  }
  [[nodiscard]] const std::vector<LSTableColumnDictionary>& Dicts()
      const noexcept {
    return dicts_;
  }
  [[nodiscard]] const std::vector<TlcsDictKind>& DictKinds() const noexcept {
    return dict_kinds_;
  }
  struct DirGroup {
    std::vector<page_id_t> pids;
    std::vector<BlockDirEntry> blocks;
  };
  struct VarcharBlockBound {
    bool has_null{false};
    bool is_const_null{false};
    bool has_min_max{false};
    bool all_canonical_utc_ts{false};
    uint8_t min_len{0};
    uint8_t max_len{0};
    bool min_truncated{false};
    bool max_truncated{false};
    std::array<char, 24> min_prefix{};
    std::array<char, 24> max_prefix{};
  };
  [[nodiscard]] Status CollectPageIds(std::vector<page_id_t>* out) const;
  [[nodiscard]] StatusOr<std::shared_ptr<const DirGroup>> GetDirGroup(
      uint32_t group_idx) const;
  [[nodiscard]] std::shared_ptr<const std::vector<VarcharBlockBound>>
  GetCachedVarcharBlockBounds(slot_t col_idx) const noexcept;
  [[nodiscard]] StatusOr<std::shared_ptr<const std::vector<VarcharBlockBound>>>
  GetOrComputeVarcharBlockBounds(slot_t col_idx, const Schema& schema) const;

  // Bounded-cache lookup helpers for stub RowPage point reads, stub full scans,
  // and lazy page hydration on UPDATE/DELETE.
  // Point-read a single row without unpacking the whole 32K-row block. Only
  // the bytes for the requested row are fetched from each column segment.
  [[nodiscard]] Status DecodeSingleRowByBlockIndex(uint32_t block_idx,
                                                   uint32_t row_idx,
                                                   const Schema& schema,
                                                   Row* out) const;
  [[nodiscard]] Status ReadRowByBlockIndex(uint32_t block_idx, uint32_t row_idx,
                                           const Schema& schema,
                                           Row* out) const;
  [[nodiscard]] Status ReadRowByPid(page_id_t pid, slot_t slot,
                                    const Schema& schema, Row* out) const;
  // #1111: resolves a stub row's encoded image from TlcsFile's cached
  // per-block images without copying the whole block's rows per call.
  // Returns kNotExists when the pid has no block or the slot is out of range.
  [[nodiscard]] StatusOr<std::shared_ptr<const std::vector<std::string>>>
  GetCachedEncodedBlockByPid(page_id_t pid, const Schema& schema) const;

  [[nodiscard]] Status ReadEncodedRowByPid(page_id_t pid, slot_t slot,
                                           const Schema& schema,
                                           std::string* out) const;
  [[nodiscard]] Status DecodeBlockEncodedRowsByPid(
      page_id_t pid, const Schema& schema, std::vector<std::string>* out) const;
  [[nodiscard]] Status ForEachBlockRow(
      const Schema& schema,
      const std::function<Status(page_id_t pid, slot_t slot, const Row& row)>&
          fn) const;
  // Single-block variant of ForEachBlockRow used by whole-block consumers
  // like tlcs compaction: fills each Row straight from the decoded block
  // instead of round-tripping through the encoded-image cache
  // (DecodeBlockEncodedRowsByPid + Row::TryDeserialize).
  [[nodiscard]] Status ForEachBlockRowByPid(
      page_id_t pid, const Schema& schema,
      const std::function<Status(slot_t slot, const Row& row)>& fn) const;

  // Attempts to build a partial projected resident LSTableStore from this file
  // when `effective_max_bytes` permits (or returns nullptr when too large).
  [[nodiscard]] std::shared_ptr<const LSTableStore> BuildProjectedResidentStore(
      const Schema& schema, const std::vector<slot_t>* projection,
      uint64_t effective_max_bytes) const;

 private:
  [[nodiscard]] static StatusOr<std::shared_ptr<TlcsFile>> OpenInternal(
      const std::string& path, const Schema* expected_schema,
      Schema* schema_out);

  struct CachedDirGroup {
    std::shared_ptr<const DirGroup> group;
    size_t bytes{0};
    std::list<uint32_t>::iterator lru_it;
  };

  struct CachedBlock {
    std::shared_ptr<const DecodedBlock> block;
    size_t bytes{0};
    std::list<uint32_t>::iterator lru_it;
  };

  // #1111: block rows in Row::Serialize wire form, built once per block from
  // the cached decoded block and reused by every later scan / stub resolve
  // instead of redoing decode -> Row -> Serialize per read.
  struct CachedEncodedBlock {
    std::shared_ptr<const std::vector<std::string>> rows;
    size_t bytes{0};
    std::list<uint32_t>::iterator lru_it;
  };

  [[nodiscard]] StatusOr<std::shared_ptr<const DirGroup>> LoadDirGroupFromFd(
      uint32_t group_idx) const;

  // Reads just a directory group's pid span (no ColumnMeta bodies) and
  // verifies its checksum. Used by the non-monotonic `FindBlockIndex` path so
  // a point lookup never parses a ~195 KB group just to find one pid.
  [[nodiscard]] Status LoadDirGroupPids(uint32_t group_idx,
                                        std::vector<page_id_t>* pids_out) const;

  // One-time, budget-guarded construction of the compact page -> block map
  // from pid spans only. Turns repeated non-monotonic lookups into O(1).
  bool TryBuildPidIndex() const noexcept;

  [[nodiscard]] StatusOr<std::shared_ptr<const DecodedBlock>>
  GetOrLoadCachedFullBlock(uint32_t block_idx, const Schema& schema) const;

  // #1111: encoded-image cache accessor; builds on a miss from
  // GetOrLoadCachedFullBlock so the column decode is also done at most once.
  [[nodiscard]] StatusOr<std::shared_ptr<const std::vector<std::string>>>
  GetOrLoadCachedEncodedRows(uint32_t block_idx, const Schema& schema) const;

  [[nodiscard]] std::shared_ptr<LSTableStore> BuildResidentStoreInternal(
      const Schema& schema, const std::vector<slot_t>* projection,
      uint64_t max_bytes,
      const std::shared_ptr<const platform::MemoryMap>& mmap) const;

  std::string path_;
  int fd_{-1};
  uint64_t file_size_{0};
  uint64_t layout_generation_{0};
  page_id_t first_pid_{0};
  uint64_t total_rows_{0};
  uint32_t block_count_{0};
  uint32_t dir_group_size_{kDirGroupSize};
  slot_t column_count_{0};
  bool groups_monotonic_pids_{false};
  std::vector<ValueType> col_types_;
  std::vector<uint8_t> col_unsigned_;
  std::vector<LSTableColumnDictionary> dicts_;
  std::vector<TlcsDictKind> dict_kinds_;
  std::vector<DirGroupMeta> dir_groups_;
  mutable std::vector<std::shared_ptr<const DirGroup>> eager_dir_groups_;
  mutable std::vector<uint32_t> flat_page_to_block_;
  mutable std::unordered_map<page_id_t, uint32_t> page_to_block_;
  mutable size_t eager_dir_reserved_bytes_{0};
  std::string serialized_column_stats_;
  uint64_t stats_file_offset_{0};
  uint64_t stats_file_size_{0};

  mutable std::mutex resident_mu_;
  mutable std::shared_ptr<const platform::MemoryMap> resident_mmap_;
  mutable std::shared_ptr<LSTableStore> resident_store_;
  size_t meta_reserved_bytes_{0};
  bool meta_emergency_{false};

  struct CacheReclaimBridge {
    std::mutex mu;
    TlcsFile* self{nullptr};
  };
  std::shared_ptr<CacheReclaimBridge> cache_reclaim_bridge_;
  ScopedReclaimHook cache_reclaim_hook_;
  size_t EvictDecodedCacheForReclaim(size_t target_bytes) const noexcept;

  mutable std::mutex dir_cache_mu_;
  mutable std::list<uint32_t> dir_cache_lru_;
  mutable std::unordered_map<uint32_t, CachedDirGroup> dir_cache_map_;
  mutable size_t dir_cache_bytes_{0};

  mutable std::mutex cache_mu_;
  mutable std::list<uint32_t> cache_lru_;
  mutable std::unordered_map<uint32_t, CachedBlock> cache_map_;
  mutable size_t cache_bytes_{0};
  // Encoded-image LRU (#1111); guarded by the same cache_mu_ so the reclaim
  // hook and destructor release both caches under one lock.
  mutable std::list<uint32_t> encoded_cache_lru_;
  mutable std::unordered_map<uint32_t, CachedEncodedBlock> encoded_cache_map_;
  mutable size_t encoded_cache_bytes_{0};

  mutable std::mutex varchar_bounds_mu_;
  mutable std::unordered_map<
      slot_t, std::shared_ptr<const std::vector<VarcharBlockBound>>>
      varchar_bounds_cache_;
  mutable size_t varchar_bounds_bytes_{0};
};

// Staged streaming `.tlcs` write state returned by `WriteTlcsTempFile` so the
// caller can swap `PageManager` to the columnar stub chain and obtain exact
// stub `page_id_t`s before finalizing header/directory and atomically renaming.
struct PendingTlcsCommit {
  int fd{-1};
  std::string temp_path;
  Schema schema;
  uint64_t layout_generation{0};
  page_id_t first_pid{0};
  uint64_t total_rows{0};
  uint32_t block_count{0};
  lsn_t max_chain_lsn{0};
  uint64_t directory_offset{0};
  std::vector<page_id_t> old_pids;
  std::vector<uint32_t> block_row_counts;
  std::vector<TlcsFile::DirGroupMeta> dir_groups;
  std::vector<LSTableColumnDictionary> dicts;
  std::vector<TlcsDictKind> dict_kinds;
  std::string serialized_column_stats;

  PendingTlcsCommit() = default;
  PendingTlcsCommit(const PendingTlcsCommit&) = delete;
  PendingTlcsCommit& operator=(const PendingTlcsCommit&) = delete;
  PendingTlcsCommit(PendingTlcsCommit&& other) noexcept;
  PendingTlcsCommit& operator=(PendingTlcsCommit&& other) noexcept;
  ~PendingTlcsCommit();

  void Abort() noexcept;

  // Publish-failure escape hatch: once the page chain points at columnar
  // stubs, the finalized temp file is the only row-image copy, so a rename
  // or reopen failure must leave it on disk for recovery instead of
  // deleting it through Abort(). Clearing temp_path keeps the file while
  // making the destructor's Abort() a no-op for it.
  void KeepTempFileOnDisk() noexcept { temp_path.clear(); }
};

[[nodiscard]] StatusOr<PendingTlcsCommit> WriteTlcsTempFile(
    Transaction& txn, const Table& table, const TlcsFile* existing_tlcs,
    const std::string& temp_path);

[[nodiscard]] Status FinalizeTlcsTempFile(
    PendingTlcsCommit& pending, const std::vector<page_id_t>& stub_pids,
    uint64_t final_layout_generation);

[[nodiscard]] Status RenameFinalizedTlcsTemp(PendingTlcsCommit& pending,
                                             const std::string& final_path);

[[nodiscard]] Status FinalizeTlcsFile(PendingTlcsCommit& pending,
                                      const std::vector<page_id_t>& stub_pids,
                                      uint64_t final_layout_generation,
                                      const std::string& final_path);

// Convenience helper for tests/callers: streams `table` into `final_path`
// without touching `PageManager` (preserving original `old_pids` as block
// pids).
[[nodiscard]] Status WriteTableToTlcsReadOnly(Transaction& txn,
                                              const Table& table,
                                              const TlcsFile* existing_tlcs,
                                              const std::string& final_path);

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_TLCS_FILE_HPP
