/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "executor/detail/ooc/chunk_codec.hpp"
#include "executor/detail/ooc/recursive_partitioner.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ops.hpp"
#include "type/collation.hpp"
#include "type/row_block_codec.hpp"

namespace tinylamb {
namespace {

constexpr size_t kGraceWriteBatchRows = 256;
constexpr size_t kGraceWriteBatchBytes = 16ULL << 10;
constexpr uint32_t kMaxGraceDepth = 8;
constexpr char kJoinNegativeIntTag = '\x7f';

uint64_t MixHash64(uint64_t h) noexcept {
  h *= 0x9E3779B97F4A7C15ULL;
  h ^= h >> 32;
  h *= 0xBF58476D1CE4E5B9ULL;
  h ^= h >> 29;
  return h;
}

uint64_t HashInt64Key(int64_t key) noexcept {
  return MixHash64(static_cast<uint64_t>(key));
}

uint64_t HashBytesKey(std::string_view key) noexcept {
  uint64_t h = 14695981039346656037ULL;
  for (const char c : key) {
    h ^= static_cast<unsigned char>(c);
    h *= 1099511628211ULL;
  }
  return MixHash64(h);
}

bool ContainsUnmatchableNaN(const Value& value) {
  if (value.type == ValueType::kDouble) {
    return std::isnan(value.value.double_value);
  }
  if (value.type != ValueType::kArray) {
    return false;
  }
  return std::ranges::any_of(value.ArrayElements(), ContainsUnmatchableNaN);
}

uint64_t BSwap64(uint64_t v) { return __builtin_bswap64(v); }

void AppendJoinComparableValue(const Value& v, std::string* out) {
  switch (v.type) {
    case ValueType::kInt64: {
      const bool negative = !v.IsUnsigned() && v.value.int_value < 0;
      out->push_back(negative ? kJoinNegativeIntTag
                              : static_cast<char>(ValueType::kInt64));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kDate: {
      out->push_back(static_cast<char>(v.type));
      const uint64_t be = BSwap64(static_cast<uint64_t>(v.value.int_value));
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      buf[0] ^= static_cast<char>(0x80);
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kVarChar: {
      std::string folded;
      std::string_view s(v.value.varchar_value.data(),
                         v.value.varchar_value.size());
      if (v.IsCaseInsensitive()) {
        folded = FoldCaseForCollation(s);
        s = folded;
      }
      out->push_back(static_cast<char>(ValueType::kVarChar));
      if (s.empty()) {
        out->append(9, '\0');
        break;
      }
      for (size_t i = 0;; i += 8) {
        if (9 <= s.size() - i) {
          out->append(s.substr(i, 8));
          out->push_back('\x09');
        } else {
          const size_t tail = s.size() - i;
          out->append(s.substr(i, tail));
          out->append(8 - tail, '\0');
          out->push_back(
              static_cast<char>((s.size() % 8) + (s.size() % 8 == 0 ? 8 : 0)));
          break;
        }
      }
      break;
    }
    case ValueType::kDouble: {
      out->push_back(static_cast<char>(ValueType::kDouble));
      double d = v.value.double_value;
      if (std::isnan(d)) {
        d = std::numeric_limits<double>::quiet_NaN();
      } else if (d == 0.0) {
        d = 0.0;
      }
      uint64_t bits = 0;
      std::memcpy(&bits, &d, sizeof(bits));
      uint64_t be = BSwap64(bits);
      if (0 <= d) {
        be |= 0x80;
      } else {
        be = ~be;
      }
      std::array<char, 8> buf{};
      std::memcpy(buf.data(), &be, buf.size());
      out->append(buf.data(), buf.size());
      break;
    }
    case ValueType::kArray:
      out->append(v.EncodeMemcomparableFormat());
      break;
    case ValueType::kNull:
      break;
  }
}

struct ExtractedJoinKey {
  bool valid{false};
  bool has_non_safe_null{false};
  uint64_t hash{0};
  int64_t int_key{0};
};

ExtractedJoinKey ExtractJoinKey(const Row& row, const std::vector<slot_t>& cols,
                                JoinHashIndex::KeyMode mode, ValueType int_type,
                                std::string* byte_out,
                                const std::vector<bool>* null_safe,
                                bool any_null_safe) {
  ExtractedJoinKey res;
  if (mode == JoinHashIndex::KeyMode::kInt64 && !any_null_safe &&
      cols.size() == 1) {
    const slot_t col = cols[0];
    if (col >= row.Count() || row[col].IsNull()) {
      res.has_non_safe_null = true;
      return res;
    }
    const Value& v = row[col];
    if (v.type == int_type) {
      if (v.IsUnsigned() && v.value.int_value < 0) {
        // UINT64 >= 2^63 shares its raw bits with a negative INT64. In int
        // mode the build side never carries such values (mode selection
        // rejects them), so this probe key cannot match any of them.
        return res;
      }
      res.valid = true;
      res.int_key = v.value.int_value;
      res.hash = HashInt64Key(v.value.int_value);
      return res;
    }
    // Cross-type fallback via byte encoding if mixed/unsigned mismatch.
  }

  byte_out->clear();
  bool saw_non_safe_null = false;
  for (size_t i = 0; i < cols.size(); ++i) {
    const slot_t c = cols[i];
    const Value& v = (c < row.Count()) ? row[c] : Value();
    const bool safe =
        null_safe != nullptr && i < null_safe->size() && (*null_safe)[i];
    if (v.IsNull()) {
      if (!safe) {
        saw_non_safe_null = true;
        continue;
      }
      byte_out->push_back('\0');
      continue;
    }
    if (!safe && ContainsUnmatchableNaN(v)) {
      res.has_non_safe_null = false;
      return res;
    }
    if (saw_non_safe_null) {
      continue;
    }
    if (any_null_safe) {
      byte_out->push_back('\1');
    }
    if (safe && v.type == ValueType::kDouble &&
        std::isnan(v.value.double_value)) {
      byte_out->append(Value(std::numeric_limits<double>::quiet_NaN())
                           .EncodeMemcomparableFormat());
    } else {
      AppendJoinComparableValue(v, byte_out);
    }
  }
  if (saw_non_safe_null) {
    res.has_non_safe_null = true;
    return res;
  }
  res.valid = true;
  res.hash = HashBytesKey(*byte_out);
  return res;
}

uint64_t PartitionHashForRow(const Row& row, const std::vector<slot_t>& cols,
                             const std::vector<bool>* null_safe,
                             bool any_null_safe, std::string* scratch,
                             bool* has_non_safe_null = nullptr) {
  const ExtractedJoinKey k =
      ExtractJoinKey(row, cols, JoinHashIndex::KeyMode::kBytes,
                     ValueType::kNull, scratch, null_safe, any_null_safe);
  if (has_non_safe_null != nullptr && k.has_non_safe_null) {
    *has_non_safe_null = true;
  }
  return k.valid ? k.hash : 0ULL;
}

Row CombineRows(const Row& left, const Row& right) { return left + right; }

Row MakeNullRow(size_t width) { return Row(std::vector<Value>(width)); }

StatusOr<bool> EvalResidual(const OocHashJoinSpec& spec, const Row& left,
                            const Row& right) {
  if (!spec.residual_predicate) {
    return true;
  }
  Row combined = CombineRows(left, right);
  const Schema& schema = spec.residual_schema.ColumnCount() == combined.Count()
                             ? spec.residual_schema
                             : Schema();
  ASSIGN_OR_RETURN(Value, v,
                   spec.residual_predicate->TryEvaluate(combined, schema));
  return relational_detail::Truthy(v);
}

constexpr size_t kSmallBuildThreshold = 64;
constexpr size_t kNestedLoopProbeLimit = 4;
constexpr size_t kAdaptiveParallelProbeLimit = size_t{1} << 15;

bool IsSemiJoinKind(JoinKind kind) noexcept { return kind == JoinKind::kSemi; }
bool IsAntiJoinKind(JoinKind kind) noexcept { return kind == JoinKind::kAnti; }
bool IsNullAwareAntiJoinKind(JoinKind kind) noexcept {
  return kind == JoinKind::kNullAwareAnti;
}

void EmitJoinTuple(const OocHashJoinSpec& spec, const Row& left,
                   const RowPosition& left_pos, const Row& right,
                   DataChunk* out) {
  if (IsSemiJoinKind(spec.kind) || IsAntiJoinKind(spec.kind) ||
      IsNullAwareAntiJoinKind(spec.kind)) {
    out->Append(left, left_pos);
    return;
  }
  out->Append(CombineRows(left, right), left_pos);
}

bool ShouldKeepProbeOnlyUnmatched(JoinKind kind, bool build_left) noexcept {
  if (build_left) {
    return kind == JoinKind::kRightOuter || kind == JoinKind::kFullOuter;
  }
  return kind == JoinKind::kLeftOuter || kind == JoinKind::kFullOuter ||
         kind == JoinKind::kAnti || kind == JoinKind::kNullAwareAnti;
}

bool ShouldKeepBuildOnlyUnmatched(JoinKind kind, bool build_left) noexcept {
  if (build_left) {
    return kind == JoinKind::kLeftOuter || kind == JoinKind::kFullOuter;
  }
  return kind == JoinKind::kRightOuter || kind == JoinKind::kFullOuter;
}

void CaptureSourceDumpSnapshot(const std::weak_ptr<PipelineSource>& weak_src,
                               std::string* snapshot_out) {
  if (snapshot_out == nullptr) {
    return;
  }
  if (const std::shared_ptr<PipelineSource> src = weak_src.lock()) {
    std::ostringstream oss;
    src->DumpExplain(oss, 0);
    *snapshot_out = oss.str();
  }
}

void RenderStreamTree(std::ostream& out, int indent, bool is_dump,
                      const std::vector<std::shared_ptr<PipelineOperator>>& ops,
                      const std::weak_ptr<PipelineSource>& weak_src,
                      const std::string& snapshot,
                      const std::shared_ptr<PipelineSource>& direct_src) {
  int cur = indent;
  for (size_t i = ops.size(); i > 0; --i) {
    if (is_dump) {
      ops[i - 1]->DumpExplain(out, cur);
    } else {
      ops[i - 1]->Explain(out, cur);
    }
    cur += 2;
  }
  std::string text;
  const std::shared_ptr<PipelineSource> src =
      direct_src != nullptr ? direct_src : weak_src.lock();
  if (src != nullptr) {
    std::ostringstream oss;
    if (is_dump) {
      src->DumpExplain(oss, 0);
    } else {
      src->Explain(oss, 0);
    }
    text = oss.str();
  } else {
    text = snapshot;
  }
  if (!text.empty()) {
    const std::string prefix(static_cast<size_t>(std::max(0, cur)), ' ');
    std::istringstream iss(text);
    std::string line;
    while (std::getline(iss, line)) {
      out << prefix << line << "\n";
    }
  }
}

void MarkBit(std::vector<uint8_t>* bits, uint64_t idx) {
  if (bits == nullptr || bits->empty()) {
    return;
  }
  (*bits)[idx >> 3] |= static_cast<uint8_t>(1U << (idx & 7U));
}

[[nodiscard]] bool TestBit(const std::vector<uint8_t>* bits, uint64_t idx) {
  if (bits == nullptr || bits->empty()) {
    return false;
  }
  return ((*bits)[idx >> 3] & static_cast<uint8_t>(1U << (idx & 7U))) != 0;
}

Status StepActiveProbeRow(
    const OocHashJoinSpec& spec, const JoinHashIndex& index,
    const std::vector<Row>& build_rows,
    const std::vector<RowPosition>& build_positions,
    std::vector<uint8_t>* build_matched, bool chunked_skew_mode,
    uint64_t build_total_rows, std::vector<uint8_t>* probe_matched_bits,
    OocAdaptiveHashProbeSource::ActiveProbeRowState* state, DataChunk* out,
    size_t max_rows) {
  while (state->next_entry != JoinHashIndex::kNil && out->Size() < max_rows) {
    const size_t bi = index.RowIndex(state->next_entry);
    state->next_entry = index.ChainNext(state->next_entry);
    const Row& brow = build_rows[bi];
    const Row& left_r = spec.build_left ? brow : state->row;
    const Row& right_r = spec.build_left ? state->row : brow;
    ASSIGN_OR_RETURN(bool, pass, EvalResidual(spec, left_r, right_r));
    if (!pass) {
      continue;
    }
    state->matched_any = true;
    if (build_matched != nullptr && !build_matched->empty()) {
      (*build_matched)[bi] = 1;
    }
    if (chunked_skew_mode) {
      if (spec.kind == JoinKind::kSemi) {
        if (!TestBit(probe_matched_bits, state->gidx)) {
          MarkBit(probe_matched_bits, state->gidx);
          EmitJoinTuple(spec, state->row, state->pos, brow, out);
        }
        state->next_entry = JoinHashIndex::kNil;
        break;
      }
      MarkBit(probe_matched_bits, state->gidx);
    }
    if (IsAntiJoinKind(spec.kind) || IsNullAwareAntiJoinKind(spec.kind)) {
      state->next_entry = JoinHashIndex::kNil;
      break;
    }
    if (spec.kind == JoinKind::kSemi) {
      EmitJoinTuple(spec, state->row, state->pos, brow, out);
      state->next_entry = JoinHashIndex::kNil;
      break;
    }
    out->Append(CombineRows(left_r, right_r),
                spec.build_left ? build_positions[bi] : state->pos);
  }

  if (state->next_entry != JoinHashIndex::kNil) {
    return Status::kSuccess;
  }

  if (!chunked_skew_mode && !state->matched_any &&
      ShouldKeepProbeOnlyUnmatched(spec.kind, spec.build_left)) {
    if (spec.kind != JoinKind::kNullAwareAnti || !state->has_non_safe_null ||
        build_total_rows == 0) {
      if (out->Size() >= max_rows) {
        return Status::kSuccess;
      }
      const size_t pad_w = spec.build_left ? spec.left_schema.ColumnCount()
                                           : spec.right_schema.ColumnCount();
      if (spec.build_left) {
        EmitJoinTuple(spec, MakeNullRow(pad_w), RowPosition(), state->row, out);
      } else {
        EmitJoinTuple(spec, state->row, state->pos, MakeNullRow(pad_w), out);
      }
    }
  }

  state->Reset();
  return Status::kSuccess;
}

size_t EstimateJoinHashIndexBytes(uint64_t row_count) noexcept {
  if (row_count == 0) {
    return 0;
  }
  size_t slots = 16;
  const uint64_t min_slots = row_count + (row_count / 2) + 8;
  while (slots < min_slots && slots < (size_t{1} << 60)) {
    slots <<= 1;
  }
  return (slots * sizeof(size_t)) + (static_cast<size_t>(row_count) * 32);
}

size_t GraceWriterBufferBytesFor(size_t budget, size_t fanout,
                                 size_t fallback) noexcept {
  if (budget == 0) {
    return fallback;
  }
  return std::clamp<size_t>(budget / std::max<size_t>(2, fanout * 2), 4096,
                            fallback);
}

size_t GraceWriteBatchBytesFor(size_t budget, size_t fanout) noexcept {
  if (budget == 0) {
    return kGraceWriteBatchBytes;
  }
  return std::clamp<size_t>(budget / std::max<size_t>(4, fanout * 4), 1024,
                            kGraceWriteBatchBytes);
}

size_t GraceTotalStagingCapFor(size_t budget) noexcept {
  if (budget == 0) {
    return kGraceWriteBatchBytes * 64;
  }
  return std::max<size_t>(4096, budget / 4);
}

Status FlushGraceBucket(
    size_t bucket, std::vector<std::unique_ptr<SpillSegmentWriter>>& writers,
    std::vector<DataChunk>& chunks, std::vector<size_t>& batch_bytes,
    size_t* total_batch_bytes, QueryMemoryGrant::LocalBatchCache* grant_cache,
    std::string* codec_scratch) {
  if (chunks[bucket].Empty()) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(WriteDataChunk(writers[bucket].get(), chunks[bucket],
                                codec_scratch, kGraceWriteBatchBytes));
  chunks[bucket].Reset();
  const size_t flushed = batch_bytes[bucket];
  batch_bytes[bucket] = 0;
  *total_batch_bytes =
      *total_batch_bytes > flushed ? (*total_batch_bytes - flushed) : 0;
  if (grant_cache != nullptr && flushed > 0) {
    grant_cache->Release(flushed);
  }
  return Status::kSuccess;
}

Status AppendToGraceBuckets(
    std::vector<std::unique_ptr<SpillSegmentWriter>>& writers,
    std::vector<DataChunk>& chunks, std::vector<size_t>& batch_bytes,
    std::vector<uint64_t>& row_counts, std::vector<uint64_t>& est_bytes,
    size_t* total_batch_bytes, size_t batch_cap, size_t total_cap,
    QueryMemoryGrant::LocalBatchCache* grant_cache, size_t bucket, Row&& row,
    RowPosition pos, size_t rb, std::string* codec_scratch) {
  if (total_cap > 0 && *total_batch_bytes + rb > total_cap) {
    size_t best = 0;
    for (size_t i = 1; i < batch_bytes.size(); ++i) {
      if (batch_bytes[i] > batch_bytes[best]) {
        best = i;
      }
    }
    if (batch_bytes[best] > 0) {
      RETURN_IF_FAIL(FlushGraceBucket(best, writers, chunks, batch_bytes,
                                      total_batch_bytes, grant_cache,
                                      codec_scratch));
    }
  }
  if (grant_cache != nullptr && !grant_cache->Reserve(rb)) {
    for (size_t i = 0; i < chunks.size(); ++i) {
      if (!chunks[i].Empty()) {
        RETURN_IF_FAIL(FlushGraceBucket(i, writers, chunks, batch_bytes,
                                        total_batch_bytes, grant_cache,
                                        codec_scratch));
      }
    }
    // Everything buffered was just flushed; if even one row cannot be reserved
    // the grant is exhausted, and appending anyway would hold memory the
    // query grant never accounted for.
    if (!grant_cache->Reserve(rb)) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "OOC hash join: grace staging grant exhausted");
    }
  }
  chunks[bucket].Append(std::move(row), pos);
  batch_bytes[bucket] += rb;
  *total_batch_bytes += rb;
  ++row_counts[bucket];
  est_bytes[bucket] += rb;
  if (chunks[bucket].Size() >= kGraceWriteBatchRows ||
      batch_bytes[bucket] >= batch_cap) {
    RETURN_IF_FAIL(FlushGraceBucket(bucket, writers, chunks, batch_bytes,
                                    total_batch_bytes, grant_cache,
                                    codec_scratch));
  }
  return Status::kSuccess;
}

}  // namespace

void OocHashJoinContext::ReleaseResidentMemory() noexcept {
  build_rows.clear();
  build_rows.shrink_to_fit();
  build_positions.clear();
  build_positions.shrink_to_fit();
  build_matched.clear();
  build_matched.shrink_to_fit();
  hash_index = JoinHashIndex();
  resident_grant_cache.reset();
  resident_guard.reset();
}

OocHashJoinContext::~OocHashJoinContext() {
  reduced_probe_store.reset();
  semi_probe_domain.reset();
  semi_kept_ordinals.clear();
  for (const OocBuildSpillBucket& b : build_buckets) {
    SpillStore::DropSegmentIfAlive(spill_store, spill_store_alive,
                                   b.segment_id);
  }
  build_buckets.clear();
  ReleaseResidentMemory();
}

// --- OocSemiProbeIntakeSink ---

OocSemiProbeIntakeSink::OocSemiProbeIntakeSink(
    std::shared_ptr<OocHashJoinContext> join_ctx, std::string label)
    : join_ctx_(std::move(join_ctx)), label_(std::move(label)) {}

OocSemiProbeIntakeSink::~OocSemiProbeIntakeSink() {
  grant_cache_.reset();
  consumer_guard_.reset();
}

Status OocSemiProbeIntakeSink::Init(PipelineContext* ctx,
                                    size_t /*worker_count*/) {
  join_ctx_->spill_store = &ctx->spill_store;
  join_ctx_->spill_store_alive = ctx->spill_store.LifetimeToken();
  join_ctx_->any_null_safe = std::ranges::any_of(
      join_ctx_->spec.key_null_safe, [](bool safe) { return safe; });
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  max_domain_keys_ = HashJoin::SemiReductionMaxDomainKeysForTest();
  const size_t refill =
      budget_bytes_ > 0
          ? std::min<size_t>(
                QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                std::max<size_t>(65536, budget_bytes_ / 4))
          : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
  grant_cache_.emplace(&ctx->grant, refill);

  SpillableStore::Options store_opts;
  store_opts.spill_store = &ctx->spill_store;
  store_opts.memory_budget_bytes =
      budget_bytes_ > 0 ? std::max<size_t>(budget_bytes_ / 2, 16384)
                        : (size_t{64} << 20);
  store_opts.grant = &ctx->grant;
  store_opts.force_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  join_ctx_->reduced_probe_store = std::make_unique<SpillableStore>(store_opts);

  domain_ = std::make_unique<JoinHashIndex>();
  domain_->Init(JoinHashIndex::KeyMode::kBytes, 0);
  domain_bytes_ = 0;
  domain_keys_ = 0;
  domain_capped_ = false;
  return Status::kSuccess;
}

Status OocSemiProbeIntakeSink::Consume(PipelineContext* /*ctx*/,
                                       size_t /*worker_id*/,
                                       const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  RETURN_IF_FAIL(join_ctx_->reduced_probe_store->AppendChunk(input));
  if (domain_capped_ || domain_ == nullptr) {
    return Status::kSuccess;
  }
  const OocHashJoinSpec& spec = join_ctx_->spec;
  const std::vector<slot_t>& probe_cols =
      spec.build_left ? spec.right_cols : spec.left_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &spec.key_null_safe : nullptr;

  for (size_t r = 0; r < input.Size(); ++r) {
    const Row row = input.RowAt(r);
    const ExtractedJoinKey k = ExtractJoinKey(
        row, probe_cols, JoinHashIndex::KeyMode::kBytes, ValueType::kInt64,
        &key_scratch_, ns, join_ctx_->any_null_safe);
    if (!k.valid) {
      continue;
    }
    if (domain_->Find(k.hash, 0, key_scratch_) != JoinHashIndex::kNil) {
      continue;
    }
    const size_t est = key_scratch_.size() + 32;
    const size_t domain_cap_bytes =
        budget_bytes_ > 0 ? std::max<size_t>(budget_bytes_ / 2, 32768)
                          : std::numeric_limits<size_t>::max();
    if (domain_keys_ >= max_domain_keys_ ||
        domain_bytes_ + est > domain_cap_bytes) {
      domain_capped_ = true;
      domain_.reset();
      break;
    }
    domain_->Insert(k.hash, 0, key_scratch_, domain_keys_);
    ++domain_keys_;
    domain_bytes_ += est;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(domain_bytes_);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocSemiProbeIntakeSink::Finalize(
    PipelineContext* /*ctx*/) {
  CaptureSourceDumpSnapshot(join_ctx_->probe_source_weak,
                            &join_ctx_->probe_explain_snapshot);
  RETURN_IF_FAIL(join_ctx_->reduced_probe_store->Seal());
  RETURN_IF_FAIL(join_ctx_->reduced_probe_store->ResetCursor());

  join_ctx_->semi_domain_capped = domain_capped_;
  if (domain_capped_) {
    join_ctx_->semi_probe_domain.reset();
    join_ctx_->semi_kept_ordinals.clear();
    join_ctx_->semi_reduction_probe_keys = 0;
    join_ctx_->semi_reduction_description = "fallback=domain_cap";
  } else {
    join_ctx_->semi_probe_domain = std::move(domain_);
    join_ctx_->semi_kept_ordinals.assign(domain_keys_, 0);
    join_ctx_->semi_reduction_probe_keys = domain_keys_;
    join_ctx_->semi_reduction_description =
        domain_keys_ == 0 ? "empty_probe_domain" : "probe_key_domain";
  }
  join_ctx_->semi_intake_completed = true;
  grant_cache_.reset();
  consumer_guard_.reset();
  return std::make_shared<ChunkVectorSource>(std::vector<DataChunk>{},
                                             "SemiProbeIntakeDone");
}

// --- OocHashBuildSink ---

OocHashBuildSink::OocHashBuildSink(std::shared_ptr<OocHashJoinContext> join_ctx,
                                   std::string label)
    : join_ctx_(std::move(join_ctx)), label_(std::move(label)) {}

OocHashBuildSink::~OocHashBuildSink() {
  for (auto& w : bucket_writers_) {
    if (w != nullptr) {
      w->Abort();
    }
  }
  bucket_writers_.clear();
  bucket_chunks_.clear();
  if (join_ctx_ != nullptr && !join_ctx_->finalized) {
    join_ctx_->ReleaseResidentMemory();
  }
  consumer_guard_.reset();
}

Status OocHashBuildSink::Init(PipelineContext* ctx, size_t /*worker_count*/) {
  join_ctx_->spill_store = &ctx->spill_store;
  join_ctx_->spill_store_alive = ctx->spill_store.LifetimeToken();
  join_ctx_->any_null_safe = std::ranges::any_of(
      join_ctx_->spec.key_null_safe, [](bool safe) { return safe; });
  consumer_guard_.emplace(&ctx->grant);
  budget_bytes_ = ctx->OperatorBudgetBytes();
  const size_t refill =
      budget_bytes_ > 0
          ? std::min<size_t>(
                QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                std::max<size_t>(65536, budget_bytes_ / 4))
          : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
  join_ctx_->resident_grant_cache =
      std::make_unique<QueryMemoryGrant::LocalBatchCache>(&ctx->grant, refill);

  join_ctx_->root_fanout_bits = RecursivePartitioner::ComputeFanoutBits(
      budget_bytes_, &join_ctx_->io_buf_bytes);
  if (join_ctx_->semi_intake_completed && !join_ctx_->semi_probe_domain) {
    semi_seen_keys_ = std::make_unique<JoinHashIndex>();
    semi_seen_keys_->Init(JoinHashIndex::KeyMode::kBytes, 0);
  }
  return Status::kSuccess;
}

Status OocHashBuildSink::TransitionToSpill(PipelineContext* ctx) {
  if (spilled_) {
    return Status::kSuccess;
  }
  const size_t fanout = size_t{1} << join_ctx_->root_fanout_bits;
  const size_t active_budget =
      ctx != nullptr ? ctx->OperatorBudgetBytes() : budget_bytes_;
  const size_t wbuf =
      GraceWriterBufferBytesFor(active_budget, fanout, join_ctx_->io_buf_bytes);
  const size_t batch_cap = GraceWriteBatchBytesFor(active_budget, fanout);
  const size_t total_cap = GraceTotalStagingCapFor(active_budget);
  bucket_writers_.resize(fanout);
  bucket_chunks_.resize(fanout);
  bucket_batch_bytes_.assign(fanout, 0);
  bucket_row_counts_.assign(fanout, 0);
  bucket_est_bytes_.assign(fanout, 0);
  bucket_total_batch_bytes_ = 0;
  for (size_t b = 0; b < fanout; ++b) {
    bucket_writers_[b] =
        std::make_unique<SpillSegmentWriter>(&ctx->spill_store, wbuf);
  }

  // Free resident row-build charge before buffering Grace write batches so
  // staging borrows against the released grant cleanly.
  if (join_ctx_->resident_grant_cache != nullptr) {
    join_ctx_->resident_grant_cache->ReleaseAll();
  }

  const std::vector<slot_t>& build_cols = join_ctx_->spec.build_left
                                              ? join_ctx_->spec.left_cols
                                              : join_ctx_->spec.right_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &join_ctx_->spec.key_null_safe : nullptr;

  for (size_t i = 0; i < join_ctx_->build_rows.size(); ++i) {
    Row& row = join_ctx_->build_rows[i];
    const RowPosition& pos = join_ctx_->build_positions[i];
    const uint64_t h = PartitionHashForRow(
        row, build_cols, ns, join_ctx_->any_null_safe, &key_scratch_);
    const uint32_t bucket = RecursivePartitioner::RadixBucket(
        h, /*depth=*/0, join_ctx_->root_fanout_bits);
    const size_t rb = EstimateRowMemoryBytes(row) + sizeof(RowPosition);
    join_ctx_->total_build_bytes += rb;
    RETURN_IF_FAIL(AppendToGraceBuckets(
        bucket_writers_, bucket_chunks_, bucket_batch_bytes_,
        bucket_row_counts_, bucket_est_bytes_, &bucket_total_batch_bytes_,
        batch_cap, total_cap, join_ctx_->resident_grant_cache.get(), bucket,
        std::move(row), pos, rb, &codec_scratch_));
  }

  join_ctx_->build_rows.clear();
  join_ctx_->build_rows.shrink_to_fit();
  join_ctx_->build_positions.clear();
  join_ctx_->build_positions.shrink_to_fit();
  mem_bytes_ = 0;
  spilled_ = true;
  return Status::kSuccess;
}

Status OocHashBuildSink::Consume(PipelineContext* ctx, size_t /*worker_id*/,
                                 const DataChunk& input) {
  if (input.Empty()) {
    return Status::kSuccess;
  }
  const std::vector<slot_t>& build_cols = join_ctx_->spec.build_left
                                              ? join_ctx_->spec.left_cols
                                              : join_ctx_->spec.right_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &join_ctx_->spec.key_null_safe : nullptr;
  const bool always_spill = ProcessMemoryBudget::Global().AlwaysSpillEnabled();
  if (!spilled_ && always_spill) {
    RETURN_IF_FAIL(TransitionToSpill(ctx));
  }

  for (size_t r = 0; r < input.Size(); ++r) {
    Row row = input.RowAt(r);
    const RowPosition pos = input.PositionAt(r);
    ++join_ctx_->build_total_rows;

    // Detect non-safe NULL build keys for NullAwareAnti (NOT IN) short-circuit.
    if (!join_ctx_->any_null_safe && !join_ctx_->build_has_null_key) {
      for (const slot_t c : build_cols) {
        if (c >= row.Count() || row[c].IsNull()) {
          join_ctx_->build_has_null_key = true;
          break;
        }
      }
    }

    if (join_ctx_->semi_intake_completed) {
      ++join_ctx_->semi_build_rows;
      const ExtractedJoinKey sk = ExtractJoinKey(
          row, build_cols, JoinHashIndex::KeyMode::kBytes, ValueType::kInt64,
          &key_scratch_, ns, join_ctx_->any_null_safe);
      if (!sk.valid) {
        continue;
      }
      if (join_ctx_->semi_probe_domain != nullptr) {
        const size_t entry =
            join_ctx_->semi_probe_domain->Find(sk.hash, 0, key_scratch_);
        if (entry == JoinHashIndex::kNil) {
          continue;
        }
        const size_t ordinal = join_ctx_->semi_probe_domain->RowIndex(entry);
        if (join_ctx_->semi_kept_ordinals[ordinal] != 0U) {
          ++join_ctx_->semi_duplicate_keys_pruned;
          continue;
        }
        join_ctx_->semi_kept_ordinals[ordinal] = 1;
      } else if (!semi_seen_capped_ && semi_seen_keys_ != nullptr) {
        if (semi_seen_keys_->Find(sk.hash, 0, key_scratch_) !=
            JoinHashIndex::kNil) {
          ++join_ctx_->semi_duplicate_keys_pruned;
          continue;
        }
        const size_t est = key_scratch_.size() + 32;
        if (budget_bytes_ > 0 &&
            semi_seen_bytes_ + est >
                std::max<size_t>(budget_bytes_ / 2, 32768)) {
          semi_seen_capped_ = true;
          semi_seen_keys_.reset();
        } else {
          semi_seen_keys_->Insert(sk.hash, 0, key_scratch_, semi_seen_bytes_);
          semi_seen_bytes_ += est;
        }
      }
      ++join_ctx_->semi_build_rows_kept;
    }

    const size_t rb = EstimateRowMemoryBytes(row) + sizeof(RowPosition);
    const size_t fanout = bucket_writers_.empty()
                              ? (size_t{1} << join_ctx_->root_fanout_bits)
                              : bucket_writers_.size();
    const size_t active_budget =
        ctx != nullptr ? ctx->OperatorBudgetBytes() : budget_bytes_;
    // ADR-0004 rule 9: no single build row may exceed 1/4 of the operator
    // budget, even when it is the first row (the resident budget guards below
    // are empty-set gated) or when it would otherwise go to a grace bucket.
    if (active_budget > 0 && rb > active_budget / 4) {
      return StatusError(StatusCode::kTooBigData,
                         "build row exceeds operator budget quarter");
    }
    const size_t batch_cap = GraceWriteBatchBytesFor(active_budget, fanout);
    const size_t total_cap = GraceTotalStagingCapFor(active_budget);
    auto spill_one_row = [&](Row&& srow) -> Status {
      const uint64_t h = PartitionHashForRow(
          srow, build_cols, ns, join_ctx_->any_null_safe, &key_scratch_);
      const uint32_t bucket = RecursivePartitioner::RadixBucket(
          h, /*depth=*/0, join_ctx_->root_fanout_bits);
      join_ctx_->total_build_bytes += rb;
      return AppendToGraceBuckets(
          bucket_writers_, bucket_chunks_, bucket_batch_bytes_,
          bucket_row_counts_, bucket_est_bytes_, &bucket_total_batch_bytes_,
          batch_cap, total_cap, join_ctx_->resident_grant_cache.get(), bucket,
          std::move(srow), pos, rb, &codec_scratch_);
    };
    if (spilled_) {
      RETURN_IF_FAIL(spill_one_row(std::move(row)));
      continue;
    }

    const size_t next_index_bytes =
        EstimateJoinHashIndexBytes(join_ctx_->build_rows.size() + 1);
    if ((budget_bytes_ > 0 && !join_ctx_->build_rows.empty() &&
         mem_bytes_ + rb + next_index_bytes > budget_bytes_) ||
        (join_ctx_->resident_grant_cache != nullptr &&
         !join_ctx_->resident_grant_cache->Reserve(rb))) {
      RETURN_IF_FAIL(TransitionToSpill(ctx));
      RETURN_IF_FAIL(spill_one_row(std::move(row)));
      continue;
    }

    join_ctx_->build_rows.push_back(std::move(row));
    join_ctx_->build_positions.push_back(pos);
    mem_bytes_ += rb;
    ProcessMemoryBudget::Global().RecordOperatorPeakBytes(mem_bytes_ +
                                                          next_index_bytes);
  }
  return Status::kSuccess;
}

StatusOr<std::shared_ptr<PipelineSource>> OocHashBuildSink::Finalize(
    PipelineContext* /*ctx*/) {
  const OocHashJoinSpec& spec = join_ctx_->spec;
  const std::vector<slot_t>& build_cols =
      spec.build_left ? spec.left_cols : spec.right_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &spec.key_null_safe : nullptr;

  CaptureSourceDumpSnapshot(join_ctx_->build_source_weak,
                            &join_ctx_->build_explain_snapshot);
  if (join_ctx_->semi_intake_completed) {
    join_ctx_->semi_probe_domain.reset();
    join_ctx_->semi_kept_ordinals.clear();
    semi_seen_keys_.reset();
    join_ctx_->semi_reduction_active = true;
  }

  if (spilled_) {
    const size_t fanout = bucket_writers_.size();
    join_ctx_->build_buckets.resize(fanout);
    for (size_t b = 0; b < fanout; ++b) {
      RETURN_IF_FAIL(FlushGraceBucket(
          b, bucket_writers_, bucket_chunks_, bucket_batch_bytes_,
          &bucket_total_batch_bytes_, join_ctx_->resident_grant_cache.get(),
          &codec_scratch_));
      if (bucket_row_counts_[b] > 0) {
        ASSIGN_OR_RETURN(SpillSegmentId, seg,
                         bucket_writers_[b]->Finish(false));
        join_ctx_->build_buckets[b] = OocBuildSpillBucket{
            .segment_id = seg,
            .row_count = bucket_row_counts_[b],
            .estimated_bytes = bucket_est_bytes_[b],
        };
      } else {
        bucket_writers_[b]->Abort();
      }
    }
    bucket_writers_.clear();
    bucket_chunks_.clear();
    if (join_ctx_->resident_grant_cache != nullptr) {
      join_ctx_->resident_grant_cache->ReleaseAll();
    }
    join_ctx_->spilled = true;
    join_ctx_->finalized = true;
    consumer_guard_.reset();
    return std::make_shared<ChunkVectorSource>(std::vector<DataChunk>{},
                                               "BuildSpilled");
  }

  // In-memory fast path: index `join_ctx_->build_rows` directly using
  // `JoinHashIndex` and optionally publish a RuntimeFilterGroup.
  join_ctx_->spilled = false;
  join_ctx_->finalized = true;
  bool all_same_int = true;
  ValueType first_t = ValueType::kNull;
  if (build_cols.size() == 1 && !join_ctx_->any_null_safe) {
    for (const Row& row : join_ctx_->build_rows) {
      const slot_t c = build_cols[0];
      if (c >= row.Count() || row[c].IsNull()) {
        continue;
      }
      const Value& v = row[c];
      if (v.type != ValueType::kInt64 && v.type != ValueType::kDate) {
        all_same_int = false;
        break;
      }
      // An unsigned value >= 2^63 has the raw bits of a negative INT64 but
      // is a different number; the int-mode index would wrongly equate them.
      if (v.IsUnsigned() && v.value.int_value < 0) {
        all_same_int = false;
        break;
      }
      if (first_t == ValueType::kNull) {
        first_t = v.type;
      } else if (v.type != first_t) {
        all_same_int = false;
        break;
      }
    }
    if (!join_ctx_->build_rows.empty() && all_same_int &&
        first_t != ValueType::kNull) {
      join_ctx_->key_mode = JoinHashIndex::KeyMode::kInt64;
      join_ctx_->int_key_type = first_t;
    } else {
      join_ctx_->key_mode = JoinHashIndex::KeyMode::kBytes;
    }
  } else {
    all_same_int = false;
    join_ctx_->key_mode = JoinHashIndex::KeyMode::kBytes;
  }

  join_ctx_->hash_index.Init(join_ctx_->key_mode, join_ctx_->build_rows.size());
  RuntimeFilterBuilder rf_builder;
  const std::vector<slot_t>& probe_cols =
      spec.build_left ? spec.right_cols : spec.left_cols;
  const bool collect_rf = spec.dynamic_filter && !join_ctx_->any_null_safe &&
                          spec.kind == JoinKind::kInner &&
                          build_cols.size() == 1 && probe_cols.size() == 1 &&
                          all_same_int;
  if (collect_rf) {
    rf_builder.Reserve(join_ctx_->build_rows.size());
  }

  for (size_t i = join_ctx_->build_rows.size(); i > 0; --i) {
    const size_t idx = i - 1;
    const Row& row = join_ctx_->build_rows[idx];
    const ExtractedJoinKey k = ExtractJoinKey(
        row, build_cols, join_ctx_->key_mode, join_ctx_->int_key_type,
        &key_scratch_, ns, join_ctx_->any_null_safe);
    if (k.valid) {
      join_ctx_->hash_index.Insert(k.hash, k.int_key, key_scratch_, idx);
      if (collect_rf && (row[build_cols[0]].type == ValueType::kInt64 ||
                         row[build_cols[0]].type == ValueType::kDate)) {
        rf_builder.Observe(row[build_cols[0]].value.int_value);
      }
    } else if (collect_rf) {
      rf_builder.ObserveNull();
    }
  }

  join_ctx_->small_build_runtime_filter =
      (spec.kind == JoinKind::kInner && !join_ctx_->any_null_safe &&
       !join_ctx_->build_rows.empty() &&
       join_ctx_->build_rows.size() <= kSmallBuildThreshold &&
       build_cols.size() == 1);

  if (collect_rf) {
    const ValueType effective_type =
        first_t != ValueType::kNull ? first_t : spec.runtime_filter_probe_type;
    std::shared_ptr<const RuntimeFilterGroup> rf =
        rf_builder.Build(/*allow_reject_all=*/true);
    if (rf != nullptr && !rf->empty() &&
        (rf->RejectsAll() || effective_type == ValueType::kInt64 ||
         effective_type == ValueType::kDate)) {
      bool pushed = false;
      if (spec.runtime_filter_probe_target &&
          spec.runtime_filter_probe_col.has_value()) {
        pushed = spec.runtime_filter_probe_target->AttachRuntimeFilter(
            rf, *spec.runtime_filter_probe_col, effective_type);
      }
      join_ctx_->dynamic_filter_published = pushed;
      join_ctx_->dynamic_filter_keys = rf->DistinctKeys();
      join_ctx_->dynamic_filter_description = rf->Describe();
      if (!pushed) {
        join_ctx_->runtime_filter = std::move(rf);
      }
    }
  }

  if (ShouldKeepBuildOnlyUnmatched(spec.kind, spec.build_left)) {
    join_ctx_->build_matched.assign(join_ctx_->build_rows.size(), 0);
  }
  if (join_ctx_->resident_grant_cache != nullptr) {
    if (!join_ctx_->resident_grant_cache->Reserve(
            EstimateJoinHashIndexBytes(join_ctx_->build_rows.size()))) {
      return StatusError(StatusCode::kMemoryLimitExceeded,
                         "OOC hash join: resident hash index grant exhausted");
    }
  }
  join_ctx_->resident_guard = std::move(consumer_guard_);
  return std::make_shared<ChunkVectorSource>(std::vector<DataChunk>{},
                                             "BuildComplete");
}

// --- OocAdaptiveHashProbeSource & Grace Partition Cursor ---

struct OocAdaptiveHashProbeSource::GracePartitionCursor {
  GracePartitionCursor() = default;
  GracePartitionCursor(const GracePartitionCursor&) = delete;
  GracePartitionCursor& operator=(const GracePartitionCursor&) = delete;
  GracePartitionCursor(GracePartitionCursor&&) = delete;
  GracePartitionCursor& operator=(GracePartitionCursor&&) = delete;

  SpillStore* spill_store{nullptr};
  std::weak_ptr<const bool> spill_store_alive;
  const OocHashJoinSpec* spec{nullptr};
  uint64_t build_total_rows{0};
  size_t budget_bytes{0};
  size_t io_buf_bytes{16384};

  SpillSegmentId build_seg{kInvalidSpillSegmentId};
  SpillSegmentId probe_seg{kInvalidSpillSegmentId};
  uint64_t probe_row_count{0};

  // Mode 1: Single build hash index (`<= budget_bytes`).
  // Mode 2: Skew chunked BNL slices (`<= budget_bytes / 2` per build slice).
  std::unique_ptr<SpillSegmentReader> build_slice_reader;
  std::unique_ptr<SpillSegmentReader> probe_reader;
  size_t probe_row_in_chunk{0};
  uint64_t probe_global_idx{0};
  size_t unmatched_build_cursor{0};

  std::vector<Row> slice_build_rows;
  std::vector<RowPosition> slice_build_positions;
  std::vector<uint8_t> slice_build_matched;
  std::vector<uint8_t> probe_matched_bits;
  std::string key_scratch;
  std::string codec_scratch;
  // Multi-slice skew match tracking across probe rows:
  OocAdaptiveHashProbeSource::ActiveProbeRowState probe_state;
  DataChunk active_probe_chunk;
  JoinHashIndex slice_index;
  QueryMemoryGrant::LocalBatchCache* grant_cache{nullptr};
  bool any_null_safe{false};
  bool build_has_null_key{false};
  bool chunked_skew_mode{false};
  bool build_eof{false};
  JoinHashIndex::KeyMode key_mode{JoinHashIndex::KeyMode::kBytes};
  ValueType int_key_type{ValueType::kInt64};
  bool probe_pass_eof{false};
  bool draining_unmatched_build{false};
  bool emitting_unmatched_probe_pass{false};
  bool finished{false};

  ~GracePartitionCursor() {
    probe_reader.reset();
    build_slice_reader.reset();
    slice_build_rows.clear();
    slice_build_positions.clear();
    slice_build_matched.clear();
    slice_index = JoinHashIndex();
    if (grant_cache != nullptr) {
      grant_cache->ReleaseAll();
    }
    SpillStore::DropSegmentIfAlive(spill_store, spill_store_alive, build_seg);
    SpillStore::DropSegmentIfAlive(spill_store, spill_store_alive, probe_seg);
  }

  void MarkProbeMatched(uint64_t idx) { MarkBit(&probe_matched_bits, idx); }
  [[nodiscard]] bool IsProbeMatched(uint64_t idx) const {
    return TestBit(&probe_matched_bits, idx);
  }

  Status LoadNextBuildSlice() {
    slice_build_rows.clear();
    slice_build_positions.clear();
    slice_build_matched.clear();
    slice_index = JoinHashIndex();
    if (grant_cache != nullptr) {
      grant_cache->ReleaseAll();
    }
    if (build_eof || build_seg == kInvalidSpillSegmentId) {
      build_eof = true;
      return Status::kSuccess;
    }
    if (!build_slice_reader) {
      build_slice_reader = std::make_unique<SpillSegmentReader>(
          spill_store, build_seg, io_buf_bytes);
      RETURN_IF_FAIL(build_slice_reader->Open());
    }
    const size_t slice_cap = chunked_skew_mode
                                 ? std::max<size_t>(16384, budget_bytes / 2)
                                 : std::numeric_limits<size_t>::max();
    size_t slice_bytes = 0;
    DataChunk chunk;
    while (true) {
      ASSIGN_OR_RETURN(
          bool, has,
          ReadDataChunk(build_slice_reader.get(), &chunk, &codec_scratch));
      if (!has) {
        build_eof = true;
        break;
      }
      for (size_t r = 0; r < chunk.Size(); ++r) {
        Row row = chunk.RowAt(r);
        slice_bytes += EstimateRowMemoryBytes(row) + sizeof(RowPosition);
        slice_build_rows.push_back(std::move(row));
        slice_build_positions.push_back(chunk.PositionAt(r));
      }
      if (chunked_skew_mode &&
          slice_bytes + EstimateJoinHashIndexBytes(slice_build_rows.size()) >=
              slice_cap) {
        break;
      }
    }
    if (grant_cache != nullptr && !slice_build_rows.empty()) {
      if (!grant_cache->Reserve(slice_bytes + EstimateJoinHashIndexBytes(
                                                  slice_build_rows.size()))) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "OOC hash join: slice index grant exhausted");
      }
    }

    if (slice_build_rows.empty()) {
      return Status::kSuccess;
    }

    const std::vector<slot_t>& build_cols =
        spec->build_left ? spec->left_cols : spec->right_cols;
    const std::vector<bool>* ns =
        any_null_safe ? &spec->key_null_safe : nullptr;

    key_mode = JoinHashIndex::KeyMode::kBytes;
    if (build_cols.size() == 1 && !any_null_safe) {
      bool all_same = true;
      ValueType ft = ValueType::kNull;
      for (const Row& r : slice_build_rows) {
        if (build_cols[0] >= r.Count() || r[build_cols[0]].IsNull()) {
          continue;
        }
        const Value& v = r[build_cols[0]];
        const ValueType vt = v.type;
        if (vt != ValueType::kInt64 && vt != ValueType::kDate) {
          all_same = false;
          break;
        }
        if (v.IsUnsigned() && v.value.int_value < 0) {
          all_same = false;
          break;
        }
        if (ft == ValueType::kNull) {
          ft = vt;
        } else if (ft != vt) {
          all_same = false;
          break;
        }
      }
      if (all_same && ft != ValueType::kNull) {
        key_mode = JoinHashIndex::KeyMode::kInt64;
        int_key_type = ft;
      }
    }

    slice_index.Init(key_mode, slice_build_rows.size());
    for (size_t i = slice_build_rows.size(); i > 0; --i) {
      const size_t idx = i - 1;
      const ExtractedJoinKey k =
          ExtractJoinKey(slice_build_rows[idx], build_cols, key_mode,
                         int_key_type, &key_scratch, ns, any_null_safe);
      if (k.valid) {
        slice_index.Insert(k.hash, k.int_key, key_scratch, idx);
      }
    }
    if (ShouldKeepBuildOnlyUnmatched(spec->kind, spec->build_left)) {
      slice_build_matched.assign(slice_build_rows.size(), 0);
    }

    // Rewind probe reader for this slice.
    probe_reader.reset();
    if (probe_seg != kInvalidSpillSegmentId) {
      probe_reader = std::make_unique<SpillSegmentReader>(
          spill_store, probe_seg, io_buf_bytes);
      RETURN_IF_FAIL(probe_reader->Open());
    }
    active_probe_chunk.Reset();
    probe_row_in_chunk = 0;
    probe_global_idx = 0;
    probe_pass_eof = (probe_seg == kInvalidSpillSegmentId);
    probe_state.Reset();
    unmatched_build_cursor = 0;
    draining_unmatched_build = false;
    return Status::kSuccess;
  }

  Status NextBatch(DataChunk* out, size_t max_rows) {
    if (finished) {
      return Status::kSuccess;
    }
    const std::vector<slot_t>& probe_cols =
        spec->build_left ? spec->right_cols : spec->left_cols;
    const std::vector<bool>* ns =
        any_null_safe ? &spec->key_null_safe : nullptr;

    while (out->Size() < max_rows) {
      if (emitting_unmatched_probe_pass) {
        while (out->Size() < max_rows) {
          while (probe_row_in_chunk >= active_probe_chunk.Size()) {
            if (!probe_reader) {
              probe_pass_eof = true;
              break;
            }
            ASSIGN_OR_RETURN(
                bool, has,
                ReadDataChunk(probe_reader.get(), &active_probe_chunk,
                              &codec_scratch));
            if (!has) {
              probe_pass_eof = true;
              break;
            }
            probe_row_in_chunk = 0;
          }
          if (probe_pass_eof) {
            finished = true;
            return Status::kSuccess;
          }
          const Row prow = active_probe_chunk.RowAt(probe_row_in_chunk);
          const RowPosition ppos =
              active_probe_chunk.PositionAt(probe_row_in_chunk);
          const uint64_t gidx = probe_global_idx++;
          ++probe_row_in_chunk;
          if (!IsProbeMatched(gidx)) {
            if (spec->kind == JoinKind::kNullAwareAnti && !any_null_safe) {
              // NULL probe keys yield UNKNOWN under NOT IN when build != empty,
              // whereas unmatchable NaN yields false -> true -> emit row.
              const ExtractedJoinKey probe_key = ExtractJoinKey(
                  prow, probe_cols, JoinHashIndex::KeyMode::kBytes,
                  ValueType::kNull, &key_scratch, ns, any_null_safe);
              if (probe_key.has_non_safe_null && build_total_rows > 0) {
                continue;
              }
            }
            const size_t pad_w = spec->build_left
                                     ? spec->left_schema.ColumnCount()
                                     : spec->right_schema.ColumnCount();
            if (spec->build_left) {
              EmitJoinTuple(*spec, MakeNullRow(pad_w), RowPosition(), prow,
                            out);
            } else {
              EmitJoinTuple(*spec, prow, ppos, MakeNullRow(pad_w), out);
            }
          }
        }
        continue;
      }

      if (draining_unmatched_build) {
        while (out->Size() < max_rows &&
               unmatched_build_cursor < slice_build_rows.size()) {
          const size_t bi = unmatched_build_cursor++;
          if (slice_build_matched[bi] == 0) {
            const Row null_probe =
                MakeNullRow(spec->build_left ? spec->right_schema.ColumnCount()
                                             : spec->left_schema.ColumnCount());
            const Row& left_r =
                spec->build_left ? slice_build_rows[bi] : null_probe;
            const Row& right_r =
                spec->build_left ? null_probe : slice_build_rows[bi];
            out->Append(
                CombineRows(left_r, right_r),
                spec->build_left ? slice_build_positions[bi] : RowPosition());
          }
        }
        if (unmatched_build_cursor < slice_build_rows.size()) {
          return Status::kSuccess;
        }
        // Finished this build slice. Either load next slice or finish
        // partition!
        if (!build_eof) {
          RETURN_IF_FAIL(LoadNextBuildSlice());
          if (!slice_build_rows.empty()) {
            continue;
          }
        }
        // Check if unmatched probe rows need final pass (skew Left/Full/Anti).
        if (chunked_skew_mode &&
            ShouldKeepProbeOnlyUnmatched(spec->kind, spec->build_left) &&
            probe_seg != kInvalidSpillSegmentId) {
          probe_reader = std::make_unique<SpillSegmentReader>(
              spill_store, probe_seg, io_buf_bytes);
          RETURN_IF_FAIL(probe_reader->Open());
          active_probe_chunk.Reset();
          probe_row_in_chunk = 0;
          probe_global_idx = 0;
          probe_pass_eof = false;
          emitting_unmatched_probe_pass = true;
          continue;
        }
        finished = true;
        return Status::kSuccess;
      }

      // Probe pass against active `slice_build_rows`:
      while (out->Size() < max_rows) {
        if (!probe_state.active) {
          while (probe_row_in_chunk >= active_probe_chunk.Size()) {
            if (probe_pass_eof || !probe_reader) {
              probe_pass_eof = true;
              break;
            }
            ASSIGN_OR_RETURN(
                bool, has,
                ReadDataChunk(probe_reader.get(), &active_probe_chunk,
                              &codec_scratch));
            if (!has) {
              probe_pass_eof = true;
              break;
            }
            probe_row_in_chunk = 0;
          }
          if (probe_pass_eof) {
            if (ShouldKeepBuildOnlyUnmatched(spec->kind, spec->build_left)) {
              draining_unmatched_build = true;
              break;
            }
            if (!build_eof) {
              RETURN_IF_FAIL(LoadNextBuildSlice());
              if (!slice_build_rows.empty()) {
                continue;
              }
            }
            if (chunked_skew_mode &&
                ShouldKeepProbeOnlyUnmatched(spec->kind, spec->build_left) &&
                probe_seg != kInvalidSpillSegmentId) {
              probe_reader = std::make_unique<SpillSegmentReader>(
                  spill_store, probe_seg, io_buf_bytes);
              RETURN_IF_FAIL(probe_reader->Open());
              active_probe_chunk.Reset();
              probe_row_in_chunk = 0;
              probe_global_idx = 0;
              probe_pass_eof = false;
              emitting_unmatched_probe_pass = true;
              break;
            }
            finished = true;
            return Status::kSuccess;
          }

          probe_state.row = active_probe_chunk.RowAt(probe_row_in_chunk);
          probe_state.pos = active_probe_chunk.PositionAt(probe_row_in_chunk);
          probe_state.gidx = probe_global_idx++;
          ++probe_row_in_chunk;

          const ExtractedJoinKey pk =
              ExtractJoinKey(probe_state.row, probe_cols, key_mode,
                             int_key_type, &key_scratch, ns, any_null_safe);
          probe_state.active = true;
          probe_state.matched_any = false;
          probe_state.has_non_safe_null = pk.has_non_safe_null;
          probe_state.next_entry =
              pk.valid ? slice_index.Find(pk.hash, pk.int_key, key_scratch)
                       : JoinHashIndex::kNil;
        }

        RETURN_IF_FAIL(StepActiveProbeRow(
            *spec, slice_index, slice_build_rows, slice_build_positions,
            &slice_build_matched, chunked_skew_mode, build_total_rows,
            &probe_matched_bits, &probe_state, out, max_rows));
      }
    }
    return Status::kSuccess;
  }
};

OocAdaptiveHashProbeSource::OocAdaptiveHashProbeSource(
    std::shared_ptr<OocHashJoinContext> join_ctx,
    std::shared_ptr<PipelineSource> probe_source,
    std::vector<std::shared_ptr<PipelineOperator>> probe_ops, std::string label)
    : join_ctx_(std::move(join_ctx)),
      probe_source_(std::move(probe_source)),
      probe_ops_(std::move(probe_ops)),
      label_(std::move(label)) {}

OocAdaptiveHashProbeSource::~OocAdaptiveHashProbeSource() {
  active_pair_cursor_.reset();
  CleanupGracePairs();
  if (grant_cache_.has_value()) {
    grant_cache_->ReleaseAll();
  }
  consumer_guard_.reset();
  if (join_ctx_ != nullptr) {
    join_ctx_->ReleaseResidentMemory();
  }
}

void OocAdaptiveHashProbeSource::CleanupGracePairs() noexcept {
  if (join_ctx_) {
    for (const PendingJoinPair& p : pending_pairs_) {
      SpillStore::DropSegmentIfAlive(join_ctx_->spill_store,
                                     join_ctx_->spill_store_alive, p.build_seg);
      SpillStore::DropSegmentIfAlive(join_ctx_->spill_store,
                                     join_ctx_->spill_store_alive, p.probe_seg);
    }
  }
  pending_pairs_.clear();
}

Status OocAdaptiveHashProbeSource::PullNextProbeChunk(PipelineContext* ctx,
                                                      DataChunk* out) {
  out->Reset();
  if (join_ctx_->semi_intake_completed &&
      join_ctx_->reduced_probe_store != nullptr) {
    while (!probe_eof_) {
      DataChunk cur;
      ASSIGN_OR_RETURN(bool, has,
                       join_ctx_->reduced_probe_store->NextChunk(&cur));
      if (!has) {
        probe_eof_ = true;
        break;
      }
      if (!cur.Empty()) {
        *out = std::move(cur);
        return Status::kSuccess;
      }
    }
    return Status::kSuccess;
  }
  while (!probe_eof_) {
    DataChunk raw;
    ASSIGN_OR_RETURN(bool, has, probe_source_->NextBatch(ctx, &raw));
    if (!has) {
      probe_eof_ = true;
      break;
    }
    bool exhausted = false;
    DataChunk cur = std::move(raw);
    DataChunk next;
    for (const auto& op : probe_ops_) {
      if (cur.Empty()) {
        break;
      }
      next.Reset();
      RETURN_IF_FAIL(op->Process(ctx, cur, &next, &exhausted));
      // Swap so `next` keeps the consumed chunk's storage for the next
      // operator's `Reset` reuse path instead of moving into a husk.
      std::swap(cur, next);
      if (exhausted) {
        probe_eof_ = true;
        break;
      }
    }
    if (!cur.Empty()) {
      *out = std::move(cur);
      return Status::kSuccess;
    }
  }
  return Status::kSuccess;
}

Status OocAdaptiveHashProbeSource::PrepareGracePartitions(
    PipelineContext* ctx) {
  const OocHashJoinSpec& spec = join_ctx_->spec;
  if (spec.kind == JoinKind::kNullAwareAnti && join_ctx_->build_has_null_key) {
    // Drop all build buckets immediately: NOT IN with NULL returns 0 rows.
    for (OocBuildSpillBucket& b : join_ctx_->build_buckets) {
      if (b.segment_id != kInvalidSpillSegmentId) {
        ctx->spill_store.DropSegment(b.segment_id);
        b.segment_id = kInvalidSpillSegmentId;
      }
    }
    probe_eof_ = true;
    return Status::kSuccess;
  }

  const size_t fanout = join_ctx_->build_buckets.size();
  const size_t active_budget = ctx != nullptr ? ctx->OperatorBudgetBytes() : 0;
  const size_t wbuf =
      GraceWriterBufferBytesFor(active_budget, fanout, join_ctx_->io_buf_bytes);
  const size_t batch_cap = GraceWriteBatchBytesFor(active_budget, fanout);
  const size_t total_cap = GraceTotalStagingCapFor(active_budget);
  QueryMemoryGrant::LocalBatchCache* cache_ptr =
      grant_cache_.has_value() ? &*grant_cache_ : nullptr;
  std::vector<std::unique_ptr<SpillSegmentWriter>> p_writers(fanout);
  std::vector<DataChunk> p_chunks(fanout);
  std::vector<size_t> p_chunk_bytes(fanout, 0);
  std::vector<uint64_t> p_row_counts(fanout, 0);
  std::vector<uint64_t> p_est_bytes(fanout, 0);
  size_t total_chunk_bytes = 0;
  std::string scratch;

  for (size_t b = 0; b < fanout; ++b) {
    p_writers[b] =
        std::make_unique<SpillSegmentWriter>(&ctx->spill_store, wbuf);
  }

  const std::vector<slot_t>& probe_cols =
      spec.build_left ? spec.right_cols : spec.left_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &spec.key_null_safe : nullptr;

  DataChunk batch;
  while (true) {
    RETURN_IF_FAIL(PullNextProbeChunk(ctx, &batch));
    if (batch.Empty()) {
      break;
    }
    for (size_t r = 0; r < batch.Size(); ++r) {
      Row row = batch.RowAt(r);
      const RowPosition pos = batch.PositionAt(r);
      const uint64_t h = PartitionHashForRow(
          row, probe_cols, ns, join_ctx_->any_null_safe, &key_scratch_);
      const uint32_t bucket = RecursivePartitioner::RadixBucket(
          h, /*depth=*/0, join_ctx_->root_fanout_bits);
      const size_t rb = EstimateRowMemoryBytes(row) + sizeof(RowPosition);
      RETURN_IF_FAIL(AppendToGraceBuckets(
          p_writers, p_chunks, p_chunk_bytes, p_row_counts, p_est_bytes,
          &total_chunk_bytes, batch_cap, total_cap, cache_ptr, bucket,
          std::move(row), pos, rb, &scratch));
    }
  }

  for (size_t b = 0; b < fanout; ++b) {
    RETURN_IF_FAIL(FlushGraceBucket(b, p_writers, p_chunks, p_chunk_bytes,
                                    &total_chunk_bytes, cache_ptr, &scratch));
    OocBuildSpillBucket& bb = join_ctx_->build_buckets[b];
    if (p_row_counts[b] == 0) {
      p_writers[b]->Abort();
      if (bb.row_count > 0 &&
          ShouldKeepBuildOnlyUnmatched(spec.kind, spec.build_left)) {
        pending_pairs_.push_back(PendingJoinPair{
            .build_seg = bb.segment_id,
            .build_rows = bb.row_count,
            .build_bytes = bb.estimated_bytes,
            .build_parent_bytes = join_ctx_->total_build_bytes,
            .probe_seg = kInvalidSpillSegmentId,
            .probe_rows = 0,
            .probe_bytes = 0,
            .depth = 0,
            .bit_offset = join_ctx_->root_fanout_bits,
        });
        bb.segment_id = kInvalidSpillSegmentId;
      } else if (bb.segment_id != kInvalidSpillSegmentId) {
        ctx->spill_store.DropSegment(bb.segment_id);
        bb.segment_id = kInvalidSpillSegmentId;
      }
      continue;
    }

    if (bb.row_count == 0) {
      if (ShouldKeepProbeOnlyUnmatched(spec.kind, spec.build_left)) {
        ASSIGN_OR_RETURN(SpillSegmentId, pseg, p_writers[b]->Finish(false));
        pending_pairs_.push_back(PendingJoinPair{
            .build_seg = kInvalidSpillSegmentId,
            .build_rows = 0,
            .build_bytes = 0,
            .build_parent_bytes = join_ctx_->total_build_bytes,
            .probe_seg = pseg,
            .probe_rows = p_row_counts[b],
            .probe_bytes = p_est_bytes[b],
            .depth = 0,
            .bit_offset = join_ctx_->root_fanout_bits,
        });
      } else {
        p_writers[b]->Abort();
      }
      continue;
    }

    ASSIGN_OR_RETURN(SpillSegmentId, pseg, p_writers[b]->Finish(false));
    pending_pairs_.push_back(PendingJoinPair{
        .build_seg = bb.segment_id,
        .build_rows = bb.row_count,
        .build_bytes = bb.estimated_bytes,
        .build_parent_bytes = join_ctx_->total_build_bytes,
        .probe_seg = pseg,
        .probe_rows = p_row_counts[b],
        .probe_bytes = p_est_bytes[b],
        .depth = 0,
        .bit_offset = join_ctx_->root_fanout_bits,
    });
    bb.segment_id = kInvalidSpillSegmentId;
  }
  if (cache_ptr != nullptr) {
    cache_ptr->ReleaseAll();
  }
  return Status::kSuccess;
}

Status OocAdaptiveHashProbeSource::OpenNextPartitionPair(PipelineContext* ctx,
                                                         PendingJoinPair pair) {
  const size_t budget = ctx->OperatorBudgetBytes();
  const OocHashJoinSpec& spec = join_ctx_->spec;
  const size_t est_index_bytes = EstimateJoinHashIndexBytes(pair.build_rows);
  const bool fits = (budget == 0) || (pair.build_rows == 0) ||
                    (pair.build_bytes + est_index_bytes <= budget);
  QueryMemoryGrant::LocalBatchCache* cache_ptr =
      grant_cache_.has_value() ? &*grant_cache_ : nullptr;

  if (fits) {
    auto cur = std::make_unique<GracePartitionCursor>();
    cur->spill_store = &ctx->spill_store;
    cur->spill_store_alive = ctx->spill_store.LifetimeToken();
    cur->spec = &spec;
    cur->any_null_safe = join_ctx_->any_null_safe;
    cur->build_has_null_key = join_ctx_->build_has_null_key;
    cur->build_total_rows = join_ctx_->build_total_rows;
    cur->budget_bytes = budget;
    cur->io_buf_bytes = join_ctx_->io_buf_bytes;
    cur->build_seg = pair.build_seg;
    cur->probe_seg = pair.probe_seg;
    cur->probe_row_count = pair.probe_rows;
    cur->chunked_skew_mode = false;
    cur->grant_cache = cache_ptr;
    if (pair.build_rows == 0) {
      cur->build_eof = true;
      cur->emitting_unmatched_probe_pass = true;
      cur->probe_reader = std::make_unique<SpillSegmentReader>(
          &ctx->spill_store, pair.probe_seg, cur->io_buf_bytes);
      RETURN_IF_FAIL(cur->probe_reader->Open());
      cur->probe_pass_eof = false;
    } else {
      RETURN_IF_FAIL(cur->LoadNextBuildSlice());
    }
    active_pair_cursor_ = std::move(cur);
    return Status::kSuccess;
  }

  // Recursive split vs skew fallback (`< 20%` shrink or max_depth):
  const bool insufficient_shrink =
      pair.build_parent_bytes > 0 &&
      pair.build_bytes >
          static_cast<uint64_t>(static_cast<double>(pair.build_parent_bytes) *
                                0.80);
  if (pair.depth >= kMaxGraceDepth || insufficient_shrink) {
    ProcessMemoryBudget::Global().RecordSkewFallback();
    auto cur = std::make_unique<GracePartitionCursor>();
    cur->spill_store = &ctx->spill_store;
    cur->spill_store_alive = ctx->spill_store.LifetimeToken();
    cur->spec = &spec;
    cur->any_null_safe = join_ctx_->any_null_safe;
    cur->build_has_null_key = join_ctx_->build_has_null_key;
    cur->build_total_rows = join_ctx_->build_total_rows;
    cur->budget_bytes = budget;
    cur->io_buf_bytes = join_ctx_->io_buf_bytes;
    cur->build_seg = pair.build_seg;
    cur->probe_seg = pair.probe_seg;
    cur->probe_row_count = pair.probe_rows;
    cur->chunked_skew_mode = true;
    cur->grant_cache = cache_ptr;
    if (ShouldKeepProbeOnlyUnmatched(spec.kind, spec.build_left) ||
        spec.kind == JoinKind::kSemi) {
      cur->probe_matched_bits.assign(
          static_cast<size_t>((pair.probe_rows + 7) / 8), 0);
    }
    RETURN_IF_FAIL(cur->LoadNextBuildSlice());
    active_pair_cursor_ = std::move(cur);
    return Status::kSuccess;
  }

  // Recurse both build & probe partition using identical non-overlapping radix
  // bits at cumulative `pair.bit_offset`.
  const uint32_t next_depth = pair.depth + 1;
  ProcessMemoryBudget::Global().RecordPartitionDepth(next_depth);

  size_t wbuf = 16384;
  const uint32_t fbits = RecursivePartitioner::ComputeFanoutBits(budget, &wbuf);
  const size_t fanout = size_t{1} << fbits;
  const size_t batch_cap = GraceWriteBatchBytesFor(budget, fanout);
  const size_t total_cap = GraceTotalStagingCapFor(budget);

  auto partition_side = [&](SpillSegmentId src_seg,
                            const std::vector<slot_t>& cols,
                            std::vector<SpillSegmentId>* out_segs,
                            std::vector<uint64_t>* out_counts,
                            std::vector<uint64_t>* out_bytes) -> Status {
    out_segs->assign(fanout, kInvalidSpillSegmentId);
    out_counts->assign(fanout, 0);
    out_bytes->assign(fanout, 0);
    if (src_seg == kInvalidSpillSegmentId) {
      return Status::kSuccess;
    }
    std::vector<std::unique_ptr<SpillSegmentWriter>> writers(fanout);
    std::vector<DataChunk> chunks(fanout);
    std::vector<size_t> cbytes(fanout, 0);
    size_t total_cbytes = 0;
    std::string scratch;
    for (size_t i = 0; i < fanout; ++i) {
      writers[i] =
          std::make_unique<SpillSegmentWriter>(&ctx->spill_store, wbuf);
    }
    SpillSegmentReader reader(&ctx->spill_store, src_seg, wbuf);
    const std::vector<bool>* ns =
        join_ctx_->any_null_safe ? &spec.key_null_safe : nullptr;
    DataChunk c;
    while (true) {
      ASSIGN_OR_RETURN(bool, has, ReadDataChunk(&reader, &c, &scratch));
      if (!has) {
        break;
      }
      for (size_t r = 0; r < c.Size(); ++r) {
        Row row = c.RowAt(r);
        const RowPosition pos = c.PositionAt(r);
        const uint64_t h = PartitionHashForRow(
            row, cols, ns, join_ctx_->any_null_safe, &key_scratch_);
        const uint32_t bk = RecursivePartitioner::RadixBucketAtOffset(
            h, pair.bit_offset, fbits);
        const size_t rb = EstimateRowMemoryBytes(row) + sizeof(RowPosition);
        RETURN_IF_FAIL(AppendToGraceBuckets(
            writers, chunks, cbytes, *out_counts, *out_bytes, &total_cbytes,
            batch_cap, total_cap, cache_ptr, bk, std::move(row), pos, rb,
            &scratch));
      }
    }
    for (size_t i = 0; i < fanout; ++i) {
      RETURN_IF_FAIL(FlushGraceBucket(i, writers, chunks, cbytes, &total_cbytes,
                                      cache_ptr, &scratch));
      if ((*out_counts)[i] > 0) {
        ASSIGN_OR_RETURN(SpillSegmentId, sid, writers[i]->Finish(false));
        (*out_segs)[i] = sid;
      } else {
        writers[i]->Abort();
      }
    }
    if (cache_ptr != nullptr) {
      cache_ptr->ReleaseAll();
    }
    return Status::kSuccess;
  };

  const std::vector<slot_t>& bcols =
      spec.build_left ? spec.left_cols : spec.right_cols;
  const std::vector<slot_t>& pcols =
      spec.build_left ? spec.right_cols : spec.left_cols;

  std::vector<SpillSegmentId> b_segs, p_segs;
  std::vector<uint64_t> b_cnts, p_cnts, b_bytes, p_bytes;
  Status st = partition_side(pair.build_seg, bcols, &b_segs, &b_cnts, &b_bytes);
  ctx->spill_store.DropSegment(pair.build_seg);
  pair.build_seg = kInvalidSpillSegmentId;
  if (st != Status::kSuccess) {
    ctx->spill_store.DropSegment(pair.probe_seg);
    for (SpillSegmentId id : b_segs) {
      ctx->spill_store.DropSegment(id);
    }
    return st;
  }
  st = partition_side(pair.probe_seg, pcols, &p_segs, &p_cnts, &p_bytes);
  ctx->spill_store.DropSegment(pair.probe_seg);
  pair.probe_seg = kInvalidSpillSegmentId;
  if (st != Status::kSuccess) {
    for (SpillSegmentId id : b_segs) {
      ctx->spill_store.DropSegment(id);
    }
    for (SpillSegmentId id : p_segs) {
      ctx->spill_store.DropSegment(id);
    }
    return st;
  }

  for (size_t i = fanout; i > 0; --i) {
    const size_t b = i - 1;
    if (b_cnts[b] == 0 && p_cnts[b] == 0) {
      continue;
    }
    if (b_cnts[b] == 0 &&
        !ShouldKeepProbeOnlyUnmatched(spec.kind, spec.build_left)) {
      ctx->spill_store.DropSegment(p_segs[b]);
      continue;
    }
    if (p_cnts[b] == 0 &&
        !ShouldKeepBuildOnlyUnmatched(spec.kind, spec.build_left)) {
      ctx->spill_store.DropSegment(b_segs[b]);
      continue;
    }
    pending_pairs_.push_back(PendingJoinPair{
        .build_seg = b_segs[b],
        .build_rows = b_cnts[b],
        .build_bytes = b_bytes[b],
        .build_parent_bytes = pair.build_bytes,
        .probe_seg = p_segs[b],
        .probe_rows = p_cnts[b],
        .probe_bytes = p_bytes[b],
        .depth = next_depth,
        .bit_offset = pair.bit_offset + fbits,
    });
  }
  return Status::kSuccess;
}

Status OocAdaptiveHashProbeSource::Open(PipelineContext* ctx) {
  if (opened_) {
    return Status::kSuccess;
  }
  opened_ = true;
  if (join_ctx_->resident_guard.has_value()) {
    consumer_guard_ = std::move(join_ctx_->resident_guard);
    join_ctx_->resident_guard.reset();
  } else if (ctx != nullptr) {
    consumer_guard_.emplace(&ctx->grant);
  }
  if (join_ctx_->spec.kind == JoinKind::kInner && join_ctx_->finalized &&
      join_ctx_->build_total_rows == 0) {
    join_ctx_->probe_opened = false;
    probe_eof_ = true;
    join_ctx_->ReleaseResidentMemory();
    consumer_guard_.reset();
    return Status::kSuccess;
  }
  join_ctx_->probe_opened = true;
  if (join_ctx_->spilled && ctx != nullptr) {
    const size_t op_budget = ctx->OperatorBudgetBytes();
    const size_t refill =
        op_budget > 0
            ? std::min<size_t>(
                  QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes,
                  std::max<size_t>(65536, op_budget / 4))
            : QueryMemoryGrant::LocalBatchCache::kDefaultRefillBytes;
    grant_cache_.emplace(&ctx->grant, refill);
  }
  if (join_ctx_->semi_intake_completed &&
      join_ctx_->reduced_probe_store != nullptr) {
    RETURN_IF_FAIL(join_ctx_->reduced_probe_store->ResetCursor());
  } else {
    if (probe_source_) {
      RETURN_IF_FAIL(probe_source_->Open(ctx));
    }
    for (const auto& op : probe_ops_) {
      RETURN_IF_FAIL(op->Open(ctx));
    }
  }
  if (join_ctx_->spilled) {
    RETURN_IF_FAIL(PrepareGracePartitions(ctx));
  } else if (join_ctx_->spec.kind == JoinKind::kNullAwareAnti &&
             join_ctx_->build_has_null_key) {
    probe_eof_ = true;
    join_ctx_->ReleaseResidentMemory();
    consumer_guard_.reset();
  }
  return Status::kSuccess;
}

StatusOr<bool> OocAdaptiveHashProbeSource::NextBatch(PipelineContext* ctx,
                                                     DataChunk* out,
                                                     size_t max_rows) {
  out->Reset();
  if (max_rows == 0) {
    return false;
  }
  if (!opened_) {
    RETURN_IF_FAIL(Open(ctx));
  }
  const OocHashJoinSpec& spec = join_ctx_->spec;

  if (join_ctx_->spilled) {
    while (out->Size() < max_rows) {
      if (active_pair_cursor_ != nullptr) {
        RETURN_IF_FAIL(active_pair_cursor_->NextBatch(out, max_rows));
        if (active_pair_cursor_->finished) {
          active_pair_cursor_.reset();
        }
        if (out->Size() >= max_rows) {
          break;
        }
        continue;
      }
      if (pending_pairs_.empty()) {
        break;
      }
      PendingJoinPair pair = pending_pairs_.back();
      pending_pairs_.pop_back();
      RETURN_IF_FAIL(OpenNextPartitionPair(ctx, pair));
    }
    if (out->Empty()) {
      CleanupGracePairs();
      grant_cache_.reset();
      consumer_guard_.reset();
      join_ctx_->ReleaseResidentMemory();
      return false;
    }
    return true;
  }

  // In-memory build streaming probe path (`O(1)` probe RAM + early LIMIT stop).
  const std::vector<slot_t>& probe_cols =
      spec.build_left ? spec.right_cols : spec.left_cols;
  const std::vector<bool>* ns =
      join_ctx_->any_null_safe ? &spec.key_null_safe : nullptr;

  while (out->Size() < max_rows) {
    if (drain_unmatched_build_) {
      while (out->Size() < max_rows &&
             unmatched_build_idx_ < join_ctx_->build_rows.size()) {
        const size_t bi = unmatched_build_idx_++;
        if (join_ctx_->build_matched[bi] == 0) {
          const Row null_probe =
              MakeNullRow(spec.build_left ? spec.right_schema.ColumnCount()
                                          : spec.left_schema.ColumnCount());
          const Row& left_r =
              spec.build_left ? join_ctx_->build_rows[bi] : null_probe;
          const Row& right_r =
              spec.build_left ? null_probe : join_ctx_->build_rows[bi];
          out->Append(
              CombineRows(left_r, right_r),
              spec.build_left ? join_ctx_->build_positions[bi] : RowPosition());
        }
      }
      if (unmatched_build_idx_ >= join_ctx_->build_rows.size()) {
        break;
      }
      continue;
    }

    while (!active_probe_state_.active) {
      while (active_probe_row_ >= active_probe_chunk_.Size()) {
        if (probe_eof_) {
          break;
        }
        RETURN_IF_FAIL(PullNextProbeChunk(ctx, &active_probe_chunk_));
        if (active_probe_chunk_.Empty()) {
          probe_eof_ = true;
          break;
        }
        active_probe_row_ = 0;
      }
      if (probe_eof_ && active_probe_row_ >= active_probe_chunk_.Size()) {
        if (ShouldKeepBuildOnlyUnmatched(spec.kind, spec.build_left)) {
          drain_unmatched_build_ = true;
          unmatched_build_idx_ = 0;
        }
        break;
      }

      if (join_ctx_->runtime_filter != nullptr &&
          !join_ctx_->dynamic_filter_published && probe_cols.size() == 1) {
        ++join_ctx_->actual_probe_rows;
        ++join_ctx_->dynamic_filter_probe_rows;
        const Row row_view = active_probe_chunk_.RowAt(active_probe_row_);
        const Value& key = row_view[probe_cols[0]];
        if (key.IsNull()) {
          ++join_ctx_->dynamic_filter_null_rejected;
          ++active_probe_row_;
          continue;
        }
        if ((key.type != ValueType::kInt64 && key.type != ValueType::kDate) ||
            join_ctx_->runtime_filter->Reject(key.value.int_value)) {
          ++join_ctx_->dynamic_filter_rejected;
          ++active_probe_row_;
          continue;
        }
      } else {
        ++join_ctx_->actual_probe_rows;
      }

      active_probe_state_.row = active_probe_chunk_.RowAt(active_probe_row_);
      active_probe_state_.pos =
          active_probe_chunk_.PositionAt(active_probe_row_);
      ++active_probe_row_;

      const ExtractedJoinKey pk = ExtractJoinKey(
          active_probe_state_.row, probe_cols, join_ctx_->key_mode,
          join_ctx_->int_key_type, &key_scratch_, ns, join_ctx_->any_null_safe);
      active_probe_state_.active = true;
      active_probe_state_.matched_any = false;
      active_probe_state_.has_non_safe_null = pk.has_non_safe_null;
      active_probe_state_.next_entry =
          (pk.valid && !join_ctx_->build_rows.empty())
              ? join_ctx_->hash_index.Find(pk.hash, pk.int_key, key_scratch_)
              : JoinHashIndex::kNil;
      if (join_ctx_->small_build_runtime_filter &&
          !join_ctx_->dynamic_filter_published &&
          join_ctx_->runtime_filter == nullptr &&
          spec.kind == JoinKind::kInner &&
          active_probe_state_.next_entry == JoinHashIndex::kNil) {
        if (!pk.valid && pk.has_non_safe_null) {
          ++join_ctx_->dynamic_filter_null_rejected;
        } else if (!pk.valid ||
                   active_probe_state_.next_entry == JoinHashIndex::kNil) {
          ++join_ctx_->dynamic_filter_rejected;
        }
      }
    }
    if (drain_unmatched_build_) {
      continue;
    }
    if (!active_probe_state_.active) {
      break;
    }

    const size_t before_out = out->Size();
    RETURN_IF_FAIL(StepActiveProbeRow(
        spec, join_ctx_->hash_index, join_ctx_->build_rows,
        join_ctx_->build_positions, &join_ctx_->build_matched,
        /*chunked_skew_mode=*/false, join_ctx_->build_total_rows, nullptr,
        &active_probe_state_, out, max_rows));
    join_ctx_->join_matches += (out->Size() - before_out);
  }
  if (out->Empty()) {
    join_ctx_->ReleaseResidentMemory();
    consumer_guard_.reset();
    return false;
  }
  return true;
}

void OocAdaptiveHashProbeSource::RenderTree(std::ostream& out, int indent,
                                            bool is_dump) const {
  const OocHashJoinSpec& spec = join_ctx_->spec;
  const std::string pad(static_cast<size_t>(std::max(0, indent)), ' ');
  const std::string child_pad(static_cast<size_t>(std::max(0, indent + 2)),
                              ' ');
  std::string_view kind_name = "PartitionedHashJoin";
  switch (spec.kind) {
    case JoinKind::kSemi:
      kind_name = "SemiHashJoin";
      break;
    case JoinKind::kAnti:
      kind_name = "AntiHashJoin";
      break;
    case JoinKind::kNullAwareAnti:
      kind_name = "NullAwareAntiHashJoin";
      break;
    case JoinKind::kLeftOuter:
      kind_name = "LeftHashJoin";
      break;
    case JoinKind::kRightOuter:
      kind_name = "RightHashJoin";
      break;
    case JoinKind::kFullOuter:
      kind_name = "FullHashJoin";
      break;
    case JoinKind::kInner:
    case JoinKind::kMark:
    case JoinKind::kSingle:
      kind_name = "PartitionedHashJoin";
      break;
  }
  out << pad << kind_name;
  if (join_ctx_->any_null_safe) {
    out << " null_safe=true";
  }
  if (spec.left_cols.size() != 1) {
    out << " keys=" << spec.left_cols.size();
  }
  if (spec.residual_predicate) {
    std::string text = spec.residual_predicate->ToString();
    if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
      text = text.substr(1, text.size() - 2);
    }
    out << " residual: " << text;
  }
  out << "\n";

  if (is_dump && join_ctx_->semi_reduction_active) {
    const size_t pruned =
        join_ctx_->semi_build_rows >= join_ctx_->semi_build_rows_kept
            ? (join_ctx_->semi_build_rows - join_ctx_->semi_build_rows_kept)
            : 0;
    out << child_pad << "SemiReduction "
        << join_ctx_->semi_reduction_description
        << " probe_keys=" << join_ctx_->semi_reduction_probe_keys
        << " build_rows=" << join_ctx_->semi_build_rows
        << " build_rows_kept=" << join_ctx_->semi_build_rows_kept
        << " build_rows_pruned=" << pruned
        << " duplicate_keys_pruned=" << join_ctx_->semi_duplicate_keys_pruned
        << "\n";
  }
  if (is_dump && !join_ctx_->dynamic_filter_description.empty() &&
      (join_ctx_->dynamic_filter_published ||
       join_ctx_->dynamic_filter_rejected != 0)) {
    out << child_pad
        << "DynamicFilter build_side=" << (spec.build_left ? "left" : "right")
        << " published="
        << (join_ctx_->dynamic_filter_published ? "scan" : "join") << " "
        << join_ctx_->dynamic_filter_description << "\n"
        << child_pad
        << "probe_rows_scanned=" << join_ctx_->dynamic_filter_probe_rows
        << " probe_rows_pruned=" << join_ctx_->dynamic_filter_rejected << "\n"
        << child_pad << "probe_rows_null_rejected="
        << join_ctx_->dynamic_filter_null_rejected << "\n";
  }
  if (is_dump && join_ctx_->finalized && spec.kind == JoinKind::kInner) {
    out << child_pad
        << "HashJoin actual_build_rows=" << join_ctx_->build_total_rows
        << " actual_probe_rows=" << join_ctx_->actual_probe_rows
        << " join_matches=" << join_ctx_->join_matches << "\n";
    if (join_ctx_->build_total_rows == 0 && !join_ctx_->probe_opened) {
      out << child_pad << "EmptyBuildShortCircuit probe_opened=false\n";
    }
    if (join_ctx_->small_build_runtime_filter) {
      const size_t total_rejected = join_ctx_->dynamic_filter_rejected +
                                    join_ctx_->dynamic_filter_null_rejected;
      out << child_pad
          << "RuntimeFilter type=exact_set keys=" << join_ctx_->build_total_rows
          << "\n"
          << child_pad << "probe_rows_rejected=" << total_rejected << "\n"
          << child_pad << "probe_rows_null_rejected="
          << join_ctx_->dynamic_filter_null_rejected << "\n";
    }
    if (!spec.dynamic_filter && !join_ctx_->spilled &&
        join_ctx_->small_build_runtime_filter &&
        join_ctx_->actual_probe_rows > kNestedLoopProbeLimit &&
        join_ctx_->actual_probe_rows < kAdaptiveParallelProbeLimit) {
      out << child_pad << "AdaptiveJoin initial=NestedLoop final=HashJoin\n"
          << child_pad << "switch_reason=outer_rows_exceeded_threshold\n";
    }
  }

  auto render_build = [&]() {
    RenderStreamTree(out, indent + 2, is_dump, join_ctx_->build_ops_explain,
                     join_ctx_->build_source_weak,
                     join_ctx_->build_explain_snapshot, nullptr);
  };
  auto render_probe = [&]() {
    if (join_ctx_->semi_intake_completed ||
        (spec.semi_reduction &&
         (spec.kind == JoinKind::kSemi || spec.kind == JoinKind::kAnti ||
          spec.kind == JoinKind::kNullAwareAnti))) {
      RenderStreamTree(out, indent + 2, is_dump, join_ctx_->probe_ops_explain,
                       join_ctx_->probe_source_weak,
                       join_ctx_->probe_explain_snapshot, nullptr);
    } else {
      RenderStreamTree(out, indent + 2, is_dump, probe_ops_, {}, {},
                       probe_source_);
    }
  };
  if (spec.build_left) {
    render_build();
    render_probe();
  } else {
    render_probe();
    render_build();
  }
}

void OocAdaptiveHashProbeSource::Explain(std::ostream& out, int indent) const {
  RenderTree(out, indent, /*is_dump=*/false);
}

void OocAdaptiveHashProbeSource::DumpExplain(std::ostream& out,
                                             int indent) const {
  RenderTree(out, indent, /*is_dump=*/true);
}

}  // namespace tinylamb
