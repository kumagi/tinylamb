/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/runtime_filter.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <utility>

namespace tinylamb {

const char* RuntimeFilterKindName(RuntimeFilterKind kind) {
  switch (kind) {
    case RuntimeFilterKind::kExactSet:
      return "exact_set";
    case RuntimeFilterKind::kMinMax:
      return "min_max";
    case RuntimeFilterKind::kBloom:
      return "bloom";
    case RuntimeFilterKind::kRejectAll:
      return "reject_all";
  }
  return "unknown";
}

ExactSetRuntimeFilter::ExactSetRuntimeFilter(
    std::vector<int64_t> sorted_distinct_keys, bool already_sorted_unique,
    bool skip_hash_table)
    : keys_(std::move(sorted_distinct_keys)) {
  if (!already_sorted_unique) {
    // Sorts (and dedupes) rather than trusting the caller: a set that is not
    // sorted turns binary_search into "reject everything", which would silently
    // drop join matches.
    std::ranges::sort(keys_);
    const auto last = std::ranges::unique(keys_);
    keys_.erase(last.begin(), last.end());
  }
  if (skip_hash_table || keys_.empty()) {
    return;
  }
  // Membership table at <= 50% load keeps Reject() to ~1.1 probes/key.
  size_t slots = 16;
  while (slots < keys_.size() * 2) {
    slots <<= 1U;
  }
  table_.assign(slots, 0);
  table_mask_ = slots - 1;
  for (size_t i = 0; i < keys_.size(); ++i) {
    auto z = static_cast<uint64_t>(keys_[i]);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    size_t slot = z & table_mask_;
    while (table_[slot] != 0) {
      slot = (slot + 1) & table_mask_;
    }
    table_[slot] = static_cast<uint32_t>(i + 1);
  }
}

bool ExactSetRuntimeFilter::Reject(int64_t key) const {
  if (keys_.empty()) {
    return true;
  }
  if (table_.empty()) [[unlikely]] {
    return !std::binary_search(keys_.begin(), keys_.end(), key);
  }
  // splitmix64 finalizer: probe keys arrive in data order, so the low bits
  // of the raw key must not decide the slot.
  auto z = static_cast<uint64_t>(key);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  size_t slot = z & table_mask_;
  while (true) {
    const uint32_t index = table_[slot];
    if (index == 0) {
      return true;
    }
    if (keys_[index - 1] == key) {
      return false;
    }
    slot = (slot + 1) & table_mask_;
  }
}

MinMaxRuntimeFilter::MinMaxRuntimeFilter(int64_t lo, int64_t hi,
                                         size_t distinct_keys)
    : lo_(lo), hi_(hi), distinct_keys_(distinct_keys) {
  // An inverted enclosure would reject every key, including the build keys
  // themselves.  Clamp instead: soundness must never depend on the caller
  // passing lo <= hi.
  if (hi_ < lo_) {
    std::swap(lo_, hi_);
  }
}

uint64_t BloomRuntimeFilter::Hash(int64_t key, unsigned probe) {
  // One splitmix64 finalizer per key; probe k > 0 advances the state by an
  // odd stride (double hashing), so the probes land on unrelated bit
  // positions while Reject pays for a single mix.  Reject inlines this
  // formula -- keep the two in sync.
  auto z = static_cast<uint64_t>(key);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  return z + ((0x9E3779B97F4A7C15ULL | 1U) * probe);
}

size_t BloomRuntimeFilter::BitsForKeys(size_t distinct_keys) {
  size_t want = distinct_keys * 10;
  want = std::max<size_t>(want, 512);
  constexpr uint64_t kMaxBloomBits =
      sizeof(size_t) >= 8 ? (uint64_t{1} << 34) : (uint64_t{1} << 30);
  size_t bits = 512;
  while (bits < want && static_cast<uint64_t>(bits) < kMaxBloomBits) {
    bits <<= 1U;
  }
  return bits;
}

BloomRuntimeFilter::BloomRuntimeFilter(std::vector<uint64_t> bits,
                                       size_t bit_count, size_t distinct_keys,
                                       size_t nominal_bit_count)
    : bits_(std::move(bits)),
      bit_count_(bit_count),
      distinct_keys_(distinct_keys),
      nominal_bit_count_(nominal_bit_count),
      mask_(bit_count - 1) {
  // A power-of-two bit count that the word vector actually covers is an
  // invariant of BitsForKeys. A non-conforming caller must not turn every
  // lookup into an out-of-bounds bit, and repairing by zeroing the bits would
  // reject every build key, so the only sound repair is to stop pruning.
  if (bit_count_ == 0 || (bit_count_ & (bit_count_ - 1)) != 0 ||
      bits_.size() * 64 < bit_count_) {
    bits_.clear();
    bit_count_ = 0;
    nominal_bit_count_ = 0;
    mask_ = 0;
    disabled_ = true;
  }
}

bool BloomRuntimeFilter::Reject(int64_t key) const {
  if (disabled_) [[unlikely]] {
    return false;
  }
  // Inlined Hash(): both bit addresses derive from one mix, so the two probe
  // loads issue in parallel instead of serializing on a second splitmix.
  auto z = static_cast<uint64_t>(key);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  const size_t bit0 = static_cast<size_t>(z) & mask_;
  const size_t bit1 =
      static_cast<size_t>(z + (0x9E3779B97F4A7C15ULL | 1U)) & mask_;
  if ((bits_[bit0 >> 6] & (uint64_t{1} << (bit0 & 63))) == 0) {
    return true;
  }
  if ((bits_[bit1 >> 6] & (uint64_t{1} << (bit1 & 63))) == 0) {
    return true;
  }
  return false;
}

RuntimeFilterGroup::RuntimeFilterGroup(
    std::vector<std::unique_ptr<RuntimeFilter>> parts,
    const std::vector<int64_t>* sorted_distinct_keys)
    : parts_(std::move(parts)) {
  for (const auto& part : parts_) {
    if (!part) {
      continue;
    }
    switch (part->Kind()) {
      case RuntimeFilterKind::kRejectAll:
        reject_all_ = true;
        break;
      case RuntimeFilterKind::kMinMax: {
        const auto& mm = static_cast<const MinMaxRuntimeFilter&>(*part);
        if (!has_bounds_) {
          has_bounds_ = true;
          lo_ = mm.Lo();
          hi_ = mm.Hi();
        } else {
          lo_ = std::max(lo_, mm.Lo());
          hi_ = std::min(hi_, mm.Hi());
        }
        break;
      }
      case RuntimeFilterKind::kExactSet: {
        const auto& exact = static_cast<const ExactSetRuntimeFilter&>(*part);
        const auto& keys = exact.Keys();
        if ((exact_keys_ == nullptr) || keys.size() < exact_keys_->size()) {
          exact_keys_ = &keys;
        }
        if (keys.empty()) {
          reject_all_ = true;
        } else if (!has_bounds_) {
          has_bounds_ = true;
          lo_ = keys.front();
          hi_ = keys.back();
        } else {
          lo_ = std::max(lo_, keys.front());
          hi_ = std::min(hi_, keys.back());
        }
        break;
      }
      case RuntimeFilterKind::kBloom:
        break;
    }
  }
  if (has_bounds_ && lo_ > hi_) {
    reject_all_ = true;
    return;
  }
  const std::vector<int64_t>* candidate_keys =
      sorted_distinct_keys != nullptr ? sorted_distinct_keys : exact_keys_;
  if (!reject_all_ && candidate_keys != nullptr && !candidate_keys->empty()) {
    if (!has_bounds_) {
      has_bounds_ = true;
      lo_ = candidate_keys->front();
      hi_ = candidate_keys->back();
    }
    if (lo_ <= hi_) {
      const uint64_t span =
          static_cast<uint64_t>(hi_) - static_cast<uint64_t>(lo_);
      if (span <= kMaxDenseSpan) {
        const auto words = static_cast<size_t>((span >> 6) + 1);
        dense_bits_.assign(words, 0);
        const bool single_part =
            (parts_.size() <= 1) ||
            (sorted_distinct_keys != nullptr && parts_.size() == 2);
        for (const int64_t k : *candidate_keys) {
          if (k < lo_ || k > hi_) {
            continue;
          }
          if (!single_part) {
            bool keep = true;
            for (const auto& part : parts_) {
              if (part->Reject(k)) {
                keep = false;
                break;
              }
            }
            if (!keep) {
              continue;
            }
          }
          const uint64_t off =
              static_cast<uint64_t>(k) - static_cast<uint64_t>(lo_);
          dense_bits_[off >> 6] |= uint64_t{1} << (off & 63);
        }
        has_dense_bits_ = true;
      }
    }
  }
}

RuntimeFilterGroup::RuntimeFilterGroup(
    std::vector<std::unique_ptr<RuntimeFilter>> parts,
    std::vector<uint64_t> prebuilt_dense_bits, int64_t lo, int64_t hi)
    : parts_(std::move(parts)),
      dense_bits_(std::move(prebuilt_dense_bits)),
      lo_(lo),
      hi_(hi),
      has_bounds_(true),
      has_dense_bits_(!dense_bits_.empty()) {
  for (const auto& part : parts_) {
    if (!part) {
      continue;
    }
    if (part->Kind() == RuntimeFilterKind::kExactSet) {
      const auto& exact = static_cast<const ExactSetRuntimeFilter&>(*part);
      const auto& keys = exact.Keys();
      if ((exact_keys_ == nullptr) || keys.size() < exact_keys_->size()) {
        exact_keys_ = &keys;
      }
    }
  }
}

bool RuntimeFilterGroup::Has(RuntimeFilterKind kind) const {
  return std::ranges::any_of(
      parts_, [&](const auto& part) { return part->Kind() == kind; });
}

size_t RuntimeFilterGroup::DistinctKeys() const {
  size_t distinct = 0;
  for (const std::unique_ptr<RuntimeFilter>& part : parts_) {
    distinct = std::max(distinct, part->DistinctKeys());
  }
  return distinct;
}

size_t RuntimeFilterGroup::MemoryBytes() const {
  size_t bytes = sizeof(*this);
  for (const std::unique_ptr<RuntimeFilter>& part : parts_) {
    bytes += part->MemoryBytes();
  }
  return bytes;
}

std::string RuntimeFilterGroup::Describe() const {
  if (parts_.empty()) {
    return "none";
  }
  std::vector<std::string> names;
  names.reserve(parts_.size());
  for (const std::unique_ptr<RuntimeFilter>& part : parts_) {
    names.emplace_back(RuntimeFilterKindName(part->Kind()));
  }
  std::sort(names.begin(), names.end());
  std::string out;
  for (size_t i = 0; i < names.size(); ++i) {
    if (i != 0) {
      out.push_back('+');
    }
    out.append(names[i]);
  }
  out.append(" keys=");
  out.append(std::to_string(DistinctKeys()));
  out.append(" bytes=");
  out.append(std::to_string(MemoryBytes()));
  return out;
}

void RuntimeFilterBuilder::Observe(int64_t key) {
  ++observations_;
  if (!has_key_) {
    has_key_ = true;
    lo_ = key;
    hi_ = key;
  } else {
    lo_ = std::min(lo_, key);
    hi_ = std::max(hi_, key);
  }
  if (overflowed_ || keys_.size() >= kMaxTrackedKeys) {
    // Never truncate the key set: a filter that forgot a build key would prune
    // a probe row that does match. Fall back to the range enclosure, which is
    // still sound because lo_/hi_ cover every observed key. The key set stays
    // empty once the cap was hit -- Build() ignores it anyway, and re-filling
    // it would burn memory for nothing.
    overflowed_ = true;
    if (!keys_.empty()) {
      keys_.clear();
      keys_.shrink_to_fit();
    }
    return;
  }
  // The key set is only ever read (deduped, sorted) at Build() time, so
  // append here keeps Observe O(1) and avoids a hash table per join.
  keys_.push_back(key);
}

void RuntimeFilterBuilder::ObserveNull() {
  ++observations_;
  saw_null_ = true;
}

std::shared_ptr<const RuntimeFilterGroup> RuntimeFilterBuilder::Build(
    bool allow_reject_all, size_t max_exact_keys) const {
  if (observations_ == 0 || !has_key_) {
    // Either nothing was observed at all, or every observed build key was
    // NULL: no probe row can match an equi-join.
    if (!allow_reject_all) {
      return nullptr;
    }
    std::vector<std::unique_ptr<RuntimeFilter>> reject_all;
    reject_all.push_back(std::make_unique<RejectAllRuntimeFilter>());
    return std::make_shared<const RuntimeFilterGroup>(std::move(reject_all));
  }
  std::vector<std::unique_ptr<RuntimeFilter>> parts;
  if (overflowed_) {
    parts.push_back(
        std::make_unique<MinMaxRuntimeFilter>(lo_, hi_, keys_.size()));
    return std::make_shared<const RuntimeFilterGroup>(std::move(parts));
  }
  if (lo_ <= hi_) {
    const uint64_t span =
        static_cast<uint64_t>(hi_) - static_cast<uint64_t>(lo_);
    if (span <= RuntimeFilterGroup::kMaxDenseSpan && keys_.size() > 64) {
      const auto words = static_cast<size_t>((span >> 6) + 1);
      std::vector<uint64_t> dense_bits(words, 0);
      size_t distinct_count = 0;
      const auto base_u = static_cast<uint64_t>(lo_);
      for (const int64_t key : keys_) {
        const uint64_t off = static_cast<uint64_t>(key) - base_u;
        const uint64_t mask = uint64_t{1} << (off & 63);
        uint64_t& word = dense_bits[off >> 6];
        if ((word & mask) == 0) {
          word |= mask;
          ++distinct_count;
        }
      }
      if (distinct_count <= max_exact_keys) {
        std::vector<int64_t> distinct;
        distinct.reserve(distinct_count);
        for (size_t w = 0; w < words; ++w) {
          uint64_t bits = dense_bits[w];
          const uint64_t word_base = base_u + (static_cast<uint64_t>(w) << 6);
          while (bits != 0) {
            const auto bit = static_cast<uint64_t>(std::countr_zero(bits));
            distinct.push_back(static_cast<int64_t>(word_base + bit));
            bits &= bits - 1;
          }
        }
        parts.push_back(std::make_unique<ExactSetRuntimeFilter>(
            std::move(distinct), /*already_sorted_unique=*/true,
            /*skip_hash_table=*/true));
        return std::make_shared<const RuntimeFilterGroup>(
            std::move(parts), std::move(dense_bits), lo_, hi_);
      }
      parts.push_back(
          std::make_unique<MinMaxRuntimeFilter>(lo_, hi_, distinct_count));
      parts.push_back(std::make_unique<BloomRuntimeFilter>(
          std::vector<uint64_t>(8, ~uint64_t{0}), 512, distinct_count,
          BloomRuntimeFilter::BitsForKeys(distinct_count)));
      return std::make_shared<const RuntimeFilterGroup>(
          std::move(parts), std::move(dense_bits), lo_, hi_);
    }
  }
  std::vector<int64_t> distinct = keys_;
  std::ranges::sort(distinct);
  const auto last = std::ranges::unique(distinct);
  distinct.erase(last.begin(), last.end());
  // `distinct` is sorted, so the enclosure is its first/last element.
  const int64_t lo = distinct.front();
  const int64_t hi = distinct.back();

  if (distinct.size() <= max_exact_keys) {
    parts.push_back(std::make_unique<ExactSetRuntimeFilter>(
        std::move(distinct), /*already_sorted_unique=*/true));
    return std::make_shared<const RuntimeFilterGroup>(std::move(parts));
  }
  parts.push_back(
      std::make_unique<MinMaxRuntimeFilter>(lo, hi, distinct.size()));
  const size_t bits = BloomRuntimeFilter::BitsForKeys(distinct.size());
  std::vector<uint64_t> words(bits / 64, 0);
  for (const int64_t key : distinct) {
    for (unsigned probe = 0; probe < BloomRuntimeFilter::kHashProbes; ++probe) {
      const size_t bit =
          static_cast<size_t>(BloomRuntimeFilter::Hash(key, probe)) &
          (bits - 1);
      words[bit / 64] |= uint64_t{1} << (bit % 64);
    }
  }
  parts.push_back(std::make_unique<BloomRuntimeFilter>(std::move(words), bits,
                                                       distinct.size()));
  return std::make_shared<const RuntimeFilterGroup>(std::move(parts),
                                                    &distinct);
}

std::shared_ptr<const RuntimeFilterGroup> RuntimeFilterBuilder::FromKeys(
    const std::vector<int64_t>& keys, size_t max_exact_keys) {
  RuntimeFilterBuilder builder;
  for (const int64_t key : keys) {
    builder.Observe(key);
  }
  return builder.Build(false, max_exact_keys);
}

}  // namespace tinylamb
