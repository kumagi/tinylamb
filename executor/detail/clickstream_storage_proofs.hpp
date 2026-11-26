/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_STORAGE_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_STORAGE_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// ClickstreamLayout / Storage
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_RunSidecar.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_TieOrientation.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_DictionaryAdmission.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_HashExceptions.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_ClockWindow.lean
// lean/TinylambOpt/ResearchProofs/ClickstreamLayout/Goal_Storage_ClockForest.lean
// (all sound)
// ============================================================================

[[nodiscard]] constexpr size_t UVarintBytes(uint64_t v) noexcept {
  size_t n = 1;
  while (v >= 0x80U) {
    v >>= 7U;
    ++n;
  }
  return n;
}

[[nodiscard]] constexpr size_t SVarintBytes(int64_t v) noexcept {
  const uint64_t zz =
      (static_cast<uint64_t>(v) << 1U) ^ static_cast<uint64_t>(v >> 63);
  return UVarintBytes(zz);
}

// RunSidecar.Goal: decompose a permutation of `[0, n)` into the minimum number
// of maximal strictly ascending runs, encode as per-logical-row run labels,
// and recover the exact physical permutation from `(labels, num_runs)`.
struct RunSidecarEncoded {
  std::vector<uint32_t> labels;
  uint32_t num_runs{0};
};

[[nodiscard]] inline RunSidecarEncoded EncodeRunSidecar(
    std::span<const uint32_t> perm) {
  RunSidecarEncoded out;
  out.labels.assign(perm.size(), 0);
  if (perm.empty()) {
    return out;
  }
  uint32_t run_id = 0;
  for (size_t i = 0; i < perm.size(); ++i) {
    if (i > 0 && perm[i - 1] >= perm[i]) {
      ++run_id;
    }
    if (perm[i] < out.labels.size()) {
      out.labels[perm[i]] = run_id;
    }
  }
  out.num_runs = run_id + 1;
  return out;
}

[[nodiscard]] inline std::vector<uint32_t> RecoverRunSidecar(
    std::span<const uint32_t> labels, uint32_t num_runs) {
  std::vector<uint32_t> recovered;
  recovered.reserve(labels.size());
  for (uint32_t r = 0; r < num_runs; ++r) {
    for (uint32_t i = 0; i < labels.size(); ++i) {
      if (labels[i] == r) {
        recovered.push_back(i);
      }
    }
  }
  return recovered;
}

// TieOrientation.Goal: two-state dynamic program finding the minimum
// front-coded byte cost over all forward/reverse orientations of tie groups.
[[nodiscard]] inline size_t FrontCodeTransitionBytes(std::string_view prev,
                                                     std::string_view cur) {
  size_t shared = 0;
  const size_t lim = std::min(prev.size(), cur.size());
  while (shared < lim && prev[shared] == cur[shared]) {
    ++shared;
  }
  const size_t suffix = cur.size() - shared;
  return UVarintBytes(shared) + UVarintBytes(suffix) + suffix;
}

[[nodiscard]] inline size_t OptimalTieOrientationBytes(
    std::span<const std::vector<std::string>> tie_groups) {
  std::vector<std::pair<std::string_view, size_t>> states = {{"", 0}};
  for (const auto& group : tie_groups) {
    if (group.empty()) {
      continue;
    }
    auto eval_order = [&](bool reverse) -> std::pair<std::string_view, size_t> {
      size_t best_cost = std::numeric_limits<size_t>::max();
      for (const auto& [prev_word, base_cost] : states) {
        size_t cost = base_cost;
        std::string_view cur_prev = prev_word;
        if (!reverse) {
          for (const std::string& w : group) {
            cost += FrontCodeTransitionBytes(cur_prev, w);
            cur_prev = w;
          }
        } else {
          for (auto it = group.rbegin(); it != group.rend(); ++it) {
            cost += FrontCodeTransitionBytes(cur_prev, *it);
            cur_prev = *it;
          }
        }
        best_cost = std::min(best_cost, cost);
      }
      return {reverse ? std::string_view(group.front())
                      : std::string_view(group.back()),
              best_cost};
    };
    std::vector<std::pair<std::string_view, size_t>> next;
    next.push_back(eval_order(false));
    next.push_back(eval_order(true));
    states = std::move(next);
  }
  size_t ans = std::numeric_limits<size_t>::max();
  for (const auto& [_, c] : states) {
    ans = std::min(ans, c);
  }
  return ans == std::numeric_limits<size_t>::max() ? 0 : ans;
}

// DictionaryAdmission.Goal: exact tree DP maximizing net savings when adopting
// a shared trained dictionary across a laminar bundle hierarchy.
struct DictAdmissionNode {
  bool is_frame{true};
  int64_t frame_saved{0};
  int64_t activation_fee{0};
  int left{-1};
  int right{-1};
};

[[nodiscard]] inline int64_t BestDictionaryAdmissionSavings(
    std::span<const DictAdmissionNode> tree, int root) {
  if (root < 0 || static_cast<size_t>(root) >= tree.size()) {
    return 0;
  }
  const auto& node = tree[static_cast<size_t>(root)];
  if (node.is_frame) {
    return std::max<int64_t>(0, node.frame_saved);
  }
  const int64_t l = BestDictionaryAdmissionSavings(tree, node.left);
  const int64_t r = BestDictionaryAdmissionSavings(tree, node.right);
  return std::max<int64_t>(0, l + r - node.activation_fee);
}

// HashExceptions.Goal: store per-dictionary-code mode hash + sparse exception
// overrides, minimizing exception count while preserving exact hash selection.
struct HashExceptionTable {
  std::unordered_map<uint32_t, uint64_t> mode_hash;
  std::unordered_map<size_t, uint64_t> exceptions;
};

[[nodiscard]] inline HashExceptionTable BuildOptimalHashExceptionTable(
    std::span<const std::pair<uint32_t, uint64_t>> code_hashes) {
  std::unordered_map<uint32_t, std::unordered_map<uint64_t, size_t>> freq;
  for (const auto& [code, h] : code_hashes) {
    ++freq[code][h];
  }
  HashExceptionTable out;
  for (const auto& [code, counts] : freq) {
    uint64_t best_h = 0;
    size_t best_c = 0;
    for (const auto& [h, c] : counts) {
      if (c > best_c) {
        best_c = c;
        best_h = h;
      }
    }
    out.mode_hash[code] = best_h;
  }
  for (size_t i = 0; i < code_hashes.size(); ++i) {
    const auto& [code, h] = code_hashes[i];
    if (out.mode_hash[code] != h) {
      out.exceptions.emplace(i, h);
    }
  }
  return out;
}

template <typename WantedFn>
[[nodiscard]] std::vector<size_t> SelectRowsByHashExceptions(
    std::span<const uint32_t> codes, const HashExceptionTable& table,
    WantedFn&& wanted) {
  std::vector<size_t> out;
  for (size_t i = 0; i < codes.size(); ++i) {
    uint64_t h = 0;
    if (const auto eit = table.exceptions.find(i);
        eit != table.exceptions.end()) {
      h = eit->second;
    } else if (const auto mit = table.mode_hash.find(codes[i]);
               mit != table.mode_hash.end()) {
      h = mit->second;
    }
    if (wanted(h)) {
      out.push_back(i);
    }
  }
  return out;
}

// ClockWindow.Goal: find the globally optimal signed residual window base `low`
// for fixed bit-width `w` by testing only `{0} U {d, d - (2^w - 1) | d in ds}`.
struct ClockWindowChoice {
  int64_t low{0};
  size_t total_bytes{0};
};

[[nodiscard]] inline ClockWindowChoice ChooseOptimalClockWindow(
    std::span<const int64_t> deltas, unsigned bit_width,
    size_t exception_bytes) {
  const int64_t span = bit_width >= 62 ? std::numeric_limits<int64_t>::max()
                                       : ((int64_t{1} << bit_width) - 1);
  const size_t packed_bytes = (deltas.size() * bit_width + 7U) / 8U;
  auto eval_cost = [&](int64_t low) -> size_t {
    size_t misses = 0;
    for (const int64_t d : deltas) {
      if (d < low || (d - low) > span) {
        ++misses;
      }
    }
    return 1U + SVarintBytes(low) + packed_bytes + misses * exception_bytes;
  };
  ClockWindowChoice best{.low = 0, .total_bytes = eval_cost(0)};
  auto try_anchor = [&](int64_t cand) {
    const size_t c = eval_cost(cand);
    if (c < best.total_bytes) {
      best = {.low = cand, .total_bytes = c};
    }
  };
  for (const int64_t d : deltas) {
    try_anchor(d);
    if (d >= std::numeric_limits<int64_t>::min() + span) {
      try_anchor(d - span);
    }
  }
  return best;
}

// ClockForest.Goal: subset DP finding the globally minimum directed acyclic
// anchor forest cost over `n` clock columns (n <= 16).
// `price(col, parent)` returns the encoded byte size of `col` stored
// independently (`parent == std::nullopt`) or delta-coded from `parent`.
template <typename PriceFn>
[[nodiscard]] size_t OptimalClockForestBytes(size_t n_cols, PriceFn&& price) {
  if (n_cols == 0 || n_cols > 16) {
    return 0;
  }
  const size_t full = size_t{1} << n_cols;
  std::vector<size_t> dp(full, std::numeric_limits<size_t>::max());
  dp[0] = 0;
  for (size_t mask = 0; mask < full; ++mask) {
    if (dp[mask] == std::numeric_limits<size_t>::max()) {
      continue;
    }
    for (size_t c = 0; c < n_cols; ++c) {
      if ((mask & (size_t{1} << c)) != 0) {
        continue;
      }
      size_t best_edge = price(c, std::nullopt);
      for (size_t p = 0; p < n_cols; ++p) {
        if ((mask & (size_t{1} << p)) != 0) {
          best_edge = std::min(best_edge, price(c, std::optional<size_t>(p)));
        }
      }
      const size_t next = mask | (size_t{1} << c);
      dp[next] = std::min(dp[next], dp[mask] + best_edge);
    }
  }
  return dp[full - 1];
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_CLICKSTREAM_STORAGE_PROOFS_HPP
