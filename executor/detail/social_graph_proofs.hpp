/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_SOCIAL_GRAPH_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_SOCIAL_GRAPH_PROOFS_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor/detail/analytical_operators_proofs.hpp"

namespace tinylamb::benchmark_rematch {

// ============================================================================
// SocialGraphJoinWeights & ForumRunsAndGraphDegrees
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/SocialGraphJoinWeights.lean
// lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/ForumRunsAndGraphDegrees.lean
// (sound: ActiveFriends, ForumRuns, ForwardReplies, LateForumWeight,
//  DirectoryDegree)
// ============================================================================

// ActiveFriends.Goal (LDBC L1): index edges by person and pull only active
// persons' adjacency lists, bounding physical reads by active edge count.
struct ActiveFriendsResult {
  std::unordered_map<uint64_t, uint64_t> country_counts;
  size_t edge_reads{0};
};

[[nodiscard]] inline ActiveFriendsResult PullActiveFriendCountries(
    std::span<const std::pair<uint64_t, uint64_t>> edges,
    std::span<const uint64_t> active_persons) {
  std::unordered_map<uint64_t, std::vector<uint64_t>> adj;
  for (const auto& [person, country] : edges) {
    adj[person].push_back(country);
  }
  std::unordered_map<uint64_t, uint64_t> active_freq;
  for (const uint64_t p : active_persons) {
    ++active_freq[p];
  }
  ActiveFriendsResult out;
  for (const auto& [person, mult] : active_freq) {
    const auto it = adj.find(person);
    if (it == adj.end()) {
      continue;
    }
    out.edge_reads += it->second.size();
    for (const uint64_t country : it->second) {
      out.country_counts[country] += mult;
    }
  }
  return out;
}

// ForumRuns.Goal (LDBC L2): clustered/sorted messages carry the qualifying
// forum member weight across adjacent equal forum IDs, reducing probe count
// to the number of forum runs.
struct ForumMessage {
  uint64_t forum_id{0};
  int64_t year{0};
  std::optional<int64_t> score;
};

struct ForumYearKey {
  uint64_t forum_id{0};
  int64_t year{0};
  constexpr bool operator==(const ForumYearKey&) const = default;
};

struct ForumYearKeyHash {
  size_t operator()(ForumYearKey k) const noexcept {
    return std::hash<uint64_t>{}(k.forum_id) ^
           (std::hash<int64_t>{}(k.year) * 0x9e3779b97f4a7c15ULL);
  }
};

struct ForumRunsResult {
  std::unordered_map<ForumYearKey, NullableTotals, ForumYearKeyHash> groups;
  size_t member_probes{0};
};

template <typename MemberCountFn>
[[nodiscard]] ForumRunsResult FoldCarriedForumRuns(
    std::span<const ForumMessage> rows, MemberCountFn&& member_count) {
  ForumRunsResult out;
  std::optional<uint64_t> prev_forum;
  uint64_t cached_weight = 0;
  for (const auto& r : rows) {
    if (!prev_forum.has_value() || *prev_forum != r.forum_id) {
      prev_forum = r.forum_id;
      cached_weight = member_count(r.forum_id);
      ++out.member_probes;
    }
    if (cached_weight == 0) {
      continue;
    }
    const ForumYearKey key{.forum_id = r.forum_id, .year = r.year};
    auto& acc = out.groups[key];
    acc = AddTotals(acc, WeightedTotal(cached_weight, r.score));
  }
  return out;
}

// ForwardReplies.Goal (LDBC L3): single-pass reply scan handles forward parent
// references by parking unresolved children until their parent arrives.
struct ReplyRow {
  uint64_t id{0};
  std::optional<uint64_t> parent_id;
  int64_t year{0};
  std::optional<int64_t> score;
};

[[nodiscard]] inline NullableTotals SinglePassForwardReplyTotals(
    std::span<const ReplyRow> rows, int64_t target_parent_year) {
  std::unordered_map<uint64_t, int64_t> seen_parent_year;
  std::unordered_map<uint64_t, NullableTotals> pending_by_parent;
  NullableTotals out;
  for (const auto& r : rows) {
    seen_parent_year[r.id] = r.year;
    if (const auto pit = pending_by_parent.find(r.id);
        pit != pending_by_parent.end()) {
      if (r.year == target_parent_year) {
        out = AddTotals(out, pit->second);
      }
      pending_by_parent.erase(pit);
    }
    if (!r.parent_id.has_value()) {
      continue;
    }
    const uint64_t pid = *r.parent_id;
    const NullableTotals delta = WeightedTotal(1, r.score);
    if (const auto sit = seen_parent_year.find(pid);
        sit != seen_parent_year.end()) {
      if (sit->second == target_parent_year) {
        out = AddTotals(out, delta);
      }
    } else {
      auto& slot = pending_by_parent[pid];
      slot = AddTotals(slot, delta);
    }
  }
  return out;
}

// LateForumWeight.Goal (LDBC L4): group filtered messages by forum_id first and
// probe qualifying forum membership once per distinct forum_id.
[[nodiscard]] inline std::unordered_map<uint64_t, NullableTotals>
ApplyLateForumMembership(
    const std::unordered_map<uint64_t, uint64_t>& forum_mult,
    std::span<const std::pair<uint64_t, std::optional<int64_t>>>
        filtered_messages) {
  std::unordered_map<uint64_t, NullableTotals> preagg;
  for (const auto& [fid, score] : filtered_messages) {
    auto& acc = preagg[fid];
    acc = AddTotals(acc, WeightedTotal(1, score));
  }
  std::unordered_map<uint64_t, NullableTotals> out;
  for (const auto& [fid, st] : preagg) {
    const auto it = forum_mult.find(fid);
    if (it != forum_mult.end() && it->second > 0) {
      out[fid] = NullableTotals{
          .cnt = st.cnt * it->second,
          .nn = st.nn * it->second,
          .sum = st.sum * static_cast<int64_t>(it->second),
      };
    }
  }
  return out;
}

// DirectoryDegree.Goal (LDBC L5): CSR offset directory yields each person's
// exact out-degree via `offsets[p + 1] - offsets[p]`.
[[nodiscard]] inline std::vector<size_t> BuildCsrOffsets(
    std::span<const std::pair<uint64_t, uint64_t>> edges, size_t width) {
  std::vector<size_t> offsets(width + 1, 0);
  for (const auto& [src, dst] : edges) {
    (void)dst;
    if (src < width) {
      ++offsets[static_cast<size_t>(src) + 1];
    }
  }
  for (size_t i = 0; i < width; ++i) {
    offsets[i + 1] += offsets[i];
  }
  return offsets;
}

[[nodiscard]] inline size_t CsrOutDegree(std::span<const size_t> offsets,
                                         size_t person) noexcept {
  if (person + 1 >= offsets.size()) {
    return 0;
  }
  return offsets[person + 1] - offsets[person];
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_SOCIAL_GRAPH_PROOFS_HPP
