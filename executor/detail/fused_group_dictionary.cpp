/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_group_dictionary.hpp"

#include <array>
#include <atomic>
#include <bit>
#include <limits>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"

namespace tinylamb::relational_detail {
bool BuildGroupDictionary(
    size_t row_count, size_t max_unique,
    const std::function<Status(std::vector<uint64_t>&)>& fill_hashes,
    const std::function<bool(size_t, size_t)>& eq_rows,
    std::vector<uint32_t>* out_subgid, uint32_t* out_count,
    const std::vector<uint8_t>* row_active,
    std::vector<uint32_t>* out_rep_rows) {
  if (row_count == 0) {
    out_subgid->clear();
    *out_count = 0;
    if (out_rep_rows != nullptr) {
      out_rep_rows->clear();
    }
    return true;
  }
  std::vector<uint64_t> row_hashes(row_count);
  if (!fill_hashes(row_hashes).ok()) {
    return false;
  }
  const size_t dict_workers = ParallelWorkerCount(row_count / 16384);
  if (dict_workers > 1 && row_count >= 16384 && max_unique <= 4096) {
    out_subgid->resize(row_count);
    const size_t local_cap =
        std::bit_ceil(std::max<size_t>(32ULL, max_unique * 2ULL));
    const size_t local_mask = local_cap - 1;
    std::vector<std::vector<uint32_t>> local_reps(dict_workers);
    std::vector<std::vector<uint64_t>> local_hashes(dict_workers);
    std::atomic<bool> overflowed{false};
    const Status dict_status = RunParallelTasks(dict_workers, [&](size_t w) {
      const size_t begin = (row_count * w) / dict_workers;
      const size_t end = (row_count * (w + 1)) / dict_workers;
      std::vector<uint32_t> buckets(local_cap,
                                    std::numeric_limits<uint32_t>::max());
      auto& l_reps = local_reps[w];
      auto& l_hashes = local_hashes[w];
      l_reps.reserve(std::min<size_t>(max_unique, 1024ULL));
      l_hashes.reserve(std::min<size_t>(max_unique, 1024ULL));
      for (size_t r = begin; r < end; ++r) {
        if (row_active != nullptr && (*row_active)[r] == 0) {
          (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
          continue;
        }
        const uint64_t h = row_hashes[r];
        size_t slot = static_cast<size_t>(h ^ (h >> 32)) & local_mask;
        bool found = false;
        uint32_t found_id = 0;
        while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
          const uint32_t id = buckets[slot];
          if (l_hashes[id] == h && eq_rows(l_reps[id], r)) {
            found = true;
            found_id = id;
            break;
          }
          slot = (slot + 1) & local_mask;
        }
        if (!found) {
          if (l_reps.size() >= max_unique) {
            overflowed.store(true, std::memory_order_relaxed);
            return;
          }
          found_id = static_cast<uint32_t>(l_reps.size());
          l_reps.push_back(static_cast<uint32_t>(r));
          l_hashes.push_back(h);
          buckets[slot] = found_id;
        }
        (*out_subgid)[r] = found_id;
      }
    });
    if (!dict_status.ok()) {
      // A worker exception leaves some local_reps incomplete; never build
      // remaps over partial results.
      out_subgid->clear();
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return false;
    }
    if (overflowed.load(std::memory_order_relaxed)) {
      out_subgid->clear();
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return false;
    }
    size_t total_local = 0;
    for (const auto& lr : local_reps) {
      total_local += lr.size();
    }
    const size_t g_cap =
        std::bit_ceil(std::max<size_t>(32ULL, total_local * 2ULL));
    const size_t g_mask = g_cap - 1;
    std::vector<uint32_t> g_buckets(g_cap,
                                    std::numeric_limits<uint32_t>::max());
    std::vector<uint32_t> rep_rows;
    std::vector<uint64_t> rep_hashes;
    rep_rows.reserve(std::min(max_unique, total_local));
    rep_hashes.reserve(std::min(max_unique, total_local));
    std::vector<std::vector<uint32_t>> remaps(dict_workers);
    for (size_t w = 0; w < dict_workers; ++w) {
      const auto& lr = local_reps[w];
      const auto& lh = local_hashes[w];
      auto& rm = remaps[w];
      rm.resize(lr.size());
      for (size_t lid = 0; lid < lr.size(); ++lid) {
        const uint32_t rr = lr[lid];
        const uint64_t h = lh[lid];
        size_t slot = static_cast<size_t>(h ^ (h >> 32)) & g_mask;
        bool found = false;
        uint32_t gid = 0;
        while (g_buckets[slot] != std::numeric_limits<uint32_t>::max()) {
          const uint32_t cand = g_buckets[slot];
          if (rep_hashes[cand] == h && eq_rows(rep_rows[cand], rr)) {
            found = true;
            gid = cand;
            break;
          }
          slot = (slot + 1) & g_mask;
        }
        if (!found) {
          if (rep_rows.size() >= max_unique) {
            out_subgid->clear();
            *out_count = 0;
            if (out_rep_rows != nullptr) {
              out_rep_rows->clear();
            }
            return false;
          }
          gid = static_cast<uint32_t>(rep_rows.size());
          rep_rows.push_back(rr);
          rep_hashes.push_back(h);
          g_buckets[slot] = gid;
        }
        rm[lid] = gid;
      }
    }
    const Status remap_status = RunParallelTasks(dict_workers, [&](size_t w) {
      const size_t begin = (row_count * w) / dict_workers;
      const size_t end = (row_count * (w + 1)) / dict_workers;
      const auto& rm = remaps[w];
      for (size_t r = begin; r < end; ++r) {
        const uint32_t lid = (*out_subgid)[r];
        if (lid != std::numeric_limits<uint32_t>::max()) {
          (*out_subgid)[r] = rm[lid];
        }
      }
    });
    if (!remap_status.ok()) {
      // A partial remap would mix local subgroupIds with global ones while
      // out_count still claims a complete global dictionary.
      out_subgid->clear();
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return false;
    }
    *out_count = static_cast<uint32_t>(rep_rows.size());
    if (out_rep_rows != nullptr) {
      *out_rep_rows = std::move(rep_rows);
    }
    return true;
  }
  if (dict_workers > 1 && row_count >= 16384 &&
      max_unique == std::numeric_limits<size_t>::max()) {
    constexpr size_t kDictParts = 64;
    const auto mix64 = [](uint64_t x) noexcept -> uint64_t {
      x ^= x >> 30;
      x *= 0xbf58476d1ce4e5b9ULL;
      x ^= x >> 27;
      x *= 0x94d049bb133111ebULL;
      return x ^ (x >> 31);
    };
    out_subgid->resize(row_count);
    std::vector<std::array<uint32_t, kDictParts>> worker_counts(dict_workers);
    const Status count_status = RunParallelTasks(dict_workers, [&](size_t w) {
      auto& counts = worker_counts[w];
      counts.fill(0);
      const size_t begin = (row_count * w) / dict_workers;
      const size_t end = (row_count * (w + 1)) / dict_workers;
      for (size_t r = begin; r < end; ++r) {
        if (row_active != nullptr && (*row_active)[r] == 0) {
          (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
          continue;
        }
        const auto p = static_cast<size_t>(mix64(row_hashes[r]) >> 58);
        ++counts[p];
      }
    });
    if (!count_status.ok()) {
      // Partial counts would make part_sizes/active_total disagree with the
      // rows actually written by the partitioning pass below.
      out_subgid->clear();
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return false;
    }
    std::array<uint32_t, kDictParts> part_sizes{};
    size_t active_total = 0;
    for (size_t p = 0; p < kDictParts; ++p) {
      uint32_t sum = 0;
      for (size_t w = 0; w < dict_workers; ++w) {
        sum += worker_counts[w][p];
      }
      part_sizes[p] = sum;
      active_total += sum;
    }
    if (active_total == 0) {
      *out_count = 0;
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
      }
      return true;
    }
    uint32_t max_part_size = 0;
    for (size_t p = 0; p < kDictParts; ++p) {
      max_part_size = std::max(max_part_size, part_sizes[p]);
    }
    if (max_part_size <= (active_total / 2) + 64) {
      std::array<size_t, kDictParts> part_offsets{};
      std::vector<std::array<size_t, kDictParts>> worker_offsets(dict_workers);
      size_t running = 0;
      for (size_t p = 0; p < kDictParts; ++p) {
        part_offsets[p] = running;
        for (size_t w = 0; w < dict_workers; ++w) {
          worker_offsets[w][p] = running;
          running += worker_counts[w][p];
        }
      }
      std::vector<uint32_t> part_rows(active_total);
      const Status scatter_status =
          RunParallelTasks(dict_workers, [&](size_t w) {
            auto cursors = worker_offsets[w];
            const size_t begin = (row_count * w) / dict_workers;
            const size_t end = (row_count * (w + 1)) / dict_workers;
            for (size_t r = begin; r < end; ++r) {
              if (row_active != nullptr && (*row_active)[r] == 0) {
                continue;
              }
              const auto p = static_cast<size_t>(mix64(row_hashes[r]) >> 58);
              part_rows[cursors[p]++] = static_cast<uint32_t>(r);
            }
          });
      if (!scatter_status.ok()) {
        // An incomplete scatter leaves zero-filled slots in part_rows;
        // consuming them would process row 0 repeatedly.
        out_subgid->clear();
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return false;
      }
      std::vector<uint32_t> part_local_ids(active_total);
      std::array<std::vector<uint32_t>, kDictParts> part_rep_rows;
      std::atomic<size_t> next_p{0};
      const Status part_status = RunParallelTasks(dict_workers, [&](size_t) {
        size_t p = 0;
        while ((p = next_p.fetch_add(1, std::memory_order_relaxed)) <
               kDictParts) {
          const uint32_t p_cnt = part_sizes[p];
          if (p_cnt == 0) {
            continue;
          }
          const size_t p_begin = part_offsets[p];
          const size_t p_cap =
              std::bit_ceil(std::max<size_t>(16ULL, p_cnt * 2ULL));
          const size_t p_mask = p_cap - 1;
          std::vector<uint32_t> buckets(p_cap,
                                        std::numeric_limits<uint32_t>::max());
          auto& preps = part_rep_rows[p];
          preps.reserve(std::min<size_t>(p_cnt, 4096ULL));
          std::vector<uint64_t> phashes;
          phashes.reserve(std::min<size_t>(p_cnt, 4096ULL));
          for (size_t idx = p_begin; idx < p_begin + p_cnt; ++idx) {
            const uint32_t r = part_rows[idx];
            const uint64_t h = row_hashes[r];
            size_t slot = static_cast<size_t>(mix64(h)) & p_mask;
            bool found = false;
            uint32_t lid = 0;
            while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
              const uint32_t id = buckets[slot];
              if (phashes[id] == h && eq_rows(preps[id], r)) {
                found = true;
                lid = id;
                break;
              }
              slot = (slot + 1) & p_mask;
            }
            if (!found) {
              lid = static_cast<uint32_t>(preps.size());
              preps.push_back(r);
              phashes.push_back(h);
              buckets[slot] = lid;
            }
            part_local_ids[idx] = lid;
          }
        }
      });
      if (!part_status.ok()) {
        // A worker exception can leave some partitions' representative rows
        // empty while part_local_ids still references them.
        out_subgid->clear();
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return false;
      }
      std::array<uint32_t, kDictParts> base_ids{};
      uint32_t total_unique = 0;
      for (size_t p = 0; p < kDictParts; ++p) {
        base_ids[p] = total_unique;
        total_unique += static_cast<uint32_t>(part_rep_rows[p].size());
      }
      if (out_rep_rows != nullptr) {
        out_rep_rows->clear();
        out_rep_rows->reserve(total_unique);
        for (size_t p = 0; p < kDictParts; ++p) {
          out_rep_rows->insert(out_rep_rows->end(), part_rep_rows[p].begin(),
                               part_rep_rows[p].end());
        }
      }
      std::atomic<size_t> next_wp{0};
      const Status write_status = RunParallelTasks(dict_workers, [&](size_t) {
        size_t p = 0;
        while ((p = next_wp.fetch_add(1, std::memory_order_relaxed)) <
               kDictParts) {
          const uint32_t p_cnt = part_sizes[p];
          const size_t p_begin = part_offsets[p];
          const uint32_t bid = base_ids[p];
          for (size_t idx = p_begin; idx < p_begin + p_cnt; ++idx) {
            (*out_subgid)[part_rows[idx]] = bid + part_local_ids[idx];
          }
        }
      });
      if (!write_status.ok()) {
        out_subgid->clear();
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return false;
      }
      *out_count = total_unique;
      return true;
    }
  }
  const size_t cap = std::bit_ceil(std::max<size_t>(16ULL, row_count * 2ULL));
  const size_t mask = cap - 1;
  std::vector<uint32_t> buckets(cap, std::numeric_limits<uint32_t>::max());
  std::vector<uint32_t> rep_rows;
  std::vector<uint64_t> rep_hashes;
  const size_t reserve_est = std::min<size_t>(row_count, 65536ULL);
  rep_rows.reserve(reserve_est);
  rep_hashes.reserve(reserve_est);
  out_subgid->resize(row_count);
  for (size_t r = 0; r < row_count; ++r) {
    if (row_active != nullptr && (*row_active)[r] == 0) {
      (*out_subgid)[r] = std::numeric_limits<uint32_t>::max();
      continue;
    }
    const uint64_t h = row_hashes[r];
    size_t slot = static_cast<size_t>(h ^ (h >> 32)) & mask;
    bool found = false;
    uint32_t found_id = 0;
    while (buckets[slot] != std::numeric_limits<uint32_t>::max()) {
      const uint32_t id = buckets[slot];
      if (rep_hashes[id] == h && eq_rows(rep_rows[id], r)) {
        found = true;
        found_id = id;
        break;
      }
      slot = (slot + 1) & mask;
    }
    if (!found) {
      if (rep_rows.size() >= max_unique) {
        out_subgid->clear();
        *out_count = 0;
        if (out_rep_rows != nullptr) {
          out_rep_rows->clear();
        }
        return false;
      }
      found_id = static_cast<uint32_t>(rep_rows.size());
      rep_rows.push_back(static_cast<uint32_t>(r));
      rep_hashes.push_back(h);
      buckets[slot] = found_id;
    }
    (*out_subgid)[r] = found_id;
  }
  *out_count = static_cast<uint32_t>(rep_rows.size());
  if (out_rep_rows != nullptr) {
    *out_rep_rows = std::move(rep_rows);
  }
  return true;
}
}  // namespace tinylamb::relational_detail
