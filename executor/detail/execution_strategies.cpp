/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/execution_strategies.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <deque>
#include <functional>
#include <limits>
#include <numeric>
#include <queue>
#include <set>
#include <unordered_set>

namespace tinylamb::execution_strategies {

// ============================================================================
// 1. FrameIndexes
// ============================================================================

void SparseMinimumTable::Build(const std::vector<int64_t>& xs) {
  st.clear();
  if (xs.empty()) {
    return;
  }
  const size_t n = xs.size();
  const auto max_k = static_cast<size_t>(std::bit_width(n));
  st.resize(max_k);
  st[0] = xs;
  for (size_t k = 1; k < max_k; ++k) {
    const size_t len = size_t{1} << k;
    const size_t half = len >> 1;
    st[k].resize(n - len + 1);
    for (size_t i = 0; i + len <= n; ++i) {
      st[k][i] = std::min(st[k - 1][i], st[k - 1][i + half]);
    }
  }
}

int64_t SparseMinimumTable::Query(size_t l, size_t r) const {
  const size_t len = r - l;
  const auto k = static_cast<size_t>(std::bit_width(len) - 1);
  return std::min(st[k][l], st[k][r - (size_t{1} << k)]);
}

void PrefixNullableSum::Build(const std::vector<std::optional<int64_t>>& xs) {
  prefix_sum.assign(xs.size() + 1, 0);
  prefix_count.assign(xs.size() + 1, 0);
  for (size_t i = 0; i < xs.size(); ++i) {
    prefix_sum[i + 1] = prefix_sum[i] + xs[i].value_or(0);
    prefix_count[i + 1] = prefix_count[i] + (xs[i].has_value() ? 1U : 0U);
  }
}

std::pair<int64_t, size_t> PrefixNullableSum::Query(size_t l, size_t r) const {
  return {prefix_sum[r] - prefix_sum[l], prefix_count[r] - prefix_count[l]};
}

std::vector<int64_t> SlidingMinimum(const std::vector<int64_t>& xs, size_t w) {
  std::vector<int64_t> out;
  if (w == 0 || xs.empty()) {
    return out;
  }
  out.reserve(xs.size());
  std::deque<size_t> dq;
  for (size_t i = 0; i < xs.size(); ++i) {
    while (!dq.empty() && dq.front() + w <= i) {
      dq.pop_front();
    }
    while (!dq.empty() && xs[dq.back()] >= xs[i]) {
      dq.pop_back();
    }
    dq.push_back(i);
    out.push_back(xs[dq.front()]);
  }
  return out;
}

size_t TileArithmetic(size_t n, size_t m, size_t i) {
  const size_t q = n / m;
  const size_t r = n % m;
  const size_t big_total = r * (q + 1);
  if (i < big_total) {
    return i / (q + 1);
  }
  return r + (q == 0 ? 0 : (i - big_total) / q);
}

size_t SessionBoundaryCount(const std::vector<int64_t>& ts, int64_t gap) {
  if (ts.empty()) {
    return 0;
  }
  size_t sessions = 1;
  for (size_t i = 1; i < ts.size(); ++i) {
    if (ts[i] - ts[i - 1] > gap) {
      ++sessions;
    }
  }
  return sessions;
}

std::vector<std::pair<size_t, size_t>> PeerRankRuns(
    const std::vector<std::pair<int64_t, size_t>>& runs) {
  std::vector<std::pair<size_t, size_t>> out;
  out.reserve(runs.size());
  size_t rank = 1;
  size_t dense = 1;
  for (const auto& [val, len] : runs) {
    (void)val;
    out.emplace_back(rank, dense);
    rank += len;
    ++dense;
  }
  return out;
}

std::optional<int64_t> ExcludePeerSelect(const std::vector<int64_t>& xs,
                                         size_t a, size_t b, size_t k) {
  const size_t total = xs.size() - (b - a);
  if (k >= total) {
    return std::nullopt;
  }
  return (k < a) ? xs[k] : xs[k + (b - a)];
}

std::string DisjointRangeFold(const std::vector<std::string>& xs, size_t l,
                              size_t r) {
  std::string out;
  for (size_t i = l; i < r && i < xs.size(); ++i) {
    out += xs[i];
  }
  return out;
}

int64_t WaveletQuantile(const std::vector<uint64_t>& xs, size_t l, size_t r,
                        size_t k) {
  std::vector<uint64_t> slice(xs.begin() + static_cast<std::ptrdiff_t>(l),
                              xs.begin() + static_cast<std::ptrdiff_t>(r));
  std::nth_element(slice.begin(),
                   slice.begin() + static_cast<std::ptrdiff_t>(k), slice.end());
  return static_cast<int64_t>(slice[k]);
}

// ============================================================================
// 2. CorrelatedProbes
// ============================================================================

bool RelationalDivision(const std::vector<std::optional<int64_t>>& need,
                        const std::vector<std::optional<int64_t>>& supplied) {
  std::unordered_set<int64_t> req;
  for (const auto& v : need) {
    if (v.has_value()) {
      req.insert(*v);
    }
  }
  if (req.empty()) {
    return true;
  }
  std::unordered_set<int64_t> hit;
  for (const auto& v : supplied) {
    if (v.has_value() && req.contains(*v)) {
      hit.insert(*v);
      if (hit.size() == req.size()) {
        return true;
      }
    }
  }
  return false;
}

std::optional<int64_t> OffsetScalar(
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows,
    uint64_t d, size_t k) {
  size_t seen = 0;
  for (const auto& [key, val] : rows) {
    if (key != d) {
      continue;
    }
    if (seen == k) {
      return val;
    }
    ++seen;
  }
  return std::nullopt;
}

std::optional<int64_t> ExcludingMinimum(const std::vector<int64_t>& xs,
                                        size_t i) {
  if (xs.size() <= 1 || i >= xs.size()) {
    return std::nullopt;
  }
  std::vector<int64_t> pref(xs.size());
  std::vector<int64_t> suff(xs.size());
  pref[0] = xs[0];
  for (size_t j = 1; j < xs.size(); ++j) {
    pref[j] = std::min(pref[j - 1], xs[j]);
  }
  suff.back() = xs.back();
  for (size_t j = xs.size() - 1; j-- > 0;) {
    suff[j] = std::min(suff[j + 1], xs[j]);
  }
  if (i == 0) {
    return suff[1];
  }
  if (i + 1 == xs.size()) {
    return pref[i - 1];
  }
  return std::min(pref[i - 1], suff[i + 1]);
}

size_t CountDistinctStop(const std::vector<std::optional<int64_t>>& xs,
                         size_t k) {
  if (k == 0) {
    return 0;
  }
  std::unordered_set<int64_t> seen;
  seen.reserve(k);
  for (const auto& v : xs) {
    if (!v.has_value()) {
      continue;
    }
    seen.insert(*v);
    if (seen.size() == k) {
      return k;
    }
  }
  return seen.size();
}

void NeWitnessSummary::Build(const std::vector<std::optional<int64_t>>& ys) {
  has_first = false;
  has_second = false;
  for (const auto& y : ys) {
    if (!y.has_value()) {
      continue;
    }
    if (!has_first) {
      has_first = true;
      first = *y;
    } else if (*y != first) {
      has_second = true;
      second = *y;
      return;
    }
  }
}

bool NeWitnessSummary::HasNotEqual(std::optional<int64_t> x) const noexcept {
  if (!x.has_value() || !has_first) {
    return false;
  }
  if (*x != first) {
    return true;
  }
  return has_second;
}

std::vector<std::pair<uint64_t, uint64_t>> OrExistsFilter(
    const std::vector<std::pair<uint64_t, uint64_t>>& xs,
    const std::vector<uint64_t>& a, const std::vector<uint64_t>& b) {
  std::unordered_set<uint64_t> sa(a.begin(), a.end());
  std::unordered_set<uint64_t> sb(b.begin(), b.end());
  std::vector<std::pair<uint64_t, uint64_t>> out;
  for (const auto& p : xs) {
    if (sa.contains(p.first) || sb.contains(p.second)) {
      out.push_back(p);
    }
  }
  return out;
}

std::vector<std::string> PrefixAntichain(std::vector<std::string> prefixes) {
  std::ranges::sort(prefixes);
  prefixes.erase(std::ranges::unique(prefixes).begin(), prefixes.end());
  std::vector<std::string> kept;
  for (std::string& p : prefixes) {
    if (!kept.empty() && p.starts_with(kept.back())) {
      continue;
    }
    kept.push_back(std::move(p));
  }
  return kept;
}

// ============================================================================
// 3. IndexPreimages
// ============================================================================

IntInterval TruncBucketPreimage(int64_t d, int64_t q) {
  if (d <= 0) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  const __int128 prod = static_cast<__int128>(q) * d;
  __int128 lo = 0;
  __int128 hi = 0;
  if (q > 0) {
    lo = prod;
    hi = prod + d - 1;
  } else if (q < 0) {
    lo = prod - d + 1;
    hi = prod;
  } else {
    lo = -(static_cast<__int128>(d) - 1);
    hi = static_cast<__int128>(d) - 1;
  }
  constexpr __int128 kMin = std::numeric_limits<int64_t>::min();
  constexpr __int128 kMax = std::numeric_limits<int64_t>::max();
  if (lo > kMax || hi < kMin) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  return IntInterval{.lo = static_cast<int64_t>(std::max(lo, kMin)),
                     .hi = static_cast<int64_t>(std::min(hi, kMax)),
                     .empty = false};
}

IntInterval RoundedBucketPreimage(int64_t d, int64_t k) {
  if (d <= 0) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  const __int128 half = static_cast<__int128>(d) / 2;
  const __int128 lo = (static_cast<__int128>(k) * d) - half;
  const __int128 hi = (static_cast<__int128>(k) * d) + d - 1 - half;
  constexpr __int128 kMin = std::numeric_limits<int64_t>::min();
  constexpr __int128 kMax = std::numeric_limits<int64_t>::max();
  if (lo > kMax || hi < kMin) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  return IntInterval{.lo = static_cast<int64_t>(std::max(lo, kMin)),
                     .hi = static_cast<int64_t>(std::min(hi, kMax)),
                     .empty = false};
}

int64_t ModularLookup(int64_t m, int64_t inv, int64_t bias, int64_t target) {
  const __int128 diff =
      ((static_cast<__int128>(target) - static_cast<__int128>(bias)) % m + m) %
      m;
  const __int128 inv_n = ((static_cast<__int128>(inv) % m) + m) % m;
  return static_cast<int64_t>((diff * inv_n) % m);
}

IntInterval DescendingBandPreimage(int64_t lo, int64_t hi, int64_t bias,
                                   int64_t d) {
  if (d <= 0 || lo > hi) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  const __int128 x_lo =
      static_cast<__int128>(bias) - ((static_cast<__int128>(hi) + 1) * d) + 1;
  const __int128 x_hi =
      static_cast<__int128>(bias) - (static_cast<__int128>(lo) * d);
  constexpr __int128 kMin = std::numeric_limits<int64_t>::min();
  constexpr __int128 kMax = std::numeric_limits<int64_t>::max();
  if (x_lo > kMax || x_hi < kMin) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  return IntInterval{.lo = static_cast<int64_t>(std::max(x_lo, kMin)),
                     .hi = static_cast<int64_t>(std::min(x_hi, kMax)),
                     .empty = false};
}

IntInterval CheckedDistanceBand(int64_t center, int64_t rad) {
  if (rad < 0) {
    return IntInterval{.lo = 0, .hi = 0, .empty = true};
  }
  const __int128 lo = static_cast<__int128>(center) - rad;
  const __int128 hi = static_cast<__int128>(center) + rad;
  constexpr __int128 kMin = std::numeric_limits<int64_t>::min();
  constexpr __int128 kMax = std::numeric_limits<int64_t>::max();
  return IntInterval{.lo = static_cast<int64_t>(std::max(lo, kMin)),
                     .hi = static_cast<int64_t>(std::min(hi, kMax)),
                     .empty = false};
}

std::pair<IntInterval, IntInterval> DistanceRingPreimage(int64_t origin,
                                                         int64_t l, int64_t h) {
  if (l < 0 || h < l) {
    return {IntInterval{.lo = 0, .hi = 0, .empty = true},
            IntInterval{.lo = 0, .hi = 0, .empty = true}};
  }
  constexpr __int128 kMin = std::numeric_limits<int64_t>::min();
  constexpr __int128 kMax = std::numeric_limits<int64_t>::max();
  auto clamp_iv = [&](__int128 a, __int128 b) -> IntInterval {
    if (a > kMax || b < kMin || a > b) {
      return IntInterval{.lo = 0, .hi = 0, .empty = true};
    }
    return IntInterval{.lo = static_cast<int64_t>(std::max(a, kMin)),
                       .hi = static_cast<int64_t>(std::min(b, kMax)),
                       .empty = false};
  };
  return {clamp_iv(static_cast<__int128>(origin) - h,
                   static_cast<__int128>(origin) - l),
          clamp_iv(static_cast<__int128>(origin) + l,
                   static_cast<__int128>(origin) + h)};
}

std::vector<std::pair<int64_t, int64_t>> CursorRangeFilter(
    const std::vector<std::pair<int64_t, int64_t>>& rows,
    std::pair<int64_t, int64_t> cursor) {
  std::vector<std::pair<int64_t, int64_t>> out;
  for (const auto& r : rows) {
    if (r.first > cursor.first ||
        (r.first == cursor.first && r.second > cursor.second)) {
      out.push_back(r);
    }
  }
  return out;
}

uint64_t MortonEncode2D(uint32_t x, uint32_t y) noexcept {
  auto part1by1 = [](uint64_t v) noexcept -> uint64_t {
    v &= 0x00000000FFFFFFFFULL;
    v = (v ^ (v << 16)) & 0x0000FFFF0000FFFFULL;
    v = (v ^ (v << 8)) & 0x00FF00FF00FF00FFULL;
    v = (v ^ (v << 4)) & 0x0F0F0F0F0F0F0F0FULL;
    v = (v ^ (v << 2)) & 0x3333333333333333ULL;
    v = (v ^ (v << 1)) & 0x5555555555555555ULL;
    return v;
  };
  return part1by1(x) | (part1by1(y) << 1);
}

std::pair<uint64_t, uint64_t> MortonSquareRange(uint32_t x, uint32_t y,
                                                uint32_t shift_bits) noexcept {
  const uint32_t mask = shift_bits >= 32 ? 0U : (~uint32_t{0} << shift_bits);
  const uint64_t base = MortonEncode2D(x & mask, y & mask);
  const uint64_t span = shift_bits >= 32
                            ? ~uint64_t{0}
                            : ((uint64_t{1} << (2U * shift_bits)) - 1);
  return {base, base + span};
}

std::optional<int64_t> PeriodicSeek(int64_t lo, int64_t hi, int64_t period,
                                    int64_t phase) {
  if (period <= 0 || phase < 0 || phase >= period || lo > hi) {
    return std::nullopt;
  }
  const int64_t rem = ((lo % period) + period) % period;
  const int64_t delta = (phase - rem + period) % period;
  const __int128 cand = static_cast<__int128>(lo) + delta;
  if (cand > hi) {
    return std::nullopt;
  }
  return static_cast<int64_t>(cand);
}

// ============================================================================
// 4. OrderedStreams
// ============================================================================

std::pair<size_t, size_t> MergeDiagonal(const std::vector<int64_t>& a,
                                        const std::vector<int64_t>& b,
                                        size_t n) {
  size_t lo = n > b.size() ? n - b.size() : 0;
  size_t hi = std::min(n, a.size());
  while (lo < hi) {
    const size_t mid = lo + ((hi - lo) >> 1);
    const size_t j = n - mid;
    if (j > 0 && mid < a.size() && b[j - 1] > a[mid]) {
      lo = mid + 1;
    } else if (mid > 0 && j < b.size() && a[mid - 1] > b[j]) {
      hi = mid;
    } else {
      return {mid, j};
    }
  }
  return {lo, n - lo};
}

std::vector<std::optional<size_t>> MonotoneProbes(
    const std::vector<int64_t>& ks, const std::vector<int64_t>& rows) {
  std::vector<std::optional<size_t>> out;
  out.reserve(ks.size());
  size_t pos = 0;
  for (int64_t k : ks) {
    if (pos >= rows.size()) {
      out.emplace_back(std::nullopt);
      continue;
    }
    // Exponential (galloping) search from `pos`.
    size_t step = 1;
    while (pos + step < rows.size() && rows[pos + step] < k) {
      pos += step;
      step <<= 1;
    }
    const size_t end = std::min(rows.size(), pos + step + 1);
    auto it =
        std::lower_bound(rows.begin() + static_cast<std::ptrdiff_t>(pos),
                         rows.begin() + static_cast<std::ptrdiff_t>(end), k);
    pos = static_cast<size_t>(it - rows.begin());
    if (pos < rows.size() && rows[pos] == k) {
      out.emplace_back(pos);
    } else {
      out.emplace_back(std::nullopt);
    }
  }
  return out;
}

std::optional<int64_t> WeightedOffset(
    const std::vector<std::pair<int64_t, size_t>>& runs, size_t k) {
  for (const auto& [val, len] : runs) {
    if (k < len) {
      return val;
    }
    k -= len;
  }
  return std::nullopt;
}

std::vector<std::pair<uint64_t, uint64_t>> RangeCoalescing(
    const std::vector<uint64_t>& sorted_keys, uint64_t gap) {
  std::vector<std::pair<uint64_t, uint64_t>> out;
  for (uint64_t k : sorted_keys) {
    if (out.empty() || k > out.back().second + gap) {
      out.emplace_back(k, k);
    } else {
      out.back().second = std::max(out.back().second, k);
    }
  }
  return out;
}

std::vector<std::pair<int64_t, int64_t>> RunIntersection(
    const std::vector<std::pair<int64_t, int64_t>>& a,
    const std::vector<std::pair<int64_t, int64_t>>& b) {
  std::vector<std::pair<int64_t, int64_t>> out;
  size_t i = 0;
  size_t j = 0;
  while (i < a.size() && j < b.size()) {
    const int64_t lo = std::max(a[i].first, b[j].first);
    const int64_t hi = std::min(a[i].second, b[j].second);
    if (lo < hi) {
      out.emplace_back(lo, hi);
    }
    if (a[i].second < b[j].second) {
      ++i;
    } else {
      ++j;
    }
  }
  return out;
}

// ============================================================================
// 5. PairAggregates
// ============================================================================

int64_t AbsoluteDistanceSum(std::vector<int64_t> xs) {
  std::ranges::sort(xs);
  __int128 pref = 0;
  __int128 half_sum = 0;
  for (size_t i = 0; i < xs.size(); ++i) {
    half_sum += (static_cast<__int128>(i) * xs[i]) - pref;
    pref += xs[i];
  }
  return static_cast<int64_t>(half_sum * 2);
}

int64_t SquaredDistanceSum(const std::vector<int64_t>& xs,
                           const std::vector<int64_t>& ys) {
  __int128 sx = 0;
  __int128 sx2 = 0;
  for (int64_t x : xs) {
    sx += x;
    sx2 += static_cast<__int128>(x) * x;
  }
  __int128 sy = 0;
  __int128 sy2 = 0;
  for (int64_t y : ys) {
    sy += y;
    sy2 += static_cast<__int128>(y) * y;
  }
  return static_cast<int64_t>((static_cast<__int128>(ys.size()) * sx2) +
                              (static_cast<__int128>(xs.size()) * sy2) -
                              (2 * sx * sy));
}

uint64_t ResidualInequalityCount(std::vector<int64_t> l, std::vector<int64_t> r,
                                 int64_t t) {
  std::ranges::sort(l);
  std::ranges::sort(r);
  uint64_t cnt = 0;
  size_t j = r.size();
  for (int64_t x : l) {
    while (j > 0 && static_cast<__int128>(x) + r[j - 1] > t) {
      --j;
    }
    cnt += j;
  }
  return cnt;
}

uint64_t SameGroupPairsCount(const std::vector<int64_t>& keys) {
  std::unordered_map<int64_t, uint64_t> freq;
  freq.reserve(keys.size());
  for (int64_t k : keys) {
    ++freq[k];
  }
  uint64_t total = 0;
  for (const auto& [k, c] : freq) {
    (void)k;
    total += (c * (c - 1)) / 2;
  }
  return total;
}

uint64_t ComplementProbeCount(const std::vector<int64_t>& xs,
                              const std::vector<int64_t>& ys, int64_t target) {
  std::unordered_map<int64_t, uint64_t> fy;
  fy.reserve(ys.size());
  for (int64_t y : ys) {
    ++fy[y];
  }
  uint64_t total = 0;
  for (int64_t x : xs) {
    if (auto it = fy.find(target - x); it != fy.end()) {
      total += it->second;
    }
  }
  return total;
}

uint64_t IncreasingTriplesCount(std::vector<int64_t> a,
                                const std::vector<int64_t>& b,
                                std::vector<int64_t> c) {
  std::ranges::sort(a);
  std::ranges::sort(c);
  uint64_t total = 0;
  for (int64_t m : b) {
    const auto left =
        static_cast<uint64_t>(std::ranges::lower_bound(a, m) - a.begin());
    const auto right =
        static_cast<uint64_t>(c.end() - std::ranges::upper_bound(c, m));
    total += left * right;
  }
  return total;
}

uint64_t XorDistanceSum(const std::vector<uint8_t>& xs,
                        const std::vector<uint8_t>& ys) {
  std::array<uint64_t, 8> cx1{};
  std::array<uint64_t, 8> cy1{};
  for (uint8_t x : xs) {
    for (unsigned b = 0; b < 8; ++b) {
      cx1[b] += (x >> b) & 1U;
    }
  }
  for (uint8_t y : ys) {
    for (unsigned b = 0; b < 8; ++b) {
      cy1[b] += (y >> b) & 1U;
    }
  }
  const uint64_t nx = xs.size();
  const uint64_t ny = ys.size();
  uint64_t total = 0;
  for (unsigned b = 0; b < 8; ++b) {
    const uint64_t diff_pairs =
        (cx1[b] * (ny - cy1[b])) + ((nx - cx1[b]) * cy1[b]);
    total += diff_pairs << b;
  }
  return total;
}

uint64_t LayerCakeMinimumSum(std::vector<uint64_t> a, std::vector<uint64_t> b) {
  std::ranges::sort(a);
  std::ranges::sort(b);
  uint64_t total = 0;
  size_t i = 0;
  size_t j = 0;
  while (i < a.size() && j < b.size()) {
    if (a[i] <= b[j]) {
      total += a[i] * static_cast<uint64_t>(b.size() - j);
      ++i;
    } else {
      total += b[j] * static_cast<uint64_t>(a.size() - i);
      ++j;
    }
  }
  return total;
}

// ============================================================================
// 6. PlaneKernels
// ============================================================================

BitPlane8x64 BitPlane8x64::FromBytes(
    const std::array<uint8_t, 64>& vals) noexcept {
  BitPlane8x64 bp;
  for (size_t lane = 0; lane < 64; ++lane) {
    const uint64_t bit = uint64_t{1} << lane;
    for (unsigned b = 0; b < 8; ++b) {
      if (((vals[lane] >> b) & 1U) != 0U) {
        bp.planes[b] |= bit;
      }
    }
  }
  return bp;
}

uint8_t BitPlane8x64::At(size_t lane) const noexcept {
  uint8_t v = 0;
  for (unsigned b = 0; b < 8; ++b) {
    v |= static_cast<uint8_t>(((planes[b] >> lane) & 1ULL) << b);
  }
  return v;
}

uint64_t PlaneParallelLess(const BitPlane8x64& x, const BitPlane8x64& y,
                           uint64_t mask) noexcept {
  uint64_t lt = 0;
  uint64_t eq = mask;
  for (int b = 7; b >= 0; --b) {
    const uint64_t xb = x.planes[static_cast<size_t>(b)];
    const uint64_t yb = y.planes[static_cast<size_t>(b)];
    lt |= eq & (~xb & yb);
    eq &= ~(xb ^ yb);
  }
  return lt;
}

BitPlane8x64 PlaneAdd(const BitPlane8x64& x, const BitPlane8x64& y) noexcept {
  BitPlane8x64 out;
  uint64_t carry = 0;
  for (size_t b = 0; b < 8; ++b) {
    const uint64_t xb = x.planes[b];
    const uint64_t yb = y.planes[b];
    out.planes[b] = xb ^ yb ^ carry;
    carry = (xb & yb) | (carry & (xb ^ yb));
  }
  return out;
}

uint64_t PlaneArgMinMask(const BitPlane8x64& x, uint64_t mask) noexcept {
  uint64_t cur = mask;
  for (int b = 7; b >= 0; --b) {
    const uint64_t zeros = cur & ~x.planes[static_cast<size_t>(b)];
    if (zeros != 0) {
      cur = zeros;
    }
  }
  return cur;
}

uint8_t PlaneRadixSelect(const BitPlane8x64& x, uint64_t mask,
                         size_t k) noexcept {
  uint64_t cur = mask;
  uint8_t ans = 0;
  for (int b = 7; b >= 0; --b) {
    const uint64_t zeros = cur & ~x.planes[static_cast<size_t>(b)];
    const auto zcnt = static_cast<size_t>(std::popcount(zeros));
    if (k < zcnt) {
      cur = zeros;
    } else {
      k -= zcnt;
      cur &= x.planes[static_cast<size_t>(b)];
      ans |= static_cast<uint8_t>(1U << b);
    }
  }
  return ans;
}

uint64_t PlaneDot(const BitPlane8x64& x, const BitPlane8x64& y,
                  uint64_t mask) noexcept {
  uint64_t total = 0;
  for (unsigned i = 0; i < 8; ++i) {
    const uint64_t xm = x.planes[i] & mask;
    if (xm == 0) {
      continue;
    }
    for (unsigned j = 0; j < 8; ++j) {
      total += static_cast<uint64_t>(std::popcount(xm & y.planes[j]))
               << (i + j);
    }
  }
  return total;
}

BitPlane8x64 PlaneMedianOfThree(const BitPlane8x64& x, const BitPlane8x64& y,
                                const BitPlane8x64& z) noexcept {
  const uint64_t xy = PlaneParallelLess(x, y, ~uint64_t{0});
  const uint64_t yz = PlaneParallelLess(y, z, ~uint64_t{0});
  const uint64_t xz = PlaneParallelLess(x, z, ~uint64_t{0});
  const uint64_t pick_y = (xy & yz) | (~xy & ~yz);
  const uint64_t pick_x = (~pick_y) & ((~xy & xz) | (xy & ~xz));
  const uint64_t pick_z = ~(pick_x | pick_y);
  BitPlane8x64 out;
  for (size_t b = 0; b < 8; ++b) {
    out.planes[b] = (x.planes[b] & pick_x) | (y.planes[b] & pick_y) |
                    (z.planes[b] & pick_z);
  }
  return out;
}

uint64_t PlaneRunBoundary(const BitPlane8x64& x, uint8_t v,
                          uint64_t mask) noexcept {
  uint64_t eq = mask;
  for (unsigned b = 0; b < 8; ++b) {
    eq &= (((v >> b) & 1U) != 0U) ? x.planes[b] : ~x.planes[b];
  }
  return eq;
}

// ============================================================================
// 7. EncodedBlocks
// ============================================================================

std::vector<size_t> PatchedRangeFilter(
    int64_t base, int64_t lo, int64_t hi, const std::vector<uint8_t>& deltas,
    const std::unordered_map<size_t, int64_t>& patches) {
  std::vector<size_t> out;
  for (size_t i = 0; i < deltas.size(); ++i) {
    int64_t val = base + static_cast<int64_t>(deltas[i]);
    if (auto it = patches.find(i); it != patches.end()) {
      val = it->second;
    }
    if (lo <= val && val <= hi) {
      out.push_back(i);
    }
  }
  return out;
}

int64_t OverlapDot(const std::vector<std::pair<int64_t, size_t>>& a,
                   const std::vector<std::pair<int64_t, size_t>>& b) {
  size_t i = 0;
  size_t j = 0;
  size_t ra = a.empty() ? 0 : a[0].second;
  size_t rb = b.empty() ? 0 : b[0].second;
  int64_t sum = 0;
  while (i < a.size() && j < b.size()) {
    const size_t take = std::min(ra, rb);
    sum += a[i].first * b[j].first * static_cast<int64_t>(take);
    ra -= take;
    rb -= take;
    if (ra == 0) {
      ++i;
      ra = i < a.size() ? a[i].second : 0;
    }
    if (rb == 0) {
      ++j;
      rb = j < b.size() ? b[j].second : 0;
    }
  }
  return sum;
}

std::vector<int64_t> ApplyRangeEdits(
    std::vector<int64_t> xs,
    const std::vector<std::tuple<size_t, size_t, int64_t>>& edits) {
  std::vector<int64_t> diff(xs.size() + 1, 0);
  for (const auto& [l, r, d] : edits) {
    if (l < r && l < xs.size()) {
      diff[l] += d;
      diff[std::min(r, xs.size())] -= d;
    }
  }
  int64_t acc = 0;
  for (size_t i = 0; i < xs.size(); ++i) {
    acc += diff[i];
    xs[i] += acc;
  }
  return xs;
}

size_t DistinctSeamsCount(
    const std::vector<std::vector<int64_t>>& sorted_pages) {
  size_t total = 0;
  bool have_prev = false;
  int64_t prev_last = 0;
  for (const auto& page : sorted_pages) {
    if (page.empty()) {
      continue;
    }
    size_t page_dist = 1;
    for (size_t i = 1; i < page.size(); ++i) {
      if (page[i] != page[i - 1]) {
        ++page_dist;
      }
    }
    total += page_dist;
    if (have_prev && prev_last == page.front()) {
      --total;
    }
    have_prev = true;
    prev_last = page.back();
  }
  return total;
}

std::vector<int64_t> SecondDifferencesDecode(int64_t p0, int64_t p1,
                                             const std::vector<int64_t>& d2) {
  std::vector<int64_t> out;
  out.reserve(2 + d2.size());
  out.push_back(p0);
  out.push_back(p1);
  int64_t prev = p1;
  int64_t slope = p1 - p0;
  for (int64_t dd : d2) {
    slope += dd;
    prev += slope;
    out.push_back(prev);
  }
  return out;
}

// ============================================================================
// 8. SearchBounds
// ============================================================================

uint64_t CollisionJoinBound(const std::vector<int64_t>& a,
                            const std::vector<int64_t>& b) {
  auto sum_sq = [](const std::vector<int64_t>& v) -> long double {
    std::unordered_map<int64_t, uint64_t> h;
    for (int64_t x : v) {
      ++h[x];
    }
    long double s = 0.0L;
    for (const auto& [k, c] : h) {
      (void)k;
      s += static_cast<long double>(c) * static_cast<long double>(c);
    }
    return s;
  };
  return static_cast<uint64_t>(std::ceil(std::sqrt(sum_sq(a) * sum_sq(b))));
}

uint64_t TriangleAglBound(size_t nr, size_t ns, size_t nt) {
  const long double prod = static_cast<long double>(nr) *
                           static_cast<long double>(ns) *
                           static_cast<long double>(nt);
  return static_cast<uint64_t>(std::floor(std::sqrt(prod)));
}

std::unordered_map<int64_t, size_t> MisraGriesCandidates(
    const std::vector<int64_t>& xs, size_t slots) {
  std::unordered_map<int64_t, size_t> counters;
  if (slots == 0) {
    return counters;
  }
  counters.reserve(slots + 1);
  for (int64_t x : xs) {
    if (auto it = counters.find(x); it != counters.end()) {
      ++it->second;
    } else if (counters.size() < slots) {
      counters.emplace(x, 1);
    } else {
      for (auto it2 = counters.begin(); it2 != counters.end();) {
        if (--it2->second == 0) {
          it2 = counters.erase(it2);
        } else {
          ++it2;
        }
      }
    }
  }
  return counters;
}

size_t CappedJoinCount(const std::vector<int64_t>& a,
                       const std::vector<int64_t>& b, size_t cap) {
  std::unordered_map<int64_t, size_t> hb;
  hb.reserve(b.size());
  for (int64_t x : b) {
    ++hb[x];
  }
  size_t rem = cap;
  for (int64_t x : a) {
    if (rem == 0) {
      break;
    }
    if (auto it = hb.find(x); it != hb.end()) {
      rem -= std::min(rem, it->second);
    }
  }
  return cap - rem;
}

bool PredicateExchangePreferFirst(uint64_t n, uint64_t ca, uint64_t sa,
                                  uint64_t cb, uint64_t sb) noexcept {
  // Cost(A then B) = n*ca + sa*cb; Cost(B then A) = n*cb + sb*ca.
  return (static_cast<__int128>(n) * ca) + (static_cast<__int128>(sa) * cb) <=
         (static_cast<__int128>(n) * cb) + (static_cast<__int128>(sb) * ca);
}

// ============================================================================
// 9. DeltaMaintenance
// ============================================================================

KeyBag DeltaJoinBatch(const KeyBag& a, const KeyBag& b, const KeyBag& da,
                      const KeyBag& db) {
  KeyBag delta;
  std::unordered_set<int64_t> keys;
  for (const auto& [k, _] : da) {
    keys.insert(k);
  }
  for (const auto& [k, _] : db) {
    keys.insert(k);
  }
  auto get = [](const KeyBag& m, int64_t k) -> int64_t {
    auto it = m.find(k);
    return it == m.end() ? 0 : it->second;
  };
  for (int64_t k : keys) {
    const int64_t ak = get(a, k);
    const int64_t bk = get(b, k);
    const int64_t dak = get(da, k);
    const int64_t dbk = get(db, k);
    const int64_t d = (dak * bk) + (ak * dbk) + (dak * dbk);
    if (d != 0) {
      delta[k] = d;
    }
  }
  return delta;
}

KeyBag DeltaOuterPadding(const KeyBag& a, const KeyBag& b, const KeyBag& db) {
  KeyBag delta;
  auto get = [](const KeyBag& m, int64_t k) -> int64_t {
    auto it = m.find(k);
    return it == m.end() ? 0 : it->second;
  };
  for (const auto& [k, dbk] : db) {
    const int64_t ak = get(a, k);
    if (ak == 0) {
      continue;
    }
    const int64_t old_b = get(b, k);
    const int64_t new_b = old_b + dbk;
    const int64_t old_fan = std::max<int64_t>(1, old_b);
    const int64_t new_fan = std::max<int64_t>(1, new_b);
    const int64_t d = ak * (new_fan - old_fan);
    if (d != 0) {
      delta[k] = d;
    }
  }
  return delta;
}

std::optional<int64_t> DeltaMinimumRepair(int64_t old_min, size_t old_min_mult,
                                          const std::vector<int64_t>& dels,
                                          const std::vector<int64_t>& adds) {
  size_t del_min_cnt = 0;
  for (int64_t x : dels) {
    if (x == old_min) {
      ++del_min_cnt;
    }
  }
  std::optional<int64_t> min_add;
  for (int64_t x : adds) {
    if (!min_add || x < *min_add) {
      min_add = x;
    }
  }
  if (min_add && *min_add < old_min) {
    return min_add;
  }
  if (del_min_cnt < old_min_mult) {
    return old_min;
  }
  if (min_add && *min_add == old_min) {
    return old_min;
  }
  return std::nullopt;
}

// ============================================================================
// 10. RecursiveJoins
// ============================================================================

bool MeetFrontiersReachable(size_t n,
                            const std::vector<std::pair<size_t, size_t>>& edges,
                            size_t s, size_t t) {
  if (s >= n || t >= n) {
    return false;
  }
  if (s == t) {
    return true;
  }
  std::vector<std::vector<size_t>> fwd(n);
  std::vector<std::vector<size_t>> bwd(n);
  for (const auto& [u, v] : edges) {
    if (u < n && v < n) {
      fwd[u].push_back(v);
      bwd[v].push_back(u);
    }
  }
  std::vector<uint8_t> vis_f(n, 0);
  std::vector<uint8_t> vis_b(n, 0);
  std::queue<size_t> qf;
  std::queue<size_t> qb;
  vis_f[s] = 1;
  vis_b[t] = 1;
  qf.push(s);
  qb.push(t);
  while (!qf.empty() && !qb.empty()) {
    auto expand = [](std::queue<size_t>& q,
                     const std::vector<std::vector<size_t>>& adj,
                     std::vector<uint8_t>& mine,
                     const std::vector<uint8_t>& other) -> bool {
      const size_t sz = q.size();
      for (size_t i = 0; i < sz; ++i) {
        const size_t u = q.front();
        q.pop();
        for (size_t v : adj[u]) {
          if (other[v] != 0) {
            return true;
          }
          if (mine[v] == 0) {
            mine[v] = 1;
            q.push(v);
          }
        }
      }
      return false;
    };
    if (qf.size() <= qb.size()) {
      if (expand(qf, fwd, vis_f, vis_b)) {
        return true;
      }
    } else {
      if (expand(qb, bwd, vis_b, vis_f)) {
        return true;
      }
    }
  }
  return false;
}

void BinaryLiftingTable::Build(const std::vector<size_t>& next,
                               size_t max_log) {
  up.assign(max_log, std::vector<size_t>(next.size(), 0));
  if (max_log == 0 || next.empty()) {
    return;
  }
  up[0] = next;
  for (size_t k = 1; k < max_log; ++k) {
    for (size_t u = 0; u < next.size(); ++u) {
      up[k][u] = up[k - 1][up[k - 1][u]];
    }
  }
}

size_t BinaryLiftingTable::Jump(size_t s, uint64_t k) const {
  size_t cur = s;
  for (size_t b = 0; b < up.size() && k != 0; ++b) {
    if ((k & (uint64_t{1} << b)) != 0) {
      cur = up[b][cur];
      k ^= (uint64_t{1} << b);
    }
  }
  return cur;
}

uint64_t ButterflyCount(size_t n_left, size_t n_right,
                        const std::vector<std::pair<size_t, size_t>>& edges) {
  std::vector<std::vector<size_t>> adj_l(n_left);
  std::vector<std::vector<size_t>> adj_r(n_right);
  for (const auto& [u, v] : edges) {
    if (u < n_left && v < n_right) {
      adj_l[u].push_back(v);
      adj_r[v].push_back(u);
    }
  }
  for (auto& vec : adj_l) {
    std::ranges::sort(vec);
    vec.erase(std::ranges::unique(vec).begin(), vec.end());
  }
  for (auto& vec : adj_r) {
    std::ranges::sort(vec);
    vec.erase(std::ranges::unique(vec).begin(), vec.end());
  }
  uint64_t butterflies = 0;
  std::vector<uint32_t> wedges(n_left, 0);
  std::vector<size_t> touched;
  for (size_t u = 0; u < n_left; ++u) {
    touched.clear();
    for (size_t v : adj_l[u]) {
      for (size_t u2 : adj_r[v]) {
        if (u2 <= u) {
          continue;
        }
        if (wedges[u2] == 0) {
          touched.push_back(u2);
        }
        ++wedges[u2];
      }
    }
    for (size_t u2 : touched) {
      const uint64_t c = wedges[u2];
      butterflies += (c * (c - 1)) / 2;
      wedges[u2] = 0;
    }
  }
  return butterflies;
}

uint64_t TriangleOrientationCount(
    size_t n, const std::vector<std::pair<size_t, size_t>>& edges) {
  std::vector<std::pair<size_t, size_t>> norm;
  norm.reserve(edges.size());
  for (const auto& [u, v] : edges) {
    if (u < n && v < n && u != v) {
      norm.emplace_back(std::min(u, v), std::max(u, v));
    }
  }
  std::ranges::sort(norm);
  norm.erase(std::ranges::unique(norm).begin(), norm.end());
  std::vector<size_t> deg(n, 0);
  for (const auto& [u, v] : norm) {
    ++deg[u];
    ++deg[v];
  }
  std::vector<std::vector<size_t>> dag(n);
  for (const auto& [u, v] : norm) {
    if (deg[u] < deg[v] || (deg[u] == deg[v] && u < v)) {
      dag[u].push_back(v);
    } else {
      dag[v].push_back(u);
    }
  }
  std::vector<uint8_t> mark(n, 0);
  uint64_t triangles = 0;
  for (size_t u = 0; u < n; ++u) {
    for (size_t v : dag[u]) {
      mark[v] = 1;
    }
    for (size_t v : dag[u]) {
      for (size_t w : dag[v]) {
        triangles += mark[w];
      }
    }
    for (size_t v : dag[u]) {
      mark[v] = 0;
    }
  }
  return triangles;
}

// ============================================================================
// 11. VersionedIndexes
// ============================================================================

std::optional<int64_t> SnapshotAnchorLookup(
    const std::vector<VersionedEntry>& ordered_entries, int64_t key,
    uint64_t t) {
  auto it = std::lower_bound(
      ordered_entries.begin(), ordered_entries.end(), key,
      [](const VersionedEntry& e, int64_t k) { return e.key < k; });
  for (; it != ordered_entries.end() && it->key == key; ++it) {
    if (it->seq <= t) {
      return it->is_tombstone ? std::nullopt
                              : std::optional<int64_t>(it->value);
    }
  }
  return std::nullopt;
}

std::optional<int64_t> RangeTombstoneLookup(
    const std::vector<VersionedEntry>& ordered_entries,
    const std::vector<RangeTombstoneMark>& tombstones, int64_t key,
    uint64_t t) {
  uint64_t max_rt_seq = 0;
  bool has_rt = false;
  for (const auto& rt : tombstones) {
    if (rt.lo <= key && key <= rt.hi && rt.seq <= t) {
      if (!has_rt || rt.seq > max_rt_seq) {
        max_rt_seq = rt.seq;
        has_rt = true;
      }
    }
  }
  auto it = std::lower_bound(
      ordered_entries.begin(), ordered_entries.end(), key,
      [](const VersionedEntry& e, int64_t k) { return e.key < k; });
  for (; it != ordered_entries.end() && it->key == key; ++it) {
    if (it->seq <= t) {
      if (has_rt && max_rt_seq >= it->seq) {
        return std::nullopt;
      }
      return it->is_tombstone ? std::nullopt
                              : std::optional<int64_t>(it->value);
    }
  }
  return std::nullopt;
}

std::vector<VersionedEntry> BottomTombstoneGC(
    const std::vector<VersionedEntry>& ordered_entries,
    uint64_t oldest_active_txn) {
  std::vector<VersionedEntry> out;
  out.reserve(ordered_entries.size());
  for (const auto& e : ordered_entries) {
    if (e.is_tombstone && e.seq <= oldest_active_txn) {
      continue;
    }
    out.push_back(e);
  }
  return out;
}

std::vector<VersionedEntry> DominatedRunCompact(
    const std::vector<VersionedEntry>& ordered_entries,
    uint64_t oldest_active_txn) {
  std::vector<VersionedEntry> out;
  out.reserve(ordered_entries.size());
  bool have_key = false;
  int64_t cur_key = 0;
  bool seen_at_or_below = false;
  for (const auto& e : ordered_entries) {
    if (!have_key || e.key != cur_key) {
      have_key = true;
      cur_key = e.key;
      seen_at_or_below = false;
    }
    if (e.seq <= oldest_active_txn) {
      if (seen_at_or_below) {
        continue;  // Dominated by a newer version already <= oldest_active_txn.
      }
      seen_at_or_below = true;
    }
    out.push_back(e);
  }
  return out;
}

// ============================================================================
// 12. SpillScheduling
// ============================================================================

std::vector<size_t> ExcessFirstSpillVictims(
    const std::vector<uint64_t>& resident_bytes, uint64_t deficit_bytes) {
  std::vector<size_t> victims;
  if (deficit_bytes == 0 || resident_bytes.empty()) {
    return victims;
  }
  std::vector<size_t> order(resident_bytes.size());
  std::iota(order.begin(), order.end(), 0);
  std::ranges::stable_sort(order, [&](size_t a, size_t b) {
    return resident_bytes[a] > resident_bytes[b];
  });
  uint64_t freed = 0;
  for (size_t idx : order) {
    if (resident_bytes[idx] == 0) {
      break;
    }
    victims.push_back(idx);
    if (resident_bytes[idx] >= deficit_bytes - freed) {
      break;
    }
    freed += resident_bytes[idx];
  }
  return victims;
}

std::vector<size_t> LptAssignWorkers(const std::vector<uint64_t>& work_units,
                                     size_t workers) {
  std::vector<size_t> assign(work_units.size(), 0);
  if (work_units.empty() || workers <= 1) {
    return assign;
  }
  std::vector<size_t> order(work_units.size());
  std::iota(order.begin(), order.end(), 0);
  std::ranges::stable_sort(
      order, [&](size_t a, size_t b) { return work_units[a] > work_units[b]; });
  using WorkerLoad = std::pair<uint64_t, size_t>;
  std::priority_queue<WorkerLoad, std::vector<WorkerLoad>, std::greater<>> pq;
  for (size_t w = 0; w < workers; ++w) {
    pq.emplace(0ULL, w);
  }
  for (size_t task : order) {
    auto [load, w] = pq.top();
    pq.pop();
    assign[task] = w;
    pq.emplace(load + work_units[task], w);
  }
  return assign;
}

// ============================================================================
// 13. RankedJoins
// ============================================================================

std::vector<int64_t> RankedJoinTopKSums(const std::vector<int64_t>& desc_a,
                                        const std::vector<int64_t>& desc_b,
                                        size_t k) {
  std::vector<int64_t> out;
  if (k == 0 || desc_a.empty() || desc_b.empty()) {
    return out;
  }
  out.reserve(std::min(k, desc_a.size() * desc_b.size()));
  struct Node {
    int64_t sum;
    size_t i;
    size_t j;
    bool operator<(const Node& o) const noexcept { return sum < o.sum; }
  };
  std::priority_queue<Node> pq;
  const size_t init_rows = std::min(k, desc_a.size());
  for (size_t i = 0; i < init_rows; ++i) {
    pq.push(Node{.sum = desc_a[i] + desc_b[0], .i = i, .j = 0});
  }
  while (!pq.empty() && out.size() < k) {
    Node cur = pq.top();
    pq.pop();
    out.push_back(cur.sum);
    if (cur.j + 1 < desc_b.size()) {
      pq.push(Node{.sum = desc_a[cur.i] + desc_b[cur.j + 1],
                   .i = cur.i,
                   .j = cur.j + 1});
    }
  }
  return out;
}

uint64_t NearPairsCount(std::vector<int64_t> a, std::vector<int64_t> b,
                        int64_t eps) {
  if (eps < 0 || a.empty() || b.empty()) {
    return 0;
  }
  std::ranges::sort(a);
  std::ranges::sort(b);
  uint64_t total = 0;
  size_t lo = 0;
  size_t hi = 0;
  for (int64_t x : a) {
    const __int128 lower =
        static_cast<__int128>(x) - static_cast<__int128>(eps);
    const __int128 upper =
        static_cast<__int128>(x) + static_cast<__int128>(eps);
    while (lo < b.size() && static_cast<__int128>(b[lo]) < lower) {
      ++lo;
    }
    while (hi < b.size() && static_cast<__int128>(b[hi]) <= upper) {
      ++hi;
    }
    if (hi > lo) {
      total += static_cast<uint64_t>(hi - lo);
    }
  }
  return total;
}

// ============================================================================
// 14. Arithmetic & SetTransforms
// ============================================================================

std::pair<int64_t, std::optional<size_t>> CheckedSumFirstOverflow(
    const std::vector<int64_t>& xs) {
  int64_t acc = 0;
  for (size_t i = 0; i < xs.size(); ++i) {
    int64_t next = 0;
    if (__builtin_add_overflow(acc, xs[i], &next)) {
      return {0, i};
    }
    acc = next;
  }
  return {acc, std::nullopt};
}

std::pair<int64_t, uint64_t> RepeatedScoreCheckedAdd(int64_t acc, int64_t v,
                                                     uint64_t reps) noexcept {
  if (reps == 0 || v == 0) {
    return {acc, reps};
  }
  if (v > 0) {
    const auto room = static_cast<uint64_t>(
        static_cast<__int128>(std::numeric_limits<int64_t>::max()) -
        static_cast<__int128>(acc));
    const uint64_t max_steps = room / static_cast<uint64_t>(v);
    const uint64_t steps = std::min(reps, max_steps);
    const __int128 res =
        static_cast<__int128>(acc) +
        (static_cast<__int128>(steps) * static_cast<__int128>(v));
    return {static_cast<int64_t>(res), steps};
  }
  const auto room = static_cast<uint64_t>(
      static_cast<__int128>(acc) -
      static_cast<__int128>(std::numeric_limits<int64_t>::min()));
  const auto abs_v = static_cast<uint64_t>(-static_cast<__int128>(v));
  const uint64_t max_steps = room / abs_v;
  const uint64_t steps = std::min(reps, max_steps);
  const __int128 res =
      static_cast<__int128>(acc) +
      (static_cast<__int128>(steps) * static_cast<__int128>(v));
  return {static_cast<int64_t>(res), steps};
}

bool ExactAverageAtLeast(int64_t grp_sum, uint64_t grp_cnt,
                         int64_t threshold_num,
                         uint64_t threshold_den) noexcept {
  if (grp_cnt == 0 || threshold_den == 0) {
    return false;
  }
  return static_cast<__int128>(grp_sum) *
             static_cast<__int128>(threshold_den) >=
         static_cast<__int128>(threshold_num) * static_cast<__int128>(grp_cnt);
}

// ============================================================================
// 15. EncodedPages & RestartedDeltaRank
// ============================================================================

uint64_t GrayTileDecode(uint64_t g) noexcept {
  g ^= (g >> 32U);
  g ^= (g >> 16U);
  g ^= (g >> 8U);
  g ^= (g >> 4U);
  g ^= (g >> 2U);
  g ^= (g >> 1U);
  return g;
}

size_t RestartedDeltaRankLE(const std::vector<int64_t>& sorted_values,
                            size_t restart_interval, int64_t target) {
  if (sorted_values.empty()) {
    return 0;
  }
  const size_t stride = std::max<size_t>(1, restart_interval);
  const size_t num_blocks = (sorted_values.size() + stride - 1) / stride;
  // Binary search restart anchors for the first block whose anchor > target.
  size_t lo = 0;
  size_t hi = num_blocks;
  while (lo < hi) {
    const size_t mid = lo + ((hi - lo) / 2);
    if (sorted_values[mid * stride] <= target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo == 0) {
    return 0;
  }
  const size_t blk_start = (lo - 1) * stride;
  const size_t blk_end = std::min(blk_start + stride, sorted_values.size());
  size_t pos = blk_start;
  while (pos < blk_end && sorted_values[pos] <= target) {
    ++pos;
  }
  return pos;
}

}  // namespace tinylamb::execution_strategies
