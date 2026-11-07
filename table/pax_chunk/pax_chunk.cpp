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

#include "table/pax_chunk/pax_chunk.hpp"

#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {

// ---------------------------------------------------------------------------
// Varchar predicate pushdown (docs/12 roadmap: native eq + RANGE over
// CONST/PLAIN/DICT/FSST). Comparison domain is the DECODED value: NULL cells
// are the empty string, exactly like ReadColumnValue/ReadRow report them.
// ---------------------------------------------------------------------------
namespace {

// Shared predicate state for one count call.
struct StrPredicate {
  StrScanOp op{StrScanOp::kEq};
  bool range{false};  // true = evaluate MatchesRange, else Matches
  std::string needle;
  std::string lo, hi;
  bool has_lo{false}, has_hi{false};

  bool Matches(std::string_view v) const {
    switch (op) {
      case StrScanOp::kEq:
        return v == needle;
      case StrScanOp::kNe:
        return v != needle;
      case StrScanOp::kLt:
        return v < needle;
      case StrScanOp::kLe:
        return v <= needle;
      case StrScanOp::kGt:
        return v > needle;
      case StrScanOp::kGe:
        return v >= needle;
      case StrScanOp::kPrefix:
        return v.size() >= needle.size() &&
               v.compare(0, needle.size(), needle) == 0;
    }
    return false;
  }
  // Half-open [lo, hi) with either side unbounded.
  bool MatchesRange(std::string_view v) const {
    if (has_lo && !(v >= std::string_view(lo))) return false;
    if (has_hi && !(v < std::string_view(hi))) return false;
    return true;
  }
  bool Eval(std::string_view v) const {
    return range ? MatchesRange(v) : Matches(v);
  }
};

// The exclusive upper bound of a prefix set, in lexicographic (unsigned
// byte) order: the needle with its last non-0xFF byte incremented and the
// rest dropped. "abc" -> "abd", "ab\xff" -> "ac". Returns false when the
// needle is all 0xFF -- then the prefix set runs to the end of the order
// and has no finite upper bound.
bool PrefixUpperBound(const std::string& needle, std::string* out) {
  size_t i = needle.size();
  while (i > 0 && static_cast<uint8_t>(needle[i - 1]) == 0xFFu) --i;
  if (i == 0) return false;
  out->assign(needle, 0, i);
  (*out)[i - 1] = static_cast<char>(static_cast<uint8_t>((*out)[i - 1]) + 1u);
  return true;
}

// One region's match count. nullopt = the fast path hit a structural
// surprise (hostile/truncated extent); the caller re-runs the row through
// the bounds-checked generic decoder instead of trusting the walk.
std::optional<uint64_t> VarcharRegionMatchCount(const char* r, Scheme s,
                                                uint32_t param, uint32_t n,
                                                const char* region_end,
                                                const StrPredicate& p) {
  if (n == 0) return uint64_t{0};
  if (region_end == nullptr || region_end <= r) return std::nullopt;
  switch (s) {
    case Scheme::kConst: {
      if (r + 4 > region_end) return std::nullopt;
      const uint32_t len = ReadBE32(r);
      if (r + 4 + len > region_end) return std::nullopt;
      return p.Eval(std::string_view(r + 4, len)) ? uint64_t{n} : uint64_t{0};
    }
    case Scheme::kPlain: {
      const char* q = r;
      uint64_t cnt = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (q + 4 > region_end) return std::nullopt;
        const uint32_t len = ReadBE32(q);
        if (q + 4 + len > region_end) return std::nullopt;
        if (p.Eval(std::string_view(q + 4, len))) ++cnt;
        q += 4 + len;
      }
      return cnt;
    }
    case Scheme::kDict: {
      // The 2-byte code-count header must be inside the region BEFORE the
      // read (the kConst/kFsst arms above and every sibling kDict decoder
      // gate the same way; a 1-byte cell at the data-area end would
      // otherwise read its ncodes from the footer's first byte).
      if (r + 2 > region_end) return std::nullopt;
      const uint16_t ncodes = ReadBE16(r);
      const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
      const size_t dict_base =
          2 + (static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8;
      if (r + dict_base > region_end) return std::nullopt;
      if (ncodes == 0) {
        // Every code decodes to "" (matches VarcharRegionAt's clamp).
        return p.Eval(std::string_view()) ? uint64_t{n} : uint64_t{0};
      }
      const char* dict = r + dict_base;
      std::vector<uint8_t> code_pass(static_cast<size_t>(ncodes) + 1, 0);
      const char* d = dict;
      for (uint16_t c = 0; c < ncodes; ++c) {
        if (d + 4 > region_end) return std::nullopt;
        const uint32_t len = ReadBE32(d);
        if (d + 4 + len > region_end) return std::nullopt;
        code_pass[c] = p.Eval(std::string_view(d + 4, len)) ? 1 : 0;
        d += 4 + len;
      }
      // Codes >= ncodes decode to "" (clamp bucket), same as the decoder.
      code_pass[ncodes] = p.Eval(std::string_view()) ? 1 : 0;
      WordBitReader br(r + 2, r + dict_base);
      uint64_t cnt = 0;
      for (uint32_t i = 0; i < n; ++i) {
        const uint64_t code = br.get(cw);
        cnt += code_pass[code < ncodes ? code : ncodes];
      }
      return cnt;
    }
    case Scheme::kOpDict: {
      // Sorted dictionary: the predicate maps to a contiguous rank window.
      // One linear walk materializes the entry views (pointer arithmetic
      // only -- the comparisons the kDict pass table spends above happen as
      // 2 x log2(d) binary-search steps instead), then the rows scan as a
      // branch-free window test. Gates mirror the kDict arm above.
      if (r + 2 > region_end) return std::nullopt;
      const uint16_t ncodes = ReadBE16(r);
      const int cw = BitsNeeded(ncodes > 0 ? ncodes - 1 : 0);
      const size_t dict_base =
          2 + (static_cast<size_t>(n) * static_cast<size_t>(cw) + 7) / 8;
      if (r + dict_base > region_end) return std::nullopt;
      if (ncodes == 0) {
        // Every code decodes to "" (matches VarcharRegionAt's clamp).
        return p.Eval(std::string_view()) ? uint64_t{n} : uint64_t{0};
      }
      std::vector<std::string_view> entries;
      entries.reserve(ncodes);
      const char* d = r + dict_base;
      for (uint16_t c = 0; c < ncodes; ++c) {
        if (d + 4 > region_end) return std::nullopt;
        const uint32_t len = ReadBE32(d);
        if (d + 4 + len > region_end) return std::nullopt;
        entries.emplace_back(d + 4, len);
        d += 4 + len;
      }
      // lb/ub over the ascending entries (char_traits compares unsigned
      // bytes, the same order StrPredicate and the encoder sort in).
      auto lb = [&](std::string_view key) {
        uint16_t a = 0, b = ncodes;
        while (a < b) {
          const uint16_t m = static_cast<uint16_t>(a + (b - a) / 2);
          if (entries[m] < key) {
            a = static_cast<uint16_t>(m + 1);
          } else {
            b = m;
          }
        }
        return a;
      };
      auto ub = [&](std::string_view key) {
        uint16_t a = 0, b = ncodes;
        while (a < b) {
          const uint16_t m = static_cast<uint16_t>(a + (b - a) / 2);
          if (!(key < entries[m])) {
            a = static_cast<uint16_t>(m + 1);
          } else {
            b = m;
          }
        }
        return a;
      };
      uint64_t wlo = 0, whi = ncodes;
      if (p.range) {
        // Half-open [lo, hi): codes of values >= lo start at lb(lo); values
        // < hi end at lb(hi).
        if (p.has_lo) wlo = lb(p.lo);
        if (p.has_hi) whi = lb(p.hi);
      } else if (p.op == StrScanOp::kNe) {
        // Complement of equality: count the rows equal to the needle and
        // subtract. A row equals needle when its code is the needle's rank
        // (present in the dict) or when it is a clamp code decoding to ""
        // and the needle is "".
        const uint16_t c = lb(p.needle);
        const bool has_eq_code = c < ncodes && entries[c] == p.needle;
        const bool clamp_eq = p.needle.empty();
        uint64_t eq = 0;
        WordBitReader br(r + 2, r + dict_base);
        for (uint32_t i = 0; i < n; ++i) {
          const uint64_t code = br.get(cw);
          eq += (has_eq_code && code == c) || (clamp_eq && code >= ncodes) ? 1
                                                                           : 0;
        }
        return static_cast<uint64_t>(n) - eq;
      } else {
        switch (p.op) {
          case StrScanOp::kEq: {
            const uint16_t c = lb(p.needle);
            wlo = c;
            whi = (c < ncodes && entries[c] == p.needle)
                      ? static_cast<uint16_t>(c + 1)
                      : c;
            break;
          }
          case StrScanOp::kLt:
            whi = lb(p.needle);
            break;
          case StrScanOp::kLe:
            whi = ub(p.needle);
            break;
          case StrScanOp::kGe:
            wlo = lb(p.needle);
            break;
          case StrScanOp::kGt:
            wlo = ub(p.needle);
            break;
          case StrScanOp::kNe:
            break;  // handled above
          case StrScanOp::kPrefix: {
            // The SORTED dictionary turns a prefix into a contiguous rank
            // window, exactly like the Front-Code's sorted region: the
            // entries in [needle, upper) are the ones that start with the
            // needle.
            wlo = lb(p.needle);
            std::string upper;
            whi = PrefixUpperBound(p.needle, &upper) ? lb(upper) : ncodes;
            break;
          }
        }
      }
      // Clamp codes decode as "": hit iff "" takes a rank inside the window.
      const uint64_t empty_rank = lb(std::string_view());
      const bool clamp_pass = empty_rank >= wlo && empty_rank < whi;
      const bool exact_width = (1ULL << cw) == ncodes;
      WordBitReader br(r + 2, r + dict_base);
      uint64_t cnt = 0;
      for (uint32_t i = 0; i < n; ++i) {
        const uint64_t code = br.get(cw);
        // Nested ternary form tripped readability-avoid-nested-conditional-
        // operator; the folded form is equivalent: an exact code space has
        // no clamp bucket, and in-window codes satisfy the predicate.
        const bool pass_window = exact_width || code < ncodes;
        const bool in_window = code - wlo < whi - wlo;
        const bool ok = pass_window ? in_window : clamp_pass;
        cnt += ok ? 1 : 0;
      }
      return cnt;
    }
    case Scheme::kFsst: {
      if (r + 6 > region_end) return std::nullopt;
      const uint16_t nsym = ReadBE16(r);
      // Same cap as VarcharRegionAt (which fails closed to "" above this):
      // accepting bigger symbol tables here would make the pushdown count
      // rows the decode path reports as "".
      if (nsym > 256) return std::nullopt;
      const char* sp = r + 2;
      const uint32_t tsl = ReadBE32(sp);
      sp += 4;
      std::vector<std::string_view> sym(nsym);
      for (uint16_t c = 0; c < nsym; ++c) {
        if (sp + 1 > region_end) return std::nullopt;
        const uint8_t sl = static_cast<uint8_t>(*sp++);
        if (sp + sl > region_end) return std::nullopt;
        sym[c] = std::string_view(sp, sl);
        sp += sl;
      }
      const char* otable = sp;
      if (otable + static_cast<size_t>(n) * 4 > region_end) return std::nullopt;
      const char* stream = otable + static_cast<size_t>(n) * 4;
      if (stream + tsl > region_end) return std::nullopt;
      std::string scratch;
      uint64_t cnt = 0;
      for (uint32_t i = 0; i < n; ++i) {
        const uint32_t code_off = ReadBE32(otable + static_cast<size_t>(i) * 4);
        const uint32_t next_off =
            (i + 1 < n) ? ReadBE32(otable + (static_cast<size_t>(i) + 1) * 4)
                        : tsl;
        if (code_off > next_off || next_off > tsl) return std::nullopt;
        scratch.clear();
        const uint8_t* cp = reinterpret_cast<const uint8_t*>(stream + code_off);
        const uint8_t* cend =
            reinterpret_cast<const uint8_t*>(stream + next_off);
        while (cp < cend) {
          const uint8_t code = *cp++;
          if (code == 0) {
            if (cp >= cend) break;
            scratch.push_back(static_cast<char>(*cp++));
          } else if (code <= nsym) {
            scratch.append(sym[code - 1]);
          }
        }
        if (p.Eval(scratch)) ++cnt;
      }
      return cnt;
    }
    case Scheme::kFrontCode: {
      const uint32_t re = param & 0x7FFFu;
      if (re == 0 || r + 6 > region_end) return std::nullopt;
      const uint32_t nrestart = ReadBE16(r);
      const uint32_t stream_len = ReadBE32(r + 2);
      const char* rtable = r + 6;
      if (rtable + static_cast<size_t>(nrestart) * 4 > region_end)
        return std::nullopt;
      const char* stream = rtable + static_cast<size_t>(nrestart) * 4;
      if (stream + stream_len > region_end) return std::nullopt;
      const char* send = stream + stream_len;

      // Sorted regions (encoder-detected, param bit 15): every op is a
      // count between two compressed-domain binary searches. value(idx)
      // replays at most restart_every chained entries, so a count never
      // materializes the column -- the flagship compressed-processing
      // property no other varchar scheme has (FSST/DICT codes are not
      // order preserving, PLAIN must walk every row).
      if ((param & 0x8000u) != 0) {
        std::string scratch;
        auto value_at = [&](uint32_t idx) -> bool {
          const uint32_t block = idx / re;
          if (block >= nrestart) return false;
          const uint32_t ro = ReadBE32(rtable + static_cast<size_t>(block) * 4);
          if (ro > stream_len) return false;
          scratch.clear();
          const char* q = stream + ro;
          for (uint32_t i = block * re; i <= idx; ++i) {
            if (q + 4 > send) return false;
            const uint32_t plen = ReadBE16(q);
            const uint32_t slen = ReadBE16(q + 2);
            if (plen > scratch.size() || q + 4 + slen > send) return false;
            scratch.resize(plen);
            scratch.append(q + 4, slen);
            q += 4 + slen;
          }
          return true;
        };
        // First index with value >= key (false) / > key (true). -1 flags a
        // structural surprise; the caller re-runs the row through the
        // bounds-checked generic decoder instead of trusting the walk.
        auto seek = [&](const std::string& key, bool excl) -> int64_t {
          uint32_t lo = 0, hi = n;
          while (lo < hi) {
            const uint32_t mid = lo + (hi - lo) / 2;
            if (!value_at(mid)) return -1;
            const int c = scratch.compare(key);
            if (c < 0 || (excl && c == 0)) {
              lo = mid + 1;
            } else {
              hi = mid;
            }
          }
          return static_cast<int64_t>(lo);
        };
        int64_t cnt;
        if (p.range) {
          const int64_t a = p.has_lo ? seek(p.lo, false) : 0;
          if (a < 0) return std::nullopt;
          const int64_t b =
              p.has_hi ? seek(p.hi, false) : static_cast<int64_t>(n);
          if (b < 0) return std::nullopt;
          cnt = std::max<int64_t>(0, b - a);
        } else {
          switch (p.op) {
            case StrScanOp::kEq: {
              const int64_t a = seek(p.needle, false);
              if (a < 0) return std::nullopt;
              const int64_t b = seek(p.needle, true);
              if (b < 0) return std::nullopt;
              cnt = b - a;
              break;
            }
            case StrScanOp::kNe: {
              const int64_t a = seek(p.needle, false);
              if (a < 0) return std::nullopt;
              const int64_t b = seek(p.needle, true);
              if (b < 0) return std::nullopt;
              cnt = static_cast<int64_t>(n) - (b - a);
              break;
            }
            case StrScanOp::kLt:
              cnt = seek(p.needle, false);
              break;
            case StrScanOp::kLe:
              cnt = seek(p.needle, true);
              break;
            case StrScanOp::kGt: {
              const int64_t b = seek(p.needle, true);
              if (b < 0) return std::nullopt;
              cnt = static_cast<int64_t>(n) - b;
              break;
            }
            case StrScanOp::kPrefix: {
              // The prefix set is the contiguous run
              // [lower_bound(needle), lower_bound(needle')) in the sorted
              // region, where needle' is the needle's lexicographic upper
              // bound (the last non-0xFF byte incremented). Two binary
              // searches, no materialization.
              const int64_t a = seek(p.needle, false);
              if (a < 0) return std::nullopt;
              int64_t b = static_cast<int64_t>(n);
              std::string upper;
              if (PrefixUpperBound(p.needle, &upper)) {
                b = seek(upper, false);
                if (b < 0) return std::nullopt;
              }
              cnt = b - a;
              break;
            }
            default: {  // kGe
              const int64_t a = seek(p.needle, false);
              if (a < 0) return std::nullopt;
              cnt = static_cast<int64_t>(n) - a;
              break;
            }
          }
          if (cnt < 0) return std::nullopt;
        }
        return static_cast<uint64_t>(cnt);
      }

      // Unsorted: prefix-chained sequential walk. Each row extends the
      // previous value in place (resize to plen + append suffix) -- no
      // per-row string construction, unlike the generic decode path.
      std::string cur;
      uint64_t cnt = 0;
      const char* q = stream;
      for (uint32_t i = 0; i < n; ++i) {
        if (q + 4 > send) return std::nullopt;
        const uint32_t plen = ReadBE16(q);
        const uint32_t slen = ReadBE16(q + 2);
        if (plen > cur.size() || q + 4 + slen > send) return std::nullopt;
        cur.resize(plen);
        cur.append(q + 4, slen);
        q += 4 + slen;
        if (p.Eval(cur)) ++cnt;
      }
      return cnt;
    }
    case Scheme::kDictX: {
      // Pass table over the dictionary pool (flat entry list or FSST
      // region over the entries), then one inner decode of the code
      // stream -- the kDict arm's shape with a cascaded code region
      // (mirrors the WASM vc_count_region s == 26 arm).
      if (r + 11 > region_end) return std::nullopt;
      const uint16_t d = ReadBE16(r);
      if (d == 0) {
        // Every code decodes to "" (matches VarcharRegionAt's clamp).
        return p.Eval(std::string_view()) ? uint64_t{n} : uint64_t{0};
      }
      const uint8_t cs = static_cast<uint8_t>(r[2]);
      const uint32_t cparam = ReadBE32(r + 3);
      const uint64_t clen = ReadBE32(r + 7);
      const char* creg = r + 11;
      if (clen > static_cast<uint64_t>(region_end - creg)) {
        return std::nullopt;
      }
      const char* pool_hdr = creg + clen;
      if (pool_hdr + 1 > region_end || !InnerSchemeOk(cs)) {
        return std::nullopt;
      }
      const uint8_t ps = static_cast<uint8_t>(pool_hdr[0]);
      std::vector<uint8_t> pass(static_cast<size_t>(d) + 1, 0);
      if (ps == 0) {
        const char* q = pool_hdr + 1;
        for (uint16_t c = 0; c < d; ++c) {
          if (q + 4 > region_end) return std::nullopt;
          const uint32_t len = ReadBE32(q);
          if (q + 4 + len > region_end) return std::nullopt;
          pass[c] = p.Eval(std::string_view(q + 4, len)) ? 1 : 0;
          q += 4 + len;
        }
      } else if (ps == static_cast<uint8_t>(Scheme::kFsst)) {
        const char* pool = pool_hdr + 1;
        if (pool + 2 > region_end) return std::nullopt;
        const uint16_t nsym = ReadBE16(pool);
        for (uint16_t c = 0; c < d; ++c) {
          Value v =
              VarcharRegionAt(pool, Scheme::kFsst, nsym, d, c, region_end);
          pass[c] = (v.type == ValueType::kVarChar && p.Eval(v.AsStringView()))
                        ? 1
                        : 0;
        }
      } else {
        return std::nullopt;
      }
      // Codes >= d decode to "" (clamp bucket), same as the decoder.
      pass[d] = p.Eval(std::string_view()) ? 1 : 0;
      std::vector<int64_t> codes(n, 0);
      DecodeInnerI64All(creg, cs, cparam, n, pool_hdr, codes.data());
      uint64_t cnt = 0;
      for (uint32_t i = 0; i < n; ++i) {
        const uint64_t code = static_cast<uint64_t>(codes[i]);
        cnt += pass[code < d ? code : d];
      }
      return cnt;
    }
    case Scheme::kTemplate: {
      // The template fold: a needle that parses against the stored skeleton
      // turns into one int64 equality PER DIGIT SEGMENT over the digit
      // planes -- the count never materializes a conforming string byte
      // (literals live in the header, read once). Structure-mismatched
      // needles eliminate every conforming row without reading a digit;
      // only the exception rows are compared as strings. Ordered
      // comparisons (LT/LE/GT/GE and the range form) decline to the generic
      // decode path: the composite lexicographic fold across segments is
      // future work.
      TemplateRegion t;
      if (!ParseTemplateRegion(r, n, param, region_end, &t))
        return std::nullopt;
      if (p.range || (p.op != StrScanOp::kEq && p.op != StrScanOp::kNe)) {
        return std::nullopt;
      }
      // Parse the needle against the template: every literal must match in
      // place and every digit slot must hold exactly `widths[s]` ASCII
      // digits. Conforming rows have exactly this shape by construction.
      bool structure_ok = true;
      int64_t target[kTemplateMaxSegments];
      {
        std::string_view nd(p.needle);
        size_t pos = 0;
        for (uint32_t seg = 0; seg < t.nseg && structure_ok; ++seg) {
          // Literal seg.
          if (pos + t.lit_len[seg] > nd.size() ||
              std::memcmp(nd.data() + pos, t.lit[seg], t.lit_len[seg]) != 0) {
            structure_ok = false;
            break;
          }
          pos += t.lit_len[seg];
          // Digit run seg: exactly widths[seg] decimal digits.
          if (pos + t.widths[seg] > nd.size()) {
            structure_ok = false;
            break;
          }
          uint64_t v = 0;
          for (uint32_t d = 0; d < t.widths[seg]; ++d) {
            const char c = nd[pos + d];
            if (c < '0' || c > '9') {
              structure_ok = false;
              break;
            }
            v = v * 10 + static_cast<uint64_t>(c - '0');
          }
          if (!structure_ok) break;
          pos += t.widths[seg];
          target[seg] = static_cast<int64_t>(v);
        }
        // Trailing literal + no extra bytes.
        if (structure_ok) {
          if (pos + t.lit_len[t.nseg] != nd.size() ||
              std::memcmp(nd.data() + pos, t.lit[t.nseg], t.lit_len[t.nseg]) !=
                  0) {
            structure_ok = false;
          }
        }
      }
      uint64_t eq_conf = 0;
      if (structure_ok && t.nconf > 0) {
        // AND per-segment equality over the digit planes. nconf <= 1024
        // (one row group), so a fixed 16-word mask covers it; the tail bits
        // past nconf are masked off before the popcount (a partial row
        // group leaves them stale otherwise).
        uint64_t mask[(PaxChunk::kRowGroupSize + 63) / 64];
        for (uint64_t& w : mask) w = ~UINT64_C(0);
        thread_local std::vector<int64_t> col;
        col.resize(t.nconf);
        for (uint32_t seg = 0; seg < t.nseg; ++seg) {
          // The digit planes are narrow (a few bits per row), so the tight
          // bulk walk is the hot path; anything else falls back to the
          // generic inner decoder (which fails closed identically).
          if (!TemplateSegmentBulk(t.seg[seg], t.seg_scheme[seg],
                                   t.seg_param[seg], t.nconf, t.seg_end[seg],
                                   col.data()) &&
              !DecodeInnerI64All(
                  t.seg[seg], static_cast<uint8_t>(t.seg_scheme[seg]),
                  t.seg_param[seg], t.nconf, t.seg_end[seg], col.data())) {
            return std::nullopt;
          }
          // Later segments only test the rows the earlier ones kept, so a
          // selective early segment shrinks the rest of the fold.
          for (uint32_t i = 0; i < t.nconf; ++i) {
            if ((mask[i >> 6] >> (i & 63)) & 1) {
              if (col[static_cast<size_t>(i)] != target[seg]) {
                mask[i >> 6] &= ~(UINT64_C(1) << (i & 63));
              }
            }
          }
        }
        const uint32_t words = (t.nconf + 63) / 64;
        if (t.nconf % 64 != 0) {
          mask[words - 1] &= (UINT64_C(1) << (t.nconf % 64)) - 1;
        }
        for (uint32_t w = 0; w < words; ++w) {
          eq_conf += static_cast<uint64_t>(__builtin_popcountll(mask[w]));
        }
      }
      // Exception rows always compare as strings (they are the only bytes
      // the fold materializes).
      uint64_t eq_exc = 0;
      {
        const char* q = t.exc;
        for (uint32_t e = 0; e < t.nexc; ++e) {
          const uint32_t len = ReadBE32(q);
          if (p.Eval(std::string_view(q + 4, len))) ++eq_exc;
          q += 4 + len;
        }
      }
      const uint64_t eq_total = eq_conf + eq_exc;
      return p.op == StrScanOp::kEq ? eq_total
                                    : static_cast<uint64_t>(n) - eq_total;
    }
    case Scheme::kDStream: {
      // The memcmp fold: encode the needle once through the STORED symbol
      // table, then every conforming row costs one u8 load and a
      // first-byte-rejected compare over its code record -- the fold never
      // materializes a conforming string byte (FSST's arm below expands
      // every row's symbols into a scratch string first). Greedy longest
      // match is a deterministic function of (table, bytes), so a row
      // equals the needle iff its record's codes equal the needle's codes,
      // and the code encoding is decodable (injective), so unequal strings
      // can never alias one code sequence. NE is the exact complement.
      // Ordered ops and ranges decline: codes are not order-preserving,
      // and a byte-prefix of a string may encode differently than the
      // prefix encoded standalone (a symbol may cross the needle's
      // boundary), so the composite lexicographic/prefix fold is future
      // work rather than a half-correct shortcut.
      DStreamRegion t;
      if (!ParseDStreamRegion(r, n, param, region_end, &t)) {
        return std::nullopt;
      }
      if (p.range || (p.op != StrScanOp::kEq && p.op != StrScanOp::kNe &&
                      p.op != StrScanOp::kPrefix)) {
        return std::nullopt;
      }
      const bool prefix = p.op == StrScanOp::kPrefix;
      std::string ncodes;
      if (!prefix) DStreamEncodeNeedle(t, p.needle, &ncodes);
      const size_t klen = ncodes.size();
      // Conforming records carry clen in one u8, so a needle encoding to
      // more than 255 code bytes cannot equal any of them; only the
      // exception rows below can match. (The PREFIX path compares BYTES,
      // so the needle's byte length is not bounded by the record's code
      // length -- a long needle simply matches no short row.)
      uint64_t eq_conf = 0;
      uint64_t prefix_conf = 0;
      if ((!prefix || t.nconf > 0) && (prefix || klen <= 255)) {
        const char* send = t.stream + t.stream_len;
        uint32_t left = t.nconf;
        for (uint32_t b = 0; b < t.ndir && left > 0; ++b) {
          const uint32_t off = ReadBE32(t.dir + static_cast<size_t>(b) * 4);
          if (off > t.stream_len) return std::nullopt;
          const char* sp = t.stream + off;
          const uint32_t rows = std::min(kDStreamRestart, left);
          for (uint32_t k = 0; k < rows; ++k) {
            if (sp + 1 > send) return std::nullopt;
            const uint32_t clen = static_cast<uint8_t>(*sp++);
            if (static_cast<size_t>(send - sp) < clen) return std::nullopt;
            if (prefix) {
              // The PREFIX fold: the needle's BYTES are compared against
              // the row's EXPANDED codes, and the row is never
              // materialized (each code contributes its symbol's bytes, or
              // the escaped byte, and the walk stops at the first
              // mismatch or once the needle is consumed).
              //
              // The code-sequence comparison CANNOT be used here: the
              // encoder's greedy longest match may split the same bytes
              // differently at the needle's boundary (a symbol may
              // straddle it), so "the row's codes start with the needle's
              // codes" is NOT "the row's bytes start with the needle's
              // bytes".
              size_t matched = 0;
              bool ok = true;
              for (uint32_t i = 0; i < clen && ok && matched < p.needle.size();
                   ++i) {
                const uint32_t c = static_cast<uint8_t>(sp[i]);
                if (c == 0) {
                  // The escape: the raw byte follows.
                  if (i + 1 >= clen) {
                    ok = false;
                    break;
                  }
                  if (sp[i + 1] != p.needle[matched]) {
                    ok = false;
                    break;
                  }
                  ++matched;
                  ++i;
                } else if (c <= t.nsym) {
                  const char* sym = t.sym[c - 1];
                  const uint32_t slen = t.sym_len[c - 1];
                  for (uint32_t sk = 0; sk < slen && matched < p.needle.size();
                       ++sk) {
                    if (sym[sk] != p.needle[matched]) {
                      ok = false;
                      break;
                    }
                    ++matched;
                  }
                } else {
                  ok = false;  // a code past the table
                }
              }
              // The row must be AT LEAST as long as the needle: a shorter
              // row that matched all its bytes is not a prefix match.
              if (ok && matched == p.needle.size()) ++prefix_conf;
            } else if (clen == klen &&
                       std::memcmp(sp, ncodes.data(), klen) == 0) {
              ++eq_conf;
            }
            sp += clen;
          }
          left -= rows;
        }
      }
      // Exception rows always compare as strings (they are the only bytes
      // the fold materializes).
      uint64_t eq_exc = 0;
      {
        const char* q = t.exc;
        for (uint32_t e = 0; e < t.nexc; ++e) {
          const uint32_t len = ReadBE32(q);
          if (p.Eval(std::string_view(q + 4, len))) ++eq_exc;
          q += 4 + len;
        }
      }
      if (prefix) {
        // The conforming rows were counted by the prefix walk above; the
        // exception rows compare as bytes.
        return prefix_conf + eq_exc;
      }
      const uint64_t eq_total = eq_conf + eq_exc;
      return p.op == StrScanOp::kEq ? eq_total
                                    : static_cast<uint64_t>(n) - eq_total;
    }
    default:
      return std::nullopt;  // generic decode path below
  }
}

uint64_t VarcharRegionMatchCountGeneric(const char* r, Scheme s, uint32_t param,
                                        uint32_t n, const char* region_end,
                                        const StrPredicate& p) {
  uint64_t cnt = 0;
  for (uint32_t i = 0; i < n; ++i) {
    const Value v = VarcharRegionAt(r, s, param, n, i, region_end);
    if (v.IsNull()) {
      if (p.Eval(std::string_view())) ++cnt;
    } else if (p.Eval(v.AsStringView())) {
      ++cnt;
    }
  }
  return cnt;
}

// The row indices (LOCAL to the region) that satisfy the predicate.
//
// The Front-Code SORTED region answers in the compressed domain: the
// matching rows are the contiguous index range the two binary searches
// bracket, so the region is never materialized. Every other scheme walks
// the region's values.
//
// Returns false when the fast path hit a structural surprise (a hostile or
// truncated extent); the caller then re-runs the region through the
// bounds-checked generic walk.
bool VarcharRegionMatchRows(const char* r, Scheme s, uint32_t param, uint32_t n,
                            const char* region_end, const StrPredicate& p,
                            std::vector<uint32_t>* out) {
  if (n == 0) return true;
  if (region_end == nullptr || region_end <= r) return false;
  if (s == Scheme::kFrontCode && (param & 0x8000u) != 0) {
    const uint32_t re = param & 0x7FFFu;
    if (re == 0 || r + 6 > region_end) return false;
    const uint32_t nrestart = ReadBE16(r);
    const uint32_t stream_len = ReadBE32(r + 2);
    const char* rtable = r + 6;
    if (rtable + static_cast<size_t>(nrestart) * 4 > region_end) return false;
    const char* stream = rtable + static_cast<size_t>(nrestart) * 4;
    if (stream + stream_len > region_end) return false;
    const char* send = stream + stream_len;
    std::string scratch;
    auto value_at = [&](uint32_t idx) -> bool {
      const uint32_t block = idx / re;
      if (block >= nrestart) return false;
      const uint32_t ro = ReadBE32(rtable + static_cast<size_t>(block) * 4);
      if (ro > stream_len) return false;
      scratch.clear();
      const char* q = stream + ro;
      for (uint32_t i = block * re; i <= idx; ++i) {
        if (q + 4 > send) return false;
        const uint32_t plen = ReadBE16(q);
        const uint32_t slen = ReadBE16(q + 2);
        if (plen > scratch.size() || q + 4 + slen > send) return false;
        scratch.resize(plen);
        scratch.append(q + 4, slen);
        q += 4 + slen;
      }
      return true;
    };
    // First index with value >= key (false) / > key (true). -1 flags a
    // structural surprise (the count path uses the same two searches).
    auto seek = [&](const std::string& key, bool excl) -> int64_t {
      uint32_t lo = 0, hi = n;
      while (lo < hi) {
        const uint32_t mid = lo + (hi - lo) / 2;
        if (!value_at(mid)) return -1;
        const int c = scratch.compare(key);
        if (c < 0 || (excl && c == 0)) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return static_cast<int64_t>(lo);
    };
    // The matching index window, in the same shape the count path computes.
    int64_t a = 0;
    int64_t b = static_cast<int64_t>(n);
    if (p.range) {
      if (p.has_lo) {
        a = seek(p.lo, false);
        if (a < 0) return false;
      }
      if (p.has_hi) {
        b = seek(p.hi, false);
        if (b < 0) return false;
      }
    } else {
      switch (p.op) {
        case StrScanOp::kEq: {
          a = seek(p.needle, false);
          if (a < 0) return false;
          b = seek(p.needle, true);
          if (b < 0) return false;
          break;
        }
        case StrScanOp::kNe: {
          // The complement of the equality window: two ranges.
          const int64_t eq_a = seek(p.needle, false);
          if (eq_a < 0) return false;
          const int64_t eq_b = seek(p.needle, true);
          if (eq_b < 0) return false;
          for (uint32_t i = 0; i < static_cast<uint32_t>(eq_a); ++i) {
            out->push_back(i);
          }
          for (int64_t i = eq_b; i < static_cast<int64_t>(n); ++i) {
            out->push_back(static_cast<uint32_t>(i));
          }
          return true;
        }
        case StrScanOp::kLt:
          a = 0;
          b = seek(p.needle, false);
          if (b < 0) return false;
          break;
        case StrScanOp::kLe:
          a = 0;
          b = seek(p.needle, true);
          if (b < 0) return false;
          break;
        case StrScanOp::kGt: {
          const int64_t gt = seek(p.needle, true);
          if (gt < 0) return false;
          for (int64_t i = gt; i < static_cast<int64_t>(n); ++i) {
            out->push_back(static_cast<uint32_t>(i));
          }
          return true;
        }
        default: {  // kGe
          a = seek(p.needle, false);
          if (a < 0) return false;
          b = static_cast<int64_t>(n);
          break;
        }
      }
    }
    if (b < a) b = a;
    for (int64_t i = a; i < b; ++i) out->push_back(static_cast<uint32_t>(i));
    return true;
  }
  // The generic walk: decode every row and evaluate. The decode is the
  // same bounds-checked one the count path uses.
  for (uint32_t i = 0; i < n; ++i) {
    const Value v = VarcharRegionAt(r, s, param, n, i, region_end);
    const bool hit =
        v.IsNull() ? p.Eval(std::string_view()) : p.Eval(v.AsStringView());
    if (hit) out->push_back(i);
  }
  return true;
}

uint64_t VarcharCountWith(const PaxChunk& chunk, slot_t col,
                          const StrPredicate& p) {
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count) return 0;
  // Hand-built-footer guard, same contract as ReduceColumn/TopKColumn.
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return 0;
  }
  uint64_t total = 0;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const uint16_t rc = f.row_counts[g];
    if (rc == 0) continue;
    const uint32_t off = g * f.column_count + col;
    // Offset-VALUE validation, same contract as every sibling consumer
    // (ReadRow / PointLookup / ReduceColumn / VectorizedScanColumn): the
    // raw `off > data.size()` gate this used to apply admitted offsets
    // pointing into the footer bytes, forming a wild pointer that only the
    // differently-scoped reg_end check below caught. RegionStartAt rejects
    // anything past the data-area end so the pointer is never formed.
    const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
    if (region == nullptr) continue;
    const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
    // An unboundable region cannot be walked safely in any path; skip it
    // (contributes 0) rather than handing the generic decoder an unbounded
    // extent.
    if (reg_end == nullptr || reg_end <= region) continue;
    const Scheme s = static_cast<Scheme>(f.encodings[off]);
    const uint32_t prm = f.params[off];
    std::optional<uint64_t> c =
        VarcharRegionMatchCount(region, s, prm, rc, reg_end, p);
    if (!c.has_value()) {
      c = VarcharRegionMatchCountGeneric(region, s, prm, rc, reg_end, p);
    }
    total += *c;
  }
  return total;
}

uint64_t VarcharGateAndRecord(const PaxChunk& chunk, slot_t col,
                              const Schema* sc, WorkloadTracker* workload) {
  if (col >= chunk.GetFooter().column_count) return 0;
  if (sc != nullptr && (col >= sc->ColumnCount() ||
                        sc->GetColumn(col).Type() != ValueType::kVarChar)) {
    return 0;  // predicate pushdown is a varchar-column surface
  }
  if (workload != nullptr && sc != nullptr) workload->RecordFilter(*sc, col);
  return 1;  // gate passed
}

}  // namespace

uint64_t VarcharFilterCount(const PaxChunk& chunk, slot_t col, StrScanOp op,
                            std::string_view needle, const Schema* sc,
                            WorkloadTracker* workload) {
  if (!VarcharGateAndRecord(chunk, col, sc, workload)) return 0;
  StrPredicate p;
  p.op = op;
  p.needle = std::string(needle);
  return VarcharCountWith(chunk, col, p);
}

uint64_t VarcharRangeCount(const PaxChunk& chunk, slot_t col,
                           std::optional<std::string_view> lo,
                           std::optional<std::string_view> hi, const Schema* sc,
                           WorkloadTracker* workload) {
  if (!VarcharGateAndRecord(chunk, col, sc, workload)) return 0;
  if (lo.has_value() && hi.has_value() && *lo >= *hi) return 0;
  StrPredicate p;
  p.range = true;
  if (lo.has_value()) {
    p.has_lo = true;
    p.lo = std::string(*lo);
  }
  if (hi.has_value()) {
    p.has_hi = true;
    p.hi = std::string(*hi);
  }
  return VarcharCountWith(chunk, col, p);
}

namespace {

// The chunk-level selection: the same region walk the count path uses, but
// the matching rows are collected instead of counted.
PaxSelectionVector VarcharSelectionWith(const PaxChunk& chunk, slot_t col,
                                        const StrPredicate& p) {
  PaxSelectionVector sv;
  const PaxChunk::Footer& f = chunk.GetFooter();
  if (col >= f.column_count) return sv;
  // Hand-built-footer guard, same contract as the count path.
  if (f.row_counts.size() < f.row_group_count ||
      f.column_offsets.size() <
          static_cast<size_t>(f.row_group_count) * f.column_count ||
      f.encodings.size() < f.column_offsets.size() ||
      f.params.size() < f.column_offsets.size()) {
    return sv;
  }
  const size_t total_rows = chunk.TotalRowCount();
  sv.bits.assign((total_rows + 63) / 64, 0);
  uint64_t base = 0;
  std::vector<uint32_t> local;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    const uint16_t rc = f.row_counts[g];
    if (rc == 0) continue;
    const uint32_t off = g * f.column_count + col;
    const char* region = RegionStartAt(f, g, col, chunk.Data(), 0);
    if (region == nullptr) {
      base += rc;
      continue;
    }
    const char* reg_end = RegionEndAt(f, g, col, chunk.Data());
    if (reg_end == nullptr || reg_end <= region) {
      base += rc;
      continue;
    }
    const Scheme s = static_cast<Scheme>(f.encodings[off]);
    const uint32_t prm = f.params[off];
    local.clear();
    if (!VarcharRegionMatchRows(region, s, prm, rc, reg_end, p, &local)) {
      // A structural surprise in the fast path: re-run the region through
      // the bounds-checked generic walk.
      local.clear();
      for (uint32_t i = 0; i < rc; ++i) {
        const Value v = VarcharRegionAt(region, s, prm, rc, i, reg_end);
        const bool hit =
            v.IsNull() ? p.Eval(std::string_view()) : p.Eval(v.AsStringView());
        if (hit) local.push_back(i);
      }
    }
    for (uint32_t i : local) {
      const uint64_t global = base + i;
      sv.bits[global >> 6] |= (uint64_t{1} << (global & 63));
      ++sv.count;
    }
    base += rc;
  }
  return sv;
}

}  // namespace

PaxSelectionVector VarcharFilterSelection(const PaxChunk& chunk, slot_t col,
                                          StrScanOp op, std::string_view needle,
                                          const Schema* sc,
                                          WorkloadTracker* workload) {
  if (!VarcharGateAndRecord(chunk, col, sc, workload)) return {};
  StrPredicate p;
  p.op = op;
  p.needle = std::string(needle);
  return VarcharSelectionWith(chunk, col, p);
}

PaxSelectionVector VarcharRangeSelection(const PaxChunk& chunk, slot_t col,
                                         std::optional<std::string_view> lo,
                                         std::optional<std::string_view> hi,
                                         const Schema* sc,
                                         WorkloadTracker* workload) {
  if (!VarcharGateAndRecord(chunk, col, sc, workload)) return {};
  if (lo.has_value() && hi.has_value() && *lo >= *hi) return {};
  StrPredicate p;
  p.range = true;
  if (lo.has_value()) {
    p.has_lo = true;
    p.lo = std::string(*lo);
  }
  if (hi.has_value()) {
    p.has_hi = true;
    p.hi = std::string(*hi);
  }
  return VarcharSelectionWith(chunk, col, p);
}

std::vector<uint32_t> SelectionRows(const PaxSelectionVector& sv) {
  std::vector<uint32_t> rows;
  rows.reserve(static_cast<size_t>(sv.count));
  const size_t words = sv.bits.size();
  for (size_t w = 0; w < words; ++w) {
    uint64_t word = sv.bits[w];
    while (word != 0) {
      const uint64_t bit = word & (~word + 1);  // the lowest set bit
      rows.push_back(static_cast<uint32_t>(
          w * 64 + static_cast<size_t>(__builtin_ctzll(bit))));
      word ^= bit;
    }
  }
  return rows;
}

}  // namespace tinylamb
