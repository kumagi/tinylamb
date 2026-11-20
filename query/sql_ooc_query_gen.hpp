/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_SQL_OOC_QUERY_GEN_HPP
#define TINYLAMB_SQL_OOC_QUERY_GEN_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace tinylamb {

// Byte-steered PRNG shared by the OOC session generator and the query
// generator: fuzzer input bytes steer the first decisions, a seeded PRNG
// takes over once they run out.
class OocRng {
 public:
  OocRng(const uint8_t* data, size_t size, uint64_t seed)
      : data_(data), size_(size), rng_(seed) {}

  uint32_t Next32() {
    if (pos_ + 4 <= size_) {
      uint32_t v = 0;
      for (int i = 0; i < 4; ++i) {
        v = (v << 8) | data_[pos_++];
      }
      return v ^ static_cast<uint32_t>(rng_());
    }
    return static_cast<uint32_t>(rng_());
  }
  int64_t Pick(int64_t lo, int64_t hi) {
    if (hi <= lo) {
      return lo;
    }
    const auto span = static_cast<uint64_t>(hi - lo + 1);
    return lo + static_cast<int64_t>(Next32() % span);
  }
  bool Chance(int pct) { return Pick(0, 99) < pct; }
  template <typename T>
  const T& OneOf(const std::vector<T>& v) {
    return v.at(static_cast<size_t>(Pick(0, static_cast<int64_t>(v.size()) - 1)));
  }

 private:
  const uint8_t* data_;
  size_t size_;
  size_t pos_{0};
  std::mt19937_64 rng_;
};

// What the query generator needs to know about one fuzz table.  Every table
// has the columns (k INT64 unique, g INT64, v INT64 NULL, t VARCHAR NULL,
// pad VARCHAR).
struct OocGenTable {
  std::string name;
  int64_t rows{0};
  int64_t key_lo{1};
  int64_t key_hi{1};
  int64_t groups{1};    // g is in [0, groups)
  int64_t v_mod{1000};  // v is (mostly) in [0, v_mod)
  size_t pad_max{0};    // longest pad stored right now
};

struct OocGenQuery {
  std::string sql;
  // Rows must match in order (the statement ends in a total ORDER BY).
  bool ordered{false};
  // One value or group state can be large (STRING_AGG, wide keys): tiny
  // operator budgets may legitimately answer kTooBigData.
  bool heavy{false};
  // Template name, for coverage accounting.
  std::string shape;
};

// Draws one random read-only query over `tables` (at least one table).
// Every query is deterministic: any two correct executions return the same
// multiset of rows (the same sequence when `ordered`).  `max_join_rows`
// bounds the estimated intermediate result of joins.
OocGenQuery GenerateOocQuery(OocRng& rng,
                             const std::vector<OocGenTable>& tables,
                             int64_t max_join_rows);

// Every template (shape name) the generator can emit.
std::vector<std::string> OocQueryShapes();

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_OOC_QUERY_GEN_HPP
