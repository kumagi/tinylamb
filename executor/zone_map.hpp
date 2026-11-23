/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_ZONE_MAP_HPP
#define TINYLAMB_EXECUTOR_ZONE_MAP_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "type/value.hpp"

namespace tinylamb {

struct LSTablePageBlock;
class Schema;

// Lean proof: lean/TinylambOpt/BlockKleene.lean
//   (aeval_contains_ceval, block_kleene_sound, block_all_pass_sound)
// 4-valued abstract domain {T, F, U, E} for compound block-level pruning.
using KleeneMask = uint8_t;
inline constexpr KleeneMask kKleeneNone = 0;
inline constexpr KleeneMask kKleeneT = 1U << 0U;
inline constexpr KleeneMask kKleeneF = 1U << 1U;
inline constexpr KleeneMask kKleeneU = 1U << 2U;
inline constexpr KleeneMask kKleeneE = 1U << 3U;
inline constexpr KleeneMask kKleeneTF = kKleeneT | kKleeneF;
inline constexpr KleeneMask kKleeneAll =
    kKleeneT | kKleeneF | kKleeneU | kKleeneE;

[[nodiscard]] KleeneMask EvaluateBlockKleene(const Expression& expr,
                                             const Schema& schema,
                                             const LSTablePageBlock& blk);

[[nodiscard]] inline bool BlockKleeneCanSkip(KleeneMask mask) noexcept {
  return (mask & (kKleeneT | kKleeneE)) == 0;
}

[[nodiscard]] inline bool BlockKleeneAllPass(KleeneMask mask) noexcept {
  return mask == kKleeneT;
}

class ZoneMap {
 public:
  void Add(const Value& value);
  // Typed variants for callers that hold unboxed storage.  They keep the
  // minimum/maximum Values correctly typed (kInt64/kDate/kDouble/kVarChar)
  // so MayMatch still compares against same-type constants.
  void AddInt(int64_t value);
  void AddDate(int64_t days);
  void AddDouble(double value);
  void AddString(std::string_view value);
  void AddNull();
  void AddNulls(size_t count);
  void AddIntRange(int64_t mn, int64_t mx, size_t count);
  void AddDateRange(int64_t mn, int64_t mx, size_t count);
  void AddDoubleRange(double mn, double mx, bool has_bounds, bool saw_nan,
                      size_t count);
  void AddStringRange(std::string_view mn, std::string_view mx, size_t count);
  // Records a non-NULL value that carries no scalar ordering (arrays): it
  // bumps value_count_ so MayMatch keeps the zone instead of pruning a batch
  // that holds only array values.
  void AddOpaque();
  void Merge(const ZoneMap& other);
  void Reset();

  [[nodiscard]] bool MayMatch(BinaryOperation operation,
                              const Value& constant) const;
  // False until any Add* observed a row.  Callers must treat an
  // uninitialized zone conservatively (it may match anything); only an
  // initialized, value-free zone proves "no non-NULL values here".
  [[nodiscard]] bool Initialized() const { return initialized_; }
  [[nodiscard]] const std::optional<Value>& Minimum() const { return minimum_; }
  [[nodiscard]] const std::optional<Value>& Maximum() const { return maximum_; }
  [[nodiscard]] size_t NullCount() const { return null_count_; }
  [[nodiscard]] size_t ValueCount() const { return value_count_; }

 private:
  std::optional<Value> minimum_;
  std::optional<Value> maximum_;
  size_t null_count_{0};
  size_t value_count_{0};
  bool initialized_{false};
  // Set when a DOUBLE NaN was added: NaN is excluded from the envelope but
  // must conservatively keep the zone eligible for every predicate.
  bool has_nan_{false};
  // A byte-order envelope is not a proof under a folding comparison.
  bool has_collated_values_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_ZONE_MAP_HPP
