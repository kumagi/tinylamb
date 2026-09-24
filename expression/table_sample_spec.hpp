#ifndef TINYLAMB_EXPRESSION_TABLE_SAMPLE_SPEC_HPP
#define TINYLAMB_EXPRESSION_TABLE_SAMPLE_SPEC_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

enum class TableSampleMethod : uint8_t {
  kBernoulli,
  kReservoir,
  kSystem,
};

enum class TableSampleUnit : uint8_t { kPercent, kRows };

inline const char* TableSampleMethodName(TableSampleMethod method) {
  switch (method) {
    case TableSampleMethod::kBernoulli:
      return "BERNOULLI";
    case TableSampleMethod::kReservoir:
      return "RESERVOIR";
    case TableSampleMethod::kSystem:
      return "SYSTEM";
  }
  return "BERNOULLI";
}

inline const char* TableSampleUnitName(TableSampleUnit unit) {
  return unit == TableSampleUnit::kRows ? "ROWS" : "PERCENT";
}

struct TableSampleSpec {
  TableSampleMethod method{TableSampleMethod::kBernoulli};
  TableSampleUnit unit{TableSampleUnit::kPercent};
  Expression size;
  std::optional<Expression> repeatable_seed;
  std::vector<Expression> partition_by;
  bool with_weight{false};
  std::string weight_alias{"weight"};

  void Dump(std::ostream& o) const {
    o << TableSampleMethodName(method) << '(';
    if (size) {
      o << size->ToString();
    }
    o << ' ' << TableSampleUnitName(unit);
    if (!partition_by.empty()) {
      o << " PARTITION BY (";
      for (size_t i = 0; i < partition_by.size(); ++i) {
        if (i != 0U) {
          o << ", ";
        }
        if (partition_by[i]) {
          o << partition_by[i]->ToString();
        }
      }
      o << ')';
    }
    if (repeatable_seed && *repeatable_seed) {
      o << " REPEATABLE(" << (*repeatable_seed)->ToString() << ')';
    }
    if (with_weight) {
      o << " WITH WEIGHT AS " << weight_alias;
    }
    o << ')';
  }
};

}  // namespace tinylamb

#endif
