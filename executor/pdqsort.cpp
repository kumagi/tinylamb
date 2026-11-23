/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/pdqsort.hpp"

#include <algorithm>
#include <numeric>
#include <utility>
#include <vector>

#include "executor/sort.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// One row's sort-key tuple, evaluated once up front. Sorting these instead of
// re-evaluating the key expressions on every comparison turns a K-key sort
// from O(K n log n) expression evaluations into O(K n).
struct MaterializedKeys {
  std::vector<Value> values;
  Status error{Status::kSuccess};
};

template <typename Rows, typename Project>
std::vector<MaterializedKeys> MaterializeSortKeys(
    const Rows& rows, const Schema& schema,
    const std::vector<SortExecutor::Key>& keys, Project&& project) {
  std::vector<MaterializedKeys> images;
  images.reserve(rows.size());
  for (const auto& entry : rows) {
    MaterializedKeys image;
    image.values.reserve(keys.size());
    for (const SortExecutor::Key& key : keys) {
      StatusOr<Value> evaluated =
          key.expression->TryEvaluate(project(entry), schema);
      if (!evaluated.HasValue()) {
        image.error = evaluated.GetStatus();
        break;
      }
      image.values.push_back(evaluated.MoveValue());
    }
    images.push_back(std::move(image));
  }
  return images;
}

int CompareMaterializedKeys(const MaterializedKeys& lhs,
                            const MaterializedKeys& rhs,
                            const std::vector<SortKeyOrder>& orders,
                            Status* error) {
  if (lhs.error != Status::kSuccess || rhs.error != Status::kSuccess) {
    // Comparators cannot abort mid-sort: record the first failure and treat
    // the pair as equal, exactly like the evaluating comparator did.
    if (error != nullptr && *error == Status::kSuccess) {
      *error = lhs.error != Status::kSuccess ? lhs.error : rhs.error;
    }
    return 0;
  }
  for (size_t i = 0; i < orders.size(); ++i) {
    // A failed materialization leaves the image's values short. Always check
    // the bounds here: an `error == nullptr` caller still must not read past
    // the end of a partial key vector.
    if (i >= lhs.values.size() || i >= rhs.values.size()) {
      if (error != nullptr && *error == Status::kSuccess) {
        *error = StatusError(StatusCode::kInvalidArgument,
                             "missing materialized sort key");
      }
      return 0;
    }
    const Value& lv = lhs.values[i];
    const Value& rv = rhs.values[i];
    if (lv.IsNull() && rv.IsNull()) {
      continue;
    }
    // MakeSortKeyOrder (precomputed below) canonicalizes NaN, adds NULL
    // placement, the direction and the key's collation -- the same total
    // order the evaluating comparator used.
    const int cmp = orders[i].Compare(lv, rv);
    if (cmp != 0) {
      return cmp;
    }
  }
  return 0;
}

// `validate` preserves the two public overloads' existing gate: the pair
// overload always validates before sorting, the plain-Row overload only does
// so when the caller supplies an error status.
template <typename Rows, typename Project>
void SortByMaterializedKeys(Rows& rows, const Schema& schema,
                            const std::vector<SortExecutor::Key>& keys,
                            Status* error, Project&& project) {
  const std::vector<MaterializedKeys> images =
      MaterializeSortKeys(rows, schema, keys, project);
  Status local_error = Status::kSuccess;
  Status* validation_error = error != nullptr ? error : &local_error;
  // Always validate: a caller that passes no error pointer still must not
  // sort (or compare) with a partially materialized key vector.
  {
    for (const MaterializedKeys& image : images) {
      if (image.error != Status::kSuccess) {
        // Preserve a pre-existing error, matching the comparator's
        // first-error-wins convention below.
        if (*validation_error == Status::kSuccess) {
          *validation_error = image.error;
        }
        return;
      }
    }
  }
  std::vector<SortKeyOrder> orders;
  orders.reserve(keys.size());
  for (const SortExecutor::Key& key : keys) {
    orders.push_back(MakeSortKeyOrder(key));
  }
  for (const MaterializedKeys& image : images) {
    const Status checked = ObserveSortKeys(orders, image.values);
    if (!checked.ok()) {
      if (validation_error->ok()) { *validation_error = checked;
}
      return;
    }
  }
  // Sort indices, not rows in place: the keys are bound to a row's original
  // position, which std::sort would move around during the permutation.
  std::vector<size_t> order(rows.size());
  std::iota(order.begin(), order.end(), 0);
  std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    return CompareMaterializedKeys(images[a], images[b], orders, error) < 0;
  });
  Rows sorted;
  sorted.reserve(rows.size());
  for (size_t index : order) {
    sorted.push_back(std::move(rows[index]));
  }
  rows = std::move(sorted);
}

}  // namespace

void PdqSort::Sort(std::vector<std::pair<Row, RowPosition>>& rows,
                   const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys, Status* error) {
  // Validation must always run: a failed key evaluation would otherwise
  // make the comparator non-transitive and std::sort undefined.
  SortByMaterializedKeys(
      rows, schema, keys, error,
      [](const std::pair<Row, RowPosition>& entry) -> const Row& {
        return entry.first;
      });
}

void PdqSort::Sort(std::vector<Row>& rows, const Schema& schema,
                   const std::vector<SortExecutor::Key>& keys, Status* error) {
  SortByMaterializedKeys(
      rows, schema, keys, error, [](const Row& row) -> const Row& {
        return row;  // NOLINT(bugprone-return-const-ref-from-parameter)
      });
}

}  // namespace tinylamb
