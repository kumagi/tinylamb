/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean proof: lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
//   (SurrogateKeyPreAggRollup_sound)
// When an exact-integer ROLLUP/GROUPING SETS partial aggregation groups only by
// attributes of a single small/medium dimension table joined on a unique INT64
// primary key (with no extra filters on that dimension and no aggregate
// arguments referencing it), pre-aggregate the remaining fact/dimension join by
// the single INT64 foreign key via parallel FusedGroupedJoin, then join the
// compact surrogate-keyed totals against the dimension table once.
// Callers must pass finest_keys_unique=false to TryCompactGroupingSets because
// distinct surrogate keys may map to identical dimension attribute tuples.
StatusOr<std::optional<Relation>> TrySurrogatePreAggExactGroupingInput(
    TransactionContext& context, const SelectStatement& head,
    const ExactIntegerGroupingSets& partial, const CteMap& ctes) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SURROGATE_ROLLUP") !=
      nullptr;
  if (disabled) {
    return std::optional<Relation>{};
  }
  const SelectStatement& part = partial.partial;
  const auto& sources = part.Sources();
  const size_t num_keys = head.GroupBy().size();
  if (sources.size() < 3 || num_keys == 0 ||
      part.SelectList().size() <= num_keys) {
    return std::optional<Relation>{};
  }
  const size_t num_aggs = part.SelectList().size() - num_keys;

  std::vector<std::shared_ptr<Table>> tables(sources.size());
  std::vector<Schema> src_schemas(sources.size());
  std::vector<std::string> src_aliases(sources.size());
  for (size_t i = 0; i < sources.size(); ++i) {
    const auto& src = sources[i];
    if (src.table.empty() || src.query != nullptr || src.unnest != nullptr ||
        src.is_lateral || src.sample.has_value() ||
        !src.using_columns.empty() ||
        (src.join_type != JoinType::kInner &&
         src.join_type != JoinType::kCross)) {
      return std::optional<Relation>{};
    }
    if (ctes.contains(AsciiToLower(src.table))) {
      return std::optional<Relation>{};
    }
    auto tbl_or = context.GetTable(src.table);
    if (!tbl_or.HasValue() || !tbl_or.Value()) {
      return std::optional<Relation>{};
    }
    tables[i] = tbl_or.Value();
    src_schemas[i] = tables[i]->GetSchema();
    src_aliases[i] = AsciiToLower(src.alias.empty() ? src.table : src.alias);
  }

  auto find_source_for_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const std::string want_name = AsciiToLower(cn.name);
    const std::string want_schema = AsciiToLower(cn.schema);
    std::optional<std::pair<size_t, slot_t>> found;
    for (size_t i = 0; i < sources.size(); ++i) {
      if (!want_schema.empty() && src_aliases[i] != want_schema) {
        continue;
      }
      for (slot_t c = 0; c < src_schemas[i].ColumnCount(); ++c) {
        if (AsciiToLower(src_schemas[i].GetColumn(c).Name().name) ==
            want_name) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = {i, c};
        }
      }
    }
    return found;
  };

  // All GROUP BY keys must resolve to a single dimension source, and the
  // surrogate key must actually reduce key width (multi-column or non-INT64).
  std::optional<size_t> dim_idx;
  bool benefits_from_surrogate = num_keys >= 2;
  for (const auto& gkey : head.GroupBy()) {
    if (!gkey || gkey->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto resolved =
        find_source_for_col(gkey->AsColumnValue().GetColumnName());
    if (!resolved) {
      return std::optional<Relation>{};
    }
    if (!dim_idx) {
      dim_idx = resolved->first;
    } else if (*dim_idx != resolved->first) {
      return std::optional<Relation>{};
    }
    const Column& col =
        src_schemas[resolved->first].GetColumn(resolved->second);
    if (col.Type() != ValueType::kInt64) {
      benefits_from_surrogate = true;
    }
  }
  if (!dim_idx || !benefits_from_surrogate) {
    return std::optional<Relation>{};
  }
  constexpr uint64_t kMaxDimRows = 500000;
  if (tables[*dim_idx]->BackingColumnarRowCount() > kMaxDimRows) {
    return std::optional<Relation>{};
  }
  // Surrogate pre-aggregation only helps when the dimension's GROUP BY
  // attribute tuple has high cardinality relative to the surrogate PK (e.g.
  // TPC-DS Q22's ~102k unique i_product_name vs 102k i_item_sk).  When the
  // attribute tuple is low-cardinality (e.g. Q86's ~154 (i_category, i_class)
  // pairs over 102k items), grouping by the surrogate key explodes group count.
  if (auto cached_dim = tables[*dim_idx]->GetLSTableStore();
      cached_dim && cached_dim->total_rows >= 2048) {
    uint64_t max_distinct_prod = 1;
    bool all_dict = true;
    for (const auto& gkey : head.GroupBy()) {
      const auto resolved =
          find_source_for_col(gkey->AsColumnValue().GetColumnName());
      if (!resolved || resolved->second >= cached_dim->dictionaries.size() ||
          !cached_dim->dictionaries[resolved->second].active) {
        all_dict = false;
        break;
      }
      const auto& dict = cached_dim->dictionaries[resolved->second];
      const uint64_t flat_n =
          dict.flat_str_offsets.empty() ? 0 : dict.flat_str_offsets.size() - 1;
      const uint64_t n = std::max({dict.entries.size(), dict.int_entries.size(),
                                   dict.double_entries.size(), flat_n});
      if (n >= cached_dim->total_rows ||
          max_distinct_prod >
              cached_dim->total_rows / std::max<uint64_t>(n, 1)) {
        max_distinct_prod = cached_dim->total_rows;
        break;
      }
      max_distinct_prod *= std::max<uint64_t>(n, 1);
    }
    if (all_dict && max_distinct_prod * 4 < cached_dim->total_rows) {
      return std::optional<Relation>{};
    }
  }

  // Collect all column refs inside an expression; return false if any column
  // fails to resolve or if a subquery is encountered.
  auto collect_col_sources = [&](const Expression& expr,
                                 bool* touches_dim) -> bool {
    *touches_dim = false;
    bool ok = true;
    const std::function<void(const Expression&)> walk =
        [&](const Expression& e) {
          if (!e || !ok) {
            return;
          }
          if (e->Type() == TypeTag::kQueryExp) {
            ok = false;
            return;
          }
          if (e->Type() == TypeTag::kColumnValue) {
            const auto res =
                find_source_for_col(e->AsColumnValue().GetColumnName());
            if (!res) {
              ok = false;
              return;
            }
            if (res->first == *dim_idx) {
              *touches_dim = true;
            }
            return;
          }
          for (const auto& ch : ExpressionChildren(e)) {
            walk(ch);
          }
        };
    walk(expr);
    return ok;
  };

  // Aggregate arguments must not reference the dimension table.
  for (size_t i = num_keys; i < part.SelectList().size(); ++i) {
    bool touches_dim = false;
    if (!collect_col_sources(part.SelectList()[i].expression, &touches_dim) ||
        touches_dim) {
      return std::optional<Relation>{};
    }
  }

  // Inspect all WHERE + INNER JOIN ON conjuncts: require exactly one conjunct
  // touching dim_idx, and it must be a plain signed INT64 equijoin fk = dim_pk.
  std::vector<Expression> all_conjuncts;
  if (part.WhereClause()) {
    auto wc = SplitConjuncts(part.WhereClause());
    all_conjuncts.insert(all_conjuncts.end(), wc.begin(), wc.end());
  }
  for (const auto& src : sources) {
    if (src.join_condition) {
      auto jc = SplitConjuncts(src.join_condition);
      all_conjuncts.insert(all_conjuncts.end(), jc.begin(), jc.end());
    }
  }

  std::vector<Expression> non_dim_conjuncts;
  Expression fk_expr;
  Expression dim_pk_expr;
  for (const auto& conj : all_conjuncts) {
    bool touches_dim = false;
    if (!collect_col_sources(conj, &touches_dim)) {
      return std::optional<Relation>{};
    }
    if (!touches_dim) {
      non_dim_conjuncts.push_back(conj);
      continue;
    }
    if (fk_expr || !conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals || !bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto lres =
        find_source_for_col(bin.Left()->AsColumnValue().GetColumnName());
    const auto rres =
        find_source_for_col(bin.Right()->AsColumnValue().GetColumnName());
    if (!lres || !rres) {
      return std::optional<Relation>{};
    }
    std::pair<size_t, slot_t> dim_res;
    std::pair<size_t, slot_t> fk_res;
    if (lres->first == *dim_idx && rres->first != *dim_idx) {
      dim_pk_expr = bin.Left();
      fk_expr = bin.Right();
      dim_res = *lres;
      fk_res = *rres;
    } else if (rres->first == *dim_idx && lres->first != *dim_idx) {
      dim_pk_expr = bin.Right();
      fk_expr = bin.Left();
      dim_res = *rres;
      fk_res = *lres;
    } else {
      return std::optional<Relation>{};
    }
    const Column& dcol = src_schemas[dim_res.first].GetColumn(dim_res.second);
    const Column& fcol = src_schemas[fk_res.first].GetColumn(fk_res.second);
    if (dcol.Type() != ValueType::kInt64 || dcol.IsUnsigned() ||
        fcol.Type() != ValueType::kInt64 || fcol.IsUnsigned()) {
      return std::optional<Relation>{};
    }
  }
  if (!fk_expr || !dim_pk_expr) {
    return std::optional<Relation>{};
  }

  // Build the reduced pre-aggregation statement over the non-dimension sources,
  // grouped by the single INT64 foreign key.
  SelectStatement pre_partial(part);
  std::vector<SelectSource> pre_sources;
  pre_sources.reserve(sources.size() - 1);
  for (size_t i = 0; i < sources.size(); ++i) {
    if (i == *dim_idx) {
      continue;
    }
    SelectSource cp = sources[i];
    if (pre_sources.empty()) {
      cp.join_type = JoinType::kInner;
    }
    cp.join_condition = Expression{};
    pre_sources.push_back(std::move(cp));
  }
  pre_partial.SetSources(std::move(pre_sources));
  pre_partial.SetWhereClause(non_dim_conjuncts.empty()
                                 ? Expression{}
                                 : CombineConjuncts(non_dim_conjuncts));
  pre_partial.SetGroupBy({fk_expr});
  std::vector<NamedExpression> pre_select;
  pre_select.reserve(1 + num_aggs);
  pre_select.emplace_back("__surrogate_key", fk_expr);
  for (size_t i = num_keys; i < part.SelectList().size(); ++i) {
    pre_select.push_back(part.SelectList()[i]);
  }
  pre_partial.SetSelectList(std::move(pre_select));

  GroupedJoinFusionSpec pre_fusion = BuildGroupedJoinFusionSpec(pre_partial);
  if (!pre_fusion.eligible) {
    return std::optional<Relation>{};
  }
  pre_fusion.require_exact_integer_totals = !partial.allow_float_sums;
  pre_fusion.columnar_output = false;

  // Scan the dimension table once (projected to PK + GROUP BY columns) and
  // verify non-NULL primary-key uniqueness before running the fact scan.
  SelectStatement dim_scan;
  SelectSource dim_src = sources[*dim_idx];
  dim_src.join_type = JoinType::kInner;
  dim_src.join_condition = Expression{};
  dim_scan.SetSources({std::move(dim_src)});
  std::vector<NamedExpression> dim_select;
  dim_select.reserve(1 + num_keys);
  dim_select.emplace_back("__dim_pk", dim_pk_expr);
  for (size_t k = 0; k < num_keys; ++k) {
    dim_select.emplace_back("__dim_k_" + std::to_string(k), head.GroupBy()[k]);
  }
  dim_scan.SetSelectList(std::move(dim_select));
  bool dim_where_applied = false;
  ASSIGN_OR_RETURN(
      Relation, dim_rel,
      (BuildInput(context, dim_scan, nullptr, ctes, &dim_where_applied, nullptr,
                  /*ignore_projection_star=*/false,
                  /*columnar_output=*/false)));
  if (!dim_where_applied && dim_scan.WhereClause()) {
    RETURN_IF_FAIL(FilterRelation(context, &dim_rel,
                                  SplitConjuncts(dim_scan.WhereClause()),
                                  nullptr, ctes));
  }
  RETURN_IF_FAIL(dim_rel.EnsureRowsMaterialized());
  if (dim_rel.HasSpill() || dim_rel.rows.size() > kMaxDimRows) {
    return std::optional<Relation>{};
  }

  const auto dim_pk_off = LocalColumnOffset(
      dim_rel.schema, dim_pk_expr->AsColumnValue().GetColumnName());
  if (!dim_pk_off) {
    return std::optional<Relation>{};
  }
  std::vector<slot_t> dim_key_offs;
  dim_key_offs.reserve(num_keys);
  for (size_t k = 0; k < num_keys; ++k) {
    const auto off = LocalColumnOffset(
        dim_rel.schema, head.GroupBy()[k]->AsColumnValue().GetColumnName());
    if (!off) {
      return std::optional<Relation>{};
    }
    dim_key_offs.push_back(*off);
  }

  // Fallback cardinality check when the dimension table wasn't already
  // dictionary-encoded in LSTableStore: sample up to 512 evenly spaced rows.
  // If < 25% of sampled rows have distinct GROUP BY tuples, the attribute
  // cardinality is much smaller than the surrogate PK, so let FusedGroupedJoin
  // aggregate directly on the dimension attributes.
  if (dim_rel.rows.size() >= 2048) {
    constexpr size_t kSampleRows = 512;
    const size_t stride = dim_rel.rows.size() / kSampleRows;
    std::unordered_set<uint64_t> seen_hashes;
    seen_hashes.reserve(kSampleRows);
    for (size_t s = 0; s < kSampleRows; ++s) {
      const Row& dr = dim_rel.rows[s * stride];
      uint64_t h = 14695981039346656037ULL;
      for (slot_t off : dim_key_offs) {
        h ^= static_cast<uint64_t>(std::hash<Value>{}(dr[off]));
        h *= 1099511628211ULL;
      }
      seen_hashes.insert(h);
    }
    if (seen_hashes.size() < kSampleRows / 4) {
      return std::optional<Relation>{};
    }
  }

  FlatIntKeyMap<size_t> dim_pk_to_row;
  dim_pk_to_row.Reserve(dim_rel.rows.size());
  int64_t min_pk = std::numeric_limits<int64_t>::max();
  int64_t max_pk = std::numeric_limits<int64_t>::min();
  for (size_t r = 0; r < dim_rel.rows.size(); ++r) {
    const Value& pk_val = dim_rel.rows[r][*dim_pk_off];
    if (pk_val.IsNull()) {
      continue;
    }
    if (pk_val.type != ValueType::kInt64 || pk_val.IsUnsigned()) {
      return std::optional<Relation>{};
    }
    const int64_t pk = pk_val.value.int_value;
    bool emplaced = false;
    size_t* slot = dim_pk_to_row.TryEmplace(pk, &emplaced);
    if (!emplaced) {
      // Duplicate non-NULL key in dimension table: inner join has fanout > 1,
      // so surrogate pre-aggregation is not sound. Fall back to standard path.
      return std::optional<Relation>{};
    }
    *slot = r;
    min_pk = std::min(min_pk, pk);
    max_pk = std::max(max_pk, pk);
  }

  // Dense-array direct parallel scan fast path: when the remaining pre_partial
  // joins one fact table against filter-only dimension table(s) with unique
  // INT64 keys and the grouping dimension's PK domain is compact (<= 500k),
  // scan the fact table's LSTableStore blocks directly with zone-map pruning
  // and per-worker dense arrays indexed by (fk - min_pk) — zero hash probes.
  const auto fk_resolved =
      find_source_for_col(fk_expr->AsColumnValue().GetColumnName());
  if (fk_resolved && max_pk >= min_pk &&
      std::cmp_less_equal(static_cast<uint64_t>(max_pk - min_pk),
                          kMaxDimRows)) {
    const size_t fact_idx = fk_resolved->first;
    const slot_t fact_fk_slot = fk_resolved->second;
    struct DenseAggSpec {
      AggregationType type{AggregationType::kSum};
      slot_t fact_col{0};
      ValueType col_type{ValueType::kInt64};
      std::string out_name;
    };
    std::vector<DenseAggSpec> dense_aggs;
    dense_aggs.reserve(num_aggs);
    bool dense_ok = true;
    for (size_t i = num_keys; i < part.SelectList().size(); ++i) {
      const auto& item = part.SelectList()[i];
      if (!item.expression ||
          item.expression->Type() != TypeTag::kAggregateExp) {
        dense_ok = false;
        break;
      }
      const auto& agg = item.expression->AsAggregateExpression();
      if ((agg.GetType() != AggregationType::kSum &&
           agg.GetType() != AggregationType::kCount) ||
          agg.Distinct() || agg.WhereFilter() || !agg.Child() ||
          agg.Child()->Type() != TypeTag::kColumnValue) {
        dense_ok = false;
        break;
      }
      const auto ares =
          find_source_for_col(agg.Child()->AsColumnValue().GetColumnName());
      if (!ares || ares->first != fact_idx) {
        dense_ok = false;
        break;
      }
      const Column& acol = src_schemas[fact_idx].GetColumn(ares->second);
      if ((acol.Type() != ValueType::kInt64 &&
           acol.Type() != ValueType::kDouble) ||
          acol.IsUnsigned()) {
        dense_ok = false;
        break;
      }
      dense_aggs.push_back(DenseAggSpec{
          .type = agg.GetType(),
          .fact_col = ares->second,
          .col_type = acol.Type(),
          .out_name = item.name,
      });
    }

    struct FilterDimSpec {
      size_t src_idx{0};
      slot_t fact_join_col{0};
      slot_t dim_pk_col{0};
      Expression dim_pk_expr;
      std::vector<Expression> dim_filters;
      int64_t min_d{std::numeric_limits<int64_t>::max()};
      int64_t max_d{std::numeric_limits<int64_t>::min()};
      bool is_contiguous{false};
      std::vector<uint8_t> bitmap;
      FlatIntKeyMap<uint8_t> key_set;
    };
    std::vector<FilterDimSpec> filter_dims;
    if (dense_ok) {
      std::unordered_map<size_t, size_t> src_to_fdim;
      for (size_t si = 0; si < sources.size(); ++si) {
        if (si == *dim_idx || si == fact_idx) {
          continue;
        }
        src_to_fdim[si] = filter_dims.size();
        FilterDimSpec fd_init;
        fd_init.src_idx = si;
        filter_dims.push_back(std::move(fd_init));
      }
      if (filter_dims.empty()) {
        dense_ok = false;
      }
      for (const auto& conj : non_dim_conjuncts) {
        if (!dense_ok) {
          break;
        }
        std::unordered_set<size_t> touched;
        bool ok_c = true;
        const std::function<void(const Expression&)> walk_c =
            [&](const Expression& e) {
              if (!e || !ok_c) {
                return;
              }
              if (e->Type() == TypeTag::kQueryExp) {
                ok_c = false;
                return;
              }
              if (e->Type() == TypeTag::kColumnValue) {
                const auto res =
                    find_source_for_col(e->AsColumnValue().GetColumnName());
                if (!res) {
                  ok_c = false;
                  return;
                }
                touched.insert(res->first);
                return;
              }
              for (const auto& ch : ExpressionChildren(e)) {
                walk_c(ch);
              }
            };
        walk_c(conj);
        if (!ok_c || touched.empty()) {
          dense_ok = false;
          break;
        }
        if (touched.size() == 1) {
          const size_t only_src = *touched.begin();
          if (only_src == fact_idx) {
            dense_ok = false;
            break;
          }
          filter_dims[src_to_fdim[only_src]].dim_filters.push_back(conj);
          continue;
        }
        if (touched.size() == 2 && touched.contains(fact_idx) &&
            conj->Type() == TypeTag::kBinaryExp) {
          const auto& bin = conj->AsBinaryExpression();
          if (bin.Op() != BinaryOperation::kEquals || !bin.Left() ||
              !bin.Right() || bin.Left()->Type() != TypeTag::kColumnValue ||
              bin.Right()->Type() != TypeTag::kColumnValue) {
            dense_ok = false;
            break;
          }
          const auto lres =
              find_source_for_col(bin.Left()->AsColumnValue().GetColumnName());
          const auto rres =
              find_source_for_col(bin.Right()->AsColumnValue().GetColumnName());
          if (!lres || !rres) {
            dense_ok = false;
            break;
          }
          std::pair<size_t, slot_t> f_side;
          std::pair<size_t, slot_t> d_side;
          Expression d_expr;
          if (lres->first == fact_idx && rres->first != fact_idx) {
            f_side = *lres;
            d_side = *rres;
            d_expr = bin.Right();
          } else if (rres->first == fact_idx && lres->first != fact_idx) {
            f_side = *rres;
            d_side = *lres;
            d_expr = bin.Left();
          } else {
            dense_ok = false;
            break;
          }
          const Column& fc = src_schemas[f_side.first].GetColumn(f_side.second);
          const Column& dc = src_schemas[d_side.first].GetColumn(d_side.second);
          if (fc.Type() != ValueType::kInt64 || fc.IsUnsigned() ||
              dc.Type() != ValueType::kInt64 || dc.IsUnsigned()) {
            dense_ok = false;
            break;
          }
          FilterDimSpec& fd = filter_dims[src_to_fdim[d_side.first]];
          if (fd.dim_pk_expr != nullptr) {
            dense_ok = false;
            break;
          }
          fd.fact_join_col = f_side.second;
          fd.dim_pk_col = d_side.second;
          fd.dim_pk_expr = d_expr;
          continue;
        }
        dense_ok = false;
        break;
      }
      for (const auto& fd : filter_dims) {
        if (!fd.dim_pk_expr) {
          dense_ok = false;
          break;
        }
      }
    }

    if (dense_ok) {
      bool any_empty_filter_dim = false;
      for (auto& fd : filter_dims) {
        SelectStatement fd_scan;
        SelectSource fd_src = sources[fd.src_idx];
        fd_src.join_type = JoinType::kInner;
        fd_src.join_condition = Expression{};
        fd_scan.SetSources({std::move(fd_src)});
        fd_scan.SetSelectList({NamedExpression("__fd_pk", fd.dim_pk_expr)});
        if (!fd.dim_filters.empty()) {
          fd_scan.SetWhereClause(CombineConjuncts(fd.dim_filters));
        }
        bool fd_where_applied = false;
        ASSIGN_OR_RETURN(
            Relation, fd_rel,
            (BuildInput(context, fd_scan, nullptr, ctes, &fd_where_applied,
                        nullptr, /*ignore_projection_star=*/false,
                        /*columnar_output=*/false)));
        if (!fd_where_applied && fd_scan.WhereClause()) {
          RETURN_IF_FAIL(FilterRelation(context, &fd_rel,
                                        SplitConjuncts(fd_scan.WhereClause()),
                                        nullptr, ctes));
        }
        RETURN_IF_FAIL(fd_rel.EnsureRowsMaterialized());
        if (fd_rel.HasSpill()) {
          dense_ok = false;
          break;
        }
        const auto fd_pk_off = LocalColumnOffset(
            fd_rel.schema, fd.dim_pk_expr->AsColumnValue().GetColumnName());
        if (!fd_pk_off) {
          dense_ok = false;
          break;
        }
        fd.key_set.Reserve(fd_rel.rows.size());
        for (const Row& r : fd_rel.rows) {
          const Value& v = r[*fd_pk_off];
          if (v.IsNull()) {
            continue;
          }
          if (v.type != ValueType::kInt64 || v.IsUnsigned()) {
            dense_ok = false;
            break;
          }
          const int64_t dv = v.value.int_value;
          bool emplaced = false;
          uint8_t* sl = fd.key_set.TryEmplace(dv, &emplaced);
          if (!emplaced) {
            // Duplicate non-NULL key in filter dim: fanout > 1 -> fallback.
            dense_ok = false;
            break;
          }
          *sl = 1;
          fd.min_d = std::min(fd.min_d, dv);
          fd.max_d = std::max(fd.max_d, dv);
        }
        if (!dense_ok) {
          break;
        }
        if (fd.key_set.Size() == 0) {
          any_empty_filter_dim = true;
          break;
        }
        const uint64_t span = static_cast<uint64_t>(fd.max_d - fd.min_d) + 1;
        if (span == fd.key_set.Size()) {
          fd.is_contiguous = true;
        } else if (span <= 1048576) {
          fd.bitmap.assign(static_cast<size_t>(span), 0);
          fd.key_set.ForEach([&](int64_t k, uint8_t) {
            fd.bitmap[static_cast<size_t>(k - fd.min_d)] = 1;
          });
        }
      }

      if (dense_ok && any_empty_filter_dim) {
        std::vector<Column> out_columns;
        out_columns.reserve(num_keys + num_aggs);
        for (size_t k = 0; k < num_keys; ++k) {
          Column col = dim_rel.schema.GetColumn(dim_key_offs[k]);
          col.Name() = head.GroupBy()[k]->AsColumnValue().GetColumnName();
          out_columns.push_back(std::move(col));
        }
        for (const auto& da : dense_aggs) {
          out_columns.emplace_back(da.out_name,
                                   da.type == AggregationType::kCount
                                       ? ValueType::kInt64
                                       : da.col_type);
        }
        Relation empty_out(context.execution_runtime());
        empty_out.schema = Schema("", std::move(out_columns));
        return std::optional<Relation>(std::move(empty_out));
      }

      if (dense_ok) {
        std::vector<slot_t> fact_needed{fact_fk_slot};
        auto add_fact_col = [&](slot_t c) {
          if (std::ranges::find(fact_needed, c) == fact_needed.end()) {
            fact_needed.push_back(c);
          }
        };
        for (const auto& fd : filter_dims) {
          add_fact_col(fd.fact_join_col);
        }
        for (const auto& da : dense_aggs) {
          add_fact_col(da.fact_col);
        }
        std::shared_ptr<const LSTableStore> fact_store =
            GetOrBuildLSTableStore(context.txn_, *tables[fact_idx],
                                   /*allow_lazy_build=*/true, &fact_needed,
                                   /*allow_block_lazy=*/false);
        if (fact_store && !fact_store->blocks.empty() &&
            std::ranges::all_of(fact_needed, [&](slot_t c) {
              return fact_store->HasColumn(c);
            })) {
          const size_t pk_range = static_cast<size_t>(max_pk - min_pk) + 1;
          std::vector<int32_t> dense_pk_to_row(pk_range, -1);
          dim_pk_to_row.ForEach([&](int64_t pk, size_t r) {
            dense_pk_to_row[static_cast<size_t>(pk - min_pk)] =
                static_cast<int32_t>(r);
          });

          const size_t num_blocks = fact_store->blocks.size();
          const size_t workers =
              std::max<size_t>(1, ParallelWorkerCount(num_blocks));
          struct DenseWorker {
            std::vector<uint8_t> seen;
            std::vector<std::vector<__int128>> int_sums;
            std::vector<std::vector<double>> dbl_sums;
            std::vector<std::vector<int64_t>> cnts;
          };
          std::vector<DenseWorker> ws(workers);
          for (auto& w : ws) {
            w.seen.assign(pk_range, 0);
            w.int_sums.resize(num_aggs);
            w.dbl_sums.resize(num_aggs);
            w.cnts.resize(num_aggs);
            for (size_t a = 0; a < num_aggs; ++a) {
              w.cnts[a].assign(pk_range, 0);
              if (dense_aggs[a].type == AggregationType::kSum) {
                if (dense_aggs[a].col_type == ValueType::kInt64) {
                  w.int_sums[a].assign(pk_range, 0);
                } else {
                  w.dbl_sums[a].assign(pk_range, 0.0);
                }
              }
            }
          }

          auto dense_scan = [&](size_t w) {
            DenseWorker& dw = ws[w];
            std::vector<uint8_t> fd_all_pass(filter_dims.size(), 0);
            for (size_t b_idx = w; b_idx < num_blocks; b_idx += workers) {
              const LSTablePageBlock& blk = fact_store->blocks[b_idx];
              const uint32_t nrows = blk.row_count;
              if (nrows == 0) {
                continue;
              }
              bool skip_blk = false;
              for (size_t fi = 0; fi < filter_dims.size(); ++fi) {
                const FilterDimSpec& fd = filter_dims[fi];
                const LSTableColumnBlock& fcol = blk.columns[fd.fact_join_col];
                if (fcol.has_min_max) {
                  if (fcol.max_int < fd.min_d || fcol.min_int > fd.max_d) {
                    skip_blk = true;
                    break;
                  }
                  fd_all_pass[fi] =
                      (fd.is_contiguous && !fcol.has_null &&
                       fcol.min_int >= fd.min_d && fcol.max_int <= fd.max_d)
                          ? 1
                          : 0;
                } else {
                  fd_all_pass[fi] = 0;
                }
              }
              if (skip_blk) {
                continue;
              }
              const LSTableColumnBlock& fk_blk = blk.columns[fact_fk_slot];
              if (fk_blk.has_min_max &&
                  (fk_blk.max_int < min_pk || fk_blk.min_int > max_pk)) {
                continue;
              }
              const bool all_dims_pass = std::ranges::all_of(
                  fd_all_pass, [](uint8_t v) { return v != 0; });
              const int64_t* __restrict fk_raw =
                  fk_blk.HasRawIntCells() ? fk_blk.int_cells.data() : nullptr;
              const bool fk_has_null = fk_blk.has_null;

              // Fast specialized loop for single INT64 SUM+COUNT (Q22) when all
              // filter dims pass the entire block.
              if (all_dims_pass && num_aggs == 2 &&
                  dense_aggs[0].type == AggregationType::kSum &&
                  dense_aggs[0].col_type == ValueType::kInt64 &&
                  dense_aggs[1].type == AggregationType::kCount &&
                  dense_aggs[1].fact_col == dense_aggs[0].fact_col &&
                  fk_raw != nullptr &&
                  blk.columns[dense_aggs[0].fact_col].HasRawIntCells()) {
                const LSTableColumnBlock& a_blk =
                    blk.columns[dense_aggs[0].fact_col];
                const int64_t* __restrict a_raw = a_blk.int_cells.data();
                const bool a_has_null = a_blk.has_null;
                __int128* __restrict sum_ptr = dw.int_sums[0].data();
                int64_t* __restrict cnt0_ptr = dw.cnts[0].data();
                int64_t* __restrict cnt1_ptr = dw.cnts[1].data();
                uint8_t* __restrict seen_ptr = dw.seen.data();
                for (uint32_t r = 0; r < nrows; ++r) {
                  if (fk_has_null && fk_blk.IsNull(r)) {
                    continue;
                  }
                  const int64_t sk = fk_raw[r];
                  if (sk < min_pk || sk > max_pk) {
                    continue;
                  }
                  const auto idx = static_cast<size_t>(sk - min_pk);
                  if (dense_pk_to_row[idx] < 0) {
                    continue;
                  }
                  seen_ptr[idx] = 1;
                  if (!a_has_null || !a_blk.IsNull(r)) {
                    sum_ptr[idx] += a_raw[r];
                    ++cnt0_ptr[idx];
                    ++cnt1_ptr[idx];
                  }
                }
                continue;
              }

              for (uint32_t r = 0; r < nrows; ++r) {
                if (!all_dims_pass) {
                  bool row_ok = true;
                  for (size_t fi = 0; fi < filter_dims.size(); ++fi) {
                    if (fd_all_pass[fi]) {
                      continue;
                    }
                    const FilterDimSpec& fd = filter_dims[fi];
                    const LSTableColumnBlock& fcol =
                        blk.columns[fd.fact_join_col];
                    if (fcol.has_null && fcol.IsNull(r)) {
                      row_ok = false;
                      break;
                    }
                    const int64_t dv = fcol.IntAt(r);
                    if (dv < fd.min_d || dv > fd.max_d) {
                      row_ok = false;
                      break;
                    }
                    if (!fd.is_contiguous) {
                      if (!fd.bitmap.empty()) {
                        if (fd.bitmap[static_cast<size_t>(dv - fd.min_d)] ==
                            0) {
                          row_ok = false;
                          break;
                        }
                      } else if (fd.key_set.Find(dv) == nullptr) {
                        row_ok = false;
                        break;
                      }
                    }
                  }
                  if (!row_ok) {
                    continue;
                  }
                }
                if (fk_has_null && fk_blk.IsNull(r)) {
                  continue;
                }
                const int64_t sk =
                    fk_raw != nullptr ? fk_raw[r] : fk_blk.IntAt(r);
                if (sk < min_pk || sk > max_pk) {
                  continue;
                }
                const auto idx = static_cast<size_t>(sk - min_pk);
                if (dense_pk_to_row[idx] < 0) {
                  continue;
                }
                dw.seen[idx] = 1;
                for (size_t a = 0; a < num_aggs; ++a) {
                  const LSTableColumnBlock& a_blk =
                      blk.columns[dense_aggs[a].fact_col];
                  if (a_blk.has_null && a_blk.IsNull(r)) {
                    continue;
                  }
                  ++dw.cnts[a][idx];
                  if (dense_aggs[a].type == AggregationType::kSum) {
                    if (dense_aggs[a].col_type == ValueType::kInt64) {
                      dw.int_sums[a][idx] += a_blk.IntAt(r);
                    } else {
                      const int64_t raw = a_blk.IntAt(r);
                      double dv = 0.0;
                      std::memcpy(&dv, &raw, sizeof(dv));
                      dw.dbl_sums[a][idx] += dv;
                    }
                  }
                }
              }
            }
          };

          if (RunParallelTasks(workers, dense_scan) == Status::kSuccess) {
            bool overflow = false;
            auto& m = ws[0];
            for (size_t w = 1; w < workers; ++w) {
              for (size_t idx = 0; idx < pk_range; ++idx) {
                if (ws[w].seen[idx] == 0) {
                  continue;
                }
                m.seen[idx] = 1;
                for (size_t a = 0; a < num_aggs; ++a) {
                  m.cnts[a][idx] += ws[w].cnts[a][idx];
                  if (dense_aggs[a].type == AggregationType::kSum) {
                    if (dense_aggs[a].col_type == ValueType::kInt64) {
                      m.int_sums[a][idx] += ws[w].int_sums[a][idx];
                    } else {
                      m.dbl_sums[a][idx] += ws[w].dbl_sums[a][idx];
                    }
                  }
                }
              }
            }
            for (size_t a = 0; a < num_aggs && !overflow; ++a) {
              if (dense_aggs[a].type == AggregationType::kSum &&
                  dense_aggs[a].col_type == ValueType::kInt64) {
                for (size_t idx = 0; idx < pk_range; ++idx) {
                  if (m.seen[idx] != 0 && m.cnts[a][idx] > 0 &&
                      (m.int_sums[a][idx] <
                           static_cast<__int128>(
                               std::numeric_limits<int64_t>::min()) ||
                       m.int_sums[a][idx] >
                           static_cast<__int128>(
                               std::numeric_limits<int64_t>::max()))) {
                    overflow = true;
                    break;
                  }
                }
              }
            }
            if (!overflow) {
              std::vector<Column> out_columns;
              out_columns.reserve(num_keys + num_aggs);
              for (size_t k = 0; k < num_keys; ++k) {
                Column col = dim_rel.schema.GetColumn(dim_key_offs[k]);
                col.Name() = head.GroupBy()[k]->AsColumnValue().GetColumnName();
                out_columns.push_back(std::move(col));
              }
              for (const auto& da : dense_aggs) {
                out_columns.emplace_back(da.out_name,
                                         da.type == AggregationType::kCount
                                             ? ValueType::kInt64
                                             : da.col_type);
              }
              Relation out(context.execution_runtime());
              out.schema = Schema("", std::move(out_columns));
              for (size_t idx = 0; idx < pk_range; ++idx) {
                if (m.seen[idx] == 0) {
                  continue;
                }
                const int32_t dim_r = dense_pk_to_row[idx];
                if (dim_r < 0) {
                  continue;
                }
                const Row& dr = dim_rel.rows[static_cast<size_t>(dim_r)];
                std::vector<Value> vals;
                vals.reserve(num_keys + num_aggs);
                for (size_t k = 0; k < num_keys; ++k) {
                  vals.push_back(dr[dim_key_offs[k]]);
                }
                for (size_t a = 0; a < num_aggs; ++a) {
                  if (dense_aggs[a].type == AggregationType::kCount) {
                    vals.emplace_back(m.cnts[a][idx]);
                  } else if (m.cnts[a][idx] == 0) {
                    vals.emplace_back();
                  } else if (dense_aggs[a].col_type == ValueType::kInt64) {
                    vals.emplace_back(static_cast<int64_t>(m.int_sums[a][idx]));
                  } else {
                    vals.emplace_back(m.dbl_sums[a][idx]);
                  }
                }
                out.AddRow(Row(std::move(vals)));
              }
              RETURN_IF_FAIL(out.GetStatus());
              return std::optional<Relation>(std::move(out));
            }
          }
        }
      }
    }
  }

  bool pre_where_applied = false;
  ASSIGN_OR_RETURN(
      Relation, pre_rel,
      (BuildInput(context, pre_partial, nullptr, ctes, &pre_where_applied,
                  &pre_fusion, /*ignore_projection_star=*/false,
                  /*columnar_output=*/false)));
  if (!pre_fusion.fired || !pre_where_applied) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(pre_rel.EnsureRowsMaterialized());
  if (pre_rel.HasSpill() || pre_rel.schema.ColumnCount() != 1 + num_aggs) {
    return std::optional<Relation>{};
  }

  // Join the compact surrogate-keyed partial totals with the dimension table.
  std::vector<Column> out_columns;
  out_columns.reserve(num_keys + num_aggs);
  for (size_t k = 0; k < num_keys; ++k) {
    Column col = dim_rel.schema.GetColumn(dim_key_offs[k]);
    col.Name() = head.GroupBy()[k]->AsColumnValue().GetColumnName();
    out_columns.push_back(std::move(col));
  }
  for (size_t a = 0; a < num_aggs; ++a) {
    out_columns.push_back(pre_rel.schema.GetColumn(1 + a));
  }

  Relation out(context.execution_runtime());
  out.schema = Schema("", std::move(out_columns));
  out.rows.reserve(pre_rel.rows.size());
  for (const Row& pr : pre_rel.rows) {
    const Value& fk_val = pr[0];
    if (fk_val.IsNull() || fk_val.type != ValueType::kInt64 ||
        fk_val.IsUnsigned()) {
      continue;
    }
    const size_t* dim_row_idx = dim_pk_to_row.Find(fk_val.value.int_value);
    if (dim_row_idx == nullptr) {
      continue;
    }
    const Row& dr = dim_rel.rows[*dim_row_idx];
    std::vector<Value> vals;
    vals.reserve(num_keys + num_aggs);
    for (size_t k = 0; k < num_keys; ++k) {
      vals.push_back(dr[dim_key_offs[k]]);
    }
    for (size_t a = 0; a < num_aggs; ++a) {
      vals.push_back(pr[1 + a]);
    }
    out.AddRow(Row(std::move(vals)));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
//   (SingleScanConditionalCountDistinct_sound)
// Collapse a cross join of K scalar-aggregate derived tables over the same base
// table T (e.g. TPC-DS Q28's 6 buckets B1..B6 over store_sales computing
// AVG/COUNT/COUNT(DISTINCT) under disjoint ss_quantity ranges and OR-ed numeric
// range predicates) into a single parallel columnar scan of T with per-worker
// accumulators and distinct sets.
StatusOr<std::optional<Relation>> TryExecuteMultiBucketScalarCrossJoin(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SINGLE_SCAN_BUCKETS") !=
      nullptr;
  if (disabled) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  if (sources.size() < 2 || sources.size() > 16 || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() ||
      !statement.OrderBy().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Distinct() ||
      statement.HasDistinctOn() || statement.Offset() != 0 ||
      !statement.WithQueries().empty() ||
      (statement.HasLimit() && statement.Limit() == 0)) {
    return std::optional<Relation>{};
  }
  // Require SELECT * (single star item).
  if (statement.SelectList().size() != 1 ||
      !statement.SelectList()[0].expression ||
      statement.SelectList()[0].expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  const ColumnName& star_cn =
      statement.SelectList()[0].expression->AsColumnValue().GetColumnName();
  if (star_cn.name != "*" || !star_cn.schema.empty()) {
    return std::optional<Relation>{};
  }

  // Verify every source is a 1-table scalar-aggregate derived table over the
  // same base table T.
  std::string shared_table_lower;
  std::string shared_table_name;
  for (size_t i = 0; i < sources.size(); ++i) {
    const auto& src = sources[i];
    if (!src.query || !src.table.empty() || src.unnest != nullptr ||
        src.is_lateral || src.sample.has_value() ||
        !src.using_columns.empty() || src.join_condition != nullptr ||
        (i > 0 && src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner)) {
      return std::optional<Relation>{};
    }
    const SelectStatement& sub = *src.query;
    if (sub.Sources().size() != 1 || !sub.WhereClause() ||
        !sub.GroupBy().empty() || sub.IsGroupAll() || sub.Having() ||
        sub.Qualify() || !sub.OrderBy().empty() || !sub.UnionAll().empty() ||
        sub.GetSetOperationTree() != nullptr || sub.Distinct() ||
        sub.HasDistinctOn() || sub.HasLimit() || sub.Offset() != 0 ||
        !sub.WithQueries().empty() || sub.SelectList().empty()) {
      return std::optional<Relation>{};
    }
    const auto& inner_src = sub.Sources()[0];
    if (inner_src.table.empty() || inner_src.query != nullptr ||
        inner_src.unnest != nullptr || inner_src.is_lateral ||
        inner_src.sample.has_value() || !inner_src.using_columns.empty()) {
      return std::optional<Relation>{};
    }
    const std::string t_lower = AsciiToLower(inner_src.table);
    if (ctes.contains(t_lower)) {
      return std::optional<Relation>{};
    }
    if (i == 0) {
      shared_table_lower = t_lower;
      shared_table_name = inner_src.table;
    } else if (t_lower != shared_table_lower) {
      return std::optional<Relation>{};
    }
  }

  auto tbl_or = context.GetTable(shared_table_name);
  if (!tbl_or.HasValue() || !tbl_or.Value()) {
    return std::optional<Relation>{};
  }
  const std::shared_ptr<Table>& table = tbl_or.Value();
  const Schema& schema = table->GetSchema();

  auto resolve_numeric_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<slot_t, ValueType>> {
    const std::string want = AsciiToLower(cn.name);
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (AsciiToLower(schema.GetColumn(c).Name().name) == want) {
        const Column& col = schema.GetColumn(c);
        if ((col.Type() == ValueType::kInt64 ||
             col.Type() == ValueType::kDouble) &&
            !col.IsUnsigned()) {
          return std::make_pair(c, col.Type());
        }
        return std::nullopt;
      }
    }
    return std::nullopt;
  };

  // Constant-fold simple numeric literals and +, -, * arithmetic (e.g. 135+10).
  const std::function<std::optional<double>(const Expression&)> eval_num_const =
      [&](const Expression& e) -> std::optional<double> {
    if (!e) {
      return std::nullopt;
    }
    if (e->Type() == TypeTag::kConstantValue) {
      const Value& v = e->AsConstantValue().GetValue();
      if (v.IsNull() || v.IsUnsigned()) {
        return std::nullopt;
      }
      if (v.type == ValueType::kInt64) {
        return static_cast<double>(v.value.int_value);
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      return std::nullopt;
    }
    if (e->Type() == TypeTag::kUnaryExp) {
      const auto& u = e->AsUnaryExpression();
      if (u.Op() == UnaryOperation::kMinus) {
        auto v = eval_num_const(u.Child());
        return v ? std::optional<double>(-*v) : std::nullopt;
      }
      return std::nullopt;
    }
    if (e->Type() == TypeTag::kBinaryExp) {
      const auto& b = e->AsBinaryExpression();
      auto l = eval_num_const(b.Left());
      auto r = eval_num_const(b.Right());
      if (!l || !r) {
        return std::nullopt;
      }
      switch (b.Op()) {
        case BinaryOperation::kAdd:
          return *l + *r;
        case BinaryOperation::kSubtract:
          return *l - *r;
        case BinaryOperation::kMultiply:
          return *l * *r;
        default:
          return std::nullopt;
      }
    }
    return std::nullopt;
  };

  struct PredNode {
    enum class Kind : uint8_t { kAnd, kOr, kCmp };
    Kind kind{Kind::kCmp};
    size_t left{0};
    size_t right{0};
    slot_t col{0};
    ValueType type{ValueType::kInt64};
    BinaryOperation op{BinaryOperation::kEquals};
    double rhs{0.0};
    int64_t rhs_int{0};
    bool rhs_is_exact_int{false};
  };

  struct AggOutSpec {
    enum class Kind : uint8_t { kAvg, kSum, kCount, kCountDistinct };
    Kind kind{Kind::kCount};
    slot_t col{0};
    ValueType col_type{ValueType::kDouble};
    std::string out_name;
    size_t metric_idx{0};  // index into bucket's deduplicated metric list
  };

  struct MetricSpec {
    slot_t col{0};
    ValueType col_type{ValueType::kDouble};
    bool need_sum{false};
    bool need_count{false};
    bool need_distinct{false};
  };

  struct BucketPlan {
    std::vector<PredNode> pred_nodes;
    size_t pred_root{0};
    bool has_dispatch_range{false};
    slot_t dispatch_col{0};
    int64_t dispatch_lo{0};
    int64_t dispatch_hi{0};
    std::vector<MetricSpec> metrics;
    std::vector<AggOutSpec> outputs;
  };

  std::vector<slot_t> needed_cols;
  auto add_needed = [&](slot_t c) {
    if (std::ranges::find(needed_cols, c) == needed_cols.end()) {
      needed_cols.push_back(c);
    }
  };

  auto flip_op = [](BinaryOperation op) -> std::optional<BinaryOperation> {
    switch (op) {
      case BinaryOperation::kEquals:
        return BinaryOperation::kEquals;
      case BinaryOperation::kNotEquals:
        return BinaryOperation::kNotEquals;
      case BinaryOperation::kLessThan:
        return BinaryOperation::kGreaterThan;
      case BinaryOperation::kLessThanEquals:
        return BinaryOperation::kGreaterThanEquals;
      case BinaryOperation::kGreaterThan:
        return BinaryOperation::kLessThan;
      case BinaryOperation::kGreaterThanEquals:
        return BinaryOperation::kLessThanEquals;
      default:
        return std::nullopt;
    }
  };

  std::vector<BucketPlan> buckets(sources.size());
  for (size_t bi = 0; bi < sources.size(); ++bi) {
    const SelectStatement& sub = *sources[bi].query;
    BucketPlan& bp = buckets[bi];

    // Compile SELECT aggregates.
    for (size_t si = 0; si < sub.SelectList().size(); ++si) {
      const auto& item = sub.SelectList()[si];
      if (!item.expression ||
          item.expression->Type() != TypeTag::kAggregateExp) {
        return std::optional<Relation>{};
      }
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.WhereFilter() || agg.Having() != AggregateHavingModifier::kNone ||
          !agg.InnerOrderBy().empty() || agg.SecondaryArg() ||
          !agg.ExtraArgs().empty() || !agg.TrailingArgs().empty() ||
          agg.NeedsGroupContext() || !agg.Child() ||
          agg.Child()->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      const auto col_info =
          resolve_numeric_col(agg.Child()->AsColumnValue().GetColumnName());
      if (!col_info) {
        return std::optional<Relation>{};
      }
      add_needed(col_info->first);

      AggOutSpec out_spec;
      out_spec.col = col_info->first;
      out_spec.col_type = col_info->second;
      out_spec.out_name = ProjectionName(item, si);
      if (agg.GetType() == AggregationType::kAvg && !agg.Distinct()) {
        out_spec.kind = AggOutSpec::Kind::kAvg;
      } else if (agg.GetType() == AggregationType::kSum && !agg.Distinct()) {
        out_spec.kind = AggOutSpec::Kind::kSum;
      } else if (agg.GetType() == AggregationType::kCount && !agg.Distinct()) {
        out_spec.kind = AggOutSpec::Kind::kCount;
      } else if (agg.GetType() == AggregationType::kCount && agg.Distinct()) {
        out_spec.kind = AggOutSpec::Kind::kCountDistinct;
      } else {
        return std::optional<Relation>{};
      }

      size_t midx = bp.metrics.size();
      for (size_t m = 0; m < bp.metrics.size(); ++m) {
        if (bp.metrics[m].col == out_spec.col) {
          midx = m;
          break;
        }
      }
      if (midx == bp.metrics.size()) {
        bp.metrics.push_back(
            MetricSpec{.col = out_spec.col, .col_type = out_spec.col_type});
      }
      if (out_spec.kind == AggOutSpec::Kind::kAvg ||
          out_spec.kind == AggOutSpec::Kind::kSum) {
        bp.metrics[midx].need_sum = true;
      }
      if (out_spec.kind == AggOutSpec::Kind::kAvg ||
          out_spec.kind == AggOutSpec::Kind::kSum ||
          out_spec.kind == AggOutSpec::Kind::kCount) {
        bp.metrics[midx].need_count = true;
      }
      if (out_spec.kind == AggOutSpec::Kind::kCountDistinct) {
        bp.metrics[midx].need_distinct = true;
      }
      out_spec.metric_idx = midx;
      bp.outputs.push_back(std::move(out_spec));
    }

    // Compile WHERE predicate tree.
    const std::function<std::optional<size_t>(const Expression&)> compile_pred =
        [&](const Expression& e) -> std::optional<size_t> {
      if (!e || e->Type() != TypeTag::kBinaryExp) {
        return std::nullopt;
      }
      const auto& b = e->AsBinaryExpression();
      if (b.Op() == BinaryOperation::kAnd || b.Op() == BinaryOperation::kOr) {
        auto l = compile_pred(b.Left());
        auto r = compile_pred(b.Right());
        if (!l || !r) {
          return std::nullopt;
        }
        PredNode node;
        node.kind = (b.Op() == BinaryOperation::kAnd) ? PredNode::Kind::kAnd
                                                      : PredNode::Kind::kOr;
        node.left = *l;
        node.right = *r;
        bp.pred_nodes.push_back(node);
        return bp.pred_nodes.size() - 1;
      }
      std::optional<std::pair<slot_t, ValueType>> col_info;
      std::optional<double> cval;
      BinaryOperation op = b.Op();
      if (b.Left() && b.Left()->Type() == TypeTag::kColumnValue) {
        col_info =
            resolve_numeric_col(b.Left()->AsColumnValue().GetColumnName());
        cval = eval_num_const(b.Right());
      } else if (b.Right() && b.Right()->Type() == TypeTag::kColumnValue) {
        col_info =
            resolve_numeric_col(b.Right()->AsColumnValue().GetColumnName());
        cval = eval_num_const(b.Left());
        auto flipped = flip_op(op);
        if (!flipped) {
          return std::nullopt;
        }
        op = *flipped;
      }
      if (!col_info || !cval || !std::isfinite(*cval)) {
        return std::nullopt;
      }
      if (op != BinaryOperation::kEquals && op != BinaryOperation::kNotEquals &&
          op != BinaryOperation::kLessThan &&
          op != BinaryOperation::kLessThanEquals &&
          op != BinaryOperation::kGreaterThan &&
          op != BinaryOperation::kGreaterThanEquals) {
        return std::nullopt;
      }
      add_needed(col_info->first);
      PredNode node;
      node.kind = PredNode::Kind::kCmp;
      node.col = col_info->first;
      node.type = col_info->second;
      node.op = op;
      node.rhs = *cval;
      if (col_info->second == ValueType::kInt64 &&
          *cval >= static_cast<double>(std::numeric_limits<int64_t>::min()) &&
          *cval <= static_cast<double>(std::numeric_limits<int64_t>::max()) &&
          std::trunc(*cval) == *cval) {
        node.rhs_int = static_cast<int64_t>(*cval);
        node.rhs_is_exact_int = true;
      }
      bp.pred_nodes.push_back(node);
      return bp.pred_nodes.size() - 1;
    };

    // Extract optional top-level INT64 range dispatch conjuncts (e.g.
    // ss_quantity >= lo AND ss_quantity <= hi) so we can reject non-matching
    // rows and dispatch straight to the matching bucket(s).
    std::vector<Expression> top_conj = SplitConjuncts(sub.WhereClause());
    std::unordered_map<slot_t, std::pair<int64_t, int64_t>> int_bounds;
    std::vector<Expression> residual_conj;
    for (const auto& c : top_conj) {
      bool absorbed = false;
      if (c && c->Type() == TypeTag::kBinaryExp) {
        const auto& b = c->AsBinaryExpression();
        std::optional<std::pair<slot_t, ValueType>> ci;
        std::optional<double> cv;
        BinaryOperation op = b.Op();
        if (b.Left() && b.Left()->Type() == TypeTag::kColumnValue) {
          ci = resolve_numeric_col(b.Left()->AsColumnValue().GetColumnName());
          cv = eval_num_const(b.Right());
        } else if (b.Right() && b.Right()->Type() == TypeTag::kColumnValue) {
          ci = resolve_numeric_col(b.Right()->AsColumnValue().GetColumnName());
          cv = eval_num_const(b.Left());
          if (auto fl = flip_op(op)) {
            op = *fl;
          } else {
            ci = std::nullopt;
          }
        }
        if (ci && ci->second == ValueType::kInt64 && cv && std::isfinite(*cv) &&
            std::trunc(*cv) == *cv && *cv >= -9e15 && *cv <= 9e15) {
          const auto iv = static_cast<int64_t>(*cv);
          auto it = int_bounds.find(ci->first);
          if (it == int_bounds.end()) {
            it = int_bounds
                     .emplace(
                         ci->first,
                         std::make_pair(std::numeric_limits<int64_t>::min(),
                                        std::numeric_limits<int64_t>::max()))
                     .first;
          }
          if (op == BinaryOperation::kGreaterThanEquals) {
            it->second.first = std::max(it->second.first, iv);
            absorbed = true;
          } else if (op == BinaryOperation::kGreaterThan) {
            it->second.first = std::max(it->second.first, iv + 1);
            absorbed = true;
          } else if (op == BinaryOperation::kLessThanEquals) {
            it->second.second = std::min(it->second.second, iv);
            absorbed = true;
          } else if (op == BinaryOperation::kLessThan) {
            it->second.second = std::min(it->second.second, iv - 1);
            absorbed = true;
          } else if (op == BinaryOperation::kEquals) {
            it->second.first = std::max(it->second.first, iv);
            it->second.second = std::min(it->second.second, iv);
            absorbed = true;
          }
        }
      }
      if (!absorbed) {
        residual_conj.push_back(c);
      }
    }
    // Check if there is a bounded INT64 column from top-level conjuncts.
    for (const auto& [slot, bounds] : int_bounds) {
      if (bounds.first > std::numeric_limits<int64_t>::min() &&
          bounds.second < std::numeric_limits<int64_t>::max() &&
          bounds.first <= bounds.second) {
        bp.has_dispatch_range = true;
        bp.dispatch_col = slot;
        bp.dispatch_lo = bounds.first;
        bp.dispatch_hi = bounds.second;
        add_needed(slot);
        break;
      }
    }
    // Compile the full WHERE predicate (or residual if dispatch matched).
    Expression to_compile = (bp.has_dispatch_range && int_bounds.size() == 1 &&
                             !residual_conj.empty())
                                ? CombineConjuncts(residual_conj)
                                : sub.WhereClause();
    if (!bp.has_dispatch_range || int_bounds.size() != 1 ||
        residual_conj.empty()) {
      bp.has_dispatch_range = false;
    }
    auto root = compile_pred(to_compile);
    if (!root) {
      return std::optional<Relation>{};
    }
    bp.pred_root = *root;
  }

  // Check if all buckets share the same dispatch INT64 column over a compact
  // range (<= 256 values) for O(1) bitmask dispatch.
  bool use_dispatch = true;
  slot_t dispatch_col = 0;
  int64_t global_lo = std::numeric_limits<int64_t>::max();
  int64_t global_hi = std::numeric_limits<int64_t>::min();
  for (size_t bi = 0; bi < buckets.size(); ++bi) {
    if (!buckets[bi].has_dispatch_range) {
      use_dispatch = false;
      break;
    }
    if (bi == 0) {
      dispatch_col = buckets[bi].dispatch_col;
    } else if (buckets[bi].dispatch_col != dispatch_col) {
      use_dispatch = false;
      break;
    }
    global_lo = std::min(global_lo, buckets[bi].dispatch_lo);
    global_hi = std::max(global_hi, buckets[bi].dispatch_hi);
  }
  std::array<uint16_t, 256> dispatch_lut{};
  bool use_dispatch_lut = false;
  if (use_dispatch && global_hi >= global_lo && (global_hi - global_lo) < 256) {
    use_dispatch_lut = true;
    for (size_t bi = 0; bi < buckets.size(); ++bi) {
      for (int64_t v = buckets[bi].dispatch_lo; v <= buckets[bi].dispatch_hi;
           ++v) {
        dispatch_lut[static_cast<size_t>(v - global_lo)] |=
            static_cast<uint16_t>(uint16_t{1} << bi);
      }
    }
  }

  std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
      context.txn_, *table, /*allow_lazy_build=*/true, &needed_cols,
      /*allow_block_lazy=*/false);
  if (!store || store->blocks.empty()) {
    return std::optional<Relation>{};
  }
  for (slot_t c : needed_cols) {
    if (!store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }

  auto eval_pred_node = [](const std::vector<PredNode>& nodes, size_t idx,
                           const LSTablePageBlock& blk, uint32_t row) -> bool {
    const auto rec = [&](auto&& self, size_t n_idx) -> bool {
      const PredNode& n = nodes[n_idx];
      if (n.kind == PredNode::Kind::kAnd) {
        return self(self, n.left) && self(self, n.right);
      }
      if (n.kind == PredNode::Kind::kOr) {
        return self(self, n.left) || self(self, n.right);
      }
      const LSTableColumnBlock& col = blk.columns[n.col];
      if (col.IsNull(row)) {
        return false;
      }
      if (n.type == ValueType::kInt64 && n.rhs_is_exact_int) {
        const int64_t v = col.IntAt(row);
        switch (n.op) {
          case BinaryOperation::kEquals:
            return v == n.rhs_int;
          case BinaryOperation::kNotEquals:
            return v != n.rhs_int;
          case BinaryOperation::kLessThan:
            return v < n.rhs_int;
          case BinaryOperation::kLessThanEquals:
            return v <= n.rhs_int;
          case BinaryOperation::kGreaterThan:
            return v > n.rhs_int;
          case BinaryOperation::kGreaterThanEquals:
            return v >= n.rhs_int;
          default:
            return false;
        }
      }
      double v = 0.0;
      if (n.type == ValueType::kDouble) {
        const int64_t raw = col.IntAt(row);
        std::memcpy(&v, &raw, sizeof(v));
      } else {
        v = static_cast<double>(col.IntAt(row));
      }
      switch (n.op) {
        case BinaryOperation::kEquals:
          return v == n.rhs;
        case BinaryOperation::kNotEquals:
          return v != n.rhs;
        case BinaryOperation::kLessThan:
          return v < n.rhs;
        case BinaryOperation::kLessThanEquals:
          return v <= n.rhs;
        case BinaryOperation::kGreaterThan:
          return v > n.rhs;
        case BinaryOperation::kGreaterThanEquals:
          return v >= n.rhs;
        default:
          return false;
      }
    };
    return rec(rec, idx);
  };

  struct MetricState {
    double sum{0.0};
    int64_t int_sum{0};
    int64_t count{0};
    FlatIntKeyMap<uint8_t> distinct_keys;
  };
  struct WorkerState {
    std::vector<std::vector<MetricState>> bucket_metrics;
  };

  const size_t num_blocks = store->blocks.size();
  const size_t workers = std::max<size_t>(1, ParallelWorkerCount(num_blocks));
  std::vector<WorkerState> worker_states(workers);
  for (auto& ws : worker_states) {
    ws.bucket_metrics.resize(buckets.size());
    for (size_t bi = 0; bi < buckets.size(); ++bi) {
      ws.bucket_metrics[bi].resize(buckets[bi].metrics.size());
    }
  }

  const auto all_buckets_mask =
      static_cast<uint16_t>((uint32_t{1} << buckets.size()) - 1U);

  struct OrDoubleRange {
    slot_t col{0};
    double lo{0.0};
    double hi{0.0};
  };
  std::vector<std::vector<OrDoubleRange>> bucket_or_ranges(buckets.size());
  bool all_buckets_or_double_ranges = use_dispatch_lut;
  if (all_buckets_or_double_ranges) {
    for (size_t bi = 0; bi < buckets.size(); ++bi) {
      const auto& nodes = buckets[bi].pred_nodes;
      std::vector<OrDoubleRange> ranges;
      const std::function<bool(size_t)> extract = [&](size_t idx) -> bool {
        const PredNode& n = nodes[idx];
        if (n.kind == PredNode::Kind::kOr) {
          return extract(n.left) && extract(n.right);
        }
        if (n.kind == PredNode::Kind::kAnd) {
          const PredNode& l = nodes[n.left];
          const PredNode& r = nodes[n.right];
          if (l.kind != PredNode::Kind::kCmp ||
              r.kind != PredNode::Kind::kCmp || l.col != r.col ||
              l.type != ValueType::kDouble || r.type != ValueType::kDouble) {
            return false;
          }
          if (l.op == BinaryOperation::kGreaterThanEquals &&
              r.op == BinaryOperation::kLessThanEquals) {
            ranges.push_back(
                OrDoubleRange{.col = l.col, .lo = l.rhs, .hi = r.rhs});
            return true;
          }
          if (r.op == BinaryOperation::kGreaterThanEquals &&
              l.op == BinaryOperation::kLessThanEquals) {
            ranges.push_back(
                OrDoubleRange{.col = l.col, .lo = r.rhs, .hi = l.rhs});
            return true;
          }
        }
        return false;
      };
      if (!extract(buckets[bi].pred_root) || ranges.empty()) {
        all_buckets_or_double_ranges = false;
        break;
      }
      bucket_or_ranges[bi] = std::move(ranges);
    }
  }

  auto scan_worker = [&](size_t w) {
    auto& my_buckets = worker_states[w].bucket_metrics;
    for (size_t b_idx = w; b_idx < num_blocks; b_idx += workers) {
      const LSTablePageBlock& blk = store->blocks[b_idx];
      const uint32_t nrows = blk.row_count;
      if (nrows == 0) {
        continue;
      }
      const LSTableColumnBlock* disp_blk =
          use_dispatch ? &blk.columns[dispatch_col] : nullptr;
      if (use_dispatch && disp_blk->has_min_max &&
          (disp_blk->max_int < global_lo || disp_blk->min_int > global_hi)) {
        continue;
      }
      const int64_t* __restrict disp_raw =
          (disp_blk != nullptr && disp_blk->HasRawIntCells())
              ? disp_blk->int_cells.data()
              : nullptr;
      const bool disp_has_null = disp_blk != nullptr && disp_blk->has_null;
      for (uint32_t r = 0; r < nrows; ++r) {
        uint16_t cand_mask = all_buckets_mask;
        if (use_dispatch) {
          if (disp_has_null && disp_blk->IsNull(r)) {
            continue;
          }
          const int64_t q =
              disp_raw != nullptr ? disp_raw[r] : disp_blk->IntAt(r);
          if (q < global_lo || q > global_hi) {
            continue;
          }
          if (use_dispatch_lut) {
            cand_mask = dispatch_lut[static_cast<size_t>(q - global_lo)];
            if (cand_mask == 0) {
              continue;
            }
          } else {
            cand_mask = 0;
            for (size_t bi = 0; bi < buckets.size(); ++bi) {
              if (q >= buckets[bi].dispatch_lo &&
                  q <= buckets[bi].dispatch_hi) {
                cand_mask |= static_cast<uint16_t>(uint16_t{1} << bi);
              }
            }
            if (cand_mask == 0) {
              continue;
            }
          }
        }
        while (cand_mask != 0) {
          const auto bi = static_cast<size_t>(std::countr_zero(cand_mask));
          cand_mask &= static_cast<uint16_t>(cand_mask - 1U);
          const BucketPlan& bp = buckets[bi];
          bool matched = false;
          if (all_buckets_or_double_ranges) {
            for (const OrDoubleRange& rng : bucket_or_ranges[bi]) {
              const LSTableColumnBlock& cblk = blk.columns[rng.col];
              if (cblk.has_null && cblk.IsNull(r)) {
                continue;
              }
              double v = 0.0;
              const int64_t raw =
                  cblk.HasRawIntCells() ? cblk.int_cells[r] : cblk.IntAt(r);
              std::memcpy(&v, &raw, sizeof(v));
              if (v >= rng.lo && v <= rng.hi) {
                matched = true;
                break;
              }
            }
          } else {
            matched = eval_pred_node(bp.pred_nodes, bp.pred_root, blk, r);
          }
          if (!matched) {
            continue;
          }
          for (size_t m = 0; m < bp.metrics.size(); ++m) {
            const MetricSpec& ms = bp.metrics[m];
            const LSTableColumnBlock& mcol = blk.columns[ms.col];
            if (mcol.IsNull(r)) {
              continue;
            }
            MetricState& st = my_buckets[bi][m];
            if (ms.col_type == ValueType::kDouble) {
              const int64_t raw = mcol.IntAt(r);
              double dv = 0.0;
              std::memcpy(&dv, &raw, sizeof(dv));
              if (ms.need_sum) {
                st.sum += dv;
              }
              if (ms.need_count) {
                ++st.count;
              }
              if (ms.need_distinct) {
                const double norm = (dv == 0.0) ? 0.0 : dv;
                int64_t key = 0;
                std::memcpy(&key, &norm, sizeof(key));
                bool emplaced = false;
                uint8_t* slot = st.distinct_keys.TryEmplace(key, &emplaced);
                if (emplaced) {
                  *slot = 1;
                }
              }
            } else {
              const int64_t iv = mcol.IntAt(r);
              if (ms.need_sum) {
                st.int_sum += iv;
                st.sum += static_cast<double>(iv);
              }
              if (ms.need_count) {
                ++st.count;
              }
              if (ms.need_distinct) {
                bool emplaced = false;
                uint8_t* slot = st.distinct_keys.TryEmplace(iv, &emplaced);
                if (emplaced) {
                  *slot = 1;
                }
              }
            }
          }
        }
      }
    }
  };

  if (RunParallelTasks(workers, scan_worker) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  // Merge worker states into worker_states[0].
  auto& merged = worker_states[0].bucket_metrics;
  for (size_t w = 1; w < workers; ++w) {
    auto& other = worker_states[w].bucket_metrics;
    for (size_t bi = 0; bi < buckets.size(); ++bi) {
      for (size_t m = 0; m < buckets[bi].metrics.size(); ++m) {
        merged[bi][m].sum += other[bi][m].sum;
        merged[bi][m].int_sum += other[bi][m].int_sum;
        merged[bi][m].count += other[bi][m].count;
        if (buckets[bi].metrics[m].need_distinct &&
            other[bi][m].distinct_keys.Size() > 0) {
          if (other[bi][m].distinct_keys.Size() >
              merged[bi][m].distinct_keys.Size()) {
            std::swap(merged[bi][m].distinct_keys, other[bi][m].distinct_keys);
          }
          other[bi][m].distinct_keys.ForEach([&](int64_t k, uint8_t) {
            bool emplaced = false;
            uint8_t* slot =
                merged[bi][m].distinct_keys.TryEmplace(k, &emplaced);
            if (emplaced) {
              *slot = 1;
            }
          });
        }
      }
    }
  }

  std::vector<Column> out_cols;
  std::vector<Value> out_vals;
  for (size_t bi = 0; bi < buckets.size(); ++bi) {
    for (const AggOutSpec& os : buckets[bi].outputs) {
      const MetricState& st = merged[bi][os.metric_idx];
      switch (os.kind) {
        case AggOutSpec::Kind::kAvg:
          out_cols.emplace_back(os.out_name, ValueType::kDouble);
          out_vals.push_back(
              st.count == 0 ? Value()
                            : Value(st.sum / static_cast<double>(st.count)));
          break;
        case AggOutSpec::Kind::kSum:
          if (os.col_type == ValueType::kDouble) {
            out_cols.emplace_back(os.out_name, ValueType::kDouble);
            out_vals.push_back(st.count == 0 ? Value() : Value(st.sum));
          } else {
            out_cols.emplace_back(os.out_name, ValueType::kInt64);
            out_vals.push_back(st.count == 0 ? Value() : Value(st.int_sum));
          }
          break;
        case AggOutSpec::Kind::kCount:
          out_cols.emplace_back(os.out_name, ValueType::kInt64);
          out_vals.emplace_back(st.count);
          break;
        case AggOutSpec::Kind::kCountDistinct:
          out_cols.emplace_back(os.out_name, ValueType::kInt64);
          out_vals.emplace_back(static_cast<int64_t>(st.distinct_keys.Size()));
          break;
      }
    }
  }

  Relation out(context.execution_runtime());
  out.schema = Schema("", std::move(out_cols));
  out.AddRow(Row(std::move(out_vals)));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
