/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean-backed (AnalyticalOperators/FrequencyBoundsAndOrderedFetch.lean:
// OrderedFetch_sound) late-materializing early-stop Top-N scan for single-table
// queries:
//   SELECT <* | col1, ...> FROM T [WHERE <simple predicates>]
//   ORDER BY <ord_col> [ASC|DESC] [NULLS FIRST|LAST] LIMIT K [OFFSET O]
//
// Only loads {ord_col} U {filter_cols} during scanning (e.g. 2 of 105 columns
// in ClickBench Q24), orders blocks by ord_col min/max to early-stop as soon
// as the K-th winner beats all remaining blocks, prunes rows against the live
// K-th cutoff before running LIKE substring search, and fetches wide output
// columns strictly for the <= K winning (block_idx, row_idx) rows via
// MaterializeBlockView.
StatusOr<std::optional<Relation>> TryExecuteOrderedFetchTopN(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_ORDERED_FETCH") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Distinct() || statement.HasDistinctOn() ||
      statement.WithTies() || statement.AsStruct() || statement.ForUpdate() ||
      !statement.HasLimit() || statement.Limit() > 4096 ||
      statement.Offset() > 1024 || statement.OrderBy().size() != 1 ||
      statement.Sources().size() != 1 ||
      StatementContainsVolatileFunction(statement) ||
      HasWindowFunctions(statement)) {
    return std::optional<Relation>{};
  }
  const SelectSource& src = statement.Sources()[0];
  if (src.query != nullptr || src.unnest != nullptr || src.is_lateral ||
      src.sample.has_value() || !src.using_columns.empty() ||
      src.table.empty() || ctes.contains(AsciiToLower(src.table))) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, context.GetTable(src.table));
  if (!table) {
    return std::optional<Relation>{};
  }
  const Schema schema = table->GetSchema();
  const std::string alias =
      AsciiToLower(src.alias.empty() ? src.table : src.alias);
  const std::string tbl_lc = AsciiToLower(src.table);
  auto resolve_slot = [&](const ColumnName& cn) -> std::optional<slot_t> {
    if (!cn.schema.empty()) {
      const std::string sc = AsciiToLower(cn.schema);
      if (sc != alias && sc != tbl_lc) {
        return std::nullopt;
      }
    }
    const std::string nm = AsciiToLower(cn.name);
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (AsciiToLower(schema.GetColumn(c).Name().name) == nm) {
        return c;
      }
    }
    return std::nullopt;
  };

  // Resolve output columns (SELECT * or plain column references).
  std::vector<slot_t> out_slots;
  std::vector<Column> out_cols_schema;
  const bool is_star =
      statement.SelectList().size() == 1 &&
      statement.SelectList()[0].expression &&
      statement.SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
      statement.SelectList()[0]
              .expression->AsColumnValue()
              .GetColumnName()
              .name == "*" &&
      statement.SelectList()[0].star_modifiers == nullptr;
  if (is_star) {
    out_slots.reserve(schema.ColumnCount());
    out_cols_schema.reserve(schema.ColumnCount());
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      const Column& col = schema.GetColumn(c);
      if (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate &&
          col.Type() != ValueType::kDouble &&
          col.Type() != ValueType::kVarChar) {
        return std::optional<Relation>{};
      }
      out_slots.push_back(c);
      out_cols_schema.push_back(col);
    }
  } else {
    out_slots.reserve(statement.SelectList().size());
    out_cols_schema.reserve(statement.SelectList().size());
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      const NamedExpression& ne = statement.SelectList()[i];
      if (!ne.expression || ne.star_modifiers != nullptr ||
          ne.expression->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      auto sl = resolve_slot(ne.expression->AsColumnValue().GetColumnName());
      if (!sl) {
        return std::optional<Relation>{};
      }
      const Column& col = schema.GetColumn(*sl);
      if (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate &&
          col.Type() != ValueType::kDouble &&
          col.Type() != ValueType::kVarChar) {
        return std::optional<Relation>{};
      }
      out_slots.push_back(*sl);
      out_cols_schema.emplace_back(ProjectionName(ne, i), col.Type());
    }
  }

  // Resolve ORDER BY column (must be kInt64, kDate, or kVarChar).
  const auto& ob0 = statement.OrderBy()[0];
  if (!ob0.expression || ob0.expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  auto ord_slot_opt =
      resolve_slot(ob0.expression->AsColumnValue().GetColumnName());
  if (!ord_slot_opt) {
    return std::optional<Relation>{};
  }
  const slot_t ord_slot = *ord_slot_opt;
  const ValueType ord_type = schema.GetColumn(ord_slot).Type();
  if (ord_type != ValueType::kInt64 && ord_type != ValueType::kDate &&
      ord_type != ValueType::kVarChar) {
    return std::optional<Relation>{};
  }
  const bool asc = ob0.ascending;
  const bool nulls_first = ob0.nulls_first.value_or(asc);
  const bool ord_is_str = (ord_type == ValueType::kVarChar);

  // Parse simple WHERE predicates (LIKE '%needle%', =, <>, <, <=, >, >=).
  enum class SimplePredKind : uint8_t {
    kStrContains,
    kStrEq,
    kStrNeq,
    kIntEq,
    kIntNeq,
    kIntLt,
    kIntLe,
    kIntGt,
    kIntGe,
  };
  struct SimplePred {
    slot_t col{0};
    SimplePredKind kind{SimplePredKind::kStrEq};
    std::string str_val;
    int64_t int_val{0};
  };
  std::vector<SimplePred> preds;
  if (statement.WhereClause()) {
    for (const Expression& conj : SplitConjuncts(statement.WhereClause())) {
      if (!conj || conj->Type() != TypeTag::kBinaryExp) {
        return std::optional<Relation>{};
      }
      const auto& be = conj->AsBinaryExpression();
      if (!be.Left() || !be.Right()) {
        return std::optional<Relation>{};
      }
      if (be.Op() == BinaryOperation::kLike) {
        if (be.Left()->Type() != TypeTag::kColumnValue ||
            be.Right()->Type() != TypeTag::kConstantValue) {
          return std::optional<Relation>{};
        }
        auto sl = resolve_slot(be.Left()->AsColumnValue().GetColumnName());
        const Value& pat_v = be.Right()->AsConstantValue().GetValue();
        if (!sl || schema.GetColumn(*sl).Type() != ValueType::kVarChar ||
            pat_v.IsNull() || pat_v.type != ValueType::kVarChar) {
          return std::optional<Relation>{};
        }
        const std::string_view pat = pat_v.value.varchar_value;
        if (pat.size() < 3 || pat.front() != '%' || pat.back() != '%') {
          return std::optional<Relation>{};
        }
        const std::string_view needle = pat.substr(1, pat.size() - 2);
        if (needle.find('%') != std::string_view::npos ||
            needle.find('_') != std::string_view::npos ||
            needle.find('\\') != std::string_view::npos) {
          return std::optional<Relation>{};
        }
        preds.push_back(SimplePred{.col = *sl,
                                   .kind = SimplePredKind::kStrContains,
                                   .str_val = std::string(needle),
                                   .int_val = 0});
        continue;
      }
      Expression col_e;
      Expression lit_e;
      BinaryOperation op = be.Op();
      if (be.Left()->Type() == TypeTag::kColumnValue &&
          be.Right()->Type() == TypeTag::kConstantValue) {
        col_e = be.Left();
        lit_e = be.Right();
      } else if (be.Right()->Type() == TypeTag::kColumnValue &&
                 be.Left()->Type() == TypeTag::kConstantValue) {
        col_e = be.Right();
        lit_e = be.Left();
        switch (op) {
          case BinaryOperation::kLessThan:
            op = BinaryOperation::kGreaterThan;
            break;
          case BinaryOperation::kLessThanEquals:
            op = BinaryOperation::kGreaterThanEquals;
            break;
          case BinaryOperation::kGreaterThan:
            op = BinaryOperation::kLessThan;
            break;
          case BinaryOperation::kGreaterThanEquals:
            op = BinaryOperation::kLessThanEquals;
            break;
          default:
            break;
        }
      } else {
        return std::optional<Relation>{};
      }
      auto sl = resolve_slot(col_e->AsColumnValue().GetColumnName());
      const Value& lv = lit_e->AsConstantValue().GetValue();
      if (!sl || lv.IsNull()) {
        return std::optional<Relation>{};
      }
      const ValueType ct = schema.GetColumn(*sl).Type();
      if (ct == ValueType::kVarChar && lv.type == ValueType::kVarChar) {
        if (op == BinaryOperation::kEquals) {
          preds.push_back(
              SimplePred{.col = *sl,
                         .kind = SimplePredKind::kStrEq,
                         .str_val = std::string(lv.value.varchar_value),
                         .int_val = 0});
        } else if (op == BinaryOperation::kNotEquals) {
          preds.push_back(
              SimplePred{.col = *sl,
                         .kind = SimplePredKind::kStrNeq,
                         .str_val = std::string(lv.value.varchar_value),
                         .int_val = 0});
        } else {
          return std::optional<Relation>{};
        }
      } else if ((ct == ValueType::kInt64 && lv.type == ValueType::kInt64) ||
                 (ct == ValueType::kDate && lv.type == ValueType::kDate)) {
        SimplePredKind k = SimplePredKind::kIntEq;
        switch (op) {
          case BinaryOperation::kEquals:
            k = SimplePredKind::kIntEq;
            break;
          case BinaryOperation::kNotEquals:
            k = SimplePredKind::kIntNeq;
            break;
          case BinaryOperation::kLessThan:
            k = SimplePredKind::kIntLt;
            break;
          case BinaryOperation::kLessThanEquals:
            k = SimplePredKind::kIntLe;
            break;
          case BinaryOperation::kGreaterThan:
            k = SimplePredKind::kIntGt;
            break;
          case BinaryOperation::kGreaterThanEquals:
            k = SimplePredKind::kIntGe;
            break;
          default:
            return std::optional<Relation>{};
        }
        preds.push_back(SimplePred{.col = *sl,
                                   .kind = k,
                                   .str_val = {},
                                   .int_val = lv.value.int_value});
      } else {
        return std::optional<Relation>{};
      }
    }
  }

  const size_t limit = statement.Limit();
  const size_t offset = statement.Offset();
  const size_t K = limit + offset;
  if (limit == 0) {
    Relation empty_out(context.execution_runtime());
    empty_out.schema = Schema("", std::move(out_cols_schema));
    return std::optional<Relation>(std::move(empty_out));
  }

  // Load ONLY {ord_slot} U {filter_cols} eagerly and cache-resident; wide
  // output columns are fetched ONLY for the <= limit winning rows via a
  // non-caching block-lazy store so they never thrash the hot scan cache
  // (OrderedFetch_sound).
  std::vector<slot_t> scan_cols{ord_slot};
  for (const SimplePred& p : preds) {
    scan_cols.push_back(p.col);
  }
  std::ranges::sort(scan_cols);
  scan_cols.erase(std::ranges::unique(scan_cols).begin(), scan_cols.end());
  std::vector<slot_t> needed_out = out_slots;
  std::ranges::sort(needed_out);
  needed_out.erase(std::ranges::unique(needed_out).begin(), needed_out.end());

  auto store = GetOrBuildLSTableStore(context.txn_, *table, true, &scan_cols,
                                      /*allow_block_lazy=*/false);
  if (!store || store->layout_generation != table->LayoutGeneration() ||
      store->first_pid != table->FirstPageId()) {
    return std::optional<Relation>{};
  }
  if (store->uncommitted_owner_txn != 0) {
    if (store->uncommitted_owner_txn != context.txn_.ID()) {
      return std::optional<Relation>{};
    }
  } else if (!context.txn_.IsReadOnly() && !context.txn_.WriteSetEmpty()) {
    return std::optional<Relation>{};
  } else if (context.txn_.HasAnyPageVersionChains()) {
    for (const page_id_t pid : store->page_ids) {
      if (context.txn_.HasPageVersionChains(pid) ||
          store->FindBlock(pid) == nullptr) {
        return std::optional<Relation>{};
      }
    }
  }
  for (slot_t c : scan_cols) {
    if (!store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }
  const size_t nb = store->blocks.size();
  if (nb == 0) {
    Relation empty_out(context.execution_runtime());
    empty_out.schema = Schema("", std::move(out_cols_schema));
    return std::optional<Relation>(std::move(empty_out));
  }

  // Compute per-block order bounds for cheap O(1) metadata (int/date min/max or
  // active string dictionary). Avoid scanning all string rows just for bounds.
  struct BlockOrder {
    size_t bi{0};
    bool has_bound{false};
    bool has_null{false};
    int64_t int_bound{0};
    std::string str_bound;
  };
  std::vector<BlockOrder> block_order(nb);
  bool any_has_bound = false;
  for (size_t b = 0; b < nb; ++b) {
    BlockOrder& bo = block_order[b];
    bo.bi = b;
    const LSTablePageBlock& blk = store->blocks[b];
    if (blk.row_count == 0) {
      continue;
    }
    const auto& c_ord = blk.columns[ord_slot];
    bo.has_null = c_ord.has_null;
    if (!ord_is_str) {
      if (c_ord.has_min_max) {
        bo.has_bound = true;
        bo.int_bound = asc ? c_ord.min_int : c_ord.max_int;
        any_has_bound = true;
      }
    } else if (c_ord.dict != nullptr && c_ord.dict->active &&
               c_ord.dict->StringEntryCount() > 0) {
      const size_t ne = c_ord.dict->StringEntryCount();
      std::string_view best = c_ord.dict->StringEntryAt(0);
      for (size_t i = 1; i < ne; ++i) {
        const std::string_view sv = c_ord.dict->StringEntryAt(i);
        if (asc ? (sv < best) : (sv > best)) {
          best = sv;
        }
      }
      bo.has_bound = true;
      bo.str_bound = std::string(best);
      any_has_bound = true;
    }
  }
  // Cost gate: only take this path when we either skip wide output columns
  // (late materialization: out_slots > scan_cols, e.g. SELECT * on ClickBench
  // hits) or have O(1) block min/max bounds for early-stop pruning.
  if (!any_has_bound && out_slots.size() <= scan_cols.size()) {
    return std::optional<Relation>{};
  }
  if (any_has_bound) {
    std::ranges::sort(block_order,
                      [&](const BlockOrder& a, const BlockOrder& b) {
                        if (nulls_first && a.has_null != b.has_null) {
                          return a.has_null;
                        }
                        if (a.has_bound != b.has_bound) {
                          return !a.has_bound;
                        }
                        if (!a.has_bound) {
                          return a.bi < b.bi;
                        }
                        if (!ord_is_str) {
                          if (a.int_bound != b.int_bound) {
                            return asc ? (a.int_bound < b.int_bound)
                                       : (a.int_bound > b.int_bound);
                          }
                        } else {
                          if (a.str_bound != b.str_bound) {
                            return asc ? (a.str_bound < b.str_bound)
                                       : (a.str_bound > b.str_bound);
                          }
                        }
                        return a.bi < b.bi;
                      });
  }
  const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));

  struct Candidate {
    bool is_null{false};
    int64_t int_key{0};
    std::string str_key;
    uint32_t bi{0};
    uint32_t ri{0};
  };
  // Returns true if `a` should appear BEFORE `b` in the final ORDER BY output.
  auto cand_before = [&](const Candidate& a, const Candidate& b) -> bool {
    if (a.is_null != b.is_null) {
      return a.is_null ? nulls_first : !nulls_first;
    }
    if (!a.is_null) {
      if (!ord_is_str) {
        if (a.int_key != b.int_key) {
          return asc ? (a.int_key < b.int_key) : (a.int_key > b.int_key);
        }
      } else {
        if (a.str_key != b.str_key) {
          return asc ? (a.str_key < b.str_key) : (a.str_key > b.str_key);
        }
      }
    }
    if (a.bi != b.bi) {
      return a.bi < b.bi;
    }
    return a.ri < b.ri;
  };
  // Max-heap comparator so heap.front() is the WORST element in top-K.
  auto heap_worse = [&](const Candidate& a, const Candidate& b) -> bool {
    return cand_before(a, b);
  };

  std::vector<Candidate> global_heap;
  global_heap.reserve(K + 1);
  auto push_cand = [&](std::vector<Candidate>& hp, Candidate c) {
    if (hp.size() < K) {
      hp.push_back(std::move(c));
      std::push_heap(hp.begin(), hp.end(), heap_worse);
    } else if (cand_before(c, hp.front())) {
      std::pop_heap(hp.begin(), hp.end(), heap_worse);
      hp.back() = std::move(c);
      std::push_heap(hp.begin(), hp.end(), heap_worse);
    }
  };

  const size_t wave_size = any_has_bound ? std::max<size_t>(nw, 16) : nb;

  for (size_t wbegin = 0; wbegin < nb; wbegin += wave_size) {
    // Early-stop check (OrderedFetch_sound): if we already have K winners and
    // the best remaining block cannot beat global_heap.front(), stop!
    if (global_heap.size() == K) {
      const Candidate& worst = global_heap.front();
      const BlockOrder& first_bo = block_order[wbegin];
      if (nulls_first && worst.is_null && !first_bo.has_null) {
        break;
      }
      if (!worst.is_null && first_bo.has_bound &&
          (!nulls_first || !first_bo.has_null)) {
        const bool beats_all =
            !ord_is_str ? (asc ? (first_bo.int_bound > worst.int_key)
                               : (first_bo.int_bound < worst.int_key))
                        : (asc ? (first_bo.str_bound > worst.str_key)
                               : (first_bo.str_bound < worst.str_key));
        if (beats_all) {
          break;
        }
      }
    }

    const size_t wend = std::min(nb, wbegin + wave_size);
    const size_t wcount = wend - wbegin;
    const size_t w_workers = std::min(nw, wcount);
    const bool has_cutoff = (global_heap.size() == K);
    const bool cut_is_null = has_cutoff ? global_heap.front().is_null : false;
    const int64_t cut_int = has_cutoff ? global_heap.front().int_key : 0;
    const std::string cut_str =
        has_cutoff && ord_is_str ? global_heap.front().str_key : std::string{};

    std::vector<std::vector<Candidate>> local_heaps(w_workers);
    auto scan_wave = [&](size_t w) {
      auto& lhp = local_heaps[w];
      lhp.reserve(K + 1);
      for (size_t wi = wbegin + w; wi < wend; wi += w_workers) {
        const BlockOrder& bo = block_order[wi];
        const LSTablePageBlock& ord_blk = store->blocks[bo.bi];
        const uint32_t nr = ord_blk.row_count;
        if (nr == 0) {
          continue;
        }
        // Per-block early skip against current wave cutoff.
        if (has_cutoff && !cut_is_null && bo.has_bound &&
            (!nulls_first || !bo.has_null)) {
          const bool skip =
              !ord_is_str
                  ? (asc ? (bo.int_bound > cut_int) : (bo.int_bound < cut_int))
                  : (asc ? (bo.str_bound > cut_str) : (bo.str_bound < cut_str));
          if (skip) {
            continue;
          }
        }
        // Precompute per-block 256-bit dict masks for string predicates.
        struct BlockPredEval {
          const SimplePred* pred{nullptr};
          const LSTableColumnBlock* col{nullptr};
          bool use_dict_mask{false};
          std::array<uint64_t, 4> dict_mask{};
          bool always_false{false};
        };
        std::vector<BlockPredEval> bpreds(preds.size());
        bool block_dead = false;
        for (size_t pi = 0; pi < preds.size(); ++pi) {
          BlockPredEval& bp = bpreds[pi];
          bp.pred = &preds[pi];
          bp.col = &ord_blk.columns[preds[pi].col];
          if ((bp.pred->kind == SimplePredKind::kStrContains ||
               bp.pred->kind == SimplePredKind::kStrEq ||
               bp.pred->kind == SimplePredKind::kStrNeq) &&
              bp.col->dict != nullptr && bp.col->dict->active &&
              !bp.col->dict_codes.empty()) {
            bp.use_dict_mask = true;
            const size_t ne =
                std::min<size_t>(256, bp.col->dict->StringEntryCount());
            bool any_hit = false;
            for (size_t ci = 0; ci < ne; ++ci) {
              const std::string_view sv = bp.col->dict->StringEntryAt(ci);
              bool ok = false;
              if (bp.pred->kind == SimplePredKind::kStrContains) {
                ok = sv.find(bp.pred->str_val) != std::string_view::npos;
              } else if (bp.pred->kind == SimplePredKind::kStrEq) {
                ok = (sv == bp.pred->str_val);
              } else {
                ok = (sv != bp.pred->str_val);
              }
              if (ok) {
                bp.dict_mask[ci >> 6] |= (uint64_t{1} << (ci & 63));
                any_hit = true;
              }
            }
            if (!any_hit) {
              block_dead = true;
              break;
            }
          }
        }
        if (block_dead) {
          continue;
        }

        const auto& c_ord = ord_blk.columns[ord_slot];
        for (uint32_t r = 0; r < nr; ++r) {
          // 1. Cheap order-key cutoff check BEFORE evaluating string LIKE.
          const bool r_null = c_ord.has_null && c_ord.IsNull(r);
          int64_t r_int = 0;
          std::string_view r_sv;
          if (!r_null) {
            if (!ord_is_str) {
              r_int = c_ord.IntAt(r);
            } else {
              r_sv = c_ord.VarcharAt(r);
            }
          }
          if (has_cutoff) {
            if (r_null) {
              if (!nulls_first && !cut_is_null) {
                continue;
              }
            } else if (!cut_is_null) {
              const bool worse =
                  !ord_is_str ? (asc ? (r_int > cut_int) : (r_int < cut_int))
                              : (asc ? (r_sv > cut_str) : (r_sv < cut_str));
              if (worse) {
                continue;
              }
            } else if (nulls_first) {
              continue;
            }
          }
          if (lhp.size() == K) {
            const Candidate& lw = lhp.front();
            if (r_null) {
              if (!nulls_first && !lw.is_null) {
                continue;
              }
            } else if (!lw.is_null) {
              const bool worse =
                  !ord_is_str
                      ? (asc ? (r_int > lw.int_key) : (r_int < lw.int_key))
                      : (asc ? (r_sv > lw.str_key) : (r_sv < lw.str_key));
              if (worse) {
                continue;
              }
            } else if (nulls_first) {
              continue;
            }
          }

          // 2. Evaluate filter predicates.
          bool match = true;
          for (const BlockPredEval& bp : bpreds) {
            if (bp.col->has_null && bp.col->IsNull(r)) {
              match = false;
              break;
            }
            if (bp.use_dict_mask) {
              const uint8_t code = bp.col->dict_codes[r];
              if ((bp.dict_mask[code >> 6] & (uint64_t{1} << (code & 63))) ==
                  0) {
                match = false;
                break;
              }
              continue;
            }
            switch (bp.pred->kind) {
              case SimplePredKind::kStrContains:
                match = bp.col->VarcharAt(r).find(bp.pred->str_val) !=
                        std::string_view::npos;
                break;
              case SimplePredKind::kStrEq:
                match = (bp.col->VarcharAt(r) == bp.pred->str_val);
                break;
              case SimplePredKind::kStrNeq:
                match = (bp.col->VarcharAt(r) != bp.pred->str_val);
                break;
              case SimplePredKind::kIntEq:
                match = (bp.col->IntAt(r) == bp.pred->int_val);
                break;
              case SimplePredKind::kIntNeq:
                match = (bp.col->IntAt(r) != bp.pred->int_val);
                break;
              case SimplePredKind::kIntLt:
                match = (bp.col->IntAt(r) < bp.pred->int_val);
                break;
              case SimplePredKind::kIntLe:
                match = (bp.col->IntAt(r) <= bp.pred->int_val);
                break;
              case SimplePredKind::kIntGt:
                match = (bp.col->IntAt(r) > bp.pred->int_val);
                break;
              case SimplePredKind::kIntGe:
                match = (bp.col->IntAt(r) >= bp.pred->int_val);
                break;
            }
            if (!match) {
              break;
            }
          }
          if (!match) {
            continue;
          }
          Candidate c;
          c.is_null = r_null;
          c.int_key = r_int;
          if (!r_null && ord_is_str) {
            c.str_key = std::string(r_sv);
          }
          c.bi = static_cast<uint32_t>(bo.bi);
          c.ri = r;
          push_cand(lhp, std::move(c));
        }
      }
    };
    if (RunParallelTasks(w_workers, scan_wave) != Status::kSuccess) {
      return std::optional<Relation>{};
    }
    for (auto& lhp : local_heaps) {
      for (Candidate& c : lhp) {
        push_cand(global_heap, std::move(c));
      }
    }
  }

  std::ranges::sort(global_heap, cand_before);
  Relation out(context.execution_runtime());
  out.schema = Schema("", std::move(out_cols_schema));
  if (global_heap.size() <= offset) {
    return std::optional<Relation>(std::move(out));
  }

  // Late materialization (OrderedFetch_sound): fetch wide output columns ONLY
  // for the <= limit winning rows via a non-caching block-lazy store so the
  // hot scan cache is never thrashed.
  auto lazy_store =
      GetOrBuildLSTableStore(context.txn_, *table,
                             /*allow_lazy_build=*/false, &needed_out,
                             /*allow_block_lazy=*/true);
  if (!lazy_store || lazy_store->blocks.size() != nb) {
    return std::optional<Relation>{};
  }
  LSTableStore::LazyBlockScratch scratch;
  const size_t emit_end = std::min(global_heap.size(), K);
  out.rows.reserve(emit_end - offset);
  for (size_t i = offset; i < emit_end; ++i) {
    const Candidate& w = global_heap[i];
    const LSTablePageBlock* blk =
        lazy_store->MaterializeBlockView(w.bi, &scratch, needed_out);
    if (blk == nullptr || w.ri >= blk->row_count) {
      return std::optional<Relation>{};
    }
    std::vector<Value> vals;
    vals.reserve(out_slots.size());
    for (slot_t c : out_slots) {
      const LSTableColumnBlock& cb = blk->columns[c];
      if (cb.has_null && cb.IsNull(w.ri)) {
        vals.emplace_back();
        continue;
      }
      switch (schema.GetColumn(c).Type()) {
        case ValueType::kInt64:
          vals.emplace_back(cb.IntAt(w.ri));
          break;
        case ValueType::kDate:
          vals.emplace_back(Value::DateFromDays(cb.IntAt(w.ri)));
          break;
        case ValueType::kDouble: {
          const int64_t bits = cb.IntAt(w.ri);
          double d = 0.0;
          std::memcpy(&d, &bits, sizeof(d));
          vals.emplace_back(d);
          break;
        }
        case ValueType::kVarChar:
          vals.emplace_back(std::string(cb.VarcharAt(w.ri)));
          break;
        default:
          return std::optional<Relation>{};
      }
    }
    out.AddRow(Row(std::move(vals)));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
