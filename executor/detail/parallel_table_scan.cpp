/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "index/index_scan_iterator.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

template <typename Left, typename Right>
bool ComparePositional(BinaryOperation op, const Left& left,
                       const Right& right) {
  switch (op) {
    case BinaryOperation::kEquals:
      return left == right;
    case BinaryOperation::kNotEquals:
      return left != right;
    case BinaryOperation::kLessThan:
      return left < right;
    case BinaryOperation::kLessThanEquals:
      return left <= right;
    case BinaryOperation::kGreaterThan:
      return left > right;
    case BinaryOperation::kGreaterThanEquals:
      return left >= right;
    default:
      return false;
  }
}

}  // namespace

bool TryParallelTableScan(TransactionContext& context, Table& table,
                          const std::vector<slot_t>* projection,
                          const RuntimeFilterGroup* runtime_filter,
                          ValueType runtime_filter_type,
                          std::optional<slot_t> full_key_column,
                          bool filter_during_scan,
                          const CompiledScanFilter* scan_filter,
                          const Schema& result_schema, const Scope* outer,
                          const CteMap& ctes, Relation* result, Status* error) {
  // A page-wide table can have many morsels but few rows. Starting sixteen
  // threads for each 10k-row outer/inner scan costs more than the selective
  // semi-join itself. Statistics choose scheduling only, never scan bounds.
  constexpr size_t kRowsPerScanWorker = 8192;
  size_t row_workers = 16;
  auto stats = context.GetStats(table.GetSchema().Name());
  if (stats.HasValue() && stats.Value()->Rows() > 0) {
    row_workers =
        std::max<size_t>(1, stats.Value()->Rows() / kRowsPerScanWorker);
  }
  std::shared_ptr<const LSTableStore> lstable_store;
  const RuntimeFilterGroup* active_lstable_rf = nullptr;
  {
    std::shared_ptr<const LSTableStore> store =
        GetOrBuildLSTableStore(context.txn_, table, true);
    if (store != nullptr &&
        store->layout_generation == table.LayoutGeneration() &&
        store->first_pid == table.FirstPageId()) {
      bool valid = true;
      if (store->uncommitted_owner_txn != 0) {
        valid = (store->uncommitted_owner_txn == context.txn_.ID());
      } else if (!context.txn_.IsReadOnly() && !context.txn_.WriteSetEmpty()) {
        valid = false;
      } else if (context.txn_.HasAnyPageVersionChains()) {
        for (const page_id_t pid : store->page_ids) {
          if (context.txn_.HasPageVersionChains(pid) ||
              store->FindBlock(pid) == nullptr) {
            valid = false;
            break;
          }
        }
      }
      if (valid) {
        lstable_store = std::move(store);
        if (runtime_filter != nullptr && !runtime_filter->empty() &&
            full_key_column.has_value() &&
            *full_key_column < table.GetSchema().ColumnCount()) {
          const Column& col = table.GetSchema().GetColumn(*full_key_column);
          if ((col.Type() == ValueType::kInt64 ||
               col.Type() == ValueType::kDate) &&
              !col.IsUnsigned() &&
              (runtime_filter_type == ValueType::kNull ||
               runtime_filter_type == col.Type())) {
            active_lstable_rf = runtime_filter;
          }
        }
      }
    }
  }

  std::vector<Table::ScanMorsel> morsels;
  constexpr size_t kMaxParallelScanWorkers = 16;
  size_t workers = 1;
  size_t lstable_blocks_per_morsel = 8;
  if (lstable_store != nullptr) {
    lstable_blocks_per_morsel =
        std::clamp<size_t>(lstable_store->blocks.size() / 64, 1, 8);
    const size_t block_morsels = std::max<size_t>(
        1, (lstable_store->blocks.size() + lstable_blocks_per_morsel - 1) /
               lstable_blocks_per_morsel);
    if (lstable_store->total_rows < 4096 || block_morsels < 2) {
      workers = 1;
    } else {
      const size_t lstable_row_workers =
          std::max<size_t>(2, lstable_store->total_rows / 4096);
      workers = std::min<size_t>(
          {kMaxParallelScanWorkers,
           static_cast<size_t>(std::thread::hardware_concurrency()),
           block_morsels, lstable_row_workers});
      workers = std::max<size_t>(1, workers);
    }
  } else {
    if (row_workers <= 1) {
      return false;
    }
    morsels = table.BuildScanMorsels(context.txn_, 8);
    workers = std::min<size_t>(
        {kMaxParallelScanWorkers,
         static_cast<size_t>(std::thread::hardware_concurrency()),
         std::max<size_t>(1, morsels.size()), row_workers});
    if (workers <= 1 || morsels.size() < 8) {
      return false;
    }
  }

  std::vector<Relation> shards(workers);
  std::vector<size_t> shard_seen(workers, 0);
  std::vector<size_t> shard_out(workers, 0);
  // When every non-simple filter component compiled to context-free
  // bytecode, workers decide rows themselves; only rows whose program
  // raised are deferred for a contextful MatchScanFilter re-check.
  // Simple-only filters keep the original tight inline loop: the generic
  // worker evaluator adds measurable per-row overhead on multi-million-row
  // scans with nothing non-simple to evaluate.
  static const bool worker_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_WORKER_FILTER") != nullptr;
  const bool has_post_simple_filter =
      filter_during_scan && scan_filter != nullptr &&
      (scan_filter->compile_decided_reject ||
       scan_filter->needs_unsigned_tagging || !scan_filter->prefix_in.empty() ||
       !scan_filter->prebuilt_membership.empty() ||
       !scan_filter->prebuilt_scalar_compare.empty() ||
       !scan_filter->residual.empty() ||
       !scan_filter->disjunctive_branches.empty());
  const WorkerScanFilterPrograms worker_programs =
      has_post_simple_filter && !worker_filter_disabled
          ? CompileWorkerFilter(*scan_filter, result_schema)
          : WorkerScanFilterPrograms{};
  std::vector<std::vector<Row>> deferred_rows(worker_programs.complete ? workers
                                                                       : 0);
  std::mutex error_mu;
  std::exception_ptr error_ex;
  std::optional<std::vector<slot_t>> proj_opt;
  if (projection != nullptr) {
    proj_opt = *projection;
  }

  const auto integer_peeks =
      filter_during_scan && scan_filter != nullptr
          ? BuildIntegerPeeks(*scan_filter, projection, table.GetSchema())
          : std::vector<IntegerPeekCompare>{};

  struct LSTableDictColFilter {
    slot_t column{0};
    std::array<uint64_t, 4> pass_mask{};
    std::array<uint8_t, 256> pass{};
  };
  struct LSTableDoubleZonePredicate {
    slot_t column{0};
    BinaryOperation op{BinaryOperation::kEquals};
    double constant{0.0};
  };
  struct LSTableIntColCompare {
    slot_t left_col{0};
    slot_t right_col{0};
    BinaryOperation op{BinaryOperation::kEquals};
  };
  struct LSTableExtraRf {
    slot_t column{0};
    const RuntimeFilterGroup* group{nullptr};
  };
  std::vector<LSTableDictColFilter> lstable_dict_filters;
  std::vector<LSTableDoubleZonePredicate> lstable_double_zones;
  std::vector<LSTableIntColCompare> lstable_col_compares;
  std::vector<LSTableExtraRf> lstable_extra_rfs;
  std::vector<uint8_t> simple_covered_by_dict;
  std::vector<uint8_t> prefix_covered_by_dict;
  std::vector<uint8_t> prebuilt_membership_covered;
  bool disjunctive_covered_by_dict = false;

  const auto to_full_col = [&](slot_t proj_slot) -> slot_t {
    return projection != nullptr ? (*projection)[proj_slot] : proj_slot;
  };
  const bool can_predecide_lstable =
      lstable_store != nullptr && filter_during_scan &&
      scan_filter != nullptr && !scan_filter->compile_decided_reject &&
      !scan_filter->needs_unsigned_tagging;
  if (can_predecide_lstable) {
    simple_covered_by_dict.assign(scan_filter->simple.size(), 0);
    prefix_covered_by_dict.assign(scan_filter->prefix_in.size(), 0);
    prebuilt_membership_covered.assign(scan_filter->prebuilt_membership.size(),
                                       0);
    std::vector<int> col_to_df(table.GetSchema().ColumnCount(), -1);
    const auto dict_size_of =
        [](const LSTableColumnDictionary& dict) noexcept -> size_t {
      if (dict.value_type == ValueType::kInt64) {
        return dict.int_entries.size();
      }
      if (dict.value_type == ValueType::kDouble) {
        return dict.double_entries.size();
      }
      return dict.entries.size();
    };
    auto get_or_init_df = [&](slot_t full_col) -> LSTableDictColFilter& {
      if (col_to_df[full_col] < 0) {
        col_to_df[full_col] = static_cast<int>(lstable_dict_filters.size());
        LSTableDictColFilter df;
        df.column = full_col;
        const auto& dict = lstable_store->dictionaries[full_col];
        const size_t ncodes = dict_size_of(dict);
        for (size_t code = 0; code < ncodes; ++code) {
          df.pass[code] = 1;
          df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
        }
        lstable_dict_filters.push_back(df);
      }
      return lstable_dict_filters[static_cast<size_t>(col_to_df[full_col])];
    };
    const auto eval_pred_on_dict_code = [&](const SimpleComparePredicate& pred,
                                            const LSTableColumnDictionary& dict,
                                            size_t code,
                                            bool* supported) noexcept -> bool {
      *supported = false;
      if (pred.column_payload || pred.constant.IsNull()) {
        return false;
      }
      if (dict.value_type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar &&
          !pred.constant.IsCaseInsensitive() &&
          (pred.op == BinaryOperation::kLike ||
           pred.op == BinaryOperation::kNotLike || pred.plain_string_payload)) {
        *supported = true;
        const std::string_view text = dict.entries[code];
        const std::string_view pat = pred.constant.value.varchar_value;
        if (pred.op == BinaryOperation::kLike ||
            pred.op == BinaryOperation::kNotLike) {
          const bool matched = FastLikeMatch(text, pat);
          return pred.op == BinaryOperation::kLike ? matched : !matched;
        }
        return ComparePositional(pred.op, text, pat);
      }
      if (dict.value_type == ValueType::kInt64 && pred.int_payload &&
          !pred.constant.IsUnsigned()) {
        *supported = true;
        return ComparePositional(pred.op, dict.int_entries[code],
                                 pred.int_constant);
      }
      if (dict.value_type == ValueType::kDouble && pred.double_payload) {
        *supported = true;
        return ComparePositional(pred.op, dict.double_entries[code],
                                 pred.double_constant);
      }
      return false;
    };
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      const SimpleComparePredicate& pred = scan_filter->simple[si];
      if (pred.column_payload) {
        const slot_t lfull = to_full_col(pred.column);
        const slot_t rfull = to_full_col(pred.right_column);
        if (lfull < table.GetSchema().ColumnCount() &&
            rfull < table.GetSchema().ColumnCount()) {
          const Column& lc = table.GetSchema().GetColumn(lfull);
          const Column& rc = table.GetSchema().GetColumn(rfull);
          if ((lc.Type() == ValueType::kInt64 ||
               lc.Type() == ValueType::kDate) &&
              (rc.Type() == ValueType::kInt64 ||
               rc.Type() == ValueType::kDate) &&
              !lc.IsUnsigned() && !rc.IsUnsigned()) {
            lstable_col_compares.push_back(
                LSTableIntColCompare{lfull, rfull, pred.op});
            simple_covered_by_dict[si] = 1;
          }
        }
        continue;
      }
      if (pred.constant.IsNull()) {
        continue;
      }
      const slot_t full_col = to_full_col(pred.column);
      if (full_col >= table.GetSchema().ColumnCount()) {
        continue;
      }
      if (pred.double_payload &&
          table.GetSchema().GetColumn(full_col).Type() == ValueType::kDouble) {
        lstable_double_zones.push_back(LSTableDoubleZonePredicate{
            full_col, pred.op, pred.double_constant});
      }
      if (!pred.double_payload &&
          !table.GetSchema().GetColumn(full_col).IsUnsigned() &&
          (pred.constant.type == ValueType::kInt64 ||
           pred.constant.type == ValueType::kDate)) {
        for (const IntegerPeekCompare& pk : integer_peeks) {
          if (pk.column == full_col && pk.op == pred.op &&
              pk.constant == pred.int_constant) {
            simple_covered_by_dict[si] = 1;
            break;
          }
        }
      }
      if (full_col < lstable_store->dictionaries.size() &&
          lstable_store->dictionaries[full_col].active &&
          !table.GetSchema().GetColumn(full_col).IsUnsigned()) {
        const auto& dict = lstable_store->dictionaries[full_col];
        bool supported = false;
        (void)eval_pred_on_dict_code(pred, dict, 0, &supported);
        if (supported) {
          LSTableDictColFilter& df = get_or_init_df(full_col);
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          const size_t ncodes = dict_size_of(dict);
          for (size_t code = 0; code < ncodes; ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            bool code_sup = false;
            if (!eval_pred_on_dict_code(pred, dict, code, &code_sup)) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          simple_covered_by_dict[si] = 1;
        }
      }
    }
    {
      for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
        const PrefixInPredicate& pred = scan_filter->prefix_in[pi];
        const slot_t full_col = to_full_col(pred.column);
        if (full_col < lstable_store->dictionaries.size() &&
            lstable_store->dictionaries[full_col].active &&
            lstable_store->dictionaries[full_col].value_type ==
                ValueType::kVarChar) {
          LSTableDictColFilter& df = get_or_init_df(full_col);
          const auto& dict = lstable_store->dictionaries[full_col];
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          for (size_t code = 0; code < dict.entries.size(); ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            const std::string_view text = dict.entries[code];
            const bool ok = std::ranges::any_of(
                pred.prefixes,
                [text](const std::string& p) { return text.starts_with(p); });
            if (!ok) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          prefix_covered_by_dict[pi] = 1;
        }
      }
      if (!scan_filter->disjunctive_branches.empty()) {
        std::optional<slot_t> common_col;
        bool single_dict_col_or = true;
        for (const auto& br : scan_filter->disjunctive_branches) {
          if (!br.prefix_in.empty() || !br.residual.empty() ||
              br.simple.empty()) {
            single_dict_col_or = false;
            break;
          }
          for (const SimpleComparePredicate& sp : br.simple) {
            const slot_t fc = to_full_col(sp.column);
            if (!common_col.has_value()) {
              common_col = fc;
            } else if (*common_col != fc) {
              single_dict_col_or = false;
              break;
            }
            if (fc >= lstable_store->dictionaries.size() ||
                !lstable_store->dictionaries[fc].active ||
                table.GetSchema().GetColumn(fc).IsUnsigned()) {
              single_dict_col_or = false;
              break;
            }
            bool sup = false;
            (void)eval_pred_on_dict_code(sp, lstable_store->dictionaries[fc], 0,
                                         &sup);
            if (!sup) {
              single_dict_col_or = false;
              break;
            }
          }
          if (!single_dict_col_or) {
            break;
          }
        }
        if (single_dict_col_or && common_col.has_value()) {
          const slot_t fc = *common_col;
          LSTableDictColFilter& df = get_or_init_df(fc);
          const auto& dict = lstable_store->dictionaries[fc];
          const size_t ncodes = dict_size_of(dict);
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          for (size_t code = 0; code < ncodes; ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            bool any_br_ok = false;
            for (const auto& br : scan_filter->disjunctive_branches) {
              bool br_ok = true;
              for (const SimpleComparePredicate& sp : br.simple) {
                bool sup = false;
                if (!eval_pred_on_dict_code(sp, dict, code, &sup)) {
                  br_ok = false;
                  break;
                }
              }
              if (br_ok) {
                any_br_ok = true;
                break;
              }
            }
            if (!any_br_ok) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          disjunctive_covered_by_dict = true;
        }
      }
      for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
        const PrebuiltMembershipPredicate& pred =
            scan_filter->prebuilt_membership[mi];
        if (pred.runtime_filter != nullptr && !pred.negated) {
          const slot_t full_col = to_full_col(pred.column);
          if (full_col < table.GetSchema().ColumnCount()) {
            const Column& col = table.GetSchema().GetColumn(full_col);
            if (col.Type() == pred.domain_type &&
                (col.Type() == ValueType::kInt64 ||
                 col.Type() == ValueType::kDate) &&
                !col.IsUnsigned()) {
              lstable_extra_rfs.push_back(
                  LSTableExtraRf{full_col, pred.runtime_filter.get()});
              prebuilt_membership_covered[mi] = 1;
            }
          }
        }
      }
    }
  }
  const auto eval_simple_on_lstable =
      [&](const LSTablePageBlock& blk, uint32_t r,
          const SimpleComparePredicate& pred, bool* checked) -> bool {
    *checked = true;
    const slot_t full_col = to_full_col(pred.column);
    if (full_col >= blk.columns.size()) {
      *checked = false;
      return true;
    }
    const LSTableColumnBlock& col = blk.columns[full_col];
    if (col.IsNull(r)) {
      return false;
    }
    if (pred.column_payload) {
      const slot_t rfull_col = to_full_col(pred.right_column);
      if (rfull_col >= blk.columns.size()) {
        *checked = false;
        return true;
      }
      const LSTableColumnBlock& rcol = blk.columns[rfull_col];
      if (rcol.IsNull(r)) {
        return false;
      }
      if ((col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
          (rcol.type == ValueType::kInt64 || rcol.type == ValueType::kDate) &&
          !col.is_unsigned && !rcol.is_unsigned) {
        return ComparePositional(pred.op, col.int_cells[r], rcol.int_cells[r]);
      }
      if (col.type == ValueType::kDouble && rcol.type == ValueType::kDouble) {
        return ComparePositional(pred.op,
                                 std::bit_cast<double>(col.int_cells[r]),
                                 std::bit_cast<double>(rcol.int_cells[r]));
      }
      if (col.type == ValueType::kVarChar && rcol.type == ValueType::kVarChar) {
        return ComparePositional(pred.op, col.VarcharAt(r), rcol.VarcharAt(r));
      }
      *checked = false;
      return true;
    }
    if (pred.constant.IsNull()) {
      return false;
    }
    if (pred.int_payload &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
        !col.is_unsigned && !pred.constant.IsUnsigned()) {
      return ComparePositional(pred.op, col.int_cells[r], pred.int_constant);
    }
    if (pred.double_payload && col.type == ValueType::kDouble) {
      return ComparePositional(pred.op, std::bit_cast<double>(col.int_cells[r]),
                               pred.double_constant);
    }
    if (col.type == ValueType::kVarChar &&
        pred.constant.type == ValueType::kVarChar &&
        !pred.constant.IsCaseInsensitive()) {
      const std::string_view text = col.VarcharAt(r);
      const std::string_view pat = pred.constant.value.varchar_value;
      if (pred.op == BinaryOperation::kLike ||
          pred.op == BinaryOperation::kNotLike) {
        const bool matched = FastLikeMatch(text, pat);
        return pred.op == BinaryOperation::kLike ? matched : !matched;
      }
      if (pred.plain_string_payload) {
        return ComparePositional(pred.op, text, pat);
      }
    }
    *checked = false;
    return true;
  };
  const auto match_lstable_pre_materialize = [&](const LSTablePageBlock& blk,
                                                 uint32_t r,
                                                 bool* fully_decided) -> bool {
    *fully_decided = false;
    if (!filter_during_scan || scan_filter == nullptr) {
      *fully_decided = true;
      return true;
    }
    if (scan_filter->compile_decided_reject) {
      *fully_decided = true;
      return false;
    }
    if (scan_filter->needs_unsigned_tagging) {
      return true;
    }
    bool all_simple_checked = true;
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      if (si < simple_covered_by_dict.size() &&
          simple_covered_by_dict[si] != 0) {
        continue;
      }
      bool checked = false;
      if (!eval_simple_on_lstable(blk, r, scan_filter->simple[si], &checked)) {
        return false;
      }
      if (!checked) {
        all_simple_checked = false;
      }
    }
    bool all_disjunctive_checked = scan_filter->disjunctive_branches.empty() ||
                                   disjunctive_covered_by_dict;
    {
      for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
        if (pi < prefix_covered_by_dict.size() &&
            prefix_covered_by_dict[pi] != 0) {
          continue;
        }
        const PrefixInPredicate& pred = scan_filter->prefix_in[pi];
        const slot_t full_col = to_full_col(pred.column);
        if (full_col >= blk.columns.size()) {
          return false;
        }
        const LSTableColumnBlock& col = blk.columns[full_col];
        if (col.IsNull(r) || col.type != ValueType::kVarChar) {
          return false;
        }
        const std::string_view text = col.VarcharAt(r);
        if (!std::ranges::any_of(pred.prefixes, [text](const std::string& p) {
              return text.starts_with(p);
            })) {
          return false;
        }
      }
      if (!scan_filter->disjunctive_branches.empty() &&
          !disjunctive_covered_by_dict) {
        bool any_branch_possible = false;
        bool every_branch_complete = true;
        for (const auto& br : scan_filter->disjunctive_branches) {
          if (!br.residual.empty()) {
            every_branch_complete = false;
          }
          bool branch_ok = true;
          for (const SimpleComparePredicate& sp : br.simple) {
            bool checked = false;
            if (!eval_simple_on_lstable(blk, r, sp, &checked)) {
              branch_ok = false;
              break;
            }
            if (!checked) {
              every_branch_complete = false;
            }
          }
          if (branch_ok) {
            for (const PrefixInPredicate& pp : br.prefix_in) {
              const slot_t fc = to_full_col(pp.column);
              if (fc >= blk.columns.size()) {
                every_branch_complete = false;
                continue;
              }
              const LSTableColumnBlock& pcol = blk.columns[fc];
              if (pcol.IsNull(r) || pcol.type != ValueType::kVarChar) {
                branch_ok = false;
                break;
              }
              const std::string_view text = pcol.VarcharAt(r);
              if (!std::ranges::any_of(pp.prefixes,
                                       [text](const std::string& p) {
                                         return text.starts_with(p);
                                       })) {
                branch_ok = false;
                break;
              }
            }
          }
          if (branch_ok) {
            any_branch_possible = true;
            if (every_branch_complete && br.residual.empty()) {
              break;
            }
          }
        }
        if (!any_branch_possible) {
          return false;
        }
        all_disjunctive_checked = every_branch_complete;
      }
      for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
        if (mi < prebuilt_membership_covered.size() &&
            prebuilt_membership_covered[mi] != 0) {
          continue;
        }
        const PrebuiltMembershipPredicate& pred =
            scan_filter->prebuilt_membership[mi];
        if (pred.runtime_filter != nullptr) {
          const slot_t full_col = to_full_col(pred.column);
          if (full_col >= blk.columns.size()) {
            return false;
          }
          const LSTableColumnBlock& col = blk.columns[full_col];
          if (col.IsNull(r) || col.type != pred.domain_type ||
              pred.runtime_filter->Reject(col.int_cells[r])) {
            return false;
          }
          continue;
        }
        const FlatIntSet& values = *pred.values;
        if (values.Empty() && !pred.contains_null) {
          if (!pred.negated) {
            return false;
          }
          continue;
        }
        const slot_t full_col = to_full_col(pred.column);
        if (full_col >= blk.columns.size()) {
          return false;
        }
        const LSTableColumnBlock& col = blk.columns[full_col];
        if (col.IsNull(r)) {
          if (pred.existence && pred.negated) {
            continue;
          }
          return false;
        }
        if (col.type != pred.domain_type) {
          return false;
        }
        const bool found = values.Contains(col.int_cells[r]);
        const bool ok =
            found ? !pred.negated : (!pred.contains_null && pred.negated);
        if (!ok) {
          return false;
        }
      }
    }
    *fully_decided =
        all_simple_checked &&
        (!has_post_simple_filter ||
         (worker_programs.complete &&
          scan_filter->prebuilt_scalar_compare.empty() &&
          all_disjunctive_checked && scan_filter->residual.empty()));
    return true;
  };
  const bool lstable_fast_loop_decides_all = [&]() {
    if (lstable_store == nullptr) {
      return false;
    }
    if (!filter_during_scan || scan_filter == nullptr) {
      return true;
    }
    if (scan_filter->compile_decided_reject ||
        scan_filter->needs_unsigned_tagging) {
      return false;
    }
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      if (si >= simple_covered_by_dict.size() ||
          simple_covered_by_dict[si] == 0) {
        return false;
      }
    }
    for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
      if (pi >= prefix_covered_by_dict.size() ||
          prefix_covered_by_dict[pi] == 0) {
        return false;
      }
    }
    if (!scan_filter->disjunctive_branches.empty() &&
        !disjunctive_covered_by_dict) {
      return false;
    }
    for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
      if (mi >= prebuilt_membership_covered.size() ||
          prebuilt_membership_covered[mi] == 0) {
        return false;
      }
    }
    if (!has_post_simple_filter) {
      return true;
    }
    return worker_programs.complete &&
           scan_filter->prebuilt_scalar_compare.empty() &&
           scan_filter->residual.empty();
  }();
  const bool has_ample_memory_budget = Relation::HasAmpleMemoryBudget();
  const bool fast_in_memory_append =
      lstable_fast_loop_decides_all && has_ample_memory_budget;
  const auto materialize_lstable_row =
      [&](const LSTablePageBlock& blk,
          const LSTableColumnBlock* const* proj_cols, size_t col_count,
          uint32_t r, Row* out_row) {
        out_row->values_.resize(col_count);
        for (size_t k = 0; k < col_count; ++k) {
          const LSTableColumnBlock& col =
              proj_cols != nullptr
                  ? *proj_cols[k]
                  : blk.columns[projection != nullptr ? (*projection)[k]
                                                      : static_cast<slot_t>(k)];
          Value& dst = out_row->values_[k];
          if (col.has_null && col.IsNull(r)) [[unlikely]] {
            if (dst.type != ValueType::kNull) {
              dst = Value();
            }
            continue;
          }
          switch (col.type) {
            case ValueType::kInt64:
              dst.type = ValueType::kInt64;
              dst.value.int_value = col.int_cells[r];
              if (col.is_unsigned) [[unlikely]] {
                dst.SetUnsigned();
              }
              break;
            case ValueType::kDate:
              dst.type = ValueType::kDate;
              dst.value.int_value = col.int_cells[r];
              break;
            case ValueType::kDouble:
              dst.type = ValueType::kDouble;
              dst.value.double_value = std::bit_cast<double>(col.int_cells[r]);
              break;
            case ValueType::kVarChar: {
              const std::string_view sv = col.VarcharAt(r);
              dst.type = ValueType::kVarChar;
              dst.owned_data.assign(sv.data(), sv.size());
              dst.value.varchar_value = dst.owned_data;
              break;
            }
            default:
              if (dst.type != ValueType::kNull) {
                dst = Value();
              }
              break;
          }
        }
      };
  const auto scan_worker = [&](size_t w) {
    try {
      auto& local = shards[w];
      if (lstable_store != nullptr) {
        local.rows.reserve(4096);
        const auto* peek_ptr = integer_peeks.empty() ? nullptr : &integer_peeks;
        const size_t total_blocks = lstable_store->blocks.size();
        (void)lstable_blocks_per_morsel;
        const size_t blk_begin = (total_blocks * w) / workers;
        const size_t blk_end = (total_blocks * (w + 1)) / workers;
        {
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const LSTablePageBlock* blk = &lstable_store->blocks[bi];
            if (blk->row_count == 0) {
              continue;
            }
            if (!blk->MatchesZoneMaps(peek_ptr, active_lstable_rf,
                                      full_key_column)) {
              continue;
            }
            bool zone_rejected = false;
            for (const LSTableDictColFilter& df : lstable_dict_filters) {
              const LSTableColumnBlock& dcol = blk->columns[df.column];
              if (((dcol.dict_mask[0] & df.pass_mask[0]) |
                   (dcol.dict_mask[1] & df.pass_mask[1]) |
                   (dcol.dict_mask[2] & df.pass_mask[2]) |
                   (dcol.dict_mask[3] & df.pass_mask[3])) == 0) {
                zone_rejected = true;
                break;
              }
            }
            if (zone_rejected) {
              continue;
            }
            for (const LSTableDoubleZonePredicate& dz : lstable_double_zones) {
              const LSTableColumnBlock& dcol = blk->columns[dz.column];
              if (!dcol.has_double_min_max) {
                zone_rejected = true;
                break;
              }
              switch (dz.op) {
                case BinaryOperation::kEquals:
                  if (dz.constant < dcol.min_double ||
                      dz.constant > dcol.max_double) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  if (dcol.min_double == dz.constant &&
                      dcol.max_double == dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (dcol.min_double >= dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (dcol.min_double > dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (dcol.max_double <= dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (dcol.max_double < dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                default:
                  break;
              }
              if (zone_rejected) {
                break;
              }
            }
            if (zone_rejected) {
              continue;
            }
            for (const LSTableIntColCompare& cc : lstable_col_compares) {
              const LSTableColumnBlock& lcol = blk->columns[cc.left_col];
              const LSTableColumnBlock& rcol = blk->columns[cc.right_col];
              if (!lcol.has_min_max || !rcol.has_min_max) {
                zone_rejected = true;
                break;
              }
              switch (cc.op) {
                case BinaryOperation::kEquals:
                  if (lcol.max_int < rcol.min_int ||
                      lcol.min_int > rcol.max_int) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  if (lcol.min_int == lcol.max_int &&
                      rcol.min_int == rcol.max_int &&
                      lcol.min_int == rcol.min_int) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (lcol.min_int >= rcol.max_int) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (lcol.min_int > rcol.max_int) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (lcol.max_int <= rcol.min_int) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (lcol.max_int < rcol.min_int) {
                    zone_rejected = true;
                  }
                  break;
                default:
                  break;
              }
              if (zone_rejected) {
                break;
              }
            }
            if (zone_rejected) {
              continue;
            }
            if (can_predecide_lstable) {
              for (const PrebuiltMembershipPredicate& pred :
                   scan_filter->prebuilt_membership) {
                if (pred.runtime_filter != nullptr) {
                  const slot_t full_col = to_full_col(pred.column);
                  if (full_col < blk->columns.size()) {
                    const LSTableColumnBlock& mcol = blk->columns[full_col];
                    if (mcol.type == pred.domain_type &&
                        (mcol.type == ValueType::kInt64 ||
                         mcol.type == ValueType::kDate)) {
                      if (!mcol.has_min_max ||
                          pred.runtime_filter->RejectRange(mcol.min_int,
                                                           mcol.max_int)) {
                        zone_rejected = true;
                        break;
                      }
                    }
                  }
                  continue;
                }
                if (pred.negated || pred.values->Empty()) {
                  continue;
                }
                const slot_t full_col = to_full_col(pred.column);
                if (full_col >= blk->columns.size()) {
                  continue;
                }
                const LSTableColumnBlock& mcol = blk->columns[full_col];
                if (mcol.type == pred.domain_type &&
                    (mcol.type == ValueType::kInt64 ||
                     mcol.type == ValueType::kDate)) {
                  if (!mcol.has_min_max || !pred.values->ContainsAnyInRange(
                                               mcol.min_int, mcol.max_int)) {
                    zone_rejected = true;
                    break;
                  }
                }
              }
              if (zone_rejected) {
                continue;
              }
            }
            const size_t col_count = projection != nullptr
                                         ? projection->size()
                                         : blk->columns.size();
            constexpr size_t kMaxFastProjCols = 16;
            const LSTableColumnBlock* proj_cols_buf[kMaxFastProjCols];
            const LSTableColumnBlock* const* proj_cols_ptr = nullptr;
            if (col_count <= kMaxFastProjCols) {
              for (size_t k = 0; k < col_count; ++k) {
                const slot_t col_idx = projection != nullptr
                                           ? (*projection)[k]
                                           : static_cast<slot_t>(k);
                proj_cols_buf[k] = &blk->columns[col_idx];
              }
              proj_cols_ptr = proj_cols_buf;
            }
            const LSTableColumnBlock* const rf_col =
                (active_lstable_rf != nullptr && full_key_column.has_value())
                    ? &blk->columns[*full_key_column]
                    : nullptr;
            const bool rf_has_dense =
                rf_col != nullptr && active_lstable_rf->has_dense_bits();
            const uint64_t* const rf_dense_bits =
                rf_has_dense ? active_lstable_rf->dense_bits_data() : nullptr;
            const size_t rf_dense_words =
                rf_has_dense ? active_lstable_rf->dense_bits_word_count() : 0;
            const int64_t rf_lo = rf_has_dense ? active_lstable_rf->Lo() : 0;
            const int64_t rf_hi = rf_has_dense ? active_lstable_rf->Hi() : 0;
            const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
            const bool rf_col_monotonic =
                rf_has_dense && !rf_col_has_null &&
                *full_key_column < lstable_store->column_is_monotonic.size() &&
                lstable_store->column_is_monotonic[*full_key_column] != 0;
            struct BlockExtraRf {
              const LSTableColumnBlock* col{nullptr};
              const uint64_t* dense_bits{nullptr};
              int64_t lo{0};
              int64_t hi{0};
              bool has_null{false};
              const RuntimeFilterGroup* group{nullptr};
            };
            constexpr size_t kMaxFastExtraRfs = 4;
            BlockExtraRf block_extra_rfs[kMaxFastExtraRfs];
            const size_t fast_extra_rf_count =
                std::min(lstable_extra_rfs.size(), kMaxFastExtraRfs);
            for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
              const LSTableExtraRf& erf = lstable_extra_rfs[ei];
              const LSTableColumnBlock* ecol = &blk->columns[erf.column];
              const bool has_dense = erf.group->has_dense_bits();
              block_extra_rfs[ei] = BlockExtraRf{
                  ecol,
                  has_dense ? erf.group->dense_bits_data() : nullptr,
                  has_dense ? erf.group->Lo() : 0,
                  has_dense ? erf.group->Hi() : 0,
                  ecol->has_null,
                  erf.group,
              };
            }
            for (uint32_t r = 0; r < blk->row_count; ++r) {
              if (rf_col != nullptr) {
                if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
                  continue;
                }
                const int64_t kv = rf_col->int_cells[r];
                if (rf_has_dense) {
                  bool accepted = false;
                  if (kv >= rf_lo && kv <= rf_hi) {
                    const uint64_t off = static_cast<uint64_t>(kv) -
                                         static_cast<uint64_t>(rf_lo);
                    accepted = (rf_dense_bits[off >> 6] &
                                (uint64_t{1} << (off & 63))) != 0;
                  }
                  if (!accepted) {
                    if (rf_col_monotonic) {
                      if (kv > rf_hi) {
                        break;
                      }
                      int64_t next_valid_key = rf_lo;
                      if (kv >= rf_lo) {
                        const uint64_t next_off = static_cast<uint64_t>(kv) -
                                                  static_cast<uint64_t>(rf_lo) +
                                                  1ULL;
                        size_t w_idx = static_cast<size_t>(next_off >> 6);
                        if (w_idx >= rf_dense_words) {
                          break;
                        }
                        uint64_t word =
                            rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                        while (word == 0) {
                          if (++w_idx >= rf_dense_words) {
                            break;
                          }
                          word = rf_dense_bits[w_idx];
                        }
                        if (w_idx >= rf_dense_words) {
                          break;
                        }
                        const uint64_t found_off =
                            (static_cast<uint64_t>(w_idx) << 6) +
                            static_cast<uint64_t>(std::countr_zero(word));
                        next_valid_key =
                            rf_lo + static_cast<int64_t>(found_off);
                        if (next_valid_key > rf_hi) {
                          break;
                        }
                      }
                      if (rf_col->has_min_max &&
                          next_valid_key > rf_col->max_int) {
                        break;
                      }
                      const uint32_t row_count = blk->row_count;
                      const int64_t* const cells = rf_col->int_cells.data();
                      if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                        uint32_t lo = r + 1;
                        uint32_t step = 4;
                        while (lo + step < row_count &&
                               cells[lo + step] < next_valid_key) {
                          lo += step;
                          step <<= 1;
                        }
                        const uint32_t hi = std::min(row_count, lo + step + 1);
                        const int64_t* found = std::lower_bound(
                            cells + lo, cells + hi, next_valid_key);
                        r = static_cast<uint32_t>(found - cells) - 1;
                      }
                    }
                    continue;
                  }
                } else if (active_lstable_rf->Reject(kv)) {
                  continue;
                }
              }
              if (fast_extra_rf_count != 0) {
                bool extra_ok = true;
                for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
                  const BlockExtraRf& erf = block_extra_rfs[ei];
                  if (erf.has_null && erf.col->IsNull(r)) [[unlikely]] {
                    extra_ok = false;
                    break;
                  }
                  const int64_t kv = erf.col->int_cells[r];
                  if (erf.dense_bits != nullptr) {
                    if (kv < erf.lo || kv > erf.hi) {
                      extra_ok = false;
                      break;
                    }
                    const uint64_t off = static_cast<uint64_t>(kv) -
                                         static_cast<uint64_t>(erf.lo);
                    if ((erf.dense_bits[off >> 6] &
                         (uint64_t{1} << (off & 63))) == 0) {
                      extra_ok = false;
                      break;
                    }
                  } else if (erf.group->Reject(kv)) {
                    extra_ok = false;
                    break;
                  }
                }
                if (extra_ok && lstable_extra_rfs.size() > kMaxFastExtraRfs)
                    [[unlikely]] {
                  for (size_t ei = kMaxFastExtraRfs;
                       ei < lstable_extra_rfs.size(); ++ei) {
                    const LSTableExtraRf& erf = lstable_extra_rfs[ei];
                    const LSTableColumnBlock& ecol = blk->columns[erf.column];
                    if ((ecol.has_null && ecol.IsNull(r)) ||
                        erf.group->Reject(ecol.int_cells[r])) {
                      extra_ok = false;
                      break;
                    }
                  }
                }
                if (!extra_ok) {
                  continue;
                }
              }
              if (!lstable_dict_filters.empty()) {
                bool dict_ok = true;
                for (const LSTableDictColFilter& df : lstable_dict_filters) {
                  const LSTableColumnBlock& dcol = blk->columns[df.column];
                  if (dcol.dict_codes.empty() ||
                      df.pass[dcol.dict_codes[r]] == 0) {
                    dict_ok = false;
                    break;
                  }
                }
                if (!dict_ok) {
                  continue;
                }
              }
              if (peek_ptr != nullptr) {
                bool peek_ok = true;
                for (const IntegerPeekCompare& pred : *peek_ptr) {
                  const LSTableColumnBlock& pcol = blk->columns[pred.column];
                  if ((pcol.has_null && pcol.IsNull(r)) ||
                      !ComparePositional(pred.op, pcol.int_cells[r],
                                         pred.constant)) {
                    peek_ok = false;
                    break;
                  }
                }
                if (!peek_ok) {
                  continue;
                }
              }
              if (!lstable_col_compares.empty()) {
                bool cc_ok = true;
                for (const LSTableIntColCompare& cc : lstable_col_compares) {
                  const LSTableColumnBlock& lcol = blk->columns[cc.left_col];
                  const LSTableColumnBlock& rcol = blk->columns[cc.right_col];
                  if ((lcol.has_null && lcol.IsNull(r)) ||
                      (rcol.has_null && rcol.IsNull(r)) ||
                      !ComparePositional(cc.op, lcol.int_cells[r],
                                         rcol.int_cells[r])) {
                    cc_ok = false;
                    break;
                  }
                }
                if (!cc_ok) {
                  continue;
                }
              }
              ++shard_seen[w];
              if (fast_in_memory_append) {
                Row& out_row = local.rows.emplace_back();
                materialize_lstable_row(*blk, proj_cols_ptr, col_count, r,
                                        &out_row);
                ++shard_out[w];
                continue;
              }
              bool fully_decided = lstable_fast_loop_decides_all;
              if (!fully_decided &&
                  !match_lstable_pre_materialize(*blk, r, &fully_decided)) {
                continue;
              }
              if (fully_decided && has_ample_memory_budget) {
                Row& out_row = local.rows.emplace_back();
                materialize_lstable_row(*blk, proj_cols_ptr, col_count, r,
                                        &out_row);
                ++shard_out[w];
                continue;
              }
              Row row;
              materialize_lstable_row(*blk, proj_cols_ptr, col_count, r, &row);
              bool matches = true;
              if (!fully_decided) {
                if (worker_programs.complete) {
                  switch (MatchScanFilterWorker(
                      row, result_schema, *scan_filter, worker_programs)) {
                    case 0:
                      matches = false;
                      break;
                    case 1:
                      break;
                    default:
                      deferred_rows[w].push_back(std::move(row));
                      matches = false;
                      break;
                  }
                } else if (filter_during_scan && scan_filter &&
                           !scan_filter->needs_unsigned_tagging) {
                  for (const SimpleComparePredicate& pred :
                       scan_filter->simple) {
                    if (!MatchSimpleCompare(row, pred)) {
                      matches = false;
                      break;
                    }
                  }
                }
              }
              if (matches) {
                local.AddRow(std::move(row));
                ++shard_out[w];
              }
            }
          }
        }
        if (has_ample_memory_budget) {
          local.peak_intermediate_rows =
              std::max(local.peak_intermediate_rows, local.rows.size());
        }
        return;
      }
      // Static rotation: worker w scans morsels w, w+W, w+2W, ... .
      // The atomic counter this replaced handed morsels to whichever
      // worker grabbed them, so shard contents — and therefore the
      // shard-order concatenation below — changed run to run.  With
      // fixed ownership the concatenated relation is a deterministic
      // function of the table state, which the FP aggregates downstream
      // (TPC-DS rank boundaries) read as exact bits.
      for (size_t mi = w; mi < morsels.size(); mi += workers) {
        Iterator iterator = table.BeginMorselScan(
            context.txn_, morsels[mi], proj_opt, nullptr, full_key_column,
            integer_peeks.empty() ? nullptr : &integer_peeks, runtime_filter,
            runtime_filter_type);
        while (iterator.IsValid()) {
          ++shard_seen[w];
          bool matches = true;
          if (worker_programs.complete) {
            switch (MatchScanFilterWorker(*iterator, result_schema,
                                          *scan_filter, worker_programs)) {
              case 0:
                matches = false;
                break;
              case 1:
                break;
              default:
                deferred_rows[w].push_back(std::move(*iterator));
                matches = false;
                break;
            }
          } else if (filter_during_scan && scan_filter &&
                     !scan_filter->needs_unsigned_tagging) {
            // Worker threads only evaluate the simple
            // (constant-vs-column) predicates. Residual predicates may
            // contain query expressions whose evaluation touches the
            // shared TransactionContext and its explicit execution
            // runtime, neither of which is safe on worker threads.
            for (const SimpleComparePredicate& pred : scan_filter->simple) {
              if (!MatchSimpleCompare(*iterator, pred)) {
                matches = false;
                break;
              }
            }
          }
          if (matches) {
            // The row is fully consumed here: move it instead of paying
            // one string copy per cell (the next ++ re-decodes anyway).
            local.AddRow(std::move(*iterator));
            ++shard_out[w];
          }
          ++iterator;
        }
        if (iterator.GetStatus() != Status::kSuccess) {
          std::scoped_lock lock(error_mu);
          if (error != nullptr && *error == Status::kSuccess) {
            *error = iterator.GetStatus();
          }
          break;
        }
      }
    } catch (...) {
      std::scoped_lock lock(error_mu);
      if (!error_ex) {
        error_ex = std::current_exception();
      }
    }
  };
  {
    std::vector<std::jthread> threads;
    threads.reserve(workers - 1);
    for (size_t w = 1; w < workers; ++w) {
      try {
        threads.emplace_back(scan_worker, w);
      } catch (...) {
        std::scoped_lock lock(error_mu);
        if (!error_ex) error_ex = std::current_exception();
        break;
      }
    }
    // Keep the same static morsel ownership and merge order while making
    // the calling SQL worker useful. Starting one fewer thread removes a
    // launch/join pair from each scan without changing aggregate ordering.
    scan_worker(0);
  }
  if (error_ex) {
    if (error != nullptr) {
      try {
        std::rethrow_exception(error_ex);
      } catch (const std::exception& ex) {
        *error = StatusError(StatusCode::kRuntimeError, ex.what());
      } catch (...) {
        *error = StatusError(StatusCode::kRuntimeError,
                             "parallel table scan worker failed");
      }
    }
    return false;
  }
  // A spill that fails mid-append is a truncated result, not a clean one.
  // Every merge branch below must surface the worker-shard status; the sticky
  // Relation::spill_error_ is otherwise discarded by ResetContents() and the
  // caller sees a short but "successful" result.
  const auto record_error = [&](Status status) {
    if (error != nullptr && error->ok() && status != Status::kSuccess) {
      *error = std::move(status);
    }
  };
  size_t total_in_memory = 0;
  for (size_t w = 0; w < workers; ++w) {
    if (!shards[w].HasSpill()) {
      total_in_memory += shards[w].rows.size();
    }
  }
  if (worker_programs.complete) {
    bool can_parallel_merge = has_ample_memory_budget && workers > 1 &&
                              total_in_memory >= 16384 &&
                              result->rows.empty() && !result->HasSpill();
    if (can_parallel_merge) {
      for (size_t w = 0; w < workers; ++w) {
        if (shards[w].HasSpill() || !deferred_rows[w].empty()) {
          can_parallel_merge = false;
          break;
        }
      }
    }
    if (can_parallel_merge) {
      std::vector<size_t> shard_offsets(workers + 1, 0);
      for (size_t w = 0; w < workers; ++w) {
        shard_offsets[w + 1] = shard_offsets[w] + shards[w].rows.size();
      }
      result->rows.resize(total_in_memory);
      {
        std::vector<std::jthread> merge_threads;
        merge_threads.reserve(workers - 1);
        const auto move_shard = [&](size_t w) {
          std::move(shards[w].rows.begin(), shards[w].rows.end(),
                    result->rows.begin() +
                        static_cast<std::ptrdiff_t>(shard_offsets[w]));
          shards[w].rows.clear();
          shards[w].rows.shrink_to_fit();
        };
        for (size_t w = 1; w < workers; ++w) {
          merge_threads.emplace_back(move_shard, w);
        }
        move_shard(0);
      }
      result->peak_intermediate_rows =
          std::max(result->peak_intermediate_rows, result->rows.size());
    } else {
      if (total_in_memory > 0 && (workers > 1 || !result->rows.empty())) {
        result->rows.reserve(result->rows.size() + total_in_memory);
      }
      // Workers decided every row except the deferred ones; move results and
      // re-check the deferred rows with the contextful evaluator.
      for (size_t w = 0; w < workers; ++w) {
        for (Row& row : deferred_rows[w]) {
          Status deferred_error{Status::kSuccess};
          if (MatchScanFilter(row, result_schema, *scan_filter, outer, context,
                              ctes, &deferred_error)) {
            result->AddRow(std::move(row));
          }
          if (!deferred_error.ok() && error != nullptr && error->ok()) {
            *error = std::move(deferred_error);
          }
        }
        deferred_rows[w].clear();
        record_error(shards[w].FinishSpill());
        if (!shards[w].HasSpill()) {
          result->MergeInMemoryShard(std::move(shards[w]));
        } else {
          for (Row& row : shards[w].rows) {
            result->AddRow(std::move(row));
          }
          if (shards[w].spill) {
            record_error(shards[w].spill->ForEachRow(
                [&](const Row& row) { result->AddRow(row); }));
          }
          if (shards[w].spill_tail_) {
            record_error(shards[w].spill_tail_->ForEachRow(
                [&](const Row& row) { result->AddRow(row); }));
          }
        }
        record_error(shards[w].GetStatus());
        shards[w].ResetContents();
      }
    }
  } else if (has_post_simple_filter) {
    // Re-apply every component the workers did not decide. Prebuilt sets
    // and prefix tests have no residual AST, so copying only residual and
    // OR branches silently drops them (notably NOT IN in TPC-H Q16).
    CompiledScanFilter residual_only = *scan_filter;
    if (!scan_filter->needs_unsigned_tagging) residual_only.simple.clear();
    if (can_predecide_lstable) {
      residual_only.prefix_in.clear();
      residual_only.prebuilt_membership.clear();
      if (disjunctive_covered_by_dict) {
        residual_only.disjunctive_branches.clear();
      }
    }
    Relation candidates(context.execution_runtime());
    candidates.schema = result_schema;
    if (total_in_memory > 0) {
      candidates.rows.reserve(total_in_memory);
    }
    for (size_t w = 0; w < workers; ++w) {
      record_error(shards[w].FinishSpill());
      if (!shards[w].HasSpill()) {
        candidates.MergeInMemoryShard(std::move(shards[w]));
      } else {
        for (Row& row : shards[w].rows) {
          candidates.AddRow(std::move(row));
        }
        if (shards[w].spill) {
          record_error(shards[w].spill->ForEachRow(
              [&](const Row& row) { candidates.AddRow(row); }));
        }
        if (shards[w].spill_tail_) {
          record_error(shards[w].spill_tail_->ForEachRow(
              [&](const Row& row) { candidates.AddRow(row); }));
        }
      }
      record_error(shards[w].GetStatus());
      shards[w].ResetContents();
    }
    record_error(candidates.FinishSpill());
    record_error(candidates.GetStatus());
    ScopedCorrelatedOuter correlated_outer(context, candidates);
    ColumnLookupCache lookup_cache;
    for (Row& row : candidates.rows) {
      if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                           ctes, error, &lookup_cache)) {
        continue;
      }
      result->AddRow(std::move(row));
    }
    if (candidates.spill) {
      record_error(candidates.spill->ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                             ctes, error, &lookup_cache)) {
          return;
        }
        result->AddRow(row);
      }));
    }
    if (candidates.spill_tail_) {
      record_error(candidates.spill_tail_->ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                             ctes, error, &lookup_cache)) {
          return;
        }
        result->AddRow(row);
      }));
    }
  } else {
    if (total_in_memory > 0) {
      result->rows.reserve(result->rows.size() + total_in_memory);
    }
    for (size_t w = 0; w < workers; ++w) {
      record_error(shards[w].FinishSpill());
      if (!shards[w].HasSpill()) {
        result->MergeInMemoryShard(std::move(shards[w]));
      } else {
        for (Row& row : shards[w].rows) {
          result->AddRow(std::move(row));
        }
        if (shards[w].spill) {
          record_error(shards[w].spill->ForEachRow(
              [&](const Row& row) { result->AddRow(row); }));
        }
        if (shards[w].spill_tail_) {
          record_error(shards[w].spill_tail_->ForEachRow(
              [&](const Row& row) { result->AddRow(row); }));
        }
      }
      record_error(shards[w].GetStatus());
      shards[w].ResetContents();
    }
  }
  for (size_t w = 0; w < workers; ++w) {
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->scan_rows += shard_seen[w];
      context.execution_runtime()->scan_values_available +=
          shard_seen[w] * table.GetSchema().ColumnCount();
      context.execution_runtime()->scan_values_decoded +=
          shard_out[w] * result_schema.ColumnCount();
      context.execution_runtime()->scan_output_rows += shard_out[w];
    }
  }
  return true;
}

}  // namespace tinylamb::relational_detail
