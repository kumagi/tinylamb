/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "index/b_plus_tree_oracle.hpp"

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <iterator>
#include <map>
#include <sstream>

#include "common/status_or.hpp"
#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

std::string PoolKey(std::mt19937& rng, size_t key_pool) {
  const size_t index = static_cast<size_t>(rng()) % key_pool;
  std::ostringstream out;
  out << 'k';
  if (index < 10) {
    out << '0';
  }
  out << index;
  return out.str();
}

std::string EdgeKey(std::mt19937& rng) {
  switch (rng() % 4) {
    case 0:
      return "";
    case 1:
      return "a";
    case 2:
      return {std::string(64, static_cast<char>('a' + (rng() % 26)))};
    default:
      return {std::string(200, static_cast<char>('a' + (rng() % 26)))};
  }
}

std::string RandomValue(std::mt19937& rng) {
  // Occasional large values force leaf splits and foster chains.
  const size_t length = rng() % 10 == 0 ? 300 : static_cast<size_t>(rng() % 16);
  std::string out;
  out.reserve(length + 1);
  out.push_back('v');
  for (size_t i = 0; i < length; ++i) {
    out.push_back(static_cast<char>('0' + (rng() % 10)));
  }
  return out;
}

std::string DescribeOp(const BPlusTreeOp& op) {
  std::ostringstream out;
  switch (op.kind) {
    case BPlusTreeOp::Kind::kInsert:
      out << "Insert";
      break;
    case BPlusTreeOp::Kind::kUpdate:
      out << "Update";
      break;
    case BPlusTreeOp::Kind::kDelete:
      out << "Delete";
      break;
  }
  out << "(" << op.key << ")";
  return out.str();
}

std::filesystem::path WorkDir(const std::string& tag) {
  return std::filesystem::temp_directory_path() /
         ("tinylamb_bpt_oracle_" + tag);
}

// Full point-read verification of the model against the tree.
std::string VerifyModel(BPlusTree& tree, Transaction& txn,
                        const std::map<std::string, std::string>& model) {
  for (const auto& [key, value] : model) {
    StatusOr<std::string_view> read = tree.Read(txn, key);
    if (!read) {
      std::ostringstream status;
      status << read.GetStatus();
      return "Read(" + key + ") failed: " + status.str();
    }
    if (read.Value() != value) {
      return "Read(" + key + ") mismatch";
    }
  }
  return "";
}

// Ordered-scan verification against the model in both directions plus one
// bounded range.
std::string VerifyScans(BPlusTree& tree, Transaction& txn,
                        const std::map<std::string, std::string>& model,
                        std::mt19937& range_rng) {
  // Ascending full scan.
  {
    BPlusTreeIterator it = tree.Begin(txn);
    auto expected = model.begin();
    while (it.IsValid()) {
      if (expected == model.end()) {
        return "ascending scan yields extra key " + it.Key();
      }
      if (it.Key() != expected->first || it.Value() != expected->second) {
        return "ascending scan mismatch at " + expected->first;
      }
      ++it;
      ++expected;
    }
    if (expected != model.end()) {
      return "ascending scan misses " + expected->first;
    }
  }
  // Descending full scan (operator-- walks backwards from the maximum).
  {
    BPlusTreeIterator it = tree.Begin(txn, "", "", false);
    auto expected = model.rbegin();
    while (it.IsValid()) {
      if (expected == model.rend()) {
        return "descending scan yields extra key " + it.Key();
      }
      if (it.Key() != expected->first || it.Value() != expected->second) {
        return "descending scan mismatch at " + expected->first;
      }
      --it;
      ++expected;
    }
    if (expected != model.rend()) {
      return "descending scan misses keys";
    }
  }
  // Bounded range scan [left, right]: both bounds are inclusive (a scan of
  // [10, 19] yields exactly the 10 rows 10..19), matching
  // [lower_bound(left), upper_bound(right)).
  if (!model.empty()) {
    auto left_it = model.begin();
    std::advance(left_it, static_cast<ptrdiff_t>(range_rng() % model.size()));
    const auto remaining = std::distance(left_it, model.end());
    auto last_it = left_it;
    // remaining >= 1 here (left_it != end), so last_it stays dereferenceable.
    std::advance(last_it, static_cast<ptrdiff_t>(
                              range_rng() % static_cast<size_t>(remaining)));
    const std::string left = left_it->first;
    const std::string right = last_it->first;
    const auto range_end = std::next(last_it);
    BPlusTreeIterator it = tree.Begin(txn, left, right, true);
    auto expected = left_it;
    while (it.IsValid()) {
      if (expected == range_end) {
        std::string detail = "range scan [";
        detail += left;
        detail += ",";
        detail += right;
        detail += "] yields extra key ";
        detail += it.Key();
        return detail;
      }
      if (it.Key() != expected->first || it.Value() != expected->second) {
        return "range scan mismatch at " + expected->first;
      }
      ++it;
      ++expected;
    }
    if (expected != range_end) {
      return "range scan [" + left + "," + right + "] misses " +
             expected->first;
    }
  }
  return "";
}

}  // namespace

std::vector<BPlusTreeOp> GenerateBPlusTreeOps(
    std::mt19937& rng, const BPlusTreeGenConfig& config) {
  const auto op_count = static_cast<size_t>(rng() % (config.max_ops + 1));
  std::vector<BPlusTreeOp> ops;
  ops.reserve(op_count);
  for (size_t i = 0; i < op_count; ++i) {
    BPlusTreeOp op;
    const auto pick = static_cast<uint32_t>(rng() % 10);
    if (pick < 5) {
      op.kind = BPlusTreeOp::Kind::kInsert;
    } else if (pick < 8) {
      op.kind = BPlusTreeOp::Kind::kUpdate;
    } else {
      op.kind = BPlusTreeOp::Kind::kDelete;
    }
    // 1 in 10 operations uses a structural edge key.
    op.key = rng() % 10 == 0 ? EdgeKey(rng) : PoolKey(rng, config.key_pool);
    op.value = op.kind == BPlusTreeOp::Kind::kDelete ? "" : RandomValue(rng);
    ops.push_back(std::move(op));
  }
  return ops;
}

std::string CheckBPlusTreeEquivalence(const std::vector<BPlusTreeOp>& ops,
                                      const std::string& tag) {
  const std::filesystem::path dir = WorkDir(tag);
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const std::string db = (dir / "tree.db").string();
  const std::string log = (dir / "tree.log").string();
  std::map<std::string, std::string> model;
  std::string problem;
  {
    auto page_manager = PageManager::Create(db, 20).MoveValue();
    auto logger = Logger::Create(log).MoveValue();
    LockManager lock_manager;
    RecoveryManager recovery_manager(log, page_manager->GetPool());
    TransactionManager manager(page_manager.get(), logger.get(),
                               &recovery_manager);
    page_id_t root = 0;
    {
      Transaction setup = manager.Begin();
      PageRef page =
          page_manager->AllocateNewPage(setup, PageType::kLeafPage).MoveValue();
      root = page->PageID();
      if (setup.PreCommit() != Status::kSuccess) {
        return "setup commit failed";
      }
    }
    BPlusTree tree(root);
    Transaction txn = manager.Begin();
    // Fixed seed: the oracle replays a deterministic op stream.
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp)
    std::mt19937 range_rng(0xC0FFEE);
    for (size_t i = 0; i < ops.size(); ++i) {
      const BPlusTreeOp& op = ops[i];
      const std::string where =
          "op " + std::to_string(i) + " " + DescribeOp(op);
      switch (op.kind) {
        case BPlusTreeOp::Kind::kInsert:
          if (tree.Insert(txn, op.key, op.value) == Status::kSuccess) {
            model[op.key] = op.value;
          }
          break;
        case BPlusTreeOp::Kind::kUpdate:
          if (tree.Update(txn, op.key, op.value) == Status::kSuccess) {
            model[op.key] = op.value;
          }
          break;
        case BPlusTreeOp::Kind::kDelete:
          if (tree.Delete(txn, op.key) == Status::kSuccess) {
            model.erase(op.key);
          }
          break;
      }
      if (!tree.SanityCheckForTest(page_manager.get())) {
        problem = "SanityCheck failed after " + where;
        break;
      }
      if (i % 25 == 24) {
        problem = VerifyModel(tree, txn, model);
        if (!problem.empty()) {
          problem += " after " + where;
          break;
        }
      }
    }
    if (problem.empty()) {
      problem = VerifyModel(tree, txn, model);
    }
    if (problem.empty()) {
      problem = VerifyScans(tree, txn, model, range_rng);
    }
  }
  std::filesystem::remove_all(dir);
  return problem;
}

std::vector<BPlusTreeOp> ShrinkBPlusTreeOps(const std::vector<BPlusTreeOp>& ops,
                                            const std::string& tag) {
  if (CheckBPlusTreeEquivalence(ops, tag).empty()) {
    return ops;
  }
  std::vector<BPlusTreeOp> current = ops;
  for (size_t i = 0; i < current.size();) {
    std::vector<BPlusTreeOp> candidate = current;
    candidate.erase(candidate.begin() + static_cast<ptrdiff_t>(i));
    if (!CheckBPlusTreeEquivalence(candidate, tag).empty()) {
      current = std::move(candidate);
    } else {
      ++i;
    }
  }
  return current;
}

}  // namespace tinylamb
