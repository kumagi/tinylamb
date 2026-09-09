/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "index/lsm_tree_oracle.hpp"

#include <filesystem>
#include <map>
#include <sstream>

#include "index/lsm_tree.hpp"

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

std::string RandomValue(std::mt19937& rng) {
  // Occasional large values cross the blob-reference threshold and exercise
  // the value-log path.
  const size_t length = rng() % 10 == 0 ? 300 : static_cast<size_t>(rng() % 16);
  std::string out;
  out.reserve(length + 1);
  out.push_back('v');
  for (size_t i = 0; i < length; ++i) {
    out.push_back(static_cast<char>('0' + (rng() % 10)));
  }
  return out;
}

std::filesystem::path WorkDir(const std::string& tag) {
  return std::filesystem::temp_directory_path() /
         ("tinylamb_lsm_oracle_" + tag);
}

// Point-read verification of the model against the tree.
std::string VerifyReads(LSMTree& tree,
                        const std::map<std::string, std::string>& model) {
  for (const auto& [key, value] : model) {
    if (!tree.Contains(key)) {
      return "Contains(" + key + ") is false";
    }
    StatusOr<std::string> read = tree.Read(key);
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

// Ordered view-scan verification against the model.
std::string VerifyViewScan(LSMTree& tree,
                           const std::map<std::string, std::string>& model) {
  LSMView view = tree.GetView();
  auto expected = model.begin();
  auto actual_so = view.Begin();
  for (LSMView::Iterator actual = actual_so.MoveValue(); actual.IsValid();
       ++actual, ++expected) {
    const std::string key = actual.Key().MoveValue();
    if (expected == model.end()) {
      return "view yields extra key " + key;
    }
    if (key != expected->first) {
      return "view key " + key + " != model " + expected->first;
    }
    if (actual.Value().MoveValue() != expected->second) {
      return "view value mismatch at " + key;
    }
    if (!tree.Contains(key).MoveValue()) {
      return "view key " + key + " not contained";
    }
  }
  if (expected != model.end()) {
    return "view misses " + expected->first;
  }
  return "";
}

std::string VerifyAll(LSMTree& tree,
                      const std::map<std::string, std::string>& model,
                      const std::string& where) {
  if (std::string problem = VerifyReads(tree, model); !problem.empty()) {
    return problem + " " + where;
  }
  if (std::string problem = VerifyViewScan(tree, model); !problem.empty()) {
    return problem + " " + where;
  }
  return "";
}

}  // namespace

std::vector<BPlusTreeOp> GenerateLsmTreeOps(std::mt19937& rng,
                                            const LsmTreeGenConfig& config) {
  const auto op_count = static_cast<size_t>(rng() % (config.max_ops + 1));
  std::vector<BPlusTreeOp> ops;
  ops.reserve(op_count);
  for (size_t i = 0; i < op_count; ++i) {
    BPlusTreeOp op;
    const auto pick = static_cast<uint32_t>(rng() % 10);
    if (pick < 6) {
      op.kind = BPlusTreeOp::Kind::kInsert;
    } else if (pick < 8) {
      op.kind = BPlusTreeOp::Kind::kUpdate;
    } else {
      op.kind = BPlusTreeOp::Kind::kDelete;
    }
    op.key = PoolKey(rng, config.key_pool);
    op.value = op.kind == BPlusTreeOp::Kind::kDelete ? "" : RandomValue(rng);
    ops.push_back(std::move(op));
  }
  return ops;
}

std::string CheckLsmTreeEquivalence(const std::vector<BPlusTreeOp>& ops,
                                    const std::string& tag) {
  const std::filesystem::path dir = WorkDir(tag);
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  std::map<std::string, std::string> model;
  std::string problem;
  {
    auto tree = LSMTree::Create(dir).MoveValue();
    for (size_t i = 0; i < ops.size(); ++i) {
      const BPlusTreeOp& op = ops[i];
      if (op.kind == BPlusTreeOp::Kind::kDelete) {
        tree->Delete(op.key, i % 20 == 10);
        model.erase(op.key);
      } else {
        tree->Write(op.key, op.value, i % 20 == 0);
        model[op.key] = op.value;
      }
    }
    tree->Sync();
    problem = VerifyAll(*tree, model, "after Sync");
  }
  // Reopen persistence: RestoreRuns must bring back the same dataset.
  if (problem.empty()) {
    auto reopened = LSMTree::Create(dir).MoveValue();
    problem = VerifyAll(*reopened, model, "after reopen");
    if (problem.empty()) {
      reopened->MergeAll();
      problem = VerifyAll(*reopened, model, "after MergeAll");
    }
  }
  std::filesystem::remove_all(dir);
  return problem;
}

}  // namespace tinylamb
