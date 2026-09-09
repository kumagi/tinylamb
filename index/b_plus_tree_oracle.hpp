/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_B_PLUS_TREE_ORACLE_HPP
#define TINYLAMB_B_PLUS_TREE_ORACLE_HPP

#include <cstddef>
#include <random>
#include <string>
#include <vector>

namespace tinylamb {

// Seeded operation-stream generator + model-based oracle for the Foster
// B+Tree (index layer PBT entry point).
//
// The byte-driven `b_plus_tree_fuzzer` evolves raw key bytes through
// libFuzzer; this oracle complements it with deterministic seeded streams
// that run in the normal test build (no libFuzzer needed) and additionally
// cover ordered/range iteration, which the fuzzer never exercises. A
// `std::map` is the ground truth:
//
//   1. point reads: Read(key) matches the model after every operation;
//   2. structural soundness: SanityCheckForTest holds after every mutation;
//   3. ordered scans: full ascending/descending iteration and bounded
//      [left, right) range scans enumerate exactly the model's key range.
//
// Any non-empty return is a logic bug. ShrinkBPlusTreeOps reduces a failing
// stream while the mismatch is preserved. The generator is deterministic in
// the RNG stream: the same seed always yields the same stream, so a failure
// replays from the seed alone. The oracle builds a real PageManager/Logger
// pair in a per-seed temp directory (storage layer, below index), so it only
// depends on equal-or-lower layers.

struct BPlusTreeOp {
  enum class Kind : uint8_t { kInsert, kUpdate, kDelete };
  Kind kind{Kind::kInsert};
  std::string key;
  std::string value;  // unused for kDelete
};

struct BPlusTreeGenConfig {
  size_t max_ops = 150;
  // Distinct zero-padded keys in the pool; small enough to collide (update /
  // delete / re-insert paths) but large enough to force splits and merges.
  size_t key_pool = 40;
};

// Deterministic in the RNG stream: the same seed always yields the same
// stream. Only uses the RNG (no map iteration, no I/O).
std::vector<BPlusTreeOp> GenerateBPlusTreeOps(
    std::mt19937& rng, const BPlusTreeGenConfig& config = {});

// Applies `ops` to a fresh tree and checks point reads, structural soundness
// and ordered scans against the model. `tag` names the temp directory (must
// be unique per concurrent caller). Returns "" when all properties hold.
std::string CheckBPlusTreeEquivalence(const std::vector<BPlusTreeOp>& ops,
                                      const std::string& tag);

// Bounded delta-shrinker: drops operations one by one while the mismatch
// (as reported by CheckBPlusTreeEquivalence with the same tag) is preserved.
// Always terminates; returns the smallest mismatch-preserving stream found
// (or the input when nothing shrinks).
std::vector<BPlusTreeOp> ShrinkBPlusTreeOps(const std::vector<BPlusTreeOp>& ops,
                                            const std::string& tag);

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_ORACLE_HPP
