/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LSM_TREE_ORACLE_HPP
#define TINYLAMB_LSM_TREE_ORACLE_HPP

#include <random>
#include <string>
#include <vector>

#include "index/b_plus_tree_oracle.hpp"

namespace tinylamb {

// Seeded operation-stream generator + model-based oracle for the WiscKey LSM
// tree (index layer PBT entry point).
//
// The byte-driven `lsm_tree_fuzzer` evolves raw key bytes through libFuzzer
// but never closes and reopens the tree; this oracle complements it with
// deterministic seeded streams that run in the normal test build and
// additionally cover the persistence path the fuzzer misses:
//
//   1. point reads: Read/Contains match the model after every operation;
//   2. ordered view scan: GetView().Begin() enumerates exactly the model;
//   3. reopen persistence: after Sync + destroy + reopen (RestoreRuns), the
//      model still matches -- quarantined runs aside, clean data restores;
//   4. merge persistence: after MergeAll the model still matches.
//
// Any non-empty return is a logic bug. The generator is deterministic in the
// RNG stream: the same seed always yields the same stream, so a failure
// replays from the seed alone. Tombstone operations reuse BPlusTreeOp with
// kInsert/kUpdate mapped to Write and kDelete mapped to Delete; the oracle
// builds a real LSMTree in a per-seed temp directory, so it only depends on
// equal-or-lower layers.

struct LsmTreeGenConfig {
  size_t max_ops = 150;
  size_t key_pool = 40;
};

// Deterministic in the RNG stream: the same seed always yields the same
// stream. Only uses the RNG (no map iteration, no I/O).
std::vector<BPlusTreeOp> GenerateLsmTreeOps(
    std::mt19937& rng, const LsmTreeGenConfig& config = {});

// Applies `ops` to a fresh tree and checks reads, view scans, reopen
// persistence and merge persistence against the model. `tag` names the temp
// directory (must be unique per concurrent caller). Returns "" when all
// properties hold.
std::string CheckLsmTreeEquivalence(const std::vector<BPlusTreeOp>& ops,
                                    const std::string& tag);

}  // namespace tinylamb

#endif  // TINYLAMB_LSM_TREE_ORACLE_HPP
