import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
Generated stores with disjoint byte intervals may be reordered. The proof checks
each memory address against both store ranges; disjointness ensures at most one
store can write the address, so the two orders agree byte by byte.

Proved

- `StorePairDisjointReorder` — reordering disjoint stores preserves modeled
  memory, allowing the JIT to schedule stores freely across the two ranges.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_StorePairDisjointReorder

open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

theorem sound : StorePairDisjointReorder.Goal := by
  intro m a b xs ys h
  funext i
  rcases h with hab | hba
  · by_cases h1 : a ≤ i
    · by_cases h2 : i - a < xs.length
      · have hnb : ¬ b ≤ i := by omega
        simp [store, h1, h2, hnb]
      · by_cases h3 : b ≤ i
        · by_cases h4 : i - b < ys.length
          · simp [store, h1, h2, h3, h4]
          · simp [store, h1, h2, h3, h4]
        · simp [store, h1, h2, h3]
    · have hnb : ¬ b ≤ i := by omega
      simp [store, h1, hnb]
  · by_cases h3 : b ≤ i
    · by_cases h4 : i - b < ys.length
      · have hna : ¬ a ≤ i := by omega
        simp [store, h3, h4, hna]
      · by_cases h1 : a ≤ i
        · by_cases h2 : i - a < xs.length
          · simp [store, h3, h4, h1, h2]
          · simp [store, h3, h4, h1, h2]
        · simp [store, h3, h4, h1]
    · have hna : ¬ a ≤ i := by omega
      simp [store, h3, hna]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_StorePairDisjointReorder
