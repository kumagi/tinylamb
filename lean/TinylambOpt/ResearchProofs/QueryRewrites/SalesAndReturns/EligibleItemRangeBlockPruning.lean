import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# EligibleItemRangeBlockPruning

A block whose item-key bounds exclude all eligible items cannot contribute a row to the
selected inventory result.

The scan can use stored key ranges to avoid decoding those inventory blocks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.EligibleItemRangeBlockPruning
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem min_le_of_mem : ∀ (p : List Nat) (k : Nat), k ∈ p → p.min?.any (fun m => m ≤ k) = true
  | [], k, h => by cases h
  | a :: t, k, h => by
      rw [List.min?_cons]
      cases h with
      | head =>
          cases hm : t.min? with
          | none => simp
          | some m => simp [Nat.min_le_left]
      | tail _ hk =>
          have ihk := min_le_of_mem t k hk
          cases hm : t.min? with
          | none => simp [hm] at ihk
          | some m =>
              simp [hm] at ihk ⊢
              exact Nat.le_trans (Nat.min_le_right a m) ihk

theorem mem_le_max_of_mem : ∀ (p : List Nat) (k : Nat), k ∈ p → p.max?.any (fun m => k ≤ m) = true
  | [], k, h => by cases h
  | a :: t, k, h => by
      rw [List.max?_cons]
      cases h with
      | head =>
          cases hm : t.max? with
          | none => simp
          | some m => simp [Nat.le_max_left]
      | tail _ hk =>
          have ihk := mem_le_max_of_mem t k hk
          cases hm : t.max? with
          | none => simp [hm] at ihk
          | some m =>
              simp [hm] at ihk ⊢
              exact Nat.le_trans ihk (Nat.le_max_right a m)

theorem possible_of_mem (cheap p : List Nat) (k : Nat) (hk : k ∈ cheap) (hkp : k ∈ p) :
    cheap.any (fun k => p.min?.any (fun m => m ≤ k) && p.max?.any (fun m => k ≤ m)) = true := by
  apply List.any_eq_true.mpr
  refine ⟨k, hk, ?_⟩
  simp [min_le_of_mem p k hkp, mem_le_max_of_mem p k hkp]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.EligibleItemRangeBlockPruning.Goal := by
  intro blocks cheap
  dsimp only
  induction blocks with
  | nil => rfl
  | cons p blocks ih =>
      by_cases hp : cheap.any (fun k => p.min?.any (fun m => m ≤ k) && p.max?.any (fun m => k ≤ m)) = true
      · simp [hp, ih]
      · have hno : p.filter cheap.contains = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro k hk
          rw [List.mem_filter] at hk
          have hkcheap : k ∈ cheap := by simpa using hk.2
          have hposs := possible_of_mem cheap p k hkcheap hk.1
          rw [hposs] at hp
          contradiction
        simp [hp, hno, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.EligibleItemRangeBlockPruning
