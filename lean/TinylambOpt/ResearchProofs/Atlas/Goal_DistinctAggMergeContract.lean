import Std
import TinylambOpt.ProposedGoals.DistinctAggMergeContract

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DistinctAggMergeContract

open TinylambOpt.ProposedGoals.DistinctAggMergeContract

private theorem dedup_mem : ∀ (x : V) (l : List V), x ∈ dedup l ↔ x ∈ l
  | x, [] => by simp [dedup]
  | x, y :: ys => by
      simp [dedup, dedup_mem]
      constructor
      · intro h
        rcases h with h | ⟨hmem, hne⟩
        · exact Or.inl h
        · by_cases hxy : x = y
          · exact Or.inl hxy
          · exact Or.inr hmem
      · intro h
        rcases h with h | h
        · exact Or.inl h
        · by_cases hxy : x = y
          · exact Or.inl hxy
          · exact Or.inr ⟨h, hxy⟩

private theorem nodup_dedup : ∀ (l : List V), (dedup l).Nodup
  | [] => by simp [dedup]
  | x :: xs => by
      rw [dedup]
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        rw [List.mem_filter] at hx
        simpa using hx.2
      · exact List.Pairwise.filter (fun y : V => y != x) (nodup_dedup xs)

private theorem mem_foldl_dedup (x : V) : ∀ (parts : List (List V)) (acc : List V),
    x ∈ parts.foldl (fun a p => a ++ dedup p) acc ↔ x ∈ acc ∨ ∃ p ∈ parts, x ∈ dedup p
  | [], acc => by simp
  | p :: ps, acc => by
      simp only [List.foldl_cons]
      rw [mem_foldl_dedup x ps (acc ++ dedup p)]
      simp [List.mem_append, or_assoc]

private theorem addOpt_comm_apply (a : Option Int) (x y : V) :
    addOpt (addOpt a x) y = addOpt (addOpt a y) x := by
  cases a <;> cases x <;> cases y <;> simp [addOpt] <;> ac_rfl

private theorem foldl_addOpt_perm {l₁ l₂ : List V} (h : l₁.Perm l₂) (a : Option Int) :
    l₁.foldl addOpt a = l₂.foldl addOpt a :=
  h.foldl_eq' (by intro x hx y hy z; exact addOpt_comm_apply z x y) a

theorem sound_DistinctAggMergeContract :
    TinylambOpt.ProposedGoals.DistinctAggMergeContract.Goal := by
  intro parts
  let rows := parts.flatten
  let merged := mergePartial parts
  have hmem : ∀ x : V, x ∈ merged ↔ x ∈ rows := by
    intro x
    unfold merged mergePartial rows
    rw [dedup_mem]
    rw [mem_foldl_dedup]
    simp [dedup_mem]
  have hperm : (dedup merged).Perm (dedup rows) := by
    apply (List.perm_ext_iff_of_nodup (nodup_dedup merged) (nodup_dedup rows)).2
    intro x
    rw [dedup_mem, dedup_mem, hmem]
  constructor
  · exact hperm.length_eq
  · exact foldl_addOpt_perm hperm none

end TinylambOpt.ResearchProofs.Atlas.Goal_DistinctAggMergeContract
