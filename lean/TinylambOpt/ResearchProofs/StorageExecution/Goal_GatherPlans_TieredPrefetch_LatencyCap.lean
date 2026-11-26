import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_LatencyCap

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TieredPrefetch

-- truth probe on small inputs
#eval (List.range 4).all (fun limit =>
  (List.range 4).all (fun spill =>
    ([[], [0], [0,4,4,20], [3,1,7,2,9], [5,2,5,2,5]] : List (List Nat)).all (fun xs =>
      (List.range 8).all (fun a =>
        (List.range 8).all (fun b =>
          if a ≤ b then
            match bestCapped (price limit spill) xs a with
            | none => true
            | some n => match bestCapped (price limit spill) xs b with
              | none => false
              | some m => m ≤ n
          else true)))))

/-- Relaxing the extent cap only enlarges the set of admissible plans, so the
    minimum cost cannot increase. This holds for every price function. -/
private theorem capped_relax (f : Price) (xs : List Nat) {a b : Nat} (hab : a ≤ b) {n : Nat}
    (h : bestCapped f xs a = some n) : ∃ m ≤ n, bestCapped f xs b = some m := by
  let base : List (List Extent) := covers (points xs)
  let capA : List Extent → Bool := fun ps => ps.all (fun (u, v) => span u v ≤ a)
  let capB : List Extent → Bool := fun ps => ps.all (fun (u, v) => span u v ≤ b)
  have hmA : ((base.filter capA).map (cost f)).min? = some n := by
    simpa [bestCapped, base, capA] using h
  rcases (List.min?_eq_some_iff.mp hmA) with ⟨hnmem, hnmin⟩
  rcases (List.mem_map.mp hnmem) with ⟨ps, hpsA, hcost⟩
  have hpsBase : ps ∈ base := (List.mem_filter.mp hpsA).1
  have hpsAc : capA ps = true := (List.mem_filter.mp hpsA).2
  have hpsAc' : ∀ e ∈ ps, span e.1 e.2 ≤ a := by
    have hall : ∀ e ∈ ps, (fun (u, v) => span u v ≤ a) e = true := by
      exact List.all_eq_true.mp (by simpa [capA] using hpsAc)
    intro e he
    simpa using hall e he
  have hpsBc : capB ps = true := by
    change ps.all (fun (u, v) => span u v ≤ b) = true
    rw [List.all_eq_true]
    intro e he
    simpa using (Nat.le_trans (hpsAc' e he) hab)
  have hpsBmem : ps ∈ base.filter capB := List.mem_filter.mpr ⟨hpsBase, hpsBc⟩
  have hnmemB : n ∈ (base.filter capB).map (cost f) :=
    List.mem_map.mpr ⟨ps, hpsBmem, hcost⟩
  cases hminB : ((base.filter capB).map (cost f)).min? with
  | none =>
      have hempty : (base.filter capB).map (cost f) = [] := List.min?_eq_none_iff.mp hminB
      exact False.elim ((List.ne_nil_of_mem hnmemB) hempty)
  | some m =>
      refine ⟨m, ?_, ?_⟩
      · exact (List.min?_eq_some_iff.mp hminB).2 n hnmemB
      · simpa [bestCapped, base, capB] using hminB

theorem sound_LatencyCap : TieredPrefetch.LatencyCap.Goal := by
  intro limit spill xs a b n hab hcap
  exact capped_relax (price limit spill) xs hab hcap

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_LatencyCap
