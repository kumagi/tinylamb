import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `MirrorZones.LatencyCap` — `∀ (zone penalty : Nat), latencyCap (price zone penalty)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_LatencyCap

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

-- A list containing an element has a minimum.
private theorem min?_exists_of_mem {l : List Nat} {n : Nat} (hn : n ∈ l) :
    ∃ m, l.min? = some m := by
  cases l with
  | nil => simp at hn
  | cons x xs =>
      refine ⟨(xs.min?).elim x (min x), ?_⟩
      simp [List.min?_cons]

theorem sound_LatencyCap : MirrorZones.LatencyCap.Goal := by
  intro zone penalty xs a b n hab hcap
  let f := MirrorZones.price zone penalty
  let cp : List (List Extent) := covers (points xs)
  let La := cp.filter (fun ps => ps.all (fun e : Extent => span e.1 e.2 ≤ a))
  let Lb := cp.filter (fun ps => ps.all (fun e : Extent => span e.1 e.2 ≤ b))
  -- Raising the cap only adds admissible plans.
  have hsub : La ⊆ Lb := by
    intro ps hps
    simp only [La, Lb, List.mem_filter] at hps ⊢
    rcases hps with ⟨hps_cp, hps_a⟩
    refine ⟨hps_cp, ?_⟩
    apply List.all_eq_true.mpr
    intro e he
    have h' : span e.1 e.2 ≤ a := of_decide_eq_true ((List.all_eq_true.mp hps_a) e he)
    exact decide_eq_true (Nat.le_trans h' hab)
  have hmapsub : La.map (cost f) ⊆ Lb.map (cost f) := by
    intro z hz
    simp only [List.mem_map] at hz ⊢
    rcases hz with ⟨ps, hps, rfl⟩
    exact ⟨ps, hsub hps, rfl⟩
  have hcapA : (La.map (cost f)).min? = some n := by
    simpa [bestCapped, f, cp, La] using hcap
  have hnmemA : n ∈ La.map (cost f) := (List.min?_eq_some_iff.mp hcapA).1
  have hnmemB : n ∈ Lb.map (cost f) := hmapsub hnmemA
  obtain ⟨m, hmmin⟩ := min?_exists_of_mem hnmemB
  refine ⟨m, ?_, ?_⟩
  · -- the relaxed minimum cannot exceed the old one
    exact (List.min?_eq_some_iff.mp hmmin).2 n hnmemB
  · simpa [bestCapped, f, cp, Lb] using hmmin

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_LatencyCap
