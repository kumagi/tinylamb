import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.PeakRecovery` — `∀ a b, a≠[] → b≠[] → ((agg.reference (a++b)).map (fun r =>
  r.2.2)).getD 0 ≥ peak a - b.min?.getD 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_PeakRecovery

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem mem_le_peak {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ peak l := by
  unfold peak
  cases h : l.max? with
  | none =>
      have hl : l = [] := List.max?_eq_none_iff.mp h
      subst l
      simp at hx
  | some m =>
      have hm := (List.max?_eq_some_iff.mp h).2 x hx
      simpa [h] using hm

private theorem min_getD_le {l : List Nat} {x d : Nat} (hx : x ∈ l) : l.min?.getD d ≤ x := by
  cases h : l.min? with
  | none =>
      have hl : l = [] := List.min?_eq_none_iff.mp h
      subst l
      simp at hx
  | some m =>
      have hm := (List.min?_eq_some_iff.mp h).2 x hx
      simpa [h] using hm

theorem sound_PeakRecovery : Drawdown.PeakRecovery.Goal := by
  intro a b ha hb
  cases a with
  | nil => simp at ha
  | cons x xs =>
    cases b with
    | nil => simp at hb
    | cons y ys =>
      let c : List Nat := (x :: xs) ++ (y :: ys)
      let m := peak (x :: xs)
      have hm_mem : m ∈ x :: xs := by
        have hmax : (x :: xs).max? = some m := by simp [m, peak]
        exact (List.max?_eq_some_iff.mp hmax).1
      obtain ⟨j, hj⟩ := List.mem_iff_get.mp hm_mem
      have hj_lt : j.val < (x :: xs).length := j.isLt
      have hc_len : j.val < c.length := by
        change j.val < ((x :: xs) ++ (y :: ys)).length
        simp only [List.length_append]
        omega
      have hcj : c[j.val] = m := by
        change ((x :: xs) ++ (y :: ys))[j.val] = m
        rw [List.getElem_append_left hj_lt]
        simpa using hj
      have hterm_mem :
          m - ((c.drop j.val).min?.getD m) ∈
            (c.zipIdx.map (fun (v : Nat × Nat) => v.1 - ((c.drop v.2).min?.getD v.1))) := by
        apply List.mem_map.mpr
        refine ⟨(c[j.val], j.val), ?_, ?_⟩
        · exact List.mk_mem_zipIdx_iff_getElem?.mpr (by
            rw [List.getElem?_eq_getElem hc_len])
        · simp [hcj]
      have hterm_le :
          m - ((c.drop j.val).min?.getD m) ≤
            peak (c.zipIdx.map (fun (v : Nat × Nat) => v.1 - ((c.drop v.2).min?.getD v.1))) :=
        mem_le_peak hterm_mem
      have hb_min_mem : (y :: ys).min?.getD 0 ∈ y :: ys := by
        have hmin : (y :: ys).min? = some ((y :: ys).min?.getD 0) := by simp
        exact List.min?_mem hmin
      have hb_min_in_drop : (y :: ys).min?.getD 0 ∈ c.drop j.val := by
        change (y :: ys).min?.getD 0 ∈ ((x :: xs) ++ (y :: ys)).drop j.val
        rw [List.drop_append_of_le_length (Nat.le_of_lt hj_lt)]
        exact List.mem_append_right _ hb_min_mem
      have hmin_le : (c.drop j.val).min?.getD m ≤ (y :: ys).min?.getD 0 :=
        min_getD_le hb_min_in_drop
      have hsub :
          peak (x :: xs) - (y :: ys).min?.getD 0 ≤
            m - ((c.drop j.val).min?.getD m) := by
        dsimp [m]
        exact Nat.sub_le_sub_left hmin_le m
      simpa [agg, c] using (Nat.le_trans hsub hterm_le)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_PeakRecovery
