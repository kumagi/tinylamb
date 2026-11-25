import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_SeekSurvivor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

/-! Brute-force sanity check: no counterexample for small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Nat × Nat) := #[]
  for n in List.range 16 do
    for s in List.range 20 do
      for p in List.range 5 do
        let c : Config := ⟨n, s, p, 0, 1, 0, []⟩
        let rs := rows layout c
        for k in List.range rs.length do
          let lhs := (rs[k]?).map Prod.fst
          let rhs := some (if k < c.split then k else k + width c)
          if lhs != rhs then bad := bad.push (n, s, p, k)
  pure bad.toList

private theorem filterMap_option_map_fst {α : Type} (f : Nat → Option α) (n : Nat) :
    ((List.range n).filterMap (fun i => (f i).map (fun v => (i, v)))).map Prod.fst =
      (List.range n).filter (fun i => (f i).isSome) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append, List.map_append, List.filter_append, ih]
      cases h : f n <;> simp [h]

private theorem rows_fst_survivors (c : Config) :
    (rows layout c).map Prod.fst =
      (List.range c.n).filter (fun i => decide (i < c.split ∨ c.split + width c ≤ i)) := by
  unfold rows
  rw [filterMap_option_map_fst]
  apply congrArg (fun f => (List.range c.n).filter f)
  funext i
  by_cases h : c.split ≤ i ∧ i < c.split + width c
  · have hnot : ¬ (i < c.split ∨ c.split + width c ≤ i) := by omega
    simp [DeletedSpan.layout, h, hnot]
  · have hy : i < c.split ∨ c.split + width c ≤ i := by omega
    simp [DeletedSpan.layout, h, hy]

private theorem filter_range_eq (n s t : Nat) (hst : s ≤ t) :
    (List.range n).filter (fun i => decide (i < s ∨ t ≤ i)) =
      (List.range (min n s)) ++ (List.range n).drop (min n t) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      simp only [List.filter_cons, List.filter_nil]
      by_cases hns : n < s
      · have hmins : min n s = n := by omega
        have hmins' : min (n + 1) s = n + 1 := by omega
        have hmint : min n t = n := by omega
        have hmint' : min (n + 1) t = n + 1 := by omega
        have hkeep : decide (n < s ∨ t ≤ n) = true := by simp [hns]
        simp [hkeep, hmins, hmins', hmint, hmint', List.range_succ]
        rw [show List.drop n (List.range n) = [] by simp]
        rw [show List.drop (n + 1) (List.range n ++ [n]) = [] by
          have hlen : (List.range n ++ [n]).length = n + 1 := by simp
          simp [hlen]]
        simp
      · have hsn : s ≤ n := by omega
        have hmins : min n s = s := by omega
        have hmins' : min (n + 1) s = s := by omega
        by_cases htn : t ≤ n
        · have hmint : min n t = t := by omega
          have hmint' : min (n + 1) t = t := by omega
          have hkeep : decide (n < s ∨ t ≤ n) = true := by simp [htn]
          rw [hkeep]
          rw [hmins, hmins', hmint, hmint']
          simp only [ite_true, List.append_assoc]
          congr 1
          rw [List.drop_append]
          simp only [List.length_range]
          have hsub : t - n = 0 := by omega
          rw [hsub]
          simp
        · have hnt : n < t := by omega
          have hmint : min n t = n := by omega
          have hmint' : min (n + 1) t = n + 1 := by omega
          have hkeep : decide (n < s ∨ t ≤ n) = false := by simp [hns, htn]
          simp [hkeep, hmins, hmins', hmint, hmint']
          rw [show List.drop n (List.range n) = [] by simp]
          rw [show List.drop (n + 1) (List.range n ++ [n]) = [] by
            have hlen : (List.range n ++ [n]).length = n + 1 := by simp
            simp [hlen]]

theorem sound_DeletedSpan_SeekSurvivor : DeletedSpan.SeekSurvivor.Goal := by
  intro c k hk
  let s := c.split
  let t := c.split + width c
  have hw : 0 < width c := by simp [width]
  have hst : s ≤ t := by dsimp [s, t]; omega
  have hkmapped : k < ((rows layout c).map Prod.fst).length := by simpa using hk
  have hk' : k < ((List.range c.n).filter (fun i => decide (i < c.split ∨ c.split + width c ≤ i))).length := by
    rw [rows_fst_survivors] at hkmapped
    exact hkmapped
  rw [← List.getElem?_map]
  rw [rows_fst_survivors]
  rw [filter_range_eq c.n c.split (c.split + width c) hst] at hk'
  rw [filter_range_eq c.n c.split (c.split + width c) hst]
  by_cases hns : c.n ≤ c.split
  · have hA : min c.n c.split = c.n := by omega
    have hB : min c.n (c.split + width c) = c.n := by omega
    rw [hA, hB] at hk' ⊢
    simp at hk'
    have hd : List.drop c.n (List.range c.n) = [] := by
      simp
    rw [hd]
    have hks : k < c.split := by omega
    simp [List.getElem?_range hk', hks]
  · have hsn : c.split < c.n := by omega
    have hA : min c.n c.split = c.split := by omega
    have hA' : (List.range (min c.n c.split)).length = c.split := by simp [hA]
    by_cases htn : c.split + width c ≤ c.n
    · have hB : min c.n (c.split + width c) = c.split + width c := by omega
      rw [hA, hB] at hk' ⊢
      simp only [List.length_append, List.length_range, List.length_drop] at hk'
      by_cases hks : k < c.split
      · rw [List.getElem?_append]
        simp [hks, List.getElem?_range hks]
      · have hksle : c.split ≤ k := by omega
        have hjlt : k - c.split < c.n - (c.split + width c) := by omega
        rw [List.getElem?_append]
        simp [hks]
        rw [List.getElem?_range (by omega)]
        have hsum : c.split + width c + (k - c.split) = k + width c := by omega
        rw [hsum]
    · have hnt : c.n < c.split + width c := by omega
      have hB : min c.n (c.split + width c) = c.n := by omega
      rw [hA, hB] at hk' ⊢
      simp [List.drop_length] at hk' ⊢
      simp [List.getElem?_range hk', hk']

end TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_SeekSurvivor
