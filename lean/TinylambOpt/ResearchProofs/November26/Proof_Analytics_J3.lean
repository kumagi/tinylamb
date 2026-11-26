import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_J3
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

def code (year : Nat → Option Nat) (k : Nat) : Nat := ((year k).map (fun y => y - 1969)).getD 0

theorem code_bound (year : Nat → Option Nat)
    (h : ∀ (k : Nat) (y : Nat), year k = some y → 1970 ≤ y ∧ y ≤ 2010) (k : Nat) :
    code year k ≤ 41 := by
  cases hy : year k with
  | none => simp [code, hy]
  | some y =>
      have hb := h k y hy
      simp [code, hy]
      omega

theorem code_eq_zero_iff (year : Nat → Option Nat)
    (h : ∀ (k : Nat) (y : Nat), year k = some y → 1970 ≤ y ∧ y ≤ 2010) (k : Nat) :
    code year k = 0 ↔ year k = none := by
  cases hy : year k with
  | none => simp [code, hy]
  | some y =>
      have hb := h k y hy
      simp [code, hy]
      omega

theorem code_add (year : Nat → Option Nat)
    (h : ∀ (k : Nat) (y : Nat), year k = some y → 1970 ≤ y ∧ y ≤ 2010) (k y : Nat)
    (hy : year k = some y) : code year k + 1969 = y := by
  have hb := h k y hy
  simp [code, hy]
  omega

theorem hits_total (movieIDs : List Nat) (year : Nat → Option Nat)
    (h : ∀ (k : Nat) (y : Nat), year k = some y → 1970 ≤ y ∧ y ≤ 2010) :
    (movieIDs.map (code year)).length - (movieIDs.map (code year)).count 0 =
      (movieIDs.filterMap year).length := by
  induction movieIDs with
  | nil => simp
  | cons k t ih =>
      have ih' : t.length - (t.map (code year)).count 0 = (t.filterMap year).length := by
        simpa using ih
      cases hyk : year k with
      | none =>
          have hc0 : code year k = 0 := by simp [code, hyk]
          simp [hc0, hyk]
          exact ih'
      | some y =>
          have hcne : code year k ≠ 0 := by
            intro hz
            have := (code_eq_zero_iff year h k).1 hz
            simp [hyk] at this
          simp [hcne, hyk]
          have hle : (t.map (code year)).count 0 ≤ t.length := by
            simpa using (List.count_le_length (l := t.map (code year)) (a := 0))
          omega

theorem weighted_sum_total (movieIDs : List Nat) (year : Nat → Option Nat)
    (h : ∀ (k : Nat) (y : Nat), year k = some y → 1970 ≤ y ∧ y ≤ 2010) :
    (movieIDs.map (code year)).sum + 1969 * ((movieIDs.map (code year)).length - (movieIDs.map (code year)).count 0) =
      (movieIDs.filterMap year).sum := by
  induction movieIDs with
  | nil => simp
  | cons k t ih =>
      have ih' : (t.map (code year)).sum + 1969 * (t.length - (t.map (code year)).count 0) =
          (t.filterMap year).sum := by
        simpa using ih
      cases hyk : year k with
      | none =>
          have hc0 : code year k = 0 := by simp [code, hyk]
          simp [hc0, hyk]
          exact ih'
      | some y =>
          have hcne : code year k ≠ 0 := by
            intro hz
            have := (code_eq_zero_iff year h k).1 hz
            simp [hyk] at this
          have hcadd : code year k + 1969 = y := code_add year h k y hyk
          have hlen :
              t.length + 1 - (code year k :: t.map (code year)).count 0 =
                (t.length - (t.map (code year)).count 0) + 1 := by
            simp [List.count_cons, hcne]
            have hle : (t.map (code year)).count 0 ≤ t.length := by
              simpa using (List.count_le_length (l := t.map (code year)) (a := 0))
            omega
          simp [hyk]
          rw [hlen]
          omega

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.J3.Goal := by
  intro movieIDs year hbound
  constructor
  · intro c hc
    rcases List.mem_map.mp hc with ⟨k, hk, rfl⟩
    exact code_bound year hbound k
  · exact Prod.ext (hits_total movieIDs year hbound) (weighted_sum_total movieIDs year hbound)
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_J3
