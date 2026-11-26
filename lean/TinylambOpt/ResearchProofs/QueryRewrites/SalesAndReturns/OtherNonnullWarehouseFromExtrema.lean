import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# OtherNonnullWarehouseFromExtrema

For an order with non-NULL warehouse keys, the extremes detect whether a different
warehouse exists for the chosen key. The executor can reuse per-order extrema instead of
rescanning catalog sales.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OtherNonnullWarehouseFromExtrema
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem optAny_ne_minmax (xs : List Nat) (w : Nat) :
    xs.any (fun v => v != w) = ((xs.min?).any (· < w) || (xs.max?).any (w < ·)) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hl
    rw [List.any_eq_true] at hl
    obtain ⟨v, hv, hvw⟩ := hl
    have hne : v ≠ w := by simpa using hvw
    rcases Nat.lt_or_gt_of_ne hne with hlt | hgt
    · have hmin : ∃ m, xs.min? = some m ∧ m < w := by
        cases xs with
        | nil => simp at hv
        | cons x xs' =>
            let m := (xs'.min?).elim x (min x)
            have hm : (x :: xs').min? = some m := by simp [m, List.min?_cons]
            have hle : ∀ b ∈ x :: xs', m ≤ b := (List.min?_eq_some_iff.mp hm).2
            have hlev := hle v hv
            have hmw : m < w := by omega
            exact ⟨m, hm, hmw⟩
      obtain ⟨m, hm, hmw⟩ := hmin
      simp [hm, hmw]
    · have hmax : ∃ M, xs.max? = some M ∧ w < M := by
        cases xs with
        | nil => simp at hv
        | cons x xs' =>
            let M := (xs'.max?).elim x (max x)
            have hM : (x :: xs').max? = some M := by simp [M, List.max?_cons]
            have hle : ∀ b ∈ x :: xs', b ≤ M := (List.max?_eq_some_iff.mp hM).2
            have hleM := hle v hv
            have hwM : w < M := by omega
            exact ⟨M, hM, hwM⟩
      obtain ⟨M, hM, hwM⟩ := hmax
      simp [hM, hwM]
  · intro hr
    by_cases hmin : (xs.min?).any (· < w) = true
    · rw [Option.any_eq_true] at hmin
      obtain ⟨m, hm, hmlt⟩ := hmin
      have hmem : m ∈ xs := (List.min?_eq_some_iff.mp hm).1
      have hne : m ≠ w := by
        have hlt : m < w := by simpa using hmlt
        omega
      exact (List.any_eq_true).2 ⟨m, hmem, by simpa [beq_iff_eq] using hne⟩
    · have hmax : (xs.max?).any (w < ·) = true := by
        simpa [Bool.or_eq_true, hmin] using hr
      rw [Option.any_eq_true] at hmax
      obtain ⟨M, hM, hwM⟩ := hmax
      have hmem : M ∈ xs := (List.max?_eq_some_iff.mp hM).1
      have hne : M ≠ w := by
        have hlt : w < M := by simpa using hwM
        omega
      exact (List.any_eq_true).2 ⟨M, hmem, by simpa [beq_iff_eq] using hne⟩

theorem optListAny (ws : List (Option Nat)) (w : Nat) :
    ws.any (fun v => v.any (fun x => x != w)) = (ws.filterMap id).any (fun v => v != w) := by
  induction ws with
  | nil => simp
  | cons v ws ih =>
      cases v with
      | none => simpa using ih
      | some x => simp [ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.OtherNonnullWarehouseFromExtrema.Goal := by
  intro warehouses w
  rw [optListAny]
  exact optAny_ne_minmax (warehouses.filterMap id) w

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
