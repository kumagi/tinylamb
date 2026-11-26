import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `ZeroFrames.Nonnegative` — `∀ x, (∀ v ∈ x, 0 ≤ v) → measure x = (windows x).countP
  (fun w => w.all (· == 0))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_Nonnegative

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames

/-! Brute-force sanity check: every series of length <= 4 over [0, 3]. -/

private def smallLists : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (smallLists n).flatMap (fun l => (List.range 4).map (fun (i : Nat) => (i : Int) :: l))

#eval ((List.range 5).flatMap smallLists).all (fun x =>
  ZeroFrames.measure x == (windows x).countP (fun w => w.all (fun v => v == 0)))

/-! ### Nonnegative sums -/

private theorem sum_nonneg {l : List Int} (h : ∀ v ∈ l, 0 ≤ v) : 0 ≤ l.sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have ha : 0 ≤ a := h a (by simp)
      have has : 0 ≤ as.sum := ih (fun v hv => h v (by simp [hv]))
      omega

private theorem sum_eq_zero_iff_all_zero {l : List Int} (h : ∀ v ∈ l, 0 ≤ v) :
    l.sum = 0 ↔ ∀ v ∈ l, v = 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons, List.mem_cons]
      have ha : 0 ≤ a := h a (by simp)
      have has_nonneg : ∀ v ∈ as, 0 ≤ v := fun v hv => h v (by simp [hv])
      have hsnonneg : 0 ≤ as.sum := sum_nonneg has_nonneg
      have ih' : as.sum = 0 ↔ ∀ v ∈ as, v = 0 := ih has_nonneg
      constructor
      · intro hsum v hv
        rcases hv with rfl | hv
        · omega
        · exact (ih'.mp (by omega)) v hv
      · intro hall
        have ha0 : a = 0 := hall a (by simp)
        have hzero : as.sum = 0 := ih'.mpr (fun v hv => hall v (by simp [hv]))
        omega

/-! ### Window decomposition -/

private theorem flatMap_range_succ {α : Type} (f : Nat → List α) (n : Nat) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = n.succ.succ by omega, List.range_succ, List.flatMap_append]
      rw [ih]
      rw [List.range_succ, List.flatMap_append]
      simp [List.append_assoc]

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) =
      (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs := by
  unfold windows
  simp only [List.length_cons]
  rw [flatMap_range_succ]
  have hsub : ∀ i : Nat, xs.length + 1 - (i + 1) = xs.length - i := by intro i; omega
  simp [List.drop_succ_cons, hsub]

/-! ### Main theorem -/

private theorem zeroCount_eq_allZeroCount (x : Series) (hx : ∀ v ∈ x, 0 ≤ v) :
    (windows x).countP (fun w => w.sum == 0) =
      (windows x).countP (fun w => w.all (fun v => v == 0)) := by
  induction x with
  | nil => simp [windows]
  | cons a xs ih =>
      rw [windows_cons, List.countP_append, List.countP_append]
      have hprefix :
          ((List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1))).countP
              (fun w => w.sum == 0) =
          ((List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1))).countP
              (fun w => w.all (fun v => v == 0)) := by
        apply List.countP_congr
        intro w hw
        have hwnonneg : ∀ v ∈ w, 0 ≤ v := by
          rcases List.mem_map.mp hw with ⟨j, hj, rfl⟩
          intro v hv
          exact hx v (List.mem_of_mem_take hv)
        constructor
        · intro hsum
          have hsum' : w.sum = 0 := beq_iff_eq.mp hsum
          have hall := (sum_eq_zero_iff_all_zero hwnonneg).mp hsum'
          exact List.all_eq_true.mpr (fun v hv => beq_iff_eq.mpr (hall v hv))
        · intro hall
          have hall' : ∀ v ∈ w, v = 0 := by
            intro v hv
            exact beq_iff_eq.mp ((List.all_eq_true.mp hall) v hv)
          have hsum' := (sum_eq_zero_iff_all_zero hwnonneg).mpr hall'
          exact beq_iff_eq.mpr hsum'
      rw [hprefix, ih (fun v hv => hx v (by simp [hv]))]

theorem sound_ZeroFrames_Nonnegative : Nonnegative.Goal := by
  intro x hx
  unfold ZeroFrames.measure
  exact_mod_cast zeroCount_eq_allZeroCount x hx

end TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_Nonnegative
