import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_PrefixCollision

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_EarliestPrune

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: every series of length ≤ 5 over [-3, 3]. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 5).filter (fun x =>
    (ZeroFrames.measure x == 0) != decide ((0 :: prefs x).Nodup)) |>.length)

/-! ### Counting equal prefix pairs -/

private def tieCount (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem tieCount_cons (a : Int) (x : Series) :
    tieCount (a :: x) = x.countP (fun b => a == b) + tieCount x := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]

private theorem tieCount_nodup_zero_iff (l : List Int) : tieCount l = 0 ↔ l.Nodup := by
  induction l with
  | nil => simp [tieCount, pairs]
  | cons a as ih =>
      rw [tieCount_cons]
      have hcount : as.countP (fun b : Int => a == b) = 0 ↔ a ∉ as := by
        rw [List.countP_eq_zero]
        constructor
        · intro h ha
          have hb := h a ha
          simp at hb
        · intro h b hb
          intro heq
          have hEq : a = b := beq_iff_eq.mp heq
          exact h (by simpa [hEq] using hb)
      constructor
      · intro h
        rw [List.nodup_cons]
        constructor
        · exact hcount.mp (by omega)
        · exact ih.mp (by omega)
      · intro h
        rw [List.nodup_cons] at h
        have h1 : as.countP (fun b : Int => a == b) = 0 := hcount.mpr h.1
        have h2 : tieCount as = 0 := ih.mpr h.2
        omega

/-! ### Main theorem -/

theorem sound_ZeroFrames_EarliestPrune : ZeroFrames.EarliestPrune.Goal := by
  intro x
  have hcoll :=
    TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_PrefixCollision.sound_ZeroFrames_PrefixCollision x
  unfold TiePairs.measure at hcoll
  change ZeroFrames.measure x = (tieCount (0 :: prefs x) : Int) at hcoll
  constructor
  · intro hzero
    have ht : tieCount (0 :: prefs x) = 0 := by
      rw [hzero] at hcoll
      exact_mod_cast hcoll.symm
    exact (tieCount_nodup_zero_iff (0 :: prefs x)).mp ht
  · intro hnodup
    have ht : tieCount (0 :: prefs x) = 0 :=
      (tieCount_nodup_zero_iff (0 :: prefs x)).mpr hnodup
    rw [show (tieCount (0 :: prefs x) : Int) = 0 by exact_mod_cast ht] at hcoll
    exact hcoll

end TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_EarliestPrune
