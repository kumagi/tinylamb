import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_Multiplicities

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PrefixExposure

/-! Brute-force sanity check: every series of length ≤ 4 over [-3, 3]. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 4).filter (fun x =>
    measure x != (x.zipIdx.map (fun (v, i) => v * ((x.length - i : Nat) : Int))).sum) |>.length)

private theorem range_map_succ {α : Type} (n : Nat) (f : Nat → α) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = (n + 1).succ by omega]
      rw [List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun s => a + s) := by
  unfold prefs
  simp only [List.length_cons]
  rw [range_map_succ]
  simp [List.take_succ_cons, List.map_map]

private theorem sum_map_add_const (l : List Int) (a : Int) :
    (l.map (fun s => a + s)).sum = (l.length : Int) * a + l.sum := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      simp only [List.map_cons, List.sum_cons, ih, List.length_cons]
      simp [Int.add_mul]
      omega

private theorem zipIdx_succ {α : Type} (l : List α) (n : Nat) :
    l.zipIdx (n + 1) = (l.zipIdx n).map (fun p => (p.1, p.2 + 1)) := by
  induction l generalizing n with
  | nil => simp
  | cons a l ih => simp [List.zipIdx, ih, Nat.add_assoc]

private theorem weighted_cons (a : Int) (xs : Series) :
    ((a :: xs).zipIdx.map (fun (v, i) => v * (((a :: xs).length - i : Nat) : Int))).sum =
      a * ((xs.length + 1 : Nat) : Int) +
        (xs.zipIdx.map (fun (v, i) => v * ((xs.length - i : Nat) : Int))).sum := by
  have hw : ∀ i : Nat, xs.length + 1 - (i + 1) = xs.length - i := by omega
  simp only [List.zipIdx, List.length_cons]
  rw [zipIdx_succ]
  simp only [List.map_cons, List.sum_cons]
  have hmap :
      (List.map (fun x : Int × Nat => x.1 * (((xs.length + 1 - x.2 : Nat) : Int)))
          (List.map (fun p : Int × Nat => (p.1, p.2 + 1)) (xs.zipIdx))).sum =
        (xs.zipIdx.map (fun (v, i) => v * ((xs.length - i : Nat) : Int))).sum := by
    rw [List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    simp [hw]
  simp only [Nat.sub_zero]
  rw [hmap]

private theorem mul_len_succ (a : Int) (n : Nat) :
    a * ((n + 1 : Nat) : Int) = (n : Int) * a + a := by
  have h : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by simp
  rw [h, Int.mul_add, Int.mul_one, Int.mul_comm a (n : Int)]

theorem sound_PrefixExposure_Multiplicities : PrefixExposure.Multiplicities.Goal := by
  intro x
  induction x with
  | nil => simp [PrefixExposure.measure, prefs]
  | cons a xs ih =>
      unfold PrefixExposure.measure at ih ⊢
      rw [prefs_cons, List.sum_cons, sum_map_add_const]
      rw [weighted_cons]
      rw [← ih]
      rw [mul_len_succ]
      simp [prefs]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixExposure_Multiplicities
