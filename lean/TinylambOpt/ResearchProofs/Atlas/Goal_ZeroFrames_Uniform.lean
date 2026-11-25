import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_Uniform

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) =
      (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs := by
  change (List.range (xs.length + 1)).flatMap (fun i =>
      (List.range (xs.length + 1 - i)).map (fun j => ((a :: xs).drop i).take (j + 1))) =
    (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs
  conv =>
    lhs
    rw [List.range_succ_eq_map]
  simp only [List.flatMap_cons, Nat.sub_zero, List.drop_zero]
  congr 1
  rw [List.flatMap_map]
  unfold windows
  congr 1
  funext i
  have hlen : xs.length + 1 - (i + 1) = xs.length - i := by omega
  rw [hlen]
  rw [List.drop_succ_cons]

private theorem sum_replicate_int (n : Nat) (a : Int) :
    (List.replicate n a).sum = (n : Int) * a := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.sum_cons, ih, Int.natCast_add, Int.natCast_one]
      grind

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem prefix_eq_replicate (n j : Nat) (a : Int) (hj : j ∈ List.range (n + 1)) :
    (a :: repeated n a).take (j + 1) = repeated (j + 1) a := by
  simp only [repeated]
  rw [← List.replicate_succ, List.take_replicate]
  have hjlt : j < n + 1 := List.mem_range.mp hj
  have hle : j + 1 ≤ n + 1 := by omega
  rw [Nat.min_eq_left hle]

theorem sound_ZeroFrames_Uniform : ZeroFrames.Uniform.Goal := by
  intro n
  induction n with
  | zero =>
      intro a
      simp [ZeroFrames.measure, repeated, windows, choose2]
  | succ n ih =>
      intro a
      unfold ZeroFrames.measure
      rw [show repeated (Nat.succ n) a = a :: repeated n a by rfl]
      rw [show Nat.succ n + 1 = n + 2 by omega]
      rw [windows_cons]
      have hlenrep : (repeated n a).length = n := by simp [repeated]
      rw [hlenrep]
      simp only [List.countP_append, Int.natCast_add]
      let pref : List (List Int) :=
        (List.range (n + 1)).map (fun j => (a :: repeated n a).take (j + 1))
      change (pref.countP (fun w => w.sum == 0) : Int) +
          ((windows (repeated n a)).countP (fun w => w.sum == 0) : Int) =
        (if a == 0 then (choose2 (n + 2) : Int) else 0)
      have htail :
          ((windows (repeated n a)).countP (fun w => w.sum == 0) : Int) =
            if a == 0 then (choose2 (n + 1) : Int) else 0 := by
        exact ih a
      have hpref_nat : pref.countP (fun w => w.sum == 0) =
          if a == 0 then n + 1 else 0 := by
        by_cases ha : a = 0
        · have hall : ∀ w ∈ pref, (fun w : List Int => w.sum == 0) w = true := by
            intro w hw
            simp only [pref, List.mem_map] at hw
            rcases hw with ⟨j, hj, rfl⟩
            rw [prefix_eq_replicate n j a hj]
            simp [repeated, ha]
          have hlen : pref.length = n + 1 := by simp [pref]
          rw [List.countP_eq_length.mpr hall, hlen]
          simp [ha]
        · have hall : ∀ w ∈ pref, ¬ (fun w : List Int => w.sum == 0) w = true := by
            intro w hw
            simp only [pref, List.mem_map] at hw
            rcases hw with ⟨j, hj, rfl⟩
            rw [prefix_eq_replicate n j a hj]
            intro hzero
            have hjpos : ((j + 1 : Nat) : Int) ≠ 0 := by
              exact_mod_cast (Nat.succ_ne_zero j)
            simp only [repeated] at hzero
            rw [sum_replicate_int] at hzero
            have hz0 : ((j + 1 : Nat) : Int) * a = 0 := by simpa using hzero
            rw [Int.mul_eq_zero] at hz0
            rcases hz0 with hz | hz
            · exact hjpos hz
            · exact ha hz
          rw [List.countP_eq_zero.mpr hall]
          simp [ha]
      by_cases ha : a = 0
      · have hp : pref.countP (fun w => w.sum == 0) = n + 1 := by
          simpa [ha] using hpref_nat
        have hpI : (pref.countP (fun w => w.sum == 0) : Int) = (n + 1 : Nat) := by
          exact_mod_cast hp
        rw [hpI, htail]
        simp [ha]
        have hc := choose2_step n
        have hci : (choose2 (n + 2) : Int) = (choose2 (n + 1) : Int) + (n + 1 : Nat) := by
          exact_mod_cast hc
        rw [hci]
        simp only [Int.natCast_add, Int.natCast_one]
        ac_rfl
      · have hp : pref.countP (fun w => w.sum == 0) = 0 := by
          simpa [ha] using hpref_nat
        rw [hp]
        simp [ha] at htail ⊢
        exact htail

end TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_Uniform
