import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ConstantPage

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem prefs_repeated (n : Nat) (a : Int) :
    prefs (repeated n a) = (List.range n).map (fun i => ((i+1:Nat):Int) * a) := by
  induction n with
  | zero => simp [repeated, prefs]
  | succ n ih =>
      change prefs (a :: repeated n a) = _
      rw [prefs_cons, ih]
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map]
      congr 1
      · simp
      · apply List.map_congr_left
        intro i hi
        grind

private theorem sum_map_sq_mul_range (n : Nat) (a : Int) :
    ((List.range n).map (fun i => (((i+1:Nat):Int)*a)^2)).sum =
      a^2 * ((List.range n).map (fun i => ((i+1:Nat):Int)^2)).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.map_append, List.map_cons, List.sum_append]
      rw [ih]
      grind

private theorem sum_sq (n : Nat) :
    6 * ((List.range n).map (fun i => (((i+1:Nat):Int)^2))).sum =
      (n:Int) * ((n:Int)+1) * (2*(n:Int)+1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.map_append, List.map_cons, List.sum_append]
      grind

theorem sound_PrefixEnergy_ConstantPage : PrefixEnergy.ConstantPage.Goal := by
  intro n a
  change 6 * ((prefs (repeated n a)).map (fun v => v^2)).sum =
    a^2 * (n:Int) * ((n:Int)+1) * (2*(n:Int)+1)
  rw [prefs_repeated, List.map_map]
  simp only [Function.comp_def]
  rw [sum_map_sq_mul_range]
  have hs := sum_sq n
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PrefixEnergy_ConstantPage
