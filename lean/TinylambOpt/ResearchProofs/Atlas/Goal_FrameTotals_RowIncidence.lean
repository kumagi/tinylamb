import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_RowIncidence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.FrameTotals

def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i =>
      (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

#eval! (vecs 4).all (fun x =>
  decide (measure x =
    (x.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) * ((x.length - i : Nat) : Int))).sum))

private theorem sum_map_add_int {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_map_const_int {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Int.natCast_add,
        Int.natCast_one]
      rw [ih]
      grind

private def g (x : Series) : Int :=
  ((List.range x.length).map (fun j => (x.take (j + 1)).sum)).sum

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

private theorem measure_cons (a : Int) (xs : Series) :
    FrameTotals.measure (a :: xs) = g (a :: xs) + FrameTotals.measure xs := by
  unfold FrameTotals.measure g
  rw [windows_cons]
  simp only [List.map_append, List.sum_append, List.map_map, Function.comp_def,
    List.length_cons]

private theorem g_cons (b : Int) (ys : Series) :
    g (b :: ys) = (ys.length + 1 : Nat) * b + g ys := by
  unfold g
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.map_cons, List.sum_cons, List.map_map, Function.comp_def, Nat.zero_add]
  have hmap : (List.range ys.length).map (fun j => ((b :: ys).take (j + 1 + 1)).sum) =
      (List.range ys.length).map (fun j => b + (ys.take (j + 1)).sum) := by
    apply List.map_congr_left
    intro j hj
    simp [Nat.add_assoc]
  rw [hmap, sum_map_add_int, sum_map_const_int]
  simp only [List.length_range]
  grind

private theorem g_eq_zip (y : Series) (p : Nat) :
    g y = ((y.zipIdx p).map (fun (v, i) => v * ((y.length + p - i : Nat) : Int))).sum := by
  induction y generalizing p with
  | nil => simp [g]
  | cons b ys ih =>
      rw [g_cons, ih (p + 1)]
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      congr 1
      · have hpos : ys.length + 1 + p - p = ys.length + 1 := by omega
        rw [hpos]
        grind
      · apply congrArg List.sum
        apply List.map_congr_left
        rintro ⟨v, i⟩ hi
        have heq : ys.length + (p + 1) - i = ys.length + 1 + p - i := by omega
        rw [heq]

private def c (x : Series) (p : Nat) : Int :=
  ((x.zipIdx p).map (fun (v, i) =>
    v * ((i + 1 - p : Nat) : Int) * ((x.length + p - i : Nat) : Int))).sum

private theorem zipIdx_index_ge {α : Type} (xs : List α) (p : Nat) :
    ∀ pair ∈ xs.zipIdx p, p ≤ pair.2 := by
  induction xs generalizing p with
  | nil => simp
  | cons a as ih =>
      intro pair hp
      rw [List.zipIdx_cons] at hp
      rcases List.mem_cons.mp hp with h | h
      · subst pair
        simp
      · have hi := ih (p + 1) pair h
        omega

private theorem zipIdx_index_lt {α : Type} (xs : List α) (p : Nat) :
    ∀ pair ∈ xs.zipIdx p, pair.2 < p + xs.length := by
  induction xs generalizing p with
  | nil => simp
  | cons a as ih =>
      intro pair hp
      rw [List.zipIdx_cons] at hp
      rcases List.mem_cons.mp hp with h | h
      · subst pair
        simp
      · have hi := ih (p + 1) pair h
        simp only [List.length_cons]
        omega

private theorem main_aux (x : Series) (p : Nat) :
    FrameTotals.measure x = c x p := by
  induction x generalizing p with
  | nil => simp [c, FrameTotals.measure, windows]
  | cons a xs ih =>
      rw [measure_cons, ih (p + 1), g_eq_zip (a :: xs) p]
      simp only [c, List.zipIdx_cons, List.map_cons, List.sum_cons, List.length_cons]
      have hhead : a * ((xs.length + 1 + p - p : Nat) : Int) =
          a * ((p + 1 - p : Nat) : Int) * ((xs.length + 1 + p - p : Nat) : Int) := by
        have hp1 : p + 1 - p = 1 := by omega
        have hlen : xs.length + 1 + p - p = xs.length + 1 := by omega
        rw [hp1, hlen]
        grind
      have htail :
          (List.map (fun x => x.fst * ((xs.length + 1 + p - x.snd : Nat) : Int))
              (xs.zipIdx (p + 1))).sum +
          (List.map (fun x => x.fst * ((x.snd + 1 - (p + 1) : Nat) : Int) *
              ((xs.length + (p + 1) - x.snd : Nat) : Int)) (xs.zipIdx (p + 1))).sum =
          (List.map (fun x => x.fst * ((x.snd + 1 - p : Nat) : Int) *
              ((xs.length + 1 + p - x.snd : Nat) : Int)) (xs.zipIdx (p + 1))).sum := by
        rw [← sum_map_add_int]
        apply congrArg List.sum
        apply List.map_congr_left
        rintro ⟨v, i⟩ hi
        have hige : p + 1 ≤ i := zipIdx_index_ge xs (p + 1) (v, i) hi
        have hilt : i < p + 1 + xs.length := zipIdx_index_lt xs (p + 1) (v, i) hi
        have h1 : xs.length + (p + 1) - i = xs.length + 1 + p - i := by omega
        have h2 : i + 1 - (p + 1) = i - p := by omega
        have h3 : i + 1 - p = (i - p) + 1 := by omega
        rw [h1, h2, h3]
        simp only [Int.natCast_add, Int.natCast_one,
          Int.ofNat_sub (show p ≤ i by omega),
          Int.ofNat_sub (show i ≤ xs.length + 1 + p by omega)]
        grind
      rw [hhead, ← htail]
      grind

theorem sound_FrameTotals_RowIncidence : FrameTotals.RowIncidence.Goal := by
  intro x
  simpa [c, Nat.add_zero, Nat.sub_zero] using main_aux x 0

end TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_RowIncidence
