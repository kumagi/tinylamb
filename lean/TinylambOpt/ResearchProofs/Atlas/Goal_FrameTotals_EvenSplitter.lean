import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `FrameTotals.EvenSplitter` — `∀ x, measure x.reverse = measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_EvenSplitter

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem zipIdx_append {α : Type} (xs ys : List α) (n : Nat) :
    (xs ++ ys).zipIdx n = xs.zipIdx n ++ ys.zipIdx (n + xs.length) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.cons_append, List.zipIdx_cons, List.length_cons, ih]
      congr 1
      ac_rfl

private theorem zipIdx_reverse (x : Series) :
    x.reverse.zipIdx = (x.zipIdx.reverse).map (fun (v, i) => (v, x.length - 1 - i)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [List.reverse_cons, zipIdx_append, ih]
      simp only [List.length_reverse, List.zipIdx_cons, List.map_cons, List.map_nil,
        List.reverse_cons, List.reverse_nil, List.nil_append, List.map_append,
        List.map_reverse, List.map_map, Function.comp_def, List.length_cons]
      · simp [Nat.zero_add]
        have hz := zipIdx_add xs 0 1
        simp only [Nat.zero_add] at hz
        rw [hz, List.map_map]
        apply List.map_congr_left
        intro p hp
        cases p with
        | mk v i =>
            simp only [Function.comp_def]
            have h2 : xs.length - (i + 1) = xs.length - 1 - i := by omega
            rw [h2]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_add_const (xs : List Int) (f : Int → Int) (c : Int) :
    (xs.map (fun x => f x + c)).sum = (xs.map f).sum + (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem sum_map_const {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem prefs_nil : prefs ([] : Series) = [] := rfl

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem prefix_exposure_cons (a : Int) (xs : Series) :
    PrefixExposure.measure (a :: xs) =
      ((xs.length + 1 : Nat) : Int) * a + PrefixExposure.measure xs := by
  unfold PrefixExposure.measure
  rw [prefs_cons]
  simp only [List.sum_cons]
  rw [show ((prefs xs).map (fun v => a + v)).sum =
      (prefs xs).sum + (prefs xs).length * a by
    simpa [Int.add_comm] using sum_map_add_const (prefs xs) (fun v => v) a]
  have hlen : (prefs xs).length = xs.length := by simp [prefs]
  rw [hlen]
  simp
  grind

private theorem prefixExposure_multiplicities (x : Series) :
    PrefixExposure.measure x =
      (x.zipIdx.map (fun (v, i) => v * ((x.length - i : Nat) : Int))).sum := by
  induction x with
  | nil => simp [PrefixExposure.measure, prefs_nil]
  | cons a xs ih =>
      rw [prefix_exposure_cons]
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      have hz := zipIdx_add xs 0 1
      simp only [Nat.zero_add] at hz
      rw [hz, List.map_map]
      simp only [Function.comp_def]
      have hweights :
          (xs.zipIdx.map (fun p => p.1 * ((xs.length + 1 - (p.2 + 1) : Nat) : Int))) =
            (xs.zipIdx.map (fun p => p.1 * ((xs.length - p.2 : Nat) : Int))) := by
        apply List.map_congr_left
        intro p hp
        have hi := (List.mem_zipIdx hp).2.1
        have hnat : xs.length + 1 - (p.2 + 1) = xs.length - p.2 := by omega
        rw [hnat]
      rw [hweights]
      grind

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem frameTotals_cons (a : Int) (xs : Series) :
    FrameTotals.measure (a :: xs) =
      FrameTotals.measure xs + PrefixExposure.measure xs +
        ((xs.length + 1 : Nat) : Int) * a := by
  unfold FrameTotals.measure
  rw [windows_cons_prefix]
  simp only [List.map_append, List.sum_append, List.map_cons, List.sum_cons,
    List.map_map, Function.comp_def]
  rw [sum_map_add, sum_map_const]
  simp only [List.length_map, List.length_range, List.sum_nil, Int.add_zero]
  unfold PrefixExposure.measure prefs
  simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
    Int.mul_one, Int.one_mul]
  ac_rfl

private theorem rowIncidence (x : Series) :
    FrameTotals.measure x =
      (x.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
        ((x.length - i : Nat) : Int))).sum := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [frameTotals_cons, prefixExposure_multiplicities, ih]
      simp only [List.zipIdx_cons, List.map_cons, List.length_cons, List.sum_cons,
        Nat.zero_add, Nat.add_sub_cancel]
      have hsum :
          (xs.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
            ((xs.length - i : Nat) : Int))).sum +
          (xs.zipIdx.map (fun (v, i) => v * ((xs.length - i : Nat) : Int))).sum =
          ((xs.zipIdx (0 + 1)).map (fun (v, i) =>
            v * ((i + 1 : Nat) : Int) * ((xs.length + 1 - i : Nat) : Int))).sum := by
        rw [zipIdx_add, List.map_map, ← sum_map_add]
        congr 1
        apply List.map_congr_left
        intro p hp
        simp only [Function.comp_def]
        change p.1 * ((p.2 + 1 : Nat) : Int) * ((xs.length - p.2 : Nat) : Int) +
            p.1 * ((xs.length - p.2 : Nat) : Int) =
          p.1 * (((p.2 + 1) + 1 : Nat) : Int) *
            ((xs.length + 1 - (p.2 + 1) : Nat) : Int)
        have hidx : xs.length + 1 - (p.2 + 1) = xs.length - p.2 := by omega
        rw [hidx]
        push_cast
        simp only [Int.mul_add, Int.add_mul, Int.mul_one]
      rw [hsum]
      simp only [Int.natCast_add, Int.natCast_one, Int.mul_one]
      ac_rfl

theorem sound_FrameTotals_EvenSplitter : FrameTotals.EvenSplitter.Goal := by
  intro x
  rw [rowIncidence, rowIncidence, zipIdx_reverse,
    List.map_map, List.map_reverse, List.sum_reverse]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  cases p with
  | mk v i =>
      have hi : i < x.length := List.snd_lt_of_mem_zipIdx hp
      have h1 : x.length - 1 - i + 1 = x.length - i := by omega
      have h2 : x.length - (x.length - 1 - i) = i + 1 := by omega
      simp only [Function.comp_def, List.length_reverse, h1, h2, Int.mul_comm]
      ac_rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_EvenSplitter
