import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Small-input truth check (run during development):
private def sampleGen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := sampleGen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])
private def sampleProbe (n : Nat) : Bool :=
  let ls := sampleGen n
  ls.all (fun init => ls.all (fun a => ls.all (fun b =>
    ([[a], [a, b]].all (fun ps =>
      (ps.map agg.reference).scanl agg.join (agg.reference init) ==
        (List.range (ps.length + 1)).map (fun i => agg.reference (init ++ (ps.take i).flatten)))))))
#eval sampleProbe 2   -- true
#eval sampleProbe 3   -- true
-/

private def zr (xs : List Nat) : Nat := (xs.takeWhile (fun x => x == 0)).length
private def sr (xs : List Nat) : Nat := zr xs.reverse

private theorem peak_cons (a : Nat) (l : List Nat) : peak (a :: l) = max a (peak l) := by
  cases hm : l.max? with
  | none => simp [peak, List.max?_cons, hm]
  | some m => simp [peak, List.max?_cons, hm]

private theorem lz_cons_ne (x : Nat) (xs : List Nat) (hx : (x == 0) = false) :
    longestZero (x :: xs) = longestZero xs := by
  unfold longestZero
  rw [show (x :: xs).length + 1 = (xs.length + 1) + 1 by simp [Nat.add_assoc]]
  rw [List.range_succ_eq_map]
  simp only [List.map_cons, List.map_map]
  rw [peak_cons]
  simp only [List.drop_zero]
  have hdrop : ∀ i : Nat, (x :: xs).drop (Nat.succ i) = xs.drop i := by simp
  simp only [Function.comp_def]
  simp only [hdrop]
  simp [hx]

private theorem lz_cons_zero (x : Nat) (xs : List Nat) (hx : (x == 0) = true) :
    longestZero (x :: xs) = max (1 + zr xs) (longestZero xs) := by
  unfold longestZero
  rw [show (x :: xs).length + 1 = (xs.length + 1) + 1 by simp [Nat.add_assoc]]
  rw [List.range_succ_eq_map]
  simp only [List.map_cons, List.map_map]
  rw [peak_cons]
  simp only [List.drop_zero]
  have hdrop : ∀ i : Nat, (x :: xs).drop (Nat.succ i) = xs.drop i := by simp
  simp only [Function.comp_def]
  simp only [hdrop]
  have hhead : ((x :: xs).takeWhile (fun y => y == 0)).length = 1 + zr xs := by
    simp [zr, hx]
    omega
  rw [hhead]

private theorem mem_takeWhile_true {p : Nat → Bool} {l : List Nat} {x : Nat}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
      by_cases ha : p a = true
      · simp [ha] at h
        rcases h with rfl | h
        · exact ha
        · exact ih h
      · have ha' : p a = false := by cases hp : p a <;> simp_all
        simp [ha'] at h

private theorem takeWhile_all_true {p : Nat → Bool} (l : List Nat)
    (h : ∀ x ∈ l, p x = true) : l.takeWhile p = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : p a = true := h a (by simp)
      simp [ha, ih (fun x hx => h x (by simp [hx]))]

private theorem zr_all {l : List Nat} : zr l = l.length ↔ ∀ x ∈ l, (x == 0) = true := by
  constructor
  · intro h x hx
    have hsub : (l.takeWhile (fun y => y == 0)).Sublist l := List.takeWhile_sublist _
    have heq : l.takeWhile (fun y => y == 0) = l := hsub.eq_of_length (by simpa [zr] using h)
    have hx' : x ∈ l.takeWhile (fun y => y == 0) := by rw [heq]; exact hx
    exact mem_takeWhile_true (p := fun y : Nat => y == 0) hx'
  · intro h
    have heq : l.takeWhile (fun y => y == 0) = l := takeWhile_all_true l h
    simp [zr, heq]

private theorem sr_all {l : List Nat} : sr l = l.length ↔ zr l = l.length := by
  unfold sr
  constructor
  · intro h
    have h' : zr l.reverse = l.reverse.length := by simpa using h
    have hall : ∀ x ∈ l.reverse, (x == 0) = true := zr_all.mp h'
    exact zr_all.mpr (fun x hx => hall x (List.mem_reverse.mpr hx))
  · intro h
    have hall : ∀ x ∈ l, (x == 0) = true := zr_all.mp h
    have h' : zr l.reverse = l.reverse.length := zr_all.mpr (fun x hx => hall x (List.mem_reverse.mp hx))
    simpa using h'

private theorem zr_append (a b : List Nat) :
    zr (a ++ b) = if zr a = a.length then a.length + zr b else zr a := by
  unfold zr
  rw [List.takeWhile_append]
  by_cases h : (a.takeWhile (fun x => x == 0)).length = a.length <;> simp [h]

private theorem sr_cons (x : Nat) (xs : List Nat) :
    sr (x :: xs) = if sr xs = xs.length then xs.length + (if x == 0 then 1 else 0) else sr xs := by
  unfold sr
  rw [List.reverse_cons]
  rw [zr_append]
  have hsingle : zr [x] = if x == 0 then 1 else 0 := by
    by_cases hx : (x == 0) = true
    · have hx0 : x = 0 := by simpa using hx
      simp [zr, hx0]
    · have hxne : x ≠ 0 := by intro h0; apply hx; simp [h0]
      simp [zr, hxne]
  rw [hsingle]
  simp [List.length_reverse]

private theorem zr_le_length (l : List Nat) : zr l ≤ l.length := by
  simpa [zr] using (List.Sublist.length_le (List.takeWhile_sublist (p := fun y : Nat => y == 0) (l := l)))

private theorem sr_le_length (l : List Nat) : sr l ≤ l.length := by
  unfold sr
  rw [← List.length_reverse]
  exact zr_le_length l.reverse

private theorem lz_le_length : ∀ l : List Nat, longestZero l ≤ l.length
  | [] => by simp [longestZero, peak]
  | x :: xs => by
      by_cases hx : (x == 0) = true
      · rw [lz_cons_zero x xs hx]
        simp only [List.length_cons]
        apply Nat.max_le.mpr
        constructor
        · have hz := zr_le_length xs
          omega
        · exact Nat.le_trans (lz_le_length xs) (Nat.le_succ _)
      · have hxf : (x == 0) = false := by cases h : x == 0 <;> simp_all
        rw [lz_cons_ne x xs hxf]
        simp only [List.length_cons]
        exact Nat.le_trans (lz_le_length xs) (Nat.le_succ _)

private theorem zr_le_lz : ∀ l : List Nat, zr l ≤ longestZero l
  | [] => by simp [zr, longestZero, peak]
  | x :: xs => by
      by_cases hx : (x == 0) = true
      · rw [lz_cons_zero x xs hx]
        have hhead : zr (x :: xs) = 1 + zr xs := by
          simp [zr, hx]
          omega
        rw [hhead]
        exact Nat.le_max_left _ _
      · have hxf : (x == 0) = false := by cases h : x == 0 <;> simp_all
        rw [lz_cons_ne x xs hxf]
        simp [zr, hxf]

private theorem sr_le_lz : ∀ l : List Nat, sr l ≤ longestZero l
  | [] => by simp [sr, zr, longestZero, peak]
  | x :: xs => by
      by_cases hx : (x == 0) = true
      · rw [lz_cons_zero x xs hx]
        by_cases hs : sr xs = xs.length
        · have hz : zr xs = xs.length := sr_all.mp hs
          have hsx : sr (x :: xs) = xs.length + 1 := by
            rw [sr_cons]
            simp [hs, hx]
          rw [hsx, hz]
          have hle : xs.length + 1 = 1 + xs.length := by omega
          rw [hle]
          exact Nat.le_max_left _ _
        · have hsx : sr (x :: xs) = sr xs := by
            rw [sr_cons]
            simp [hs]
          rw [hsx]
          exact Nat.le_trans (sr_le_lz xs) (Nat.le_max_right _ _)
      · have hxf : (x == 0) = false := by cases h : x == 0 <;> simp_all
        rw [lz_cons_ne x xs hxf]
        have hsx : sr (x :: xs) = sr xs := by
          rw [sr_cons]
          by_cases hs : sr xs = xs.length
          · simp [hs, hxf]
          · simp [hs]
        rw [hsx]
        exact sr_le_lz xs

private theorem prefix_component (a b : List Nat) :
    ((a ++ b).takeWhile (fun x => x == 0)).length =
      if ((a.takeWhile (fun x => x == 0)).length == a.length) then
        a.length + (b.takeWhile (fun x => x == 0)).length
      else (a.takeWhile (fun x => x == 0)).length := by
  by_cases h : (a.takeWhile (fun x => x == 0)).length = a.length
  · have hb : ((a.takeWhile (fun x => x == 0)).length == a.length) = true := by simp [h]
    simp [List.takeWhile_append, h]
  · have hb : ((a.takeWhile (fun x => x == 0)).length == a.length) = false := by
      cases hEq : ((a.takeWhile (fun x => x == 0)).length == a.length) <;> simp_all
    simp [List.takeWhile_append, h, hb]

private theorem suffix_component (a b : List Nat) :
    ((a ++ b).reverse.takeWhile (fun x => x == 0)).length =
      if ((b.reverse.takeWhile (fun x => x == 0)).length == b.length) then
        b.length + (a.reverse.takeWhile (fun x => x == 0)).length
      else (b.reverse.takeWhile (fun x => x == 0)).length := by
  rw [List.reverse_append]
  simpa [List.length_reverse] using prefix_component b.reverse a.reverse

private theorem bridge (a b : List Nat) :
    longestZero (a ++ b) =
      max (max (longestZero a) (longestZero b)) (sr a + zr b) := by
  induction a with
  | nil =>
      have hz : zr ([] : List Nat) = 0 := by simp [zr]
      have hs : sr ([] : List Nat) = 0 := by simp [sr, zr]
      have hl : longestZero ([] : List Nat) = 0 := by simp [longestZero, peak]
      simp [hs, hl]
      exact (Nat.max_eq_left (zr_le_lz b)).symm
  | cons x xs ih =>
      by_cases hx : (x == 0) = true
      · rw [show (x :: xs) ++ b = x :: (xs ++ b) by rfl]
        rw [lz_cons_zero x (xs ++ b) hx]
        rw [zr_append]
        rw [ih]
        rw [lz_cons_zero x xs hx]
        rw [sr_cons]
        by_cases hsr : sr xs = xs.length
        · have hz : zr xs = xs.length := sr_all.mp hsr
          have hl : longestZero xs = xs.length := by
            have hle := lz_le_length xs
            have hge : xs.length ≤ longestZero xs := by
              rw [← hz]
              exact zr_le_lz xs
            omega
          simp [hsr, hx, hz, hl, Nat.add_assoc]
          omega
        · have hzr : zr xs ≠ xs.length := by
            intro h
            exact hsr (sr_all.mpr h)
          simp [hsr, hzr, Nat.max_assoc, Nat.max_left_comm, Nat.max_comm]
      · have hxf : (x == 0) = false := by cases h : x == 0 <;> simp_all
        rw [show (x :: xs) ++ b = x :: (xs ++ b) by rfl]
        rw [lz_cons_ne x (xs ++ b) hxf]
        rw [ih]
        rw [lz_cons_ne x xs hxf]
        rw [sr_cons]
        by_cases hsr : sr xs = xs.length
        · simp [hsr, hxf]
        · simp [hsr]

private theorem stitch_agg : stitch agg := by
  intro a b
  change agg.join (agg.reference a) (agg.reference b) = agg.reference (a ++ b)
  simp [agg, List.length_append, prefix_component, bridge, sr, zr]

section Generic

variable {σ : Type} (f : Fold σ)

private theorem prefixCheckpoint_of_stitch (h : stitch f) : prefixCheckpoint f := by
  intro init ps
  have hlen : (ps.map f.reference).length = ps.length := by simp
  apply List.ext_getElem
  · simp [List.length_scanl, hlen]
  · intro i hi1 hi2
    simp only [List.length_scanl, List.length_map, hlen, List.length_range] at hi1 hi2
    simp only [List.getElem_scanl, List.getElem_map, List.getElem_range]
    have gen : ∀ (l : List (List Nat)) (acc : List Nat),
        List.foldl f.join (f.reference acc) (l.map f.reference) = f.reference (acc ++ l.flatten) := by
      intro l
      induction l with
      | nil => intro acc; simp
      | cons p ps ih =>
        intro acc
        simp only [List.map_cons, List.foldl, List.flatten_cons]
        rw [h acc p, ih (acc ++ p)]
        simp [List.append_assoc]
    have htake : (List.take i (ps.map f.reference)) = (ps.take i).map f.reference := by
      simp [List.map_take]
    rw [htake, gen]

end Generic

theorem sound_PrefixCheckpoint : LongestHole.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_PrefixCheckpoint
