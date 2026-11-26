import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PlateauFrames.BoundaryRuns` — `∀ x y, measure (x++y) = measure x + measure y + seam
  x y (fun a b => if a == b then (matchesFirst x.reverse:Int)*matchesFirst y else 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_BoundaryRuns

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.range (Nat.succ n + 1) = List.range (n + 1) ++ [n + 1] := by
          simp [Nat.succ_eq_add_one, List.range_succ]
        _ = (0 :: (List.range n).map (fun i => i + 1)) ++ [n + 1] := by rw [ih]
        _ = 0 :: ((List.range n).map (fun i => i + 1) ++ [n + 1]) := by simp
        _ = 0 :: (List.range (n + 1)).map (fun i => i + 1) := by simp [List.range_succ]

theorem range_flatMap_succ {α : Type} (n : Nat) (f : Nat → List α) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  rw [range_succ_cons n]
  simp [List.flatMap_map]

def prefixWindows (x : Series) := (List.range x.length).map (fun j => x.take (j + 1))

theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) = prefixWindows (a :: xs) ++ windows xs := by
  unfold windows prefixWindows
  simp only [List.length_cons]
  rw [range_flatMap_succ]
  simp [List.drop_succ_cons]

theorem matchesFirst_cons (a : Int) (xs : Series) :
    matchesFirst (a :: xs) = (xs.takeWhile (fun c => c == a)).length + 1 := by
  simp [matchesFirst]

theorem matchesFirst_eq_takeWhile_length (z : Series) (hz : z ≠ []) :
    matchesFirst z = (z.takeWhile (fun e => some e == z.head?)).length := by
  cases z with
  | nil => contradiction
  | cons c cs => simp [matchesFirst]

theorem take_all_ne (a b : Int) (bs : List Int) (j : Nat) (h : b ≠ a) :
    ((b :: bs).take (j + 1)).all (fun c => c == a) = false := by
  rw [List.take_succ_cons]
  simp [h]

theorem takeWhile_length_eq_iff_allEq (a : Int) (xs : Series) :
    (xs.takeWhile (fun e => e == a)).length = xs.length ↔ ∀ e ∈ xs, e = a := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : b = a
      · subst b
        simp [ih]
      · simp [hb]

theorem count_take_eq (a : Int) (xs : Series) :
    (List.range (xs.length + 1)).countP
        (fun j => ((a :: xs).take (j + 1)).all (fun c => c == a)) =
      (xs.takeWhile (fun c => c == a)).length + 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.length_cons]
      rw [range_succ_cons (bs.length + 1)]
      change List.countP (fun j => ((a :: b :: bs).take (j + 1)).all (fun c => c == a))
          (0 :: (List.range (bs.length + 1)).map (fun i => i + 1)) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      rw [List.countP_cons, List.countP_map]
      change List.countP (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a))
          (List.range (bs.length + 1)) +
          (if ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true then 1 else 0) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      have hp0 : ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true := by simp
      rw [ite_eq_left hp0]
      have hpred : (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a)) =
          (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) := by
        funext i
        simp only [Nat.add_assoc, List.take_succ_cons, List.all_cons]
        simp
      rw [hpred]
      by_cases hba : b = a
      · subst b
        rw [ih]
        simp
      · have hshift : (List.range (bs.length + 1)).countP
              (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            rw [take_all_ne a b bs i hba]
            simp
        rw [hshift]
        simp [hba]

theorem countP_prefixWindows (a : Int) (xs : Series) :
    (prefixWindows (a :: xs)).countP (fun w => w.all (fun c => some c == w.head?)) =
      matchesFirst (a :: xs) := by
  unfold prefixWindows
  rw [List.countP_map]
  simp only [List.length_cons]
  change List.countP
      (fun j => ((a :: xs).take (j + 1)).all (fun c => some c == ((a :: xs).take (j + 1)).head?))
      (List.range (xs.length + 1)) = matchesFirst (a :: xs)
  have hhead : ∀ j, ((a :: xs).take (j + 1)).head? = some a := by
    intro j
    simp
  simp only [hhead]
  rw [matchesFirst_cons]
  exact count_take_eq a xs

theorem measure_cons (a : Int) (xs : Series) :
    PlateauFrames.measure (a :: xs) = matchesFirst (a :: xs) + PlateauFrames.measure xs := by
  unfold PlateauFrames.measure
  rw [windows_cons, List.countP_append, countP_prefixWindows]
  omega

theorem matchesFirst_append_single (c d : Int) (rest : Series) :
    matchesFirst ((c :: rest) ++ [d]) =
      matchesFirst (c :: rest) + (if (∀ e ∈ rest, e = c) ∧ d = c then 1 else 0) := by
  rw [List.cons_append]
  rw [matchesFirst_eq_takeWhile_length (c :: (rest ++ [d])) (by simp),
      matchesFirst_eq_takeWhile_length (c :: rest) (by simp)]
  simp only [List.takeWhile_cons]
  simp
  rw [List.takeWhile_append]
  by_cases hall : ∀ e ∈ rest, e = c
  · have ht : (rest.takeWhile (fun e => e == c)).length = rest.length :=
      (takeWhile_length_eq_iff_allEq c rest).2 hall
    rw [ite_eq_left ht]
    by_cases hdc : d = c
    · rw [ite_eq_left ⟨hall, hdc⟩]
      simp [ht, hdc]
    · rw [ite_eq_right (fun h => hdc h.2)]
      simp [ht, hdc]
  · have ht : (rest.takeWhile (fun e => e == c)).length ≠ rest.length := by
      intro heq
      exact hall ((takeWhile_length_eq_iff_allEq c rest).1 heq)
    rw [ite_eq_right ht]
    simp [hall]

theorem measure_snoc (x : Series) (b : Int) :
    PlateauFrames.measure (x ++ [b]) =
      PlateauFrames.measure x + matchesFirst ((x ++ [b]).reverse) := by
  induction x with
  | nil =>
      simp [PlateauFrames.measure, windows, matchesFirst]
  | cons a xs ih =>
      rw [List.cons_append, measure_cons, ih, measure_cons]
      have hlead := matchesFirst_append_single a b xs
      have hlead' : matchesFirst (a :: (xs ++ [b])) =
          matchesFirst (a :: xs) + (if (∀ e ∈ xs, e = a) ∧ b = a then 1 else 0) := by
        simpa [List.cons_append] using hlead
      have hrev : (a :: (xs ++ [b])).reverse = (b :: xs.reverse) ++ [a] := by
        simp [List.reverse_append, List.reverse_cons, List.cons_append]
      rw [hrev]
      have hrevbase : (xs ++ [b]).reverse = b :: xs.reverse := by simp
      rw [hrevbase]
      have htrail := matchesFirst_append_single b a xs.reverse
      rw [hlead', htrail]
      by_cases hab : a = b
      · subst b
        simp [List.mem_reverse]
        omega
      · have hcond1 : ¬ ((∀ e ∈ xs, e = a) ∧ b = a) := by
          intro h
          exact hab h.2.symm
        have hcond2 : ¬ ((∀ e ∈ xs.reverse, e = b) ∧ a = b) := by
          intro h
          exact hab h.2
        rw [ite_eq_right hcond1, ite_eq_right hcond2]
        omega

theorem matchesFirst_cons_relation (b : Int) (ys : Series) :
    matchesFirst (b :: ys) = if some b == ys.head? then matchesFirst ys + 1 else 1 := by
  cases ys with
  | nil => simp [matchesFirst]
  | cons c cs =>
      by_cases hbc : b = c
      · subst c
        simp [matchesFirst_eq_takeWhile_length]
      · have hcb : c ≠ b := by intro hcbeq; exact hbc hcbeq.symm
        simp [matchesFirst_cons, hcb, hbc]

theorem lastLen_snoc (x : Series) (b : Int) :
    matchesFirst ((x ++ [b]).reverse) = (x.reverse.takeWhile (fun e => e == b)).length + 1 := by
  simp [List.reverse_append, matchesFirst_cons]

theorem lastLen_of_last_eq (x : Series) (b : Int) (hx : x ≠ []) (hlast : x.getLast? = some b) :
    matchesFirst x.reverse = (x.reverse.takeWhile (fun e => e == b)).length := by
  have hz : x.reverse ≠ [] := by simpa using hx
  rw [matchesFirst_eq_takeWhile_length x.reverse hz, List.head?_reverse, hlast]
  simp

theorem takeWhile_reverse_of_last_ne (x : Series) (b : Int) (h : x.getLast? ≠ some b) :
    (x.reverse.takeWhile (fun e => e == b)).length = 0 := by
  cases hrev : x.reverse with
  | nil =>
      have : x = [] := by simpa using congrArg List.reverse hrev
      subst x
      simp
  | cons c cs =>
      have hc : c ≠ b := by
        intro hcb
        apply h
        rw [← List.head?_reverse]
        simp [hrev, hcb]
      simp [hc]

theorem seam_snoc (x ys : Series) (b : Int) :
    seam (x ++ [b]) ys
        (fun a c => if a == c then (matchesFirst ((x ++ [b]).reverse) : Int) * matchesFirst ys else 0) =
      if some b == ys.head? then (matchesFirst ((x ++ [b]).reverse) : Int) * matchesFirst ys else 0 := by
  cases h : ys.head? with
  | none => simp [seam, List.getLast?_append, h]
  | some c =>
      by_cases hbc : b = c
      · subst c
        simp [seam, List.getLast?_append, h]
      · simp [seam, List.getLast?_append, h, hbc]

theorem seam_cons (x ys : Series) (b : Int) :
    seam x (b :: ys)
        (fun a c => if a == c then (matchesFirst x.reverse : Int) * matchesFirst (b :: ys) else 0) =
      if x.getLast? == some b then (matchesFirst x.reverse : Int) * matchesFirst (b :: ys) else 0 := by
  cases h : x.getLast? with
  | none => simp [seam, h]
  | some c =>
      by_cases hcb : c = b
      · subst b
        simp [seam, h]
      · simp [seam, h, hcb]

theorem alg1 (t W : Int) : (t + 1) + (t + 1) * W = (W + 1) + t * (W + 1) := by
  simp [Int.mul_add, Int.mul_comm, Int.add_assoc, Int.add_left_comm, Int.add_comm]

theorem sound_PlateauFrames_BoundaryRuns : PlateauFrames.BoundaryRuns.Goal := by
  intro x y
  induction y generalizing x with
  | nil =>
      simp [seam, PlateauFrames.measure, windows]
  | cons b ys ih =>
      have happ : x ++ (b :: ys) = (x ++ [b]) ++ ys := by simp [List.append_assoc]
      rw [happ, ih (x ++ [b])]
      rw [measure_snoc, measure_cons]
      rw [seam_snoc, seam_cons]
      have hM := matchesFirst_cons_relation b ys
      have hT' : matchesFirst (b :: x.reverse) =
          (x.reverse.takeWhile (fun e => e == b)).length + 1 := by
        simpa using lastLen_snoc x b
      by_cases hxnil : x = []
      · subst x
        have hb1 : matchesFirst [b] = 1 := by simp [matchesFirst]
        simp [hb1]
        rw [hM]
        by_cases hbyP : some b = ys.head?
        · simp [hbyP]
          omega
        · have hbyB : ¬ some b == ys.head? := by simpa using hbyP
          simp [hbyP, hbyB]
          omega
      · by_cases hlast : x.getLast? = some b
        · have hL := lastLen_of_last_eq x b hxnil hlast
          by_cases hby : some b == ys.head?
          · simp [hby, hlast, hL, hT', hM] at *
            have hAlg := alg1 ((x.reverse.takeWhile (fun e => e == b)).length : Int) (matchesFirst ys : Int)
            omega
          · simp [hby, hlast, hL, hT', hM] at *
            omega
        · have ht0 := takeWhile_reverse_of_last_ne x b hlast
          by_cases hby : some b == ys.head?
          · simp [hby, hlast, ht0, hT', hM] at *
            omega
          · simp [hby, hlast, ht0, hT', hM] at *
            omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_BoundaryRuns
