import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PlateauFrames.BoundaryRuns` — `∀ x y, measure (x++y) = measure x + measure y + seam
  x y (fun a b => if a == b then (matchesFirst x.reverse:Int)*matchesFirst y else 0)`
- `PlateauFrames.ExpandRuns` — `∀ x n, measure (x.flatMap (repeated n)) = size x *
  choose2 (n+1) + (n:Int)^2*(measure x-size x)`
- `PlateauFrames.InjectiveCodes` — `∀ x (f : Int → Int), (∀ a ∈ x, ∀ b ∈ x, f a = f b →
  a = b) → measure (x.map f) = measure x`
- `PlateauFrames.TieFence` — `∀ x, measure x ≤ size x + TiePairs.measure x`
- `PlateauFrames.Uniform` — `∀ n a, measure (repeated n a) = (choose2 (n+1):Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list helpers -/

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

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega
  · have he : n = 2 * (n / 2) + 1 := by omega
    rw [he]
    have hsub : 2 * (n / 2) + 1 - 1 = 2 * (n / 2) := by omega
    rw [hsub]
    have hd : 2 ∣ (2 * (n / 2) + 1) * (2 * (n / 2)) := by
      refine ⟨(2 * (n / 2) + 1) * (n / 2), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_twice n
  have h2 := choose2_twice (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

/-! ### Window enumeration -/

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

/-! ### Leading-run counts -/

private def leadCount (a : Int) (x : Series) : Nat := (x.takeWhile (fun b => b == a)).length

private theorem count_range_takeWhile (a : Int) (xs : Series) :
    (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) =
      leadCount a xs := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · rw [List.length_cons, List.range_succ_eq_map, List.countP_cons, countP_map_comp]
        simp [List.take_succ_cons, hba, ih]
      · simp [hba, List.length_cons, List.range_succ_eq_map, countP_map_comp,
          List.take_succ_cons]

private theorem measure_cons (a : Int) (xs : Series) :
    measure (a :: xs) = (1 : Int) + (leadCount a xs : Int) + measure xs := by
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure
  rw [windows_cons_prefix]
  rw [List.countP_append, List.countP_append]
  have hsingle :
      ([[a]] : List (List Int)).countP (fun w => w.all (fun c => some c == w.head?)) = 1 := by
    simp
  rw [hsingle]
  have hmid :
      ((List.range xs.length).map (fun i => a :: xs.take (i + 1))).countP
          (fun w => w.all (fun c => some c == w.head?)) =
        (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) := by
    rw [countP_map_comp]
    congr 1
    funext i
    simp
  rw [hmid, count_range_takeWhile]
  simp

private theorem measure_nil :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure ([] : Series) = 0 := rfl

private theorem takeWhile_some_eq (a : Int) (xs : Series) :
    xs.takeWhile (fun b => some b == some a) = xs.takeWhile (fun b => b == a) := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      by_cases hba : b == a
      · have hopt : some b == some a := by simp [hba]
        simp [hba, hopt, ih]
      · have hopt : (some b == some a) = false := by simp [hba]
        simp [hba, hopt]

private theorem matchesFirst_cons (a : Int) (xs : Series) :
    matchesFirst (a :: xs) = leadCount a xs + 1 := by
  unfold matchesFirst leadCount
  simp only [List.head?_cons]
  rw [takeWhile_some_eq]
  simp

private theorem matchesFirst_nil : matchesFirst ([] : Series) = 0 := rfl

private theorem leadCount_le_length (a : Int) (x : Series) : leadCount a x ≤ x.length := by
  unfold leadCount
  induction x with
  | nil => simp
  | cons b bs ih => by_cases h : b == a <;> simp [h, ih]

private theorem leadCount_append (a : Int) (xs y : Series) :
    (xs ++ y).takeWhile (fun b => b == a) =
      (xs.takeWhile (fun b => b == a)) ++
        (if (xs.takeWhile (fun b => b == a)).length = xs.length then
          y.takeWhile (fun b => b == a)
        else []) := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · simp only [List.cons_append, List.takeWhile_cons, hba, Bool.true_and,
          List.length_cons, Nat.succ.injEq]
        rw [ih]
        split <;> simp_all
      · simp only [List.cons_append, List.takeWhile_cons, hba, List.length_cons]
        have hne : bs.length + 1 ≠ (bs.takeWhile (fun b => b == a)).length := by
          have hle := leadCount_le_length a bs
          unfold leadCount at hle
          omega
        simp [hne]

private theorem leadCount_append_count (a : Int) (xs y : Series) :
    leadCount a (xs ++ y) = leadCount a xs +
      (if leadCount a xs = xs.length then leadCount a y else 0) := by
  unfold leadCount
  rw [leadCount_append]
  by_cases h : (xs.takeWhile (fun b => b == a)).length = xs.length <;> simp [h]

private theorem leadCount_of_head_eq (a : Int) (x : Series) (h : x.head? = some a) :
    leadCount a x = matchesFirst x := by
  unfold leadCount matchesFirst
  simp [h]

private theorem leadCount_of_head_ne (a : Int) (x : Series) (h : x.head? ≠ some a) :
    leadCount a x = 0 := by
  cases x with
  | nil => rfl
  | cons b bs =>
      simp only [List.head?_cons] at h
      have hba : b ≠ a := by
        intro heq
        apply h
        simp [heq]
      unfold leadCount
      simp [hba]

private theorem repeated_length (n : Nat) (a : Int) : (repeated n a).length = n := by
  unfold repeated
  simp

private theorem leadCount_repeated (n : Nat) (a : Int) : leadCount a (repeated n a) = n := by
  unfold leadCount repeated
  simp

private theorem repeated_reverse (n : Nat) (a : Int) : (repeated n a).reverse = repeated n a := by
  unfold repeated
  simp

/-! ### Uniform -/

theorem sound_Uniform :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.Uniform.Goal := by
  intro n a
  induction n with
  | zero => simp [repeated, measure_nil, choose2]
  | succ n ih =>
      change measure (a :: repeated n a) = (choose2 (n + 2) : Int)
      rw [measure_cons, leadCount_repeated, ih, choose2_succ (n + 1)]
      push_cast
      omega

/-! ### InjectiveCodes -/

private theorem leadCount_map_eq (f : Int → Int) (a : Int) (xs : Series)
    (hpair : ∀ c ∈ xs, ∀ d ∈ xs, f c = f d → c = d)
    (hhead : ∀ c ∈ xs, f c = f a → c = a) :
    leadCount (f a) (xs.map f) = leadCount a xs := by
  induction xs with
  | nil => rfl
  | cons c cs ih =>
      have hpair' : ∀ d ∈ cs, ∀ e ∈ cs, f d = f e → d = e := by
        intro d hd e he heq
        exact hpair d (by simp [hd]) e (by simp [he]) heq
      have hhead' : ∀ d ∈ cs, f d = f a → d = a := by
        intro d hd heq
        exact hhead d (by simp [hd]) heq
      by_cases hca : c == a
      · have hcaeq : c = a := by simpa using hca
        subst c
        simpa [leadCount] using ih hpair' hhead'
      · have hfc : f c ≠ f a := by
          intro heq
          have hcaeq : c = a := hhead c (by simp) heq
          exact hca (by simp [hcaeq])
        simp [leadCount, hca, hfc]

theorem sound_InjectiveCodes :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.InjectiveCodes.Goal := by
  intro x f hf
  induction x with
  | nil => simp [measure_nil]
  | cons a xs ih =>
      have hf' : ∀ b ∈ xs, ∀ c ∈ xs, f b = f c → b = c := by
        intro b hb c hc heq
        exact hf b (by simp [hb]) c (by simp [hc]) heq
      have hhead : ∀ b ∈ xs, f b = f a → b = a := by
        intro b hb heq
        exact hf b (by simp [hb]) a (by simp) heq
      rw [List.map_cons, measure_cons, ih hf', measure_cons,
        leadCount_map_eq f a xs hf' hhead]



/-! ### TieFence -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using (eq_comm : a = b ↔ b = a)

private def tieCount (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem tieCount_cons (a : Int) (xs : Series) :
    tieCount (a :: xs) = tieCount xs + xs.countP (fun b => b == a) := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  have hbeq : (fun x : Int => a == x) = (fun x : Int => x == a) := by
    funext x
    exact beq_comm_int a x
  rw [hbeq]
  omega

private theorem leadCount_le_count (a : Int) (xs : Series) :
    leadCount a xs ≤ xs.countP (fun b => b == a) := by
  induction xs with
  | nil => simp [leadCount]
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · simp [hba]
        omega
      · simp [hba]

theorem sound_TieFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.TieFence.Goal := by
  intro x
  induction x with
  | nil => simp [measure_nil, size, TiePairs.measure]
  | cons a xs ih =>
      rw [measure_cons]
      dsimp [size, TiePairs.measure] at ih ⊢
      change measure xs ≤ (xs.length : Int) + (tieCount xs : Int) at ih
      change (1 : Int) + (leadCount a xs : Int) + measure xs ≤
        (xs.length : Int) + 1 + (tieCount (a :: xs) : Int)
      rw [tieCount_cons]
      have hlead : (leadCount a xs : Int) ≤ (xs.countP (fun b => b == a) : Int) := by
        exact_mod_cast leadCount_le_count a xs
      push_cast
      omega

/-! ### BoundaryRuns -/

private theorem leadCount_eq_length_iff (a : Int) (xs : Series) :
    leadCount a xs = xs.length ↔ xs.all (fun b => b == a) = true := by
  induction xs with
  | nil => simp [leadCount]
  | cons b bs ih =>
      by_cases hba : b == a
      · unfold leadCount at ih ⊢
        simp [hba, ih]
      · have hlead0 : leadCount a (b :: bs) = 0 := by
          unfold leadCount
          simp [hba]
        have hallfalse : (b :: bs).all (fun c => c == a) = false := by simp [hba]
        rw [hlead0, hallfalse]
        simp

private theorem leadCount_append_matches (a : Int) (xs y : Series) :
    (leadCount a (xs ++ y) : Int) = (leadCount a xs : Int) +
      (if xs.all (fun b => b == a) = true ∧ y.head? = some a then (matchesFirst y : Int) else 0) := by
  have hnat := leadCount_append_count a xs y
  by_cases hall : xs.all (fun b => b == a) = true
  · have hlen : leadCount a xs = xs.length := (leadCount_eq_length_iff a xs).2 hall
    rw [if_pos hlen] at hnat
    push_cast at hnat
    rw [hnat]
    push_cast
    by_cases hy : y.head? = some a
    · rw [if_pos ⟨hall, hy⟩, leadCount_of_head_eq a y hy]
    · rw [if_neg (fun h : xs.all (fun b => b == a) = true ∧ y.head? = some a => hy h.2)]
      rw [leadCount_of_head_ne a y hy]
      simp
  · have hlen : leadCount a xs ≠ xs.length := by
      intro h
      exact hall ((leadCount_eq_length_iff a xs).1 h)
    rw [if_neg hlen] at hnat
    push_cast at hnat
    rw [hnat]
    simp [hall]

private theorem matchesFirst_append_single (ys : Series) (a : Int) :
    matchesFirst (ys ++ [a]) = matchesFirst ys +
      (if ys.all (fun b => b == a) = true then 1 else 0) := by
  induction ys with
  | nil =>
      simp [matchesFirst_cons, matchesFirst_nil, leadCount]
  | cons b bs ih =>
      by_cases hba : b == a
      · have hbeq : b = a := by simpa using hba
        subst b
        rw [List.cons_append, matchesFirst_cons, matchesFirst_cons, leadCount_append_count]
        have hlen : leadCount a bs = bs.length ↔ bs.all (fun d => d == a) = true :=
          leadCount_eq_length_iff a bs
        have hlaa : leadCount a [a] = 1 := by simp [leadCount]
        have hallc : (a :: bs).all (fun d => d == a) = bs.all (fun d => d == a) := by simp
        rw [hlaa, hallc]
        by_cases hl : leadCount a bs = bs.length
        · rw [if_pos hl, if_pos (hlen.1 hl)]
        · rw [if_neg hl, if_neg (fun h => hl (hlen.2 h))]
      · have hbaFalse : (b == a) = false := by
          cases h : b == a <;> simp_all
        have hab : (a == b) = false := by
          rw [beq_comm_int]
          exact hbaFalse
        have hallfalse : (b :: bs).all (fun c => c == a) = false := by simp [hbaFalse]
        have hlead : leadCount b (bs ++ [a]) = leadCount b bs := by
          rw [leadCount_append_count]
          have hlen : leadCount b bs = bs.length ↔ bs.all (fun d => d == b) = true :=
            leadCount_eq_length_iff b bs
          by_cases hl : leadCount b bs = bs.length
          · rw [if_pos hl]
            have hzero : leadCount b [a] = 0 := by
              unfold leadCount
              simp [hab]
            rw [hzero]
            rw [Nat.add_zero]
          · rw [if_neg hl]
            rw [Nat.add_zero]
        rw [List.cons_append, matchesFirst_cons, matchesFirst_cons, hlead, hallfalse]
        simp

private def tailRunLen (x : Series) : Nat := matchesFirst x.reverse

private theorem tailRunLen_cons (a : Int) (xs : Series) :
    tailRunLen (a :: xs) =
      tailRunLen xs + (if (a :: xs).all (fun b => b == a) = true then 1 else 0) := by
  unfold tailRunLen
  rw [List.reverse_cons, matchesFirst_append_single]
  congr 1
  rw [List.all_reverse]
  simp [List.all_cons]

private theorem matchesFirst_singleton (a : Int) : matchesFirst [a] = 1 := by
  rw [matchesFirst_cons]
  simp [leadCount]

private theorem getLast?_eq_of_all : ∀ (x : Series) (a : Int), x ≠ [] →
    x.all (fun b => b == a) = true → x.getLast? = some a
  | [], a, hne, hall => by contradiction
  | b :: bs, a, hne, hall => by
      simp only [List.all_cons] at hall
      by_cases hba : b == a
      · have hbsall : bs.all (fun d => d == a) = true := by
          simpa [hba] using hall
        rw [List.getLast?_cons]
        cases hgl : bs.getLast? with
        | none =>
            have hbeq : b = a := by simpa using hba
            simp [hgl, hbeq]
        | some d =>
            have hnebs : bs ≠ [] := by
              intro h
              subst bs
              simp at hgl
            have hres := getLast?_eq_of_all bs a hnebs hbsall
            simp [hgl] at hres
            simp [hgl, hres]
      · simp [hba] at hall

private theorem cast_ite_nat_int (h : Prop) [Decidable h] (n : Nat) :
    ((if h then n else 0 : Nat) : Int) = if h then (n : Int) else 0 := by
  by_cases hh : h <;> simp [hh]

private def cross (x y : Series) : Nat :=
  match x with
  | [] => 0
  | a :: xs =>
      cross xs y +
        (if xs.all (fun b => b == a) = true ∧ y.head? = some a then matchesFirst y else 0)
termination_by x.length

private theorem cross_cons (a : Int) (xs y : Series) :
    cross (a :: xs) y = cross xs y +
      (if xs.all (fun b => b == a) = true ∧ y.head? = some a then matchesFirst y else 0) := by
  rw [cross]

private theorem measure_append_cross (x y : Series) :
    measure (x ++ y) = measure x + measure y + (cross x y : Int) := by
  induction x with
  | nil => simp [cross, measure_nil]
  | cons a xs ih =>
      rw [List.cons_append, measure_cons, measure_cons, ih, cross_cons]
      push_cast
      rw [leadCount_append_matches, cast_ite_nat_int]
      omega

private theorem getLast?_eq_some_getD (x : Series) (a : Int) (hne : x ≠ []) :
    x.getLast? = some (x.getLast?.getD a) := by
  cases x with
  | nil => contradiction
  | cons a xs => simp [List.getLast?_cons]

private theorem seam_plateau (x y : Series) :
    seam x y (fun a b => if a == b then (matchesFirst x.reverse : Int) * matchesFirst y else 0) =
      if x.getLast? == y.head? then (matchesFirst x.reverse : Int) * matchesFirst y else 0 := by
  cases x with
  | nil => cases y <;> simp [seam, matchesFirst_nil]
  | cons a xs =>
      cases y with
      | nil => simp [seam, matchesFirst_nil]
      | cons b ys =>
          unfold seam
          simp only [List.getLast?_cons, List.head?_cons, Option.bind_some,
            Option.map_some, Option.getD_some]
          have hopt : (some (xs.getLast?.getD a) == some b) =
              (xs.getLast?.getD a == b) := by simp
          rw [hopt]

private theorem cross_eq_seam (x y : Series) :
    (cross x y : Int) =
      if x.getLast? == y.head? then (tailRunLen x : Int) * (matchesFirst y : Int) else 0 := by
  induction x with
  | nil => simp [cross, tailRunLen, matchesFirst_nil]
  | cons a xs ih =>
      cases xs with
      | nil =>
          rw [cross_cons]
          push_cast
          simp [cross, tailRunLen, matchesFirst_singleton]
          by_cases hy : y.head? = some a
          · have hc : (some a == y.head?) = true := by
              rw [beq_iff_eq]
              simp [hy]
            simp [hy, hc]
          · have hc : (some a == y.head?) = false := by
              cases h : some a == y.head? <;> simp_all [beq_iff_eq]
            have hne : some a ≠ y.head? := by
              intro h
              exact hy h.symm
            simp [hy, hc, hne]
      | cons b bs =>
          rw [cross_cons]
          push_cast
          rw [ih, cast_ite_nat_int]
          rw [show (a :: b :: bs).getLast? = (b :: bs).getLast? from List.getLast?_cons_cons]
          have htail : tailRunLen (a :: b :: bs) =
              tailRunLen (b :: bs) + (if (a :: b :: bs).all (fun c => c == a) = true then 1 else 0) :=
            tailRunLen_cons a (b :: bs)
          by_cases hall : (b :: bs).all (fun c => c == a) = true
          · have hxall : (a :: b :: bs).all (fun c => c == a) = true := by simp [hall]
            have hlast : (b :: bs).getLast? = some a :=
              getLast?_eq_of_all (b :: bs) a (by simp) hall
            by_cases hy : y.head? = some a
            · have hbeqhead : (some a == y.head?) = true := by
                rw [beq_iff_eq]
                simp [hy]
              rw [htail]
              simp [hall, hxall, hy, hlast, hbeqhead]
              push_cast
              rw [Int.add_mul, Int.one_mul]
            · have hbeqhead : (some a == y.head?) = false := by
                cases h : some a == y.head? <;> simp_all [beq_iff_eq]
              rw [htail]
              simp [hall, hxall, hy, hlast, hbeqhead]
          · have hxallfalse : (a :: b :: bs).all (fun c => c == a) = false := by simp [hall]
            rw [htail]
            simp [hall, hxallfalse]

theorem sound_BoundaryRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.BoundaryRuns.Goal := by
  intro x y
  rw [measure_append_cross, cross_eq_seam, seam_plateau]
  unfold tailRunLen
  omega

/-! ### ExpandRuns -/

private theorem matchesFirst_append_repeated (n : Nat) (a : Int) (ys : Series) :
    matchesFirst (repeated n a ++ ys) =
      if n = 0 then matchesFirst ys
      else n + (if ys.head? = some a then matchesFirst ys else 0) := by
  induction n with
  | zero => simp [repeated]
  | succ n ih =>
      simp only [Nat.succ_ne_zero, if_false]
      change matchesFirst (a :: (repeated n a ++ ys)) =
        n + 1 + (if ys.head? = some a then matchesFirst ys else 0)
      rw [matchesFirst_cons, leadCount_append_count, leadCount_repeated, repeated_length]
      rw [if_pos rfl]
      by_cases hy : ys.head? = some a
      · rw [if_pos hy, leadCount_of_head_eq a ys hy]
        omega
      · rw [if_neg hy, leadCount_of_head_ne a ys hy]

private theorem getLast?_repeated_pos : ∀ (n : Nat) (a : Int), 0 < n →
    (repeated n a).getLast? = some a
  | 0, a, h => by omega
  | n + 1, a, h => by
      cases n with
      | zero => simp [repeated, List.getLast?_cons]
      | succ n =>
          rw [repeated, List.replicate_succ, List.getLast?_cons]
          have ih := getLast?_repeated_pos (n + 1) a (by omega)
          change (List.replicate (n + 1) a).getLast? = some a at ih
          rw [ih]
          simp

private theorem getLast?_repeated (n : Nat) (a : Int) :
    (repeated n a).getLast? = if n = 0 then none else some a := by
  by_cases hn : n = 0
  · subst n
    simp [repeated]
  · rw [if_neg hn]
    exact getLast?_repeated_pos n a (by omega)

private theorem tailRunLen_repeated (n : Nat) (a : Int) : tailRunLen (repeated n a) = n := by
  unfold tailRunLen
  rw [repeated_reverse]
  have h := matchesFirst_append_repeated n a ([] : Series)
  by_cases hn : n = 0
  · simp [hn, matchesFirst_nil] at h ⊢
    exact h
  · simp [hn, matchesFirst_nil] at h ⊢
    exact h

private theorem head?_flatMap_repeated (n : Nat) (xs : Series) :
    (xs.flatMap (repeated n)).head? = if n = 0 then none else xs.head? := by
  cases xs with
  | nil => simp
  | cons a as =>
      by_cases hn : n = 0
      · simp [hn, repeated]
      · have hhead : (repeated n a).head? = some a := by
          induction n with
          | zero => omega
          | succ n ih => simp [repeated, List.replicate_succ]
        simp [hn, hhead]

private theorem matchesFirst_cons_head (b : Int) (bs : Series) :
    matchesFirst (b :: bs) = 1 + (if bs.head? = some b then matchesFirst bs else 0) := by
  cases bs with
  | nil => simp [matchesFirst_cons, leadCount]
  | cons c cs =>
      by_cases hcb : c == b
      · have hcbeq : c = b := beq_iff_eq.mp hcb
        have hh : (c :: cs).head? = some b := by simp [hcbeq]
        subst c
        rw [if_pos hh]
        simp [matchesFirst_cons, leadCount]
        omega
      · have hcbFalse : c ≠ b := by
          intro h
          apply hcb
          simp [h]
        have hhead : (c :: cs).head? ≠ some b := by
          intro h
          simp only [List.head?_cons, Option.some.injEq] at h
          exact hcbFalse h
        have hlead : leadCount b (c :: cs) = 0 := by
          unfold leadCount
          have hbeqFalse : (c == b) = false := by
            cases h : c == b <;> simp_all
          simp [hbeqFalse]
        rw [if_neg hhead, matchesFirst_cons, hlead]

private theorem matchesFirst_flatMap_repeated (xs : Series) (n : Nat) :
    matchesFirst (xs.flatMap (repeated n)) = n * matchesFirst xs := by
  induction xs with
  | nil => simp [matchesFirst_nil]
  | cons b bs ih =>
      rw [List.flatMap_cons, matchesFirst_append_repeated]
      by_cases hn : n = 0
      · subst n
        simpa using ih
      · rw [if_neg hn]
        by_cases hbs : bs = []
        · subst bs
          simp [hn, matchesFirst_cons, leadCount]
        · rw [head?_flatMap_repeated]
          rw [show (if n = 0 then none else bs.head?) = bs.head? by simp [hn]]
          by_cases hcb : bs.head? = some b
          · rw [if_pos hcb, ih, matchesFirst_cons_head, if_pos hcb]
            rw [Nat.mul_add, Nat.mul_one]
          · rw [if_neg hcb, matchesFirst_cons_head, if_neg hcb]
            simp

private theorem int_sq (a : Int) : a ^ 2 = a * a := by grind

private theorem algebra_expand_true (T N M L c : Int) :
    T + ((N+1)*T + c*(1+L+M-(N+1))) + c*(L+1) =
      (N+2)*T + c*(3+2*L+M-(N+2)) := by
  grind

private theorem algebra_expand_false (T N M L c : Int) :
    T + ((N+1)*T + c*(1+L+M-(N+1))) = (N+2)*T + c*(2+L+M-(N+2)) := by
  grind

theorem sound_ExpandRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.ExpandRuns.Goal := by
  intro x n
  induction x with
  | nil => simp [size, measure_nil]
  | cons a xs ih =>
      rw [List.flatMap_cons, measure_append_cross, ih]
      rw [sound_Uniform, cross_eq_seam]
      rw [getLast?_repeated, head?_flatMap_repeated]
      rw [tailRunLen_repeated]
      by_cases hn : n = 0
      · subst n
        dsimp [size]
        simp [choose2]
      · cases xs with
        | nil =>
            simp [hn, size, measure_cons, measure_nil, leadCount]
        | cons b bs =>
            rw [show (if n = 0 then none else some a) = some a by simp [hn]]
            rw [show (if n = 0 then none else (b :: bs).head?) = some b by simp [hn]]
            by_cases hba : b == a
            · have hba' : a = b := by
                have : b = a := beq_iff_eq.mp hba
                exact this.symm
              have hcond : (some a == some b) = true := by simp [hba']
              rw [if_pos hcond, matchesFirst_flatMap_repeated]
              have hhead : (b :: bs).head? = some a := by simp [hba']
              rw [← leadCount_of_head_eq a (b :: bs) hhead]
              have hlead2 : leadCount a (b :: bs) = leadCount b bs + 1 := by
                unfold leadCount
                rw [hba']
                simp
              have hm2 : measure (b :: bs) = 1 + (leadCount b bs : Int) + measure bs := by
                rw [measure_cons]
              have hm1 : measure (a :: b :: bs) = 3 + 2 * (leadCount b bs : Int) + measure bs := by
                rw [measure_cons, hlead2, hm2]
                omega
              rw [hlead2, hm1, hm2]
              dsimp [size]
              push_cast
              rw [int_sq]
              rw [← Int.mul_assoc]
              exact algebra_expand_true _ _ _ _ _
            · have hcondn : ¬ ((some a == some b) = true) := by
                have hne : a ≠ b := by
                  intro heq
                  apply hba
                  simp [heq]
                simpa [beq_iff_eq] using hne
              rw [if_neg hcondn]
              have hlead : leadCount a (b :: bs) = 0 := by
                unfold leadCount
                have hbeqFalse : (b == a) = false := by
                  cases h : b == a <;> simp_all
                simp [hbeqFalse]
              have hm2 : measure (b :: bs) = 1 + (leadCount b bs : Int) + measure bs := by
                rw [measure_cons]
              have hm1 : measure (a :: b :: bs) = 2 + (leadCount b bs : Int) + measure bs := by
                rw [measure_cons, hlead, hm2]
                omega
              rw [hm1, hm2]
              dsimp [size]
              rw [int_sq]
              rw [show ((bs.length : Int) + 1 + 1) = (bs.length : Int) + 2 by omega]
              simpa using
                (algebra_expand_false (choose2 (n + 1) : Int) (bs.length : Int)
                  (measure bs) (leadCount b bs : Int) ((n : Int) * (n : Int)))

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames
