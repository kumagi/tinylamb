import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures2

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ## Generic list and arithmetic helpers -/

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem perm_sum {xs ys : List Int} (h : xs.Perm ys) : xs.sum = ys.sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b xs => simp [Int.add_comm, Int.add_left_comm, Int.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  exact Int.le_max_left _ _

private theorem le_max0_of_mem {xs : List Int} {a : Int} (h : a ∈ xs) : a ≤ max0 xs := by
  unfold max0
  rcases hm : xs.max? with _ | m
  · have hn := List.max?_eq_none_iff.mp hm
    simp [hn] at h
  · have hparts := List.max?_eq_some_iff.mp hm
    have ham : a ≤ m := hparts.2 a h
    exact Int.le_trans ham (Int.le_max_right _ _)

private theorem max0_le (hc : 0 ≤ c) {xs : List Int} (h : ∀ a ∈ xs, a ≤ c) : max0 xs ≤ c := by
  unfold max0
  apply Int.max_le.mpr
  constructor
  · exact hc
  · rcases hm : xs.max? with _ | m
    · simpa using hc
    · have hparts := List.max?_eq_some_iff.mp hm
      exact (List.max?_le_iff hm).mpr (fun b hb => h b hb)

private theorem max0_append (xs ys : List Int) : max0 (xs ++ ys) = max (max0 xs) (max0 ys) := by
  apply Int.le_antisymm
  · apply max0_le (c := max (max0 xs) (max0 ys))
      (Int.le_trans (max0_nonneg xs) (Int.le_max_left _ _))
    intro a ha
    rcases List.mem_append.mp ha with ha | ha
    · exact Int.le_trans (le_max0_of_mem ha) (Int.le_max_left _ _)
    · exact Int.le_trans (le_max0_of_mem ha) (Int.le_max_right _ _)
  · apply Int.max_le.mpr
    constructor
    · apply max0_le (max0_nonneg _)
      intro a ha
      exact le_max0_of_mem (List.mem_append_left _ ha)
    · apply max0_le (max0_nonneg _)
      intro a ha
      exact le_max0_of_mem (List.mem_append_right _ ha)

private theorem sum_map_add (xs : List Int) (f g : Int → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_sub (xs : List Int) (f g : Int → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_add_const (xs : List Int) (f : Int → Int) (c : Int) :
    (xs.map (fun x => f x + c)).sum = (xs.map f).sum + (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem sum_map_le {xs : List Int} {f g : Int → Int}
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have ht := ih (fun x hx => h x (by simp [hx]))
      omega

private theorem sum_map_nonneg {xs : List Int} {f : Int → Int}
    (h : ∀ x ∈ xs, 0 ≤ f x) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have ht := ih (fun x hx => h x (by simp [hx]))
      omega

private theorem sum_map_mul_left (xs : List Int) (c : Int) (f : Int → Int) :
    (xs.map (fun x => c * f x)).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_sq_affine (c : Int) (xs : List Int) :
    (xs.map (fun v => (c + v) ^ 2)).sum =
      (xs.length : Int) * c ^ 2 + 2 * c * xs.sum + (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  rw [show a ^ 2 = a * a by grind]
  by_cases h : 0 ≤ a
  · exact Int.mul_nonneg h h
  · have hn : 0 ≤ -a := by omega
    have hm := Int.mul_nonneg hn hn
    grind

private theorem sum_sq_cauchy (xs : List Int) :
    xs.sum ^ 2 ≤ (xs.length : Int) * (xs.map (fun x => x ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons, List.sum_cons]
      have hcast : ((xs.length + 1 : Nat) : Int) = (xs.length : Int) + 1 := by simp
      rw [hcast]
      by_cases hk : xs.length = 0
      · simp [hk]
        grind
      · have hkposNat : 0 < xs.length := Nat.pos_of_ne_zero hk
        have hkpos : (0 : Int) < xs.length := by exact_mod_cast hkposNat
        let k : Int := xs.length
        let Q : Int := (xs.map fun x => x ^ 2).sum
        let S : Int := xs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hs : 0 ≤ (k * a - S) ^ 2 := sq_nonneg _
        have hid : (k * a - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * a ^ 2 - 2 * a * S + Q) := by grind
        have hp : 0 ≤ k * (k * a ^ 2 - 2 * a * S + Q) := by
          rw [← hid]
          omega
        have hmain : 0 ≤ k * a ^ 2 - 2 * a * S + Q := by
          by_cases hnonneg : 0 ≤ k * a ^ 2 - 2 * a * S + Q
          · exact hnonneg
          · have hneg : k * a ^ 2 - 2 * a * S + Q < 0 := by omega
            have hmneg := Int.mul_neg_of_pos_of_neg hkpos hneg
            have hcontr : False := (Int.lt_irrefl 0) (Int.lt_of_le_of_lt hp hmneg)
            exact False.elim hcontr
        change (a + S) ^ 2 ≤ (k + 1) * (a ^ 2 + Q)
        rw [show (a + S) ^ 2 = a ^ 2 + 2 * a * S + S ^ 2 by grind]
        rw [show (k + 1) * (a ^ 2 + Q) = a ^ 2 + Q + k * a ^ 2 + k * Q by grind]
        omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ n => unfold choose2; grind

private theorem choose2_self (n : Nat) : choose2 (n + 1) = n * (n + 1) / 2 := by
  unfold choose2
  grind

private theorem choose2_double (n : Nat) :
    2 * (choose2 (n + 1) : Int) = (n : Int) * ((n : Int) + 1) := by
  rw [choose2_self]
  have hm := Nat.mod_add_div n 2
  rcases Nat.mod_two_eq_zero_or_one n with h | h
  · have hd : 2 ∣ n * (n + 1) := by
      refine ⟨n / 2 * (n + 1), ?_⟩
      calc
        n * (n + 1) = (n / 2 * 2) * (n + 1) := by congr 1; omega
        _ = 2 * (n / 2 * (n + 1)) := by grind
    exact_mod_cast Nat.mul_div_cancel' hd
  · have hd : 2 ∣ n * (n + 1) := by
      refine ⟨n * ((n + 1) / 2), ?_⟩
      calc
        n * (n + 1) = n * (2 * ((n + 1) / 2)) := by congr 1; omega
        _ = 2 * (n * ((n + 1) / 2)) := by grind
    exact_mod_cast Nat.mul_div_cancel' hd

private theorem repeated_length (n : Nat) (a : Int) : (repeated n a).length = n := by
  simp [repeated]

/-! ## Prefix sums -/

private theorem prefs_nil : prefs [] = [] := rfl

private theorem flatten_replicate_succ (x : Series) (n : Nat) :
    (List.replicate (n + 1) x).flatten = x ++ (List.replicate n x).flatten := by
  simp [List.replicate_succ, List.flatten_cons]

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  simp [prefs, List.range_succ_eq_map, List.take_succ_cons, List.map_map,
    Function.comp_def]

private theorem prefs_length (x : Series) : (prefs x).length = x.length := by
  simp [prefs]

private theorem prefs_singleton (a : Int) : prefs [a] = [a] := by
  simp [prefs]

private theorem prefs_append (xs ys : Series) :
    prefs (xs ++ ys) = prefs xs ++ (prefs ys).map (fun v => xs.sum + v) := by
  induction xs with
  | nil => simp [prefs_nil]
  | cons a xs ih =>
      rw [List.cons_append, prefs_cons, prefs_cons, ih]
      simp only [List.map_append, List.map_cons, List.cons_append, List.map_map,
        Function.comp_def, List.sum_cons]
      rw [show (prefs ys).map (fun x => a + (xs.sum + x)) =
          (prefs ys).map (fun v => a + xs.sum + v) by
        apply List.map_congr_left
        intro v hv
        grind]

private theorem prefs_scaled (a : Int) (xs : Series) :
    prefs (scaled a xs) = (prefs xs).map (fun v => a * v) := by
  induction xs with
  | nil => simp [scaled, prefs_nil]
  | cons b xs ih =>
      simp only [scaled, List.map_cons, prefs_cons, List.map_cons]
      rw [show prefs (List.map (fun x => a * x) xs) =
          (prefs xs).map (fun v => a * v) by simpa [scaled] using ih]
      simp only [List.map_map, Function.comp_def]
      congr 1
      apply congrArg (fun h : Int → Int => (prefs xs).map h)
      funext v
      grind

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

private theorem prefix_energy_cons (a : Int) (xs : Series) :
    PrefixEnergy.measure (a :: xs) =
      a ^ 2 + 2 * a * PrefixExposure.measure xs +
        (xs.length : Int) * a ^ 2 + PrefixEnergy.measure xs := by
  change ((prefs (a :: xs)).map (fun v => v ^ 2)).sum = _
  rw [prefs_cons]
  simp only [List.map_cons, List.sum_cons, List.map_map, Function.comp_def]
  have hsq : ((prefs xs).map (fun v => (a + v) ^ 2)).sum =
      (xs.length : Int) * a ^ 2 + 2 * a * PrefixExposure.measure xs +
        PrefixEnergy.measure xs := by
    simpa only [PrefixExposure.measure, PrefixEnergy.measure, prefs_length] using
      sum_map_sq_affine a (prefs xs)
  rw [hsq]
  grind

/-! ## PrefixExposure -/

theorem sound_PrefixExposure_Multiplicities : PrefixExposure.Multiplicities.Goal := by
  intro x
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
      have hweights : ((xs.zipIdx 0).map (fun p => p.1 * ((xs.length + 1 - (p.2 + 1) : Nat) : Int))) =
          ((xs.zipIdx 0).map (fun p => p.1 * ((xs.length - p.2 : Nat) : Int))) := by
        apply List.map_congr_left
        intro p hp
        have hi := (List.mem_zipIdx hp).2.1
        have hnat : xs.length + 1 - (p.2 + 1) = xs.length - p.2 := by omega
        rw [hnat]
      rw [hweights]
      grind

theorem sound_PrefixExposure_InheritedBalance : PrefixExposure.InheritedBalance.Goal := by
  intro x y
  unfold PrefixExposure.measure size
  rw [prefs_append, List.sum_append,
    show ((prefs y).map (fun v => x.sum + v)).sum =
      (prefs y).sum + (prefs y).length * x.sum by
      simpa [Int.add_comm] using sum_map_add_const (prefs y) (fun v => v) x.sum]
  simp [prefs]
  grind

theorem sound_PrefixExposure_ReverseComplement : PrefixExposure.ReverseComplement.Goal := by
  intro x
  induction x with
  | nil => simp [PrefixExposure.measure, prefs_nil]
  | cons a xs ih =>
      rw [prefix_exposure_cons]
      have hrev := sound_PrefixExposure_InheritedBalance xs.reverse [a]
      have hsum : xs.reverse.sum = xs.sum := by
        induction xs with
        | nil => simp
        | cons b bs ih => simp only [List.reverse_cons, List.sum_append, List.sum_cons,
            List.sum_singleton, ih]; grind
      simp [prefs_nil, PrefixExposure.measure, size] at hrev
      simp only [List.reverse_cons]
      unfold PrefixExposure.measure at ih ⊢
      rw [hrev]
      have hpre : (prefs [a]).sum = a := by rw [prefs_singleton]; simp
      rw [hpre]
      simp only [size, List.length_cons, List.sum_cons, List.sum_nil, Int.add_zero] at ih ⊢
      grind

theorem sound_PrefixExposure_Deposit : PrefixExposure.Deposit.Goal := by
  intro b x
  induction x with
  | nil => simp [shifted, PrefixExposure.measure, prefs_nil, choose2]
  | cons a xs ih =>
      simp only [shifted, List.map_cons] at ih ⊢
      rw [prefix_exposure_cons, prefix_exposure_cons]
      simp only [List.length_cons] at ih ⊢
      rw [ih]
      rw [choose2_succ (xs.length + 1)]
      simp
      grind

private theorem sum_flatten_replicate (x : Series) (n : Nat) :
    (List.replicate n x).flatten.sum = (n : Int) * x.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [flatten_replicate_succ, List.sum_append, ih]
      grind

private theorem size_flatten_replicate (x : Series) (n : Nat) :
    size (List.replicate n x).flatten = (n : Int) * size x := by
  induction n with
  | zero => simp [size]
  | succ n ih =>
      rw [flatten_replicate_succ]
      unfold size at ih ⊢
      simp only [List.length_append, ih]
      simp
      grind

theorem sound_PrefixExposure_BatchRepeat : PrefixExposure.BatchRepeat.Goal := by
  intro x n
  induction n with
  | zero => simp [PrefixExposure.measure, prefs_nil, choose2]
  | succ n ih =>
      have hflat : (List.replicate (n + 1) x).flatten =
          x ++ (List.replicate n x).flatten := flatten_replicate_succ x n
      have hinc := sound_PrefixExposure_InheritedBalance x (List.replicate n x).flatten
      rw [hflat, hinc, ih, choose2_succ, size_flatten_replicate]
      simp
      grind

/-! ## PrefixEnergy -/

theorem sound_PrefixEnergy_OffsetQuadratic : PrefixEnergy.OffsetQuadratic.Goal := by
  intro x y
  change ((prefs (x ++ y)).map (fun v => v ^ 2)).sum =
    ((prefs x).map (fun v => v ^ 2)).sum + ((prefs y).map (fun v => v ^ 2)).sum +
      2 * x.sum * (prefs y).sum + (y.length : Int) * x.sum ^ 2
  rw [prefs_append, List.map_append, List.sum_append, List.map_map, Function.comp_def]
  have hsq := sum_map_sq_affine x.sum (prefs y)
  rw [prefs_length] at hsq
  rw [hsq]
  grind

private theorem prefix_exposure_repeated (n : Nat) (a : Int) :
    PrefixExposure.measure (repeated n a) = (choose2 (n + 1) : Int) * a := by
  induction n with
  | zero => simp [repeated, PrefixExposure.measure, prefs_nil, choose2]
  | succ n ih =>
      have hrep : repeated (n + 1) a = a :: repeated n a := by
        simp [repeated, List.replicate_succ]
      rw [hrep, prefix_exposure_cons, ih, choose2_succ (n + 1)]
      simp only [repeated, List.length_replicate]
      simp
      grind

theorem sound_PrefixEnergy_ConstantPage : PrefixEnergy.ConstantPage.Goal := by
  intro n a
  induction n with
  | zero => simp [repeated, PrefixEnergy.measure, prefs_nil]
  | succ n ih =>
      have hrep : repeated (n + 1) a = a :: repeated n a := by
        simp [repeated, List.replicate_succ]
      rw [hrep, prefix_energy_cons, prefix_exposure_repeated, repeated_length]
      change 6 * (a ^ 2 + 2 * a * ((choose2 (n + 1) : Int) * a) +
          (n : Int) * a ^ 2 + PrefixEnergy.measure (repeated n a)) =
        a ^ 2 * ((n : Int) + 1) * ((n : Int) + 2) * (2 * (n : Int) + 3)
      have hc := choose2_double n
      grind

theorem sound_PrefixEnergy_UnitScaling : PrefixEnergy.UnitScaling.Goal := by
  intro a x
  unfold PrefixEnergy.measure
  rw [prefs_scaled, List.map_map, Function.comp_def]
  rw [show ((prefs x).map (fun v => (a * v) ^ 2)) =
      ((prefs x).map (fun v => a ^ 2 * v ^ 2)) by
    apply List.map_congr_left
    intro v hv
    grind]
  rw [sum_map_mul_left]

theorem sound_PrefixEnergy_ReverseEnergy : PrefixEnergy.ReverseEnergy.Goal := by
  intro x
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [prefix_energy_cons, prefix_exposure_cons]
      have hrev : PrefixEnergy.measure (xs.reverse ++ [a]) =
          PrefixEnergy.measure xs.reverse + 2 * xs.sum * a + a ^ 2 + xs.sum ^ 2 := by
        unfold PrefixEnergy.measure
        rw [prefs_append, List.map_append, List.sum_append, List.sum_reverse]
        have hpre : prefs [a] = [a] := prefs_singleton a
        rw [hpre]
        simp only [List.map_singleton, List.sum_singleton]
        rw [show (xs.sum + a) ^ 2 = xs.sum ^ 2 + 2 * xs.sum * a + a ^ 2 by grind]
        grind
      simp only [List.reverse_cons]
      rw [hrev, ih]
      simp only [size, List.length_cons, List.sum_cons, List.sum_nil, Int.add_zero] at ih ⊢
      rw [show (a + xs.sum) ^ 2 = a ^ 2 + 2 * a * xs.sum + xs.sum ^ 2 by grind]
      grind

theorem sound_PrefixEnergy_ThresholdBound : PrefixEnergy.ThresholdBound.Goal := by
  intro x
  unfold PrefixExposure.measure PrefixEnergy.measure size
  rw [← prefs_length x]
  exact sum_sq_cauchy (prefs x)

/-! ## RunTransitions -/

private theorem run_nil : RunTransitions.measure [] = 0 := rfl

private theorem run_singleton (a : Int) : RunTransitions.measure [a] = 0 := rfl

private theorem run_nonneg (x : Series) : 0 ≤ RunTransitions.measure x := by
  unfold RunTransitions.measure
  exact Int.natCast_nonneg _

private theorem run_cons (a : Int) (xs : Series) :
    RunTransitions.measure (a :: xs) = RunTransitions.measure xs +
      match xs.head? with
      | none => 0
      | some b => if a == b then 0 else 1 := by
  cases xs with
  | nil => simp [run_nil, run_singleton]
  | cons b bs =>
      simp only [RunTransitions.measure, adj, List.drop_one, List.tail_cons]
      rw [List.zip_cons_cons, List.countP_cons]
      by_cases h : a = b
      · subst b
        simp
      · simp [h, beq_iff_eq]

private theorem seam_cons_cons (a b : Int) (bs ys : Series) (f : Int → Int → Int) :
    seam (a :: b :: bs) ys f = seam (b :: bs) ys f := rfl

private theorem run_append (xs ys : Series) :
    RunTransitions.measure (xs ++ ys) = RunTransitions.measure xs +
      RunTransitions.measure ys + seam xs ys (fun a b => if a == b then 0 else 1) := by
  induction xs with
  | nil => simp [run_nil, seam]
  | cons a xs ih =>
      cases xs with
      | nil =>
          rw [List.cons_append, List.nil_append, run_cons]
          cases ys with
          | nil => simp [run_nil, run_singleton, seam]
          | cons b bs => simp [run_nil, run_singleton, seam]
      | cons b bs =>
          simp only [List.cons_append]
          calc
            RunTransitions.measure (a :: (b :: (bs ++ ys))) =
                RunTransitions.measure (b :: (bs ++ ys)) +
                  (if a == b then (0 : Int) else 1) := by
                    rw [run_cons]
                    simp
            _ = (RunTransitions.measure (b :: bs) + RunTransitions.measure ys +
                  seam (b :: bs) ys (fun a b => if a == b then 0 else 1)) +
                  (if a == b then (0 : Int) else 1) := by
                    simpa only [List.cons_append] using congrArg
                      (fun z => z + (if a == b then (0 : Int) else 1)) ih
            _ = RunTransitions.measure (a :: b :: bs) + RunTransitions.measure ys +
                  seam (a :: b :: bs) ys (fun a b => if a == b then 0 else 1) := by
                    rw [run_cons (a := a) (xs := b :: bs), seam_cons_cons]
                    grind

theorem sound_RunTransitions_PageBoundary : RunTransitions.PageBoundary.Goal := by
  exact run_append

theorem sound_RunTransitions_CoarseDictionary : RunTransitions.CoarseDictionary.Goal := by
  intro x f
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          change RunTransitions.measure (b :: bs) -
              RunTransitions.measure (f b :: List.map f bs) = _ at ih
          simp only [List.map_cons]
          calc
            RunTransitions.measure (a :: b :: bs) - RunTransitions.measure (f a :: f b :: List.map f bs) =
                (RunTransitions.measure (b :: bs) +
                  (if a == b then (0 : Int) else 1)) -
                (RunTransitions.measure (f b :: List.map f bs) +
                  (if f a == f b then (0 : Int) else 1)) := by
                    simp only [run_cons (a := a) (xs := b :: bs),
                      run_cons (a := f a) (xs := f b :: List.map f bs), List.head?_cons]
            _ = (RunTransitions.measure (b :: bs) - RunTransitions.measure (f b :: List.map f bs)) +
                (if a == b then (0 : Int) else 1) -
                (if f a == f b then (0 : Int) else 1) := by grind
            _ = ((adj (b :: bs)).countP (fun t => t.1 != t.2 && f t.1 == f t.2) : Int) +
                (if a == b then (0 : Int) else 1) -
                (if f a == f b then (0 : Int) else 1) := by rw [ih]
            _ = ((adj (a :: b :: bs)).countP (fun t => t.1 != t.2 && f t.1 == f t.2) : Int) := by
                simp only [adj, List.drop_one, List.tail_cons]
                rw [List.zip_cons_cons, List.countP_cons]
                by_cases hab : a = b
                · subst b
                  simp
                · by_cases hfab : f a = f b
                  · simp [hab, hfab, beq_iff_eq]
                  · simp [hab, hfab, beq_iff_eq]

private theorem run_filter_drop (a : Int) (xs : Series) (keep : Int → Bool)
    (hk : keep a = false) :
    RunTransitions.measure ((a :: xs).filter keep) = RunTransitions.measure (xs.filter keep) := by
  rw [List.filter_cons, if_neg (by simp [hk])]

private theorem run_filter_keep (a : Int) (xs : Series) (keep : Int → Bool)
    (hk : keep a = true) :
    RunTransitions.measure ((a :: xs).filter keep) =
      RunTransitions.measure (xs.filter keep) +
        match (xs.filter keep).head? with
        | none => 0
        | some b => if a == b then 0 else 1 := by
  rw [List.filter_cons, if_pos hk, run_cons]

private def runFrom (prev : Option Int) (xs : Series) : Int :=
  RunTransitions.measure xs +
    match prev, xs.head? with
    | some a, some b => if a == b then 0 else 1
    | _, _ => 0

private theorem runFrom_none (xs : Series) : runFrom none xs = RunTransitions.measure xs := by
  simp [runFrom]

private theorem runFrom_cons (prev : Option Int) (a : Int) (xs : Series) :
    runFrom prev (a :: xs) =
      (match prev with
       | none => 0
       | some p => if p == a then 0 else 1) + RunTransitions.measure (a :: xs) := by
  cases prev <;> simp [runFrom, Int.add_comm]

private theorem runFrom_none_cons (a : Int) (xs : Series) :
    runFrom none (a :: xs) = runFrom (some a) xs := by
  cases xs <;> simp [runFrom, run_cons]

private theorem run_cons_eq_runFrom (a : Int) (xs : Series) :
    RunTransitions.measure (a :: xs) = runFrom (some a) xs := by
  simpa [runFrom] using (runFrom_none_cons a xs)

private theorem runFrom_cons_prev (prev : Option Int) (a : Int) (xs : Series) :
    runFrom prev (a :: xs) =
      (match prev with
       | none => 0
       | some p => if p == a then 0 else 1) + runFrom (some a) xs := by
  rw [runFrom_cons, run_cons_eq_runFrom]

private theorem runFrom_cons_ge (prev : Option Int) (a : Int) (xs : Series) :
    runFrom prev xs ≤ runFrom prev (a :: xs) := by
  cases prev with
  | none =>
      rw [runFrom_none, runFrom_none, run_cons]
      split <;> omega
  | some p =>
      cases xs with
      | nil => simp [runFrom, run_nil, run_singleton]; split <;> omega
      | cons b bs =>
          simp only [runFrom, List.head?_cons, run_cons]
          have hi : (if p == b then (0 : Int) else 1) ≤
              (if p == a then (0 : Int) else 1) + (if a == b then (0 : Int) else 1) := by
            by_cases hpa : p = a
            · subst p
              simp
            · by_cases hab : a = b
              · subst b
                simp [hpa, beq_iff_eq]
              · by_cases hpb : p = b <;> simp [hpa, hab, hpb, beq_iff_eq] <;> omega
          omega

private theorem runFrom_ge (prev : Option Int) (xs : Series) :
    RunTransitions.measure xs ≤ runFrom prev xs := by
  unfold runFrom
  cases prev <;> cases hx : xs.head? <;> simp <;> omega

theorem sound_RunTransitions_MaskedSubsequence : RunTransitions.MaskedSubsequence.Goal := by
  intro x keep
  have haux : ∀ (prev : Option Int) (xs : Series),
      runFrom prev (xs.filter keep) ≤ runFrom prev xs := by
    intro prev xs
    induction xs generalizing prev with
    | nil => exact Int.le_refl _
    | cons a xs ih =>
        cases hk : keep a
        · rw [List.filter_cons, if_neg (by simp [hk])]
          exact Int.le_trans (ih prev) (runFrom_cons_ge prev a xs)
        · rw [List.filter_cons, if_pos hk, runFrom_cons_prev, runFrom_cons_prev]
          exact Int.add_le_add_left (ih (some a)) _
  simpa [runFrom_none] using haux none x

theorem sound_RunTransitions_PeriodicBoundary : RunTransitions.PeriodicBoundary.Goal := by
  intro x n
  induction n with
  | zero => simp [run_nil, seam]
  | succ n ih =>
      rw [flatten_replicate_succ, run_append, ih]
      have hs : seam x (List.replicate n x).flatten
          (fun a b => if a == b then 0 else 1) =
          if n = 0 then 0 else seam x x (fun a b => if a == b then 0 else 1) := by
        cases n with
        | zero => simp [seam]
        | succ m =>
            have hfl : (List.replicate (m + 1) x).flatten =
                x ++ (List.replicate m x).flatten := flatten_replicate_succ x m
            rw [hfl]
            cases x with
            | nil => simp [seam]
            | cons a xs => simp [seam]
      rw [hs]
      cases n with
      | zero => simp [run_nil, seam]
      | succ m => simp; grind

private theorem eraseDups_sorted_cons (a b : Int) (bs : Series)
    (hab : a ≤ b) (hs : (b :: bs).Pairwise (· ≤ ·)) :
    (a :: b :: bs).eraseDups.length =
      if a = b then (b :: bs).eraseDups.length else (b :: bs).eraseDups.length + 1 := by
  by_cases heq : a = b
  · subst b
    simp [List.eraseDups_cons]
  · have hnot : a ∉ b :: bs := by
      intro hm
      rcases List.mem_cons.mp hm with rfl | hm
      · exact heq rfl
      · have hba := (List.pairwise_cons.mp hs).1 a hm
        omega
    have hself : (b :: bs).filter (fun z => !z == a) = b :: bs := by
      apply List.filter_eq_self.mpr
      intro z hz
      have hza : z ≠ a := by
        intro h
        subst z
        exact hnot hz
      simp [hza, beq_iff_eq]
    rw [List.eraseDups_cons, hself]
    simp [heq]

theorem sound_RunTransitions_SortedDictionary : RunTransitions.SortedDictionary.Goal := by
  intro x hs
  induction x with
  | nil => simp [run_nil]
  | cons a xs ih =>
      cases xs with
      | nil => simp [run_singleton, List.eraseDups_cons]
      | cons b bs =>
          have htail : (b :: bs).Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
          have hab : a ≤ b := (List.pairwise_cons.mp hs).1 b (by simp)
          have hih := ih htail
          rw [run_cons]
          simp only [List.head?_cons, hih]
          rw [eraseDups_sorted_cons a b bs hab htail]
          have hlenpos : 0 < (b :: bs).eraseDups.length := by
            simp [List.eraseDups_cons]
          by_cases heq : a = b
          · subst b
            simp
          · have hne : (a == b) = false := by
              apply Bool.eq_false_of_ne_true
              intro h
              exact heq (of_decide_eq_true h)
            simp [hne, heq]
            omega

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures2
