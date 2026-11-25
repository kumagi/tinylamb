import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P9

open TinylambOpt.ResearchGoals.Atlas.EncodedPages


private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := p.length_eq

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  apply filterMap_congr'
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    simp [ha, hb]
  · have hb : ¬ b := fun hb => ha (h.mpr hb)
    simp [ha, hb]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    ((l.map fun x => f x + g x).sum) = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_le_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem sum_map_sub_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, g x ≤ f x) :
    ((l.map fun x => f x - g x).sum) = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hle : (xs.map g).sum ≤ (xs.map f).sum :=
        sum_map_le_nat xs g f (fun y hy => h y (by simp [hy]))
      have hx : g x ≤ f x := h x (by simp)
      simp only [List.map_cons, List.sum_cons]
      rw [ih (fun y hy => h y (by simp [hy]))]
      omega

private theorem countP_eq_sum {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem countP_between_eq_sub {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, q x = true → p x = true) :
    l.countP (fun x => p x && !q x) = l.countP p - l.countP q := by
  have hqlen : l.countP q = (l.filter q).length := List.countP_eq_length_filter
  have hqa : l.countP q = (l.filter q).countP p := by
    rw [hqlen]
    exact (List.countP_eq_length.mpr (fun x hx =>
      h x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx).2)).symm
  have hleft : l.countP (fun x => p x && !q x) = (l.filter fun a => !q a).countP p := by
    rw [← List.countP_filter (p := p) (q := fun a => !q a)]
  have hpart := List.countP_eq_countP_filter_add l p q
  rw [hleft, hpart, hqa]
  exact (Nat.add_sub_cancel_left _ _).symm

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]


private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Nat) * k := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

private theorem partialSum_succ (a : Lane) (k : Nat) :
    partialSum a (k + 1) = partialSum a k + (a.base + (k : Int) * a.step) := by
  rw [partialSum, partialSum, tri_succ]
  simp [Int.natCast_add, Int.add_mul]
  omega

private theorem partialSum_eq_sum (a : Lane) (k : Nat) :
    partialSum a k = ((List.range k).map (fun i : Nat => a.base + (i : Int) * a.step)).sum := by
  induction k with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [partialSum_succ, ih]
      rw [List.range_succ, List.map_append, List.sum_append]
      simp

private theorem sum_squares_eq (k : Nat) :
    ((List.range k).map (fun i : Nat => (i : Int) * (i : Int))).sum = (squares k : Int) := by
  induction k with
  | zero => simp [squares]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]

/--! ## Finite progressions and CRT intersections (mirrors EncodedPages4) -/

private def progVal (a : Progression) (k : Nat) : Int := a.lo + (k : Int) * a.step

private def progMem (a : Progression) (v : Int) : Prop :=
  ∃ k, k < a.n ∧ v = progVal a k

private theorem contains_iff_mem (a : Progression) (v : Int) :
    contains a v = true ↔ progMem a v := by
  unfold contains lastVal progMem progVal
  constructor
  · intro h
    have h' : 0 < a.n ∧ a.lo ≤ v ∧ v ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step ∧
        (a.step == 0 || (v - a.lo) % (a.step : Int) == 0) = true := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using h
    rcases h' with ⟨hn, hlo, hhi, hmod⟩
    by_cases hs : a.step == 0
    · have hs' : a.step = 0 := Nat.eq_of_beq_eq_true hs
      refine ⟨0, hn, ?_⟩
      simp only [hs', Int.mul_zero, Int.add_zero] at hhi hlo ⊢
      omega
    · have hs' : a.step ≠ 0 := by simpa using hs
      have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs'
      have hmod' : (v - a.lo) % (a.step : Int) = 0 := by
        simpa [hs] using hmod
      let q := (v - a.lo) / a.step
      have hqmul : a.step * q = v - a.lo := by
        simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
      have hqmul' : q * a.step = v - a.lo := by simpa only [Int.mul_comm] using hqmul
      have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
      have hqle : q ≤ (a.n - 1 : Nat) := by
        apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
        change a.step * ((v - a.lo) / a.step) ≤ a.step * (a.n - 1 : Nat)
        have hh : a.step * ((v - a.lo) / a.step) = v - a.lo := by
          simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
        rw [hh]
        have hh2 := Int.sub_le_sub_right hhi a.lo
        have hcancel : a.lo + ((a.n - 1 : Nat) : Int) * a.step - a.lo =
            ((a.n - 1 : Nat) : Int) * a.step := by omega
        rw [hcancel] at hh2
        simpa only [Int.mul_comm] using hh2
      have hqnat : q.toNat < a.n := by
        have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
        omega
      refine ⟨q.toNat, hqnat, ?_⟩
      rw [Int.toNat_of_nonneg hqnon]
      symm
      calc
        a.lo + q * a.step = a.lo + (v - a.lo) := congrArg (fun x : Int => a.lo + x) hqmul'
        _ = v := by omega
  · rintro ⟨k, hk, rfl⟩
    change (decide (0 < a.n) && decide (a.lo ≤ a.lo + (k : Int) * a.step) &&
      decide (a.lo + (k : Int) * a.step ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step) &&
      (a.step == 0 || (a.lo + (k : Int) * a.step - a.lo) % (a.step : Int) == 0)) = true
    have hsub : a.lo + (k : Int) * a.step - a.lo = (k : Int) * a.step := by omega
    rw [hsub]
    by_cases hs : a.step = 0
    · simp [hs]
      omega
    · have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs
      have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
      have hmul : a.step * (k : Int) ≤ a.step * (a.n - 1 : Nat) := by
        simpa only [Int.mul_comm] using Int.mul_le_mul_of_nonneg_left hkint (Int.le_of_lt hpos)
      have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
        simpa only [Int.mul_comm] using hmul
      simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc]
      refine ⟨by omega, ?_, ?_, ?_⟩
      · exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hpos))
      · exact Int.add_le_add_left hmul' a.lo
      · rw [Bool.or_eq_true]
        right
        change (((k : Int) * a.step) % a.step == 0) = true
        rw [beq_iff_eq]
        apply Int.emod_eq_zero_of_dvd
        exact ⟨(k : Int), Int.mul_comm _ _⟩

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        simp only [hbez]
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem reduced_coprime (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    (a / a.gcd b).gcd (b / a.gcd b) = 1 := by
  exact Nat.gcd_div_gcd_div_gcd_of_pos_left ha

private theorem bezout_reduced_inverse (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    ((((a / a.gcd b : Nat) : Int) * (bezout (a / a.gcd b) (b / a.gcd b)).1) - 1) %
        (b / a.gcd b : Nat) = 0 := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left b ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right a hb
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using reduced_coprime a b ha hb
  have hspec := bezout_spec A B
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(bezout A B).2, ?_⟩
  change (A : Int) * (bezout A B).1 - 1 = (B : Int) * -(bezout A B).2
  rw [hspec]
  grind

private theorem mem_of_bounds_dvd (a : Progression) (v : Int) (hn : 0 < a.n) (hs : 0 < a.step)
    (hlo : a.lo ≤ v) (hhi : v ≤ lastVal a) (hdvd : (a.step : Int) ∣ v - a.lo) :
    contains a v = true := by
  obtain ⟨q, hq⟩ := hdvd
  have hpos : (0 : Int) < a.step := by exact_mod_cast hs
  have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
  have hqle : q ≤ (a.n - 1 : Nat) := by
    apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
    rw [← hq]
    unfold lastVal at hhi
    rw [Int.mul_comm ((a.n - 1 : Nat) : Int) a.step] at hhi
    omega
  apply (contains_iff_mem a v).mpr
  refine ⟨q.toNat, ?_, ?_⟩
  · have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
    omega
  · unfold progVal
    rw [Int.toNat_of_nonneg hqnon]
    change v = a.lo + q * a.step
    rw [Int.mul_comm a.step q] at hq
    omega

private theorem span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (contains_iff_mem ⟨first, step.toNat, if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    obtain ⟨k, hk, hval⟩ := hm
    unfold progVal at hval
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    simp only [hstepcast] at hval
    by_cases hfh : first ≤ high
    · change k < (if first ≤ high then ((high - first) / step).toNat + 1 else 0) at hk
      rw [if_pos hfh] at hk
      have ht : 0 ≤ (high - first) / step :=
        Int.ediv_nonneg (by omega) (Int.le_of_lt hs)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hkq : (k : Int) ≤ (high - first) / step :=
        (Int.le_ediv_iff_mul_le hs).mpr (by
          have hkNat : k ≤ ((high - first) / step).toNat := by omega
          have hkInt : (k : Int) ≤ (((high - first) / step).toNat : Int) := by exact_mod_cast hkNat
          calc
            (k : Int) * step ≤ (((high - first) / step).toNat : Int) * step :=
              Int.mul_le_mul_of_nonneg_right hkInt (Int.le_of_lt hs)
            _ = ((high - first) / step) * step := by rw [htcast]
            _ ≤ high - first := Int.ediv_mul_le _ (by omega))
      have hkle : (k : Int) * step ≤ high - first :=
        (Int.le_ediv_iff_mul_le hs).mp hkq
      refine ⟨?_, ?_, ?_⟩
      · rw [hval]
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hs))
      · calc
          x = first + (k : Int) * step := hval
          _ ≤ first + (high - first) := Int.add_le_add_left hkle first
          _ = high := by omega
      · refine ⟨(k : Int), ?_⟩
        calc
          x - first = (k : Int) * step := by omega
          _ = step * (k : Int) := Int.mul_comm _ _
    · simp [hfh] at hk
  · rintro ⟨hlo, hhi, hdvd⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    obtain ⟨q, hq⟩ := hdvd
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (Int.le_of_lt hs))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (contains_iff_mem ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    refine ⟨q.toNat, ?_, ?_⟩
    · have hle : q.toNat ≤ ((high - first) / step).toNat := by
        apply (Int.toNat_le (m := q) (n := ((high - first) / step).toNat)).mpr
        rw [htcast]
        exact hqle
      exact Nat.lt_succ_of_le hle
    · unfold progVal
      rw [Int.toNat_of_nonneg hqnon]
      change x = first + q * (step.toNat : Int)
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ step)]
      rw [Int.mul_comm step q] at hq
      omega

private theorem root_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (alo blo gap : Int)
    (hgapdef : gap = blo - alo) (hgap : (as.gcd bs : Int) ∣ gap) :
    let g := as.gcd bs
    let A := as / g
    let B := bs / g
    let root := alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int))
    (as : Int) ∣ root - alo ∧ (bs : Int) ∣ root - blo := by
  dsimp only
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  let t := (gap / (g : Int) * (bezout A B).1) % (B : Int)
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hb
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨z, hz⟩ := hgap
  have hzdiv : gap / (g : Int) = z := Int.ediv_eq_of_eq_mul_right (by omega) hz
  have hinv := bezout_reduced_inverse as bs ha hb
  have hinv' : (B : Int) ∣ (A : Int) * (bezout A B).1 - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have hmod := Int.mul_ediv_add_emod (z * (bezout A B).1) (B : Int)
  have hrem : z * (bezout A B).1 - t =
      (B : Int) * ((z * (bezout A B).1) / (B : Int)) := by
    dsimp [t]
    rw [hzdiv]
    omega
  have hdiff : (B : Int) ∣ z * ((A : Int) * (bezout A B).1 - 1) :=
    Int.dvd_mul_of_dvd_right hinv'
  have hmult : (B : Int) ∣ (A : Int) * (z * (bezout A B).1 - t) :=
    Int.dvd_mul_of_dvd_right (Int.dvd_of_emod_eq_zero (by
      rw [hrem]
      exact Int.emod_eq_zero_of_dvd ⟨_, rfl⟩))
  have hcomb : (B : Int) ∣ (A : Int) * t - z := by
    have heq : z * ((A : Int) * (bezout A B).1 - 1) -
        (A : Int) * (z * (bezout A B).1 - t) = (A : Int) * t - z := by grind
    rw [← heq]
    exact Int.dvd_sub hdiff hmult
  have hbs : (bs : Int) ∣ (as : Int) * t - gap := by
    rw [hbsg, hasg, hz]
    have hh := Int.mul_dvd_mul_left (g : Int) hcomb
    simpa only [Int.mul_sub, Int.mul_assoc] using hh
  constructor
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - alo =
        (as : Int) * t := by dsimp [t]; omega
    rw [hroot]
    exact ⟨t, rfl⟩
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - blo =
        (as : Int) * t - gap := by dsimp [t]; omega
    rw [hroot]
    exact hbs

private theorem first_root_dvd (low root first period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) : period ∣ first - root := by
  have hm := Int.mul_ediv_add_emod (root - low) period
  refine ⟨-((root - low) / period), ?_⟩
  grind

private theorem first_le_of_congruence (low root first x period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) (hx : low ≤ x)
    (hxdvd : period ∣ x - root) : first ≤ x := by
  have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega)
  have hremlt : (root - low) % period < period := Int.emod_lt_of_pos _ hp
  have hflow : low ≤ first := by omega
  have hfhigh : first < low + period := by omega
  have hfroot := first_root_dvd low root first period hp hfirst
  by_cases hle : first ≤ x
  · exact hle
  · exfalso
    have hlt : x < first := by omega
    have hpos : 0 < first - x := by omega
    have hltp : first - x < period := by omega
    have hdiff : period ∣ first - x := by
      have heq : (first - root) - (x - root) = first - x := by omega
      rw [← heq]
      exact Int.dvd_sub hfroot hxdvd
    obtain ⟨k, hk⟩ := hdiff
    have hkpos : 0 < k := Int.pos_of_mul_pos_right (by omega) hp
    have hkge : 1 ≤ k := by omega
    have hmul := Int.mul_le_mul_of_nonneg_left hkge (Int.le_of_lt hp)
    omega

private theorem contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  simp [contains, lastVal]
  omega








private theorem contains_step_one (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 1, n⟩ v = true ↔ 0 < n ∧ lo ≤ v ∧ v - lo < (n : Int) := by
  rw [contains_iff_mem]
  unfold progMem progVal
  simp only
  constructor
  · rintro ⟨k, hk, hv⟩
    refine ⟨?_, ?_, ?_⟩
    · omega
    · rw [hv]
      simp
      exact Int.le_add_of_nonneg_right (Int.natCast_nonneg k)
    · rw [hv]
      simp
      have hk' : (k : Int) < (n : Int) := by exact_mod_cast hk
      omega
  · rintro ⟨hn, hlo, hlt⟩
    let k := (v - lo).toNat
    have hnon : 0 ≤ v - lo := by omega
    have hkcast : (k : Int) = v - lo := Int.toNat_of_nonneg hnon
    have hklt : k < n := by
      have h' : (k : Int) < (n : Int) := by rw [hkcast]; exact hlt
      exact_mod_cast h'
    refine ⟨k, hklt, ?_⟩
    simp
    omega

private theorem contains_of_step_zero (a : Progression) (ha : a.step = 0) (v : Int) :
    contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
  rcases a with ⟨lo, st, n⟩
  simp at ha
  subst st
  simpa using (contains_const_iff lo n v)

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hsnon : (0 : Int) ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hmul' : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hsnon
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by exact_mod_cast Nat.zero_le k) hsnon)
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  grind

private theorem period_dvd_delta (s t : Nat) (hs : 0 < s) (ht : 0 < t) (delta : Int)
    (hds : (s : Int) ∣ delta) (hdt : (t : Int) ∣ delta) :
    (((s / Nat.gcd s t) * t : Nat) : Int) ∣ delta := by
  let g := s.gcd t
  let sa := s / g
  let tb := t / g
  have hcop : Nat.gcd sa tb = 1 := by simpa [sa, tb, g] using reduced_coprime s t hs ht
  obtain ⟨k1, hk1⟩ := hds
  obtain ⟨k2, hk2⟩ := hdt
  rcases hpair : bezout sa tb with ⟨u, v⟩
  have hbez := bezout_spec sa tb
  rw [hpair] at hbez
  change (Nat.gcd sa tb : Int) = (sa : Int) * u + (tb : Int) * v at hbez
  rw [hcop] at hbez
  have hsgi : (s : Int) = (g : Int) * (sa : Int) := by
    exact_mod_cast (show s = g * sa from (Nat.mul_div_cancel' (Nat.gcd_dvd_left s t)).symm)
  have htgi : (t : Int) = (g : Int) * (tb : Int) := by
    exact_mod_cast (show t = g * tb from (Nat.mul_div_cancel' (Nat.gcd_dvd_right s t)).symm)
  refine ⟨u * k2 + v * k1, ?_⟩
  change delta = (((sa * t : Nat) : Int)) * (u * k2 + v * k1)
  push_cast
  grind

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hg1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hg2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1' : ((a.step.gcd b.step : Nat) : Int) ∣ (j : Int) * (b.step : Int) :=
    Int.dvd_mul_of_dvd_right hg1
  have h1 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) * (j : Int) := by
    simpa [Int.mul_comm] using h1'
  have h2' : ((a.step.gcd b.step : Nat) : Int) ∣ (i : Int) * (a.step : Int) :=
    Int.dvd_mul_of_dvd_right hg2
  have h2 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) * (i : Int) := by
    simpa [Int.mul_comm] using h2'
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    have hi' : (a.step : Int) * (i : Int) = v - a.lo := by
      rw [Int.mul_comm]
      omega
    have hj' : (b.step : Int) * (j : Int) = v - b.lo := by
      rw [Int.mul_comm]
      omega
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · simp [han, intersection, contains]
  by_cases hbn : b.n = 0
  · simp [hbn, intersection, contains]
  have hanp : 0 < a.n := by omega
  have hbnp : 0 < b.n := by omega
  by_cases has : a.step = 0
  · unfold intersection
    simp [han, hbn, has]
    have haC := contains_of_step_zero a has v
    constructor
    · intro h
      rcases (contains_step_one a.lo (if contains b a.lo then 1 else 0) v).mp h with
        ⟨_, hlo, hlt⟩
      by_cases hba : contains b a.lo
      · simp [hba] at hlt
        have hv : v = a.lo := by omega
        subst v
        constructor
        · exact haC.mpr ⟨hanp, rfl⟩
        · exact hba
      · simp [hba] at hlt
        omega
    · rintro ⟨ha, hb⟩
      have hv : v = a.lo := (haC.mp ha).2
      subst v
      have hba : contains b a.lo = true := hb
      exact (contains_step_one a.lo (if contains b a.lo then 1 else 0) a.lo).mpr
        ⟨by simp [hba], by simp, by simp [hba]⟩
  · by_cases hbs : b.step = 0
    · unfold intersection
      simp [han, hbn, has, hbs]
      have hbC := contains_of_step_zero b hbs v
      constructor
      · intro h
        rcases (contains_step_one b.lo (if contains a b.lo then 1 else 0) v).mp h with
          ⟨_, hlo, hlt⟩
        by_cases hab : contains a b.lo
        · simp [hab] at hlt
          have hv : v = b.lo := by omega
          subst v
          constructor
          · exact hab
          · exact hbC.mpr ⟨hbnp, rfl⟩
        · simp [hab] at hlt
          omega
      · rintro ⟨ha, hb⟩
        have hv : v = b.lo := (hbC.mp hb).2
        subst v
        have hab : contains a b.lo = true := ha
        exact (contains_step_one b.lo (if contains a b.lo then 1 else 0) b.lo).mpr
          ⟨by simp [hab], by simp, by simp [hab]⟩
    · have hasp : 0 < a.step := Nat.pos_of_ne_zero has
      have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs
      let g := a.step.gcd b.step
      let gap := b.lo - a.lo
      let period := (a.step / g) * b.step
      let unit := (bezout (a.step / g) (b.step / g)).1
      let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
      let low := max a.lo b.lo
      let first := low + (root - low) % (period : Int)
      let high := min (lastVal a) (lastVal b)
      have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
      have hdivpos : 0 < a.step / g :=
        Nat.div_pos (Nat.le_of_dvd hasp (Nat.gcd_dvd_left a.step b.step)) hgpos
      have hpNat : 0 < period := by dsimp [period]; exact Nat.mul_pos hdivpos hbsp
      have hp : (0 : Int) < period := by exact_mod_cast hpNat
      by_cases hgapmod : gap % (g : Int) = 0
      · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
        have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap rfl hgap
        have hrootA : (a.step : Int) ∣ root - a.lo := by simpa [root, g, gap] using hroot.1
        have hrootB : (b.step : Int) ∣ root - b.lo := by simpa [root, g, gap] using hroot.2
        have hfirst : first = low + (root - low) % (period : Int) := rfl
        have hfroot := first_root_dvd low root first period hp hfirst
        have hga : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
        have hgb : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
        have hmul1 : period * g = a.step * b.step := by
          dsimp [period]
          calc
            ((a.step / g) * b.step) * g = (a.step / g) * g * b.step := by ac_rfl
            _ = a.step * b.step := by rw [Nat.div_mul_cancel hga]
        have hmul2 : (a.step * (b.step / g)) * g = a.step * b.step := by
          calc
            (a.step * (b.step / g)) * g = a.step * ((b.step / g) * g) := by ac_rfl
            _ = a.step * b.step := by rw [Nat.div_mul_cancel hgb]
        have hpaNat : period = a.step * (b.step / g) :=
          Nat.mul_right_cancel hgpos (hmul1.trans hmul2.symm)
        have hpa : (a.step : Int) ∣ (period : Int) := by
          refine ⟨((b.step / g : Nat) : Int), ?_⟩
          rw [hpaNat, Int.natCast_mul]
        have hpb : (b.step : Int) ∣ (period : Int) := by
          refine ⟨((a.step / g : Nat) : Int), ?_⟩
          dsimp [period]
          ac_rfl
        have hgapmod' : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 := by
          simpa [g, gap] using hgapmod
        unfold intersection
        simp [han, hbn, has, hbs, hgapmod']
        change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
        have htonatZ : (((period : Int).toNat : Nat) : Int) = (period : Int) :=
          Int.toNat_of_nonneg (by omega : 0 ≤ (period : Int))
        have htonat : (period : Int).toNat = period := by exact_mod_cast htonatZ
        rw [← htonat]
        constructor
        · intro hc
          rcases (span_contains_iff first (period : Int) high v hp).mp hc with
            ⟨hfirstv, hvhigh, hvdvd⟩
          have hxroot : (period : Int) ∣ v - root := by
            have heq : v - root = (v - first) + (first - root) := by omega
            rw [heq]
            exact Int.dvd_add hvdvd hfroot
          have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
          have hxalo0 : (a.step : Int) ∣ (v - root) + (root - a.lo) :=
            Int.dvd_add hxrootA hrootA
          have hxalo : (a.step : Int) ∣ v - a.lo := by
            rw [show (v - root) + (root - a.lo) = v - a.lo by omega] at hxalo0
            exact hxalo0
          have hxrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
          have hxblo0 : (b.step : Int) ∣ (v - root) + (root - b.lo) :=
            Int.dvd_add hxrootB hrootB
          have hxblo : (b.step : Int) ∣ v - b.lo := by
            rw [show (v - root) + (root - b.lo) = v - b.lo by omega] at hxblo0
            exact hxblo0
          have hlowfirst : low ≤ first := by
            dsimp [first]
            have hrem : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            omega
          have hlowv' : low ≤ v := Int.le_trans hlowfirst hfirstv
          have hlowv : a.lo ≤ v := by
            have : max a.lo b.lo ≤ v := by simpa [low] using hlowv'
            omega
          have vhigha : v ≤ lastVal a := by
            have : v ≤ min (lastVal a) (lastVal b) := by simpa [high] using hvhigh
            omega
          have hlowvB : b.lo ≤ v := by
            have : max a.lo b.lo ≤ v := by simpa [low] using hlowv'
            omega
          have vhighB : v ≤ lastVal b := by
            have : v ≤ min (lastVal a) (lastVal b) := by simpa [high] using hvhigh
            omega
          constructor
          · exact mem_of_bounds_dvd a v hanp hasp hlowv vhigha hxalo
          · exact mem_of_bounds_dvd b v hbnp hbsp hlowvB vhighB hxblo
        · intro hc
          apply (span_contains_iff first (period : Int) high v hp).mpr
          rcases hc with ⟨ha, hb⟩
          have habd := contains_bounds a v ha
          have hbbd := contains_bounds b v hb
          have hva := contains_dvd a v ha
          have hvb := contains_dvd b v hb
          have hdA0 : (a.step : Int) ∣ (v - a.lo) - (root - a.lo) :=
            Int.dvd_sub hva hrootA
          have hdA : (a.step : Int) ∣ v - root := by
            rw [show (v - a.lo) - (root - a.lo) = v - root by omega] at hdA0
            exact hdA0
          have hdB0 : (b.step : Int) ∣ (v - b.lo) - (root - b.lo) :=
            Int.dvd_sub hvb hrootB
          have hdB : (b.step : Int) ∣ v - root := by
            rw [show (v - b.lo) - (root - b.lo) = v - root by omega] at hdB0
            exact hdB0
          have hxroot : (period : Int) ∣ v - root := by
            simpa [period] using period_dvd_delta a.step b.step hasp hbsp (v - root) hdA hdB
          have hxfirst0 : (period : Int) ∣ (v - root) - (first - root) :=
            Int.dvd_sub hxroot hfroot
          have hxfirst : (period : Int) ∣ v - first := by
            rw [show (v - root) - (first - root) = v - first by omega] at hxfirst0
            exact hxfirst0
          refine ⟨?_, ?_, hxfirst⟩
          · apply first_le_of_congruence low root first v period hp hfirst
            · show max a.lo b.lo ≤ v
              omega
            · exact hxroot
          · omega
      · have hgapmod' : ¬ (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 := by
          simpa [g, gap] using hgapmod
        unfold intersection
        simp [han, hbn, has, hbs, hgapmod']
        change contains ⟨0, 1, 0⟩ v = true ↔ _
        have hz : contains ⟨0, 1, 0⟩ v = false := by simp [contains]
        rw [hz]
        simp only [Bool.false_eq_true, false_iff]
        rintro ⟨ha, hb⟩
        have hg := common_gap_dvd a b v ha hb
        exact hgapmod (Int.emod_eq_zero_of_dvd hg)

private theorem nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.nodup_cons] at hl ⊢
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, hbeq⟩
        have hab : a = b := h a (by simp) b (by simp [hb]) hbeq.symm
        exact hl.1 (by simpa [hab] using hb)
      · exact ih (fun x hx y hy hxy => h x (by simp [hx]) y (by simp [hy]) hxy) hl.2

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem residue_lt_of_count (N p r q : Nat) (hp : 0 < p) (hr : r < p)
    (hq : q < (N - r + p - 1) / p) : r + q * p < N := by
  by_cases hle : r ≤ N
  · have hmul : q * p < N - r + p - 1 - (p - 1) := (Nat.lt_div_iff_mul_lt hp).mp hq
    have hdiff : N - r + p - 1 - (p - 1) = N - r := by omega
    rw [hdiff] at hmul
    omega
  · have hz : (N - r + p - 1) / p = 0 := residue_count_eq_zero_of_ge N r p (by omega) hp
    omega

/-! ## Enumerating progression intersections -/

private def progList (p : Progression) : List Int := (List.range p.n).map (progVal p)

private theorem mem_progList (p : Progression) (v : Int) :
    v ∈ progList p ↔ contains p v = true := by
  rw [contains_iff_mem]
  unfold progList
  rw [List.mem_map]
  constructor
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, List.mem_range.mp hk, hkv.symm⟩
  · rintro ⟨k, hk, hkv⟩
    exact ⟨k, List.mem_range.mpr hk, hkv.symm⟩

private theorem progList_nodup (p : Progression) (hp : 0 < p.step) : (progList p).Nodup := by
  unfold progList
  apply nodup_map_of_injective _ _ _ List.nodup_range
  intro i _ j _ he
  unfold progVal at he
  have hmul : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hij : (i : Int) = (j : Int) :=
    Int.eq_of_mul_eq_mul_right (by exact_mod_cast (Nat.ne_of_gt hp)) hmul
  exact_mod_cast hij

private theorem intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  by_cases han : a.n = 0
  · simp [intersection, han]
  by_cases hbn : b.n = 0
  · simp [intersection, han, hbn]
  by_cases has : a.step = 0
  · simp [intersection, han, hbn, has]
  by_cases hbs : b.step = 0
  · simp [intersection, han, hbn, has, hbs]
  have hg : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero has)
  have hdiv : 0 < a.step / a.step.gcd b.step :=
    Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero has) (Nat.gcd_dvd_left a.step b.step)) hg
  have hper : 0 < (a.step / a.step.gcd b.step) * b.step := Nat.mul_pos hdiv (Nat.pos_of_ne_zero hbs)
  by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0
  · simp [intersection, han, hbn, has, hbs, hgap, hper]
  · simp [intersection, han, hbn, has, hbs, hgap]

private theorem nodup_filter' {α : Type} (l : List α) (p : α → Bool) (hl : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rcases List.nodup_cons.mp hl with ⟨hx, hxs⟩
      by_cases hp : p x
      · simp [hp]
        exact ⟨hx, ih hxs⟩
      · simp [hp]
        exact ih hxs

private theorem progList_inter (a b : Progression) (ha : 0 < a.step) :
    (progList (intersection a b)).Perm ((progList a).filter (fun v => contains b v)) := by
  apply (List.perm_ext_iff_of_nodup
    (progList_nodup _ (intersection_step_pos a b))
    (nodup_filter' _ _ (progList_nodup a ha))).mpr
  intro v
  rw [mem_progList]
  rw [List.mem_filter]
  rw [mem_progList]
  constructor
  · intro h
    exact (intersection_contains a b v).mp h
  · rintro ⟨hva, hvb⟩
    exact (intersection_contains a b v).mpr ⟨hva, hvb⟩

private theorem filter_map_indices {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem inter_count_indices (a b : Progression) (ha : 0 < a.step) :
    ((List.range a.n).filter (fun i => contains b (progVal a i))).length =
      (intersection a b).n := by
  have hp := progList_inter a b ha
  have hl := hp.length_eq
  unfold progList at hl
  simp only [List.length_map] at hl
  have hfm := filter_map_indices (List.range a.n) (progVal a) (fun v => contains b v)
  have hlen := congrArg List.length hfm
  simp only [List.length_map] at hlen
  rw [hlen] at hl
  simpa using hl.symm

private theorem range_filter_beq (n k : Nat) :
    (List.range n).filter (fun i => i == k) = if k < n then [k] else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      by_cases hk : k = n
      · subst k
        simp
      · have hnk : n ≠ k := by omega
        by_cases hkn : k < n
        · simp [hk, hnk, hkn]
          omega
        · have hgt : n < k := by omega
          simp [hk, hnk, hkn, hgt]
          omega

/-! ## Values of a lane as a finite progression -/

private theorem contains_values_lane (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i < a.n, v = a.base + (i : Int) * a.step := by
  constructor
  · intro h
    rcases (contains_iff_mem (values a) v).mp h with ⟨k, hk, hv⟩
    unfold progVal at hv
    dsimp [values] at hv hk
    by_cases hneg : a.step < 0
    · have hAbs : ((a.step.natAbs : Nat) : Int) = -a.step := by
        have ht := Int.natAbs_of_nonneg (show 0 ≤ -a.step by omega)
        simpa [Int.natAbs_neg] using ht
      simp [hneg, hAbs, show ¬ 0 ≤ a.step by omega] at hv
      refine ⟨a.n - 1 - k, ?_, ?_⟩
      · omega
      · have hcast : ((a.n - 1 - k : Nat) : Int) = (a.n : Int) - 1 - (k : Int) := by omega
        rw [hcast]
        grind
    · have hAbs : ((a.step.natAbs : Nat) : Int) = a.step :=
        Int.natAbs_of_nonneg (by omega)
      simp [hneg, hAbs] at hv
      exact ⟨k, hk, by omega⟩
  · rintro ⟨i, hi, rfl⟩
    rw [contains_iff_mem]
    unfold progMem progVal
    dsimp [values] at hi ⊢
    by_cases hneg : a.step < 0
    · have hAbs : ((a.step.natAbs : Nat) : Int) = -a.step := by
        have ht := Int.natAbs_of_nonneg (show 0 ≤ -a.step by omega)
        simpa [Int.natAbs_neg] using ht
      simp [hneg, hAbs, show ¬ 0 ≤ a.step by omega]
      refine ⟨a.n - 1 - i, ?_, ?_⟩
      · omega
      · have hcast : ((a.n - 1 - i : Nat) : Int) = (a.n : Int) - 1 - (i : Int) := by omega
        rw [hcast]
        grind
    · have hAbs : ((a.step.natAbs : Nat) : Int) = a.step :=
        Int.natAbs_of_nonneg (by omega)
      simp [hneg, hAbs]
      exact ⟨i, hi, rfl⟩

private theorem laneValues_perm (a : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step)).Perm (progList (values a)) := by
  have hnod1 : ((List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step)).Nodup := by
    exact List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Int => x ≠ y)
      (fun i : Nat => a.base + (i : Int) * a.step)
      (fun i j hij he => by
        have hmul : (i : Int) * a.step = (j : Int) * a.step := by omega
        have hji : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right ha hmul
        exact hij (by exact_mod_cast hji)) List.nodup_range
  have hnod2 : (progList (values a)).Nodup := by
    apply progList_nodup
    change 0 < a.step.natAbs
    exact Int.natAbs_pos.mpr ha
  exact (List.perm_ext_iff_of_nodup hnod1 hnod2).mpr (by
    intro v
    rw [mem_progList, contains_values_lane]
    rw [List.mem_map]
    constructor
    · rintro ⟨i, hi, hv⟩
      exact ⟨i, List.mem_range.mp hi, hv.symm⟩
    · rintro ⟨i, hi, hv⟩
      exact ⟨i, List.mem_range.mpr hi, hv.symm⟩)

private theorem lane_count_values (a b : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).filter (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step))).length =
      (intersection (values a) (values b)).n := by
  have hp := laneValues_perm a ha
  have hpf := hp.filter (fun v => contains (values b) v)
  have hlen := hpf.length_eq
  dsimp [progList] at hlen
  have hfm := filter_map_indices (List.range a.n) (fun i : Nat => a.base + (i : Int) * a.step)
    (fun v => contains (values b) v)
  have hlen2 := congrArg List.length hfm
  simp only [List.length_map] at hlen2
  rw [hlen2] at hlen
  have hfm2 := filter_map_indices (List.range (values a).n) (progVal (values a))
    (fun v => contains (values b) v)
  have hlen3 := congrArg List.length hfm2
  simp only [List.length_map] at hlen3
  rw [hlen3] at hlen
  show ((List.range a.n).filter (fun i : Nat => contains (values b) (a.base + (i : Int) * a.step))).length =
    (intersection (values a) (values b)).n
  exact hlen.trans (inter_count_indices (values a) (values b)
    (by dsimp [values]; exact Int.natAbs_pos.mpr ha))

/-! ## Equi-join counts -/

private theorem sum_map_mul_left_nat {α : Type} (l : List α) (c : Nat) (f : α → Nat) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.mul_add]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ : Nat => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_range_const_nat (n v : Nat) :
    ((List.range n).map (fun _ : Nat => v)).sum = n * v := by
  induction n with
  | zero => simp
  | succ n ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp [Nat.succ_mul]

private theorem sum_ite_nat {α : Type} (l : List α) (p : α → Bool) (v : Nat) :
    (l.map (fun x => if p x then v else 0)).sum = v * (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Nat.mul_add] <;> omega

private theorem countP_lane_values (b : Lane) (v : Int) :
    (List.range b.n).countP (fun j : Nat => v == b.base + (j : Int) * b.step) =
      if b.step == 0 then (if v == b.base then b.n else 0)
      else (if contains (values b) v then 1 else 0) := by
  by_cases hb0 : b.step = 0
  · rw [List.countP_eq_length_filter]
    by_cases hvEq : v = b.base
    · have hf : (fun j : Nat => v == b.base + (j : Int) * b.step) = (fun _ : Nat => true) := by
        funext j
        simp [hb0, hvEq]
      rw [hf, List.filter_eq_self.mpr (by intro x hx; rfl)]
      simp [hb0, hvEq]
    · have hf : (fun j : Nat => v == b.base + (j : Int) * b.step) = (fun _ : Nat => false) := by
        funext j
        simp [hb0, hvEq]
      rw [hf]
      simp [hb0, hvEq]
  · have hb0f : (b.step == 0) = false := by
      cases hb : b.step == 0 with
      | false => rfl
      | true =>
          exfalso
          apply hb0
          exact of_decide_eq_true hb
    simp [hb0f, if_false]
    by_cases hc : contains (values b) v
    · rcases (contains_values_lane b v).mp hc with ⟨j0, hj0, hvj0⟩
      have huniq : ∀ j ∈ List.range b.n,
          (v == b.base + (j : Int) * b.step) = (j == j0) := by
        intro j hj
        have hjlt : j < b.n := List.mem_range.mp hj
        by_cases heq : j = j0
        · subst j
          simp [hvj0]
        · have hne1 : v ≠ b.base + (j : Int) * b.step := by
            intro he
            have hmul : (j : Int) * b.step = (j0 : Int) * b.step := by omega
            have hji : (j : Int) = (j0 : Int) := Int.eq_of_mul_eq_mul_right hb0 hmul
            exact heq (by exact_mod_cast hji)
          rw [beq_eq_false_iff_ne.mpr hne1, beq_eq_false_iff_ne.mpr heq]
      rw [List.countP_eq_length_filter]
      rw [show (List.range b.n).filter (fun j : Nat => v == b.base + (j : Int) * b.step) =
          (List.range b.n).filter (fun j : Nat => j == j0) by
        apply filter_congr_bool
        intro j hj
        exact huniq j hj]
      rw [range_filter_beq]
      simp [hc, hj0]
    · rw [List.countP_eq_length_filter]
      rw [show (List.range b.n).filter (fun j : Nat => v == b.base + (j : Int) * b.step) = [] by
        apply List.filter_eq_nil_iff.mpr
        intro j hj he
        have heq : v = b.base + (j : Int) * b.step := of_decide_eq_true he
        exact hc ((contains_values_lane b v).mpr ⟨j, List.mem_range.mp hj, heq⟩)]
      simp [hc]

private theorem intersection_const_left_n (a b : Progression) (ha : a.step = 0)
    (hanp : 0 < a.n) :
    (intersection a b).n = if contains b a.lo = true then 1 else 0 := by
  by_cases hbn : b.n = 0
  · have hc : contains b a.lo = false := by
      cases h' : contains b a.lo
      · rfl
      · obtain ⟨k, hk, _⟩ := (contains_iff_mem b a.lo).mp h'
        omega
    simp [intersection, hbn, hc]
  · unfold intersection
    simp [Nat.ne_of_gt hanp, hbn, ha]

private theorem lane_indicator_sum (a b : Lane) :
    ((List.range a.n).map (fun i : Nat =>
      if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum =
      (intersection (values a) (values b)).n * (if a.step = 0 then a.n else 1) := by
  by_cases ha0 : a.step = 0
  · by_cases han : a.n = 0
    · simp [han, ha0]
    have hanp : 0 < a.n := Nat.pos_of_ne_zero han
    have hvconst : ∀ i : Nat, a.base + (i : Int) * a.step = a.base := by
      intro i
      simp [ha0]
    rw [show (List.range a.n).map (fun i : Nat =>
        if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) =
        (List.range a.n).map (fun _ : Nat =>
          if contains (values b) a.base then 1 else 0) by
      apply List.map_congr_left
      intro i hi
      rw [hvconst i]]
    rw [sum_range_const_nat]
    have hIn := intersection_const_left_n (values a) (values b)
      (by dsimp [values]; simp [ha0]) (by dsimp [values]; exact hanp)
    have hlo : (values a).lo = a.base := by dsimp [values]; simp [ha0]
    rw [hIn, hlo]
    simp [ha0, Nat.mul_comm]
  · rw [sum_ite_nat]
    rw [lane_count_values a b ha0]
    simp [ha0]


private theorem lane_residue (a b : Lane) :
    ((List.range a.n).map (fun i : Nat =>
      (List.range b.n).countP (fun j : Nat =>
        a.base + (i : Int) * a.step == b.base + (j : Int) * b.step))).sum = joinCount a b := by
  by_cases hbn0 : b.n = 0
  · simp [hbn0, joinCount, intersection, values, sum_map_zero_nat]
  have hind := lane_indicator_sum a b
  by_cases hb0 : b.step = 0
  · have hmult : ∀ v : Int,
        (List.range b.n).countP (fun j : Nat => v == b.base + (j : Int) * b.step) =
          b.n * (if contains (values b) v then 1 else 0) := by
      intro v
      rw [countP_lane_values]
      simp [hb0]
      by_cases hc : contains (values b) v
      · have hval : v = b.base :=
          (contains_const_iff b.base b.n v).mp (by simpa [values, hb0] using hc) |>.2
        have hcb : contains (values b) b.base = true := by simpa [hval] using hc
        simp [hc, hval, hcb]
      · have hval : v ≠ b.base := by
          intro he
          apply hc
          have hc' : contains ⟨b.base, 0, b.n⟩ v = true :=
            (contains_const_iff b.base b.n v).mpr ⟨Nat.pos_of_ne_zero hbn0, he⟩
          simpa [values, hb0] using hc'
        simp [hc, hval]
    rw [show (List.range a.n).map (fun i : Nat =>
        (List.range b.n).countP (fun j : Nat =>
          a.base + (i : Int) * a.step == b.base + (j : Int) * b.step)) =
        (List.range a.n).map (fun i : Nat =>
          b.n * (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)) by
      apply List.map_congr_left
      intro i hi
      exact hmult _]
    rw [sum_map_mul_left_nat]
    rw [hind]
    unfold joinCount
    simp [hb0, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  · rw [show (List.range a.n).map (fun i : Nat =>
        (List.range b.n).countP (fun j : Nat =>
          a.base + (i : Int) * a.step == b.base + (j : Int) * b.step)) =
        (List.range a.n).map (fun i : Nat =>
          if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) by
      apply List.map_congr_left
      intro i hi
      rw [countP_lane_values]
      simp [hb0]]
    rw [hind]
    unfold joinCount
    simp [hb0]

/-! ## Rows as a permutation of striped lanes -/

private theorem nodup_pairs_range (n : Nat) (f : Nat → Int) :
    ((List.range n).map (fun i => (i, f i))).Nodup := by
  apply List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Nat × Int => x ≠ y)
    (fun i : Nat => (i, f i))
    (fun i j hij he => hij (congrArg Prod.fst he)) List.nodup_range

private theorem pairwise_range_rel (p : Nat) (R : Nat → Nat → Prop)
    (h : ∀ a < p, ∀ b < p, a ≠ b → R a b) : (List.range p).Pairwise R := by
  induction p with
  | zero => simp
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.pairwise_append]
      refine ⟨ih (fun a ha b hb hab => h a (by omega) b (by omega) hab), by simp, ?_⟩
      intro a ha
      have ha' : a < p := List.mem_range.mp ha
      intro b hb
      have hb' : b = p := by simpa using hb
      subst b
      exact h a (by omega) p (by omega) (by omega)

private theorem stripedLanes_nodup (c : Config) (p : Nat) (hp : 0 < p) (make : Nat → Int × Int) :
    ((striped c p make).flatMap lanePoints).Nodup := by
  unfold striped
  rw [List.flatMap_map]
  change List.Pairwise (fun x y : Nat × Int => x ≠ y)
    ((List.range (min p c.n)).flatMap (fun r => lanePoints (match make r with | (z, d) => lane c.n r p z d)))
  rw [List.pairwise_flatMap]
  constructor
  · intro r hr
    unfold lanePoints lane
    dsimp
    apply nodup_map_of_injective _ _ _ List.nodup_range
    intro i _ j _ he
    have hij : r + i * p = r + j * p := congrArg Prod.fst he
    exact Nat.eq_of_mul_eq_mul_left hp (by simpa [Nat.mul_comm] using Nat.add_left_cancel hij)
  · apply pairwise_range_rel (min p c.n)
    intro a ha b hb hab x hx y hy he
    have ha' : a < p := by omega
    have hb' : b < p := by omega
    cases hma : make a
    cases hmb : make b
    have hpos : x.1 = y.1 := congrArg Prod.fst he
    rcases List.mem_map.mp hx with ⟨qa, hqa, hxeq⟩
    rcases List.mem_map.mp hy with ⟨qb, hqb, hyeq⟩
    have hamod : x.1 % p = a := by
      have hx1 : x.1 = a + qa * p := by rw [← hxeq]; rfl
      rw [hx1, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ha']
    have hbmod : y.1 % p = b := by
      have hy1 : y.1 = b + qb * p := by rw [← hyeq]; rfl
      rw [hy1, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hb']
    exact hab (by rw [← hamod, ← hbmod, hpos])

private theorem ceilDiv_eq (d s : Nat) (hs : 0 < s) : d + s - 1 - (s - 1) = d := by omega

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start := by omega
    rw [hrewrite]
    omega

private theorem stripedRowsPerm (c : Config) (p : Nat) (hp : 0 < p)
    (make : Nat → Int × Int) (f : Nat → Int)
    (hval : ∀ i : Nat, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int)) :
    ((List.range c.n).map (fun i : Nat => (i, f i))).Perm ((striped c p make).flatMap lanePoints) := by
  have hd1 : ((List.range c.n).map (fun i : Nat => (i, f i))).Nodup := nodup_pairs_range c.n f
  have hd2 : ((striped c p make).flatMap lanePoints).Nodup := stripedLanes_nodup c p hp make
  exact (List.perm_ext_iff_of_nodup hd1 hd2).mpr (by
    intro z
    constructor
    · intro hz
      rcases List.mem_map.mp hz with ⟨i, hi, rfl⟩
      have hin : i < c.n := List.mem_range.mp hi
      have hrK : i % p < min p c.n := by
        have hrp : i % p < p := Nat.mod_lt i hp
        have hrn : i % p < c.n := by
          have hd := Nat.div_add_mod i p
          omega
        exact Nat.lt_min.mpr ⟨hrp, hrn⟩
      have hq : i / p < (c.n - i % p + p - 1) / p := by
        have hdecomp : i % p + (i / p) * p = i := by
          have hd := Nat.div_add_mod i p
          rw [Nat.mul_comm p (i / p)] at hd
          omega
        exact (pos_lt_iff (i % p) p (i / p) c.n hp).mp (by rw [hdecomp]; exact hin)
      unfold striped
      apply List.mem_flatMap.mpr
      refine ⟨lane c.n (i % p) p (make (i % p)).1 (make (i % p)).2, ?_⟩
      constructor
      · apply List.mem_map.mpr
        refine ⟨i % p, List.mem_range.mpr hrK, ?_⟩
        rfl
      · apply List.mem_map.mpr
        refine ⟨i / p, List.mem_range.mpr hq, ?_⟩
        have hdecomp : i % p + (i / p) * p = i := by
          have hd := Nat.div_add_mod i p
          rw [Nat.mul_comm p (i / p)] at hd
          omega
        dsimp [lanePoints, lane]
        simpa [hdecomp, hval, Int.mul_comm]
    · intro hz
      rcases List.mem_flatMap.mp hz with ⟨la, hla, hz⟩
      rcases List.mem_map.mp hla with ⟨r, hr, rfl⟩
      rcases List.mem_map.mp hz with ⟨q, hq, hz⟩
      have hrp : r < p := by have := List.mem_range.mp hr; omega
      have hrn : r + q * p < c.n := residue_lt_of_count c.n p r q hp hrp (List.mem_range.mp hq)
      apply List.mem_map.mpr
      refine ⟨r + q * p, List.mem_range.mpr hrn, ?_⟩
      have hmod : (r + q * p) % p = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrp]
      have hdiv : (r + q * p) / p = q := by
        rw [Nat.add_mul_div_right r q hp, Nat.div_eq_of_lt hrp]
        omega
      rw [hval, hmod, hdiv]
      dsimp [lanePoints, lane] at hz
      subst z
      cases hmk : make r <;> simp [hmk, Int.mul_comm])

private theorem flatMap_residue (ls : List Lane) (b : Lane) :
    ((ls.flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun j : Nat => r.2 == b.base + (j : Int) * b.step))).sum =
      (ls.map (joinCount · b)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      rw [ih]
      congr 1
      unfold lanePoints
      rw [List.map_map]
      simpa [Function.comp_def, Int.mul_comm] using lane_residue a b

/-! ### TriangleWave -/

namespace TriangleWave

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step * (min (i % (2 * width c)) (2 * width c - i % (2 * width c)) : Nat))) := by
  unfold rows
  simp [layout]

private theorem rows_lanes (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  let w := width c
  let p := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hp : 0 < p := by dsimp [p]; omega
  let make : Nat → Int × Int := fun r => (c.base + c.step * (min r (2 * w - r) : Nat), 0)
  let f : Nat → Int := fun i => c.base + c.step * (min (i % p) (p - i % p) : Nat)
  have hval : ∀ i : Nat, f i = (make (i % p)).1 + (make (i % p)).2 * ((i / p : Nat) : Int) := by
    intro i
    simp [f, make, p]
  have hrows : rows layout c = (List.range c.n).map (fun i : Nat => (i, f i)) := by
    simpa [f, p, w] using rows_eq c
  rw [hrows]
  have hlanes : layout.lanes c = striped c p make := by
    dsimp [layout, striped, make, p, w]
  rw [hlanes]
  exact stripedRowsPerm c p hp make f hval

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, 0 < a.stride := by
  intro a ha
  simp [layout, striped] at ha ⊢
  obtain ⟨r, hr, rfl⟩ := ha
  have hr' : r < 2 * width c := by omega
  dsimp [lane]
  omega

theorem TriangleWave_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.ResidueJoin.Goal := by
  intro c b
  have hp := (rows_lanes c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))
  have hs : ((rows layout c).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
      (((layout.lanes c).flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum :=
    list_sum_perm_nat hp
  rw [hs]
  simpa [Int.mul_comm] using flatMap_residue (layout.lanes c) b

end TriangleWave

/-! ### LaneMajor -/

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step * ((i / width c : Nat) : Int) + c.bias * ((i % width c : Nat) : Int))) := by
  unfold rows
  simp [layout]

private theorem rows_lanes (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  have hw : 0 < width c := by dsimp [width]; omega
  let make : Nat → Int × Int := fun r => (c.base + c.bias * (r : Int), c.step)
  let f : Nat → Int := fun i => c.base + c.step * ((i / width c : Nat) : Int) + c.bias * ((i % width c : Nat) : Int)
  have hval : ∀ i : Nat, f i = (make (i % width c)).1 +
      (make (i % width c)).2 * ((i / width c : Nat) : Int) := by
    intro i
    simp [f, make]
    omega
  rw [rows_eq]
  have hlanes : layout.lanes c = striped c (width c) make := by
    dsimp [layout, striped, make]
  rw [hlanes]
  exact stripedRowsPerm c (width c) hw make f hval

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, 0 < a.stride := by
  intro a ha
  simp [layout, striped] at ha ⊢
  obtain ⟨r, hr, rfl⟩ := ha
  have hr' : r < min (width c) c.n := by simpa using hr
  dsimp [lane, width]
  omega

theorem LaneMajor_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor.ResidueJoin.Goal := by
  intro c b
  have hp := (rows_lanes c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))
  have hs : ((rows layout c).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
      (((layout.lanes c).flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum :=
    list_sum_perm_nat hp
  rw [hs]
  simpa [Int.mul_comm] using flatMap_residue (layout.lanes c) b

end LaneMajor

/-! ### Boustrophedon -/

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

private theorem rows_lanes (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  let w := width c
  let per := 2 * w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hper : 0 < per := by dsimp [per]; omega
  let make : Nat → Int × Int := fun r =>
    (c.base + c.step * (if r < w then r else 2 * w - 1 - (r - w) : Nat), c.step * per)
  let f : Nat → Int := fun i =>
    let k := i / w
    let r := i % w
    c.base + c.step * (k * w + (if k % 2 == 0 then r else w - 1 - r) : Nat)
  have hval : ∀ i : Nat, f i = (make (i % per)).1 + (make (i % per)).2 * ((i / per : Nat) : Int) := by
    intro i
    let s := i % per
    let q := i / per
    have hs_lt : s < per := by dsimp [s]; exact Nat.mod_lt i hper
    have hdecomp0 : i = per * q + s := by
      dsimp [s, q]
      exact (Nat.div_add_mod i per).symm
    have hmul : per * q = (2 * q) * w := by
      dsimp [per]
      simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    have hdecomp : i = s + (2 * q) * w := by omega
    by_cases hsw : s < w
    · have hdivw : i / w = 2 * q := by
        rw [hdecomp, Nat.add_mul_div_right s (2 * q) hw]
        have hsw0 : s / w = 0 := Nat.div_eq_of_lt hsw
        rw [hsw0]
        omega
      have hmodw : i % w = s := by
        rw [hdecomp, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hsw]
      have hpar : (i / w) % 2 = 0 := by
        rw [hdivw]
        exact Nat.mul_mod_right 2 q
      have hgoal : f i = (make s).1 + (make s).2 * (q : Int) := by
        have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
            (2 * q) * w + s := by
          rw [hdivw, hmodw]
          simp [hpar]
        change c.base + c.step * ((i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w)) : Nat)) =
          (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
        rw [hinside]
        have hmodper : (i % per) = s := rfl
        rw [hmodper]
        have hdivper : (i / per) = q := rfl
        rw [Int.ofNat_ediv_ofNat]
        rw [hdivper]
        dsimp [make]
        rw [if_pos hsw]
        dsimp [per]
        simp [Int.natCast_add, Int.mul_add, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
        omega
      simpa [s, q] using hgoal
    · have hswge : w ≤ s := by omega
      let t := s - w
      have htlt : t < w := by dsimp [t]; dsimp [per] at hs_lt; omega
      have hs_eq : s = w + t := by dsimp [t]; omega
      have hi : i = t + (2 * q + 1) * w := by
        rw [hdecomp, hs_eq]
        simp [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      have hdivw : i / w = 2 * q + 1 := by
        rw [hi, Nat.add_mul_div_right t (2 * q + 1) hw]
        have htw0 : t / w = 0 := Nat.div_eq_of_lt htlt
        rw [htw0]
        omega
      have hmodw : i % w = t := by
        rw [hi, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt htlt]
      have hpar : (i / w) % 2 = 1 := by
        rw [hdivw]
        omega
      have hgoal : f i = (make s).1 + (make s).2 * (q : Int) := by
        have hinside : (i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w) : Nat)) =
            (2 * q + 1) * w + (w - 1 - t) := by
          rw [hdivw, hmodw]
          simp [hpar]
        change c.base + c.step * ((i / w * w + (if (i / w) % 2 == 0 then i % w else w - 1 - (i % w)) : Nat)) =
          (make (i % per)).1 + (make (i % per)).2 * (i / per : Int)
        rw [hinside]
        have hmodper : (i % per) = s := rfl
        rw [hmodper]
        have hdivper : (i / per) = q := rfl
        rw [Int.ofNat_ediv_ofNat]
        rw [hdivper]
        dsimp [make]
        rw [if_neg hsw]
        rw [hs_eq]
        have hsub : 2 * w - 1 - (w + t - w) = w + (w - 1 - t) := by omega
        rw [hsub]
        dsimp [per]
        simp [Int.natCast_add, Int.mul_add, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
        omega
      simpa [s, q] using hgoal
  have hrows : rows layout c = (List.range c.n).map (fun i : Nat => (i, f i)) := by
    unfold rows
    apply range_filterMap_all
    intro i hi
    simp [layout, f, w]
  rw [hrows]
  have hlanes : layout.lanes c = striped c per make := by
    dsimp [layout, striped, make, per, w]
  rw [hlanes]
  exact stripedRowsPerm c per hper make f hval

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, 0 < a.stride := by
  intro a ha
  simp [layout, striped] at ha ⊢
  obtain ⟨r, hr, rfl⟩ := ha
  have hr' : r < 2 * width c := by omega
  dsimp [lane]
  omega

theorem Boustrophedon_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon.ResidueJoin.Goal := by
  intro c b
  have hp := (rows_lanes c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))
  have hs : ((rows layout c).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
      (((layout.lanes c).flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum :=
    list_sum_perm_nat hp
  rw [hs]
  simpa [Int.mul_comm] using flatMap_residue (layout.lanes c) b

end Boustrophedon

/-! ## Deleting positions in an arithmetic progression -/

private theorem sum_filter_compl {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem positions_mem (a : Lane) (i : Nat) (hi : i < a.n) :
    contains (positions a) (a.start + i * a.stride) = true := by
  rw [contains_iff_mem]
  unfold progMem progVal positions
  exact ⟨i, hi, rfl⟩

private theorem inter_count_positions (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).length =
      (intersection (positions a) m).n := by
  have h := inter_count_indices (positions a) m (by dsimp [positions]; exact ha)
  simpa [positions, progVal, Int.mul_comm] using h

private theorem inter_step_dvd (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (hm : 0 < m.step) (hpos : 0 < (intersection (positions a) m).n) :
    (intersection (positions a) m).step % a.stride = 0 := by
  let g := a.stride.gcd m.step
  let per := (a.stride / g) * m.step
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left m.step ha
  have hdivpos : 0 < a.stride / g :=
    Nat.div_pos (Nat.le_of_dvd ha (Nat.gcd_dvd_left a.stride m.step)) hgpos
  have hperpos : 0 < per := Nat.mul_pos hdivpos hm
  have hga : g ∣ a.stride := Nat.gcd_dvd_left a.stride m.step
  have hgm : g ∣ m.step := Nat.gcd_dvd_right a.stride m.step
  have hmul1 : per * g = a.stride * m.step := by
    dsimp [per]
    calc
      ((a.stride / g) * m.step) * g = (a.stride / g) * g * m.step := by ac_rfl
      _ = a.stride * m.step := by rw [Nat.div_mul_cancel hga]
  have hmul2 : (a.stride * (m.step / g)) * g = a.stride * m.step := by
    calc
      (a.stride * (m.step / g)) * g = a.stride * ((m.step / g) * g) := by ac_rfl
      _ = a.stride * m.step := by rw [Nat.div_mul_cancel hgm]
  have hper : per = a.stride * (m.step / g) :=
    Nat.mul_right_cancel hgpos (hmul1.trans hmul2.symm)
  have han : a.n ≠ 0 := by
    intro h
    have hz : (intersection (positions a) m).n = 0 := by simp [intersection, positions, h]
    omega
  have hmn : m.n ≠ 0 := by
    intro h
    have hz : (intersection (positions a) m).n = 0 := by simp [intersection, h]
    omega
  by_cases hgap : (m.lo - a.start) % (g : Int) = 0
  · dsimp [g] at hgap ⊢
    have hI : (intersection (positions a) m).step = per := by
      unfold intersection
      simp [positions, han, hmn, Nat.ne_of_gt ha, Nat.ne_of_gt hm, hgap]
      dsimp [per, g]
    rw [hI, hper]
    exact Nat.mod_eq_zero_of_dvd ⟨m.step / g, rfl⟩
  · dsimp [g] at hgap ⊢
    have hI : (intersection (positions a) m).n = 0 := by
      unfold intersection
      simp [positions, han, hmn, Nat.ne_of_gt ha, Nat.ne_of_gt hm, hgap]
    omega

private theorem inter_lo_index (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (hpos : 0 < (intersection (positions a) m).n) :
    ∃ i < a.n, (intersection (positions a) m).lo = a.start + i * a.stride := by
  let I := intersection (positions a) m
  have hmemI : contains I I.lo = true := by
    rw [contains_iff_mem]
    unfold progMem progVal
    exact ⟨0, hpos, by simp⟩
  have hmemP : contains (positions a) I.lo = true := ((intersection_contains (positions a) m I.lo).mp hmemI).1
  rw [contains_iff_mem] at hmemP
  unfold progMem progVal positions at hmemP
  rcases hmemP with ⟨i, hi, hv⟩
  exact ⟨i, hi, hv⟩

private theorem common_pos_value (a : Lane) (ha : 0 < a.stride) (q : Progression)
    (hmem : ∀ v : Int, contains q v = true → contains (positions a) v = true)
    (hdvd : q.step % a.stride = 0) (hqn : 0 < q.n) (k : Nat) (hk : k < q.n) :
    a.base + ((progVal q k - a.start) / (a.stride : Int)) * a.step =
      firstAt a q + (k : Int) * slopeAt a q := by
  have hmemQlo : contains q q.lo = true := by
    rw [contains_iff_mem]
    unfold progMem progVal
    exact ⟨0, hqn, by simp⟩
  have hmemP : contains (positions a) q.lo = true := hmem q.lo hmemQlo
  rw [contains_iff_mem] at hmemP
  unfold progMem progVal positions at hmemP
  rcases hmemP with ⟨i0, hi0, hlo⟩
  have hstep : q.step = (q.step / a.stride) * a.stride :=
    (Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hdvd)).symm
  have hqk : progVal q k = q.lo + (k : Int) * (q.step : Int) := rfl
  have hcastlo : (q.lo - a.start : Int) = (i0 : Int) * (a.stride : Int) := by grind
  have hn : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt ha)
  have hdivlo : (q.lo - a.start) / (a.stride : Int) = (i0 : Int) := by
    rw [hcastlo, Int.mul_comm (i0 : Int) (a.stride : Int)]
    exact Int.mul_ediv_cancel_left _ hn
  have hcaststep : (q.step : Int) = ((q.step / a.stride : Nat) : Int) * (a.stride : Int) := by
    exact_mod_cast hstep
  have hval : progVal q k - a.start =
      ((i0 : Int) + (k : Int) * (q.step / a.stride : Int)) * (a.stride : Int) := by
    rw [hqk]
    calc
      q.lo + (k : Int) * (q.step : Int) - a.start =
          (q.lo - a.start) + (k : Int) * (q.step : Int) := by grind
      _ = (i0 : Int) * (a.stride : Int) +
          (k : Int) * (((q.step / a.stride : Nat) : Int) * (a.stride : Int)) := by
            rw [hcastlo, hcaststep]
      _ = ((i0 : Int) + (k : Int) * (q.step / a.stride : Int)) * (a.stride : Int) := by grind
  have hdiv : (progVal q k - a.start) / (a.stride : Int) =
      (i0 : Int) + (k : Int) * (q.step / a.stride : Int) := by
    rw [hval]
    rw [Int.mul_comm (((i0 : Int) + (k : Int) * (q.step / a.stride : Int))) (a.stride : Int)]
    exact Int.mul_ediv_cancel_left _ hn
  dsimp [firstAt, slopeAt]
  rw [hdivlo, hdiv]
  rw [hstep]
  push_cast
  grind


private theorem sum_filter_const_int {α : Type} (l : List α) (p : α → Bool) (v : Int) :
    ((l.filter p).map (fun _ => v)).sum = ((l.filter p).length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih, Int.natCast_add, Int.add_mul] <;> omega

private theorem sum_affine (n : Nat) (x d : Int) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * d)).sum = (n : Int) * x + tri n * d := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [tri_succ]
      push_cast
      grind

private theorem nodup_of_length_le_one {α : Type} (l : List α) (h : l.length ≤ 1) : l.Nodup := by
  cases l with
  | nil => simp
  | cons x xs =>
      cases xs with
      | nil => simp
      | cons y ys => simp at h

private theorem deleted_at_most_one (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (hm : m.step = 0) : (intersection (positions a) m).n ≤ 1 := by
  unfold intersection
  by_cases han : a.n = 0
  · simp [positions, han]
  by_cases hmn : m.n = 0
  · simp [positions, hmn]
  by_cases hcs : contains (positions a) m.lo
  · by_cases hc2 : contains (⟨a.start, a.stride, a.n⟩ : Progression) m.lo <;>
      simp [positions, han, hmn, Nat.ne_of_gt ha, hm, hcs, hc2]
  · by_cases hc2 : contains (⟨a.start, a.stride, a.n⟩ : Progression) m.lo <;>
      simp [positions, han, hmn, Nat.ne_of_gt ha, hm, hcs, hc2]

private theorem lane_delete (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((((List.range a.n).map (fun i : Nat =>
      (a.start + i * a.stride, a.base + (i : Int) * a.step))).filter
      (fun r => !contains m r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a m := by
  let I := intersection (positions a) m
  let L := (List.range a.n).map (fun i : Nat =>
      (a.start + i * a.stride, a.base + (i : Int) * a.step))
  have hcnt := inter_count_positions a ha m
  have htotal : ((L.map Prod.snd).sum) = partialSum a a.n := by
    dsimp [L]
    rw [List.map_map]
    simpa [Function.comp_def, Int.mul_comm] using (partialSum_eq_sum a a.n).symm
  have hsplit := sum_filter_compl L (fun r : Nat × Int => contains m r.1) Prod.snd
  rw [htotal] at hsplit
  have hkept : ((L.filter (fun r : Nat × Int => !contains m r.1)).map Prod.snd).sum =
      partialSum a a.n - ((L.filter (fun r : Nat × Int => contains m r.1)).map Prod.snd).sum := by
    omega
  dsimp [L] at hkept ⊢
  rw [hkept]
  congr 1
  have hfilt : ((List.range a.n).map (fun i : Nat =>
      (a.start + i * a.stride, a.base + (i : Int) * a.step))).filter
      (fun r : Nat × Int => contains m r.1) =
      ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
      (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step)) := by
    exact filter_map_indices (List.range a.n) (fun i : Nat =>
      (a.start + i * a.stride, a.base + (i : Int) * a.step))
      (fun r : Nat × Int => contains m r.1)
  rw [hfilt, List.map_map]
  change (((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
    (fun i : Nat => a.base + (i : Int) * a.step)).sum = deletedSum a m
  by_cases ha0 : a.step = 0
  · have hvconst : ∀ i : Nat, a.base + (i : Int) * a.step = a.base := by
      intro i
      simp [ha0]
    rw [show ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
        (fun i : Nat => a.base + (i : Int) * a.step) =
        ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
        (fun _ : Nat => a.base) by
      apply List.map_congr_left
      intro i hi
      rw [hvconst i]]
    rw [sum_filter_const_int, hcnt]
    dsimp [I, deletedSum, firstAt, slopeAt]
    simp [ha0]
  by_cases hIn0 : I.n = 0
  · have hlen : ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).length = 0 := by
      rw [hcnt]
      exact hIn0
    have hnil : (List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride)) = [] := by
      cases hf : (List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride)) with
      | nil => rfl
      | cons x xs => simp [hf] at hlen
    rw [hnil]
    simp [I, deletedSum, hIn0, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  have hInpos : 0 < I.n := Nat.pos_of_ne_zero hIn0
  have hmemI : ∀ v : Int, contains I v = true → contains (positions a) v = true := by
    intro v hv
    exact ((intersection_contains (positions a) m v).mp hv).1
  by_cases hm0 : m.step = 0
  · have hIn1 : I.n = 1 := Nat.le_antisymm (deleted_at_most_one a ha m hm0) hInpos
    have hlen : ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).length = 1 := by
      rw [hcnt, hIn1]
    have hnonnil : (List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride)) ≠ [] := by
      intro h
      rw [h] at hlen
      simp at hlen
    obtain ⟨i, tl, hlist⟩ := List.exists_cons_of_ne_nil hnonnil
    have htl : tl = [] := by
      have htmp : (i :: tl).length = 1 := by rw [← hlist]; exact hlen
      cases tl with
      | nil => rfl
      | cons y ys => simp at htmp
    subst tl
    rw [hlist]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    have hiMemL : i ∈ (List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride)) := by
      rw [hlist]
      simp
    have hi_mem : contains m (a.start + i * a.stride) = true := (List.mem_filter.mp hiMemL).2
    have hiI : contains I (a.start + i * a.stride) = true :=
      (intersection_contains (positions a) m (a.start + i * a.stride)).mpr
        ⟨positions_mem a i (List.mem_range.mp (List.mem_filter.mp hiMemL).1), hi_mem⟩
    have hIlo : I.lo = a.start + i * a.stride := by
      have hmI := (contains_iff_mem I (a.start + i * a.stride)).mp hiI
      rcases hmI with ⟨k, hk, hvk⟩
      have hk0 : k = 0 := by omega
      simpa [hk0, progVal] using hvk.symm
    have hfi : firstAt a I = a.base + (i : Int) * a.step := by
      dsimp [firstAt]
      rw [hIlo]
      have hdiv : (a.start + i * a.stride - a.start) / (a.stride : Int) = (i : Int) := by
        have hc : (a.start + i * a.stride - a.start : Int) = (i : Int) * (a.stride : Int) := by omega
        rw [hc, Int.mul_comm (i : Int) (a.stride : Int)]
        exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
      rw [hdiv]
    dsimp [I, deletedSum] at hIn1 ⊢
    simp [hIn1, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
    change a.base + (i : Int) * a.step = firstAt a I
    exact hfi.symm
  · have hdvd := inter_step_dvd a ha m (Nat.pos_of_ne_zero hm0) hInpos
    let R := (List.range I.n).map (fun k : Nat => firstAt a I + (k : Int) * slopeAt a I)
    have hidxNodup : ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).Nodup :=
      nodup_filter' (List.range a.n) (fun i : Nat => contains m (a.start + i * a.stride)) List.nodup_range
    have hnodL : (((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
        (fun i : Nat => a.base + (i : Int) * a.step)).Nodup := by
      exact List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Int => x ≠ y)
        (fun i : Nat => a.base + (i : Int) * a.step)
        (fun i j hij he => by
          have hmul : (i : Int) * a.step = (j : Int) * a.step := by omega
          have hji : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right ha0 hmul
          exact hij (by exact_mod_cast hji)) hidxNodup
    have hnodR : R.Nodup := by
      dsimp [R]
      by_cases hsmall : I.n ≤ 1
      · apply nodup_of_length_le_one
        simpa [R] using hsmall
      · have hIn2 : 2 ≤ I.n := by omega
        have hIsteppos : 0 < I.step := by
          by_cases hs : I.step = 0
          · exfalso
            have hle0 : I.n = 0 := by
              dsimp [I] at hs ⊢
              unfold intersection at hs ⊢
              by_cases han : a.n = 0
              · simp [positions, han]
              by_cases hmn : m.n = 0
              · simp [hmn]
              have hgpos : 0 < a.stride.gcd m.step := Nat.gcd_pos_of_pos_left m.step ha
              have hdivpos : 0 < a.stride / a.stride.gcd m.step :=
                Nat.div_pos (Nat.le_of_dvd ha (Nat.gcd_dvd_left a.stride m.step)) hgpos
              have hperpos : 0 < (a.stride / a.stride.gcd m.step) * m.step :=
                Nat.mul_pos hdivpos (Nat.pos_of_ne_zero hm0)
              by_cases hgap : (m.lo - a.start) % (a.stride.gcd m.step : Int) = 0
              · simp [positions, han, hmn, Nat.ne_of_gt ha, hm0, hgap] at hs
                omega
              · simp [positions, han, hmn, Nat.ne_of_gt ha, hm0, hgap]
            exact hsmall (by omega : I.n ≤ 1)
          · exact Nat.pos_of_ne_zero hs
        have htpos : 0 < I.step / a.stride := by
          have hle : a.stride ≤ I.step := by
            have hdvd' : a.stride ∣ I.step := Nat.dvd_of_mod_eq_zero hdvd
            obtain ⟨t, ht⟩ := hdvd'
            have ht0 : t ≠ 0 := by
              intro h
              subst t
              omega
            rw [ht]
            exact Nat.le_mul_of_pos_right a.stride (Nat.pos_of_ne_zero ht0)
          exact Nat.div_pos hle ha
        have hslope : slopeAt a I ≠ 0 := by
          dsimp [slopeAt]
          exact Int.mul_ne_zero (by exact_mod_cast (Nat.ne_of_gt htpos)) ha0
        exact List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Int => x ≠ y)
          (fun k : Nat => firstAt a I + (k : Int) * slopeAt a I)
          (fun i j hij he => by
            have hmul : (i : Int) * slopeAt a I = (j : Int) * slopeAt a I := by omega
            have hji : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right hslope hmul
            exact hij (by exact_mod_cast hji)) List.nodup_range
    have hperm : ((List.range a.n).filter (fun i : Nat => contains m (a.start + i * a.stride))).map
        (fun i : Nat => a.base + (i : Int) * a.step) |>.Perm R := by
      apply (List.perm_ext_iff_of_nodup hnodL hnodR).mpr
      intro v
      dsimp [R]
      constructor
      · intro hv
        rcases List.mem_map.mp hv with ⟨i, hiF, hvEq⟩
        rcases List.mem_filter.mp hiF with ⟨hi, hmpos⟩
        have hiI : contains I (a.start + i * a.stride) = true :=
          (intersection_contains (positions a) m (a.start + i * a.stride)).mpr
            ⟨positions_mem a i (List.mem_range.mp hi), hmpos⟩
        rcases (contains_iff_mem I (a.start + i * a.stride)).mp hiI with ⟨k, hk, hkv⟩
        have hdivI : (progVal I k - a.start) / (a.stride : Int) = (i : Int) := by
          rw [← hkv]
          have hc : (a.start + i * a.stride - a.start : Int) = (i : Int) * (a.stride : Int) := by omega
          rw [hc, Int.mul_comm (i : Int) (a.stride : Int)]
          exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
        have hcp := common_pos_value a ha I hmemI hdvd hInpos k hk
        rw [hdivI] at hcp
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        rw [← hvEq]
        exact hcp.symm
      · intro hv
        rcases List.mem_map.mp hv with ⟨k, hk, hvEq⟩
        have hkI : contains I (progVal I k) = true := by
          rw [contains_iff_mem]
          exact ⟨k, List.mem_range.mp hk, rfl⟩
        have hkp : contains (positions a) (progVal I k) = true := hmemI _ hkI
        rcases (contains_iff_mem (positions a) (progVal I k)).mp hkp with ⟨i, hi, hiv⟩
        have hipos : progVal I k = a.start + i * a.stride := by
          simpa [positions, progVal, Int.mul_comm] using hiv
        apply List.mem_map.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_filter.mpr
          refine ⟨List.mem_range.mpr hi, ?_⟩
          have hkm : contains m (a.start + i * a.stride) = true :=
            ((intersection_contains (positions a) m (a.start + i * a.stride)).mp
              (by simpa [hipos] using hkI)).2
          exact hkm
        · have hdivI : (progVal I k - a.start) / (a.stride : Int) = (i : Int) := by
            rw [hipos]
            have hc : (a.start + i * a.stride - a.start : Int) = (i : Int) * (a.stride : Int) := by omega
            rw [hc, Int.mul_comm (i : Int) (a.stride : Int)]
            exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
          have hcp := common_pos_value a ha I hmemI hdvd hInpos k (List.mem_range.mp hk)
          rw [hdivI] at hcp
          rw [← hvEq]
          exact hcp
    have hsum := list_sum_perm hperm
    have hright : R.sum = deletedSum a m := by
      dsimp [R, I, deletedSum]
      exact sum_affine I.n (firstAt a (intersection (positions a) m)) (slopeAt a (intersection (positions a) m))
    exact hsum.trans hright

private theorem flatMap_delete (ls : List Lane) (m : Progression)
    (hs : ∀ a ∈ ls, 0 < a.stride) :
    ((((ls.flatMap lanePoints).filter (fun r : Nat × Int => !contains m r.1)).map Prod.snd).sum) =
      (ls.map (fun a => partialSum a a.n - deletedSum a m)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.flatMap_cons, List.filter_append, List.map_append, List.sum_append,
        List.map_cons, List.sum_cons]
      rw [show (((lanePoints a).filter (fun r : Nat × Int => !contains m r.1)).map Prod.snd).sum =
          partialSum a a.n - deletedSum a m by
        dsimp [lanePoints]
        exact lane_delete a (hs a (by simp)) m]
      rw [ih (fun b hb => hs b (by simp [hb]))]

private theorem delete_from_rows (lay : Layout) (c : Config) (m : Progression)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    ((((rows lay c).filter (fun r : Nat × Int => !contains m r.1)).map Prod.snd).sum) =
      ((lay.lanes c).map (fun a => partialSum a a.n - deletedSum a m)).sum := by
  have hp := hperm.filter (fun r : Nat × Int => !contains m r.1)
  have hs' := list_sum_perm (hp.map Prod.snd)
  rw [hs']
  exact flatMap_delete (lay.lanes c) m hs

namespace TriangleWave

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

theorem TriangleWave_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.DeleteProgression.Goal := by
  intro c mask
  exact delete_from_rows layout c mask (rows_lanes c) (lanes_stride c)

end TriangleWave

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

theorem LaneMajor_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor.DeleteProgression.Goal := by
  intro c mask
  exact delete_from_rows layout c mask (rows_lanes c) (lanes_stride c)

end LaneMajor

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

theorem Boustrophedon_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon.DeleteProgression.Goal := by
  intro c mask
  exact delete_from_rows layout c mask (rows_lanes c) (lanes_stride c)

end Boustrophedon

/-! ## Aligned products over common positions -/

private theorem inner_align (a b : Lane) (hb : 0 < b.stride) (i : Nat) :
    ((List.range b.n).map (fun j : Nat =>
      if a.start + i * a.stride == b.start + j * b.stride then
        (a.base + (i : Int) * a.step) * (b.base + (j : Int) * b.step) else 0)).sum =
    if contains (positions b) (a.start + i * a.stride) then
      (a.base + (i : Int) * a.step) *
        (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step)
    else 0 := by
  let pos := a.start + i * a.stride
  by_cases hc : contains (positions b) pos
  · rcases (contains_iff_mem (positions b) pos).mp hc with ⟨j0, hj0, hv0⟩
    dsimp [progVal, positions] at hv0 hj0
    have hfilter : (List.range b.n).filter (fun j : Nat => pos == b.start + j * b.stride) = [j0] := by
      have hpred : ∀ j ∈ List.range b.n,
          (pos == b.start + j * b.stride) = (j == j0) := by
        intro j hj
        by_cases hji : j = j0
        · subst j
          have hvNat : pos = b.start + j0 * b.stride := by exact_mod_cast hv0
          simp [hvNat]
        · have hne : pos ≠ b.start + j * b.stride := by
            intro h
            have hp1 : (pos : Int) = (b.start : Int) + (j : Int) * (b.stride : Int) := by exact_mod_cast h
            have heq : (j : Int) * (b.stride : Int) = (j0 : Int) * (b.stride : Int) := by omega
            have hjj : (j : Int) = (j0 : Int) :=
              Int.eq_of_mul_eq_mul_right (by exact_mod_cast (Nat.ne_of_gt hb)) heq
            exact hji (by exact_mod_cast hjj)
          rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr hji]
      rw [show (List.range b.n).filter (fun j : Nat => pos == b.start + j * b.stride) =
          (List.range b.n).filter (fun j : Nat => j == j0) by
        apply filter_congr_bool
        intro j hj
        exact hpred j hj]
      rw [range_filter_beq]
      simp [hj0]
    have hdiv : (pos - b.start) / (b.stride : Int) = (j0 : Int) := by
      have hv0' : pos = b.start + (j0 : Int) * (b.stride : Int) := by simpa using hv0
      have hc1 : (pos - b.start : Int) = (j0 : Int) * (b.stride : Int) := by grind
      rw [hc1, Int.mul_comm (j0 : Int) (b.stride : Int)]
      exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt hb))
    rw [← sum_map_ite_zero (List.range b.n) (fun j : Nat => pos == b.start + j * b.stride)
      (fun j : Nat => (a.base + (i : Int) * a.step) * (b.base + (j : Int) * b.step))]
    rw [hfilter]
    dsimp [pos] at hdiv hc ⊢
    simp [hc, hdiv]
  · have hnever : ∀ j ∈ List.range b.n, (pos == b.start + j * b.stride) = false := by
      intro j hj
      by_cases he : pos == b.start + j * b.stride
      · have heq : pos = b.start + j * b.stride := by exact_mod_cast (of_decide_eq_true he)
        exact False.elim (hc ((contains_iff_mem (positions b) pos).mpr ⟨j, List.mem_range.mp hj,
          by dsimp [progVal, positions]; exact_mod_cast heq⟩))
      · exact beq_eq_false_iff_ne.mpr (by
          intro h
          exact he (by rw [h]; simp))
    rw [show (List.range b.n).map (fun j : Nat =>
      if pos == b.start + j * b.stride then
        (a.base + (i : Int) * a.step) * (b.base + (j : Int) * b.step) else 0) =
      (List.range b.n).map (fun _ : Nat => (0 : Int)) by
      apply List.map_congr_left
      intro j hj
      simp [hnever j hj]]
    rw [show ((List.range b.n).map (fun _ : Nat => (0 : Int))).sum = 0 by induction List.range b.n <;> simp_all]
    dsimp [pos] at hc
    simp [hc]

private theorem inter_step_pos_both (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride)
    (hpos : 0 < (intersection (positions a) (positions b)).n) :
    0 < (intersection (positions a) (positions b)).step := by
  let I := intersection (positions a) (positions b)
  change 0 < I.n at hpos
  by_cases hs : I.step = 0
  · exfalso
    have hle1 : I.n = 0 := by
      dsimp [I] at hs ⊢
      unfold intersection at hs ⊢
      by_cases han : a.n = 0
      · simp [positions, han]
      by_cases hbn : b.n = 0
      · simp [positions, hbn]
      have hgpos : 0 < a.stride.gcd b.stride := Nat.gcd_pos_of_pos_left b.stride ha
      have hdivpos : 0 < a.stride / a.stride.gcd b.stride :=
        Nat.div_pos (Nat.le_of_dvd ha (Nat.gcd_dvd_left a.stride b.stride)) hgpos
      have hperpos : 0 < (a.stride / a.stride.gcd b.stride) * b.stride := Nat.mul_pos hdivpos hb
      by_cases hgap : (b.start - a.start) % (a.stride.gcd b.stride : Int) = 0
      · simp [positions, han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgap] at hs
        omega
      · simp [positions, han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgap]
    omega
  · exact Nat.pos_of_ne_zero hs

private theorem common_indices (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    ((List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride))).Perm
      ((List.range (intersection (positions a) (positions b)).n).map (fun k : Nat =>
        ((progVal (intersection (positions a) (positions b)) k - a.start) / (a.stride : Int)).toNat)) := by
  let I := intersection (positions a) (positions b)
  by_cases hIn0 : I.n = 0
  · have hlen : ((List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride))).length = 0 := by
      rw [inter_count_positions a ha (positions b)]
      exact hIn0
    have hnil : (List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride)) = [] := by
      cases hf : (List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride)) with
      | nil => rfl
      | cons x xs => simp [hf] at hlen
    rw [hnil]
    simp [I, hIn0]
  have hInpos : 0 < I.n := Nat.pos_of_ne_zero hIn0
  have hIsteppos := inter_step_pos_both a b ha hb hInpos
  have hdvdA := inter_step_dvd a ha (positions b) hb hInpos
  obtain ⟨i0, hi0, hlo⟩ := inter_lo_index a ha (positions b) hInpos
  have hq : 0 < I.step / a.stride := by
    have hle : a.stride ≤ I.step := by
      obtain ⟨t, ht⟩ := Nat.dvd_of_mod_eq_zero hdvdA
      have ht0 : t ≠ 0 := by
        intro hz
        rw [hz] at ht
        simp at ht
        omega
      rw [ht]
      exact Nat.le_mul_of_pos_right a.stride (Nat.pos_of_ne_zero ht0)
    exact Nat.div_pos hle ha
  have hidx : ∀ k : Nat, k < I.n →
      ((progVal I k - a.start) / (a.stride : Int)).toNat = i0 + k * (I.step / a.stride) := by
    intro k hk
    have hqlo : (I.lo - a.start : Int) = (i0 : Int) * (a.stride : Int) := by grind
    have hqstep : (I.step : Int) = ((I.step / a.stride : Nat) : Int) * (a.stride : Int) := by
      exact_mod_cast (Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hdvdA)).symm
    have hval : progVal I k - a.start =
        ((i0 + k * (I.step / a.stride) : Nat) : Int) * (a.stride : Int) := by
      unfold progVal
      have htmp : I.lo + (k : Int) * (I.step : Int) - a.start =
          (I.lo - a.start) + (k : Int) * (I.step : Int) := by grind
      rw [htmp, hqlo, hqstep]
      push_cast
      grind
    have hdiv : (progVal I k - a.start) / (a.stride : Int) =
        ((i0 + k * (I.step / a.stride) : Nat) : Int) := by
      rw [hval, Int.mul_comm (((i0 + k * (I.step / a.stride) : Nat) : Int)) (a.stride : Int)]
      exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
    have ht : ((progVal I k - a.start) / (a.stride : Int)).toNat =
        (↑(i0 + k * (I.step / a.stride)) : Int).toNat := by
      exact congrArg Int.toNat hdiv
    rw [ht]
    rfl
  have hnodL : ((List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride))).Nodup :=
    nodup_filter' (List.range a.n) (fun i : Nat => contains (positions b) (a.start + i * a.stride)) List.nodup_range
  have hnodR : ((List.range I.n).map (fun k : Nat =>
      ((progVal I k - a.start) / (a.stride : Int)).toNat)).Nodup := by
    rw [show (List.range I.n).map (fun k : Nat =>
      ((progVal I k - a.start) / (a.stride : Int)).toNat) =
      (List.range I.n).map (fun k : Nat => i0 + k * (I.step / a.stride)) by
      apply List.map_congr_left
      intro k hk
      exact hidx k (List.mem_range.mp hk)]
    exact List.Pairwise.map (R := fun i j : Nat => i ≠ j) (S := fun x y : Nat => x ≠ y)
      (fun k : Nat => i0 + k * (I.step / a.stride))
      (fun k l hkl he => by
        have hmul : k * (I.step / a.stride) = l * (I.step / a.stride) := by omega
        exact hkl (Nat.eq_of_mul_eq_mul_right hq hmul)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnodL hnodR).mpr
  intro i
  rw [List.mem_filter]
  constructor
  · intro hi
    rcases hi with ⟨hiRange, hmpos⟩
    have hiI : contains I (a.start + i * a.stride) = true :=
      (intersection_contains (positions a) (positions b) (a.start + i * a.stride)).mpr
        ⟨positions_mem a i (List.mem_range.mp hiRange), hmpos⟩
    rcases (contains_iff_mem I (a.start + i * a.stride)).mp hiI with ⟨k, hk, hvk⟩
    apply List.mem_map.mpr
    refine ⟨k, List.mem_range.mpr hk, ?_⟩
    rw [hidx k hk]
    have hdiv : (progVal I k - a.start) / (a.stride : Int) = (i : Int) := by
      rw [← hvk]
      have hc : (a.start + i * a.stride - a.start : Int) = (i : Int) * (a.stride : Int) := by grind
      rw [hc, Int.mul_comm (i : Int) (a.stride : Int)]
      exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
    have ht : ((progVal I k - a.start) / (a.stride : Int)).toNat = i := by
      simpa using congrArg Int.toNat hdiv
    rw [hidx k hk] at ht
    exact ht
  · intro hi
    rcases List.mem_map.mp hi with ⟨k, hk, hik⟩
    refine ⟨?_, ?_⟩
    · have hindex : i = i0 + k * (I.step / a.stride) := by
        rw [← hik]
        exact hidx k (List.mem_range.mp hk)
      apply List.mem_range.mpr
      have hposk := contains_iff_mem I (progVal I k) |>.mpr ⟨k, List.mem_range.mp hk, rfl⟩
      have hpa := ((intersection_contains (positions a) (positions b) (progVal I k)).mp hposk).1
      rcases (contains_iff_mem (positions a) (progVal I k)).mp hpa with ⟨j, hj, hval⟩
      have hjEq : j = i := by
        have hd : (progVal I k - a.start) / (a.stride : Int) = (j : Int) := by
          dsimp [progVal, positions] at hval
          have hc : (progVal I k - a.start : Int) = (j : Int) * (a.stride : Int) := by
            unfold progVal
            grind
          rw [hc, Int.mul_comm (j : Int) (a.stride : Int)]
          exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
        have ht : ((progVal I k - a.start) / (a.stride : Int)).toNat = j := by
          simpa using congrArg Int.toNat hd
        rw [hidx k (List.mem_range.mp hk)] at ht
        rw [← hik]
        rw [hidx k (List.mem_range.mp hk)]
        exact ht.symm
      rw [← hjEq]
      exact hj
    · have hposk := contains_iff_mem I (progVal I k) |>.mpr ⟨k, List.mem_range.mp hk, rfl⟩
      have hpb := ((intersection_contains (positions a) (positions b) (progVal I k)).mp hposk).2
      have hpa := ((intersection_contains (positions a) (positions b) (progVal I k)).mp hposk).1
      rcases (contains_iff_mem (positions a) (progVal I k)).mp hpa with ⟨j, hj, hval⟩
      have hji : j = i := by
        have hd : (progVal I k - a.start) / (a.stride : Int) = (j : Int) := by
          dsimp [progVal, positions] at hval
          have hc : (progVal I k - a.start : Int) = (j : Int) * (a.stride : Int) := by
            unfold progVal
            grind
          rw [hc, Int.mul_comm (j : Int) (a.stride : Int)]
          exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
        have ht : ((progVal I k - a.start) / (a.stride : Int)).toNat = j := by
          simpa using congrArg Int.toNat hd
        rw [hidx k (List.mem_range.mp hk)] at ht
        rw [← hik]
        rw [hidx k (List.mem_range.mp hk)]
        exact ht.symm
      have hprog : progVal I k = a.start + i * a.stride := by
        dsimp [progVal, positions] at hval
        rw [hji] at hval
        unfold progVal
        exact hval
      simpa [hprog] using hpb

private theorem inter_step_dvd_second (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride)
    (hpos : 0 < (intersection (positions a) (positions b)).n) :
    (intersection (positions a) (positions b)).step % b.stride = 0 := by
  let g := a.stride.gcd b.stride
  let per := (a.stride / g) * b.stride
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.stride ha
  have hdivpos : 0 < a.stride / g :=
    Nat.div_pos (Nat.le_of_dvd ha (Nat.gcd_dvd_left a.stride b.stride)) hgpos
  have hperpos : 0 < per := Nat.mul_pos hdivpos hb
  have han : a.n ≠ 0 := by
    intro h
    have hz : (intersection (positions a) (positions b)).n = 0 := by simp [intersection, positions, h]
    omega
  have hbn : b.n ≠ 0 := by
    intro h
    have hz : (intersection (positions a) (positions b)).n = 0 := by simp [intersection, positions, h]
    omega
  by_cases hgap : (b.start - a.start) % (g : Int) = 0
  · dsimp [g] at hgap ⊢
    have hI : (intersection (positions a) (positions b)).step = per := by
      unfold intersection
      simp [positions, han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgap]
      dsimp [per, g]
    rw [hI]
    exact Nat.mod_eq_zero_of_dvd ⟨a.stride / g, by dsimp [per]; rw [Nat.mul_comm]⟩
  · dsimp [g] at hgap ⊢
    have hI : (intersection (positions a) (positions b)).n = 0 := by
      unfold intersection
      simp [positions, han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgap]
    omega

private theorem sum_product_affine (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih, tri_succ, squares_succ]
      push_cast
      grind

private theorem intersection_n_sym (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    (intersection (positions a) (positions b)).n = (intersection (positions b) (positions a)).n := by
  let I := intersection (positions a) (positions b)
  let J := intersection (positions b) (positions a)
  have hnodI : (progList I).Nodup := progList_nodup I (intersection_step_pos _ _)
  have hnodJ : (progList J).Nodup := progList_nodup J (intersection_step_pos _ _)
  have hmem : ∀ v : Int, v ∈ progList I ↔ v ∈ progList J := by
    intro v
    rw [mem_progList, mem_progList]
    rw [intersection_contains, intersection_contains]
    constructor
    · rintro ⟨ha', hb'⟩
      exact ⟨hb', ha'⟩
    · rintro ⟨hb', ha'⟩
      exact ⟨ha', hb'⟩
  have hp := (List.perm_ext_iff_of_nodup hnodI hnodJ).mpr hmem
  have hl := hp.length_eq
  dsimp [progList] at hl
  simpa using hl

private theorem lane_aligned (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    ((List.range a.n).map (fun i : Nat =>
      ((List.range b.n).map (fun j : Nat =>
        if a.start + i * a.stride == b.start + j * b.stride then
          (a.base + (i : Int) * a.step) * (b.base + (j : Int) * b.step) else 0)).sum)).sum =
      aligned a b := by
  rw [show (List.range a.n).map (fun i : Nat =>
      ((List.range b.n).map (fun j : Nat =>
        if a.start + i * a.stride == b.start + j * b.stride then
          (a.base + (i : Int) * a.step) * (b.base + (j : Int) * b.step) else 0)).sum) =
      (List.range a.n).map (fun i : Nat =>
        if contains (positions b) (a.start + i * a.stride) then
          (a.base + (i : Int) * a.step) *
            (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step)
        else 0) by
      apply List.map_congr_left
      intro i hi
      exact inner_align a b hb i]
  rw [← sum_map_ite_zero (List.range a.n) (fun i : Nat => contains (positions b) (a.start + i * a.stride))
    (fun i : Nat => (a.base + (i : Int) * a.step) *
      (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step))]
  let I := intersection (positions a) (positions b)
  by_cases hIn0 : I.n = 0
  · have hlen : ((List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride))).length = 0 := by
      rw [inter_count_positions a ha (positions b)]
      exact hIn0
    have hnil : (List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride)) = [] := by
      cases hf : (List.range a.n).filter (fun i : Nat => contains (positions b) (a.start + i * a.stride)) with
      | nil => rfl
      | cons x xs => simp [hf] at hlen
    rw [hnil]
    simp [I, aligned, hIn0, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  have hInpos : 0 < I.n := Nat.pos_of_ne_zero hIn0
  have hdvdA := inter_step_dvd a ha (positions b) hb hInpos
  have hdvdB := inter_step_dvd_second a b ha hb hInpos
  have hmemA : ∀ v : Int, contains I v = true → contains (positions a) v = true := by
    intro v hv
    exact ((intersection_contains (positions a) (positions b) v).mp hv).1
  have hmemB : ∀ v : Int, contains I v = true → contains (positions b) v = true := by
    intro v hv
    exact ((intersection_contains (positions a) (positions b) v).mp hv).2
  have hperm := common_indices a b ha hb
  have hpI := hperm.map (fun i : Nat =>
    (a.base + (i : Int) * a.step) *
      (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step))
  have hsI := list_sum_perm hpI
  rw [hsI, List.map_map]
  change ((List.range I.n).map ((fun i : Nat => (a.base + (i : Int) * a.step) *
      (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step)) ∘
      (fun k : Nat => ((progVal I k - a.start) / (a.stride : Int)).toNat))).sum = aligned a b
  have hterm : ∀ k ∈ List.range I.n,
      (a.base + ((((progVal I k - a.start) / (a.stride : Int)).toNat : Nat) : Int) * a.step) *
        (b.base + ((a.start + ((progVal I k - a.start) / (a.stride : Int)).toNat * a.stride - b.start) /
          (b.stride : Int)) * b.step) =
      (firstAt a I + (k : Int) * slopeAt a I) * (firstAt b I + (k : Int) * slopeAt b I) := by
    intro k hk
    have hkN : k < I.n := List.mem_range.mp hk
    have hkI : contains I (progVal I k) = true := by
      rw [contains_iff_mem]
      exact ⟨k, hkN, rfl⟩
    have hpa := hmemA _ hkI
    rcases (contains_iff_mem (positions a) (progVal I k)).mp hpa with ⟨j, hj, hval⟩
    dsimp [progVal, positions] at hval
    have hdivJ : (progVal I k - a.start) / (a.stride : Int) = (j : Int) := by
      have hc : (progVal I k - a.start : Int) = (j : Int) * (a.stride : Int) := by
        unfold progVal
        grind
      rw [hc, Int.mul_comm (j : Int) (a.stride : Int)]
      exact Int.mul_ediv_cancel_left _ (by exact_mod_cast (Nat.ne_of_gt ha))
    have hjNat : ((progVal I k - a.start) / (a.stride : Int)).toNat = j := by
      simpa using congrArg Int.toNat hdivJ
    have hposEq : a.start + ((progVal I k - a.start) / (a.stride : Int)).toNat * a.stride =
        progVal I k := by
      rw [hjNat]
      unfold progVal
      exact hval.symm
    have hcpA := common_pos_value a ha I hmemA hdvdA hInpos k hkN
    have hcpB := common_pos_value b hb I hmemB hdvdB hInpos k hkN
    rw [hposEq]
    rw [hdivJ]
    simp
    have hcpA' : a.base + (j : Int) * a.step = firstAt a I + (k : Int) * slopeAt a I := by
      rw [← hdivJ]
      exact hcpA
    rw [hcpA', hcpB]
  have hmapeq : (List.range I.n).map ((fun i : Nat => (a.base + (i : Int) * a.step) *
      (b.base + ((a.start + i * a.stride - b.start) / (b.stride : Int)) * b.step)) ∘
      (fun k : Nat => ((progVal I k - a.start) / (a.stride : Int)).toNat)) =
      (List.range I.n).map (fun k : Nat =>
        (firstAt a I + (k : Int) * slopeAt a I) * (firstAt b I + (k : Int) * slopeAt b I)) :=
    List.map_congr_left hterm
  rw [hmapeq]
  rw [sum_product_affine]
  rfl


private theorem flatMap_aligned (ls : List Lane) (b : Lane) (hb : 0 < b.stride)
    (hs : ∀ a ∈ ls, 0 < a.stride) :
    (((ls.flatMap lanePoints).map (fun r : Nat × Int =>
      ((List.range b.n).map (fun j : Nat =>
        if r.1 == b.start + j * b.stride then r.2 * (b.base + (j : Int) * b.step) else 0)).sum)).sum) =
      (ls.map (aligned · b)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      dsimp [lanePoints]
      rw [show (((List.range a.n).map (fun i : Nat =>
          (a.start + i * a.stride, a.base + (i : Int) * a.step))).map (fun r : Nat × Int =>
        ((List.range b.n).map (fun j : Nat =>
          if r.1 == b.start + j * b.stride then r.2 * (b.base + (j : Int) * b.step) else 0)).sum)).sum = aligned a b by
        rw [List.map_map]
        dsimp [Function.comp_def]
        simpa [Int.mul_comm] using lane_aligned a b (hs a (by simp)) hb]
      rw [ih (fun c hc => hs c (by simp [hc]))]

private theorem aligned_from_rows (lay : Layout) (c : Config) (b : Lane) (hb : 0 < b.stride)
    (hperm : (rows lay c).Perm ((lay.lanes c).flatMap lanePoints))
    (hs : ∀ a ∈ lay.lanes c, 0 < a.stride) :
    (((rows lay c).map (fun r : Nat × Int =>
      ((List.range b.n).map (fun j : Nat =>
        if r.1 == b.start + j * b.stride then r.2 * (b.base + (j : Int) * b.step) else 0)).sum)).sum) =
      ((lay.lanes c).map (aligned · b)).sum := by
  have hp := hperm.map (fun r : Nat × Int =>
    ((List.range b.n).map (fun j : Nat =>
      if r.1 == b.start + j * b.stride then r.2 * (b.base + (j : Int) * b.step) else 0)).sum)
  have hs' := list_sum_perm hp
  rw [hs']
  exact flatMap_aligned (lay.lanes c) b hb hs

namespace TriangleWave

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

theorem TriangleWave_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave.AlignedProduct.Goal := by
  intro c b hb
  simpa [Int.mul_comm] using aligned_from_rows layout c b hb (rows_lanes c) (lanes_stride c)

end TriangleWave

namespace LaneMajor

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

theorem LaneMajor_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor.AlignedProduct.Goal := by
  intro c b hb
  simpa [Int.mul_comm] using aligned_from_rows layout c b hb (rows_lanes c) (lanes_stride c)

end LaneMajor

namespace Boustrophedon

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

theorem Boustrophedon_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon.AlignedProduct.Goal := by
  intro c b hb
  simpa [Int.mul_comm] using aligned_from_rows layout c b hb (rows_lanes c) (lanes_stride c)

end Boustrophedon

/-! ## GrayTile -/

namespace GrayTile

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step * ((i / width c * width c +
        ((i % width c) ^^^ ((i % width c) / 2)) : Nat) : Int))) := by
  unfold rows
  simp [layout]

private theorem rows_lanes (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  have hw : 0 < width c := by dsimp [width]; omega
  let make : Nat → Int × Int := fun r =>
    (c.base + c.step * (r ^^^ (r / 2) : Nat), c.step * width c)
  let f : Nat → Int := fun i =>
    c.base + c.step * ((i / width c * width c +
      ((i % width c) ^^^ ((i % width c) / 2)) : Nat) : Int)
  have hval : ∀ i : Nat, f i = (make (i % width c)).1 +
      (make (i % width c)).2 * ((i / width c : Nat) : Int) := by
    intro i
    dsimp [f, make]
    simp only [Int.natCast_add, Int.natCast_mul, Int.mul_add, Int.add_assoc,
      Int.add_left_comm, Int.add_comm, Int.mul_assoc, Int.mul_left_comm, Int.mul_comm]
  rw [rows_eq]
  have hlanes : layout.lanes c = striped c (width c) make := by
    dsimp [layout, striped, make]
  rw [hlanes]
  exact stripedRowsPerm c (width c) hw make f hval

private theorem lanes_stride (c : Config) : ∀ a ∈ layout.lanes c, 0 < a.stride := by
  intro a ha
  simp [layout, striped] at ha ⊢
  obtain ⟨r, hr, rfl⟩ := ha
  have hr' : r < min (width c) c.n := by simpa using hr
  dsimp [lane, width]
  omega

theorem GrayTile_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.ResidueJoin.Goal := by
  intro c b
  have hp := (rows_lanes c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))
  have hs : ((rows layout c).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
      (((layout.lanes c).flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum :=
    list_sum_perm_nat hp
  rw [hs]
  simpa [Int.mul_comm] using flatMap_residue (layout.lanes c) b

theorem GrayTile_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.DeleteProgression.Goal := by
  intro c mask
  exact delete_from_rows layout c mask (rows_lanes c) (lanes_stride c)

theorem GrayTile_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.AlignedProduct.Goal := by
  intro c b hb
  simpa [Int.mul_comm] using aligned_from_rows layout c b hb (rows_lanes c) (lanes_stride c)

end GrayTile

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P9
