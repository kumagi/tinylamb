import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 100000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.MirroredTail.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

-- =====================================================================
-- Generic list helpers
-- =====================================================================

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := p.length_eq

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

private theorem sum_map_mul_int (l : List Int) (c : Int) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_const_int {α : Type} (l : List α) (v : Int) :
    (l.map (fun _ => v)).sum = (l.length : Int) * v := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul]; omega

private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_map_ite_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem sum_filter_add_compl {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> grind

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

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

private theorem countP_const_bool (n : Nat) (b : Bool) :
    (List.range n).countP (fun _ => b) = if b then n else 0 := by
  induction n with
  | zero => simp
  | succ n ih => by_cases h : b <;> simp [h, ih]

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α] {x : α} :
    ∀ {l : List α}, l.Nodup → x ∈ l → l.count x = 1
  | [], hnd, hx => by simp at hx
  | a :: as, hnd, hx => by
      rw [List.nodup_cons] at hnd
      by_cases hxa : x = a
      · subst x
        rw [List.count_cons_self]
        have hnot : a ∉ as := hnd.1
        rw [List.count_eq_zero.mpr hnot]
      · have hxas : x ∈ as := by
          simp at hx
          rcases hx with hxa' | hxas
          · exact (hxa hxa').elim
          · exact hxas
        have hax : a ≠ x := by intro h'; exact hxa h'.symm
        rw [List.count_cons_of_ne hax]
        exact count_eq_one_of_mem_nodup hnd.2 hxas

private theorem perm_of_count_eq {α : Type} [BEq α] [LawfulBEq α] (l₁ l₂ : List α)
    (h : ∀ x, l₁.count x = l₂.count x) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro x hx
        have hnzero : l₂.count x ≠ 0 := by
          intro hz
          exact (List.count_eq_zero.mp hz) hx
        have hc := h x
        simp at hc
        omega
      simp [hnil]
  | cons a as ih =>
      have hal₂ : a ∈ l₂ := by
        by_cases hn : a ∈ l₂
        · exact hn
        · have hz : l₂.count a = 0 := List.count_eq_zero.mpr hn
          have hc := h a
          simp only [List.count_cons_self] at hc
          omega
      have hp₁ : l₂.Perm (a :: l₂.erase a) := List.perm_cons_erase hal₂
      have htail : ∀ x, as.count x = (l₂.erase a).count x := by
        intro x
        have hx := h x
        rw [List.count_cons] at hx
        rw [List.count_erase]
        by_cases hax : (a == x) = true
        · have heq : a = x := beq_iff_eq.mp hax
          subst x
          simp at hx ⊢
          omega
        · simp [hax] at hx ⊢
          omega
      exact (List.Perm.cons a (ih (l₂.erase a) htail)).trans hp₁.symm

private theorem perm_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  apply perm_of_count_eq
  intro x
  by_cases hx : x ∈ l₁
  · rw [count_eq_one_of_mem_nodup h₁ hx, count_eq_one_of_mem_nodup h₂ ((hmem x).mp hx)]
  · have hx₂ : x ∉ l₂ := by intro hx₂; exact hx ((hmem x).mpr hx₂)
    rw [List.count_eq_zero.mpr hx, List.count_eq_zero.mpr hx₂]

-- =====================================================================
-- Arithmetic on lanes
-- =====================================================================

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

-- =====================================================================
-- Finite progressions and CRT intersections
-- =====================================================================

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

private theorem progVal_mem (a : Progression) (k : Nat) (hk : k < a.n) :
    contains a (progVal a k) = true := by
  exact (contains_iff_mem a (progVal a k)).mpr ⟨k, hk, rfl⟩

private theorem progVal_inj {a : Progression} (ha : 0 < a.step) {i j : Nat}
    (hi : i < a.n) (hj : j < a.n) (h : progVal a i = progVal a j) : i = j := by
  unfold progVal at h
  have hpos : (0 : Int) < a.step := by exact_mod_cast ha
  have hmul : (i : Int) * (a.step : Int) = (j : Int) * (a.step : Int) := by omega
  exact Int.ofNat_inj.mp (Int.eq_of_mul_eq_mul_right (by omega : (a.step : Int) ≠ 0) hmul)

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

private theorem span_contains_iff_nat (first high x : Int) (step : Nat) (hs : 0 < step) :
    contains ⟨first, step,
      if first ≤ high then ((high - first) / (step : Int)).toNat + 1 else 0⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  simpa using span_contains_iff first (step : Int) high x (by exact_mod_cast hs)

private theorem lcm_dvd_of_dvd {a b d : Nat} (ha0 : 0 < a) (hb0 : 0 < b)
    (ha : a ∣ d) (hb : b ∣ d) : (a / a.gcd b) * b ∣ d := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha0
  have hga : a = g * A := by
    dsimp [g, A]
    exact (Nat.mul_div_cancel' (Nat.gcd_dvd_left a b)).symm
  have hgb : b = g * B := by
    dsimp [g, B]
    exact (Nat.mul_div_cancel' (Nat.gcd_dvd_right a b)).symm
  obtain ⟨x, hx⟩ := ha
  obtain ⟨y, hy⟩ := hb
  have hgdvd : g ∣ d := by
    refine ⟨A * x, ?_⟩
    rw [hx, hga]
    grind
  obtain ⟨t, rfl⟩ := hgdvd
  have hEq1 : g * t = g * A * x := by simpa [hga] using hx
  have hEq2 : g * t = g * B * y := by simpa [hgb] using hy
  have htx : t = A * x := Nat.eq_of_mul_eq_mul_left hg (by simpa [Nat.mul_assoc] using hEq1)
  have hty : t = B * y := Nat.eq_of_mul_eq_mul_left hg (by simpa [Nat.mul_assoc] using hEq2)
  have hAt : A ∣ t := ⟨x, htx⟩
  have hBt : B ∣ t := ⟨y, hty⟩
  have hcop : A.Coprime B := by
    dsimp [A, B, g]
    exact Nat.coprime_div_gcd_div_gcd hg
  have hAB : A * B ∣ t := hcop.mul_dvd_of_dvd_of_dvd hAt hBt
  obtain ⟨m, hm⟩ := hAB
  refine ⟨m, ?_⟩
  dsimp [A, B, g] at hm ⊢
  grind

private theorem crt_period_dvd (ast bst : Nat) (hastp : 0 < ast) (hbstp : 0 < bst)
    (d : Int) (ha : (ast : Int) ∣ d) (hb : (bst : Int) ∣ d) :
    ((((ast / ast.gcd bst) * bst : Nat) : Int)) ∣ d := by
  have haN : ast ∣ d.natAbs := (Int.natAbs_dvd_natAbs).mpr ha
  have hbN : bst ∣ d.natAbs := (Int.natAbs_dvd_natAbs).mpr hb
  have hperN : (ast / ast.gcd bst) * bst ∣ d.natAbs :=
    lcm_dvd_of_dvd hastp hbstp haN hbN
  have hnabs : d = (d.natAbs : Int) ∨ d = -(d.natAbs : Int) :=
    (Int.natAbs_eq_iff).mp rfl
  obtain ⟨k, hk⟩ := hperN
  rcases hnabs with hd | hd
  · refine ⟨(k : Int), ?_⟩
    rw [hd, hk]
    grind
  · refine ⟨-(k : Int), ?_⟩
    rw [hd, hk]
    grind

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
  unfold contains lastVal
  simp
  omega

private theorem contains_one_zero (lo v : Int) (k : Nat) (hk : k ≤ 1) :
    contains ⟨lo, 1, k⟩ v = true ↔ k = 1 ∧ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨m, hm, hv⟩
    change m < k at hm
    change v = lo + (m : Int) * 1 at hv
    have hm0 : m = 0 := by omega
    subst m
    simp at hv
    subst v
    exact ⟨by omega, rfl⟩
  · rintro ⟨hk1, hv⟩
    rw [hk1]
    rw [hv]
    exact ⟨0, Nat.zero_lt_succ 0, by simp [progVal]⟩

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  have hsnon : (0 : Int) ≤ (a.step : Int) := by exact_mod_cast (Nat.zero_le a.step)
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega)
  have hmul' : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) :=
    Int.mul_le_mul_of_nonneg_right hkint hsnon
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hsnon)
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
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
  have h1 : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) * (i : Int) := by
    obtain ⟨c, hc⟩ := hg2
    refine ⟨c * (i : Int), ?_⟩
    rw [hc]
    grind
  have h2 : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) * (j : Int) := by
    obtain ⟨c, hc⟩ := hg1
    refine ⟨c * (j : Int), ?_⟩
    rw [hc]
    grind
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    grind
  rw [← heq]
  exact Int.dvd_sub h1 h2

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  rcases a with ⟨alo, ast, an⟩
  rcases b with ⟨blo, bst, bn⟩
  by_cases han : an = 0
  · subst an
    simp [intersection, contains]
  · by_cases hbn : bn = 0
    · subst bn
      simp [intersection, contains]
    · have hanp : 0 < an := Nat.pos_of_ne_zero han
      have hbnp : 0 < bn := Nat.pos_of_ne_zero hbn
      by_cases hast : ast = 0
      · subst ast
        by_cases hc : contains ⟨blo, bst, bn⟩ alo = true
        · rw [show intersection ⟨alo, 0, an⟩ ⟨blo, bst, bn⟩ = ⟨alo, 1, 1⟩ by
            simp [intersection, han, hbn, hc]]
          rw [contains_iff_mem]
          constructor
          · rintro ⟨m, hm, hv⟩
            change m < 1 at hm
            change v = alo + (m : Int) * 1 at hv
            have hm0 : m = 0 := by omega
            subst m
            simp at hv
            subst v
            exact ⟨(contains_iff_mem ⟨alo, 0, an⟩ alo).mpr
              ⟨0, by omega, by simp [progVal]⟩, hc⟩
          · rintro ⟨ha, hb⟩
            obtain ⟨m, hm, hv⟩ := (contains_iff_mem ⟨alo, 0, an⟩ v).mp ha
            change m < an at hm
            change v = alo + (m : Int) * 0 at hv
            simp at hv
            subst v
            exact ⟨0, Nat.zero_lt_succ 0, by simp [progVal]⟩
        · rw [show intersection ⟨alo, 0, an⟩ ⟨blo, bst, bn⟩ = ⟨alo, 1, 0⟩ by
            simp [intersection, han, hbn, hc]]
          rw [contains_iff_mem]
          constructor
          · rintro ⟨m, hm, hv⟩
            change m < 0 at hm
            omega
          · rintro ⟨ha, hb⟩
            obtain ⟨m, hm, hv⟩ := (contains_iff_mem ⟨alo, 0, an⟩ v).mp ha
            change v = alo + (m : Int) * 0 at hv
            simp at hv
            subst v
            exact False.elim (hc hb)
      · by_cases hbst : bst = 0
        · subst bst
          by_cases hc : contains ⟨alo, ast, an⟩ blo = true
          · rw [show intersection ⟨alo, ast, an⟩ ⟨blo, 0, bn⟩ = ⟨blo, 1, 1⟩ by
              simp [intersection, han, hbn, hast, hc]]
            rw [contains_iff_mem]
            constructor
            · rintro ⟨m, hm, hv⟩
              change m < 1 at hm
              change v = blo + (m : Int) * 1 at hv
              have hm0 : m = 0 := by omega
              subst m
              simp at hv
              subst v
              exact ⟨hc, (contains_iff_mem ⟨blo, 0, bn⟩ blo).mpr
                ⟨0, by omega, by simp [progVal]⟩⟩
            · rintro ⟨ha, hb⟩
              obtain ⟨m, hm, hv⟩ := (contains_iff_mem ⟨blo, 0, bn⟩ v).mp hb
              change m < bn at hm
              change v = blo + (m : Int) * 0 at hv
              simp at hv
              subst v
              exact ⟨0, Nat.zero_lt_succ 0, by simp [progVal]⟩
          · rw [show intersection ⟨alo, ast, an⟩ ⟨blo, 0, bn⟩ = ⟨blo, 1, 0⟩ by
              simp [intersection, han, hbn, hast, hc]]
            rw [contains_iff_mem]
            constructor
            · rintro ⟨m, hm, hv⟩
              change m < 0 at hm
              omega
            · rintro ⟨ha, hb⟩
              obtain ⟨m, hm, hv⟩ := (contains_iff_mem ⟨blo, 0, bn⟩ v).mp hb
              change v = blo + (m : Int) * 0 at hv
              simp at hv
              subst v
              exact False.elim (hc ha)
        · have hastp : 0 < ast := Nat.pos_of_ne_zero hast
          have hbstp : 0 < bst := Nat.pos_of_ne_zero hbst
          let g := ast.gcd bst
          let gap := blo - alo
          let period := (ast / g) * bst
          let unit := (bezout (ast / g) (bst / g)).1
          let root := alo + (ast : Int) * ((gap / (g : Int) * unit) % ((bst / g : Nat) : Int))
          let low := max alo blo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal ⟨alo, ast, an⟩) (lastVal ⟨blo, bst, bn⟩)
          have hgpos : 0 < g := Nat.gcd_pos_of_pos_left bst hastp
          have hastdiv : 0 < ast / g := Nat.div_gcd_pos_of_pos_left bst hastp
          have hp : (0 : Int) < (period : Int) := by
            exact_mod_cast (Nat.mul_pos hastdiv hbstp)
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd ast bst hastp hbstp alo blo gap rfl hgap
            have hfroot := first_root_dvd low root first (period : Int) hp rfl
            have hdiva : (ast : Int) = (g : Int) * (ast / g : Nat) := by
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left ast bst)).symm
            have hdivb : (bst : Int) = (g : Int) * (bst / g : Nat) := by
              exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right ast bst)).symm
            have hpa : (ast : Int) ∣ (period : Int) := by
              refine ⟨((bst / g : Nat) : Int), ?_⟩
              dsimp [period]
              grind
            have hpb : (bst : Int) ∣ (period : Int) := by
              refine ⟨((ast / g : Nat) : Int), ?_⟩
              dsimp [period]
              grind
            have hgapmod' : (blo - alo) % (ast.gcd bst : Int) = 0 := by
              simpa [gap, g] using hgapmod
            simp [intersection, han, hbn, hast, hbst, hgapmod']
            change contains ⟨first, period,
              if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
            rw [span_contains_iff_nat first high v period (by exact_mod_cast hp)]
            constructor
            · rintro ⟨hfv, hvhigh, hvfirst⟩
              have hvroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hvfirst hfroot
              have hvroot_a : (ast : Int) ∣ v - root := Int.dvd_trans hpa hvroot
              have hvroot_b : (bst : Int) ∣ v - root := Int.dvd_trans hpb hvroot
              have hvalo : (ast : Int) ∣ v - alo := by
                have heq : v - alo = (v - root) + (root - alo) := by omega
                rw [heq]
                exact Int.dvd_add hvroot_a hroot.1
              have hvlo : (bst : Int) ∣ v - blo := by
                have heq : v - blo = (v - root) + (root - blo) := by omega
                rw [heq]
                exact Int.dvd_add hvroot_b hroot.2
              have hremnon : 0 ≤ (root - low) % (period : Int) :=
                Int.emod_nonneg _ (by omega)
              have hfv' : low + (root - low) % (period : Int) ≤ v := by
                simpa [first] using hfv
              have halo_low : alo ≤ low := by dsimp [low]; omega
              have hblo_low : blo ≤ low := by dsimp [low]; omega
              have hhigh_a : high ≤ lastVal ⟨alo, ast, an⟩ := by dsimp [high]; omega
              have hhigh_b : high ≤ lastVal ⟨blo, bst, bn⟩ := by dsimp [high]; omega
              have halo_v : alo ≤ v := by omega
              have hblo_v : blo ≤ v := by omega
              have hvlasta : v ≤ lastVal ⟨alo, ast, an⟩ := by omega
              have hvlastb : v ≤ lastVal ⟨blo, bst, bn⟩ := by omega
              constructor
              · exact mem_of_bounds_dvd ⟨alo, ast, an⟩ v hanp hastp
                  (by change alo ≤ v; exact halo_v)
                  (by exact hvlasta) hvalo
              · exact mem_of_bounds_dvd ⟨blo, bst, bn⟩ v hbnp hbstp
                  (by change blo ≤ v; exact hblo_v)
                  (by exact hvlastb) hvlo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds ⟨alo, ast, an⟩ v ha
              have hbb := contains_bounds ⟨blo, bst, bn⟩ v hb
              have hvalo := contains_dvd ⟨alo, ast, an⟩ v ha
              have hvlo := contains_dvd ⟨blo, bst, bn⟩ v hb
              have hroot_a : (ast : Int) ∣ root - alo := hroot.1
              have hroot_b : (bst : Int) ∣ root - blo := hroot.2
              have hd_a : (ast : Int) ∣ v - root := by
                have heq : v - root = (v - alo) - (root - alo) := by omega
                rw [heq]
                exact Int.dvd_sub hvalo hroot_a
              have hd_b : (bst : Int) ∣ v - root := by
                have heq : v - root = (v - blo) - (root - blo) := by omega
                rw [heq]
                exact Int.dvd_sub hvlo hroot_b
              have hvroot : (period : Int) ∣ v - root := by
                change ((((ast / ast.gcd bst) * bst : Nat) : Int)) ∣ v - root
                exact crt_period_dvd ast bst hastp hbstp (v - root) hd_a hd_b
              have hvfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hvroot hfroot
              have hlow : low ≤ v := by dsimp [low] at hab hbb ⊢; omega
              have hfirstle : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp rfl hlow hvroot
              have hvhigh' : v ≤ high := by dsimp [high] at hab hbb ⊢; omega
              exact ⟨hfirstle, hvhigh', hvfirst⟩
          · have hgapne : (blo - alo) % (ast.gcd bst : Int) ≠ 0 := by
              simpa [gap, g] using hgapmod
            have hleft : contains (intersection ⟨alo, ast, an⟩ ⟨blo, bst, bn⟩) v = false := by
              simp [intersection, han, hbn, hast, hbst, hgapne, contains]
            rw [hleft]
            simp only [Bool.false_eq_true, false_iff]
            rintro ⟨ha, hb⟩
            have hg := common_gap_dvd ⟨alo, ast, an⟩ ⟨blo, bst, bn⟩ v ha hb
            exact hgapne (Int.emod_eq_zero_of_dvd hg)

-- =====================================================================
-- Counting progression members via list permutations
-- =====================================================================

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at h
      rw [List.filter_cons]
      by_cases hp : p a = true
      · rw [if_pos hp]
        rw [List.nodup_cons]
        refine ⟨?_, ih h.2⟩
        intro ha
        have : a ∈ as := (List.mem_filter.mp ha).1
        exact h.1 this
      · rw [if_neg hp]
        exact ih h.2

private theorem nodup_map_of_inj {α β : Type} (f : α → β) {l : List α} (h : l.Nodup)
    (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at h
      rw [List.map_cons, List.nodup_cons]
      refine ⟨?_, ih h.2 (fun x hx y hy hxy => hinj x (by simp [hx]) y (by simp [hy]) hxy)⟩
      intro hmem
      obtain ⟨y, hy, hfy⟩ := List.mem_map.mp hmem
      have haeq : a = y := hinj a (by simp) y (by simp [hy]) hfy.symm
      subst y
      exact h.1 hy

private theorem idx_of_progVal {a : Progression} (ha : 0 < a.step) {k : Nat} (hk : k < a.n) :
    ((progVal a k - a.lo) / (a.step : Int)).toNat = k := by
  unfold progVal
  have hne : (a.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt ha)
  have hsub : a.lo + (k : Int) * (a.step : Int) - a.lo = (k : Int) * (a.step : Int) := by omega
  rw [hsub]
  have hdiv : ((k : Int) * (a.step : Int)) / (a.step : Int) = (k : Int) := by
    rw [Int.mul_comm]
    exact Int.mul_ediv_cancel_left (a := (a.step : Int)) (k : Int) hne
  rw [hdiv]
  exact Int.toNat_natCast k

private theorem intersection_step_pos (a b : Progression) (ha : 0 < a.step)
    (hn : 0 < (intersection a b).n) : 0 < (intersection a b).step := by
  rcases a with ⟨alo, ast, an⟩
  rcases b with ⟨blo, bst, bn⟩
  simp only at ha hn ⊢
  by_cases han : an = 0
  · simp [intersection, han] at hn
  · by_cases hbn : bn = 0
    · simp [intersection, han, hbn] at hn
    · by_cases hast : ast = 0
      · omega
      · by_cases hbst : bst = 0
        · simp [intersection, han, hbn, hast, hbst]
        · have hgpos : 0 < ast.gcd bst := Nat.gcd_pos_of_pos_left bst ha
          have hastdiv : 0 < ast / ast.gcd bst := Nat.div_gcd_pos_of_pos_left bst ha
          have hperiod : 0 < (ast / ast.gcd bst) * bst := Nat.mul_pos hastdiv (Nat.pos_of_ne_zero hbst)
          by_cases hgapmod : (blo - alo) % (ast.gcd bst : Int) = 0
          · simp [intersection, han, hbn, hast, hbst, hgapmod]
            exact hperiod
          · simp [intersection, han, hbn, hast, hbst, hgapmod] at hn

private theorem count_prog_mem (a b : Progression) :
    (List.range a.n).countP (fun k => contains b (progVal a k)) =
      (intersection a b).n * (if a.step == 0 then a.n else 1) := by
  by_cases han : a.n = 0
  · have hrange : List.range a.n = [] := by rw [han]; simp
    rw [hrange]
    simp [intersection, han]
  · have hanp : 0 < a.n := Nat.pos_of_ne_zero han
    by_cases haz : a.step = 0
    · have hconst : ∀ k, progVal a k = a.lo := by
        intro k
        simp [progVal, haz]
      rw [show (List.range a.n).countP (fun k => contains b (progVal a k)) =
          (List.range a.n).countP (fun _ => contains b a.lo) by
        congr 1
        funext k
        rw [hconst k]]
      rw [countP_const_bool]
      by_cases hbn : b.n = 0
      · have hbfalse : contains b a.lo = false := by
          simp [contains, hbn]
        simp [intersection, han, haz, hbn, hbfalse]
      · rw [show intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ by
          dsimp [intersection]
          simp [han, haz, hbn]]
        simp [haz]
        by_cases hc : contains b a.lo = true <;> simp [hc]
    · have hapos : 0 < a.step := Nat.pos_of_ne_zero haz
      by_cases hin : (intersection a b).n = 0
      · rw [hin]
        simp
        intro k hk
        by_cases hkb : contains b (progVal a k) = true
        · have hka : contains a (progVal a k) = true := progVal_mem a k hk
          have hki : contains (intersection a b) (progVal a k) = true :=
            (intersection_contains a b (progVal a k)).mpr ⟨hka, hkb⟩
          obtain ⟨m, hm, hval⟩ := (contains_iff_mem (intersection a b) (progVal a k)).mp hki
          omega
        · exact Bool.eq_false_of_ne_true hkb
      · have hinp : 0 < (intersection a b).n := Nat.pos_of_ne_zero hin
        have hipos : 0 < (intersection a b).step := intersection_step_pos a b hapos hinp
        let i := intersection a b
        let idx : Nat → Nat := fun m => ((progVal i m - a.lo) / (a.step : Int)).toNat
        have hidx_lt : ∀ m, m < i.n → idx m < a.n := by
          intro m hm
          have him : contains i (progVal i m) = true := progVal_mem i m hm
          have hia : contains a (progVal i m) = true :=
            ((intersection_contains a b (progVal i m)).mp him).1
          obtain ⟨k, hk, hkv⟩ := (contains_iff_mem a (progVal i m)).mp hia
          have hidx : idx m = k := by
            dsimp [idx]
            rw [hkv]
            exact idx_of_progVal hapos hk
          rw [hidx]
          exact hk
        have hidx_val : ∀ m, m < i.n → progVal a (idx m) = progVal i m := by
          intro m hm
          have him : contains i (progVal i m) = true := progVal_mem i m hm
          have hia : contains a (progVal i m) = true :=
            ((intersection_contains a b (progVal i m)).mp him).1
          obtain ⟨k, hk, hkv⟩ := (contains_iff_mem a (progVal i m)).mp hia
          have hidx : idx m = k := by
            dsimp [idx]
            rw [hkv]
            exact idx_of_progVal hapos hk
          rw [hidx, hkv]
        have hidx_inj : ∀ m1, m1 < i.n → ∀ m2, m2 < i.n → idx m1 = idx m2 → m1 = m2 := by
          intro m1 hm1 m2 hm2 heq
          have hv := congrArg (progVal a) heq
          rw [hidx_val m1 hm1, hidx_val m2 hm2] at hv
          exact progVal_inj hipos hm1 hm2 hv
        let la := (List.range a.n).filter (fun k => contains b (progVal a k))
        let li := (List.range i.n).map idx
        have hlaNodup : la.Nodup := by
          dsimp [la]
          exact nodup_filter _ List.nodup_range
        have hliNodup : li.Nodup := by
          dsimp [li]
          apply nodup_map_of_inj
          · exact List.nodup_range
          · intro m1 hm1 m2 hm2 heq
            exact hidx_inj m1 (List.mem_range.mp hm1) m2 (List.mem_range.mp hm2) heq
        have hmem : ∀ k, k ∈ la ↔ k ∈ li := by
          intro k
          constructor
          · intro hk
            have hk' : k < a.n ∧ contains b (progVal a k) = true := by
              simpa [la] using hk
            have hka : contains a (progVal a k) = true := progVal_mem a k hk'.1
            have hki : contains i (progVal a k) = true :=
              (intersection_contains a b (progVal a k)).mpr ⟨hka, hk'.2⟩
            obtain ⟨m, hm, hmv⟩ := (contains_iff_mem i (progVal a k)).mp hki
            have hidx : idx m = k := by
              dsimp [idx]
              rw [← hmv]
              exact idx_of_progVal hapos hk'.1
            apply List.mem_map.mpr
            exact ⟨m, List.mem_range.mpr hm, hidx⟩
          · intro hk
            obtain ⟨m, hm, hmk⟩ := List.mem_map.mp hk
            have hmlt : m < i.n := List.mem_range.mp hm
            have hklt : k < a.n := by rw [← hmk]; exact hidx_lt m hmlt
            have him : contains i (progVal i m) = true := progVal_mem i m hmlt
            have hib : contains b (progVal i m) = true :=
              ((intersection_contains a b (progVal i m)).mp him).2
            have hval := hidx_val m hmlt
            have hbval : contains b (progVal a k) = true := by
              rw [← hmk]
              rw [hval]
              exact hib
            simpa [la] using And.intro hklt hbval
        have hp := perm_of_nodup_mem hlaNodup hliNodup hmem
        have hlen : la.length = li.length := hp.length_eq
        dsimp [la, li] at hlen
        rw [List.countP_eq_length_filter]
        rw [hlen]
        simp
        simp [haz]
        rfl

-- =====================================================================
-- Lane-level aggregates
-- =====================================================================

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem values_progVal_nonneg (a : Lane) (hs : 0 ≤ a.step) (i : Nat) :
    progVal (values a) i = a.base + (i : Int) * a.step := by
  unfold values progVal
  have hneg : ¬ a.step < 0 := by omega
  simp [hneg]
  have hnat : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg hs
  rw [hnat]

private theorem values_progVal_neg (a : Lane) (hs : a.step < 0) (i : Nat) (hi : i < a.n) :
    progVal (values a) i = a.base + a.step * ((a.n - 1 - i : Nat) : Int) := by
  unfold values progVal
  have htrue : a.step < 0 := hs
  simp [htrue]
  have hnat : (a.step.natAbs : Int) = -a.step := by
    calc
      (a.step.natAbs : Int) = ((-a.step).natAbs : Int) := by rw [Int.natAbs_neg]
      _ = -a.step := Int.natAbs_of_nonneg (by omega)
  rw [hnat]
  have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
  rw [hcast]
  grind

private theorem countP_reverse (n : Nat) (f : Nat → Bool) :
    (List.range n).countP (fun i => f (n - 1 - i)) = (List.range n).countP f := by
  let g : Nat → Nat := fun i => n - 1 - i
  have hgNodup : ((List.range n).map g).Nodup := by
    apply nodup_map_of_inj
    · exact List.nodup_range
    · intro i hi j hj heq
      dsimp [g] at heq
      have hi' : i < n := List.mem_range.mp hi
      have hj' : j < n := List.mem_range.mp hj
      omega
  have hmem : ∀ x, x ∈ (List.range n).map g ↔ x ∈ List.range n := by
    intro x
    constructor
    · intro hx
      obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
      have hi' : i < n := List.mem_range.mp hi
      apply List.mem_range.mpr
      dsimp [g]
      omega
    · intro hx
      have hx' : x < n := List.mem_range.mp hx
      apply List.mem_map.mpr
      refine ⟨n - 1 - x, ?_, ?_⟩
      · apply List.mem_range.mpr
        omega
      · dsimp [g]
        omega
  have hp : ((List.range n).map g).Perm (List.range n) :=
    perm_of_nodup_mem hgNodup List.nodup_range hmem
  have hcount := List.Perm.countP_eq f hp
  simpa [g, Function.comp_def] using hcount

private theorem count_prog_hit (p : Progression) (hp : 0 < p.step) (v : Int) :
    (List.range p.n).countP (fun (i : Nat) => v == progVal p i) =
      if contains p v then 1 else 0 := by
  let f := (List.range p.n).filter (fun (i : Nat) => v == progVal p i)
  have hfNodup : f.Nodup := by
    dsimp [f]
    exact nodup_filter _ List.nodup_range
  have hLon (i : Nat) (hi : i < p.n) : (v == progVal p i) = true ↔ v = progVal p i := beq_iff_eq
  by_cases hm : contains p v = true
  · obtain ⟨i, hi, hiv⟩ := (contains_iff_mem p v).mp hm
    have hsing : f.Perm [i] := by
      apply perm_of_nodup_mem hfNodup (by simp)
      intro x
      constructor
      · intro hx
        have hx' := List.mem_filter.mp hx
        have hxv : v = progVal p x := (hLon x (List.mem_range.mp hx'.1)).mp hx'.2
        have hieq : x = i := by
          unfold progVal at hiv hxv
          have heq : (x : Int) * (p.step : Int) = (i : Int) * (p.step : Int) := by omega
          apply Int.ofNat_inj.mp
          exact Int.eq_of_mul_eq_mul_right (by exact_mod_cast (Nat.ne_of_gt hp)) heq
        simp [hieq]
      · intro hx
        have hxi : x = i := by simpa using hx
        subst x
        apply List.mem_filter.mpr
        exact ⟨List.mem_range.mpr hi, (hLon i hi).mpr hiv⟩
    rw [List.countP_eq_length_filter]
    change f.length = _
    rw [hsing.length_eq]
    simp [hm]
  · have hfNil : f = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro x hx
      have hx' := List.mem_filter.mp hx
      exact hm ((contains_iff_mem p v).mpr
        ⟨x, List.mem_range.mp hx'.1, (hLon x (List.mem_range.mp hx'.1)).mp hx'.2⟩)
    rw [List.countP_eq_length_filter]
    change f.length = _
    rw [hfNil]
    simp [hm]

private theorem count_value (b : Lane) (v : Int) :
    (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
      (if contains (values b) v then 1 else 0) * (if b.step == 0 then b.n else 1) := by
  by_cases hbn : b.n = 0
  · have hcvfalse : contains (values b) v = false := by
      simp [contains, values, hbn]
    simp [hbn, hcvfalse]
  · have hbnp : 0 < b.n := Nat.pos_of_ne_zero hbn
    by_cases hbst : b.step = 0
    · have hconst : ∀ (i : Nat), b.base + b.step * (i : Int) = b.base := by
        intro i
        simp [hbst]
      rw [show (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
          (List.range b.n).countP (fun (_ : Nat) => v == b.base) by
        apply List.countP_congr
        intro i hi
        rw [hconst i]]
      rw [countP_const_bool]
      have hcv : contains (values b) v = true ↔ (v == b.base) = true := by
        unfold values
        by_cases hneg : b.step < 0 <;> simp [hbst, hneg, contains, lastVal] <;> omega
      by_cases h : v == b.base
      · have hc : contains (values b) v = true := hcv.mpr h
        simp [h, hc, hbst]
      · have hc : contains (values b) v = false := by
          by_cases hb : contains (values b) v = true
          · exact False.elim (h (beq_iff_eq.mp (hcv.mp hb)))
          · exact Bool.eq_false_of_ne_true hb
        simp [h, hc, hbst]
    · have hsteppos : 0 < (values b).step := by
        unfold values
        exact Int.natAbs_pos.mpr hbst
      have hhit : (List.range b.n).countP (fun (i : Nat) => v == progVal (values b) i) =
          (if contains (values b) v then 1 else 0) := by
        simpa [values] using count_prog_hit (values b) hsteppos v
      by_cases hneg : b.step < 0
      · have hrev : (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
            (List.range b.n).countP (fun (i : Nat) => v == progVal (values b) i) := by
          symm
          rw [← countP_reverse b.n (fun (i : Nat) => v == progVal (values b) i)]
          apply List.countP_congr
          intro i hi
          have hi' : i < b.n := List.mem_range.mp hi
          rw [values_progVal_neg b hneg (b.n - 1 - i) (by omega)]
          have hsub : b.n - 1 - (b.n - 1 - i) = i := by omega
          rw [hsub]
        rw [hrev, hhit]
        simp [hbst]
      · have hnonneg : 0 ≤ b.step := by omega
        have heq : (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
            (List.range b.n).countP (fun (i : Nat) => v == progVal (values b) i) := by
          apply List.countP_congr
          intro i hi
          have hi' : i < b.n := List.mem_range.mp hi
          rw [values_progVal_nonneg b hnonneg i]
          grind
        rw [heq, hhit]
        simp [hbst]

private theorem sum_map_ite_mul {α : Type} (l : List α) (p : α → Bool) (m : Nat) :
    (l.map (fun x => (if p x then 1 else 0) * m)).sum = l.countP p * m := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> grind

private theorem count_lane_values (a b : Lane) :
    (List.range a.n).countP (fun (j : Nat) => contains (values b) (a.base + a.step * (j : Int))) =
      (intersection (values a) (values b)).n * (if a.step == 0 then a.n else 1) := by
  by_cases haz : a.step = 0
  · have heq : ∀ (j : Nat), j < a.n →
        progVal (values a) j = a.base + a.step * (j : Int) := by
      intro j hj
      rw [values_progVal_nonneg a (by omega) j]
      simp [haz]
    rw [show (List.range a.n).countP (fun (j : Nat) => contains (values b) (a.base + a.step * (j : Int))) =
        (List.range a.n).countP (fun (j : Nat) => contains (values b) (progVal (values a) j)) by
      apply List.countP_congr
      intro j hj
      rw [heq j (List.mem_range.mp hj)]]
    simpa [values, haz] using count_prog_mem (values a) (values b)
  · by_cases hneg : a.step < 0
    · have heq : ∀ (j : Nat), j < a.n →
          progVal (values a) (a.n - 1 - j) = a.base + a.step * (j : Int) := by
        intro j hj
        rw [values_progVal_neg a hneg (a.n - 1 - j) (by omega)]
        have hsub : a.n - 1 - (a.n - 1 - j) = j := by omega
        rw [hsub]
      rw [show (List.range a.n).countP (fun (j : Nat) => contains (values b) (a.base + a.step * (j : Int))) =
          (List.range a.n).countP (fun (j : Nat) => contains (values b) (progVal (values a) j)) by
        symm
        rw [← countP_reverse a.n (fun (j : Nat) => contains (values b) (progVal (values a) j))]
        apply List.countP_congr
        intro j hj
        rw [heq j (List.mem_range.mp hj)]]
      have hvstep : (values a).step = a.step.natAbs := by rfl
      have hvzero : ((values a).step == 0) = false := by
        simp [hvstep]
        exact haz
      simpa [values, hvzero] using count_prog_mem (values a) (values b)
    · have hnonneg : 0 ≤ a.step := by omega
      have heq : ∀ (j : Nat), j < a.n →
          progVal (values a) j = a.base + a.step * (j : Int) := by
        intro j hj
        rw [values_progVal_nonneg a hnonneg j]
        grind
      rw [show (List.range a.n).countP (fun (j : Nat) => contains (values b) (a.base + a.step * (j : Int))) =
          (List.range a.n).countP (fun (j : Nat) => contains (values b) (progVal (values a) j)) by
        apply List.countP_congr
        intro j hj
        rw [heq j (List.mem_range.mp hj)]]
      have hvzero : ((values a).step == 0) = false := by
        simp [values]
        exact haz
      simpa [values, hvzero] using count_prog_mem (values a) (values b)

private theorem laneJoinCount (a b : Lane) :
    ((lanePoints a).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum = joinCount a b := by
  let mb : Nat := if b.step == 0 then b.n else 1
  have hcv : ∀ (j : Nat),
      (List.range b.n).countP (fun (i : Nat) => (a.base + (j : Int) * a.step) == b.base + b.step * (i : Int)) =
        (if contains (values b) (a.base + a.step * (j : Int)) then 1 else 0) * mb := by
    intro j
    dsimp [mb]
    simpa [Int.mul_comm] using count_value b (a.base + a.step * (j : Int))
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map (fun (j : Nat) => (List.range b.n).countP
      (fun (i : Nat) => (a.base + (j : Int) * a.step) == b.base + b.step * (i : Int)))).sum = _
  rw [show (List.range a.n).map (fun (j : Nat) => (List.range b.n).countP
      (fun (i : Nat) => (a.base + (j : Int) * a.step) == b.base + b.step * (i : Int))) =
      (List.range a.n).map (fun (j : Nat) =>
        (if contains (values b) (a.base + a.step * (j : Int)) then 1 else 0) * mb) by
    apply List.map_congr_left
    intro j hj
    exact hcv j]
  rw [sum_map_ite_mul]
  rw [count_lane_values a b]
  unfold joinCount
  dsimp [mb]

-- =====================================================================
-- MirroredTail rows are the two lane point lists
-- =====================================================================

private def mirrorA (c : Config) : Lane :=
  span c 0 c.split c.base c.step
private def mirrorB (c : Config) : Lane :=
  span c c.split c.n (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) (-c.step)

private theorem mirror_lanes (c : Config) :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.layout.lanes c = [mirrorA c, mirrorB c] := by
  rfl

private theorem rows_mirror (c : Config) :
    rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.layout c =
      lanePoints (mirrorA c) ++ lanePoints (mirrorB c) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun i =>
      (TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).map (fun (i : Nat) =>
        (i, if i < c.split then c.base + c.step * (i : Int)
          else c.base + c.step * ((c.n - 1 - i : Nat) : Int))) by
    apply range_filterMap_all
    intro i hi
    by_cases hlt : i < c.split <;>
      simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.layout, hlt]]
  by_cases hsc : c.split ≤ c.n
  · have hna : (mirrorA c).n = c.split := by
      unfold mirrorA span lane
      simp [Nat.min_eq_right hsc]
    have hnb : (mirrorB c).n = c.n - c.split := by
      unfold mirrorB span lane
      simp [Nat.min_eq_left]
    have hstartb : (mirrorB c).start = c.split := by rfl
    have hstepb : (mirrorB c).step = -c.step := by rfl
    have hbaseb : (mirrorB c).base = c.base + c.step * ((c.n - 1 - c.split : Nat) : Int) := by rfl
    unfold lanePoints
    simp only [hna, hnb, hstartb, hstepb, hbaseb]
    have hsplit : mirrorA c = ⟨0, 1, c.split, c.base, c.step⟩ := by
      unfold mirrorA span lane
      simp [Nat.min_eq_right hsc]
    have hsplitb : mirrorB c = ⟨c.split, 1, c.n - c.split,
        c.base + c.step * ((c.n - 1 - c.split : Nat) : Int), -c.step⟩ := by
      unfold mirrorB span lane
      simp [Nat.min_eq_left]
    rw [hsplit, hsplitb]
    simp only
    have hrange : List.range c.n = List.range c.split ++
        (List.range (c.n - c.split)).map (fun x => c.split + x) := by
      calc
        List.range c.n = List.range (c.split + (c.n - c.split)) := by rw [Nat.add_sub_of_le hsc]
        _ = List.range c.split ++ (List.range (c.n - c.split)).map (fun x => c.split + x) := List.range_add
    rw [hrange]
    simp only [List.map_append, List.map_map, Function.comp_apply]
    congr 1
    · apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi']
      grind
    · apply List.map_congr_left
      intro i hi
      have hi' : i < c.n - c.split := List.mem_range.mp hi
      have hnot : ¬ c.split + i < c.split := by omega
      have hsub : c.n - 1 - (c.split + i) = c.n - 1 - c.split - i := by omega
      simp [hnot, hsub]
      have hle : c.n - 1 - c.split - i + i = c.n - 1 - c.split := by omega
      grind
  · have hnlt : c.n < c.split := by omega
    have hna : (mirrorA c).n = c.n := by
      unfold mirrorA span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    have hnb : (mirrorB c).n = 0 := by
      unfold mirrorB span lane
      simp
      omega
    have hsplit : mirrorA c = ⟨0, 1, c.n, c.base, c.step⟩ := by
      unfold mirrorA span lane
      simp [Nat.min_eq_left (by omega : c.n ≤ c.split)]
    unfold lanePoints
    rw [hsplit]
    simp only [hnb, List.range_zero, List.map_nil, List.append_nil]
    apply List.map_congr_left
    intro i hi
    have hi' : i < c.n := List.mem_range.mp hi
    have hlt : i < c.split := by omega
    simp [hlt]
    grind

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.ResidueJoin.Goal := by
  intro c b
  rw [rows_mirror, mirror_lanes]
  simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  rw [laneJoinCount (mirrorA c) b, laneJoinCount (mirrorB c) b]
  rfl

-- =====================================================================
-- Overlap index permutation for two progressions
-- =====================================================================

private theorem overlap_perm (pa pb : Progression) (ha : 0 < pa.step) :
    let i := intersection pa pb
    ((List.range pa.n).filter (fun k => contains pb (progVal pa k))).Perm
      ((List.range i.n).map (fun m => ((progVal i m - pa.lo) / (pa.step : Int)).toNat)) := by
  dsimp only
  let i := intersection pa pb
  let idx : Nat → Nat := fun m => ((progVal i m - pa.lo) / (pa.step : Int)).toNat
  by_cases hin : i.n = 0
  · have hleft : (List.range pa.n).filter (fun k => contains pb (progVal pa k)) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro k hk
      have hk' := List.mem_filter.mp hk
      have hpa : contains pa (progVal pa k) = true := progVal_mem pa k (List.mem_range.mp hk'.1)
      have hpi : contains i (progVal pa k) = true :=
        (intersection_contains pa pb (progVal pa k)).mpr ⟨hpa, hk'.2⟩
      obtain ⟨m, hm, hmv⟩ := (contains_iff_mem i (progVal pa k)).mp hpi
      omega
    rw [hleft, show (List.range i.n).map idx = [] by simp [hin]]
  · have hinp : 0 < i.n := Nat.pos_of_ne_zero hin
    have hipos : 0 < i.step := intersection_step_pos pa pb ha hinp
    have hidx_lt : ∀ m, m < i.n → idx m < pa.n := by
      intro m hm
      have him : contains i (progVal i m) = true := progVal_mem i m hm
      have hia : contains pa (progVal i m) = true :=
        ((intersection_contains pa pb (progVal i m)).mp him).1
      obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pa (progVal i m)).mp hia
      have hidx : idx m = k := by
        dsimp [idx]
        rw [hkv]
        exact idx_of_progVal ha hk
      rw [hidx]
      exact hk
    have hidx_val : ∀ m, m < i.n → progVal pa (idx m) = progVal i m := by
      intro m hm
      have him : contains i (progVal i m) = true := progVal_mem i m hm
      have hia : contains pa (progVal i m) = true :=
        ((intersection_contains pa pb (progVal i m)).mp him).1
      obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pa (progVal i m)).mp hia
      have hidx : idx m = k := by
        dsimp [idx]
        rw [hkv]
        exact idx_of_progVal ha hk
      rw [hidx, hkv]
    have hidx_inj : ∀ m1, m1 < i.n → ∀ m2, m2 < i.n → idx m1 = idx m2 → m1 = m2 := by
      intro m1 hm1 m2 hm2 heq
      have hv := congrArg (progVal pa) heq
      rw [hidx_val m1 hm1, hidx_val m2 hm2] at hv
      exact progVal_inj hipos hm1 hm2 hv
    let l₁ := (List.range pa.n).filter (fun k => contains pb (progVal pa k))
    let l₂ := (List.range i.n).map idx
    have hn1 : l₁.Nodup := by
      dsimp [l₁]
      exact nodup_filter _ List.nodup_range
    have hn2 : l₂.Nodup := by
      dsimp [l₂]
      apply nodup_map_of_inj
      · exact List.nodup_range
      · intro m1 hm1 m2 hm2 heq
        exact hidx_inj m1 (List.mem_range.mp hm1) m2 (List.mem_range.mp hm2) heq
    have hmem : ∀ k, k ∈ l₁ ↔ k ∈ l₂ := by
      intro k
      constructor
      · intro hk
        have hk' := List.mem_filter.mp hk
        have hpa : contains pa (progVal pa k) = true := progVal_mem pa k (List.mem_range.mp hk'.1)
        have hpi : contains i (progVal pa k) = true :=
          (intersection_contains pa pb (progVal pa k)).mpr ⟨hpa, hk'.2⟩
        obtain ⟨m, hm, hmv⟩ := (contains_iff_mem i (progVal pa k)).mp hpi
        have hidx : idx m = k := by
          dsimp [idx]
          rw [← hmv]
          exact idx_of_progVal ha (List.mem_range.mp hk'.1)
        apply List.mem_map.mpr
        exact ⟨m, List.mem_range.mpr hm, hidx⟩
      · intro hk
        obtain ⟨m, hm, hmk⟩ := List.mem_map.mp hk
        have hmlt : m < i.n := List.mem_range.mp hm
        have hklt : k < pa.n := by rw [← hmk]; exact hidx_lt m hmlt
        have him : contains i (progVal i m) = true := progVal_mem i m hmlt
        have hib : contains pb (progVal i m) = true := by
          exact ((intersection_contains pa pb (progVal i m)).mp him).2
        have hval := hidx_val m hmlt
        have hbval : contains pb (progVal pa k) = true := by
          rw [← hmk]
          rw [hval]
          exact hib
        apply List.mem_filter.mpr
        exact ⟨List.mem_range.mpr hklt, hbval⟩
    simpa [l₁, l₂, idx] using perm_of_nodup_mem hn1 hn2 hmem

-- =====================================================================
-- Deleted-sum lane lemma (stride one)
-- =====================================================================

private theorem laneDeletedSum_stride1 (a : Lane) (ha : a.stride = 1) (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  let pa : Progression := positions a
  let i : Progression := intersection pa mask
  let idx : Nat → Nat := fun m => ((progVal i m - pa.lo) / (pa.step : Int)).toNat
  have hpa_step : pa.step = a.stride := by rfl
  have hpa_lo : pa.lo = a.start := by rfl
  have hpa_pos : 0 < pa.step := by rw [hpa_step, ha]; decide
  have hpperm := overlap_perm pa mask hpa_pos
  have hpperm' : ((List.range pa.n).filter (fun j => contains mask (progVal pa j))).Perm
      ((List.range i.n).map idx) := by
    simpa [i, pa, idx] using hpperm
  have hprog : ∀ j, j < a.n → progVal pa j = a.start + j := by
    intro j hj
    simp [pa, progVal, positions, ha]
  have htotal : ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
    unfold lanePoints
    rw [List.map_map]
    change ((List.range a.n).map (fun j : Nat => a.base + (j : Int) * a.step)).sum = partialSum a a.n
    rw [partialSum_eq_sum]
  have hidx_of_mem : ∀ m, m < i.n → idx m = (progVal i m - pa.lo).toNat := by
    intro m hm
    dsimp [idx]
    have hp : (pa.step : Int) = 1 := by simp [hpa_step, ha]
    rw [hp]
    simp
  have hi_ge : ∀ m, m < i.n → pa.lo ≤ progVal i m := by
    intro m hm
    have him : contains i (progVal i m) = true := progVal_mem i m hm
    have hpa : contains pa (progVal i m) = true :=
      ((intersection_contains pa mask (progVal i m)).mp him).1
    obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pa (progVal i m)).mp hpa
    unfold progVal at hkv
    change pa.lo ≤ i.lo + (m : Int) * i.step
    rw [hkv]
    have hpnon : (0 : Int) ≤ (pa.step : Int) := by omega
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hpnon)
  have hterm : ∀ m, m < i.n →
      a.base + (idx m : Int) * a.step = firstAt a i + (m : Int) * slopeAt a i := by
    intro m hm
    have hge := hi_ge m hm
    have hidx := hidx_of_mem m hm
    have hcast : ((progVal i m - pa.lo).toNat : Int) = progVal i m - pa.lo :=
      Int.toNat_of_nonneg (by omega)
    have hp1 : (pa.step : Int) = 1 := by simp [hpa_step, ha]
    have hpa_lo' : pa.lo = (a.start : Int) := by rfl
    rw [hpa_lo'] at hcast
    unfold firstAt slopeAt
    rw [ha]
    dsimp
    rw [hidx]
    rw [hpa_lo']
    rw [hcast]
    unfold progVal at hcast ⊢
    simp
    grind
  have hsumdel : (((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd).sum =
      deletedSum a mask := by
    have hlist : ((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd =
        ((List.range pa.n).filter (fun j => contains mask (progVal pa j))).map
          (fun (j : Nat) => a.base + (j : Int) * a.step) := by
      unfold lanePoints
      rw [List.filter_map]
      rw [List.map_map]
      simp only [Function.comp_apply, ha, Nat.mul_one]
      change ((List.range a.n).filter (fun (j : Nat) => contains mask (a.start + (j : Int)))).map
          (fun (j : Nat) => a.base + (j : Int) * a.step) =
        ((List.range pa.n).filter (fun j => contains mask (progVal pa j))).map
          (fun (j : Nat) => a.base + (j : Int) * a.step)
      have hfilt : (List.range a.n).filter (fun (j : Nat) => contains mask (a.start + (j : Int))) =
          (List.range pa.n).filter (fun j => contains mask (progVal pa j)) := by
        apply List.filter_congr
        intro j hj
        rw [hprog j (List.mem_range.mp hj)]
      rw [hfilt]
    rw [hlist]
    have hpv := hpperm'.map (fun (j : Nat) => a.base + (j : Int) * a.step)
    rw [list_sum_perm hpv]
    rw [show ((List.range i.n).map idx).map (fun (j : Nat) => a.base + (j : Int) * a.step) =
        (List.range i.n).map (fun (m : Nat) => firstAt a i + (m : Int) * slopeAt a i) by
      rw [List.map_map]
      apply List.map_congr_left
      intro m hm
      exact hterm m (List.mem_range.mp hm)]
    rw [← partialSum_eq_sum ⟨0, 1, i.n, firstAt a i, slopeAt a i⟩ i.n]
    rfl
  have hcompl := sum_filter_add_compl (lanePoints a) (fun r => contains mask r.1) Prod.snd
  rw [← hcompl] at htotal
  have hdeldef : deletedSum a mask = (i.n : Int) * firstAt a i + tri i.n * slopeAt a i := by rfl
  omega

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.DeleteProgression.Goal := by
  intro c mask
  rw [rows_mirror, mirror_lanes]
  simp only [List.filter_append, List.map_append, List.sum_append,
    List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  rw [laneDeletedSum_stride1 (mirrorA c) rfl mask,
      laneDeletedSum_stride1 (mirrorB c) rfl mask]
  simp

-- =====================================================================
-- Aligned product lane lemma (stride one)
-- =====================================================================

private theorem positions_progVal (a : Lane) (i : Nat) :
    progVal (positions a) i = a.start + i * a.stride := by
  rfl

private theorem inner_aligned (a b : Lane) (hb : 0 < b.stride) (j : Nat) (hj : j < a.n) :
    ((List.range b.n).map (fun (i : Nat) =>
      if a.start + j == b.start + i * b.stride then
        (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
      else 0)).sum =
    if contains (positions b) (a.start + j) then
      (a.base + (j : Int) * a.step) *
        (b.base + b.step * (((((a.start + j) - b.start) / b.stride : Nat) : Int)))
    else 0 := by
  let p := a.start + j
  let f := (List.range b.n).filter (fun (i : Nat) => p == b.start + i * b.stride)
  by_cases hm : contains (positions b) p = true
  · obtain ⟨i0, hi0, hi0v⟩ := (contains_iff_mem (positions b) p).mp hm
    have hi0v' : p = b.start + i0 * b.stride := by
      rw [positions_progVal] at hi0v
      exact_mod_cast hi0v
    have hfPerm : f.Perm [i0] := by
      apply perm_of_nodup_mem
      · dsimp [f]
        exact nodup_filter _ List.nodup_range
      · simp
      · intro x
        constructor
        · intro hx
          have hx' := List.mem_filter.mp hx
          have hxv : p == b.start + x * b.stride := hx'.2
          have hxv' : p = b.start + x * b.stride := beq_iff_eq.mp hxv
          have hxi : x = i0 := by
            have hmuli : x * b.stride = i0 * b.stride := by omega
            exact Nat.eq_of_mul_eq_mul_right hb hmuli
          simp [hxi]
        · intro hx
          have hxi : x = i0 := by simpa using hx
          subst x
          apply List.mem_filter.mpr
          exact ⟨List.mem_range.mpr hi0, beq_iff_eq.mpr hi0v'⟩
    rw [← sum_map_ite_zero]
    rw [list_sum_perm (hfPerm.map (fun (i : Nat) => (a.base + (j : Int) * a.step) *
        (b.base + b.step * (i : Int))))]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    have hdiv : (((p - b.start) / b.stride : Nat) : Int) = (i0 : Int) := by
      have hsub : p - b.start = i0 * b.stride := by omega
      rw [hsub]
      rw [Nat.mul_comm]
      rw [Nat.mul_div_right i0 hb]
    rw [hdiv]
    have hm' : contains (positions b) (a.start + j) = true := by simpa [p] using hm
    rw [if_pos hm']
    simp
  · have hfNil : f = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro x hx
      have hx' := List.mem_filter.mp hx
      have hxv : p = b.start + x * b.stride := beq_iff_eq.mp hx'.2
      have hxp : contains (positions b) p = true := by
        apply (contains_iff_mem (positions b) p).mpr
        refine ⟨x, List.mem_range.mp hx'.1, ?_⟩
        rw [positions_progVal]
        exact_mod_cast hxv
      exact hm hxp
    rw [← sum_map_ite_zero]
    change (((List.range b.n).filter (fun (i : Nat) => p == b.start + i * b.stride)).map
      (fun (i : Nat) => (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int)))).sum = _
    dsimp [f] at hfNil
    rw [hfNil]
    have hm' : ¬ contains (positions b) (a.start + j) = true := by simpa [p] using hm
    rw [if_neg hm']
    simp

private theorem laneAligned_stride1 (a b : Lane) (ha : a.stride = 1) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun (i : Nat) =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned a b := by
  let pa : Progression := positions a
  let pb : Progression := positions b
  let i : Progression := intersection pa pb
  let idx : Nat → Nat := fun m => ((progVal i m - pa.lo) / (pa.step : Int)).toNat
  let jdx : Nat → Nat := fun m => ((progVal i m - pb.lo) / (pb.step : Int)).toNat
  have hpa_pos : 0 < pa.step := by change 0 < a.stride; rw [ha]; decide
  have hpbpos : 0 < pb.step := by change 0 < b.stride; exact hb
  have hpperm := overlap_perm pa pb hpa_pos
  have hpperm' : ((List.range pa.n).filter (fun j => contains pb (progVal pa j))).Perm
      ((List.range i.n).map idx) := by simpa [pa, pb, i, idx] using hpperm
  have hterm_a : ∀ m, m < i.n → a.base + (idx m : Int) * a.step = firstAt a i + (m : Int) * slopeAt a i := by
    intro m hm
    have him : contains i (progVal i m) = true := progVal_mem i m hm
    have hia : contains pa (progVal i m) = true := ((intersection_contains pa pb (progVal i m)).mp him).1
    obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pa (progVal i m)).mp hia
    have hidx : idx m = k := by
      dsimp [idx]
      rw [hkv]
      exact idx_of_progVal hpa_pos hk
    have hkval : (k : Int) = i.lo + (m : Int) * i.step - (a.start : Int) := by
      unfold progVal at hkv
      have hpalo : pa.lo = (a.start : Int) := by rfl
      have hpast : pa.step = a.stride := by rfl
      rw [hpalo, hpast, ha] at hkv
      simp at hkv
      omega
    rw [hidx, hkval]
    unfold firstAt slopeAt
    rw [ha]
    dsimp
    simp
    grind
  have hterm_b : ∀ m, m < i.n → b.base + (jdx m : Int) * b.step = firstAt b i + (m : Int) * slopeAt b i := by
    have hpblo : pb.lo = (b.start : Int) := by rfl
    intro m hm
    have him : contains i (progVal i m) = true := progVal_mem i m hm
    have hiNpos : 0 < i.n := by omega
    have hi0mem : contains i i.lo = true := by
      have h0 := progVal_mem i 0 hiNpos
      simpa [progVal] using h0
    obtain ⟨k0, hk0, hk0v⟩ := (contains_iff_mem pb i.lo).mp
      (((intersection_contains pa pb i.lo).mp hi0mem).2)
    have hk0eq : i.lo - pb.lo = (k0 : Int) * (pb.step : Int) := by
      unfold progVal at hk0v
      omega
    by_cases hi1 : i.n = 1
    · have hm0 : m = 0 := by omega
      subst m
      have hjdx0 : jdx 0 = k0 := by
        dsimp [jdx]
        rw [show progVal i 0 = i.lo by simp [progVal], hk0eq]
        rw [Int.mul_comm]
        rw [Int.mul_ediv_cancel_left (a := (pb.step : Int)) (k0 : Int) (by omega)]
        exact Int.toNat_natCast k0
      rw [hjdx0]
      unfold firstAt slopeAt
      dsimp
      have hk0eq' : i.lo - (b.start : Int) = (k0 : Int) * (b.stride : Int) := by
        rw [← hpblo, hk0eq]
        rfl
      rw [hk0eq']
      have hdiv : ((k0 : Int) * (b.stride : Int)) / (b.stride : Int) = (k0 : Int) := by
        rw [Int.mul_comm]
        exact Int.mul_ediv_cancel_left (a := (b.stride : Int)) (k0 : Int) (by omega)
      rw [hdiv]
      simp
    · have hi1lt : 1 < i.n := by omega
      have hi1mem : contains i (progVal i 1) = true := progVal_mem i 1 hi1lt
      obtain ⟨k1, hk1, hk1v⟩ := (contains_iff_mem pb (progVal i 1)).mp
        (((intersection_contains pa pb (progVal i 1)).mp hi1mem).2)
      have hk1eq : progVal i 1 - pb.lo = (k1 : Int) * (pb.step : Int) := by
        unfold progVal at hk1v
        change i.lo + (1 : Int) * i.step - pb.lo = (k1 : Int) * (pb.step : Int)
        omega
      have hk1eq' : i.lo + (i.step : Int) - pb.lo = (k1 : Int) * (pb.step : Int) := by
        rw [show progVal i 1 = i.lo + (1 : Int) * i.step by rfl] at hk1eq
        simpa using hk1eq
      have hstepInt : (i.step : Int) = ((k1 : Int) - (k0 : Int)) * (pb.step : Int) := by
        calc
          (i.step : Int) = (i.lo + (i.step : Int) - pb.lo) - (i.lo - pb.lo) := by omega
          _ = (k1 : Int) * (pb.step : Int) - (k0 : Int) * (pb.step : Int) := by rw [hk1eq', hk0eq]
          _ = ((k1 : Int) - (k0 : Int)) * (pb.step : Int) := by rw [Int.sub_mul]
      have hleval : (k0 : Int) ≤ (k1 : Int) := by
        have hspos : (0 : Int) < (i.step : Int) := by
          exact_mod_cast intersection_step_pos pa pb (by omega : 0 < pa.step) hiNpos
        have hkdiffpos : (0 : Int) < (k1 : Int) - (k0 : Int) :=
          Int.pos_of_mul_pos_right (by rw [Int.mul_comm, ← hstepInt]; exact hspos) (by omega)
        omega
      have hk0leNat : k0 ≤ k1 := by exact_mod_cast hleval
      let q : Nat := k1 - k0
      have hstepNat : i.step = pb.step * q := by
        dsimp [q]
        have hcast : ((k1 - k0 : Nat) : Int) = (k1 : Int) - (k0 : Int) := by omega
        have hnat : i.step = (k1 - k0) * pb.step := by exact_mod_cast hstepInt
        rw [Nat.mul_comm] at hnat
        exact hnat
      have hjdx : jdx m = k0 + m * q := by
        dsimp [jdx]
        have hge : pb.lo ≤ progVal i m := by
          have hib : contains pb (progVal i m) = true :=
            ((intersection_contains pa pb (progVal i m)).mp him).2
          obtain ⟨k, hk, hkv2⟩ := (contains_iff_mem pb (progVal i m)).mp hib
          unfold progVal at hkv2
          change pb.lo ≤ i.lo + (m : Int) * i.step
          rw [hkv2]
          have hpnon : (0 : Int) ≤ (pb.step : Int) := by omega
          exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hpnon)
        have hk0lo : i.lo = pb.lo + (k0 : Int) * (pb.step : Int) := by omega
        have hval : progVal i m = pb.lo + ((k0 : Int) + (m : Int) * (q : Int)) * (pb.step : Int) := by
          unfold progVal
          rw [hk0lo]
          have hstepMul : (m : Int) * (i.step : Int) = (m : Int) * ((k1 : Int) - (k0 : Int)) * (pb.step : Int) := by
            rw [hstepInt]
            grind
          rw [hstepMul]
          dsimp [q]
          grind
        rw [hval]
        have hdiv : ((pb.lo + ((k0 : Int) + (m : Int) * (q : Int)) * (pb.step : Int) - pb.lo) /
            (pb.step : Int)).toNat = k0 + m * q := by
          have hn : pb.lo + ((k0 : Int) + (m : Int) * (q : Int)) * (pb.step : Int) - pb.lo =
              ((k0 + m * q : Nat) : Int) * (pb.step : Int) := by
            have hc : ((k0 + m * q : Nat) : Int) = (k0 : Int) + (m : Int) * (q : Int) := by
              simp [Int.natCast_add, Int.natCast_mul]
            rw [hc]
            grind
          rw [hn]
          have hcalc : (((k0 + m * q : Nat) : Int) * (pb.step : Int)) / (pb.step : Int) =
              ((k0 + m * q : Nat) : Int) := by
            rw [Int.mul_comm]
            exact Int.mul_ediv_cancel_left (a := (pb.step : Int)) ((k0 + m * q : Nat) : Int) (by omega)
          rw [hcalc]
          exact Int.toNat_natCast (k0 + m * q)
        exact hdiv
      rw [hjdx]
      have hk0eq' : i.lo - (b.start : Int) = (k0 : Int) * (b.stride : Int) := by
        rw [← hpblo, hk0eq]
        rfl
      unfold firstAt slopeAt
      dsimp
      rw [hk0eq']
      have hdiv0 : ((k0 : Int) * (b.stride : Int)) / (b.stride : Int) = (k0 : Int) := by
        rw [Int.mul_comm]
        exact Int.mul_ediv_cancel_left (a := (b.stride : Int)) (k0 : Int) (by omega)
      rw [hdiv0]
      have hdivstep : (i.step / pb.step : Nat) = q := by
        change (i.step / b.stride : Nat) = q
        rw [hstepNat, Nat.mul_comm]
        exact Nat.mul_div_left q hpbpos
      have hdivInt : (i.step : Int) / (b.stride : Int) = (q : Int) := by
        have hc : (i.step : Int) = (b.stride : Int) * (q : Int) := by exact_mod_cast hstepNat
        rw [hc]
        exact Int.mul_ediv_cancel_left (a := (b.stride : Int)) (q : Int) (by omega)
      rw [hdivInt]
      grind
  have hinner : ∀ j, j < a.n →
      ((List.range b.n).map (fun (i : Nat) =>
        if a.start + j == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int)) else 0)).sum =
      if contains pb (a.start + j) then
        (a.base + (j : Int) * a.step) *
          (b.base + b.step * (((((a.start + j) - b.start) / b.stride : Nat) : Int)))
      else 0 := by
    intro j hj
    simpa [pb] using inner_aligned a b hb j hj
  have hlane : lanePoints a = (List.range a.n).map (fun (j : Nat) => (a.start + j, a.base + (j : Int) * a.step)) := by
    unfold lanePoints
    apply List.map_congr_left
    intro j hj
    simp [ha]
  have houter : ((lanePoints a).map (fun r => ((List.range b.n).map (fun (i : Nat) =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
      ((List.range a.n).map (fun (j : Nat) =>
        if contains pb (a.start + j) then
          (a.base + (j : Int) * a.step) *
            (b.base + b.step * (((((a.start + j) - b.start) / b.stride : Nat) : Int)))
        else 0)).sum := by
    rw [hlane]
    rw [List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    exact hinner j (List.mem_range.mp hj)
  rw [houter]
  rw [← sum_map_ite_zero]
  have hfilt : (List.range a.n).filter (fun (j : Nat) => contains pb (a.start + j)) =
      (List.range pa.n).filter (fun j => contains pb (progVal pa j)) := by
    apply List.filter_congr
    intro j hj
    simp [pa, progVal, positions, ha]
  rw [hfilt]
  have hpv := hpperm'.map (fun (j : Nat) =>
    (a.base + (j : Int) * a.step) *
      (b.base + b.step * (((((a.start + j) - b.start) / b.stride : Nat) : Int))))
  rw [list_sum_perm hpv]
  have hmap : ((List.range i.n).map idx).map (fun (j : Nat) =>
      (a.base + (j : Int) * a.step) *
        (b.base + b.step * (((((a.start + j) - b.start) / b.stride : Nat) : Int)))) =
      (List.range i.n).map (fun (m : Nat) =>
        (firstAt a i + (m : Int) * slopeAt a i) * (firstAt b i + (m : Int) * slopeAt b i)) := by
    rw [List.map_map]
    change (List.range i.n).map (fun (m : Nat) =>
      (a.base + (idx m : Int) * a.step) *
        (b.base + b.step * (((((a.start + idx m) - b.start) / b.stride : Nat) : Int)))) = _
    apply List.map_congr_left
    intro m hm
    have hmlt : m < i.n := List.mem_range.mp hm
    have hia := hterm_a m hmlt
    have hibv := hterm_b m hmlt
    have hpblo : pb.lo = (b.start : Int) := by rfl
    have hpos : a.start + idx m = progVal i m := by
      have hi_ge : a.start ≤ progVal i m := by
        have him' : contains i (progVal i m) = true := progVal_mem i m hmlt
        have hia' : contains pa (progVal i m) = true := ((intersection_contains pa pb (progVal i m)).mp him').1
        obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pa (progVal i m)).mp hia'
        unfold progVal at hkv
        change a.start ≤ i.lo + (m : Int) * i.step
        rw [hkv]
        have hpnon : (0 : Int) ≤ (pa.step : Int) := by omega
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hpnon)
      have hidxdef : idx m = ((progVal i m - (a.start : Int)) / 1).toNat := by
        dsimp [idx]
        have hlo : pa.lo = (a.start : Int) := by rfl
        have hst : pa.step = 1 := by change a.stride = 1; exact ha
        rw [hlo, hst]
        simp
      rw [hidxdef]
      simp
      omega
    have hposInt : ((a.start + idx m : Nat) : Int) = progVal i m := hpos
    have hge2 : b.start ≤ a.start + idx m := by
      have hle : pb.lo ≤ progVal i m := by
        have hib : contains pb (progVal i m) = true := ((intersection_contains pa pb (progVal i m)).mp (progVal_mem i m hmlt)).2
        obtain ⟨k, hk, hkv⟩ := (contains_iff_mem pb (progVal i m)).mp hib
        unfold progVal at hkv
        change pb.lo ≤ i.lo + (m : Int) * i.step
        rw [hkv]
        have hpnon : (0 : Int) ≤ (pb.step : Int) := by omega
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) hpnon)
      rw [hpblo] at hle
      omega
    have hEqInt : ((a.start + idx m - b.start : Nat) : Int) = progVal i m - pb.lo := by
      have hsubcast : ((a.start + idx m - b.start : Nat) : Int) =
          ((a.start + idx m : Nat) : Int) - (b.start : Int) := by omega
      rw [hsubcast, hposInt, hpblo]
    have hjd : (((a.start + idx m) - b.start) / b.stride : Nat) = jdx m := by
      dsimp [jdx]
      change (a.start + idx m - b.start) / pb.step =
        ((progVal i m - pb.lo) / (pb.step : Int)).toNat
      rw [← hEqInt]
      rw [Int.ofNat_ediv_ofNat]
      exact (Int.toNat_natCast ((a.start + idx m - b.start) / pb.step)).symm
    rw [hjd]
    rw [hia]
    have hibv' : b.base + b.step * (jdx m : Int) = firstAt b i + (m : Int) * slopeAt b i := by
      rw [Int.mul_comm b.step (jdx m : Int)]
      exact hibv
    rw [hibv']
  rw [hmap]
  have hprodSum : ((List.range i.n).map (fun (m : Nat) =>
      (firstAt a i + (m : Int) * slopeAt a i) * (firstAt b i + (m : Int) * slopeAt b i))).sum =
      (i.n : Int) * firstAt a i * firstAt b i +
        tri i.n * (firstAt a i * slopeAt b i + firstAt b i * slopeAt a i) +
        squares i.n * slopeAt a i * slopeAt b i := by
    induction i.n with
    | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
    | succ n ih =>
        rw [List.range_succ, List.map_append, List.sum_append, ih]
        simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
        rw [tri_succ, squares_succ]
        simp [Int.natCast_add, Int.natCast_mul]
        grind
  rw [hprodSum]
  unfold aligned
  rfl

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.AlignedProduct.Goal := by
  intro c b hb
  rw [rows_mirror, mirror_lanes]
  simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  rw [laneAligned_stride1 (mirrorA c) b rfl hb, laneAligned_stride1 (mirrorB c) b rfl hb]
  simp

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.MirroredTail.Remaining
