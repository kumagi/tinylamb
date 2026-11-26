import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `GrayTile.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout c).map (fun
  r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
- `GrayTile.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `GrayTile.PowerOfTwo` — `∀ c (d : Nat), width c = 2^d → c.n%width c = 0 → ((rows
  layout c).map Prod.snd).Perm ((rows LinearOrdinal.layout c).map Prod.snd)`
- `GrayTile.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r => (List.range
  b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes
  c).map (joinCount · b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.GrayTilePowerOfTwoAndProgressions

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

/--! ## Generic list helpers -/

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
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

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

/--! ## Arithmetic on lanes -/

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
  unfold contains lastVal
  simp
  by_cases hn : 0 < n
  · have hncast : ((n - 1 : Nat) : Int) = (n : Int) - 1 := by omega
    rw [hncast]
    omega
  · simp [hn]

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

private theorem int_lcm_dvd (s t : Nat) (hs : 0 < s) (ht : 0 < t) (x : Int)
    (h1 : (s : Int) ∣ x) (h2 : (t : Int) ∣ x) : ((Nat.lcm s t : Nat) : Int) ∣ x := by
  apply Int.natAbs_dvd_natAbs.mp
  simp only [Int.natAbs_natCast]
  have h1n : s ∣ x.natAbs := by simpa using (Int.natAbs_dvd_natAbs.mpr h1)
  have h2n : t ∣ x.natAbs := by simpa using (Int.natAbs_dvd_natAbs.mpr h2)
  exact Nat.lcm_dvd h1n h2n

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · simp [han, intersection, contains]
  · by_cases hbn : b.n = 0
    · simp [hbn, intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · unfold intersection
        rw [if_neg (by simp [han, hbn]), if_pos (by simp [has])]
        change contains ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ v = true ↔
          contains a v = true ∧ contains b v = true
        have haeq : a = ⟨a.lo, 0, a.n⟩ := by cases a; simp_all
        have haconst : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
          rw [haeq]
          exact contains_const_iff a.lo a.n v
        by_cases hba : contains b a.lo
        · rw [if_pos hba, contains_step_one]
          constructor
          · rintro ⟨_, hlo, hlt⟩
            have hvn : v = a.lo := by omega
            exact ⟨haconst.mpr ⟨hanp, hvn⟩, by simpa [hvn] using hba⟩
          · rintro ⟨ha, hb⟩
            have hvn : v = a.lo := (haconst.mp ha).2
            rw [hvn]
            omega
        · rw [if_neg hba, contains_step_one]
          constructor
          · intro ha
            omega
          · rintro ⟨ha, hb⟩
            have hvn : v = a.lo := (haconst.mp ha).2
            exact False.elim (hba (by simpa [hvn] using hb))
      · by_cases hbs : b.step = 0
        · unfold intersection
          rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]), if_pos (by simp [hbs])]
          change contains ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ v = true ↔
            contains a v = true ∧ contains b v = true
          have hbeq : b = ⟨b.lo, 0, b.n⟩ := by cases b; simp_all
          have hbconst : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
            rw [hbeq]
            exact contains_const_iff b.lo b.n v
          by_cases hab : contains a b.lo
          · rw [if_pos hab, contains_step_one]
            constructor
            · rintro ⟨_, hlo, hlt⟩
              have hvn : v = b.lo := by omega
              exact ⟨by simpa [hvn] using hab, hbconst.mpr ⟨hbnp, hvn⟩⟩
            · rintro ⟨ha, hb⟩
              have hvn : v = b.lo := (hbconst.mp hb).2
              rw [hvn]
              omega
          · rw [if_neg hab, contains_step_one]
            constructor
            · intro ha
              omega
            · rintro ⟨ha, hb⟩
              have hvn : v = b.lo := (hbconst.mp hb).2
              exact False.elim (hab (by simpa [hvn] using ha))
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
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hbdiv : b.step = g * (b.step / g) :=
              (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
            have hadiv : a.step = g * (a.step / g) :=
              (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
            have hadiv' : a.step / g * g = a.step :=
              Nat.div_mul_cancel (Nat.gcd_dvd_left a.step b.step)
            have hbdiv' : b.step / g * g = b.step :=
              Nat.div_mul_cancel (Nat.gcd_dvd_right a.step b.step)
            have hperA : period = a.step * (b.step / g) := by
              dsimp [period]
              calc
                a.step / g * b.step = a.step / g * (b.step / g * g) := by rw [hbdiv']
                _ = (a.step / g * g) * (b.step / g) := by grind
                _ = a.step * (b.step / g) := by rw [hadiv']
            have hperB : period = b.step * (a.step / g) := by
              calc
                period = a.step * (b.step / g) := hperA
                _ = (a.step / g * g) * (b.step / g) := by rw [hadiv']
                _ = (b.step / g * g) * (a.step / g) := by grind
                _ = b.step * (a.step / g) := by rw [hbdiv']
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hperA
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hperB
            unfold intersection
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_neg (by
                change ¬ ((gap % (g : Int) != 0) = true)
                intro hc
                have hne : (gap % (g : Int) != 0) = false := by
                  simp [beq_iff_eq, hgapmod]
                rw [hne] at hc
                contradiction)]
            change contains ⟨first, (period : Int).toNat,
              if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
              contains a v = true ∧ contains b v = true
            rw [show contains ⟨first, (period : Int).toNat,
                if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
                first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first from
                span_contains_iff first (period : Int) high v hp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hvrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hvrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have heq : v - a.lo = (v - root) + (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_add hvrootA hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have heq : v - b.lo = (v - root) + (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_add hvrootB hroot.2
              have hxlow : low ≤ v := by omega
              have hab := contains_bounds a v
              have hbb := contains_bounds b v
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo : (a.step : Int) ∣ v - a.lo := contains_dvd a v ha
              have hxblo : (b.step : Int) ∣ v - b.lo := contains_dvd b v hb
              have hvar : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxalo hroot.1
              have hvbr : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo hroot.2
              have hperLcmNat : period = Nat.lcm a.step b.step := by
                rw [Nat.lcm_eq_mul_div]
                dsimp [period, g]
                exact (Nat.mul_div_right_comm (Nat.gcd_dvd_left a.step b.step) b.step).symm
              have hxroot : (period : Int) ∣ v - root := by
                have hl := int_lcm_dvd a.step b.step hasp hbsp (v - root) hvar hvbr
                simpa [hperLcmNat] using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hxlow : low ≤ v := by
                have hab := contains_bounds a v ha
                have hbb := contains_bounds b v hb
                dsimp [low]
                omega
              have hfirstx : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp hfirst hxlow hxroot
              have hxhigh : v ≤ high := by
                have hab := contains_bounds a v ha
                have hbb := contains_bounds b v hb
                dsimp [high]
                omega
              exact ⟨hfirstx, hxhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            unfold intersection
            rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]),
              if_neg (by simp [hbs]), if_pos (by
                have hz : (b.lo - a.lo) % (a.step.gcd b.step : Int) ≠ 0 := by
                  simpa [gap, g] using hgapne
                simp [hz])]
            change contains ⟨0, 1, 0⟩ v = true ↔ contains a v = true ∧ contains b v = true
            have hl : contains ⟨0, 1, 0⟩ v = false := by simp [contains]
            rw [hl]
            constructor
            · intro h
              cases h
            · rintro ⟨ha, hb⟩
              exfalso
              have hg := common_gap_dvd a b v ha hb
              exact hgapne (by
                have hz := Int.emod_eq_zero_of_dvd hg
                simpa [gap, g] using hz)


/-! ## Additional generic helpers -/

private theorem perm_sum_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem progVal_inj_of_pos (p : Progression) (hp : 0 < p.step) {i j : Nat}
    (h : progVal p i = progVal p j) : i = j := by
  unfold progVal at h
  have hs : (p.step : Int) ≠ 0 := by omega
  have hm : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hij : (i : Int) = (j : Int) := Int.eq_of_mul_eq_mul_right hs hm
  exact_mod_cast hij

private theorem contains_progVal_self (p : Progression) (k : Nat) (hk : k < p.n) :
    contains p (progVal p k) = true := by
  apply (contains_iff_mem p (progVal p k)).mpr
  exact ⟨k, hk, rfl⟩

private theorem intersection_step_pos (a b : Progression) :
    0 < (intersection a b).step := by
  unfold intersection
  by_cases h1 : a.n == 0
  · simp [h1]
  by_cases h2 : b.n == 0
  · simp [h1, h2]
  by_cases h3 : a.step == 0
  · simp [h1, h2, h3]
  by_cases h4 : b.step == 0
  · simp [h1, h2, h3, h4]
  by_cases h5 : (b.lo - a.lo) % (↑(a.step.gcd b.step) : Int) == 0
  · have h5p : (b.lo - a.lo) % (↑(a.step.gcd b.step) : Int) = 0 := by
      simpa using h5
    simp [h1, h2, h3, h4, h5p]
    have hg : 0 < a.step.gcd b.step :=
      Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero (by simpa using h3))
    have hdiv : 0 < a.step / a.step.gcd b.step :=
      Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero (by simpa using h3)) (Nat.gcd_dvd_left a.step b.step)) hg
    exact Nat.mul_pos hdiv (Nat.pos_of_ne_zero (by simpa using h4))
  · have h5p : (b.lo - a.lo) % (↑(a.step.gcd b.step) : Int) ≠ 0 := by
      simpa using h5
    simp [h1, h2, h3, h4, h5p]

private theorem contains_b_of_inter (a b : Progression) {v : Int}
    (h : contains (intersection a b) v = true) : contains b v = true :=
  ((intersection_contains a b v).mp h).2

private theorem contains_a_of_inter (a b : Progression) {v : Int}
    (h : contains (intersection a b) v = true) : contains a v = true :=
  ((intersection_contains a b v).mp h).1

private theorem positions_progVal (a : Lane) (k : Nat) :
    progVal (positions a) k = a.start + k * a.stride := by
  unfold progVal positions
  rfl

private theorem contains_positions_self (a : Lane) (k : Nat) (hk : k < a.n) :
    contains (positions a) (a.start + k * a.stride) = true := by
  rw [← positions_progVal]
  exact contains_progVal_self (positions a) k (by simpa [positions] using hk)
private theorem decide_eq_of_iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    simp [ha, hb]
  · have hb : ¬ b := fun hb => ha (h.mpr hb)
    simp [ha, hb]

private theorem filter_or_singleton_stats (l : List Nat) (hl : l.Nodup) (k : Nat) (hk : k ∈ l)
    (p : Nat → Bool) (hpk : ¬ p k) (g : Nat → Int) :
    let l0 := l.filter p
    let l1 := l.filter (fun x => p x || x == k)
    (l1.length, (l1.map g).sum) = (l0.length + 1, (l0.map g).sum + g k) := by
  induction l with
  | nil => simp at hk
  | cons x xs ih =>
      simp only [List.nodup_cons] at hl
      rcases hl with ⟨hxnot, hnodup⟩
      by_cases hxk : x = k
      · subst x
        have hfil : xs.filter (fun y => p y || y == k) = xs.filter p := by
          apply List.filter_congr
          intro y hy
          have hyk : y ≠ k := by
            intro h
            apply hxnot
            simpa [h] using hy
          simp [hyk]
        dsimp
        simp [hpk, hfil]
        omega
      · have hkxs : k ∈ xs := by
          rcases List.mem_cons.mp hk with hEq | hmem
          · subst k
            exact False.elim (hxk rfl)
          · exact hmem
        have ih' := ih hnodup hkxs
        dsimp at ih' ⊢
        by_cases hpx : p x
        · simp [hpx, hxk] at ih' ⊢
          rw [ih'.1, ih'.2]
          simp
          omega
        · simp [hpx, hxk] at ih' ⊢
          rw [ih'.1, ih'.2]
          simp

private theorem stats_filter_contains_aux (n : Nat) :
    ∀ (a : Lane) (ha : 0 < a.stride) (r : Progression), r.n = n → 0 < r.step →
      (∀ j, j < r.n → contains (positions a) (progVal r j) = true) →
      ∀ (f : Nat → Int),
      (∀ j, j < r.n → ∀ k, k < a.n → progVal (positions a) k = progVal r j →
        a.base + (k : Int) * a.step = f j) →
      let ks := (List.range a.n).filter (fun k => contains r (progVal (positions a) k))
      (ks.length, (ks.map (fun k : Nat => a.base + (k : Int) * a.step)).sum) =
        (r.n, ((List.range r.n).map f).sum) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro a ha r hrn hr hall f hf
      cases n with
      | zero =>
          let ks := (List.range a.n).filter (fun k => contains r (progVal (positions a) k))
          have hfalse : ∀ k, contains r (progVal (positions a) k) = false := by
            intro k
            simp [contains, hrn]
          have hfil : ks = [] := by
            dsimp [ks]
            apply List.eq_nil_of_length_eq_zero
            simp [List.countP_eq_length_filter, hfalse]
          dsimp only
          change (ks.length, (ks.map (fun k : Nat => a.base + (k : Int) * a.step)).sum) =
            (r.n, ((List.range r.n).map f).sum)
          rw [hfil]
          simp [hrn]
      | succ j =>
          let r0 : Progression := ⟨r.lo, r.step, j⟩
          have hr0n : r0.n = j := rfl
          have hr0step : 0 < r0.step := hr
          have hprog_r0 : ∀ t, progVal r0 t = progVal r t := by intro t; rfl
          have hall0 : ∀ t, t < r0.n → contains (positions a) (progVal r0 t) = true := by
            intro t ht
            rw [hprog_r0]
            apply hall t
            omega
          have hf0 : ∀ t, t < r0.n → ∀ k, k < a.n →
              progVal (positions a) k = progVal r0 t → a.base + (k : Int) * a.step = f t := by
            intro t ht k hk hval
            apply hf t
            · omega
            · exact hk
            · simpa [hprog_r0] using hval
          have ih0 := ih j (by omega) a ha r0 hr0n hr0step hall0 f hf0
          dsimp at ih0
          have hallj : contains (positions a) (progVal r j) = true := hall j (by omega)
          obtain ⟨kj, hkj, hvkj⟩ := (contains_iff_mem (positions a) (progVal r j)).mp hallj
          have hpk : ¬ contains r0 (progVal (positions a) kj) := by
            intro hc
            obtain ⟨t, ht, htv⟩ := (contains_iff_mem r0 (progVal (positions a) kj)).mp hc
            have htj : t = j := by
              apply progVal_inj_of_pos r hr
              rw [← hprog_r0 t, hvkj]
              exact htv.symm
            omega
          have hpred : ∀ k ∈ List.range a.n,
              (contains r (progVal (positions a) k) = true) ↔
                (contains r0 (progVal (positions a) k) = true ∨ k = kj) := by
            intro k hk
            constructor
            · intro hc
              obtain ⟨t, ht, htv⟩ := (contains_iff_mem r (progVal (positions a) k)).mp hc
              have htj : t ≤ j := by omega
              by_cases hlt : t < j
              · left
                apply (contains_iff_mem r0 (progVal (positions a) k)).mpr
                refine ⟨t, by simpa [hr0n] using hlt, ?_⟩
                simpa [hprog_r0] using htv
              · right
                have htEq : t = j := by omega
                subst t
                apply progVal_inj_of_pos (positions a) ha
                rw [htv, hvkj]
            · intro hc
              rcases hc with hc | hkj
              · obtain ⟨t, ht, htv⟩ := (contains_iff_mem r0 (progVal (positions a) k)).mp hc
                apply (contains_iff_mem r (progVal (positions a) k)).mpr
                refine ⟨t, by omega, ?_⟩
                simpa [hprog_r0] using htv
              · subst k
                apply (contains_iff_mem r (progVal (positions a) kj)).mpr
                refine ⟨j, by omega, hvkj.symm⟩
          let ks := (List.range a.n).filter (fun k => contains r (progVal (positions a) k))
          let ks0 := (List.range a.n).filter (fun k => contains r0 (progVal (positions a) k))
          let ks1 := (List.range a.n).filter (fun k => contains r0 (progVal (positions a) k) || k == kj)
          have hks : ks = ks1 := by
            dsimp [ks, ks1]
            apply List.filter_congr
            intro k hk
            have hp := hpred k hk
            by_cases hL : contains r (progVal (positions a) k)
            · have hRprop := hp.mp hL
              have hR : (contains r0 (progVal (positions a) k) || k == kj) = true := by
                rcases hRprop with h0 | hk'
                · simp [h0]
                · simp [hk']
              simp [hL, hR]
            · have hRnot : (contains r0 (progVal (positions a) k) || k == kj) ≠ true := by
                intro hR
                have hRprop : contains r0 (progVal (positions a) k) = true ∨ k = kj := by
                  simpa [Bool.or_eq_true, beq_iff_eq] using hR
                exact hL (hp.mpr hRprop)
              have hR : (contains r0 (progVal (positions a) k) || k == kj) = false := by
                cases hb : (contains r0 (progVal (positions a) k) || k == kj) <;> simp_all
              simp [hL, hR]
          have hkja : kj < a.n := by simpa [positions] using hkj
          have hor := filter_or_singleton_stats (List.range a.n) List.nodup_range kj (by simp [hkja])
            (fun k => contains r0 (progVal (positions a) k)) hpk
            (fun k : Nat => a.base + (k : Int) * a.step)
          dsimp at hor
          have hval : a.base + (kj : Int) * a.step = f j := hf j (by omega) kj hkja hvkj.symm
          dsimp only
          change (ks.length, (ks.map (fun k : Nat => a.base + (k : Int) * a.step)).sum) =
            (r.n, ((List.range r.n).map f).sum)
          rw [hks]
          rw [hor]
          have ih01 :
              (List.filter (fun k => contains r0 (progVal (positions a) k)) (List.range a.n)).length =
                r0.n := by simpa using congrArg Prod.fst ih0
          have ih02 :
              (List.map (fun k : Nat => a.base + (k : Int) * a.step)
                (List.filter (fun k => contains r0 (progVal (positions a) k)) (List.range a.n))).sum =
                ((List.range r0.n).map f).sum := by simpa using congrArg Prod.snd ih0
          rw [ih01, ih02]
          rw [hval]
          rw [hrn, hr0n]
          rw [List.range_succ, List.map_append, List.sum_append]
          simp


private theorem stats_filter_contains (a : Lane) (ha : 0 < a.stride) (r : Progression)
    (hr : 0 < r.step) (hall : ∀ j, j < r.n → contains (positions a) (progVal r j) = true)
    (f : Nat → Int)
    (hf : ∀ j, j < r.n → ∀ k, k < a.n → progVal (positions a) k = progVal r j →
      a.base + (k : Int) * a.step = f j) :
    let ks := (List.range a.n).filter (fun k => contains r (progVal (positions a) k))
    (ks.length, (ks.map (fun k : Nat => a.base + (k : Int) * a.step)).sum) =
      (r.n, ((List.range r.n).map f).sum) :=
  stats_filter_contains_aux r.n a ha r rfl hr hall f hf
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
  · have hps' : start < p := by omega
    rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start :=
      ceilDiv_eq (p - start) stride hs
    rw [hrewrite]
    omega

private def grayCode (r : Nat) : Nat := r ^^^ (r / 2)

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2*p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n - r + p - 1) / p = n / p + if r < n % p then 1 else 0 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases hnr : n < r
  · have hdiv : n / p = 0 := Nat.div_eq_of_lt (by omega)
    have hm : n - r + p - 1 = p - 1 := by grind
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by
      grind
    rw [hsub, Nat.mul_comm (n / p) p]
    rw [Nat.mul_add_div hp (n / p) (n % p + p - 1 - r)]
    by_cases hlt : r < n % p
    · have hge : p ≤ n % p + p - 1 - r := by omega
      have hlt2 : n % p + p - 1 - r < 2 * p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem succ_mod_step (n p : Nat) (hp : 0 < p) :
    (n + 1) % p = if n % p + 1 < p then n % p + 1 else 0 := by
  by_cases hp1 : p = 1
  · subst p
    have hm : n % 1 = 0 := by
      have := Nat.mod_lt n (by omega : 0 < 1)
      omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]
      simp [h]
    · have heq : n % p + 1 = p := by
        have hs := Nat.mod_lt n hp
        omega
      rw [heq, Nat.mod_self]
      simp [h]

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
    simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
    simp [h]

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      have hm : (0 - r + p - 1) / p = 0 := by
        have : 0 - r + p - 1 = p - 1 := by omega
        rw [this, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
      simp [hm, hpdiv]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      rw [mod_step_count n p r hp hr, mod_step_count (n+1) p r hp hr]
      rw [succ_mod_step n p hp, succ_div_step n p hp]
      by_cases hn : n % p == r
      · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
        have hltold : ¬ r < n % p := by omega
        have htail : List.map (fun q => r + q * p) (List.range (n / p)) ++ [n] =
            List.map (fun q => r + q * p) (List.range (n / p + 1)) := by
          rw [show n / p + 1 = Nat.succ (n / p) by omega, List.range_succ, List.map_append]
          simp
          have hd := Nat.div_add_mod n p
          rw [Nat.mul_comm p (n / p)] at hd
          have hval : r + n / p * p = n := by omega
          rw [hval]
        by_cases hs : r + 1 < p
        · have hltnew : r < r + 1 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
        · have hltnew : ¬ r < 0 := by omega
          simp [hn, hltold, hnp, hs, hltnew]
          exact htail
      · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
        have hltold : r < n % p ∨ n % p < r := by
          by_cases hrlt : r < n % p
          · exact Or.inl hrlt
          · right; omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have hne : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
          have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_update (p r0 : Nat) (hr0 : r0 < p) (s : Nat → Int) (v : Int) :
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  induction p generalizing r0 s v with
  | zero => omega
  | succ p ih =>
      rw [show p + 1 = Nat.succ p by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r0
      · have hb : p == r0 := by rw [heq]; simp
        rw [if_pos hb]
        have hpref : (List.range p).map (fun r => if r == r0 then v + s r else s r) =
            (List.range p).map s := by
          apply List.map_congr_left
          intro r hr
          have hrlt : r < p := List.mem_range.mp hr
          have hne : ¬ r == r0 := by rw [beq_iff_eq]; omega
          simp [hne]
        rw [hpref]
        omega
      · have hb : ¬ (p == r0) := by rw [beq_iff_eq]; exact heq
        rw [if_neg hb]
        have hlt : r0 < p := by omega
        rw [ih r0 hlt s v]
        omega

private theorem striped_sum_partition (p : Nat) (hp : 0 < p) (F : Nat → Int) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update p (x % p) (Nat.mod_lt x hp)]
      apply congrArg List.sum
      apply List.map_congr_left
      intro r hr
      by_cases h : x % p == r
      · have h' : x % p = r := Nat.eq_of_beq_eq_true h
        simp [h, h']
      · have h' : x % p ≠ r := by intro e; exact h (by rw [e]; simp)
        simp [h']
        intro hrx
        exact (h' hrx.symm).elim

private def stripedValue (p : Nat) (make : Nat → Int × Int) (i : Nat) : Int :=
  let (z, d) := make (i % p)
  z + d * ((i / p : Nat) : Int)

private theorem stripedValue_residue (p : Nat) (hp : 0 < p) (make : Nat → Int × Int)
    (r q : Nat) (hr : r < p) :
    stripedValue p make (r + q * p) =
      (make r).1 + (make r).2 * (q : Int) := by
  unfold stripedValue
  have hmod : (r + q * p) % p = r := by
    rw [Nat.add_mul_mod_self_right]
    exact Nat.mod_eq_of_lt hr
  have hdiv : (r + q * p) / p = q := by
    rw [Nat.mul_comm q p, Nat.add_mul_div_left r q hp, Nat.div_eq_of_lt hr]
    simp
  rw [hmod, hdiv]

private theorem striped_sum (p : Nat) (hp : 0 < p) (n : Nat) (make : Nat → Int × Int)
    (g : Nat → Int → Int) :
    ((List.range n).map (fun i => g i (stripedValue p make i))).sum =
      ((List.range p).map (fun r =>
        let (z, d) := make r
        ((List.range ((n - r + p - 1) / p)).map (fun q => g (r + q*p) (z + d*(q:Int)))).sum)).sum := by
  rw [striped_sum_partition p hp (fun i => g i (stripedValue p make i)) (List.range n)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := List.mem_range.mp hr
  rw [range_filter_mod_eq n p r hp hrp]
  simp only [List.map_map, Function.comp_apply]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  change g (r + q * p) (stripedValue p make (r + q * p)) =
    g (r + q * p) ((make r).1 + (make r).2 * (q : Int))
  rw [stripedValue_residue p hp make r q hrp]

private theorem rows_linearOrdinal (c : Config) :
    (rows LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

/-! ## Gray code -/

private theorem width_pos (c : Config) : 0 < width c := by dsimp [width]; omega

private def gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem gray_div_two (r : Nat) : gray r / 2 = gray (r / 2) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [← Nat.testBit_succ]
  simp only [gray, Nat.testBit_xor]
  rw [Nat.testBit_succ, Nat.testBit_succ]

private theorem bit_mod_two (x : Nat) : (if x.testBit 0 then 1 else 0) = x % 2 := by
  rw [Nat.testBit_zero]
  have hlt : x % 2 < 2 := Nat.mod_lt _ (by decide)
  by_cases h : x % 2 = 1
  · simp [h]
  · have h0 : x % 2 = 0 := by omega
    simp [h0]

private theorem gray_mod_two (r : Nat) : gray r % 2 = (r % 2) ^^^ ((r / 2) % 2) := by
  rw [← bit_mod_two, gray, Nat.testBit_xor, ← bit_mod_two, ← bit_mod_two]
  cases r.testBit 0 <;> cases (r / 2).testBit 0 <;> decide

private theorem gray_inj (a b : Nat) (h : gray a = gray b) : a = b := by
  induction hsum : a + b using Nat.strongRecOn generalizing a b with
  | ind s ih =>
      have hsab : a + b = s := hsum
      by_cases hz : a + b = 0
      · have ha0 : a = 0 := by omega
        have hb0 : b = 0 := by omega
        subst a; subst b; rfl
      · have hlt : a / 2 + b / 2 < s := by
          rw [← hsab]
          by_cases ha0 : a = 0
          · subst a
            have hbpos : 0 < b := by omega
            have hbdiv : b / 2 < b := Nat.div_lt_self hbpos (by decide : 1 < 2)
            simpa using hbdiv
          · have hapos : 0 < a := Nat.pos_of_ne_zero ha0
            have hadiv : a / 2 < a := Nat.div_lt_self hapos (by decide : 1 < 2)
            have hble : b / 2 ≤ b := Nat.div_le_self b 2
            omega
        have hdiv : gray (a / 2) = gray (b / 2) := by
          have hh := congrArg (fun x => x / 2) h
          simpa [gray_div_two] using hh
        have hab : a / 2 = b / 2 := ih (a / 2 + b / 2) hlt (a / 2) (b / 2) hdiv rfl
        have hmod : (a % 2) ^^^ ((a / 2) % 2) = (b % 2) ^^^ ((b / 2) % 2) := by
          have hh := congrArg (fun x => x % 2) h
          change gray a % 2 = gray b % 2 at hh
          simpa [gray_mod_two] using hh
        rw [hab] at hmod
        have hmod' : a % 2 = b % 2 := by
          have hh := congrArg (fun x => x ^^^ ((b / 2) % 2)) hmod
          simpa [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero] using hh
        have ha := Nat.div_add_mod a 2
        have hb := Nat.div_add_mod b 2
        omega

private theorem gray_xor_half (g u : Nat) (hu : gray u = g / 2) : (g ^^^ u) / 2 = u := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [← Nat.testBit_succ, Nat.testBit_xor]
  have hbit := congrArg (fun x => x.testBit i) hu
  change (u ^^^ (u / 2)).testBit i = (g / 2).testBit i at hbit
  rw [Nat.testBit_xor] at hbit
  have hgsucc : (g / 2).testBit i = g.testBit (i + 1) := (Nat.testBit_succ g i).symm
  have husucc : (u / 2).testBit i = u.testBit (i + 1) := (Nat.testBit_succ u i).symm
  rw [hgsucc, husucc] at hbit
  cases hg : g.testBit (i + 1) <;> cases hu1 : u.testBit (i + 1) <;>
    cases hui : u.testBit i <;> simp_all

private def ungray : Nat → Nat
  | 0 => 0
  | Nat.succ g => Nat.succ g ^^^ ungray (Nat.succ g / 2)
termination_by x => x
decreasing_by
  exact Nat.div_lt_self (Nat.succ_pos _) (by decide : 1 < 2)

private theorem gray_ungray (x : Nat) : gray (ungray x) = x := by
  induction x using Nat.strongRecOn with
  | ind x ih =>
      cases x with
      | zero => rw [ungray.eq_1]; rfl
      | succ x' =>
          have hlt : (x' + 1) / 2 < x' + 1 := Nat.div_lt_self (by omega) (by decide : 1 < 2)
          have hu : gray (ungray ((x' + 1) / 2)) = (x' + 1) / 2 := ih ((x' + 1) / 2) hlt
          have hhalf : ((x' + 1) ^^^ ungray ((x' + 1) / 2)) / 2 = ungray ((x' + 1) / 2) :=
            gray_xor_half (x' + 1) (ungray ((x' + 1) / 2)) hu
          change gray (ungray (Nat.succ x')) = Nat.succ x'
          rw [ungray.eq_2]
          unfold gray
          rw [hhalf]
          rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem gray_lt_pow_iff (s d : Nat) : gray s < 2 ^ d ↔ s < 2 ^ d := by
  induction d generalizing s with
  | zero =>
      simp only [Nat.pow_zero, Nat.lt_one_iff]
      constructor
      · intro h
        have hz : gray s = 0 := by omega
        unfold gray at hz
        have hsq : s = s / 2 := by
          have hx := congrArg (fun x => x ^^^ (s / 2)) hz
          simpa [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero] using hx
        by_cases hs : s = 0
        · omega
        · have hlt : s / 2 < s := Nat.div_lt_self (Nat.pos_of_ne_zero hs) (by decide : 1 < 2)
          omega
      · intro hs
        subst s
        simp [gray]
  | succ d ih =>
      constructor
      · intro h
        have hdiv : gray (s / 2) < 2 ^ d := by
          rw [← gray_div_two]
          have hmul : gray s < 2 * 2 ^ d := by
            rw [show 2 ^ (d + 1) = 2 * 2 ^ d by rw [Nat.pow_succ, Nat.mul_comm]] at h
            exact h
          omega
        have hs2 : s / 2 < 2 ^ d := (ih (s / 2)).mp hdiv
        have hsle : s ≤ (s / 2) * 2 + 1 := by
          have hm := Nat.div_add_mod s 2
          have hmod : s % 2 < 2 := Nat.mod_lt _ (by decide)
          omega
        rw [show 2 ^ (d + 1) = 2 * 2 ^ d by rw [Nat.pow_succ, Nat.mul_comm]]
        omega
      · intro hs
        exact Nat.xor_lt_two_pow hs (Nat.lt_of_le_of_lt (Nat.div_le_self s 2) hs)

private theorem gray_surj_pow (d r : Nat) (hr : r < 2 ^ d) :
    ∃ s, s < 2 ^ d ∧ gray s = r := by
  refine ⟨ungray r, ?_, gray_ungray r⟩
  exact (gray_lt_pow_iff (ungray r) d).mp (by simpa [gray_ungray] using hr)

private theorem rows_grayTile (c : Config) :
    (rows GrayTile.layout c).map Prod.snd =
      (List.range c.n).map (fun (i : Nat) =>
        c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int)) := by
  have hrows : rows GrayTile.layout c =
      (List.range c.n).map (fun (i : Nat) =>
        (i, c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun (i : Nat) =>
      (i, c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [GrayTile.layout, gray]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

private theorem grayTile_index_inj (w : Nat) (hw : 0 < w)
    (hgray : ∀ r, r < w → gray r < w)
    {i₁ i₂ : Nat}
    (h : i₁ / w * w + gray (i₁ % w) = i₂ / w * w + gray (i₂ % w)) : i₁ = i₂ := by
  have hr1 : gray (i₁ % w) < w := hgray _ (Nat.mod_lt _ hw)
  have hr2 : gray (i₂ % w) < w := hgray _ (Nat.mod_lt _ hw)
  have hk1 : (i₁ / w * w + gray (i₁ % w)) / w = i₁ / w := by
    rw [Nat.add_comm (i₁ / w * w) (gray (i₁ % w)), Nat.mul_comm (i₁ / w) w,
      Nat.add_mul_div_left (gray (i₁ % w)) (i₁ / w) hw, Nat.div_eq_of_lt hr1, Nat.zero_add]
  have hk2 : (i₂ / w * w + gray (i₂ % w)) / w = i₂ / w := by
    rw [Nat.add_comm (i₂ / w * w) (gray (i₂ % w)), Nat.mul_comm (i₂ / w) w,
      Nat.add_mul_div_left (gray (i₂ % w)) (i₂ / w) hw, Nat.div_eq_of_lt hr2, Nat.zero_add]
  have hkeq : i₁ / w = i₂ / w := by
    have hh := congrArg (fun x => x / w) h
    simpa [hk1, hk2] using hh
  have hgreq : gray (i₁ % w) = gray (i₂ % w) := by
    rw [hkeq] at h
    omega
  have hreq : i₁ % w = i₂ % w := gray_inj _ _ hgreq
  have hk1' : w * (i₁ / w) = w * (i₂ / w) := by rw [hkeq]
  calc
    i₁ = w * (i₁ / w) + i₁ % w := (Nat.div_add_mod i₁ w).symm
    _ = w * (i₂ / w) + i₂ % w := by rw [hk1', hreq]
    _ = i₂ := Nat.div_add_mod i₂ w

private theorem grayTile_index_perm (n w d : Nat) (hw : 0 < w) (hw2 : w = 2 ^ d)
    (hn : n % w = 0) :
    List.Perm ((List.range n).map (fun i : Nat => i / w * w + gray (i % w))) (List.range n) := by
  let f : Nat → Nat := fun i => i / w * w + gray (i % w)
  have hq : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hgray : ∀ r, r < w → gray r < w := by
    intro r hr
    have hr' : r < 2 ^ d := by simpa [hw2] using hr
    have hh := (gray_lt_pow_iff r d).mpr hr'
    simpa [hw2] using hh
  have hfinj : ∀ i₁ i₂, f i₁ = f i₂ → i₁ = i₂ := by
    intro i₁ i₂ h
    exact grayTile_index_inj w hw hgray h
  have hnd : ((List.range n).map f).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b) f
      (fun a b hne hEq => hne (hfinj a b hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    have hi' : i < n := List.mem_range.mp hi
    have hr : gray (i % w) < w := hgray _ (Nat.mod_lt _ hw)
    have hklt : i / w < n / w := by
      rw [Nat.div_lt_iff_lt_mul hw]
      rw [← hq]
      exact hi'
    have hbound : f i < ((i / w) + 1) * w := by
      dsimp [f]
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left hr (i / w * w)
    have hle : ((i / w) + 1) * w ≤ n := by
      rw [hq]
      exact Nat.mul_le_mul_right w (by omega)
    exact List.mem_range.mpr (by omega)
  · intro hx
    have hx' : x < n := List.mem_range.mp hx
    let k := x / w
    let r := x % w
    have hkw : k < n / w := by
      dsimp [k]
      rw [Nat.div_lt_iff_lt_mul hw]
      rw [← hq]
      exact hx'
    have hr : r < w := Nat.mod_lt _ hw
    obtain ⟨s, hsw, hgs⟩ := gray_surj_pow d r (by simpa [hw2] using hr)
    have hsw' : s < w := by simpa [hw2] using hsw
    let i := k * w + s
    have hin : i < n := by
      dsimp [i]
      rw [hq]
      have hkle : k + 1 ≤ n / w := by omega
      have hmul := Nat.mul_le_mul_right w hkle
      have hbound : k * w + s < (k + 1) * w := by
        rw [Nat.add_mul, Nat.one_mul]
        exact Nat.add_lt_add_left hsw' (k * w)
      omega
    refine List.mem_map.mpr ⟨i, List.mem_range.mpr hin, ?_⟩
    dsimp [f, i, k, r]
    have hiw : (k * w + s) / w = k := by
      rw [Nat.add_comm (k * w) s, Nat.mul_comm k w, Nat.add_mul_div_left s k hw,
        Nat.div_eq_of_lt hsw', Nat.zero_add]
    have him : (k * w + s) % w = s := by
      rw [Nat.add_comm (k * w) s, Nat.mul_comm k w, Nat.add_mul_mod_self_left]
      exact Nat.mod_eq_of_lt hsw'
    rw [hiw, him, hgs]
    have hxdecomp := Nat.div_add_mod x w
    rw [Nat.mul_comm w (x / w)] at hxdecomp
    dsimp [k, r] at *
    exact hxdecomp

theorem sound_PowerOfTwo :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.PowerOfTwo.Goal := by
  intro c d hw hd
  rw [rows_grayTile c, rows_linearOrdinal c]
  by_cases hs : c.step = 0
  · simp [hs]
  · have hwpos : 0 < width c := width_pos c
    have hperm := grayTile_index_perm c.n (width c) d hwpos hw hd
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hperm
    simpa [Function.comp_def, Nat.mul_assoc] using hval



private def progVals (p : Progression) : List Int :=
  (List.range p.n).map (fun i : Nat => p.lo + (i : Int) * p.step)

private theorem contains_iff_progVals (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ progVals p := by
  rw [contains_iff_mem]
  unfold progMem progVal progVals
  rw [List.mem_map]
  constructor
  · rintro ⟨k, hk, hval⟩
    refine ⟨k, ?_, hval.symm⟩
    simpa using hk
  · rintro ⟨k, hk, hval⟩
    refine ⟨k, ?_, hval.symm⟩
    simpa using hk

private theorem count_eq_one_of_nodup_mem (l : List Int) (hnd : l.Nodup) {a : Int}
    (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      rw [List.count_cons]
      simp only [List.nodup_cons] at hnd
      rcases hnd with ⟨hxnot, hxsnd⟩
      simp only [List.mem_cons] at ha
      by_cases hxa : x = a
      · subst x
        have hc : xs.count a = 0 := List.count_eq_zero_of_not_mem hxnot
        simp [hc]
      · have hax : a ∈ xs := by rcases ha with rfl | h <;> simp_all
        have hc := ih hxsnd hax
        simp [hxa, hc]

private theorem perm_of_nodup_mem (l₁ l₂ : List Int) (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ a : Int, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  apply (List.perm_iff_count).mpr
  intro a
  by_cases ha : a ∈ l₁
  · have ha2 : a ∈ l₂ := (hmem a).mp ha
    rw [count_eq_one_of_nodup_mem l₁ h₁ ha, count_eq_one_of_nodup_mem l₂ h₂ ha2]
  · have ha2 : ¬ a ∈ l₂ := fun h => ha ((hmem a).mpr h)
    rw [List.count_eq_zero_of_not_mem ha, List.count_eq_zero_of_not_mem ha2]

private theorem nodup_map_of_injective {α β : Type} (f : α → β) (l : List α)
    (hf : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hnd : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.nodup_cons] at hnd ⊢
      rcases hnd with ⟨hxnot, hxsnd⟩
      constructor
      · intro hxmem
        simp only [List.mem_map] at hxmem
        rcases hxmem with ⟨y, hy, hfy⟩
        have hxy : x = y := hf x (by simp) y (by simp [hy]) hfy.symm
        subst y
        exact hxnot hy
      · exact ih (fun a ha b hb hab => hf a (by simp [ha]) b (by simp [hb]) hab) hxsnd

private theorem progVals_nodup (p : Progression) (hp : 0 < p.step) : (progVals p).Nodup := by
  unfold progVals
  apply nodup_map_of_injective
  · intro a ha b hb hab
    have hstep : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hp)
    have hmul : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
    have habI : (a : Int) = (b : Int) := (Int.mul_eq_mul_right_iff hstep).mp hmul
    exact_mod_cast habI
  · exact List.nodup_range

private theorem countP_contains_prog (p q : Progression) :
    (progVals p).countP (fun v => contains q v) =
      (intersection p q).n * (if p.step == 0 then p.n else 1) := by
  by_cases hpn : p.n = 0
  · simp [progVals, hpn, intersection, contains]
  by_cases hqn : q.n = 0
  · simp [progVals, hqn, intersection, contains]
  by_cases hps0 : p.step = 0
  · have hvals : progVals p = (List.range p.n).map (fun _ : Nat => p.lo) := by
      unfold progVals
      apply List.map_congr_left
      intro i hi
      simp [hps0]
    have hcnt : ((List.range p.n).map (fun _ : Nat => p.lo)).countP (fun v => contains q v) =
        if contains q p.lo then p.n else 0 := by
      induction p.n with
      | zero => simp
      | succ n ih =>
          rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append,
            List.countP_append]
          by_cases hc : contains q p.lo <;> simp [hc, ih]
    have hinteq : intersection p q = ⟨p.lo, 1, if contains q p.lo then 1 else 0⟩ := by
      unfold intersection
      simp [hpn, hqn, hps0]
    rw [hvals, hcnt, hinteq]
    by_cases hc : contains q p.lo <;> simp [hc, hps0, Nat.mul_comm]
  · have hps : 0 < p.step := Nat.pos_of_ne_zero hps0
    let r := intersection p q
    have hrstep : 0 < r.step := by dsimp [r]; exact intersection_step_pos p q
    have hvalsnodup : (progVals p).Nodup := progVals_nodup p hps
    have hrnodup : (progVals r).Nodup := progVals_nodup r hrstep
    have hfilternodup : ((progVals p).filter (fun v => contains q v)).Nodup :=
      hvalsnodup.filter (fun v => contains q v)
    have hperm : List.Perm ((progVals p).filter (fun v => contains q v)) (progVals r) :=
      perm_of_nodup_mem _ _ hfilternodup hrnodup (by
        intro v
        rw [List.mem_filter]
        rw [← contains_iff_progVals p v]
        rw [← contains_iff_progVals r v]
        simpa [r] using (intersection_contains p q v).symm)
    change (progVals p).countP (fun v => contains q v) = r.n * (if p.step == 0 then p.n else 1)
    rw [List.countP_eq_length_filter, hperm.length_eq]
    simp [progVals, hps0]

/-! ## Lane values versus progression values -/

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_nat_mul_indicator (l : List Int) (c : Nat) (p : Int → Bool) :
    (l.map (fun v => c * (if p v then 1 else 0))).sum = c * l.countP p := by
  rw [countP_eq_sum_nat]
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih, Nat.mul_add]

private def laneVals (a : Lane) : List Int :=
  (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step)

private theorem reverse_range_perm (n : Nat) :
    ((List.range n).map (fun q : Nat => n - 1 - q)).Perm (List.range n) := by
  have hnod : ((List.range n).map (fun q : Nat => n - 1 - q)).Nodup := by
    apply nodup_map_of_injective
    · intro a ha b hb hab
      have ha' : a < n := List.mem_range.mp ha
      have hb' : b < n := List.mem_range.mp hb
      omega
    · exact List.nodup_range
  exact (List.perm_ext_iff_of_nodup hnod List.nodup_range).mpr (by
    intro x
    simp only [List.mem_map, List.mem_range]
    constructor
    · rintro ⟨q, hq, rfl⟩
      omega
    · intro hx
      refine ⟨n - 1 - x, by omega, ?_⟩
      have hxle : x ≤ n - 1 := by omega
      exact Nat.sub_sub_self hxle)

private theorem laneVals_perm_progVals (a : Lane) :
    (laneVals a).Perm (progVals (values a)) := by
  unfold laneVals progVals
  by_cases hzero : a.step = 0
  · have hv : values a = ⟨a.base, 0, a.n⟩ := by simp [values, hzero]
    rw [hv]
    have heq : (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step) =
        (List.range a.n).map (fun i : Nat => a.base + (i : Int) * (0 : Int)) := by
      apply List.map_congr_left
      intro q hq
      simp [hzero]
    rw [heq]
    simp
  · by_cases hpos : 0 < a.step
    · have hv : values a = ⟨a.base, a.step.natAbs, a.n⟩ := by
        unfold values
        simp [hpos, (show ¬ a.step < 0 by omega)]
      have hnat : ((a.step.natAbs : Nat) : Int) = a.step := Int.ofNat_natAbs_of_nonneg (by omega)
      rw [hv, hnat]
    · have hneg : a.step < 0 := by omega
      have hv : values a = ⟨a.base + ((a.n - 1 : Nat) : Int) * a.step, a.step.natAbs, a.n⟩ := by
        unfold values
        simp [hneg]
      have hnat : ((a.step.natAbs : Nat) : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.ofNat_natAbs_of_nonneg (by omega : (0 : Int) ≤ -a.step)
      rw [hv, hnat]
      let f : Nat → Int := fun q => a.base + ((a.n - 1 - q : Nat) : Int) * a.step
      have hleft : (List.range a.n).map (fun q : Nat => a.base + (q : Int) * a.step) =
          ((List.range a.n).map (fun q : Nat => a.n - 1 - q)).map f := by
        rw [List.map_map]
        apply List.map_congr_left
        intro q hq
        dsimp [f]
        have hq' : q < a.n := List.mem_range.mp hq
        have hqle : q ≤ a.n - 1 := by omega
        have hsub : a.n - 1 - (a.n - 1 - q) = q := Nat.sub_sub_self hqle
        rw [hsub]
      have hright : (List.range a.n).map f =
          (List.range a.n).map (fun i : Nat => a.base + ((a.n - 1 : Nat) : Int) * a.step + (i : Int) * (-a.step)) := by
        apply List.map_congr_left
        intro i hi
        dsimp [f]
        have hi' : i < a.n := List.mem_range.mp hi
        have hile : i ≤ a.n - 1 := by omega
        rw [Int.ofNat_sub hile]
        grind
      change (List.map (fun q : Nat => a.base + (q : Int) * a.step) (List.range a.n)).Perm
        (List.map (fun i : Nat => a.base + ((a.n - 1 : Nat) : Int) * a.step + (i : Int) * (-a.step)) (List.range a.n))
      rw [hleft]
      rw [← hright]
      exact (reverse_range_perm a.n).map f

private theorem countP_contains_lane (a : Lane) (q : Progression) :
    (laneVals a).countP (fun v => contains q v) =
      (intersection (values a) q).n * (if a.step == 0 then a.n else 1) := by
  rw [(laneVals_perm_progVals a).countP_eq (fun v => contains q v)]
  have hvstep : ((values a).step == 0) = (a.step == 0) := by
    unfold values
    by_cases h0 : a.step = 0
    · simp [h0]
    · have hn : a.step.natAbs ≠ 0 := by
        intro hn0
        exact h0 (Int.natAbs_eq_zero.mp hn0)
      rw [beq_eq_false_iff_ne.mpr hn, beq_eq_false_iff_ne.mpr h0]
  have hvnum : (values a).n = a.n := rfl
  simpa [hvstep, hvnum] using countP_contains_prog (values a) q

private theorem countP_eq_laneValue (b : Lane) (hbs : b.step ≠ 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then 1 else 0 := by
  by_cases hc : contains (values b) v = true
  · have hmem : v ∈ progVals (values b) := (contains_iff_progVals (values b) v).mp hc
    have hperm := laneVals_perm_progVals b
    have hmem' : v ∈ laneVals b := hperm.mem_iff.mpr hmem
    unfold laneVals at hmem'
    rcases List.mem_map.mp hmem' with ⟨i, hi, hival⟩
    have hnod : (laneVals b).Nodup := by
      rw [(laneVals_perm_progVals b).nodup_iff]
      apply progVals_nodup (values b)
      dsimp [values]
      exact Int.natAbs_pos.mpr hbs
    have hfperm : ((List.range b.n).filter (fun j : Nat => v == b.base + b.step * (j : Int))).Perm [i] := by
      have hfnod : ((List.range b.n).filter (fun j : Nat => v == b.base + b.step * (j : Int))).Nodup :=
        List.nodup_range.filter _
      exact (List.perm_ext_iff_of_nodup hfnod (by simp)).mpr (by
        intro j
        simp only [List.mem_filter, List.mem_singleton]
        constructor
        · rintro ⟨hj, hbeq⟩
          have hjv : v = b.base + b.step * (j : Int) := (beq_iff_eq.mp hbeq)
          have hiv : v = b.base + b.step * (i : Int) := by simpa [Int.mul_comm] using hival.symm
          have hi_eq : v = b.base + (i : Int) * b.step := by simpa [Int.mul_comm] using hival.symm
          have hj_eq : v = b.base + b.step * (j : Int) := beq_iff_eq.mp hbeq
          have hcancel : (i : Int) * b.step = b.step * (j : Int) := by
            calc
              (i : Int) * b.step = b.base + (i : Int) * b.step - b.base := by omega
              _ = v - b.base := by rw [hi_eq]
              _ = b.base + b.step * (j : Int) - b.base := by rw [hj_eq]
              _ = b.step * (j : Int) := by omega
          have hmul : (j : Int) * b.step = (i : Int) * b.step := by simpa [Int.mul_comm] using hcancel.symm
          have hstep : b.step ≠ 0 := hbs
          exact_mod_cast (Int.mul_eq_mul_right_iff hstep).mp hmul
        · intro hji
          subst j
          exact ⟨hi, by simpa [Int.mul_comm] using hival.symm⟩)
    rw [List.countP_eq_length_filter, hfperm.length_eq]
    simp [hc]
  · have hzero : (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) = 0 := by
      rw [List.countP_eq_zero]
      intro i hi
      intro hbeq
      have hv' : v = b.base + b.step * (i : Int) := beq_iff_eq.mp hbeq
      apply hc
      apply (contains_iff_progVals (values b) v).mpr
      rw [← (laneVals_perm_progVals b).mem_iff]
      unfold laneVals
      exact List.mem_map.mpr ⟨i, hi, by simpa [Int.mul_comm] using hv'.symm⟩
    simp [hzero, hc]

private theorem countP_eq_laneValue_const (b : Lane) (hbz : b.step = 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then b.n else 0 := by
  by_cases hv : v == b.base
  · have hv' : v = b.base := beq_iff_eq.mp hv
    subst v
    have hc : contains (values b) b.base = true ↔ 0 < b.n := by
      rw [contains_iff_progVals]
      unfold progVals values
      simp [hbz]
      constructor
      · rintro ⟨k, hk⟩
        omega
      · intro hk
        exact ⟨0, hk⟩
    by_cases hn : 0 < b.n
    · have hc' : contains (values b) b.base = true := hc.mpr hn
      simp [hbz, hv, hc']
    · have hc' : contains (values b) b.base ≠ true := by simpa [hc] using hn
      have hbzero : b.n = 0 := by omega
      simp [hbz, hv, hc', hbzero]
  · have hc' : contains (values b) v ≠ true := by
      intro hh
      have hmem := (contains_iff_progVals (values b) v).mp hh
      have hmem' : v ∈ laneVals b := (laneVals_perm_progVals b).mem_iff.mpr hmem
      unfold laneVals at hmem'
      rcases List.mem_map.mp hmem' with ⟨i, hi, hival⟩
      have : v = b.base := by simpa [hbz] using hival.symm
      exact hv (by rw [this]; simp)
    simp [hbz, hv, hc']

private theorem lane_join_count (a b : Lane) :
    ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  have hcnt := countP_contains_lane a (values b)
  unfold joinCount
  by_cases hbz : b.step = 0
  · have hrep : ∀ v : Int,
        (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
          b.n * (if contains (values b) v then 1 else 0) := by
      intro v
      rw [countP_eq_laneValue_const b hbz v]
      by_cases hc : contains (values b) v <;> simp [hc]
    rw [show ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      ((laneVals a).map (fun v => b.n * (if contains (values b) v then 1 else 0))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      exact hrep v]
    rw [sum_nat_mul_indicator]
    rw [hcnt]
    simp [hbz]
    ac_rfl
  · have hrep : ∀ v : Int,
        (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
          if contains (values b) v then 1 else 0 := countP_eq_laneValue b hbz
    rw [show ((laneVals a).map (fun v =>
      (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)))).sum =
      (laneVals a).countP (fun v => contains (values b) v) by
      rw [countP_eq_sum_nat]
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      rw [hrep v]]
    rw [hcnt]
    simp [hbz]

/-! ## The alternating-sign page as two lanes -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun q : Nat => (a.start + q * a.stride, a.base + (q : Int) * a.step))

private theorem mem_lanePoints (a : Lane) (x : Nat × Int) :
    x ∈ lanePoints a ↔ ∃ q, q < a.n ∧ x = (a.start + q * a.stride, a.base + (q : Int) * a.step) := by
  unfold lanePoints
  rw [List.mem_map]
  constructor
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mp hq, hval.symm⟩
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mpr hq, hval.symm⟩

private theorem lanePoints_nodup (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  apply nodup_map_of_injective
  · intro i hi j hj h
    have hfst := congrArg Prod.fst h
    have hmul : i * a.stride = j * a.stride := Nat.add_left_cancel hfst
    exact Nat.eq_of_mul_eq_mul_right hs hmul
  · exact List.nodup_range

private theorem rows_gray_pairs (c : Config) :
    rows GrayTile.layout c =
      (List.range c.n).map (fun i => (i, c.base + c.step *
        ((i / width c * width c + gray (i % width c) : Nat) : Int))) := by
  apply rows_range_map
  intro i hi
  simp [GrayTile.layout, gray]

private theorem gray_cross_lane_ne (c : Config) :
    let w := width c
    ∀ r1 r2, r1 < min w c.n → r2 < min w c.n → r1 ≠ r2 →
      ∀ x ∈ lanePoints (lane c.n r1 w (c.base + c.step * gray r1) (c.step * w)),
      ∀ y ∈ lanePoints (lane c.n r2 w (c.base + c.step * gray r2) (c.step * w)), x ≠ y := by
  dsimp only
  intro r1 r2 hr1 hr2 hrne x hx y hy heq
  rcases (mem_lanePoints _ x).mp hx with ⟨j1, hj1, hxv⟩
  rcases (mem_lanePoints _ y).mp hy with ⟨j2, hj2, hyv⟩
  have hfst := congrArg Prod.fst (hxv.symm.trans (heq.trans hyv))
  dsimp [lane] at hfst
  have hw : 0 < width c := by dsimp [width]; omega
  have hm1 : (r1 + j1 * width c) % width c = r1 := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
  have hm2 : (r2 + j2 * width c) % width c = r2 := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
  have hr : r1 = r2 := by
    rw [← hm1, ← hm2, hfst]
  exact hrne hr

private theorem gray_rows_perm (c : Config) :
    (rows GrayTile.layout c).Perm ((GrayTile.layout.lanes c).flatMap lanePoints) := by
  rw [rows_gray_pairs c]
  have hw : 0 < width c := by dsimp [width]; omega
  let laneAt : Nat → Lane := fun r => lane c.n r (width c) (c.base + c.step * gray r) (c.step * width c)
  have hlanes : GrayTile.layout.lanes c = (List.range (min (width c) c.n)).map laneAt := by
    dsimp [GrayTile.layout, striped, laneAt]
    simp [gray]
  rw [hlanes]
  have hL : ((List.range c.n).map (fun i => (i, c.base + c.step *
      ((i / width c * width c + gray (i % width c) : Nat) : Int)))).Nodup := by
    apply nodup_map_of_injective
    · intro i hi j hj h
      exact congrArg Prod.fst h
    · exact List.nodup_range
  have hcross := gray_cross_lane_ne c
  have hR2gen : ∀ xs : List Nat, xs.Nodup → (∀ a ∈ xs, a < min (width c) c.n) →
      xs.Pairwise (fun r1 r2 => ∀ x ∈ lanePoints (laneAt r1),
        ∀ y ∈ lanePoints (laneAt r2), x ≠ y) := by
    intro xs
    induction xs with
    | nil => simp
    | cons a as ih =>
        intro hnd hbound
        rw [List.nodup_cons] at hnd
        rcases hnd with ⟨ha_not, hnd_as⟩
        rw [List.pairwise_cons]
        constructor
        · intro b hb
          apply hcross a b (hbound a (by simp)) (hbound b (by simp [hb]))
          intro hr
          subst b
          exact ha_not hb
        · apply ih hnd_as
          intro b hb
          exact hbound b (by simp [hb])
  have hR2 := hR2gen (List.range (min (width c) c.n)) List.nodup_range (fun a ha => List.mem_range.mp ha)
  have hR : ((List.range (min (width c) c.n)).flatMap (fun r => lanePoints (laneAt r))).Nodup := by
    rw [List.nodup_iff_pairwise_ne, List.pairwise_flatMap]
    constructor
    · intro r hr
      apply lanePoints_nodup
      dsimp [laneAt, lane]
      simp [hw]
    · exact hR2
  rw [List.flatMap_map]
  apply (List.perm_ext_iff_of_nodup hL hR).mpr
  intro x
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hin : i < c.n := List.mem_range.mp hi
    let r := i % width c
    let j := i / width c
    have hr : r < min (width c) c.n := by
      exact (Nat.lt_min).mpr ⟨Nat.mod_lt _ hw, Nat.lt_of_le_of_lt (Nat.mod_le _ _) hin⟩
    have hi_decomp : i = r + j * width c := by
      dsimp [r, j]
      have hd := Nat.div_add_mod i (width c)
      rw [Nat.mul_comm (width c) (i / width c)] at hd
      omega
    have hj : j < (lane c.n r (width c) (c.base + c.step * gray r) (c.step * width c)).n := by
      unfold lane
      apply (pos_lt_iff r (width c) j c.n hw).mp
      omega
    apply List.mem_flatMap.mpr
    refine ⟨r, List.mem_range.mpr hr, ?_⟩
    apply (mem_lanePoints (laneAt r) _).mpr
    refine ⟨j, by simpa [laneAt, lane] using hj, ?_⟩
    apply Prod.ext
    · dsimp [laneAt, lane]
      omega
    · dsimp [laneAt, lane, r, j]
      simp only [Int.mul_add, Int.natCast_add, Int.natCast_mul]
      ac_rfl
  · intro hx
    rcases List.mem_flatMap.mp hx with ⟨r, hr, hx⟩
    rcases (mem_lanePoints (laneAt r) x).mp hx with ⟨j, hj, rfl⟩
    have hr' : r < min (width c) c.n := List.mem_range.mp hr
    have hjlt : j < (lane c.n r (width c) (c.base + c.step * gray r) (c.step * width c)).n := by
      simpa [laneAt] using hj
    have hi : r + j * width c < c.n := by
      dsimp [lane] at hjlt
      exact (pos_lt_iff r (width c) j c.n hw).mpr hjlt
    apply List.mem_map.mpr
    refine ⟨r + j * width c, List.mem_range.mpr hi, ?_⟩
    apply Prod.ext
    · dsimp [laneAt, lane]

    · have hdivij : (r + j * width c) / width c = j := by
        rw [Nat.mul_comm j (width c), Nat.add_mul_div_left r j hw,
          Nat.div_eq_of_lt (by omega)]
        simp
      have hmodij : (r + j * width c) % width c = r := by
        rw [Nat.mul_comm j (width c), Nat.add_mul_mod_self_left,
          Nat.mod_eq_of_lt (by omega)]
      rw [hdivij, hmodij]
      dsimp [laneAt, lane]
      simp only [Int.mul_add, Int.natCast_add, Int.natCast_mul]
      ac_rfl

private theorem lanePoints_count_sum (a b : Lane) :
    ((lanePoints a).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map ((fun v : Int =>
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int))) ∘
      (fun q : Nat => a.base + (q : Int) * a.step))).sum = joinCount a b
  have h := lane_join_count a b
  unfold laneVals at h
  simpa [Function.comp_apply, Int.mul_comm] using h

private theorem flatMap_lanePoints_count_sum (ls : List Lane) (b : Lane) :
    ((ls.flatMap lanePoints).map (fun r : Nat × Int =>
      (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      (ls.map (fun a => joinCount a b)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih, lanePoints_count_sum]

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.ResidueJoin.Goal := by
  intro c b
  have hp := (gray_rows_perm c).map (fun r : Nat × Int =>
    (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))
  rw [perm_sum_nat hp]
  exact flatMap_lanePoints_count_sum (GrayTile.layout.lanes c) b


/-! ## Round 2: DeleteProgression and AlignedProduct for GrayTile lanes -/

private def laneValueAt (a : Lane) (v : Int) : Int :=
  a.base + ((v - (a.start : Int)) / (a.stride : Int)) * a.step

private theorem progVal_positions (a : Lane) (k : Nat) :
    progVal (positions a) k = a.start + k * a.stride := by
  unfold progVal positions
  rfl

private theorem laneValueAt_apply (a : Lane) (ha : 0 < a.stride) (k : Nat) :
    laneValueAt a (a.start + k * a.stride) = a.base + (k : Int) * a.step := by
  dsimp [laneValueAt]
  have hsub : (a.start + k * a.stride : Int) - (a.start : Int) =
      (k : Int) * (a.stride : Int) := by
    push_cast
    omega
  rw [hsub]
  have hne : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt ha)
  have hdiv : ((k : Int) * (a.stride : Int)) / (a.stride : Int) = (k : Int) := by
    rw [Int.mul_comm]
    exact Int.mul_ediv_cancel_left (k : Int) hne
  rw [hdiv]

private theorem progVals_filter_inter_perm (p q : Progression) (hp : 0 < p.step) :
    List.Perm ((progVals p).filter (fun v => contains q v)) (progVals (intersection p q)) := by
  apply perm_of_nodup_mem
  · exact (progVals_nodup p hp).filter _
  · exact progVals_nodup _ (intersection_step_pos p q)
  · intro v
    rw [List.mem_filter]
    rw [← contains_iff_progVals p v, ← contains_iff_progVals (intersection p q) v]
    exact (intersection_contains p q v).symm

private theorem lanePoints_filter_snd_sum (a : Lane) (ha : 0 < a.stride) (p : Int → Bool) :
    (((lanePoints a).filter (fun r => p r.1)).map Prod.snd).sum =
      (((progVals (positions a)).filter p).map (laneValueAt a)).sum := by
  unfold lanePoints progVals
  dsimp [positions]
  induction (List.range a.n) with
  | nil => simp
  | cons k ks ih =>
      by_cases hk : p (a.start + k * a.stride)
      · simp [hk, ih, laneValueAt_apply a ha k]
      · simp [hk, ih]

private theorem sum_arith (n : Nat) (x dx : Int) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * dx)).sum =
      (n : Int) * x + tri n * dx := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [sum_range_succ, ih, tri_succ]
      simp [Int.natCast_add]
      grind

private theorem sum_product_arith (x y dx dy : Int) (n : Nat) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [sum_range_succ, ih, tri_succ, squares_succ]
      simp [Int.natCast_add]
      grind

private theorem inter_step_dvd_of_pos (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hrn : 0 < (intersection p q).n) : p.step ∣ (intersection p q).step := by
  have hpn : p.n ≠ 0 := by
    intro h
    simp [intersection, h] at hrn
  have hqn : q.n ≠ 0 := by
    intro h
    simp [intersection, h] at hrn
  have hps0 : p.step ≠ 0 := by omega
  have hqs0 : q.step ≠ 0 := by omega
  have hgap : (q.lo - p.lo) % (p.step.gcd q.step : Int) = 0 := by
    by_cases hz : (q.lo - p.lo) % (p.step.gcd q.step : Int) = 0
    · exact hz
    · exfalso
      have hrn0 : (intersection p q).n = 0 := by
        unfold intersection
        rw [if_neg (by simp [hpn, hqn]), if_neg (by simp [hps0]),
          if_neg (by simp [hqs0]), if_pos (by simpa using hz)]
        rfl
      omega
  have hperLcm : (p.step / p.step.gcd q.step) * q.step = Nat.lcm p.step q.step := by
    rw [Nat.lcm_eq_mul_div]
    exact (Nat.mul_div_right_comm (Nat.gcd_dvd_left p.step q.step) q.step).symm
  have hstepEq : (intersection p q).step = (p.step / p.step.gcd q.step) * q.step := by
    unfold intersection
    rw [if_neg (by simp [hpn, hqn]), if_neg (by simp [hps0]),
      if_neg (by simp [hqs0]), if_neg (by
        change ¬ ((q.lo - p.lo) % (p.step.gcd q.step : Int) != 0) = true
        simp [hgap])]
    rfl
  rw [hstepEq, hperLcm]
  exact Nat.dvd_lcm_left _ _

private theorem inter_step_of_const (p q : Progression) (hp : 0 < p.step)
    (hq0 : q.step = 0) : (intersection p q).step = 1 := by
  unfold intersection
  by_cases hp0 : p.n = 0
  · simp [hp0]
  by_cases hqn : q.n = 0
  · simp [hp0, hqn]
  have hps0 : p.step ≠ 0 := by omega
  rw [if_neg (by simp [hp0, hqn]), if_neg (by simp [hps0]), if_pos (by simp [hq0])]
  rfl

private theorem inter_n_le_one_of_const (p q : Progression) (hp : 0 < p.step)
    (hq0 : q.step = 0) : (intersection p q).n ≤ 1 := by
  unfold intersection
  by_cases hp0 : p.n = 0
  · simp [hp0]
  by_cases hqn : q.n = 0
  · simp [hp0, hqn]
  have hps0 : p.step ≠ 0 := by omega
  rw [if_neg (by simp [hp0, hqn]), if_neg (by simp [hps0]), if_pos (by simp [hq0])]
  change (if contains p q.lo = true then 1 else 0) ≤ 1
  split <;> omega


private theorem deletedSum_eq_filter_sum (a : Lane) (ha : 0 < a.stride) (mask : Progression) :
    (((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd).sum = deletedSum a mask := by
  rw [lanePoints_filter_snd_sum a ha]
  let r := intersection (positions a) mask
  have hp : 0 < (positions a).step := by simpa [positions] using ha
  have hrp := progVals_filter_inter_perm (positions a) mask hp
  change (((progVals (positions a)).filter (fun v => contains mask v)).map (laneValueAt a)).sum =
    deletedSum a mask
  rw [list_sum_perm (hrp.map (laneValueAt a))]
  rw [show ((progVals r).map (laneValueAt a)) =
      ((List.range r.n).map (fun j : Nat => laneValueAt a (progVal r j))) by
    unfold progVals
    rw [List.map_map]
    rfl]
  change ((List.range r.n).map (fun j : Nat => laneValueAt a (progVal r j))).sum =
    (r.n : Int) * firstAt a r + tri r.n * slopeAt a r
  by_cases hm0 : mask.step = 0
  · have hrnle : r.n ≤ 1 := by
      dsimp [r]
      exact inter_n_le_one_of_const (positions a) mask hp hm0
    by_cases hrn0 : r.n = 0
    · rw [hrn0]
      simp [deletedSum, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
    · have hrn1 : r.n = 1 := by omega
      rw [hrn1]
      simp [deletedSum, firstAt, laneValueAt, tri,
        TinylambOpt.ResearchGoals.PairAggregates.choose2, progVal]
  · have hmp : 0 < mask.step := by omega
    by_cases hrn0 : r.n = 0
    · rw [hrn0]
      simp [deletedSum, tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
    · have hrnpos : 0 < r.n := by omega
      have hdvd : a.stride ∣ r.step := by
        dsimp [r]
        exact inter_step_dvd_of_pos (positions a) mask hp hmp hrnpos
      have hstepdiv : r.step = a.stride * (r.step / a.stride) := (Nat.mul_div_cancel' hdvd).symm
      have hfirstmem : contains r (progVal r 0) = true := by
        apply (contains_iff_mem r _).mpr
        exact ⟨0, hrnpos, by simp [progVal]⟩
      have hfirstA0 : contains (positions a) r.lo = true := by
        have hc := ((intersection_contains (positions a) mask r.lo).mp ?_).1
        · simpa [progVal] using hc
        · simpa [progVal] using hfirstmem
      rcases (contains_iff_mem (positions a) r.lo).mp hfirstA0 with ⟨k0, hk0, hv0⟩
      unfold progVal at hv0
      have hv0' : r.lo - (a.start : Int) = (k0 : Int) * (a.stride : Int) := by
        dsimp [positions] at hv0
        omega
      have hne : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt ha)
      have hdiv0 : (r.lo - (a.start : Int)) / (a.stride : Int) = (k0 : Int) := by
        rw [hv0', Int.mul_comm]
        exact Int.mul_ediv_cancel_left (k0 : Int) hne
      have hterm : ∀ j, j < r.n → laneValueAt a (progVal r j) =
          firstAt a r + (j : Int) * slopeAt a r := by
        intro j hj
        let m : Nat := r.step / a.stride
        have hsub : r.lo + (j : Int) * (r.step : Int) - (a.start : Int) =
            (k0 : Int) * (a.stride : Int) + (j : Int) * (r.step : Int) := by
          omega
        have hstepdivInt : (r.step : Int) = (a.stride : Int) * (m : Int) := by
          dsimp [m]
          exact_mod_cast hstepdiv
        have hleft : laneValueAt a (progVal r j) =
            a.base + ((k0 : Int) + (j : Int) * (m : Int)) * a.step := by
          dsimp [laneValueAt]
          rw [progVal, hsub, hstepdivInt]
          have hrewrite : (k0 : Int) * (a.stride : Int) +
                (j : Int) * ((a.stride : Int) * (m : Int)) =
              ((k0 : Int) + (j : Int) * (m : Int)) * (a.stride : Int) := by
            grind
          rw [hrewrite]
          have hdiv : ((k0 : Int) + (j : Int) * (m : Int)) * (a.stride : Int) /
              (a.stride : Int) = (k0 : Int) + (j : Int) * (m : Int) := by
            rw [Int.mul_comm]
            exact Int.mul_ediv_cancel_left _ hne
          rw [hdiv]
        have hright : firstAt a r + (j : Int) * slopeAt a r =
            a.base + ((k0 : Int) + (j : Int) * (m : Int)) * a.step := by
          dsimp [firstAt, slopeAt]
          rw [hdiv0]
          dsimp [m]
          grind
        exact hleft.trans hright.symm
      rw [show ((List.range r.n).map (fun j : Nat => laneValueAt a (progVal r j))) =
          ((List.range r.n).map (fun j : Nat => firstAt a r + (j : Int) * slopeAt a r)) by
        apply List.map_congr_left
        intro j hj
        exact hterm j (List.mem_range.mp hj)]
      rw [sum_arith]

private theorem flatMap_filter_snd_sum (ls : List Lane) (p : Nat × Int → Bool) :
    (((ls.flatMap lanePoints).filter p).map Prod.snd).sum =
      (ls.map (fun a => (((lanePoints a).filter p).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.filter_append, List.map_append, List.sum_append, ih]

private theorem lanePoints_snd_sum (a : Lane) :
    ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map (fun k : Nat => a.base + (k : Int) * a.step)).sum = _
  rw [← partialSum_eq_sum]

private theorem sum_filter_compl {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem gray_lane_delete_sum (a : Lane) (ha : 0 < a.stride) (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  have hp := sum_filter_compl (lanePoints a) (fun r : Nat × Int => contains mask r.1) Prod.snd
  have hdel := deletedSum_eq_filter_sum a ha mask
  have htot := lanePoints_snd_sum a
  rw [hdel, htot] at hp
  omega

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.DeleteProgression.Goal := by
  intro c mask
  have hp := List.Perm.filter (fun r : Nat × Int => !contains mask r.1) (gray_rows_perm c)
  rw [list_sum_perm (hp.map Prod.snd)]
  rw [flatMap_filter_snd_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have hstride : 0 < a.stride := by
    have hw : 0 < width c := width_pos c
    simp [GrayTile.layout, striped] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, hw]
  exact gray_lane_delete_sum a hstride mask


/-! ## AlignedProduct -/

private theorem inter_step_dvd_right (p q : Progression) (hp : 0 < p.step) (hq : 0 < q.step)
    (hrn : 0 < (intersection p q).n) : q.step ∣ (intersection p q).step := by
  have hpn : p.n ≠ 0 := by
    intro h
    simp [intersection, h] at hrn
  have hqn : q.n ≠ 0 := by
    intro h
    simp [intersection, h] at hrn
  have hps0 : p.step ≠ 0 := by omega
  have hqs0 : q.step ≠ 0 := by omega
  have hgap : (q.lo - p.lo) % (p.step.gcd q.step : Int) = 0 := by
    by_cases hz : (q.lo - p.lo) % (p.step.gcd q.step : Int) = 0
    · exact hz
    · exfalso
      have hrn0 : (intersection p q).n = 0 := by
        unfold intersection
        rw [if_neg (by simp [hpn, hqn]), if_neg (by simp [hps0]),
          if_neg (by simp [hqs0]), if_pos (by simpa using hz)]
        rfl
      omega
  have hperLcm : (p.step / p.step.gcd q.step) * q.step = Nat.lcm p.step q.step := by
    rw [Nat.lcm_eq_mul_div]
    exact (Nat.mul_div_right_comm (Nat.gcd_dvd_left p.step q.step) q.step).symm
  have hstepEq : (intersection p q).step = (p.step / p.step.gcd q.step) * q.step := by
    unfold intersection
    rw [if_neg (by simp [hpn, hqn]), if_neg (by simp [hps0]),
      if_neg (by simp [hqs0]), if_neg (by
        change ¬ ((q.lo - p.lo) % (p.step.gcd q.step : Int) != 0) = true
        simp [hgap])]
    rfl
  rw [hstepEq, hperLcm]
  exact Nat.dvd_lcm_right _ _

private theorem laneValueAt_of_prog (a : Lane) (ha : 0 < a.stride) (r : Progression)
    (hrnpos : 0 < r.n) (hdvd : a.stride ∣ r.step)
    (hall : ∀ j, j < r.n → contains (positions a) (progVal r j) = true) :
    ∀ j, j < r.n → laneValueAt a (progVal r j) = firstAt a r + (j : Int) * slopeAt a r := by
  have hfirst := hall 0 hrnpos
  have hfirstA0 : contains (positions a) r.lo = true := by simpa [progVal] using hfirst
  rcases (contains_iff_mem (positions a) r.lo).mp hfirstA0 with ⟨k0, hk0, hv0⟩
  unfold progVal at hv0
  have hv0' : r.lo - (a.start : Int) = (k0 : Int) * (a.stride : Int) := by
    dsimp [positions] at hv0
    omega
  have hne : (a.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt ha)
  have hdiv0 : (r.lo - (a.start : Int)) / (a.stride : Int) = (k0 : Int) := by
    rw [hv0', Int.mul_comm]
    exact Int.mul_ediv_cancel_left (k0 : Int) hne
  have hstepdiv : r.step = a.stride * (r.step / a.stride) := (Nat.mul_div_cancel' hdvd).symm
  intro j hj
  let m : Nat := r.step / a.stride
  have hsub : r.lo + (j : Int) * (r.step : Int) - (a.start : Int) =
      (k0 : Int) * (a.stride : Int) + (j : Int) * (r.step : Int) := by
    omega
  have hstepdivInt : (r.step : Int) = (a.stride : Int) * (m : Int) := by
    dsimp [m]
    exact_mod_cast hstepdiv
  have hleft : laneValueAt a (progVal r j) =
      a.base + ((k0 : Int) + (j : Int) * (m : Int)) * a.step := by
    dsimp [laneValueAt]
    rw [progVal, hsub, hstepdivInt]
    have hrewrite : (k0 : Int) * (a.stride : Int) +
          (j : Int) * ((a.stride : Int) * (m : Int)) =
        ((k0 : Int) + (j : Int) * (m : Int)) * (a.stride : Int) := by
      grind
    rw [hrewrite]
    have hdiv : ((k0 : Int) + (j : Int) * (m : Int)) * (a.stride : Int) /
        (a.stride : Int) = (k0 : Int) + (j : Int) * (m : Int) := by
      rw [Int.mul_comm]
      exact Int.mul_ediv_cancel_left _ hne
    rw [hdiv]
  have hright : firstAt a r + (j : Int) * slopeAt a r =
      a.base + ((k0 : Int) + (j : Int) * (m : Int)) * a.step := by
    dsimp [firstAt, slopeAt]
    rw [hdiv0]
    dsimp [m]
    grind
  exact hleft.trans hright.symm

private def bValueAt (b : Lane) (v : Int) : Int :=
  b.base + ((v - (b.start : Int)) / (b.stride : Int)) * b.step

private theorem bValueAt_eq (b : Lane) (v : Int) : bValueAt b v = laneValueAt b v := rfl

private theorem sum_ite_eq_filter {α : Type} (p : α → Prop) [DecidablePred p] (l : List α)
    (f : α → Int) : ((l.map (fun i => if p i then f i else 0)).sum) = (((l.filter p).map f).sum) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem range_filter_eq_single (n j : Nat) :
    (List.range n).filter (fun i : Nat => i == j) = if j < n then [j] else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.filter_append, ih]
      by_cases hj : j < n
      · have hjn : j < n + 1 := by omega
        have hnj : n ≠ j := by omega
        simp [hj, hjn, hnj]
      · by_cases hjeq : j = n
        · subst j
          simp [hj]
        · have hjn : ¬ j < n + 1 := by omega
          have hnj : n ≠ j := by omega
          simp [hj, hjeq, hjn, hnj]

private theorem lanePoints_filter_map (a : Lane) (ha : 0 < a.stride) (p : Int → Bool)
    (g : Int → Int → Int) :
    ((lanePoints a).filter (fun r => p r.1)).map (fun r => g r.1 r.2) =
      ((progVals (positions a)).filter p).map (fun v => g v (laneValueAt a v)) := by
  unfold lanePoints progVals
  dsimp [positions]
  induction (List.range a.n) with
  | nil => simp
  | cons k ks ih =>
      by_cases hk : p (a.start + k * a.stride)
      · simp [hk, ih, laneValueAt_apply a ha k]
      · simp [hk, ih]

private theorem inner_aligned (a : Lane) (b : Lane) (hb : 0 < b.stride) (r : Nat × Int) :
    ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum =
    if contains (positions b) (r.1 : Int) then r.2 * bValueAt b (r.1 : Int) else 0 := by
  by_cases hc : contains (positions b) (r.1 : Int) = true
  · rw [if_pos hc]
    rcases (contains_iff_mem (positions b) (r.1 : Int)).mp hc with ⟨j, hj, hvj⟩
    have hvjInt : (r.1 : Int) = (b.start : Int) + (j : Int) * (b.stride : Int) := by
      simpa [positions, progVal] using hvj
    have hvjNat : r.1 = b.start + j * b.stride := by exact_mod_cast hvjInt
    have hne : (b.stride : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hb)
    have hdiv : ((r.1 : Int) - (b.start : Int)) / (b.stride : Int) = (j : Int) := by
      rw [hvjInt]
      have hsub : (b.start : Int) + (j : Int) * (b.stride : Int) - (b.start : Int) =
          (j : Int) * (b.stride : Int) := by omega
      rw [hsub, Int.mul_comm]
      exact Int.mul_ediv_cancel_left (j : Int) hne
    have hfilt : (List.range b.n).filter (fun i : Nat => r.1 == b.start + i * b.stride) =
        (List.range b.n).filter (fun i : Nat => i == j) := by
      apply List.filter_congr
      intro i hi
      have hij : (i == j) = (r.1 == b.start + i * b.stride) := by
        by_cases h : i = j
        · subst i
          simp [hvjNat]
        · have hbfalse : (r.1 == b.start + i * b.stride) = false := by
            apply Bool.eq_false_of_ne_true
            intro ht
            have heqNat : r.1 = b.start + i * b.stride := Nat.eq_of_beq_eq_true ht
            have heqInt : (b.start : Int) + (j : Int) * (b.stride : Int) =
                (b.start : Int) + (i : Int) * (b.stride : Int) := by
              exact_mod_cast (hvjNat.symm.trans heqNat)
            have hmul : (j : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int) := by omega
            have hji : (j : Int) = (i : Int) := Int.eq_of_mul_eq_mul_right hne hmul
            exact h (by exact_mod_cast hji.symm)
          simp [h, hbfalse]
      exact hij.symm
    rw [← sum_map_ite_zero]
    rw [hfilt]
    rw [range_filter_eq_single]
    have hjb : j < b.n := by simpa [positions] using hj
    rw [if_pos hjb]
    simp only [List.map_singleton, List.sum_singleton]
    dsimp [bValueAt]
    rw [hdiv]
    grind
  · rw [if_neg hc]
    rw [← sum_map_ite_zero]
    have hnil : (List.range b.n).filter (fun i : Nat => r.1 == b.start + i * b.stride) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro i hi heq
      have hil : i < b.n := List.mem_range.mp hi
      apply hc
      apply (contains_iff_mem (positions b) (r.1 : Int)).mpr
      refine ⟨i, hil, ?_⟩
      have hv : (r.1 : Int) = (b.start + i * b.stride : Nat) := by exact_mod_cast (beq_iff_eq.mp heq)
      simpa [positions, progVal] using hv
    rw [hnil]
    simp

private theorem lane_aligned_sum (a : Lane) (ha : 0 < a.stride) (b : Lane) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned a b := by
  rw [show ((lanePoints a).map (fun r => ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)) =
      ((lanePoints a).map (fun r =>
        if contains (positions b) (r.1 : Int) then r.2 * bValueAt b (r.1 : Int) else 0)) by
    apply List.map_congr_left
    intro r hr
    exact inner_aligned a b hb r]
  rw [← sum_map_ite_zero]
  rw [lanePoints_filter_map a ha (fun v => contains (positions b) v) (fun v w => w * bValueAt b v)]
  let r := intersection (positions a) (positions b)
  have hp : 0 < (positions a).step := by simpa [positions] using ha
  have hq : 0 < (positions b).step := by simpa [positions] using hb
  have hrp := progVals_filter_inter_perm (positions a) (positions b) hp
  rw [list_sum_perm (hrp.map (fun v => laneValueAt a v * bValueAt b v))]
  rw [show ((progVals r).map (fun v => laneValueAt a v * bValueAt b v)) =
      ((List.range r.n).map (fun j : Nat =>
        laneValueAt a (progVal r j) * bValueAt b (progVal r j))) by
    unfold progVals
    rw [List.map_map]
    rfl]
  change ((List.range r.n).map (fun j : Nat =>
      laneValueAt a (progVal r j) * bValueAt b (progVal r j))).sum =
    (r.n : Int) * firstAt a r * firstAt b r +
      tri r.n * (firstAt a r * slopeAt b r + firstAt b r * slopeAt a r) +
      squares r.n * slopeAt a r * slopeAt b r
  by_cases hrn0 : r.n = 0
  · rw [hrn0]
    simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  · have hrnpos : 0 < r.n := by omega
    have hdvdA : a.stride ∣ r.step := by
      dsimp [r]
      exact inter_step_dvd_of_pos (positions a) (positions b) hp hq hrnpos
    have hdvdB : b.stride ∣ r.step := by
      dsimp [r]
      exact inter_step_dvd_right (positions a) (positions b) hp hq hrnpos
    have hallA : ∀ j, j < r.n → contains (positions a) (progVal r j) = true := by
      intro j hj
      have hc : contains r (progVal r j) = true := by
        apply (contains_iff_mem r _).mpr
        exact ⟨j, hj, rfl⟩
      exact ((intersection_contains (positions a) (positions b) (progVal r j)).mp hc).1
    have hallB : ∀ j, j < r.n → contains (positions b) (progVal r j) = true := by
      intro j hj
      have hc : contains r (progVal r j) = true := by
        apply (contains_iff_mem r _).mpr
        exact ⟨j, hj, rfl⟩
      exact ((intersection_contains (positions a) (positions b) (progVal r j)).mp hc).2
    have htermA := laneValueAt_of_prog a ha r hrnpos hdvdA hallA
    have htermB := laneValueAt_of_prog b hb r hrnpos hdvdB hallB
    rw [show ((List.range r.n).map (fun j : Nat =>
        laneValueAt a (progVal r j) * bValueAt b (progVal r j))) =
        ((List.range r.n).map (fun j : Nat =>
          (firstAt a r + (j : Int) * slopeAt a r) *
            (firstAt b r + (j : Int) * slopeAt b r))) by
      apply List.map_congr_left
      intro j hj
      rw [htermA j (List.mem_range.mp hj), bValueAt_eq, htermB j (List.mem_range.mp hj)]]
    rw [sum_product_arith]

private theorem flatMap_map_sum {α β : Type} (f : α → List β) (g : β → Int) (ls : List α) :
    ((ls.flatMap f).map g).sum = (ls.map (fun x => ((f x).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih => simp [List.map_append, List.sum_append, ih]

theorem sound_AlignedProduct :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.AlignedProduct.Goal := by
  intro c b hb
  let g : Nat × Int → Int := fun r =>
    ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum
  have hp := (gray_rows_perm c).map g
  rw [list_sum_perm hp]
  rw [flatMap_map_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have hstride : 0 < a.stride := by
    have hw : 0 < width c := width_pos c
    simp [GrayTile.layout, striped] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, hw]
  exact lane_aligned_sum a hstride b hb
