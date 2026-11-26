import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `DeletedSpan.DeleteProgression` — `∀ c mask, (((rows layout c).filter (fun r =>
  !contains mask r.1)).map Prod.snd).sum = ((layout.lanes c).map (fun a => partialSum
  a a.n - deletedSum a mask)).sum`
- `DeletedSpan.ResidueJoin` — `∀ c (b : Lane), ((rows layout c).map (fun r =>
  (List.range b.n).countP (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum =
  ((layout.lanes c).map (joinCount · b)).sum`
- `DeletedSpan.SeekSurvivor` — `∀ c (k : Nat), k < (rows layout c).length → ((rows
  layout c)[k]?).map Prod.fst = some (if k < c.split then k else k+width c)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpanSeekAndProgressions

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.length = l₂.length := p.length_eq

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

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by
      rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by
      rw [hmul1, hmul2]
      omega
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


private theorem contains_const_iff2 (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  simp [contains, lastVal]
  omega

private theorem contains_bounds2 (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  have hstepnon : (0 : Int) ≤ a.step := by exact_mod_cast (Nat.zero_le a.step)
  have hknon : (0 : Int) ≤ (k : Int) := by omega
  have hmulnon : (0 : Int) ≤ (k : Int) * a.step := Int.mul_nonneg hknon hstepnon
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by exact_mod_cast (by omega : k ≤ a.n - 1)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint hstepnon
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  constructor <;> omega

private theorem contains_dvd2 (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm (k : Int) a.step]
  omega

private theorem common_gap_dvd2 (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgd : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgl : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) :=
    Int.dvd_mul_of_dvd_left hgd
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) :=
    Int.dvd_mul_of_dvd_left hgl
  rw [Int.mul_comm (i : Int) a.step] at hi
  rw [Int.mul_comm (j : Int) b.step] at hj
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    omega
  rw [← heq]
  exact Int.dvd_sub h2 h1


private theorem period_dvd_of_both (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (d : Int)
    (had : (as : Int) ∣ d) (hbd : (bs : Int) ∣ d) :
    (((as / as.gcd bs) * bs : Nat) : Int) ∣ d := by
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  have hg : 0 < g := by dsimp [g]; exact Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := by dsimp [A, g]; exact Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := by dsimp [B, g]; exact Nat.div_gcd_pos_of_pos_right as hb
  have hcop : A.gcd B = 1 := by simpa [A, B, g] using reduced_coprime as bs ha hb
  have hgne : (g : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hg)
  have hAs : (as : Int) = (g : Int) * (A : Int) := by
    dsimp [A, g]
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hBs : (bs : Int) = (g : Int) * (B : Int) := by
    dsimp [B, g]
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  have hgd : (g : Int) ∣ d := by
    have hgin : (g : Int) ∣ (as : Int) := by
      exact_mod_cast (Nat.gcd_dvd_left as bs)
    exact Int.dvd_trans hgin had
  obtain ⟨z, hz⟩ := hgd
  have hAdiv : (A : Int) ∣ z := by
    obtain ⟨q, hq⟩ := had
    refine ⟨q, ?_⟩
    apply Int.eq_of_mul_eq_mul_left hgne
    rw [← hz, hq, hAs]
    grind
  have hBdiv : (B : Int) ∣ z := by
    obtain ⟨q, hq⟩ := hbd
    refine ⟨q, ?_⟩
    apply Int.eq_of_mul_eq_mul_left hgne
    rw [← hz, hq, hBs]
    grind
  have hAnat : A ∣ z.natAbs := by
    have := (Int.natAbs_dvd_natAbs).mpr hAdiv
    simpa using this
  have hBnat : B ∣ z.natAbs := by
    have := (Int.natAbs_dvd_natAbs).mpr hBdiv
    simpa using this
  have hABnat : A * B ∣ z.natAbs :=
    (Nat.coprime_div_gcd_div_gcd (by simpa [g] using hg)).mul_dvd_of_dvd_of_dvd hAnat hBnat
  have hABint : ((A * B : Nat) : Int) ∣ z := by
    exact (Int.dvd_natAbs (a := ((A * B : Nat) : Int)) (b := z)).mp (by exact_mod_cast hABnat)
  obtain ⟨k, hk⟩ := hABint
  refine ⟨k, ?_⟩
  dsimp [period_dvd_of_both] 
  rw [hz, hk]
  dsimp [A, B, g] at *
  rw [hBs]
  grind

private theorem prog_eq_const_of_step_zero {a : Progression} (hs : a.step = 0) :
    a = ⟨a.lo, 0, a.n⟩ := by
  cases a with
  | mk lo st n => simp_all

private theorem intersection_contains2 (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · rcases a with ⟨alo, ast, an⟩
    simp at han
    subst an
    simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · rcases b with ⟨blo, bst, bn⟩
      simp at hbn
      subst bn
      simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · simp only [intersection]
        rw [if_neg (by simp [han, hbn]), if_pos (by simp [has])]
        have hav : contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
          rw [prog_eq_const_of_step_zero has]
          exact contains_const_iff2 a.lo a.n v
        by_cases hba : contains b a.lo = true
        · rw [if_pos hba]
          constructor
          · intro h
            have hv : v = a.lo := by
              simp [contains, lastVal] at h
              omega
            exact ⟨hav.mpr ⟨hanp, hv⟩, hv ▸ hba⟩
          · rintro ⟨ha, hb⟩
            have hv : v = a.lo := (hav.mp ha).2
            subst v
            simp [contains, lastVal]
        · have hbaf : contains b a.lo = false := Bool.eq_false_of_ne_true hba
          rw [if_neg (by simpa using hbaf)]
          constructor
          · intro h
            simp [contains] at h
          · rintro ⟨ha, hb⟩
            have hv : v = a.lo := (hav.mp ha).2
            subst v
            exact (hba hb).elim
      · by_cases hbs : b.step = 0
        · simp only [intersection]
          rw [if_neg (by simp [han, hbn]), if_neg (by simp [has]), if_pos (by simp [hbs])]
          have hbv : contains b v = true ↔ 0 < b.n ∧ v = b.lo := by
            rw [prog_eq_const_of_step_zero hbs]
            exact contains_const_iff2 b.lo b.n v
          by_cases hab : contains a b.lo = true
          · rw [if_pos hab]
            constructor
            · intro h
              have hv : v = b.lo := by
                simp [contains, lastVal] at h
                omega
              exact ⟨hv ▸ hab, hbv.mpr ⟨hbnp, hv⟩⟩
            · rintro ⟨ha, hb⟩
              have hv : v = b.lo := (hbv.mp hb).2
              subst v
              simp [contains, lastVal]
          · have habf : contains a b.lo = false := Bool.eq_false_of_ne_true hab
            rw [if_neg (by simpa using habf)]
            constructor
            · intro h
              simp [contains] at h
            · rintro ⟨ha, hb⟩
              have hv : v = b.lo := (hbv.mp hb).2
              subst v
              exact (hab ha).elim
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
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step hasp) hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          have hgpos : 0 < g := by dsimp [g]; exact Nat.gcd_pos_of_pos_left b.step hasp
          have hperiod_nat : a.step / g * b.step = a.step * (b.step / g) := by
            have hga := Nat.gcd_dvd_left a.step b.step
            have hgb := Nat.gcd_dvd_right a.step b.step
            calc
              a.step / g * b.step = a.step / g * (g * (b.step / g)) := by
                rw [Nat.mul_div_cancel' hgb]
              _ = g * (a.step / g) * (b.step / g) := by grind
              _ = a.step * (b.step / g) := by rw [Nat.mul_div_cancel' hga]
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
            have hflow : low ≤ first := by dsimp [first]; omega
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              have hc : (period : Int) = ((a.step * (b.step / g) : Nat) : Int) := by
                dsimp [period]
                exact_mod_cast hperiod_nat
              rw [hc]
              push_cast
              rfl
            have hperiod_sym : a.step / g * b.step = b.step * (a.step / g) := by
              rw [hperiod_nat]
              grind
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              have hc : (period : Int) = ((b.step * (a.step / g) : Nat) : Int) := by
                dsimp [period]
                exact_mod_cast hperiod_sym
              rw [hc]
              push_cast
              rfl
            have hc1 : (a.n == 0 || b.n == 0) = false := by simp [beq_iff_eq, han, hbn]
            have hc2 : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
            have hc3 : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbs
            have hc4 : ((b.lo - a.lo) % (g : Int) != 0) = false := by
              dsimp [gap] at hgapmod
              have hz : (b.lo - a.lo) % (g : Int) = 0 := by simpa using hgapmod
              rw [hz]
              decide
            have hc1' : ¬ ((a.n == 0 || b.n == 0) = true) := by rw [hc1]; decide
            have hc2' : ¬ ((a.step == 0) = true) := by rw [hc2]; decide
            have hc3' : ¬ ((b.step == 0) = true) := by rw [hc3]; decide
            have hc4' : ¬ (((b.lo - a.lo) % (g : Int) != 0) = true) := by rw [hc4]; decide
            have hc4e : ¬ (((b.lo - a.lo) % ((a.step.gcd b.step : Nat) : Int) != 0) = true) := by
              simpa [g] using hc4'
            unfold intersection
            simp only [Id.run, if_neg hc1', if_neg hc2', if_neg hc3', if_neg hc4e]
            change contains ⟨first, (period : Int).toNat,
                if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
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
              have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
              have hxrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have heq : (v - root) + (root - a.lo) = v - a.lo := by omega
                rw [← heq]
                exact Int.dvd_add hxrootA hroot.1
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have heq : (v - root) + (root - b.lo) = v - b.lo := by omega
                rw [← heq]
                exact Int.dvd_add hxrootB hroot.2
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds2 a v ha
              have hbb := contains_bounds2 b v hb
              have hxalo := contains_dvd2 a v ha
              have hxblo := contains_dvd2 b v hb
              have hdivA : (a.step : Int) ∣ root - v := by
                have heq : (root - a.lo) - (v - a.lo) = root - v := by omega
                rw [← heq]
                exact Int.dvd_sub hroot.1 hxalo
              have hdivB : (b.step : Int) ∣ root - v := by
                have heq : (root - b.lo) - (v - b.lo) = root - v := by omega
                rw [← heq]
                exact Int.dvd_sub hroot.2 hxblo
              have hxroot : (period : Int) ∣ v - root := by
                have htmp := period_dvd_of_both a.step b.step hasp hbsp (root - v) hdivA hdivB
                have heq : v - root = -(root - v) := by omega
                rw [heq]
                exact Int.dvd_neg.mpr htmp
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              exact ⟨first_le_of_congruence low root first v period hp hfirst (by omega) hxroot,
                by dsimp [high]; exact Int.le_min.mpr ⟨hab.2, hbb.2⟩, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            have hc1 : (a.n == 0 || b.n == 0) = false := by simp [beq_iff_eq, han, hbn]
            have hc2 : (a.step == 0) = false := beq_eq_false_iff_ne.mpr has
            have hc3 : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hbs
            have hc4 : ((b.lo - a.lo) % (g : Int) != 0) = true := by
              dsimp [gap] at hgapne
              have hnz : (b.lo - a.lo) % (g : Int) ≠ 0 := by simpa using hgapne
              simp [hnz]
            have hc1' : ¬ ((a.n == 0 || b.n == 0) = true) := by rw [hc1]; decide
            have hc2' : ¬ ((a.step == 0) = true) := by rw [hc2]; decide
            have hc3' : ¬ ((b.step == 0) = true) := by rw [hc3]; decide
            have hc4e : ((b.lo - a.lo) % ((a.step.gcd b.step : Nat) : Int) != 0) = true := by
              simpa [g] using hc4
            unfold intersection
            simp only [Id.run, if_neg hc1', if_neg hc2', if_neg hc3', if_pos hc4e]
            change contains ⟨0, 1, 0⟩ v = true ↔ contains a v = true ∧ contains b v = true
            constructor
            · intro h
              simp [contains] at h
            · rintro ⟨ha, hb⟩
              have hg := common_gap_dvd2 a b v ha hb
              exact False.elim (hgapne (by simpa [gap, g] using Int.emod_eq_zero_of_dvd hg))


private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat =>
    (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem filterMap_some_eq_map2 {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private def progPoints (a : Progression) : List Int :=
  (List.range a.n).map (progVal a)

private theorem mem_progPoints {a : Progression} {v : Int} :
    v ∈ progPoints a ↔ contains a v = true := by
  unfold progPoints
  constructor
  · intro hv
    obtain ⟨k, hk, hval⟩ := List.mem_map.mp hv
    rw [← hval]
    exact (contains_iff_mem a (progVal a k)).mpr ⟨k, List.mem_range.mp hk, rfl⟩
  · intro h
    obtain ⟨k, hk, hval⟩ := (contains_iff_mem a v).mp h
    exact List.mem_map.mpr ⟨k, List.mem_range.mpr hk, hval.symm⟩

private theorem hne_of_inj {α β : Type} {f : α → β} (hf : Function.Injective f) {x y : α} :
    x ≠ y → f x ≠ f y := by
  intro hxy heq
  exact hxy (hf heq)

private theorem progPoints_nodup_of_pos (a : Progression) (hs : 0 < a.step) :
    (progPoints a).Nodup := by
  have hinj : Function.Injective (progVal a) := by
    intro x y hxy
    unfold progVal at hxy
    have hstepne : (a.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hs)
    have hmul : (x : Int) * (a.step : Int) = (y : Int) * (a.step : Int) := by omega
    have heq : (x : Int) = (y : Int) :=
      (Int.mul_eq_mul_right_iff hstepne).mp hmul
    exact_mod_cast heq
  unfold progPoints
  exact List.Pairwise.map (progVal a) (fun x y hne => hne_of_inj hinj hne) List.nodup_range

private theorem not_mem_erase_self_of_nodup (a : Int) {l : List Int} (hnd : l.Nodup) :
    ¬ a ∈ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hbne : ¬ b ∈ bs := (List.nodup_cons.mp hnd).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hba : b == a
      · have hbeq : b = a := beq_iff_eq.mp hba
        subst b
        rw [List.erase_cons_head]
        exact hbne
      · rw [List.erase_cons_tail hba]
        intro ha_mem
        rw [List.mem_cons] at ha_mem
        rcases ha_mem with hab | habs
        · have hbnea : b ≠ a := by intro h; apply hba; rw [h]; simp
          exact hbnea hab.symm
        · exact (ih hbsnd) habs

private theorem perm_of_mem_iff_nodup {l₁ l₂ : List Int}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => exact List.Perm.nil
      | cons b bs =>
          have : b ∈ ([] : List Int) := (hm b).mpr (by simp)
          simp at this
  | cons a as ih =>
      have ha2 : a ∈ l₂ := (hm a).mp (by simp)
      have hasnd : as.Nodup := h₁.tail
      have herasend : (l₂.erase a).Nodup := h₂.erase a
      have hm' : ∀ v, v ∈ as ↔ v ∈ l₂.erase a := by
        intro v
        constructor
        · intro hv
          have hv1 : v ∈ a :: as := by simp [hv]
          have hv2 : v ∈ l₂ := (hm v).mp hv1
          have hne : v ≠ a := by
            intro heq
            subst v
            exact (List.nodup_cons.mp h₁).1 hv
          exact (List.mem_erase_of_ne hne).mpr hv2
        · intro hv
          have hv2 : v ∈ l₂ := List.mem_of_mem_erase hv
          have hv1 : v ∈ a :: as := (hm v).mpr hv2
          rw [List.mem_cons] at hv1
          rcases hv1 with hva | hvas
          · have : v = a := by simpa using hva
            subst v
            exact False.elim (not_mem_erase_self_of_nodup a h₂ hv)
          · exact hvas
      have ihperm := ih hasnd herasend hm'
      exact (List.Perm.cons a ihperm).trans (List.perm_cons_erase ha2).symm

private theorem sum_eq_of_nodup_mem {l₁ l₂ : List Int}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) (F : Int → Int) :
    (l₁.map F).sum = (l₂.map F).sum := by
  exact list_sum_perm ((perm_of_mem_iff_nodup h₁ h₂ hm).map F)

private theorem intersection_step_pos (a b : Progression) :
    0 < (intersection a b).step := by
  unfold intersection
  by_cases h1 : a.n == 0 <;> by_cases h2 : b.n == 0 <;>
    by_cases h3 : a.step == 0 <;> by_cases h4 : b.step == 0 <;>
    by_cases h5 : (b.lo - a.lo) % ((a.step.gcd b.step : Nat) : Int) != 0 <;>
    simp [h1, h2, h3, h4, h5]
  all_goals
    have ha : 0 < a.step := Nat.pos_of_ne_zero (by simpa using h3)
    have hb : 0 < b.step := Nat.pos_of_ne_zero (by simpa using h4)
    exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step ha) hb

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sum_eq_of_nodup_mem_nat {l₁ l₂ : List Int}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) (F : Int → Nat) :
    (l₁.map F).sum = (l₂.map F).sum := by
  exact list_sum_perm_nat ((perm_of_mem_iff_nodup h₁ h₂ hm).map F)

private theorem sum_progPoints_intersection_nat (a b : Progression) (ha : 0 < a.step)
    (F : Int → Nat) :
    ((progPoints (intersection a b)).map F).sum =
      (((progPoints a).filter (fun v => contains b v)).map F).sum := by
  have hnd1 : (progPoints (intersection a b)).Nodup :=
    progPoints_nodup_of_pos _ (intersection_step_pos a b)
  have hnd2 : ((progPoints a).filter (fun v => contains b v)).Nodup :=
    (progPoints_nodup_of_pos a ha).filter _
  apply sum_eq_of_nodup_mem_nat hnd1 hnd2
  intro v
  rw [List.mem_filter]
  constructor
  · intro hv
    have hc := (mem_progPoints.mp hv)
    have hboth := (intersection_contains2 a b v).mp hc
    exact ⟨mem_progPoints.mpr hboth.1, hboth.2⟩
  · rintro ⟨hva, hvb⟩
    exact mem_progPoints.mpr
      ((intersection_contains2 a b v).mpr ⟨mem_progPoints.mp hva, hvb⟩)

private theorem sum_progPoints_intersection (a b : Progression) (ha : 0 < a.step)
    (F : Int → Int) :
    ((progPoints (intersection a b)).map F).sum =
      (((progPoints a).filter (fun v => contains b v)).map F).sum := by
  have hnd1 : (progPoints (intersection a b)).Nodup :=
    progPoints_nodup_of_pos _ (intersection_step_pos a b)
  have hnd2 : ((progPoints a).filter (fun v => contains b v)).Nodup :=
    (progPoints_nodup_of_pos a ha).filter _
  apply sum_eq_of_nodup_mem hnd1 hnd2
  intro v
  rw [List.mem_filter]
  constructor
  · intro hv
    have hc := (mem_progPoints.mp hv)
    have hboth := (intersection_contains2 a b v).mp hc
    exact ⟨mem_progPoints.mpr hboth.1, hboth.2⟩
  · rintro ⟨hva, hvb⟩
    exact mem_progPoints.mpr
      ((intersection_contains2 a b v).mpr ⟨mem_progPoints.mp hva, hvb⟩)


private theorem filterMap_range_split {α β : Type} (n s : Nat) (f g : Nat → Option β) :
    (List.range n).filterMap (fun i => if i < s then f i else g i) =
      (List.range (min n s)).filterMap f ++
        (List.range (n - s)).filterMap (fun j => g (s + j)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filterMap_append, ih]
      by_cases h : n < s
      · have hmins : min n s = n := Nat.min_eq_left (by omega)
        have hmins' : min (n + 1) s = n + 1 := Nat.min_eq_left (by omega)
        have hz : n + 1 - s = 0 := by omega
        have hz' : n - s = 0 := by omega
        cases hfn : f n with
        | none => simp [h, hfn, hmins, hmins', hz, hz', List.range_succ, List.filterMap_append]
        | some x => simp [h, hfn, hmins, hmins', hz, hz', List.range_succ, List.filterMap_append]
      · have hmins : min n s = s := Nat.min_eq_right (by omega)
        have hmins' : min (n + 1) s = s := Nat.min_eq_right (by omega)
        have hs : n + 1 - s = (n - s) + 1 := by omega
        have hs2 : s + (n - s) = n := by omega
        cases hgn : g n with
        | none => simp [h, hgn, hmins, hmins', hs, hs2, List.range_succ, List.filterMap_append]
        | some x => simp [h, hgn, hmins, hmins', hs, hs2, List.range_succ, List.filterMap_append]

private theorem filterMap_none {α β : Type} (l : List α) :
    l.filterMap (fun _ : α => (none : Option β)) = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem rows_lanes (c : Config) :
    rows layout c =
      lanePoints (span c 0 c.split c.base c.step) ++
      lanePoints (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) := by
  unfold rows
  have hread : (List.range c.n).filterMap
      (fun i => (layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i =>
        if i < c.split then some (i, c.base + c.step * (i : Int))
        else if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int))) := by
    apply filterMap_congr'
    intro i hi
    simp only [layout]
    by_cases h1 : i < c.split <;> by_cases h2 : i < c.split + width c <;> simp [h1, h2] <;> omega
  rw [hread]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) c.n c.split
      (fun i : Nat => some (i, c.base + c.step * (i : Int)))
      (fun i => if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int)))]
  rw [filterMap_some_eq_map2]
  have hfilter : (List.range (c.n - c.split)).filterMap
      (fun j => if c.split + j < c.split + width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) =
      (List.range (c.n - c.split)).filterMap
      (fun j => if j < width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) := by
    apply filterMap_congr'
    intro j hj
    have heq : (c.split + j < c.split + width c) = (j < width c) := by
      rw [Nat.add_lt_add_iff_left]
    by_cases h : j < width c <;> simp [h, heq]
  rw [hfilter]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) (c.n - c.split) (width c)
      (fun _ : Nat => (none : Option (Nat × Int)))
      (fun j => some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int)))]
  rw [filterMap_none (α := Nat) (β := Nat × Int)]
  simp only [List.nil_append]
  have hleft : (List.range (min c.n c.split)).map
      (fun i : Nat => (i, c.base + c.step * (i : Int))) =
      lanePoints (span c 0 c.split c.base c.step) := by
    unfold lanePoints span lane
    simp only
    have hc : (min c.n c.split - 0 + 1 - 1) / 1 = min c.n c.split := by omega
    rw [hc]
    apply List.map_congr_left
    intro i hi
    simp
    rw [Int.mul_comm]
  have hcnt : c.n - (c.split + width c) = c.n - c.split - width c := by
    dsimp [width]
    rw [Nat.sub_sub]
  have hright : (List.range (c.n - c.split - width c)).map
      (fun j : Nat => (c.split + (width c + j), c.base + c.step * ((c.split + (width c + j) : Nat) : Int))) =
      lanePoints (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) := by
    unfold lanePoints span lane
    simp only
    have hc2 : (min c.n c.n - (c.split + width c) + 1 - 1) / 1 =
        c.n - (c.split + width c) := by omega
    rw [hc2, hcnt]
    apply List.map_congr_left
    intro j hj
    simp [Int.mul_add, Int.add_mul]
    constructor
    · omega
    · rw [Int.mul_comm (j : Int) c.step]
      omega
  rw [filterMap_some_eq_map2 (l := List.range (c.n - c.split - width c))
        (f := fun j : Nat => (c.split + (width c + j), c.base + c.step * ((c.split + (width c + j) : Nat) : Int)))]
  rw [hleft, hright]

private theorem lanes_flatMap (c : Config) :
    (layout.lanes c).flatMap lanePoints =
      lanePoints (span c 0 c.split c.base c.step) ++
      lanePoints (span c (c.split + width c) c.n
        (c.base + c.step * ((c.split + width c : Nat) : Int)) c.step) := by
  simp [layout]

private theorem rows_eq_flatMap (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_lanes c, lanes_flatMap c]


private theorem rows_explicit (c : Config) :
    rows layout c =
      (List.range (min c.n c.split)).map
        (fun i : Nat => (i, c.base + c.step * (i : Int))) ++
      (List.range (c.n - c.split - width c)).map
        (fun j : Nat => (c.split + (width c + j),
          c.base + c.step * ((c.split + (width c + j) : Nat) : Int))) := by
  unfold rows
  have hread : (List.range c.n).filterMap
      (fun i => (layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i =>
        if i < c.split then some (i, c.base + c.step * (i : Int))
        else if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int))) := by
    apply filterMap_congr'
    intro i hi
    simp only [layout]
    by_cases h1 : i < c.split <;> by_cases h2 : i < c.split + width c <;> simp [h1, h2] <;> omega
  rw [hread]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) c.n c.split
      (fun i : Nat => some (i, c.base + c.step * (i : Int)))
      (fun i : Nat => if i < c.split + width c then none
        else some (i, c.base + c.step * (i : Int)))]
  rw [filterMap_some_eq_map2 (l := List.range (min c.n c.split))
        (f := fun i : Nat => (i, c.base + c.step * (i : Int)))]
  have hfilter : (List.range (c.n - c.split)).filterMap
      (fun j => if c.split + j < c.split + width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) =
      (List.range (c.n - c.split)).filterMap
      (fun j => if j < width c then none
        else some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int))) := by
    apply filterMap_congr'
    intro j hj
    have heq : (c.split + j < c.split + width c) = (j < width c) := by
      rw [Nat.add_lt_add_iff_left]
    by_cases h : j < width c <;> simp [h, heq]
  rw [hfilter]
  rw [filterMap_range_split (α := Nat) (β := Nat × Int) (c.n - c.split) (width c)
      (fun _ : Nat => (none : Option (Nat × Int)))
      (fun j : Nat => some (c.split + j, c.base + c.step * ((c.split + j : Nat) : Int)))]
  rw [filterMap_none (α := Nat) (β := Nat × Int)]
  simp only [List.nil_append]
  rw [filterMap_some_eq_map2 (l := List.range (c.n - c.split - width c))
        (f := fun j : Nat => (c.split + (width c + j),
          c.base + c.step * ((c.split + (width c + j) : Nat) : Int)))]

theorem sound_SeekSurvivor :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.SeekSurvivor.Goal := by
  intro c k hk
  rw [rows_explicit c] at hk ⊢
  simp only [List.length_append, List.length_map, List.length_range] at hk
  by_cases hk1 : k < min c.n c.split
  · rw [List.getElem?_append_left (by simpa using hk1), List.getElem?_map]
    have hidx : (List.range (min c.n c.split))[k]? = some k := by
      rw [List.getElem?_eq_getElem (by simpa using hk1)]
      simp
    rw [hidx]
    simp
    have hklt : k < c.split := by omega
    intro hsk
    omega
  · have hkge : min c.n c.split ≤ k := by omega
    rw [List.getElem?_append_right (by simpa using hkge), List.getElem?_map]
    have hj : k - min c.n c.split < c.n - c.split - width c := by omega
    have hget : (List.range (c.n - c.split - width c))[k - min c.n c.split]? =
        some (k - min c.n c.split) := by
      rw [List.getElem?_eq_getElem (by simpa using hj)]
      simp
    simp only [List.length_map, List.length_range]
    rw [hget]
    simp
    have hsplit : c.split ≤ c.n := by
      by_cases hh : c.split ≤ c.n
      · exact hh
      · have hmin : min c.n c.split = c.n := Nat.min_eq_left (by omega)
        have hlen0 : c.n - c.split - width c = 0 := by omega
        omega
    have hmin_eq : min c.n c.split = c.split := Nat.min_eq_right hsplit
    rw [hmin_eq] at hj hkge hget ⊢
    have hksplit : ¬ k < c.split := by omega
    simp [hksplit]
    have hkdecomp : k = c.split + (k - c.split) := (Nat.add_sub_of_le (by omega)).symm
    omega


private theorem lane_snd_eq (a : Lane) :
    (lanePoints a).map Prod.snd =
      (List.range a.n).map (fun i : Nat => a.base + (i : Int) * a.step) := by
  unfold lanePoints
  rw [List.map_map]
  apply List.map_congr_left
  intro i hi
  simp [Int.mul_comm]

private theorem mem_values_lane (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i, i < a.n ∧ v = a.base + (i : Int) * a.step := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hval⟩
    unfold progVal at hval
    have hk' : k < a.n := by simpa [values] using hk
    by_cases hneg : a.step < 0
    · dsimp [values] at hval
      rw [if_pos hneg] at hval
      have hnat : ((a.step.natAbs : Nat) : Int) = -a.step := by
        rw [← Int.natAbs_neg a.step]
        exact Int.natAbs_of_nonneg (by omega : 0 ≤ -a.step)
      rw [hnat] at hval
      refine ⟨a.n - 1 - k, ?_, ?_⟩
      · omega
      · have hcast : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by omega
        rw [hcast]
        grind
    · dsimp [values] at hval
      rw [if_neg hneg] at hval
      have hnat : ((a.step.natAbs : Nat) : Int) = a.step :=
        Int.natAbs_of_nonneg (by omega : 0 ≤ a.step)
      rw [hnat] at hval
      refine ⟨k, hk, ?_⟩
      omega
  · rintro ⟨i, hi, hval⟩
    by_cases hneg : a.step < 0
    · refine ⟨a.n - 1 - i, ?_, ?_⟩
      · simpa [values] using (by omega : a.n - 1 - i < a.n)
      · unfold progVal
        dsimp [values]
        rw [if_pos hneg]
        have hnat : ((a.step.natAbs : Nat) : Int) = -a.step := by
          rw [← Int.natAbs_neg a.step]
          exact Int.natAbs_of_nonneg (by omega : 0 ≤ -a.step)
        rw [hnat]
        have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
        rw [hcast]
        grind
    · refine ⟨i, hi, ?_⟩
      unfold progVal
      dsimp [values]
      rw [if_neg hneg]
      have hnat : ((a.step.natAbs : Nat) : Int) = a.step :=
        Int.natAbs_of_nonneg (by omega : 0 ≤ a.step)
      rw [hnat]
      omega

private theorem progPoints_values_perm (a : Lane) (has : a.step ≠ 0) :
    (progPoints (values a)).Perm ((lanePoints a).map Prod.snd) := by
  apply perm_of_mem_iff_nodup
  · apply progPoints_nodup_of_pos
    dsimp [values]
    exact (Int.natAbs_pos.mpr has)
  · rw [lane_snd_eq]
    have hinj : Function.Injective (fun i : Nat => a.base + (i : Int) * a.step) := by
      intro x y hxy
      have hmul : (x : Int) * a.step = (y : Int) * a.step := by grind
      have heq : (x : Int) = (y : Int) := (Int.mul_eq_mul_right_iff has).mp hmul
      exact_mod_cast heq
    exact List.Pairwise.map (fun i : Nat => a.base + (i : Int) * a.step)
      (fun x y hne => hne_of_inj hinj hne) List.nodup_range
  · intro v
    constructor
    · intro hv
      have hc : contains (values a) v = true := mem_progPoints.mp hv
      obtain ⟨i, hi, hival⟩ := (mem_values_lane a v).mp hc
      rw [lane_snd_eq]
      exact List.mem_map.mpr ⟨i, List.mem_range.mpr hi, hival.symm⟩
    · intro hv
      rw [lane_snd_eq] at hv
      obtain ⟨i, hi, hival⟩ := List.mem_map.mp hv
      have hc : contains (values a) v = true :=
        (mem_values_lane a v).mpr ⟨i, List.mem_range.mp hi, hival.symm⟩
      exact mem_progPoints.mpr hc


private def bCount (b : Lane) (v : Int) : Nat :=
  (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int))

private theorem filter_range_eq (n k : Nat) :
    (List.range n).filter (fun i => i == k) = if k < n then [k] else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      by_cases hk : k = n
      · subst k
        simp at ih ⊢
      · have hnk : n ≠ k := by omega
        by_cases h : k < n
        · simp [ih, h, hnk]
          omega
        · have hkgt : n < k := by omega
          simp [ih, h, hkgt, hnk]
          omega

private theorem filter_eq_nil_of_never {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons, h x (by simp), ih (fun y hy => h y (by simp [hy]))]
      simp

private theorem countP_eq_length_filter {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = (l.filter p).length := List.countP_eq_length_filter

private theorem bCount_const (b : Lane) (hb : b.step = 0) (v : Int) :
    bCount b v = if b.base == v then b.n else 0 := by
  unfold bCount
  rw [countP_eq_length_filter]
  by_cases h : b.base == v
  · have hbv : b.base = v := beq_iff_eq.mp h
    have hf : (List.range b.n).filter (fun i : Nat => v == b.base + b.step * (i : Int)) =
        List.range b.n := by
      apply List.filter_eq_self.mpr
      intro i hi
      simp [hb, hbv]
    rw [hf]
    simp [h]
  · have hbne : b.base ≠ v := by intro he; exact h (by rw [he]; simp)
    have hf : (List.range b.n).filter (fun i : Nat => v == b.base + b.step * (i : Int)) = [] := by
      apply filter_eq_nil_of_never
      intro i hi
      have hne : v ≠ b.base := by exact hbne.symm
      simp [hb, hne]
    rw [hf]
    simp [h]

private theorem bCount_step_ne_zero (b : Lane) (hb : b.step ≠ 0) (v : Int) :
    bCount b v = if contains (values b) v then 1 else 0 := by
  unfold bCount
  rw [countP_eq_length_filter]
  by_cases h : contains (values b) v = true
  · obtain ⟨i, hi, hval⟩ := (mem_values_lane b v).mp h
    have hfmem : ∀ j ∈ List.range b.n,
        (v == b.base + b.step * (j : Int)) = (j == i) := by
      intro j hj
      have hj' : j < b.n := List.mem_range.mp hj
      by_cases hji : j = i
      · subst j
        rw [Int.mul_comm (i : Int) b.step] at hval
        simp [hval]
      · have hne : v ≠ b.base + b.step * (j : Int) := by
          intro hvj
          have hmul : b.step * (j : Int) = b.step * (i : Int) := by
            rw [hval] at hvj
            grind
          have hji' : (j : Int) = (i : Int) := (Int.mul_eq_mul_left_iff hb).mp hmul
          exact hji (by exact_mod_cast hji')
        rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr hji]
    rw [List.filter_congr hfmem, filter_range_eq]
    simp [hi, h]
  · have hf : (List.range b.n).filter (fun j : Nat => v == b.base + b.step * (j : Int)) = [] := by
      apply filter_eq_nil_of_never
      intro j hj
      have hj' : j < b.n := List.mem_range.mp hj
      have hne : v ≠ b.base + b.step * (j : Int) := by
        intro hvj
        exact h ((mem_values_lane b v).mpr ⟨j, hj', by rw [hvj, Int.mul_comm]⟩)
      exact beq_eq_false_iff_ne.mpr hne
    rw [hf]
    simp [h]


private theorem countP_eq_zero {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_range_const_nat (n v : Nat) :
    ((List.range n).map (fun _ : Nat => v)).sum = n * v := by
  induction n with
  | zero => simp
  | succ n ih => rw [show n + 1 = Nat.succ n by omega, List.range_succ,
      List.map_append, List.sum_append]; simp [ih, Nat.succ_mul]

private theorem sum_map_ite_zero_nat {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem lane_points_count_sum (a : Lane) (f : Int → Nat) :
    ((lanePoints a).map (fun r => f r.2)).sum =
      ((List.range a.n).map (fun i : Nat => f (a.base + (i : Int) * a.step))).sum := by
  unfold lanePoints
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro i hi
  simp [Int.mul_comm]

private theorem bCount_eq_zero_of_not_contains (b : Lane) (v : Int)
    (h : ¬ contains (values b) v = true) : bCount b v = 0 := by
  unfold bCount
  apply countP_eq_zero
  intro i hi
  have hi' : i < b.n := List.mem_range.mp hi
  have hne : v ≠ b.base + b.step * (i : Int) := by
    intro hv
    exact h ((mem_values_lane b v).mpr ⟨i, hi', by rw [hv, Int.mul_comm]⟩)
  exact beq_eq_false_iff_ne.mpr hne

private theorem intersection_const_n (a b : Progression) (ha : a.step = 0) (han : 0 < a.n) :
    (intersection a b).n = if contains b a.lo then 1 else 0 := by
  by_cases hbn : b.n = 0
  · simp [intersection, contains, hbn]
  · have hc1 : (a.n == 0 || b.n == 0) = false := by simp [beq_iff_eq, Nat.ne_of_gt han, hbn]
    have hc1' : ¬ ((a.n == 0 || b.n == 0) = true) := by rw [hc1]; decide
    unfold intersection
    simp only [Id.run]
    rw [if_neg hc1', if_pos (by simp [ha])]
    by_cases hc : contains b a.lo
    · rw [if_pos hc]
      rfl
    · rw [if_neg hc]
      rfl

private theorem sum_bCount_intersection (a b : Lane) (ha : a.step ≠ 0) :
    ((progPoints (intersection (values a) (values b))).map (fun v => bCount b v)).sum =
      joinCount a b := by
  unfold joinCount
  have hfa : (if a.step == 0 then a.n else 1) = 1 := by
    exact if_neg (by simp [ha])
  rw [hfa]
  by_cases hb : b.step = 0
  · rw [if_pos (by simp [hb])]
    have hfct : ∀ v ∈ progPoints (intersection (values a) (values b)), bCount b v = b.n := by
      intro v hv
      have hc := (intersection_contains2 (values a) (values b) v).mp (mem_progPoints.mp hv)
      rw [bCount_const b hb]
      have hvals : values b = ⟨b.base, 0, b.n⟩ := by
        unfold values
        simp [hb]
      rw [hvals] at hc
      have hveq : b.base = v := by
        have ht := (contains_const_iff2 b.base b.n v).mp hc.2
        exact ht.2.symm
      simp [hveq]
    rw [show (List.map (fun v => bCount b v) (progPoints (intersection (values a) (values b)))) =
        (List.map (fun _ : Int => b.n) (progPoints (intersection (values a) (values b)))) by
      apply List.map_congr_left
      intro v hv
      exact hfct v hv]
    unfold progPoints
    rw [List.map_map]
    change ((List.range (intersection (values a) (values b)).n).map (fun _ : Nat => b.n)).sum = _
    rw [sum_range_const_nat]
    simp [Nat.mul_assoc]
  · rw [if_neg (by simp [hb])]
    have hfct : ∀ v ∈ progPoints (intersection (values a) (values b)), bCount b v = 1 := by
      intro v hv
      have hc := (intersection_contains2 (values a) (values b) v).mp (mem_progPoints.mp hv)
      rw [bCount_step_ne_zero b hb]
      simp [hc.2]
    rw [show (List.map (fun v => bCount b v) (progPoints (intersection (values a) (values b)))) =
        (List.map (fun _ : Int => 1) (progPoints (intersection (values a) (values b)))) by
      apply List.map_congr_left
      intro v hv
      exact hfct v hv]
    unfold progPoints
    rw [List.map_map]
    change ((List.range (intersection (values a) (values b)).n).map (fun _ : Nat => 1)).sum = _
    rw [sum_range_const_nat]
    simp [Nat.mul_assoc]

private theorem lane_residue_join (a b : Lane) :
    ((lanePoints a).map (fun r => bCount b r.2)).sum = joinCount a b := by
  by_cases has : a.step = 0
  · rw [lane_points_count_sum]
    have hsum : ((List.range a.n).map (fun i : Nat =>
        bCount b (a.base + (i : Int) * a.step))).sum = a.n * bCount b a.base := by
      rw [show (List.map (fun i : Nat => bCount b (a.base + (i : Int) * a.step)) (List.range a.n)) =
          (List.map (fun _ : Nat => bCount b a.base) (List.range a.n)) by
        apply List.map_congr_left
        intro i hi
        simp [has]]
      exact sum_range_const_nat a.n (bCount b a.base)
    rw [hsum]
    unfold joinCount
    have hvals : values a = ⟨a.base, 0, a.n⟩ := by
      unfold values
      simp [has]
    rw [hvals]
    rw [if_pos (by simp [has])]
    by_cases han : a.n = 0
    · simp [han]
    · rw [intersection_const_n ⟨a.base, 0, a.n⟩ (values b) rfl (Nat.pos_of_ne_zero han)]
      by_cases hc : contains (values b) a.base = true
      · rw [if_pos hc]
        have hbm : (if b.step == 0 then b.n else 1) = bCount b a.base := by
          by_cases hb : b.step = 0
          · rw [if_pos (by simp [hb]), bCount_const b hb]
            have hbvals : values b = ⟨b.base, 0, b.n⟩ := by
              unfold values
              simp [hb]
            rw [hbvals] at hc
            have ht := (contains_const_iff2 b.base b.n a.base).mp hc
            simp [ht.2.symm]
          · rw [if_neg (by simp [hb]), bCount_step_ne_zero b hb]
            simp [hc]
        rw [hbm]
        simp [Nat.mul_assoc, Nat.mul_comm]
      · rw [if_neg hc]
        have hz := bCount_eq_zero_of_not_contains b a.base hc
        simp [hz]
  · have hperm := progPoints_values_perm a has
    have hsum1 : ((lanePoints a).map (fun r => bCount b r.2)).sum =
        ((progPoints (values a)).map (fun v => bCount b v)).sum := by
      have hmapeq : (List.map (fun r : Nat × Int => bCount b r.snd) (lanePoints a)) =
          List.map (fun v : Int => bCount b v) ((lanePoints a).map Prod.snd) := by
        rw [List.map_map]
        rfl
      rw [hmapeq]
      exact list_sum_perm_nat (hperm.symm.map (fun v : Int => bCount b v))
    rw [hsum1]
    have hzero : ∀ v, ¬ contains (values b) v = true → bCount b v = 0 := by
      intro v hv
      exact bCount_eq_zero_of_not_contains b v hv
    have hpoint : ((progPoints (values a)).map (fun v => bCount b v)).sum =
        ((progPoints (values a)).map (fun v => if contains (values b) v then bCount b v else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      by_cases hc : contains (values b) v = true <;> simp [hc, hzero]
    rw [hpoint, ← sum_map_ite_zero_nat]
    rw [← sum_progPoints_intersection_nat (values a) (values b) (by dsimp [values]; exact Int.natAbs_pos.mpr has)
      (fun v => bCount b v)]
    exact sum_bCount_intersection a b has



private theorem flatMap_count_sum (ls : List Lane) (b : Lane) :
    ((ls.flatMap lanePoints).map (fun r => bCount b r.2)).sum =
      (ls.map fun a => ((lanePoints a).map (fun r => bCount b r.2)).sum).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.ResidueJoin.Goal := by
  intro c b
  change ((rows layout c).map (fun r => (List.range b.n).countP
    (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      ((layout.lanes c).map (joinCount · b)).sum
  change ((rows layout c).map (fun r => bCount b r.2)).sum =
    ((layout.lanes c).map (joinCount · b)).sum
  rw [rows_eq_flatMap c]
  rw [flatMap_count_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_residue_join a b


private theorem sum_filter_not_eq_sub {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter (fun x => !p x)).map f).sum =
      (l.map f).sum - ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x <;> simp [h, ih] <;> omega

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_range_linear (n : Nat) (x d : Int) :
    ((List.range n).map (fun k : Nat => x + (k : Int) * d)).sum =
      (n : Int) * x + tri n * d := by
  induction n with
  | zero => simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [range_succ_map_sum, ih, tri_succ]
      simp [Int.natCast_add, Int.add_mul]
      grind

private theorem deletedSum_eq_sum (a : Lane) (mask : Progression) (hs : a.stride = 1) :
    deletedSum a mask =
      (((progPoints (positions a)).filter (fun p => contains mask p)).map
        (fun p => a.base + (p - (a.start : Int)) * a.step)).sum := by
  unfold deletedSum
  dsimp [firstAt, slopeAt]
  rw [hs]
  simp only [Nat.div_one, Int.ediv_one, Int.ofNat_one, Int.mul_one]
  rw [← sum_progPoints_intersection (positions a) mask (by simp [positions, hs])
    (fun p => a.base + (p - (a.start : Int)) * a.step)]
  unfold progPoints positions progVal
  rw [← sum_range_linear]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  grind

private theorem progPoints_positions (a : Lane) (hs : a.stride = 1) :
    progPoints (positions a) = ((lanePoints a).map Prod.fst).map (fun n : Nat => (n : Int)) := by
  unfold progPoints positions progVal lanePoints
  simp [hs, Int.natCast_mul]

private theorem lane_delete_progression (a : Lane) (mask : Progression) (hs : a.stride = 1) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
      partialSum a a.n - deletedSum a mask := by
  rw [sum_filter_not_eq_sub]
  have htotal : ((lanePoints a).map Prod.snd).sum = partialSum a a.n := by
    rw [lane_snd_eq, partialSum_eq_sum]
  rw [htotal]
  have hpoint : ∀ r ∈ lanePoints a,
      r.snd = a.base + ((r.fst : Int) - (a.start : Int)) * a.step := by
    unfold lanePoints
    intro r hr
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hr
    simp [hs]
    have hcast : (a.start : Int) + (i : Int) - (a.start : Int) = (i : Int) := by omega
    rw [hcast]
  have hperm : ((lanePoints a).filter (fun r => contains mask r.1)).map Prod.snd =
      (((progPoints (positions a)).filter (fun p => contains mask p)).map
        (fun p => a.base + (p - (a.start : Int)) * a.step)) := by
    rw [progPoints_positions a hs]
    rw [List.filter_map]
    rw [List.filter_map]
    rw [show List.filter (((fun p => contains mask p) ∘ fun n : Nat => (n : Int)) ∘ Prod.fst) (lanePoints a) =
        List.filter (fun r => contains mask ((r.fst : Nat) : Int)) (lanePoints a) by rfl]
    rw [List.map_map, List.map_map]
    change List.map Prod.snd (List.filter (fun r => contains mask ((r.fst : Nat) : Int)) (lanePoints a)) =
      List.map (fun r => a.base + (((r.fst : Nat) : Int) - (a.start : Int)) * a.step)
        (List.filter (fun r => contains mask ((r.fst : Nat) : Int)) (lanePoints a))
    apply List.map_congr_left
    intro r hr
    exact hpoint r (List.mem_filter.mp hr).1
  rw [hperm, deletedSum_eq_sum a mask hs]



private theorem flatMap_filter_snd_sum (ls : List Lane) (p : Nat × Int → Bool) :
    (((ls.flatMap lanePoints).filter p).map Prod.snd).sum =
      (ls.map fun a => (((lanePoints a).filter p).map Prod.snd).sum).sum := by
  induction ls with
  | nil => simp
  | cons a as ih => simp [List.filter_append, List.map_append, List.sum_append, ih]

theorem sound_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.DeleteProgression.Goal := by
  intro c mask
  change (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map fun a => partialSum a a.n - deletedSum a mask).sum
  rw [rows_eq_flatMap c]
  rw [flatMap_filter_snd_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have hstride : a.stride = 1 := by
    simp [layout] at ha
    rcases ha with rfl | rfl <;> simp [span, lane]
  exact lane_delete_progression a mask hstride


private theorem sum_range_product_linear (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [range_succ_map_sum, ih, tri_succ, squares_succ]
      simp [Int.natCast_add, Int.add_mul]
      grind

private theorem aligned_eq_sum_linear (a b : Lane) :
    aligned a b =
      ((List.range (intersection (positions a) (positions b)).n).map (fun k : Nat =>
        (firstAt a (intersection (positions a) (positions b)) + (k : Int) *
          slopeAt a (intersection (positions a) (positions b))) *
        (firstAt b (intersection (positions a) (positions b)) + (k : Int) *
          slopeAt b (intersection (positions a) (positions b))))).sum := by
  unfold aligned
  let r := intersection (positions a) (positions b)
  rw [sum_range_product_linear]



end TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpanSeekAndProgressions
