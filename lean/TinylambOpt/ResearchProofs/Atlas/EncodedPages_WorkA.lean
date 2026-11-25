import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_DeleteProgression
import TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_AlignedProduct
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_MirroredTail_Remaining
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_BucketRuns_Remaining
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_BucketRuns_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkA

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Common list helpers -/

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_sub {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_map_mul_left {α : Type} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
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

private theorem countP_eq_sum_nat {α : Type} (l : List α) (p : α → Bool) :
    l.countP p = ((l.map (fun x => if p x then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem sum_filterMap_if_some {α β : Type} (l : List α) (p : α → Prop)
    [DecidablePred p] (f g : α → β) :
    l.filterMap (fun x => if p x then some (f x) else some (g x)) =
      l.map (fun x => if p x then f x else g x) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem option_map_if_some {α β : Type} (p : Prop) [Decidable p] (f : α → β) (a b : α) :
    Option.map f (if p then some a else some b) = if p then some (f a) else some (f b) := by
  by_cases h : p <;> simp [h]

private theorem filterMap_if_some_eq_map_prop {α β : Type} (l : List α) (p : α → Prop)
    [DecidablePred p] (f g : α → β) :
    l.filterMap (fun x => if p x then some (f x) else some (g x)) =
      l.map (fun x => if p x then f x else g x) := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem range_split_if {α : Type} (n split : Nat) (f g : Nat → α) :
    (List.range n).map (fun i => if i < split then f i else g i) =
      (List.range (min n split)).map f ++ (List.range (n - split)).map (fun j => g (split + j)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      by_cases h : n < split
      · have hm1 : min n split = n := Nat.min_eq_left (by omega)
        have hm2 : min (n + 1) split = n + 1 := Nat.min_eq_left (by omega)
        have hs0 : n + 1 - split = 0 := by omega
        have hs0' : n - split = 0 := by omega
        simp [h, hm1, hm2, hs0, hs0', List.range_succ]
      · have hm1 : min n split = split := Nat.min_eq_right (by omega)
        have hm2 : min (n + 1) split = split := Nat.min_eq_right (by omega)
        have hs1 : n + 1 - split = (n - split) + 1 := by omega
        have hnsub : split + (n - split) = n := by omega
        simp [h, hm1, hm2, hs1, List.range_succ, hnsub]

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

private theorem map_sum_flatMap {α : Type} (ls : List α) (f : α → List (Nat × Int)) :
    ((ls.flatMap f).map Prod.snd).sum = (ls.map (fun x => ((f x).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih => simp [List.map_append, list_sum_append_int, ih]
private theorem length_flatMap {α β : Type} (ls : List α) (f : α → List β) :
    (ls.flatMap f).length = (ls.map fun x => (f x).length).sum := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem countP_flatMap {α β : Type} (ls : List α) (f : α → List β) (p : β → Bool) :
    (ls.flatMap f).countP p = (ls.map (fun x => (f x).countP p)).sum := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [List.countP_append, ih]

private theorem filter_flatMap {α β : Type} (ls : List α) (f : α → List β) (p : β → Bool) :
    (ls.flatMap f).filter p = ls.flatMap (fun x => (f x).filter p) := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [ih, List.filter_append]

/-! ## Lane points -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private def allPoints (ls : List Lane) : List (Nat × Int) := ls.flatMap lanePoints

private theorem lanePoints_stride1 (a : Lane) (hs : a.stride = 1) :
    lanePoints a = (List.range a.n).map (fun (i : Nat) => (a.start + i, a.base + (i : Int) * a.step)) := by
  unfold lanePoints
  simp [hs]

private theorem lanePoints_map_snd (a : Lane) :
    (lanePoints a).map Prod.snd = (List.range a.n).map (fun (i : Nat) => a.base + (i : Int) * a.step) := by
  simp [lanePoints]

/-! ## Arithmetic -/

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
    partialSum a k = ((List.range k).map (fun (i : Nat) => a.base + (i : Int) * a.step)).sum := by
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
/-! ## Finite progressions and CRT intersection -/

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
  constructor
  · intro h
    have h' : (0 < n ∧ lo ≤ v) ∧ v ≤ lo := by
      simpa [contains, lastVal] using h
    exact ⟨h'.1.1, by omega⟩
  · rintro ⟨hn, rfl⟩
    simp [contains, lastVal, hn]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by
    exact_mod_cast (Nat.le_pred_of_lt hk)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  have hknon : (0 : Int) ≤ k := by omega
  have hstepnon : (0 : Int) ≤ (a.step : Int) := Int.natCast_nonneg _
  have hkmulnon : (0 : Int) ≤ (k : Int) * a.step := Int.mul_nonneg hknon hstepnon
  constructor
  · rw [hval]
    omega
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm (k : Int) (a.step : Int)]
  omega

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgd : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgl : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  obtain ⟨tg, htg⟩ := hgd
  obtain ⟨tl, htl⟩ := hgl
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨tg * (j : Int), ?_⟩
    rw [htg, Int.mul_assoc]
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨tl * (i : Int), ?_⟩
    rw [htl, Int.mul_assoc]
  have hi' : (i : Int) * a.step = v - a.lo := by omega
  have hj' : (j : Int) * b.step = v - b.lo := by omega
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by
    have h1' : (b.step : Int) * (j : Int) = (j : Int) * b.step := Int.mul_comm _ _
    have h2' : (a.step : Int) * (i : Int) = (i : Int) * a.step := Int.mul_comm _ _
    rw [h1', h2', hj', hi']
    omega
  have hdvd : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  have hneg : (a.step.gcd b.step : Int) ∣ -(a.lo - b.lo) := Int.dvd_neg.mpr hdvd
  simpa [Int.neg_sub] using hneg

private theorem contains_unit_one (lo : Int) (v : Int) :
    contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    have hk1 : k < 1 := by simpa using hk
    have hk0 : k = 0 := by omega
    subst k
    unfold progVal at hv
    simpa using hv
  · intro hv
    subst v
    refine ⟨0, ?_, ?_⟩
    · change 0 < (1 : Nat)
      decide
    · unfold progVal
      simp

private theorem contains_unit_zero (lo : Int) (v : Int) :
    contains ⟨lo, 1, 0⟩ v = true ↔ False := by
  rw [contains_iff_mem]
  constructor
  · rintro ⟨k, hk, _⟩
    have hk0 : k < 0 := by simpa using hk
    omega
  · intro h
    exact h.elim

private theorem mul_div_gcd_eq_lcm (a b : Nat) (ha : 0 < a) :
    a * (b / a.gcd b) = Nat.lcm a b := by
  have hg : 0 < a.gcd b := Nat.gcd_pos_of_pos_left b ha
  apply Nat.eq_of_mul_eq_mul_left (n := a.gcd b) hg
  rw [Nat.gcd_mul_lcm]
  calc
    a.gcd b * (a * (b / a.gcd b)) = a * (a.gcd b * (b / a.gcd b)) := by grind
    _ = a * b := by rw [Nat.mul_comm (a.gcd b), Nat.div_mul_cancel (Nat.gcd_dvd_right a b)]

private theorem lcm_dvd_of_dvd_of_dvd (a b : Int) (X : Int) (ha : a ∣ X) (hb : b ∣ X) :
    (Int.lcm a b : Int) ∣ X := by
  have hxa : a ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr ha
  have hxb : b ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr hb
  have h : Int.lcm a b ∣ X.natAbs := Int.lcm_dvd hxa hxb
  exact Int.dvd_natAbs.mp (by exact_mod_cast h)

private theorem intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · cases a with
    | mk alo astep an =>
        simp at han
        subst an
        simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · cases b with
      | mk blo bstep bn =>
          simp at hbn
          subst bn
          simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · cases a with
        | mk alo astep an =>
          simp at has han hanp
          subst astep
          have hInt : intersection ⟨alo, 0, an⟩ b =
              ⟨alo, 1, if contains b alo then 1 else 0⟩ := by
            simp [intersection, han, hbn]
          rw [hInt]
          by_cases hb : contains b alo = true
          · rw [if_pos hb]
            constructor
            · intro h
              have hvalo : v = alo := (contains_unit_one alo v).mp h
              subst v
              exact ⟨(contains_const_iff alo an alo).mpr ⟨hanp, rfl⟩, hb⟩
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (contains_unit_one alo alo).mpr rfl
          · rw [if_neg hb]
            constructor
            · intro h
              exact (contains_unit_zero alo v).mp h |>.elim
            · rintro ⟨ha, hbv⟩
              have hva := (contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (hb hbv).elim
      · by_cases hbs : b.step = 0
        · cases b with
          | mk blo bstep bn =>
            simp at hbs hbn hbnp
            subst bstep
            have hInt : intersection a ⟨blo, 0, bn⟩ =
                ⟨blo, 1, if contains a blo then 1 else 0⟩ := by
              simp [intersection, han, hbn, has]
            rw [hInt]
            by_cases ha' : contains a blo = true
            · rw [if_pos ha']
              constructor
              · intro h
                have hvlo : v = blo := (contains_unit_one blo v).mp h
                subst v
                exact ⟨ha', (contains_const_iff blo bn blo).mpr ⟨hbnp, rfl⟩⟩
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (contains_unit_one blo blo).mpr rfl
            · rw [if_neg ha']
              constructor
              · intro h
                exact (contains_unit_zero blo v).mp h |>.elim
              · rintro ⟨ha, hbv⟩
                have hvb := (contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (ha' ha).elim
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
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hasp
          have hApos : 0 < a.step / a.step.gcd b.step := Nat.div_gcd_pos_of_pos_left b.step hasp
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos hApos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := first_root_dvd low root first period hp hfirst
            have hpaEq : period = a.step * (b.step / g) := by
              dsimp [period, g]
              have hbdvd : a.step.gcd b.step ∣ b.step := Nat.gcd_dvd_right a.step b.step
              have hadvd : a.step.gcd b.step ∣ a.step := Nat.gcd_dvd_left a.step b.step
              calc (a.step / a.step.gcd b.step) * b.step
                  = (a.step / a.step.gcd b.step) * (a.step.gcd b.step * (b.step / a.step.gcd b.step)) := by
                      rw [Nat.mul_div_cancel' hbdvd]
                _ = (a.step / a.step.gcd b.step * a.step.gcd b.step) * (b.step / a.step.gcd b.step) := by
                      rw [Nat.mul_assoc]
                _ = a.step * (b.step / a.step.gcd b.step) := by
                      rw [Nat.div_mul_cancel hadvd]
            have hpbEq : period = b.step * (a.step / g) := by
              dsimp [period, g]
              rw [Nat.mul_comm]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpaEq
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpbEq
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (↑period : Int)).toNat + 1 else 0⟩ v = true ↔
              contains a v = true ∧ contains b v = true
            have hsp := span_contains_iff first (↑period) high v hp
            simp only [Int.toNat_natCast] at hsp
            rw [hsp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have h1 : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
                have h2 : (a.step : Int) ∣ (v - root) + (root - a.lo) := Int.dvd_add h1 hroot.1
                simpa [show (v - root) + (root - a.lo) = v - a.lo by omega] using h2
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have h1 : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
                have h2 : (b.step : Int) ∣ (v - root) + (root - b.lo) := Int.dvd_add h1 hroot.2
                simpa [show (v - root) + (root - b.lo) = v - b.lo by omega] using h2
              have hxlow : low ≤ v := by
                have hflow : low ≤ first := by
                  dsimp [first]
                  have := Int.emod_nonneg (root - low) (show (↑period : Int) ≠ 0 by omega)
                  omega
                omega
              constructor
              · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := contains_bounds a v ha
              have hbb := contains_bounds b v hb
              have hxalo0 := contains_dvd a v ha
              have hxblo0 := contains_dvd b v hb
              have hvar : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxalo0 hroot.1
              have hvbr : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo0 hroot.2
              have hperLcmNat : period = Nat.lcm a.step b.step := by
                rw [hpaEq]
                exact mul_div_gcd_eq_lcm a.step b.step hasp
              have hxroot : (period : Int) ∣ v - root := by
                have hl := lcm_dvd_of_dvd_of_dvd (a.step : Int) (b.step : Int) (v - root) hvar hvbr
                rw [hperLcmNat]
                simpa using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hxlow : low ≤ v := by
                dsimp [low]
                omega
              have hfirstle : first ≤ v :=
                first_le_of_congruence low root first v (period : Int) hp hfirst hxlow hxroot
              have hxhigh : v ≤ high := by
                dsimp [high]
                omega
              exact ⟨hfirstle, hxhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            simp only [contains]
            constructor
            · intro h; simp at h
            · intro ⟨ha, hb⟩
              have hg := common_gap_dvd a b v ha hb
              exact (hgapne (Int.emod_eq_zero_of_dvd hg)).elim

/-! ## Per-lane generic identities -/

private theorem list_sum_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]


/-! ## Value membership in lane progressions -/

private theorem values_lo_step (a : Lane) :
    (values a).lo = if a.step < 0 then a.base + ((a.n - 1 : Nat) : Int) * a.step else a.base := rfl

private theorem values_step (a : Lane) : (values a).step = a.step.natAbs := rfl

private theorem values_n (a : Lane) : (values a).n = a.n := rfl

private theorem laneVal_eq_progVal (a : Lane) (i : Nat) (hi : i < a.n) :
    a.base + (i : Int) * a.step =
      (values a).lo + ((if a.step < 0 then a.n - 1 - i else i : Nat) : Int) * (values a).step := by
  unfold values
  by_cases hs : a.step < 0
  · simp only [hs, ite_true]
    have hcast : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by
      have : i ≤ a.n - 1 := by omega
      omega
    rw [hcast]
    have hmul : (((a.n - 1 : Nat) : Int) - (i : Int)) * (a.step.natAbs : Int) =
        ((a.n - 1 : Nat) : Int) * (a.step.natAbs : Int) - (i : Int) * (a.step.natAbs : Int) := by
      rw [Int.sub_mul]
    rw [hmul]
    have hnatabs : (a.step.natAbs : Int) = -a.step := by
      have hle : a.step ≤ 0 := Int.le_of_lt hs
      exact Int.ofNat_natAbs_of_nonpos hle
    rw [hnatabs]
    simp [Int.mul_neg]
    omega
  · simp only [hs, ite_false]
    have hnatabs : (a.step.natAbs : Int) = a.step := by
      have hge : 0 ≤ a.step := Int.not_lt.mp hs
      exact Int.natAbs_of_nonneg hge
    rw [hnatabs]
private theorem laneVal_mem_values (a : Lane) (i : Nat) (hi : i < a.n) :
    contains (values a) (a.base + (i : Int) * a.step) = true := by
  apply (contains_iff_mem _ _).mpr
  refine ⟨if a.step < 0 then a.n - 1 - i else i, ?_, ?_⟩
  · by_cases hs : a.step < 0 <;> simp [hs, values_n] <;> omega
  · unfold progVal
    exact laneVal_eq_progVal a i hi

private theorem values_mem_iff_laneVal (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i, i < a.n ∧ v = a.base + (i : Int) * a.step := by
  rw [contains_iff_mem]
  unfold progMem
  constructor
  · rintro ⟨k, hk, hv⟩
    have hkn : k < a.n := by simpa [values_n] using hk
    by_cases hs : a.step < 0
    · refine ⟨a.n - 1 - k, ?_, ?_⟩
      · omega
      · rw [hv]
        unfold progVal values
        simp only [hs, ite_true]
        have hk' : k ≤ a.n - 1 := by omega
        have hcast2 : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by
          omega
        rw [hcast2]
        have hnatabs : (a.step.natAbs : Int) = -a.step := Int.ofNat_natAbs_of_nonpos (Int.le_of_lt hs)
        rw [hnatabs]
        rw [Int.sub_mul]
        simp [Int.mul_neg, Int.sub_eq_add_neg, Int.add_assoc]
    · refine ⟨k, hkn, ?_⟩
      rw [hv]
      unfold progVal values
      simp only [hs, ite_false]
      have hnatabs : (a.step.natAbs : Int) = a.step := Int.natAbs_of_nonneg (Int.not_lt.mp hs)
      simp [hnatabs]
  · rintro ⟨i, hi, rfl⟩
    exact (contains_iff_mem (values a) (a.base + ↑i * a.step)).mp (laneVal_mem_values a i hi)
/-! ## Per-lane value counting -/

private def laneCount (a : Lane) (v : Int) : Nat :=
  (List.range a.n).countP (fun (i : Nat) => a.base + (i : Int) * a.step == v)

private theorem laneCount_eq_zero_of_not_mem (a : Lane) (v : Int)
    (h : contains (values a) v = false) : laneCount a v = 0 := by
  unfold laneCount
  apply List.countP_eq_zero.mpr
  intro i hi
  have hi' : i < a.n := List.mem_range.mp hi
  have hne : ¬ (a.base + (i : Int) * a.step == v) = true := by
    intro heq
    have hv : a.base + (i : Int) * a.step = v := eq_of_beq heq
    have hmem : contains (values a) v = true := by
      rw [← hv]
      exact laneVal_mem_values a i hi'
    simp [h] at hmem
  cases hb : (a.base + (i : Int) * a.step == v)
  · simp
  · exact (hne hb).elim
private theorem laneCount_const (a : Lane) (v : Int) (hs : a.step = 0) :
    laneCount a v = if a.base == v then a.n else 0 := by
  unfold laneCount
  simp only [hs, Int.mul_zero, Int.add_zero]
  by_cases hv : a.base == v
  · rw [if_pos hv]
    have h : ∀ i ∈ List.range a.n, (a.base == v) = true := fun _ _ => hv
    rw [List.countP_eq_length.mpr h, List.length_range]
  · rw [if_neg hv]
    apply List.countP_eq_zero.mpr
    intro i hi; simpa using hv
private theorem laneCount_step_ne (a : Lane) (v : Int) (hs : a.step ≠ 0)
    (hmem : contains (values a) v = true) : laneCount a v = 1 := by
  unfold laneCount
  rw [values_mem_iff_laneVal] at hmem
  obtain ⟨i, hi, rfl⟩ := hmem
  have hpred : ∀ j ∈ List.range a.n,
      (a.base + (j : Int) * a.step == a.base + (i : Int) * a.step) = decide (j = i) := by
    intro j hj
    by_cases hji : j = i
    · simp [hji]
    · have hne : a.base + (j : Int) * a.step ≠ a.base + (i : Int) * a.step := by
        intro heq
        have hmul : (j : Int) * a.step = (i : Int) * a.step := by omega
        have hsub : ((j : Int) - (i : Int)) * a.step = 0 := by
          rw [Int.sub_mul]; omega
        rcases Int.mul_eq_zero.mp hsub with hji0 | hstep0
        · have : (j : Int) = (i : Int) := by omega
          exact hji (by omega)
        · exact hs hstep0
      simp [hji, beq_eq_false_iff_ne.mpr hne]
  have hcount : (List.range a.n).countP (fun (j : Nat) => a.base + (j : Int) * a.step == a.base + (i : Int) * a.step)
      = (List.range a.n).countP (fun (j : Nat) => decide (j = i)) := by
    apply List.countP_congr
    intro j hj
    simp [hpred j hj]
  rw [hcount]
  rw [List.countP_eq_length_filter]
  have hle : (List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n)).length ≤ 1 := by
    have hnodup : (List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n)).Nodup :=
      (List.nodup_range (n := a.n)).filter _
    have hsub : ∀ x ∈ List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n), x = i := by
      intro x hx; exact of_decide_eq_true (List.mem_filter.mp hx).2
    cases h : List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n) with
    | nil => simp
    | cons x xs =>
      have hx := hsub x (by simp [h])
      have hxs : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          have hy := hsub y (by simp [h])
          have : x = y := by rw [hx, hy]
          subst this
          simp [h] at hnodup
      simp [h, hxs]
  have hge : 1 ≤ (List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n)).length := by
    have hmem : i ∈ List.filter (fun (j : Nat) => decide (j = i)) (List.range a.n) := by
      rw [List.mem_filter]
      exact ⟨List.mem_range.mpr hi, by simp⟩
    exact List.length_pos_of_mem hmem |> Nat.succ_le_of_lt
  omega

private theorem laneCount_eq_mult (a : Lane) (v : Int) :
    laneCount a v = if contains (values a) v then (if a.step == 0 then a.n else 1) else 0 := by
  by_cases hmem : contains (values a) v = true
  · rw [if_pos hmem]
    by_cases hs : a.step == 0
    · rw [if_pos hs]
      have hs0 : a.step = 0 := eq_of_beq hs
      rw [laneCount_const a v hs0]
      rw [values_mem_iff_laneVal] at hmem
      obtain ⟨i, hi, hv⟩ := hmem
      simp [hs0] at hv
      simp [hv]
    · rw [if_neg hs]
      have hs0 : a.step ≠ 0 := by simpa using hs
      exact laneCount_step_ne a v hs0 hmem
  · have hfalse : contains (values a) v = false := by
      cases h : contains (values a) v <;> simp_all
    rw [if_neg hmem]
    exact laneCount_eq_zero_of_not_mem a v hfalse
/-! ## ResidueJoin -/

private theorem countP_range_lane (b : Lane) (v : Int) :
    (List.range b.n).countP (fun (i : Nat) => v == b.base + (i : Int) * b.step) = laneCount b v := by
  unfold laneCount
  congr 1
  funext i
  simp [Bool.beq_comm]
private theorem lane_residueJoin_const (a b : Lane) (ha : a.step = 0) :
    ((List.range a.n).map (fun _ =>
      (List.range b.n).countP (fun (i : Nat) => a.base == b.base + (i : Int) * b.step))).sum =
    ((if contains (values b) a.base then (if b.step == 0 then b.n else 1) else 0) * a.n : Nat) := by
  have hcnt : (List.range b.n).countP (fun (i : Nat) => a.base == b.base + (i : Int) * b.step) =
      if contains (values b) a.base then (if b.step == 0 then b.n else 1) else 0 := by
    rw [countP_range_lane, laneCount_eq_mult]
  simp only [hcnt, List.map_const', List.length_range]
  have hsum : ∀ n m : Nat, (List.replicate n m).sum = n * m := by
    intro n m; induction n with
    | zero => simp
    | succ n ih => simp [List.replicate, List.sum_cons, ih, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]; rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]
  rw [hsum]
  simp [Nat.mul_comm]
/-! ## Progression hit counting -/

private theorem intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  unfold intersection
  dsimp only
  split
  · simp
  next h1 =>
    split
    · simp
    next h2 =>
      split
      · simp
      next h3 =>
        split
        · simp
        next h4 =>
          have hps : 0 < a.step := Nat.pos_of_ne_zero (by simpa using h2)
          have hqs : 0 < b.step := Nat.pos_of_ne_zero (by simpa using h3)
          have hg : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hps
          have hdiv : 0 < a.step / a.step.gcd b.step :=
            Nat.div_pos (Nat.le_of_dvd hps (Nat.gcd_dvd_left a.step b.step)) hg
          exact Nat.mul_pos hdiv hqs
private def progCount (p : Progression) (v : Int) : Nat :=
  (List.range p.n).countP (fun k => progVal p k == v)
private theorem progCount_eq_zero_of_not_mem (p : Progression) (v : Int)
    (h : contains p v = false) : progCount p v = 0 := by
  unfold progCount
  apply List.countP_eq_zero.mpr
  intro k hk
  have hk' : k < p.n := List.mem_range.mp hk
  have hne : ¬ (progVal p k == v) = true := by
    intro heq
    have hv : progVal p k = v := eq_of_beq heq
    have hmem : contains p v = true := by
      rw [← hv, contains_iff_mem]
      exact ⟨k, hk', rfl⟩
    simp [h] at hmem
  cases hb : (progVal p k == v)
  · simp
  · exact (hne hb).elim
private theorem progCount_eq_one_of_mem_step_pos (p : Progression) (v : Int)
    (hs : 0 < p.step) (hmem : contains p v = true) : progCount p v = 1 := by
  unfold progCount
  rw [contains_iff_mem] at hmem
  obtain ⟨k, hk, rfl⟩ := hmem
  have hpred : ∀ j ∈ List.range p.n,
      (progVal p j == progVal p k) = decide (j = k) := by
    intro j hj
    by_cases hjk : j = k
    · simp [hjk]
    · have hne : progVal p j ≠ progVal p k := by
        intro heq
        unfold progVal at heq
        have hmul : (j : Int) * (p.step : Int) = (k : Int) * (p.step : Int) := by omega
        have hstep : (p.step : Int) ≠ 0 := by exact_mod_cast Nat.ne_of_gt hs
        have hsub : ((j : Int) - (k : Int)) * (p.step : Int) = 0 := by
          rw [Int.sub_mul]; omega
        rcases Int.mul_eq_zero.mp hsub with hjk0 | hstep0
        · exact hjk (by omega)
        · exact hstep hstep0
      simp [hjk, beq_eq_false_iff_ne.mpr hne]
  have hcount : (List.range p.n).countP (fun j => progVal p j == progVal p k)
      = (List.range p.n).countP (fun (j : Nat) => decide (j = k)) := by
    apply List.countP_congr
    intro j hj
    simp [hpred j hj]
  rw [hcount, List.countP_eq_length_filter]
  have hle : (List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n)).length ≤ 1 := by
    have hnodup : (List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n)).Nodup :=
      (List.nodup_range (n := p.n)).filter _
    have hsub : ∀ x ∈ List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n), x = k := by
      intro x hx; exact of_decide_eq_true (List.mem_filter.mp hx).2
    cases h : List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n) with
    | nil => simp
    | cons x xs =>
      have hx := hsub x (by simp [h])
      have hxs : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          have hy := hsub y (by simp [h])
          have : x = y := by rw [hx, hy]
          subst this
          simp [h] at hnodup
      simp [h, hxs]
  have hge : 1 ≤ (List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n)).length := by
    have hmem' : k ∈ List.filter (fun (j : Nat) => decide (j = k)) (List.range p.n) := by
      rw [List.mem_filter]
      exact ⟨List.mem_range.mpr hk, by simp⟩
    exact List.length_pos_of_mem hmem' |> Nat.succ_le_of_lt
  omega

private theorem progCount_inter (a b : Progression) (v : Int) :
    progCount (intersection a b) v = if contains (intersection a b) v then 1 else 0 := by
  by_cases hmem : contains (intersection a b) v = true
  · rw [if_pos hmem]
    exact progCount_eq_one_of_mem_step_pos (intersection a b) v (intersection_step_pos a b) hmem
  · have hfalse : contains (intersection a b) v = false := by
      cases h : contains (intersection a b) v <;> simp_all
    rw [if_neg hmem]
    exact progCount_eq_zero_of_not_mem (intersection a b) v hfalse
/-! ## Sum swap -/

private theorem sum_range_swap_nat (m n : Nat) (f : Nat → Nat → Nat) :
    ((List.range m).map (fun i => ((List.range n).map (fun k => f i k)).sum)).sum =
      ((List.range n).map (fun k => ((List.range m).map (fun i => f i k)).sum)).sum := by
  induction m with
  | zero =>
      simp only [List.range_zero, List.map_nil, List.sum_nil]
      have hz : ∀ k, ((List.range k).map (fun _ => (0:Nat))).sum = 0 := by
        intro k; induction k with
        | zero => simp
        | succ k ih => simp [List.range_succ, List.map_append, List.sum_append, ih]
      exact (hz n).symm
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_singleton, ih]
      have hdist : ∀ (l : List Nat),
          (l.map (fun k => ((List.range m).map (fun i => f i k)).sum + f m k)).sum =
          (l.map (fun k => ((List.range m).map (fun i => f i k)).sum)).sum + (l.map (fun k => f m k)).sum := by
        intro l; induction l with
        | nil => simp
        | cons x xs ih2 => simp [List.map_cons, List.sum_cons, ih2]; omega
      simp only [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_singleton]
      rw [hdist]

private theorem hitCount_step_ne (a b : Lane) (ha : a.step ≠ 0) :
    ((List.range a.n).map (fun (i : Nat) =>
      if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum =
    (intersection (values a) (values b)).n := by
  let I := intersection (values a) (values b)
  have hleft : ∀ i, i < a.n →
      (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) =
      progCount I (a.base + (i : Int) * a.step) := by
    intro i hi
    have hia : contains (values a) (a.base + (i : Int) * a.step) = true := laneVal_mem_values a i hi
    have hcond : contains (values b) (a.base + (i : Int) * a.step) =
        contains I (a.base + (i : Int) * a.step) := by
      cases hb : contains (values b) (a.base + (i : Int) * a.step)
      · cases hI' : contains I (a.base + (i : Int) * a.step)
        · rfl
        · exfalso
          have := (intersection_contains (values a) (values b) (a.base + (i : Int) * a.step)).mp hI'
          simp [hb] at this
      · have hI' := (intersection_contains (values a) (values b) (a.base + (i : Int) * a.step)).mpr ⟨hia, hb⟩
        exact hI'.symm
    rw [hcond, progCount_inter]
  have hsum :
      ((List.range a.n).map (fun (i : Nat) =>
        if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum =
      ((List.range a.n).map (fun (i : Nat) =>
        progCount I (a.base + (i : Int) * a.step))).sum := by
    congr 1
    apply List.map_congr_left
    intro i hi
    exact hleft i (List.mem_range.mp hi)
  rw [hsum]
  unfold progCount
  have hswap :
      ((List.range a.n).map (fun (i : Nat) =>
        (List.range I.n).countP (fun (k : Nat) => progVal I k == a.base + (i : Int) * a.step))).sum =
      ((List.range I.n).map (fun (k : Nat) =>
        (List.range a.n).countP (fun (i : Nat) => progVal I k == a.base + (i : Int) * a.step))).sum := by
    have hcountP : ∀ {α : Type} (p : α → Bool) (l : List α),
        l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
      intro α p l
      induction l with
      | nil => simp
      | cons x xs ih =>
          cases hp : p x <;> simp [List.countP_cons, hp, ih, List.map_cons, List.sum_cons, Nat.add_comm]
    have hL : ((List.range a.n).map (fun (i : Nat) =>
        (List.range I.n).countP (fun (k : Nat) => progVal I k == a.base + (i : Int) * a.step))).sum =
      ((List.range a.n).map (fun (i : Nat) =>
        ((List.range I.n).map (fun (k : Nat) => if progVal I k == a.base + (i : Int) * a.step then 1 else 0)).sum)).sum := by
      congr 1
      apply List.map_congr_left
      intro i _
      rw [hcountP]
    have hR : ((List.range I.n).map (fun (k : Nat) =>
        (List.range a.n).countP (fun (i : Nat) => progVal I k == a.base + (i : Int) * a.step))).sum =
      ((List.range I.n).map (fun (k : Nat) =>
        ((List.range a.n).map (fun (i : Nat) => if progVal I k == a.base + (i : Int) * a.step then 1 else 0)).sum)).sum := by
      congr 1
      apply List.map_congr_left
      intro k _
      rw [hcountP]
    rw [hL, hR]
    exact sum_range_swap_nat a.n I.n (fun i k => if progVal I k == a.base + (i : Int) * a.step then 1 else 0)
  rw [hswap]
  have hinner : ∀ k, k < I.n →
      (List.range a.n).countP (fun (i : Nat) => progVal I k == a.base + (i : Int) * a.step) = 1 := by
    intro k hk
    have hmemI : contains I (progVal I k) = true := (contains_iff_mem I (progVal I k)).mpr ⟨k, hk, rfl⟩
    have hmemA : contains (values a) (progVal I k) = true :=
      ((intersection_contains (values a) (values b) (progVal I k)).mp hmemI).1
    have hlc : laneCount a (progVal I k) = 1 := by
      rw [laneCount_eq_mult]
      rw [if_pos hmemA]
      have hstep : (a.step == 0) = false := by
        cases hs' : (a.step == 0)
        · rfl
        · exact absurd (eq_of_beq hs') ha
      simp [hstep]
    unfold laneCount at hlc
    have hcomm : (List.range a.n).countP (fun (i : Nat) => progVal I k == a.base + (i : Int) * a.step) =
        (List.range a.n).countP (fun (i : Nat) => a.base + (i : Int) * a.step == progVal I k) := by
      congr 1
      funext i
      cases h1 : (progVal I k == a.base + (i : Int) * a.step) <;>
        cases h2 : (a.base + (i : Int) * a.step == progVal I k) <;>
          simp_all [beq_iff_eq]
    rw [hcomm]
    exact hlc
  have hsum1 : ((List.range I.n).map (fun (k : Nat) =>
      (List.range a.n).countP (fun (i : Nat) => progVal I k == a.base + (i : Int) * a.step))).sum =
      ((List.range I.n).map (fun _ => 1)).sum := by
    congr 1
    apply List.map_congr_left
    intro k hk
    exact hinner k (List.mem_range.mp hk)
  rw [hsum1]
  have hsumc : ((List.range I.n).map (fun _ => 1)).sum = I.n := by
    have h1 : (List.range I.n).map (fun _ => 1) = List.replicate I.n 1 := by
      rw [List.map_const', List.length_range]
    rw [h1]
    induction I.n with
    | zero => simp
    | succ n ih => simp [List.replicate, List.sum_cons, ih, Nat.add_comm]
  exact hsumc
private theorem sum_map_flatMap_nat {α β : Type} (ls : List α) (f : α → List β) (g : β → Nat) :
    ((ls.flatMap f).map g).sum = (ls.map (fun x => ((f x).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.map_append, List.sum_append, ih]

private theorem sum_map_flatMap_int {α β : Type} (ls : List α) (f : α → List β) (g : β → Int) :
    ((ls.flatMap f).map g).sum = (ls.map (fun x => ((f x).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih =>
      simp [List.map_append, list_sum_append_int, ih]
private theorem lane_residueJoin (a b : Lane) :
    ((List.range a.n).map (fun (i : Nat) =>
      (List.range b.n).countP (fun (j : Nat) => a.base + (i : Int) * a.step == b.base + (j : Int) * b.step))).sum =
    joinCount a b := by
  by_cases ha0 : a.step = 0
  · by_cases hn : a.n = 0
    · simp [hn, joinCount, ha0]
    · have hinner : ∀ i, i < a.n →
          (List.range b.n).countP (fun (j : Nat) => a.base + (i : Int) * a.step == b.base + (j : Int) * b.step) =
          (List.range b.n).countP (fun (j : Nat) => a.base == b.base + (j : Int) * b.step) := by
        intro i _
        congr 1
        funext j
        simp [ha0]
      have hsum : ((List.range a.n).map (fun (i : Nat) =>
          (List.range b.n).countP (fun (j : Nat) => a.base + (i : Int) * a.step == b.base + (j : Int) * b.step))).sum =
        ((List.range a.n).map (fun _ =>
          (List.range b.n).countP (fun (j : Nat) => a.base == b.base + (j : Int) * b.step))).sum := by
        congr 1
        apply List.map_congr_left
        intro i hi
        exact hinner i (List.mem_range.mp hi)
      rw [hsum, lane_residueJoin_const a b ha0]
      unfold joinCount
      have hstepa : (a.step == 0) = true := by simp [ha0]
      simp only [hstepa, ite_true]
      have hinter_n : (intersection (values a) (values b)).n =
          if contains (values b) a.base then 1 else 0 := by
        unfold intersection values
        simp only [ha0, Int.natAbs_zero, beq_self_eq_true, ite_true]
        have hnan : (a.n == 0) = false := by simp [hn]
        simp only [hnan, Bool.false_or]
        by_cases hbn : b.n = 0
        · simp [contains, hbn]
        · have hbnb : (b.n == 0) = false := by simp [hbn]
          simp only [hbnb, Bool.or_false]
          by_cases hbs : b.step.natAbs = 0
          · have hbs0 : b.step = 0 := by exact Int.natAbs_eq_zero.mp hbs
            simp only [hbs, ite_true]
            simp_all [contains, values, lastVal]
          · try simp only [hbs, ite_false]
            try unfold contains lastVal
            try simp_all [contains, values, lastVal]
            try omega
            try rfl
      rw [hinter_n]
      by_cases hcb : contains (values b) a.base = true <;> simp [hcb, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  · have hinner : ∀ i, i < a.n →
        (List.range b.n).countP (fun (j : Nat) => a.base + (i : Int) * a.step == b.base + (j : Int) * b.step) =
        laneCount b (a.base + (i : Int) * a.step) := by
      intro i _
      rw [countP_range_lane]
    have hsum : ((List.range a.n).map (fun (i : Nat) =>
        (List.range b.n).countP (fun (j : Nat) => a.base + (i : Int) * a.step == b.base + (j : Int) * b.step))).sum =
      ((List.range a.n).map (fun (i : Nat) => laneCount b (a.base + (i : Int) * a.step))).sum := by
      congr 1
      apply List.map_congr_left
      intro i hi
      exact hinner i (List.mem_range.mp hi)
    rw [hsum]
    have hfactor : ∀ i, i < a.n →
        laneCount b (a.base + (i : Int) * a.step) =
        (if contains (values b) (a.base + (i : Int) * a.step) then (if b.step == 0 then b.n else 1) else 0) := by
      intro i hi
      rw [laneCount_eq_mult]
    have hsum2 : ((List.range a.n).map (fun (i : Nat) => laneCount b (a.base + (i : Int) * a.step))).sum =
        ((List.range a.n).map (fun (i : Nat) =>
          (if contains (values b) (a.base + (i : Int) * a.step) then (if b.step == 0 then b.n else 1) else 0))).sum := by
      congr 1
      apply List.map_congr_left
      intro i hi
      exact hfactor i (List.mem_range.mp hi)
    rw [hsum2]
    have hpull : ((List.range a.n).map (fun (i : Nat) =>
        (if contains (values b) (a.base + (i : Int) * a.step) then (if b.step == 0 then b.n else 1) else 0))).sum =
        ((List.range a.n).map (fun (i : Nat) =>
          (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) * (if b.step == 0 then b.n else 1))).sum := by
      congr 1
      apply List.map_congr_left
      intro i _
      by_cases hc : contains (values b) (a.base + (i : Int) * a.step) = true <;> simp [hc]
    rw [hpull]
    have hdist : ∀ (l : List Nat) (c : Nat), (l.map (fun x => x * c)).sum = l.sum * c := by
      intro l c
      induction l with
      | nil => simp
      | cons x xs ih => simp [List.map_cons, List.sum_cons, ih, Nat.add_mul]
    have hmul := hdist ((List.range a.n).map (fun (i : Nat) =>
        if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)) (if b.step == 0 then b.n else 1)
    have hmap : ((List.range a.n).map (fun (i : Nat) =>
        (if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) * (if b.step == 0 then b.n else 1))).sum =
        (((List.range a.n).map (fun (i : Nat) => if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).map (fun x => x * (if b.step == 0 then b.n else 1))).sum := by
      congr 1
      rw [List.map_map]
      rfl
    rw [hmap, hmul]
    have hhit : ((List.range a.n).map (fun (i : Nat) =>
        if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0)).sum =
        (intersection (values a) (values b)).n :=
      hitCount_step_ne a b ha0
    rw [hhit]
    unfold joinCount
    have hstepa : (a.step == 0) = false := by
      cases h : (a.step == 0)
      · rfl
      · exact absurd (eq_of_beq h) ha0
    simp [hstepa]

private theorem allPoints_residueJoin (ls : List Lane) (b : Lane) :
    ((allPoints ls).map (fun r =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    (ls.map (joinCount · b)).sum := by
  unfold allPoints
  rw [sum_map_flatMap_nat]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  unfold lanePoints
  rw [List.map_map]
  have hr := lane_residueJoin a b
  simp only [Function.comp, Int.mul_comm] at hr ⊢
  exact hr

/-! ## DeleteProgression / AlignedProduct generic stubs -/


namespace LinearOrdinal

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp [layout, lanePoints, allPoints, lane, Int.mul_comm]

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  rw [rows_eq]
  exact allPoints_residueJoin (layout.lanes c) b

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_DeleteProgression.sound_LinearOrdinal_DeleteProgression c mask

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_AlignedProduct.sound_LinearOrdinal_AlignedProduct c b hb

end LinearOrdinal

namespace SaturatingCounter

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp [allPoints, lanePoints, span, lane, Int.mul_comm]


theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  rw [rows_eq]
  exact allPoints_residueJoin (layout.lanes c) b

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1.sound_SaturatingCounter_DeleteProgression c mask

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1.sound_SaturatingCounter_AlignedProduct c b hb

end SaturatingCounter

namespace SegmentedSlope

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp [allPoints, lanePoints, span, lane, Int.mul_comm]


theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  rw [rows_eq]
  exact allPoints_residueJoin (layout.lanes c) b

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1.sound_SegmentedSlope_DeleteProgression c mask

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E1.sound_SegmentedSlope_AlignedProduct c b hb

end SegmentedSlope

namespace MirroredTail

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  unfold rows
  simp only [layout, option_map_if_some]
  rw [filterMap_if_some_eq_map_prop]
  rw [range_split_if]
  simp only [allPoints, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  have h1 : lanePoints (span c 0 c.split c.base c.step) =
      (List.range (min c.n c.split)).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
    simp [lanePoints, span, lane, Int.mul_comm]
  have h2 : lanePoints (span c c.split c.n
      (c.base + c.step * ((c.n - 1 - c.split : Nat) : Int)) (-c.step)) =
      (List.range (c.n - c.split)).map (fun (j : Nat) =>
        (c.split + j, c.base + c.step * ((c.n - 1 - (c.split + j) : Nat) : Int))) := by
    simp [lanePoints, span, lane]
    intro j hj
    have hsj : c.split + j < c.n := by omega
    have hsub : c.n - 1 - (c.split + j) = c.n - 1 - c.split - j := by omega
    rw [hsub]
    have hcast : ((c.n - 1 - c.split - j : Nat) : Int) = ((c.n - 1 - c.split : Nat) : Int) - (j : Int) := by omega
    rw [hcast, Int.mul_sub]
    have hcomm : (j : Int) * (-c.step) = -(c.step * (j : Int)) := by
      rw [Int.mul_comm, Int.neg_mul]
    rw [hcomm]
    omega
  rw [h1, h2]


theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  rw [rows_eq]
  exact allPoints_residueJoin (layout.lanes c) b

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.MirroredTail.Remaining.sound_DeleteProgression c mask

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.MirroredTail.Remaining.sound_AlignedProduct c b hb

end MirroredTail

namespace BucketRuns

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

private theorem rows_eq (c : Config) : rows layout c = allPoints (layout.lanes c) := by
  have hrows : rows layout c = (List.range c.n).map (fun i => (i, c.base + c.step * ((i / width c : Nat) : Int))) := by
    unfold rows layout
    simp [filterMap_some_eq_map, lane]
  have hall : allPoints (layout.lanes c) = (List.range c.n).map (fun i => (i, c.base + c.step * ((i / width c : Nat) : Int))) := by
    unfold allPoints layout
    let w := width c
    have hw : 0 < w := by dsimp [w, width]; omega
    have hspan : ∀ j, lanePoints (span c (j * w) ((j + 1) * w) (c.base + c.step * (j : Int)) 0) =
        (List.range (min c.n ((j + 1) * w) - min c.n (j * w))).map
          (fun k => (min c.n (j * w) + k, c.base + c.step * (j : Int))) := by
      intro j
      unfold lanePoints span
      simp only [lane, Nat.mul_one, Int.mul_zero, Int.add_zero]
      have hlen : (min c.n ((j + 1) * w) - j * w + 1 - 1) / 1 = min c.n ((j + 1) * w) - j * w := by omega
      rw [hlen]
      by_cases hjw : j * w ≤ c.n
      · have hminjw : min c.n (j * w) = j * w := Nat.min_eq_right hjw
        simp [hminjw, Nat.add_assoc]
      · have hge : c.n ≤ j * w := Nat.le_of_not_ge hjw
        have hm1 : min c.n (j * w) = c.n := Nat.min_eq_left hge
        have hm2 : min c.n ((j + 1) * w) = c.n := Nat.min_eq_left (Nat.le_trans hge (Nat.mul_le_mul_right w (Nat.le_succ j)))
        simp [hm1, hm2, Nat.sub_eq_zero_of_le hge]
    suffices h : ∀ m, (List.range m).flatMap (fun j =>
        (List.range (min c.n ((j + 1) * w) - min c.n (j * w))).map
          (fun k => (min c.n (j * w) + k, c.base + c.step * (j : Int)))) =
        (List.range (min c.n (m * w))).map (fun i => (i, c.base + c.step * ((i / w : Nat) : Int))) by
      have hm : c.n ≤ ((c.n + w - 1) / w) * w := by
        have hmod : (c.n + w - 1) % w < w := Nat.mod_lt _ hw
        have hdiv : (c.n + w - 1) / w * w + (c.n + w - 1) % w = c.n + w - 1 := by
          have h := Nat.div_add_mod (c.n + w - 1) w
          rw [Nat.mul_comm] at h
          exact h
        omega
      have hmin : min c.n (((c.n + w - 1) / w) * w) = c.n := Nat.min_eq_left hm
      have key := h ((c.n + w - 1) / w)
      rw [hmin] at key
      rw [List.flatMap_map]
      simpa [w, hspan] using key
    intro m
    induction m with
    | zero => simp
    | succ m ih =>
      rw [List.range_succ, List.flatMap_append, ih]
      simp only [List.flatMap_singleton]
      have hminm : min c.n (m * w) ≤ min c.n ((m + 1) * w) := by
        exact Nat.le_min.mpr ⟨Nat.min_le_left _ _, Nat.le_trans (Nat.min_le_right _ _) (Nat.mul_le_mul_right w (Nat.le_succ m))⟩
      have hlen2 : min c.n (m * w) + (min c.n ((m + 1) * w) - min c.n (m * w)) =
          min c.n ((m + 1) * w) := Nat.add_sub_of_le hminm
      have hsplit : List.range (min c.n ((m + 1) * w)) =
          List.range (min c.n (m * w)) ++
            (List.range (min c.n ((m + 1) * w) - min c.n (m * w))).map (· + min c.n (m * w)) := by
        rw [← hlen2, List.range_add]; simp [Nat.add_comm]
      rw [hsplit, List.map_append, List.map_map]
      congr 1
      apply List.map_congr_left
      intro k hk
      have hk' : k < min c.n ((m + 1) * w) - min c.n (m * w) := List.mem_range.mp hk
      have hklt : min c.n (m * w) + k < min c.n ((m + 1) * w) := by omega
      have hi_hi : min c.n (m * w) + k < (m + 1) * w := Nat.lt_of_lt_of_le hklt (Nat.min_le_right _ _)
      have hi_lo : m * w ≤ min c.n (m * w) + k := by
        have hne : min c.n (m * w) ≠ c.n := by
          intro heq; rw [heq] at hk'; have : min c.n ((m + 1) * w) ≤ c.n := Nat.min_le_left _ _; omega
        have hmw : min c.n (m * w) = m * w := by omega
        omega
      have hdiv : (min c.n (m * w) + k) / w = m :=
        @Nat.div_eq_of_lt_le m w (min c.n (m * w) + k) hi_lo hi_hi
      simp only [Function.comp_apply]
      refine Prod.ext ?_ ?_
      · omega
      · simp only; congr 1; norm_cast; rw [Nat.add_comm k (min c.n (m * w)), hdiv]
  rw [hrows, hall]


theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  rw [rows_eq]
  exact allPoints_residueJoin (layout.lanes c) b

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.BucketRuns.Remaining.sound_DeleteProgression c mask

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact TinylambOpt.ResearchProofs.Round2.Task36.sound_AlignedProduct c b hb

end BucketRuns
