import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SparseSample_PredicateRank

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SparseSample

/-! Brute-force sanity check: no counterexample for n < 50, several widths,
    positive/negative/zero steps, and a grid of cut points. -/
#eval do
  let mut bad : Array (Nat × Nat × Nat × Int × Int × Int × Int) := #[]
  for n in List.range 50 do
    for p in [0, 1, 2, 3, 5, 8] do
      for sp in [0, 1, 2, 5, 13] do
        for b in [-5, -1, 0, 2, 7] do
          for s in [-7, -2, 0, 4, 11] do
            for lo in [-8, -1, 0, 3, 9] do
              for hi in [-7, 0, 2, 5, 15] do
                let c : Config := ⟨n, sp, p, b, s, 0, []⟩
                let lhs := (rows layout c).countP
                  (fun (r : Nat × Int) => lo ≤ r.2 && r.2 < hi)
                let rhs := ((layout.lanes c).map (fun a => band a lo hi)).sum
                if lhs != rhs then bad := bad.push (n, p, sp, b, s, lo, hi)
  pure (bad.toList.take 20, bad.size)

/-! ## Generic list helpers -/

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

private theorem map_congr {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_map_le {α : Type} (l : List α) (f g : α → Nat)
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
        sum_map_le xs g f (fun y hy => h y (by simp [hy]))
      have hx : g x ≤ f x := h x (by simp)
      rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
      rw [ih (fun y hy => h y (by simp [hy]))]
      let a := f x
      let b := g x
      let cc := (List.map f xs).sum
      let d := (List.map g xs).sum
      change (a - b) + (cc - d) = (a + cc) - (b + d)
      omega

private theorem sum_ite_decide_nat {α : Type} (l : List α) (P : α → Prop) [DecidablePred P] :
    ((l.map (fun x => if P x then 1 else 0)).sum : Nat) =
      (l.filter (fun x => P x)).length := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : P x <;> simp [h, ih] <;> omega

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem mem_of_mem_filter {α : Type} {l : List α} {p : α → Bool} {a : α}
    (h : a ∈ l.filter p) : a ∈ l := (List.mem_filter.mp h).1

private theorem filter_subset_length {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = true → q x = true) :
    (l.filter p).length ≤ (l.filter q).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x = true
      · have hqx := h x (by simp) hx
        have hi := ih (fun y hy => h y (by simp [hy]))
        have hp : (x :: xs).filter p = x :: (xs.filter p) := by simp [hx]
        have hq : (x :: xs).filter q = x :: (xs.filter q) := by simp [hqx]
        rw [hp, hq, List.length_cons, List.length_cons]
        omega
      · have hx0 : p x = false := by
          cases hpx : p x
          · rfl
          · exfalso; exact hx (by simp [hpx])
        by_cases hqx : q x = true
        · have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = x :: (xs.filter q) := by simp [hqx]
          rw [hp, hq, List.length_cons]
          omega
        · have hqx0 : q x = false := by
            cases hqx' : q x
            · rfl
            · exfalso; exact hqx (by simp [hqx'])
          have hi := ih (fun y hy => h y (by simp [hy]))
          have hp : (x :: xs).filter p = xs.filter p := by simp [hx0]
          have hq : (x :: xs).filter q = xs.filter q := by simp [hqx0]
          rw [hp, hq]
          omega

private theorem range_count_lt (n t : Nat) :
    ((List.range n).filter (fun i => i < t)).length = min n t := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n < t <;> simp [h, ih] <;> omega

private theorem length_filter_not {α : Type} (l : List α) (p : α → Bool) :
    (l.filter (fun x => !p x)).length = l.length - (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
      · have hf : p x = false := by cases h' : p x <;> simp_all
        simp [hf, ih]
        have hle : (List.filter p xs).length ≤ xs.length := List.length_filter_le p xs
        omega

private theorem range_count_ge (n t : Nat) :
    ((List.range n).filter (fun i => t ≤ i)).length = n - min n t := by
  have hnot : (List.range n).filter (fun i => t ≤ i) =
      (List.range n).filter (fun i => !(i < t)) := by
    apply filter_congr_bool
    intro i _
    by_cases h : i < t
    · simp [h]
    · simp [h]
      omega
  rw [hnot, length_filter_not, range_count_lt, List.length_range]

/-! ## Residue-list enumeration -/

private theorem filterMap_ite_eq {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    l.filterMap (fun x => if p x then some (f x) else none) = (l.filter p).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2 * p) : t / p = 1 := by
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
    have hm : n - r + p - 1 = p - 1 := by omega
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by grind
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
    have hm : n % 1 = 0 := by have := Nat.mod_lt n (by omega : 0 < 1); omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]; simp [h]
    · have heq : n % p + 1 = p := by have hs := Nat.mod_lt n hp; omega
      rw [heq, Nat.mod_self]; simp [h]

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]; simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]; simp [h]

private theorem residue_count_succ (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n + 1 - r + p - 1) / p = (n - r + p - 1) / p + if n % p == r then 1 else 0 := by
  rw [mod_step_count (n + 1) p r hp hr, mod_step_count n p r hp hr]
  rw [succ_mod_step n p hp, succ_div_step n p hp]
  by_cases hn : n % p == r
  · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
    have hltold : ¬ r < n % p := by omega
    by_cases hs : r + 1 < p
    · have hltnew : r < r + 1 := by omega
      simp [hnp, hs, hltnew, hltold]
    · have hltnew : ¬ r < 0 := by omega
      simp [hnp, hs, hltnew, hltold]
  · have hltold : r < n % p ∨ n % p < r := by
      by_cases hrlt : r < n % p
      · exact Or.inl hrlt
      · right
        have hs := Nat.mod_lt n hp
        have hne : n % p ≠ r := by
          intro he
          exact hn (by simp [he])
        omega
    by_cases h : n % p + 1 < p
    · rcases hltold with hlt | hgt
      · have hltnew : r < n % p + 1 := by omega
        simp [hn, h, hlt, hltnew]
      · have hltnew : ¬ r < n % p + 1 := by omega
        have hnlt : ¬ r < n % p := by omega
        simp [hn, h, hnlt, hltnew]
    · have heq : n % p + 1 = p := by
        have hs' := Nat.mod_lt n hp
        omega
      have hlt : r < n % p := by omega
      have hltnew : ¬ r < 0 := by omega
      simp [hn, h, hlt, hltnew]

private theorem count_residue_mod (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    ((List.range n).filter (fun i => i % p == r)).length = (n - r + p - 1) / p := by
  induction n with
  | zero =>
      have hsub : 0 - r = 0 := by omega
      rw [hsub, Nat.zero_add, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih,
        residue_count_succ n p r hp hr]
      by_cases h : n % p == r <;> simp [h]

private theorem residue_count_eq_zero_of_ge (N r p : Nat) (hr : N ≤ r) (hp : 0 < p) :
    (N - r + p - 1) / p = 0 := by
  have hsub : N - r = 0 := by omega
  rw [hsub, Nat.zero_add]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      rw [residue_count_eq_zero_of_ge 0 r p (by omega) hp]
      simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih,
        mod_step_count (n + 1) p r hp hr, mod_step_count n p r hp hr,
        succ_mod_step n p hp, succ_div_step n p hp]
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
          · right
            have hs := Nat.mod_lt n hp
            omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

/-! ## Lane points and value bands -/

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + a.step * (i : Int)))

private theorem flatMap_countP (ls : List Lane) (p : Nat × Int → Bool) :
    (ls.flatMap lanePoints).countP p =
      (ls.map (fun a => (lanePoints a).countP p)).sum := by
  induction ls with
  | nil => simp [lanePoints]
  | cons a as ih => simp [List.countP_append, ih]

private theorem lt_ceil_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) < x ↔ i < ((x + d - 1) / d).toNat := by
  let q := (x + d - 1) / d
  constructor
  · intro h
    have hceil : d * ((i : Int) + 1) ≤ x + d - 1 := by
      rw [Int.mul_add]
      omega
    have hq : (i : Int) + 1 ≤ q :=
      (Int.le_ediv_iff_mul_le hd).mpr (by simpa [Int.mul_comm] using hceil)
    change i < q.toNat
    exact (Int.toNat_lt_toNat (n := (i : Int)) (by omega : 0 < q)).mpr (by omega)
  · intro h
    have hiq : (i : Int) < q := Int.lt_of_toNat_lt h
    have hceil0 : ((i : Int) + 1) * d ≤ x + d - 1 :=
      (Int.le_ediv_iff_mul_le hd).mp (by omega : (i : Int) + 1 ≤ q)
    have hceil : d * ((i : Int) + 1) ≤ x + d - 1 := by
      simpa [Int.mul_comm] using hceil0
    rw [Int.mul_add] at hceil
    omega

private theorem gt_floor_iff (d : Int) (hd : 0 < d) (i : Nat) (x : Int) :
    d * (i : Int) > x ↔ ((x / d) + 1).toNat ≤ i := by
  let q := x / d
  constructor
  · intro h
    have h1 : d * q ≤ x := by
      simpa [Int.mul_comm] using Int.ediv_mul_le x (by omega : d ≠ 0)
    have h2 : d * q < d * (i : Int) := by omega
    have h3 : q < (i : Int) := Int.lt_of_mul_lt_mul_left h2 (by omega : 0 ≤ d)
    have hqi : q + 1 ≤ (i : Int) := by omega
    change (q + 1).toNat ≤ i
    rw [Int.toNat_le]
    exact hqi
  · intro h
    have hiq : q + 1 ≤ (i : Int) := (Int.toNat_le).mp h
    have hmul : d * (q + 1) ≤ d * (i : Int) :=
      Int.mul_le_mul_of_nonneg_left hiq (by omega : 0 ≤ d)
    have hlt : x < d * (q + 1) := by
      simpa [Int.mul_comm] using Int.lt_ediv_add_one_mul_self x hd
    omega

private theorem beforeVal_eq_count (a : Lane) (cut : Int) :
    beforeVal a cut = ((List.range a.n).filter
      (fun i => a.base + a.step * ((i : Nat) : Int) < cut)).length := by
  by_cases hz : a.step = 0
  · by_cases h : a.base < cut
    · unfold beforeVal
      rw [hz]
      simp [h]
      have hf : (List.range a.n).filter (fun i => true) = List.range a.n := by
        apply List.filter_eq_self.mpr
        intro i _
        rfl
      rw [hf]
      simp [List.length_range]
    · unfold beforeVal
      rw [hz]
      simp [h]
      have hf : (List.range a.n).filter (fun i => false) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro i _
        simp
      rw [hf]
      simp [List.length_range]
  · by_cases hp : 0 < a.step
    · unfold beforeVal
      rw [if_neg (by simp [hz]), if_pos hp]
      have hf : (List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < cut) =
          (List.range a.n).filter (fun i => i <
            ((cut - a.base + a.step - 1) / a.step).toNat) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := lt_ceil_iff a.step hp i (cut - a.base)
        change a.step * (i : Int) < cut - a.base ↔ _ at hh
        constructor <;> intro h <;> omega
      rw [hf, range_count_lt]
    · have hn : a.step < 0 := by omega
      unfold beforeVal
      rw [if_neg (by simp [hz]), if_neg hp]
      have hd : 0 < -a.step := by omega
      have hf : (List.range a.n).filter (fun i => a.base + a.step * ((i : Nat) : Int) < cut) =
          (List.range a.n).filter (fun i => ((a.base - cut) / (-a.step) + 1).toNat ≤ i) := by
        apply filter_congr_bool
        intro i _
        apply decide_eq_of_iff
        have hh := gt_floor_iff (-a.step) hd i (a.base - cut)
        change (-a.step) * (i : Int) > a.base - cut ↔ _ at hh
        rw [Int.neg_mul] at hh
        omega
      rw [hf, range_count_ge]

private theorem lane_band (a : Lane) (lo hi : Int) :
    band a lo hi = (lanePoints a).countP (fun r => lo ≤ r.2 && r.2 < hi) := by
  rw [countP_eq_sum (lanePoints a) (fun v => lo ≤ v && v < hi)]
  rw [lanePoints]
  simp only [List.map_map, Function.comp_apply]
  change band a lo hi =
    ((List.range a.n).map (fun i =>
      if lo ≤ a.base + a.step * ((i : Nat) : Int) &&
         a.base + a.step * ((i : Nat) : Int) < hi then 1 else 0)).sum
  by_cases hle : lo ≤ hi
  · rw [show (List.range a.n).map (fun i =>
        if lo ≤ a.base + a.step * ((i : Nat) : Int) &&
           a.base + a.step * ((i : Nat) : Int) < hi then 1 else 0) =
        (List.range a.n).map (fun i =>
          (if a.base + a.step * ((i : Nat) : Int) < hi then 1 else 0) -
          (if a.base + a.step * ((i : Nat) : Int) < lo then 1 else 0)) by
      apply map_congr
      intro i _
      by_cases hh : a.base + a.step * ((i : Nat) : Int) < hi <;>
        by_cases hl : a.base + a.step * ((i : Nat) : Int) < lo <;>
        by_cases hlo : lo ≤ a.base + a.step * ((i : Nat) : Int) <;>
        simp [hh, hl, hlo] <;> omega]
    rw [sum_map_sub_nat (h := by
      intro i _
      by_cases hh : a.base + a.step * ((i : Nat) : Int) < hi <;>
        by_cases hl : a.base + a.step * ((i : Nat) : Int) < lo <;> simp [hh, hl] <;> omega),
      sum_ite_decide_nat, sum_ite_decide_nat,
      ← beforeVal_eq_count a hi, ← beforeVal_eq_count a lo]
    rfl
  · have hzmap : (List.range a.n).map (fun i =>
        if lo ≤ a.base + a.step * ((i : Nat) : Int) &&
           a.base + a.step * ((i : Nat) : Int) < hi then 1 else 0) =
        (List.range a.n).map (fun _ => (0 : Nat)) := by
      apply map_congr
      intro i _
      have hlt : hi < lo := by omega
      by_cases hh : a.base + a.step * ((i : Nat) : Int) < hi
      · have hl : a.base + a.step * ((i : Nat) : Int) < lo := by omega
        simp [hh, hl, hle]
      · simp [hh]
    rw [hzmap, sum_map_zero_nat]
    have hmono : beforeVal a hi ≤ beforeVal a lo := by
      rw [beforeVal_eq_count a hi, beforeVal_eq_count a lo]
      apply filter_subset_length
      intro i _ hh
      have hvhi : a.base + a.step * (i : Int) < hi := of_decide_eq_true hh
      have hvlo : a.base + a.step * (i : Int) < lo := by omega
      simp [hvlo]
    unfold band
    omega

/-! ## SparseSample rows are exactly the points of its single lane -/

private theorem rows_lanes_eq (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  let w := width c
  let r := c.split % w
  have hw : 0 < w := by dsimp [w, width]; omega
  have hr : r < w := by dsimp [r]; exact Nat.mod_lt _ hw
  have hrows : rows layout c =
      ((List.range c.n).filter (fun i => i % w == r)).map
        (fun i => (i, c.base + c.step * ((i / w : Nat) : Int))) := by
    unfold rows
    simp only [layout]
    rw [← filterMap_ite_eq]
    apply filterMap_congr'
    intro i _
    by_cases h : i % w == r <;> simp [layout, w, r, h]
  rw [hrows, range_filter_mod_eq c.n w r hw hr]
  simp only [List.map_map, Function.comp_apply]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  unfold lanePoints lane
  dsimp
  apply map_congr
  intro q _
  have hdiv : (r + q * w) / w = q := by
    rw [Nat.add_mul_div_right r q hw, Nat.div_eq_of_lt hr]
    simp
  apply Prod.ext
  · rfl
  · simp [hdiv, Int.natCast_add, Int.natCast_mul, Int.mul_add, Int.mul_comm, Int.mul_assoc]
    have hcast : (((r + q * w) / w : Nat) : Int) = (q : Int) := by exact_mod_cast hdiv
    simpa [Int.natCast_add, Int.natCast_mul] using congrArg (fun z => c.step * z) hcast

theorem sound_SparseSample_PredicateRank : SparseSample.PredicateRank.Goal := by
  intro c lo hi
  rw [rows_lanes_eq, flatMap_countP]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a _
  exact (lane_band a lo hi).symm

end TinylambOpt.ResearchProofs.Atlas.Goal_SparseSample_PredicateRank
