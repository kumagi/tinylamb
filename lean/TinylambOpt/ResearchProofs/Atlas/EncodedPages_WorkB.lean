import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkB

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic helpers -/

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem map_filterMap_some {α β : Type} (l : List α) (f : α → β) :
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

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun i => (lay.read c i).map (i, ·)) =
      (List.range c.n).filterMap (fun i => some (i, f i)) from by
    apply filterMap_congr'
    intro i hi
    have hi' := List.mem_range.mp hi
    simp [h i hi']]
  exact map_filterMap_some _ _

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

private theorem map_snd_append {α β : Type} (l₁ l₂ : List (α × β)) :
    (l₁ ++ l₂).map Prod.snd = l₁.map Prod.snd ++ l₂.map Prod.snd := by
  induction l₁ <;> simp [*]

private theorem sum_flatMap_snd {α : Type} (ls : List α) (f : α → List (Nat × Int)) :
    ((ls.flatMap f).map Prod.snd).sum = (ls.map (fun x => ((f x).map Prod.snd).sum)).sum := by
  induction ls with
  | nil => simp
  | cons l ls ih =>
      simp [List.sum_cons, ih]

private theorem map_flatMap {α β γ : Type} (ls : List α) (f : α → List β) (g : β → γ) :
    (ls.flatMap f).map g = ls.flatMap (fun x => (f x).map g) := by
  induction ls <;> simp [*, List.map_append]

private theorem sum_map_flatMap_int {α β : Type} (ls : List α) (f : α → List β) (g : β → Int) :
    ((ls.flatMap f).map g).sum = (ls.map (fun x => ((f x).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih =>
      simp [List.sum_cons, ih]

private theorem sum_map_flatMap_nat {α β : Type} (ls : List α) (f : α → List β) (g : β → Nat) :
    ((ls.flatMap f).map g).sum = (ls.map (fun x => ((f x).map g).sum)).sum := by
  induction ls with
  | nil => simp
  | cons x xs ih =>
      simp [List.sum_cons, ih]

private theorem filter_flatMap {α β : Type} (ls : List α) (f : α → List β) (p : β → Bool) :
    (ls.flatMap f).filter p = ls.flatMap (fun x => (f x).filter p) := by
  induction ls with
  | nil => rfl
  | cons x xs ih => simp [List.filter_append, ih]

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
    have hmul2 : k * (k - 1) = k * k - k := by
      rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

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
      rw [partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]
      simp

private theorem partialSum_step0 (a : Lane) (h : a.step = 0) (k : Nat) :
    partialSum a k = (k : Int) * a.base := by
  rw [partialSum, h]
  simp [tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]

/-! ## Lane/step-0 helpers -/

private theorem map_lanePoints_step0_snd (a : Lane) (hs : a.step = 0) :
    (lanePoints a).map Prod.snd = (List.range a.n).map (fun _ => a.base) := by
  unfold lanePoints
  rw [List.map_map]
  apply List.map_congr_left
  intro i _
  simp [hs]

private theorem sum_map_const_nat {α : Type} (l : List α) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_mul, Nat.add_comm]

private theorem sum_map_const_int {α : Type} (l : List α) (c : Int) :
    (l.map (fun _ => c)).sum = (l.length : Int) * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_mul, Int.add_comm]

private theorem countP_const_true {α : Type} (l : List α) : l.countP (fun _ => true) = l.length := by
  induction l <;> simp [*]

private theorem countP_const_false {α : Type} (l : List α) : l.countP (fun _ => false) = 0 := by
  induction l <;> simp [*]

/-! ## Progression / contains helpers -/

private theorem lastVal_zero_step (lo : Int) (n : Nat) :
    lastVal ⟨lo, 0, n⟩ = lo := by
  unfold lastVal
  simp

private theorem contains_step0_progression (b : Progression) (v : Int) (hb : b.step = 0) :
    contains b v = (0 < b.n && b.lo == v) := by
  unfold contains lastVal
  simp only [hb, beq_self_eq_true, Bool.true_or, Bool.and_true, ite_true]
  by_cases hn : 0 < b.n
  · simp only [hn, decide_true, Bool.true_and]
    by_cases hlo : b.lo ≤ v
    · by_cases hhi : v ≤ b.lo
      · have heq : b.lo = v := by omega
        simp [hlo, hhi, heq]
      · have hne : b.lo ≠ v := by omega
        simp [hlo, hhi, hne]
    · have hne : b.lo ≠ v := by omega
      simp [hlo, hne]
  · simp [hn]

/-! ## intersection infrastructure omitted (not needed for step-0 ResidueJoin) -/

private theorem list_map_append {α β : Type} (f : α → β) (l₁ l₂ : List α) :
    (l₁ ++ l₂).map f = l₁.map f ++ l₂.map f := by
  induction l₁ <;> simp [*]

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

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l <;> simp [*]

private theorem sum_update_nat : ∀ (p r0 : Nat), r0 < p → ∀ (s : Nat → Nat) (v : Nat),
    ((List.range p).map (fun r => if r == r0 then v + s r else s r)).sum =
      v + ((List.range p).map s).sum := by
  intro p
  induction p with
  | zero => intro r0 h; omega
  | succ p ih =>
      intro r0 hr0 s v
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

private theorem striped_sum_partition_nat (p : Nat) (hp : 0 < p) (F : Nat → Nat) :
    ∀ xs : List Nat, (xs.map F).sum =
      ((List.range p).map (fun r => ((xs.filter (fun i => i % p == r)).map F).sum)).sum := by
  intro xs
  induction xs with
  | nil =>
      rw [List.map_nil, List.sum_nil]
      exact (sum_map_zero_nat (List.range p)).symm
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      rw [← sum_update_nat p (x % p) (Nat.mod_lt x hp)]
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

private theorem striped_sum_nat (p : Nat) (hp : 0 < p) (n : Nat) (make : Nat → Int × Int)
    (F : Int → Nat) :
    ((List.range n).map (fun i => F (stripedValue p make i))).sum =
      ((List.range p).map (fun r =>
        let (z, d) := make r
        ((List.range ((n - r + p - 1) / p)).map (fun q : Nat => F (z + d*(q:Int)))).sum)).sum := by
  rw [striped_sum_partition_nat p hp (fun i => F (stripedValue p make i)) (List.range n)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := List.mem_range.mp hr
  rw [range_filter_mod_eq n p r hp hrp]
  simp only [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro q hq
  change F (stripedValue p make (r + q * p)) = F ((make r).1 + (make r).2 * (q : Int))
  rw [stripedValue_residue p hp make r q hrp]

private theorem sum_range_const_nat (m v : Nat) :
    ((List.range m).map (fun _ => v)).sum = m * v := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.map_append]
      rw [List.sum_append]
      simp [ih, Nat.succ_mul]

private theorem sum_range_ite_nat (m : Nat) (b : Bool) (v : Nat) :
    ((List.range m).map (fun _ => if b then v else 0)).sum = if b then m * v else 0 := by
  by_cases h : b <;> simp [h, sum_range_const_nat, sum_map_zero_nat]

private theorem countP_eq_sum (l : List (Nat × Int)) (f : Int → Bool) :
    l.countP (fun x => f x.2) = ((l.map (fun x => if f x.2 then 1 else 0)).sum : Nat) := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : f x.2 <;> simp [h, ih] <;> omega

private theorem sum_eq_single (p r : Nat) (hr : r < p) (f : Nat → Nat) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  have hconst : ∀ x, (if x == r then f x else 0) = if x == r then f r else 0 := by
    intro x
    by_cases h : x == r
    · have h' : x = r := Nat.eq_of_beq_eq_true h
      simp [h, h']
    · simp [h]
  rw [show (List.range p).map (fun x => if x == r then f x else 0) =
      (List.range p).map (fun x => if x == r then f r else 0) from
    List.map_congr_left (fun x hx => hconst x)]
  rw [show (List.range p).map (fun x => if x == r then f r else 0) =
      (List.range p).map (fun x => if x == r then f r + 0 else 0) by
    apply List.map_congr_left
    intro x hx
    by_cases h : x == r <;> simp [h]]
  rw [sum_update_nat p r hr (fun _ : Nat => 0) (f r), sum_map_zero_nat]
  simp

/-! ## countP / joinCount for step-0 lanes -/

private theorem countP_range_eq_ap (base step val : Int) (n : Nat) (hstep : step ≠ 0) :
    (List.range n).countP (fun (i : Nat) => base + step * (i : Int) == val) =
      if (val - base) % step == 0 then
        let k := (val - base) / step
        if 0 ≤ k && k < (n : Int) then 1 else 0
      else 0 := by
  by_cases hmod : (val - base) % step = 0
  · have hmodb : ((val - base) % step == 0) = true := beq_iff_eq.mpr hmod
    rw [hmodb]
    simp
    have hk : step * ((val - base) / step) = val - base := by
      have h := Int.ediv_mul_cancel (Int.dvd_of_emod_eq_zero hmod)
      rw [Int.mul_comm] at h; exact h
    let k := (val - base) / step
    by_cases hkbound : 0 ≤ k ∧ k < (n : Int)
    · have hkboundb : (0 ≤ k && k < (n : Int)) = true := by
        simp [hkbound.1, hkbound.2]
      rw [if_pos hkbound]
      have hknat : k.toNat < n := by omega
      have hkcast : (k.toNat : Int) = k := Int.toNat_of_nonneg hkbound.1
      have h1 : (List.range k.toNat).countP (fun (i : Nat) => base + step * (i : Int) == val) = 0 := by
        apply List.countP_eq_zero.mpr
        intro i hi
        have hil : i < k.toNat := List.mem_range.mp hi
        intro he
        have heq : base + step * (i : Int) = val := beq_iff_eq.mp he
        have h1 : step * (i : Int) = val - base := by omega
        have h2 : (i : Int) = k := by
          have h2' : (step * (i : Int)) / step = k := by rw [h1]
          rw [Int.mul_ediv_cancel_left _ hstep] at h2'; exact h2'
        have : (i : Int) = (k.toNat : Int) := by rw [h2, hkcast]
        have : i = k.toNat := Int.ofNat.inj this
        omega
      have h2 : (base + step * (k.toNat : Int) == val) = true := by
        have : base + step * max k 0 = val := by
          have hmax : max k 0 = k := Int.max_eq_left hkbound.1
          rw [hmax, hk]
          omega
        simp [this]
      have h3 : ((List.range n).drop (k.toNat + 1)).countP (fun (i : Nat) => base + step * (i : Int) == val) = 0 := by
        apply List.countP_eq_zero.mpr
        intro i hi
        have hmemd := List.mem_drop_iff_getElem.mp hi
        rcases hmemd with ⟨j, hm, heq⟩
        have hi' : i = k.toNat + 1 + j := by
          have hrange : (List.range n)[k.toNat + 1 + j] = k.toNat + 1 + j := by simp
          rw [hrange] at heq
          exact heq.symm
        intro he
        have heqval : base + step * (i : Int) = val := beq_iff_eq.mp he
        have h1 : step * (i : Int) = val - base := by omega
        have h2 : (i : Int) = k := by
          have h2' : (step * (i : Int)) / step = k := by rw [h1]
          rw [Int.mul_ediv_cancel_left _ hstep] at h2'; exact h2'
        have : (i : Int) = k := h2
        have : i ≥ k.toNat + 1 := by omega
        omega
      have hsplitL : List.range n = List.range k.toNat ++ [k.toNat] ++ (List.range n).drop (k.toNat + 1) := by
        conv => lhs; rw [← List.take_append_drop (k.toNat + 1) (List.range n)]
        rw [List.take_range]
        have : min (k.toNat + 1) n = k.toNat + 1 := by omega
        rw [this]
        have h1 : List.range n = List.range (k.toNat + 1) ++ (List.range n).drop (k.toNat + 1) := by
          calc List.range n
              = List.take (k.toNat + 1) (List.range n) ++ (List.range n).drop (k.toNat + 1) :=
                (List.take_append_drop (k.toNat + 1) (List.range n)).symm
            _ = List.range (min (k.toNat + 1) n) ++ (List.range n).drop (k.toNat + 1) := by
                rw [List.take_range]
            _ = List.range (k.toNat + 1) ++ (List.range n).drop (k.toNat + 1) := by
                have : min (k.toNat + 1) n = k.toNat + 1 := by omega
                rw [this]
        have hsplitL : List.range n = List.range k.toNat ++ [k.toNat] ++ (List.range n).drop (k.toNat + 1) := by
          conv => lhs; rw [h1]
          conv => lhs; arg 1; rw [List.range_succ (n := k.toNat)]
      rw [hsplitL, List.countP_append, List.countP_append]
      rw [h1, h3]
      simp [h2]
    · have hkboundb : (0 ≤ k && k < (n : Int)) = false := by
        by_cases h1 : 0 ≤ k <;> by_cases h2 : k < (n : Int) <;> simp [h1, h2]; omega
      rw [hkboundb]
      simp
      apply List.countP_eq_zero.mpr
      intro i hi
      intro he
      have heq : base + step * (i : Int) = val := beq_iff_eq.mp he
      have h1 : step * (i : Int) = val - base := by omega
      have h2 : (i : Int) = k := by
        have h2' : (step * (i : Int)) / step = k := by rw [h1]
        rw [Int.mul_ediv_cancel_left _ hstep] at h2'; exact h2'
      have : i ≥ 0 := by omega
      omega
  · have hmodb : ((val - base) % step == 0) = false := by
      cases h : ((val - base) % step == 0) with
      | false => rfl
      | true =>
          have : (val - base) % step = 0 := eq_of_beq h
          exact (hmod this).elim
    rw [hmodb]
    simp
    apply List.countP_eq_zero.mpr
    intro i hi
    intro he
    have heq : base + step * (i : Int) = val := beq_iff_eq.mp he
    apply hmod
    have h1 : val - base = step * (i : Int) := by omega
    rw [h1, Int.mul_emod_right]

private theorem intersection_step0_left (a b : Progression) (ha : a.step = 0) :
    intersection a b = if a.n == 0 || b.n == 0 then ⟨0,1,0⟩
      else ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
  unfold intersection
  simp [ha]

private theorem joinCount_step0_a (a b : Lane) (ha : a.step = 0) :
    joinCount a b = (if contains (values b) a.base then 1 else 0) * a.n * (if b.step == 0 then b.n else 1) := by
  unfold joinCount
  have hvs : (values a).step = 0 := by simp [values, ha]
  have hvlo : (values a).lo = a.base := by simp [values, ha]
  have hvn : (values a).n = a.n := by simp [values]
  rw [intersection_step0_left (values a) (values b) hvs]
  simp only [hvn, hvlo, ha, ↓reduceIte]
  by_cases han : a.n == 0
  · simp [han]
  · by_cases hbn : b.n == 0
    · simp [han, hbn, contains]
    · simp [han, hbn]

private theorem countP_b_step0 (b : Lane) (v : Int) (hb : b.step = 0) :
    (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
      if v == b.base then b.n else 0 := by
  simp only [hb, Int.mul_zero, Int.add_zero]
  by_cases hv : v == b.base
  · simp [hv, countP_const_true]
  · simp [hv, countP_const_false]

private theorem contains_values_b_step0 (b : Lane) (v : Int) (hb : b.step = 0) :
    contains (values b) v = (0 < b.n && v == b.base) := by
  have hvb : (values b).step = 0 := by simp [values, hb]
  have hvlo : (values b).lo = b.base := by simp [values, hb]
  have hvn : (values b).n = b.n := by simp [values]
  rw [contains_step0_progression (values b) v hvb, hvlo, hvn]
  by_cases hn : 0 < b.n <;> by_cases he : v == b.base <;> simp [hn, he]
  · have hv : v = b.base := beq_iff_eq.mp he
    have h : b.base == v := beq_iff_eq.mpr hv.symm
    simp [h]
  · have hne : b.base ≠ v := by intro h; exact he (by simp [h])
    simp [hne]

private theorem contains_values_b_nonzero (b : Lane) (v : Int) (hb : b.step ≠ 0) :
    contains (values b) v =
      (0 < b.n && (v - b.base) % b.step == 0 &&
        0 ≤ (v - b.base) / b.step && (v - b.base) / b.step < (b.n : Int)) := by
  unfold contains values lastVal
  have hbs : b.step.natAbs ≠ 0 := by intro h; apply hb; exact Int.natAbs_eq_zero.mp h
  simp only [hbs, beq_eq_false_iff_ne, ne_eq, not_false_eq_true, Bool.false_or, Bool.and_eq_true,
    decide_eq_true_eq]
  by_cases hbn : 0 < b.n
  · simp only [hbn, true_and]
    by_cases hsign : b.step < 0
    · simp only [hsign, ↓reduceIte]
      have hnatAbs : (b.step.natAbs : Int) = -b.step := by
        have : b.step.natAbs = (-b.step).toNat := by omega
        rw [this]; exact Int.toNat_of_nonneg (by omega)
      constructor
      · intro ⟨hlo, hhi, hmod⟩
        rw [hnatAbs] at hmod
        have hmod' : (v - b.base) % b.step = 0 := by
          have hdvd : -(b.step) ∣ (v - (b.base + ((b.n - 1 : Nat) : Int) * b.step)) := Int.dvd_of_emod_eq_zero hmod
          rcases hdvd with ⟨k, hk⟩
          have hveq2 : v - b.base = b.step * (((b.n - 1 : Nat) : Int) - k) := by
            have hveq : v = b.base + ((b.n - 1 : Nat) : Int) * b.step + (-b.step) * k := by omega
            rw [hveq]; omega
          exact Int.emod_eq_zero_of_dvd ⟨_, hveq2⟩
        refine ⟨hmod', ?_, ?_⟩
        · have hlast : b.base + ((b.n - 1 : Nat) : Int) * b.step + (((b.n - 1 : Nat) : Int) * (b.step.natAbs : Int)) = b.base := by
            rw [hnatAbs]; omega
          have hle_last : v ≤ b.base := by grind
          exact Int.ediv_nonneg_of_nonpos_of_nonpos (by omega) (by omega)
        · have hdvd : b.step ∣ (v - b.base) := Int.dvd_of_emod_eq_zero hmod'
          have hkval := Int.ediv_mul_cancel hdvd
          have h1 : ((b.n - 1 : Nat) : Int) * b.step ≤ v - b.base := by grind
          have hkval' : b.step * ((v - b.base) / b.step) = v - b.base := by rw [Int.mul_comm] at hkval; exact hkval
          have hkle : (v - b.base) / b.step ≤ ((b.n - 1 : Nat) : Int) := by
            grind
          omega
      · intro ⟨hmod, hlo_k, hhi_k⟩
        have hdvd : b.step ∣ (v - b.base) := Int.dvd_of_emod_eq_zero hmod
        have hkval := Int.ediv_mul_cancel hdvd
        have hveq : v = b.base + b.step * ((v - b.base) / b.step) := by
          have := hkval; rw [Int.mul_comm] at this; omega
        refine ⟨?_, ?_, ?_⟩
        · rw [hveq]
          have hk_le : (v - b.base) / b.step ≤ ((b.n - 1 : Nat) : Int) := by omega
          grind
        · rw [hveq]
          grind
        · rw [hnatAbs, hveq]
          have hk := (v - b.base) / b.step
          have heq : (b.base + b.step * hk - (b.base + ((b.n - 1 : Nat) : Int) * b.step)) = (-b.step) * (((b.n - 1 : Nat) : Int) - hk) := by omega
          rw [heq, Int.mul_emod_right]
    · have hpos : 0 < b.step := by omega
      simp only [hsign, ↓reduceIte]
      have hnatAbs : (b.step.natAbs : Int) = b.step := Int.ofNat_natAbs_of_nonneg (by omega)
      constructor
      · intro ⟨hlo, hhi, hmod⟩
        rw [hnatAbs] at hmod
        refine ⟨hmod, Int.ediv_nonneg (by omega) (by omega), ?_⟩
        have hdvd : b.step ∣ (v - b.base) := Int.dvd_of_emod_eq_zero hmod
        have hkval := Int.ediv_mul_cancel hdvd
        have hveq : v = b.base + b.step * ((v - b.base) / b.step) := by rw [Int.mul_comm] at hkval; omega
        have h1 : b.step * ((v - b.base) / b.step) ≤ b.step * ((b.n - 1 : Nat) : Int) := by grind
        have h2 : (v - b.base) / b.step ≤ ((b.n - 1 : Nat) : Int) := by grind
        omega
      · intro ⟨hmod, hlo_k, hhi_k⟩
        have hdvd : b.step ∣ (v - b.base) := Int.dvd_of_emod_eq_zero hmod
        have hkval := Int.ediv_mul_cancel hdvd
        have hveq : v = b.base + b.step * ((v - b.base) / b.step) := by rw [Int.mul_comm] at hkval; omega
        refine ⟨?_, ?_, ?_⟩
        · rw [hveq]; grind
        · rw [hveq, hnatAbs]; grind
        · rw [hnatAbs]; exact hmod
  · simp [hbn]

private theorem countP_b_eq_contains (b : Lane) (v : Int) :
    (List.range b.n).countP (fun (i : Nat) => v == b.base + b.step * (i : Int)) =
      (if contains (values b) v then 1 else 0) * (if b.step == 0 then b.n else 1) := by
  by_cases hb : b.step = 0
  · rw [countP_b_step0 b v hb, contains_values_b_step0 b v hb]
    simp only [hb, ↓reduceIte]
    by_cases hbn : b.n = 0
    · subst hbn; simp
    · have hbn' : 0 < b.n := Nat.pos_of_ne_zero hbn
      by_cases hv : v = b.base
      · simp [hv, hbn']
      · have hvb : (v == b.base) = false := by simp [hv]
        have hcond : (0 < b.n && v == b.base) = false := by simp [hvb]
        simp [hvb, hcond]
  · have hbne : (b.step == 0) = false := beq_eq_false_iff_ne.mpr hb
    simp only [hbne, ↓reduceIte, Nat.mul_one]
    rw [contains_values_b_nonzero b v hb]
    have hcount := countP_range_eq_ap b.base b.step v b.n hb
    rw [hcount]
    by_cases hmod : (v - b.base) % b.step = 0
    · have hmodb : ((v - b.base) % b.step == 0) = true := beq_iff_eq.mpr hmod
      simp only [hmodb, ↓reduceIte, Bool.true_and]
      by_cases hk : 0 ≤ (v - b.base) / b.step ∧ (v - b.base) / b.step < (b.n : Int)
      · have hbn : 0 < b.n := by omega
        simp [hk, hbn]
      · by_cases hbn : 0 < b.n
        · simp [hk, hbn]
        · have hbn0 : b.n = 0 := by omega
          simp [hbn0]
    · have hmodb : ((v - b.base) % b.step == 0) = false := by
        cases h : ((v - b.base) % b.step == 0) with
        | false => rfl
        | true =>
            have : (v - b.base) % b.step = 0 := eq_of_beq h
            exact (hmod this).elim
      simp only [hmodb, Bool.false_and, ↓reduceIte]
      by_cases hbn : 0 < b.n <;> simp [hbn, hmod]

private theorem residueJoin_lane_step0 (a b : Lane) (ha : a.step = 0) :
    ((lanePoints a).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    joinCount a b := by
  rw [joinCount_step0_a a b ha]
  unfold lanePoints
  rw [List.map_map]
  simp only [show (fun i : Nat => (List.range b.n).countP
      (fun j => ((a.start + i * a.stride, a.base + (i : Int) * a.step).2) == b.base + b.step * (j : Int))) =
    (fun i : Nat => (List.range b.n).countP
      (fun j => a.base == b.base + b.step * (j : Int))) from by
    funext i; simp [ha]]
  rw [sum_map_const_nat (List.range a.n) ((List.range b.n).countP (fun j => a.base == b.base + b.step * (j : Int)))]
  simp only [List.length_range]
  rw [countP_b_eq_contains b a.base]
  simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]

/-! ## WrappingSequence -/

namespace WrappingSequence

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private theorem rows_eq (c : Config) :
    rows layout c = (List.range c.n).map (fun i => (i, c.base + c.step * ((i % width c : Nat) : Int))) := by
  apply rows_range_map
  intro i hi
  simp [layout]

private def make (c : Config) : Nat → Int × Int := fun r => (c.base + c.step * (r : Int), 0)

private theorem stripedValue_eq (c : Config) (i : Nat) :
    stripedValue (width c) (make c) i = c.base + c.step * ((i % width c : Nat) : Int) := by
  unfold stripedValue make
  simp

private theorem lanes_eq (c : Config) :
    layout.lanes c = (List.range (min (width c) c.n)).map (fun r =>
      lane c.n r (width c) (c.base + c.step * (r : Int)) 0) := by
  simp [layout, striped, make]

private theorem lane_r_step0 (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.step * (r : Int)) 0).step = 0 := by
  simp [lane]

private theorem lane_r_base (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.step * (r : Int)) 0).base = c.base + c.step * (r : Int) := by
  simp [lane]

private theorem lane_r_n (c : Config) (r : Nat) :
    (lane c.n r (width c) (c.base + c.step * (r : Int)) 0).n = (c.n - r + width c - 1) / width c := by
  simp [lane]

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  rw [rows_eq]
  simp only [List.map_map]
  -- LHS: sum over i < c.n of countP_b(value i)
  change ((List.range c.n).map (fun i =>
    (List.range b.n).countP (fun j => c.base + c.step * ((i % p : Nat) : Int) == b.base + b.step * (j : Int)))).sum =
    ((layout.lanes c).map (joinCount · b)).sum
  -- rewrite value as stripedValue
  have hval : ∀ i, c.base + c.step * ((i % p : Nat) : Int) = stripedValue p (make c) i := by
    intro i; rw [stripedValue_eq]
  simp only [hval]
  rw [striped_sum_nat p hp c.n (make c) (fun v => (List.range b.n).countP (fun (j : Nat) => v == b.base + b.step * (j : Int)))]
  -- RHS: expand lanes
  rw [lanes_eq]
  simp only [List.map_map]
  -- show per-residue equality
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrp : r < p := by
    have := List.mem_range.mp hr
    omega
  simp only [make]
  simp only [Int.zero_mul, Int.add_zero]
  -- LHS term: sum of constant countP_b over (n-r+p-1)/p elements
  rw [sum_map_const_nat]
  simp only [List.length_range]
  -- RHS term: joinCount of lane r
  have hstep0 : (lane c.n r p (c.base + c.step * (r : Int)) 0).step = 0 := by simp [lane]
  have hbase : (lane c.n r p (c.base + c.step * (r : Int)) 0).base = c.base + c.step * (r : Int) := by simp [lane]
  have hn : (lane c.n r p (c.base + c.step * (r : Int)) 0).n = (c.n - r + p - 1) / p := by simp [lane]
  rw [joinCount_step0_a _ b hstep0, hbase, hn]
  rw [countP_b_eq_contains b (c.base + c.step * (r : Int))]
  grind
