import TinylambOpt.ResearchGoals.Atlas.EncodedPages

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryDrift.Remaining

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option linter.unusedVariables false

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

private theorem list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

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

private theorem rows_drift (c : Config) (hp : 0 < c.dict.length) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, stripedValue c.dict.length (fun r => (c.dict[r]?.getD 0, c.step)) i)) := by
  unfold rows
  simp only [layout]
  rw [range_filterMap_all c.n]
  intro i hi
  have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt _ hp
  have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
    rw [List.getElem?_eq_some_iff]
    exact ⟨hlt, rfl⟩
  simp [hget, stripedValue]

private theorem rows_drift_empty (c : Config) (h : c.dict = []) : rows layout c = [] := by
  unfold rows
  simp [layout, h]

/-! ## Arithmetic progressions and their intersection -/

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
  unfold contains lastVal
  simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc, Int.mul_zero, Int.add_zero]
  constructor
  · intro h
    rcases h with ⟨hn, hlo, hhi, _⟩
    omega
  · rintro ⟨hn, rfl⟩
    simp [hn]

private theorem contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold progVal at hval
  have hk_le : k ≤ a.n - 1 := by omega
  have hmul : (k : Int) * (a.step : Int) ≤ ((a.n - 1 : Nat) : Int) * (a.step : Int) := by
    exact_mod_cast (Nat.mul_le_mul_right a.step hk_le)
  unfold lastVal
  constructor
  · rw [hval]
    exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (by omega))
  · rw [hval]
    omega

private theorem contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (contains_iff_mem a v).mp h
  unfold progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  grind

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (contains_iff_mem b v).mp hb
  unfold progVal at hi hj
  have hgR : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgL : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) :=
    Int.dvd_mul_of_dvd_left hgR
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) :=
    Int.dvd_mul_of_dvd_left hgL
  have hi' : (i : Int) * (a.step : Int) = v - a.lo := by rw [hi]; grind
  have hj' : (j : Int) * (b.step : Int) = v - b.lo := by rw [hj]; grind
  have heq : (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) = b.lo - a.lo := by
    calc
      (a.step : Int) * (i : Int) - (b.step : Int) * (j : Int) =
          (i : Int) * (a.step : Int) - (j : Int) * (b.step : Int) := by grind
      _ = (v - a.lo) - (v - b.lo) := by rw [hi', hj']
      _ = b.lo - a.lo := by grind
  rw [← heq]
  exact Int.dvd_sub h2 h1

private theorem dvd_of_eq {m x y : Int} (h : m ∣ x) (heq : x = y) : m ∣ y := by
  rw [← heq]
  exact h

private theorem period_eq_lcm (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) :
    as.lcm bs = (as / as.gcd bs) * bs := by
  let g := as.gcd bs
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  apply Nat.eq_of_mul_eq_mul_left hg
  rw [Nat.gcd_mul_lcm]
  dsimp [g]
  have hd : g ∣ as := Nat.gcd_dvd_left as bs
  have heq : g * ((as / g) * bs) = as * bs := by
    calc
      g * ((as / g) * bs) = (g * (as / g)) * bs := by grind
      _ = as * bs := by rw [Nat.mul_div_cancel' hd]
  rw [heq]

private theorem period_dvd_of_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (x : Int)
    (hxa : (as : Int) ∣ x) (hxb : (bs : Int) ∣ x) :
    (((as / as.gcd bs) * bs : Nat) : Int) ∣ x := by
  have hna : as ∣ x.natAbs := (Int.natAbs_dvd_natAbs.mpr (by simpa using hxa))
  have hnb : bs ∣ x.natAbs := (Int.natAbs_dvd_natAbs.mpr (by simpa using hxb))
  have hl : as.lcm bs ∣ x.natAbs := Nat.lcm_dvd hna hnb
  rw [period_eq_lcm as bs ha hb] at hl
  have hres : (((as / as.gcd bs) * bs : Nat) : Int) ∣ x :=
    (Int.natAbs_dvd_natAbs
      (a := (((as / as.gcd bs) * bs : Nat) : Int)) (b := x)).mp hl
  simpa using hres

private theorem intersection_contains (a b : Progression) (v : Int)
    (hanp : 0 < a.n) (hbnp : 0 < b.n) (hasp : 0 < a.step) (hbsp : 0 < b.step) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  let g := a.step.gcd b.step
  let gap := b.lo - a.lo
  let period := (a.step / g) * b.step
  let unit := (bezout (a.step / g) (b.step / g)).1
  let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
  let low := max a.lo b.lo
  let first := low + (root - low) % (period : Int)
  let high := min (lastVal a) (lastVal b)
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step hasp
  have hApos : 0 < a.step / g := Nat.div_gcd_pos_of_pos_left b.step hasp
  have hPpos : 0 < period := by
    dsimp [period]
    exact Nat.mul_pos hApos hbsp
  have hp : (0 : Int) < (period : Int) := by exact_mod_cast hPpos
  have hgapdef : gap = b.lo - a.lo := rfl
  have han0 : a.n ≠ 0 := by omega
  have hbn0 : b.n ≠ 0 := by omega
  have has0 : a.step ≠ 0 := by omega
  have hbs0 : b.step ≠ 0 := by omega
  by_cases hgapmod : gap % (g : Int) = 0
  · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
    have hroot := root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
    have hfirst : first = low + (root - low) % (period : Int) := rfl
    have hfroot := first_root_dvd low root first period hp hfirst
    have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega)
    have hflow : low ≤ first := by dsimp [first]; omega
    have hpa : (a.step : Int) ∣ (period : Int) := by
      have hdvd : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
      have hdvd2 : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
      have hnat : a.step * (b.step / g) = (a.step / g) * b.step := by
        calc
          a.step * (b.step / g) = (g * (a.step / g)) * (b.step / g) := by
            rw [Nat.mul_div_cancel' hdvd]
          _ = (a.step / g) * (g * (b.step / g)) := by grind
          _ = (a.step / g) * b.step := by rw [Nat.mul_div_cancel' hdvd2]
      refine ⟨((b.step / g : Nat) : Int), ?_⟩
      dsimp [period]
      exact_mod_cast hnat.symm
    have hpb : (b.step : Int) ∣ (period : Int) := by
      have hdvd : g ∣ a.step := Nat.gcd_dvd_left a.step b.step
      have hdvd2 : g ∣ b.step := Nat.gcd_dvd_right a.step b.step
      have hnat : b.step * (a.step / g) = (a.step / g) * b.step := by grind
      refine ⟨((a.step / g : Nat) : Int), ?_⟩
      dsimp [period]
      exact_mod_cast hnat.symm
    unfold intersection
    rw [ite_eq_right (by simp [han0, hbn0])]
    rw [ite_eq_right (by simp [has0])]
    rw [ite_eq_right (by simp [hbs0])]
    rw [ite_eq_right (by change ¬ ((gap % (g : Int) != 0) = true); simp [hgapmod])]
    change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
    have hspan : contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔
        first ≤ v ∧ v ≤ high ∧ (period : Int) ∣ v - first := by
      simpa using (span_contains_iff first (period : Int) high v hp)
    rw [hspan]
    constructor
    · rintro ⟨hfirstx, hxhigh, hxfirst⟩
      have hxroot : (period : Int) ∣ v - root := by
        have h := Int.dvd_add hxfirst hfroot
        exact dvd_of_eq h (by grind)
      have hxrootA : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
      have hxrootB : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
      have hxalo : (a.step : Int) ∣ v - a.lo := by
        have h := Int.dvd_add hxrootA hroot.1
        exact dvd_of_eq h (by grind)
      have hxblo : (b.step : Int) ∣ v - b.lo := by
        have h := Int.dvd_add hxrootB hroot.2
        exact dvd_of_eq h (by grind)
      constructor
      · exact mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
      · exact mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
    · rintro ⟨ha, hb⟩
      have hab := contains_bounds a v ha
      have hbb := contains_bounds b v hb
      have hxalo := contains_dvd a v ha
      have hxblo := contains_dvd b v hb
      have hxra : (a.step : Int) ∣ v - root := by
        have h := Int.dvd_sub hxalo hroot.1
        exact dvd_of_eq h (by grind)
      have hxrb : (b.step : Int) ∣ v - root := by
        have h := Int.dvd_sub hxblo hroot.2
        exact dvd_of_eq h (by grind)
      have hxroot : (period : Int) ∣ v - root := by
        simpa [period] using period_dvd_of_dvd a.step b.step hasp hbsp (v - root) hxra hxrb
      have hxfirst : (period : Int) ∣ v - first := by
        have h := Int.dvd_sub hxroot hfroot
        exact dvd_of_eq h (by grind)
      have hba := contains_bounds a v ha
      have hbb := contains_bounds b v hb
      have hlowv : low ≤ v := by
        dsimp [low]
        omega
      have hhighv : v ≤ high := by
        dsimp [high]
        omega
      exact ⟨first_le_of_congruence low root first v period hp rfl hlowv hxroot, hhighv, hxfirst⟩
  · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
    unfold intersection
    rw [ite_eq_right (by simp [han0, hbn0])]
    rw [ite_eq_right (by simp [has0])]
    rw [ite_eq_right (by simp [hbs0])]
    rw [ite_eq_left (by change ((gap % (g : Int) != 0) = true); simp [hgapmod])]
    constructor
    · intro h
      simp [contains] at h
    · intro ha
      have hgdvd : (a.step.gcd b.step : Int) ∣ b.lo - a.lo :=
        common_gap_dvd a b v ha.1 ha.2
      have hzero : gap % (g : Int) = 0 := by
        apply Int.emod_eq_zero_of_dvd
        simpa [g, gap] using hgdvd
      exact (hgapne hzero).elim

/-! ## Counting common elements of two progressions -/

private theorem not_mem_erase_self (a : Int) (l : List Int) (hn : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hxnot : b ∉ bs := (List.nodup_cons.mp hn).1
      have hnbs : bs.Nodup := (List.nodup_cons.mp hn).2
      by_cases h : b = a
      · subst b
        rw [List.erase_cons_head]
        exact hxnot
      · rw [List.erase_cons_tail (by rw [beq_iff_eq]; exact h)]
        intro ha
        rw [List.mem_cons] at ha
        rcases ha with rfl | ha
        · exact h rfl
        · exact ih hnbs ha

private theorem cons_erase_perm (a : Int) (l : List Int) (h : a ∈ l) (hn : l.Nodup) :
    (a :: l.erase a).Perm l := by
  induction l with
  | nil => simp at h
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        rw [List.erase_cons_head]
      · have hbne : ¬ (b == a) = true := by rw [beq_iff_eq]; exact hba
        have hbs : a ∈ bs := by
          have hh := h
          rw [List.mem_cons] at hh
          rcases hh with rfl | hh
          · contradiction
          · exact hh
        have hnbs : bs.Nodup := (List.nodup_cons.mp hn).2
        rw [List.erase_cons_tail hbne]
        exact (List.Perm.swap b a (bs.erase a)).trans (ih hbs hnbs |>.cons b)

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      have hi := ih hxs
      by_cases hx : p x = true
      · rw [List.filter_cons, if_pos hx]
        exact List.nodup_cons.mpr ⟨by
          intro hxmem
          exact hxnot (List.mem_filter.mp hxmem).1, hi⟩
      · rw [List.filter_cons, if_neg hx]
        exact hi

private theorem perm_of_nodup_mem (l₁ l₂ : List Int) (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hm : ∀ v, v ∈ l₁ ↔ v ∈ l₂) : l₁.Perm l₂ := by
  revert l₂ h₁ h₂ hm
  induction l₁ with
  | nil =>
      intro l₂ h₁ h₂ hm
      have h2nil : l₂ = [] := by
        cases l₂ with
        | nil => rfl
        | cons b bs =>
            have : b ∈ ([] : List Int) := (hm b).mpr (by simp)
            simp at this
      simp [h2nil]
  | cons a xs ih =>
      intro l₂ h₁ h₂ hm
      have ha2 : a ∈ l₂ := (hm a).mp (by simp)
      have hxnot : a ∉ xs := (List.nodup_cons.mp h₁).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h₁).2
      have hle : (l₂.erase a).Nodup := h₂.erase a
      have hm' : ∀ v, v ∈ xs ↔ v ∈ l₂.erase a := by
        intro v
        constructor
        · intro hv
          have hv2 : v ∈ l₂ := (hm v).mp (by simp [hv])
          have hne : v ≠ a := by intro e; subst v; exact hxnot hv
          exact (List.mem_erase_of_ne hne).mpr hv2
        · intro hv
          have hne : v ≠ a := by
            intro e
            subst v
            exact not_mem_erase_self a l₂ h₂ hv
          have hv2 : v ∈ l₂ := (List.mem_erase_of_ne hne).mp hv
          have hvcons : v ∈ a :: xs := (hm v).mpr hv2
          exact (List.mem_cons.mp hvcons).resolve_left hne
      exact ((ih (l₂.erase a) hxs hle hm').cons a).trans (cons_erase_perm a l₂ ha2 h₂)

private theorem nodup_map_of_inj {α β : Type} (l : List α) (f : α → β)
    (hf : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (h : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      have hi := ih (fun a ha b hb he => hf a (by simp [ha]) b (by simp [hb]) he) hxs
      rw [List.map_cons]
      exact List.nodup_cons.mpr ⟨by
        intro hxmem
        rcases List.mem_map.mp hxmem with ⟨y, hy, hfy⟩
        have hxy : y = x := hf y (by simp [hy]) x (by simp) hfy
        subst y
        exact hxnot hy, hi⟩

private theorem progVal_inj {p : Progression} (hs : 0 < p.step) {i j : Nat}
    (hi : i < p.n) (hj : j < p.n) (h : progVal p i = progVal p j) : i = j := by
  unfold progVal at h
  have hsne : (p.step : Int) ≠ 0 := by exact_mod_cast (Nat.ne_of_gt hs)
  have hmul : (p.step : Int) * (i : Int) = (p.step : Int) * (j : Int) := by
    rw [Int.mul_comm (p.step : Int) (i : Int), Int.mul_comm (p.step : Int) (j : Int)]
    omega
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hsne).mp hmul
  exact_mod_cast hij

private theorem intersection_step_pos (P Q : Progression) (hPn : 0 < P.n) (hQn : 0 < Q.n)
    (hPs : 0 < P.step) (hQs : 0 < Q.step) : 0 < (intersection P Q).step := by
  have han0 : P.n ≠ 0 := by omega
  have hbn0 : Q.n ≠ 0 := by omega
  have has0 : P.step ≠ 0 := by omega
  have hbs0 : Q.step ≠ 0 := by omega
  unfold intersection
  rw [ite_eq_right (by simp [han0, hbn0])]
  rw [ite_eq_right (by simp [has0])]
  rw [ite_eq_right (by simp [hbs0])]
  by_cases hg : (Q.lo - P.lo) % ((P.step.gcd Q.step : Nat) : Int) = 0
  · rw [ite_eq_right (by change ¬ (((Q.lo - P.lo) % ((P.step.gcd Q.step : Nat) : Int) != 0) = true); simp [hg])]
    dsimp
    have hgd : P.step.gcd Q.step ∣ P.step := Nat.gcd_dvd_left P.step Q.step
    have hA : 0 < P.step / P.step.gcd Q.step := Nat.div_gcd_pos_of_pos_left Q.step hPs
    exact Nat.mul_pos hA hQs
  · rw [ite_eq_left (by change (((Q.lo - P.lo) % ((P.step.gcd Q.step : Nat) : Int) != 0) = true); simp [hg])]
    simp

private theorem count_common (P Q : Progression) (hPn : 0 < P.n) (hQn : 0 < Q.n)
    (hPs : 0 < P.step) (hQs : 0 < Q.step) :
    (List.range P.n).countP (fun q => contains Q (progVal P q)) = (intersection P Q).n := by
  let I := intersection P Q
  have hIs : 0 < I.step := by
    dsimp [I]
    exact intersection_step_pos P Q hPn hQn hPs hQs
  let L := (List.range P.n).filter (fun q => contains Q (progVal P q)) |>.map (progVal P)
  let R := (List.range I.n).map (progVal I)
  have hnodL : L.Nodup := by
    dsimp [L]
    exact nodup_map_of_inj _ _ (fun i hi j hj he =>
      progVal_inj hPs (List.mem_range.mp (List.mem_filter.mp hi).1)
        (List.mem_range.mp (List.mem_filter.mp hj).1) he)
      (nodup_filter _ List.nodup_range)
  have hnodR : R.Nodup := by
    dsimp [R]
    exact nodup_map_of_inj _ _ (fun i hi j hj he =>
      progVal_inj hIs (List.mem_range.mp hi) (List.mem_range.mp hj) he)
      List.nodup_range
  have hmemLR : ∀ v, v ∈ L ↔ v ∈ R := by
    intro v
    constructor
    · intro hv
      rcases List.mem_map.mp hv with ⟨q, hq, hqv⟩
      have hqf := List.mem_filter.mp hq
      have hqP : contains P v = true := by
        apply (contains_iff_mem P v).mpr
        exact ⟨q, List.mem_range.mp hqf.1, by simpa [hqv]⟩
      have hqQ : contains Q v = true := by simpa [hqv] using hqf.2
      have hqI : contains I v = true := by
        dsimp [I]
        exact (intersection_contains P Q v hPn hQn hPs hQs).mpr ⟨hqP, hqQ⟩
      rcases (contains_iff_mem I v).mp hqI with ⟨t, ht, htv⟩
      exact List.mem_map.mpr ⟨t, List.mem_range.mpr ht, by simpa [htv]⟩
    · intro hv
      rcases List.mem_map.mp hv with ⟨t, ht, htv⟩
      have htI : contains I v = true := by
        apply (contains_iff_mem I v).mpr
        exact ⟨t, List.mem_range.mp ht, by simpa [htv]⟩
      have hb := (intersection_contains P Q v hPn hQn hPs hQs).mp (by simpa [I] using htI)
      rcases (contains_iff_mem P v).mp hb.1 with ⟨q, hq, hqv⟩
      have hfq : q ∈ (List.range P.n).filter (fun q => contains Q (progVal P q)) := by
        apply List.mem_filter.mpr
        refine ⟨List.mem_range.mpr hq, ?_⟩
        simpa [hqv] using hb.2
      exact List.mem_map.mpr ⟨q, hfq, by simpa [hqv]⟩
  have hperm : L.Perm R := perm_of_nodup_mem L R hnodL hnodR hmemLR
  have hlen := hperm.length_eq
  rw [List.countP_eq_length_filter]
  change ((List.range P.n).filter (fun q => contains Q (progVal P q))).length = I.n
  simpa [L, R] using hlen

/-! ## Counting helpers for lane-wise identities -/

private theorem countP_range_succ (n : Nat) (f : Nat → Bool) :
    (List.range (n+1)).countP f = (List.range n).countP f + (if f n = true then 1 else 0) := by
  rw [show n+1 = Nat.succ n by omega, List.range_succ, List.countP_append]
  simp

private theorem countP_perm {α : Type} {l₁ l₂ : List α} (h : l₁.Perm l₂) (P : α → Bool) :
    l₁.countP P = l₂.countP P := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact (h.filter P).length_eq

private theorem countP_eq_one_iff_exists_unique (n : Nat) (f : Nat → Bool)
    (huniq : ∀ i, i < n → ∀ j, j < n → f i = true → f j = true → i = j) :
    (List.range n).countP f = if (∃ i, i < n ∧ f i = true) then 1 else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [countP_range_succ]
      have huniq' : ∀ i, i < n → ∀ j, j < n → f i = true → f j = true → i = j := by
        intro i hi j hj hfi hfj
        exact huniq i (by omega) j (by omega) hfi hfj
      rw [ih huniq']
      by_cases hn : f n = true
      · have hnone : ¬ ∃ i, i < n ∧ f i = true := by
          rintro ⟨i, hi, hfi⟩
          have := huniq i (by omega) n (by omega) hfi hn
          omega
        have hex : (∃ i, i < n+1 ∧ f i = true) := ⟨n, by omega, hn⟩
        simp [hn, hnone, hex]
      · have hex : (∃ i, i < n+1 ∧ f i = true) ↔ (∃ i, i < n ∧ f i = true) := by
          constructor
          · rintro ⟨i, hi, hfi⟩
            by_cases hin : i < n
            · exact ⟨i, hin, hfi⟩
            · have : i = n := by omega
              subst i
              exact False.elim (hn hfi)
          · rintro ⟨i, hi, hfi⟩
            exact ⟨i, by omega, hfi⟩
        by_cases hx : ∃ i, i < n ∧ f i = true
        · simp [hn, hx, hex.mpr hx]
        · have hx' : ¬ ∃ i, i < n+1 ∧ f i = true := by
            intro h'
            exact hx (hex.mp h')
          simp [hn, hx, hx']

private theorem affine_inj {z d : Int} (hd : d ≠ 0) {i j : Nat}
    (h : z + d * (i : Int) = z + d * (j : Int)) : i = j := by
  have hm : d * (i : Int) = d * (j : Int) := by omega
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hd).mp hm
  exact_mod_cast hij

private theorem affine_reverse_perm (n : Nat) (z d : Int) (hd : d ≠ 0) :
    ((List.range n).map (fun q : Nat => z + d * (q : Int))).Perm
      ((List.range n).map (fun q : Nat => z + d * ((n - 1 - q : Nat) : Int))) := by
  apply perm_of_nodup_mem
  · exact nodup_map_of_inj (List.range n) (fun q : Nat => z + d * (q : Int))
      (fun i hi j hj he => affine_inj (z := z) (d := d) hd he) List.nodup_range
  · exact nodup_map_of_inj (List.range n) (fun q : Nat => z + d * ((n - 1 - q : Nat) : Int))
      (fun i hi j hj he => by
        have ht := affine_inj (z := z) (d := d) hd he
        have hit : i < n := List.mem_range.mp hi
        have hjt : j < n := List.mem_range.mp hj
        omega) List.nodup_range
  · intro v
    constructor
    · intro hv
      rcases List.mem_map.mp hv with ⟨q, hq, hqv⟩
      have hqn : q < n := List.mem_range.mp hq
      let t := n - 1 - q
      have htn : t < n := by dsimp [t]; omega
      have htq : n - 1 - t = q := by dsimp [t]; omega
      apply List.mem_map.mpr
      refine ⟨t, List.mem_range.mpr htn, ?_⟩
      rw [htq]
      exact hqv
    · intro hv
      rcases List.mem_map.mp hv with ⟨t, ht, htv⟩
      have htn : t < n := List.mem_range.mp ht
      let q := n - 1 - t
      have hqn : q < n := by dsimp [q]; omega
      have hqt : n - 1 - q = t := by dsimp [q]; omega
      apply List.mem_map.mpr
      refine ⟨q, List.mem_range.mpr hqn, ?_⟩
      change z + d * ((n - 1 - t : Nat) : Int) = v
      exact htv

private theorem countP_reverse_affine (n : Nat) (z d : Int) (P : Int → Bool) :
    (List.range n).countP (fun (q : Nat) => P (z + d * (q : Int))) =
      (List.range n).countP (fun (q : Nat) => P (z + d * ((n - 1 - q : Nat) : Int))) := by
  by_cases hd : d = 0
  · subst d
    simp
  · have hp := countP_perm (affine_reverse_perm n z d hd) P
    simpa [List.countP_map, Function.comp_def] using hp

private theorem countP_eq_sum_indicator {α : Type} (l : List α) (P : α → Bool) :
    l.countP P = (l.map (fun x => if P x = true then (1 : Nat) else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.countP_cons, List.map_cons, List.sum_cons, ih]
      by_cases h : P x = true <;> simp [h] <;> omega

private theorem count_eq_one_of_mem (Q : Progression) (hQs : 0 < Q.step) (v : Int) :
    (List.range Q.n).countP (fun i => v == progVal Q i) =
      if contains Q v = true then 1 else 0 := by
  have huniq : ∀ i, i < Q.n → ∀ j, j < Q.n →
      (v == progVal Q i) = true → (v == progVal Q j) = true → i = j := by
    intro i hi j hj hvi hvj
    have hvi' : v = progVal Q i := beq_iff_eq.mp hvi
    have hvj' : v = progVal Q j := beq_iff_eq.mp hvj
    exact progVal_inj hQs hi hj (hvi'.symm.trans hvj')
  rw [countP_eq_one_iff_exists_unique Q.n _ huniq]
  have hex : (∃ i, i < Q.n ∧ v = progVal Q i) ↔ contains Q v = true := by
    constructor
    · rintro ⟨i, hi, hvi⟩
      exact (contains_iff_mem Q v).mpr ⟨i, hi, hvi⟩
    · intro hv
      rcases (contains_iff_mem Q v).mp hv with ⟨i, hi, hvi⟩
      exact ⟨i, hi, hvi⟩
  by_cases h : contains Q v = true
  · have hx : ∃ i, i < Q.n ∧ v = progVal Q i := hex.mpr h
    simp [h, hx, beq_iff_eq]
  · have hx : ¬ ∃ i, i < Q.n ∧ v = progVal Q i := by
      intro hx'
      exact h (hex.mp hx')
    simp [h, hx, beq_iff_eq]

private theorem joinCount_zero_left (a b : Lane) (h : a.n = 0) : joinCount a b = 0 := by
  unfold joinCount values
  rw [h]
  simp [intersection]

private theorem joinCount_zero_right (a b : Lane) (h : b.n = 0) : joinCount a b = 0 := by
  unfold joinCount values
  rw [h]
  simp [intersection]

private theorem sum_ite_countP_mul {α : Type} (l : List α) (P : α → Bool) (k : Nat) :
    (l.map (fun x => if P x = true then k else 0)).sum = k * l.countP P := by
  rw [List.countP_eq_length_filter]
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : P x = true
      · simp [h, ih, Nat.mul_succ]
        omega
      · simp [h, ih]

private theorem progVal_values_of_nonneg (a : Lane) (h : 0 ≤ a.step) (q : Nat) :
    progVal (values a) q = a.base + a.step * (q : Int) := by
  unfold values progVal
  rw [if_neg (by omega : ¬ a.step < 0)]
  rw [Int.natAbs_of_nonneg h]
  grind

private theorem progVal_values_of_neg (a : Lane) (h : a.step < 0) (q : Nat) (hq : q < a.n) :
    progVal (values a) q = a.base + a.step * ((a.n - 1 - q : Nat) : Int) := by
  unfold values progVal
  rw [if_pos h]
  have hna : (a.step.natAbs : Int) = -a.step := by
    rw [← Int.natAbs_neg]
    exact Int.natAbs_of_nonneg (by omega)
  rw [hna]
  have hsub : (a.n - 1 : Nat) = (a.n - 1 - q) + q := by omega
  have hcast : ((a.n - 1 : Nat) : Int) = ((a.n - 1 - q : Nat) : Int) + (q : Int) := by
    exact_mod_cast hsub
  grind

private theorem count_affine_eq_mem (a : Lane) (ha0 : 0 < a.n) (has : a.step ≠ 0) (v : Int) :
    (List.range a.n).countP (fun (q : Nat) => v == a.base + a.step * (q : Int)) =
      if contains (values a) v = true then 1 else 0 := by
  have hstepV : 0 < (values a).step := by
    unfold values
    by_cases h : a.step < 0
    · simp [h]
      omega
    · simp [h]
      omega
  have hbase : (List.range a.n).countP (fun (q : Nat) => v == progVal (values a) q) =
      if contains (values a) v = true then 1 else 0 := by
    simpa [values] using count_eq_one_of_mem (values a) hstepV v
  by_cases hpos : 0 < a.step
  · have hcong : (List.range a.n).countP (fun (q : Nat) => v == a.base + a.step * (q : Int)) =
        (List.range a.n).countP (fun (q : Nat) => v == progVal (values a) q) := by
      apply List.countP_congr
      intro q hq
      rw [progVal_values_of_nonneg a (by omega) q]
    rw [hcong, hbase]
  · have hneg : a.step < 0 := by omega
    have hrev := countP_reverse_affine a.n a.base a.step (fun w => v == w)
    have hcong : (List.range a.n).countP (fun (q : Nat) => v == progVal (values a) q) =
        (List.range a.n).countP (fun (q : Nat) => v == a.base + a.step * ((a.n - 1 - q : Nat) : Int)) := by
      apply List.countP_congr
      intro q hq
      rw [progVal_values_of_neg a hneg q (List.mem_range.mp hq)]
    rw [hbase] at hcong
    rw [hrev, hcong]

private theorem joinCount_left_const (a b : Lane) (hanp : 0 < a.n) (hbnp : 0 < b.n)
    (ha0 : a.step = 0) :
    joinCount a b = (if contains (values b) a.base = true then 1 else 0) * a.n *
      (if b.step = 0 then b.n else 1) := by
  unfold joinCount
  unfold intersection
  rw [if_neg (by simp [values]; omega)]
  rw [if_pos (by simp [values, ha0])]
  have hlo : (values a).lo = a.base := by simp [values, ha0]
  rw [hlo]
  simp [ha0]

private theorem joinCount_right_const (a b : Lane) (hanp : 0 < a.n) (hbnp : 0 < b.n)
    (ha0 : a.step ≠ 0) (hb0 : b.step = 0) :
    joinCount a b = (if contains (values a) b.base = true then 1 else 0) *
      (if a.step = 0 then a.n else 1) * b.n := by
  unfold joinCount
  unfold intersection
  rw [if_neg (by simp [values]; omega)]
  rw [if_neg (by simp [values, ha0])]
  rw [if_pos (by simp [values, hb0])]
  have hlo : (values b).lo = b.base := by simp [values, hb0]
  rw [hlo]
  simp [ha0, hb0]

private theorem countP_const_bool (n : Nat) (c : Bool) :
    (List.range n).countP (fun _ => c) = if c = true then n else 0 := by
  by_cases h : c = true <;> simp [h]

private theorem countP_eq_const (n : Nat) (c v : Int) :
    (List.range n).countP (fun (_ : Nat) => v == c) = if v == c then n else 0 := by
  by_cases h : v == c <;> simp [h]

private theorem lane_residue (a b : Lane) :
    ((List.range a.n).map (fun (q : Nat) => (List.range b.n).countP
      (fun (i : Nat) => a.base + a.step * (q : Int) == b.base + b.step * (i : Int)))).sum = joinCount a b := by
  by_cases ha0 : a.n = 0
  · simp [ha0, joinCount_zero_left]
  by_cases hb0 : b.n = 0
  · have hzero : (List.range a.n).map (fun (q : Nat) => (List.range b.n).countP
        (fun (i : Nat) => a.base + a.step * (q : Int) == b.base + b.step * (i : Int))) =
        (List.range a.n).map (fun (_ : Nat) => (0 : Nat)) := by
      apply List.map_congr_left
      intro q hq
      simp [hb0]
    rw [hzero, sum_map_zero_nat, joinCount_zero_right a b hb0]
  have hap : 0 < a.n := by omega
  have hbp : 0 < b.n := by omega
  by_cases haz : a.step = 0
  · rw [haz]
    simp only [Int.zero_mul, Int.add_zero]
    by_cases hbz : b.step = 0
    · rw [hbz]
      simp only [Int.zero_mul, Int.add_zero]
      rw [show (List.range a.n).map (fun q => (List.range b.n).countP
          (fun i => a.base == b.base)) = (List.range a.n).map (fun _ => if a.base == b.base then b.n else 0) by
        apply List.map_congr_left
        intro q hq
        by_cases h : a.base == b.base <;> simp [h]]
      rw [sum_ite_countP_mul]
      rw [countP_const_bool]
      unfold joinCount values
      simp [ha0, hb0, haz, hbz, contains_const_iff, hap, hbp, intersection]
      by_cases h : a.base = b.base <;> simp [h] <;> grind
    · have hperq : ∀ (q : Nat), (List.range b.n).countP
          (fun (i : Nat) => a.base == b.base + b.step * (i : Int)) =
          if contains (values b) a.base = true then 1 else 0 := by
        intro q
        exact count_affine_eq_mem b hbp hbz a.base
      rw [show (List.range a.n).map (fun q => (List.range b.n).countP
          (fun (i : Nat) => a.base == b.base + b.step * (i : Int))) =
          (List.range a.n).map (fun _ => if contains (values b) a.base = true then 1 else 0) by
        apply List.map_congr_left
        intro q hq
        exact hperq q]
      rw [sum_ite_countP_mul]
      rw [countP_const_bool]
      rw [joinCount_left_const a b hap hbp haz]
      simp only [haz, hbz, beq_self_eq_true, ↓reduceIte, Nat.one_mul]
      grind
  · have hastep : a.step ≠ 0 := haz
    by_cases hbz : b.step = 0
    · rw [hbz]
      simp only [Int.zero_mul, Int.add_zero]
      rw [show (List.range a.n).map (fun (q : Nat) => (List.range b.n).countP
          (fun (i : Nat) => a.base + a.step * (q : Int) == b.base)) =
          (List.range a.n).map (fun (q : Nat) => if a.base + a.step * (q : Int) == b.base then b.n else 0) by
        apply List.map_congr_left
        intro q hq
        rw [countP_eq_const]]
      rw [sum_ite_countP_mul]
      have hflip : (List.range a.n).countP (fun (q : Nat) => a.base + a.step * (q : Int) == b.base) =
          (List.range a.n).countP (fun (q : Nat) => b.base == a.base + a.step * (q : Int)) := by
        apply List.countP_congr
        intro q hq
        by_cases h : (a.base + a.step * (q : Int) == b.base) = true
        · have hb : (b.base == a.base + a.step * (q : Int)) = true := by
            rw [beq_iff_eq]
            exact (beq_iff_eq.mp h).symm
          simp [h, hb]
        · have hb : (b.base == a.base + a.step * (q : Int)) = false := by
            rw [beq_eq_false_iff_ne]
            intro he
            exact h (by rw [beq_iff_eq]; exact he.symm)
          simp [h, hb]
      rw [hflip, count_affine_eq_mem a hap hastep b.base]
      rw [joinCount_right_const a b hap hbp hastep hbz]
      simp only [haz, hbz, beq_self_eq_true, ↓reduceIte, Nat.one_mul]
      grind
    · have hbsp : 0 < (values b).step := by
        dsimp [values]
        exact Int.natAbs_pos.mpr hbz
      have hF : ∀ (q : Nat), (List.range b.n).countP
          (fun (i : Nat) => a.base + a.step * (q : Int) == b.base + b.step * (i : Int)) =
          if contains (values b) (a.base + a.step * (q : Int)) = true then 1 else 0 := by
        intro q
        exact count_affine_eq_mem b hbp hbz (a.base + a.step * (q : Int))
      rw [show (List.range a.n).map (fun (q : Nat) => (List.range b.n).countP
          (fun (i : Nat) => a.base + a.step * (q : Int) == b.base + b.step * (i : Int))) =
          (List.range a.n).map (fun (q : Nat) => if contains (values b) (a.base + a.step * (q : Int)) = true then 1 else 0) by
        apply List.map_congr_left
        intro q hq
        exact hF q]
      rw [← countP_eq_sum_indicator]
      have hcnt0 := count_common (values a) (values b) (by unfold values; simp [hap]) (by unfold values; simp [hbp])
          (by dsimp [values]; exact Int.natAbs_pos.mpr hastep)
          hbsp
      have hcnt : (List.range a.n).countP (fun (q : Nat) => contains (values b) (progVal (values a) q)) =
          (intersection (values a) (values b)).n := by
        simpa [values] using hcnt0
      by_cases hpos : 0 < a.step
      · have hcong : (List.range a.n).countP (fun (q : Nat) => contains (values b) (a.base + a.step * (q : Int))) =
            (List.range a.n).countP (fun (q : Nat) => contains (values b) (progVal (values a) q)) := by
          apply List.countP_congr
          intro q hq
          rw [progVal_values_of_nonneg a (by omega) q]
        rw [hcong, hcnt]
        unfold joinCount
        simp [haz, hbz]
      · have hneg : a.step < 0 := by omega
        have hrev := countP_reverse_affine a.n a.base a.step (fun w => contains (values b) w)
        have hcong : (List.range a.n).countP (fun (q : Nat) => contains (values b) (progVal (values a) q)) =
            (List.range a.n).countP (fun (q : Nat) => contains (values b) (a.base + a.step * ((a.n - 1 - q : Nat) : Int))) := by
          apply List.countP_congr
          intro q hq
          rw [progVal_values_of_neg a hneg q (List.mem_range.mp hq)]
        rw [hrev, ← hcong, hcnt]
        unfold joinCount
        simp [haz, hbz]

private theorem lane_n_zero_of_ge (n r p : Nat) (hp : 0 < p) (z d : Int) (hr : n ≤ r) :
    (lane n r p z d).n = 0 := by
  dsimp [lane]
  have hsub : n - r = 0 := by omega
  rw [hsub]
  have heq : 0 + p - 1 = p - 1 := by omega
  rw [heq]
  exact Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))

private theorem sum_range_trunc (f : Nat → Nat) (k p : Nat) (hk : k ≤ p) :
    ((List.range p).map (fun r => if r < k then f r else 0)).sum = ((List.range k).map f).sum := by
  obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hk
  rw [List.range_add, List.map_append, List.sum_append]
  have hzero : ((List.map (fun x => k + x) (List.range d)).map
      (fun r => if r < k then f r else 0)).sum = 0 := by
    rw [show (List.map (fun x => k + x) (List.range d)).map (fun r => if r < k then f r else 0) =
        (List.map (fun x => k + x) (List.range d)).map (fun _ => 0) by
      apply List.map_congr_left
      intro r hr
      simp only [List.mem_map, List.mem_range] at hr
      rcases hr with ⟨x, hx, rfl⟩
      by_cases hlt : k + x < k
      · omega
      · simp [hlt]]
    apply sum_map_zero_nat
  rw [hzero]
  simp only [Nat.add_zero]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  have hrlt : r < k := List.mem_range.mp hr
  simp [hrlt]

private theorem lanes_sum_apply (c : Config) (hp : 0 < c.dict.length) (F : Lane → Nat)
    (hzero : ∀ a : Lane, a.n = 0 → F a = 0) :
    ((layout.lanes c).map F).sum =
      ((List.range c.dict.length).map (fun r =>
        F (lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step))).sum := by
  unfold layout
  rw [List.map_map]
  let k := min c.dict.length c.n
  have hklen : k ≤ c.dict.length := by dsimp [k]; omega
  rw [show (List.range c.dict.length).map (fun r =>
      F (lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step)) =
      (List.range c.dict.length).map (fun r =>
        if r < k then F (lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step) else 0) by
    apply List.map_congr_left
    intro r hr
    by_cases hrk : r < k
    · simp [hrk]
    · have hrn : c.n ≤ r := by
        by_cases hle : c.n ≤ c.dict.length
        · have hmin : k = c.n := by dsimp [k]; exact Nat.min_eq_right hle
          rw [hmin] at hrk
          omega
        · have hle' : c.dict.length ≤ c.n := by omega
          have hmin : k = c.dict.length := by dsimp [k]; exact Nat.min_eq_left hle'
          rw [hmin] at hrk
          have hrlt : r < c.dict.length := List.mem_range.mp hr
          omega
      rw [if_neg hrk]
      exact hzero (lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step)
        (lane_n_zero_of_ge c.n r c.dict.length hp _ _ hrn)]
  rw [sum_range_trunc _ k c.dict.length hklen]
  simp [Function.comp_def, k]

theorem sound_ResidueJoin :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift.ResidueJoin.Goal := by
  intro c b
  by_cases hd : c.dict = []
  · rw [rows_drift_empty c hd]
    simp [layout, hd]
  · have hp : 0 < c.dict.length := List.length_pos_iff.mpr hd
    rw [rows_drift c hp]
    let make : Nat → Int × Int := fun r => (c.dict[r]?.getD 0, c.step)
    let F : Int → Nat := fun v => (List.range b.n).countP
      (fun (i : Nat) => v == b.base + b.step * (i : Int))
    rw [List.map_map]
    change ((List.range c.n).map (fun i : Nat => F (stripedValue c.dict.length make i))).sum =
      ((layout.lanes c).map (fun a => joinCount a b)).sum
    rw [striped_sum_nat c.dict.length hp c.n make F]
    rw [lanes_sum_apply c hp (fun a => joinCount a b) (fun a ha => joinCount_zero_left a b ha)]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    have hrp : r < c.dict.length := List.mem_range.mp hr
    have hget : c.dict[r]?.getD 0 = c.dict[r]?.getD 0 := rfl
    dsimp [make, F]
    exact lane_residue (lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step) b

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.DictionaryDrift.Remaining
