import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_EncodedPages_G2

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

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

private theorem range_succ_map' (f : Nat → Nat) (n : Nat) :
    (List.range (n+1)).map f = f 0 :: (List.range n).map (fun i => f (i+1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + 1 = Nat.succ (n+1) by omega, List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem sum_range_get_count : ∀ (l : List Int) (v : Int),
    ((List.range l.length).map (fun r => if l[r]?.getD 0 == v then 1 else 0)).sum = l.count v := by
  intro l
  induction l with
  | nil => intro v; simp
  | cons a as ih =>
      intro v
      rw [List.count_cons]
      simp only [List.length_cons]
      rw [range_succ_map']
      simp only [List.map_cons, List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
        List.getElem?_cons_succ]
      rw [ih]
      omega

private theorem count_take_get : ∀ (k : Nat) (l : List Int) (v : Int),
    (l.take k).count v =
      ((List.range k).map (fun r => if r < l.length && l[r]?.getD 0 == v then 1 else 0)).sum := by
  intro k
  induction k with
  | zero => intro l v; simp
  | succ k ih =>
      intro l v
      cases l with
      | nil => simp [sum_map_zero_nat]
      | cons a as =>
          simp only [List.length_cons]
          rw [show k + 1 = Nat.succ k by omega, List.take_succ_cons]
          rw [List.count_cons]
          rw [ih as v]
          rw [range_succ_map']
          simp only [List.sum_cons, List.getElem?_cons_zero, Option.getD_some,
            List.getElem?_cons_succ]
          have hsimp : ∀ i : Nat,
              (if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              if i < as.length && as[i]?.getD 0 == v then 1 else 0 := by
            intro i
            by_cases h : i < as.length && as[i]?.getD 0 == v <;> simp [h] <;> omega
          rw [show (List.range k).map (fun i : Nat =>
              if i + 1 < as.length + 1 && as[i]?.getD 0 == v then 1 else 0) =
              (List.range k).map (fun i : Nat =>
              if i < as.length && as[i]?.getD 0 == v then 1 else 0) from
            List.map_congr_left (fun i hi => hsimp i)]
          simp
          omega

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

private theorem sum_range_add (f g : Nat → Nat) (n : Nat) :
    ((List.range n).map (fun i => f i + g i)).sum =
      ((List.range n).map f).sum + ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append,
        List.map_append, List.sum_append, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      omega

private theorem sum_range_mul_const (f : Nat → Nat) (n c : Nat) :
    ((List.range n).map (fun i => c * f i)).sum = c * ((List.range n).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [ih]
      simp only [List.map_append, List.sum_append, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      rw [Nat.mul_add]
      simp

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n+1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem even_or_odd (n : Nat) : n % 2 = 0 ∨ n % 2 = 1 := by
  have h := Nat.mod_lt n (by omega : 0 < 2)
  omega

namespace WrappingSequence

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.WrappingSequence

private theorem rows_wrap (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, stripedValue (width c) (fun r => (c.base + c.step * ((r : Nat) : Int), 0)) i)) := by
  unfold rows
  simp [layout, stripedValue]

theorem QuotientHistogram_sound : QuotientHistogram.Goal := by
  intro c r hs hr
  let p := width c
  have hp : 0 < p := by dsimp [p, width]; omega
  let make : Nat → Int × Int := fun r => (c.base + c.step * ((r : Nat) : Int), 0)
  have heqval : ∀ r' : Nat,
      (c.base + c.step * ((r' : Nat) : Int) == c.base + c.step * ((r : Nat) : Int)) = (r' == r) := by
    intro r'
    by_cases h : r' = r
    · simp [h]
    · have hne : c.base + c.step * ((r' : Nat) : Int) ≠ c.base + c.step * ((r : Nat) : Int) := by
        intro hh
        have hm : c.step * ((r' : Nat) : Int) = c.step * ((r : Nat) : Int) := Int.add_left_cancel hh
        have hc : ((r' : Nat) : Int) = ((r : Nat) : Int) := (Int.mul_eq_mul_left_iff hs).mp hm
        exact h (by exact_mod_cast hc)
      rw [beq_eq_false_iff_ne.mpr hne, beq_eq_false_iff_ne.mpr h]
  unfold occurrence
  rw [rows_wrap]
  rw [countP_eq_sum (f := fun v => v == c.base + c.step * ((r : Nat) : Int))]
  simp only [List.map_map]
  change ((List.range c.n).map (fun i : Nat =>
    if stripedValue p make i == c.base + c.step * ((r : Nat) : Int) then 1 else 0)).sum =
    c.n / width c + if r < c.n % width c then 1 else 0
  rw [striped_sum_nat p hp c.n make (fun v => if v == c.base + c.step * ((r : Nat) : Int) then 1 else 0)]
  simp only [make]
  simp only [Int.zero_mul, Int.add_zero]
  have hinner : ∀ x : Nat,
      ((List.range ((c.n - x + p - 1) / p)).map (fun _ : Nat =>
        if c.base + c.step * ((x : Nat) : Int) == c.base + c.step * ((r : Nat) : Int) then 1 else 0)).sum =
      if x == r then (c.n - x + p - 1) / p else 0 := by
    intro x
    rw [heqval x]
    simpa using sum_range_ite_nat ((c.n - x + p - 1) / p) (x == r) 1
  simp only [hinner]
  have hsum : ((List.range p).map (fun x =>
      if x == r then ((c.n - x + p - 1) / p) else 0)).sum =
      (c.n - r + p - 1) / p := by
    exact sum_eq_single p r (by dsimp [p] at hr ⊢; exact hr) (fun x => (c.n - x + p - 1) / p)
  rw [hsum]
  rw [mod_step_count c.n p r hp (by dsimp [p] at hr ⊢; exact hr)]
-- TODO(SliceState)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end WrappingSequence

namespace DictionaryCycle

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

private theorem rows_dict (c : Config) (hne : c.dict ≠ []) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, stripedValue c.dict.length (fun r => (c.dict[r]?.getD 0, 0)) i)) := by
  unfold rows
  simp only [layout]
  induction (List.range c.n) with
  | nil => simp
  | cons i is ih =>
      have hpos : 0 < c.dict.length := List.length_pos_iff.mpr hne
      have hlt : i % c.dict.length < c.dict.length := Nat.mod_lt i hpos
      have hget : c.dict[i % c.dict.length]? = some (c.dict[i % c.dict.length]) := by
        rw [List.getElem?_eq_some_iff]
        exact ⟨hlt, rfl⟩
      simp [hget, stripedValue, ih]

theorem DuplicateCodes_sound : DuplicateCodes.Goal := by
  intro c v hne
  let p := c.dict.length
  have hp : 0 < p := List.length_pos_iff.mpr hne
  let make : Nat → Int × Int := fun r => (c.dict[r]?.getD 0, 0)
  unfold occurrence
  rw [rows_dict c hne]
  rw [countP_eq_sum (f := fun x => x == v)]
  simp only [List.map_map]
  change ((List.range c.n).map (fun i : Nat =>
    if stripedValue p make i == v then 1 else 0)).sum =
    (c.n / p) * c.dict.count v + (c.dict.take (c.n % p)).count v
  rw [striped_sum_nat p hp c.n make (fun x => if x == v then 1 else 0)]
  simp only [make]
  simp only [Int.zero_mul, Int.add_zero]
  have hinner : ∀ x : Nat,
      ((List.range ((c.n - x + p - 1) / p)).map (fun _ : Nat =>
        if c.dict[x]?.getD 0 == v then 1 else 0)).sum =
      ((c.n - x + p - 1) / p) * (if c.dict[x]?.getD 0 == v then 1 else 0) := by
    intro x
    exact sum_range_const_nat _ _
  simp only [hinner]
  let it : Nat → Nat := fun r => if c.dict[r]?.getD 0 == v then 1 else 0
  have hmod : ∀ r : Nat, r < p → (c.n - r + p - 1) / p = c.n / p + if r < c.n % p then 1 else 0 := by
    intro r hr
    exact mod_step_count c.n p r hp (by dsimp [p] at hr ⊢; exact hr)
  have hsum2 : ((List.range p).map (fun r => (c.n - r + p - 1) / p * it r)).sum =
      ((List.range p).map (fun r => (c.n / p) * it r + (if r < c.n % p then it r else 0))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rw [hmod r (List.mem_range.mp hr)]
    dsimp [it]
    by_cases hlt : r < c.n % p <;> by_cases heq : c.dict[r]?.getD 0 == v <;> simp [hlt, heq] <;> omega
  change ((List.range p).map (fun r => (c.n - r + p - 1) / p * it r)).sum =
    (c.n / p) * c.dict.count v + (c.dict.take (c.n % p)).count v
  rw [hsum2, sum_range_add, sum_range_mul_const]
  rw [show ((List.range p).map it).sum = c.dict.count v by
    dsimp [it, p]
    exact sum_range_get_count c.dict v]
  rw [show ((List.range p).map (fun r => if r < c.n % p then it r else 0)).sum =
      ((List.range (c.n % p)).map it).sum by
    exact sum_range_trunc it (c.n % p) p (Nat.le_of_lt (Nat.mod_lt c.n hp))]
  rw [show ((List.range (c.n % p)).map it).sum = (c.dict.take (c.n % p)).count v by
    rw [count_take_get]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    have hrlt : r < c.n % p := List.mem_range.mp hr
    have hltlen : r < c.dict.length := by
      have hmodlt : c.n % p < p := Nat.mod_lt _ hp
      have hmodlt' : c.n % c.dict.length < c.dict.length := by simpa [p] using hmodlt
      omega
    simp [it, hltlen]]
-- TODO(SliceState)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end DictionaryCycle

namespace DictionaryDrift

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryDrift

-- TODO(PredicateRank)

-- TODO(SliceState)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

end DictionaryDrift


namespace AlternatingSign

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign

private def altTerm (c : Config) (i : Nat) : Int :=
  (if i % 2 == 0 then (1:Int) else -1) * (c.base + c.step*(i:Int))

private def altSum (c : Config) (n : Nat) : Int := ((List.range n).map (altTerm c)).sum

private theorem rows_alt (c : Config) :
    (rows layout c).map Prod.snd = (List.range c.n).map (altTerm c) := by
  unfold rows
  simp [layout, altTerm]

private theorem alt_sum_succ (c : Config) (n : Nat) :
    altSum c (n+1) = altSum c n + altTerm c n := by
  unfold altSum
  exact range_succ_map_sum (altTerm c) n

private theorem alt_pair_even (c : Config) (k : Nat) (hk : k % 2 = 0) :
    altTerm c k + altTerm c (k+1) = -c.step := by
  have hk1 : (k+1) % 2 = 1 := by omega
  simp [altTerm, hk, hk1, Int.natCast_add, Int.mul_add]
  omega

private theorem alt_pair_odd (c : Config) (k : Nat) (hk : k % 2 = 1) :
    altTerm c k + altTerm c (k+1) = c.step := by
  have hk1 : (k+1) % 2 = 0 := by omega
  simp [altTerm, hk, hk1, Int.natCast_add, Int.mul_add]
  omega

private theorem alt_sum_closed (c : Config) : ∀ n : Nat,
    altSum c n = -c.step*((n/2:Nat):Int) + if n%2 == 1 then c.base+c.step*(n-1:Nat) else 0 := by
  intro n
  induction n with
  | zero => simp [altSum]
  | succ n ih =>
      by_cases hn : n = 0
      · subst n
        simp [altSum, altTerm]
      · have hnpos : 0 < n := by omega
        rcases even_or_odd n with he | ho
        · have hn2 : (n+1)/2 = n/2 := by omega
          have hodd1 : (n+1)%2 = 1 := by omega
          have hnot : ¬ n%2 = 1 := by omega
          rw [alt_sum_succ, ih, hn2]
          simp [altTerm, he, hodd1]
        · have hn2 : (n+1)/2 = n/2 + 1 := by omega
          have hodd0 : (n+1)%2 = 0 := by omega
          have hncast : ((n : Nat) : Int) = ((n-1 : Nat) : Int) + 1 := by omega
          rw [alt_sum_succ, ih, hn2]
          simp [altTerm, ho, hodd0, Int.natCast_add]
          rw [hncast]
          simp [Int.mul_add]
          omega

-- TODO(SliceState)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

theorem CancelPairs_sound : CancelPairs.Goal := by
  intro c
  rw [rows_alt]
  exact alt_sum_closed c c.n

end AlternatingSign

namespace TriangleWave

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.TriangleWave

private theorem filter_map_pair_snd {α : Type} [BEq α] (xs : List Nat) (f : Nat → α) (v : α) :
    ((xs.map (fun i => (i, f i))).filter (fun r => r.2 == v)).map Prod.fst =
      xs.filter (fun i => f i == v) := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : f x == v <;> simp [h, ih]

private theorem rows_tri (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step*(min (i%(2*width c)) (2*width c - i%(2*width c)) : Nat))) := by
  unfold rows
  simp [layout]

private theorem peak_bool (c : Config) (hs : c.step ≠ 0) (i : Nat) :
    (c.base + c.step*(min (i%(2*width c)) (2*width c - i%(2*width c)) : Nat) ==
       c.base + c.step*(width c)) = (i%(2*width c) == width c) := by
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  have hpos : 0 < 2*w := by omega
  have hlt : i % (2*w) < 2*w := Nat.mod_lt _ hpos
  by_cases hle : i%(2*w) ≤ w
  · have hmin : min (i%(2*w)) (2*w - i%(2*w)) = i%(2*w) := Nat.min_eq_left (by omega)
    have hval : (c.base + c.step*((i%(2*w) : Nat) : Int) == c.base + c.step*(w:Int)) = (i%(2*w) == w) := by
      by_cases hrw : i%(2*w) = w
      · have hcast : ((i%(2*w) : Nat) : Int) = (w:Int) := by exact_mod_cast hrw
        rw [hcast]
        simp [hrw]
      · have hne : c.base + c.step*((i%(2*w) : Nat) : Int) ≠ c.base + c.step*(w:Int) := by
          intro hh
          have hm : c.step*((i%(2*w) : Nat) : Int) = c.step*(w:Int) := by
            exact Int.add_left_cancel hh
          have hc : ((i%(2*w) : Nat) : Int) = (w:Int) := (Int.mul_eq_mul_left_iff hs).mp hm
          exact hrw (by exact_mod_cast hc)
        have hb1 : (c.base + c.step*((i%(2*w) : Nat) : Int) == c.base + c.step*(w:Int)) = false :=
          beq_eq_false_iff_ne.mpr hne
        have hb2 : (i%(2*w) == w) = false := beq_eq_false_iff_ne.mpr hrw
        rw [hb1, hb2]
    simpa [w, hmin] using hval
  · have hgt : w < i%(2*w) := by omega
    have hmin : min (i%(2*w)) (2*w - i%(2*w)) = 2*w - i%(2*w) := Nat.min_eq_right (by omega)
    have hneq : 2*w - i%(2*w) ≠ w := by omega
    have hneq' : i%(2*w) ≠ w := by omega
    have hneqInt : c.base + c.step*((2*w - i%(2*w) : Nat) : Int) ≠ c.base + c.step*(w:Int) := by
      intro hh
      have hm : c.step*((2*w - i%(2*w) : Nat) : Int) = c.step*(w:Int) := by
        exact Int.add_left_cancel hh
      have hc : ((2*w - i%(2*w) : Nat) : Int) = (w:Int) := (Int.mul_eq_mul_left_iff hs).mp hm
      have hn : (2*w - i%(2*w) : Nat) = w := by exact_mod_cast hc
      exact hneq hn
    have hb1 : (c.base + c.step*((2*w - i%(2*w) : Nat) : Int) == c.base + c.step*(w:Int)) = false :=
      beq_eq_false_iff_ne.mpr hneqInt
    have hb2 : (i%(2*w) == w) = false := beq_eq_false_iff_ne.mpr hneq'
    have hres : (c.base + c.step*((2*w - i%(2*w) : Nat) : Int) == c.base + c.step*(w:Int)) =
        (i%(2*w) == w) := by rw [hb1, hb2]
    simpa [w, hmin] using hres

-- TODO(SliceState)
-- TODO(ResidueJoin)
-- TODO(DeleteProgression)
-- TODO(AlignedProduct)

theorem PeakPositions_sound : PeakPositions.Goal := by
  intro c hs
  rw [rows_tri]
  rw [filter_map_pair_snd]
  apply List.filter_congr
  intro i hi
  simpa using peak_bool c hs i

end TriangleWave

end TinylambOpt.ResearchProofs.Atlas_EncodedPages_G2
