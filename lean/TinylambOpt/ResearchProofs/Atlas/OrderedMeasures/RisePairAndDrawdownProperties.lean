import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved (10 claims, including the following)

- `Drawdown.Bidirectional` — `∀ x, max (measure x) (measure x.reverse) = spread x`
- `Drawdown.CrossExtreme` — `∀ x y, x ≠ [] → y ≠ [] → measure (x++y) = max (max
  (measure x) (measure y)) (x.max?.getD 0-y.min?.getD 0)`
- `Drawdown.DeleteMonotone` — `∀ x (keep : Int → Bool), measure (x.filter keep) ≤
  measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.RisePairAndDrawdownProperties

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### RisePairs: pair-counting helpers -/

private theorem rp_pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem rp_pairs_singleton (a : Int) : pairs [a] = [] := by
  simp [pairs]

private theorem rp_countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem rp_countP_congr_bool {α : Type} (P Q : α → Bool) (l : List α)
    (h : ∀ a ∈ l, P a = Q a) : l.countP P = l.countP Q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, h a (by simp),
        ih (fun b hb => h b (by simp [hb]))]

private theorem rp_cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem rp_pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, rp_pairs_cons, rp_pairs_cons, rp_cart_cons]
      simp only [List.countP_append, List.map_append]
      rw [rp_countP_map_comp, rp_countP_map_comp]
      rw [ih]
      omega

private theorem rp_cart_countP (P : Int × Int → Bool) (x y : Series) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [rp_cart_cons]
      simp only [List.countP_append, List.map_cons, List.sum_cons]
      rw [rp_countP_map_comp, ih]

private theorem rp_mem_pairs {x : Series} {p : Int × Int} (h : p ∈ pairs x) :
    p.1 ∈ x ∧ p.2 ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons a xs ih =>
      rw [rp_pairs_cons] at h
      rw [List.mem_append] at h
      rcases h with h | h
      · rw [List.mem_map] at h
        obtain ⟨b, hb, rfl⟩ := h
        exact ⟨List.mem_cons_self, List.mem_cons_of_mem a hb⟩
      · obtain ⟨h1, h2⟩ := ih h
        exact ⟨List.mem_cons_of_mem a h1, List.mem_cons_of_mem a h2⟩

private theorem rp_pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [List.map_cons, rp_pairs_cons, rp_pairs_cons, ih, List.map_append, List.map_map,
        List.map_map]
      rfl

private theorem rp_map_sum_mul_left (xs : List Int) (n : Nat) (f : Int → Nat) :
    (xs.map (fun a => n * f a)).sum = n * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, ih, Nat.mul_add]

private theorem rp_countP_replicate_flatten (a : Int) (x : Series) (n : Nat) :
    ((List.replicate n x).flatten).countP (a < ·) = n * x.countP (a < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, List.countP_append, ih, Nat.succ_mul]
      omega

/-! ### `choose2` arithmetic -/

private theorem rp_choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  unfold choose2
  have h1 : (n + 1) * ((n + 1) - 1) = n * (n - 1) + 2 * n := by
    cases n with
    | zero => decide
    | succ m =>
        have e1 : m + 1 + 1 - 1 = m + 1 := by omega
        have e2 : m + 1 - 1 = m := by omega
        rw [e1, e2]
        grind
  rw [h1]
  exact Nat.add_mul_div_left (n * (n - 1)) n (by decide : 0 < 2)

/-! ### Permutation helpers -/

private theorem rp_perm_sum : ∀ {l₁ l₂ : List Nat}, l₁.Perm l₂ → l₁.sum = l₂.sum
  | _, _, .nil => rfl
  | _, _, .cons a p => by simp [rp_perm_sum p]
  | _, _, .swap a b l => by simp; grind
  | _, _, .trans p q => (rp_perm_sum p).trans (rp_perm_sum q)

private theorem rp_perm_map_sum (f : Int → Nat) {l₁ l₂ : List Int} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  exact rp_perm_sum (p.map f)

private theorem rp_nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro ha
          have hfa : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp ha
          simp at hfa
        · have hle : (as.filter (fun b => !b == a)).length ≤ as.length :=
            List.length_filter_le (fun b => !b == a) as
          have hlt : (as.filter (fun b => !b == a)).length < n := by
            simp at hlen
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

private theorem rp_beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem rp_sum_map_eq_count_head (k : Int) (ks : List Int) (f : Int → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := by simpa [rp_beq_comm k x] using hx
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem rp_dedup_weight_sum (f : Int → Nat) (keys : List Int) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Int, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          have hle := List.length_filter_le (fun b : Int => !(b == k)) ks
          omega
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have haf : (!(a == k)) = true := (List.mem_filter.mp hamem').2
            simpa using haf
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := by simpa [rp_beq_comm k a] using hane
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using rp_sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem rp_eraseDups_cons_count (a : Int) (xs : Series) :
    ((a :: xs).eraseDups.map (fun v => choose2 ((a :: xs).count v))).sum =
      (xs.eraseDups.map (fun v => choose2 (xs.count v))).sum + xs.count a := by
  let rest := xs.filter (fun b => !(b == a))
  have hcounta : (a :: xs).count a = xs.count a + 1 := by
    simp
  have hcountv : ∀ v ∈ rest.eraseDups, (a :: xs).count v = xs.count v := by
    intro v hv
    have hvrest : v ∈ rest := List.mem_eraseDups.mp hv
    have hvne : (v == a) = false := by
      have hmems : v ∈ xs.filter (fun b : Int => !(b == a)) := by simpa only [rest] using hvrest
      have hnot : (!(v == a)) = true := (List.mem_filter (p := fun b : Int => !(b == a))).mp hmems |>.2
      cases h : (v == a) <;> simp [h] at hnot ⊢
    have hva : (a == v) = false := by simpa [rp_beq_comm a v] using hvne
    rw [List.count_cons, hva]
    simp
  rw [List.eraseDups_cons]
  simp only [List.map_cons, List.sum_cons]
  rw [hcounta, rp_choose2_succ]
  rw [show ((rest.eraseDups.map (fun v => choose2 ((a :: xs).count v))).sum) =
           ((rest.eraseDups.map (fun v => choose2 (xs.count v))).sum) by
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      rw [hcountv v hv]]
  by_cases ha : a ∈ xs
  · have hperm : xs.eraseDups.Perm (a :: rest.eraseDups) := by
      apply (List.perm_ext_iff_of_nodup (rp_nodup_eraseDups xs) (by
        apply List.nodup_cons.mpr
        constructor
        · intro hmem
          have hm' : a ∈ rest := List.mem_eraseDups.mp hmem
          have hm'' : a ∈ xs.filter (fun b => !(b == a)) := by simpa only [rest] using hm'
          simp at hm''
        · exact rp_nodup_eraseDups rest)).mpr
      intro v
      constructor
      · intro hv
        have hvxs : v ∈ xs := List.mem_eraseDups.mp hv
        by_cases hva : v == a
        · have hveq : v = a := beq_iff_eq.mp hva
          simp [hveq]
        · simp [rest, hvxs, hva]
      · intro hv
        simp only [List.mem_cons] at hv
        rcases hv with hva | hv
        · subst v
          exact List.mem_eraseDups.mpr ha
        · have hvr : v ∈ rest := (List.mem_eraseDups (l := rest)).mp hv
          have hvx : v ∈ xs := (List.mem_filter.mp (by simpa only [rest] using hvr)).1
          exact List.mem_eraseDups.mpr hvx
    have hp := rp_perm_map_sum (fun v => choose2 (xs.count v)) hperm
    simp only [List.map_cons, List.sum_cons] at hp
    omega
  · have hperm : xs.eraseDups.Perm rest.eraseDups := by
      apply (List.perm_ext_iff_of_nodup (rp_nodup_eraseDups xs) (rp_nodup_eraseDups rest)).mpr
      intro v
      constructor
      · intro hv
        have hvxs : v ∈ xs := List.mem_eraseDups.mp hv
        have hvne : v ≠ a := by intro hveq; exact ha (by simpa [hveq] using hvxs)
        exact List.mem_eraseDups.mpr (by simp [rest, hvxs, hvne])
      · intro hv
        have hvrest : v ∈ rest := List.mem_eraseDups.mp hv
        have hvxs : v ∈ xs := (List.mem_filter.mp (by simpa only [rest] using hvrest)).1
        exact List.mem_eraseDups.mpr hvxs
    have hp := rp_perm_map_sum (fun v => choose2 (xs.count v)) hperm
    have hz : xs.count a = 0 := (List.count_eq_zero).mpr ha
    rw [hp, hz]
    simp [choose2]

/-! ### Counting the three kinds of pairs -/

private def rp_eqPair (p : Int × Int) : Bool := p.1 == p.2

private theorem rp_trichotomy_count (a : Int) (xs : Series) :
    xs.countP (a < ·) + xs.countP (· < a) + xs.count a = xs.length := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.countP_cons, List.count_cons, List.length_cons]
      by_cases hab : a < b
      · have hba : ¬ b < a := by omega
        have hne : (b == a) = false := by
          simp
          omega
        simp [hab, hba, hne]
        omega
      · by_cases hba : b < a
        · have hne : (b == a) = false := by
            simp
            omega
          simp [hab, hba, hne]
          omega
        · have heq : b = a := by omega
          subst b
          simp
          omega

private theorem rp_countP_beq_comm_left (a : Int) (xs : Series) :
    xs.countP (fun b => a == b) = xs.count a := by
  calc
    xs.countP (fun b => a == b) = xs.countP (fun b => b == a) := by
      apply List.countP_congr
      intro b hb
      rw [rp_beq_comm]
    _ = xs.count a := (List.count_eq_countP).symm

private theorem rp_pair_trichotomy (x : Series) :
    (pairs x).countP rising + (pairs x).countP falling + (pairs x).countP rp_eqPair =
      choose2 x.length := by
  induction x with
  | nil => simp [pairs, choose2]
  | cons a xs ih =>
      rw [rp_pairs_cons]
      simp only [List.countP_append, List.length_cons]
      rw [rp_countP_map_comp, rp_countP_map_comp, rp_countP_map_comp]
      simp only [rising, falling, rp_eqPair]
      rw [rp_countP_beq_comm_left a xs]
      rw [rp_choose2_succ]
      have ht := rp_trichotomy_count a xs
      omega

private theorem rp_count_eq_dedup (x : Series) :
    (pairs x).countP rp_eqPair = (x.eraseDups.map (fun a => choose2 (x.count a))).sum := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [rp_pairs_cons, List.countP_append, rp_countP_map_comp]
      simp only [rp_eqPair]
      rw [ih, rp_countP_beq_comm_left a xs]
      rw [rp_eraseDups_cons_count]
      omega

private theorem rp_cart_singleton_rising (xs : Series) (a : Int) :
    (cart xs [a]).countP rising = xs.countP (· < a) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih =>
      rw [rp_cart_cons]
      simp only [List.map_cons, List.map_nil, List.countP_append, List.countP_cons,
        List.countP_nil, Nat.zero_add]
      rw [ih]
      simp [rising]
      omega

private theorem rp_rising_reverse_eq_falling (x : Series) :
    (pairs x.reverse).countP rising = (pairs x).countP falling := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      rw [rp_pairs_countP_append, rp_pairs_singleton, rp_pairs_cons, List.countP_append]
      have hcart : (cart xs.reverse [a]).countP rising = xs.countP (· < a) := by
        rw [rp_cart_singleton_rising, List.countP_reverse]
      rw [ih, hcart]
      rw [rp_countP_map_comp]
      simp [falling]
      omega

private theorem rp_falling_sorted (x : Series) (hx : x.Pairwise (· ≤ ·)) :
    (pairs x).countP falling = 0 := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.pairwise_cons] at hx
      rw [rp_pairs_cons, List.countP_append]
      have hcross : (xs.map (fun b => (a, b))).countP falling = 0 := by
        rw [List.countP_eq_zero]
        intro p hp
        rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        have hab : a ≤ b := hx.1 b hb
        have hnlt : ¬ b < a := by omega
        simp [falling, hnlt]
      rw [hcross, ih hx.2]


/-! ### RisePairs: goals -/

theorem sound_RisePairs_HistogramBridge :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.HistogramBridge.Goal := by
  intro x y
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  rw [rp_pairs_countP_append rising x y]
  have hcross : (cart x y).countP rising =
      (x.eraseDups.map (fun a => x.count a * y.countP (a < ·))).sum := by
    rw [rp_cart_countP rising x y]
    rw [show (x.map (fun a => y.countP (fun b => rising (a, b)))).sum =
           (x.map (fun a => y.countP (a < ·))).sum by rfl]
    exact (rp_dedup_weight_sum (fun a => y.countP (a < ·)) x).symm
  rw [hcross]
  simp only [Int.natCast_add]

theorem sound_RisePairs_ReverseDebt :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.ReverseDebt.Goal := by
  intro x
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  have ht := rp_pair_trichotomy x
  have heq := rp_count_eq_dedup x
  have hrev := rp_rising_reverse_eq_falling x
  omega

theorem sound_RisePairs_RankCoding :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.RankCoding.Goal := by
  intro x f hf
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  rw [rp_pairs_map]
  rw [rp_countP_map_comp]
  apply congrArg Int.ofNat
  apply List.countP_congr
  intro p hp
  have hmem := rp_mem_pairs hp
  have hiff : f p.1 < f p.2 ↔ p.1 < p.2 := hf p.1 hmem.1 p.2 hmem.2
  simp [rising, hiff]

theorem sound_RisePairs_BlockRepeat :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.BlockRepeat.Goal := by
  intro x n
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  induction n with
  | zero => simp [pairs, choose2]
  | succ n ih =>
      change ((pairs (x ++ (List.replicate n x).flatten)).countP rising : Int) = _
      rw [rp_pairs_countP_append rising x ((List.replicate n x).flatten)]
      have hcross : (cart x ((List.replicate n x).flatten)).countP rising =
          n * (cart x x).countP rising := by
        rw [rp_cart_countP rising x ((List.replicate n x).flatten), rp_cart_countP rising x x]
        simp only [rising]
        rw [show (x.map (fun a => ((List.replicate n x).flatten).countP (a < ·))).sum =
               (x.map (fun a => n * x.countP (a < ·))).sum by
            apply congrArg List.sum
            apply List.map_congr_left
            intro a ha
            exact rp_countP_replicate_flatten a x n]
        rw [rp_map_sum_mul_left]
      simp only [Int.natCast_add]
      rw [ih, hcross, rp_choose2_succ]
      simp only [Int.natCast_add, Int.natCast_mul]
      grind

theorem sound_RisePairs_MergeSorted :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.MergeSorted.Goal := by
  intro x hx
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  have ht := rp_pair_trichotomy x
  have heq := rp_count_eq_dedup x
  have hfall := rp_falling_sorted x hx
  omega

/-! ### Drawdown: max0 / pairs / extreme helpers -/

private theorem dd_flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem dd_zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem dd_pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, dd_zipIdx_add]
  rw [dd_flatMap_map]

private theorem dd_pairs_nil : pairs [] = [] := by
  unfold pairs
  rw [List.zipIdx_nil]
  rfl

private theorem dd_pairs_singleton (a : Int) : pairs [a] = [] := by
  simpa [dd_pairs_nil] using dd_pairs_cons a []

private theorem dd_cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem dd_pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, dd_pairs_cons, ih, List.map_append, List.map_map,
        Function.comp_def]

private theorem dd_cart_singleton (xs : Series) (a : Int) :
    cart xs [a] = xs.map (fun b : Int => (b, a)) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih => simp [dd_cart_cons, ih]

private theorem dd_cart_singleton_map_diff (xs : Series) (a : Int) :
    ((cart xs [a]).map (fun p : Int × Int => p.1 - p.2)) =
      xs.map (fun b : Int => b - a) := by
  rw [dd_cart_singleton]
  simp

private theorem dd_mem_of_mem_pairs {x : Series} {a b : Int} (h : (a, b) ∈ pairs x) :
    a ∈ x ∧ b ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons c cs ih =>
      rw [dd_pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨d, hd, heq⟩
        cases heq
        simp [hd]
      · have hh := ih h
        exact ⟨by simp [hh.1], by simp [hh.2]⟩

private theorem dd_pairs_append_perm (x y : Series) :
    (pairs (x ++ y)).Perm (pairs x ++ pairs y ++ cart x y) := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, dd_pairs_cons, dd_pairs_cons, dd_cart_cons]
      simp only [List.map_append]
      let A : List (Int × Int) := xs.map (fun b : Int => (a, b))
      let B : List (Int × Int) := y.map (fun b : Int => (a, b))
      let P : List (Int × Int) := pairs xs
      let Q : List (Int × Int) := pairs y
      let C : List (Int × Int) := cart xs y
      have h1 := (List.Perm.append_left (A ++ B)) ih
      have h2 := ((List.perm_append_comm (l₁ := B) (l₂ := P)).append_right (Q ++ C)).append_left A
      have h3 := ((List.perm_append_comm (l₁ := B) (l₂ := Q)).append_right C).append_left (A ++ P)
      have h1' : (A ++ (B ++ pairs (xs ++ y))).Perm (A ++ (B ++ (P ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h1
      have h2' : (A ++ (B ++ (P ++ (Q ++ C)))).Perm (A ++ (P ++ (B ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h2
      have h3' : (A ++ (P ++ (B ++ (Q ++ C)))).Perm (A ++ (P ++ (Q ++ (B ++ C)))) := by
        simpa [List.append_assoc] using h3
      simpa [List.append_assoc, A, B, P, Q, C] using (h1'.trans (h2'.trans h3'))

private theorem dd_max0_cons (a : Int) (xs : List Int) :
    max0 (a :: xs) = max 0 (max a (max0 xs)) := by
  simp only [max0, List.max?_cons]
  cases hx : xs.max? with
  | none => simp
  | some b =>
      simp only [Option.getD_some, Option.elim_some]
      change max 0 (max a b) = max 0 (max a (max 0 b))
      omega

private theorem dd_max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simp only [max0]
  omega

private theorem dd_le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      rw [dd_max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have hi := ih ha
        omega

private theorem dd_max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b)
    (h : ∀ a ∈ xs, a ≤ b) : max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons x xs ih =>
      rw [dd_max0_cons]
      exact Int.max_le.mpr ⟨hb, Int.max_le.mpr ⟨h x (by simp),
        ih (by intro a ha; exact h a (by simp [ha]))⟩⟩

private theorem dd_max0_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : max0 l₁ = max0 l₂ := by
  apply Int.le_antisymm
  · apply dd_max0_le (dd_max0_nonneg _)
    intro a ha
    exact dd_le_max0_of_mem (p.subset ha)
  · apply dd_max0_le (dd_max0_nonneg _)
    intro a ha
    exact dd_le_max0_of_mem (p.symm.subset ha)

private theorem dd_max0_append (l₁ l₂ : List Int) : max0 (l₁ ++ l₂) = max (max0 l₁) (max0 l₂) := by
  induction l₁ with
  | nil => simp [max0]
  | cons a as ih =>
      rw [List.cons_append, dd_max0_cons, ih, dd_max0_cons]
      omega

private theorem dd_max0_map_sublist {α : Type} (f : α → Int) {xs ys : List α}
    (h : xs.Sublist ys) : max0 (xs.map f) ≤ max0 (ys.map f) := by
  apply dd_max0_le (dd_max0_nonneg _)
  intro a ha
  exact dd_le_max0_of_mem ((h.map f).mem ha)

private theorem dd_max_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.max?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).max? with
      | none => exact False.elim (h (List.max?_eq_none_iff.mp hm))
      | some M => exact (List.max?_eq_some_iff.mp hm).1

private theorem dd_min_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.min?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).min? with
      | none => exact False.elim (h (List.min?_eq_none_iff.mp hm))
      | some M => exact (List.min?_eq_some_iff.mp hm).1

private theorem dd_le_max_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : a ≤ xs.max?.getD 0 := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).max? with
      | none =>
          have heq : b :: bs = [] := List.max?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.max?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

private theorem dd_min_le_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : xs.min?.getD 0 ≤ a := by
  cases xs with
  | nil => simp at ha
  | cons b bs =>
      cases hm : (b :: bs).min? with
      | none =>
          have heq : b :: bs = [] := List.min?_eq_none_iff.mp hm
          cases heq
      | some M =>
          have hspec := List.min?_eq_some_iff.mp hm
          simpa [hm] using hspec.2 a ha

private theorem dd_max_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).max?.getD 0 = max a (xs.max?.getD 0) := by
  cases hm : xs.max? with
  | none => exact False.elim (hxs (List.max?_eq_none_iff.mp hm))
  | some M => simp [List.max?_cons, hm]

private theorem dd_min_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).min?.getD 0 = min a (xs.min?.getD 0) := by
  cases hm : xs.min? with
  | none => exact False.elim (hxs (List.min?_eq_none_iff.mp hm))
  | some M => simp [List.min?_cons, hm]

private theorem dd_spread_nonneg (x : Series) : 0 ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [spread]
  · have hm := dd_min_mem_of_ne_nil hx
    have hle := dd_le_max_of_mem hm
    unfold spread
    omega

private theorem dd_max0_cart_diff (x y : Series) (hx : x ≠ []) (hy : y ≠ []) :
    max0 ((cart x y).map (fun p : Int × Int => p.1 - p.2)) =
      max 0 (x.max?.getD 0 - y.min?.getD 0) := by
  have hMmem : x.max?.getD 0 ∈ x := dd_max_mem_of_ne_nil hx
  have hmmem : y.min?.getD 0 ∈ y := dd_min_mem_of_ne_nil hy
  apply Int.le_antisymm
  · apply dd_max0_le (by omega : 0 ≤ max 0 (x.max?.getD 0 - y.min?.getD 0))
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    unfold cart at hp
    rcases List.mem_flatMap.mp hp with ⟨u, hu, hmem⟩
    rcases List.mem_map.mp hmem with ⟨b', hb', heq⟩
    cases heq
    have haM : a ≤ x.max?.getD 0 := dd_le_max_of_mem hu
    have hmb : y.min?.getD 0 ≤ b := dd_min_le_of_mem hb'
    omega
  · have hpair : (x.max?.getD 0, y.min?.getD 0) ∈ cart x y := by
      unfold cart
      apply List.mem_flatMap.mpr
      refine ⟨x.max?.getD 0, hMmem, ?_⟩
      exact List.mem_map.mpr ⟨y.min?.getD 0, hmmem, rfl⟩
    have hdiff : x.max?.getD 0 - y.min?.getD 0 ∈
        (cart x y).map (fun p : Int × Int => p.1 - p.2) :=
      List.mem_map.mpr ⟨(x.max?.getD 0, y.min?.getD 0), hpair, rfl⟩
    have hle := dd_le_max0_of_mem hdiff
    have hnonneg := dd_max0_nonneg ((cart x y).map (fun p : Int × Int => p.1 - p.2))
    omega

private theorem dd_measure_le_spread (x : Series) : Drawdown.measure x ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · apply dd_max0_le (dd_spread_nonneg x)
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    have hmem := dd_mem_of_mem_pairs hp
    have haM := dd_le_max_of_mem hmem.1
    have hmb := dd_min_le_of_mem hmem.2
    unfold spread
    change a - b ≤ x.max?.getD 0 - x.min?.getD 0
    omega

private theorem dd_measure_append_ge_left (x y : Series) :
    Drawdown.measure x ≤ Drawdown.measure (x ++ y) := by
  unfold Drawdown.measure
  apply dd_max0_le (dd_max0_nonneg _)
  intro v hv
  rcases List.mem_map.mp hv with ⟨p, hp, rfl⟩
  have hprhs : p ∈ pairs x ++ pairs y ++ cart x y := by simp [hp]
  have hp' : p ∈ pairs (x ++ y) := (dd_pairs_append_perm x y).symm.subset hprhs
  exact dd_le_max0_of_mem (List.mem_map.mpr ⟨p, hp', rfl⟩)

private theorem dd_mem_flatten_replicate {a : Int} {k : Nat} {x : Series}
    (h : a ∈ (List.replicate k x).flatten) : a ∈ x := by
  induction k with
  | zero => simp at h
  | succ k ih =>
      simp only [List.replicate, List.flatten_cons, List.mem_append] at h
      rcases h with h | h
      · exact h
      · exact ih h

private theorem dd_measure_le_of_mem (l x : Series) (hall : ∀ a ∈ l, a ∈ x) :
    Drawdown.measure l ≤ spread x := by
  apply dd_max0_le (dd_spread_nonneg x)
  intro v hv
  rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
  have hmem := dd_mem_of_mem_pairs hp
  have ha := hall a hmem.1
  have hb := hall b hmem.2
  have haM := dd_le_max_of_mem ha
  have hmb := dd_min_le_of_mem hb
  unfold spread
  change a - b ≤ x.max?.getD 0 - x.min?.getD 0
  omega

private theorem dd_measure_cons (a : Int) (xs : Series) :
    Drawdown.measure (a :: xs) =
      max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs) := by
  unfold Drawdown.measure
  rw [dd_pairs_cons]
  simp only [List.map_append, List.map_map, Function.comp_def]
  rw [dd_max0_append]

private theorem dd_pairs_reverse_perm (x : Series) :
    (pairs x.reverse).Perm ((pairs x).map (fun p : Int × Int => (p.2, p.1))) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      have hp := dd_pairs_append_perm xs.reverse [a]
      simp only [dd_pairs_singleton, List.append_nil] at hp
      rw [dd_cart_singleton] at hp
      have hrev : ((xs.reverse).map (fun b : Int => (b, a))).Perm
          (xs.map (fun b : Int => (b, a))) := by
        simp
      have h1 := List.Perm.append ih hrev
      have h2 := List.perm_append_comm
        (l₁ := (pairs xs).map (fun p : Int × Int => (p.2, p.1)))
        (l₂ := xs.map (fun b : Int => (b, a)))
      simpa [dd_pairs_cons, List.map_append, List.map_map, Function.comp_def] using
        (hp.trans (h1.trans h2))

private theorem dd_measure_reverse_append_singleton (a : Int) (xs : Series) :
    Drawdown.measure (xs.reverse ++ [a]) =
      max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a))) := by
  unfold Drawdown.measure
  have hp := (dd_pairs_append_perm xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)
  have hm := dd_max0_perm hp
  rw [hm]
  simp only [dd_pairs_singleton, List.append_nil, List.map_append]
  rw [dd_max0_append]
  have hcartmax : max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
      max0 (xs.map (fun b : Int => b - a)) := by
    calc
      max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
          max0 ((xs.reverse).map (fun b : Int => b - a)) := by
            rw [dd_cart_singleton_map_diff]
      _ = max0 (xs.map (fun b : Int => b - a)) := by
        apply dd_max0_perm
        simp
  rw [hcartmax]

/-! ### Goals -/


/-! ### Drawdown: goals -/

theorem sound_Drawdown_CrossExtreme :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.CrossExtreme.Goal := by
  intro x y hx hy
  unfold Drawdown.measure
  have hp := (dd_pairs_append_perm x y).map (fun p : Int × Int => p.1 - p.2)
  have hm := dd_max0_perm hp
  rw [hm]
  simp only [List.map_append]
  rw [dd_max0_append, dd_max0_append, dd_max0_cart_diff x y hx hy]
  have hnonneg : 0 ≤ max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) := dd_max0_nonneg _
  omega

theorem sound_Drawdown_Bidirectional :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.Bidirectional.Goal := by
  intro x
  induction x with
  | nil =>
      simp [Drawdown.measure, spread, max0, pairs]
  | cons a xs ih =>
      by_cases hxs : xs = []
      · subst xs
        simp [Drawdown.measure, spread, max0, pairs]
      · rw [List.reverse_cons, dd_measure_cons, dd_measure_reverse_append_singleton]
        have hMmem : xs.max?.getD 0 ∈ xs := dd_max_mem_of_ne_nil hxs
        have hmmem : xs.min?.getD 0 ∈ xs := dd_min_mem_of_ne_nil hxs
        have hMle : ∀ b ∈ xs, b ≤ xs.max?.getD 0 := by
          intro b hb
          exact dd_le_max_of_mem hb
        have hmle : ∀ b ∈ xs, xs.min?.getD 0 ≤ b := by
          intro b hb
          exact dd_min_le_of_mem hb
        have hspread : 0 ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          have h := hMle (xs.min?.getD 0) hmmem
          omega
        have hAge : a - xs.min?.getD 0 ≤ max0 (xs.map (fun b : Int => a - b)) :=
          dd_le_max0_of_mem (List.mem_map.mpr ⟨xs.min?.getD 0, hmmem, rfl⟩)
        have hBge : xs.max?.getD 0 - a ≤ max0 (xs.map (fun b : Int => b - a)) :=
          dd_le_max0_of_mem (List.mem_map.mpr ⟨xs.max?.getD 0, hMmem, rfl⟩)
        have hih : max (Drawdown.measure xs) (Drawdown.measure xs.reverse) =
            xs.max?.getD 0 - xs.min?.getD 0 := by
          simpa [spread] using ih
        have hD_le_m : Drawdown.measure xs ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          rw [← hih]
          exact Int.le_max_left _ _
        have hR_le_m : Drawdown.measure xs.reverse ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          rw [← hih]
          exact Int.le_max_right _ _
        have hspread_cons : spread (a :: xs) =
            max a (xs.max?.getD 0) - min a (xs.min?.getD 0) := by
          cases hm : xs.max? with
          | none => exact False.elim (hxs (List.max?_eq_none_iff.mp hm))
          | some M =>
              cases hn : xs.min? with
              | none => exact False.elim (hxs (List.min?_eq_none_iff.mp hn))
              | some m =>
                  simp [spread, List.max?_cons, List.min?_cons, hm, hn]
        rw [hspread_cons]
        by_cases hle : a ≤ xs.min?.getD 0
        · have hMge_a : 0 ≤ xs.max?.getD 0 - a := by
            have h := hMle (xs.min?.getD 0) hmmem
            omega
          have hA_le_a : max0 (xs.map (fun b : Int => a - b)) ≤ xs.max?.getD 0 - a := by
            apply dd_max0_le hMge_a
            intro v hv
            rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
            have hbm := hmle b hb
            omega
          have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ xs.max?.getD 0 - a := by
            apply dd_max0_le hMge_a
            intro v hv
            rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
            have hbM := hMle b hb
            omega
          have hB_eq : max0 (xs.map (fun b : Int => b - a)) = xs.max?.getD 0 - a :=
            Int.le_antisymm hB_le_a hBge
          have hD_le_a : Drawdown.measure xs ≤ xs.max?.getD 0 - a := by omega
          have hR_le_a : Drawdown.measure xs.reverse ≤ xs.max?.getD 0 - a := by omega
          omega
        · by_cases hge : xs.max?.getD 0 ≤ a
          · have hamge : 0 ≤ a - xs.min?.getD 0 := by
              have h := hMle (xs.min?.getD 0) hmmem
              omega
            have hA_le_a : max0 (xs.map (fun b : Int => a - b)) ≤ a - xs.min?.getD 0 := by
              apply dd_max0_le hamge
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ a - xs.min?.getD 0 := by
              apply dd_max0_le hamge
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbM := hMle b hb
              omega
            have hA_eq : max0 (xs.map (fun b : Int => a - b)) = a - xs.min?.getD 0 :=
              Int.le_antisymm hA_le_a hAge
            have hD_le_a : Drawdown.measure xs ≤ a - xs.min?.getD 0 := by omega
            have hR_le_a : Drawdown.measure xs.reverse ≤ a - xs.min?.getD 0 := by omega
            omega
          · have hRHS : max a (xs.max?.getD 0) - min a (xs.min?.getD 0) =
                xs.max?.getD 0 - xs.min?.getD 0 := by omega
            rw [hRHS]
            have hA_le_m : max0 (xs.map (fun b : Int => a - b)) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by
              apply dd_max0_le hspread
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_m : max0 (xs.map (fun b : Int => b - a)) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by
              apply dd_max0_le hspread
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbM := hMle b hb
              omega
            have hleft_le : max (max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs))
                (max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a)))) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by omega
            have hleft_ge : xs.max?.getD 0 - xs.min?.getD 0 ≤
                max (max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs))
                (max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a)))) := by omega
            exact Int.le_antisymm hleft_le hleft_ge

theorem sound_Drawdown_QuoteFlip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.QuoteFlip.Goal := by
  intro x
  unfold Drawdown.measure scaled
  rw [dd_pairs_map]
  have hneg : ((pairs x).map (fun p : Int × Int => ((-1 : Int) * p.1, (-1 : Int) * p.2))).map
        (fun p : Int × Int => p.1 - p.2) = (pairs x).map (fun p : Int × Int => p.2 - p.1) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨u, v⟩
    simp
    omega
  rw [hneg]
  have hp := (dd_pairs_reverse_perm x).map (fun p : Int × Int => p.1 - p.2)
  have hm := dd_max0_perm hp
  rw [hm]
  rw [show ((pairs x).map (fun p : Int × Int => (p.2, p.1))).map
        (fun p : Int × Int => p.1 - p.2) =
      (pairs x).map (fun p : Int × Int => p.2 - p.1) by
        rw [List.map_map]
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨u, v⟩
        simp]

private theorem dd_pairs_filter_sublist (x : Series) (keep : Int → Bool) :
    (pairs (x.filter keep)).Sublist (pairs x) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      by_cases h : keep a
      · simp [h, dd_pairs_cons]
        exact List.Sublist.append
          ((List.Sublist.map (fun b : Int => (a, b)) (List.filter_sublist : (xs.filter keep).Sublist xs))) ih
      · simp [h]
        refine ih.trans ?_
        rw [dd_pairs_cons]
        simp

theorem sound_Drawdown_DeleteMonotone :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.DeleteMonotone.Goal := by
  intro x keep
  unfold Drawdown.measure
  exact dd_max0_map_sublist (fun p : Int × Int => p.1 - p.2) (dd_pairs_filter_sublist x keep)

private theorem dd_measure_self_concat (x : Series) : Drawdown.measure (x ++ x) = spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · unfold Drawdown.measure
    have hp := (dd_pairs_append_perm x x).map (fun p : Int × Int => p.1 - p.2)
    have hm := dd_max0_perm hp
    rw [hm]
    simp only [List.map_append]
    rw [dd_max0_append, dd_max0_append, dd_max0_cart_diff x x hx hx]
    unfold spread
    have hle : max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) ≤
        x.max?.getD 0 - x.min?.getD 0 := by
      simpa [Drawdown.measure, spread] using dd_measure_le_spread x
    have hs : 0 ≤ x.max?.getD 0 - x.min?.getD 0 := by
      have hm := dd_min_mem_of_ne_nil hx
      have hle' := dd_le_max_of_mem hm
      omega
    omega

theorem sound_Drawdown_DuplicatePeriods :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.DuplicatePeriods.Goal := by
  intro x n hn
  rcases Nat.exists_eq_add_of_le hn with ⟨k, hk⟩
  subst n
  have hrep : List.replicate (2 + k) x = x :: x :: List.replicate k x := by
    rw [show 2 + k = Nat.succ (Nat.succ k) by omega]
    simp only [List.replicate_succ]
  rw [hrep]
  simp only [List.flatten_cons]
  let rest : Series := (List.replicate k x).flatten
  change Drawdown.measure (x ++ (x ++ rest)) = spread x
  have hlow : spread x ≤ Drawdown.measure (x ++ (x ++ rest)) := by
    rw [← List.append_assoc]
    rw [← dd_measure_self_concat x]
    exact dd_measure_append_ge_left (x ++ x) rest
  have hall : ∀ a ∈ x ++ (x ++ rest), a ∈ x := by
    intro a ha
    simp only [List.mem_append] at ha
    rcases ha with ha | ha | ha
    · exact ha
    · exact ha
    · exact dd_mem_flatten_replicate ha
  have hup : Drawdown.measure (x ++ (x ++ rest)) ≤ spread x :=
    dd_measure_le_of_mem (x ++ (x ++ rest)) x hall
  exact Int.le_antisymm hup hlow

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.RisePairAndDrawdownProperties
