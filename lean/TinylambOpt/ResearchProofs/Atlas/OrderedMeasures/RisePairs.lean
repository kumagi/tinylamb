import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RisePairs.BlockRepeat` — `∀ x n, measure (List.replicate n x |>.flatten) =
  (n:Int)*measure x + (choose2 n:Int)*(cart x x).countP rising`
- `RisePairs.HistogramBridge` — `∀ x y, measure (x++y) = measure x + measure y +
  ((x.eraseDups.map (fun a => x.count a * y.countP (a < ·))).sum:Nat)`
- `RisePairs.MergeSorted` — `∀ x, x.Pairwise (· ≤ ·) → measure x = (choose2
  x.length:Int) - (x.eraseDups.map (fun a => choose2 (x.count a))).sum`
- `RisePairs.RankCoding` — `∀ x (f : Int → Int), (∀ a ∈ x, ∀ b ∈ x, f a < f b ↔ a < b)
  → measure (x.map f) = measure x`
- `RisePairs.ReverseDebt` — `∀ x, measure x + measure x.reverse + (x.eraseDups.map (fun
  a => choose2 (x.count a))).sum = (choose2 x.length:Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.RisePairs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Basic list algebra for `pairs` and `cart` -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simp [pairs]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem countP_congr_bool {α : Type} (P Q : α → Bool) (l : List α)
    (h : ∀ a ∈ l, P a = Q a) : l.countP P = l.countP Q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, h a (by simp),
        ih (fun b hb => h b (by simp [hb]))]

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append, List.map_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem cart_countP (P : Int × Int → Bool) (x y : Series) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons]
      simp only [List.countP_append, List.map_cons, List.sum_cons]
      rw [countP_map_comp, ih]

private theorem mem_pairs {x : Series} {p : Int × Int} (h : p ∈ pairs x) :
    p.1 ∈ x ∧ p.2 ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons a xs ih =>
      rw [pairs_cons] at h
      rw [List.mem_append] at h
      rcases h with h | h
      · rw [List.mem_map] at h
        obtain ⟨b, hb, rfl⟩ := h
        exact ⟨List.mem_cons_self, List.mem_cons_of_mem a hb⟩
      · obtain ⟨h1, h2⟩ := ih h
        exact ⟨List.mem_cons_of_mem a h1, List.mem_cons_of_mem a h2⟩

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [List.map_cons, pairs_cons, pairs_cons, ih, List.map_append, List.map_map,
        List.map_map]
      rfl

private theorem map_sum_mul_left (xs : List Int) (n : Nat) (f : Int → Nat) :
    (xs.map (fun a => n * f a)).sum = n * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, ih, Nat.mul_add]

private theorem countP_replicate_flatten (a : Int) (x : Series) (n : Nat) :
    ((List.replicate n x).flatten).countP (a < ·) = n * x.countP (a < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, List.countP_append, ih, Nat.succ_mul]
      omega

/-! ### `choose2` arithmetic -/

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
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

private theorem perm_sum : ∀ {l₁ l₂ : List Nat}, l₁.Perm l₂ → l₁.sum = l₂.sum
  | _, _, .nil => rfl
  | _, _, .cons a p => by simp [perm_sum p]
  | _, _, .swap a b l => by simp; grind
  | _, _, .trans p q => (perm_sum p).trans (perm_sum q)

private theorem perm_map_sum (f : Int → Nat) {l₁ l₂ : List Int} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  exact perm_sum (p.map f)

private theorem nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
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

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_eq_count_head (k : Int) (ks : List Int) (f : Int → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := by simpa [beq_comm k x] using hx
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Int → Nat) (keys : List Int) :
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
          have hka : (k == a) = false := by simpa [beq_comm k a] using hane
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
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem eraseDups_cons_count (a : Int) (xs : Series) :
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
    have hva : (a == v) = false := by simpa [beq_comm a v] using hvne
    rw [List.count_cons, hva]
    simp
  rw [List.eraseDups_cons]
  simp only [List.map_cons, List.sum_cons]
  rw [hcounta, choose2_succ]
  rw [show ((rest.eraseDups.map (fun v => choose2 ((a :: xs).count v))).sum) =
           ((rest.eraseDups.map (fun v => choose2 (xs.count v))).sum) by
      apply congrArg List.sum
      apply List.map_congr_left
      intro v hv
      rw [hcountv v hv]]
  by_cases ha : a ∈ xs
  · have hperm : xs.eraseDups.Perm (a :: rest.eraseDups) := by
      apply (List.perm_ext_iff_of_nodup (nodup_eraseDups xs) (by
        apply List.nodup_cons.mpr
        constructor
        · intro hmem
          have hm' : a ∈ rest := List.mem_eraseDups.mp hmem
          have hm'' : a ∈ xs.filter (fun b => !(b == a)) := by simpa only [rest] using hm'
          simp at hm''
        · exact nodup_eraseDups rest)).mpr
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
    have hp := perm_map_sum (fun v => choose2 (xs.count v)) hperm
    simp only [List.map_cons, List.sum_cons] at hp
    omega
  · have hperm : xs.eraseDups.Perm rest.eraseDups := by
      apply (List.perm_ext_iff_of_nodup (nodup_eraseDups xs) (nodup_eraseDups rest)).mpr
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
    have hp := perm_map_sum (fun v => choose2 (xs.count v)) hperm
    have hz : xs.count a = 0 := (List.count_eq_zero).mpr ha
    rw [hp, hz]
    simp [choose2]

/-! ### Counting the three kinds of pairs -/

private def eqPair (p : Int × Int) : Bool := p.1 == p.2

private theorem trichotomy_count (a : Int) (xs : Series) :
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

private theorem countP_beq_comm_left (a : Int) (xs : Series) :
    xs.countP (fun b => a == b) = xs.count a := by
  calc
    xs.countP (fun b => a == b) = xs.countP (fun b => b == a) := by
      apply List.countP_congr
      intro b hb
      rw [beq_comm]
    _ = xs.count a := (List.count_eq_countP).symm

private theorem pair_trichotomy (x : Series) :
    (pairs x).countP rising + (pairs x).countP falling + (pairs x).countP eqPair =
      choose2 x.length := by
  induction x with
  | nil => simp [pairs, choose2]
  | cons a xs ih =>
      rw [pairs_cons]
      simp only [List.countP_append, List.length_cons]
      rw [countP_map_comp, countP_map_comp, countP_map_comp]
      simp only [rising, falling, eqPair]
      rw [countP_beq_comm_left a xs]
      rw [choose2_succ]
      have ht := trichotomy_count a xs
      omega

private theorem count_eq_dedup (x : Series) :
    (pairs x).countP eqPair = (x.eraseDups.map (fun a => choose2 (x.count a))).sum := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [pairs_cons, List.countP_append, countP_map_comp]
      simp only [eqPair]
      rw [ih, countP_beq_comm_left a xs]
      rw [eraseDups_cons_count]
      omega

private theorem cart_singleton_rising (xs : Series) (a : Int) :
    (cart xs [a]).countP rising = xs.countP (· < a) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih =>
      rw [cart_cons]
      simp only [List.map_cons, List.map_nil, List.countP_append, List.countP_cons,
        List.countP_nil, Nat.zero_add]
      rw [ih]
      simp [rising]
      omega

private theorem rising_reverse_eq_falling (x : Series) :
    (pairs x.reverse).countP rising = (pairs x).countP falling := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      rw [pairs_countP_append, pairs_singleton, pairs_cons, List.countP_append]
      have hcart : (cart xs.reverse [a]).countP rising = xs.countP (· < a) := by
        rw [cart_singleton_rising, List.countP_reverse]
      rw [ih, hcart]
      rw [countP_map_comp]
      simp [falling]
      omega

private theorem falling_sorted (x : Series) (hx : x.Pairwise (· ≤ ·)) :
    (pairs x).countP falling = 0 := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.pairwise_cons] at hx
      rw [pairs_cons, List.countP_append]
      have hcross : (xs.map (fun b => (a, b))).countP falling = 0 := by
        rw [List.countP_eq_zero]
        intro p hp
        rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        have hab : a ≤ b := hx.1 b hb
        have hnlt : ¬ b < a := by omega
        simp [falling, hnlt]
      rw [hcross, ih hx.2]

/-! ### The five goals -/

theorem sound_HistogramBridge :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.HistogramBridge.Goal := by
  intro x y
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  rw [pairs_countP_append rising x y]
  have hcross : (cart x y).countP rising =
      (x.eraseDups.map (fun a => x.count a * y.countP (a < ·))).sum := by
    rw [cart_countP rising x y]
    rw [show (x.map (fun a => y.countP (fun b => rising (a, b)))).sum =
           (x.map (fun a => y.countP (a < ·))).sum by rfl]
    exact (dedup_weight_sum (fun a => y.countP (a < ·)) x).symm
  rw [hcross]
  simp only [Int.natCast_add]

theorem sound_ReverseDebt :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.ReverseDebt.Goal := by
  intro x
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  have ht := pair_trichotomy x
  have heq := count_eq_dedup x
  have hrev := rising_reverse_eq_falling x
  omega

theorem sound_RankCoding :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.RankCoding.Goal := by
  intro x f hf
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  rw [pairs_map]
  rw [countP_map_comp]
  apply congrArg Int.ofNat
  apply List.countP_congr
  intro p hp
  have hmem := mem_pairs hp
  have hiff : f p.1 < f p.2 ↔ p.1 < p.2 := hf p.1 hmem.1 p.2 hmem.2
  simp [rising, hiff]

theorem sound_BlockRepeat :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.BlockRepeat.Goal := by
  intro x n
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  induction n with
  | zero => simp [pairs, choose2]
  | succ n ih =>
      change ((pairs (x ++ (List.replicate n x).flatten)).countP rising : Int) = _
      rw [pairs_countP_append rising x ((List.replicate n x).flatten)]
      have hcross : (cart x ((List.replicate n x).flatten)).countP rising =
          n * (cart x x).countP rising := by
        rw [cart_countP rising x ((List.replicate n x).flatten), cart_countP rising x x]
        simp only [rising]
        rw [show (x.map (fun a => ((List.replicate n x).flatten).countP (a < ·))).sum =
               (x.map (fun a => n * x.countP (a < ·))).sum by
            apply congrArg List.sum
            apply List.map_congr_left
            intro a ha
            exact countP_replicate_flatten a x n]
        rw [map_sum_mul_left]
      simp only [Int.natCast_add]
      rw [ih, hcross, choose2_succ]
      simp only [Int.natCast_add, Int.natCast_mul]
      grind

theorem sound_MergeSorted :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.MergeSorted.Goal := by
  intro x hx
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs.measure
  have ht := pair_trichotomy x
  have heq := count_eq_dedup x
  have hfall := falling_sorted x hx
  omega

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.RisePairs
