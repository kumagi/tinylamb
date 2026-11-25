import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_PlateauFrames_Remaining
import TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_BestFrame_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures_WorkA

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)


-- ===== RisePairs =====
namespace RisePairsProofs

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

end RisePairsProofs


-- ===== Drawdown =====
namespace DrawdownProofs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### Generic helpers (pairs, max0, extremes) -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem pairs_nil : pairs [] = [] := by
  unfold pairs
  rw [List.zipIdx_nil]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simpa [pairs_nil] using pairs_cons a []

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, pairs_cons, ih, List.map_append, List.map_map,
        Function.comp_def]

private theorem cart_singleton (xs : Series) (a : Int) :
    cart xs [a] = xs.map (fun b : Int => (b, a)) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih => simp [cart_cons, ih]

private theorem cart_singleton_map_diff (xs : Series) (a : Int) :
    ((cart xs [a]).map (fun p : Int × Int => p.1 - p.2)) =
      xs.map (fun b : Int => b - a) := by
  rw [cart_singleton]
  simp

private theorem mem_of_mem_pairs {x : Series} {a b : Int} (h : (a, b) ∈ pairs x) :
    a ∈ x ∧ b ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons c cs ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨d, hd, heq⟩
        cases heq
        simp [hd]
      · have hh := ih h
        exact ⟨by simp [hh.1], by simp [hh.2]⟩

private theorem pairs_append_perm (x y : Series) :
    (pairs (x ++ y)).Perm (pairs x ++ pairs y ++ cart x y) := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
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

private theorem max0_cons (a : Int) (xs : List Int) :
    max0 (a :: xs) = max 0 (max a (max0 xs)) := by
  simp only [max0, List.max?_cons]
  cases hx : xs.max? with
  | none => simp
  | some b =>
      simp only [Option.getD_some, Option.elim_some]
      change max 0 (max a b) = max 0 (max a (max 0 b))
      omega

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simp only [max0]
  omega

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      rw [max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have hi := ih ha
        omega

private theorem max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b)
    (h : ∀ a ∈ xs, a ≤ b) : max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons x xs ih =>
      rw [max0_cons]
      exact Int.max_le.mpr ⟨hb, Int.max_le.mpr ⟨h x (by simp),
        ih (by intro a ha; exact h a (by simp [ha]))⟩⟩

private theorem max0_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : max0 l₁ = max0 l₂ := by
  apply Int.le_antisymm
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.subset ha)
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.symm.subset ha)

private theorem max0_append (l₁ l₂ : List Int) : max0 (l₁ ++ l₂) = max (max0 l₁) (max0 l₂) := by
  induction l₁ with
  | nil => simp [max0]
  | cons a as ih =>
      rw [List.cons_append, max0_cons, ih, max0_cons]
      omega

private theorem max0_map_sublist {α : Type} (f : α → Int) {xs ys : List α}
    (h : xs.Sublist ys) : max0 (xs.map f) ≤ max0 (ys.map f) := by
  apply max0_le (max0_nonneg _)
  intro a ha
  exact le_max0_of_mem ((h.map f).mem ha)

private theorem max_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.max?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).max? with
      | none => exact False.elim (h (List.max?_eq_none_iff.mp hm))
      | some M => exact (List.max?_eq_some_iff.mp hm).1

private theorem min_mem_of_ne_nil {xs : Series} (h : xs ≠ []) : xs.min?.getD 0 ∈ xs := by
  cases xs with
  | nil => contradiction
  | cons a as =>
      cases hm : (a :: as).min? with
      | none => exact False.elim (h (List.min?_eq_none_iff.mp hm))
      | some M => exact (List.min?_eq_some_iff.mp hm).1

private theorem le_max_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : a ≤ xs.max?.getD 0 := by
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

private theorem min_le_of_mem {xs : Series} {a : Int} (ha : a ∈ xs) : xs.min?.getD 0 ≤ a := by
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

private theorem max_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).max?.getD 0 = max a (xs.max?.getD 0) := by
  cases hm : xs.max? with
  | none => exact False.elim (hxs (List.max?_eq_none_iff.mp hm))
  | some M => simp [List.max?_cons, hm]

private theorem min_cons_eq_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).min?.getD 0 = min a (xs.min?.getD 0) := by
  cases hm : xs.min? with
  | none => exact False.elim (hxs (List.min?_eq_none_iff.mp hm))
  | some M => simp [List.min?_cons, hm]

private theorem spread_nonneg (x : Series) : 0 ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [spread]
  · have hm := min_mem_of_ne_nil hx
    have hle := le_max_of_mem hm
    unfold spread
    omega

private theorem max0_cart_diff (x y : Series) (hx : x ≠ []) (hy : y ≠ []) :
    max0 ((cart x y).map (fun p : Int × Int => p.1 - p.2)) =
      max 0 (x.max?.getD 0 - y.min?.getD 0) := by
  have hMmem : x.max?.getD 0 ∈ x := max_mem_of_ne_nil hx
  have hmmem : y.min?.getD 0 ∈ y := min_mem_of_ne_nil hy
  apply Int.le_antisymm
  · apply max0_le (by omega : 0 ≤ max 0 (x.max?.getD 0 - y.min?.getD 0))
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    unfold cart at hp
    rcases List.mem_flatMap.mp hp with ⟨u, hu, hmem⟩
    rcases List.mem_map.mp hmem with ⟨b', hb', heq⟩
    cases heq
    have haM : a ≤ x.max?.getD 0 := le_max_of_mem hu
    have hmb : y.min?.getD 0 ≤ b := min_le_of_mem hb'
    omega
  · have hpair : (x.max?.getD 0, y.min?.getD 0) ∈ cart x y := by
      unfold cart
      apply List.mem_flatMap.mpr
      refine ⟨x.max?.getD 0, hMmem, ?_⟩
      exact List.mem_map.mpr ⟨y.min?.getD 0, hmmem, rfl⟩
    have hdiff : x.max?.getD 0 - y.min?.getD 0 ∈
        (cart x y).map (fun p : Int × Int => p.1 - p.2) :=
      List.mem_map.mpr ⟨(x.max?.getD 0, y.min?.getD 0), hpair, rfl⟩
    have hle := le_max0_of_mem hdiff
    have hnonneg := max0_nonneg ((cart x y).map (fun p : Int × Int => p.1 - p.2))
    omega

private theorem measure_le_spread (x : Series) : Drawdown.measure x ≤ spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · apply max0_le (spread_nonneg x)
    intro v hv
    rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
    have hmem := mem_of_mem_pairs hp
    have haM := le_max_of_mem hmem.1
    have hmb := min_le_of_mem hmem.2
    unfold spread
    change a - b ≤ x.max?.getD 0 - x.min?.getD 0
    omega

private theorem measure_append_ge_left (x y : Series) :
    Drawdown.measure x ≤ Drawdown.measure (x ++ y) := by
  unfold Drawdown.measure
  apply max0_le (max0_nonneg _)
  intro v hv
  rcases List.mem_map.mp hv with ⟨p, hp, rfl⟩
  have hprhs : p ∈ pairs x ++ pairs y ++ cart x y := by simp [hp]
  have hp' : p ∈ pairs (x ++ y) := (pairs_append_perm x y).symm.subset hprhs
  exact le_max0_of_mem (List.mem_map.mpr ⟨p, hp', rfl⟩)

private theorem mem_flatten_replicate {a : Int} {k : Nat} {x : Series}
    (h : a ∈ (List.replicate k x).flatten) : a ∈ x := by
  induction k with
  | zero => simp at h
  | succ k ih =>
      simp only [List.replicate, List.flatten_cons, List.mem_append] at h
      rcases h with h | h
      · exact h
      · exact ih h

private theorem measure_le_of_mem (l x : Series) (hall : ∀ a ∈ l, a ∈ x) :
    Drawdown.measure l ≤ spread x := by
  apply max0_le (spread_nonneg x)
  intro v hv
  rcases List.mem_map.mp hv with ⟨⟨a, b⟩, hp, rfl⟩
  have hmem := mem_of_mem_pairs hp
  have ha := hall a hmem.1
  have hb := hall b hmem.2
  have haM := le_max_of_mem ha
  have hmb := min_le_of_mem hb
  unfold spread
  change a - b ≤ x.max?.getD 0 - x.min?.getD 0
  omega

private theorem measure_cons (a : Int) (xs : Series) :
    Drawdown.measure (a :: xs) =
      max (max0 (xs.map (fun b : Int => a - b))) (Drawdown.measure xs) := by
  unfold Drawdown.measure
  rw [pairs_cons]
  simp only [List.map_append, List.map_map, Function.comp_def]
  rw [max0_append]

private theorem pairs_reverse_perm (x : Series) :
    (pairs x.reverse).Perm ((pairs x).map (fun p : Int × Int => (p.2, p.1))) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      have hp := pairs_append_perm xs.reverse [a]
      simp only [pairs_singleton, List.append_nil] at hp
      rw [cart_singleton] at hp
      have hrev : ((xs.reverse).map (fun b : Int => (b, a))).Perm
          (xs.map (fun b : Int => (b, a))) := by
        simp
      have h1 := List.Perm.append ih hrev
      have h2 := List.perm_append_comm
        (l₁ := (pairs xs).map (fun p : Int × Int => (p.2, p.1)))
        (l₂ := xs.map (fun b : Int => (b, a)))
      simpa [pairs_cons, List.map_append, List.map_map, Function.comp_def] using
        (hp.trans (h1.trans h2))

private theorem measure_reverse_append_singleton (a : Int) (xs : Series) :
    Drawdown.measure (xs.reverse ++ [a]) =
      max (Drawdown.measure xs.reverse) (max0 (xs.map (fun b : Int => b - a))) := by
  unfold Drawdown.measure
  have hp := (pairs_append_perm xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  simp only [pairs_singleton, List.append_nil, List.map_append]
  rw [max0_append]
  have hcartmax : max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
      max0 (xs.map (fun b : Int => b - a)) := by
    calc
      max0 ((cart xs.reverse [a]).map (fun p : Int × Int => p.1 - p.2)) =
          max0 ((xs.reverse).map (fun b : Int => b - a)) := by
            rw [cart_singleton_map_diff]
      _ = max0 (xs.map (fun b : Int => b - a)) := by
        apply max0_perm
        simp
  rw [hcartmax]

/-! ### Goals -/

theorem sound_CrossExtreme :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.CrossExtreme.Goal := by
  intro x y hx hy
  unfold Drawdown.measure
  have hp := (pairs_append_perm x y).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  simp only [List.map_append]
  rw [max0_append, max0_append, max0_cart_diff x y hx hy]
  have hnonneg : 0 ≤ max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) := max0_nonneg _
  omega

theorem sound_Bidirectional :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.Bidirectional.Goal := by
  intro x
  induction x with
  | nil =>
      simp [Drawdown.measure, spread, max0, pairs]
  | cons a xs ih =>
      by_cases hxs : xs = []
      · subst xs
        simp [Drawdown.measure, spread, max0, pairs]
      · rw [List.reverse_cons, measure_cons, measure_reverse_append_singleton]
        have hMmem : xs.max?.getD 0 ∈ xs := max_mem_of_ne_nil hxs
        have hmmem : xs.min?.getD 0 ∈ xs := min_mem_of_ne_nil hxs
        have hMle : ∀ b ∈ xs, b ≤ xs.max?.getD 0 := by
          intro b hb
          exact le_max_of_mem hb
        have hmle : ∀ b ∈ xs, xs.min?.getD 0 ≤ b := by
          intro b hb
          exact min_le_of_mem hb
        have hspread : 0 ≤ xs.max?.getD 0 - xs.min?.getD 0 := by
          have h := hMle (xs.min?.getD 0) hmmem
          omega
        have hAge : a - xs.min?.getD 0 ≤ max0 (xs.map (fun b : Int => a - b)) :=
          le_max0_of_mem (List.mem_map.mpr ⟨xs.min?.getD 0, hmmem, rfl⟩)
        have hBge : xs.max?.getD 0 - a ≤ max0 (xs.map (fun b : Int => b - a)) :=
          le_max0_of_mem (List.mem_map.mpr ⟨xs.max?.getD 0, hMmem, rfl⟩)
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
            apply max0_le hMge_a
            intro v hv
            rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
            have hbm := hmle b hb
            omega
          have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ xs.max?.getD 0 - a := by
            apply max0_le hMge_a
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
              apply max0_le hamge
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_a : max0 (xs.map (fun b : Int => b - a)) ≤ a - xs.min?.getD 0 := by
              apply max0_le hamge
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
              apply max0_le hspread
              intro v hv
              rcases List.mem_map.mp hv with ⟨b, hb, rfl⟩
              have hbm := hmle b hb
              omega
            have hB_le_m : max0 (xs.map (fun b : Int => b - a)) ≤
                xs.max?.getD 0 - xs.min?.getD 0 := by
              apply max0_le hspread
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

theorem sound_QuoteFlip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.QuoteFlip.Goal := by
  intro x
  unfold Drawdown.measure scaled
  rw [pairs_map]
  have hneg : ((pairs x).map (fun p : Int × Int => ((-1 : Int) * p.1, (-1 : Int) * p.2))).map
        (fun p : Int × Int => p.1 - p.2) = (pairs x).map (fun p : Int × Int => p.2 - p.1) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨u, v⟩
    simp
    omega
  rw [hneg]
  have hp := (pairs_reverse_perm x).map (fun p : Int × Int => p.1 - p.2)
  have hm := max0_perm hp
  rw [hm]
  rw [show ((pairs x).map (fun p : Int × Int => (p.2, p.1))).map
        (fun p : Int × Int => p.1 - p.2) =
      (pairs x).map (fun p : Int × Int => p.2 - p.1) by
        rw [List.map_map]
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨u, v⟩
        simp]

private theorem pairs_filter_sublist (x : Series) (keep : Int → Bool) :
    (pairs (x.filter keep)).Sublist (pairs x) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      by_cases h : keep a
      · simp [h, pairs_cons]
        exact List.Sublist.append
          ((List.Sublist.map (fun b : Int => (a, b)) (List.filter_sublist : (xs.filter keep).Sublist xs))) ih
      · simp [h]
        refine ih.trans ?_
        rw [pairs_cons]
        simp

theorem sound_DeleteMonotone :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.Drawdown.DeleteMonotone.Goal := by
  intro x keep
  unfold Drawdown.measure
  exact max0_map_sublist (fun p : Int × Int => p.1 - p.2) (pairs_filter_sublist x keep)

private theorem measure_self_concat (x : Series) : Drawdown.measure (x ++ x) = spread x := by
  by_cases hx : x = []
  · subst x
    simp [Drawdown.measure, spread, max0, pairs]
  · unfold Drawdown.measure
    have hp := (pairs_append_perm x x).map (fun p : Int × Int => p.1 - p.2)
    have hm := max0_perm hp
    rw [hm]
    simp only [List.map_append]
    rw [max0_append, max0_append, max0_cart_diff x x hx hx]
    unfold spread
    have hle : max0 ((pairs x).map (fun p : Int × Int => p.1 - p.2)) ≤
        x.max?.getD 0 - x.min?.getD 0 := by
      simpa [Drawdown.measure, spread] using measure_le_spread x
    have hs : 0 ≤ x.max?.getD 0 - x.min?.getD 0 := by
      have hm := min_mem_of_ne_nil hx
      have hle' := le_max_of_mem hm
      omega
    omega

theorem sound_DuplicatePeriods :
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
    rw [← measure_self_concat x]
    exact measure_append_ge_left (x ++ x) rest
  have hall : ∀ a ∈ x ++ (x ++ rest), a ∈ x := by
    intro a ha
    simp only [List.mem_append] at ha
    rcases ha with ha | ha | ha
    · exact ha
    · exact ha
    · exact mem_flatten_replicate ha
  have hup : Drawdown.measure (x ++ (x ++ rest)) ≤ spread x :=
    measure_le_of_mem (x ++ (x ++ rest)) x hall
  exact Int.le_antisymm hup hlow

end DrawdownProofs


-- ===== PlateauFrames =====
namespace PlateauFramesProofs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list helpers -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega
  · have he : n = 2 * (n / 2) + 1 := by omega
    rw [he]
    have hsub : 2 * (n / 2) + 1 - 1 = 2 * (n / 2) := by omega
    rw [hsub]
    have hd : 2 ∣ (2 * (n / 2) + 1) * (2 * (n / 2)) := by
      refine ⟨(2 * (n / 2) + 1) * (n / 2), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_twice n
  have h2 := choose2_twice (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

/-! ### Window enumeration -/

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

/-! ### Leading-run counts -/

private def leadCount (a : Int) (x : Series) : Nat := (x.takeWhile (fun b => b == a)).length

private theorem count_range_takeWhile (a : Int) (xs : Series) :
    (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) =
      leadCount a xs := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · rw [List.length_cons, List.range_succ_eq_map, List.countP_cons, countP_map_comp]
        simp [List.take_succ_cons, hba, ih]
      · simp [hba, List.length_cons, List.range_succ_eq_map, countP_map_comp,
          List.take_succ_cons]

private theorem measure_cons (a : Int) (xs : Series) :
    measure (a :: xs) = (1 : Int) + (leadCount a xs : Int) + measure xs := by
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure
  rw [windows_cons_prefix]
  rw [List.countP_append, List.countP_append]
  have hsingle :
      ([[a]] : List (List Int)).countP (fun w => w.all (fun c => some c == w.head?)) = 1 := by
    simp
  rw [hsingle]
  have hmid :
      ((List.range xs.length).map (fun i => a :: xs.take (i + 1))).countP
          (fun w => w.all (fun c => some c == w.head?)) =
        (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) := by
    rw [countP_map_comp]
    congr 1
    funext i
    simp
  rw [hmid, count_range_takeWhile]
  simp

private theorem measure_nil :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure ([] : Series) = 0 := rfl

private theorem takeWhile_some_eq (a : Int) (xs : Series) :
    xs.takeWhile (fun b => some b == some a) = xs.takeWhile (fun b => b == a) := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      by_cases hba : b == a
      · have hopt : some b == some a := by simp [hba]
        simp [hba, hopt, ih]
      · have hopt : (some b == some a) = false := by simp [hba]
        simp [hba, hopt]

private theorem matchesFirst_cons (a : Int) (xs : Series) :
    matchesFirst (a :: xs) = leadCount a xs + 1 := by
  unfold matchesFirst leadCount
  simp only [List.head?_cons]
  rw [takeWhile_some_eq]
  simp

private theorem matchesFirst_nil : matchesFirst ([] : Series) = 0 := rfl

private theorem leadCount_le_length (a : Int) (x : Series) : leadCount a x ≤ x.length := by
  unfold leadCount
  induction x with
  | nil => simp
  | cons b bs ih => by_cases h : b == a <;> simp [h, ih]

private theorem leadCount_append (a : Int) (xs y : Series) :
    (xs ++ y).takeWhile (fun b => b == a) =
      (xs.takeWhile (fun b => b == a)) ++
        (if (xs.takeWhile (fun b => b == a)).length = xs.length then
          y.takeWhile (fun b => b == a)
        else []) := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · simp only [List.cons_append, List.takeWhile_cons, hba, Bool.true_and,
          List.length_cons, Nat.succ.injEq]
        rw [ih]
        split <;> simp_all
      · simp only [List.cons_append, List.takeWhile_cons, hba, List.length_cons]
        have hne : bs.length + 1 ≠ (bs.takeWhile (fun b => b == a)).length := by
          have hle := leadCount_le_length a bs
          unfold leadCount at hle
          omega
        simp [hne]

private theorem leadCount_append_count (a : Int) (xs y : Series) :
    leadCount a (xs ++ y) = leadCount a xs +
      (if leadCount a xs = xs.length then leadCount a y else 0) := by
  unfold leadCount
  rw [leadCount_append]
  by_cases h : (xs.takeWhile (fun b => b == a)).length = xs.length <;> simp [h]

private theorem leadCount_of_head_eq (a : Int) (x : Series) (h : x.head? = some a) :
    leadCount a x = matchesFirst x := by
  unfold leadCount matchesFirst
  simp [h]

private theorem leadCount_of_head_ne (a : Int) (x : Series) (h : x.head? ≠ some a) :
    leadCount a x = 0 := by
  cases x with
  | nil => rfl
  | cons b bs =>
      simp only [List.head?_cons] at h
      have hba : b ≠ a := by
        intro heq
        apply h
        simp [heq]
      unfold leadCount
      simp [hba]

private theorem repeated_length (n : Nat) (a : Int) : (repeated n a).length = n := by
  unfold repeated
  simp

private theorem leadCount_repeated (n : Nat) (a : Int) : leadCount a (repeated n a) = n := by
  unfold leadCount repeated
  simp

private theorem repeated_reverse (n : Nat) (a : Int) : (repeated n a).reverse = repeated n a := by
  unfold repeated
  simp

/-! ### Uniform -/

theorem sound_Uniform :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.Uniform.Goal := by
  intro n a
  induction n with
  | zero => simp [repeated, measure_nil, choose2]
  | succ n ih =>
      change measure (a :: repeated n a) = (choose2 (n + 2) : Int)
      rw [measure_cons, leadCount_repeated, ih, choose2_succ (n + 1)]
      push_cast
      omega

/-! ### InjectiveCodes -/

private theorem leadCount_map_eq (f : Int → Int) (a : Int) (xs : Series)
    (hpair : ∀ c ∈ xs, ∀ d ∈ xs, f c = f d → c = d)
    (hhead : ∀ c ∈ xs, f c = f a → c = a) :
    leadCount (f a) (xs.map f) = leadCount a xs := by
  induction xs with
  | nil => rfl
  | cons c cs ih =>
      have hpair' : ∀ d ∈ cs, ∀ e ∈ cs, f d = f e → d = e := by
        intro d hd e he heq
        exact hpair d (by simp [hd]) e (by simp [he]) heq
      have hhead' : ∀ d ∈ cs, f d = f a → d = a := by
        intro d hd heq
        exact hhead d (by simp [hd]) heq
      by_cases hca : c == a
      · have hcaeq : c = a := by simpa using hca
        subst c
        simpa [leadCount] using ih hpair' hhead'
      · have hfc : f c ≠ f a := by
          intro heq
          have hcaeq : c = a := hhead c (by simp) heq
          exact hca (by simp [hcaeq])
        simp [leadCount, hca, hfc]

theorem sound_InjectiveCodes :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.InjectiveCodes.Goal := by
  intro x f hf
  induction x with
  | nil => simp [measure_nil]
  | cons a xs ih =>
      have hf' : ∀ b ∈ xs, ∀ c ∈ xs, f b = f c → b = c := by
        intro b hb c hc heq
        exact hf b (by simp [hb]) c (by simp [hc]) heq
      have hhead : ∀ b ∈ xs, f b = f a → b = a := by
        intro b hb heq
        exact hf b (by simp [hb]) a (by simp) heq
      rw [List.map_cons, measure_cons, ih hf', measure_cons,
        leadCount_map_eq f a xs hf' hhead]



/-! ### TieFence -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using (eq_comm : a = b ↔ b = a)

private def tieCount (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem tieCount_cons (a : Int) (xs : Series) :
    tieCount (a :: xs) = tieCount xs + xs.countP (fun b => b == a) := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  have hbeq : (fun x : Int => a == x) = (fun x : Int => x == a) := by
    funext x
    exact beq_comm_int a x
  rw [hbeq]
  omega

private theorem leadCount_le_count (a : Int) (xs : Series) :
    leadCount a xs ≤ xs.countP (fun b => b == a) := by
  induction xs with
  | nil => simp [leadCount]
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · simp [hba]
        omega
      · simp [hba]

theorem sound_TieFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.TieFence.Goal := by
  intro x
  induction x with
  | nil => simp [measure_nil, size, TiePairs.measure]
  | cons a xs ih =>
      rw [measure_cons]
      dsimp [size, TiePairs.measure] at ih ⊢
      change measure xs ≤ (xs.length : Int) + (tieCount xs : Int) at ih
      change (1 : Int) + (leadCount a xs : Int) + measure xs ≤
        (xs.length : Int) + 1 + (tieCount (a :: xs) : Int)
      rw [tieCount_cons]
      have hlead : (leadCount a xs : Int) ≤ (xs.countP (fun b => b == a) : Int) := by
        exact_mod_cast leadCount_le_count a xs
      push_cast
      omega

/-! ### BoundaryRuns -/

private theorem leadCount_eq_length_iff (a : Int) (xs : Series) :
    leadCount a xs = xs.length ↔ xs.all (fun b => b == a) = true := by
  induction xs with
  | nil => simp [leadCount]
  | cons b bs ih =>
      by_cases hba : b == a
      · unfold leadCount at ih ⊢
        simp [hba, ih]
      · have hlead0 : leadCount a (b :: bs) = 0 := by
          unfold leadCount
          simp [hba]
        have hallfalse : (b :: bs).all (fun c => c == a) = false := by simp [hba]
        rw [hlead0, hallfalse]
        simp

private theorem leadCount_append_matches (a : Int) (xs y : Series) :
    (leadCount a (xs ++ y) : Int) = (leadCount a xs : Int) +
      (if xs.all (fun b => b == a) = true ∧ y.head? = some a then (matchesFirst y : Int) else 0) := by
  have hnat := leadCount_append_count a xs y
  by_cases hall : xs.all (fun b => b == a) = true
  · have hlen : leadCount a xs = xs.length := (leadCount_eq_length_iff a xs).2 hall
    rw [if_pos hlen] at hnat
    push_cast at hnat
    rw [hnat]
    push_cast
    by_cases hy : y.head? = some a
    · rw [if_pos ⟨hall, hy⟩, leadCount_of_head_eq a y hy]
    · rw [if_neg (fun h : xs.all (fun b => b == a) = true ∧ y.head? = some a => hy h.2)]
      rw [leadCount_of_head_ne a y hy]
      simp
  · have hlen : leadCount a xs ≠ xs.length := by
      intro h
      exact hall ((leadCount_eq_length_iff a xs).1 h)
    rw [if_neg hlen] at hnat
    push_cast at hnat
    rw [hnat]
    simp [hall]

private theorem matchesFirst_append_single (ys : Series) (a : Int) :
    matchesFirst (ys ++ [a]) = matchesFirst ys +
      (if ys.all (fun b => b == a) = true then 1 else 0) := by
  induction ys with
  | nil =>
      simp [matchesFirst_cons, matchesFirst_nil, leadCount]
  | cons b bs ih =>
      by_cases hba : b == a
      · have hbeq : b = a := by simpa using hba
        subst b
        rw [List.cons_append, matchesFirst_cons, matchesFirst_cons, leadCount_append_count]
        have hlen : leadCount a bs = bs.length ↔ bs.all (fun d => d == a) = true :=
          leadCount_eq_length_iff a bs
        have hlaa : leadCount a [a] = 1 := by simp [leadCount]
        have hallc : (a :: bs).all (fun d => d == a) = bs.all (fun d => d == a) := by simp
        rw [hlaa, hallc]
        by_cases hl : leadCount a bs = bs.length
        · rw [if_pos hl, if_pos (hlen.1 hl)]
        · rw [if_neg hl, if_neg (fun h => hl (hlen.2 h))]
      · have hbaFalse : (b == a) = false := by
          cases h : b == a <;> simp_all
        have hab : (a == b) = false := by
          rw [beq_comm_int]
          exact hbaFalse
        have hallfalse : (b :: bs).all (fun c => c == a) = false := by simp [hbaFalse]
        have hlead : leadCount b (bs ++ [a]) = leadCount b bs := by
          rw [leadCount_append_count]
          have hlen : leadCount b bs = bs.length ↔ bs.all (fun d => d == b) = true :=
            leadCount_eq_length_iff b bs
          by_cases hl : leadCount b bs = bs.length
          · rw [if_pos hl]
            have hzero : leadCount b [a] = 0 := by
              unfold leadCount
              simp [hab]
            rw [hzero]
            rw [Nat.add_zero]
          · rw [if_neg hl]
            rw [Nat.add_zero]
        rw [List.cons_append, matchesFirst_cons, matchesFirst_cons, hlead, hallfalse]
        simp

private def tailRunLen (x : Series) : Nat := matchesFirst x.reverse

private theorem tailRunLen_cons (a : Int) (xs : Series) :
    tailRunLen (a :: xs) =
      tailRunLen xs + (if (a :: xs).all (fun b => b == a) = true then 1 else 0) := by
  unfold tailRunLen
  rw [List.reverse_cons, matchesFirst_append_single]
  congr 1
  rw [List.all_reverse]
  simp [List.all_cons]

private theorem matchesFirst_singleton (a : Int) : matchesFirst [a] = 1 := by
  rw [matchesFirst_cons]
  simp [leadCount]

private theorem getLast?_eq_of_all : ∀ (x : Series) (a : Int), x ≠ [] →
    x.all (fun b => b == a) = true → x.getLast? = some a
  | [], a, hne, hall => by contradiction
  | b :: bs, a, hne, hall => by
      simp only [List.all_cons] at hall
      by_cases hba : b == a
      · have hbsall : bs.all (fun d => d == a) = true := by
          simpa [hba] using hall
        rw [List.getLast?_cons]
        cases hgl : bs.getLast? with
        | none =>
            have hbeq : b = a := by simpa using hba
            simp [hgl, hbeq]
        | some d =>
            have hnebs : bs ≠ [] := by
              intro h
              subst bs
              simp at hgl
            have hres := getLast?_eq_of_all bs a hnebs hbsall
            simp [hgl] at hres
            simp [hgl, hres]
      · simp [hba] at hall

private theorem cast_ite_nat_int (h : Prop) [Decidable h] (n : Nat) :
    ((if h then n else 0 : Nat) : Int) = if h then (n : Int) else 0 := by
  by_cases hh : h <;> simp [hh]

private def cross (x y : Series) : Nat :=
  match x with
  | [] => 0
  | a :: xs =>
      cross xs y +
        (if xs.all (fun b => b == a) = true ∧ y.head? = some a then matchesFirst y else 0)
termination_by x.length

private theorem cross_cons (a : Int) (xs y : Series) :
    cross (a :: xs) y = cross xs y +
      (if xs.all (fun b => b == a) = true ∧ y.head? = some a then matchesFirst y else 0) := by
  rw [cross]

private theorem measure_append_cross (x y : Series) :
    measure (x ++ y) = measure x + measure y + (cross x y : Int) := by
  induction x with
  | nil => simp [cross, measure_nil]
  | cons a xs ih =>
      rw [List.cons_append, measure_cons, measure_cons, ih, cross_cons]
      push_cast
      rw [leadCount_append_matches, cast_ite_nat_int]
      omega

private theorem getLast?_eq_some_getD (x : Series) (a : Int) (hne : x ≠ []) :
    x.getLast? = some (x.getLast?.getD a) := by
  cases x with
  | nil => contradiction
  | cons a xs => simp [List.getLast?_cons]

private theorem seam_plateau (x y : Series) :
    seam x y (fun a b => if a == b then (matchesFirst x.reverse : Int) * matchesFirst y else 0) =
      if x.getLast? == y.head? then (matchesFirst x.reverse : Int) * matchesFirst y else 0 := by
  cases x with
  | nil => cases y <;> simp [seam, matchesFirst_nil]
  | cons a xs =>
      cases y with
      | nil => simp [seam, matchesFirst_nil]
      | cons b ys =>
          unfold seam
          simp only [List.getLast?_cons, List.head?_cons, Option.bind_some,
            Option.map_some, Option.getD_some]
          have hopt : (some (xs.getLast?.getD a) == some b) =
              (xs.getLast?.getD a == b) := by simp
          rw [hopt]

private theorem cross_eq_seam (x y : Series) :
    (cross x y : Int) =
      if x.getLast? == y.head? then (tailRunLen x : Int) * (matchesFirst y : Int) else 0 := by
  induction x with
  | nil => simp [cross, tailRunLen, matchesFirst_nil]
  | cons a xs ih =>
      cases xs with
      | nil =>
          rw [cross_cons]
          push_cast
          simp [cross, tailRunLen, matchesFirst_singleton]
          by_cases hy : y.head? = some a
          · have hc : (some a == y.head?) = true := by
              rw [beq_iff_eq]
              simp [hy]
            simp [hy, hc]
          · have hc : (some a == y.head?) = false := by
              cases h : some a == y.head? <;> simp_all [beq_iff_eq]
            have hne : some a ≠ y.head? := by
              intro h
              exact hy h.symm
            simp [hy, hc, hne]
      | cons b bs =>
          rw [cross_cons]
          push_cast
          rw [ih, cast_ite_nat_int]
          rw [show (a :: b :: bs).getLast? = (b :: bs).getLast? from List.getLast?_cons_cons]
          have htail : tailRunLen (a :: b :: bs) =
              tailRunLen (b :: bs) + (if (a :: b :: bs).all (fun c => c == a) = true then 1 else 0) :=
            tailRunLen_cons a (b :: bs)
          by_cases hall : (b :: bs).all (fun c => c == a) = true
          · have hxall : (a :: b :: bs).all (fun c => c == a) = true := by simp [hall]
            have hlast : (b :: bs).getLast? = some a :=
              getLast?_eq_of_all (b :: bs) a (by simp) hall
            by_cases hy : y.head? = some a
            · have hbeqhead : (some a == y.head?) = true := by
                rw [beq_iff_eq]
                simp [hy]
              rw [htail]
              simp [hall, hxall, hy, hlast, hbeqhead]
              push_cast
              rw [Int.add_mul, Int.one_mul]
            · have hbeqhead : (some a == y.head?) = false := by
                cases h : some a == y.head? <;> simp_all [beq_iff_eq]
              rw [htail]
              simp [hall, hxall, hy, hlast, hbeqhead]
          · have hxallfalse : (a :: b :: bs).all (fun c => c == a) = false := by simp [hall]
            rw [htail]
            simp [hall, hxallfalse]

theorem sound_BoundaryRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.BoundaryRuns.Goal := by
  intro x y
  rw [measure_append_cross, cross_eq_seam, seam_plateau]
  unfold tailRunLen
  omega

/-! ### ExpandRuns -/

-- Diagonal (within-block) vs off-diagonal (cross-block) plateau windows under
-- block expansion, following the split size x * choose2 (n+1) + n^2 * (measure x - size x).
theorem sound_ExpandRuns :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.ExpandRuns.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PlateauFrames.Remaining.sound_ExpandRuns

end PlateauFramesProofs


-- ===== TripleRise =====
namespace TripleRiseProofs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list helpers -/

private theorem countP_flatMap {α β : Type} (f : α → List β) (P : β → Bool) (xs : List α) :
    (xs.flatMap f).countP P = (xs.map (fun a => (f a).countP P)).sum := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem countP_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.countP_cons]
      rw [h x (by simp)]
      simp [ih (fun y hy => h y (by simp [hy]))]

private theorem sum_map_add {α : Type} (f g : α → Nat) (l : List α) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem countP_repeated (n : Nat) (a : Int) (Q : Int → Bool) :
    (repeated n a).countP Q = if Q a then n else 0 := by
  induction n with
  | zero => simp [repeated]
  | succ n ih =>
      change List.countP Q (a :: List.replicate n a) = if Q a then n + 1 else 0
      simp only [List.countP_cons]
      by_cases h : Q a = true
      · simp [h]
        simpa [repeated, h] using ih
      · simp [h]

private theorem sum_replicate_nat (n k : Nat) : (List.replicate n k).sum = n * k := by
  induction n with
  | zero => simp
  | succ n ih => rw [List.replicate_succ, List.sum_cons, ih]; grind

private theorem countP_flatMap_repeated (n : Nat) (xs : Series) (Q : Int → Bool) :
    (xs.flatMap (repeated n)).countP Q = n * xs.countP Q := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.flatMap_cons, List.countP_append]
      rw [countP_repeated, ih]
      by_cases h : Q a = true
      · simp [h]; grind
      · simp [h]

/-! ### Pair / triple enumeration helpers -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

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
      simp only [List.countP_append]
      rw [List.map_append, List.countP_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun (b, c) => (a, b, c)) ++ triples xs := by
  unfold triples
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem pairs_countP_eq (xs : Series) (P : Int × Int → Bool) :
    (pairs xs).countP P =
      (xs.zipIdx.map (fun (b, j) => (xs.drop (j + 1)).countP (fun c => P (b, c)))).sum := by
  unfold pairs
  rw [countP_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨b, j⟩
  exact countP_map_comp P (fun c => (b, c)) (xs.drop (j + 1))

private theorem pairs_fst_mem {xs : Series} {p : Int × Int} (h : p ∈ pairs xs) : p.1 ∈ xs := by
  induction xs with
  | nil => simp [pairs] at h
  | cons a ys ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨b, hb, hp⟩
        rw [← hp]
        simp
      · exact List.mem_cons_of_mem a (ih h)

private theorem pairs_mem_rising (xs : Series) (h : xs.Pairwise (· < ·)) :
    ∀ p ∈ pairs xs, rising p = true := by
  induction xs with
  | nil => simp [pairs]
  | cons a ys ih =>
      intro p hp
      rw [List.pairwise_cons] at h
      rw [pairs_cons] at hp
      rcases List.mem_append.mp hp with hp | hp
      · rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        exact decide_eq_true (h.1 b hb)
      · exact ih h.2 p hp

/-! ### Binomial coefficient recurrences -/

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => rfl
  | succ n => exact choose2_step n

private theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      simp only [List.length_cons]
      rw [choose2_succ]
      omega

private theorem choose3_step (n : Nat) :
    choose3 (n + 3) = choose3 (n + 2) + choose2 (n + 2) := by
  unfold choose3 choose2
  simp only [show n + 3 - 1 = n + 2 by omega,
    show n + 3 - 2 = n + 1 by omega, show n + 2 - 1 = n + 1 by omega,
    show n + 2 - 2 = n by omega]
  have hb : 2 ∣ (n + 2) * (n + 1) := by
    rcases Nat.mod_two_eq_zero_or_one n with h | h
    · have hd : 2 ∣ n + 2 := by omega
      exact Nat.dvd_mul_right_of_dvd hd (n + 1)
    · have hd : 2 ∣ n + 1 := by omega
      exact Nat.dvd_mul_left_of_dvd hd (n + 2)
  have h2 : 2 ∣ (n + 2) * (n + 1) * n := Nat.dvd_mul_right_of_dvd hb n
  have h3 : 3 ∣ (n + 2) * (n + 1) * n := by
    have hm := Nat.mod_lt n (by decide : 0 < 3)
    rcases (show n % 3 = 0 ∨ n % 3 = 1 ∨ n % 3 = 2 by omega) with h | h | h
    · have hd : 3 ∣ n := Nat.dvd_of_mod_eq_zero h
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 2) * (n + 1))
    · have hd : 3 ∣ n + 2 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 1) * n)
    · have hd : 3 ∣ n + 1 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using
        Nat.dvd_mul_left_of_dvd hd ((n + 2) * n)
  have hc : 6 ∣ (n + 2) * (n + 1) * n :=
    (by decide : Nat.Coprime 2 3).mul_dvd_of_dvd_of_dvd h2 h3
  have h3B : 6 ∣ 3 * ((n + 2) * (n + 1)) := by
    rcases hb with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm]
  have hAC : (n + 3) * (n + 2) * (n + 1) =
      (n + 2) * (n + 1) * n + 3 * ((n + 2) * (n + 1)) := by grind
  rw [hAC, Nat.add_div (by decide),
    Nat.mod_eq_zero_of_dvd hc, Nat.mod_eq_zero_of_dvd h3B]
  simp only [Nat.add_zero]
  rcases hb with ⟨k, hk⟩
  rw [hk]
  simp [Nat.mul_assoc, Nat.mul_comm]

private theorem choose3_succ (n : Nat) : choose3 (n + 1) = choose3 n + choose2 n := by
  cases n with
  | zero => rfl
  | succ n =>
      cases n with
      | zero => rfl
      | succ n => exact choose3_step n

/-! ### MiddlePivot -/

private def midSum (x : Series) : Nat :=
  (x.zipIdx.map (fun (v, i) =>
    (x.take i).countP (fun a => a < v) * (x.drop (i + 1)).countP (fun c => v < c))).sum

private theorem natCast_sum_map {α : Type} (f : α → Nat) (l : List α) :
    ((l.map f).sum : Int) = (l.map (fun a => (f a : Int))).sum := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem natCast_prod_sum_map {α : Type} (f g : α → Nat) (l : List α) :
    ((l.map (fun a => f a * g a)).sum : Int) =
      (l.map (fun a => (f a : Int) * (g a : Int))).sum := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih, Int.natCast_mul]

private def measureNat (x : Series) : Nat := (triples x).countP strict3

private theorem midSum_cons (a : Int) (xs : Series) :
    midSum (a :: xs) =
      (xs.zipIdx.map (fun (v, j) =>
        (if a < v then 1 else 0) * (xs.drop (j + 1)).countP (fun c => v < c))).sum +
        midSum xs := by
  unfold midSum
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.take_zero, List.countP_nil,
    Nat.zero_mul, Nat.zero_add]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add, List.map_map]
  simp only [Function.comp_def, Nat.zero_add]
  simp only [List.take_succ_cons, List.drop_succ_cons, Nat.add_assoc]
  simp only [List.countP_cons, Nat.add_mul, sum_map_add]
  simp
  omega

private theorem pairs_countP_left (a : Int) (xs : Series) :
    (pairs xs).countP (fun (b, c) => a < b && b < c) =
      (xs.zipIdx.map (fun (v, j) =>
        (if a < v then 1 else 0) * (xs.drop (j + 1)).countP (fun c => v < c))).sum := by
  rw [pairs_countP_eq]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨v, j⟩
  by_cases h : a < v
  · simp [h]
  · simp [h]

private theorem measureNat_cons (a : Int) (xs : Series) :
    measureNat (a :: xs) =
      (pairs xs).countP (fun (b, c) => a < b && b < c) + measureNat xs := by
  unfold measureNat
  rw [triples_cons, List.countP_append, countP_map_comp]
  rfl

private theorem measureNat_eq_midSum (x : Series) : measureNat x = midSum x := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [measureNat_cons, ih, pairs_countP_left, midSum_cons]

theorem sound_MiddlePivot :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.MiddlePivot.Goal := by
  intro x
  unfold TripleRise.measure
  rw [← natCast_prod_sum_map
    (fun p : Int × Nat => (x.take p.2).countP (fun a => a < p.1))
    (fun p : Int × Nat => (x.drop (p.2 + 1)).countP (fun c => p.1 < c)) x.zipIdx]
  exact_mod_cast measureNat_eq_midSum x

/-! ### StrictPage -/

private theorem pairs_countP_rising_pairwise (xs : Series) (h : xs.Pairwise (· < ·)) :
    (pairs xs).countP rising = choose2 xs.length := by
  rw [← pairs_length xs]
  rw [List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_eq_self.mpr
  intro p hp
  exact pairs_mem_rising xs h p hp

theorem sound_StrictPage :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.StrictPage.Goal := by
  intro x hx
  induction x with
  | nil => simp [TripleRise.measure, triples, choose3]
  | cons a xs ih =>
      rw [List.pairwise_cons] at hx
      have hxs : xs.Pairwise (· < ·) := hx.2
      have ha : ∀ b ∈ xs, a < b := hx.1
      have hfirst : (pairs xs).countP (fun p : Int × Int => a < p.1 && p.1 < p.2) =
          (pairs xs).countP rising := by
        rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
        apply congrArg List.length
        apply List.filter_congr
        intro p hp
        rcases p with ⟨b, c⟩
        have hbmem : b ∈ xs := pairs_fst_mem hp
        have hab : a < b := ha b hbmem
        simp [rising, hab]
      have hm : measureNat (a :: xs) = (pairs xs).countP rising + measureNat xs := by
        rw [measureNat_cons, hfirst]
      have ih' : (measureNat xs : Int) = (choose3 xs.length : Int) := by
        simpa [TripleRise.measure, measureNat] using ih hxs
      change (measureNat (a :: xs) : Int) = (choose3 (xs.length + 1) : Int)
      rw [hm, Int.natCast_add, ih', pairs_countP_rising_pairwise xs hxs]
      have hc : (choose3 (xs.length + 1) : Int) =
          (choose3 xs.length : Int) + (choose2 xs.length : Int) := by
        exact_mod_cast (choose3_succ xs.length)
      rw [hc]
      grind

/-! ### StableExpand -/

private theorem cart_countP (x y : Series) (P : Int × Int → Bool) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [show cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y by simp [cart]]
      rw [List.countP_append, countP_map_comp, ih]
      simp only [List.map_cons, List.sum_cons]

private theorem cart_countP_repeated (n : Nat) (a : Int) (z : Series) (P : Int × Int → Bool) :
    (cart (repeated n a) z).countP P = n * z.countP (fun b => P (a, b)) := by
  rw [cart_countP]
  rw [repeated, List.map_replicate, sum_replicate_nat]

private theorem pairs_repeated_countP (n : Nat) (a : Int) (P : Int × Int → Bool)
    (hdiag : P (a, a) = false) : (pairs (repeated n a)).countP P = 0 := by
  induction n with
  | zero => simp [repeated, pairs]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
        change List.replicate (n + 1) a = a :: List.replicate n a
        rw [List.replicate_succ]]
      rw [pairs_cons, List.countP_append]
      have hmap : ((repeated n a).map (fun b => (a, b))).countP P = 0 := by
        rw [countP_map_comp, countP_repeated, hdiag]
        simp
      rw [hmap, Nat.zero_add, ih]

private theorem pairs_expand_countP (n : Nat) (xs : Series) (P : Int × Int → Bool)
    (hdiag : ∀ v, P (v, v) = false) :
    (pairs (xs.flatMap (repeated n))).countP P = n * n * (pairs xs).countP P := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [show (a :: xs).flatMap (repeated n) = repeated n a ++ xs.flatMap (repeated n) by rfl]
      rw [pairs_countP_append]
      rw [ih]
      rw [pairs_repeated_countP n a P (hdiag a)]
      rw [cart_countP_repeated n a (xs.flatMap (repeated n)) P]
      rw [countP_flatMap_repeated n xs (fun b => P (a, b))]
      simp only [Nat.zero_add]
      rw [pairs_cons, List.countP_append, countP_map_comp]
      grind

private theorem pairs_prepend_block_countP (n : Nat) (a : Int) (z : Series) :
    (pairs (repeated n a ++ z)).countP (fun (b, c) => a < b && b < c) =
      (pairs z).countP (fun (b, c) => a < b && b < c) := by
  induction n with
  | zero => simp [repeated]
  | succ n ih =>
      rw [show repeated (n + 1) a ++ z = a :: (repeated n a ++ z) by
        change List.replicate (n + 1) a ++ z = a :: (List.replicate n a ++ z)
        rw [List.replicate_succ]
        simp]
      rw [pairs_cons, List.countP_append]
      have hmap : ((repeated n a ++ z).map (fun b => (a, b))).countP
          (fun (b, c) => a < b && b < c) = 0 := by
        rw [countP_map_comp]
        apply countP_false
        intro b hb
        simp
      rw [hmap, Nat.zero_add, ih]

private theorem prepend_block_measureNat (n : Nat) (a : Int) (z : Series) :
    measureNat (repeated n a ++ z) =
      n * (pairs z).countP (fun (b, c) => a < b && b < c) + measureNat z := by
  induction n with
  | zero => simp [repeated, measureNat]
  | succ n ih =>
      rw [show repeated (n + 1) a ++ z = a :: (repeated n a ++ z) by
        change List.replicate (n + 1) a ++ z = a :: (List.replicate n a ++ z)
        rw [List.replicate_succ]
        simp]
      rw [measureNat_cons]
      change (pairs (repeated n a ++ z)).countP (fun (b, c) => a < b && b < c) +
          measureNat (repeated n a ++ z) =
        (n + 1) * (pairs z).countP (fun (b, c) => a < b && b < c) + measureNat z
      rw [pairs_prepend_block_countP, ih]
      grind

private theorem measureNat_expand (x : Series) (n : Nat) :
    measureNat (x.flatMap (repeated n)) = n ^ 3 * measureNat x := by
  induction x with
  | nil => simp [measureNat, triples]
  | cons a xs ih =>
      rw [show (a :: xs).flatMap (repeated n) = repeated n a ++ xs.flatMap (repeated n) by rfl]
      rw [prepend_block_measureNat]
      rw [pairs_expand_countP n xs (fun (b, c) => a < b && b < c) (by intro v; simp)]
      rw [ih]
      rw [measureNat_cons]
      grind

theorem sound_StableExpand :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.StableExpand.Goal := by
  intro x n
  unfold TripleRise.measure
  change (measureNat (x.flatMap (repeated n)) : Int) = (n : Int) ^ 3 * (measureNat x : Int)
  rw [measureNat_expand]
  rw [Int.natCast_mul, Int.natCast_pow]

end TripleRiseProofs


-- ===== AlternatingBalance =====
namespace AlternatingBalanceProofs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance

/-! #### Alternating (parity-weighted) sum helpers -/

private def alt (k : Nat) (x : Series) : Int :=
  ((x.zipIdx k).map (fun (v, i) => (-1 : Int) ^ i * v)).sum

private theorem measure_alt (x : Series) : AlternatingBalance.measure x = alt 0 x := by
  rfl

private theorem alt_cons (k : Nat) (a : Int) (xs : Series) :
    alt k (a :: xs) = (-1 : Int) ^ k * a + alt (k + 1) xs := by
  simp [alt, List.zipIdx_cons]

private theorem neg_one_pow_succ (n : Nat) : (-1 : Int) ^ (n + 1) = -((-1 : Int) ^ n) := by
  rw [Int.pow_succ]
  omega

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1)]
      rw [alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem alt_append (k : Nat) (x y : Series) :
    alt k (x ++ y) = alt k x + alt (k + x.length) y := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [List.cons_append, alt_cons, alt_cons, ih (k + 1)]
      rw [show k + 1 + xs.length = k + (xs.length + 1) by omega]
      simp only [List.length_cons]
      omega

private theorem measure_cons (a : Int) (xs : Series) :
    AlternatingBalance.measure (a :: xs) = a - AlternatingBalance.measure xs := by
  rw [measure_alt, measure_alt, alt_cons, alt_shift 1]
  rw [neg_one_pow_succ]
  simp only [Int.pow_zero]
  grind

private theorem neg_one_pow_even (k : Nat) : (-1 : Int) ^ (2 * k) = 1 := by
  induction k with
  | zero => decide
  | succ k ih =>
      rw [show 2 * (k + 1) = 2 * k + 2 by omega]
      rw [Int.pow_add, ih]
      rw [show (-1 : Int) ^ 2 = 1 by decide]
      omega

private theorem neg_one_pow_odd (k : Nat) : (-1 : Int) ^ (2 * k + 1) = -1 := by
  rw [Int.pow_succ, neg_one_pow_even]
  omega

private theorem neg_one_pow_mod (n : Nat) :
    (-1 : Int) ^ n = if n % 2 == 0 then 1 else -1 := by
  by_cases h : n % 2 == 0
  · have h0 : n % 2 = 0 := by simpa using h
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) := by omega
    rw [ite_eq_left h, hn]
    exact neg_one_pow_even (n / 2)
  · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
    have hn0 : ¬ n % 2 = 0 := by simpa using h
    have h0 : n % 2 = 1 := by omega
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) + 1 := by omega
    rw [ite_eq_right h, hn]
    exact neg_one_pow_odd (n / 2)

private theorem measure_singleton (a : Int) : AlternatingBalance.measure [a] = a := by
  simp [AlternatingBalance.measure]

private theorem measure_repeated (n : Nat) (a : Int) :
    AlternatingBalance.measure (repeated n a) = ((n % 2 : Nat) : Int) * a := by
  induction n with
  | zero => simp [repeated, AlternatingBalance.measure]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
        simp [repeated, List.replicate_succ], measure_cons, ih]
      have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
      by_cases h : n % 2 == 0
      · have h0 : n % 2 = 0 := by simpa using h
        have h1 : (n + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        have h1 : (n + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

/-! #### Goals -/

theorem sound_ParityCarry : ParityCarry.Goal := by
  intro x y
  rw [measure_alt, measure_alt, measure_alt]
  rw [alt_append]
  simp only [Nat.zero_add]
  rw [alt_shift (x.length) y]

theorem sound_BackwardPhase : BackwardPhase.Goal := by
  intro x
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.reverse_cons]
      rw [sound_ParityCarry]
      rw [measure_singleton]
      rw [List.length_reverse]
      rw [ih]
      rw [measure_cons]
      cases xs with
      | nil => simp [AlternatingBalance.measure]
      | cons b bs =>
          simp only [List.length_cons, Nat.add_sub_cancel_right]
          rw [measure_cons]
          rw [neg_one_pow_succ]
          grind

theorem sound_PeriodicParity : PeriodicParity.Goal := by
  intro x n
  induction n with
  | zero => simp [AlternatingBalance.measure]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [sound_ParityCarry, ih]
      by_cases hl : x.length % 2 == 0
      · have hpow : (-1 : Int) ^ x.length = 1 := by
          rw [neg_one_pow_mod, ite_eq_left hl]
        rw [ite_eq_left hl] at ih ⊢
        rw [hpow]
        grind
      · have hpow : (-1 : Int) ^ x.length = -1 := by
          rw [neg_one_pow_mod, ite_eq_right hl]
        rw [ite_eq_right hl] at ih ⊢
        rw [hpow]
        by_cases hn : n % 2 == 0
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have h0 : n % 2 = 0 := by simpa using hn
          have h1 : (n + 1) % 2 = 1 := by omega
          rw [h0, h1]
          grind
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have hn0 : ¬ n % 2 = 0 := by simpa using hn
          have h0 : n % 2 = 1 := by omega
          have h1 : (n + 1) % 2 = 0 := by omega
          rw [h0, h1]
          grind

theorem sound_DuplicateCancellation : DuplicateCancellation.Goal := by
  intro x n
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.flatMap_cons]
      rw [sound_ParityCarry]
      rw [measure_repeated]
      rw [ih]
      rw [measure_cons]
      rw [show (repeated n a).length = n by simp [repeated]]
      by_cases h : n % 2 == 0
      · have hpow : (-1 : Int) ^ n = 1 := by rw [neg_one_pow_mod, ite_eq_left h]
        rw [ite_eq_left h, hpow]
        have h0 : n % 2 = 0 := by simpa using h
        rw [h0]
        grind
      · have hpow : (-1 : Int) ^ n = -1 := by rw [neg_one_pow_mod, ite_eq_right h]
        rw [ite_eq_right h, hpow]
        have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        rw [h0]
        grind

theorem sound_ShiftResidue : ShiftResidue.Goal := by
  intro b x
  induction x with
  | nil => simp [AlternatingBalance.measure, shifted]
  | cons a xs ih =>
      change AlternatingBalance.measure ((a + b) :: shifted b xs) =
        AlternatingBalance.measure (a :: xs) + (((xs.length + 1) % 2 : Nat) : Int) * b
      rw [measure_cons, measure_cons, ih]
      by_cases h : xs.length % 2 == 0
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have h0 : xs.length % 2 = 0 := by simpa using h
        have h1 : (xs.length + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ xs.length % 2 = 0 := by simpa using h
        have h0 : xs.length % 2 = 1 := by omega
        have h1 : (xs.length + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

end AlternatingBalanceProofs


-- ===== PositiveJumps =====
namespace PositiveJumpsProofs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps

private abbrev up (a b : Int) : Int := max 0 (b - a)

/-- Induction from the right end of a list (no Mathlib `reverseRecOn` here). -/
private theorem snoc_induction {motive : Series → Prop}
    (h_nil : motive []) (h_snoc : ∀ xs a, motive xs → motive (xs ++ [a])) :
    ∀ xs, motive xs := by
  have hrev : ∀ ys : Series, motive ys.reverse := by
    intro ys
    induction ys with
    | nil => simpa using h_nil
    | cons a ys ih =>
        rw [List.reverse_cons]
        exact h_snoc ys.reverse a ih
  intro xs
  simpa using hrev xs.reverse

private theorem delta_cons_cons (a b : Int) (bs : Series) :
    delta (a :: b :: bs) = (b - a) :: delta (b :: bs) := by
  simp [delta, adj]

private theorem getLast_snoc_getD (xs : Series) (a d : Int) :
    (xs ++ [a]).getLast?.getD d = a := by
  rw [List.getLast?_concat]
  simp

private theorem sum_map_max_nonneg (ds : List Int) : 0 ≤ (ds.map (max 0)).sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      have hmax : 0 ≤ max 0 d := Int.le_max_left 0 d
      omega

private theorem measure_nonneg (x : Series) : 0 ≤ PositiveJumps.measure x := by
  simpa [PositiveJumps.measure, delta] using sum_map_max_nonneg (delta x)

private theorem measure_cons (b : Int) (l : Series) :
    PositiveJumps.measure (b :: l) = PositiveJumps.measure l + up b (l.head?.getD b) := by
  cases l with
  | nil => simp [PositiveJumps.measure, delta, adj, up]
  | cons c cs =>
      simp [PositiveJumps.measure, delta, adj, up]
      omega

private theorem measure_append (x : Series) : ∀ y : Series,
    PositiveJumps.measure (x ++ y) = PositiveJumps.measure x + PositiveJumps.measure y +
      seam x y up := by
  induction x with
  | nil =>
      intro y
      simp [PositiveJumps.measure, delta, adj, seam]
  | cons a xs ih =>
      intro y
      cases y with
      | nil => simp [PositiveJumps.measure, delta, adj, seam]
      | cons b ys =>
          rw [List.cons_append, measure_cons]
          rw [ih (b :: ys)]
          cases xs with
          | nil =>
              simp [seam, measure_cons, up]
          | cons c cs =>
              simp [seam, measure_cons, up] at ih ⊢
              omega

private theorem measure_snoc (xs : Series) (a : Int) :
    PositiveJumps.measure (xs ++ [a]) = PositiveJumps.measure xs + up (xs.getLast?.getD a) a := by
  have h := measure_append xs [a]
  cases hx : xs.getLast? with
  | none =>
      have hxnone : xs = [] := by
        cases xs with
        | nil => rfl
        | cons b bs =>
            simp at hx
      subst xs
      simp [PositiveJumps.measure, delta, adj, up]
  | some l =>
      have hseam : seam xs [a] up = up l a := by simp [seam, hx]
      rw [hseam] at h
      simpa [PositiveJumps.measure, delta, adj, hx, up] using h

private theorem delta_sum (x : Series) : (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          simp [delta, adj] at ih ⊢
          omega

private theorem pos_neg_diff (d : Int) : max 0 d - max 0 (-d) = d := by
  by_cases h : 0 ≤ d
  · have h1 : max 0 d = d := Int.max_eq_right h
    have h2 : max 0 (-d) = 0 := Int.max_eq_left (by omega)
    rw [h1, h2]
    omega
  · have h1 : max 0 d = 0 := Int.max_eq_left (by omega)
    have h2 : max 0 (-d) = -d := Int.max_eq_right (by omega)
    rw [h1, h2]
    omega

private theorem twice_pos (d : Int) : 2 * max 0 d = (d.natAbs : Int) + d := by
  by_cases h : 0 ≤ d
  · rw [Int.max_eq_right h, Int.natAbs_of_nonneg h]
    omega
  · have hd : d ≤ 0 := by omega
    have hn : 0 ≤ -d := by omega
    have hnegabs : (-d).natAbs = d.natAbs := Int.natAbs_neg d
    have hnabs : ((-d).natAbs : Int) = -d := Int.natAbs_of_nonneg hn
    rw [Int.max_eq_left hd, ← hnegabs, hnabs]
    omega

private theorem twice_pos_sum (ds : List Int) :
    2 * (ds.map (max 0)).sum = (ds.map (fun d => (d.natAbs : Int))).sum + ds.sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Int.mul_add, ih, twice_pos]
      omega

private theorem delta_nonpos : ∀ x : Series, x.Pairwise (· ≥ ·) →
    ∀ v ∈ delta x, v ≤ 0
  | [], hx => by simp [delta, adj]
  | [a], hx => by simp [delta, adj]
  | a :: b :: bs, hx => by
      simp only [List.pairwise_cons] at hx
      have hba : b ≤ a := hx.1 b (by simp)
      intro v hv
      rw [delta_cons_cons] at hv
      simp only [List.mem_cons] at hv
      rcases hv with hv | hv
      · subst v
        omega
      · have htail : (b :: bs).Pairwise (· ≥ ·) :=
          (List.pairwise_cons).mpr ⟨hx.2.1, hx.2.2⟩
        exact delta_nonpos (b :: bs) htail v hv

private theorem sum_max_zero_of_nonpos (ds : List Int) (h : ∀ v ∈ ds, v ≤ 0) :
    (ds.map (max 0)).sum = 0 := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have hd : d ≤ 0 := h d (by simp)
      have ht : ∀ v ∈ ds, v ≤ 0 := by intro v hv; exact h v (by simp [hv])
      simp only [List.map_cons, List.sum_cons]
      rw [Int.max_eq_left hd, ih ht]
      simp

private theorem up_triangle (a b c : Int) : up a c ≤ up a b + up b c := by
  dsimp [up]
  have hsum : c - a = (b - a) + (c - b) := by omega
  rw [hsum]
  apply (Int.max_le).mpr
  constructor
  · have h1 : 0 ≤ max 0 (b - a) := Int.le_max_left 0 (b - a)
    have h2 : 0 ≤ max 0 (c - b) := Int.le_max_left 0 (c - b)
    omega
  · have h1 : b - a ≤ max 0 (b - a) := Int.le_max_right 0 (b - a)
    have h2 : c - b ≤ max 0 (c - b) := Int.le_max_right 0 (c - b)
    omega

private theorem head_triangle (a b : Int) (xs : Series) :
    up a (xs.head?.getD a) ≤ up b (xs.head?.getD b) + up a b := by
  cases xs with
  | nil =>
      dsimp [up]
      have h : 0 ≤ max 0 (b - a) := Int.le_max_left 0 (b - a)
      omega
  | cons c cs =>
      simp only [List.head?_cons, Option.getD_some]
      have htri := up_triangle a b c
      omega

private theorem filter_up_bound (xs : Series) (keep : Int → Bool) : ∀ a : Int,
    PositiveJumps.measure (xs.filter keep) + up a ((xs.filter keep).head?.getD a) ≤
      PositiveJumps.measure xs + up a (xs.head?.getD a) := by
  induction xs with
  | nil =>
      intro a
      simp [PositiveJumps.measure, delta, adj, up]
  | cons b bs ih =>
      intro a
      by_cases hb : keep b = true
      · have hih := ih b
        calc
          PositiveJumps.measure ((b :: bs).filter keep) + up a (((b :: bs).filter keep).head?.getD a)
              = PositiveJumps.measure (b :: bs.filter keep) + up a b := by simp [hb]
          _ = (PositiveJumps.measure (bs.filter keep) + up b ((bs.filter keep).head?.getD b)) + up a b := by
                rw [measure_cons]
          _ ≤ (PositiveJumps.measure bs + up b (bs.head?.getD b)) + up a b := by omega
          _ = PositiveJumps.measure (b :: bs) + up a ((b :: bs).head?.getD a) := by
                rw [measure_cons]
                simp
      · have hih := ih a
        have htri := head_triangle a b bs
        calc
          PositiveJumps.measure ((b :: bs).filter keep) + up a (((b :: bs).filter keep).head?.getD a)
              = PositiveJumps.measure (bs.filter keep) + up a ((bs.filter keep).head?.getD a) := by simp [hb]
          _ ≤ PositiveJumps.measure bs + up a (bs.head?.getD a) := hih
          _ ≤ PositiveJumps.measure bs + (up b (bs.head?.getD b) + up a b) := by omega
          _ = PositiveJumps.measure (b :: bs) + up a ((b :: bs).head?.getD a) := by
                rw [measure_cons]
                simp
                omega

theorem sound_BoundaryJump :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.BoundaryJump.Goal := by
  intro x y
  exact measure_append x y

theorem sound_ReverseBalance :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.ReverseBalance.Goal := by
  intro x
  have hmain : PositiveJumps.measure x - PositiveJumps.measure x.reverse = (delta x).sum := by
    refine snoc_induction
      (motive := fun z => PositiveJumps.measure z - PositiveJumps.measure z.reverse = (delta z).sum)
      ?_ ?_ x
    · simp [PositiveJumps.measure, delta, adj]
    · intro xs a ih
      rw [delta_sum]
      rw [getLast_snoc_getD]
      rw [measure_snoc]
      rw [show (xs ++ [a]).reverse = a :: xs.reverse by simp]
      rw [measure_cons]
      have hih := ih
      rw [delta_sum] at hih
      cases xs with
      | nil =>
          simp [PositiveJumps.measure, delta, adj, up]
      | cons b bs =>
          have hlast : (b :: bs).getLast?.getD a = (b :: bs).getLast?.getD 0 := by
            simp [List.getLast?_cons]
          have hrevhead : (b :: bs).reverse.head?.getD a = (b :: bs).getLast?.getD 0 := by
            rw [List.head?_reverse]
            simp [List.getLast?_cons]
          rw [hlast, hrevhead]
          have hhead : (b :: bs ++ [a]).head?.getD 0 = b := rfl
          rw [hhead]
          have hhead0 : (b :: bs).head?.getD 0 = b := rfl
          rw [hhead0] at hih
          have hposneg := pos_neg_diff (a - (b :: bs).getLast?.getD 0)
          dsimp [up] at hposneg ⊢
          omega
  rw [hmain, delta_sum]

theorem sound_SharedMagnitude :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.SharedMagnitude.Goal := by
  intro x
  have h := twice_pos_sum (delta x)
  rw [delta_sum] at h
  dsimp [PositiveJumps.measure, TotalVariation.measure] at h ⊢
  omega

theorem sound_NonincreasingSkip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.NonincreasingSkip.Goal := by
  intro x hx
  have hd := delta_nonpos x hx
  simpa [PositiveJumps.measure] using sum_max_zero_of_nonpos (delta x) hd

theorem sound_FilterBudget :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.FilterBudget.Goal := by
  intro x keep
  let a : Int := x.head?.getD 0
  have h := filter_up_bound x keep a
  have hzero : up a (x.head?.getD a) = 0 := by
    cases x with
    | nil => simp [a, up]
    | cons b bs => simp [a, up]
  rw [hzero] at h
  have hnonneg : 0 ≤ up a ((x.filter keep).head?.getD a) := by
    dsimp [up]
    exact Int.le_max_left 0 _
  omega

end PositiveJumpsProofs




-- ===== BestFrame =====
namespace BestFrameProofs

-- Window/prefix-suffix bookkeeping follows the built
-- Atlas_OrderedMeasures_BestFrame_Remaining development.
theorem sound_CrossFrame :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.CrossFrame.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_CrossFrame

theorem sound_RepeatedGain :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.RepeatedGain.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_RepeatedGain

theorem sound_NegativePage :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.NegativePage.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_NegativePage

theorem sound_PrefixDrawdown :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.PrefixDrawdown.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_PrefixDrawdown

theorem sound_DeletionFence :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.BestFrame.DeletionFence.Goal :=
  TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.BestFrame.Remaining.sound_DeletionFence

end BestFrameProofs

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures_WorkA
