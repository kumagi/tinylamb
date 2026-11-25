import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_Rearrangement
import TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_TransportLower

namespace TinylambOpt.ResearchProofs.Atlas.SynopsisBounds

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.PageMoments

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ### Generic list algebra helpers -/

private theorem sq_nonneg_int (a : Int) : 0 ≤ a ^ 2 := by
  have hsq : a ^ 2 = a * a := by grind
  rw [hsq]
  by_cases h : 0 ≤ a
  · exact Int.mul_nonneg h h
  · have hn : 0 ≤ -a := by omega
    have hmul := Int.mul_nonneg hn hn
    grind

private theorem sq_le_sq_of_nonneg {a b : Int} (ha : 0 ≤ a) (hab : a ≤ b) : a ^ 2 ≤ b ^ 2 := by
  have hb : 0 ≤ b := by omega
  have ha2 : a ^ 2 = a * a := by grind
  have hb2 : b ^ 2 = b * b := by grind
  rw [ha2, hb2]
  have h1 : a * a ≤ a * b := Int.mul_le_mul_of_nonneg_left hab ha
  have h2 : a * b ≤ b * b := Int.mul_le_mul_of_nonneg_right hab hb
  omega

private theorem natAbs_sq_cast (z : Int) : (((z.natAbs : Nat) : Int) ^ 2) = z ^ 2 := by
  have h1 : (((z.natAbs : Nat) : Int) ^ 2) = ((z.natAbs : Nat) : Int) * ((z.natAbs : Nat) : Int) := by grind
  have h2 : z ^ 2 = z * z := by grind
  rw [h1, h2]
  exact Int.natAbs_mul_self

private theorem sum_map_nonneg {α : Type} (l : List α) (f : α → Int)
    (h : ∀ x ∈ l, 0 ≤ f x) : 0 ≤ (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, 0 ≤ f y := by intro y hy; exact h y (by simp [hy])
      have := ih ht
      omega

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map fun x => f x + g x).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_le_sum {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      have := ih ht
      omega

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a == b
  · have hab : a = b := of_decide_eq_true h
    subst b
    simp
  · have hne : a ≠ b := by intro heq; exact h (by simp [heq])
    have h' : (b == a) = false := by
      apply Bool.eq_false_of_ne_true
      intro hb
      exact hne (of_decide_eq_true hb).symm
    simp [h, h']

private theorem countP_eq_count_nat (xs : List Nat) (a : Nat) :
    xs.countP (fun y => y == a) = xs.count a := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.countP_cons, List.count_cons, ih]

/-! ### Samples combinatorics -/

private theorem samples_length {α : Type} : ∀ (xs : List α) (k : Nat),
    (samples k xs).length = choose xs.length k
  | [], k => by
      cases k with
      | zero => rfl
      | succ k => rfl
  | x :: xs, k => by
      cases k with
      | zero => rfl
      | succ k =>
          simp only [samples, List.length_append, List.length_map,
            samples_length xs (k + 1), samples_length xs k, List.length_cons, choose]
          omega

private theorem samples_nil_of_length_lt {α : Type} (xs : List α) (k : Nat)
    (hk : xs.length < k) : samples k xs = [] := by
  induction xs generalizing k with
  | nil =>
      cases k with
      | zero => simp at hk
      | succ k => rfl
  | cons x xs ih =>
      cases k with
      | zero => simp at hk
      | succ k =>
          simp only [List.length_cons] at hk
          rw [samples]
          have h1 : samples (k + 1) xs = [] := ih (k + 1) (by omega)
          have h2 : samples k xs = [] := ih k (by omega)
          simp [h1, h2]

private theorem samples_self {α : Type} : ∀ (xs : List α), samples xs.length xs = [xs]
  | [] => rfl
  | x :: xs => by
      change samples (xs.length + 1) (x :: xs) = [x :: xs]
      rw [samples]
      have h1 : samples (xs.length + 1) xs = [] :=
        samples_nil_of_length_lt xs (xs.length + 1) (by omega)
      have h2 := samples_self xs
      rw [h1, h2]
      rfl

private theorem sums_zero (_xs : List Int) : sums ([] : List Int) 0 = [0] := rfl

private theorem sums_cons (x : Int) (xs : List Int) (k : Nat) :
    sums (x :: xs) (k + 1) = sums xs (k + 1) ++ (sums xs k).map (x + ·) := by
  simp [sums, samples, List.map_append, List.map_map, Function.comp_def]

private theorem sum_map_add_const (zs : List Int) (x : Int) :
    (zs.map (x + ·)).sum = zs.length * x + zs.sum := by
  induction zs with
  | nil => simp
  | cons z zs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

private theorem sum_map_square_add (zs : List Int) (x : Int) :
    (zs.map fun z => (x + z) ^ 2).sum =
      zs.length * x ^ 2 + 2 * x * zs.sum + (zs.map fun z => z ^ 2).sum := by
  induction zs with
  | nil => simp
  | cons z zs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

private theorem choose_eq_zero_of_lt (n k : Nat) (h : n < k) : choose n k = 0 := by
  cases n with
  | zero =>
      cases k with
      | zero => omega
      | succ k => rfl
  | succ n =>
      cases k with
      | zero => omega
      | succ k =>
          rw [choose, choose_eq_zero_of_lt n (k + 1) (by omega)]
          simp [choose_eq_zero_of_lt n k (by omega)]

private theorem choose_diag (n : Nat) : choose n n = 1 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [choose, ih, choose_eq_zero_of_lt n (n + 1) (by omega)]

private theorem choose_split (n k : Nat) (hk : 0 < k) (hkn : k ≤ n) :
    choose n k = choose (n - 1) k + choose (n - 1) (k - 1) := by
  have hn0 : n ≠ 0 := by
    intro hn
    subst n
    omega
  obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hn0
  cases k with
  | zero => omega
  | succ k =>
      simp [choose]
      omega

private theorem countP_map_comp {α β : Type} (ss : List α) (f : α → β) (p : β → Bool) :
    (ss.map f).countP p = ss.countP (fun s => p (f s)) := by
  induction ss with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.countP_cons]
      rw [ih]

private theorem countP_const_false {α : Type} (ss : List α) :
    ss.countP (fun _ => false) = 0 := by
  induction ss with
  | nil => rfl
  | cons s ss ih => simp [ih]

private theorem empty_sample_count : ∀ (xs : List Bool) (k : Nat),
    (samples k xs).countP (fun s => !s.contains true) = choose (xs.count false) k
  | [], k => by
      cases k <;> simp [samples, choose]
  | x :: xs, k => by
      cases k with
      | zero => simp [samples, choose]
      | succ k =>
          rw [samples, List.countP_append, countP_map_comp]
          have ih1 := empty_sample_count xs (k + 1)
          have ih0 := empty_sample_count xs k
          cases x with
          | false =>
              have hpred : (fun s : List Bool => !((false :: s).contains true)) =
                  (fun s : List Bool => !(s.contains true)) := by
                funext s
                simp [List.contains_cons]
              rw [hpred, ih1, ih0]
              simp [choose]
              omega
          | true =>
              have hpred : (fun s : List Bool => !((true :: s).contains true)) =
                  (fun _ : List Bool => false) := by
                funext s
                simp [List.contains_cons]
              rw [hpred, countP_const_false, ih1]
              simp [choose]

theorem sound_Samples_EmptySample : Samples.EmptySample.Goal := by
  intro xs k
  exact empty_sample_count xs k

private theorem count_true_le_length (s : List Bool) : s.count true ≤ s.length := by
  induction s with
  | nil => simp
  | cons x s ih =>
      cases x <;> simp [List.count_cons] <;> omega

private theorem samples_mem_length {α : Type} : ∀ (xs : List α) (k : Nat) (s : List α),
    s ∈ samples k xs → s.length = k
  | [], k, s, hs => by
      cases k with
      | zero => simp [samples] at hs; subst s; rfl
      | succ k => simp [samples] at hs
  | x :: xs, k, s, hs => by
      cases k with
      | zero => simp [samples] at hs; subst s; rfl
      | succ k =>
          rw [samples, List.mem_append] at hs
          rcases hs with hs | hs
          · exact samples_mem_length xs (k + 1) s hs
          · rw [List.mem_map] at hs
            rcases hs with ⟨t, ht, rfl⟩
            simp only [List.length_cons]
            rw [samples_mem_length xs k t ht]

private theorem countP_true_count_zero (ss : List (List Bool)) (k h : Nat)
    (hlen : ∀ s ∈ ss, s.length = k) (hh : k < h) :
    ss.countP (fun s => s.count true == h) = 0 := by
  apply List.countP_eq_zero.mpr
  intro s hs hp
  have hct := count_true_le_length s
  have hslen := hlen s hs
  have heq : s.count true = h := of_decide_eq_true hp
  omega

private theorem selectivity_count : ∀ (xs : List Bool) (k h : Nat), h ≤ k →
    (samples k xs).countP (fun s => s.count true == h) =
      choose (xs.count true) h * choose (xs.count false) (k - h)
  | [], k, h, hh => by
      cases k with
      | zero =>
          have hz : h = 0 := by omega
          subst h
          simp [samples, choose]
      | succ k =>
          have hempty : samples (k + 1) ([] : List Bool) = [] := rfl
          rw [hempty]
          simp only [List.countP_nil]
          cases h <;> simp [choose]
  | x :: xs, k, h, hh => by
      cases k with
      | zero =>
          have hz : h = 0 := by omega
          subst h
          simp [samples, choose]
      | succ k =>
          rw [samples, List.countP_append, countP_map_comp]
          have ih1 := selectivity_count xs (k + 1) h (by omega)
          have ihz := selectivity_count xs k 0 (by omega)
          cases h with
          | zero =>
              cases x with
              | false =>
                  have hpred : (fun s : List Bool => (false :: s).count true == 0) =
                      (fun s : List Bool => s.count true == 0) := by
                    funext s
                    simp [List.count_cons]
                  rw [hpred, ih1, ihz]
                  simp [choose]
                  grind
              | true =>
                  have hpred : (fun s : List Bool => (true :: s).count true == 0) =
                      (fun _ : List Bool => false) := by
                    funext s
                    simp [List.count_cons]
                  rw [hpred, countP_const_false, ih1]
                  simp [choose]
          | succ h =>
              cases x with
              | false =>
                  have hpred : (fun s : List Bool => (false :: s).count true == h + 1) =
                      (fun s : List Bool => s.count true == h + 1) := by
                    funext s
                    simp [List.count_cons]
                  rw [hpred, ih1]
                  by_cases heq : h = k
                  · subst k
                    have hz := countP_true_count_zero (samples h xs) h (h + 1)
                      (fun s hs => samples_mem_length xs h s hs) (by omega)
                    rw [hz]
                    simp [choose]
                  · have ih0 := selectivity_count xs k (h + 1) (by omega)
                    rw [ih0]
                    have haddsub : k + 1 - (h + 1) = k - h := by omega
                    rw [haddsub]
                    have hm : k - h = (k - h - 1) + 1 := by omega
                    have hm' : k - (h + 1) = k - h - 1 := by omega
                    rw [hm, hm']
                    have hcf : (false :: xs).count false = xs.count false + 1 := by
                      simp [List.count_cons]
                    rw [hcf]
                    change choose (xs.count true) (h + 1) * choose (xs.count false) (k - h - 1 + 1) +
                        choose (xs.count true) (h + 1) * choose (xs.count false) (k - h - 1) =
                      choose (xs.count true) (h + 1) *
                        choose (xs.count false + 1) (k - h - 1 + 1)
                    rw [choose, Nat.mul_add]
                    grind
              | true =>
                  have ih0 := selectivity_count xs k h (by omega)
                  have hpred : (fun s : List Bool => (true :: s).count true == h + 1) =
                      (fun s : List Bool => s.count true == h) := by
                    funext s
                    simp [List.count_cons]
                  rw [hpred, ih1, ih0]
                  have haddsub : k + 1 - (h + 1) = k - h := by omega
                  rw [haddsub]
                  have hct : (true :: xs).count true = xs.count true + 1 := by
                    simp [List.count_cons]
                  rw [hct]
                  change choose (xs.count true) (h + 1) * choose (xs.count false) (k - h) +
                      choose (xs.count true) h * choose (xs.count false) (k - h) =
                    choose (xs.count true + 1) (h + 1) * choose (xs.count false) (k - h)
                  rw [choose]
                  grind

theorem sound_Samples_SelectivityLaw : Samples.SelectivityLaw.Goal := by
  intro xs k h hh
  exact selectivity_count xs k h hh

private theorem samples_sum_total (xs : List Int) : ∀ (k : Nat), 0 < k → k ≤ xs.length →
    (sums xs k).sum = (choose (xs.length - 1) (k - 1) : Int) * xs.sum := by
  induction xs with
  | nil =>
      intro k hk hlen
      simp only [List.length_nil] at hlen
      omega
  | cons x xs ih =>
      intro k
      cases k with
      | zero => omega
      | succ k =>
          intro hk hlen
          simp only [List.length_cons] at hlen
          rw [sums_cons, List.sum_append]
          have hlen_sums : (sums xs k).length = choose xs.length k := by
            simp [sums, samples_length]
          have hadd : ((sums xs k).map (x + ·)).sum =
              x * (choose xs.length k : Int) + (sums xs k).sum := by
            rw [sum_map_add_const, hlen_sums]
            grind
          by_cases hk0 : k = 0
          · subst k
            simp only [Nat.zero_add]
            by_cases hnil : xs = []
            · subst xs
              simp [sums, samples, choose]
            · have ih1 := ih 1 Nat.zero_lt_one (List.length_pos_iff.mpr hnil)
              rw [ih1]
              simp [sums, samples, choose]
              grind
          · have hkpos : 0 < k := by omega
            by_cases hbound : xs.length = k
            · have htail : sums xs (k + 1) = [] := by
                simp [sums, samples_nil_of_length_lt xs (k + 1) (by omega)]
              have ih0 := ih k hkpos (by omega)
              rw [htail, hadd, ih0]
              simp only [List.length_cons, Nat.add_sub_cancel, List.sum_cons]
              rw [hbound, choose_diag k, choose_diag (k - 1)]
              grind
            · have hklen : k ≤ xs.length := by omega
              have hsucc_len : k + 1 ≤ xs.length := by
                have hne : k ≠ xs.length := Ne.symm hbound
                exact Nat.succ_le_of_lt (Nat.lt_of_le_of_ne hklen hne)
              have ih1 := ih (k + 1) (by omega) hsucc_len
              have ih0 := ih k hkpos hklen
              rw [ih1, hadd, ih0]
              have hsplit := choose_split xs.length k hkpos hklen
              have hsplitInt : (choose xs.length k : Int) =
                  (choose (xs.length - 1) k : Int) +
                    (choose (xs.length - 1) (k - 1) : Int) := by
                exact_mod_cast hsplit
              rw [hsplitInt]
              grind

theorem sound_Samples_SampleMean : Samples.SampleMean.Goal := by
  intro xs k hk hlen
  exact samples_sum_total xs k hk hlen

private theorem perm_sum_int {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp [Int.add_comm, Int.add_left_comm, Int.add_assoc]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem perm_append_singleton_cons (x : Int) : ∀ xs : List Int,
    (xs ++ [x]).Perm (x :: xs)
  | [] => by simp
  | y :: ys => by
      have hs : (x :: y :: ys).Perm (y :: x :: ys) := List.Perm.swap y x ys
      have ht := perm_append_singleton_cons x ys
      exact (ht.cons y).trans hs.symm

private theorem sums_one_perm (xs : List Int) : (sums xs 1).Perm xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hc : sums (x :: xs) 1 = sums xs 1 ++ (sums xs 0).map (x + ·) := by
        simpa using sums_cons x xs 0
      rw [hc]
      simpa [sums, samples] using (ih.append_right [x]).trans (perm_append_singleton_cons x xs)

private theorem samples_energy_one (xs : List Int) :
    ((sums xs 1).map (· ^ 2)).sum = (xs.map (· ^ 2)).sum := by
  exact perm_sum_int ((sums_one_perm xs).map (· ^ 2))

private theorem samples_energy_cons (x : Int) (xs : List Int) (k : Nat) :
    ((sums (x :: xs) (k + 1)).map (· ^ 2)).sum =
      ((sums xs (k + 1)).map (· ^ 2)).sum +
        x ^ 2 * (choose xs.length k : Int) + 2 * x * (sums xs k).sum +
          ((sums xs k).map (· ^ 2)).sum := by
  rw [sums_cons, List.map_append, List.sum_append, List.map_map]
  change ((sums xs (k + 1)).map (· ^ 2)).sum +
      ((sums xs k).map fun z => (x + z) ^ 2).sum = _
  rw [sum_map_square_add]
  have hlen_sums : (sums xs k).length = choose xs.length k := by
    simp [sums, samples_length]
  rw [hlen_sums]
  grind

private theorem samples_energy_total (xs : List Int) : ∀ (k : Nat), 2 ≤ k → k ≤ xs.length →
    ((sums xs k).map (· ^ 2)).sum =
      (choose (xs.length - 1) (k - 1) : Int) * (xs.map (· ^ 2)).sum +
        (choose (xs.length - 2) (k - 2) : Int) *
          (xs.sum ^ 2 - (xs.map (· ^ 2)).sum) := by
  induction xs with
  | nil =>
      intro k hk hlen
      simp only [List.length_nil] at hlen
      omega
  | cons x xs ih =>
      intro k hk hlen
      cases k with
      | zero => omega
      | succ k =>
          cases k with
          | zero => omega
          | succ k =>
              simp only [List.length_cons] at hlen ⊢
              have hlen2 : xs.length + 1 - 2 = xs.length - 1 := by omega
              rw [hlen2]
              simp only [Nat.add_sub_cancel, List.map_cons, List.sum_cons]
              let j := k + 1
              have hjpos : 0 < j := by dsimp [j]; omega
              have hjlen : j ≤ xs.length := by dsimp [j] at *; omega
              have hrec := samples_energy_cons x xs j
              have hF := samples_sum_total xs j hjpos hjlen
              by_cases hbound : xs.length = j
              · have htail : sums xs (j + 1) = [] := by
                  simp [sums, samples_nil_of_length_lt xs (j + 1) (by omega)]
                have hself : sums xs j = [xs.sum] := by
                  unfold sums
                  rw [← hbound, samples_self]
                  rfl
                dsimp [j] at hbound hF ⊢
                rw [hrec, htail, hF, hself]
                simp only [List.map_nil, List.sum_nil, List.map_cons, List.sum_cons,
                  Nat.add_sub_cancel]
                dsimp [j] at ⊢
                have hidx : k + 1 + 1 - 2 = k := by omega
                rw [hidx, hbound]
                simp only [Nat.add_sub_cancel]
                rw [choose_diag (k + 1), choose_diag k]
                grind
              · have hsucc_len : j + 1 ≤ xs.length := by
                  have hne : j ≠ xs.length := Ne.symm hbound
                  exact Nat.succ_le_of_lt (Nat.lt_of_le_of_ne hjlen hne)
                have hhigh := ih (j + 1) (by omega) hsucc_len
                rw [hrec, hhigh, hF]
                by_cases hj1 : j = 1
                · have hk0 : k = 0 := by dsimp [j] at hj1; omega
                  subst k
                  dsimp [j] at hhigh hF ⊢
                  rw [samples_energy_one]
                  simp only [Nat.add_sub_cancel] at hhigh ⊢
                  have hsplit := choose_split xs.length 1 Nat.zero_lt_one (by omega)
                  have hsplitInt : (choose xs.length 1 : Int) =
                      (choose (xs.length - 1) 1 : Int) +
                        (choose (xs.length - 1) 0 : Int) := by
                    exact_mod_cast hsplit
                  rw [hsplitInt]
                  simp only [choose]
                  grind
                · have hjtwo : 2 ≤ j := by omega
                  have hlow := ih j hjtwo hjlen
                  rw [hlow]
                  simp only [Nat.add_sub_cancel] at hhigh hlow ⊢
                  have hsplitA := choose_split xs.length j hjpos hjlen
                  have hsplitAInt : (choose xs.length j : Int) =
                      (choose (xs.length - 1) j : Int) +
                        (choose (xs.length - 1) (j - 1) : Int) := by
                    exact_mod_cast hsplitA
                  have hsplitB := choose_split (xs.length - 1) (j - 1) (by omega) (by omega)
                  have hsplitBInt : (choose (xs.length - 1) (j - 1) : Int) =
                      (choose (xs.length - 2) (j - 1) : Int) +
                        (choose (xs.length - 2) (j - 2) : Int) := by
                    exact_mod_cast hsplitB
                  rw [hsplitAInt, hsplitBInt]
                  grind

theorem sound_Samples_SampleEnergy : Samples.SampleEnergy.Goal := by
  intro xs k hk hlen
  exact samples_energy_total xs k hk hlen

/-! ### Samples.ShardSampling -/

private theorem perm_flatMap {α β : Type} (f : α → List β) {l₁ l₂ : List α}
    (p : l₁.Perm l₂) : (l₁.flatMap f).Perm (l₂.flatMap f) := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih.append_left (f a)
  | swap a b l =>
      simpa using (List.perm_append_comm (l₁ := f b) (l₂ := f a)).append_right (l.flatMap f)
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem perm_flatMap_fun {α β : Type} {l : List α} (f g : α → List β)
    (h : ∀ a ∈ l, (f a).Perm (g a)) : (l.flatMap f).Perm (l.flatMap g) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.flatMap_cons]
      have ha := h a (by simp)
      have ht : ∀ z ∈ l, (f z).Perm (g z) := by intro z hz; exact h z (by simp [hz])
      exact (ha.append_right (l.flatMap f)).trans ((ih ht).append_left (g a))

private theorem perm_four {α : Type} (a b c d : List α) :
    (a ++ b ++ (c ++ d)).Perm ((a ++ c) ++ (b ++ d)) := by
  simpa only [List.append_assoc] using
    ((List.perm_append_comm (l₁ := b) (l₂ := c)).append_left a).append_right d

private theorem flatMap_append_parts_perm {α β γ : Type} (l : List α) (f g : α → List β)
    (h : α → β → List γ) :
    (l.flatMap (fun a => (f a ++ g a).flatMap (h a))).Perm
      ((l.flatMap fun a => (f a).flatMap (h a)) ++
        (l.flatMap fun a => (g a).flatMap (h a))) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.flatMap_cons]
      rw [List.flatMap_append]
      have h1 : (List.flatMap (h a) (f a) ++ List.flatMap (h a) (g a) ++
          l.flatMap (fun a => (f a ++ g a).flatMap (h a))).Perm
          (List.flatMap (h a) (f a) ++ List.flatMap (h a) (g a) ++
            ((l.flatMap fun a => (f a).flatMap (h a)) ++
              (l.flatMap fun a => (g a).flatMap (h a)))) := by
        simpa only [List.append_assoc] using ih.append_left
          (List.flatMap (h a) (f a) ++ List.flatMap (h a) (g a))
      exact h1.trans (perm_four _ _ _ _)

private theorem flatMap_const_nil {α β : Type} (l : List α) :
    l.flatMap (fun _ => ([] : List β)) = [] := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem flatMap_map_add_list (xs : List Int) (g : Int → List Int) (x : Int) :
    xs.flatMap (fun s => (g s).map (x + ·)) = (xs.flatMap g).map (x + ·) := by
  induction xs with
  | nil => rfl
  | cons s ss ih =>
      rw [List.flatMap_cons, List.flatMap_cons, ih, List.map_append]

private theorem flatMap_flatMap_add {α : Type} (l : List α) (f : α → List Int)
    (g : α → Int → List Int) (x : Int) :
    l.flatMap (fun a => (f a).flatMap fun s => (g a s).map (x + ·)) =
      (l.flatMap fun a => (f a).flatMap fun s => g a s).map (x + ·) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      rw [List.flatMap_cons, List.flatMap_cons, List.map_append, ih,
        flatMap_map_add_list]

private theorem perm_prefix_sums (xs bs : List Int) (x : Int) :
    ((xs.map (x + ·)).flatMap fun v => bs.map (v + ·)).Perm
      ((xs.flatMap fun s => bs.map (s + ·)).map (x + ·)) := by
  induction xs with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.flatMap_cons, List.map_append, List.map_map]
      have hhead : (bs.map fun t => (x + s) + t) = bs.map fun t => x + (s + t) := by
        apply List.map_congr_left
        intro t ht
        grind
      rw [hhead]
      exact (List.Perm.refl _).append ih

private theorem sums_append_perm (a b : List Int) : ∀ (k : Nat),
    (sums (a ++ b) k).Perm ((List.range (k + 1)).flatMap (fun i =>
      (sums a i).flatMap (fun x => (sums b (k - i)).map (x + ·)))) := by
  induction a with
  | nil =>
      intro k
      cases k with
      | zero => simp [sums, samples]
      | succ k =>
          have hr : List.range (k + 1 + 1) = 0 :: List.map Nat.succ (List.range (k + 1)) :=
            List.range_succ_eq_map
          rw [hr]
          simp only [List.flatMap_cons, List.flatMap_map]
          have hz : sums ([] : List Int) 0 = [0] := rfl
          have hs : ∀ i : Nat, sums ([] : List Int) (i + 1) = [] := by
            intro i
            rfl
          simp [hz, hs]
          rw [flatMap_const_nil]
          simp
  | cons x xs ih =>
      intro k
      cases k with
      | zero => simp [sums, samples]
      | succ k =>
          have hc := sums_cons x (xs ++ b) k
          have hr : List.range (k + 1 + 1) = 0 :: List.map Nat.succ (List.range (k + 1)) :=
            List.range_succ_eq_map
          simp only [List.cons_append]
          rw [hc, hr]
          simp only [List.flatMap_cons, List.flatMap_map]
          have hzero : (sums (x :: xs) 0).flatMap
              (fun v => (sums b (k + 1 - 0)).map (v + ·)) = sums b (k + 1) := by
            simp [sums, samples]
          rw [hzero]
          have hsplit : ((List.range (k + 1)).flatMap (fun i =>
              (sums (x :: xs) (i + 1)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              (((List.range (k + 1)).flatMap (fun i =>
                (sums xs (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))) ++
              ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).map (x + ·)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·))))) := by
            simpa only [sums_cons] using
              flatMap_append_parts_perm (List.range (k + 1))
                (fun i => sums xs (i + 1)) (fun i => (sums xs i).map (x + ·))
                (fun (i : Nat) (v : Int) =>
                  (sums b ((k : Nat) + 1 - (i + 1))).map (fun t => v + t))
          have hrange : (List.range (k + 1 + 1)).flatMap (fun i =>
              (sums xs i).flatMap
                (fun v => (sums b (k + 1 - i)).map (v + ·))) =
              sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
                (sums xs (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·))) := by
            rw [List.range_succ_eq_map, List.flatMap_cons, List.flatMap_map]
            simp [sums, samples, Nat.sub_zero]
          have hp1 : (sums b (k + 1) ++ (List.range (k + 1)).flatMap (fun i =>
              (sums xs (i + 1)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              (sums (xs ++ b) (k + 1)) := by
            have hp := (ih (k + 1)).symm
            rw [hrange] at hp
            exact hp
          have hp2 : ((List.range (k + 1)).flatMap (fun i =>
              ((sums xs i).map (x + ·)).flatMap
                (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
              ((sums (xs ++ b) k).map (x + ·)) := by
            have hlocal : ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).map (x + ·)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))).Perm
                ((List.range (k + 1)).flatMap (fun i =>
                  ((sums xs i).flatMap fun s =>
                    (sums b (k + 1 - (i + 1))).map (s + ·)).map (x + ·))) := by
              apply perm_flatMap_fun
              intro i hi
              simpa only [List.flatMap_map] using
                perm_prefix_sums (sums xs i) (sums b (k + 1 - (i + 1))) x
            have hsub : ∀ i : Nat, k + 1 - (i + 1) = k - i := by
              intro i
              omega
            have hih : (sums (xs ++ b) k).Perm ((List.range (k + 1)).flatMap (fun i =>
                (sums xs i).flatMap
                  (fun s => (sums b (k + 1 - (i + 1))).map (s + ·)))) := by
              simpa only [hsub] using ih k
            have hmapEq : ((List.range (k + 1)).flatMap (fun i =>
                  (sums xs i).flatMap
                    (fun s => (sums b (k + 1 - (i + 1))).map (s + ·)))).map (x + ·) =
                (List.range (k + 1)).flatMap (fun i =>
                  ((sums xs i).flatMap
                    (fun s => (sums b (k + 1 - (i + 1))).map (s + ·))).map (x + ·)) :=
              List.map_flatMap
            have hright : ((List.range (k + 1)).flatMap (fun i =>
                  ((sums xs i).flatMap
                    (fun s => (sums b (k + 1 - (i + 1))).map (s + ·))).map (x + ·))).Perm
                ((sums (xs ++ b) k).map (x + ·)) := by
              rw [← hmapEq]
              exact (hih.symm).map (x + ·)
            exact hlocal.trans hright
          have hcomb : (sums (xs ++ b) (k + 1) ++
              (sums (xs ++ b) k).map (x + ·)).Perm
              (sums b (k + 1) ++ (((List.range (k + 1)).flatMap (fun i =>
                (sums xs (i + 1)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))) ++
              ((List.range (k + 1)).flatMap (fun i =>
                ((sums xs i).map (x + ·)).flatMap
                  (fun v => (sums b (k + 1 - (i + 1))).map (v + ·)))))) := by
            simpa only [List.append_assoc] using (hp1.append hp2).symm
          simpa only [List.append_assoc] using
            hcomb.trans (hsplit.append_left (sums b (k + 1))).symm

theorem sound_Samples_ShardSampling : Samples.ShardSampling.Goal := by
  intro a b k
  exact sums_append_perm a b k

/-! ### Couplings.Rearrangement and TransportLower -/

theorem sound_Couplings_Rearrangement : Couplings.Rearrangement.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_Rearrangement.sound_Couplings_Rearrangement

theorem sound_Couplings_TransportLower : Couplings.TransportLower.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_TransportLower.sound_Couplings_TransportLower

/-! ### Couplings.FarRows -/

private theorem far_rows_aux (rs : Couplings.Rows) (d : Nat) :
    (d + 1) * (rs.countP fun r : Int × Int => d < (r.1 - r.2).natAbs) ≤
      (rs.map fun r : Int × Int => (r.1 - r.2).natAbs).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : d < (r.1 - r.2).natAbs
      · simp [h]
        have hsucc : d + 1 ≤ (r.1 - r.2).natAbs := Nat.succ_le_of_lt h
        have hdistr : (d + 1) *
            ((rs.countP fun r : Int × Int => d < (r.1 - r.2).natAbs) + 1) =
            (d + 1) * (rs.countP fun r : Int × Int => d < (r.1 - r.2).natAbs) + (d + 1) := by
          grind
        rw [hdistr]
        omega
      · simp [h]
        omega

theorem sound_Couplings_FarRows : Couplings.FarRows.Goal := by
  intro rs d
  simpa [Couplings.cost] using far_rows_aux rs d

/-! ### PageMoments.ShiftOrigin -/

theorem sound_PageMoments_ShiftOrigin : PageMoments.ShiftOrigin.Goal := by
  intro s a b
  induction s with
  | nil => simp [PageMoments.energy]
  | cons x xs ih =>
      simp only [PageMoments.energy, List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

/-! ### PageMoments.AllInside -/

private theorem term_le_energy (s : List Int) (c x : Int) (hx : x ∈ s) :
    (x - c) ^ 2 ≤ PageMoments.energy s c := by
  induction s with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [PageMoments.energy, List.map_cons, List.sum_cons] at *
      rw [List.mem_cons] at hx
      rcases hx with rfl | hx
      · have hnon : 0 ≤ (ys.map fun z => (z - c) ^ 2).sum := by
          apply sum_map_nonneg
          intro z hz
          exact sq_nonneg_int (z - c)
        omega
      · have hnon : 0 ≤ (y - c) ^ 2 := sq_nonneg_int (y - c)
        have ht := ih hx
        omega

theorem sound_PageMoments_AllInside : PageMoments.AllInside.Goal := by
  intro s c r hen
  apply List.filter_eq_self.mpr
  intro x hx
  by_cases hleNat : (x - c).natAbs ≤ r
  · exact decide_eq_true hleNat
  · have hr : r < (x - c).natAbs := Nat.lt_of_not_ge hleNat
    have hle : ((r + 1 : Nat) : Int) ≤ ((x - c).natAbs : Int) := by
      exact_mod_cast (Nat.succ_le_of_lt hr)
    have hrnon : 0 ≤ ((r + 1 : Nat) : Int) := by exact_mod_cast Nat.zero_le _
    have hsq : ((r + 1 : Nat) : Int) ^ 2 ≤ (((x - c).natAbs : Nat) : Int) ^ 2 :=
      sq_le_sq_of_nonneg hrnon hle
    rw [natAbs_sq_cast] at hsq
    have hterm := term_le_energy s c x hx
    omega

/-! ### PageMoments.UnroundedTail -/

private theorem scaled_sum_sq_aux (s : List Int) (n S : Int) :
    (s.map fun x => (n * x - S) ^ 2).sum =
      n ^ 2 * (s.map fun x => x ^ 2).sum - 2 * n * S * s.sum + (s.length : Int) * S ^ 2 := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

private theorem mul_succ_cast (c : Nat) (R : Int) : ((c : Int) + 1) * R = (c : Int) * R + R := by
  grind

private theorem far_count_aux (s : List Int) (n S : Int) (r : Nat) :
    (s.countP (fun x => r < (n * x - S).natAbs) : Int) * ((r : Int) + 1) ^ 2 ≤
      (s.map fun x => (n * x - S) ^ 2).sum := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : r < (n * x - S).natAbs
      · simp [h]
        have hsucc : r + 1 ≤ (n * x - S).natAbs := Nat.succ_le_of_lt h
        have hle : ((r + 1 : Nat) : Int) ≤ (((n * x - S).natAbs : Nat) : Int) := by
          exact_mod_cast hsucc
        have hrnon : 0 ≤ ((r + 1 : Nat) : Int) := by exact_mod_cast Nat.zero_le _
        have hsq' : ((r + 1 : Nat) : Int) ^ 2 ≤ (n * x - S) ^ 2 := by
          rw [← natAbs_sq_cast (n * x - S)]
          exact sq_le_sq_of_nonneg hrnon hle
        have hcast : ((r + 1 : Nat) : Int) = (r : Int) + 1 := by simp
        rw [hcast] at hsq'
        rw [mul_succ_cast ((xs.countP fun x => r < (n * x - S).natAbs)) (((r : Int) + 1) ^ 2)]
        omega
      · simp [h]
        have hnon : 0 ≤ (n * x - S) ^ 2 := sq_nonneg_int (n * x - S)
        omega

theorem sound_PageMoments_UnroundedTail : PageMoments.UnroundedTail.Goal := by
  intro s r
  have hcnt := far_count_aux s (s.length : Int) s.sum r
  have hscl := scaled_sum_sq_aux s (s.length : Int) s.sum
  have hlen : (s.length : Int) ^ 2 * (s.map fun x => x ^ 2).sum -
        2 * (s.length : Int) * s.sum * s.sum + (s.length : Int) * s.sum ^ 2 =
      (s.length : Int) * ((s.length : Int) * (s.map fun x => x ^ 2).sum - s.sum ^ 2) := by
    grind
  have hcast : ((r + 1 : Nat) : Int) = (r : Int) + 1 := by simp
  rw [hcast]
  rw [hscl] at hcnt
  rw [hlen] at hcnt
  exact hcnt

/-! ### PageMoments.TrimError -/

private theorem sum_sq_cauchy (zs : List Int) :
    zs.sum ^ 2 ≤ (zs.length : Int) * (zs.map fun z => z ^ 2).sum := by
  induction zs with
  | nil => simp
  | cons z zs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons]
      have hcast : (((zs.length + 1 : Nat) : Int)) = (zs.length : Int) + 1 := by simp
      rw [hcast]
      by_cases hk : zs.length = 0
      · simp [hk]
        grind
      · have hkposNat : 0 < zs.length := Nat.pos_of_ne_zero hk
        have hkpos : (0 : Int) < zs.length := by exact_mod_cast hkposNat
        let k : Int := zs.length
        let Q : Int := (zs.map fun z => z ^ 2).sum
        let S : Int := zs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hsq : 0 ≤ (k * z - S) ^ 2 := sq_nonneg_int (k * z - S)
        have hident : (k * z - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * z ^ 2 - 2 * z * S + Q) := by grind
        have hprod : 0 ≤ k * (k * z ^ 2 - 2 * z * S + Q) := by
          rw [← hident]
          omega
        have hmain : 0 ≤ k * z ^ 2 - 2 * z * S + Q := by
          by_cases hnonneg : 0 ≤ k * z ^ 2 - 2 * z * S + Q
          · exact hnonneg
          · have hneg : k * z ^ 2 - 2 * z * S + Q < 0 := by omega
            have hmulneg : k * (k * z ^ 2 - 2 * z * S + Q) < 0 :=
              Int.mul_neg_of_pos_of_neg (by simpa [k] using hkpos) hneg
            omega
        have hlhs : (z + S) ^ 2 = z ^ 2 + 2 * z * S + S ^ 2 := by grind
        have hrhs : (k + 1) * (z ^ 2 + Q) = z ^ 2 + Q + k * z ^ 2 + k * Q := by grind
        change (z + S) ^ 2 ≤ (k + 1) * (z ^ 2 + Q)
        rw [hlhs, hrhs]
        omega

private theorem energy_partition (s : List Int) (c : Int) (r : Nat) :
    PageMoments.energy s c =
      PageMoments.energy (PageMoments.trimmed s c r) c +
        PageMoments.energy (s.filter fun x => !(x - c).natAbs ≤ r) c := by
  unfold PageMoments.energy PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h, ih]
        grind
      · simp [h, ih]
        grind

private theorem centered_partition (s : List Int) (c : Int) (r : Nat) :
    s.sum - (s.length : Int) * c =
      ((PageMoments.trimmed s c r).sum - ((PageMoments.trimmed s c r).length : Int) * c) +
        ((s.filter fun x => !(x - c).natAbs ≤ r).map fun x => x - c).sum := by
  unfold PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h]
        have hleft : x + xs.sum - ((xs.length : Int) + 1) * c =
            (x - c) + (xs.sum - (xs.length : Int) * c) := by grind
        rw [hleft, ih]
        grind
      · simp [h]
        have hleft : x + xs.sum - ((xs.length : Int) + 1) * c =
            (x - c) + (xs.sum - (xs.length : Int) * c) := by grind
        rw [hleft, ih]
        grind

private theorem trimmed_length_add_outside (s : List Int) (c : Int) (r : Nat) :
    (PageMoments.trimmed s c r).length + (s.filter fun x => !(x - c).natAbs ≤ r).length = s.length := by
  unfold PageMoments.trimmed
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      by_cases h : (x - c).natAbs ≤ r
      · simp [h]
        omega
      · simp [h]
        omega

theorem sound_PageMoments_TrimError : PageMoments.TrimError.Goal := by
  intro s c r
  let inside := PageMoments.trimmed s c r
  let outside := s.filter fun x => !(x - c).natAbs ≤ r
  have hE := energy_partition s c r
  have hC := centered_partition s c r
  have hcauchy := sum_sq_cauchy (outside.map fun x => x - c)
  have hlen_in : inside.length + outside.length = s.length := by
    exact trimmed_length_add_outside s c r
  have hlen_eq : s.length - inside.length = outside.length := by
    rw [← hlen_in, Nat.add_sub_cancel_left]
  have hC' : s.sum - (s.length : Int) * c =
      (inside.sum - (inside.length : Int) * c) + (outside.map fun x => x - c).sum := by
    simpa [inside, outside] using hC
  have hsum_zs : (outside.map fun x => x - c).sum =
      (s.sum - (s.length : Int) * c) - (inside.sum - (inside.length : Int) * c) := by
    omega
  have heq_energy : PageMoments.energy s c - PageMoments.energy inside c =
      (outside.map fun x => (x - c) ^ 2).sum := by
    unfold inside
    rw [hE]
    unfold PageMoments.energy
    grind
  change ((s.sum - (s.length : Int) * c) - (inside.sum - (inside.length : Int) * c)) ^ 2 ≤
      ((s.length - inside.length : Nat) : Int) *
        (PageMoments.energy s c - PageMoments.energy inside c)
  rw [← hsum_zs, heq_energy, hlen_eq]
  simpa [Function.comp_def] using hcauchy

/-! ### HeavyBounds.TwoHashes -/

private theorem sum_map_le_sum_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      have := ih ht
      omega

private theorem count_le_hash_count (b : List Nat) (h : Nat → Nat) (x : Nat) :
    b.count x ≤ (b.map h).count (h x) := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      rw [List.count_cons, List.map_cons, List.count_cons]
      by_cases hyb : (y == x) = true
      · have hy : y = x := of_decide_eq_true hyb
        subst y
        simp [ih]
      · by_cases hhyb : (h y == h x) = true
        · simp [hyb, hhyb]
          omega
        · simp [hyb, hhyb]
          exact ih

theorem sound_HeavyBounds_TwoHashes : HeavyBounds.TwoHashes.Goal := by
  intro a b h g
  change (a.map fun x => b.count x).sum ≤
    (a.map fun x => min ((b.map h).count (h x)) ((b.map g).count (g x))).sum
  apply sum_map_le_sum_nat
  intro x hx
  have hh := count_le_hash_count b h x
  have hg := count_le_hash_count b g x
  exact (Nat.le_min).2 ⟨hh, hg⟩

/-! ### HeavyBounds.JointKey -/

private theorem countP_hash_eq_count (h g : Nat → Nat) (b : List Nat) (x : Nat)
    (hinj : ∀ y ∈ b, h x = h y → g x = g y → x = y) :
    b.countP (fun y => h x == h y && g x == g y) = b.count x := by
  induction b with
  | nil => rfl
  | cons y ys ih =>
      simp only [List.countP_cons, List.count_cons]
      have ih' := ih (by
        intro z hz h1 h2
        exact hinj z (by simp [hz]) h1 h2)
      by_cases hy : y = x
      · subst y
        simp [ih']
      · have hyb : (y == x) = false := Bool.eq_false_of_ne_true (by
          intro hb
          exact hy (of_decide_eq_true hb))
        have hinj' : ¬(h x = h y ∧ g x = g y) := by
          intro hp
          have hxy := hinj y (by simp) hp.1 hp.2
          exact hy hxy.symm
        have hcond : ¬ ((h x == h y && g x == g y) = true) := by
          intro hp
          have hp1 : (h x == h y) = true := (Bool.and_eq_true_iff.mp hp).1
          have hp2 : (g x == g y) = true := (Bool.and_eq_true_iff.mp hp).2
          exact hinj' ⟨of_decide_eq_true hp1, of_decide_eq_true hp2⟩
        have hybeq : ¬ ((y == x) = true) := by
          intro ht
          rw [hyb] at ht
          contradiction
        rw [ite_eq_right hcond, ite_eq_right hybeq]
        exact ih'

theorem sound_HeavyBounds_JointKey : HeavyBounds.JointKey.Goal := by
  intro a b h g hinj
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  have hcount := countP_hash_eq_count h g b x (by
    intro y hy h1 h2
    exact hinj x hx y hy h1 h2)
  exact hcount.symm

/-! ### HeavyBounds.TailFanout -/

private theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        have hm := List.mem_eraseDups.mp hmem
        simp at hm
      · exact eraseDups_nodup (as.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  have hlen := List.length_filter_le (fun b : Nat => ! b == a) as
  change (List.filter (fun b : Nat => ! b == a) as).length < as.length + 1
  omega

private theorem sum_eq_single_of_mem (h : List Nat) (hnd : h.Nodup) (x : Nat) (f : Nat → Nat) :
    (h.map fun v => if x == v then f v else 0).sum = if h.contains x then f x else 0 := by
  induction h with
  | nil => simp
  | cons v hs ih =>
      have hvnot : v ∉ hs := (List.nodup_cons.mp hnd).1
      have hnds : hs.Nodup := (List.nodup_cons.mp hnd).2
      rw [List.map_cons, List.sum_cons, List.contains_cons]
      by_cases hxv : (x == v) = true
      · have hxeq : x = v := of_decide_eq_true hxv
        subst x
        have hnot : (hs.contains v) = false := by
          apply Bool.eq_false_of_ne_true
          intro hc
          exact hvnot (List.contains_iff_mem.mp hc)
        rw [ih hnds, hnot]
        simp
      · have ihs := ih hnds
        rw [ihs]
        simp [hxv]

private theorem sum_const_nat {α : Type} (l : List α) (c : Nat) :
    (l.map fun _ => c).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hs : (xs.length + 1) * c = xs.length * c + c := by grind
      rw [hs]
      omega

private theorem hot_sum_eq_captured (a b hot : List Nat) :
    (a.map fun x => if x ∈ hot then b.count x else 0).sum =
      (hot.eraseDups.map fun v => a.count v * b.count v).sum := by
  induction a with
  | nil =>
      simp
      rw [sum_const_nat]
      simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.count_cons]
      rw [ih]
      have hmap : (hot.eraseDups.map fun v =>
          (xs.count v + (if x == v then 1 else 0)) * b.count v).sum =
          (hot.eraseDups.map fun v =>
            xs.count v * b.count v + (if x == v then b.count v else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro v hv
        by_cases h : (x == v) = true <;> simp [h] <;> grind
      rw [hmap, sum_map_add_nat]
      rw [sum_eq_single_of_mem hot.eraseDups (eraseDups_nodup hot) x b.count]
      by_cases hx : x ∈ hot
      · have hc : hot.eraseDups.contains x = true :=
          List.contains_iff_mem.mpr (List.mem_eraseDups.mpr hx)
        simp [hx, hc]
        omega
      · have hc : hot.eraseDups.contains x = false := by
          apply Bool.eq_false_of_ne_true
          intro hct
          exact hx (List.mem_eraseDups.mp (List.contains_iff_mem.mp hct))
        simp [hx, hc]

private theorem count_le_cap (xs : List Nat) (x : Nat) (hx : x ∈ xs) :
    xs.count x ≤ HeavyBounds.cap xs := by
  unfold HeavyBounds.cap
  have hmem1 : x ∈ xs.eraseDups := List.mem_eraseDups.mpr hx
  have hmem2 : xs.count x ∈ xs.eraseDups.map xs.count := by
    exact List.mem_map.mpr ⟨x, hmem1, rfl⟩
  exact List.le_max?_getD_of_mem hmem2

private theorem joinSize_le_length_mul_cap (a b : List Nat) :
    joinSize a b ≤ a.length * HeavyBounds.cap b := by
  unfold joinSize
  calc
    (a.map fun x => b.count x).sum ≤ (a.map fun _ => HeavyBounds.cap b).sum := by
      apply sum_map_le_sum_nat
      intro x hx
      by_cases hxb : x ∈ b
      · exact count_le_cap b x hxb
      · have hz : b.count x = 0 := (List.count_eq_zero).mpr hxb
        omega
    _ = a.length * HeavyBounds.cap b := sum_const_nat a (HeavyBounds.cap b)

private theorem sum_ite_filter {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    (l.map fun x => if p x then f x else 0).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
      · simp [h, ih]

private theorem count_filter_eq_self {α : Type} [DecidableEq α]
    (l : List α) (p : α → Bool) (x : α) (hx : p x = true) :
    (l.filter p).count x = l.count x := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      simp only [List.filter_cons]
      by_cases hpy : p y = true
      · rw [if_pos hpy]
        by_cases hy : y = x
        · subst y
          simp [ih]
        · simp [hy, ih]
      · rw [if_neg hpy]
        have hyx : ¬ y = x := by
          intro heq
          exact hpy (by simpa [heq] using hx)
        simp [hyx, ih]

private theorem rest_count_eq (b hot : List Nat) (x : Nat) (hx : x ∉ hot) :
    (HeavyBounds.rest b hot).count x = b.count x := by
  unfold HeavyBounds.rest
  apply count_filter_eq_self
  have hc : hot.contains x = false := by
    apply Bool.eq_false_of_ne_true
    intro hct
    exact hx (List.contains_iff_mem.mp hct)
  change (!hot.contains x) = true
  rw [hc]
  rfl

private theorem joinSize_partition (a b hot : List Nat) :
    joinSize a b =
      (a.map fun x => if hot.contains x then b.count x else 0).sum +
        joinSize (HeavyBounds.rest a hot) (HeavyBounds.rest b hot) := by
  unfold joinSize HeavyBounds.rest
  have hp : ∀ x : Nat, b.count x =
      (if hot.contains x then b.count x else 0) +
        (if !hot.contains x then (List.filter (fun y => !hot.contains y) b).count x else 0) := by
    intro x
    by_cases hc : (hot.contains x) = true
    · have hn : (!hot.contains x) = false := by rw [hc]; rfl
      have hnnot : ¬ ((!hot.contains x) = true) := by
        intro h
        rw [hn] at h
        contradiction
      rw [ite_eq_left hc, ite_eq_right hnnot]
      omega
    · have hcf : (hot.contains x) = false := Bool.eq_false_of_ne_true hc
      have hn : (!hot.contains x) = true := by rw [hcf]; rfl
      have hxmem : x ∉ hot := by
        intro hm
        exact hc (List.contains_iff_mem.mpr hm)
      have hr := rest_count_eq b hot x hxmem
      unfold HeavyBounds.rest at hr
      rw [ite_eq_right hc, ite_eq_left hn, hr]
      omega
  have hmap := congrArg List.sum (show (a.map fun x => b.count x) =
      (a.map fun x => (if hot.contains x then b.count x else 0) +
        (if !hot.contains x then (List.filter (fun y => !hot.contains y) b).count x else 0)) by
        apply List.map_congr_left
        intro x hxa
        exact hp x)
  rw [hmap, sum_map_add_nat]
  exact congrArg (fun t => (a.map fun x => if hot.contains x then b.count x else 0).sum + t)
    (sum_ite_filter a (fun x => !hot.contains x)
      (fun x => (List.filter (fun y => !hot.contains y) b).count x))

private theorem sum_map_indicator_eq_countP (bs : List Nat) (x : Nat) :
    (bs.map fun y => if y == x then 1 else 0).sum = bs.countP (fun y => y == x) := by
  induction bs with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      rw [ih]
      omega

private theorem sum_map_count_swap (a b : List Nat) :
    (a.map fun x => b.count x).sum = (b.map fun y => a.count y).sum := by
  induction a with
  | nil =>
      simp
      rw [sum_const_nat]
      simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      have htail : ∀ bs : List Nat,
          (bs.map fun y => (x :: xs).count y).sum = bs.count x + (bs.map fun y => xs.count y).sum := by
        intro bs
        have h1 : (bs.map fun y => (x :: xs).count y).sum =
            (bs.map fun y => (if y == x then 1 else 0) + xs.count y).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro y hy
          rw [List.count_cons]
          rw [beq_comm_nat y x]
          omega
        rw [h1, sum_map_add_nat, sum_map_indicator_eq_countP, countP_eq_count_nat]
      rw [← htail b]

private theorem joinSize_comm (a b : List Nat) : joinSize a b = joinSize b a := by
  unfold joinSize
  exact sum_map_count_swap a b

theorem sound_HeavyBounds_TailFanout : HeavyBounds.TailFanout.Goal := by
  intro a b hot
  let la := HeavyBounds.rest a hot
  let rb := HeavyBounds.rest b hot
  have hpart := joinSize_partition a b hot
  have hcapt0 : captured a b hot = (a.map fun x => if x ∈ hot then b.count x else 0).sum := by
    unfold captured
    exact (hot_sum_eq_captured a b hot).symm
  have hconv : (a.map fun x => if x ∈ hot then b.count x else 0).sum =
      (a.map fun x => if hot.contains x then b.count x else 0).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    by_cases hm : x ∈ hot
    · have hc := List.contains_iff_mem.mpr hm
      simp [hm, hc]
    · have hc : hot.contains x = false := by
        apply Bool.eq_false_of_ne_true
        intro hct
        exact hm (List.contains_iff_mem.mp hct)
      simp [hm, hc]
  have hcapt : captured a b hot = (a.map fun x => if hot.contains x then b.count x else 0).sum :=
    hcapt0.trans hconv
  rw [hpart, ← hcapt]
  unfold upper
  change captured a b hot + joinSize (HeavyBounds.rest a hot) (HeavyBounds.rest b hot) ≤
    captured a b hot + min ((HeavyBounds.rest a hot).length * HeavyBounds.cap (HeavyBounds.rest b hot))
      ((HeavyBounds.rest b hot).length * HeavyBounds.cap (HeavyBounds.rest a hot))
  have h1 : joinSize (HeavyBounds.rest a hot) (HeavyBounds.rest b hot) ≤
      (HeavyBounds.rest a hot).length * HeavyBounds.cap (HeavyBounds.rest b hot) :=
    joinSize_le_length_mul_cap _ _
  have h2 : joinSize (HeavyBounds.rest a hot) (HeavyBounds.rest b hot) ≤
      (HeavyBounds.rest b hot).length * HeavyBounds.cap (HeavyBounds.rest a hot) := by
    rw [joinSize_comm]
    exact joinSize_le_length_mul_cap _ _
  have hmin : joinSize (HeavyBounds.rest a hot) (HeavyBounds.rest b hot) ≤
      min ((HeavyBounds.rest a hot).length * HeavyBounds.cap (HeavyBounds.rest b hot))
        ((HeavyBounds.rest b hot).length * HeavyBounds.cap (HeavyBounds.rest a hot)) :=
    (Nat.le_min).2 ⟨h1, h2⟩
  omega

/-! ### HeavyBounds.MonotoneRefine -/

private theorem rest_cons_eq_filter (a : List Nat) (h : Nat) (hot : List Nat) :
    HeavyBounds.rest a (h :: hot) = (HeavyBounds.rest a hot).filter (fun x => x != h) := by
  unfold HeavyBounds.rest
  rw [List.filter_filter]
  apply congrArg (fun p : Nat → Bool => a.filter p)
  funext x
  rw [List.contains_cons]
  change (!(x == h || hot.contains x)) = ((!(x == h)) && (!(hot.contains x)))
  cases hxh : x == h <;> cases hxhot : hot.contains x <;> rfl

private theorem captured_cons_mem (a b hot : List Nat) (h : Nat) (hh : h ∈ hot) :
    captured a b (h :: hot) = captured a b hot := by
  unfold captured
  rw [← hot_sum_eq_captured a b (h :: hot), ← hot_sum_eq_captured a b hot]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  by_cases hxhot : x ∈ hot
  · have hxhh : x ∈ h :: hot := by simp [hxhot]
    simp [hxhot, hxhh]
  · have hxhh : ¬ x ∈ h :: hot := by
      intro hm
      simp at hm
      rcases hm with rfl | hm
      · exact hxhot hh
      · exact hxhot hm
    simp [hxhot, hxhh]

private theorem countM_single (l : List Nat) (h c : Nat) :
    (l.map fun x => if x == h then c else 0).sum = l.count h * c := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons]
      by_cases hy : (y == h) = true
      · rw [ite_eq_left hy]
        have hyeq : y = h := of_decide_eq_true hy
        subst y
        rw [List.count_cons]
        have hyy : (h == h) = true := by simp
        rw [if_pos hyy, ih]
        have hs : (List.count h ys + 1) * c = List.count h ys * c + c := by grind
        rw [hs]
        omega
      · rw [if_neg hy]
        rw [List.count_cons]
        rw [if_neg hy, ih]
        simp only [Nat.zero_add, Nat.add_zero]

private theorem captured_cons (a b hot : List Nat) (h : Nat) (hh : h ∉ hot) :
    captured a b (h :: hot) = captured a b hot + a.count h * b.count h := by
  unfold captured
  rw [← hot_sum_eq_captured a b (h :: hot), ← hot_sum_eq_captured a b hot]
  have hpoint : ∀ x : Nat, (if x ∈ h :: hot then b.count x else 0) =
      (if x ∈ hot then b.count x else 0) + (if x == h then b.count h else 0) := by
    intro x
    by_cases hxhot : x ∈ hot
    · have hxhh : x ∈ h :: hot := by simp [hxhot]
      have hxh : (x == h) = false := by
        apply Bool.eq_false_of_ne_true
        intro heq
        exact hh (by simpa [of_decide_eq_true heq] using hxhot)
      simp [hxhot, hxhh, hxh]
    · by_cases hxhEq : x = h
      · subst x
        simp [hh]
      · have hxhh : ¬ x ∈ h :: hot := by
          intro hm
          simp at hm
          rcases hm with rfl | hm
          · exact hxhEq rfl
          · exact hxhot hm
        have hxh : (x == h) = false := by
          apply Bool.eq_false_of_ne_true
          intro heq
          exact hxhEq (of_decide_eq_true heq)
        simp [hxhot, hxhh, hxh]
  have hmap := congrArg List.sum (show (a.map fun x => if x ∈ h :: hot then b.count x else 0) =
      (a.map fun x => (if x ∈ hot then b.count x else 0) + (if x == h then b.count h else 0)) by
      apply List.map_congr_left
      intro x hx
      exact hpoint x)
  rw [hmap, sum_map_add_nat, countM_single]

private theorem count_filter_eq_countP (l : List Nat) (p : Nat → Bool) (x : Nat) :
    (l.filter p).count x = l.countP (fun y => y == x && p y) := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      rw [List.filter_cons]
      by_cases hp : p y = true
      · rw [if_pos hp, List.count_cons, List.countP_cons, ih]
        by_cases hy : (y == x) = true <;> simp [hp, hy]
      · rw [if_neg hp, List.countP_cons, ih]
        by_cases hy : (y == x) = true <;> simp [hp, hy]

private theorem countP_and_le_left (l : List Nat) (p q : Nat → Bool) :
    l.countP (fun x => p x && q x) ≤ l.countP p := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      simp only [List.countP_cons]
      by_cases hp : p y = true <;> by_cases hq : q y = true <;> simp [hp, hq] <;> omega

private theorem count_filter_le (l : List Nat) (p : Nat → Bool) (x : Nat) :
    (l.filter p).count x ≤ l.count x := by
  rw [count_filter_eq_countP, ← countP_eq_count_nat l x]
  exact countP_and_le_left l (fun y => y == x) p

private theorem cap_filter_le (xs : List Nat) (p : Nat → Bool) :
    HeavyBounds.cap (xs.filter p) ≤ HeavyBounds.cap xs := by
  unfold HeavyBounds.cap
  let f := (xs.filter p).eraseDups.map (xs.filter p).count
  let g := xs.eraseDups.map xs.count
  by_cases hf : f = []
  · simp [f, hf]
  · rcases List.exists_mem_of_ne_nil f hf with ⟨v, hv⟩
    have hsome := List.isSome_max?_of_mem hv
    rcases (Option.isSome_iff_exists.mp hsome) with ⟨M, hM⟩
    have hget : f.max?.getD 0 = M := by rw [hM]; rfl
    have hm : f.max? = some (f.max?.getD 0) := by rw [hget, hM]
    have hbound : ∀ z ∈ f, z ≤ g.max?.getD 0 := by
      intro z hz
      rcases List.mem_map.mp hz with ⟨y, hy, rfl⟩
      have hymem : y ∈ xs.filter p := List.mem_eraseDups.mp hy
      have hyxs : y ∈ xs := (List.mem_filter.mp hymem).1
      have hle1 : (xs.filter p).count y ≤ xs.count y := count_filter_le xs p y
      have hle2 : xs.count y ≤ g.max?.getD 0 := by
        apply List.le_max?_getD_of_mem
        exact List.mem_map.mpr ⟨y, List.mem_eraseDups.mpr hyxs, rfl⟩
      omega
    have hmle : f.max?.getD 0 ≤ g.max?.getD 0 := (List.max?_le_iff hm).mpr hbound
    simpa [f, g] using hmle

private theorem rest_length_cons (a hot : List Nat) (h : Nat) (hh : h ∉ hot) :
    (HeavyBounds.rest a hot).length = (HeavyBounds.rest a (h :: hot)).length + a.count h := by
  rw [rest_cons_eq_filter]
  have hcn := rest_count_eq a hot h hh
  have hlen := List.length_eq_countP_add_countP (fun x : Nat => x == h) (l := HeavyBounds.rest a hot)
  have hc1 : (HeavyBounds.rest a hot).countP (fun x => x == h) = a.count h := by
    rw [countP_eq_count_nat, hcn]
  have hc2 : (HeavyBounds.rest a hot).countP (fun x => decide ¬(x == h) = true) =
      ((HeavyBounds.rest a hot).filter fun x => x != h).length := by
    rw [List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro x hx
    change (decide ¬(x == h) = true) = (!(x == h))
    cases hxeq : x == h <;> rfl
  rw [hc1, hc2] at hlen
  omega

theorem sound_HeavyBounds_MonotoneRefine : HeavyBounds.MonotoneRefine.Goal := by
  intro a b hot h
  by_cases hh : h ∈ hot
  · have hcont : ∀ x : Nat, (h :: hot).contains x = hot.contains x := by
      intro x
      rw [List.contains_cons]
      by_cases hx : (x == h) = true
      · have hxeq : x = h := of_decide_eq_true hx
        subst x
        have hht : hot.contains h = true := List.contains_iff_mem.mpr hh
        simpa [hht] using hh
      · simp [hx]
    have hrestA : HeavyBounds.rest a (h :: hot) = HeavyBounds.rest a hot := by
      unfold HeavyBounds.rest
      induction a with
      | nil => simp
      | cons x xs ih =>
          rw [List.filter_cons, List.filter_cons, ih, hcont x]
    have hrestB : HeavyBounds.rest b (h :: hot) = HeavyBounds.rest b hot := by
      unfold HeavyBounds.rest
      induction b with
      | nil => simp
      | cons x xs ih =>
          rw [List.filter_cons, List.filter_cons, ih, hcont x]
    unfold upper
    rw [captured_cons_mem a b hot h hh, hrestA, hrestB]
    exact Nat.le_refl _
  · have hcap := captured_cons a b hot h hh
    have hrestA := rest_cons_eq_filter a h hot
    have hrestB := rest_cons_eq_filter b h hot
    have hlenA := rest_length_cons a hot h hh
    have hlenB := rest_length_cons b hot h hh
    have hcapB : HeavyBounds.cap (HeavyBounds.rest b (h :: hot)) ≤
        HeavyBounds.cap (HeavyBounds.rest b hot) := by
      rw [hrestB]
      exact cap_filter_le (HeavyBounds.rest b hot) (fun x => x != h)
    have hcapA : HeavyBounds.cap (HeavyBounds.rest a (h :: hot)) ≤
        HeavyBounds.cap (HeavyBounds.rest a hot) := by
      rw [hrestA]
      exact cap_filter_le (HeavyBounds.rest a hot) (fun x => x != h)
    have hcb : b.count h ≤ HeavyBounds.cap (HeavyBounds.rest b hot) := by
      by_cases hm : h ∈ b
      · have hmem_rb : h ∈ HeavyBounds.rest b hot := by
          unfold HeavyBounds.rest
          apply List.mem_filter.mpr
          constructor
          · exact hm
          · have hc : hot.contains h = false := by
              apply Bool.eq_false_of_ne_true
              intro hct
              exact hh (List.contains_iff_mem.mp hct)
            change (!(hot.contains h)) = true
            rw [hc]
            rfl
        have hr := rest_count_eq b hot h hh
        rw [← hr]
        exact count_le_cap (HeavyBounds.rest b hot) h hmem_rb
      · have hz : b.count h = 0 := (List.count_eq_zero).mpr hm
        rw [hz]
        exact Nat.zero_le _
    have hca : a.count h ≤ HeavyBounds.cap (HeavyBounds.rest a hot) := by
      by_cases hm : h ∈ a
      · have hmem_ra : h ∈ HeavyBounds.rest a hot := by
          unfold HeavyBounds.rest
          apply List.mem_filter.mpr
          constructor
          · exact hm
          · have hc : hot.contains h = false := by
              apply Bool.eq_false_of_ne_true
              intro hct
              exact hh (List.contains_iff_mem.mp hct)
            change (!(hot.contains h)) = true
            rw [hc]
            rfl
        have hr := rest_count_eq a hot h hh
        rw [← hr]
        exact count_le_cap (HeavyBounds.rest a hot) h hmem_ra
      · have hz : a.count h = 0 := (List.count_eq_zero).mpr hm
        rw [hz]
        exact Nat.zero_le _
    let la := (HeavyBounds.rest a hot).length
    let rb := (HeavyBounds.rest b hot).length
    let la' := (HeavyBounds.rest a (h :: hot)).length
    let rb' := (HeavyBounds.rest b (h :: hot)).length
    let ma := HeavyBounds.cap (HeavyBounds.rest a hot)
    let mb := HeavyBounds.cap (HeavyBounds.rest b hot)
    let ma' := HeavyBounds.cap (HeavyBounds.rest a (h :: hot))
    let mb' := HeavyBounds.cap (HeavyBounds.rest b (h :: hot))
    let ca := a.count h
    let cb := b.count h
    have hla : la = la' + ca := by simpa [la, la', ca] using hlenA
    have hrb : rb = rb' + cb := by simpa [rb, rb', cb] using hlenB
    have hma : ma' ≤ ma := by simpa [ma, ma'] using hcapA
    have hmb : mb' ≤ mb := by simpa [mb, mb'] using hcapB
    have hcb' : cb ≤ mb := by simpa [cb, mb] using hcb
    have hca' : ca ≤ ma := by simpa [ca, ma] using hca
    have hx : la' * mb' + ca * cb ≤ la * mb := by
      have h1 : la' * mb' ≤ la' * mb := Nat.mul_le_mul_left la' hmb
      have h2 : ca * cb ≤ ca * mb := Nat.mul_le_mul_left ca hcb'
      rw [hla]
      have hs : (la' + ca) * mb = la' * mb + ca * mb := by grind
      rw [hs]
      omega
    have hy : rb' * ma' + ca * cb ≤ rb * ma := by
      have h1 : rb' * ma' ≤ rb' * ma := Nat.mul_le_mul_left rb' hma
      have h2 : ca * cb ≤ ma * cb := Nat.mul_le_mul_right cb hca'
      have h2' : ma * cb = cb * ma := by grind
      rw [h2'] at h2
      rw [hrb]
      have hs : (rb' + cb) * ma = rb' * ma + cb * ma := by grind
      rw [hs]
      omega
    have hmin' : min (la' * mb') (rb' * ma') + ca * cb ≤ min (la * mb) (rb * ma) := by
      have hlmin := Nat.min_le_left (la' * mb') (rb' * ma')
      have hrmin := Nat.min_le_right (la' * mb') (rb' * ma')
      have hl : min (la' * mb') (rb' * ma') + ca * cb ≤ la * mb := by omega
      have hr : min (la' * mb') (rb' * ma') + ca * cb ≤ rb * ma := by omega
      exact (Nat.le_min).2 ⟨hl, hr⟩
    unfold upper
    rw [hcap, hrestA, hrestB]
    have hfinal := Nat.add_le_add_left hmin' (captured a b hot)
    simpa [la, la', rb, rb', ma, mb, ma', mb', ca, cb, hrestA, hrestB,
      Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hfinal

/-! ### HeavyBounds.AliasError -/

private theorem countP_map_count (b : List Nat) (f : Nat → Nat) (x : Nat) :
    (b.map f).count (f x) = b.countP (fun y => f x == f y) := by
  have h : (b.map f).count (f x) = b.countP (fun y => f y == f x) := by
    induction b with
    | nil => simp
    | cons y ys ih =>
        simp only [List.count_cons, List.map_cons, List.countP_cons]
        by_cases hy : (f y == f x) = true
        · simp [hy, ih]
        · simp [hy, ih]
  rw [h]
  congr 1
  funext y
  rw [beq_comm_nat]

private theorem countP_bucket_split (b : List Nat) (f : Nat → Nat) (x : Nat) :
    b.countP (fun y => f x == f y) =
      b.count x + b.countP (fun y => y != x && f x == f y) := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.countP_cons, List.count_cons]
      by_cases hyb : (y == x) = true
      · have hy : y = x := of_decide_eq_true hyb
        subst y
        simp [ih]
        omega
      · by_cases hfy : (f x == f y) = true
        · simp [hyb, hfy, ih]
          have hyne : ¬ y = x := by intro heq; exact hyb (by simp [heq])
          simp [hyne]
          omega
        · simp [hyb, hfy, ih]

theorem sound_HeavyBounds_AliasError : HeavyBounds.AliasError.Goal := by
  intro a b f
  have hpoint : ∀ x : Nat, (b.map f).count (f x) =
      b.count x + b.countP (fun y => y != x && f x == f y) := by
    intro x
    rw [countP_map_count, countP_bucket_split]
  have hmap : (a.map fun x => (b.map f).count (f x)).sum =
      (a.map fun x => b.count x + b.countP (fun y => y != x && f x == f y)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact hpoint x
  have hjoin : joinSize (a.map f) (b.map f) = (a.map fun x => (b.map f).count (f x)).sum := by
    unfold joinSize
    rw [List.map_map]
    rfl
  rw [hjoin]
  change (a.map fun x => (b.map f).count (f x)).sum - (a.map fun x => b.count x).sum =
    (a.map fun x => b.countP (fun y => y != x && f x == f y)).sum
  rw [hmap, sum_map_add_nat]
  omega

/-! ### Known counterexamples -/

/-- Counterexample: rows [(0,1),(1,0)] have equal marginals but cost 2. -/
theorem refutation_Couplings_AlignedMarginals : ¬ Couplings.AlignedMarginals.Goal := by
  intro h
  let rs : Couplings.Rows := [(0, 1), (1, 0)]
  have hperm : (rs.map Prod.fst).Perm (rs.map Prod.snd) := by native_decide
  have hcost : Couplings.cost rs = 2 := by native_decide
  have hz := h rs hperm
  omega

/-- Counterexample: [-2,1,1] and [-1,-1,2] share length/sum/sum-of-squares
    but differ in the count of entries `>= 2`. -/
theorem refutation_PageMoments_MomentHistogram : ¬ PageMoments.MomentHistogram.Goal := by
  intro h
  let a : PageMoments.Page := [-2, 1, 1]
  let b : PageMoments.Page := [-1, -1, 2]
  have hlen : a.length = b.length := by native_decide
  have hsum : a.sum = b.sum := by native_decide
  have hen : PageMoments.energy a 0 = PageMoments.energy b 0 := by native_decide
  have hc := h a b hlen hsum hen 2
  have ha : a.countP (fun x => x ≥ 2) = 0 := by native_decide
  have hb : b.countP (fun x => x ≥ 2) = 1 := by native_decide
  omega

end TinylambOpt.ResearchProofs.Atlas.SynopsisBounds
