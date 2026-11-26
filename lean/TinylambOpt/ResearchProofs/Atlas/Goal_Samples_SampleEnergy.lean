import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.

Proved

- `Samples.SampleEnergy` — `∀ xs k, 2 ≤ k → k ≤ xs.length → let q := (xs.map (·^2)).sum
  ((sums xs k).map (·^2)).sum = (choose (xs.length-1) (k-1):Int)*q + (choose
  (xs.length-2) (k-2):Int)*(xs.sum^2-q)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SampleEnergy

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-! Exhaustive small-input sanity check (values in [-2,2], length <= 3,
    k = 2 or 3): the pilot SUM second moment identity holds on every case. -/

def enum (n : Nat) : List (List Int) :=
  (List.range (5 ^ n)).map (fun c =>
    (List.range n).map (fun i => ((c / 5 ^ i) % 5 : Nat) - 2))

def checkCase (xs : List Int) (k : Nat) : Bool :=
  let q := (xs.map (fun x => x ^ 2)).sum
  ((sums xs k).map (fun z => z ^ 2)).sum =
    (choose (xs.length - 1) (k - 1) : Int) * q +
      (choose (xs.length - 2) (k - 2) : Int) * (xs.sum ^ 2 - q)

example : (enum 3).all (fun xs =>
    (List.range (xs.length + 1)).all (fun k => k < 2 || checkCase xs k)) := by
  native_decide

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

theorem sound_SampleEnergy : SampleEnergy.Goal := by
  intro xs k hk hlen
  exact samples_energy_total xs k hk hlen

end TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SampleEnergy
