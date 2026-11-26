import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SplitHigh.CommonUpper` — `∀ xs h, xs≠[] → (∀ x∈xs, x/256=h) → (write xs).length =
  xs.length+ubytes h+ubytes xs.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_CommonUpper

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

/-- Local wire facts, specialised so the proof is self-contained. -/
private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have hh := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
        have h128 : (128 : Nat) = 2 ^ 7 := by decide
        rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_one (n : Nat) : (uleb n).length ≤ 1 ↔ n < 128 := by
  by_cases hn : n < 128
  · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
    rw [h1]
    simp [hn]
  · have hlen : 2 ≤ (uleb n).length := by
      rw [uleb_len_succ n hn]
      have := uleb_len_pos (n / 128)
      omega
    constructor
    · intro h; omega
    · intro h; omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      simpa using uleb_len_le_one n
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
              have hk1 : 1 ≤ k + 1 + 1 := by omega
              simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
      ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

/-- A list whose every lane has upper byte `h` maps to the constant list. -/
private theorem map_div_eq_replicate {xs : List Nat} {h : Nat}
    (hall : ∀ x ∈ xs, x / 256 = h) :
    xs.map (fun x => x / 256) = List.replicate xs.length h := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.length_cons]
      simp only [List.replicate, List.cons.injEq]
      constructor
      · exact hall x (by simp)
      · apply ih
        intro y hy
        exact hall y (by simp [hy])

/-- Runs of a nonempty constant list form a single run. -/
private theorem runs_replicate_succ (h n : Nat) :
    runs (List.replicate (n + 1) h) = [(h, n + 1)] := by
  induction n with
  | zero => simp [runs]
  | succ n ih =>
      change runs (h :: List.replicate (n + 1) h) = [(h, n + 1 + 1)]
      rw [runs, ih]
      simp

private theorem runs_replicate (h n : Nat) (hn : 0 < n) :
    runs (List.replicate n h) = [(h, n)] := by
  obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hn)
  exact runs_replicate_succ h k

/-- 入力確認: 共通 upper を持つ小さいリストで等式が成り立つ。 -/
private def gen (k n : Nat) : List (List Nat) :=
  match n with
  | 0 => [[]]
  | n + 1 => (gen k n).flatMap (fun xs => (List.range k).map (fun x => x :: xs))

#eval let hs : List Nat := [0, 1, 127, 128, 16384]
      let cs : List (Nat × List Nat) := hs.flatMap (fun h =>
        ((List.range 4).filter (fun n => 0 < n)).flatMap (fun n =>
          ((gen 3 n).filter (fun xs => xs ≠ [])).map (fun xs => (h, xs.map (fun v => h*256+v)))))
      cs.all (fun (h, xs) => (write xs).length == xs.length + ubytes h + ubytes xs.length)

theorem sound_CommonUpper : SplitHigh.CommonUpper.Goal := by
  intro xs h hne hall
  have hlen : 0 < xs.length := by
    cases xs with
    | nil => contradiction
    | cons x xs => simp
  have hmap : xs.map (fun x => x / 256) = List.replicate xs.length h :=
    map_div_eq_replicate hall
  have hruns : runs (xs.map (fun x => x / 256)) = [(h, xs.length)] := by
    rw [hmap]
    exact runs_replicate h xs.length hlen
  rw [write, hruns]
  simp [uleb_len]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_CommonUpper
