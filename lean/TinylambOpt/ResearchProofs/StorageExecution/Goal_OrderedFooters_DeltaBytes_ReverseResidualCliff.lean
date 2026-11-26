import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_ReverseResidualCliff

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

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

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  have hb_lt : b < 128 ^ (ubytes b) := (ubytes_le_iff b (ubytes b) hbpos).mp (Nat.le_refl _)
  have ha_lt : a < 128 ^ (ubytes b) := Nat.lt_of_le_of_lt h hb_lt
  exact (ubytes_le_iff a (ubytes b) hbpos).mpr ha_lt

private theorem ubytes_succ_le (n : Nat) : ubytes (n + 1) ≤ ubytes n + 1 := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  have hnlt : n < 128 ^ ubytes n := (ubytes_le_iff n (ubytes n) hpos).mp (Nat.le_refl _)
  have hle : n + 1 ≤ 128 ^ ubytes n := Nat.succ_le_of_lt hnlt
  have hstep : 128 ^ ubytes n < 128 ^ (ubytes n + 1) :=
    Nat.pow_lt_pow_right (by decide : 1 < 128) (by omega)
  have hlt : n + 1 < 128 ^ (ubytes n + 1) := Nat.lt_of_le_of_lt hle hstep
  exact (ubytes_le_iff (n + 1) (ubytes n + 1) (by omega)).mpr hlt

-- Zigzag maps d and -d to either the same number or to consecutive numbers.
private theorem zig_neg_le (d : Int) : zig (-d) ≤ zig d + 1 := by
  cases d with
  | ofNat n =>
      cases n with
      | zero => simp [zig]
      | succ n =>
          rw [show -(Int.ofNat (Nat.succ n)) = Int.negSucc n by
            rw [Int.negSucc_eq]; simp]
          simp [zig]
          omega
  | negSucc n =>
      rw [Int.neg_negSucc]
      simp [zig]
      omega

-- Reversing one predecessor edge costs at most one extra byte.
private theorem sbytes_neg_le (a b : Nat) : sbytes ((a : Int) - b) ≤ sbytes ((b : Int) - a) + 1 := by
  unfold sbytes
  have hneg : -((b : Int) - a) = (a : Int) - b := by omega
  rw [← hneg]
  have hm := ubytes_mono (zig_neg_le ((b : Int) - a))
  have hs := ubytes_succ_le (zig ((b : Int) - a))
  omega

private def size (l : List Nat) : Nat := ((agg.reference l).map (fun v => v.2.2)).getD 0

-- Forward byte cost, and the cost of the same edges traversed backwards.
private def costFrom (a : Nat) : List Nat → Nat
  | [] => 0
  | x :: xs => sbytes ((x : Int) - a) + costFrom x xs

private def costFromR (a : Nat) : List Nat → Nat
  | [] => 0
  | x :: xs => costFromR x xs + sbytes ((a : Int) - x)

private theorem size_cons : ∀ (xs : List Nat) (a : Nat), size (a :: xs) = costFrom a xs
  | [], a => by simp [size, agg, costFrom, edges]
  | x :: xs, a => by
      simp [size, agg, edges, costFrom]
      exact size_cons xs x

private theorem costFrom_append_pair (a : Nat) : ∀ (l : List Nat) (x y : Nat),
    costFrom a (l ++ [x, y]) = costFrom a (l ++ [x]) + sbytes ((y : Int) - (x : Int))
  | [], x, y => by simp [costFrom]
  | b :: l, x, y => by
      simp [costFrom, costFrom_append_pair b l x y]
      omega

private theorem size_append_pair : ∀ (l : List Nat) (x y : Nat),
    size (l ++ [x, y]) = size (l ++ [x]) + sbytes ((y : Int) - (x : Int))
  | [], x, y => by simp [size, agg, edges]
  | a :: l, x, y => by
      simp [size_cons, costFrom_append_pair]

-- (a :: l).reverse = l.reverse ++ [a], whose byte cost is costFromR a l.
private theorem size_rev_append (a : Nat) : ∀ l, size (l.reverse ++ [a]) = costFromR a l
  | [] => by simp [size, agg, edges, costFromR]
  | x :: xs => by
      have hpair := size_append_pair xs.reverse x a
      rw [size_rev_append x xs] at hpair
      simpa [List.reverse_cons, List.append_assoc, costFromR] using hpair

private theorem rev_le (a : Nat) : ∀ xs, costFromR a xs ≤ costFrom a xs + xs.length
  | [] => by simp [costFromR, costFrom]
  | x :: xs => by
      have ih := rev_le x xs
      have hEdge := sbytes_neg_le a x
      simp [costFromR, costFrom]
      omega

theorem sound_ReverseResidualCliff : DeltaBytes.ReverseResidualCliff.Goal := by
  intro xs
  change size xs.reverse ≤ size xs + (xs.length - 1)
  cases xs with
  | nil => simp [size, agg]
  | cons a l =>
      rw [List.reverse_cons]
      rw [size_rev_append]
      rw [size_cons]
      simpa using rev_le a l

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_ReverseResidualCliff
