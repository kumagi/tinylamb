/-
Scratch proof for `SlidingWindowBacklogCap.Goal`. If every contiguous window
has arrival excess at most `cap`, no prefix backlog can exceed `cap`, even
when early service credits are unused. A prefix debt equals the largest
excess over a suffix of that prefix, and every such suffix is one of the
bounded windows of the original trace.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.SlidingWindowBacklogCap.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_SlidingWindowBacklogCap

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Total arrivals of a trace. -/
def sumAdd (ts : List Tick) : Nat := (ts.map Tick.added).sum

/-- Total service capacity of a trace. -/
def sumCap (ts : List Tick) : Nat := (ts.map Tick.capacity).sum

/-- Reflected-walk maximum: the final debt for any initial debt. -/
def worst (owed : Nat) : List Tick → Nat
  | [] => owed
  | t :: ts => max (owed + sumAdd (t :: ts) - sumCap (t :: ts)) (worst 0 ts)

/-- Splitting an initial credit across a following trace: arithmetic identity. -/
theorem worst_arith (x cap sa sc : Nat) :
    (x - cap + sa) - sc = max ((x + sa) - (cap + sc)) (sa - sc) := by
  by_cases h1 : x ≤ cap
  · by_cases h2 : sa ≤ sc
    · have hA : (x + sa) - (cap + sc) = 0 := by omega
      have hB : sa - sc = 0 := by omega
      simp [h1, hA, hB]
    · have hle : (x + sa) - (cap + sc) ≤ sa - sc := by omega
      rw [Nat.max_eq_right hle]
      simp [h1]
  · have hcap : cap ≤ x := Nat.le_of_lt (Nat.lt_of_not_ge h1)
    by_cases h2 : cap + sc ≤ x + sa
    · have hle : sa - sc ≤ (x + sa) - (cap + sc) := by omega
      rw [Nat.max_eq_left hle]
      omega
    · have hlt : x + sa < cap + sc := Nat.lt_of_not_ge h2
      have hA : (x - cap + sa) - sc = 0 := by omega
      have hB : sa - sc = 0 := by omega
      have hC : (x + sa) - (cap + sc) = 0 := by omega
      simp [hA, hB, hC]

/-- For zero initial debt the head recurrence is exactly the trace excess. -/
theorem worst_zero_cons (t : Tick) (ts : List Tick) :
    worst 0 (t :: ts) = max (excess (t :: ts)) (worst 0 ts) := by
  change max (0 + sumAdd (t :: ts) - sumCap (t :: ts)) (worst 0 ts) =
    max (excess (t :: ts)) (worst 0 ts)
  congr 1
  simp [excess, sumAdd, sumCap]

/-- The whole-trace excess is one of the suffixes covered by `worst`. -/
theorem excess_le_worst (ts : List Tick) : excess ts ≤ worst 0 ts := by
  cases ts with
  | nil => simp [excess, worst]
  | cons t ts =>
      rw [worst_zero_cons]
      exact Nat.le_max_left _ _

/-- The head of a trace can be folded into the initial debt. -/
theorem worst_prepend (owed : Nat) (t : Tick) (ts : List Tick) :
    worst (step owed t) ts = worst owed (t :: ts) := by
  cases ts with
  | nil =>
      simp [worst, step, sumAdd, sumCap]
  | cons u us =>
      change max (step owed t + sumAdd (u :: us) - sumCap (u :: us)) (worst 0 us) =
        max (owed + sumAdd (t :: u :: us) - sumCap (t :: u :: us)) (worst 0 (u :: us))
      rw [worst_zero_cons]
      have hleft : step owed t + sumAdd (u :: us) - sumCap (u :: us) =
          max (owed + sumAdd (t :: u :: us) - sumCap (t :: u :: us)) (excess (u :: us)) := by
        have haux := worst_arith (owed + t.added) t.capacity (sumAdd (u :: us)) (sumCap (u :: us))
        unfold step
        simp [sumAdd, sumCap, excess, Nat.add_assoc] at haux ⊢
        simpa [Nat.add_assoc] using haux
      rw [hleft, Nat.max_assoc]

/-- Folded debt is exactly the reflected-walk maximum. -/
theorem endDebt_eq_worst (owed : Nat) (ts : List Tick) : endDebt owed ts = worst owed ts := by
  induction ts generalizing owed with
  | nil => simp [endDebt, worst]
  | cons t ts ih =>
      change endDebt (step owed t) ts = worst owed (t :: ts)
      rw [ih (step owed t), worst_prepend]

/-- Suffix maxima satisfy the expected head recurrence. -/
theorem suffixExcess_cons (t : Tick) (ts : List Tick) :
    suffixExcess (t :: ts) = max (excess (t :: ts)) (suffixExcess ts) := by
  unfold suffixExcess
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.map_cons, List.map_map, Function.comp_def, List.drop_zero]
  have hmap :
      List.map (fun k => excess (List.drop (Nat.succ k) (t :: ts))) (List.range (ts.length + 1)) =
        List.map (fun k => excess (List.drop k ts)) (List.range (ts.length + 1)) := by
    apply List.map_congr_left
    intro k hk
    simp [List.drop_succ_cons]
  rw [hmap]
  cases hm : (List.map (fun k => excess (List.drop k ts)) (List.range (ts.length + 1))).max? <;>
    simp [List.max?_cons, hm]

/-- Final debt from an empty queue is the largest suffix excess. -/
theorem endDebt_eq_suffixExcess (ts : List Tick) : endDebt 0 ts = suffixExcess ts := by
  rw [endDebt_eq_worst]
  induction ts with
  | nil => simp [worst, suffixExcess, excess]
  | cons t ts ih =>
      rw [worst_zero_cons]
      rw [suffixExcess_cons, ih]

/-- The maximum of a list, with a default, respects a common upper bound. -/
theorem max?_getD_le (d cap : Nat) : ∀ xs : List Nat,
    (∀ x ∈ xs, x ≤ cap) → d ≤ cap → xs.max?.getD d ≤ cap
  | [], hx, hd => hd
  | x :: xs, hx, hd => by
      have hxle : x ≤ cap := hx x (by simp)
      have hxs : ∀ y ∈ xs, y ≤ cap := by
        intro y hy
        exact hx y (by simp [hy])
      cases hm : xs.max? with
      | none =>
          simpa [List.max?_cons, hm] using hxle
      | some m =>
          have hmle : m ≤ cap := by
            have h := max?_getD_le 0 cap xs hxs (Nat.zero_le cap)
            simpa [hm] using h
          simpa [List.max?_cons, hm, Nat.max_le] using ⟨hxle, hmle⟩

/-- A scan from `owed` is the sequence of debts of successive prefixes. -/
theorem scanl_eq_prefixDebts (owed : Nat) (ts : List Tick) :
    ts.scanl step owed = (List.range (ts.length + 1)).map (fun j => endDebt owed (ts.take j)) := by
  induction ts generalizing owed with
  | nil => simp [endDebt]
  | cons t ts ih =>
      rw [List.scanl_cons, ih (step owed t)]
      change owed :: List.map (fun j => endDebt (step owed t) (ts.take j)) (List.range (ts.length + 1)) =
        List.map (fun j => endDebt owed ((t :: ts).take j)) (List.range ((t :: ts).length + 1))
      rw [List.length_cons]
      have hrange : List.range (ts.length + 1 + 1) =
          0 :: List.map Nat.succ (List.range (ts.length + 1)) := by
        exact List.range_succ_eq_map
      rw [hrange]
      simp only [List.map_cons, List.map_map]
      simp only [List.take_zero, endDebt]
      congr 1

/-- A suffix maximum bounded by every whole-suffix excess. -/
theorem suffixExcess_le_of_windows (ts : List Tick) (cap : Nat)
    (h : ∀ k, excess (ts.drop k) ≤ cap) : suffixExcess ts ≤ cap := by
  unfold suffixExcess
  apply max?_getD_le
  · intro x hx
    rcases List.mem_map.mp hx with ⟨k, hk, rfl⟩
    exact h k
  · exact Nat.zero_le cap

/-- Bounded window excess implies a bounded high-water mark. -/
theorem sound : SlidingWindowBacklogCap.Goal := by
  intro ts cap hwin
  unfold high
  apply max?_getD_le
  · intro x hx
    rw [scanl_eq_prefixDebts] at hx
    rcases List.mem_map.mp hx with ⟨j, hj, rfl⟩
    rw [endDebt_eq_suffixExcess]
    apply suffixExcess_le_of_windows
    intro k
    simpa [List.drop_take] using hwin k (j - k)
  · exact Nat.zero_le cap

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_SlidingWindowBacklogCap
