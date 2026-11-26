/-
Scratch proof for `BusyPeriodDebtEnvelope.Goal`. Starting empty, the debt left
by a trace equals the largest excess over a suffix of that trace: this is the
last busy period. Unused early service only shifts the start of the busy
period; it cannot raise the final debt. The identity is what lets checkpoint
and compaction schedulers size backlog from suffix excesses.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_BusyPeriodDebtEnvelope. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_BusyPeriodDebtEnvelope

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
theorem sound : BusyPeriodDebtEnvelope.Goal := by
  intro ts
  rw [endDebt_eq_worst]
  induction ts with
  | nil => simp [worst, suffixExcess, excess]
  | cons t ts ih =>
      rw [worst_zero_cons]
      rw [suffixExcess_cons, ih]

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_BusyPeriodDebtEnvelope
