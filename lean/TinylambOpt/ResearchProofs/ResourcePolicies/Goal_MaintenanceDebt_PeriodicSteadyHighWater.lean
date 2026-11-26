/-
Scratch proof for `PeriodicSteadyHighWater.Goal`. For a sustainable repeated
trace, total service covers total arrivals per cycle, so the debt after every
later cycle is no larger than after the first one. Consequently the high-water
mark of any number of cycles is already reached within the first two cycles.
This justifies checking only two cycles when provisioning a periodic
maintenance workflow.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.PeriodicSteadyHighWater.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_PeriodicSteadyHighWater

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

/-- A single service step is monotone in the incoming debt. -/
theorem step_mono (t : Tick) {a b : Nat} (h : a ≤ b) : step a t ≤ step b t := by
  unfold step
  exact Nat.sub_le_sub_right (Nat.add_le_add_right h t.added) t.capacity

/-- A trace is monotone in the incoming debt. -/
theorem endDebt_mono (ts : List Tick) {a b : Nat} (h : a ≤ b) : endDebt a ts ≤ endDebt b ts := by
  induction ts generalizing a b with
  | nil => simpa [endDebt] using h
  | cons t ts ih =>
      change endDebt (step a t) ts ≤ endDebt (step b t) ts
      exact ih (step_mono t h)

/-- The tail maximum used by `worst`. -/
def tailWorst : List Tick → Nat
  | [] => 0
  | _ :: ts => worst 0 ts

/-- Closed form of the reflected-walk maximum. -/
theorem worst_eq (owed : Nat) (ts : List Tick) :
    worst owed ts = max (owed + sumAdd ts - sumCap ts) (tailWorst ts) := by
  cases ts with
  | nil => simp [worst, tailWorst, sumAdd, sumCap]
  | cons t ts => rfl

/-- A sustainable trace cannot increase the debt beyond the larger of its
incoming debt and its empty-start debt. -/
theorem endDebt_le_max (ts : List Tick) (hs : sumAdd ts ≤ sumCap ts) (d : Nat) :
    endDebt d ts ≤ max d (endDebt 0 ts) := by
  rw [endDebt_eq_worst, worst_eq, endDebt_eq_worst, worst_eq]
  have hterm : (d + sumAdd ts) - sumCap ts ≤ d := by
    unfold sumAdd sumCap at hs ⊢
    omega
  have hzero : 0 + sumAdd ts - sumCap ts = 0 := by
    unfold sumAdd sumCap at hs ⊢
    omega
  rw [hzero, Nat.zero_max]
  exact Nat.max_le.mpr ⟨Nat.le_trans hterm (Nat.le_max_left d (tailWorst ts)),
    Nat.le_max_right d (tailWorst ts)⟩

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
          have hle : max x m ≤ cap := (Nat.max_le).mpr ⟨hxle, hmle⟩
          simpa [List.max?_cons, hm] using hle

/-- `max?` is none exactly for the empty list. -/
theorem max?_eq_nil (l : List Nat) : l.max? = none ↔ l = [] := by
  cases l with
  | nil => simp
  | cons x xs => simp [List.max?_cons]

/-- The maximum of a list is at least any of its members. -/
theorem le_max?_getD (l : List Nat) (d : Nat) {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD d := by
  induction l with
  | nil => simp at hx
  | cons y ys ih =>
      cases hm : ys.max? with
      | none =>
          have hnil : ys = [] := (max?_eq_nil ys).mp hm
          subst ys
          have hxy : x = y := by simpa using hx
          subst x
          simp [List.max?_cons]
      | some m =>
          rcases List.mem_cons.mp hx with hxy | hxy
          · subst x
            have hy : y ≤ max y m := Nat.le_max_left y m
            simpa [List.max?_cons, hm] using hy
          · have hy : x ≤ ys.max?.getD d := ih hxy
            have hy' : x ≤ m := by simpa [hm] using hy
            have hle : x ≤ max y m := Nat.le_trans hy' (Nat.le_max_right y m)
            simpa [List.max?_cons, hm] using hle

/-- The maximum of an append is the maximum of the two maxima. -/
theorem max?_getD_append (l1 l2 : List Nat) :
    (l1 ++ l2).max?.getD 0 = max (l1.max?.getD 0) (l2.max?.getD 0) := by
  apply Nat.le_antisymm
  · apply max?_getD_le 0 (max (l1.max?.getD 0) (l2.max?.getD 0))
    · intro x hx
      rcases List.mem_append.mp hx with hx1 | hx2
      · exact Nat.le_trans (le_max?_getD l1 0 hx1) (Nat.le_max_left _ _)
      · exact Nat.le_trans (le_max?_getD l2 0 hx2) (Nat.le_max_right _ _)
    · exact Nat.zero_le _
  · apply Nat.max_le.mpr
    constructor
    · apply max?_getD_le 0 ((l1 ++ l2).max?.getD 0) l1
      · intro x hx
        exact le_max?_getD (l1 ++ l2) 0 (by simp [hx])
      · exact Nat.zero_le _
    · apply max?_getD_le 0 ((l1 ++ l2).max?.getD 0) l2
      · intro x hx
        exact le_max?_getD (l1 ++ l2) 0 (by simp [hx])
      · exact Nat.zero_le _

/-- A nonempty cons list does not depend on the default of `getD`. -/
theorem cons_max?_getD_eq (x : Nat) (xs : List Nat) (d : Nat) :
    (x :: xs).max?.getD d = (x :: xs).max?.getD 0 := by
  cases hxs : xs.max? with
  | none => simp [List.max?_cons, hxs]
  | some m => simp [List.max?_cons, hxs]

/-- `high` can use the canonical default zero. -/
theorem high_eq_scanl_zero (d : Nat) (ts : List Tick) :
    high d ts = (ts.scanl step d).max?.getD 0 := by
  unfold high
  cases ts with
  | nil => simp
  | cons t ts =>
      simp only [List.scanl_cons]
      rw [cons_max?_getD_eq]

/-- A scan always starts with its initial debt. -/
theorem scanl_eq_cons_tail (d : Nat) (ts : List Tick) :
    ts.scanl step d = d :: (ts.scanl step d).tail := by
  cases ts with
  | nil => rfl
  | cons t ts => simp [List.scanl_cons]

/-- The final debt of a prefix appears among the scan values. -/
theorem endDebt_mem_scanl (d : Nat) (ts : List Tick) : endDebt d ts ∈ ts.scanl step d := by
  induction ts generalizing d with
  | nil => simp [endDebt]
  | cons t ts ih =>
      rw [List.scanl_cons]
      exact List.mem_cons_of_mem _ (ih (step d t))

/-- The final debt is never above the high-water mark. -/
theorem endDebt_le_high (d : Nat) (ts : List Tick) : endDebt d ts ≤ high d ts := by
  rw [high_eq_scanl_zero]
  exact le_max?_getD (ts.scanl step d) 0 (endDebt_mem_scanl d ts)

/-- High water of a concatenation: max of the two segments. -/
theorem high_append (u v : List Tick) :
    high 0 (u ++ v) = max (high 0 u) (high (endDebt 0 u) v) := by
  rw [high_eq_scanl_zero, high_eq_scanl_zero, high_eq_scanl_zero]
  rw [List.scanl_append]
  change (List.scanl step 0 u ++ (List.scanl step (endDebt 0 u) v).tail).max?.getD 0 =
    max ((List.scanl step 0 u).max?.getD 0) ((List.scanl step (endDebt 0 u) v).max?.getD 0)
  rw [max?_getD_append]
  apply Nat.le_antisymm
  · apply Nat.max_le.mpr
    constructor
    · exact Nat.le_max_left _ _
    · apply max?_getD_le 0 (max ((List.scanl step 0 u).max?.getD 0)
          ((List.scanl step (endDebt 0 u) v).max?.getD 0))
      · intro x hx
        have hfull : x ≤ (List.scanl step (endDebt 0 u) v).max?.getD 0 :=
          le_max?_getD (List.scanl step (endDebt 0 u) v) 0 (List.mem_of_mem_tail hx)
        exact Nat.le_trans hfull (Nat.le_max_right _ _)
      · exact Nat.zero_le _
  · apply Nat.max_le.mpr
    constructor
    · exact Nat.le_max_left _ _
    · apply max?_getD_le 0 (max ((List.scanl step 0 u).max?.getD 0)
          ((List.scanl step (endDebt 0 u) v).tail.max?.getD 0))
      · intro x hx
        rw [scanl_eq_cons_tail] at hx
        rcases List.mem_cons.mp hx with hxhead | hxtail
        · subst x
          have hdu : endDebt 0 u ≤ (List.scanl step 0 u).max?.getD 0 := by
            rw [← high_eq_scanl_zero]
            exact endDebt_le_high 0 u
          exact Nat.le_trans hdu (Nat.le_max_left _ _)
        · have hxt : x ≤ (List.scanl step (endDebt 0 u) v).tail.max?.getD 0 :=
            le_max?_getD _ 0 hxtail
          exact Nat.le_trans hxt (Nat.le_max_right _ _)
      · exact Nat.zero_le _

/-- Comparing two list maxima when heads and tails are bounded. -/
theorem max?_getD_le_cons {x y : Nat} {xs ys : List Nat} {d : Nat}
    (hxy : x ≤ y) (htail : xs.max?.getD 0 ≤ ys.max?.getD 0) :
    (x :: xs).max?.getD d ≤ (y :: ys).max?.getD d := by
  cases hxs : xs.max? with
  | none =>
      cases hys : ys.max? with
      | none => simpa [List.max?_cons, hxs, hys] using hxy
      | some m =>
          have hle : x ≤ max y m := Nat.le_trans hxy (Nat.le_max_left y m)
          simpa [List.max?_cons, hxs, hys] using hle
  | some a =>
      cases hys : ys.max? with
      | none =>
          have ha0 : a ≤ 0 := by simpa [hxs, hys] using htail
          have hle : max x a ≤ y := by omega
          simpa [List.max?_cons, hxs, hys] using hle
      | some b =>
          have hab : a ≤ b := by simpa [hxs, hys] using htail
          have hle : max x a ≤ max y b := (Nat.max_le).mpr
            ⟨Nat.le_trans hxy (Nat.le_max_left y b), Nat.le_trans hab (Nat.le_max_right y b)⟩
          simpa [List.max?_cons, hxs, hys] using hle

/-- High water is monotone in the initial backlog. -/
theorem high_mono (ts : List Tick) {a b : Nat} (hab : a ≤ b) : high a ts ≤ high b ts := by
  induction ts generalizing a b with
  | nil => simpa [high] using hab
  | cons t ts ih =>
      rw [high_eq_scanl_zero, high_eq_scanl_zero]
      simp only [List.scanl_cons]
      apply max?_getD_le_cons (d := 0)
      · exact hab
      · rw [← high_eq_scanl_zero, ← high_eq_scanl_zero]
        exact ih (step_mono t hab)

/-- The repeated trace after `cycles` copies. -/
def cyclesTrace (ts : List Tick) (cycles : Nat) : List Tick :=
  (List.replicate cycles ts).flatten

/-- One extra copy appends one period at the end. -/
theorem cyclesTrace_succ (ts : List Tick) (n : Nat) :
    cyclesTrace ts (n + 1) = cyclesTrace ts n ++ ts := by
  simp [cyclesTrace, List.replicate_succ', List.flatten_append]

/-- After any positive number of sustainable cycles, debt stays at or below
the debt after the first cycle. -/
theorem cycle_debt_le (ts : List Tick) (hs : sumAdd ts ≤ sumCap ts) :
    ∀ n, 0 < n → endDebt 0 (cyclesTrace ts n) ≤ endDebt 0 ts
  | 0, h => by omega
  | 1, h => by simp [cyclesTrace]
  | n + 2, h => by
      have hprev : endDebt 0 (cyclesTrace ts (n + 1)) ≤ endDebt 0 ts :=
        cycle_debt_le ts hs (n + 1) (by omega)
      change endDebt 0 (cyclesTrace ts ((n + 1) + 1)) ≤ endDebt 0 ts
      rw [cyclesTrace_succ]
      simp only [endDebt, List.foldl_append]
      change endDebt (endDebt 0 (cyclesTrace ts (n + 1))) ts ≤ endDebt 0 ts
      have hb := endDebt_le_max ts hs (endDebt 0 (cyclesTrace ts (n + 1)))
      rw [Nat.max_eq_right hprev] at hb
      exact hb

/-- The high-water mark never exceeds the one established by two cycles. -/
theorem sound : PeriodicSteadyHighWater.Goal := by
  intro ts cycles hs
  let d1 := endDebt 0 ts
  let H := high 0 (ts ++ ts)
  have hs' : sumAdd ts ≤ sumCap ts := by
    simpa [sumAdd, sumCap] using hs
  have hhigh_ts : high 0 ts ≤ H := by
    change high 0 ts ≤ high 0 (ts ++ ts)
    rw [high_append]
    exact Nat.le_max_left _ _
  induction cycles with
  | zero => simp [high]
  | succ n ih =>
      change high 0 (cyclesTrace ts (n + 1)) ≤ H
      rw [cyclesTrace_succ, high_append]
      apply Nat.max_le.mpr
      constructor
      · exact ih
      · cases n with
        | zero =>
            change high 0 ts ≤ H
            exact hhigh_ts
        | succ n =>
            have hdle : endDebt 0 (cyclesTrace ts (Nat.succ n)) ≤ d1 :=
              cycle_debt_le ts hs' (Nat.succ n) (by omega)
            have hmono := high_mono ts hdle
            have hsecond : high d1 ts ≤ H := by
              change high d1 ts ≤ high 0 (ts ++ ts)
              rw [high_append]
              exact Nat.le_max_right _ _
            exact Nat.le_trans hmono hsecond

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_PeriodicSteadyHighWater
