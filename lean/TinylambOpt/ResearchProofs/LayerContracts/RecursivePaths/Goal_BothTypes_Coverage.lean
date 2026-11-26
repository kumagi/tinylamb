import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved (8 claims, including the following)

- `BothTypes.Coverage` — `∀ ls, (advance model 0 ls).any model.accept = (ls.contains 1
  && ls.contains 2)`
- `BothTypes.RepeatLabel` — `∀ q x, advance model q [x,x] = advance model q [x]`
- `LastBlue.LastTag` — `∀ ls, (advance model 0 ls).any model.accept = (ls.getLast? ==
  some 2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_Coverage

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

-- ============ OrderedClass ============
theorem oc_desc : OrderedClass.DescendingPair.Goal := by
  intro a b h
  simp [advance, OrderedClass.model]
  by_cases ha : a < 3 <;> by_cases hb : b < 3 <;> simp [ha, hb] <;> omega

def ocF (q x : Nat) : Option Nat := if decide (q ≤ x) && decide (x < 3) then some x else none

theorem ocF_ret : ∀ q x v, ocF q x = some v → v = x := by
  intro q x v h
  unfold ocF at h
  split at h
  · cases h
    rfl
  · contradiction

theorem foldlKeep {f : Nat → Nat → Option Nat} (hf : ∀ q x v, f q x = some v → v = x) :
    ∀ (ls : List Nat) (x q v : Nat), List.foldlM f q (ls ++ [x]) = some v → v = x := by
  intro ls
  induction ls with
  | nil =>
      intro x q v h
      simp at h
      exact hf q x v h
  | cons y ys ih =>
      intro x q v h
      rw [List.cons_append] at h
      rw [List.foldlM_cons] at h
      cases hfy : f q y with
      | none => simp [hfy] at h
      | some w =>
          simp [hfy] at h
          have h' : List.foldlM f w (ys ++ [x]) = some v := by
            simpa [List.foldlM_append] using h
          exact ih x w v h'

theorem oc_keep : OrderedClass.KeepLast.Goal := by
  intro ls x q h
  apply foldlKeep (f := ocF) ocF_ret ls x 0
  change List.foldlM ocF 0 (ls ++ [x]) = some q at h
  exact h

-- ============ LastBlue ============
def lbF (q x : Nat) : Option Nat := some (if x == 2 then 1 else 0)

def lastState (q : Nat) : List Nat → Nat
  | [] => q
  | [x] => if x == 2 then 1 else 0
  | _ :: y :: ys => lastState 0 (y :: ys)

theorem lastState_tail : ∀ (a q q' : Nat) (ls : List Nat),
    lastState q (a :: ls) = lastState q' (a :: ls) := by
  intro a q q' ls
  cases ls with
  | nil => simp [lastState]
  | cons b bs => simp [lastState]

theorem lbFold : ∀ (q : Nat) (ls : List Nat), List.foldlM lbF q ls = some (lastState q ls) := by
  intro q ls
  induction ls generalizing q with
  | nil => simp [lastState]
  | cons l ls ih =>
      rw [List.foldlM_cons]
      dsimp [lbF]
      rw [ih]
      cases ls with
      | nil => simp [lastState]
      | cons l' ls' =>
          rw [lastState_tail l' (if l == 2 then 1 else 0) 0 ls']
          simp [lastState]

theorem lastState_spec0 : ∀ (ls : List Nat), (lastState 0 ls == 1) = (ls.getLast? == some 2) := by
  intro ls
  induction ls with
  | nil => simp [lastState]
  | cons l ls ih =>
      cases ls with
      | nil =>
          by_cases h : l == 2 <;> simp [lastState, h]
      | cons l' ls' =>
          simp [lastState]
          exact ih

theorem lb_last : LastBlue.LastTag.Goal := by
  intro ls
  change (List.foldlM lbF 0 ls).any LastBlue.model.accept = (ls.getLast? == some 2)
  rw [lbFold]
  exact lastState_spec0 ls

theorem lb_reset : LastBlue.Reset.Goal := by
  intro q ls hne
  cases ls with
  | nil => contradiction
  | cons l ls' => simp [advance, LastBlue.model]

-- ============ SingleRed ============
def srF (q x : Nat) : Option Nat := if x != 1 then some q else if q == 0 then some 1 else none

theorem srCountZero : ∀ (ls : List Nat), (ls.count 1 == 0) = !(decide (1 ∈ ls)) := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      by_cases hl : l = 1
      · simp [List.count, hl, ih]
      · have hl' : ¬ 1 = l := by omega
        simpa [List.count, hl, hl'] using ih

theorem srF1 : ∀ (ls : List Nat), List.foldlM srF 1 ls = if 1 ∈ ls then none else some 1 := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      rw [List.foldlM_cons]
      by_cases hl : l = 1
      · subst l
        simp [srF]
      · have hl' : ¬ 1 = l := by omega
        simp [srF, hl, hl', ih]

theorem srF0 : ∀ (ls : List Nat), List.foldlM srF 0 ls =
    if ls.count 1 == 0 then some 0 else if ls.count 1 == 1 then some 1 else none := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      rw [List.foldlM_cons]
      by_cases hl : l = 1
      · subst l
        rw [srF]
        simp [List.count]
        rw [srF1]
        by_cases hc : ls.count 1 == 0
        · have hnmem : ¬ 1 ∈ ls := by
            have hz := srCountZero ls
            simp [hc] at hz
            simpa using hz
          simp [hc, hnmem]
        · have hmem : 1 ∈ ls := by
            have hz := srCountZero ls
            simp [hc] at hz
            simpa using hz
          by_cases h1 : ls.count 1 == 1
          · simp [hc, hmem, h1]
          · have hc' : ls.count 1 ≠ 0 := by
              intro hz
              apply hc
              simp [hz]
            have hpos : 0 < ls.count 1 := Nat.pos_of_ne_zero hc'
            have hne : ls.count 1 + 1 ≠ 1 := by omega
            simp [hc, hne, hmem]
      · have hl' : ¬ 1 = l := by omega
        simp [srF, hl, hl', ih]

theorem srF0' : ∀ ls : List Nat, List.foldlM srF 0 ls =
    if ls.count 1 = 0 then some 0 else if ls.count 1 = 1 then some 1 else none := by
  intro ls
  simpa using srF0 ls

theorem sr_count : SingleRed.Count.Goal := by
  intro ls
  change (List.foldlM srF 0 ls).any SingleRed.model.accept = (ls.count 1 == 1)
  rw [srF0']
  by_cases h0 : ls.count 1 = 0
  · simp [h0, SingleRed.model]
  · by_cases h1 : ls.count 1 = 1
    · simp [h0, h1, SingleRed.model]
    · simp [h0, h1, SingleRed.model]

theorem sr_prune : SingleRed.PruneSecond.Goal := by
  intro ls h
  change List.foldlM srF 0 ls = none
  rw [srF0']
  have h0 : ls.count 1 ≠ 0 := by omega
  have h1 : ls.count 1 ≠ 1 := by omega
  simp [h0, h1]

-- ============ BothTypes ============
def btMask (x : Nat) : Nat := if x == 1 then 1 else if x == 2 then 2 else 0
def btF (q x : Nat) : Option Nat := some (q ||| btMask x)

theorem btFold : ∀ (q : Nat) (ls : List Nat),
    List.foldlM btF q ls = some (q ||| ls.foldr (fun x acc => btMask x ||| acc) 0) := by
  intro q ls
  induction ls generalizing q with
  | nil => simp
  | cons l ls ih =>
      rw [List.foldlM_cons]
      dsimp [btF]
      rw [ih (q ||| btMask l)]
      simp [Nat.or_assoc]

theorem btMaskFold : ∀ (ls : List Nat),
    ls.foldr (fun x acc => btMask x ||| acc) 0 =
      (if ls.contains 1 then 1 else 0) ||| (if ls.contains 2 then 2 else 0) := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      simp only [List.foldr]
      rw [ih]
      by_cases hl1 : l = 1
      · subst l
        by_cases c1 : 1 ∈ ls <;> by_cases c2 : 2 ∈ ls <;>
        simp [btMask, c1, c2] <;> native_decide
      · by_cases hl2 : l = 2
        · subst l
          by_cases c1 : 1 ∈ ls <;> by_cases c2 : 2 ∈ ls <;>
          simp [btMask, c1, c2] <;> native_decide
        · have h1l : ¬ 1 = l := by omega
          have h2l : ¬ 2 = l := by omega
          by_cases c1 : 1 ∈ ls <;> by_cases c2 : 2 ∈ ls <;>
          simp [btMask, hl1, hl2, h1l, h2l, c1, c2] <;> native_decide

theorem sound : BothTypes.Coverage.Goal := by
  intro ls
  change (List.foldlM btF 0 ls).any BothTypes.model.accept = (ls.contains 1 && ls.contains 2)
  rw [btFold]
  rw [btMaskFold]
  by_cases c1 : 1 ∈ ls <;> by_cases c2 : 2 ∈ ls <;> simp [c1, c2, BothTypes.model]

theorem bt_repeat : BothTypes.RepeatLabel.Goal := by
  intro q x
  simp [advance, BothTypes.model, btMask, Nat.or_assoc]

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_Coverage
