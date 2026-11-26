import Std
import TinylambOpt.ProposedGoals.FilterAggregateContract

/-!
Proved

- `Goal` — Staged evaluation preserves value, NULL and first error; the FILTER
  aggregate is chunk-mergeable under the row-order contract.

The reference model is defined in `TinylambOpt.ProposedGoals.FilterAggregateContract`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FilterAggregateContract

open TinylambOpt.ProposedGoals.FilterAggregateContract

private def errIdx (p : Pred) : Rows → Option Nat
  | [] => none
  | v :: vs => if (p v).isNone then some 0 else (errIdx p vs).map Nat.succ

private def addVals (rows : Rows) : Option Int :=
  List.foldl (fun s v => addVal s v) none rows

private def filteredVals (p : Pred) (rows : Rows) : Rows :=
  ((rows.map fun v => (v, p v)).filter fun x => x.2 == some TV.t).map Prod.fst

private def expected (p : Pred) (rows : Rows) : Result :=
  match errIdx p rows with
  | some i => .failed i
  | none => .ok (addVals (filteredVals p rows))

private theorem errIdx_eq_findIdx (p : Pred) (rows : Rows) :
    errIdx p rows = (rows.map fun v => (v, p v)).findIdx? (fun x => x.2 == none) := by
  induction rows with
  | nil => rfl
  | cons v vs ih =>
      rw [List.map_cons, List.findIdx?_cons]
      by_cases h : p v = none
      · simp [errIdx, h]
      · simp only [errIdx]
        rw [ih]
        simp

private theorem filteredVals_eq_filter (p : Pred) (rows : Rows) :
    filteredVals p rows = rows.filter (fun v => p v == some TV.t) := by
  induction rows with
  | nil => rfl
  | cons v vs ih =>
      unfold filteredVals
      rw [List.map_cons, List.filter_cons]
      by_cases h : p v == some TV.t
      · simp [h]
        exact ih
      · simp [h]
        exact ih

private theorem filteredVals_cons (p : Pred) (v : Option Int) (vs : Rows) :
    filteredVals p (v :: vs) =
      if p v == some TV.t then v :: filteredVals p vs else filteredVals p vs := by
  rw [filteredVals_eq_filter, filteredVals_eq_filter, List.filter_cons]

private theorem filteredVals_append (p : Pred) (l₁ l₂ : Rows) :
    filteredVals p (l₁ ++ l₂) = filteredVals p l₁ ++ filteredVals p l₂ := by
  simp [filteredVals_eq_filter, List.filter_append]

private theorem staged_eq_expected (p : Pred) (rows : Rows) :
    staged p rows = expected p rows := by
  unfold staged expected
  rw [errIdx_eq_findIdx p rows]
  rfl

private theorem addVal_none_id (v : Option Int) : addVal none v = v := by
  cases v <;> simp [addVal]

private theorem addVal_comm (acc : Option Int) (a b : Option Int) :
    addVal (addVal acc a) b = addVal (addVal acc b) a := by
  cases acc <;> cases a <;> cases b <;> simp [addVal, Int.add_comm, Int.add_left_comm]

private theorem addVal_assoc (acc : Option Int) (a b : Option Int) :
    addVal (addVal acc a) b = addVal acc (addVal a b) := by
  cases acc <;> cases a <;> cases b <;> simp [addVal, Int.add_assoc]

private theorem foldl_addVal (acc : Option Int) (l : Rows) :
    List.foldl (fun s v => addVal s v) acc l = addVal acc (addVals l) := by
  induction l generalizing acc with
  | nil => simp [addVals, addVal]
  | cons v l ih =>
      rw [List.foldl_cons]
      rw [ih (addVal acc v)]
      have hcons : addVals (v :: l) = addVal (addVals l) v := by
        unfold addVals
        rw [List.foldl_cons]
        rw [ih (addVal none v)]
        change addVal (addVal none v) (addVals l) = addVal (addVals l) v
        rw [addVal_none_id]
        simpa [addVal_none_id] using (addVal_comm none v (addVals l))
      rw [hcons]
      rw [addVal_assoc]
      congr 1
      simpa [addVal_none_id] using (addVal_comm none v (addVals l))

private theorem addVals_cons (v : Option Int) (l : Rows) :
    addVals (v :: l) = addVal (addVals l) v := by
  unfold addVals
  rw [List.foldl_cons]
  rw [foldl_addVal (addVal none v) l]
  change addVal (addVal none v) (addVals l) = addVal (addVals l) v
  rw [addVal_none_id]
  simpa [addVal_none_id] using (addVal_comm none v (addVals l))

private theorem addVals_append (l₁ l₂ : Rows) :
    addVals (l₁ ++ l₂) = addVal (addVals l₂) (addVals l₁) := by
  unfold addVals
  rw [List.foldl_append]
  rw [foldl_addVal]
  change addVal (addVals l₁) (addVals l₂) = addVal (addVals l₂) (addVals l₁)
  simpa [addVal_none_id] using (addVal_comm none (addVals l₁) (addVals l₂))

private theorem scan_eq_expected (p : Pred) : ∀ (rows : Rows) (i : Nat),
    scan p rows i =
      match errIdx p rows with
      | some j => .failed (i + j)
      | none => .ok (addVals (filteredVals p rows)) := by
  intro rows
  induction rows with
  | nil => intro i; rfl
  | cons v vs ih =>
      intro i
      by_cases hp : p v = none
      · simp [scan, errIdx, hp]
      · by_cases ht : p v = some TV.t
        · rw [scan, filteredVals_cons]
          simp [ht]
          rw [ih (i + 1)]
          cases hidx : errIdx p vs with
          | none =>
              simp [errIdx, hp, hidx, addVals_cons]
          | some j =>
              simp [errIdx, hp, hidx]
              omega
        · have hnot : ¬(p v == some TV.t) := by simp [ht]
          rw [scan, filteredVals_cons]
          simp [hnot]
          rw [ih (i + 1)]
          cases hidx : errIdx p vs with
          | none => simp [errIdx, hp, hidx]
          | some j =>
              simp [errIdx, hp, hidx]
              omega

private theorem errIdx_append (p : Pred) (l₁ l₂ : Rows) :
    errIdx p (l₁ ++ l₂) =
      match errIdx p l₁ with
      | some i => some i
      | none => (errIdx p l₂).map (fun j => l₁.length + j) := by
  induction l₁ with
  | nil => simp [errIdx, Nat.zero_add]
  | cons v l₁ ih =>
      by_cases hp : p v = none
      · simp [errIdx, hp]
      · simp only [List.cons_append]
        rw [show errIdx p (v :: (l₁ ++ l₂)) = (errIdx p (l₁ ++ l₂)).map Nat.succ by simp [errIdx, hp]]
        rw [show errIdx p (v :: l₁) = (errIdx p l₁).map Nat.succ by simp [errIdx, hp]]
        rw [ih]
        cases hidx : errIdx p l₁ with
        | none =>
            simp [Function.comp_def]
            congr 1
            funext j
            omega
        | some i => simp

theorem sound_FilterAggregateContract : TinylambOpt.ProposedGoals.FilterAggregateContract.Goal := by
  intro p rows
  constructor
  · rw [staged_eq_expected, scan_eq_expected p rows 0]
    simp [expected]
  · intro pre suf h
    rw [← h]
    unfold merged
    rw [scan_eq_expected p pre 0, scan_eq_expected p suf 0, scan_eq_expected p (pre ++ suf) 0]
    generalize hip : errIdx p pre = ip
    generalize his : errIdx p suf = is
    cases ip with
    | none =>
        cases is with
        | none =>
            rw [errIdx_append, hip, his]
            simp only [Option.map_none]
            rw [filteredVals_append, addVals_append]
        | some j =>
            rw [errIdx_append, hip, his]
            simp [Nat.add_comm]
    | some i =>
        rw [errIdx_append, hip, his]

end TinylambOpt.ResearchProofs.Atlas.Goal_FilterAggregateContract
