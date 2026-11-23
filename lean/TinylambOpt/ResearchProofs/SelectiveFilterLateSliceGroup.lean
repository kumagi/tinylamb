import Std
import TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup

namespace TinylambOpt.ResearchProofs.SelectiveFilterLateSliceGroup

open TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup

private theorem foldl_filter_eq {α β : Type} (f : α → β → α) (a : α) (p : β → Bool)
    (l : List β) :
    (l.filter p).foldl f a = l.foldl (fun acc x => if p x then f acc x else acc) a := by
  induction l generalizing a with
  | nil => rfl
  | cons x xs ih =>
      cases h : p x <;> simp [List.filter, h, ih]

private def refStep (c : Bool) (m : List (GroupKey × Nat)) (r : RawRow) :
    List (GroupKey × Nat) :=
  let okStr := if c then r.strVal != "" else true
  if r.intColsPass && okStr then
    let src := if r.useRef then r.refVal else ""
    addGroupCnt m (r.strVal, src)
  else m

private def optBody (c : Bool) (st : TraceOut) (r : RawRow) : TraceOut :=
  let passStr := if c then r.strNonEmpty else true
  if !passStr then st
  else
    let src := if r.useRef then r.refVal else ""
    { groups := addGroupCnt st.groups (r.strVal, src)
      intMatched := st.intMatched
      strSliceDecodes := st.strSliceDecodes + 1
      refSliceDecodes := st.refSliceDecodes + (if r.useRef then 1 else 0) }

private def optStep (c : Bool) (st : TraceOut) (r : RawRow) : TraceOut :=
  if r.intColsPass then optBody c st r else st

private theorem refFilterGroup_eq (rows : List RawRow) (c : Bool) :
    refFilterGroup rows c = rows.foldl (refStep c) [] := by
  rfl

private theorem optLateSliceGroup_eq (rows : List RawRow) (c : Bool) :
    optLateSliceGroup rows c =
      (rows.filter RawRow.intColsPass).foldl (optBody c)
        { groups := [], intMatched := (rows.filter RawRow.intColsPass).length
          strSliceDecodes := 0, refSliceDecodes := 0 } := by
  rfl

private def incCount (c : Bool) (r : RawRow) : Nat :=
  if r.intColsPass && (if c then r.strNonEmpty else true) then 1 else 0

private def incRefCount (c : Bool) (r : RawRow) : Nat :=
  if r.intColsPass && (if c then r.strNonEmpty else true) && r.useRef then 1 else 0

private def counted (c : Bool) : List RawRow → Nat
  | [] => 0
  | r :: rs => incCount c r + counted c rs

private def refCounted (c : Bool) : List RawRow → Nat
  | [] => 0
  | r :: rs => incRefCount c r + refCounted c rs

private theorem counted_cons (c : Bool) (r : RawRow) (rs : List RawRow) :
    counted c (r :: rs) = incCount c r + counted c rs := by
  rfl

private theorem refCounted_cons (c : Bool) (r : RawRow) (rs : List RawRow) :
    refCounted c (r :: rs) = incRefCount c r + refCounted c rs := by
  rfl

private theorem optStep_groups (c : Bool) (st : TraceOut) (r : RawRow) (hv : ValidRowLengths r) :
    (optStep c st r).groups = refStep c st.groups r := by
  unfold ValidRowLengths at hv
  unfold optStep optBody refStep
  cases hi : r.intColsPass
  · simp
  · simp
    cases c
    · simp
    · cases hne : (r.strVal != "")
      · have hsn : r.strNonEmpty = false := by rw [hv, hne]
        simp at hne
        simp [hne, hsn]
      · have hsn : r.strNonEmpty = true := by rw [hv, hne]
        simp at hne
        simp [hne, hsn]

private theorem groups_fold_eq (c : Bool) (l : List RawRow) (st : TraceOut)
    (hv : ∀ r ∈ l, ValidRowLengths r) :
    (l.foldl (optStep c) st).groups = l.foldl (refStep c) st.groups := by
  induction l generalizing st with
  | nil => rfl
  | cons r rs ih =>
      simp [List.foldl]
      rw [ih (optStep c st r) (by intro x hx; exact hv x (by simp [hx]))]
      rw [optStep_groups c st r (hv r (by simp))]

private theorem optStep_stats (c : Bool) (st : TraceOut) (r : RawRow) :
    let st' := optStep c st r
    st'.intMatched = st.intMatched ∧
    st'.strSliceDecodes = st.strSliceDecodes + incCount c r ∧
    st'.refSliceDecodes = st.refSliceDecodes + incRefCount c r := by
  dsimp
  cases hi : r.intColsPass <;>
    cases hp : (if c then r.strNonEmpty else true) <;>
    cases hu : r.useRef <;>
    simp [optStep, optBody, incCount, incRefCount, hi, hp, hu]

private theorem stats_fold (c : Bool) (l : List RawRow) (st : TraceOut) :
    let st' := l.foldl (optStep c) st
    st'.intMatched = st.intMatched ∧
    st'.strSliceDecodes = st.strSliceDecodes + counted c l ∧
    st'.refSliceDecodes = st.refSliceDecodes + refCounted c l := by
  induction l generalizing st with
  | nil => simp [counted, refCounted]
  | cons r rs ih =>
      dsimp
      have hs := optStep_stats c st r
      dsimp at hs
      have hih := ih (optStep c st r)
      dsimp at hih
      rcases hs with ⟨hsI, hsS, hsR⟩
      rcases hih with ⟨hihI, hihS, hihR⟩
      constructor
      · exact hihI.trans hsI
      constructor
      · rw [hihS, hsS, counted_cons]
        omega
      · rw [hihR, hsR, refCounted_cons]
        omega

private theorem counted_le_int (c : Bool) (l : List RawRow) :
    counted c l ≤ (l.filter RawRow.intColsPass).length := by
  induction l with
  | nil => simp [counted]
  | cons r rs ih =>
      cases c <;> cases hi : r.intColsPass <;> cases hs : r.strNonEmpty <;>
        simp [counted, incCount, hi, hs] <;> omega

private theorem refCounted_le_counted (c : Bool) (l : List RawRow) :
    refCounted c l ≤ counted c l := by
  induction l with
  | nil => simp [counted, refCounted]
  | cons r rs ih =>
      cases c <;> cases hi : r.intColsPass <;> cases hs : r.strNonEmpty <;>
        cases hu : r.useRef <;>
        simp [counted, refCounted, incCount, incRefCount, hi, hs, hu] <;> omega

private theorem refCounted_eq_zero (c : Bool) (l : List RawRow)
    (hno : ∀ r ∈ l, r.intColsPass = true → r.useRef = false) :
    refCounted c l = 0 := by
  induction l with
  | nil => simp [refCounted]
  | cons r rs ih =>
      have hrno : ∀ x ∈ rs, x.intColsPass = true → x.useRef = false := by
        intro x hx hxi
        exact hno x (by simp [hx]) hxi
      have ihrs := ih hrno
      cases hi : r.intColsPass
      · simp [refCounted, incRefCount, hi, ihrs]
      · have huse : r.useRef = false := hno r (by simp) hi
        cases c <;> cases hs : r.strNonEmpty <;>
          simp [refCounted, incRefCount, hi, hs, huse, ihrs]

theorem sound : TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup.Goal := by
  intro rows c hv
  dsimp only
  let sel := rows.filter RawRow.intColsPass
  let init : TraceOut :=
    { groups := [], intMatched := sel.length, strSliceDecodes := 0, refSliceDecodes := 0 }
  have hfold : optLateSliceGroup rows c = rows.foldl (optStep c) init := by
    rw [optLateSliceGroup_eq]
    dsimp [sel, init]
    rw [foldl_filter_eq (optBody c) _ RawRow.intColsPass rows]
    rfl
  have hstats := stats_fold c rows init
  dsimp at hstats
  rcases hstats with ⟨hI, hS, hR⟩
  have hi0 : init.intMatched = (rows.filter RawRow.intColsPass).length := by rfl
  have hs0 : init.strSliceDecodes = 0 := by rfl
  have hr0 : init.refSliceDecodes = 0 := by rfl
  rw [hi0] at hI
  rw [hs0] at hS
  rw [hr0] at hR
  rw [hfold]
  constructor
  · have hg := groups_fold_eq c rows init hv
    change (rows.foldl (optStep c) init).groups = rows.foldl (refStep c) []
    simpa [init] using hg
  · constructor
    · have hle := counted_le_int c rows
      omega
    · constructor
      · have hle := refCounted_le_counted c rows
        omega
      · intro hno
        have hz := refCounted_eq_zero c rows hno
        omega

end TinylambOpt.ResearchProofs.SelectiveFilterLateSliceGroup
