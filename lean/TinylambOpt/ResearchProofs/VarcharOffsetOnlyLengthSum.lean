import TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum

/-!
Proved

- `Goal` — Offset-only `BYTE_LENGTH` & non-empty count on VarChar blocks (ClickBench
  Q28): when a block satisfies `ValidVarcharBlock` (monotone end-offsets with
  zero-length NULL spans), `∑_{URL≠""} BYTE_LENGTH(URL)` equals `ends.getLast!` in
  `O(1)` time and `O(1)` byte reads, and `COUNT(*)` reads only `(nulls, ends)`
  without touching string payload bytes.

The reference model is defined in
`TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum`.
-/

namespace TinylambOpt.ResearchProofs.VarcharOffsetOnlyLengthSum

open TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum

/-! ### String facts -/

private theorem string_length_zero_iff (s : String) : s.length = 0 ↔ s = "" := by
  constructor
  · intro h
    apply String.ext
    have hl : s.toList = [] := List.length_eq_zero_iff.mp h
    simpa using hl
  · intro h
    subst h
    rfl

private theorem string_beq_false_eq (s : String) (h : (s != "") = false) : s = "" := by
  simpa using h

private theorem string_empty_length_zero (s : String) (h : (s != "") = false) : s.length = 0 := by
  exact (string_length_zero_iff s).mpr (string_beq_false_eq s h)

private theorem string_ne_empty_length_pos (s : String) (h : (s != "") = true) : 0 < s.length := by
  have hs : s ≠ "" := by
    intro heq
    subst heq
    simp at h
  have hlen : s.length ≠ 0 := by
    intro hz
    exact hs ((string_length_zero_iff s).mp hz)
  omega

private theorem bool_ne_false (b : Bool) (h : ¬ b = true) : b = false := by
  cases b <;> simp_all

/-! ### Reference-list head lemmas -/

private theorem refLen_nil : refNonEmptyLenSum [] = 0 := rfl

private theorem refLen_cons_none (rs : List (Option String)) :
    refNonEmptyLenSum (none :: rs) = refNonEmptyLenSum rs := rfl

private theorem refLen_cons_some (s : String) (rs : List (Option String)) :
    refNonEmptyLenSum (some s :: rs) = (if s != "" then s.length else 0) + refNonEmptyLenSum rs := by
  by_cases h : (s != "") = true
  · have hs : s ≠ "" := by
      intro heq
      subst heq
      simp at h
    simp [refNonEmptyLenSum, h, hs]
  · have hf : (s != "") = false := bool_ne_false _ h
    have hs : s = "" := string_beq_false_eq s hf
    simp [refNonEmptyLenSum, hs]

private theorem refCount_nil : refNonEmptyCount [] = 0 := rfl

private theorem refCount_cons_none (rs : List (Option String)) :
    refNonEmptyCount (none :: rs) = refNonEmptyCount rs := rfl

private theorem refCount_cons_some (s : String) (rs : List (Option String)) :
    refNonEmptyCount (some s :: rs) = (if s != "" then 1 else 0) + refNonEmptyCount rs := by
  by_cases h : (s != "") = true
  · have hs : s ≠ "" := by
      intro heq
      subst heq
      simp at h
    simp [refNonEmptyCount, h, Nat.add_comm]
  · have hf : (s != "") = false := bool_ne_false _ h
    have hs : s = "" := string_beq_false_eq s hf
    simp [refNonEmptyCount, hs]

/-! ### Validity relative to a base offset -/

private def pEnd (p : Nat) (es : List Nat) (i : Nat) : Nat :=
  if i == 0 then p else es.getD (i - 1) 0

private def rowSpanProp (r : Option String) (e pe : Nat) : Prop :=
  match r with
  | none => e = pe
  | some s => e - pe = s.length

private def ValidFrom (p : Nat) (rows : List (Option String)) (ends : List Nat) : Prop :=
  rows.length = ends.length ∧
  ∀ i : Nat, i < ends.length →
    pEnd p ends i ≤ ends.getD i 0 ∧
    rowSpanProp (rows.getD i none) (ends.getD i 0) (pEnd p ends i)

private theorem validFrom_tail (p e : Nat) (r : Option String) (rs : List (Option String))
    (es : List Nat) (hv : ValidFrom p (r :: rs) (e :: es)) : ValidFrom e rs es := by
  rcases hv with ⟨hlen, hvalid⟩
  have hlen' : rs.length = es.length := Nat.succ.inj hlen
  refine ⟨hlen', ?_⟩
  intro i hi
  have hi' : i + 1 < (e :: es).length := by
    simpa using Nat.succ_lt_succ hi
  have hvi := hvalid (i + 1) hi'
  cases i with
  | zero =>
      simpa [pEnd] using hvi
  | succ j =>
      simpa [pEnd] using hvi

private theorem validFrom_head (p e : Nat) (r : Option String) (rs : List (Option String))
    (es : List Nat) (hv : ValidFrom p (r :: rs) (e :: es)) :
    p ≤ e ∧ rowSpanProp r e p := by
  have hvi := hv.2 0 (by simp)
  simpa [pEnd] using hvi

/-! ### Telescoping the offset spans -/

private def sumSpansFrom (p : Nat) (es : List Nat) : Nat :=
  match es with
  | [] => 0
  | e :: es' => (e - p) + sumSpansFrom e es'

private theorem sumSpansFrom_add_last (p : Nat) (es : List Nat)
    (hmono : ∀ i : Nat, i < es.length → pEnd p es i ≤ es.getD i 0) :
    sumSpansFrom p es + p = if es.isEmpty then p else es.getLast! := by
  induction es generalizing p with
  | nil => simp [sumSpansFrom]
  | cons e es' ih =>
      have h0 : p ≤ e := by
        have h := hmono 0 (by simp)
        simpa [pEnd] using h
      have htail : ∀ i : Nat, i < es'.length → pEnd e es' i ≤ es'.getD i 0 := by
        intro i hi
        have hi' : i + 1 < (e :: es').length := by simpa using Nat.succ_lt_succ hi
        have h := hmono (i + 1) hi'
        cases i with
        | zero => simpa [pEnd] using h
        | succ j => simpa [pEnd] using h
      have ih' := ih e htail
      have hstep : sumSpansFrom p (e :: es') + p = sumSpansFrom e es' + e := by
        simp only [sumSpansFrom]
        omega
      rw [hstep, ih']
      cases es' with
      | nil => simp
      | cons a as => simp

private theorem sumSpansFrom_eq_last (p : Nat) (es : List Nat)
    (hmono : ∀ i : Nat, i < es.length → pEnd p es i ≤ es.getD i 0) :
    sumSpansFrom p es = (if es.isEmpty then p else es.getLast!) - p := by
  have h := sumSpansFrom_add_last p es hmono
  by_cases hempty : es.isEmpty
  · simp [hempty] at h ⊢
    omega
  · simp [hempty] at h ⊢
    omega

/-! ### Reference length sum equals the telescoped spans -/

private theorem refLen_eq_sumSpans (p : Nat) (rows : List (Option String)) (es : List Nat)
    (hv : ValidFrom p rows es) : refNonEmptyLenSum rows = sumSpansFrom p es := by
  induction rows generalizing p es with
  | nil =>
      have hlen0 : es.length = 0 := by
        simpa using hv.1.symm
      have hnil : es = [] := List.length_eq_zero_iff.mp hlen0
      subst es
      simp [refLen_nil, sumSpansFrom]
  | cons r rs ih =>
      cases es with
      | nil =>
          have hbad := hv.1
          simp at hbad
      | cons e es' =>
          have hvtail : ValidFrom e rs es' := validFrom_tail p e r rs es' hv
          have ih' := ih e es' hvtail
          have hh := validFrom_head p e r rs es' hv
          cases r with
          | none =>
              have hep : e = p := hh.2
              rw [refLen_cons_none, ih']
              simp [sumSpansFrom, hep]
          | some s =>
              have hspan : e - p = s.length := hh.2
              by_cases hne : (s != "") = true
              · have hhead : s.length = e - p := hspan.symm
                rw [refLen_cons_some, ih']
                simp [sumSpansFrom, hne, hhead]
              · have hnefalse : (s != "") = false := bool_ne_false _ hne
                have hsl0 : s.length = 0 := string_empty_length_zero s hnefalse
                have hep : e - p = 0 := by omega
                rw [refLen_cons_some, ih']
                simp [sumSpansFrom, hnefalse, hep]

/-! ### Reference non-empty count equals the span indicators -/

private def countSpansFrom (p : Nat) (nulls : List Bool) (es : List Nat) : Nat :=
  match nulls, es with
  | n :: ns, e :: es' => (if !n && p < e then 1 else 0) + countSpansFrom e ns es'
  | _, _ => 0

private theorem refCount_eq_countSpans (p : Nat) (rows : List (Option String)) (es : List Nat)
    (hv : ValidFrom p rows es) :
    refNonEmptyCount rows = countSpansFrom p (rows.map Option.isNone) es := by
  induction rows generalizing p es with
  | nil =>
      have hlen0 : es.length = 0 := by
        simpa using hv.1.symm
      have hnil : es = [] := List.length_eq_zero_iff.mp hlen0
      subst es
      simp [refCount_nil, countSpansFrom]
  | cons r rs ih =>
      cases es with
      | nil =>
          have hbad := hv.1
          simp at hbad
      | cons e es' =>
          have hvtail : ValidFrom e rs es' := validFrom_tail p e r rs es' hv
          have ih' := ih e es' hvtail
          have hh := validFrom_head p e r rs es' hv
          cases r with
          | none =>
              have hep : e = p := hh.2
              rw [refCount_cons_none, ih']
              simp [countSpansFrom, hep]
          | some s =>
              have hspan : e - p = s.length := hh.2
              by_cases hne : (s != "") = true
              · have hpos : 0 < s.length := string_ne_empty_length_pos s hne
                have hlt : p < e := by omega
                rw [refCount_cons_some, ih']
                simp [countSpansFrom, hne, hlt]
              · have hnefalse : (s != "") = false := bool_ne_false _ hne
                have hsl0 : s.length = 0 := string_empty_length_zero s hnefalse
                have hnlt : ¬ p < e := by omega
                rw [refCount_cons_some, ih']
                simp [countSpansFrom, hnefalse, hnlt]

/-! ### The list-range fold used by the optimised count -/

private def countStep (p : Nat) (nulls : List Bool) (es : List Nat) (acc i : Nat) : Nat :=
  if !nulls.getD i true && pEnd p es i < es.getD i 0 then acc + 1 else acc

private def countFoldAcc (p : Nat) (nulls : List Bool) (es : List Nat) (acc : Nat) : Nat :=
  (List.range es.length).foldl (countStep p nulls es) acc

private theorem countFoldAcc_eq (p : Nat) (nulls : List Bool) (es : List Nat) (n acc : Nat) :
    (List.range n).foldl (countStep p nulls es) acc =
      acc + (List.range n).foldl (countStep p nulls es) 0 := by
  induction n generalizing acc with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.foldl_append, List.foldl_append]
      rw [ih acc, ih 0]
      by_cases hP : (nulls[n]?.getD true = false ∧ pEnd p es n < es[n]?.getD 0)
      · simp [countStep, hP, Nat.add_assoc]
      · simp [countStep, hP]

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun j => j + 1) := by
  calc
    List.range (n + 1) = List.range' 0 (n + 1) := by rw [List.range_eq_range']
    _ = 0 :: List.range' 1 n := by rw [List.range'_succ]
    _ = 0 :: (List.range n).map (fun x => 1 + x) := by rw [List.range'_eq_map_range]
    _ = 0 :: (List.range n).map (fun j => j + 1) := by
      congr 1
      apply List.map_congr_left
      intro x hx
      omega

private theorem countFold_eq_countSpans (p : Nat) (nulls : List Bool) (es : List Nat)
    (hlen : nulls.length = es.length) : countFoldAcc p nulls es 0 = countSpansFrom p nulls es := by
  induction nulls generalizing p es with
  | nil =>
      have hniles : es = [] := by
        apply List.length_eq_zero_iff.mp
        simpa using hlen.symm
      subst es
      simp [countFoldAcc, countSpansFrom]
  | cons n ns ih =>
      cases es with
      | nil =>
          have hbad := hlen
          simp at hbad
      | cons e es' =>
          have hlen' : ns.length = es'.length := Nat.succ.inj hlen
          unfold countFoldAcc
          rw [show (e :: es').length = es'.length + 1 by simp, range_succ_cons]
          simp only [List.foldl_cons, List.foldl_map]
          have hstep : ∀ (acc j : Nat),
              countStep p (n :: ns) (e :: es') acc (j + 1) = countStep e ns es' acc j := by
            intro acc j
            cases j <;> simp [countStep, pEnd]
          simp only [hstep]
          have hhead : countStep p (n :: ns) (e :: es') 0 0 =
              (if !n && p < e then 1 else 0) := by
            simp [countStep, pEnd]
          rw [hhead]
          rw [countFoldAcc_eq]
          change (if !n && p < e then 1 else 0) + countFoldAcc e ns es' 0 =
            countSpansFrom p (n :: ns) (e :: es')
          rw [ih e es' hlen']
          simp [countSpansFrom]

/-! ### Soundness -/

theorem sound : TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum.Goal := by
  constructor
  · intro b hv
    rcases hv with ⟨hlen, hnulls, hvalid⟩
    have hvFrom : ValidFrom 0 b.rows b.ends := by
      refine ⟨hlen, ?_⟩
      intro i hi
      have hi' : i < b.rows.length := by simpa [hlen] using hi
      have hvi := hvalid i hi'
      constructor
      · simpa [pEnd, prevEndAt] using hvi.1
      · cases hri : b.rows[i]?.getD none with
        | none =>
            have h2 : b.ends[i]?.getD 0 = prevEndAt b.ends i := by
              simpa [hri] using hvi.2
            simpa [pEnd, rowSpanProp, prevEndAt, hri] using h2
        | some s =>
            have h2 : b.ends[i]?.getD 0 - prevEndAt b.ends i = s.length := by
              simpa [hri] using hvi.2
            simpa [pEnd, rowSpanProp, prevEndAt, hri] using h2
    have hmono : ∀ i : Nat, i < b.ends.length → pEnd 0 b.ends i ≤ b.ends.getD i 0 := by
      intro i hi
      exact (hvFrom.2 i hi).1
    have hLenRef : refNonEmptyLenSum b.rows = sumSpansFrom 0 b.ends :=
      refLen_eq_sumSpans 0 b.rows b.ends hvFrom
    have hLenSpans : sumSpansFrom 0 b.ends =
        (if b.ends.isEmpty then 0 else b.ends.getLast!) - 0 :=
      sumSpansFrom_eq_last 0 b.ends hmono
    have hLen : optO1EndsLastLenSum b = refNonEmptyLenSum b.rows := by
      rw [hLenRef]
      simpa [optO1EndsLastLenSum] using hLenSpans.symm
    have hCountRef : refNonEmptyCount b.rows = countSpansFrom 0 (b.rows.map Option.isNone) b.ends :=
      refCount_eq_countSpans 0 b.rows b.ends hvFrom
    have hnullLen : b.nulls.length = b.ends.length := by
      rw [hnulls, List.length_map, hlen]
    have hCountFold : countFoldAcc 0 b.nulls b.ends 0 = countSpansFrom 0 b.nulls b.ends :=
      countFold_eq_countSpans 0 b.nulls b.ends hnullLen
    have hCount : optOffsetOnlyNonEmptyCount b = refNonEmptyCount b.rows := by
      change countFoldAcc 0 b.nulls b.ends 0 = refNonEmptyCount b.rows
      rw [hCountFold, hCountRef, hnulls]
    exact ⟨hLen, hCount⟩
  · decide

end TinylambOpt.ResearchProofs.VarcharOffsetOnlyLengthSum

-- C++ references to this model or its proof.
-- executor/detail/columnar_tlcs_bindings.cpp
