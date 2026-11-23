import TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup

namespace TinylambOpt.ResearchProofs.SmallDimDenseArrayDirectLookup

open TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup

private theorem beq_symm_int (a b : Int) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    simp
  · have h' : b ≠ a := by intro hb; exact h hb.symm
    rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr h']

private def optFactVal (d1 d2 : List DimRow) (min1 : Int) (span1 : Nat)
    (min2 : Int) (span2 : Nat) (f : FactRow) (a1 a2 : Int) : Option (Option Int) :=
  match lookupDense (buildDenseDimTable d1 min1 span1) min1 span1 f.fk1,
        lookupDense (buildDenseDimTable d2 min2 span2) min2 span2 f.fk2 with
  | some attr1, some attr2 =>
      if attr1 == a1 && attr2 == a2 then some f.val else none
  | _, _ => none

private def refFactRows (d1 d2 : List DimRow) (f : FactRow) (a1 a2 : Int) : List (Option Int) :=
  (d1.filter DimRow.pred).flatMap fun r1 =>
    match f.fk1, r1.k with
    | some fk1, some k1 =>
        if fk1 == k1 then
          (d2.filter DimRow.pred).filterMap fun r2 =>
            match f.fk2, r2.k with
            | some fk2, some k2 =>
                if fk2 == k2 && r1.attr == a1 && r2.attr == a2 then some f.val else none
            | _, _ => none
        else []
    | _, _ => []

private theorem filterMap_eq_flatMap_singleton {α β : Type} (f : α → Option β) (l : List α) :
    l.filterMap f = l.flatMap (fun x => match f x with | some y => [y] | none => []) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases hfa : f a with
      | none => simp [List.flatMap_cons, hfa, ih]
      | some b => simp [List.flatMap_cons, hfa, ih]

private theorem flatMap_if_eq_filter_flatMap {α β : Type} (l : List α) (p : α → Bool) (g : α → List β) :
    l.flatMap (fun x => if p x then g x else []) = (l.filter p).flatMap g := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases h : p a = true
      · simp [List.flatMap_cons, h, ih]
      · simp [List.flatMap_cons, h, ih]

private theorem find_eq_none_of_filter_nil {α : Type} (l : List α) (p : α → Bool) (hf : l.filter p = []) :
    l.find? p = none := by
  apply List.find?_eq_none.mpr
  intro x hx hp
  have hmem : x ∈ l.filter p := List.mem_filter.mpr ⟨hx, hp⟩
  simp [hf] at hmem

private theorem find_eq_some_of_filter_singleton {α : Type} (l : List α) (p : α → Bool) (a : α)
    (hf : l.filter p = [a]) : l.find? p = some a := by
  induction l with
  | nil => simp at hf
  | cons b bs ih =>
      by_cases hp : p b = true
      · have hb : b = a := by
          have hmem : b ∈ (b :: bs).filter p := by
            rw [List.filter_cons]
            simp [hp]
          simpa [hf] using hmem
        subst a
        simp [hp]
      · have hf' : bs.filter p = [a] := by
          simpa [List.filter_cons, hp] using hf
        simp [hp, ih hf']

private theorem flatMap_unique_find {α β : Type} (l : List α) (p : α → Bool) (g : α → List β)
    (huniq : ∀ _x : α, (l.filter p).length ≤ 1) :
    l.flatMap (fun x => if p x then g x else []) =
      match l.find? p with | some x => g x | none => [] := by
  rw [flatMap_if_eq_filter_flatMap]
  generalize hf : l.filter p = fl
  cases fl with
  | nil =>
      have hfind : l.find? p = none := find_eq_none_of_filter_nil l p hf
      simp [hfind]
  | cons a rest =>
      have hlen : (a :: rest).length ≤ 1 := by simpa [hf] using (huniq a)
      have hlen' : rest.length = 0 := by
        have hle0 : rest.length ≤ 0 := by simpa using hlen
        exact Nat.eq_zero_of_le_zero hle0
      have hrest : rest = [] := List.eq_nil_of_length_eq_zero hlen'
      subst rest
      have hfind : l.find? p = some a := find_eq_some_of_filter_singleton l p a hf
      simp [hfind]

private theorem find_filter_pred_key (d : List DimRow) (k : Int) :
    (d.filter DimRow.pred).find? (fun r => r.k == some k) =
      d.find? (fun r => r.pred && r.k == some k) := by
  induction d with
  | nil => simp
  | cons r rs ih =>
      cases hpred : r.pred <;> simp [List.find?_cons, hpred, ih]

private theorem filter_filter_length_le {α : Type} (l : List α) (p q : α → Bool) :
    ((l.filter p).filter q).length ≤ (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true <;> by_cases hq : q a = true <;>
        simp [hp, hq] at ih ⊢ <;> omega

private theorem filter_key_length_le_one (d : List DimRow) (minK : Int) (span : Nat)
    (h : UniqueNonNullDimInRange d minK span) (k : Int) :
    ((d.filter DimRow.pred).filter (fun r => r.k == some k)).length ≤ 1 := by
  have hall := h.2 k
  have hle := filter_filter_length_le d DimRow.pred (fun r : DimRow => r.k == some k)
  omega

private theorem lookupDense_build_eq_find_in_range (d : List DimRow) (minK : Int) (span : Nat) (k : Int)
    (hlo : minK ≤ k) (hhi : k < minK + Int.ofNat span) :
    lookupDense (buildDenseDimTable d minK span) minK span (some k) =
      match d.find? (fun r => r.pred && r.k == some k) with
      | some r => some r.attr
      | none => none := by
  unfold lookupDense
  have hhi' : k < minK + (span : Int) := by simpa [Int.ofNat_eq_natCast] using hhi
  have hnot1 : ¬ k < minK := by omega
  have hnot2 : ¬ minK + (span : Int) ≤ k := by omega
  simp [hnot1, hnot2, Int.ofNat_eq_natCast]
  let i : Nat := (k - minK).toNat
  have hnonneg : 0 ≤ k - minK := by omega
  have hicast : (i : Int) = k - minK := by
    dsimp [i]
    exact Int.toNat_of_nonneg hnonneg
  have hispan : i < span := by
    dsimp [i]
    exact (Int.toNat_lt hnonneg).mpr (by omega)
  have hk_eq : minK + Int.ofNat i = k := by
    rw [Int.ofNat_eq_natCast, hicast]
    omega
  have hlen : (buildDenseDimTable d minK span).length = span := by
    simp [buildDenseDimTable]
  have hi_len : i < (buildDenseDimTable d minK span).length := by omega
  have hget : (buildDenseDimTable d minK span)[i]? =
      some (match d.find? (fun r => r.pred && r.k == some (minK + Int.ofNat i)) with
            | some r => some r.attr
            | none => none) := by
    simp [buildDenseDimTable, List.getElem?_map, List.getElem?_range hispan]
    rfl
  rw [hget]
  have hk_eq' : minK + (i : Int) = k := by
    simpa [Int.ofNat_eq_natCast] using hk_eq
  simp [hk_eq']

private theorem lookupDense_build_eq_find (d : List DimRow) (minK : Int) (span : Nat)
    (h : UniqueNonNullDimInRange d minK span) (k : Int) :
    lookupDense (buildDenseDimTable d minK span) minK span (some k) =
      match d.find? (fun r => r.pred && r.k == some k) with
      | some r => some r.attr
      | none => none := by
  by_cases hk : minK ≤ k ∧ k < minK + Int.ofNat span
  · exact lookupDense_build_eq_find_in_range d minK span k hk.1 hk.2
  · have hl : lookupDense (buildDenseDimTable d minK span) minK span (some k) = none := by
      unfold lookupDense
      have hk' : ¬ (minK ≤ k ∧ k < minK + (span : Int)) := by
        simpa [Int.ofNat_eq_natCast] using hk
      have hor : k < minK ∨ minK + (span : Int) ≤ k := by omega
      rcases hor with hlt | hge
      · simp [hlt]
      · simp [Int.ofNat_eq_natCast, hge]
    have hf : d.find? (fun r => r.pred && r.k == some k) = none := by
      apply List.find?_eq_none.mpr
      intro r hr hq
      have hkbeq : (r.k == some k) = true := by
        by_cases hp : r.pred = true
        · simpa [hp] using hq
        · have hfalse : (r.pred && r.k == some k) = false := by simp [hp]
          rw [hfalse] at hq
          contradiction
      have hkeq : r.k = some k := beq_iff_eq.mp hkbeq
      have hrange := h.1 r hr
      have hrange' : minK ≤ k ∧ k < minK + Int.ofNat span := by
        simpa [hkeq] using hrange
      exact hk hrange'
    simp [hl, hf]

private theorem refFactRows_eq_optFactVal
    (d1 d2 : List DimRow) (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (h1 : UniqueNonNullDimInRange d1 min1 span1)
    (h2 : UniqueNonNullDimInRange d2 min2 span2)
    (f : FactRow) (a1 a2 : Int) :
    refFactRows d1 d2 f a1 a2 =
      match optFactVal d1 d2 min1 span1 min2 span2 f a1 a2 with
      | some v => [v]
      | none => [] := by
  unfold refFactRows optFactVal
  cases f with
  | mk fk1 fk2 val =>
      cases fk1 with
      | none => simp [lookupDense]
      | some k1 =>
          cases fk2 with
          | none =>
              have hf2 :
                  (d2.filter DimRow.pred).filterMap (fun _ : DimRow => (none : Option (Option Int))) = [] := by
                induction (d2.filter DimRow.pred) with
                | nil => simp
                | cons r rs ih => simp [ih]
              simp [lookupDense, hf2]
              intro x hx hp
              cases x.k <;> rfl
          | some k2 =>
              change
                (d1.filter DimRow.pred).flatMap (fun r1 =>
                  match (some k1 : Option Int), r1.k with
                  | some fk1', some k1' =>
                      if fk1' == k1' then
                        (d2.filter DimRow.pred).filterMap (fun r2 =>
                          match (some k2 : Option Int), r2.k with
                          | some fk2', some k2' =>
                              if fk2' == k2' && r1.attr == a1 && r2.attr == a2 then some val else none
                          | _, _ => none)
                      else []
                  | _, _ => []) =
                match (match lookupDense (buildDenseDimTable d1 min1 span1) min1 span1 (some k1),
                             lookupDense (buildDenseDimTable d2 min2 span2) min2 span2 (some k2) with
                       | some attr1, some attr2 =>
                           if attr1 == a1 && attr2 == a2 then some val else none
                       | _, _ => none) with
                | some v => [v]
                | none => []
              let p1 : DimRow → Bool := fun r => r.k == some k1
              let p2 : DimRow → Bool := fun r => r.k == some k2
              have hrow2 : ∀ (r1 r2 : DimRow),
                  (match (some k2 : Option Int), r2.k with
                   | some fk2', some k2' =>
                       if fk2' == k2' && r1.attr == a1 && r2.attr == a2 then some val else none
                   | _, _ => none) =
                  (if p2 r2 && r1.attr == a1 && r2.attr == a2 then some val else none) := by
                intro r1 r2
                cases hk2' : r2.k with
                | none => simp [p2, hk2']
                | some k2' => simp [p2, hk2', beq_symm_int k2 k2']
              simp only [hrow2]
              have hrow1 : ∀ r1 : DimRow,
                  (match (some k1 : Option Int), r1.k with
                   | some fk1', some k1' =>
                       if fk1' == k1' then
                         (d2.filter DimRow.pred).filterMap (fun r2 =>
                           if p2 r2 && r1.attr == a1 && r2.attr == a2 then some val else none)
                       else []
                   | _, _ => [])
                  =
                  (if p1 r1 then
                     (d2.filter DimRow.pred).filterMap (fun r2 =>
                       if p2 r2 && r1.attr == a1 && r2.attr == a2 then some val else none)
                   else []) := by
                intro r1
                cases hk1' : r1.k with
                | none => simp [p1, hk1']
                | some k1' => simp [p1, hk1', beq_symm_int k1 k1']
              simp only [hrow1]
              have hinner : ∀ r1 : DimRow,
                  (d2.filter DimRow.pred).filterMap (fun r2 =>
                     if p2 r2 && r1.attr == a1 && r2.attr == a2 then some val else none)
                  =
                  match (d2.filter DimRow.pred).find? p2 with
                  | some x => if r1.attr == a1 && x.attr == a2 then [val] else []
                  | none => [] := by
                intro r1
                have hfilter_flat :
                    (d2.filter DimRow.pred).filterMap (fun x =>
                        if p2 x && r1.attr == a1 && x.attr == a2 then some val else none) =
                      (d2.filter DimRow.pred).flatMap (fun x =>
                        if p2 x then (if r1.attr == a1 && x.attr == a2 then [val] else []) else []) := by
                  induction (d2.filter DimRow.pred) with
                  | nil => simp
                  | cons a as ih =>
                      by_cases hq : p2 a = true <;>
                        by_cases he : r1.attr == a1 && a.attr == a2 <;>
                          simp only [List.filterMap_cons, List.flatMap_cons, hq, he, ih,
                            Bool.true_and, Bool.false_and, Bool.false_eq_true,
                            ite_true, ite_false, List.nil_append, List.cons_append]
                rw [hfilter_flat]
                rw [flatMap_unique_find (d2.filter DimRow.pred) p2
                  (fun x => if r1.attr == a1 && x.attr == a2 then [val] else [])
                  (by
                    intro x
                    dsimp [p2]
                    exact filter_key_length_le_one d2 min2 span2 h2 k2)]
                cases hf2 : (d2.filter DimRow.pred).find? p2 with
                | none => simp
                | some x =>
                    simp
              simp only [hinner]
              rw [flatMap_unique_find (d1.filter DimRow.pred) p1
                    (fun r1 => match (d2.filter DimRow.pred).find? p2 with
                               | some r2 => if r1.attr == a1 && r2.attr == a2 then [val] else []
                               | none => [])
                    (by
                      intro x
                      dsimp [p1]
                      exact filter_key_length_le_one d1 min1 span1 h1 k1)]
              rw [find_filter_pred_key d1 k1]
              rw [find_filter_pred_key d2 k2]
              rw [lookupDense_build_eq_find d1 min1 span1 h1 k1]
              rw [lookupDense_build_eq_find d2 min2 span2 h2 k2]
              cases hq1 : d1.find? (fun r => r.pred && r.k == some k1) with
              | none => simp
              | some r1 =>
                  cases hq2 : d2.find? (fun r => r.pred && r.k == some k2) with
                  | none => simp
                  | some r2 =>
                      by_cases ha1 : r1.attr == a1 <;>
                        by_cases ha2 : r2.attr == a2 <;> simp [ha1, ha2]

private theorem optStarDirect_eq_refTwoHashJoins
    (d1 d2 : List DimRow) (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (h1 : UniqueNonNullDimInRange d1 min1 span1)
    (h2 : UniqueNonNullDimInRange d2 min2 span2)
    (facts : List FactRow) (a1 a2 : Int) :
    optStarDirect d1 d2 min1 span1 min2 span2 facts a1 a2 =
      refTwoHashJoins d1 d2 facts a1 a2 := by
  unfold optStarDirect refTwoHashJoins
  change
    (((facts.filterMap (fun f => optFactVal d1 d2 min1 span1 min2 span2 f a1 a2)).length,
      sumOptList (facts.filterMap (fun f => optFactVal d1 d2 min1 span1 min2 span2 f a1 a2))) =
     ((facts.flatMap (fun f => refFactRows d1 d2 f a1 a2)).length,
      sumOptList (facts.flatMap (fun f => refFactRows d1 d2 f a1 a2))))
  have hlist :
      facts.filterMap (fun f => optFactVal d1 d2 min1 span1 min2 span2 f a1 a2) =
        facts.flatMap (fun f => refFactRows d1 d2 f a1 a2) := by
    induction facts with
    | nil => simp
    | cons f fs ih =>
        cases hopt : optFactVal d1 d2 min1 span1 min2 span2 f a1 a2 with
        | none =>
            simp [List.flatMap_cons, hopt,
              refFactRows_eq_optFactVal d1 d2 min1 span1 min2 span2 h1 h2 f a1 a2, ih]
        | some v =>
            simp [List.flatMap_cons, hopt,
              refFactRows_eq_optFactVal d1 d2 min1 span1 min2 span2 h1 h2 f a1 a2, ih]
  rw [hlist]

theorem sound : TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup.Goal := by
  constructor
  · intro d1 d2 min1 span1 min2 span2 facts a1 a2 h1 h2
    exact optStarDirect_eq_refTwoHashJoins d1 d2 min1 span1 min2 span2 h1 h2 facts a1 a2
  · decide

end TinylambOpt.ResearchProofs.SmallDimDenseArrayDirectLookup
