import Std
import TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset

/-!
Proved

- `Goal` — Disjunctive `EXISTS` cascading short-circuit bitset narrowing (TPC-DS
  Q10/Q35): rows satisfying `EXISTS(S1) ∧ EXISTS(S2)` never probe `S3`, preserving
  exact filtered output across NULLs while executing zero `S3` probes whenever `B_1 ⊆
  B_2`.

The reference model is defined in
`TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset`.
-/

open TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset

namespace TinylambOpt.ResearchProofs.DisjunctiveExistsCascadingBitset

abbrev IdxRow := Option Int × Nat

private theorem mem_insertIdx {x y : IdxRow} {ys : List IdxRow} :
    y ∈ insertIdx x ys ↔ y = x ∨ y ∈ ys := by
  induction ys with
  | nil => simp [insertIdx]
  | cons z zs ih =>
      by_cases h : x.2 ≤ z.2
      · simp [insertIdx, h]
      · simp [insertIdx, h, ih, or_left_comm]

private theorem insertIdx_perm (x : IdxRow) (ys : List IdxRow) :
    (insertIdx x ys).Perm (x :: ys) := by
  induction ys with
  | nil => simp [insertIdx]
  | cons z zs ih =>
      by_cases h : x.2 ≤ z.2
      · simp [insertIdx, h]
      · rw [insertIdx, ite_eq_right h]
        exact (ih.cons z).trans (List.Perm.swap z x zs).symm

private theorem pairwise_insertIdx_le {x : IdxRow} {ys : List IdxRow}
    (hys : ys.Pairwise (fun a b : IdxRow => a.2 ≤ b.2)) :
    (insertIdx x ys).Pairwise (fun a b : IdxRow => a.2 ≤ b.2) := by
  induction ys with
  | nil => simp [insertIdx]
  | cons z zs ih =>
      by_cases h : x.2 ≤ z.2
      · rw [insertIdx, ite_eq_left h]
        rw [List.pairwise_cons] at hys ⊢
        constructor
        · intro w hw
          simp at hw
          rcases hw with rfl | hw
          · exact h
          · have hzw : z.2 ≤ w.2 := hys.1 w hw
            omega
        · exact List.pairwise_cons.mpr hys
      · rw [insertIdx, ite_eq_right h]
        rw [List.pairwise_cons] at hys ⊢
        constructor
        · intro w hw
          rw [mem_insertIdx] at hw
          rcases hw with rfl | hw
          · omega
          · exact hys.1 w hw
        · exact ih hys.2

private theorem pairwise_sortByIdx_le (l : List IdxRow) :
    (sortByIdx l).Pairwise (fun a b : IdxRow => a.2 ≤ b.2) := by
  induction l with
  | nil => simp [sortByIdx]
  | cons x xs ih =>
      simpa [sortByIdx] using
        (pairwise_insertIdx_le (x := x) (ys := sortByIdx xs) ih)

private theorem sortByIdx_perm (l : List IdxRow) : (sortByIdx l).Perm l := by
  induction l with
  | nil => simp [sortByIdx]
  | cons x xs ih =>
      simpa [sortByIdx] using (insertIdx_perm x (sortByIdx xs)).trans (ih.cons x)

private theorem map_fst_zipIdx_filter (c : List (Option Int)) (n : Nat)
    (P : Option Int → Bool) :
    ((c.zipIdx n).filter (fun p => P p.1)).map Prod.fst = c.filter P := by
  induction c generalizing n with
  | nil => simp
  | cons a t ih =>
      by_cases h : P a
      · simp [List.zipIdx_cons, h, ih]
      · simp [List.zipIdx_cons, h, ih]

private theorem fst_mem_of_mem_zipIdx {c : List (Option Int)} {n : Nat} {p : IdxRow}
    (hp : p ∈ c.zipIdx n) : p.1 ∈ c := by
  induction c generalizing n with
  | nil => simp at hp
  | cons a t ih =>
      rw [List.zipIdx_cons] at hp
      simp at hp
      rcases hp with hp | hp
      · subst p
        simp
      · have hmem : p.1 ∈ t := ih hp
        simp [hmem]

private theorem zipIdx_idx_inj {c : List (Option Int)} {a b : IdxRow}
    (ha : a ∈ c.zipIdx) (hb : b ∈ c.zipIdx) (hidx : a.2 = b.2) : a = b := by
  have hama := List.mem_zipIdx ha
  have hbmb := List.mem_zipIdx hb
  have haval : a.1 = c[a.2] := by simpa using hama.2.2
  have hbval : b.1 = c[b.2] := by simpa using hbmb.2.2
  rcases a with ⟨av, ai⟩
  rcases b with ⟨bv, bi⟩
  simp at haval hbval hidx
  subst bi
  congr
  exact haval.trans hbval.symm

private theorem zipIdx_pairwise_le (c : List (Option Int)) (n : Nat) :
    (c.zipIdx n).Pairwise (fun a b : IdxRow => a.2 ≤ b.2) := by
  induction c generalizing n with
  | nil => simp
  | cons a t ih =>
      rw [List.zipIdx_cons]
      rw [List.pairwise_cons]
      constructor
      · intro p hp
        have hp' := (List.mem_zipIdx hp).1
        omega
      · exact ih (n + 1)

theorem sound : TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset.Goal := by
  intro c s1 s2 s3
  let P1 : Option Int → Bool := fun k => optExistsIn k s1
  let P2 : Option Int → Bool := fun k => optExistsIn k s2
  let P3 : Option Int → Bool := fun k => optExistsIn k s3
  let step1 : List IdxRow := c.zipIdx.filter (fun p => P1 p.1)
  let hit2 : List IdxRow := step1.filter (fun p => P2 p.1)
  let rem : List IdxRow := step1.filter (fun p => !P2 p.1)
  let hit3 : List IdxRow := rem.filter (fun p => P3 p.1)
  let refZipped : List IdxRow :=
    c.zipIdx.filter (fun p => P1 p.1 && (P2 p.1 || P3 p.1))

  have hstep_filter : step1.filter (fun p => P2 p.1 || P3 p.1) = refZipped := by
    simp [step1, refZipped, List.filter_filter, Bool.and_comm]

  have hcore_perm : (hit2 ++ hit3).Perm refZipped := by
    let f : IdxRow → Bool := fun p => P2 p.1 || P3 p.1
    have hpart :
        (step1.filter (fun p => P2 p.1) ++
          (step1.filter (fun p => !P2 p.1)).filter (fun p => P3 p.1)).Perm
          (step1.filter f) := by
      have h := (List.filter_append_perm (fun p : IdxRow => P2 p.1) step1).filter f
      have hlhs :
          (step1.filter (fun p => P2 p.1) ++ step1.filter (fun p => !P2 p.1)).filter f =
            step1.filter (fun p => P2 p.1) ++
              (step1.filter (fun p => !P2 p.1)).filter (fun p => P3 p.1) := by
        rw [List.filter_append]
        congr 1
        · apply List.filter_eq_self.mpr
          intro x hx
          have hxq : P2 x.1 = true := (List.mem_filter.mp hx).2
          simp [f, hxq]
        · apply List.filter_congr
          intro x hx
          have hnq : (!P2 x.1) = true := (List.mem_filter.mp hx).2
          have hq : P2 x.1 = false := by
            cases hq' : P2 x.1 <;> simp_all
          simp [f, hq]
      rw [hlhs] at h
      exact h
    have hf : step1.filter f = refZipped := by
      simpa [f] using hstep_filter
    simpa [hit2, rem, hit3, hf] using hpart

  have href_sorted : refZipped.Pairwise (fun a b : IdxRow => a.2 ≤ b.2) := by
    exact List.Pairwise.sublist (List.filter_sublist (p := fun p : IdxRow => P1 p.1 && (P2 p.1 || P3 p.1))) (zipIdx_pairwise_le c 0)
  have hsort_perm : (sortByIdx (hit2 ++ hit3)).Perm (hit2 ++ hit3) :=
    sortByIdx_perm (hit2 ++ hit3)
  have hsort_sorted :
      (sortByIdx (hit2 ++ hit3)).Pairwise (fun a b : IdxRow => a.2 ≤ b.2) :=
    pairwise_sortByIdx_le (hit2 ++ hit3)

  have heqSort : sortByIdx (hit2 ++ hit3) = refZipped := by
    apply List.Perm.eq_of_pairwise
    · intro a b ha hb hab hba
      have ha1 : a ∈ hit2 ++ hit3 := hsort_perm.mem_iff.mp ha
      have haStep : a ∈ step1 := by
        rcases List.mem_append.mp ha1 with ha2 | ha3
        · exact List.Sublist.mem (by simpa [hit2] using ha2) (List.filter_sublist (p := fun p : IdxRow => P2 p.1))
        · have hrem : a ∈ rem :=
            List.Sublist.mem (by simpa [hit3] using ha3) (List.filter_sublist (p := fun p : IdxRow => P3 p.1))
          exact List.Sublist.mem (by simpa [rem] using hrem) (List.filter_sublist (p := fun p : IdxRow => !P2 p.1))
      have ha0 : a ∈ c.zipIdx :=
        List.Sublist.mem (by simpa [step1] using haStep) (List.filter_sublist (p := fun p : IdxRow => P1 p.1))
      have hb0 : b ∈ c.zipIdx :=
        List.Sublist.mem (by simpa [refZipped] using hb) (List.filter_sublist (p := fun p : IdxRow => P1 p.1 && (P2 p.1 || P3 p.1)))
      have hidx : a.2 = b.2 := Nat.le_antisymm hab hba
      exact zipIdx_idx_inj ha0 hb0 hidx
    · exact hsort_sorted
    · exact href_sorted
    · exact hsort_perm.trans hcore_perm

  have hmapRef : refZipped.map Prod.fst =
      c.filter (fun k => P1 k && (P2 k || P3 k)) := by
    simpa [refZipped] using
      (map_fst_zipIdx_filter c 0 (fun k => P1 k && (P2 k || P3 k)))

  have hs3 :
      (optCascadingFilter c s1 s2 s3).s3Probes = rem.length := by
    simp [optCascadingFilter, step1, rem, P1, P2]

  have hrowEq : (optCascadingFilter c s1 s2 s3).rows = refFilter c s1 s2 s3 := by
    simp only [optCascadingFilter]
    rw [show (c.zipIdx.filter (fun p => optExistsIn p.1 s1)) = step1 by
      simp [step1, P1]]
    rw [show (step1.filter (fun p => optExistsIn p.1 s2)) = hit2 by
      simp [hit2, P2]]
    rw [show (step1.filter (fun p => !optExistsIn p.1 s2)) = rem by
      simp [rem, P2]]
    rw [show (rem.filter (fun p => optExistsIn p.1 s3)) = hit3 by
      simp [hit3, P3]]
    rw [heqSort, hmapRef]
    simp [refFilter, P1, P2, P3]

  have hlenStep : step1.length = (c.filter (fun k => optExistsIn k s1)).length := by
    have hm := congrArg List.length (map_fst_zipIdx_filter c 0 P1)
    simpa [step1, P1] using hm
  have hremLe : rem.length ≤ step1.length := by
    exact List.length_filter_le (fun p : IdxRow => !P2 p.1) step1
  have hprobeLe : rem.length ≤ (c.filter (fun k => optExistsIn k s1)).length := by
    rw [← hlenStep]
    exact hremLe

  dsimp only
  constructor
  · exact hrowEq
  constructor
  · rw [hs3]
    exact hprobeLe
  · intro hsubset
    rw [hs3]
    have hnil : rem = [] := by
      apply (List.filter_eq_nil_iff).2
      intro p hp
      have hpStep : p ∈ c.zipIdx.filter (fun p : IdxRow => P1 p.1) := by
        simpa [step1] using hp
      have hp1 : P1 p.1 = true := (List.mem_filter.mp hpStep).2
      have hpz : p ∈ c.zipIdx := List.Sublist.mem hpStep List.filter_sublist
      have hk : p.1 ∈ c := fst_mem_of_mem_zipIdx hpz
      have hP2 : P2 p.1 = true := hsubset p.1 hk (by simpa [P1] using hp1)
      simp [hP2]
    simp [hnil]

end TinylambOpt.ResearchProofs.DisjunctiveExistsCascadingBitset
