import Std
import TinylambOpt.ProposedGoals.DistinctOnTop1

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DistinctOnTop1

open TinylambOpt.ProposedGoals.DistinctOnTop1

private theorem beforeRow_iff (a b : Row × Nat) :
    beforeRow a b = true ↔
      a.1.2.1 < b.1.2.1 ∨ (a.1.2.1 = b.1.2.1 ∧ a.2 < b.2) := by
  unfold beforeRow
  by_cases hlt : a.1.2.1 < b.1.2.1
  · simp [hlt]
  · by_cases heq : a.1.2.1 == b.1.2.1
    · have heq' : a.1.2.1 = b.1.2.1 := by simpa using heq
      simp [hlt, heq, heq']
    · have hne : a.1.2.1 ≠ b.1.2.1 := by simpa using heq
      simp [hlt, heq, hne]

private theorem beforeRow_irrefl (a : Row × Nat) : beforeRow a a = false := by
  simp [beforeRow]

private theorem beforeRow_trans {a b c : Row × Nat}
    (hab : beforeRow a b = true) (hbc : beforeRow b c = true) :
    beforeRow a c = true := by
  rw [beforeRow_iff] at hab hbc ⊢
  rcases hab with hab | ⟨heqab, hiab⟩
  · rcases hbc with hbc | ⟨heqbc, hibc⟩
    · left; omega
    · left; omega
  · rcases hbc with hbc | ⟨heqbc, hibc⟩
    · left; omega
    · right; constructor <;> omega

private theorem beforeRow_cmp (a b : Row × Nat) :
    beforeRow a b = true ∨ beforeRow b a = true ∨
      (a.1.2.1 = b.1.2.1 ∧ a.2 = b.2) := by
  rw [beforeRow_iff, beforeRow_iff]
  by_cases hlt : a.1.2.1 < b.1.2.1
  · simp [hlt]
  · by_cases hgt : b.1.2.1 < a.1.2.1
    · simp [hlt, hgt]
    · have heqk : a.1.2.1 = b.1.2.1 := by omega
      by_cases hi : a.2 < b.2
      · simp [hlt, hgt, heqk, hi]
      · by_cases hi' : b.2 < a.2
        · simp [hlt, hgt, heqk, hi, hi']
        · right; right; constructor <;> omega

private theorem beforeRow_asymm {a b : Row × Nat}
    (hab : beforeRow a b = true) (hba : beforeRow b a = true) : False := by
  have haa := beforeRow_trans hab hba
  have hf := beforeRow_irrefl a
  rw [haa] at hf
  contradiction

private def leB (a b : Row × Nat) : Bool := !beforeRow b a

private theorem leB_trans {a b c : Row × Nat} (hab : leB a b = true) (hbc : leB b c = true) :
    leB a c = true := by
  unfold leB at hab hbc ⊢
  have hnba : beforeRow b a = false := by
    by_cases h : beforeRow b a = true
    · simp [h] at hab
    · cases h' : beforeRow b a <;> simp_all
  have hncb : beforeRow c b = false := by
    by_cases h : beforeRow c b = true
    · simp [h] at hbc
    · cases h' : beforeRow c b <;> simp_all
  by_cases htca : beforeRow c a = true
  · rcases beforeRow_cmp c b with hcb | hbc' | ⟨hk, hi⟩
    · simp [hcb] at hncb
    · have hba := beforeRow_trans hbc' htca
      simp [hba] at hnba
    · have hca' : beforeRow c a = beforeRow b a := by
        simp [beforeRow, hk, hi]
      rw [← hca'] at hnba
      simp [hnba] at htca
  · cases h' : beforeRow c a <;> simp_all

private theorem leB_total (a b : Row × Nat) : (leB a b || leB b a) = true := by
  unfold leB
  by_cases hab : beforeRow a b = true
  · have hba : beforeRow b a = false := by
      cases h' : beforeRow b a
      · rfl
      · exact False.elim (beforeRow_asymm hab h')
    simp [hab, hba]
  · have habF : beforeRow a b = false := by
      cases h' : beforeRow a b
      · rfl
      · exact False.elim (hab h')
    simp [habF]

private theorem leB_of_beforeRow {a b : Row × Nat} (h : beforeRow a b = true) : leB a b = true := by
  unfold leB
  have hba : beforeRow b a = false := by
    cases h' : beforeRow b a
    · rfl
    · exact False.elim (beforeRow_asymm h h')
  simp [hba]

private theorem zipIdx_index_unique {rows : List Row} {a b : Row × Nat}
    (ha : a ∈ rows.zipIdx) (hb : b ∈ rows.zipIdx) (hidx : a.2 = b.2) : a = b := by
  rcases a with ⟨ar, ai⟩
  rcases b with ⟨br, bi⟩
  simp only at hidx
  subst bi
  have ha' := List.mem_zipIdx ha
  have hb' := List.mem_zipIdx hb
  simp at ha' hb'
  have hrow : ar = br := by rw [ha'.2, hb'.2]
  subst br
  rfl

private theorem pairwise_merge_beforeRow (rows : List Row)
    (xs ys : List (Row × Nat))
    (hxZ : ∀ x ∈ xs, x ∈ rows.zipIdx) (hyZ : ∀ y ∈ ys, y ∈ rows.zipIdx)
    (hxs : xs.Pairwise (fun a b => leB a b = true))
    (hys : ys.Pairwise (fun a b => leB a b = true)) :
    (xs.merge ys beforeRow).Pairwise (fun a b => leB a b = true) := by
  induction xs generalizing ys with
  | nil => simpa [List.merge] using hys
  | cons x xs ihxs =>
      induction ys with
      | nil => simpa [List.merge] using hxs
      | cons y ys ihys =>
          simp only [List.merge]
          split
          · rename_i hxy
            apply List.Pairwise.cons
            · intro z hz
              rw [List.mem_merge, List.mem_cons] at hz
              rcases hz with hz | rfl | hz
              · exact (List.pairwise_cons.mp hxs).1 z hz
              · exact leB_of_beforeRow hxy
              · exact leB_trans (leB_of_beforeRow hxy) ((List.pairwise_cons.mp hys).1 z hz)
            · apply ihxs
              · intro a ha; exact hxZ a (by simp [ha])
              · intro a ha; exact hyZ a (by simp [ha])
              · exact hxs.tail
              · exact hys
          · rename_i hxy
            apply List.Pairwise.cons
            · intro z hz
              rw [List.mem_merge, List.mem_cons] at hz
              rcases hz with (rfl | hz) | hz
              · unfold leB; simp [hxy]
              · exact leB_trans (by unfold leB; simp [hxy]) ((List.pairwise_cons.mp hxs).1 z hz)
              · exact (List.pairwise_cons.mp hys).1 z hz
            · apply ihys
              · intro a ha; exact hyZ a (by simp [ha])
              · exact hys.tail

private theorem pairwise_ms (rows : List Row) : ∀ l : List (Row × Nat),
    (∀ x ∈ l, x ∈ rows.zipIdx) →
    (l.mergeSort beforeRow).Pairwise (fun a b => leB a b = true)
  | [], hmem => by simp
  | [a], hmem => by simp
  | a :: b :: xs, hmem => by
      rw [List.mergeSort]
      let f := (List.MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).1.1
      let g := (List.MergeSort.Internal.splitInTwo ⟨a :: b :: xs, rfl⟩).2.1
      change ((f.mergeSort beforeRow).merge (g.mergeSort beforeRow) beforeRow).Pairwise
        (fun x y => leB x y = true)
      have hcat : f ++ g = a :: b :: xs := by
        simpa [f, g] using
          (List.MergeSort.Internal.splitInTwo_fst_append_splitInTwo_snd
            (l := ⟨a :: b :: xs, rfl⟩))
      apply pairwise_merge_beforeRow rows
      · intro x hx
        have hx' : x ∈ f := (List.mergeSort_perm f beforeRow).mem_iff.mp hx
        exact hmem x (by rw [← hcat]; simp [hx'])
      · intro x hx
        have hx' : x ∈ g := (List.mergeSort_perm g beforeRow).mem_iff.mp hx
        exact hmem x (by rw [← hcat]; simp [hx'])
      · exact pairwise_ms rows f (by
          intro x hx
          exact hmem x (by rw [← hcat]; simp [hx]))
      · exact pairwise_ms rows g (by
          intro x hx
          exact hmem x (by rw [← hcat]; simp [hx]))
termination_by l => l.length
decreasing_by
  · simp [f]; omega
  · simp [g]; omega

private def step (k : Option Int) (acc : Option (Row × Nat)) (ir : Row × Nat) : Option (Row × Nat) :=
  if keyOf ir.1 != k then acc else
  match acc with
  | none => some ir
  | some best => if beforeRow ir best then some ir else acc

private def IsBest (k : Option Int) (seen : List (Row × Nat)) (m : Row × Nat) : Prop :=
  (keyOf m.1 == k) = true ∧ m ∈ seen ∧
    ∀ x ∈ seen, (keyOf x.1 == k) = true → beforeRow m x = true ∨ x = m

private def AccOk (rows : List Row) (k : Option Int) (seen : List (Row × Nat))
    (acc : Option (Row × Nat)) : Prop :=
  (acc = none ∧ ∀ x ∈ seen, (keyOf x.1 == k) = true → False) ∨
  (∃ m, acc = some m ∧ IsBest k seen m ∧ m ∈ rows.zipIdx)

private theorem step_ok (rows : List Row) (k : Option Int) (seen : List (Row × Nat))
    (acc : Option (Row × Nat)) (ir : Row × Nat) (rest : List (Row × Nat))
    (hpref : seen ++ ir :: rest = rows.zipIdx)
    (hacc : AccOk rows k seen acc) :
    AccOk rows k (seen ++ [ir]) (step k acc ir) := by
  have hirZip : ir ∈ rows.zipIdx := by rw [← hpref]; simp
  by_cases hk : (keyOf ir.1 == k) = true
  · have hkEq : keyOf ir.1 = k := by simpa using hk
    have hne : (keyOf ir.1 != k) = false := by simp [hkEq]
    rcases hacc with ⟨haccNone, hnone⟩ | ⟨m, haccSome, hbest, hmZip⟩
    · right
      refine ⟨ir, ?_, ?_, hirZip⟩
      · simp [step, hne, haccNone]
      · constructor
        · exact hk
        · constructor
          · simp
          · intro x hx hxk
            simp at hx
            rcases hx with hx | hxEq
            · exact False.elim (hnone x hx hxk)
            · subst x; exact Or.inr rfl
    · right
      by_cases hb : beforeRow ir m = true
      · refine ⟨ir, ?_, ?_, hirZip⟩
        · simp [step, hne, haccSome, hb]
        · constructor
          · exact hk
          · constructor
            · simp
            · intro x hx hxk
              simp at hx
              rcases hx with hx | hxEq
              · rcases hbest.2.2 x hx hxk with hmx | hxm
                · exact Or.inl (beforeRow_trans hb hmx)
                · subst x; exact Or.inl hb
              · subst x; exact Or.inr rfl
      · refine ⟨m, ?_, ?_, hmZip⟩
        · simp [step, hne, haccSome, hb]
        · constructor
          · exact hbest.1
          · constructor
            · exact List.mem_append.mpr (Or.inl hbest.2.1)
            · intro x hx hxk
              simp at hx
              rcases hx with hx | hxEq
              · exact hbest.2.2 x hx hxk
              · subst x
                rcases beforeRow_cmp ir m with hib | hmi | heqIdx
                · exact False.elim (hb hib)
                · exact Or.inl hmi
                · exact Or.inr (zipIdx_index_unique hirZip hmZip heqIdx.2)
  · have hne : (keyOf ir.1 != k) = true := by
      cases h' : keyOf ir.1 == k <;> simp_all
    rcases hacc with ⟨haccNone, hnone⟩ | ⟨m, haccSome, hbest, hmZip⟩
    · left
      constructor
      · simp [step, hne, haccNone]
      · intro x hx hxk
        simp at hx
        rcases hx with hx | hxEq
        · exact hnone x hx hxk
        · subst x; exact False.elim (hk hxk)
    · right
      refine ⟨m, ?_, ?_, hmZip⟩
      · simpa [step, hne] using haccSome
      · constructor
        · exact hbest.1
        · constructor
          · exact List.mem_append.mpr (Or.inl hbest.2.1)
          · intro x hx hxk
            simp at hx
            rcases hx with hx | hxEq
            · exact hbest.2.2 x hx hxk
            · subst x; exact False.elim (hk hxk)

private theorem foldl_ok (rows : List Row) (k : Option Int) (seen l : List (Row × Nat))
    (acc : Option (Row × Nat))
    (hpref : ∃ t, (seen ++ l) ++ t = rows.zipIdx)
    (hacc : AccOk rows k seen acc) :
    AccOk rows k (seen ++ l) (l.foldl (step k) acc) := by
  induction l generalizing seen acc with
  | nil => simpa using hacc
  | cons ir t ih =>
      rcases hpref with ⟨u, hu⟩
      have hu' : seen ++ ir :: (t ++ u) = rows.zipIdx := by simpa [List.append_assoc] using hu
      have hstep : AccOk rows k (seen ++ [ir]) (step k acc ir) :=
        step_ok rows k seen acc ir (t ++ u) hu' hacc
      have hpref2 : ∃ v, ((seen ++ [ir]) ++ t) ++ v = rows.zipIdx := by
        refine ⟨u, ?_⟩
        simpa [List.append_assoc] using hu
      simpa [List.foldl] using (ih (seen := seen ++ [ir]) (acc := step k acc ir) hpref2 hstep)

theorem sound_DistinctOnTop1 : TinylambOpt.ProposedGoals.DistinctOnTop1.Goal := by
  intro rows k
  unfold top1Fold sortDedup
  change ((rows.zipIdx.foldl (step k) none).map Prod.fst) =
    (((rows.zipIdx.mergeSort beforeRow).find? (fun ir => keyOf ir.1 == k)).map Prod.fst)
  let sorted := rows.zipIdx.mergeSort beforeRow
  have hperm : sorted.Perm rows.zipIdx := by
    simpa [sorted] using (List.mergeSort_perm rows.zipIdx beforeRow)
  have hsorted : sorted.Pairwise (fun a b => leB a b = true) := by
    simpa [sorted] using (pairwise_ms rows rows.zipIdx (by intro x hx; exact hx))
  have hpref : ∃ t, ([] ++ rows.zipIdx) ++ t = rows.zipIdx := ⟨[], by simp⟩
  have hacc0 : AccOk rows k [] none := by
    left
    constructor
    · rfl
    · intro x hx; simp at hx
  have hres := foldl_ok rows k [] rows.zipIdx none hpref hacc0
  simp only [List.nil_append] at hres
  rcases hres with ⟨hnoneAcc, hnokey⟩ | ⟨m, hmSome, hbest, hmZip⟩
  · have hfind : sorted.find? (fun ir => keyOf ir.1 == k) = none := by
      apply List.find?_eq_none.mpr
      intro x hx hpx
      exact hnokey x (hperm.mem_iff.mp hx) hpx
    simp [sorted, hfind, hnoneAcc]
  · have hmSorted : m ∈ sorted := hperm.mem_iff.mpr hmZip
    have hfind : sorted.find? (fun ir => keyOf ir.1 == k) = some m := by
      cases hfind : sorted.find? (fun ir => keyOf ir.1 == k) with
      | none =>
          have hn := List.find?_eq_none.mp hfind
          exact False.elim (hn m hmSorted hbest.1)
      | some y =>
          rcases List.find?_eq_some_iff_getElem.mp hfind with ⟨hpy, j, hj, hsy, hprior⟩
          rcases List.mem_iff_getElem.mp hmSorted with ⟨i, hi, hsi⟩
          by_cases hij : i = j
          · subst j
            have hyEq : y = m := by rw [← hsy, hsi]
            simpa [hyEq]
          · by_cases hlt : i < j
            · have hpri := hprior i hlt
              rw [hsi] at hpri
              simp [hbest.1] at hpri
            · have hjlt : j < i := by omega
              have hle := hsorted.rel_getElem_of_lt hj hi hjlt
              have hle' : leB y m = true := by simpa [hsy, hsi] using hle
              have hySorted : y ∈ sorted := List.mem_iff_getElem.mpr ⟨j, hj, hsy⟩
              have hyZip : y ∈ rows.zipIdx := hperm.mem_iff.mp hySorted
              rcases hbest.2.2 y hyZip hpy with hmy | hyEq
              · unfold leB at hle'
                simp [hmy] at hle'
              · simpa [hyEq]
    simp [sorted, hfind, hmSome]

end TinylambOpt.ResearchProofs.Atlas.Goal_DistinctOnTop1
