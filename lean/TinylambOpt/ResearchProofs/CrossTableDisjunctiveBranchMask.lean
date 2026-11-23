import TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask

namespace TinylambOpt.ResearchProofs.CrossTableDisjunctiveBranchMask

open TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask

private theorem isTrue3_sqlAnd (a b : Option Bool) :
    isTrue3 (sqlAnd a b) = true ↔ a = some true ∧ b = some true := by
  rcases a with (_ | (_ | _)) <;> rcases b with (_ | (_ | _)) <;>
    simp [sqlAnd, isTrue3]

private theorem isTrue3_sqlOr (a b : Option Bool) :
    isTrue3 (sqlOr a b) = true ↔ isTrue3 a = true ∨ isTrue3 b = true := by
  rcases a with (_ | (_ | _)) <;> rcases b with (_ | (_ | _)) <;>
    simp [sqlOr, isTrue3]

private theorem sqlOr_fold_true {α : Type} (l : List α) (g : α → Option Bool) (init : Option Bool) :
    isTrue3 (l.foldl (fun acc x => sqlOr acc (g x)) init) = true ↔
      isTrue3 init = true ∨ ∃ x ∈ l, isTrue3 (g x) = true := by
  induction l generalizing init with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih (sqlOr init (g x)), isTrue3_sqlOr]
      constructor
      · rintro ((hinit | hgx) | ⟨y, hy, hgy⟩)
        · exact Or.inl hinit
        · exact Or.inr ⟨x, by simp, hgx⟩
        · exact Or.inr ⟨y, by simp [hy], hgy⟩
      · rintro (hinit | ⟨y, hy, hgy⟩)
        · exact Or.inl (Or.inl hinit)
        · have hy' : y = x ∨ y ∈ xs := by simpa using hy
          rcases hy' with rfl | hy
          · exact Or.inl (Or.inr hgy)
          · exact Or.inr ⟨y, hy, hgy⟩

private theorem evalBranches_true_iff {D F : Type} (branches : List ((D → Option Bool) × (F → Option Bool)))
    (d : D) (f : F) :
    isTrue3 (evalBranches branches d f) = true ↔
      ∃ p ∈ branches, p.1 d = some true ∧ p.2 f = some true := by
  unfold evalBranches
  rw [sqlOr_fold_true]
  have hinit : isTrue3 (some false) = true ↔ False := by simp [isTrue3]
  rw [hinit, false_or]
  simp [isTrue3_sqlAnd]

private theorem maskFoldProp_testBit {α : Type} (l : List ((α → Option Bool) × Nat)) (a : α)
    (acc : Nat) (idx : Nat) :
    (l.foldl (fun m x => if x.1 a = some true then m ||| (1 <<< x.2) else m) acc).testBit idx =
      (acc.testBit idx || decide (∃ x ∈ l, x.2 = idx ∧ x.1 a = some true)) := by
  induction l generalizing acc with
  | nil => simp
  | cons x t ih =>
      rw [List.foldl_cons]
      by_cases hpa : x.1 a = some true
      · rw [ite_eq_left hpa]
        rw [ih (acc ||| (1 <<< x.2))]
        rw [Nat.testBit_or, Nat.one_shiftLeft, Nat.testBit_two_pow]
        by_cases hj : x.2 = idx
        · subst idx
          simp [hpa]
        · by_cases htail : ∃ y ∈ t, y.2 = idx ∧ y.1 a = some true
          · simp [hpa, hj, htail]
          · simp [hpa, hj, htail]
      · rw [ite_eq_right hpa]
        rw [ih acc]
        by_cases htail : ∃ y ∈ t, y.2 = idx ∧ y.1 a = some true
        · simp [hpa, htail]
        · simp [hpa, htail]

private theorem maskFoldBool_eq_Prop {α : Type} (l : List ((α → Option Bool) × Nat)) (a : α) (acc : Nat) :
    l.foldl (fun m x => if x.1 a == some true then m ||| (1 <<< x.2) else m) acc =
      l.foldl (fun m x => if x.1 a = some true then m ||| (1 <<< x.2) else m) acc := by
  induction l generalizing acc with
  | nil => rfl
  | cons x t ih =>
      rw [List.foldl_cons, List.foldl_cons]
      by_cases h : x.1 a = some true
      · have hb : (x.1 a == some true) = true := by
          rw [Bool.beq_eq_decide_eq]
          simp [h]
        rw [show (if x.1 a == some true then acc ||| (1 <<< x.2) else acc) = acc ||| (1 <<< x.2) by simp [hb]]
        rw [show (if x.1 a = some true then acc ||| (1 <<< x.2) else acc) = acc ||| (1 <<< x.2) by simp [h]]
        exact ih (acc ||| (1 <<< x.2))
      · have hb : (x.1 a == some true) = false := by
          rw [Bool.beq_eq_decide_eq]
          simp [h]
        rw [show (if x.1 a == some true then acc ||| (1 <<< x.2) else acc) = acc by simp [hb]]
        rw [show (if x.1 a = some true then acc ||| (1 <<< x.2) else acc) = acc by simp [h]]
        exact ih acc

private theorem maskOfPreds_testBit {α : Type} (preds : List (α → Option Bool)) (a : α) (idx : Nat) :
    (maskOfPreds preds a).testBit idx = true ↔
      ∃ p ∈ preds.zipIdx, p.2 = idx ∧ p.1 a = some true := by
  unfold maskOfPreds
  rw [maskFoldBool_eq_Prop]
  rw [maskFoldProp_testBit]
  simp

private theorem zipIdx_index_ge {α : Type} {l : List α} {n idx : Nat} {p : α}
    (h : (p, idx) ∈ l.zipIdx n) : n ≤ idx := by
  induction l generalizing n with
  | nil => cases h
  | cons a t ih =>
      rw [List.zipIdx_cons] at h
      simp only [List.mem_cons] at h
      rcases h with h | h
      · have heq : (p, idx) = (a, n) := h
        cases heq
        omega
      · have ht := ih (n := n + 1) h
        omega

private theorem zipIdx_getElem {α : Type} {l : List α} {n idx : Nat} {p : α}
    (h : (p, idx) ∈ l.zipIdx n) : l[idx - n]? = some p := by
  induction l generalizing n with
  | nil => cases h
  | cons a t ih =>
      rw [List.zipIdx_cons] at h
      simp only [List.mem_cons] at h
      rcases h with h | htail
      · have heq : (p, idx) = (a, n) := h
        cases heq
        simp
      · have ht := ih (n := n + 1) htail
        have hn : n + 1 ≤ idx := zipIdx_index_ge htail
        have hsub : idx - n = (idx - (n + 1)) + 1 := by omega
        rw [hsub]
        simp [ht]

private theorem mem_zipIdx_of_getElem {α : Type} {l : List α} {n idx : Nat} {p : α}
    (h : l[idx]? = some p) : (p, n + idx) ∈ l.zipIdx n := by
  induction l generalizing idx n with
  | nil => simp at h
  | cons a t ih =>
      cases idx with
      | zero =>
          simp at h
          subst p
          rw [List.zipIdx_cons]
          simp
      | succ idx =>
          have ht : t[idx]? = some p := by simpa using h
          have hz := ih (n := n + 1) (idx := idx) ht
          rw [List.zipIdx_cons]
          simpa [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] using hz

private theorem maskOfPreds_testBit_getElem {α : Type} (preds : List (α → Option Bool)) (a : α) (idx : Nat) :
    (maskOfPreds preds a).testBit idx = true ↔ ∃ p, preds[idx]? = some p ∧ p a = some true := by
  rw [maskOfPreds_testBit]
  constructor
  · rintro ⟨x, hxmem, hxidx, hxtrue⟩
    have hx := zipIdx_getElem (l := preds) (n := 0) (idx := x.2) (p := x.1) hxmem
    refine ⟨x.1, ?_, hxtrue⟩
    simpa [hxidx] using hx
  · rintro ⟨p, hpget, hptrue⟩
    refine ⟨(p, idx), ?_, rfl, hptrue⟩
    simpa using (mem_zipIdx_of_getElem (l := preds) (n := 0) (idx := idx) hpget)

private theorem and_ne_zero_iff (x y : Nat) :
    x &&& y != 0 ↔ ∃ i, x.testBit i = true ∧ y.testBit i = true := by
  constructor
  · intro h
    have hnz : x &&& y ≠ 0 := by simpa using h
    obtain ⟨i, hi⟩ := Nat.exists_testBit_of_ne_zero hnz
    have hi' : (x &&& y).testBit i = true := by simpa using hi
    rw [Nat.testBit_and] at hi'
    have hxi : x.testBit i = true := by
      cases hx : x.testBit i <;> cases hy : y.testBit i <;> simp [hx, hy] at hi' ⊢
    have hyi : y.testBit i = true := by
      cases hx : x.testBit i <;> cases hy : y.testBit i <;> simp [hx, hy] at hi' ⊢
    exact ⟨i, hxi, hyi⟩
  · rintro ⟨i, hi, hj⟩
    have hzero : (x &&& y).testBit i = true := by rw [Nat.testBit_and, hi, hj]; rfl
    have hprop : x &&& y ≠ 0 := by
      intro hz
      have : (x &&& y).testBit i = false := by simp [hz]
      simp [hzero] at this
    simpa using hprop

private theorem getElem_exists_iff_mem {α : Type} (l : List α) (P : α → Prop) :
    (∃ (idx : Nat), ∃ a, l[idx]? = some a ∧ P a) ↔ ∃ a ∈ l, P a := by
  constructor
  · rintro ⟨idx, a, ha, hP⟩
    rw [List.getElem?_eq_some_iff] at ha
    rcases ha with ⟨hlt, heq⟩
    exact ⟨a, by rw [← heq]; exact List.getElem_mem hlt, hP⟩
  · rintro ⟨a, ha, hP⟩
    rcases (List.mem_iff_getElem.mp ha) with ⟨i, hlt, heq⟩
    refine ⟨i, a, ?_, hP⟩
    exact (List.getElem?_eq_some_iff).2 ⟨hlt, heq⟩

private theorem branchMasks_and_ne_zero_iff {D F : Type}
    (branches : List ((D → Option Bool) × (F → Option Bool))) (d : D) (f : F) :
    maskOfPreds (branches.map Prod.fst) d &&& maskOfPreds (branches.map Prod.snd) f != 0 ↔
      ∃ (idx : Nat), ∃ b : (D → Option Bool) × (F → Option Bool), branches[idx]? = some b ∧ b.1 d = some true ∧ b.2 f = some true := by
  rw [and_ne_zero_iff]
  constructor
  · rintro ⟨idx, hmd, hmf⟩
    rw [maskOfPreds_testBit_getElem] at hmd hmf
    rcases hmd with ⟨pD, hDget, hDtrue⟩
    rcases hmf with ⟨pF, hFget, hFtrue⟩
    rw [List.getElem?_map] at hDget hFget
    cases hbidx : branches[idx]? with
    | none => simp [hbidx] at hDget
    | some b =>
        simp [hbidx] at hDget hFget
        subst pD
        subst pF
        exact ⟨idx, b, hbidx, hDtrue, hFtrue⟩
  · rintro ⟨idx, b, hbget, hbD, hbF⟩
    refine ⟨idx, ?_, ?_⟩
    · rw [maskOfPreds_testBit_getElem]
      refine ⟨b.1, ?_, hbD⟩
      rw [List.getElem?_map, hbget]
      rfl
    · rw [maskOfPreds_testBit_getElem]
      refine ⟨b.2, ?_, hbF⟩
      rw [List.getElem?_map, hbget]
      rfl

private theorem maskCondition_iff {D F : Type}
    (branches : List ((D → Option Bool) × (F → Option Bool))) (d : D) (f : F) :
    (maskOfPreds (branches.map Prod.fst) d &&& maskOfPreds (branches.map Prod.snd) f != 0) ↔
      isTrue3 (evalBranches branches d f) = true := by
  rw [branchMasks_and_ne_zero_iff]
  rw [getElem_exists_iff_mem]
  rw [evalBranches_true_iff]

private theorem maskCondition_eq {D F : Type}
    (branches : List ((D → Option Bool) × (F → Option Bool))) (d : D) (f : F) :
    (maskOfPreds (branches.map Prod.fst) d &&& maskOfPreds (branches.map Prod.snd) f != 0) =
      isTrue3 (evalBranches branches d f) := by
  exact Bool.eq_iff_iff.mpr (maskCondition_iff branches d f)

private theorem fsMasked_filterMap_eq {D F : Type} (fs : List F) (fPreds : List (F → Option Bool))
    (d : D) (md : Nat) :
    ((fs.map (fun f => (f, maskOfPreds fPreds f))).filter (fun p => p.2 != 0)).filterMap
        (fun (f, mf) => if md &&& mf != 0 then some (d, f) else none)
      = fs.filterMap (fun f => if md &&& maskOfPreds fPreds f != 0 then some (d, f) else none) := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
      by_cases hzero : maskOfPreds fPreds f = 0
      · have hfilt : List.filter (fun p => p.2 != 0)
            ((f, maskOfPreds fPreds f) :: fs.map (fun f => (f, maskOfPreds fPreds f))) =
            List.filter (fun p => p.2 != 0) (fs.map (fun f => (f, maskOfPreds fPreds f))) := by
          apply List.filter_cons_of_neg
          simp [hzero]
        rw [List.map_cons, hfilt, ih]
        simp [hzero]
      · have hfilt : List.filter (fun p => p.2 != 0)
            ((f, maskOfPreds fPreds f) :: fs.map (fun f => (f, maskOfPreds fPreds f))) =
            (f, maskOfPreds fPreds f) :: List.filter (fun p => p.2 != 0)
              (fs.map (fun f => (f, maskOfPreds fPreds f))) := by
          apply List.filter_cons_of_pos
          simp [hzero]
        rw [List.map_cons, hfilt, List.filterMap_cons, ih]
        rfl

private theorem optMaskedJoin_aux {D F : Type} (ds : List D) (fs : List F)
    (dPreds : List (D → Option Bool)) (fPreds : List (F → Option Bool)) :
    (((ds.map (fun d => (d, maskOfPreds dPreds d))).filter (fun p => p.2 != 0)).flatMap fun (d, md) =>
        ((fs.map (fun f => (f, maskOfPreds fPreds f))).filter (fun p => p.2 != 0)).filterMap
          (fun (f, mf) => if md &&& mf != 0 then some (d, f) else none))
    = ds.flatMap fun d =>
        fs.filterMap fun f =>
          if maskOfPreds dPreds d &&& maskOfPreds fPreds f != 0 then some (d, f) else none := by
  induction ds with
  | nil => rfl
  | cons d ds ih =>
      by_cases hzero : maskOfPreds dPreds d = 0
      · have hfilt : List.filter (fun p => p.2 != 0)
            ((d, maskOfPreds dPreds d) :: ds.map (fun d => (d, maskOfPreds dPreds d))) =
            List.filter (fun p => p.2 != 0) (ds.map (fun d => (d, maskOfPreds dPreds d))) := by
          apply List.filter_cons_of_neg
          simp [hzero]
        rw [List.map_cons, hfilt, ih, List.flatMap_cons]
        simp [hzero]
      · have hfilt : List.filter (fun p => p.2 != 0)
            ((d, maskOfPreds dPreds d) :: ds.map (fun d => (d, maskOfPreds dPreds d))) =
            (d, maskOfPreds dPreds d) :: List.filter (fun p => p.2 != 0)
              (ds.map (fun d => (d, maskOfPreds dPreds d))) := by
          apply List.filter_cons_of_pos
          simp [hzero]
        rw [List.map_cons, hfilt, List.flatMap_cons]
        dsimp
        rw [fsMasked_filterMap_eq, ih, List.flatMap_cons]

theorem sound : TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask.Goal := by
  intro D F ds fs branches
  unfold optMaskedJoin refJoin
  rw [optMaskedJoin_aux]
  apply congrArg (fun g => ds.flatMap g)
  funext d
  apply congrArg (fun g => fs.filterMap g)
  funext f
  rw [maskCondition_eq]

end TinylambOpt.ResearchProofs.CrossTableDisjunctiveBranchMask
