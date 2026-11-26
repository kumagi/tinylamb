import TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate

/-!
Proved

- `Goal` — Multi-block per-block local-dictionary two-stage aggregation
  (DictionaryJoinsAndLateLabels B1 / JOB J4 / LDBC L1, L5): accumulating into local
  code arrays without reading dictionary strings per row and decoding only occupied
  dictionary slots at global merge matches eager full-string decoding across blocks
  with distinct codebooks, duplicate dictionary entries, and NULL keys/values.

The reference model is defined in
`TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate`.
-/

namespace TinylambOpt.ResearchProofs.MultiBlockLocalDictTwoStageAggregate

open TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate

/-! Helper representation: a partial aggregate is a triple
    `(row count, non-null value count, sum of values)`.  The two-stage
    evaluator accumulates such triples in code-indexed slots, while the
    reference evaluator uses `Option Int` sums; `optOfAcc` bridges them. -/

private def addAcc (a b : Nat × Nat × Int) : Nat × Nat × Int :=
  (a.1 + b.1, a.2.1 + b.2.1, a.2.2 + b.2.2)

private def sumAcc : List (Nat × Nat × Int) → Nat × Nat × Int
  | [] => (0, 0, 0)
  | a :: l => addAcc a (sumAcc l)

private theorem addAcc_assoc (a b c : Nat × Nat × Int) :
    addAcc (addAcc a b) c = addAcc a (addAcc b c) := by
  ext <;> simp [addAcc] <;> omega

private theorem addAcc_comm (a b : Nat × Nat × Int) : addAcc a b = addAcc b a := by
  ext <;> simp [addAcc] <;> omega

private theorem addAcc_left_comm (a b c : Nat × Nat × Int) :
    addAcc a (addAcc b c) = addAcc b (addAcc a c) := by
  ext <;> simp [addAcc] <;> omega

private theorem addAcc_zero_left (a : Nat × Nat × Int) : addAcc (0, 0, 0) a = a := by
  ext <;> simp [addAcc]

private theorem addAcc_zero_right (a : Nat × Nat × Int) : addAcc a (0, 0, 0) = a := by
  ext <;> simp [addAcc]

private theorem foldl_addAcc (l : List (Nat × Nat × Int)) (st : Nat × Nat × Int) :
    l.foldl addAcc st = addAcc st (sumAcc l) := by
  induction l generalizing st with
  | nil => simp [sumAcc, addAcc_zero_right]
  | cons a l ih =>
      rw [List.foldl_cons, ih, sumAcc, addAcc_assoc]

private theorem sumAcc_append (l₁ l₂ : List (Nat × Nat × Int)) :
    sumAcc (l₁ ++ l₂) = addAcc (sumAcc l₁) (sumAcc l₂) := by
  induction l₁ with
  | nil => simp [sumAcc, addAcc_zero_left]
  | cons a l ih => simp [sumAcc, ih, addAcc_assoc]

private def mergeOpt (acc v : Option Int) : Option Int :=
  match acc, v with
  | none, y => y
  | x, none => x
  | some a, some b => some (a + b)

private def stepTriple : Option Int → Nat × Nat × Int
  | none => (1, 0, 0)
  | some x => (1, 1, x)

private def optOfAcc (t : Nat × Nat × Int) : Option Int :=
  if t.2.1 == 0 then none else some t.2.2

private def invAcc (t : Nat × Nat × Int) : Prop := t.2.1 = 0 → t.2.2 = 0

private def accTriple (a : LocalAcc) : Nat × Nat × Int := (a.cnt, a.nn, a.sum)

private def updSlot (ov : Option Int) (a : LocalAcc) : LocalAcc :=
  match ov with
  | none => { a with cnt := a.cnt + 1 }
  | some x => { cnt := a.cnt + 1, nn := a.nn + 1, sum := a.sum + x }

private def idxOf (L : Nat) (oc : Option Nat) : Nat :=
  match oc with
  | some c => c
  | none => L

private def decodeKey (b : DictBlock) (oc : Option Nat) : Option String :=
  oc.bind (fun c => b.dict[c]?)

private def matchIdx (b : DictBlock) (target : Option String) (i : Nat) : Bool :=
  match target with
  | none => i == b.dict.length
  | some s => if i < b.dict.length then b.dict[i]? == some s else false

private theorem invAcc_zero : invAcc (0, 0, 0) := by
  simp [invAcc]

private theorem invAcc_add_step (t : Nat × Nat × Int) (h : invAcc t) (v : Option Int) :
    invAcc (addAcc t (stepTriple v)) := by
  cases v with
  | none =>
      intro hz
      simp [stepTriple, addAcc] at hz
      simpa [stepTriple, addAcc] using h hz
  | some x =>
      intro hz
      simp [stepTriple, addAcc] at hz

private theorem mergeOpt_optOfAcc (t : Nat × Nat × Int) (h : invAcc t) (v : Option Int) :
    mergeOpt (optOfAcc t) v = optOfAcc (addAcc t (stepTriple v)) := by
  cases v with
  | none =>
      by_cases hz : t.2.1 == 0
      · have h0 : t.2.2 = 0 := h (by simpa using hz)
        simp [optOfAcc, addAcc, stepTriple, mergeOpt, hz, h0]
      · simp [optOfAcc, addAcc, stepTriple, mergeOpt, hz]
  | some x =>
      by_cases hz : t.2.1 == 0
      · have h0 : t.2.2 = 0 := h (by simpa using hz)
        simp [optOfAcc, addAcc, stepTriple, mergeOpt, hz, h0]
      · simp [optOfAcc, addAcc, stepTriple, mergeOpt, hz]

private theorem foldl_mergeOpt (vs : List (Option Int)) (t : Nat × Nat × Int) (h : invAcc t) :
    vs.foldl mergeOpt (optOfAcc t) = optOfAcc (addAcc t (sumAcc (vs.map stepTriple))) := by
  induction vs generalizing t with
  | nil => simp [sumAcc, addAcc_zero_right]
  | cons v vs ih =>
      rw [List.foldl_cons, mergeOpt_optOfAcc t h v]
      rw [ih (addAcc t (stepTriple v)) (invAcc_add_step t h v)]
      simp [sumAcc, addAcc_assoc]

private theorem sumOptList_eq_optOfAcc (vs : List (Option Int)) :
    sumOptList vs = optOfAcc (sumAcc (vs.map stepTriple)) := by
  change vs.foldl mergeOpt none = optOfAcc (sumAcc (vs.map stepTriple))
  rw [show (none : Option Int) = optOfAcc (0, 0, 0) by rfl]
  simpa [addAcc_zero_left] using foldl_mergeOpt vs (0, 0, 0) invAcc_zero

private theorem foldl_if_addAcc_eq_sum {α : Type} (rows : List α)
    (p : α → Bool) (w : α → Nat × Nat × Int) (st : Nat × Nat × Int) :
    rows.foldl (fun acc x => if p x then addAcc acc (w x) else acc) st =
      addAcc st (sumAcc ((rows.filter p).map w)) := by
  induction rows generalizing st with
  | nil => simp [sumAcc, addAcc_zero_right]
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih, sumAcc, addAcc_assoc]
      · have hf : p x = false := by cases hpx : p x <;> simp_all
        simp [hf, ih]

/-! ### Reading and updating local slots -/

private theorem length_incLocalSlot (slots : List LocalAcc) (idx : Nat) (ov : Option Int) :
    (incLocalSlot slots idx ov).length = slots.length := by
  simp [incLocalSlot]

private theorem getElem_incLocalSlot (slots : List LocalAcc) (idx i : Nat) (ov : Option Int) :
    (incLocalSlot slots idx ov)[i]? =
      Option.map (fun a => if i == idx then updSlot ov a else a) slots[i]? := by
  simp [incLocalSlot, updSlot, List.getElem?_map, List.getElem?_zipIdx, Function.comp_def]
  cases slots[i]? <;> rfl

private theorem getD_incLocalSlot (slots : List LocalAcc) (idx i : Nat) (ov : Option Int)
    (d : LocalAcc) (hidx : idx < slots.length) :
    (incLocalSlot slots idx ov).getD i d =
      if i == idx then updSlot ov (slots.getD i d) else slots.getD i d := by
  by_cases h : i = idx
  · subst i
    rw [List.getD_eq_getElem?_getD, getElem_incLocalSlot, List.getD_eq_getElem?_getD]
    rw [List.getElem?_eq_getElem hidx]
    simp
  · rw [List.getD_eq_getElem?_getD, getElem_incLocalSlot, List.getD_eq_getElem?_getD]
    simp [h]

private theorem accTriple_updSlot (ov : Option Int) (a : LocalAcc) :
    accTriple (updSlot ov a) = addAcc (accTriple a) (stepTriple ov) := by
  cases ov <;> simp [accTriple, updSlot, stepTriple, addAcc]

private theorem blockValid_idxOf (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length) :
    ∀ p ∈ b.rows, idxOf b.dict.length p.1 < b.dict.length + 1 := by
  intro p hp
  cases hp1 : p.1 with
  | none => simp [idxOf]
  | some c =>
      have hc := hvalid p hp
      simp [hp1] at hc
      simp [idxOf, hp1]
      omega

/-! ### Stage 1 computes per-code triples -/

private def accOfTriple (t : Nat × Nat × Int) : LocalAcc :=
  { cnt := t.1, nn := t.2.1, sum := t.2.2 }

private theorem accTriple_accOfTriple (t : Nat × Nat × Int) : accTriple (accOfTriple t) = t := by
  rfl

private theorem updSlot_accOfTriple (ov : Option Int) (t : Nat × Nat × Int) :
    updSlot ov (accOfTriple t) = accOfTriple (addAcc t (stepTriple ov)) := by
  cases ov <;> simp [updSlot, accOfTriple, addAcc, stepTriple] <;> omega

private theorem fold_rows_slot_correct (L : Nat) (rows : List (Option Nat × Option Int))
    (hvalid : ∀ p ∈ rows, idxOf L p.1 < L + 1)
    (f : Nat → Nat × Nat × Int) (slots : List LocalAcc)
    (hslots : slots.length = L + 1)
    (hcorrect : ∀ i, i < L + 1 → slots.getD i {} = accOfTriple (f i)) :
    ∀ i, i < L + 1 →
      (rows.foldl (fun sl p => incLocalSlot sl (idxOf L p.1) p.2) slots).getD i {} =
        accOfTriple (rows.foldl
          (fun t p => if (idxOf L p.1) == i then addAcc t (stepTriple p.2) else t) (f i)) := by
  revert slots f hvalid
  induction rows with
  | nil =>
      intro hvalid f slots hslots hcorrect i hi
      simpa using hcorrect i hi
  | cons p ps ih =>
      intro hvalid f slots hslots hcorrect i hi
      let idx := idxOf L p.1
      have hidx : idx < slots.length := by
        simpa [idx, hslots] using hvalid p (by simp)
      let f' : Nat → Nat × Nat × Int := fun j => if idx == j then addAcc (f j) (stepTriple p.2) else f j
      let slots' := incLocalSlot slots idx p.2
      have hslots' : slots'.length = L + 1 := by
        rw [length_incLocalSlot]
        exact hslots
      have hidxL : idx < L + 1 := by omega
      have hcidx : slots[idx]?.getD ({}) = accOfTriple (f idx) := by
        rw [← List.getD_eq_getElem?_getD]
        exact hcorrect idx hidxL
      have hcorrect' : ∀ j, j < L + 1 → slots'.getD j {} = accOfTriple (f' j) := by
        intro j hj
        have hg := getD_incLocalSlot slots idx j p.2 ({}) hidx
        by_cases hji : j = idx
        · subst j
          rw [show slots' = incLocalSlot slots idx p.2 by rfl, hg]
          simp [f', hcidx, updSlot_accOfTriple]
        · have hne : (j == idx) = false := by simp [hji]
          have hji' : idx ≠ j := fun h => hji h.symm
          have hne' : (idx == j) = false := by simp [hji']
          have hcj : slots[j]?.getD ({}) = accOfTriple (f j) := by
            rw [← List.getD_eq_getElem?_getD]
            exact hcorrect j hj
          rw [show slots' = incLocalSlot slots idx p.2 by rfl, hg]
          simp [f', hne, hne', hji', hcj]
      have hvalid' : ∀ q ∈ ps, idxOf L q.1 < L + 1 := by
        intro q hq
        exact hvalid q (by simp [hq])
      have hres := ih hvalid' f' slots' hslots' hcorrect' i hi
      simpa [slots', f', idx] using hres

private theorem stage1_correct (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (i : Nat) (hi : i < b.dict.length + 1) :
    (stage1BlockLocal b).getD i {} =
      accOfTriple (b.rows.foldl
        (fun t p => if (idxOf b.dict.length p.1) == i then addAcc t (stepTriple p.2) else t)
        (0, 0, 0)) := by
  unfold stage1BlockLocal
  exact fold_rows_slot_correct b.dict.length b.rows (blockValid_idxOf b hvalid)
    (fun _ => (0, 0, 0)) (List.replicate (b.dict.length + 1) {}) (by simp)
    (by intro j hj; simp [List.getElem?_replicate, hj]; rfl) i hi

private theorem cnt_sumAcc_stepTriple (vs : List (Option Int)) :
    (sumAcc (vs.map stepTriple)).1 = vs.length := by
  induction vs with
  | nil => simp [sumAcc]
  | cons v vs ih => cases v <;> simp [sumAcc, stepTriple, addAcc, ih] <;> omega

private theorem zero_cnt_sumAcc_stepTriple (vs : List (Option Int))
    (h : (sumAcc (vs.map stepTriple)).1 = 0) : sumAcc (vs.map stepTriple) = (0, 0, 0) := by
  induction vs with
  | nil => simp [sumAcc]
  | cons v vs ih =>
      cases v <;> simp [sumAcc, stepTriple, addAcc] at h ⊢ <;> omega

/-! ### Selecting slots and summing their triples -/

private def selectSum (p : Nat → Bool) (slots : List LocalAcc) : Nat × Nat × Int :=
  sumAcc ((List.range slots.length).map
    (fun i => if p i then accTriple (slots.getD i {}) else (0, 0, 0)))

private theorem sumAcc_range_congr (N : Nat) (f g : Nat → Nat × Nat × Int)
    (h : ∀ i, i < N → f i = g i) :
    sumAcc ((List.range N).map f) = sumAcc ((List.range N).map g) := by
  induction N with
  | zero => simp [sumAcc]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_append, sumAcc_append, sumAcc_append]
      simp only [List.map_cons, List.map_nil, sumAcc]
      rw [addAcc_zero_right, addAcc_zero_right]
      rw [ih (fun i hi => h i (by omega))]
      rw [h n (by omega)]

private theorem sumAcc_range_update (N idx : Nat) (hidx : idx < N)
    (f g : Nat → Nat × Nat × Int) (d : Nat × Nat × Int)
    (hne : ∀ i, i < N → i ≠ idx → f i = g i)
    (hat : f idx = addAcc (g idx) d) :
    sumAcc ((List.range N).map f) = addAcc (sumAcc ((List.range N).map g)) d := by
  revert idx
  induction N with
  | zero => intro idx hidx hne hat; omega
  | succ n ih =>
      intro idx hidx hne hat
      have hlt_or : idx < n ∨ idx = n := by omega
      rw [List.range_succ, List.map_append, List.map_append, sumAcc_append, sumAcc_append]
      simp only [List.map_cons, List.map_nil, sumAcc]
      rw [addAcc_zero_right, addAcc_zero_right]
      rcases hlt_or with hlt | heq
      · have hprefix := ih idx hlt
          (fun i hi hnei => hne i (by omega) hnei) hat
        rw [hprefix]
        have hn : f n = g n := hne n (by omega) (by omega)
        rw [hn]
        simp [addAcc_assoc, addAcc_comm, addAcc_left_comm]
      · have hne' : ∀ i, i < n → i ≠ n → f i = g i := by
          intro i hi hin
          exact hne i (by omega) (fun hix => hin (hix.trans heq))
        have hat' : f n = addAcc (g n) d := by
          rw [← heq]
          exact hat
        have hprefix := sumAcc_range_congr n f g (fun i hi => hne' i hi (by omega))
        rw [hprefix, hat']
        simp [addAcc_assoc]

private theorem selectSum_inc (slots : List LocalAcc) (idx : Nat) (ov : Option Int)
    (p : Nat → Bool) (hidx : idx < slots.length) :
    selectSum p (incLocalSlot slots idx ov) =
      addAcc (selectSum p slots) (if p idx then stepTriple ov else (0, 0, 0)) := by
  unfold selectSum
  rw [length_incLocalSlot]
  apply sumAcc_range_update slots.length idx hidx
  · intro i hi hne
    have hg0 := getD_incLocalSlot slots idx i ov ({}) hidx
    have hg : (incLocalSlot slots idx ov)[i]?.getD ({}) =
        if i == idx then updSlot ov (slots[i]?.getD ({})) else slots[i]?.getD ({}) := by
      simpa [List.getD_eq_getElem?_getD] using hg0
    by_cases hp : p i = true
    · simp [hp, hne, hg]
    · have hpf : p i = false := by cases hpi : p i <;> simp_all
      simp [hpf]
  · by_cases hp : p idx = true
    · have hg0 := getD_incLocalSlot slots idx idx ov ({}) hidx
      have hg : (incLocalSlot slots idx ov)[idx]?.getD ({}) =
          updSlot ov (slots[idx]?.getD ({})) := by
        simpa [List.getD_eq_getElem?_getD] using hg0
      simp [hp, hg, accTriple_updSlot]
    · have hpf : p idx = false := by cases hpi : p idx <;> simp_all
      simp [hpf, addAcc_zero_left]

private theorem selectSum_fold (L : Nat) (rows : List (Option Nat × Option Int))
    (slots : List LocalAcc) (p : Nat → Bool)
    (hvalid : ∀ q ∈ rows, idxOf L q.1 < L + 1) (hslots : slots.length = L + 1) :
    selectSum p (rows.foldl (fun sl q => incLocalSlot sl (idxOf L q.1) q.2) slots) =
      addAcc (selectSum p slots)
        (sumAcc ((rows.filter (fun q => p (idxOf L q.1))).map (fun q => stepTriple q.2))) := by
  induction rows generalizing slots with
  | nil =>
      simp [selectSum]
      rw [show sumAcc ([] : List (Nat × Nat × Int)) = (0, 0, 0) by rfl, addAcc_zero_right]
  | cons q qs ih =>
      have hidx : idxOf L q.1 < slots.length := by simpa [hslots] using hvalid q (by simp)
      have hvalid' : ∀ r ∈ qs, idxOf L r.1 < L + 1 := by
        intro r hr
        exact hvalid r (by simp [hr])
      have hlen : (incLocalSlot slots (idxOf L q.1) q.2).length = L + 1 := by
        simpa [length_incLocalSlot] using hslots
      rw [List.foldl_cons]
      rw [ih (incLocalSlot slots (idxOf L q.1) q.2) hvalid' hlen]
      rw [selectSum_inc slots (idxOf L q.1) q.2 p hidx]
      by_cases hp : p (idxOf L q.1) = true
      · simp [hp, sumAcc, addAcc_assoc]
      · have hpf : p (idxOf L q.1) = false := by cases h : p (idxOf L q.1) <;> simp_all
        simp [hpf, addAcc_assoc, addAcc_zero_right]

/-! ### Extracting a single slot and relating the predicates -/

private theorem sumAcc_range_zero (N : Nat) :
    sumAcc ((List.range N).map (fun _ => ((0, 0, 0) : Nat × Nat × Int))) = (0, 0, 0) := by
  induction N with
  | zero => simp [sumAcc]
  | succ n ih =>
      rw [List.range_succ, List.map_append, sumAcc_append, ih]
      simp [sumAcc, addAcc_zero_left, addAcc_zero_right]

private theorem sumAcc_range_single (N idx : Nat) (hidx : idx < N) (v : Nat × Nat × Int) :
    sumAcc ((List.range N).map (fun i => if i == idx then v else (0, 0, 0))) = v := by
  have h := sumAcc_range_update N idx hidx
    (fun i => if i == idx then v else (0, 0, 0))
    (fun _ => (0, 0, 0)) v
    (by intro i hi hnei; simp [hnei])
    (by simp [addAcc_zero_left])
  rw [h, sumAcc_range_zero, addAcc_zero_left]

private theorem selectSum_zero (L : Nat) (p : Nat → Bool) :
    selectSum p (List.replicate L ({} : LocalAcc)) = (0, 0, 0) := by
  unfold selectSum
  rw [List.length_replicate]
  rw [sumAcc_range_congr L
    (fun i => if p i then accTriple ((List.replicate L ({} : LocalAcc)).getD i {}) else (0, 0, 0))
    (fun _ => (0, 0, 0) : Nat → Nat × Nat × Int)
    (by
      intro i hi
      simp [List.getD_eq_getElem?_getD, List.getElem?_replicate, hi, accTriple])]
  exact sumAcc_range_zero L

private theorem selectSum_eq_single (slots : List LocalAcc) (idx : Nat) (hidx : idx < slots.length) :
    selectSum (fun i => i == idx) slots = accTriple (slots.getD idx {}) := by
  unfold selectSum
  rw [sumAcc_range_congr slots.length
    (fun i => if i == idx then accTriple (slots.getD i {}) else (0, 0, 0))
    (fun i => if i == idx then accTriple (slots.getD idx {}) else (0, 0, 0))
    (by intro i hi; by_cases h : i = idx <;> simp [h])]
  exact sumAcc_range_single slots.length idx hidx (accTriple (slots.getD idx {}))

private theorem fold_inc_length (L : Nat) (rows : List (Option Nat × Option Int)) (slots : List LocalAcc) :
    (rows.foldl (fun sl p => incLocalSlot sl (idxOf L p.1) p.2) slots).length = slots.length := by
  induction rows generalizing slots with
  | nil => rfl
  | cons p ps ih => rw [List.foldl_cons, ih, length_incLocalSlot]

private theorem stage1_length (b : DictBlock) :
    (stage1BlockLocal b).length = b.dict.length + 1 := by
  unfold stage1BlockLocal
  change (b.rows.foldl (fun sl p => incLocalSlot sl (idxOf b.dict.length p.1) p.2)
    (List.replicate (b.dict.length + 1) {})).length = b.dict.length + 1
  rw [fold_inc_length]
  simp

private theorem stage1_select (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (target : Option String) :
    selectSum (matchIdx b target) (stage1BlockLocal b) =
      b.rows.foldl
        (fun t p => if matchIdx b target (idxOf b.dict.length p.1) then addAcc t (stepTriple p.2) else t)
        (0, 0, 0) := by
  unfold stage1BlockLocal
  change selectSum (matchIdx b target)
    (b.rows.foldl (fun sl p => incLocalSlot sl (idxOf b.dict.length p.1) p.2)
      (List.replicate (b.dict.length + 1) {})) = _
  rw [selectSum_fold]
  · rw [selectSum_zero, addAcc_zero_left]
    rw [foldl_if_addAcc_eq_sum]
    simp [addAcc_zero_left]
  · exact blockValid_idxOf b hvalid
  · simp

private theorem matchIdx_iff_decodeKey (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (p : Option Nat × Option Int) (target : Option String) (hp : p ∈ b.rows) :
    matchIdx b target (idxOf b.dict.length p.1) = (decodeKey b p.1 == target) := by
  cases hp1 : p.1 with
  | none =>
      cases target <;> simp [matchIdx, decodeKey, idxOf, hp1]
  | some c =>
      have hc : c < b.dict.length := by
        have hh := hvalid p hp
        simpa [hp1] using hh
      have hne : c ≠ b.dict.length := by omega
      have hg : b.dict[c]? = some (b.dict[c]) := List.getElem?_eq_getElem hc
      cases target with
      | none => simp [matchIdx, decodeKey, idxOf, hp1, hc, hne]
      | some s => simp [matchIdx, decodeKey, idxOf, hp1, hc, hg]

private theorem foldl_triple_pred_congr {α : Type} (rows : List α)
    (p q : α → Bool) (w : α → Nat × Nat × Int)
    (h : ∀ x ∈ rows, p x = q x) (st : Nat × Nat × Int) :
    rows.foldl (fun acc x => if p x then addAcc acc (w x) else acc) st =
      rows.foldl (fun acc x => if q x then addAcc acc (w x) else acc) st := by
  induction rows generalizing st with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons, h x (by simp)]
      exact ih (fun y hy => h y (by simp [hy])) (if q x then addAcc st (w x) else st)

private theorem filter_map_eq {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p (f x) = true
      · simp [h, ih]
      · have hf : p (f x) = false := by cases h' : p (f x) <;> simp_all
        simp [hf, ih]

private def refHitsBlock (b : DictBlock) (target : Option String) : List (Option String × Option Int) :=
  (b.rows.map (fun p => (decodeKey b p.1, p.2))).filter (fun p => p.1 == target)

private def blockRowTriple (b : DictBlock) (target : Option String) : Nat × Nat × Int :=
  sumAcc ((refHitsBlock b target).map (fun p => stepTriple p.2))

private theorem refHitsBlock_values (b : DictBlock) (target : Option String) :
    (refHitsBlock b target).map (fun p => p.2) =
      (b.rows.filter (fun p => decodeKey b p.1 == target)).map (fun p => p.2) := by
  unfold refHitsBlock
  rw [filter_map_eq]
  simp [List.map_map, Function.comp_def]

private theorem refHitsBlock_step_values (b : DictBlock) (target : Option String) :
    (refHitsBlock b target).map (fun p => stepTriple p.2) =
      (b.rows.filter (fun p => decodeKey b p.1 == target)).map (fun p => stepTriple p.2) := by
  unfold refHitsBlock
  rw [filter_map_eq]
  simp [List.map_map, Function.comp_def]

private theorem rowFold_eq_blockRowTriple (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (target : Option String) :
    b.rows.foldl
        (fun t p => if matchIdx b target (idxOf b.dict.length p.1) then addAcc t (stepTriple p.2) else t)
        (0, 0, 0) = blockRowTriple b target := by
  rw [foldl_triple_pred_congr b.rows
      (fun p => matchIdx b target (idxOf b.dict.length p.1))
      (fun p => decodeKey b p.1 == target) (fun p => stepTriple p.2)
      (fun p hp => matchIdx_iff_decodeKey b hvalid p target hp) (0, 0, 0)]
  rw [foldl_if_addAcc_eq_sum]
  unfold blockRowTriple
  rw [refHitsBlock_step_values]
  simp [addAcc_zero_left]

private theorem zero_cnt_sumAcc (l : List (Nat × Nat × Int))
    (hpos : ∀ t ∈ l, 0 < t.1) (hz : (sumAcc l).1 = 0) : sumAcc l = (0, 0, 0) := by
  induction l with
  | nil => simp [sumAcc]
  | cons a l ih =>
      have ha : 0 < a.1 := hpos a (by simp)
      have hz' : a.1 + (sumAcc l).1 = 0 := by simpa [sumAcc, addAcc] using hz
      omega

private theorem zero_cnt_slot (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (i : Nat) (hi : i < b.dict.length + 1)
    (hz : ((stage1BlockLocal b).getD i {}).cnt = 0) :
    accTriple ((stage1BlockLocal b).getD i {}) = (0, 0, 0) := by
  rw [stage1_correct b hvalid i hi] at hz ⊢
  change (b.rows.foldl
      (fun t p => if idxOf b.dict.length p.1 == i then addAcc t (stepTriple p.2) else t)
      (0, 0, 0)).1 = 0 at hz
  change (b.rows.foldl
      (fun t p => if idxOf b.dict.length p.1 == i then addAcc t (stepTriple p.2) else t)
      (0, 0, 0)) = (0, 0, 0)
  rw [foldl_if_addAcc_eq_sum] at hz ⊢
  rw [addAcc_zero_left] at hz ⊢
  have hz' : (sumAcc
      ((b.rows.filter (fun p => idxOf b.dict.length p.1 == i)).map (fun p => stepTriple p.2))).1 = 0 := hz
  apply zero_cnt_sumAcc
  · intro t ht
    rcases List.mem_map.mp ht with ⟨p, hp, rfl⟩
    cases p.2 <;> simp [stepTriple]
  · exact hz'

private def nullTerm (b : DictBlock) (target : Option String) (slots : List LocalAcc) : Nat × Nat × Int :=
  if target.isNone then accTriple (slots.getD b.dict.length {}) else (0, 0, 0)

private def dictSelectSum (b : DictBlock) (target : Option String) (slots : List LocalAcc) : Nat × Nat × Int :=
  sumAcc (b.dict.zipIdx.map (fun x =>
    if 0 < (slots.getD x.2 {}).cnt && target == some x.1
    then accTriple (slots.getD x.2 {}) else (0, 0, 0)))

private def blockDelta (b : DictBlock) (target : Option String) : Nat × Nat × Int :=
  addAcc (nullTerm b target (stage1BlockLocal b))
    (dictSelectSum b target (stage1BlockLocal b))

/-! ### Per-block equality of the two-stage delta and the reference triple -/

private theorem sumAcc_map_zero {α : Type} (l : List α) :
    sumAcc (l.map (fun _ => ((0, 0, 0) : Nat × Nat × Int))) = (0, 0, 0) := by
  induction l with
  | nil => simp [sumAcc]
  | cons a l ih => simp [sumAcc, addAcc_zero_left, ih]

private theorem dictSelectSum_none_zero (b : DictBlock) (slots : List LocalAcc) :
    sumAcc (b.dict.zipIdx.map (fun x =>
      let ls := slots.getD x.2 {}
      if 0 < ls.cnt && none == some x.1 then accTriple ls else (0, 0, 0))) = (0, 0, 0) := by
  let l := b.dict.zipIdx
  change sumAcc (l.map (fun x =>
      let ls := slots.getD x.2 {}
      if 0 < ls.cnt && none == some x.1 then accTriple ls else (0, 0, 0))) = (0, 0, 0)
  induction l with
  | nil => simp [sumAcc]
  | cons x xs ih =>
      simp [sumAcc]
      rw [addAcc_zero_left]
      exact sumAcc_map_zero xs

private theorem blockDelta_none (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length) :
    blockDelta b none = blockRowTriple b none := by
  unfold blockDelta nullTerm dictSelectSum
  rw [dictSelectSum_none_zero, addAcc_zero_right]
  simp only [Option.isNone_none, ite_true]
  have hlen : b.dict.length < (stage1BlockLocal b).length := by
    rw [stage1_length]
    omega
  rw [← selectSum_eq_single (stage1BlockLocal b) b.dict.length hlen]
  have hm : matchIdx b none = fun i => i == b.dict.length := by funext i; rfl
  rw [← hm]
  rw [stage1_select b hvalid none]
  exact rowFold_eq_blockRowTriple b hvalid none

private theorem dict_zipIdx_eq_map_range (d : List String) :
    d.zipIdx = (List.range d.length).map (fun i => (d.getD i "", i)) := by
  apply List.ext_getElem?
  intro j
  by_cases hj : j < d.length
  · rw [List.getElem?_zipIdx, List.getElem?_map, List.getElem?_eq_getElem hj, List.getElem?_range hj]
    simp [List.getD_eq_getElem?_getD, hj]
  · rw [List.getElem?_zipIdx, List.getElem?_map]
    have hd : d[j]? = none := List.getElem?_eq_none (by omega)
    have hr : (List.range d.length)[j]? = none := List.getElem?_eq_none (by simp; omega)
    rw [hd, hr]
    simp

private theorem sumAcc_range_succ_zero_last (N : Nat) (f : Nat → Nat × Nat × Int)
    (hlast : f N = (0, 0, 0)) :
    sumAcc ((List.range (N + 1)).map f) = sumAcc ((List.range N).map f) := by
  rw [List.range_succ, List.map_append, sumAcc_append]
  simp [sumAcc, hlast, addAcc_zero_right]

private theorem blockDelta_some (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (s : String) :
    blockDelta b (some s) = blockRowTriple b (some s) := by
  unfold blockDelta nullTerm dictSelectSum
  simp only [Option.isNone_some, ite_false, addAcc_zero_left]
  let slots := stage1BlockLocal b
  let L := b.dict.length
  let g : Nat → Nat × Nat × Int := fun code =>
    if 0 < (slots.getD code {}).cnt && b.dict[code]? == some s then accTriple (slots.getD code {}) else (0, 0, 0)
  let f : Nat → Nat × Nat × Int := fun code =>
    if matchIdx b (some s) code then accTriple (slots.getD code {}) else (0, 0, 0)
  have hdict : sumAcc (b.dict.zipIdx.map (fun x =>
      let ls := slots.getD x.2 {}
      if 0 < ls.cnt && some s == some x.1 then accTriple ls else (0, 0, 0))) =
      sumAcc ((List.range L).map g) := by
    rw [dict_zipIdx_eq_map_range]
    simp only [List.map_map]
    apply sumAcc_range_congr
    intro i hi
    simp only [g]
    have hcodeL : b.dict[i]? = some (b.dict[i]) := List.getElem?_eq_getElem hi
    have hgetD : b.dict.getD i "" = b.dict[i] := by
      simp [List.getD_eq_getElem?_getD, hi]
    change (if 0 < (slots.getD i {}).cnt && some s == some (b.dict.getD i "")
              then accTriple (slots.getD i {}) else (0, 0, 0)) =
           (if 0 < (slots.getD i {}).cnt && b.dict[i]? == some s
              then accTriple (slots.getD i {}) else (0, 0, 0))
    rw [hgetD, hcodeL]
    by_cases hcnt0 : 0 < (slots.getD i {}).cnt
    · by_cases heq : s = b.dict[i]
      · subst s
        simp [hcnt0]
      · have heq' : b.dict[i] ≠ s := fun h => heq h.symm
        simp [hcnt0, heq, heq']
    · by_cases heq : s = b.dict[i]
      · subst s
        simp [hcnt0]
      · have heq' : b.dict[i] ≠ s := fun h => heq h.symm
        simp [hcnt0, heq, heq']
  rw [hdict]
  have hcongr : sumAcc ((List.range L).map g) = sumAcc ((List.range L).map f) := by
    apply sumAcc_range_congr
    intro code hcodeL
    change (if 0 < (slots.getD code {}).cnt && b.dict[code]? == some s
              then accTriple (slots.getD code {}) else (0, 0, 0)) =
           (if matchIdx b (some s) code
              then accTriple (slots.getD code {}) else (0, 0, 0))
    have hcodeL' : code < b.dict.length := by simpa [L] using hcodeL
    by_cases hcnt : 0 < (slots.getD code {}).cnt
    · have hcnt' : 0 < (Option.getD (slots[code]?) ({} : LocalAcc)).cnt := by
        simpa [List.getD_eq_getElem?_getD] using hcnt
      by_cases hsEq : b.dict[code]? = some s
      · have hsBool : (b.dict[code]? == some s) = true := (beq_iff_eq).mpr hsEq
        simp [hcnt', hsBool, matchIdx, hcodeL', hsEq]
      · have hsBool : (b.dict[code]? == some s) = false := by
          cases hb : b.dict[code]? == some s
          · rfl
          · exact absurd ((beq_iff_eq).mp hb) hsEq
        simp [hcnt', hsBool, matchIdx, hcodeL', hsEq]
    · have hcnt0 : (slots.getD code {}).cnt = 0 := by omega
      have hz := zero_cnt_slot b hvalid code (by omega) hcnt0
      have hcnt' : ¬ 0 < (Option.getD (slots[code]?) ({} : LocalAcc)).cnt := by
        simpa [List.getD_eq_getElem?_getD] using hcnt
      have hz' : accTriple (Option.getD (slots[code]?) ({} : LocalAcc)) = (0, 0, 0) := by
        simpa [slots, List.getD_eq_getElem?_getD] using hz
      simp [hcnt', hz', matchIdx, hcodeL']
  rw [hcongr]
  have hslotslen : slots.length = L + 1 := by
    change (stage1BlockLocal b).length = b.dict.length + 1
    exact stage1_length b
  have hselect : sumAcc ((List.range L).map f) = selectSum (matchIdx b (some s)) slots := by
    unfold selectSum
    rw [hslotslen]
    symm
    apply sumAcc_range_succ_zero_last
    simp [f, matchIdx, L]
  rw [hselect]
  rw [show slots = stage1BlockLocal b by rfl]
  rw [stage1_select b hvalid (some s)]
  simpa [addAcc_zero_left] using (rowFold_eq_blockRowTriple b hvalid (some s))

private theorem blockDelta_eq_blockRowTriple (b : DictBlock)
    (hvalid : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length)
    (target : Option String) : blockDelta b target = blockRowTriple b target := by
  cases target with
  | none => exact blockDelta_none b hvalid
  | some s => exact blockDelta_some b hvalid s

/-! ### Aggregating over all blocks -/

private def sumNat : List Nat → Nat
  | [] => 0
  | a :: l => a + sumNat l

private theorem list_sum_eq_sumNat (l : List Nat) : l.sum = sumNat l := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [sumNat, List.sum_cons, ih]

private theorem sumAcc_fst (l : List (Nat × Nat × Int)) :
    (sumAcc l).1 = sumNat (l.map (fun t => t.1)) := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [sumAcc, sumNat, addAcc, ih] <;> omega

private theorem cnt_sumAcc_map {α : Type} (l : List α) (w : α → Nat × Nat × Int)
    (hw : ∀ x, (w x).1 = 1) : (sumAcc (l.map w)).1 = l.length := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [sumAcc, addAcc]
      rw [hw a, ih]
      omega

private theorem cnt_blockRowTriple (b : DictBlock) (target : Option String) :
    (blockRowTriple b target).1 = (refHitsBlock b target).length := by
  unfold blockRowTriple
  apply cnt_sumAcc_map
  intro p
  cases p.2 <;> rfl

private theorem hits_eq_flatMap (blocks : List DictBlock) (target : Option String) :
    (blocks.flatMap (fun b => b.rows.map (fun p => (decodeKey b p.1, p.2)))).filter (fun p => p.1 == target) =
      blocks.flatMap (fun b => refHitsBlock b target) := by
  rw [List.filter_flatMap]
  simp [refHitsBlock]

private theorem hits_length_eq (blocks : List DictBlock) (target : Option String) :
    (blocks.flatMap (fun b => refHitsBlock b target)).length =
      (sumAcc (blocks.map (fun b => blockRowTriple b target))).1 := by
  rw [List.length_flatMap, list_sum_eq_sumNat, sumAcc_fst]
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp [sumNat, cnt_blockRowTriple, ih]

private theorem hits_sumAcc_eq (blocks : List DictBlock) (target : Option String) :
    sumAcc ((blocks.flatMap (fun b => refHitsBlock b target)).map (fun p => stepTriple p.2)) =
      sumAcc (blocks.map (fun b => blockRowTriple b target)) := by
  induction blocks with
  | nil => simp [sumAcc]
  | cons b bs ih =>
      rw [List.flatMap_cons, List.map_append, sumAcc_append, ih]
      rfl

private theorem ref_sumOpt_eq (blocks : List DictBlock) (target : Option String) :
    sumOptList ((blocks.flatMap (fun b => refHitsBlock b target)).map Prod.snd) =
      optOfAcc (sumAcc (blocks.map (fun b => blockRowTriple b target))) := by
  rw [sumOptList_eq_optOfAcc]
  change optOfAcc (sumAcc (((blocks.flatMap (fun b => refHitsBlock b target)).map Prod.snd).map stepTriple)) = _
  rw [List.map_map]
  change optOfAcc (sumAcc ((blocks.flatMap (fun b => refHitsBlock b target)).map
    (fun p => stepTriple p.2))) = _
  rw [hits_sumAcc_eq]

private theorem refGlobalAgg_eq (blocks : List DictBlock) (target : Option String) :
    refGlobalAgg blocks target =
      let T := sumAcc (blocks.map (fun b => blockRowTriple b target))
      (T.1, optOfAcc T) := by
  unfold refGlobalAgg
  let hits := (blocks.flatMap (fun b => b.rows.map (fun p => (decodeKey b p.1, p.2)))).filter
    (fun p => p.1 == target)
  change (hits.length, sumOptList (hits.map Prod.snd)) = _
  rw [show hits = blocks.flatMap (fun b => refHitsBlock b target) by
    exact hits_eq_flatMap blocks target]
  rw [hits_length_eq, ref_sumOpt_eq]

private def optStep (st : Nat × Nat × Int) (b : DictBlock) (target : Option String) : Nat × Nat × Int :=
  let slots := stage1BlockLocal b
  let nullSlot := slots.getD b.dict.length {}
  let st' := if target.isNone then
    (st.1 + nullSlot.cnt, st.2.1 + nullSlot.nn, st.2.2 + nullSlot.sum) else st
  b.dict.zipIdx.foldl (fun acc x =>
    let ls := slots.getD x.2 {}
    if 0 < ls.cnt && target == some x.1 then
      (acc.1 + ls.cnt, acc.2.1 + ls.nn, acc.2.2 + ls.sum) else acc) st'

private theorem sumAcc_filter_map_eq (l : List (String × Nat))
    (p : String × Nat → Bool) (f : String × Nat → Nat × Nat × Int) :
    sumAcc ((l.filter p).map f) = sumAcc (l.map (fun x => if p x then f x else (0, 0, 0))) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x = true
      · rw [List.filter_cons_of_pos h, List.map_cons, List.map_cons, ite_eq_left h, sumAcc, sumAcc, ih]
      · rw [List.filter_cons_of_neg h, List.map_cons, ite_eq_right h, sumAcc, addAcc_zero_left]
        exact ih

private theorem dict_fold_eq (slots : List LocalAcc) (b : DictBlock)
    (target : Option String) (st : Nat × Nat × Int) :
    b.dict.zipIdx.foldl (fun acc x =>
      let ls := slots.getD x.2 {}
      if 0 < ls.cnt && target == some x.1 then addAcc acc (accTriple ls) else acc) st =
      addAcc st (dictSelectSum b target slots) := by
  unfold dictSelectSum
  have h := foldl_if_addAcc_eq_sum b.dict.zipIdx
    (fun x => 0 < (slots.getD x.2 {}).cnt && target == some x.1)
    (fun x => accTriple (slots.getD x.2 {})) st
  rw [h]
  exact congrArg (addAcc st) (sumAcc_filter_map_eq b.dict.zipIdx
    (fun x => 0 < (slots.getD x.2 {}).cnt && target == some x.1)
    (fun x => accTriple (slots.getD x.2 {})))

private theorem optStep_add (st : Nat × Nat × Int) (b : DictBlock) (target : Option String) :
    optStep st b target = addAcc st (blockDelta b target) := by
  unfold optStep blockDelta nullTerm
  change (let slots := stage1BlockLocal b
          let st' := if target.isNone then addAcc st (accTriple (slots.getD b.dict.length {})) else st
          b.dict.zipIdx.foldl (fun acc x =>
            let ls := slots.getD x.2 {}
            if 0 < ls.cnt && target == some x.1 then addAcc acc (accTriple ls) else acc) st') =
      addAcc st (addAcc
        (if target.isNone then accTriple ((stage1BlockLocal b).getD b.dict.length {}) else (0, 0, 0))
        (dictSelectSum b target (stage1BlockLocal b)))
  rw [dict_fold_eq]
  by_cases h : target.isNone = true
  · simp [h, addAcc_assoc]
  · have hf : target.isNone = false := by cases hn : target.isNone <;> simp_all
    simp [hf, addAcc_zero_left]

private theorem foldl_congr {α β : Type} (f g : α → β → α) (l : List β) (st : α)
    (h : ∀ x ∈ l, ∀ acc, f acc x = g acc x) : l.foldl f st = l.foldl g st := by
  induction l generalizing st with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, h x (by simp) st]
      exact ih (g st x) (fun y hy acc => h y (by simp [hy]) acc)

private theorem goalStep_add (st : Nat × Nat × Int) (b : DictBlock) (target : Option String) :
    (let localSlots := stage1BlockLocal b
     let nullSlot := localSlots.getD b.dict.length {}
     let st' := if target.isNone then
       (st.1 + nullSlot.cnt, st.2.1 + nullSlot.nn, st.2.2 + nullSlot.sum) else st
     b.dict.zipIdx.foldl (fun acc x =>
       match x with
       | (s, code) =>
         let ls := localSlots.getD code {}
         if 0 < ls.cnt && target == some s then
           (acc.1 + ls.cnt, acc.2.1 + ls.nn, acc.2.2 + ls.sum) else acc) st') =
      addAcc st (blockDelta b target) := by
  change optStep st b target = addAcc st (blockDelta b target)
  exact optStep_add st b target

private theorem opt_eq_sumAcc (blocks : List DictBlock) (target : Option String) :
    optTwoStageAgg blocks target =
      let T := sumAcc (blocks.map (fun b => blockDelta b target))
      (T.1, optOfAcc T) := by
  unfold optTwoStageAgg
  let step : Nat × Nat × Int → DictBlock → Nat × Nat × Int := fun st b =>
    let localSlots := stage1BlockLocal b
    let nullSlot := localSlots.getD b.dict.length {}
    let st' := if target.isNone then
      (st.1 + nullSlot.cnt, st.2.1 + nullSlot.nn, st.2.2 + nullSlot.sum) else st
    b.dict.zipIdx.foldl (fun acc x =>
      match x with
      | (s, code) =>
        let ls := localSlots.getD code {}
        if 0 < ls.cnt && target == some s then
          (acc.1 + ls.cnt, acc.2.1 + ls.nn, acc.2.2 + ls.sum) else acc) st'
  change (let combined := blocks.foldl step (0, 0, 0)
    (combined.1, if combined.2.1 == 0 then none else some combined.2.2)) = _
  dsimp only
  have hfold : blocks.foldl step (0, 0, 0) =
      sumAcc (blocks.map (fun b => blockDelta b target)) := by
    rw [← addAcc_zero_left (sumAcc (blocks.map (fun b => blockDelta b target)))]
    rw [← foldl_addAcc]
    rw [List.foldl_map]
    apply foldl_congr
    intro b hb st
    exact goalStep_add st b target
  rw [hfold]
  rfl

private theorem blockDeltas_eq (blocks : List DictBlock) (target : Option String) (hvalid : BlocksValid blocks) :
    blocks.map (fun b => blockDelta b target) = blocks.map (fun b => blockRowTriple b target) := by
  induction blocks with
  | nil => rfl
  | cons b bs ih =>
      have hb : ∀ p ∈ b.rows, match p.1 with | none => True | some c => c < b.dict.length := by
        intro p hp
        exact hvalid b (by simp) p hp
      have hbs : BlocksValid bs := by
        intro x hx p hp
        exact hvalid x (by simp [hx]) p hp
      simp [blockDelta_eq_blockRowTriple b hb target, ih hbs]

theorem sound : TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate.Goal := by
  intro blocks hvalid target
  rw [refGlobalAgg_eq, opt_eq_sumAcc]
  rw [blockDeltas_eq blocks target hvalid]

end TinylambOpt.ResearchProofs.MultiBlockLocalDictTwoStageAggregate

-- C++ references to this model or its proof.
-- executor/detail/typed_stream_aggregate_partitioned.cpp
-- executor/detail/typed_stream_aggregate_partitioned.hpp
