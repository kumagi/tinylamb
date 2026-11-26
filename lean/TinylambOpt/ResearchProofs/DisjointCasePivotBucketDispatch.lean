import TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch

/-!
Proved

- `Goal` — Disjoint CASE-pivot multi-bucket dispatch (TPC-DS Q02/Q21/Q59/Q62/Q66/Q90/
  Q99): classifying each row into at most one mutually exclusive bucket `0..K-1` and
  updating only that slot matches `K` independent conditional `SUM` passes under both
  `ELSE 0` and `ELSE NULL` (including empty inputs and rows where a bucket matches
  `val = NULL`), while updating `≤ 1` slot per row.

The reference model is defined in
`TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch`.
-/

namespace TinylambOpt.ResearchProofs.DisjointCasePivotBucketDispatch

open TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch

private def bumpSlot (v : Option Int) (s : SlotAcc) : SlotAcc :=
  match v with
  | none => { s with bucketHits := s.bucketHits + 1 }
  | some x =>
      { bucketHits := s.bucketHits + 1
        nonNullHits := s.nonNullHits + 1
        sumVal := s.sumVal + x }

private theorem updateOneSlot_length (slots : List SlotAcc) (j : Nat) (v : Option Int) :
    (updateOneSlot slots j v).1.length = slots.length := by
  simp [updateOneSlot]

private theorem updateOneSlot_getElem? (slots : List SlotAcc) (j idx : Nat) (v : Option Int) :
    (updateOneSlot slots j v).1[idx]? =
      match slots[idx]? with
      | none => none
      | some s => if idx == j then some (bumpSlot v s) else some s := by
  rw [updateOneSlot]
  simp only [List.getElem?_map, List.getElem?_zipIdx, Option.map_map, Nat.zero_add]
  cases slots[idx]? with
  | none => simp
  | some s =>
      by_cases h : idx = j
      · subst j
        simp [bumpSlot]
        cases v <;> rfl
      · simp [h]

private theorem updateOneSlot_touched_le_one (slots : List SlotAcc) (j : Nat) (v : Option Int) :
    (updateOneSlot slots j v).2 ≤ 1 := by
  simp [updateOneSlot]
  split <;> omega

private def refTerm {α : Type} (mode : ElseMode) (bucket : α → Option Nat)
    (val : α → Option Int) (j : Nat) (r : α) : Option Int :=
  if bucket r == some j then val r
  else match mode with
       | .elseZero => some 0
       | .elseNull => none

private def refBucketSumFrom {α : Type} (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) (j : Nat)
    (acc : Option Int) : Option Int :=
  rows.foldl (fun acc r => combineOptInt acc (refTerm mode bucket val j r)) acc

private def accSlotFrom {α : Type} (bucket : α → Option Nat) (val : α → Option Int)
    (j : Nat) (s : SlotAcc) (rows : List α) : SlotAcc :=
  rows.foldl (fun s r => if bucket r == some j then bumpSlot (val r) s else s) s

private def optStep {α : Type} (bucket : α → Option Nat) (val : α → Option Int)
    (st : DispatchState) (r : α) : DispatchState :=
  match bucket r with
  | none => { st with totalRows := st.totalRows + 1 }
  | some j =>
      let (slots', touched) := updateOneSlot st.slots j (val r)
      { totalRows := st.totalRows + 1
        slots := slots'
        maxTouchesPerRow := max st.maxTouchesPerRow touched }

private def initState (k : Nat) : DispatchState :=
  { totalRows := 0
    slots := List.replicate k {}
    maxTouchesPerRow := 0 }

private theorem optPivotSums_eq {α : Type} (k : Nat) (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) :
    optPivotSums k rows mode bucket val =
      (let st := rows.foldl (optStep bucket val) (initState k)
       (st.slots.map (finalizeSlot mode st.totalRows), st.maxTouchesPerRow)) := by
  rfl

private theorem combine_none (a : Option Int) : combineOptInt a none = a := by
  cases a <;> rfl

private theorem combine_some (a : Option Int) (x : Int) :
    combineOptInt a (some x) = some (x + a.getD 0) := by
  cases a <;> simp [combineOptInt] <;> omega

private theorem finalizeZero_getD (s : SlotAcc) (n : Nat) :
    (finalizeSlot .elseZero n s).getD 0 = if 0 < s.nonNullHits then s.sumVal else 0 := by
  by_cases h : 0 < s.nonNullHits
  · simp [finalizeSlot, h]
  · by_cases hlt : s.bucketHits < n <;> simp [finalizeSlot, h, hlt]

private theorem finalizeNull_getD (s : SlotAcc) (n : Nat) :
    (finalizeSlot .elseNull n s).getD 0 = if s.nonNullHits = 0 then 0 else s.sumVal := by
  by_cases h : s.nonNullHits = 0 <;> simp [finalizeSlot, h]

private theorem combine_finalize {α : Type} (mode : ElseMode) (bucket : α → Option Nat)
    (val : α → Option Int) (j : Nat) (r : α) (s : SlotAcc) (n : Nat)
    (hn : s.bucketHits ≤ n) (hsum : s.nonNullHits = 0 → s.sumVal = 0) :
    combineOptInt (finalizeSlot mode n s) (refTerm mode bucket val j r) =
      finalizeSlot mode (n + 1)
        (if bucket r == some j then bumpSlot (val r) s else s) := by
  cases mode
  · by_cases h : bucket r = some j
    · cases hv : val r with
      | none =>
          rw [show refTerm .elseZero bucket val j r = none by simp [refTerm, h, hv]]
          rw [combine_none]
          simp [bumpSlot, finalizeSlot, h]
      | some x =>
          rw [show refTerm .elseZero bucket val j r = some x by simp [refTerm, h, hv]]
          rw [combine_some, finalizeZero_getD]
          simp [bumpSlot, finalizeSlot, h]
          by_cases hnn : 0 < s.nonNullHits
          · simp [hnn]
            omega
          · simp [hnn]
            have hs0 : s.sumVal = 0 := hsum (by omega)
            simp [hs0]
    · rw [show refTerm .elseZero bucket val j r = some 0 by simp [refTerm, h]]
      rw [combine_some, finalizeZero_getD]
      simp [finalizeSlot, h]
      by_cases hnn : 0 < s.nonNullHits
      · simp [hnn]
      · simp [hnn]
        have hlt : s.bucketHits < n + 1 := by omega
        simp [hlt]
  · by_cases h : bucket r = some j
    · cases hv : val r with
      | none =>
          rw [show refTerm .elseNull bucket val j r = none by simp [refTerm, h, hv]]
          rw [combine_none]
          simp [bumpSlot, finalizeSlot, h]
      | some x =>
          rw [show refTerm .elseNull bucket val j r = some x by simp [refTerm, h, hv]]
          rw [combine_some, finalizeNull_getD]
          simp [bumpSlot, finalizeSlot, h]
          by_cases hz : s.nonNullHits = 0
          · simp [hz]
            have hs0 : s.sumVal = 0 := hsum hz
            simp [hs0]
          · simp [hz]
            omega
    · rw [show refTerm .elseNull bucket val j r = none by simp [refTerm, h]]
      rw [combine_none]
      simp [finalizeSlot, h]

private theorem ref_fold_from_slot {α : Type} (rows : List α) (mode : ElseMode)
    (bucket : α → Option Nat) (val : α → Option Int) (j : Nat)
    (s : SlotAcc) (n : Nat) (hn : s.bucketHits ≤ n)
    (hsum : s.nonNullHits = 0 → s.sumVal = 0) :
    refBucketSumFrom rows mode bucket val j (finalizeSlot mode n s) =
      finalizeSlot mode (n + rows.length) (accSlotFrom bucket val j s rows) := by
  induction rows generalizing s n with
  | nil => simp [refBucketSumFrom, accSlotFrom]
  | cons r rs ih =>
      let s1 := if bucket r == some j then bumpSlot (val r) s else s
      have hn1 : s1.bucketHits ≤ n + 1 := by
        by_cases h : bucket r = some j
        · cases hv : val r <;> simp [s1, h, hv, bumpSlot] <;> omega
        · simp [s1, h]
          omega
      have hsum1 : s1.nonNullHits = 0 → s1.sumVal = 0 := by
        by_cases h : bucket r = some j
        · cases hv : val r with
          | none => simpa [s1, h, hv, bumpSlot] using hsum
          | some x => simp [s1, h, hv, bumpSlot]
        · simpa [s1, h] using hsum
      change refBucketSumFrom rs mode bucket val j
          (combineOptInt (finalizeSlot mode n s) (refTerm mode bucket val j r)) =
        finalizeSlot mode (n + (r :: rs).length)
          (accSlotFrom bucket val j s1 rs)
      rw [combine_finalize mode bucket val j r s n hn hsum]
      rw [show n + (r :: rs).length = (n + 1) + rs.length by simp; omega]
      rw [ih s1 (n + 1) hn1 hsum1]

private theorem opt_fold_invariants {α : Type} (k : Nat) (bucket : α → Option Nat)
    (val : α → Option Int) (rows : List α) (st0 : DispatchState)
    (hlen0 : st0.slots.length = k) (htouch0 : st0.maxTouchesPerRow ≤ 1) :
    let st := rows.foldl (optStep bucket val) st0
    st.totalRows = st0.totalRows + rows.length ∧
    st.slots.length = k ∧
    (∀ j < k, st.slots[j]? = some (accSlotFrom bucket val j (st0.slots.getD j {}) rows)) ∧
    st.maxTouchesPerRow ≤ 1 := by
  induction rows generalizing st0 with
  | nil =>
      refine ⟨?_, hlen0, ?_, htouch0⟩
      · simp
      · intro j hj
        change st0.slots[j]? = some (accSlotFrom bucket val j (st0.slots.getD j {}) [])
        have hjlen : j < st0.slots.length := by omega
        have hsjget : st0.slots.getD j {} = st0.slots[j] := by
          rw [show st0.slots.getD j {} = (st0.slots[j]?).getD {} by rfl]
          rw [List.getElem?_eq_getElem hjlen]
          simp
        rw [List.getElem?_eq_getElem hjlen]
        rw [show accSlotFrom bucket val j (st0.slots.getD j {}) [] = st0.slots.getD j {} by rfl]
        rw [hsjget]
  | cons r rs ih =>
      simp only [List.foldl_cons]
      let st1 := optStep bucket val st0 r
      have hlen1 : st1.slots.length = k := by
        cases hb : bucket r with
        | none => simpa [st1, optStep, hb] using hlen0
        | some j => simpa [st1, optStep, hb, updateOneSlot_length] using hlen0
      have htotal1 : st1.totalRows = st0.totalRows + 1 := by
        cases hb : bucket r <;> simp [st1, optStep, hb]
      have hslots1 : ∀ j < k, st1.slots[j]? =
          some (accSlotFrom bucket val j (st0.slots.getD j {}) [r]) := by
        intro j hj
        have hjlen : j < st0.slots.length := by omega
        cases hget : st0.slots[j]? with
        | none =>
            have hsome' := List.getElem?_eq_getElem hjlen
            rw [hget] at hsome'
            cases hsome'
        | some s =>
            have hs_eq : st0.slots.getD j {} = s := by
              rw [show st0.slots.getD j {} = (st0.slots[j]?).getD {} by rfl, hget]
              simp
            rw [hs_eq]
            cases hb : bucket r with
            | none =>
                change (optStep bucket val st0 r).slots[j]? =
                  some (accSlotFrom bucket val j s [r])
                simp [optStep, hb, hget, accSlotFrom]
            | some b =>
                by_cases hbj : b = j
                · subst b
                  change (optStep bucket val st0 r).slots[j]? =
                    some (accSlotFrom bucket val j s [r])
                  rw [show (optStep bucket val st0 r).slots =
                      (updateOneSlot st0.slots j (val r)).1 by simp [optStep, hb]]
                  rw [updateOneSlot_getElem?, hget]
                  simp [accSlotFrom, hb, bumpSlot]
                · have hjb : j ≠ b := by omega
                  change (optStep bucket val st0 r).slots[j]? =
                    some (accSlotFrom bucket val j s [r])
                  rw [show (optStep bucket val st0 r).slots =
                      (updateOneSlot st0.slots b (val r)).1 by simp [optStep, hb]]
                  rw [updateOneSlot_getElem?, hget]
                  simp [accSlotFrom, hb, hbj, hjb]
      have htouch1 : st1.maxTouchesPerRow ≤ 1 := by
        cases hb : bucket r with
        | none => simpa [st1, optStep, hb] using htouch0
        | some j =>
            have ht := updateOneSlot_touched_le_one st0.slots j (val r)
            simpa [st1, optStep, hb] using (Nat.max_le).2 ⟨htouch0, ht⟩
      rcases ih st1 hlen1 htouch1 with ⟨htot, hlenF, hslotsF, htouchF⟩
      refine ⟨?_, hlenF, ?_, htouchF⟩
      · have htot' : (List.foldl (optStep bucket val) st1 rs).totalRows =
            st1.totalRows + rs.length := htot
        rw [htot', htotal1]
        simp [Nat.add_comm, Nat.add_left_comm]
      · intro j hj
        have hget1 : st1.slots.getD j {} = accSlotFrom bucket val j (st0.slots.getD j {}) [r] := by
          rw [show st1.slots.getD j {} = (st1.slots[j]?).getD {} by rfl]
          rw [hslots1 j hj]
          simp
        have hf := hslotsF j hj
        rw [hget1] at hf
        have hfold : accSlotFrom bucket val j (st0.slots.getD j {}) (r :: rs) =
            accSlotFrom bucket val j (accSlotFrom bucket val j (st0.slots.getD j {}) [r]) rs := by
          simp [accSlotFrom]
        rw [hfold]
        exact hf

private theorem initSlots_getD (k j : Nat) (hj : j < k) :
    (initState k).slots.getD j {} = ({} : SlotAcc) := by
  rw [show (initState k).slots.getD j {} = ((initState k).slots[j]?).getD {} by rfl]
  rw [List.getElem?_eq_getElem]
  · simp [initState]
  · simp [initState, hj]

theorem sound : TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch.Goal := by
  intro α k rows mode bucket val
  dsimp
  rw [optPivotSums_eq]
  let st := rows.foldl (optStep bucket val) (initState k)
  have hlen0 : (initState k).slots.length = k := by simp [initState]
  have ht0 : (initState k).maxTouchesPerRow ≤ 1 := by simp [initState]
  have hinv := opt_fold_invariants k bucket val rows (initState k) hlen0 ht0
  change (st.slots.map (finalizeSlot mode st.totalRows) =
      refPivotSums k rows mode bucket val) ∧ st.maxTouchesPerRow ≤ 1
  rcases hinv with ⟨htotal, hlen, hslots, htouches⟩
  have hinitTotal : (initState k).totalRows = 0 := rfl
  constructor
  · change st.slots.map (finalizeSlot mode st.totalRows) =
      (List.range k).map (refBucketSum rows mode bucket val)
    rw [htotal, hinitTotal]
    simp only [Nat.zero_add]
    apply List.ext_getElem?
    intro i
    by_cases hi : i < k
    · have hs := hslots i hi
      rw [initSlots_getD k i hi] at hs
      have href : refBucketSum rows mode bucket val i =
          finalizeSlot mode rows.length (accSlotFrom bucket val i ({} : SlotAcc) rows) := by
        have hbase : finalizeSlot mode 0 ({} : SlotAcc) = none := by
          cases mode <;> rfl
        have h0 := ref_fold_from_slot rows mode bucket val i ({} : SlotAcc) 0
          (by simp) (by simp)
        rw [hbase] at h0
        change refBucketSumFrom rows mode bucket val i none =
          finalizeSlot mode rows.length (accSlotFrom bucket val i ({} : SlotAcc) rows)
        simpa only [Nat.zero_add] using h0
      rw [List.getElem?_map, List.getElem?_map, hs, List.getElem?_range hi]
      simp [href]
    · have hlen' : st.slots.length = k := hlen
      have hle : st.slots.length ≤ i := by omega
      have hnone1 : st.slots[i]? = none := List.getElem?_eq_none hle
      have hk_le : k ≤ i := by omega
      have hnone2 : ((List.range k).map (refBucketSum rows mode bucket val))[i]? = none := by
        rw [List.getElem?_map]
        rw [List.getElem?_eq_none (by simpa using hk_le)]
        simp
      rw [List.getElem?_map, hnone1, hnone2]
      simp
  · exact htouches

end TinylambOpt.ResearchProofs.DisjointCasePivotBucketDispatch
