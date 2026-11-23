import TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate

namespace TinylambOpt.ResearchProofs.BoundedMultiKeyDirectArrayAggregate

open TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate

private def addVal (s : Slot) (ov : Option Int) : Slot :=
  match ov with
  | none => { s with seen := true, cntStar := s.cntStar + 1 }
  | some x => { seen := true, cntStar := s.cntStar + 1, nnCnt := s.nnCnt + 1, sumVal := s.sumVal + x }

private def someCount : List (Option Int) → Nat
  | [] => 0
  | none :: vs => someCount vs
  | some _ :: vs => 1 + someCount vs

private def sumSome : List (Option Int) → Int
  | [] => 0
  | none :: vs => sumSome vs
  | some x :: vs => x + sumSome vs

@[simp] private theorem someCount_nil : someCount [] = 0 := rfl
@[simp] private theorem sumSome_nil : sumSome [] = 0 := rfl
@[simp] private theorem someCount_cons_none (vs : List (Option Int)) : someCount (none :: vs) = someCount vs := rfl
@[simp] private theorem someCount_cons_some (x : Int) (vs : List (Option Int)) : someCount (some x :: vs) = 1 + someCount vs := rfl
@[simp] private theorem sumSome_cons_none (vs : List (Option Int)) : sumSome (none :: vs) = sumSome vs := rfl
@[simp] private theorem sumSome_cons_some (x : Int) (vs : List (Option Int)) : sumSome (some x :: vs) = x + sumSome vs := rfl

private theorem someCount_foldl (vs : List (Option Int)) (n : Nat) :
    vs.foldl (fun acc v => match v with | none => acc | some _ => acc + 1) n = n + someCount vs := by
  induction vs generalizing n with
  | nil => simp
  | cons v vs ih => cases v <;> simp [ih, Nat.add_assoc]

private theorem sumSome_foldl (vs : List (Option Int)) (a : Int) :
    vs.foldl (fun acc v => match v with | none => acc | some x => acc + x) a = a + sumSome vs := by
  induction vs generalizing a with
  | nil => simp
  | cons v vs ih =>
      cases v with
      | none => simp [ih]
      | some x => simp [ih, Int.add_assoc]

private theorem sumOptList_foldl (vs : List (Option Int)) (acc : Option Int) :
    vs.foldl
      (fun acc v =>
        match acc, v with
        | none, y => y
        | x, none => x
        | some a, some b => some (a + b)) acc =
      match acc with
      | none => vs.foldl
          (fun acc v =>
            match acc, v with
            | none, y => y
            | x, none => x
            | some a, some b => some (a + b)) none
      | some a => some (a + sumSome vs) := by
  induction vs generalizing acc with
  | nil => cases acc <;> simp
  | cons v vs ih =>
      cases acc with
      | none => rfl
      | some a =>
          cases v with
          | none => simp [ih]
          | some x => simp [ih, Int.add_assoc]

private theorem sumOptList_cons_none (vs : List (Option Int)) :
    sumOptList (none :: vs) = sumOptList vs := rfl

private theorem sumOptList_eq (vs : List (Option Int)) :
    sumOptList vs = if someCount vs = 0 then none else some (sumSome vs) := by
  induction vs with
  | nil => simp [sumOptList]
  | cons v vs ih =>
      cases v with
      | none =>
          simp only [sumOptList_cons_none, someCount_cons_none, sumSome_cons_none]
          rw [ih]
          rfl
      | some x =>
          have hsum : sumOptList (some x :: vs) = some (x + sumSome vs) := by
            exact sumOptList_foldl vs (some x)
          rw [hsum]
          simp

private theorem someCount_append (a b : List (Option Int)) :
    someCount (a ++ b) = someCount a + someCount b := by
  induction a with
  | nil => simp
  | cons v vs ih => cases v <;> simp [ih, Nat.add_assoc]

private theorem sumSome_append (a b : List (Option Int)) :
    sumSome (a ++ b) = sumSome a + sumSome b := by
  induction a with
  | nil => simp
  | cons v vs ih =>
      cases v with
      | none => simp [ih]
      | some x => simp [ih, Int.add_assoc]

private def matchB (k1 k2 : Option Int) (r : InputRow) : Bool := r.k1 == k1 && r.k2 == k2

private def upd (k1 k2 : Option Int) (s : Slot) (r : InputRow) : Slot :=
  if matchB k1 k2 r = true then addVal s r.v else s

private theorem slot_ext {a b : Slot}
    (h1 : a.seen = b.seen) (h2 : a.cntStar = b.cntStar)
    (h3 : a.nnCnt = b.nnCnt) (h4 : a.sumVal = b.sumVal) : a = b := by
  cases a
  cases b
  simp_all

private theorem mergeSlot_assoc (a b c : Slot) :
    mergeSlot (mergeSlot a b) c = mergeSlot a (mergeSlot b c) := by
  apply slot_ext <;> simp [mergeSlot, Bool.or_assoc, Nat.add_assoc, Int.add_assoc]

private theorem addVal_eq_merge (s : Slot) (ov : Option Int) :
    addVal s ov = mergeSlot s (addVal {} ov) := by
  cases ov <;> apply slot_ext <;> simp [addVal, mergeSlot]

private theorem fold_upd_additive (rows : List InputRow) (k1 k2 : Option Int) (s : Slot) :
    rows.foldl (upd k1 k2) s = mergeSlot s (rows.foldl (upd k1 k2) {}) := by
  induction rows generalizing s with
  | nil =>
      apply slot_ext <;> simp [mergeSlot]
  | cons r rs ih =>
      simp only [List.foldl_cons]
      have h1 := ih (upd k1 k2 s r)
      have h2 := ih (upd k1 k2 {} r)
      rw [h1]
      rw [h2]
      by_cases h : matchB k1 k2 r
      · have hup1 : upd k1 k2 s r = addVal s r.v := by simp [upd, h]
        have hup2 : upd k1 k2 {} r = addVal {} r.v := by simp [upd, h]
        rw [hup1, hup2]
        rw [show addVal s r.v = mergeSlot s (addVal {} r.v) from addVal_eq_merge s r.v]
        rw [mergeSlot_assoc]
      · simp [upd, h, mergeSlot]

private theorem fold_upd_spec (rows : List InputRow) (k1 k2 : Option Int) :
    let s := rows.foldl (upd k1 k2) {}
    let m := rows.filter (matchB k1 k2)
    s.seen = !m.isEmpty ∧
    s.cntStar = m.length ∧
    s.nnCnt = someCount (m.map InputRow.v) ∧
    s.sumVal = sumSome (m.map InputRow.v) := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      by_cases h : matchB k1 k2 r
      · have hadd := fold_upd_additive rs k1 k2 (addVal {} r.v)
        simp only [List.foldl_cons, upd, h, ↓reduceIte] at hadd ⊢
        rw [hadd]
        generalize hr : r.v = v
        cases v <;> simp [hr, mergeSlot, addVal, ih, h, Nat.add_comm]
      · simp [upd, h, ih]

private theorem incFlatSlot_getElem? (arr : List Slot) (idx : Nat) (ov : Option Int) (j : Nat) :
    (incFlatSlot arr idx ov)[j]? = Option.map (fun s => if j = idx then addVal s ov else s) arr[j]? := by
  simp [incFlatSlot, addVal, List.getElem?_map, List.getElem?_zipIdx, Function.comp_def]
  cases arr[j]? <;> cases ov <;> rfl

private def rowCoord (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat) (r : InputRow) : Nat :=
  slotCoord min1 span1 r.k1 * (span2 + 1) + slotCoord min2 span2 r.k2

private theorem foldl_incFlatSlot_getElem?
    (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (rows : List InputRow) (arr : List Slot) (idx : Nat)
    (P : InputRow → Prop) [DecidablePred P]
    (hP : ∀ r ∈ rows, (idx = rowCoord min1 span1 min2 span2 r) ↔ P r) :
    ((rows.foldl
        (fun a r => incFlatSlot a (rowCoord min1 span1 min2 span2 r) r.v) arr)[idx]?) =
      Option.map
        (fun s => rows.foldl (fun s r => if P r then addVal s r.v else s) s)
        arr[idx]? := by
  induction rows generalizing arr with
  | nil => simp
  | cons r rs ih =>
      have hPr : (idx = rowCoord min1 span1 min2 span2 r) ↔ P r := hP r (by simp)
      have hPrs : ∀ x ∈ rs, (idx = rowCoord min1 span1 min2 span2 x) ↔ P x := by
        intro x hx
        exact hP x (by simp [hx])
      simp only [List.foldl_cons]
      rw [ih _ hPrs]
      rw [incFlatSlot_getElem?]
      simp only [Option.map_map]
      congr 1
      funext s
      by_cases h : idx = rowCoord min1 span1 min2 span2 r
      · have hp : P r := hPr.1 h
        simp [h, hp]
      · have hp : ¬ P r := by intro hp; exact h (hPr.2 hp)
        simp [h, hp]

private def InBound (min : Int) (span : Nat) (o : Option Int) : Prop :=
  match o with
  | none => True
  | some a => min ≤ a ∧ a < min + Int.ofNat span

private theorem slotCoord_le_span (min : Int) (span : Nat) (o : Option Int)
    (hb : InBound min span o) : slotCoord min span o ≤ span := by
  cases o with
  | none => simp [slotCoord]
  | some k =>
      simp [slotCoord]
      rcases hb with ⟨hlo, hhi⟩
      have hk0 : 0 ≤ k - min := by omega
      have hlt0 : k - min < Int.ofNat span := by omega
      have hlt : (k - min).toNat < span := (Int.toNat_lt hk0).2 hlt0
      omega

private theorem slotCoord_inj (min : Int) (span : Nat) (o o' : Option Int)
    (hb : InBound min span o) (hb' : InBound min span o')
    (h : slotCoord min span o = slotCoord min span o') : o = o' := by
  cases o with
  | none =>
      cases o' with
      | none => rfl
      | some b =>
          simp [slotCoord] at h
          rcases hb' with ⟨hlo, hhi⟩
          have hb0 : 0 ≤ b - min := by omega
          have hlt0 : b - min < Int.ofNat span := by omega
          have hlt : (b - min).toNat < span := (Int.toNat_lt hb0).2 hlt0
          omega
  | some a =>
      cases o' with
      | none =>
          simp [slotCoord] at h
          rcases hb with ⟨hlo, hhi⟩
          have ha0 : 0 ≤ a - min := by omega
          have hlt0 : a - min < Int.ofNat span := by omega
          have hlt : (a - min).toNat < span := (Int.toNat_lt ha0).2 hlt0
          omega
      | some b =>
          simp [slotCoord] at h
          rcases hb with ⟨ha_lo, ha_hi⟩
          rcases hb' with ⟨hb_lo, hb_hi⟩
          have ha0 : 0 ≤ a - min := by omega
          have hb0 : 0 ≤ b - min := by omega
          have haT : ((a - min).toNat : Int) = a - min := Int.toNat_of_nonneg ha0
          have hbT : ((b - min).toNat : Int) = b - min := Int.toNat_of_nonneg hb0
          have hab : a = b := by omega
          subst b
          rfl

private theorem flatIdx_inj (span2 : Nat) (i1 i2 j1 j2 : Nat)
    (hi2 : i2 ≤ span2) (hj2 : j2 ≤ span2)
    (h : i1 * (span2 + 1) + i2 = j1 * (span2 + 1) + j2) :
    i1 = j1 ∧ i2 = j2 := by
  have hd : 0 < span2 + 1 := by omega
  have hi2lt : i2 < span2 + 1 := by omega
  have hj2lt : j2 < span2 + 1 := by omega
  have hi_mod : (i1 * (span2 + 1) + i2) % (span2 + 1) = i2 := by
    rw [Nat.mul_comm i1 (span2 + 1), Nat.add_comm ((span2 + 1) * i1) i2,
      Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hi2lt]
  have hj_mod : (j1 * (span2 + 1) + j2) % (span2 + 1) = j2 := by
    rw [Nat.mul_comm j1 (span2 + 1), Nat.add_comm ((span2 + 1) * j1) j2,
      Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hj2lt]
  have hi_div : (i1 * (span2 + 1) + i2) / (span2 + 1) = i1 := by
    rw [Nat.mul_comm i1 (span2 + 1), Nat.add_comm ((span2 + 1) * i1) i2,
      Nat.add_mul_div_left i2 i1 hd, Nat.div_eq_of_lt hi2lt]
    omega
  have hj_div : (j1 * (span2 + 1) + j2) / (span2 + 1) = j1 := by
    rw [Nat.mul_comm j1 (span2 + 1), Nat.add_comm ((span2 + 1) * j1) j2,
      Nat.add_mul_div_left j2 j1 hd, Nat.div_eq_of_lt hj2lt]
    omega
  have hm := congrArg (fun x : Nat => x % (span2 + 1)) h
  have hdq := congrArg (fun x : Nat => x / (span2 + 1)) h
  omega

private theorem coord_eq_iff_keys_eq
    (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (r : InputRow) (k1 k2 : Option Int)
    (hb1r : InBound min1 span1 r.k1) (hb2r : InBound min2 span2 r.k2)
    (hb1q : InBound min1 span1 k1) (hb2q : InBound min2 span2 k2) :
    (slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2 =
        rowCoord min1 span1 min2 span2 r) ↔ (r.k1 = k1 ∧ r.k2 = k2) := by
  constructor
  · intro h
    have hle1 := slotCoord_le_span min2 span2 k2 hb2q
    have hle2 := slotCoord_le_span min2 span2 r.k2 hb2r
    have hp := flatIdx_inj span2
      (slotCoord min1 span1 k1) (slotCoord min2 span2 k2)
      (slotCoord min1 span1 r.k1) (slotCoord min2 span2 r.k2) hle1 hle2 h
    have hk1' : k1 = r.k1 := slotCoord_inj min1 span1 k1 r.k1 hb1q hb1r hp.1
    have hk2' : k2 = r.k2 := slotCoord_inj min2 span2 k2 r.k2 hb2q hb2r hp.2
    exact ⟨hk1'.symm, hk2'.symm⟩
  · rintro ⟨hk1, hk2⟩
    simp [rowCoord, hk1, hk2]

private theorem coord_lt_total (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (k1 k2 : Option Int) (hb1 : InBound min1 span1 k1) (hb2 : InBound min2 span2 k2) :
    slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2 <
      (span1 + 1) * (span2 + 1) := by
  have hc1 := slotCoord_le_span min1 span1 k1 hb1
  have hc2 := slotCoord_le_span min2 span2 k2 hb2
  have hm : slotCoord min1 span1 k1 * (span2 + 1) ≤ span1 * (span2 + 1) :=
    Nat.mul_le_mul_right (span2 + 1) hc1
  calc
    slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2
        ≤ span1 * (span2 + 1) + span2 := Nat.add_le_add hm hc2
    _ < (span1 + 1) * (span2 + 1) := by
        rw [Nat.add_mul]
        omega

private theorem localFlatSlot (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (rows : List InputRow) (k1 k2 : Option Int)
    (hb1r : ∀ r ∈ rows, InBound min1 span1 r.k1)
    (hb2r : ∀ r ∈ rows, InBound min2 span2 r.k2)
    (hb1q : InBound min1 span1 k1) (hb2q : InBound min2 span2 k2) :
    (localFlatBuild min1 span1 min2 span2 rows)[
        slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2]? =
      some (rows.foldl (fun s r => if matchB k1 k2 r = true then addVal s r.v else s) {}) := by
  let idx := slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2
  have hlt : idx < (span1 + 1) * (span2 + 1) := coord_lt_total min1 span1 min2 span2 k1 k2 hb1q hb2q
  have hrep : (List.replicate ((span1 + 1) * (span2 + 1)) ({} : Slot))[idx]? = some {} := by
    rw [List.getElem?_replicate]
    simp [hlt]
  have hfl := foldl_incFlatSlot_getElem? min1 span1 min2 span2 rows
    (List.replicate ((span1 + 1) * (span2 + 1)) ({} : Slot)) idx
    (fun r => matchB k1 k2 r)
    (by
      intro r hr
      have hb1 : InBound min1 span1 r.k1 := hb1r r hr
      have hb2 : InBound min2 span2 r.k2 := hb2r r hr
      rw [coord_eq_iff_keys_eq min1 span1 min2 span2 r k1 k2 hb1 hb2 hb1q hb2q]
      simp [matchB, beq_iff_eq])
  simpa [localFlatBuild, rowCoord, idx, hrep] using hfl

private theorem zip_map_merge_eq_zipWith (a b : List Slot) :
    (a.zip b).map (fun p => mergeSlot p.1 p.2) = List.zipWith mergeSlot a b := by
  induction a generalizing b with
  | nil => rfl
  | cons x xs ih =>
      cases b with
      | nil => rfl
      | cons y ys => simp [ih]

private theorem merged_getD (a b : List Slot) (idx : Nat) (sA sB : Slot)
    (ha : a[idx]? = some sA) (hb : b[idx]? = some sB) :
    ((a.zip b).map (fun p => mergeSlot p.1 p.2)).getD idx {} = mergeSlot sA sB := by
  simp [List.getD, zip_map_merge_eq_zipWith, List.getElem?_zipWith, ha, hb]

theorem sound : TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate.Goal := by
  intro min1 span1 min2 span2 rowsA rowsB hbounds k1 k2 hk1 hk2
  have hbq1 : InBound min1 span1 k1 := by
    cases k1 with
    | none => trivial
    | some a => exact hk1
  have hbq2 : InBound min2 span2 k2 := by
    cases k2 with
    | none => trivial
    | some b => exact hk2
  have hbA1 : ∀ r ∈ rowsA, InBound min1 span1 r.k1 := by
    intro r hr
    exact (hbounds r (by simp [hr])).1
  have hbA2 : ∀ r ∈ rowsA, InBound min2 span2 r.k2 := by
    intro r hr
    exact (hbounds r (by simp [hr])).2
  have hbB1 : ∀ r ∈ rowsB, InBound min1 span1 r.k1 := by
    intro r hr
    exact (hbounds r (by simp [hr])).1
  have hbB2 : ∀ r ∈ rowsB, InBound min2 span2 r.k2 := by
    intro r hr
    exact (hbounds r (by simp [hr])).2
  let idx := slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2
  let arrA := localFlatBuild min1 span1 min2 span2 rowsA
  let arrB := localFlatBuild min1 span1 min2 span2 rowsB
  let sA := rowsA.foldl (upd k1 k2) {}
  let sB := rowsB.foldl (upd k1 k2) {}
  have hsA : arrA[idx]? = some sA := by
    change (localFlatBuild min1 span1 min2 span2 rowsA)[idx]? =
      some (rowsA.foldl (fun s r => if matchB k1 k2 r = true then addVal s r.v else s) {})
    exact localFlatSlot min1 span1 min2 span2 rowsA k1 k2 hbA1 hbA2 hbq1 hbq2
  have hsB : arrB[idx]? = some sB := by
    change (localFlatBuild min1 span1 min2 span2 rowsB)[idx]? =
      some (rowsB.foldl (fun s r => if matchB k1 k2 r = true then addVal s r.v else s) {})
    exact localFlatSlot min1 span1 min2 span2 rowsB k1 k2 hbB1 hbB2 hbq1 hbq2
  have hmerged : ((arrA.zip arrB).map (fun p => mergeSlot p.1 p.2)).getD idx {} = mergeSlot sA sB :=
    merged_getD arrA arrB idx sA sB hsA hsB
  have hspecA := fold_upd_spec rowsA k1 k2
  have hspecB := fold_upd_spec rowsB k1 k2
  let mA := rowsA.filter (matchB k1 k2)
  let mB := rowsB.filter (matchB k1 k2)
  let M := (rowsA ++ rowsB).filter (matchB k1 k2)
  let vs := M.map InputRow.v
  have hM : M = mA ++ mB := by simp [M, mA, mB, List.filter_append]
  have hvs : vs = (mA.map InputRow.v) ++ (mB.map InputRow.v) := by
    simp [vs, hM, List.map_append]
  have hseen : (mergeSlot sA sB).seen = !M.isEmpty := by
    have hA : sA.seen = !mA.isEmpty := by simpa [sA, mA] using hspecA.1
    have hB : sB.seen = !mB.isEmpty := by simpa [sB, mB] using hspecB.1
    rw [hM]
    cases hA0 : mA with
    | nil =>
        cases hB0 : mB with
        | nil => simp [mergeSlot, hA, hB, hA0, hB0]
        | cons b bs => simp [mergeSlot, hA, hB, hA0, hB0]
    | cons a as =>
        cases hB0 : mB with
        | nil => simp [mergeSlot, hA, hB, hA0, hB0]
        | cons b bs => simp [mergeSlot, hA, hB, hA0, hB0]
  have hcnt : (mergeSlot sA sB).cntStar = M.length := by
    have hA : sA.cntStar = mA.length := by simpa [sA, mA] using hspecA.2.1
    have hB : sB.cntStar = mB.length := by simpa [sB, mB] using hspecB.2.1
    simp [mergeSlot, hA, hB, hM]
  have hnn : (mergeSlot sA sB).nnCnt = someCount vs := by
    have hA : sA.nnCnt = someCount (mA.map InputRow.v) := by simpa [sA, mA] using hspecA.2.2.1
    have hB : sB.nnCnt = someCount (mB.map InputRow.v) := by simpa [sB, mB] using hspecB.2.2.1
    simp [mergeSlot, hA, hB, hvs, someCount_append]
  have hsum : (mergeSlot sA sB).sumVal = sumSome vs := by
    have hA : sA.sumVal = sumSome (mA.map InputRow.v) := by simpa [sA, mA] using hspecA.2.2.2
    have hB : sB.sumVal = sumSome (mB.map InputRow.v) := by simpa [sB, mB] using hspecB.2.2.2
    simp [mergeSlot, hA, hB, hvs, sumSome_append]
  have hopt : optParallelFlatAgg min1 span1 min2 span2 rowsA rowsB k1 k2 =
      (if !(mergeSlot sA sB).seen then none
       else some ((mergeSlot sA sB).cntStar,
         if (mergeSlot sA sB).nnCnt == 0 then none else some (mergeSlot sA sB).sumVal)) := by
    unfold optParallelFlatAgg
    dsimp
    rw [hmerged]
  have href : refGroupAgg (rowsA ++ rowsB) k1 k2 =
      (if M.isEmpty then none else some (M.length, sumOptList vs)) := by
    rfl
  rw [hopt, href]
  rw [hseen, hcnt, hnn, hsum]
  by_cases hMempty : M.isEmpty
  · simp [hMempty]
  · simp [hMempty, sumOptList_eq]

end TinylambOpt.ResearchProofs.BoundedMultiKeyDirectArrayAggregate
