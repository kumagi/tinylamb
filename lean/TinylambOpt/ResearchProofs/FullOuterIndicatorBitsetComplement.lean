import TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement

namespace TinylambOpt.ResearchProofs.FullOuterIndicatorBitsetComplement

open TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement

/-! ## Classifiers and local shapes of the two pipelines -/

private def nnPair : Key → Option (Int × Int)
  | (some k1, some k2) => some (k1, k2)
  | _ => none

private def isNN : Key → Bool
  | (k1, k2) => k1.isSome && k2.isSome

private def isPartial : Key → Bool
  | (k1, k2) => k1.isSome && k2.isNone

private def leftOnly : Option Int × Option Int → Bool
  | (l, r) => l.isSome && r.isNone

private def rightOnly : Option Int × Option Int → Bool
  | (l, r) => l.isNone && r.isSome

private def bothSide : Option Int × Option Int → Bool
  | (l, r) => l.isSome && r.isSome

private def nnHitA (dB : List Key) (k : Key) : Bool :=
  match nnPair k with
  | some p => (dB.filterMap nnPair).contains p
  | none => false

private def nnHitB (dA : List Key) (k : Key) : Bool :=
  match nnPair k with
  | some p => (dA.filterMap nnPair).contains p
  | none => false

private def fA (dB : List Key) (k : Key) : Option Int × Option Int :=
  match k.1, k.2 with
  | some x, some y => if (some x, some y) ∈ dB then (some x, some x) else (some x, none)
  | some x, none => (some x, none)
  | _, _ => (none, none)

private def gB (dA : List Key) (k : Key) : Option (Option Int × Option Int) :=
  if dA.any (fun ak => eqJoinKey ak k) then none else some (none, k.1)

private def refOn (dA dB : List Key) : ThreeCounts :=
  let leftPairs : List (Option Int × Option Int) :=
    dA.flatMap fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1))
  let rightUnmatched : List (Option Int × Option Int) :=
    dB.filterMap (gB dA)
  let allPairs := leftPairs ++ rightUnmatched
  { storeOnly := (allPairs.filter leftOnly).length
    catalogOnly := (allPairs.filter rightOnly).length
    storeAndCatalog := (allPairs.filter bothSide).length }

private def optOn (dA dB : List Key) : ThreeCounts :=
  let nnA := dA.filterMap nnPair
  let nnB := dB.filterMap nnPair
  let nullItemA := (dA.filter isPartial).length
  let nullItemB := (dB.filter isPartial).length
  let both := (nnA.filter (fun p => nnB.contains p)).length
  let leftNNOnly := (nnA.filter (fun p => !nnB.contains p)).length
  let rightNNOnly := (nnB.filter (fun p => !nnA.contains p)).length
  { storeOnly := leftNNOnly + nullItemA
    catalogOnly := rightNNOnly + nullItemB
    storeAndCatalog := both }

/-! ## Generic list-counting helpers -/

private theorem countP_congr {α : Type} (p q : α → Bool) (xs : List α)
    (h : ∀ a ∈ xs, p a = q a) : xs.countP p = xs.countP q := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.countP_cons]
      rw [h a List.mem_cons_self]
      rw [ih (fun b hb => h b (List.mem_cons_of_mem a hb))]

private theorem countP_or_disjoint {α : Type} (p q : α → Bool) (xs : List α)
    (h : ∀ a ∈ xs, ¬(p a = true ∧ q a = true)) :
    xs.countP (fun a => p a || q a) = xs.countP p + xs.countP q := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      have htail : ∀ b ∈ xs, ¬(p b = true ∧ q b = true) :=
        fun b hb => h b (List.mem_cons_of_mem a hb)
      by_cases hp : p a = true
      · by_cases hq : q a = true
        · exact (h a List.mem_cons_self ⟨hp, hq⟩).elim
        · have hqf : q a = false := by simpa using hq
          simp [hp, hqf, ih htail]
          omega
      · have hpf : p a = false := by simpa using hp
        by_cases hq : q a = true
        · simp [hpf, hq, ih htail]
          omega
        · have hqf : q a = false := by simpa using hq
          simp [hpf, hqf, ih htail]

private theorem countP_filterMap {α β : Type} (f : α → Option β) (Q : β → Bool)
    (xs : List α) :
    (xs.filterMap f).countP Q =
      xs.countP (fun a => match f a with | some b => Q b | none => false) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      rw [List.countP_cons, List.filterMap_cons]
      cases hf : f a with
      | none => simp [hf, ih]
      | some b =>
          rw [List.countP_cons]
          by_cases hQ : Q b = true
          · simp [hf, hQ, ih]
          · have hQf : Q b = false := by simpa using hQ
            simp [hf, hQf, ih]

private theorem length_filter_map {α β : Type} (f : α → β) (Q : β → Bool)
    (xs : List α) : ((xs.map f).filter Q).length = xs.countP (fun a => Q (f a)) := by
  rw [← List.countP_eq_length_filter]
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, List.countP_cons]
      rw [ih]

private theorem length_filter_filterMap {α β : Type} (f : α → Option β) (Q : β → Bool)
    (xs : List α) :
    ((xs.filterMap f).filter Q).length =
      xs.countP (fun a => match f a with | some b => Q b | none => false) := by
  rw [← List.countP_eq_length_filter]
  exact countP_filterMap f Q xs

private theorem map_count_eq_zero {α β : Type} (f : α → β) (Q : β → Bool)
    (xs : List α) (h : ∀ a, Q (f a) = false) :
    ((xs.map f).filter Q).length = 0 := by
  rw [length_filter_map]
  induction xs with
  | nil => rfl
  | cons a xs ih => rw [List.countP_cons, h a, ih]; rfl

private theorem filterMap_count_eq_zero {α β : Type} (f : α → Option β) (Q : β → Bool)
    (xs : List α)
    (h : ∀ a, (match f a with | some b => Q b | none => false) = false) :
    ((xs.filterMap f).filter Q).length = 0 := by
  rw [length_filter_filterMap]
  induction xs with
  | nil => rfl
  | cons a xs ih => rw [List.countP_cons, h a, ih]; rfl

/-! ## The join predicate -/

private theorem eqJoinKey_iff_eq (a b : Key) :
    eqJoinKey a b = true ↔ isNN a = true ∧ b = a := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  cases a1 <;> cases a2 <;> cases b1 <;> cases b2 <;>
    simp [eqJoinKey, isNN, beq_iff_eq] <;> omega

private theorem anyB_iff (dA : List Key) (bk : Key) :
    (dA.any (fun ak => eqJoinKey ak bk) = true) ↔ isNN bk = true ∧ bk ∈ dA := by
  rw [List.any_eq_true]
  constructor
  · rintro ⟨ak, hak, h⟩
    rcases (eqJoinKey_iff_eq ak bk).mp h with ⟨hNN, rfl⟩
    exact ⟨hNN, hak⟩
  · rintro ⟨hNN, hmem⟩
    exact ⟨bk, hmem, (eqJoinKey_iff_eq bk bk).mpr ⟨hNN, rfl⟩⟩

/-! ## Deduplication -/

private theorem insertUniq_nodup {xs : List Key} (h : xs.Nodup) (k : Key) :
    (insertUniq xs k).Nodup := by
  unfold insertUniq
  by_cases hkm : k ∈ xs
  · have hc : xs.contains k = true := by simpa using hkm
    simp [hc, hkm, h]
  · have hc : xs.contains k = false := by simpa using hkm
    have happ : (xs ++ [k]).Nodup := by
      rw [List.nodup_append]
      refine ⟨h, by simp, ?_⟩
      intro a ha b hb
      simp only [List.mem_singleton] at hb
      subst b
      intro haeq
      exact hkm (by simpa [haeq] using ha)
    simp [hc, hkm, happ]

private theorem foldl_insertUniq_nodup (xs acc : List Key) (hacc : acc.Nodup) :
    (xs.foldl insertUniq acc).Nodup := by
  induction xs generalizing acc with
  | nil => exact hacc
  | cons k xs ih =>
      apply ih
      exact insertUniq_nodup hacc k

private theorem dedupKeys_nodup (xs : List Key) : (dedupKeys xs).Nodup := by
  unfold dedupKeys
  exact foldl_insertUniq_nodup xs [] List.nodup_nil

/-! ## Non-NULL pair membership -/

private theorem contains_iff_mem {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (a : α) : xs.contains a = true ↔ a ∈ xs := by
  simp

private theorem mem_filterMap_nnPair {dB : List Key} {p : Int × Int} :
    (dB.filterMap nnPair).contains p = true ↔ (some p.1, some p.2) ∈ dB := by
  rw [contains_iff_mem, List.mem_filterMap]
  constructor
  · rintro ⟨a, ha, hnn⟩
    rcases a with ⟨a1, a2⟩
    cases a1 <;> cases a2 <;> simp [nnPair] at hnn
    rw [← hnn]
    exact ha
  · intro h
    exact ⟨(some p.1, some p.2), h, by simp [nnPair]⟩

private theorem nnHitA_eq (dB : List Key) (k : Key) :
    nnHitA dB k = true ↔ isNN k = true ∧ k ∈ dB := by
  unfold nnHitA
  rcases k with ⟨k1, k2⟩
  cases k1 with
  | none => cases k2 <;> simp [nnPair, isNN]
  | some x =>
      cases k2 with
      | none => simp [nnPair, isNN]
      | some y =>
          simpa [nnPair, isNN] using (mem_filterMap_nnPair (dB := dB) (p := (x, y)))

private theorem nnHitB_eq (dA : List Key) (k : Key) :
    nnHitB dA k = true ↔ isNN k = true ∧ k ∈ dA := by
  unfold nnHitB
  rcases k with ⟨k1, k2⟩
  cases k1 with
  | none => cases k2 <;> simp [nnPair, isNN]
  | some x =>
      cases k2 with
      | none => simp [nnPair, isNN]
      | some y =>
          simpa [nnPair, isNN] using (mem_filterMap_nnPair (dB := dA) (p := (x, y)))

private theorem anyB_eq_nnHitB (dA : List Key) (k : Key) :
    (dA.any (fun ak => eqJoinKey ak k) = true) ↔ nnHitB dA k = true := by
  rw [anyB_iff, nnHitB_eq]

/-! ## Shape of `leftPairs` -/

private theorem filter_eqJoinKey_eq (x y : Int) (dB : List Key) (hB : dB.Nodup) :
    dB.filter (eqJoinKey (some x, some y)) =
      if (some x, some y) ∈ dB then [(some x, some y)] else [] := by
  induction dB with
  | nil => simp
  | cons bk rest ih =>
      have hB' : rest.Nodup := (List.nodup_cons.mp hB).2
      simp only [List.filter_cons]
      rw [ih hB']
      by_cases hbk : bk = (some x, some y)
      · subst bk
        have hnot : (some x, some y) ∉ rest := (List.nodup_cons.mp hB).1
        simp [eqJoinKey, hnot]
      · have hbk' : (some x, some y) ≠ bk := fun h => hbk h.symm
        have hp : eqJoinKey (some x, some y) bk = false := by
          cases hE : eqJoinKey (some x, some y) bk with
          | true =>
              have hbkeq : bk = (some x, some y) := by
                simpa [isNN] using (eqJoinKey_iff_eq (some x, some y) bk).mp hE
              exact (hbk hbkeq).elim
          | false => rfl
        simp [hp, hbk, hbk']

private theorem leftPairs_eq (dA dB : List Key) (hB : dB.Nodup) :
    dA.flatMap (fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1)))
      = dA.map (fA dB) := by
  induction dA with
  | nil => rfl
  | cons ak rest ih =>
      rw [List.flatMap_cons, ih, List.map_cons]
      congr 1
      rcases ak with ⟨a1, a2⟩
      cases a1 with
      | none =>
          cases a2 <;> simp [fA, eqJoinKey, List.any_eq_true]
      | some x =>
          cases a2 with
          | none => simp [fA, eqJoinKey, List.any_eq_true]
          | some y =>
              rw [filter_eqJoinKey_eq x y dB hB]
              by_cases hmem : (some x, some y) ∈ dB <;> simp [fA, hmem, eqJoinKey]

/-! ## Pointwise classifiers of the simplified pipelines -/

private theorem fA_leftOnly_iff (dB : List Key) (k : Key) :
    leftOnly (fA dB k) = true ↔
      isPartial k = true ∨ (isNN k = true ∧ nnHitA dB k = false) := by
  rcases k with ⟨k1, k2⟩
  cases k1 with
  | none => cases k2 <;> simp [fA, leftOnly, isPartial, isNN, nnHitA]
  | some x =>
      cases k2 with
      | none => simp [fA, leftOnly, isPartial, isNN, nnHitA]
      | some y =>
          have hnn : nnHitA dB (some x, some y) = true ↔ (some x, some y) ∈ dB := by
            simpa [isNN] using (nnHitA_eq dB (some x, some y))
          by_cases hmem : (some x, some y) ∈ dB
          · have hc : nnHitA dB (some x, some y) = true := hnn.mpr hmem
            simp [fA, leftOnly, isPartial, isNN, hmem, hc]
          · have hc : nnHitA dB (some x, some y) = false :=
              Bool.eq_false_iff.mpr (fun htrue => hmem (hnn.mp htrue))
            simp [fA, leftOnly, isPartial, isNN, hmem, hc]

private theorem fA_bothSide_iff (dB : List Key) (k : Key) :
    bothSide (fA dB k) = true ↔ isNN k = true ∧ nnHitA dB k = true := by
  rcases k with ⟨k1, k2⟩
  cases k1 with
  | none => cases k2 <;> simp [fA, bothSide, isNN, nnHitA]
  | some x =>
      cases k2 with
      | none => simp [fA, bothSide, isNN, nnHitA]
      | some y =>
          have hnn : nnHitA dB (some x, some y) = true ↔ (some x, some y) ∈ dB := by
            simpa [isNN] using (nnHitA_eq dB (some x, some y))
          by_cases hmem : (some x, some y) ∈ dB
          · have hc : nnHitA dB (some x, some y) = true := hnn.mpr hmem
            simp [fA, bothSide, isNN, hmem, hc]
          · have hc : nnHitA dB (some x, some y) = false :=
              Bool.eq_false_iff.mpr (fun htrue => hmem (hnn.mp htrue))
            simp [fA, bothSide, isNN, hmem, hc]

private theorem fA_rightOnly_false (dB : List Key) (k : Key) :
    rightOnly (fA dB k) = false := by
  rcases k with ⟨k1, k2⟩
  cases k1 with
  | none => cases k2 <;> simp [fA, rightOnly]
  | some x =>
      cases k2 with
      | none => simp [fA, rightOnly]
      | some y =>
          by_cases hmem : (some x, some y) ∈ dB <;> simp [fA, rightOnly, hmem]

private theorem gB_rightOnly_iff (dA : List Key) (k : Key) :
    (match gB dA k with | some p => rightOnly p | none => false) = true ↔
      isPartial k = true ∨ (isNN k = true ∧ nnHitB dA k = false) := by
  by_cases hany : dA.any (fun ak => eqJoinKey ak k) = true
  · have hnn : nnHitB dA k = true := (anyB_eq_nnHitB dA k).mp hany
    have hi : isNN k = true := ((anyB_iff dA k).mp hany).1
    rcases k with ⟨k1, k2⟩
    cases k1 <;> cases k2 <;>
      simp [gB, hany, rightOnly, isPartial, isNN, hnn] at hi ⊢
  · have hanyf : dA.any (fun ak => eqJoinKey ak k) = false := by simpa using hany
    have hnn : nnHitB dA k = false := by
      apply Bool.eq_false_iff.mpr
      intro ht
      exact hany ((anyB_eq_nnHitB dA k).mpr ht)
    rcases k with ⟨k1, k2⟩
    cases k1 <;> cases k2 <;>
      simp [gB, hanyf, rightOnly, isPartial, isNN, hnn]

private theorem gB_other_false (dA : List Key) (P : Option Int × Option Int → Bool)
    (hP : ∀ x : Option Int, P (none, x) = false) (k : Key) :
    (match gB dA k with | some p => P p | none => false) = false := by
  by_cases hany : dA.any (fun ak => eqJoinKey ak k) = true <;> simp [gB, hany, hP]

/-! ## Counts of the pieces -/

private theorem nnHitA_count (dA dB : List Key) :
    ((dA.filterMap nnPair).filter (fun p => (dB.filterMap nnPair).contains p)).length =
      dA.countP (fun k => isNN k && nnHitA dB k) := by
  rw [length_filter_filterMap]
  apply countP_congr
  intro k hk
  unfold nnHitA
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [nnPair, isNN]

private theorem nnMissA_count (dA dB : List Key) :
    ((dA.filterMap nnPair).filter (fun p => !(dB.filterMap nnPair).contains p)).length =
      dA.countP (fun k => isNN k && !nnHitA dB k) := by
  rw [length_filter_filterMap]
  apply countP_congr
  intro k hk
  unfold nnHitA
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [nnPair, isNN]

private theorem nnHitB_count (dB dA : List Key) :
    ((dB.filterMap nnPair).filter (fun p => (dA.filterMap nnPair).contains p)).length =
      dB.countP (fun k => isNN k && nnHitB dA k) := by
  rw [length_filter_filterMap]
  apply countP_congr
  intro k hk
  unfold nnHitB
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [nnPair, isNN]

private theorem nnMissB_count (dB dA : List Key) :
    ((dB.filterMap nnPair).filter (fun p => !(dA.filterMap nnPair).contains p)).length =
      dB.countP (fun k => isNN k && !nnHitB dA k) := by
  rw [length_filter_filterMap]
  apply countP_congr
  intro k hk
  unfold nnHitB
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [nnPair, isNN]

private theorem leftPairs_leftOnly_count (dA dB : List Key) (hB : dB.Nodup) :
    ((dA.flatMap (fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1)))).filter leftOnly).length =
      dA.countP (fun k => isPartial k || (isNN k && !nnHitA dB k)) := by
  rw [leftPairs_eq dA dB hB, length_filter_map]
  apply countP_congr
  intro k hk
  apply Bool.eq_iff_iff.mpr
  simpa [fA_leftOnly_iff, Bool.or_eq_true, Bool.and_eq_true]

private theorem leftPairs_bothSide_count (dA dB : List Key) (hB : dB.Nodup) :
    ((dA.flatMap (fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1)))).filter bothSide).length =
      dA.countP (fun k => isNN k && nnHitA dB k) := by
  rw [leftPairs_eq dA dB hB, length_filter_map]
  apply countP_congr
  intro k hk
  apply Bool.eq_iff_iff.mpr
  simpa [fA_bothSide_iff, Bool.and_eq_true]

private theorem leftPairs_rightOnly_count (dA dB : List Key) (hB : dB.Nodup) :
    ((dA.flatMap (fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1)))).filter rightOnly).length = 0 := by
  rw [leftPairs_eq dA dB hB]
  exact map_count_eq_zero (fA dB) rightOnly dA (fun k => fA_rightOnly_false dB k)

private theorem rightUnmatched_rightOnly_count (dA dB : List Key) :
    ((dB.filterMap (gB dA)).filter rightOnly).length =
      dB.countP (fun k => isPartial k || (isNN k && !nnHitB dA k)) := by
  rw [length_filter_filterMap]
  apply countP_congr
  intro k hk
  apply Bool.eq_iff_iff.mpr
  by_cases hany : dA.any (fun ak => eqJoinKey ak k) = true
  · have hnn : nnHitB dA k = true := (anyB_eq_nnHitB dA k).mp hany
    have hi : isNN k = true := ((anyB_iff dA k).mp hany).1
    rcases k with ⟨k1, k2⟩
    cases k1 <;> cases k2 <;>
      simp [gB, hany, rightOnly, isPartial, isNN, hnn, Bool.or_eq_true, Bool.and_eq_true] at hi ⊢
  · have hanyf : dA.any (fun ak => eqJoinKey ak k) = false := by simpa using hany
    have hnn : nnHitB dA k = false := by
      apply Bool.eq_false_iff.mpr
      intro ht
      exact hany ((anyB_eq_nnHitB dA k).mpr ht)
    rcases k with ⟨k1, k2⟩
    cases k1 <;> cases k2 <;>
      simp [gB, hanyf, rightOnly, isPartial, isNN, hnn, Bool.or_eq_true, Bool.and_eq_true]

private theorem rightUnmatched_leftOnly_count (dA dB : List Key) :
    ((dB.filterMap (gB dA)).filter leftOnly).length = 0 := by
  rw [length_filter_filterMap]
  induction dB with
  | nil => rfl
  | cons bk rest ih =>
      rw [List.countP_cons, ih]
      by_cases hany : dA.any (fun ak => eqJoinKey ak bk) = true <;>
        simp [gB, hany, leftOnly]

private theorem rightUnmatched_bothSide_count (dA dB : List Key) :
    ((dB.filterMap (gB dA)).filter bothSide).length = 0 := by
  rw [length_filter_filterMap]
  induction dB with
  | nil => rfl
  | cons bk rest ih =>
      rw [List.countP_cons, ih]
      by_cases hany : dA.any (fun ak => eqJoinKey ak bk) = true <;>
        simp [gB, hany, bothSide]

/-! ## Main induction-free equivalence -/

private theorem disjoint_partial_nn (dA dB : List Key) :
    ∀ k ∈ dA, ¬(isPartial k = true ∧ (isNN k && !nnHitA dB k) = true) := by
  intro k hk h
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [isPartial, isNN] at h

private theorem disjoint_partial_nnB (dB dA : List Key) :
    ∀ k ∈ dB, ¬(isPartial k = true ∧ (isNN k && !nnHitB dA k) = true) := by
  intro k hk h
  rcases k with ⟨k1, k2⟩
  cases k1 <;> cases k2 <;> simp [isPartial, isNN] at h

private theorem main (dA dB : List Key) (hA : dA.Nodup) (hB : dB.Nodup) :
    optOn dA dB = refOn dA dB := by
  simp only [refOn, optOn, ThreeCounts.mk.injEq]
  constructor
  · -- storeOnly
    have hsplit := countP_or_disjoint isPartial (fun k => isNN k && !nnHitA dB k) dA
      (disjoint_partial_nn dA dB)
    rw [nnMissA_count, ← List.countP_eq_length_filter]
    rw [List.filter_append, List.length_append]
    rw [leftPairs_leftOnly_count dA dB hB, rightUnmatched_leftOnly_count dA dB]
    rw [Nat.add_zero, hsplit]
    omega
  constructor
  · -- catalogOnly
    have hsplit := countP_or_disjoint isPartial (fun k => isNN k && !nnHitB dA k) dB
      (disjoint_partial_nnB dB dA)
    rw [nnMissB_count, ← List.countP_eq_length_filter]
    rw [List.filter_append, List.length_append]
    rw [leftPairs_rightOnly_count dA dB hB, rightUnmatched_rightOnly_count dA dB]
    rw [Nat.zero_add, hsplit]
    omega
  · -- storeAndCatalog
    rw [nnHitA_count]
    rw [List.filter_append, List.length_append]
    rw [leftPairs_bothSide_count dA dB hB, rightUnmatched_bothSide_count dA dB]
    rw [Nat.add_zero]

theorem sound : TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement.Goal := by
  intro a b
  unfold optBitsetCounts refFullOuterCounts
  change optOn (dedupKeys a) (dedupKeys b) = refOn (dedupKeys a) (dedupKeys b)
  exact main (dedupKeys a) (dedupKeys b) (dedupKeys_nodup a) (dedupKeys_nodup b)

end TinylambOpt.ResearchProofs.FullOuterIndicatorBitsetComplement
