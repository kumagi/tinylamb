import TinylambOpt.ResearchGoals.ExecutionStrategies.VersionedIndexes

namespace TinylambOpt.ResearchProofs.Wave7VersionedIndexes

open TinylambOpt.ResearchGoals.VersionedIndexes

/-! ---------- Generic list helpers ---------- -/

private theorem find?_filter_eq {α : Type} (p q : α → Bool) {l : List α} {b : α}
    (h : l.find? q = some b) (hp : p b = true) : (l.filter p).find? q = some b := by
  rw [List.find?_filter]
  apply List.find?_eq_some_iff_getElem.mpr
  rcases List.find?_eq_some_iff_getElem.mp h with ⟨hbq, i, hi, hli, hbefore⟩
  refine ⟨?_, i, hi, hli, ?_⟩
  · exact decide_eq_true ⟨hp, hbq⟩
  · intro j hj
    have hb := hbefore j hj
    simp at hb ⊢
    exact Or.inr hb

private theorem find?_filter_none {α : Type} (p q : α → Bool) {l : List α}
    (h : l.find? q = none) : (l.filter p).find? q = none := by
  rw [List.find?_filter]
  apply List.find?_eq_none.mpr
  intro x hx
  have hx' := (List.find?_eq_none.mp h) x hx
  intro hboth
  exact hx' (of_decide_eq_true hboth).2

/-! ---------- BatchedLookup ---------- -/

private theorem hm_fold_getD (l : List Nat) (f : Nat → Option Int)
    (m : Std.HashMap Nat (Option Int)) (k : Nat) :
    (l.foldl (fun m k => m.insert k (f k)) m).getD k none =
      if k ∈ l then f k else m.getD k none := by
  induction l generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      by_cases hxk : x == k
      · have hxk' : x = k := by simpa using hxk
        subst x
        simp [Std.HashMap.getD_insert]
      · have hxk' : x ≠ k := by
          intro heq
          apply hxk
          simp [heq]
        rw [Std.HashMap.getD_insert]
        have hkx : ¬ k = x := by intro h; exact hxk' h.symm
        by_cases hmem : k ∈ xs <;> simp [hxk, hkx, hmem]

theorem BatchedLookup_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.BatchedLookup.Goal := by
  intro xs ks t
  unfold probeBatch
  let f := fun k => valueAt xs k t
  apply List.map_congr_left
  intro k hk
  rw [hm_fold_getD]
  simp [List.mem_eraseDups, hk]

/-! ---------- OverlayAggregate ---------- -/

private theorem total_cons (xs : List Entry) (ks : List Nat) (t k : Nat) :
    total xs (k :: ks) t = (valueAt xs k t).getD 0 + total xs ks t := by
  unfold total
  rw [List.filterMap_cons]
  cases h : valueAt xs k t <;> simp [h]

private theorem valueAt_append_key (a b : List Entry) (k t : Nat) :
    (valueAt (a ++ b) k t).getD 0 =
      (valueAt a k t).getD 0 +
        (if (visible a k t).isNone then (valueAt b k t).getD 0 else 0) := by
  unfold valueAt visible
  rw [List.find?_append]
  generalize ha : a.find? (fun e => e.key == k && e.seq ≤ t) = A
  generalize hb : b.find? (fun e => e.key == k && e.seq ≤ t) = B
  cases A <;> cases B <;> simp [ha, hb]

/-! ---------- compact / SnapshotAnchor / BottomTombstone ---------- -/

private def buildStep (floor : Nat) (p : List Nat × List Entry) (e : Entry) : List Nat × List Entry :=
  if floor < e.seq then (p.1, e :: p.2)
  else if p.1.contains e.key then (p.1, p.2)
  else (e.key :: p.1, e :: p.2)

private def build (xs : List Entry) (seen : List Nat) (out : List Entry) (floor : Nat) : List Entry :=
  (xs.foldl (buildStep floor) (seen, out)).2

private def compactSeen (xs : List Entry) (seen : List Nat) (floor : Nat) : List Entry :=
  (build xs seen [] floor).reverse

private theorem foldl_build_snd (xs : List Entry) (seen : List Nat) (out : List Entry) (floor : Nat) :
    (xs.foldl (buildStep floor) (seen, out)).2 =
      (xs.foldl (buildStep floor) (seen, [])).2 ++ out := by
  induction xs generalizing seen out with
  | nil => simp
  | cons e xs ih =>
      simp only [List.foldl_cons]
      by_cases hf : floor < e.seq
      · simp only [buildStep, if_pos hf]
        rw [ih seen (e :: out), ih seen [e]]
        simp [List.append_assoc]
      · by_cases hs : seen.contains e.key
        · simp only [buildStep, if_neg hf, if_pos hs]
          exact ih seen out
        · simp only [buildStep, if_neg hf, if_neg hs]
          rw [ih (e.key :: seen) (e :: out), ih (e.key :: seen) [e]]
          simp [List.append_assoc]

private theorem build_append (xs : List Entry) (seen : List Nat) (out : List Entry) (floor : Nat) :
    build xs seen out floor = build xs seen [] floor ++ out := by
  exact foldl_build_snd xs seen out floor

private theorem compactSeen_cons (e : Entry) (xs : List Entry) (seen : List Nat) (floor : Nat) :
    compactSeen (e :: xs) seen floor =
      if floor < e.seq then e :: compactSeen xs seen floor
      else if seen.contains e.key then compactSeen xs seen floor
      else e :: compactSeen xs (e.key :: seen) floor := by
  unfold compactSeen build
  simp only [List.foldl_cons]
  by_cases hf : floor < e.seq
  · simp only [buildStep, if_pos hf]
    rw [foldl_build_snd]
    simp
  · by_cases hs : seen.contains e.key
    · simp only [buildStep, if_neg hf, if_pos hs]
    · simp only [buildStep, if_neg hf, if_neg hs]
      rw [foldl_build_snd]
      simp

private theorem compact_eq (xs : List Entry) (floor : Nat) :
    compact xs floor = compactSeen xs [] floor := by
  rfl

/-- When every entry is at or below the floor, the first entry per key survives. -/
private theorem compactSeen_sound_le : ∀ (xs : List Entry) (seen : List Nat) (floor k t : Nat),
    ordered xs → (∀ f ∈ xs, f.seq ≤ floor) → floor ≤ t → k ∉ seen →
    visible (compactSeen xs seen floor) k t = visible xs k t := by
  intro xs
  induction xs with
  | nil => intros; simp [compactSeen, build]
  | cons e xs ih =>
      intro seen floor k t hord hall hfk hk
      have hxsord : ordered xs := hord.of_cons
      have heseq : e.seq ≤ floor := hall e (by simp)
      have hf : ¬ floor < e.seq := by omega
      rw [compactSeen_cons]
      by_cases heq : (e.key == k) = true
      · have heqeq : e.key = k := by simpa using heq
        have hseenP : e.key ∉ seen := by simpa [heqeq] using hk
        have hseen : seen.contains e.key = false := by simpa using hseenP
        have het : e.seq ≤ t := by omega
        simp [visible, hf, hseenP, heq, het]
      · by_cases hseenP : e.key ∈ seen
        · simp [visible, hf, hseenP, heq]
          exact ih seen floor k t hxsord (by
            intro f hfmem
            exact hall f (by simp [hfmem])) hfk hk
        · have hk' : k ∉ e.key :: seen := by
            intro hmem
            simp only [List.mem_cons] at hmem
            rcases hmem with hke | hks
            · exact heq (by simpa [hke])
            · exact hk hks
          simp [visible, hf, hseenP, heq]
          exact ih (e.key :: seen) floor k t hxsord (by
            intro f hfmem
            exact hall f (by simp [hfmem])) hfk hk'

private theorem compact_sound : ∀ (xs : List Entry) (floor k t : Nat),
    ordered xs → floor ≤ t →
    visible (compact xs floor) k t = visible xs k t := by
  intro xs
  induction xs with
  | nil => intros; simp [compact]
  | cons e xs ih =>
      intro floor k t hord hft
      have hxsord : ordered xs := hord.of_cons
      rw [compact_eq, compactSeen_cons]
      by_cases heq : (e.key == k) = true
      · by_cases hf : floor < e.seq
        · by_cases het : e.seq ≤ t
          · simp [visible, hf, heq, het]
          · simp [visible, hf, heq, het]
            rw [← compact_eq xs floor]
            exact ih floor k t hxsord hft
        · have het : e.seq ≤ t := by omega
          simp [visible, hf, heq, het]
      · by_cases hf : floor < e.seq
        · simp [visible, hf, heq]
          rw [← compact_eq xs floor]
          exact ih floor k t hxsord hft
        · simp [visible, hf, heq]
          have hall : ∀ f ∈ xs, f.seq ≤ floor := by
            intro f hfmem
            have hrel := (List.pairwise_cons.mp hord).1 f hfmem
            omega
          have hk' : k ∉ [e.key] := by
            intro hmem
            simp only [List.mem_singleton] at hmem
            exact heq (by simpa [hmem])
          exact compactSeen_sound_le xs [e.key] floor k t hxsord hall hft hk'

theorem SnapshotAnchor_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.SnapshotAnchor.Goal := by
  intro xs oldest k t hord hft
  exact compact_sound xs oldest k t hord hft

/-! ---------- BottomTombstone ---------- -/

private theorem visible_cons_match {xs : List Entry} {k t : Nat} {e : Entry}
    (h : (e.key == k && e.seq ≤ t) = true) : visible (e :: xs) k t = some e := by
  simp [visible, h]

private theorem visible_cons_nomatch {xs : List Entry} {k t : Nat} {e : Entry}
    (h : (e.key == k && e.seq ≤ t) = false) : visible (e :: xs) k t = visible xs k t := by
  simp [visible, h]

private theorem valueAt_cons_match {xs : List Entry} {k t : Nat} {e : Entry}
    (h : (e.key == k && e.seq ≤ t) = true) : valueAt (e :: xs) k t = e.value := by
  simp [valueAt, visible_cons_match h]

private theorem valueAt_cons_nomatch {xs : List Entry} {k t : Nat} {e : Entry}
    (h : (e.key == k && e.seq ≤ t) = false) : valueAt (e :: xs) k t = valueAt xs k t := by
  simp [valueAt, visible_cons_nomatch h]

private theorem valueAt_of_visible_none {xs : List Entry} {k t : Nat}
    (h : visible xs k t = none) : valueAt xs k t = none := by
  simp [valueAt, h]

private theorem compactSeen_cons_of_lt {e : Entry} {xs : List Entry} {seen : List Nat} {floor : Nat}
    (h : floor < e.seq) : compactSeen (e :: xs) seen floor = e :: compactSeen xs seen floor := by
  rw [compactSeen_cons, if_pos h]

private theorem compactSeen_cons_of_drop {e : Entry} {xs : List Entry} {seen : List Nat} {floor : Nat}
    (h1 : ¬ floor < e.seq) (h2 : e.key ∈ seen) :
    compactSeen (e :: xs) seen floor = compactSeen xs seen floor := by
  rw [compactSeen_cons, if_neg h1, if_pos (by simpa using h2)]

private theorem compactSeen_cons_of_keep {e : Entry} {xs : List Entry} {seen : List Nat} {floor : Nat}
    (h1 : ¬ floor < e.seq) (h2 : e.key ∉ seen) :
    compactSeen (e :: xs) seen floor = e :: compactSeen xs (e.key :: seen) floor := by
  rw [compactSeen_cons, if_neg h1, if_neg (by simpa using h2)]

private theorem valueAt_filter_of_visible_none {xs : List Entry} {k t : Nat}
    (p : Entry → Bool) (h : visible xs k t = none) :
    valueAt (xs.filter p) k t = none := by
  have hv0 : xs.find? (fun e => e.key == k && e.seq ≤ t) = none := by
    simpa [visible] using h
  have hv1 : (xs.filter p).find? (fun e => e.key == k && e.seq ≤ t) = none :=
    find?_filter_none p (fun e => e.key == k && e.seq ≤ t) hv0
  simp [valueAt, visible, hv1]

private theorem compactSeen_none_of_mem : ∀ (xs : List Entry) (seen : List Nat) (floor k t : Nat),
    ordered xs → (∀ f ∈ xs, f.seq ≤ floor) → k ∈ seen →
    visible (compactSeen xs seen floor) k t = none := by
  intro xs
  induction xs with
  | nil => intros; rfl
  | cons e xs ih =>
      intro seen floor k t hord hall hkmem
      have hf : ¬ floor < e.seq := by
        have := hall e (by simp)
        omega
      have hall' : ∀ f ∈ xs, f.seq ≤ floor := by
        intro f hfmem
        exact hall f (by simp [hfmem])
      by_cases hes : e.key ∈ seen
      · rw [compactSeen_cons_of_drop hf hes]
        exact ih seen floor k t hord.of_cons hall' hkmem
      · rw [compactSeen_cons_of_keep hf hes]
        have heqEq : e.key ≠ k := by
          intro heqk
          exact hes (by simpa [heqk] using hkmem)
        have hnomatch : (e.key == k && e.seq ≤ t) = false := by simp [heqEq]
        rw [visible_cons_nomatch hnomatch]
        exact ih (e.key :: seen) floor k t hord.of_cons hall' (by simp [hkmem])

private theorem dropTomb_compactSeen_sound : ∀ (xs : List Entry) (seen : List Nat) (floor k t : Nat),
    ordered xs → (∀ f ∈ xs, f.seq ≤ floor) → floor ≤ t → k ∉ seen →
    valueAt ((compactSeen xs seen floor).filter (fun e => floor < e.seq || e.value.isSome)) k t =
      valueAt xs k t := by
  intro xs
  induction xs with
  | nil => intros; rfl
  | cons e xs ih =>
      intro seen floor k t hord hall hft hk
      have hf : ¬ floor < e.seq := by
        have := hall e (by simp)
        omega
      have hall' : ∀ f ∈ xs, f.seq ≤ floor := by
        intro f hfmem
        exact hall f (by simp [hfmem])
      by_cases heq : (e.key == k) = true
      · have hEq : e.key = k := by simpa using heq
        have hes : e.key ∉ seen := by simpa [hEq] using hk
        rw [compactSeen_cons_of_keep hf hes]
        have het : e.seq ≤ t := by
          have := hall e (by simp)
          omega
        have hmatch : (e.key == k && e.seq ≤ t) = true := by simp [heq, het]
        cases hval : e.value with
        | none =>
            have hnone := compactSeen_none_of_mem xs (e.key :: seen) floor k t hord.of_cons hall'
              (by
                have hEq : e.key = k := by simpa using heq
                simp [hEq])
            have hvnone : valueAt ((compactSeen xs (e.key :: seen) floor).filter
                (fun e => floor < e.seq || e.value.isSome)) k t = none :=
              valueAt_filter_of_visible_none _ hnone
            have hkeep : (floor < e.seq || e.value.isSome) = false := by simp [hf, hval]
            rw [List.filter_cons]
            simp [hkeep, hf, valueAt_cons_match hmatch, hval, hvnone]
        | some v =>
            have hkeep : (floor < e.seq || e.value.isSome) = true := by simp [hval]
            rw [List.filter_cons]
            simp [hkeep, hf, valueAt_cons_match hmatch, hval]
      · by_cases hes : e.key ∈ seen
        · rw [compactSeen_cons_of_drop hf hes]
          have hnomatch : (e.key == k && e.seq ≤ t) = false := by
            have heqEq : e.key ≠ k := by
              intro heqk
              exact heq (by simpa [heqk])
            simp [heqEq]
          rw [valueAt_cons_nomatch hnomatch]
          exact ih seen floor k t hord.of_cons hall' hft hk
        · rw [compactSeen_cons_of_keep hf hes]
          have hk' : k ∉ e.key :: seen := by
            intro hmem
            simp only [List.mem_cons] at hmem
            rcases hmem with hke | hks
            · exact heq (by simpa [hke])
            · exact hk hks
          have hnomatch : (e.key == k && e.seq ≤ t) = false := by
            have heqEq : e.key ≠ k := by
              intro heqk
              exact heq (by simpa [heqk])
            simp [heqEq]
          by_cases hkeep : (floor < e.seq || e.value.isSome) = true
          · rw [List.filter_cons]
            simp [hkeep]
            rw [valueAt_cons_nomatch hnomatch]
            rw [valueAt_cons_nomatch hnomatch]
            exact ih (e.key :: seen) floor k t hord.of_cons hall' hft hk'
          · rw [List.filter_cons]
            simp [hkeep]
            rw [valueAt_cons_nomatch hnomatch]
            exact ih (e.key :: seen) floor k t hord.of_cons hall' hft hk'

private theorem bottomTombstone_aux : ∀ (xs : List Entry) (floor k t : Nat),
    ordered xs → floor ≤ t →
    valueAt ((compact xs floor).filter (fun e => floor < e.seq || e.value.isSome)) k t =
      valueAt xs k t := by
  intro xs
  induction xs with
  | nil => intros; simp [compact]
  | cons e xs ih =>
      intro floor k t hord hft
      have hxsord : ordered xs := hord.of_cons
      rw [compact_eq]
      by_cases heq : (e.key == k) = true
      · by_cases hf : floor < e.seq
        · rw [compactSeen_cons_of_lt hf]
          by_cases het : e.seq ≤ t
          · have hmatch : (e.key == k && e.seq ≤ t) = true := by simp [heq, het]
            have hkeep : (floor < e.seq || e.value.isSome) = true := by simp [hf]
            rw [List.filter_cons]
            simp [hkeep, hf, valueAt_cons_match hmatch]
          · have hnomatch : (e.key == k && e.seq ≤ t) = false := by simp [heq, het]
            have hkeep : (floor < e.seq || e.value.isSome) = true := by simp [hf]
            rw [List.filter_cons]
            simp [hkeep, hf, valueAt_cons_nomatch hnomatch]
            rw [← compact_eq xs floor]
            exact ih floor k t hxsord hft
        · rw [compactSeen_cons_of_keep hf (by simp)]
          have hall' : ∀ f ∈ xs, f.seq ≤ floor := by
            intro f hfmem
            have := (List.pairwise_cons.mp hord).1 f hfmem
            omega
          have het : e.seq ≤ t := by omega
          have hmatch : (e.key == k && e.seq ≤ t) = true := by simp [heq, het]
          cases hval : e.value with
          | none =>
              have hnone := compactSeen_none_of_mem xs [e.key] floor k t hxsord hall'
                (by
                  have hEq : e.key = k := by simpa using heq
                  simp [hEq])
              have hvnone : valueAt ((compactSeen xs [e.key] floor).filter
                  (fun e => floor < e.seq || e.value.isSome)) k t = none :=
                valueAt_filter_of_visible_none _ hnone
              have hkeep : (floor < e.seq || e.value.isSome) = false := by simp [hf, hval]
              rw [List.filter_cons]
              simp [hkeep, hf, valueAt_cons_match hmatch, hval, hvnone]
          | some v =>
              have hkeep : (floor < e.seq || e.value.isSome) = true := by simp [hval]
              rw [List.filter_cons]
              simp [hkeep, hf, valueAt_cons_match hmatch, hval]
      · by_cases hf : floor < e.seq
        · rw [compactSeen_cons_of_lt hf]
          have hnomatch : (e.key == k && e.seq ≤ t) = false := by
            have heqEq : e.key ≠ k := by intro heqk; exact heq (by simpa [heqk])
            simp [heqEq]
          have hkeep : (floor < e.seq || e.value.isSome) = true := by simp [hf]
          rw [List.filter_cons]
          simp [hkeep, valueAt_cons_nomatch hnomatch]
          rw [← compact_eq xs floor]
          exact ih floor k t hxsord hft
        · rw [compactSeen_cons_of_keep hf (by simp)]
          have hall' : ∀ f ∈ xs, f.seq ≤ floor := by
            intro f hfmem
            have := (List.pairwise_cons.mp hord).1 f hfmem
            omega
          have hnomatch : (e.key == k && e.seq ≤ t) = false := by
            have heqEq : e.key ≠ k := by intro heqk; exact heq (by simpa [heqk])
            simp [heqEq]
          by_cases hkeep : (floor < e.seq || e.value.isSome) = true
          · rw [List.filter_cons]
            simp [hkeep, valueAt_cons_nomatch hnomatch]
            exact dropTomb_compactSeen_sound xs [e.key] floor k t hxsord hall' hft (by
              intro hmem
              simp only [List.mem_singleton] at hmem
              exact heq (by simpa [hmem]))
          · rw [List.filter_cons]
            simp [hkeep]
            rw [valueAt_cons_nomatch hnomatch]
            exact dropTomb_compactSeen_sound xs [e.key] floor k t hxsord hall' hft (by
              intro hmem
              simp only [List.mem_singleton] at hmem
              exact heq (by simpa [hmem]))

theorem BottomTombstone_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.BottomTombstone.Goal := by
  intro xs oldest k t hord hft
  exact bottomTombstone_aux xs oldest k t hord hft

/-! ---------- SparsePins ---------- -/

private theorem entry_beq_self (e : Entry) : (e == e) = true := by
  change TinylambOpt.ResearchGoals.VersionedIndexes.instBEqEntry.beq e e = true
  rw [TinylambOpt.ResearchGoals.VersionedIndexes.instBEqEntry.beq]
  cases e <;> simp

private theorem find?_of_newer {xs : List Entry} {k t t' : Nat} {e : Entry}
    (h : xs.find? (fun f => f.key == k && f.seq ≤ t) = some e)
    (heq : e.seq ≤ t') (htt' : t' ≤ t) :
    xs.find? (fun f => f.key == e.key && f.seq ≤ t') = some e := by
  rcases List.find?_eq_some_iff_getElem.mp h with ⟨hp, i, hi, hli, hbefore⟩
  have hp' : (e.key == k) = true := by
    have := hp
    rw [Bool.and_eq_true] at this
    exact this.1
  apply List.find?_eq_some_iff_getElem.mpr
  constructor
  · rw [Bool.and_eq_true]
    exact ⟨by simp, decide_eq_true heq⟩
  · refine ⟨i, hi, hli, ?_⟩
    intro j hj
    have hb : ¬ xs[j].key = k ∨ t < xs[j].seq := by
      simpa using hbefore j hj
    by_cases hm : (xs[j].key == e.key && xs[j].seq ≤ t') = true
    · exfalso
      have hm' := hm
      rw [Bool.and_eq_true] at hm'
      have h1 : xs[j].key = e.key := by simpa using hm'.1
      have h2 : e.key = k := by simpa using hp'
      have hjkey : xs[j].key = k := h1.trans h2
      have hjseq' : xs[j].seq ≤ t' := of_decide_eq_true hm'.2
      have hjseq : xs[j].seq ≤ t := by omega
      exact hb.elim (fun hk => hk hjkey) (fun ht => by omega)
    · simpa [hm]

theorem SparsePins_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.SparsePins.Goal := by
  intro xs pins latest k t hord hcond
  unfold visible
  unfold keepPinned
  let pred := fun e : Entry => e.key == k && e.seq ≤ t
  let keep := fun e : Entry =>
    latest < e.seq || (latest :: pins).any fun u =>
      (xs.find? fun f => f.key == e.key && f.seq ≤ u) == some e
  change (xs.filter keep).find? pred = xs.find? pred
  cases hv : xs.find? pred with
  | none =>
      exact find?_filter_none keep pred hv
  | some e =>
      have hp : (e.key == k && e.seq ≤ t) = true := by
        simpa [pred] using List.find?_some hv
      have hkey : (e.key == k) = true := by
        have hp' := hp
        rw [Bool.and_eq_true] at hp'
        exact hp'.1
      have heseq : e.seq ≤ t := by
        have hp' := hp
        rw [Bool.and_eq_true] at hp'
        exact of_decide_eq_true hp'.2
      have hkeep : keep e = true := by
        by_cases hlt : latest < e.seq
        · simp [keep, hlt]
        · have heseq_latest : e.seq ≤ latest := by omega
          rcases hcond with htpin | hlatest
          · have hvis : (xs.find? fun f => f.key == e.key && f.seq ≤ t) = some e := by
              have hkeq : e.key = k := by simpa using hkey
              simpa [pred, hkeq] using hv
            simp [keep, hlt]
            right
            exact ⟨t, htpin, by simp [hvis, entry_beq_self]⟩
          · have hvlatest : xs.find? (fun f => f.key == e.key && f.seq ≤ latest) = some e :=
              find?_of_newer hv heseq_latest hlatest
            simp [keep, hlt, hvlatest, entry_beq_self]
      exact find?_filter_eq keep pred hv hkeep

/-! ---------- CoveredPredicate ---------- -/

theorem CoveredPredicate_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.CoveredPredicate.Goal := by
  intro xs p k t hord huniq
  unfold valueAt
  cases hv : visible xs k t with
  | none =>
      by_cases hR : xs.any (fun e => e.key == k && e.seq ≤ t && e.value.any p &&
          !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t)) = true
      · rcases List.any_eq_true.mp hR with ⟨w, hwmem, hw⟩
        simp only [Bool.and_eq_true] at hw
        have hwP : (w.key == k && w.seq ≤ t) = true := by
          rw [Bool.and_eq_true]
          exact ⟨hw.1.1.1, decide_eq_true (of_decide_eq_true hw.1.1.2)⟩
        have hsome : (visible xs k t).isSome = true := by
          rw [visible]
          exact List.find?_isSome.mpr ⟨w, hwmem, hwP⟩
        simp [hv] at hsome
      · simp [hR]
  | some e =>
      have hvf : xs.find? (fun f => f.key == k && f.seq ≤ t) = some e := by
        simpa [visible] using hv
      rcases List.find?_eq_some_iff_getElem.mp hvf with ⟨hp, i, hi, hli, hbefore⟩
      have hkey : (e.key == k) = true := by
        have hp' := hp
        rw [Bool.and_eq_true] at hp'
        exact hp'.1
      have heseq : e.seq ≤ t := by
        have hp' := hp
        rw [Bool.and_eq_true] at hp'
        exact of_decide_eq_true hp'.2
      have hemem : e ∈ xs := List.mem_of_find?_eq_some hvf
      by_cases hpE : e.value.any p = true
      · have hno : (xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t) = false := by
          apply List.any_eq_false.mpr
          intro f hfmem hfw
          simp only [Bool.and_eq_true] at hfw
          have hfkey : (f.key == k) = true := hfw.1.1
          have hfnew : e.seq < f.seq := of_decide_eq_true hfw.1.2
          have hfseq : f.seq ≤ t := of_decide_eq_true hfw.2
          rcases List.mem_iff_getElem.mp hfmem with ⟨j, hjlen, hfj⟩
          by_cases hji : j = i
          · subst j
            have hfe : f = e := hfj.symm.trans hli
            have : e.seq < e.seq := by simpa [hfe] using hfnew
            omega
          · by_cases hlt : j < i
            · have hb := hbefore j hlt
              simp at hb
              exfalso
              exact hb.elim
                (fun hk => hk (by simpa [hfj] using hfkey))
                (fun ht => by
                  have hxj : xs[j].seq = f.seq := by rw [hfj]
                  omega)
            · have hij : i < j := by omega
              have hseqle := hord.rel_getElem_of_lt hi hjlen hij
              have hxj : xs[j].seq = f.seq := by rw [hfj]
              have hxi : xs[i].seq = e.seq := by rw [hli]
              omega
        have hR : xs.any (fun e => e.key == k && e.seq ≤ t && e.value.any p &&
            !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t)) = true := by
          apply List.any_eq_true.mpr
          refine ⟨e, hemem, ?_⟩
          rw [Bool.and_eq_true]
          refine ⟨?_, by simp [hno]⟩
          rw [Bool.and_eq_true]
          refine ⟨?_, hpE⟩
          rw [Bool.and_eq_true]
          exact ⟨hkey, decide_eq_true heseq⟩
        simp [hpE, hR]
      · by_cases hR : xs.any (fun e => e.key == k && e.seq ≤ t && e.value.any p &&
            !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t)) = true
        · rcases List.any_eq_true.mp hR with ⟨w, hwmem, hw⟩
          simp only [Bool.and_eq_true] at hw
          have hwkey : (w.key == k) = true := hw.1.1.1
          have hwseq : w.seq ≤ t := of_decide_eq_true hw.1.1.2
          have hwval : w.value.any p = true := hw.1.2
          have hwno : (xs.any fun f => f.key == k && w.seq < f.seq && f.seq ≤ t) = false := by
            simpa using hw.2
          rcases List.mem_iff_getElem.mp hwmem with ⟨j, hjlen, hwj⟩
          have hnotlt : ¬ j < i := by
            intro hlt
            have hb := hbefore j hlt
            simp at hb
            exact hb.elim
              (fun hk => hk (by simpa [hwj] using hwkey))
              (fun ht => by
                have hxj : xs[j].seq = w.seq := by rw [hwj]
                omega)
          by_cases hji : j = i
          · subst j
            have hwe : w = e := hwj.symm.trans hli
            have hvale : e.value.any p = true := by simpa [hwe] using hwval
            exact (hpE hvale).elim
          · have hij : i < j := by omega
            have hseqle := hord.rel_getElem_of_lt hi hjlen hij
            have hxj : xs[j].seq = w.seq := by rw [hwj]
            have hxi : xs[i].seq = e.seq := by rw [hli]
            have hnotnew : ¬ w.seq < e.seq := by
              intro hwlt
              have hany : (xs.any fun f => f.key == k && w.seq < f.seq && f.seq ≤ t) = true := by
                apply List.any_eq_true.mpr
                refine ⟨e, hemem, ?_⟩
                rw [Bool.and_eq_true]
                refine ⟨?_, decide_eq_true heseq⟩
                rw [Bool.and_eq_true]
                exact ⟨hkey, decide_eq_true hwlt⟩
              rw [hany] at hwno
              simp at hwno
            have hweq : w.seq = e.seq := by omega
            have hwkey' : w.key = k := by simpa using hwkey
            have hekey' : e.key = k := by simpa using hkey
            have hi_map : i < (xs.map fun z : Entry => (z.key, z.seq)).length := by simpa using hi
            have hj_map : j < (xs.map fun z : Entry => (z.key, z.seq)).length := by simpa using hjlen
            have hpair : (xs.map fun z : Entry => (z.key, z.seq))[i] =
                (xs.map fun z : Entry => (z.key, z.seq))[j] := by
              rw [List.getElem_map (f := fun z : Entry => (z.key, z.seq)) (h := hi_map),
                  List.getElem_map (f := fun z : Entry => (z.key, z.seq)) (h := hj_map)]
              simp [hli, hwj, hwkey', hekey', hweq]
            have hij' : i = j := (huniq.getElem_inj (hi := hi_map) (hj := hj_map)).mp hpair
            exfalso
            exact hji hij'.symm
        · simp [hpE, hR]

/-! ---------- RangeTombstone ---------- -/

private def cmpR (a b : Entry) : Bool :=
  a.seq > b.seq || (a.seq == b.seq && (a.value.isNone || b.value.isSome))

private theorem cmpR_seq {a b : Entry} (h : cmpR a b = true) : b.seq ≤ a.seq := by
  unfold cmpR at h
  simp at h
  rcases h with hgt | hrest
  · omega
  · have hs : a.seq = b.seq := by simpa using hrest.1
    omega

private theorem cmpR_trans : ∀ a b c : Entry, cmpR a b = true → cmpR b c = true → cmpR a c = true := by
  intro a b c hab hbc
  unfold cmpR at *
  simp at hab hbc ⊢
  rcases hab with hgt | hrest
  · rcases hbc with hgt' | hrest'
    · omega
    · omega
  · rcases hbc with hgt' | hrest'
    · omega
    · rcases hrest with ⟨habseq, habval⟩
      rcases hrest' with ⟨hbcseq, hbcval⟩
      by_cases hc : c.value.isSome = true
      · right; constructor
        · omega
        · right; exact hc
      · have hbnone : b.value = none := by
          cases hbv : b.value with
          | none => rfl
          | some v =>
              exfalso
              have hcsome : c.value.isSome = true := by
                rcases hbcval with hbnone' | hc'
                · simp [hbv] at hbnone'
                · exact hc'
              exact hc hcsome
        have hanone : a.value = none := by
          cases hav : a.value with
          | none => rfl
          | some v =>
              exfalso
              have hbsome : b.value.isSome = true := by
                rcases habval with hanone' | hbs'
                · simp [hav] at hanone'
                · exact hbs'
              simp [hbnone] at hbsome
        right; constructor
        · omega
        · left; simp [hanone]

private theorem cmpR_total : ∀ a b : Entry, (cmpR a b || cmpR b a) = true := by
  intro a b
  unfold cmpR
  by_cases hlt : a.seq < b.seq
  · have hgt : b.seq > a.seq := by omega
    have hn : ¬ a.seq > b.seq := by omega
    simp [hgt, hn]
  · by_cases hgt : a.seq > b.seq
    · have hn : ¬ b.seq > a.seq := by omega
      simp [hgt, hn]
    · have heq : a.seq = b.seq := by omega
      rw [heq]
      cases ha : a.value <;> cases hb : b.value <;> simp

private theorem visible_match {xs : List Entry} {k t : Nat} {e : Entry}
    (h : visible xs k t = some e) : (e.key == k && e.seq ≤ t) = true := by
  have hf : xs.find? (fun f => f.key == k && f.seq ≤ t) = some e := by
    simpa [visible] using h
  exact (List.find?_eq_some_iff_getElem.mp hf).1

theorem RangeTombstone_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.RangeTombstone.Goal := by
  intro xs ms k t hord huniq
  let markers := (ms.filter fun m => covers m k m.seq).map fun m => Entry.mk k m.seq none
  let L := xs ++ markers
  let SC := L.mergeSort cmpR
  have hsorted : SC.Pairwise (fun a b => cmpR a b = true) := by
    unfold SC L
    exact List.pairwise_mergeSort cmpR_trans cmpR_total _
  have hSC_of_xs : ∀ e, e ∈ xs → e ∈ SC := by
    intro e he
    unfold SC L
    exact List.mem_mergeSort.mpr (List.mem_append_left markers he)
  have hSC_of_marker : ∀ e, e ∈ markers → e ∈ SC := by
    intro e he
    unfold SC L
    exact List.mem_mergeSort.mpr (List.mem_append_right xs he)
  unfold applyMarkers
  change valueAt SC k t =
    ((visible xs k t).bind fun e =>
      if (ms.filter (fun m => covers m k t)).any (fun m => e.seq ≤ m.seq) then none else e.value)

  cases hv : visible SC k t with
  | none =>
      cases hvx : visible xs k t with
      | none => simp [valueAt, hv, hvx]
      | some e =>
          have hpred : (e.key == k && e.seq ≤ t) = true := visible_match hvx
          have hsome : (visible SC k t).isSome = true := by
            rw [visible]
            exact List.find?_isSome.mpr ⟨e,
              hSC_of_xs e (List.mem_of_find?_eq_some (by simpa [visible] using hvx)), hpred⟩
          simp [hv] at hsome
  | some z =>
      have hvf : SC.find? (fun e => e.key == k && e.seq ≤ t) = some z := by
        simpa [visible] using hv
      rcases List.find?_eq_some_iff_getElem.mp hvf with ⟨hpz, i, hi, hiz, hbefore⟩
      have hzkey : (z.key == k) = true := by
        have hp := hpz; rw [Bool.and_eq_true] at hp; exact hp.1
      have hzseq : z.seq ≤ t := by
        have hp := hpz; rw [Bool.and_eq_true] at hp; exact of_decide_eq_true hp.2
      have hzmem : z ∈ L := by
        have : z ∈ SC := List.mem_of_find?_eq_some hvf
        simpa [SC] using (List.mem_mergeSort.mp this)
      cases hvx : visible xs k t with
      | none =>
          have hzmarkers : z ∈ markers := by
            rcases List.mem_append.mp hzmem with hzxs | hzm
            · exfalso
              have hpred : (z.key == k && z.seq ≤ t) = true := by
                rw [Bool.and_eq_true]; exact ⟨hzkey, decide_eq_true hzseq⟩
              have hs : (visible xs k t).isSome = true := by
                rw [visible]; exact List.find?_isSome.mpr ⟨z, hzxs, hpred⟩
              simp [hvx] at hs
            · exact hzm
          have hzval : z.value = none := by
            rcases List.mem_map.mp hzmarkers with ⟨m, hmfilter, hmz⟩
            have htmp := congrArg Entry.value hmz
            simpa using htmp.symm
          simp [valueAt, hv, hvx, hzval]
      | some e =>
          have hpred_e : (e.key == k && e.seq ≤ t) = true := visible_match hvx
          have hekey : (e.key == k) = true := by
            have hp := hpred_e; rw [Bool.and_eq_true] at hp; exact hp.1
          have heseq : e.seq ≤ t := by
            have hp := hpred_e; rw [Bool.and_eq_true] at hp; exact of_decide_eq_true hp.2
          have hemem : e ∈ xs := List.mem_of_find?_eq_some (by simpa [visible] using hvx)
          have hSCe : e ∈ SC := hSC_of_xs e hemem
          rcases List.mem_iff_getElem.mp hSCe with ⟨j, hjlen, hje⟩
          have hjge : i ≤ j := by
            by_cases hjlt : j < i
            · have hb := hbefore j hjlt
              simp at hb
              have hbtrue : (SC[j].key == k && SC[j].seq ≤ t) = true := by
                rw [hje]; exact hpred_e
              rw [Bool.and_eq_true] at hbtrue
              exfalso
              exact hb.elim (fun hk => hk (by simpa using hbtrue.1))
                (fun ht => by have := of_decide_eq_true hbtrue.2; omega)
            · omega
          have hseqle : e.seq ≤ z.seq := by
            by_cases hji : j = i
            · subst j
              rw [hiz] at hje
              have hez : e = z := hje.symm
              subst e
              omega
            · have hij : i < j := by omega
              have hrel := hsorted.rel_getElem_of_lt hi hjlen hij
              rw [hiz, hje] at hrel
              exact cmpR_seq hrel
          rcases List.mem_append.mp hzmem with hzxs | hzmarkers
          · have hzseqle : z.seq ≤ e.seq := by
              by_cases hlt : e.seq < z.seq
              · rcases List.find?_eq_some_iff_getElem.mp (by simpa [visible] using hvx) with
                  ⟨_, ie2, hie2len, hie2, hbeforexs⟩
                rcases List.mem_iff_getElem.mp hzxs with ⟨iz, hizlenxs, hizz⟩
                have hizlt : iz < ie2 := by
                  by_cases hb : iz < ie2
                  · exact hb
                  · have hle : ie2 ≤ iz := by omega
                    have hne : ie2 ≠ iz := by
                      intro heq
                      subst ie2
                      have hez' : e = z := hie2.symm.trans hizz
                      have hseqeq : e.seq = z.seq := by rw [hez']
                      omega
                    have hlt2 : ie2 < iz := by omega
                    have hrel := hord.rel_getElem_of_lt hie2len hizlenxs hlt2
                    have hzle : z.seq ≤ e.seq := by simpa [hie2, hizz] using hrel
                    omega
                have hbxs0 := hbeforexs iz hizlt
                simp at hbxs0
                rw [hizz] at hbxs0
                have hzpred : (z.key == k && z.seq ≤ t) = true := by
                  rw [Bool.and_eq_true]; exact ⟨hzkey, decide_eq_true hzseq⟩
                exfalso
                exact hbxs0.elim
                  (fun hk => hk (by simpa using hzkey))
                  (fun ht => by omega)
              · exact Nat.le_of_not_gt hlt
            have hzeqseq : z.seq = e.seq := by omega
            have hzkey' : z.key = k := by simpa using hzkey
            have hekey' : e.key = k := by simpa using hekey
            have hze : z = e := by
              rcases List.mem_iff_getElem.mp hzxs with ⟨iz, hizlen, hizz⟩
              rcases List.mem_iff_getElem.mp hemem with ⟨ie, hielen, hie⟩
              have hi_map : iz < (xs.map fun q : Entry => (q.key, q.seq)).length := by simpa using hizlen
              have hj_map : ie < (xs.map fun q : Entry => (q.key, q.seq)).length := by simpa using hielen
              have hpair : (xs.map fun q : Entry => (q.key, q.seq))[iz] =
                  (xs.map fun q : Entry => (q.key, q.seq))[ie] := by
                rw [List.getElem_map (f := fun q : Entry => (q.key, q.seq)) (h := hi_map),
                    List.getElem_map (f := fun q : Entry => (q.key, q.seq)) (h := hj_map)]
                simp [hizz, hie, hzkey', hekey', hzeqseq]
              have hidx : iz = ie := (huniq.getElem_inj (hi := hi_map) (hj := hj_map)).mp hpair
              cases hidx
              exact hizz.symm.trans hie
            by_cases hval : e.value = none
            · simp [valueAt, hv, hvx, hze, hval]
            · have hno : (ms.filter (fun m => covers m k t)).any (fun m => e.seq ≤ m.seq) = false := by
                by_cases hany : (ms.filter (fun m => covers m k t)).any (fun m => e.seq ≤ m.seq) = true
                · rcases List.any_eq_true.mp hany with ⟨m, hmfilter, hmleB⟩
                  have hmle : e.seq ≤ m.seq := of_decide_eq_true hmleB
                  have hcovt : covers m k t := (List.mem_filter.mp hmfilter).2
                  have hcovseq : covers m k m.seq := by
                    rw [covers] at hcovt ⊢
                    simp at hcovt ⊢
                    omega
                  have hymark : Entry.mk k m.seq none ∈ markers := by
                    unfold markers
                    exact List.mem_map.mpr ⟨m,
                      List.mem_filter.mpr ⟨(List.mem_filter.mp hmfilter).1, hcovseq⟩, rfl⟩
                  have hySC : Entry.mk k m.seq none ∈ SC := hSC_of_marker _ hymark
                  rcases List.mem_iff_getElem.mp hySC with ⟨jy, hjylen, hjey⟩
                  have hymatch : ((Entry.mk k m.seq none).key == k &&
                      (Entry.mk k m.seq none).seq ≤ t) = true := by
                    rw [Bool.and_eq_true]
                    refine ⟨by simp, decide_eq_true ?_⟩
                    have hmseingt : m.seq ≤ t := by
                      have hcovt' := hcovt
                      rw [covers] at hcovt'
                      simp at hcovt'
                      omega
                    exact hmseingt
                  by_cases hylt : jy < i
                  · have hb := hbefore jy hylt
                    simp at hb
                    have hytrue : (SC[jy].key == k && SC[jy].seq ≤ t) = true := by
                      rw [hjey]; exact hymatch
                    rw [Bool.and_eq_true] at hytrue
                    exfalso
                    exact hb.elim (fun hk => hk (by simpa using hytrue.1))
                      (fun ht => by have := of_decide_eq_true hytrue.2; omega)
                  · have hijy : i < jy ∨ i = jy := by omega
                    rcases hijy with hijy | hieq
                    · have hrel := hsorted.rel_getElem_of_lt hi hjylen hijy
                      rw [hiz, hjey] at hrel
                      have hmle' : m.seq ≤ z.seq := cmpR_seq hrel
                      have hmle'' : m.seq ≤ e.seq := by simpa [hze] using hmle'
                      have hmseqeq : m.seq = e.seq := by omega
                      have hrelE : cmpR e (Entry.mk k m.seq none) = true := by
                        simpa [hze] using hrel
                      have hties : ¬ cmpR e (Entry.mk k m.seq none) = true := by
                        unfold cmpR
                        simp [hmseqeq, hval]
                      exfalso
                      exact hties hrelE
                    · subst jy
                      rw [hjey] at hiz
                      have hzy : z = Entry.mk k m.seq none := hiz.symm
                      have : e = Entry.mk k m.seq none := hze.symm.trans hzy
                      simp [this] at hval
                · simp [hany]
              rw [show valueAt SC k t = z.value by simp [valueAt, hv], hze]
              simp only [Option.bind_some]
              simp [hno]
          · rcases List.mem_map.mp hzmarkers with ⟨m, hmfilter, hmz⟩
            have hcovseq : covers m k m.seq := (List.mem_filter.mp hmfilter).2
            have hzval : z.value = none := by
              have htmp := congrArg Entry.value hmz
              simpa using htmp.symm
            have hmseingt : m.seq ≤ t := by
              have htmp := congrArg Entry.seq hmz
              have hmzseq : m.seq = z.seq := by simpa using htmp
              omega
            have hmseqle : e.seq ≤ m.seq := by
              by_cases hle : e.seq ≤ m.seq
              · exact hle
              · have hlt : m.seq < e.seq := by omega
                by_cases hjlt : j < i
                · have hb := hbefore j hjlt
                  simp at hb
                  have hbtrue : (SC[j].key == k && SC[j].seq ≤ t) = true := by
                    rw [hje]; exact hpred_e
                  rw [Bool.and_eq_true] at hbtrue
                  exfalso
                  exact hb.elim (fun hk => hk (by simpa using hbtrue.1))
                    (fun ht => by have := of_decide_eq_true hbtrue.2; omega)
                · have hij : i < j ∨ i = j := by omega
                  rcases hij with hij | hieq
                  · have hrel := hsorted.rel_getElem_of_lt hi hjlen hij
                    rw [hiz, hje] at hrel
                    rw [← hmz] at hrel
                    have hle' : e.seq ≤ m.seq := by simpa using cmpR_seq hrel
                    omega
                  · subst j
                    rw [hje] at hiz
                    have hze : z = e := hiz.symm
                    rw [hze] at hmz
                    have : m.seq = e.seq := by
                      have := congrArg Entry.seq hmz
                      simpa using this
                    omega
            have hcovt : covers m k t := by
              rw [covers] at hcovseq ⊢
              simp at hcovseq ⊢
              omega
            have hany : (ms.filter (fun mm => covers mm k t)).any (fun mm => e.seq ≤ mm.seq) = true := by
              apply List.any_eq_true.mpr
              refine ⟨m, List.mem_filter.mpr ⟨(List.mem_filter.mp hmfilter).1, hcovt⟩, decide_eq_true hmseqle⟩
            rw [show valueAt SC k t = z.value by simp [valueAt, hv], hzval]
            simp only [Option.bind_some]
            simp [hany]

/-! ---------- DominatedRun ----------
`newest` is a stable sort by descending sequence. Std exposes that stability via
`mergeSort_zipIdx`: sorting the index-tagged list with `zipIdxLE` is the same as
sorting the plain list, and `zipIdxLE` breaks sequence ties by input position.
So the tagged sort is a strict total order on a list with distinct indices,
which lets us identify the visible winner and argue that a strictly older run
can never overtake it. -/

private def leD (a b : Entry) : Bool := b.seq ≤ a.seq

private theorem newest_eq (xs : List Entry) : newest xs = xs.mergeSort leD := by
  rfl

private theorem leD_trans : ∀ a b c : Entry, leD a b = true → leD b c = true → leD a c = true := by
  intro a b c hab hbc
  simp [leD] at hab hbc ⊢
  omega

private theorem leD_total : ∀ a b : Entry, (leD a b || leD b a) = true := by
  intro a b
  unfold leD
  by_cases h : b.seq ≤ a.seq
  · simp [h]
  · have h2 : a.seq ≤ b.seq := by omega
    simp [h, h2]

private theorem relD_old (e f : Entry) (i j : Nat) (hlt : f.seq < e.seq) :
    List.zipIdxLE leD (e, i) (f, j) = true := by
  unfold List.zipIdxLE leD
  have h1 : f.seq ≤ e.seq := by omega
  have h2 : ¬ e.seq ≤ f.seq := by omega
  simp [h1, h2]

private theorem relD_both_same_idx {u v : Entry × Nat}
    (huv : List.zipIdxLE leD u v = true) (hvu : List.zipIdxLE leD v u = true) :
    u.2 = v.2 := by
  unfold List.zipIdxLE leD at huv hvu
  simp at huv hvu
  omega

private theorem relD_antisymm_of_mem {S : List (Entry × Nat)}
    (hnd : (S.map (fun z : Entry × Nat => z.2)).Nodup)
    {u v : Entry × Nat} (hu : u ∈ S) (hv : v ∈ S)
    (huv : List.zipIdxLE leD u v = true) (hvu : List.zipIdxLE leD v u = true) : u = v := by
  have hidx : u.2 = v.2 := relD_both_same_idx huv hvu
  rcases List.mem_iff_getElem.mp hu with ⟨i1, hi1, hui⟩
  rcases List.mem_iff_getElem.mp hv with ⟨i2, hi2, hvi⟩
  have hi1m : i1 < (S.map (fun z : Entry × Nat => z.2)).length := by simpa using hi1
  have hi2m : i2 < (S.map (fun z : Entry × Nat => z.2)).length := by simpa using hi2
  have hsnds : (S.map (fun z : Entry × Nat => z.2))[i1] =
      (S.map (fun z : Entry × Nat => z.2))[i2] := by
    rw [List.getElem_map (f := fun z : Entry × Nat => z.2) (h := hi1m),
        List.getElem_map (f := fun z : Entry × Nat => z.2) (h := hi2m), hui, hvi, hidx]
  have hi_eq : i1 = i2 := hnd.eq_of_getElem_eq (by simpa using hi1) (by simpa using hi2) hsnds
  subst i2
  exact hui.symm.trans hvi

private theorem nodup_snd_mergeSort (l : List Entry) :
    ((l.zipIdx.mergeSort (List.zipIdxLE leD)).map (fun z : Entry × Nat => z.2)).Nodup := by
  have hp := List.mergeSort_perm l.zipIdx (List.zipIdxLE leD)
  have hpm := hp.map (fun z : Entry × Nat => z.2)
  have hnd : (l.zipIdx.map (fun z : Entry × Nat => z.2)).Nodup := by
    rw [List.zipIdx_map_snd]
    exact List.nodup_range'
  exact hnd.perm hpm.symm

private theorem find?_map_fst_lift {L : List (Entry × Nat)} {p : Entry → Bool} {e : Entry}
    (h : (L.map Prod.fst).find? p = some e) :
    ∃ z ∈ L, z.1 = e ∧ L.find? (fun z : Entry × Nat => p z.1) = some z := by
  rcases List.find?_eq_some_iff_getElem.mp h with ⟨hpE, i, hi, hLi, hbefore⟩
  have hiL : i < L.length := by simpa using hi
  have hLi1 : L[i].1 = e := by
    have hLi' := hLi
    rw [List.getElem_map (f := Prod.fst) (h := hi)] at hLi'
    exact hLi'
  refine ⟨L[i], List.getElem_mem hiL, hLi1, ?_⟩
  apply List.find?_eq_some_iff_getElem.mpr
  constructor
  · simpa [hLi1] using hpE
  · refine ⟨i, hiL, rfl, ?_⟩
    intro j hj
    have hb := hbefore j hj
    have hjm : j < (L.map Prod.fst).length := by omega
    simpa [List.getElem_map (f := Prod.fst) (h := hjm)] using hb

private theorem find?_of_find?_map : ∀ (L : List (Entry × Nat)) {p : Entry → Bool}
    {z : Entry × Nat}, L.find? (fun w : Entry × Nat => p w.1) = some z →
    (L.map Prod.fst).find? p = some z.1
  | [], _, _, h => by simp at h
  | u :: us, p, z, h => by
      simp only [List.map_cons, List.find?_cons]
      by_cases hpu : p u.1 = true
      · have huz : u = z := by simpa [hpu] using h
        subst z
        simp [hpu]
      · have htail : us.find? (fun w : Entry × Nat => p w.1) = some z := by
          simpa [hpu] using h
        have ht := find?_of_find?_map us htail
        simp [hpu, ht]

private theorem find?_min_of_sorted {L : List (Entry × Nat)}
    (hs : L.Pairwise (fun u v : Entry × Nat => List.zipIdxLE leD u v = true))
    {q : Entry × Nat → Bool} {z : Entry × Nat}
    (hz : L.find? q = some z) :
    ∀ y ∈ L, q y = true → y = z ∨ List.zipIdxLE leD z y = true := by
  rcases List.find?_eq_some_iff_getElem.mp hz with ⟨hqz, i, hi, hLi, hbefore⟩
  intro y hy hqy
  rcases List.mem_iff_getElem.mp hy with ⟨j, hjlen, hjy⟩
  by_cases hji : j = i
  · subst j
    left
    exact hjy.symm.trans hLi
  · by_cases hlt : j < i
    · have hb := hbefore j hlt
      simp at hb
      have hqj : q L[j] = true := by simpa [hjy] using hqy
      rw [hqj] at hb
      simp at hb
    · have hij : i < j := by omega
      right
      have hrel := hs.rel_getElem_of_lt hi hjlen hij
      rw [hLi, hjy] at hrel
      exact hrel

theorem DominatedRun_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.DominatedRun.Goal := by
  intro a b k t e hvis hb
  let le := List.zipIdxLE leD
  let SA := a.zipIdx.mergeSort le
  let SB := (a ++ b).zipIdx.mergeSort le
  let p : Entry → Bool := fun x => x.key == k && x.seq ≤ t
  let q : Entry × Nat → Bool := fun z => p z.1
  have hsa : SA.Pairwise (fun u v : Entry × Nat => le u v = true) := by
    dsimp [SA, le]
    exact List.pairwise_mergeSort (List.zipIdxLE_trans leD_trans) (List.zipIdxLE_total leD_total) _
  have hsb : SB.Pairwise (fun u v : Entry × Nat => le u v = true) := by
    dsimp [SB, le]
    exact List.pairwise_mergeSort (List.zipIdxLE_trans leD_trans) (List.zipIdxLE_total leD_total) _
  have hSAeq : SA.map Prod.fst = newest a := by
    dsimp [SA, le]
    simpa [newest_eq] using (List.mergeSort_zipIdx (le := leD) (l := a))
  have hSBeq : SB.map Prod.fst = newest (a ++ b) := by
    dsimp [SB, le]
    simpa [newest_eq] using (List.mergeSort_zipIdx (le := leD) (l := a ++ b))
  have hvismap : (SA.map Prod.fst).find? p = some e := by
    simpa [visible, p, hSAeq] using hvis
  rcases find?_map_fst_lift hvismap with ⟨⟨e', i⟩, hzeSA, hze1, hfindSA⟩
  simp at hze1
  subst e'
  have hqze : q (e, i) = true := List.find?_some hfindSA
  have hminA : ∀ y ∈ SA, q y = true → y = (e, i) ∨ le (e, i) y = true :=
    find?_min_of_sorted hsa hfindSA
  have hndSB : (SB.map (fun z : Entry × Nat => z.2)).Nodup := by
    dsimp [SB, le]
    exact nodup_snd_mergeSort (a ++ b)
  have hzeA_sort : (e, i) ∈ a.zipIdx.mergeSort le := by simpa [SA] using hzeSA
  have hzeInAzip : (e, i) ∈ a.zipIdx :=
    (List.mem_mergeSort (a := (e, i)) (l := a.zipIdx) (le := le)).mp hzeA_sort
  have hzeInABzip : (e, i) ∈ (a ++ b).zipIdx := by
    rw [List.zipIdx_append]
    exact List.mem_append_left _ hzeInAzip
  have hzeAB_sort : (e, i) ∈ (a ++ b).zipIdx.mergeSort le := by
    exact (List.mem_mergeSort (a := (e, i)) (l := (a ++ b).zipIdx) (le := le)).mpr hzeInABzip
  have hzeSB : (e, i) ∈ SB := by simpa [SB] using hzeAB_sort
  have hminB : ∀ z ∈ SB, q z = true → z = (e, i) ∨ le (e, i) z = true := by
    intro z hzSB hqz
    have hzsort : z ∈ (a ++ b).zipIdx.mergeSort le := by simpa [SB] using hzSB
    have hzzip : z ∈ (a ++ b).zipIdx :=
      (List.mem_mergeSort (a := z) (l := (a ++ b).zipIdx) (le := le)).mp hzsort
    rw [List.zipIdx_append] at hzzip
    rcases List.mem_append.mp hzzip with hza | hzb
    · have hza_sort : z ∈ a.zipIdx.mergeSort le :=
        (List.mem_mergeSort (a := z) (l := a.zipIdx) (le := le)).mpr hza
      have hzSA : z ∈ SA := by simpa [SA] using hza_sort
      exact hminA z hzSA hqz
    · have hfbin : z.1 ∈ b := List.fst_mem_of_mem_zipIdx hzb
      have hseq : z.1.seq < e.seq := hb z.1 hfbin
      right
      dsimp [le]
      exact relD_old e z.1 i z.2 hseq
  rcases List.mem_iff_getElem.mp hzeSB with ⟨idx, hidxlen, hidxval⟩
  have hfindSB : SB.find? q = some (e, i) := by
    apply List.find?_eq_some_iff_getElem.mpr
    constructor
    · exact hqze
    · refine ⟨idx, hidxlen, hidxval, ?_⟩
      intro j hjlt
      by_cases hqj : q SB[j] = true
      · exfalso
        have hjlen : j < SB.length := by omega
        have hneq : SB[j] ≠ (e, i) := by
          intro heq
          have hjlenM : j < (SB.map (fun z : Entry × Nat => z.2)).length := by simpa using hjlen
          have hidxlenM : idx < (SB.map (fun z : Entry × Nat => z.2)).length := by simpa using hidxlen
          have hsnds : (SB.map (fun z : Entry × Nat => z.2))[j] =
              (SB.map (fun z : Entry × Nat => z.2))[idx] := by
            rw [List.getElem_map (f := fun z : Entry × Nat => z.2) (h := hjlenM),
                List.getElem_map (f := fun z : Entry × Nat => z.2) (h := hidxlenM), hidxval, heq]
          have hjidx : j = idx := hndSB.eq_of_getElem_eq hjlenM hidxlenM hsnds
          omega
        have hrelfwd : le SB[j] (e, i) = true := by
          have hrel := hsb.rel_getElem_of_lt hjlen hidxlen hjlt
          rw [hidxval] at hrel
          exact hrel
        rcases hminB SB[j] (List.getElem_mem hjlen) hqj with hjEq | hjRel
        · exact hneq hjEq
        · have heq : SB[j] = (e, i) := by
            apply relD_antisymm_of_mem hndSB (List.getElem_mem hjlen) hzeSB
            · exact hrelfwd
            · exact hjRel
          exact hneq heq
      · simp [hqj]
  have hmapfinal : (SB.map Prod.fst).find? p = some e := by
    simpa using find?_of_find?_map SB hfindSB
  simpa [visible, p, hSBeq] using hmapfinal

theorem OverlayAggregate_sound :
    TinylambOpt.ResearchGoals.VersionedIndexes.OverlayAggregate.Goal := by
  intro a b ks t
  induction ks with
  | nil => simp [total]
  | cons k ks ih =>
      rw [List.filter_cons]
      by_cases h : (visible a k t).isNone
      · simp [h]
        rw [total_cons (a ++ b), total_cons a, total_cons b, ih, valueAt_append_key]
        simp [h, Int.add_assoc, Int.add_left_comm, Int.add_comm]
      · simp [h]
        rw [total_cons (a ++ b), total_cons a, ih, valueAt_append_key]
        simp [h, Int.add_assoc]

end TinylambOpt.ResearchProofs.Wave7VersionedIndexes
