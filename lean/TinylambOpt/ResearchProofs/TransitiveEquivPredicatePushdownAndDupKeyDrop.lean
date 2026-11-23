import TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop

namespace TinylambOpt.ResearchProofs.TransitiveEquivPredicatePushdownAndDupKeyDrop

open TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop

/-- The predicate actually emitted by the reference (three-key) join. -/
private def emitRef (a : RowA) (b : RowB) (c : RowC) : Option (Nat × Nat × Nat) :=
  match a.k, b.k, c.k with
  | some ka, some kb, some kc =>
      if kb == kc && ka == kc then some (a.id, b.id, c.id) else none
  | _, _, _ => none

/-- The predicate emitted by the optimized join after dup-key dropping. -/
private def emitOpt (a : RowA) (b : RowB) (c : RowC) : Option (Nat × Nat × Nat) :=
  match b.k, c.k with
  | some kb, some kc => if kb == kc then some (a.id, b.id, c.id) else none
  | _, _ => none

private theorem ref_eq (as : List RowA) (bs : List RowB) (cs : List RowC) :
    refInner3 as bs cs =
      (innerAB as bs).flatMap fun (a, b) => cs.filterMap (emitRef a b) := by
  rfl

private theorem opt_eq (as : List RowA) (bs : List RowB) (cs : List RowC) :
    optInner3 as bs cs =
      (innerAB as bs).flatMap fun (a, b) =>
        (cs.filter (fun c : RowC =>
          match c.k with
          | some kc => (as.filterMap RowA.k).contains kc
          | none => false)).filterMap (emitOpt a b) := by
  rfl

/-- A pair coming out of `innerAB` has equal, non-NULL keys and came from `as`/`bs`. -/
private theorem innerAB_keys {as : List RowA} {bs : List RowB} {a : RowA} {b : RowB}
    (h : (a, b) ∈ innerAB as bs) :
    ∃ k : Int, a.k = some k ∧ b.k = some k ∧ a ∈ as ∧ b ∈ bs := by
  unfold innerAB at h
  rcases List.mem_flatMap.mp h with ⟨a', ha', hmem⟩
  rcases List.mem_filterMap.mp hmem with ⟨b', hb', hfb'⟩
  cases h1 : a'.k with
  | none => simp [h1] at hfb'
  | some ka =>
      cases h2 : b'.k with
      | none => simp [h1, h2] at hfb'
      | some kb =>
          by_cases heq : ka == kb
          · have heq' : ka = kb := beq_iff_eq.mp heq
            simp [h1, h2, heq] at hfb'
            rcases hfb' with ⟨rfl, rfl⟩
            exact ⟨ka, h1, by rw [h2, ← heq'], ha', hb'⟩
          · simp [h1, h2, heq] at hfb'

/-- `emitOpt` can only emit when both keys are present and equal. -/
private theorem emitOpt_keys {a : RowA} {b : RowB} {c : RowC}
    (h : (emitOpt a b c).isSome = true) :
    ∃ kb kc : Int, b.k = some kb ∧ c.k = some kc ∧ kb = kc := by
  cases hbk : b.k with
  | none => simp [emitOpt, hbk] at h
  | some kb =>
      cases hck : c.k with
      | none => simp [emitOpt, hbk, hck] at h
      | some kc =>
          by_cases heq : kb == kc
          · exact ⟨kb, kc, rfl, rfl, beq_iff_eq.mp heq⟩
          · simp [emitOpt, hbk, hck, heq] at h

/-- On an `innerAB` pair (where `a.k = b.k`) the redundant equality is truly redundant. -/
private theorem emitRef_eq_emitOpt {a : RowA} {b : RowB} (hk : a.k = b.k) (c : RowC) :
    emitRef a b c = emitOpt a b c := by
  cases hbk : b.k with
  | none =>
      have hak : a.k = none := by rw [hk, hbk]
      simp [emitRef, emitOpt, hbk, hak]
  | some kb =>
      have hak : a.k = some kb := by rw [hk, hbk]
      cases hck : c.k with
      | none => simp [emitRef, emitOpt, hbk, hak, hck]
      | some kc =>
          by_cases heq : kb == kc <;> simp [emitRef, emitOpt, hbk, hak, hck, heq]

private theorem flatMap_congr {α β : Type} (f g : α → List β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : f x = g x := h x (by simp)
      have ih' := ih (fun y hy => h y (by simp [hy]))
      simp [List.flatMap_cons, hx, ih']

private theorem filterMap_congr {α β : Type} (f g : α → Option β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.filterMap f = xs.filterMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : f x = g x := h x (by simp)
      have ih' := ih (fun y hy => h y (by simp [hy]))
      cases hf : f x with
      | none =>
          have hg : g x = none := by rw [← hx, hf]
          simp [hf, hg, ih']
      | some y =>
          have hg : g x = some y := by rw [← hx, hf]
          simp [hf, hg, ih']

private theorem filterMap_filter_eq_self {α β : Type} (p : α → Bool) (f : α → Option β)
    (xs : List α) (h : ∀ x ∈ xs, (f x).isSome = true → p x = true) :
    (xs.filter p).filterMap f = xs.filterMap f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases hf : f x with
      | none =>
          have ih' := ih (fun y hy hys => h y (by simp [hy]) hys)
          by_cases hp : p x = true <;> simp [hf, hp, ih']
      | some y =>
          have hp : p x = true := h x (by simp) (by simp [hf])
          have ih' := ih (fun y' hy hys => h y' (by simp [hy]) hys)
          simp [hf, hp, ih']

theorem sound : TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop.Goal := by
  constructor
  · intro as bs cs
    let aKeys : List Int := as.filterMap RowA.k
    let pruned : RowC → Bool := fun c =>
      match c.k with
      | some kc => aKeys.contains kc
      | none => false
    have href := ref_eq as bs cs
    have hopt : optInner3 as bs cs =
        (innerAB as bs).flatMap fun (a, b) => (cs.filter pruned).filterMap (emitOpt a b) := by
      simpa [pruned, aKeys] using (opt_eq as bs cs)
    rw [href, hopt]
    apply flatMap_congr
    rintro ⟨a, b⟩ hab
    rcases innerAB_keys hab with ⟨k, hak, hbk, ha, hb⟩
    change (cs.filter pruned).filterMap (emitOpt a b) = cs.filterMap (emitRef a b)
    have hpruned : ∀ c ∈ cs, (emitOpt a b c).isSome = true → pruned c = true := by
      intro c hc hs
      rcases emitOpt_keys hs with ⟨kb, kc, hbk', hck, hbeq⟩
      have hkbk : kb = k := by
        rw [hbk] at hbk'
        cases hbk'
        rfl
      have hkck : kc = k := by
        rw [← hbeq, hkbk]
      have hakc : a.k = some kc := by
        rw [hak, hkck]
      have hmem : kc ∈ aKeys := by
        exact List.mem_filterMap.mpr ⟨a, ha, hakc⟩
      simpa [pruned, hck] using hmem
    have hfilter : (cs.filter pruned).filterMap (emitOpt a b) = cs.filterMap (emitOpt a b) :=
      filterMap_filter_eq_self pruned (emitOpt a b) cs hpruned
    rw [hfilter]
    apply filterMap_congr
    intro c hc
    exact (emitRef_eq_emitOpt (by rw [hak, hbk]) c).symm
  · refine ⟨[{ id := 1, k := some 10 }], [], [{ id := 100, k := some 10 }], ?_⟩
    decide

end TinylambOpt.ResearchProofs.TransitiveEquivPredicatePushdownAndDupKeyDrop
