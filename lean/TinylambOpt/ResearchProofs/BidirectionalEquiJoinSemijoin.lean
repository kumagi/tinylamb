import TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin

namespace TinylambOpt.ResearchProofs.BidirectionalEquiJoinSemijoin

open TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin

private def emitChain (a : RowA) (b : RowB) (c : RowC) : Option (RowA × RowB × RowC) :=
  match a.ka, b.kb1, b.kb2, c.kc with
  | some x, some y, some z, some w =>
      if x == y && z == w then some (a, b, c) else none
  | _, _, _, _ => none

private theorem refJoinChain_eq (as : List RowA) (bs : List RowB) (cs : List RowC) :
    refJoinChain as bs cs =
      as.flatMap fun a => bs.flatMap fun b => cs.filterMap fun c => emitChain a b c := by
  rfl

private theorem emit_keys {a : RowA} {b : RowB} {c : RowC} {t : Triple}
    (he : emitChain a b c = some t) :
    ∃ x y z w : Int,
      a.ka = some x ∧ b.kb1 = some y ∧ b.kb2 = some z ∧ c.kc = some w ∧ x = y ∧ z = w := by
  unfold emitChain at he
  cases h1 : a.ka with
  | none => simp [h1] at he
  | some x =>
      cases h2 : b.kb1 with
      | none => simp [h1, h2] at he
      | some y =>
          cases h3 : b.kb2 with
          | none => simp [h1, h2, h3] at he
          | some z =>
              cases h4 : c.kc with
              | none => simp [h1, h2, h3, h4] at he
              | some w =>
                  by_cases hc : x == y && z == w
                  · have hc' : (x == y && z == w) = true := hc
                    rw [Bool.and_eq_true] at hc'
                    have hxy : x = y := by simpa using hc'.1
                    have hzw : z = w := by simpa using hc'.2
                    exact ⟨x, y, z, w, rfl, rfl, rfl, rfl, hxy, hzw⟩
                  · have hcf : (x == y && z == w) = false := Bool.eq_false_of_ne_true hc
                    simp [h1, h2, h3, h4, hcf] at he

private theorem emit_keys_of_isSome {a : RowA} {b : RowB} {c : RowC}
    (h : (emitChain a b c).isSome = true) :
    ∃ x y z w : Int,
      a.ka = some x ∧ b.kb1 = some y ∧ b.kb2 = some z ∧ c.kc = some w ∧ x = y ∧ z = w := by
  cases he : emitChain a b c with
  | none => simp [he] at h
  | some t => exact emit_keys he

private def goodB (as : List RowA) (cs : List RowC) (b : RowB) : Bool :=
  memKey b.kb1 (as.filterMap RowA.ka) && memKey b.kb2 (cs.filterMap RowC.kc)

@[simp] private theorem memKey_none (keys : List Int) : memKey none keys = false := rfl

@[simp] private theorem memKey_some (k : Int) (keys : List Int) :
    memKey (some k) keys = keys.contains k := rfl

private theorem contains_of_mem {l : List Int} {k : Int} (h : k ∈ l) : l.contains k = true :=
  List.contains_iff_mem.mpr h

private theorem memKey_of_joinB {as : List RowA} {cs : List RowC} {a b c}
    (ha : a ∈ as) (hc : c ∈ cs) (h : (emitChain a b c).isSome = true) :
    goodB as cs b = true := by
  rcases emit_keys_of_isSome h with ⟨x, y, z, w, hka, hkb1, hkb2, hkc, hxy, hzw⟩
  have hxmem : x ∈ as.filterMap RowA.ka := List.mem_filterMap.mpr ⟨a, ha, hka⟩
  have hwmem : w ∈ cs.filterMap RowC.kc := List.mem_filterMap.mpr ⟨c, hc, hkc⟩
  have hymem : y ∈ as.filterMap RowA.ka := by simpa [← hxy] using hxmem
  have hzmem : z ∈ cs.filterMap RowC.kc := by simpa [← hzw] using hwmem
  simp [goodB, hkb1, hkb2, hymem, hzmem]

private theorem memKeyA_of_join {bs' : List RowB} {a b c} (hb : b ∈ bs')
    (h : (emitChain a b c).isSome = true) :
    memKey a.ka (bs'.filterMap RowB.kb1) = true := by
  rcases emit_keys_of_isSome h with ⟨x, y, z, w, hka, hkb1, hkb2, hkc, hxy, hzw⟩
  have hymem : y ∈ bs'.filterMap RowB.kb1 := List.mem_filterMap.mpr ⟨b, hb, hkb1⟩
  have hxmem : x ∈ bs'.filterMap RowB.kb1 := by simpa [hxy] using hymem
  simpa [hka] using (contains_of_mem hxmem)

private theorem memKeyC_of_join {bs' : List RowB} {a b c} (hb : b ∈ bs')
    (h : (emitChain a b c).isSome = true) :
    memKey c.kc (bs'.filterMap RowB.kb2) = true := by
  rcases emit_keys_of_isSome h with ⟨x, y, z, w, hka, hkb1, hkb2, hkc, hxy, hzw⟩
  have hzmem : z ∈ bs'.filterMap RowB.kb2 := List.mem_filterMap.mpr ⟨b, hb, hkb2⟩
  have hwmem : w ∈ bs'.filterMap RowB.kb2 := by simpa [hzw] using hzmem
  simpa [hkc] using (contains_of_mem hwmem)

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

private theorem flatMap_congr {α β : Type} (f g : α → List β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : f x = g x := h x (by simp)
      have ih' := ih (fun y hy => h y (by simp [hy]))
      simp [List.flatMap_cons, hx, ih']

private theorem flatMap_filter_eq_self {α β : Type} (p : α → Bool) (f : α → List β)
    (xs : List α) (h : ∀ x ∈ xs, p x = false → f x = []) :
    (xs.filter p).flatMap f = xs.flatMap f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hp : p x = true
      · have ih' := ih (fun y hy hpf => h y (by simp [hy]) hpf)
        simp [hp, ih']
      · have hpf : p x = false := Bool.eq_false_of_ne_true hp
        have hf : f x = [] := h x (by simp) hpf
        have ih' := ih (fun y hy hpf => h y (by simp [hy]) hpf)
        simp [hp, hf, ih']

private theorem csFilterMap_nil_of_no_match (a : RowA) (b : RowB) (cs : List RowC)
    (h : ∀ c ∈ cs, (emitChain a b c).isSome = false) :
    cs.filterMap (emitChain a b) = [] := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
      have hc : (emitChain a b c).isSome = false := h c (by simp)
      cases he : emitChain a b c with
      | none =>
          have ih' := ih (fun c' hc' => h c' (by simp [hc']))
          simp [he, ih']
      | some t =>
          have htrue : (emitChain a b c).isSome = true := by simp [he]
          rw [hc] at htrue
          cases htrue

private theorem out_nil_of_no_match (a : RowA) (bs : List RowB) (cs : List RowC)
    (h : ∀ b ∈ bs, ∀ c ∈ cs, (emitChain a b c).isSome = false) :
    bs.flatMap (fun b => cs.filterMap (emitChain a b)) = [] := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      have hb : cs.filterMap (emitChain a b) = [] :=
        csFilterMap_nil_of_no_match a b cs (fun c hc => h b (by simp) c hc)
      have ih' := ih (fun b' hb' c hc => h b' (by simp [hb']) c hc)
      simp [List.flatMap_cons, hb, ih']

private theorem replace_cs (as : List RowA) (bs : List RowB) (cs : List RowC) (Q : RowC → Bool)
    (hQ : ∀ a ∈ as, ∀ b ∈ bs, ∀ c ∈ cs, (emitChain a b c).isSome = true → Q c = true) :
    refJoinChain as bs (cs.filter Q) = refJoinChain as bs cs := by
  simp only [refJoinChain_eq]
  apply flatMap_congr
  intro a ha
  apply flatMap_congr
  intro b hb
  exact filterMap_filter_eq_self Q (emitChain a b) cs
    (fun c hc hsome => hQ a ha b hb c hc hsome)

private theorem replace_as (as : List RowA) (bs : List RowB) (cs : List RowC) (P : RowA → Bool)
    (hP : ∀ a ∈ as, P a = false → ∀ b ∈ bs, ∀ c ∈ cs, (emitChain a b c).isSome = false) :
    refJoinChain (as.filter P) bs cs = refJoinChain as bs cs := by
  simp only [refJoinChain_eq]
  apply flatMap_filter_eq_self
  intro a ha hPa
  exact out_nil_of_no_match a bs cs (fun b hb c hc => hP a ha hPa b hb c hc)

private theorem replace_bs (as : List RowA) (bs : List RowB) (cs : List RowC) (P : RowB → Bool)
    (hP : ∀ a ∈ as, ∀ b ∈ bs, P b = false → ∀ c ∈ cs, (emitChain a b c).isSome = false) :
    refJoinChain as (bs.filter P) cs = refJoinChain as bs cs := by
  simp only [refJoinChain_eq]
  apply flatMap_congr
  intro a ha
  apply flatMap_filter_eq_self
  intro b hb hPb
  exact csFilterMap_nil_of_no_match a b cs (fun c hc => hP a ha b hb hPb c hc)

theorem sound : TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin.Goal := by
  constructor
  · intro as bs cs
    let keysA := as.filterMap RowA.ka
    let keysC := cs.filterMap RowC.kc
    let bs' := bs.filter (fun b => memKey b.kb1 keysA && memKey b.kb2 keysC)
    let keysB1 := bs'.filterMap RowB.kb1
    let keysB2 := bs'.filterMap RowB.kb2
    let as' := as.filter (fun a => memKey a.ka keysB1)
    let cs' := cs.filter (fun c => memKey c.kc keysB2)
    change refJoinChain as' bs' cs' = refJoinChain as bs cs
    have hQ : ∀ a ∈ as', ∀ b ∈ bs', ∀ c ∈ cs,
        (emitChain a b c).isSome = true → memKey c.kc keysB2 = true := by
      intro a ha b hb c hc hs
      simpa [keysB2] using (memKeyC_of_join (bs' := bs') hb hs)
    have h1 : refJoinChain as' bs' cs' = refJoinChain as' bs' cs := by
      simpa [cs'] using (replace_cs as' bs' cs (fun c => memKey c.kc keysB2) hQ)
    have hPa : ∀ a ∈ as, memKey a.ka keysB1 = false →
        ∀ b ∈ bs', ∀ c ∈ cs, (emitChain a b c).isSome = false := by
      intro a ha hfa b hb c hc
      cases he : emitChain a b c with
      | none => simp
      | some t =>
          have hs : (emitChain a b c).isSome = true := by simp [he]
          have hmem : memKey a.ka keysB1 = true := by
            simpa [keysB1] using (memKeyA_of_join (bs' := bs') hb hs)
          rw [hfa] at hmem
          cases hmem
    have h2 : refJoinChain as' bs' cs = refJoinChain as bs' cs := by
      simpa [as'] using (replace_as as bs' cs (fun a => memKey a.ka keysB1) hPa)
    have hPb : ∀ a ∈ as, ∀ b ∈ bs,
        (memKey b.kb1 keysA && memKey b.kb2 keysC) = false →
        ∀ c ∈ cs, (emitChain a b c).isSome = false := by
      intro a ha b hb hfb c hc
      cases he : emitChain a b c with
      | none => simp
      | some t =>
          have hs : (emitChain a b c).isSome = true := by simp [he]
          have hmem : goodB as cs b = true := memKey_of_joinB ha hc hs
          have hgood : (memKey b.kb1 keysA && memKey b.kb2 keysC) = true := by
            simpa [goodB, keysA, keysC] using hmem
          rw [hfb] at hgood
          cases hgood
    have h3 : refJoinChain as bs' cs = refJoinChain as bs cs := by
      simpa [bs'] using
        (replace_bs as bs cs (fun b => memKey b.kb1 keysA && memKey b.kb2 keysC) hPb)
    exact h1.trans (h2.trans h3)
  · intro x y a b hxy
    apply Bool.eq_iff_iff.mpr
    unfold symPairFactored symPairRef
    simp only [Bool.and_eq_true, Bool.or_eq_true]
    simp
    omega

end TinylambOpt.ResearchProofs.BidirectionalEquiJoinSemijoin
