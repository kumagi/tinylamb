import TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge

/-!
Proved

- `Goal` — Two-level radix partitioned parallel aggregate merge
  (`executor/detail/typed_stream_aggregate_partitioned.cpp`, ClickBench Q14/17/
  19/29/32/34/35): routing worker-local `(count, sum)` maps to `numParts` disjoint
  partitions by `hash k % numParts` yields zero duplicate output keys across
  partitions and exact parity with single-thread aggregation.

The reference model is defined in
`TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge`.
-/

-- # Soundness of two-level radix-partitioned parallel `(count, sum)` merge
-- Lean proof: lean/TinylambOpt/ResearchProofs/TwoLevelPartitionedAggMerge.lean (sound)

open List

namespace TinylambOpt.ResearchProofs.TwoLevelPartitionedAggMerge

open TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge

private abbrev V := Nat × Int
private abbrev E := Key × V
private abbrev M := List E

private theorem beq_key_iff {a b : Key} : ((a == b) = true) ↔ a = b := by
  cases a <;> cases b <;> simp

private theorem beq_key_self (a : Key) : (a == a) = true :=
  beq_key_iff.mpr rfl

private theorem beq_key_false {a b : Key} (h : a ≠ b) : (a == b) = false :=
  Bool.eq_false_iff.mpr (mt beq_key_iff.mp h)

private theorem beq_nat_iff {a b : Nat} : ((a == b) = true) ↔ a = b := by
  simp

private def plus (o₁ o₂ : Option V) : Option V :=
  match o₁, o₂ with
  | none, r => r
  | some x, none => some x
  | some (c1, s1), some (c2, s2) => some (c1 + c2, s1 + s2)

private theorem plus_none (o : Option V) : plus o none = o := by
  cases o <;> rfl

private theorem none_plus (o : Option V) : plus none o = o := by
  cases o <;> rfl

private theorem plus_comm (o₁ o₂ : Option V) : plus o₁ o₂ = plus o₂ o₁ := by
  cases o₁ with
  | none => cases o₂ <;> rfl
  | some p1 =>
      cases o₂ with
      | none => rfl
      | some p2 =>
          simp [plus, Nat.add_comm, Int.add_comm]

private theorem plus_assoc (o₁ o₂ o₃ : Option V) :
    plus (plus o₁ o₂) o₃ = plus o₁ (plus o₂ o₃) := by
  cases o₁ with
  | none => cases o₂ <;> cases o₃ <;> rfl
  | some p1 =>
      cases o₂ with
      | none => cases o₃ <;> rfl
      | some p2 =>
          cases o₃ with
          | none => rfl
          | some p3 =>
              simp [plus, Nat.add_assoc, Int.add_assoc]

private def foldV (R : M) (o : Option V) : Option V :=
  R.foldl (fun acc e => plus acc (some e.2)) o

private theorem foldV_nil (o : Option V) : foldV [] o = o := rfl

private theorem foldV_append (R₁ R₂ : M) (o : Option V) :
    foldV (R₁ ++ R₂) o = foldV R₂ (foldV R₁ o) :=
  List.foldl_append

private theorem foldV_comm_init (R : M) (o : Option V) :
    foldV R o = plus o (foldV R none) := by
  induction R generalizing o with
  | nil => rw [foldV_nil, foldV_nil, plus_none]
  | cons e R ih =>
      show foldV R (plus o (some e.2)) = plus o (foldV R (plus none (some e.2)))
      rw [ih (plus o (some e.2)), ih (plus none (some e.2)), none_plus, plus_assoc]

private def comb (P : E → Bool) (L : M) (k : Key) : Option V :=
  foldV (L.filter (fun e => P e && e.1 == k)) none

private def combTop (L : M) (k : Key) : Option V := comb (fun _ => true) L k

private theorem comb_nil (P : E → Bool) (k : Key) : comb P [] k = none := rfl

private theorem comb_cons (P : E → Bool) (e : E) (L : M) (k : Key) :
    comb P (e :: L) k =
      if P e && e.1 == k then plus (some e.2) (comb P L k) else comb P L k := by
  show foldV ((e :: L).filter (fun x => P x && x.1 == k)) none = _
  cases h : P e && e.1 == k
  · simp [h, comb, foldV]
  · simp [h, comb]
    show foldV (e :: L.filter (fun x => P x && x.1 == k)) none = plus (some e.2) _
    rw [foldV, List.foldl_cons, none_plus]
    exact foldV_comm_init _ (some e.2)

private theorem comb_append (P : E → Bool) (L₁ L₂ : M) (k : Key) :
    comb P (L₁ ++ L₂) k = plus (comb P L₁ k) (comb P L₂ k) := by
  show foldV ((L₁ ++ L₂).filter (fun e => P e && e.1 == k)) none = _
  rw [List.filter_append, foldV_append, foldV_comm_init]
  rfl

private theorem comb_congr (P₁ P₂ : E → Bool) (L : M) (k : Key)
    (h : ∀ e ∈ L, (P₁ e && e.1 == k) = (P₂ e && e.1 == k)) :
    comb P₁ L k = comb P₂ L k := by
  show foldV (L.filter (fun e => P₁ e && e.1 == k)) none =
    foldV (L.filter (fun e => P₂ e && e.1 == k)) none
  rw [List.filter_congr h]

private theorem comb_false (P : E → Bool) (L : M) (k : Key)
    (h : ∀ e ∈ L, (P e && e.1 == k) = false) :
    comb P L k = none := by
  have hf : L.filter (fun e => P e && e.1 == k) = [] := by
    rw [List.filter_eq_nil_iff]
    intro e he hc
    have h0 := h e he
    rw [hc] at h0
    cases h0
  show foldV (L.filter (fun e => P e && e.1 == k)) none = none
  rw [hf, foldV_nil]

private theorem comb_top_eq_foldV (L : M) (k : Key) :
    combTop L k = foldV (L.filter (fun e => e.1 == k)) none := by
  show foldV (L.filter (fun e => true && e.1 == k)) none = _
  rw [List.filter_congr (fun x _ => Bool.true_and (x.1 == k))]

private theorem lookup_add (m : M) (ke : Key) (c : Nat) (s : Int) (k : Key) :
    lookupCountSum (addCountSum m ke c s) k =
      if ke == k then plus (lookupCountSum m k) (some (c, s)) else lookupCountSum m k := by
  induction m with
  | nil =>
      by_cases h : ke = k
      · subst h
        simp [addCountSum, lookupCountSum, beq_key_self, plus]
      · have hf : (ke == k) = false := beq_key_false h
        simp only [addCountSum, lookupCountSum, hf, Bool.false_eq_true, ↓reduceIte]
  | cons x xs ih =>
      rcases x with ⟨k', c', s'⟩
      by_cases h1 : k' = ke
      · subst h1
        rw [addCountSum, beq_key_self]
        simp only [↓reduceIte]
        by_cases h2 : k' = k
        · subst h2
          simp [lookupCountSum, plus]
        · have hf : (k' == k) = false := beq_key_false h2
          simp [lookupCountSum, hf]
      · have h1f : (k' == ke) = false := beq_key_false h1
        rw [addCountSum, h1f]
        simp only [Bool.false_eq_true, ↓reduceIte]
        by_cases h2 : k' = k
        · subst h2
          have h3f : (ke == k') = false := beq_key_false (Ne.symm h1)
          simp [lookupCountSum, h3f]
        · have h2f : (k' == k) = false := beq_key_false h2
          simp only [lookupCountSum, h2f, Bool.false_eq_true, ↓reduceIte, ih]

private theorem lookup_empty (k : Key) : lookupCountSum [] k = none := rfl

private theorem mem_add_keys (m : M) (ke : Key) (c : Nat) (s : Int) (a : Key) :
    a ∈ (addCountSum m ke c s).map Prod.fst → a ∈ m.map Prod.fst ∨ a = ke := by
  induction m with
  | nil =>
      intro h
      simp only [addCountSum, map_cons, map_nil, mem_cons, not_mem_nil, or_false] at h
      exact Or.inr h
  | cons x xs ih =>
      intro h
      rcases x with ⟨k', c', s'⟩
      by_cases h1 : k' = ke
      · subst h1
        simp only [addCountSum, beq_key_self, ↓reduceIte, map_cons, mem_cons] at h
        rcases h with rfl | hrest
        · exact Or.inl mem_cons_self
        · exact Or.inl (mem_cons.mpr (Or.inr hrest))
      · have h1f : (k' == ke) = false := beq_key_false h1
        simp only [addCountSum, h1f, Bool.false_eq_true, ↓reduceIte, map_cons, mem_cons] at h
        rcases h with rfl | hrest
        · exact Or.inl mem_cons_self
        · exact (ih hrest).imp (fun ha => mem_cons.mpr (Or.inr ha)) id

private theorem nodup_add (m : M) (k : Key) (c : Nat) (s : Int)
    (h : (m.map Prod.fst).Nodup) : ((addCountSum m k c s).map Prod.fst).Nodup := by
  induction m with
  | nil => simp [addCountSum]
  | cons x xs ih =>
      rcases x with ⟨k', c', s'⟩
      simp only [map_cons, nodup_cons] at h
      obtain ⟨hnot, hrest⟩ := h
      by_cases h1 : k' = k
      · subst h1
        simp only [addCountSum, beq_key_self, ↓reduceIte, map_cons, nodup_cons]
        exact ⟨hnot, hrest⟩
      · have h1f : (k' == k) = false := beq_key_false h1
        simp only [addCountSum, h1f, Bool.false_eq_true, ↓reduceIte, map_cons, nodup_cons]
        refine ⟨fun hmem => ?_, ih hrest⟩
        rcases mem_add_keys xs k c s k' hmem with hin | heq
        · exact hnot hin
        · exact h1 heq

private def stepP (P : E → Bool) (acc : M) (e : E) : M :=
  if P e then addCountSum acc e.1 e.2.1 e.2.2 else acc

private def foldE (P : E → Bool) (L : M) (init : M) : M :=
  L.foldl (stepP P) init

private def applyOf (P : E → Bool) (ws : List M) (init : M) : M :=
  ws.foldl (fun acc w => foldE P w acc) init

private theorem foldE_nil (P : E → Bool) (init : M) : foldE P [] init = init := rfl
private theorem foldE_cons (P : E → Bool) (e : E) (L init : M) :
    foldE P (e :: L) init = foldE P L (stepP P init e) := rfl
private theorem applyOf_nil (P : E → Bool) (init : M) : applyOf P [] init = init := rfl
private theorem applyOf_cons (P : E → Bool) (w : M) (ws : List M) (init : M) :
    applyOf P (w :: ws) init = applyOf P ws (foldE P w init) := rfl

private theorem foldE_lookup (P : E → Bool) :
    ∀ (L : M) (init : M) (k : Key),
      lookupCountSum (foldE P L init) k = plus (lookupCountSum init k) (comb P L k) := by
  intro L
  induction L with
  | nil => intro init k; rw [foldE_nil, comb_nil, plus_none]
  | cons e L ih =>
      intro init k
      rw [foldE_cons, ih, comb_cons]
      have hstep : lookupCountSum (stepP P init e) k =
          if P e && e.1 == k then plus (lookupCountSum init k) (some e.2)
          else lookupCountSum init k := by
        show lookupCountSum (if P e then addCountSum init e.1 e.2.1 e.2.2 else init) k = _
        by_cases hP : P e = true
        · rw [if_pos hP, lookup_add]
          simp [hP]
        · have hPf : P e = false := Bool.eq_false_iff.mpr hP
          rw [if_neg (by simp [hPf])]
          simp [hPf]
      rw [hstep]
      cases hcond : (P e && e.1 == k)
      · simp
      · simp [plus_assoc]

private theorem applyOf_lookup (P : E → Bool) :
    ∀ (ws : List M) (init : M) (k : Key),
      lookupCountSum (applyOf P ws init) k = plus (lookupCountSum init k) (comb P ws.flatten k) := by
  intro ws
  induction ws with
  | nil => intro init k; rw [applyOf_nil, flatten_nil, comb_nil, plus_none]
  | cons w ws ih =>
      intro init k
      rw [applyOf_cons, ih, foldE_lookup, flatten_cons, comb_append, plus_assoc]

private theorem mem_foldE_keys (P : E → Bool) :
    ∀ (L : M) (init : M) (a : Key),
      a ∈ (foldE P L init).map Prod.fst →
        a ∈ init.map Prod.fst ∨ ∃ e ∈ L, P e = true ∧ e.1 = a := by
  intro L
  induction L with
  | nil => intro init a h; exact Or.inl h
  | cons e L ih =>
      intro init a h
      rw [foldE_cons] at h
      rcases ih (stepP P init e) a h with hinit | ⟨f, hf, hPf, hfa⟩
      · rw [stepP] at hinit
        cases hP : P e
        · simp only [hP, Bool.false_eq_true, ↓reduceIte] at hinit; exact Or.inl hinit
        · simp only [hP, ↓reduceIte] at hinit
          rcases mem_add_keys init e.1 e.2.1 e.2.2 a hinit with hi | rfl
          · exact Or.inl hi
          · exact Or.inr ⟨e, mem_cons_self, hP, rfl⟩
      · exact Or.inr ⟨f, mem_cons.mpr (Or.inr hf), hPf, hfa⟩

private theorem mem_applyOf_keys (P : E → Bool) :
    ∀ (ws : List M) (init : M) (a : Key),
      a ∈ (applyOf P ws init).map Prod.fst →
        a ∈ init.map Prod.fst ∨ ∃ w ∈ ws, ∃ e ∈ w, P e = true ∧ e.1 = a := by
  intro ws
  induction ws with
  | nil => intro init a h; exact Or.inl h
  | cons w ws ih =>
      intro init a h
      rw [applyOf_cons] at h
      rcases ih (foldE P w init) a h with hinit | ⟨v, hv, e, he, hPv, hva⟩
      · rcases mem_foldE_keys P w init a hinit with hi | he
        · exact Or.inl hi
        · exact Or.inr ⟨w, mem_cons_self, he⟩
      · exact Or.inr ⟨v, mem_cons.mpr (Or.inr hv), e, he, hPv, hva⟩

private theorem foldE_nodup (P : E → Bool) :
    ∀ (L init : M), (init.map Prod.fst).Nodup → ((foldE P L init).map Prod.fst).Nodup := by
  intro L
  induction L with
  | nil => intro init h; exact h
  | cons e L ih =>
      intro init h
      rw [foldE_cons]
      apply ih
      show ((if P e then addCountSum init e.1 e.2.1 e.2.2 else init).map Prod.fst).Nodup
      cases hP : P e
      · simp only [Bool.false_eq_true, ↓reduceIte]; exact h
      · simp only [↓reduceIte]; exact nodup_add _ _ _ _ h

private theorem applyOf_nodup (P : E → Bool) :
    ∀ (ws : List M) (init : M),
      (init.map Prod.fst).Nodup → ((applyOf P ws init).map Prod.fst).Nodup := by
  intro ws
  induction ws with
  | nil => intro init h; exact h
  | cons w ws ih =>
      intro init h
      rw [applyOf_cons]
      exact ih _ (foldE_nodup P w init h)

private theorem combTop_append (L₁ L₂ : M) (k : Key) :
    combTop (L₁ ++ L₂) k = plus (combTop L₁ k) (combTop L₂ k) := comb_append _ _ _ _

private theorem combTop_eq_lookup :
    ∀ (m : M) (k : Key), (m.map Prod.fst).Nodup → combTop m k = lookupCountSum m k := by
  intro m
  induction m with
  | nil => intro k _; rfl
  | cons x xs ih =>
      intro k h
      rcases x with ⟨k', cs'⟩
      simp only [map_cons, nodup_cons] at h
      obtain ⟨hnot, hrest⟩ := h
      rw [comb_top_eq_foldV]
      by_cases hk : k' = k
      · subst hk
        have hnil : xs.filter (fun e => e.1 == k') = [] := by
          rw [List.filter_eq_nil_iff]
          intro e he hc
          have : e.1 = k' := beq_key_iff.mp hc
          exact hnot (List.mem_map.mpr ⟨e, he, this⟩)
        rw [List.filter_cons, beq_key_self]
        simp only [↓reduceIte, hnil]
        simp [foldV, none_plus, lookupCountSum]
      · have hkf : (k' == k) = false := beq_key_false hk
        rw [List.filter_cons, hkf, lookupCountSum, hkf]
        simp only [Bool.false_eq_true, ↓reduceIte]
        rw [← comb_top_eq_foldV]
        exact ih k hrest

private def stepRef (acc : M) (row : Key × Int) : M :=
  addCountSum acc row.1 1 row.2

private def foldRef (rows : List (Key × Int)) (init : M) : M :=
  rows.foldl stepRef init

private theorem refAgg_eq_foldRef (rows : List (Key × Int)) :
    refAgg rows = foldRef rows [] := rfl

private theorem foldRef_nodup :
    ∀ (rows : List (Key × Int)) (init : M),
      (init.map Prod.fst).Nodup → ((foldRef rows init).map Prod.fst).Nodup := by
  intro rows
  induction rows with
  | nil => intro init h; exact h
  | cons r rs ih =>
      intro init h
      show ((foldRef rs (addCountSum init r.1 1 r.2)).map Prod.fst).Nodup
      exact ih _ (nodup_add _ _ _ _ h)

private theorem refAgg_nodup (rows : List (Key × Int)) :
    ((refAgg rows).map Prod.fst).Nodup :=
  foldRef_nodup rows [] (by simp)

private def combRowsGo (rows : List (Key × Int)) (k : Key) (init : Option V) : Option V :=
  rows.foldl (fun acc r => if r.1 == k then plus acc (some (1, r.2)) else acc) init

private def combRows (rows : List (Key × Int)) (k : Key) : Option V :=
  combRowsGo rows k none

private theorem combRowsGo_comm_init (rows : List (Key × Int)) (k : Key) (o : Option V) :
    combRowsGo rows k o = plus o (combRows rows k) := by
  unfold combRows
  induction rows generalizing o with
  | nil => simp [combRowsGo, plus_none]
  | cons x xs ih =>
      show combRowsGo xs k (if x.1 == k then plus o (some (1, x.2)) else o) =
        plus o (combRowsGo xs k (if x.1 == k then plus none (some (1, x.2)) else none))
      by_cases hx : (x.1 == k) = true
      · simp only [hx, ↓reduceIte]
        rw [ih (plus o (some (1, x.2))), ih (plus none (some (1, x.2))), none_plus, plus_assoc]
      · simp only [hx, Bool.false_eq_true, ↓reduceIte]
        exact ih o

private theorem combRows_cons (r : Key × Int) (rs : List (Key × Int)) (k : Key) :
    combRows (r :: rs) k =
      if r.1 == k then plus (some (1, r.2)) (combRows rs k) else combRows rs k := by
  show combRowsGo rs k (if r.1 == k then plus none (some (1, r.2)) else none) = _
  by_cases hr : (r.1 == k) = true
  · simp only [hr, ↓reduceIte, none_plus]
    exact combRowsGo_comm_init rs k (some (1, r.2))
  · simp [combRows, hr]

private theorem combRows_append (r1 r2 : List (Key × Int)) (k : Key) :
    combRows (r1 ++ r2) k = plus (combRows r1 k) (combRows r2 k) := by
  show (r1 ++ r2).foldl _ none = plus (combRows r1 k) (combRows r2 k)
  rw [List.foldl_append]
  exact combRowsGo_comm_init r2 k (combRows r1 k)

private theorem foldRef_lookup :
    ∀ (rows : List (Key × Int)) (init : M) (k : Key),
      lookupCountSum (foldRef rows init) k = plus (lookupCountSum init k) (combRows rows k) := by
  intro rows
  induction rows with
  | nil => intro init k; simp [foldRef, combRows, combRowsGo, plus_none]
  | cons r rs ih =>
      intro init k
      show lookupCountSum (foldRef rs (addCountSum init r.1 1 r.2)) k = _
      rw [ih, lookup_add, combRows_cons]
      cases hr : (r.1 == k)
      · simp
      · simp [plus_assoc]

private theorem refAgg_lookup (rows : List (Key × Int)) (k : Key) :
    lookupCountSum (refAgg rows) k = combRows rows k := by
  rw [refAgg_eq_foldRef, foldRef_lookup, lookup_empty, none_plus]

private theorem combTop_map_refAgg (chunks : List (List (Key × Int))) (k : Key) :
    combTop (chunks.map refAgg).flatten k = lookupCountSum (refAgg chunks.flatten) k := by
  rw [refAgg_lookup]
  induction chunks with
  | nil => simp [combTop, comb, foldV, combRows, combRowsGo]
  | cons c cs ih =>
      rw [map_cons, flatten_cons, combTop_append, flatten_cons, combRows_append,
        combTop_eq_lookup (refAgg c) k (refAgg_nodup c), refAgg_lookup, ih]

private theorem twoLevelAgg_eq_map (numParts : Nat) (hash : Key → Nat)
    (chunks : List (List (Key × Int))) :
    twoLevelAgg numParts hash chunks =
      (List.range numParts).map fun p =>
        applyOf (fun e => hash e.1 % numParts == p) (chunks.map refAgg) [] := by
  unfold twoLevelAgg applyOf foldE stepP
  rfl

private theorem lookupPartitioned_all_none {k : Key} :
    ∀ (qs : List Nat) (f : Nat → M),
      (∀ x ∈ qs, lookupCountSum (f x) k = none) →
      lookupPartitioned (qs.map f) k = none := by
  intro qs f hnone
  unfold lookupPartitioned
  induction qs with
  | nil => rfl
  | cons y ys ih =>
      simp only [map_cons, foldl_cons]
      rw [hnone y mem_cons_self]
      exact ih (fun z hz => hnone z (mem_cons.mpr (Or.inr hz)))

private theorem lookupPartitioned_eq_of_index (k : Key) :
    ∀ (ps : List Nat) (f : Nat → M) (p₀ : Nat),
      ps.Nodup →
      p₀ ∈ ps →
      (∀ x ∈ ps, x ≠ p₀ → lookupCountSum (f x) k = none) →
      lookupPartitioned (ps.map f) k = lookupCountSum (f p₀) k := by
  intro ps f p₀ hnod hp hother
  unfold lookupPartitioned
  induction ps with
  | nil => simp at hp
  | cons q qs ih =>
      simp only [map_cons, foldl_cons]
      have hnq : qs.Nodup ∧ q ∉ qs := ⟨(nodup_cons.mp hnod).2, (nodup_cons.mp hnod).1⟩
      rcases mem_cons.mp hp with rfl | hmem
      · cases hlu : lookupCountSum (f p₀) k with
        | none =>
            have hrest : ∀ x ∈ qs, lookupCountSum (f x) k = none := fun x hx =>
              hother x (mem_cons.mpr (Or.inr hx)) (fun hxq => hnq.2 (hxq ▸ hx))
            clear ih hp hother hnq hnod hlu
            simpa [lookupPartitioned] using lookupPartitioned_all_none qs f hrest
        | some cs =>
            clear ih hp hother hnq hnod hlu
            induction qs with
            | nil => rfl
            | cons _ _ ihqs => exact ihqs
      · have hne : q ≠ p₀ := fun hqp => hnq.2 (hqp ▸ hmem)
        rw [hother q mem_cons_self hne]
        exact ih hnq.1 hmem (fun x hx hxne => hother x (mem_cons.mpr (Or.inr hx)) hxne)

private theorem nodup_flatMap_disjoint {α β : Type} (l : List α) (g : α → List β)
    (hnod : l.Nodup)
    (hpart : ∀ a ∈ l, (g a).Nodup)
    (hdisj : ∀ a ∈ l, ∀ b ∈ l, a ≠ b → ∀ x, x ∈ g a → x ∉ g b) :
    (l.flatMap g).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [flatMap_cons]
      have ha_nod : (g a).Nodup := hpart a mem_cons_self
      have hl_nod : l.Nodup := (nodup_cons.mp hnod).2
      have ha_nmem : a ∉ l := (nodup_cons.mp hnod).1
      have hrec : (l.flatMap g).Nodup :=
        ih hl_nod
          (fun b hb => hpart b (mem_cons.mpr (Or.inr hb)))
          (fun x hx y hy hne z hzx hzy =>
            hdisj x (mem_cons.mpr (Or.inr hx)) y (mem_cons.mpr (Or.inr hy)) hne z hzx hzy)
      refine nodup_append.mpr ⟨ha_nod, hrec, ?_⟩
      intro x hxa y hyl hxy
      subst hxy
      rw [mem_flatMap] at hyl
      rcases hyl with ⟨b, hb, hxb⟩
      have hab : a ≠ b := fun h => ha_nmem (h ▸ hb)
      exact hdisj a mem_cons_self b (mem_cons.mpr (Or.inr hb)) hab x hxa hxb

theorem sound : Goal := by
  intro numParts hash chunks hpos
  let P : Nat → E → Bool := fun p e => hash e.1 % numParts == p
  have hpartEq :
      twoLevelAgg numParts hash chunks =
        (List.range numParts).map (fun p => applyOf (P p) (chunks.map refAgg) []) :=
    twoLevelAgg_eq_map numParts hash chunks
  constructor
  · rw [hpartEq, List.flatMap_map]
    refine nodup_flatMap_disjoint (List.range numParts)
      (fun p => (applyOf (P p) (chunks.map refAgg) []).map Prod.fst)
      nodup_range ?_ ?_
    · intro p _
      exact applyOf_nodup (P p) (chunks.map refAgg) [] (by simp)
    · intro p₁ _ p₂ _ hne a ha₁ ha₂
      rcases mem_applyOf_keys (P p₁) (chunks.map refAgg) [] a ha₁ with h0 | ⟨w, _, e, _, hP₁, rfl⟩
      · simp at h0
      rcases mem_applyOf_keys (P p₂) (chunks.map refAgg) [] e.1 ha₂ with h0 | ⟨w', _, e', _, hP₂, heq⟩
      · simp at h0
      have hp1 : hash e.1 % numParts = p₁ := beq_nat_iff.mp hP₁
      have hp2 : hash e'.1 % numParts = p₂ := beq_nat_iff.mp hP₂
      rw [heq] at hp2
      exact hne (hp1.symm.trans hp2)
  · intro k
    let p₀ := hash k % numParts
    have hp₀_lt : p₀ < numParts := Nat.mod_lt _ hpos
    have hp₀_mem : p₀ ∈ List.range numParts := mem_range.mpr hp₀_lt
    rw [hpartEq]
    rw [lookupPartitioned_eq_of_index k (List.range numParts)
      (fun p => applyOf (P p) (chunks.map refAgg) []) p₀ nodup_range hp₀_mem ?_]
    · rw [applyOf_lookup, lookup_empty, none_plus]
      have hcongr :
          comb (P p₀) (chunks.map refAgg).flatten k =
            combTop (chunks.map refAgg).flatten k := by
        refine comb_congr (P p₀) (fun _ => true) _ k (fun e _ => ?_)
        cases hk : (e.1 == k)
        · simp
        · have hek : e.1 = k := beq_key_iff.mp hk
          have hPp₀ : P p₀ e = true := by
            show (hash e.1 % numParts == p₀) = true
            rw [hek, beq_nat_iff]
          simp [hPp₀]
      rw [hcongr, combTop_map_refAgg]
    · intro q _ hne
      rw [applyOf_lookup, lookup_empty, none_plus]
      refine comb_false (P q) _ k (fun e _ => ?_)
      cases hk : (e.1 == k)
      · simp
      · have hek : e.1 = k := beq_key_iff.mp hk
        have hPf : P q e = false := by
          show (hash e.1 % numParts == q) = false
          exact Bool.eq_false_iff.mpr (mt beq_nat_iff.mp (by rw [hek]; exact Ne.symm hne))
        simp [hPf]

end TinylambOpt.ResearchProofs.TwoLevelPartitionedAggMerge

-- C++ references to this model or its proof.
-- executor/detail/typed_stream_aggregate_partitioned.cpp
