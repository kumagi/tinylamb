import TinylambOpt.ResearchGoals.BlockLazyZonePrune

/-!
Proved

- `Goal` — Two-stage lazy block pruning (`table/tlcs_file.cpp`,
  `executor/relational_regression_test_part4.cpp`): ZoneMap headers gate key
  decoding, and payload columns are decoded only on blocks with ≥ 1 match.

The reference model is defined in `TinylambOpt.ResearchGoals.BlockLazyZonePrune`.
-/

namespace TinylambOpt.ResearchProofs.BlockLazyZonePrune
open ResearchGoals.BlockLazyZonePrune
open List

/-! ## Decide helpers -/

private theorem dtrue {p : Prop} [inst : Decidable p] (h : p) : decide p = true := by
  cases inst with
  | isTrue _ => rfl
  | isFalse hn => exact absurd h hn

private theorem bool_eq_false_of_ne {b : Bool} (h : ¬ b = true) : b = false := by
  cases b with
  | true => exact (h rfl).elim
  | false => rfl

/-! ## Per-block zone-map prune soundness -/

/-- If some row key of a valid block lies in `[lo, hi]`, the block header must
overlap `[lo, hi]`, i.e. `mayMatch` fires: the header-only prune never drops a
block that would have produced output. -/
private theorem any_match_mayMatch (lo hi : Int) (b : Block) (hb : ValidHeader b) :
    b.rows.any (fun r => inRange lo hi r.1) = true → mayMatch lo hi b = true := by
  intro h
  rcases List.any_eq_true.mp h with ⟨⟨ok, v⟩, hr, hmr⟩
  cases ok with
  | none => exact absurd hmr (by simp [inRange])
  | some k =>
      have hrange : lo ≤ k ∧ k ≤ hi := of_decide_eq_true hmr
      have hbound : b.minKey ≤ k ∧ k ≤ b.maxKey := hb ⟨some k, v⟩ hr
      exact dtrue (by obtain ⟨h1, h2⟩ := hrange; obtain ⟨h3, h4⟩ := hbound; omega)

/-- Pruned blocks (header says "no overlap") contain no matching row. -/
private theorem prune (lo hi : Int) (b : Block) (hb : ValidHeader b)
    (hmm : mayMatch lo hi b = false) :
    b.rows.filter (fun r => inRange lo hi r.1) = [] := by
  rw [List.filter_eq_nil_iff]
  intro r hr hmem
  have h := any_match_mayMatch lo hi b hb (List.any_eq_true.mpr ⟨r, hr, hmem⟩)
  rw [h] at hmm
  exact absurd hmm (by decide)

private theorem prune_any (lo hi : Int) (b : Block) (hb : ValidHeader b)
    (hmm : mayMatch lo hi b = false) :
    b.rows.any (fun r => inRange lo hi r.1) = false := by
  cases h : b.rows.any (fun r => inRange lo hi r.1) with
  | true =>
      rw [any_match_mayMatch lo hi b hb h] at hmm
      exact absurd hmm (by decide)
  | false => rfl

/-! ## List reshuffling helpers -/

private theorem filter_ne_nil_iff_any {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p ≠ []) ↔ l.any p = true := by
  induction l with
  | nil => simp
  | cons a l ih =>
      cases h : p a <;> simp [h, ih]

private theorem filterMap_if_match (lo hi : Int) :
    ∀ l : List (Option Int × Int),
      l.filterMap (fun r => if inRange lo hi r.1 then some r.2 else none) =
        (l.filter (fun r => inRange lo hi r.1)).map Prod.snd := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih =>
      cases h : inRange lo hi a.1 <;> simp [h, ih]

private theorem flatMap_kmatch (lo hi : Int) :
    ∀ l : List (Block × Nat),
      l.flatMap (fun p => (p.1.rows.filter (fun r => inRange lo hi r.1)).map Prod.snd) =
        ((l.flatMap (fun p => p.1.rows)).filter (fun r => inRange lo hi r.1)).map Prod.snd := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih =>
      cases a with
      | mk b i =>
          simp only [List.flatMap_cons, List.filter_append, List.map_append, ih]

private theorem flatMap_fst {α β : Type} (f : α → List β) :
    ∀ l : List (α × Nat), l.flatMap (fun p => f p.1) = (l.map Prod.fst).flatMap f := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih =>
      cases a with
      | mk x i => simp only [List.flatMap_cons, List.map_cons, ih]

private theorem zipIdx_filter_char {f : Block → Bool} (bs : List Block) :
    ∀ idx : Nat,
      idx ∈ (bs.zipIdx).filterMap (fun p => if f p.1 then some p.2 else none) ↔
        ∃ b : Block, bs[idx]? = some b ∧ f b = true := by
  intro idx
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨p, hzip, hpeq⟩
    have hget : bs[p.2]? = some p.1 := (List.mem_zipIdx_iff_getElem?).mp hzip
    have hf : f p.1 = true := by
      by_cases h : f p.1
      · exact h
      · rw [bool_eq_false_of_ne h] at hpeq
        simp at hpeq
    rw [hf] at hpeq
    simp at hpeq
    subst hpeq
    exact ⟨p.1, hget, hf⟩
  · rintro ⟨b, hb, hf⟩
    refine ⟨⟨b, idx⟩, (List.mem_zipIdx_iff_getElem?).mpr hb, ?_⟩
    simp [hf]

/-! ## Fold invariant for `twoStageScan` -/

private def step (lo hi : Int) (st : ScanTrace) (p : Block × Nat) : ScanTrace :=
  match p with
  | (b, idx) =>
    if !mayMatch lo hi b then
      st
    else
      let matched := b.rows.filter (fun r => inRange lo hi r.1)
      if matched.isEmpty then
        { st with keyOpened := st.keyOpened ++ [idx] }
      else
        { payloads := st.payloads ++ matched.map Prod.snd
          keyOpened := st.keyOpened ++ [idx]
          payOpened := st.payOpened ++ [idx] }

private theorem fold_inv (lo hi : Int) :
    ∀ (l : List (Block × Nat)) (st : ScanTrace),
      (∀ b ∈ l.map Prod.fst, ValidHeader b) →
        (l.foldl (step lo hi) st).payloads =
          st.payloads ++ l.flatMap (fun p => (p.1.rows.filter (fun r => inRange lo hi r.1)).map Prod.snd) ∧
        (l.foldl (step lo hi) st).keyOpened =
          st.keyOpened ++ l.filterMap (fun p => if mayMatch lo hi p.1 then some p.2 else none) ∧
        (l.foldl (step lo hi) st).payOpened =
          st.payOpened ++ l.filterMap (fun p => if p.1.rows.any (fun r => inRange lo hi r.1) then some p.2 else none) := by
  intro l
  induction l with
  | nil => intro st _; simp
  | cons a l ih =>
      cases a with
      | mk b i =>
          intro st hval
          have hb : ValidHeader b := hval b List.mem_cons_self
          have hl : ∀ x ∈ l.map Prod.fst, ValidHeader x :=
            fun x hx => hval x (List.mem_cons_of_mem b hx)
          by_cases hmm : mayMatch lo hi b
          · by_cases hem : (b.rows.filter (fun r => inRange lo hi r.1)).isEmpty
            · -- header fires, but no row matches: key-only open
              have hnil : b.rows.filter (fun r => inRange lo hi r.1) = [] :=
                List.isEmpty_iff.mp hem
              have hanyf : b.rows.any (fun r => inRange lo hi r.1) = false := by
                by_cases h : b.rows.any (fun r => inRange lo hi r.1)
                · rw [← filter_ne_nil_iff_any] at h
                  exact absurd h (by simp [hnil])
                · exact bool_eq_false_of_ne h
              have hstep : step lo hi st (b, i) =
                  { st with keyOpened := st.keyOpened ++ [i] } := by
                simp [step, hmm, hem]
              obtain ⟨hp, hk, hpay⟩ := ih { st with keyOpened := st.keyOpened ++ [i] } hl
              rw [List.foldl_cons, hstep]
              refine ⟨?_, ?_, ?_⟩
              · rw [hp]; simp [hnil]
              · rw [hk]; simp [hmm]
              · rw [hpay]; simp [hanyf]
            · -- header fires and at least one row matches: payload open
              have hne : b.rows.filter (fun r => inRange lo hi r.1) ≠ [] :=
                (List.isEmpty_eq_false_iff).mp (bool_eq_false_of_ne hem)
              have hany : b.rows.any (fun r => inRange lo hi r.1) = true :=
                (filter_ne_nil_iff_any (p := fun r => inRange lo hi r.1) b.rows).mp hne
              have hstep : step lo hi st (b, i) =
                  { payloads := st.payloads ++ (b.rows.filter (fun r => inRange lo hi r.1)).map Prod.snd,
                    keyOpened := st.keyOpened ++ [i],
                    payOpened := st.payOpened ++ [i] } := by
                simp [step, hmm, hem]
              obtain ⟨hp, hk, hpay⟩ := ih
                  { payloads := st.payloads ++ (b.rows.filter (fun r => inRange lo hi r.1)).map Prod.snd,
                    keyOpened := st.keyOpened ++ [i],
                    payOpened := st.payOpened ++ [i] } hl
              rw [List.foldl_cons, hstep]
              refine ⟨?_, ?_, ?_⟩
              · rw [hp]; simp
              · rw [hk]; simp [hmm]
              · rw [hpay]; simp [hany]
          · -- the header prunes the block entirely
            have hfb : mayMatch lo hi b = false := bool_eq_false_of_ne hmm
            have hstep : step lo hi st (b, i) = st := by simp [step, hfb]
            have hnil := prune lo hi b hb hfb
            have hany := prune_any lo hi b hb hfb
            obtain ⟨hp, hk, hpay⟩ := ih st hl
            rw [List.foldl_cons, hstep]
            refine ⟨?_, ?_, ?_⟩
            · rw [hp]; simp [hnil]
            · rw [hk]; simp [hfb]
            · rw [hpay]; simp [hany]

private theorem twoStageScan_eq (lo hi : Int) (bs : List Block) :
    twoStageScan lo hi bs = bs.zipIdx.foldl (step lo hi) {} := rfl

/-! ## Main theorem -/

theorem sound : Goal := by
  intro lo hi bs hval
  dsimp only
  have hvalid : ∀ b ∈ bs.zipIdx.map Prod.fst, ValidHeader b := by
    intro b hb
    simp only [List.zipIdx_map_fst] at hb
    exact hval b hb
  obtain ⟨hp, hk, hpay⟩ := fold_inv lo hi bs.zipIdx {} hvalid
  simp at hp hk hpay
  have hscan := twoStageScan_eq lo hi bs
  rw [hscan]
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- result correctness: the opened blocks yield exactly the reference scan
    rw [hp, refScan, filterMap_if_match, flatMap_kmatch, flatMap_fst]
    simp only [List.zipIdx_map_fst]
  · -- key-open footprint ↔ header overlap
    intro idx
    rw [hk]
    exact zipIdx_filter_char (f := mayMatch lo hi) bs idx
  · -- payload-open footprint ↔ blocks with at least one matching row
    intro idx
    rw [hpay]
    exact zipIdx_filter_char (f := fun b => b.rows.any (fun r => inRange lo hi r.1)) bs idx
  · -- payload opens are a subset of key opens
    intro idx h
    rw [hpay] at h
    rcases (zipIdx_filter_char
      (f := fun b => b.rows.any (fun r => inRange lo hi r.1)) bs idx).mp h with ⟨b, hb, hany⟩
    rw [hk]
    exact (zipIdx_filter_char (f := mayMatch lo hi) bs idx).mpr
      ⟨b, hb, any_match_mayMatch lo hi b (hval b (List.mem_of_getElem? hb)) hany⟩

end TinylambOpt.ResearchProofs.BlockLazyZonePrune
