import TinylambOpt.ResearchGoals.GatheredSmallDictFastPath

/-!
Proved

- `Goal` — Compact gathered dictionary remap (`executor/detail/fused_grouped_join.cpp`
  `left_pack` `kGathered` `total_rows ≤ 4096`): remapping active canonical dictionary
  IDs to dense IDs `0..m-1` while keeping `none` (NULL) separate from unreferenced
  entries preserves grouped `SUM` for every key `k`.

The reference model is defined in `TinylambOpt.ResearchGoals.GatheredSmallDictFastPath`.
-/

namespace TinylambOpt.ResearchProofs.GatheredSmallDictFastPath
open TinylambOpt.ResearchGoals.GatheredSmallDictFastPath

/-!
# Gathered small-dictionary fast path

`compactGroupedSum` builds a dense sub-dictionary from the codes actually
referenced by `sel` and remaps grouped sums to dense IDs `0..m-1`, keeping
the NULL bucket (`none`) separate in `nullSum`.  We show that querying the
compact aggregate agrees with the general per-row decode `refGroupedSum`
for every key `k : Option Int`.
-/

private def codesOf (_dict : List Int) (codes : List (Option Nat)) (sel : List Nat) : List Nat :=
  List.filterMap (fun rowIdx =>
    match codes[rowIdx]? with
    | some (some cid) => some cid
    | _ => none) sel

private def usedOf (dict : List Int) (codes : List (Option Nat)) (sel : List Nat) : List Nat :=
  (codesOf dict codes sel).eraseDups

private def refStep (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (m : List (Option Int × Int)) (rowIdx : Nat) : List (Option Int × Int) :=
  addAssoc m (refDecodeRow dict codes rowIdx) (vals[rowIdx]?.getD 0)

private def cmpStep (_dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (used : List Nat) (x : List (Nat × Int) × Option Int) (rowIdx : Nat) :
    List (Nat × Int) × Option Int :=
  let v := vals[rowIdx]?.getD 0
  match codes[rowIdx]? with
  | some (some origCid) =>
      match used.findIdx? (fun x => x == origCid) with
      | some newId => (addAssoc x.1 newId v, x.2)
      | none => (x.1, x.2)
  | _ => (x.1, some (x.2.getD 0 + v))

private theorem refGroupedSum_fold (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) :
    refGroupedSum dict codes sel vals = sel.foldl (refStep dict codes vals) [] := rfl

private theorem cg_compactDict (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) :
    (compactGroupedSum dict codes sel vals).compactDict =
      (usedOf dict codes sel).filterMap (fun cid => dict[cid]?) := rfl

private theorem cg_compactSums (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) :
    (compactGroupedSum dict codes sel vals).compactSums =
      (sel.foldl (cmpStep dict codes vals (usedOf dict codes sel)) ([], none)).1 := rfl

private theorem cg_nullSum (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) :
    (compactGroupedSum dict codes sel vals).nullSum =
      (sel.foldl (cmpStep dict codes vals (usedOf dict codes sel)) ([], none)).2 := rfl

/-! ### `addAssoc` / `lookupAssoc` -/

private theorem lookupAssoc_nil {α : Type} [DecidableEq α] (k : α) :
    lookupAssoc ([] : List (α × Int)) k = none := rfl

private theorem lookup_addAssoc_same {α : Type} [DecidableEq α] (m : List (α × Int)) (k : α)
    (v : Int) :
    lookupAssoc (addAssoc m k v) k = some ((lookupAssoc m k).getD 0 + v) := by
  induction m generalizing k with
  | nil => simp [addAssoc, lookupAssoc]
  | cons p rest ih =>
      rcases p with ⟨k', s⟩
      by_cases hk : k' = k
      · subst k'
        simp [addAssoc, lookupAssoc]
      · have hb : (k' == k) = false := by simpa using hk
        simp [addAssoc, lookupAssoc, hb]
        exact ih k

private theorem lookup_addAssoc_other {α : Type} [DecidableEq α] (m : List (α × Int)) (v : Int) :
    ∀ (k j : α), ¬ (k = j) →
      lookupAssoc (addAssoc m k v) j = lookupAssoc m j := by
  induction m with
  | nil => intro k j hkj; simp [addAssoc, lookupAssoc, hkj]
  | cons p rest ih =>
      rcases p with ⟨k', s⟩
      intro k j hkj
      by_cases h1 : k' = k
      · subst k'
        by_cases h2 : k = j
        · exact absurd h2 hkj
        · simp [addAssoc, lookupAssoc, h2]
      · by_cases h2 : k' = j
        · subst j
          simp [addAssoc, lookupAssoc, h1]
        · simp [addAssoc, lookupAssoc, h1, h2, ih k j hkj]

/-! ### `findIdx?` -/

private theorem findIdx?_eq_some_get {α : Type} [DecidableEq α] {l : List α} {c : α} {i : Nat}
    (w : l.findIdx? (fun x => x == c) = some i) : ∃ a, l[i]? = some a ∧ a = c := by
  have h := List.of_findIdx?_eq_some w
  match hgt : l[i]? with
  | none =>
      rw [hgt] at h
      exact absurd h (by simp)
  | some a =>
      rw [hgt] at h
      exact ⟨a, rfl, by simpa using h⟩

private theorem getElem?_lt_length {α : Type} {l : List α} {i : Nat} {a : α}
    (h : l[i]? = some a) : i < l.length :=
  Nat.lt_of_not_le (fun hle => by
    rw [List.getElem?_eq_none hle] at h
    exact absurd h (by simp))

/-! ### membership / `Nodup` for `codesOf` and `usedOf` -/

private theorem codeOf_eq_some_iff (_dict : List Int) (codes : List (Option Nat)) (rowIdx c : Nat) :
    (match codes[rowIdx]? with
      | some (some cid) => some cid
      | _ => none) = some c ↔ codes[rowIdx]? = some (some c) := by
  match codes[rowIdx]? with
  | none => simp
  | some none => simp
  | some (some c') => simp

private theorem mem_codesOf (dict : List Int) (codes : List (Option Nat)) (sel : List Nat)
    (c : Nat) :
    c ∈ codesOf dict codes sel ↔ ∃ rowIdx ∈ sel, codes[rowIdx]? = some (some c) := by
  simp only [codesOf, List.mem_filterMap]
  constructor
  · rintro ⟨rowIdx, hr, h⟩
    exact ⟨rowIdx, hr, (codeOf_eq_some_iff dict codes rowIdx c).mp h⟩
  · rintro ⟨rowIdx, hr, h⟩
    exact ⟨rowIdx, hr, (codeOf_eq_some_iff dict codes rowIdx c).mpr h⟩

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have key : ∀ (n : Nat) (l : List Nat), l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
        intro l hl
        match l with
        | [] => simp
        | (a :: as) => simp [List.length_cons] at hl
    | succ n ih =>
        intro l hl
        match l with
        | [] => simp
        | (a :: as) =>
            rw [List.eraseDups_cons]
            rw [List.nodup_cons]
            refine ⟨?_, ih _ ?_⟩
            · intro h
              have h' := List.mem_eraseDups.mp h
              rw [List.mem_filter] at h'
              simp_all
            · exact Nat.le_trans (List.length_filter_le _ as) (by simpa using hl)
  exact key l.length l (Nat.le_refl _)

private theorem usedOf_nodup (dict : List Int) (codes : List (Option Nat)) (sel : List Nat) :
    (usedOf dict codes sel).Nodup := nodup_eraseDups _

private theorem mem_usedOf (dict : List Int) (codes : List (Option Nat)) (sel : List Nat)
    (c : Nat) :
    c ∈ usedOf dict codes sel ↔ ∃ rowIdx ∈ sel, codes[rowIdx]? = some (some c) := by
  simp only [usedOf, List.mem_eraseDups, mem_codesOf]

private theorem usedOf_bound (dict : List Int) (codes : List (Option Nat)) (sel : List Nat)
    (vals : List Int)
    (hb : ∀ rowIdx ∈ sel,
      rowIdx < codes.length ∧ rowIdx < vals.length ∧
      match codes[rowIdx]? with
      | some (some cid) => cid < dict.length
      | some none => true
      | none => false)
    (c : Nat) (hc : c ∈ usedOf dict codes sel) : c < dict.length := by
  obtain ⟨rowIdx, hr, hcode⟩ := (mem_usedOf dict codes sel c).mp hc
  obtain ⟨_, _, hm⟩ := hb rowIdx hr
  rw [hcode] at hm
  exact hm

private theorem codeBound (dict : List Int) (codes : List (Option Nat)) (sel : List Nat)
    (vals : List Int)
    (hb : ∀ rowIdx ∈ sel,
      rowIdx < codes.length ∧ rowIdx < vals.length ∧
      match codes[rowIdx]? with
      | some (some cid) => cid < dict.length
      | some none => true
      | none => false)
    (r : Nat) (hr : r ∈ sel) (c : Nat) (hc : codes[r]? = some (some c)) : c < dict.length := by
  obtain ⟨_, _, hm⟩ := hb r hr
  rw [hc] at hm
  exact hm

/-! ### the compact dictionary as a pure map -/

private theorem filterMap_eq_map_getD {α : Type} (f : α → Option Int) (l : List α)
    (h : ∀ x ∈ l, (f x).isSome) :
    l.filterMap f = l.map (fun x => (f x).getD 0) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases hf : f a with
      | none =>
          have h' := h a List.mem_cons_self
          simp [hf] at h'
      | some v =>
          rw [List.filterMap_cons_some hf, List.map_cons,
            ih (fun x m => h x (List.mem_cons_of_mem a m)), hf, Option.getD_some]

private theorem usedOf_getD (dict : List Int) (codes : List (Option Nat)) (sel : List Nat)
    (vals : List Int)
    (hb : ∀ rowIdx ∈ sel,
      rowIdx < codes.length ∧ rowIdx < vals.length ∧
      match codes[rowIdx]? with
      | some (some cid) => cid < dict.length
      | some none => true
      | none => false) :
    (usedOf dict codes sel).filterMap (fun cid => dict[cid]?) =
      (usedOf dict codes sel).map (fun cid => (dict[cid]?).getD 0) := by
  refine filterMap_eq_map_getD (fun cid => dict[cid]?) (usedOf dict codes sel) ?_
  intro c hc
  have hb' := usedOf_bound dict codes sel vals hb c hc
  rw [List.getElem?_eq_getElem hb']
  simp

/-! ### fold invariants -/

private theorem foldl_none (f : Option Int → Nat → Option Int) :
    ∀ (sel : List Nat), (∀ r ∈ sel, f none r = none) → sel.foldl f none = none := by
  intro sel
  induction sel with
  | nil => intro h; rfl
  | cons r rest ih =>
      intro h
      rw [List.foldl_cons, h r List.mem_cons_self]
      exact ih (fun r' hr' => h r' (List.mem_cons_of_mem r hr'))

private theorem foldl_congr {α : Type} (f g : α → Nat → α) :
    ∀ (sel : List Nat) (b : α), (∀ r ∈ sel, ∀ acc, f acc r = g acc r) →
      sel.foldl f b = sel.foldl g b := by
  intro sel
  induction sel with
  | nil => intro b h; rfl
  | cons r rest ih =>
      intro b h
      rw [List.foldl_cons, h r List.mem_cons_self]
      exact ih (g b r) (fun r' hr' acc => h r' (List.mem_cons_of_mem r hr') acc)

private theorem lookup_refStep (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (m : List (Option Int × Int)) (key : Option Int) (r : Nat) :
    lookupAssoc (refStep dict codes vals m r) key =
      if refDecodeRow dict codes r = key then
        some ((lookupAssoc m key).getD 0 + vals[r]?.getD 0)
      else lookupAssoc m key := by
  simp only [refStep]
  split
  · next h =>
      rw [h]
      exact lookup_addAssoc_same m _ _
  · next h =>
      exact lookup_addAssoc_other m _ _ key h

private theorem lookup_ref_fold (dict : List Int) (codes : List (Option Nat)) (vals : List Int) :
    ∀ (rs : List Nat) (m : List (Option Int × Int)) (key : Option Int),
    lookupAssoc (rs.foldl (refStep dict codes vals) m) key =
      rs.foldl (fun acc rowIdx =>
        if refDecodeRow dict codes rowIdx = key then
          some (acc.getD 0 + vals[rowIdx]?.getD 0)
        else acc) (lookupAssoc m key) := by
  intro rs
  induction rs with
  | nil => intro m key; rfl
  | cons r rest ih =>
      intro m key
      rw [List.foldl_cons, ih (refStep dict codes vals m r) key,
        lookup_refStep dict codes vals m key r]
      rw [List.foldl_cons]

private theorem lookup_cmpStep (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (used : List Nat) (hused : used.Nodup) (cm : List (Nat × Int)) (ns : Option Int) (i : Nat)
    (hi : i < used.length) (r : Nat) :
    lookupAssoc (cmpStep dict codes vals used (cm, ns) r).1 i =
      if codes[r]? = some (some used[i]) then
        some ((lookupAssoc cm i).getD 0 + vals[r]?.getD 0)
      else lookupAssoc cm i := by
  match hc : codes[r]? with
  | none => simp [cmpStep, hc]
  | some none => simp [cmpStep, hc]
  | some (some c) =>
      match hj : used.findIdx? (fun x => x == c) with
      | none =>
          have hne : ¬ (some (some c) = some (some used[i])) := by
            intro hcon
            have hcu : c = used[i] := by simpa using hcon
            have hmem : used[i] ∈ used :=
              List.mem_of_getElem? (List.getElem?_eq_getElem hi)
            have hall := List.findIdx?_eq_none_iff.mp hj (used[i]) hmem
            rw [hcu] at hall
            simp at hall
          simp [cmpStep, hc, hj, hne]
      | some j =>
          obtain ⟨a, hja, haj⟩ := findIdx?_eq_some_get hj
          have hjl : j < used.length := getElem?_lt_length hja
          by_cases hij : j = i
          · subst j
            have hc' : c = used[i] := by
              rw [← haj]
              exact Option.some.inj (hja.symm.trans (List.getElem?_eq_getElem hi))
            have hcond : some (some c) = some (some used[i]) :=
              congrArg (fun z => some (some z)) hc'
            simp only [cmpStep, hc]
            rw [hj]
            simp only [lookup_addAssoc_same]
            rw [ite_eq_left hcond]
          · have hc' : ¬ (some (some c) = some (some used[i])) := by
              intro hcon
              have hcu : c = used[i] := by simpa using hcon
              apply hij
              have h1 : used[j]? = some c := hja.trans (congrArg some haj)
              have h2 : some c = some used[i] := congrArg some hcu
              have h3 : some used[i] = used[i]? := (List.getElem?_eq_getElem hi).symm
              exact hused.eq_of_getElem?_eq hjl (h1.trans (h2.trans h3))
            simp only [cmpStep, hc, hj]
            rw [ite_eq_right hc']
            exact lookup_addAssoc_other cm (vals[r]?.getD 0) j i hij

private theorem lookup_cmp_fold (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (used : List Nat) (hused : used.Nodup) :
    ∀ (rs : List Nat) (cm : List (Nat × Int)) (ns : Option Int) (i : Nat)
      (hi : i < used.length),
    lookupAssoc ((rs.foldl (cmpStep dict codes vals used) (cm, ns)).1) i =
      rs.foldl (fun acc rowIdx =>
        if codes[rowIdx]? = some (some used[i]) then
          some (acc.getD 0 + vals[rowIdx]?.getD 0)
        else acc) (lookupAssoc cm i) := by
  intro rs
  induction rs with
  | nil => intro cm ns i hi; rfl
  | cons r rest ih =>
      intro cm ns i hi
      rw [List.foldl_cons,
        ih (cmpStep dict codes vals used (cm, ns) r).1
          (cmpStep dict codes vals used (cm, ns) r).2 i hi,
        lookup_cmpStep dict codes vals used hused cm ns i hi r]
      rw [List.foldl_cons]

private theorem null_cmpStep (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (used : List Nat) (cm : List (Nat × Int)) (ns : Option Int) (r : Nat) :
    (cmpStep dict codes vals used (cm, ns) r).2 =
      if (codes[r]?.join) = none then
        some (ns.getD 0 + vals[r]?.getD 0)
      else ns := by
  match hc : codes[r]? with
  | none => simp [cmpStep, hc]
  | some none => simp [cmpStep, hc]
  | some (some c) =>
      simp only [cmpStep, hc]
      match used.findIdx? (fun x => x == c) with
      | some _ => simp
      | none => simp

private theorem nullSum_fold (dict : List Int) (codes : List (Option Nat)) (vals : List Int)
    (used : List Nat) :
    ∀ (rs : List Nat) (cm : List (Nat × Int)) (ns : Option Int),
    (rs.foldl (cmpStep dict codes vals used) (cm, ns)).2 =
      rs.foldl (fun acc rowIdx =>
        if (codes[rowIdx]?.join) = none then
          some (acc.getD 0 + vals[rowIdx]?.getD 0)
        else acc) ns := by
  intro rs
  induction rs with
  | nil => intro cm ns; rfl
  | cons r rest ih =>
      intro cm ns
      rw [List.foldl_cons, ih (cmpStep dict codes vals used (cm, ns) r).1
        (cmpStep dict codes vals used (cm, ns) r).2,
        null_cmpStep dict codes vals used cm ns r]
      rw [List.foldl_cons]

/-! ### the fast path is sound -/

theorem sound : TinylambOpt.ResearchGoals.GatheredSmallDictFastPath.Goal := by
  intro dict codes sel vals k hnd hb
  have hused : (usedOf dict codes sel).Nodup := usedOf_nodup dict codes sel
  have hcd : (usedOf dict codes sel).filterMap (fun cid => dict[cid]?) =
      (usedOf dict codes sel).map (fun cid => (dict[cid]?).getD 0) :=
    usedOf_getD dict codes sel vals hb
  cases k with
  | none =>
      simp only [queryCompactSum]
      rw [cg_nullSum, refGroupedSum_fold,
        nullSum_fold dict codes vals (usedOf dict codes sel),
        lookup_ref_fold dict codes vals sel [] none, lookupAssoc_nil]
      refine foldl_congr (fun acc r =>
          if (codes[r]?.join) = none then some (acc.getD 0 + vals[r]?.getD 0) else acc)
        (fun acc r =>
          if refDecodeRow dict codes r = none then some (acc.getD 0 + vals[r]?.getD 0) else acc)
        sel none ?_
      intro r hr acc
      match hc : codes[r]? with
      | none => simp [refDecodeRow, hc]
      | some none => simp [refDecodeRow, hc]
      | some (some c) =>
          have hbound : c < dict.length := codeBound dict codes sel vals hb r hr c hc
          have hne : refDecodeRow dict codes r ≠ none := by
            simp only [refDecodeRow, hc]
            rw [List.getElem?_eq_getElem hbound]
            exact Option.some_ne_none _
          simp [hne]
  | some target =>
      simp only [queryCompactSum]
      rw [cg_compactDict, cg_compactSums, refGroupedSum_fold,
        lookup_ref_fold dict codes vals sel [] (some target), lookupAssoc_nil, hcd]
      match hfind :
          ((usedOf dict codes sel).map (fun cid => (dict[cid]?).getD 0)).findIdx?
            (fun x => x == target) with
      | none =>
          refine (foldl_none (fun acc r =>
              if refDecodeRow dict codes r = some target then
                some (acc.getD 0 + vals[r]?.getD 0)
              else acc) sel ?_).symm
          intro r hr
          have hne : refDecodeRow dict codes r ≠ some target := by
            match hc : codes[r]? with
            | none => simp [refDecodeRow, hc]
            | some none => simp [refDecodeRow, hc]
            | some (some c) =>
                have hc' : c ∈ usedOf dict codes sel :=
                  (mem_usedOf dict codes sel c).mpr ⟨r, hr, hc⟩
                have hall := List.findIdx?_eq_none_iff.mp hfind ((dict[c]?).getD 0)
                  (List.mem_map.mpr ⟨c, hc', rfl⟩)
                simp only [refDecodeRow, hc]
                match hd : dict[c]? with
                | none => simp
                | some v =>
                    have hv : ¬ (v = target) := by
                      have h2 := hall
                      rw [hd, Option.getD_some] at h2
                      simpa using h2
                    intro hcon
                    exact hv (Option.some.inj hcon)
          simp [hne]
      | some p =>
          obtain ⟨a, hpa, ha⟩ :=
            findIdx?_eq_some_get (α := Int) hfind
          have hplt : p < (usedOf dict codes sel).length := by
            have hlt : p < ((usedOf dict codes sel).map
                (fun cid => (dict[cid]?).getD 0)).length := getElem?_lt_length hpa
            rwa [List.length_map] at hlt
          have hdp : dict[(usedOf dict codes sel)[p]'hplt]? = some target := by
            have h1 : Option.map (fun cid => (dict[cid]?).getD 0)
                ((usedOf dict codes sel)[p]?) = some target := by
              rw [← List.getElem?_map, hpa, ha]
            rw [List.getElem?_eq_getElem hplt] at h1
            have h2 : (dict[(usedOf dict codes sel)[p]'hplt]?).getD 0 = target := by
              simpa using h1
            have hmem : (usedOf dict codes sel)[p]'hplt ∈ usedOf dict codes sel :=
              List.mem_of_getElem? (List.getElem?_eq_getElem hplt)
            have hx : (usedOf dict codes sel)[p]'hplt < dict.length :=
              usedOf_bound dict codes sel vals hb ((usedOf dict codes sel)[p]'hplt) hmem
            obtain ⟨v, hv⟩ : ∃ v, dict[(usedOf dict codes sel)[p]'hplt]? = some v :=
              ⟨dict[(usedOf dict codes sel)[p]'hplt]'hx, List.getElem?_eq_getElem hx⟩
            rw [hv, Option.getD_some] at h2
            exact hv.trans (congrArg some h2)
          show lookupAssoc (List.foldl (cmpStep dict codes vals (usedOf dict codes sel))
              ([], none) sel).1 p
              = List.foldl (fun acc rowIdx =>
                  if refDecodeRow dict codes rowIdx = some target then
                    some (acc.getD 0 + vals[rowIdx]?.getD 0)
                  else acc) none sel
          rw [lookup_cmp_fold dict codes vals (usedOf dict codes sel) hused sel [] none p hplt,
            lookupAssoc_nil]
          refine foldl_congr (fun acc r =>
              if codes[r]? = some (some ((usedOf dict codes sel)[p]'hplt)) then
                some (acc.getD 0 + vals[r]?.getD 0)
              else acc)
            (fun acc r =>
              if refDecodeRow dict codes r = some target then
                some (acc.getD 0 + vals[r]?.getD 0)
              else acc)
            sel none ?_
          intro r hr acc
          match hc : codes[r]? with
          | none => simp [refDecodeRow, hc]
          | some none => simp [refDecodeRow, hc]
          | some (some c) =>
              have hbound : c < dict.length := codeBound dict codes sel vals hb r hr c hc
              have hkey : dict[c]? = some target ↔ c = (usedOf dict codes sel)[p]'hplt := by
                constructor
                · intro hc'
                  exact hnd.eq_of_getElem?_eq hbound (hc'.trans hdp.symm)
                · intro hcon
                  rw [hcon]
                  exact hdp
              by_cases hcu : c = (usedOf dict codes sel)[p]'hplt
              · simp [hc, refDecodeRow, hdp, hcu]
              · simp [hc, refDecodeRow, hkey, hcu]
end TinylambOpt.ResearchProofs.GatheredSmallDictFastPath

-- C++ references to this model or its proof.
-- executor/detail/fused_dense_split.hpp
