import TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim

namespace TinylambOpt.ResearchProofs.SurrogateKeyChannelPivotLateDim

open TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim

private def stepSk (sk yr : Nat) : Bool × Int → FactRow → Bool × Int
  | (seen, acc), fr => if fr.sk == sk && fr.year == yr then (true, acc + fr.amt) else (seen, acc)

private def stepDr (sk0 : Nat) (bk0 : String) (amt : Int) : Bool × Int → DimRow → Bool × Int
  | (sn, sm), dr => if dr.sk == sk0 && dr.bk == bk0 then (true, sm + amt) else (sn, sm)

private def stepBk (d : List DimRow) (bk : String) (yr : Nat) : Bool × Int → FactRow → Bool × Int
  | (seen, acc), fr => if fr.year != yr then (seen, acc) else d.foldl (stepDr fr.sk bk fr.amt) (seen, acc)

private theorem boolAndFalse {a b : Bool} (h : ¬ (a = true ∧ b = true)) : (a && b) = false := by
  cases a <;> cases b <;> simp_all

private theorem fold_d_no_hit (d : List DimRow) (sk0 : Nat) (bk0 : String) (amt : Int)
    (h : ∀ x ∈ d, ¬ (x.sk = sk0 ∧ x.bk = bk0)) (s : Bool × Int) :
    d.foldl (stepDr sk0 bk0 amt) s = s := by
  induction d generalizing s with
  | nil => rfl
  | cons x xs ih =>
      have hx : ¬ (x.sk = sk0 ∧ x.bk = bk0) := h x (by simp)
      have hxBool : (x.sk == sk0 && x.bk == bk0) = false := by
        apply boolAndFalse
        intro hpair
        exact hx ⟨by simpa using hpair.1, by simpa using hpair.2⟩
      rw [List.foldl_cons]
      rw [show stepDr sk0 bk0 amt s x = s by cases s <;> simp [stepDr, hxBool]]
      exact ih (by intro y hy; exact h y (by simp [hy])) s

private theorem fold_d_unique_hit (d : List DimRow) (hu : UniqueSk d) (hi : InjectiveBk d)
    (sk0 : Nat) (bk0 : String) (amt : Int) (seen : Bool) (acc : Int) :
    d.foldl (stepDr sk0 bk0 amt) (seen, acc) =
      if (∃ x ∈ d, x.sk = sk0 ∧ x.bk = bk0) then (true, acc + amt) else (seen, acc) := by
  induction d generalizing seen acc with
  | nil => simp
  | cons x xs ih =>
      have hu' := hu
      simp [UniqueSk] at hu'
      have hi_tail : InjectiveBk xs := by
        intro a ha b hb hbk
        exact hi a (by simp [ha]) b (by simp [hb]) hbk
      rw [List.foldl_cons]
      by_cases hx : x.sk = sk0 ∧ x.bk = bk0
      · have hno : ¬ ∃ y ∈ xs, y.sk = sk0 ∧ y.bk = bk0 := by
          rintro ⟨y, hy, hysk, hybk⟩
          have hbkxy : x.bk = y.bk := hx.2.trans hybk.symm
          have hskxy : x.sk = y.sk := hi x (by simp) y (by simp [hy]) hbkxy
          exact hu'.1 y hy hskxy.symm
        rw [show stepDr sk0 bk0 amt (seen, acc) x = (true, acc + amt) by simp [stepDr, hx]]
        rw [fold_d_no_hit xs sk0 bk0 amt
          (by intro y hy hpair; exact hno ⟨y, hy, hpair⟩) (true, acc + amt)]
        simp [hx]
      · have hxBool : (x.sk == sk0 && x.bk == bk0) = false := by
          apply boolAndFalse
          intro hpair
          exact hx ⟨by simpa using hpair.1, by simpa using hpair.2⟩
        rw [show stepDr sk0 bk0 amt (seen, acc) x = (seen, acc) by simp [stepDr, hxBool]]
        rw [ih hu'.2 hi_tail seen acc]
        simp [hx]

private theorem sumForBkYear_eq_sumForSkYear (d : List DimRow) (f : List FactRow)
    (hu : UniqueSk d) (hi : InjectiveBk d) (dr : DimRow) (hdr : dr ∈ d) (yr : Nat) :
    sumForBkYear d f dr.bk yr = sumForSkYear f dr.sk yr := by
  unfold sumForBkYear sumForSkYear
  have H : ∀ (s : Bool × Int),
      f.foldl (stepBk d dr.bk yr) s = f.foldl (stepSk dr.sk yr) s := by
    intro s
    induction f generalizing s with
    | nil => rfl
    | cons fr f ih =>
        rcases s with ⟨seen, acc⟩
        rw [List.foldl_cons, List.foldl_cons]
        by_cases hy : (fr.year == yr) = true
        · have hyEq : fr.year = yr := by simpa using hy
          have hne : (fr.year != yr) = false := by simp [hyEq]
          have hfold := fold_d_unique_hit d hu hi fr.sk dr.bk fr.amt seen acc
          have hex : (∃ x ∈ d, x.sk = fr.sk ∧ x.bk = dr.bk) ↔ dr.sk = fr.sk := by
            constructor
            · rintro ⟨x, hx, hxsk, hxbk⟩
              have hskx : x.sk = dr.sk := hi x hx dr hdr hxbk
              exact hskx.symm.trans hxsk
            · intro h
              exact ⟨dr, hdr, h, rfl⟩
          rw [show stepBk d dr.bk yr (seen, acc) fr =
                (if dr.sk == fr.sk then (true, acc + fr.amt) else (seen, acc)) by
              simp [stepBk, hne, hfold, hex]]
          rw [show stepSk dr.sk yr (seen, acc) fr =
                (if dr.sk == fr.sk then (true, acc + fr.amt) else (seen, acc)) by
              by_cases hs : (dr.sk == fr.sk) = true
              · have heq : dr.sk = fr.sk := by simpa using hs
                have hs' : (fr.sk == dr.sk) = true := by simp [heq.symm]
                simp [stepSk, hs, hs', hyEq]
              · have hsne : (fr.sk == dr.sk) = false := by
                  cases hfr : (fr.sk == dr.sk) <;> simp_all
                simp [stepSk, hs, hsne]]
          exact ih (if dr.sk == fr.sk then (true, acc + fr.amt) else (seen, acc))
        · have hne : (fr.year != yr) = true := by
            cases h : (fr.year == yr) <;> simp_all
          rw [show stepBk d dr.bk yr (seen, acc) fr = (seen, acc) by simp [stepBk, hne]]
          have hyfalse : (fr.sk == dr.sk && fr.year == yr) = false := by
            apply boolAndFalse
            intro hpair
            exact hy (by simpa using hpair.2)
          rw [show stepSk dr.sk yr (seen, acc) fr = (seen, acc) by simp [stepSk, hyfalse]]
          exact ih (seen, acc)
  exact H (false, 0)

private theorem bk_nodup_of (d : List DimRow) (hu : UniqueSk d) (hi : InjectiveBk d) :
    (d.map DimRow.bk).Nodup := by
  induction d with
  | nil => simp
  | cons x xs ih =>
      have hu' := hu
      simp [UniqueSk] at hu'
      have hi_tail : InjectiveBk xs := by
        intro a ha b hb hbk
        exact hi a (by simp [ha]) b (by simp [hb]) hbk
      change (x.bk :: xs.map DimRow.bk).Nodup
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rw [List.mem_map] at hmem
        rcases hmem with ⟨y, hy, hybk⟩
        have hsk : x.sk = y.sk := hi x (by simp) y (by simp [hy]) hybk.symm
        exact hu'.1 y hy hsk.symm
      · exact ih hu'.2 hi_tail

private theorem foldl_dedup_acc (l : List String) (hnd : l.Nodup) (acc : List String)
    (hdisj : ∀ x ∈ acc, x ∉ l) :
    l.foldl (fun acc bk => if acc.contains bk then acc else acc ++ [bk]) acc = acc ++ l := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      have hnd' : b ∉ bs ∧ bs.Nodup := by simpa using hnd
      have hbnotbs : b ∉ bs := hnd'.1
      have hbsnd : bs.Nodup := hnd'.2
      have hbnot : b ∉ acc := by
        intro hbacc
        exact hdisj b hbacc (by simp)
      have hbfalse : acc.contains b = false := by
        cases hc : acc.contains b <;> simp_all
      rw [List.foldl_cons]
      have hif : (if acc.contains b = true then acc else acc ++ [b]) = acc ++ [b] := by
        simp [hbnot]
      rw [hif]
      rw [ih hbsnd (acc ++ [b])]
      · simp [List.append_assoc]
      · intro z hz
        have hz' : z ∈ acc ∨ z = b := by simpa using hz
        rcases hz' with hzacc | hzb
        · intro hzbs
          exact hdisj z hzacc (by simp [hzbs])
        · simpa [hzb] using hbnotbs

private theorem uniqBks_eq_of (d : List DimRow) (hu : UniqueSk d) (hi : InjectiveBk d) :
    uniqBks d = d.map DimRow.bk := by
  unfold uniqBks
  change (d.map DimRow.bk).foldl
    (fun acc bk => if acc.contains bk then acc else acc ++ [bk]) [] = d.map DimRow.bk
  have hbk : (d.map DimRow.bk).Nodup := bk_nodup_of d hu hi
  simpa using (foldl_dedup_acc (d.map DimRow.bk) hbk [] (by simp))

private theorem lookupWideForBk_self (d : List DimRow) (hu : UniqueSk d) (hi : InjectiveBk d)
    (dr : DimRow) (hdr : dr ∈ d) : lookupWideForBk d dr.bk = dr.wide := by
  unfold lookupWideForBk
  induction d with
  | nil => simp at hdr
  | cons x xs ih =>
      have hu' := hu
      simp [UniqueSk] at hu'
      have hi_tail : InjectiveBk xs := by
        intro a ha b hb hbk
        exact hi a (by simp [ha]) b (by simp [hb]) hbk
      by_cases hbk : (x.bk == dr.bk) = true
      · have hbk_eq : x.bk = dr.bk := by simpa using hbk
        have hsk_eq : x.sk = dr.sk := hi x (by simp) dr (by simp [hdr]) hbk_eq
        have hxdr : x = dr := by
          by_cases hEq : x = dr
          · exact hEq
          · exfalso
            have hdr_xs : dr ∈ xs := by
              rcases (by simpa using hdr : dr = x ∨ dr ∈ xs) with hdx | hxs
              · exfalso
                exact hEq hdx.symm
              · exact hxs
            exact hu'.1 dr hdr_xs hsk_eq.symm
        subst x
        simp
      · have hdr_xs : dr ∈ xs := by
          rcases (by simpa using hdr : dr = x ∨ dr ∈ xs) with rfl | hxs
          · exact (hbk (by simp)).elim
          · exact hxs
        simp [hbk]
        exact ih hu'.2 hi_tail hdr_xs

private theorem sumForSkYear_filter_contains_acc (f : List FactRow) (active : List Nat)
    (sk yr : Nat) (h : active.contains sk = true) (s : Bool × Int) :
    (f.filter (fun fr => active.contains fr.sk)).foldl (stepSk sk yr) s =
      f.foldl (stepSk sk yr) s := by
  induction f generalizing s with
  | nil => rfl
  | cons fr f ih =>
      rw [List.filter_cons]
      by_cases hkeep : active.contains fr.sk = true
      · rw [ite_eq_left hkeep, List.foldl_cons, List.foldl_cons]
        exact ih _
      · rw [ite_eq_right hkeep, List.foldl_cons]
        have hne : fr.sk ≠ sk := by
          intro heq
          apply hkeep
          simpa [heq] using h
        have hstep : stepSk sk yr s fr = s := by
          rcases s with ⟨seen, acc⟩
          by_cases hs : (fr.sk == sk) = true
          · exact (hne (by simpa using hs)).elim
          · simp [stepSk, hs]
        rw [hstep]
        exact ih s

private theorem sumForSkYear_filter_contains (f : List FactRow) (active : List Nat)
    (sk yr : Nat) (h : active.contains sk = true) :
    sumForSkYear (f.filter (fun fr => active.contains fr.sk)) sk yr = sumForSkYear f sk yr := by
  unfold sumForSkYear
  exact sumForSkYear_filter_contains_acc f active sk yr h (false, 0)

private def refEmit (d : List DimRow) (fS fL : List FactRow) (bk : String) : Option PivotOut :=
  let (hs1, s1) := sumForBkYear d fS bk 1
  let (hs2, s2) := sumForBkYear d fS bk 2
  let (hl1, l1) := sumForBkYear d fL bk 1
  let (hl2, l2) := sumForBkYear d fL bk 2
  if hs1 && hs2 && hl1 && hl2 && 0 < s1 && 0 < l1 && l2 * s1 > s2 * l1 then
    some ⟨bk, lookupWideForBk d bk, s1, s2, l1, l2⟩
  else none

private def validSk (fS : List FactRow) (dr : DimRow) : Option (DimRow × Int × Int) :=
  let (hs1, s1) := sumForSkYear fS dr.sk 1
  let (hs2, s2) := sumForSkYear fS dr.sk 2
  if hs1 && hs2 && 0 < s1 then some (dr, s1, s2) else none

private def activeSks (d : List DimRow) (fS : List FactRow) : List Nat :=
  (d.filterMap (validSk fS)).map (fun (dr, _, _) => dr.sk)

private def narrow (d : List DimRow) (fS fL : List FactRow) : List FactRow :=
  fL.filter (fun fr => (activeSks d fS).contains fr.sk)

private def optEmit (fNarrow : List FactRow) (v : DimRow × Int × Int) : Option PivotOut :=
  let (dr, s1, s2) := v
  let (hl1, l1) := sumForSkYear fNarrow dr.sk 1
  let (hl2, l2) := sumForSkYear fNarrow dr.sk 2
  if hl1 && hl2 && 0 < l1 && l2 * s1 > s2 * l1 then
    some ⟨dr.bk, dr.wide, s1, s2, l1, l2⟩
  else none

private theorem emit_eq (d : List DimRow) (fS fL : List FactRow)
    (hu : UniqueSk d) (hi : InjectiveBk d) (dr : DimRow) (hdr : dr ∈ d) :
    refEmit d fS fL dr.bk =
      match validSk fS dr with
      | none => none
      | some v => optEmit (narrow d fS fL) v := by
  have hb1 := sumForBkYear_eq_sumForSkYear d fS hu hi dr hdr 1
  have hb2 := sumForBkYear_eq_sumForSkYear d fS hu hi dr hdr 2
  have hl1 := sumForBkYear_eq_sumForSkYear d fL hu hi dr hdr 1
  have hl2 := sumForBkYear_eq_sumForSkYear d fL hu hi dr hdr 2
  have hw := lookupWideForBk_self d hu hi dr hdr
  unfold refEmit validSk narrow activeSks optEmit
  rw [hb1, hb2, hl1, hl2, hw]
  cases hx1 : sumForSkYear fS dr.sk 1 with
  | mk hs1 s1 =>
  cases hx2 : sumForSkYear fS dr.sk 2 with
  | mk hs2 s2 =>
  cases hy1 : sumForSkYear fL dr.sk 1 with
  | mk hyl1 l1 =>
  cases hy2 : sumForSkYear fL dr.sk 2 with
  | mk hyl2 l2 =>
    dsimp only
    by_cases hsmall : (hs1 && hs2 && decide (0 < s1)) = true
    · have hv_mem : (dr, s1, s2) ∈ d.filterMap (validSk fS) := by
        rw [List.mem_filterMap]
        refine ⟨dr, hdr, ?_⟩
        simp [validSk, hx1, hx2, hsmall]
      have hsk_mem : dr.sk ∈ (d.filterMap (validSk fS)).map (fun v : DimRow × Int × Int => v.1.sk) := by
        rw [List.mem_map]
        exact ⟨(dr, s1, s2), hv_mem, rfl⟩
      have hcont : ((d.filterMap (validSk fS)).map (fun v : DimRow × Int × Int => v.1.sk)).contains dr.sk = true := by
        simpa using hsk_mem
      have hn1 := sumForSkYear_filter_contains fL
        ((d.filterMap (validSk fS)).map (fun v : DimRow × Int × Int => v.1.sk)) dr.sk 1 hcont
      have hn2 := sumForSkYear_filter_contains fL
        ((d.filterMap (validSk fS)).map (fun v : DimRow × Int × Int => v.1.sk)) dr.sk 2 hcont
      rw [show (if (hs1 && hs2 && decide (0 < s1)) = true then some (dr, s1, s2) else none) =
          some (dr, s1, s2) by simp [hsmall]]
      simp only
      rw [hn1, hn2, hy1, hy2]
      have hsmallProp : (hs1 = true ∧ hs2 = true) ∧ 0 < s1 := by
        simpa [Bool.and_eq_true] using hsmall
      rcases hsmallProp with ⟨⟨hhs1, hhs2⟩, hpos⟩
      simp [hhs1, hhs2, hpos]
    · have hsmallProp : ¬ ((hs1 = true ∧ hs2 = true) ∧ 0 < s1) := by
        intro h
        apply hsmall
        simpa [Bool.and_eq_true] using h
      simp [hsmallProp]
      intro h1 h2 _ _ h5 _
      exact (hsmallProp ⟨⟨h1, h2⟩, h5⟩).elim

private theorem filterMap_filterMap (l : List DimRow)
    (f : DimRow → Option (DimRow × Int × Int)) (g : DimRow × Int × Int → Option PivotOut) :
    (l.filterMap f).filterMap g =
      l.filterMap (fun a => match f a with | none => none | some b => g b) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      cases hfa : f a with
      | none => simp [hfa, ih]
      | some b => simp [List.filterMap_cons, hfa, ih]

private theorem filterMap_ext (l : List DimRow) (f g : DimRow → Option PivotOut)
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      cases hfa : f a with
      | none =>
          have hga : g a = none := by
            have hh := h a (by simp)
            rw [hfa] at hh
            exact hh.symm
          simp [hfa, hga, ih (by intro x hx; exact h x (by simp [hx]))]
      | some b =>
          have hga : g a = some b := by
            have hh := h a (by simp)
            rw [hfa] at hh
            exact hh.symm
          simp [hfa, hga, ih (by intro x hx; exact h x (by simp [hx]))]

private def refOut (d : List DimRow) (fS fL : List FactRow) : List PivotOut :=
  (uniqBks d).filterMap (refEmit d fS fL)

private def optOut (d : List DimRow) (fS fL : List FactRow) : List PivotOut :=
  (d.filterMap (validSk fS)).filterMap (optEmit (narrow d fS fL))

private theorem lists_eq (d : List DimRow) (fS fL : List FactRow)
    (hu : UniqueSk d) (hi : InjectiveBk d) : optOut d fS fL = refOut d fS fL := by
  unfold optOut refOut
  rw [uniqBks_eq_of d hu hi]
  rw [List.filterMap_map]
  rw [filterMap_filterMap]
  apply filterMap_ext
  intro dr hdr
  exact (emit_eq d fS fL hu hi dr hdr).symm

theorem sound : TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim.Goal := by
  constructor
  · intro d fS fL hu hi
    unfold optSkPivotLateDim refPivot
    change sortOut (optOut d fS fL) = sortOut (refOut d fS fL)
    rw [lists_eq d fS fL hu hi]
  · refine ⟨[{ sk := 1, bk := "C1", wide := "Alice" }, { sk := 2, bk := "C1", wide := "Alice" }],
      [{ sk := 1, year := 1, amt := 10 }, { sk := 2, year := 2, amt := 10 }],
      [{ sk := 1, year := 1, amt := 10 }, { sk := 2, year := 2, amt := 30 }], ?_, ?_⟩
    · simp [UniqueSk]
    · decide

end TinylambOpt.ResearchProofs.SurrogateKeyChannelPivotLateDim
