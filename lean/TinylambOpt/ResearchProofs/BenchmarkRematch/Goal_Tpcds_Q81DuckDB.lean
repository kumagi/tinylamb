import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q81DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem stable_sort_fst {α : Type} {le : α → α → Bool}
    (trans : ∀ (a b c : α), le a b = true → le b c = true → le a c = true)
    (total : ∀ (a b : α), (le a b || le b a) = true) :
    ∀ (l : List (α × Nat)),
      l.Pairwise (fun p q => p.snd < q.snd) →
      List.map Prod.fst (l.mergeSort (List.zipIdxLE le)) = (List.map Prod.fst l).mergeSort le := by
  intro l0
  have main : ∀ n : Nat, ∀ l : List (α × Nat), l.length = n →
      l.Pairwise (fun p q => p.snd < q.snd) →
      List.map Prod.fst (l.mergeSort (List.zipIdxLE le)) = (List.map Prod.fst l).mergeSort le := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
      intro l hlen hp
      subst n
      cases l with
      | nil => simp
      | cons a t =>
          cases t with
          | nil =>
              simp [List.mergeSort_singleton]
          | cons b t =>
              let l0 : List (α × Nat) := a :: b :: t
              let L : { l : List (α × Nat) // l.length = l0.length } := ⟨l0, rfl⟩
              let f : List (α × Nat) := (List.MergeSort.Internal.splitInTwo L).1.val
              let s : List (α × Nat) := (List.MergeSort.Internal.splitInTwo L).2.val
              have hfs : f ++ s = l0 := List.MergeSort.Internal.splitInTwo_fst_append_splitInTwo_snd L
              have hf_def : f = List.take ((l0.length + 1) / 2) l0 :=
                congrArg Subtype.val (List.MergeSort.Internal.splitInTwo_fst L)
              have hs_def : s = List.drop ((l0.length + 1) / 2) l0 :=
                congrArg Subtype.val (List.MergeSort.Internal.splitInTwo_snd L)
              have hf_len : f.length = (l0.length + 1) / 2 := by
                simpa [f, L] using (List.MergeSort.Internal.splitInTwo L).1.property
              have hs_len : s.length = l0.length / 2 := by
                simpa [s, L] using (List.MergeSort.Internal.splitInTwo L).2.property
              have hn2 : 2 ≤ l0.length := by simp [l0]
              have hf_lt : f.length < l0.length := by
                rw [hf_len]
                omega
              have hs_lt : s.length < l0.length := by
                rw [hs_len]
                omega
              have hpf : f.Pairwise (fun p q => p.snd < q.snd) := by
                rw [hf_def]
                exact hp.take
              have hps : s.Pairwise (fun p q => p.snd < q.snd) := by
                rw [hs_def]
                exact hp.drop
              have ihf := ih f.length hf_lt f rfl hpf
              have ihs := ih s.length hs_lt s rfl hps
              have hcross_lt : ∀ x ∈ f, ∀ y ∈ s, x.snd < y.snd := by
                intro x hx y hy
                simpa [L] using
                  (List.MergeSort.Internal.splitInTwo_fst_le_splitInTwo_snd
                    (l := L) hp x y hx hy)
              have hcross_le : ∀ x ∈ f, ∀ y ∈ s, x.snd ≤ y.snd := by
                intro x hx y hy
                exact Nat.le_of_lt (hcross_lt x hx y hy)
              have hsf_le : s.length ≤ f.length := by
                rw [hf_len, hs_len]
                omega
              have hfs_le : f.length ≤ s.length + 1 := by
                rw [hf_len, hs_len]
                omega
              calc
                List.map Prod.fst (l0.mergeSort (List.zipIdxLE le))
                    = List.map Prod.fst ((f.mergeSort (List.zipIdxLE le)).merge (s.mergeSort (List.zipIdxLE le)) (List.zipIdxLE le)) := by
                        rw [← hfs]
                        rw [List.mergeSort_append f s hsf_le hfs_le]
                _ = (List.map Prod.fst (f.mergeSort (List.zipIdxLE le))).merge
                      (List.map Prod.fst (s.mergeSort (List.zipIdxLE le))) le := by
                        exact List.merge_stable (f.mergeSort (List.zipIdxLE le)) (s.mergeSort (List.zipIdxLE le)) (by
                          intro x y hx hy
                          have hx' : x ∈ f := List.mem_mergeSort.mp hx
                          have hy' : y ∈ s := List.mem_mergeSort.mp hy
                          exact hcross_le x hx' y hy')
                _ = ((List.map Prod.fst f).mergeSort le).merge ((List.map Prod.fst s).mergeSort le) le := by
                        rw [ihf, ihs]
                _ = ((List.map Prod.fst f ++ List.map Prod.fst s).mergeSort le) := by
                        rw [List.mergeSort_append (List.map Prod.fst f) (List.map Prod.fst s)
                          (by simpa using hsf_le) (by simpa using hfs_le)]
                _ = (List.map Prod.fst l0).mergeSort le := by
                        rw [← List.map_append, hfs]
  exact main l0.length l0 rfl

private theorem pairwise_lt_snd_eq_of_mem {α : Type} {l : List (α × Nat)}
    (hp : l.Pairwise (fun p q => p.snd < q.snd)) {x y : α × Nat}
    (hx : x ∈ l) (hy : y ∈ l) (hidx : x.snd = y.snd) : x = y := by
  induction l generalizing x y with
  | nil => simp at hx
  | cons z t ih =>
      have hp' := List.pairwise_cons.mp hp
      simp only [List.mem_cons] at hx hy
      rcases hx with rfl | hx
      · rcases hy with rfl | hy
        · rfl
        · have hlt := hp'.1 y hy
          omega
      · rcases hy with rfl | hy
        · have hlt := hp'.1 x hx
          omega
        · exact ih hp'.2 hx hy hidx

private theorem zipIdxLE_both_snd_le {α : Type} {le : α → α → Bool} {x y : α × Nat}
    (hxy : List.zipIdxLE le x y = true) (hyx : List.zipIdxLE le y x = true) :
    x.snd ≤ y.snd ∧ y.snd ≤ x.snd := by
  unfold List.zipIdxLE at hxy hyx
  by_cases h1 : le x.fst y.fst = true
  · by_cases h2 : le y.fst x.fst = true
    · simp [h1, h2] at hxy hyx
      exact ⟨by simpa using hxy, by simpa using hyx⟩
    · simp [h2] at hyx
  · simp [h1] at hxy

private theorem stable_filter {α : Type} {le : α → α → Bool}
    (trans : ∀ (a b c : α), le a b = true → le b c = true → le a c = true)
    (total : ∀ (a b : α), (le a b || le b a) = true)
    {q : α × Nat → Bool} {l : List (α × Nat)}
    (hp : l.Pairwise (fun p q => p.snd < q.snd)) :
    ((l.mergeSort (List.zipIdxLE le)).filter q) =
      (l.filter q).mergeSort (List.zipIdxLE le) := by
  let zle := List.zipIdxLE le
  have hpLeft : ((l.mergeSort zle).filter q).Pairwise (fun a b => zle a b = true) := by
    exact (List.pairwise_mergeSort (List.zipIdxLE_trans trans) (List.zipIdxLE_total total) l).filter q
  have hpRight : ((l.filter q).mergeSort zle).Pairwise (fun a b => zle a b = true) := by
    exact List.pairwise_mergeSort (List.zipIdxLE_trans trans) (List.zipIdxLE_total total) (l.filter q)
  have hperm1 : ((l.mergeSort zle).filter q).Perm (l.filter q) := by
    exact (List.mergeSort_perm l zle).filter q
  have hperm2 : (l.filter q).Perm ((l.filter q).mergeSort zle) := by
    exact (List.mergeSort_perm (l.filter q) zle).symm
  have hperm := hperm1.trans hperm2
  exact List.Perm.eq_of_pairwise
    (by
      intro x y hx hy hxy hyx
      have hxL : x ∈ l.mergeSort zle := (List.mem_filter.mp hx).1
      have hyF : y ∈ l.filter q := List.mem_mergeSort.mp hy
      have hxl : x ∈ l := List.mem_mergeSort.mp hxL
      have hyl : y ∈ l := (List.mem_filter.mp hyF).1
      have hidxPair := zipIdxLE_both_snd_le hxy hyx
      have hidx : x.snd = y.snd := Nat.le_antisymm hidxPair.1 hidxPair.2
      exact pairwise_lt_snd_eq_of_mem hp hxl hyl hidx)
    hpLeft hpRight hperm

private theorem map_fst_zipIdx : ∀ {α : Type} (l : List α) (k : Nat),
    (l.zipIdx k).map Prod.fst = l := by
  intro α l
  induction l with
  | nil => intro k; simp
  | cons a t ih =>
      intro k
      rw [List.zipIdx_cons]
      simp [ih]

private theorem map_fst_filter {α β : Type} (f : α → β) (p : β → Bool) : ∀ l : List α,
    (List.map f (l.filter (fun x => p (f x)))) = (List.map f l).filter p := by
  intro l
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : p (f a) = true <;> simp [h, ih]

private theorem filter_prefix {α : Type} (le : α → α → Prop) (p : α → Bool)
    (hclosed : ∀ a b, le a b → p b = true → p a = true) :
    ∀ l : List α, l.Pairwise le → l.filter p = l.take (l.filter p).length := by
  intro l hp
  induction l with
  | nil => simp
  | cons a t ih =>
      have hp' := List.pairwise_cons.mp hp
      by_cases ha : p a = true
      · have iht := ih hp'.2
        rw [List.filter_cons, ite_eq_left ha]
        simp only [List.length_cons]
        change a :: t.filter p = a :: t.take (t.filter p).length
        exact congrArg (List.cons a) iht
      · have hz : t.filter p = [] := by
          rw [List.filter_eq_nil_iff]
          intro b hb hpb
          exact ha (hclosed a b (hp'.1 b hb) hpb)
        simp [ha, hz]

private theorem sorted_getElem_le {l : List Nat} (hp : l.Pairwise (fun a b => a ≤ b))
    {i j : Nat} (hi : i < l.length) (hj : j < l.length) (hij : i ≤ j) : l[i] ≤ l[j] := by
  rw [List.pairwise_iff_getElem] at hp
  by_cases hEq : i = j
  · subst j
    exact Nat.le_refl _
  · exact hp i j hi hj (Nat.lt_of_le_of_ne hij hEq)


theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q81DuckDB.Goal := by
  intro ids addressKey customerKey
  dsimp only
  let cmp : Nat → Nat → Bool := fun a b => decide (customerKey a < customerKey b ∨
    (customerKey a = customerKey b ∧ addressKey a ≤ addressKey b))
  let prefixKeys : List Nat := (ids.map customerKey).mergeSort
  let p : Nat → Bool := fun i => prefixKeys[99]?.all (customerKey i ≤ ·)
  let full : List Nat := ids.mergeSort cmp
  change ((ids.filter p).mergeSort cmp).take 100 = full.take 100

  have hcmp_key_le : ∀ a b : Nat, cmp a b = true → customerKey a ≤ customerKey b := by
    intro a b h
    simp [cmp] at h
    rcases h with h | ⟨h, _⟩ <;> omega
  have hcmp_trans : ∀ a b c : Nat, cmp a b = true → cmp b c = true → cmp a c = true := by
    intro a b c hab hbc
    simp [cmp] at hab hbc ⊢
    rcases hab with hab | ⟨hab, habaddr⟩
    · rcases hbc with hbc | ⟨hbc, hbcaddr⟩
      · omega
      · left
        omega
    · rcases hbc with hbc | ⟨hbc, hbcaddr⟩
      · left
        omega
      · right
        constructor <;> omega
  have hcmp_total : ∀ a b : Nat, (cmp a b || cmp b a) = true := by
    intro a b
    by_cases hlt : customerKey a < customerKey b
    · simp [cmp, hlt]
    · by_cases heq : customerKey a = customerKey b
      · by_cases haddr : addressKey a ≤ addressKey b
        · simp [cmp, heq, haddr]
        · have haddr' : addressKey b ≤ addressKey a := by omega
          simp [cmp, heq, haddr, haddr']
      · have hltba : customerKey b < customerKey a := by omega
        simp [cmp, hlt, hltba]

  have hnat_trans : ∀ a b c : Nat, decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intro a b c hab hbc
    simp at hab hbc ⊢
    omega
  have hnat_total : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have h' : b ≤ a := by omega
      simp [h, h']

  have hprefixPair : prefixKeys.Pairwise (fun a b => a ≤ b) := by
    simpa [prefixKeys] using
      (List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b)) hnat_trans hnat_total
        (ids.map customerKey))
  have hfullPair : full.Pairwise (fun a b => cmp a b = true) := by
    simpa [full] using (List.pairwise_mergeSort (le := cmp) hcmp_trans hcmp_total ids)
  have hkeyPair : (full.map customerKey).Pairwise (fun a b => a ≤ b) := by
    have hp := hfullPair
    rw [List.pairwise_iff_getElem] at hp ⊢
    intro i j hi hj hij
    have hiF : i < full.length := by simpa using hi
    have hjF : j < full.length := by simpa using hj
    have hle := hcmp_key_le full[i] full[j] (hp i j hiF hjF hij)
    simpa [List.getElem_map customerKey (l := full) (h := hi),
      List.getElem_map customerKey (l := full) (h := hj)] using hle
  have hpermFull : (full.map customerKey).Perm (ids.map customerKey) := by
    simpa [full] using (List.mergeSort_perm ids (le := cmp)).map customerKey
  have hpermPrefix : prefixKeys.Perm (ids.map customerKey) := by
    simpa [prefixKeys] using
      (List.mergeSort_perm (ids.map customerKey) (le := fun a b : Nat => decide (a ≤ b)))
  have hkeyEq : full.map customerKey = prefixKeys := by
    exact List.Perm.eq_of_pairwise
      (by intro a b ha hb hab hba; exact Nat.le_antisymm hab hba)
      hkeyPair hprefixPair (hpermFull.trans hpermPrefix.symm)

  have hfirstP : ∀ i : Nat, (hiN : i < full.length) → i < 100 → p full[i] = true := by
    intro i hiN hi100
    rcases hopt : prefixKeys[99]? with _ | t
    · simp [p, hopt]
    · have h99lt : 99 < prefixKeys.length := by
        by_cases h : 99 < prefixKeys.length
        · exact h
        · have hle : prefixKeys.length ≤ 99 := Nat.le_of_not_gt h
          have hn : prefixKeys[99]? = none := List.getElem?_eq_none_iff.mpr hle
          exact absurd hopt (by rw [hn]; simp)
      have htEq : prefixKeys[99] = t := by
        have hs := List.getElem?_eq_getElem h99lt
        simpa [hopt] using hs.symm
      have hlenEq : prefixKeys.length = full.length := by
        have hlen := congrArg List.length hkeyEq
        simpa using hlen.symm
      have hiPref : i < prefixKeys.length := by omega
      have hiMap : i < (full.map customerKey).length := by simpa using hiN
      have hmapOpt : prefixKeys[i]? = some (customerKey full[i]) := by
        have hm := List.getElem?_map (f := customerKey) (l := full) (i := i)
        have hfsome := List.getElem?_eq_getElem hiN
        rw [hfsome] at hm
        simpa [hkeyEq] using hm
      have hprefSome := List.getElem?_eq_getElem hiPref
      have hkeyIdx : customerKey full[i] = prefixKeys[i] := by
        exact Option.some.inj (hmapOpt.symm.trans hprefSome)
      have hleKey : prefixKeys[i] ≤ prefixKeys[99] :=
        sorted_getElem_le hprefixPair hiPref h99lt (by omega)
      have hleT : prefixKeys[i] ≤ t := by simpa [htEq] using hleKey
      simp [p, hopt, hkeyIdx, hleT]

  have htakeP : ∀ x ∈ full.take 100, p x = true := by
    intro x hx
    rcases List.mem_iff_getElem.mp hx with ⟨i, hi, hxi⟩
    have hiTakeLen : i < (full.take 100).length := hi
    have hi100 : i < 100 := by
      have hlen := List.length_take (i := 100) (l := full)
      omega
    have hiN : i < full.length := by
      have hlen := List.length_take (i := 100) (l := full)
      omega
    have hxEq : full[i] = x := by
      rw [← hxi]
      exact (List.getElem_take (xs := full) (j := 100) (i := i) (h := hiTakeLen)).symm
    rw [← hxEq]
    exact hfirstP i hiN hi100
  have htakeFilter : (full.take 100).filter p = full.take 100 :=
    List.filter_eq_self.mpr htakeP

  have hclosed : ∀ a b : Nat, cmp a b = true → p b = true → p a = true := by
    intro a b hab hpb
    rcases hopt : prefixKeys[99]? with _ | t
    · simp [p, hopt]
    · simp [p, hopt] at hpb ⊢
      have hle := hcmp_key_le a b hab
      omega
  have hprefixFull : full.filter p = full.take (full.filter p).length :=
    filter_prefix (fun a b => cmp a b = true) p hclosed full hfullPair
  have hlenLowerMin : min 100 full.length ≤ (full.filter p).length := by
    have hsublen := ((List.take_sublist 100 full).filter p).length_le
    rw [htakeFilter] at hsublen
    simpa [List.length_take] using hsublen

  have htakeFull : (full.filter p).take 100 = full.take 100 := by
    by_cases hle100 : full.length ≤ 100
    · have hlower : full.length ≤ (full.filter p).length := by
        have := hlenLowerMin
        simpa [Nat.min_eq_right hle100] using this
      have hupper : (full.filter p).length ≤ full.length := List.length_filter_le p full
      have hflen : (full.filter p).length = full.length := Nat.le_antisymm hupper hlower
      rw [hprefixFull, hflen, List.take_length]
    · have h100len : 100 ≤ full.length := by omega
      have hflen100 : 100 ≤ (full.filter p).length := by
        have := hlenLowerMin
        simpa [Nat.min_eq_left h100len] using this
      rw [hprefixFull, List.take_take]
      rw [Nat.min_eq_left hflen100]

  let idsZ : List (Nat × Nat) := ids.zipIdx
  let q : Nat × Nat → Bool := fun z => p z.1
  have hzPair : idsZ.Pairwise (fun z w => z.snd < w.snd) := by
    rw [List.pairwise_iff_getElem]
    intro i j hi hj hij
    have hi' : i < (ids.zipIdx).length := by simpa [idsZ] using hi
    have hj' : j < (ids.zipIdx).length := by simpa [idsZ] using hj
    have hiIds : i < ids.length := by simpa using hi'
    have hjIds : j < ids.length := by simpa using hj' 
    rw [show idsZ[i] = (ids[i], i) by simp [idsZ],
      show idsZ[j] = (ids[j], j) by simp [idsZ]]
    simp
    omega
  have hstable :
      ((idsZ.mergeSort (List.zipIdxLE cmp)).filter q) =
        (idsZ.filter q).mergeSort (List.zipIdxLE cmp) :=
    stable_filter hcmp_trans hcmp_total hzPair
  have hmapLeft :
      List.map Prod.fst ((idsZ.mergeSort (List.zipIdxLE cmp)).filter q) = full.filter p := by
    have h1 : List.map Prod.fst ((idsZ.mergeSort (List.zipIdxLE cmp)).filter q) =
        (List.map Prod.fst (idsZ.mergeSort (List.zipIdxLE cmp))).filter p := by
      simpa [q] using
        (map_fst_filter Prod.fst p (idsZ.mergeSort (List.zipIdxLE cmp)))
    have hmapFull : List.map Prod.fst (idsZ.mergeSort (List.zipIdxLE cmp)) = full := by
      simpa [idsZ, full] using (List.mergeSort_zipIdx (le := cmp) (l := ids))
    rw [hmapFull] at h1
    exact h1
  have hmapRight :
      List.map Prod.fst ((idsZ.filter q).mergeSort (List.zipIdxLE cmp)) =
        (ids.filter p).mergeSort cmp := by
    have hs := stable_sort_fst hcmp_trans hcmp_total (idsZ.filter q) (hzPair.filter q)
    have hmf : List.map Prod.fst (idsZ.filter q) = ids.filter p := by
      have h := map_fst_filter Prod.fst p idsZ
      simpa [q, idsZ, map_fst_zipIdx] using h
    rw [hmf] at hs
    exact hs
  have hcandEqFilter : (ids.filter p).mergeSort cmp = full.filter p := by
    have hmapStable := congrArg (List.map Prod.fst) hstable
    rw [hmapLeft, hmapRight] at hmapStable
    exact hmapStable.symm

  calc
    ((ids.filter p).mergeSort cmp).take 100 = (full.filter p).take 100 := by rw [hcandEqFilter]
    _ = full.take 100 := htakeFull

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q81DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q81DuckDB.sound
