import TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping

/-!
These claims cover skipping blocks with certified predicate summaries, using
string offsets to test emptiness, and bounding grouping work for LIMIT or
top-k. They specify when a SQL scan can avoid decoding a block or completing
groups that cannot affect the modeled output.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.BlockPruningAndBoundedGrouping

open TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-! ## Goals 1 and 3: zone-map pruning and offset-only empty-string filter -/

theorem BlockConjunctionZoneBitmapPrune_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping.BlockConjunctionZoneBitmapPrune.Goal := by
  intro rows z aEq loB hiB hbounds hmatch
  rw [List.eq_nil_iff_forall_not_mem]
  intro r hr
  have hr' := (List.mem_filter.mp hr)
  have hpred : ((r.1 == aEq) = true ∧ loB ≤ r.2) ∧ r.2 ≤ hiB := by
    simpa only [Bool.and_eq_true, decide_eq_true_eq] using hr'.2
  have hreq : r.1 = aEq := beq_iff_eq.mp hpred.1.1
  have hb := hbounds r hr'.1
  have hAlo : z.minA ≤ aEq := by simpa [hreq] using hb.1
  have hAhi : aEq ≤ z.maxA := by simpa [hreq] using hb.2.1
  have hBlo : loB ≤ z.maxB := by omega
  have hBhi : z.minB ≤ hiB := by omega
  have hz : zoneMayMatch z aEq loB hiB = true := by
    unfold zoneMayMatch
    simp [hAlo, hAhi, show ¬z.maxB < loB by omega, show ¬hiB < z.minB by omega]
  rw [hmatch] at hz
  contradiction

theorem NonEmptyStringFilterBeforeDictDecode_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping.NonEmptyStringFilterBeforeDictDecode.Goal := by
  intro s
  cases s with
  | none => simp [offsetNonEmpty]
  | some txt =>
      by_cases hlen : txt.length = 0
      · have ht : txt = "" := String.length_eq_zero_iff.mp hlen
        simp [offsetNonEmpty, ht]
      · have ht : txt ≠ "" := by
          intro h
          apply hlen
          rw [h]
          rfl
        have hpos : 0 < txt.length := Nat.pos_of_ne_zero hlen
        simp [offsetNonEmpty, ht, hpos]

/-! ## Goal 4: top-K count pruning by remaining rows -/

private theorem foldl_hist_getD_nat (xs : List Nat) (H : Std.HashMap Nat Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) H).getD k 0 =
      H.getD k 0 + xs.count k := by
  induction xs generalizing H with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.getD_insert, List.count_cons]
      by_cases h : x = k
      · subst x
        simp
        omega
      · have hbeq : (x == k) = false := beq_eq_false_iff_ne.mpr h
        simp [hbeq]

private theorem histogram_getD_nat (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = xs.count k := by
  unfold histogram
  rw [foldl_hist_getD_nat]
  simp

private def incOnlyStep (h : Std.HashMap Nat Nat) (k : Nat) : Std.HashMap Nat Nat :=
  match h[k]? with
  | some c => h.insert k (c + 1)
  | none => h

private theorem foldl_incOnly_inv (base : Std.HashMap Nat Nat) (key : Nat) :
    ∀ (processed rest : List Nat) (h : Std.HashMap Nat Nat),
      key ∈ h →
      h.getD key 0 = base.getD key 0 + processed.count key →
      let h' := rest.foldl incOnlyStep h
      key ∈ h' ∧ h'.getD key 0 = base.getD key 0 + (processed ++ rest).count key := by
  intro processed rest
  induction rest generalizing processed with
  | nil =>
      intro h hmem hcnt
      simpa using And.intro hmem hcnt
  | cons x xs ih =>
      intro h hmem hcnt
      by_cases hx : x = key
      · subst x
        have hsome : h[key]? = some (h.getD key 0) := Std.HashMap.getElem?_eq_some_getD hmem
        have hstep : incOnlyStep h key = h.insert key (h.getD key 0 + 1) := by
          simp [incOnlyStep, hsome]
        rw [List.foldl_cons, hstep]
        have hmem' : key ∈ h.insert key (h.getD key 0 + 1) := Std.HashMap.mem_insert_self
        have hcnt' : (h.insert key (h.getD key 0 + 1)).getD key 0 =
            base.getD key 0 + (processed ++ [key]).count key := by
          rw [Std.HashMap.getD_insert_self, hcnt]
          simp [List.count_append]
          omega
        have ih' := ih (processed ++ [key]) (h.insert key (h.getD key 0 + 1)) hmem' hcnt'
        simpa [List.count_append, List.append_assoc] using ih'
      · have hstep_mem : key ∈ incOnlyStep h x := by
          by_cases hxmem : x ∈ h
          · have hsome : h[x]? = some (h.getD x 0) := Std.HashMap.getElem?_eq_some_getD hxmem
            simp [incOnlyStep, hsome, Std.HashMap.mem_insert, hmem]
          · have hnone : h[x]? = none := Std.HashMap.getElem?_eq_none hxmem
            simp [incOnlyStep, hnone, hmem]
        have hstep_get : (incOnlyStep h x).getD key 0 = h.getD key 0 := by
          by_cases hxmem : x ∈ h
          · have hsome : h[x]? = some (h.getD x 0) := Std.HashMap.getElem?_eq_some_getD hxmem
            have hb : (x == key) = false := beq_eq_false_iff_ne.mpr hx
            simp [incOnlyStep, hsome, Std.HashMap.getD_insert, hb]
          · have hnone : h[x]? = none := Std.HashMap.getElem?_eq_none hxmem
            simp [incOnlyStep, hnone]
        rw [List.foldl_cons]
        have hcnt' : (incOnlyStep h x).getD key 0 =
            base.getD key 0 + (processed ++ [x]).count key := by
          rw [hstep_get, hcnt]
          have hb : (x == key) = false := beq_eq_false_iff_ne.mpr hx
          simp [List.count_append, List.count_cons, hb]
        have ih' := ih (processed ++ [x]) (incOnlyStep h x) hstep_mem hcnt'
        simpa [List.count_append, List.append_assoc] using ih'

theorem TopKCountPruneByRemainingRows_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping.TopKCountPruneByRemainingRows.Goal := by
  intro pref suff cK k
  dsimp
  intro hgt
  have hgt' : pref.count k + suff.count k > suff.length := by
    simpa [histogram_getD_nat, List.count_append] using hgt
  have hpc : 0 < pref.count k := by
    have hle : suff.count k ≤ suff.length := List.count_le_length
    omega
  have hbasepos : 0 < (histogram pref).getD k 0 := by
    simpa [histogram_getD_nat] using hpc
  have hbasemem : k ∈ histogram pref := by
    by_cases hn : k ∈ histogram pref
    · exact hn
    · have hz : (histogram pref).getD k 0 = 0 := Std.HashMap.getD_eq_fallback hn
      omega
  by_cases hck : cK > suff.length
  · have hinv := foldl_incOnly_inv (histogram pref) k [] suff (histogram pref) hbasemem (by simp)
    dsimp at hinv
    have hres : (suff.foldl (fun h k =>
        match h[k]? with | some c => h.insert k (c + 1) | none => h) (histogram pref)).getD k 0 =
        pref.count k + suff.count k := by
      change (suff.foldl incOnlyStep (histogram pref)).getD k 0 =
        pref.count k + suff.count k
      rw [hinv.2, histogram_getD_nat]
    simp [prunedTailCount, hck, histogram_getD_nat, List.count_append]
    exact hres
  · have hres : (suff.foldl (fun h k => h.insert k (h.getD k 0 + 1)) (histogram pref)).getD k 0 =
        pref.count k + suff.count k := by
      rw [foldl_hist_getD_nat, histogram_getD_nat]
    simp [prunedTailCount, hck, histogram_getD_nat, List.count_append]
    exact hres

/-! ## Goal 2: GROUP BY ... LIMIT without ORDER BY freezes the key set -/

private def frozenStep (limit : Nat) (h : Std.HashMap Nat Nat) (k : Nat) : Std.HashMap Nat Nat :=
  match h[k]? with
  | some c => h.insert k (c + 1)
  | none => if h.size < limit then h.insert k 1 else h

private theorem frozen_fold_inv (limit : Nat) :
    ∀ (processed rest : List Nat) (h : Std.HashMap Nat Nat),
      h.size ≤ limit →
      (∀ j, j ∈ h → h.getD j 0 = processed.count j ∧ 0 < h.getD j 0) →
      (∀ j, 0 < processed.count j → j ∈ h ∨ limit ≤ h.size) →
      let h' := rest.foldl (frozenStep limit) h
      h'.size ≤ limit ∧
        (∀ j, j ∈ h' → h'.getD j 0 = (processed ++ rest).count j ∧ 0 < h'.getD j 0) ∧
        (∀ j, 0 < (processed ++ rest).count j → j ∈ h' ∨ limit ≤ h'.size) := by
  intro processed rest
  induction rest generalizing processed with
  | nil =>
      intro h hsize hcnt hseen
      simpa using And.intro hsize (And.intro hcnt hseen)
  | cons x xs ih =>
      intro h hsize hcnt hseen
      by_cases hmem : x ∈ h
      · have hsome : h[x]? = some (h.getD x 0) := Std.HashMap.getElem?_eq_some_getD hmem
        have hstep : frozenStep limit h x = h.insert x (h.getD x 0 + 1) := by
          simp [frozenStep, hsome]
        have hsize' : (h.insert x (h.getD x 0 + 1)).size ≤ limit := by
          rw [Std.HashMap.size_insert]
          simp [hmem, hsize]
        have hcnt' : ∀ j, j ∈ h.insert x (h.getD x 0 + 1) →
            (h.insert x (h.getD x 0 + 1)).getD j 0 = (processed ++ [x]).count j ∧
              0 < (h.insert x (h.getD x 0 + 1)).getD j 0 := by
          intro j hj
          by_cases hjx : j = x
          · subst j
            have hcx := hcnt x hmem
            rw [Std.HashMap.getD_insert_self, hcx.1]
            simp [List.count_append]
          · have hjin : j ∈ h := by
              rw [Std.HashMap.mem_insert] at hj
              rcases hj with hEq | hjin
              · have : x = j := beq_iff_eq.mp hEq
                exact (hjx this.symm).elim
              · exact hjin
            have hget : (h.insert x (h.getD x 0 + 1)).getD j 0 = h.getD j 0 := by
              rw [Std.HashMap.getD_insert]
              have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
              simp [hb]
            have hcj := hcnt j hjin
            rw [hget]
            constructor
            · rw [hcj.1]
              have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
              simp [List.count_append, List.count_cons, hb]
            · exact hcj.2
        have hseen' : ∀ j, 0 < (processed ++ [x]).count j →
            j ∈ h.insert x (h.getD x 0 + 1) ∨ limit ≤ (h.insert x (h.getD x 0 + 1)).size := by
          intro j hjpos
          by_cases hjx : j = x
          · left
            subst j
            exact Std.HashMap.mem_insert_self
          · have hprev : 0 < processed.count j := by
              have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
              simpa [List.count_append, List.count_cons, hb] using hjpos
            have hs := hseen j hprev
            rcases hs with hs | hfull
            · left
              rw [Std.HashMap.mem_insert]
              exact Or.inr hs
            · right
              rw [Std.HashMap.size_insert]
              simp [hmem, hfull]
        rw [List.foldl_cons, hstep]
        have ih' := ih (processed ++ [x]) (h.insert x (h.getD x 0 + 1)) hsize' hcnt' hseen'
        simpa [List.count_append, List.append_assoc] using ih'
      · have hnone : h[x]? = none := Std.HashMap.getElem?_eq_none hmem
        by_cases hlt : h.size < limit
        · have hstep : frozenStep limit h x = h.insert x 1 := by
            simp [frozenStep, hnone, hlt]
          have hsize' : (h.insert x 1).size ≤ limit := by
            rw [Std.HashMap.size_insert]
            simp [hmem]
            omega
          have hxcount0 : processed.count x = 0 := by
            have hp : ¬0 < processed.count x := by
              intro hp
              have hs := hseen x hp
              rcases hs with hs | hfull
              · exact hmem hs
              · omega
            omega
          have hcnt' : ∀ j, j ∈ h.insert x 1 →
              (h.insert x 1).getD j 0 = (processed ++ [x]).count j ∧
                0 < (h.insert x 1).getD j 0 := by
            intro j hj
            by_cases hjx : j = x
            · subst j
              rw [Std.HashMap.getD_insert_self]
              simp [List.count_append, hxcount0]
            · have hjin : j ∈ h := by
                rw [Std.HashMap.mem_insert] at hj
                rcases hj with hEq | hjin
                · have : x = j := beq_iff_eq.mp hEq
                  exact (hjx this.symm).elim
                · exact hjin
              have hget : (h.insert x 1).getD j 0 = h.getD j 0 := by
                rw [Std.HashMap.getD_insert]
                have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
                simp [hb]
              have hcj := hcnt j hjin
              rw [hget]
              constructor
              · rw [hcj.1]
                have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
                simp [List.count_append, List.count_cons, hb]
              · exact hcj.2
          have hseen' : ∀ j, 0 < (processed ++ [x]).count j →
              j ∈ h.insert x 1 ∨ limit ≤ (h.insert x 1).size := by
            intro j hjpos
            by_cases hjx : j = x
            · left
              subst j
              exact Std.HashMap.mem_insert_self
            · have hprev : 0 < processed.count j := by
                have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
                simpa [List.count_append, List.count_cons, hb] using hjpos
              have hs := hseen j hprev
              rcases hs with hs | hfull
              · left
                rw [Std.HashMap.mem_insert]
                exact Or.inr hs
              · right
                rw [Std.HashMap.size_insert]
                simp [hmem]
                omega
          rw [List.foldl_cons, hstep]
          have ih' := ih (processed ++ [x]) (h.insert x 1) hsize' hcnt' hseen'
          simpa [List.count_append, List.append_assoc] using ih'
        · have hstep : frozenStep limit h x = h := by
            simp [frozenStep, hnone, hlt]
          have hcnt' : ∀ j, j ∈ h →
              h.getD j 0 = (processed ++ [x]).count j ∧ 0 < h.getD j 0 := by
            intro j hj
            have hcj := hcnt j hj
            constructor
            · rw [hcj.1]
              have hjx : j ≠ x := by
                intro e
                subst j
                exact hmem hj
              have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
              simp [List.count_append, List.count_cons, hb]
            · exact hcj.2
          have hseen' : ∀ j, 0 < (processed ++ [x]).count j → j ∈ h ∨ limit ≤ h.size := by
            intro j hjpos
            by_cases hjx : j = x
            · right
              subst j
              omega
            · have hprev : 0 < processed.count j := by
                have hb : (x == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjx e.symm)
                simpa [List.count_append, List.count_cons, hb] using hjpos
              exact hseen j hprev
          rw [List.foldl_cons, hstep]
          have ih' := ih (processed ++ [x]) h hsize hcnt' hseen'
          simpa [List.count_append, List.append_assoc] using ih'

theorem GroupByLimitAnyOrderEarlyStop_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.BlockPruningAndBoundedGrouping.GroupByLimitAnyOrderEarlyStop.Goal := by
  intro rows limit k
  dsimp
  change (rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat)).size ≤ limit ∧
    (∀ c, (rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat))[k]? = some c →
      c = rows.count k ∧ 0 < c)
  have hi := frozen_fold_inv limit [] rows ({} : Std.HashMap Nat Nat)
    (by simp) (by simp) (by simp)
  dsimp at hi
  rcases hi with ⟨hsize, hcnt, hseen⟩
  constructor
  · exact hsize
  · intro c hc
    have his : (rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat))[k]?.isSome := by
      simp [hc]
    have hkin : k ∈ rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat) :=
      Std.HashMap.isSome_getElem?_iff_mem.mp his
    have hget : (rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat))[k]? =
        some ((rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat)).getD k 0) :=
      Std.HashMap.getElem?_eq_some_getD hkin
    have hceq : c = (rows.foldl (frozenStep limit) ({} : Std.HashMap Nat Nat)).getD k 0 := by
      rw [hget] at hc
      cases hc
      rfl
    have hcval := hcnt k hkin
    constructor
    · rw [hceq, hcval.1]
    · simpa [hceq] using hcval.2

end TinylambOpt.ResearchProofs.QueryRewrites.BlockPruningAndBoundedGrouping

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.hpp
-- executor/detail/analytics_factorized_join_test.cpp
-- executor/detail/columnar_tlcs_block.cpp
-- executor/detail/columnar_tlcs_predicates.cpp
-- executor/detail/partitioned_aggregate_topk.cpp
