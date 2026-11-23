import TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow

namespace TinylambOpt.ResearchProofs.RankSelfJoinToLagLeadAndHashWindow

open TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow

-- ---------------------------------------------------------------- basic perm

private theorem insertSortedW_perm (x : WRow) : ∀ l : List WRow, (insertSortedW x l).Perm (x :: l)
  | [] => by simp [insertSortedW]
  | y :: ys => by
      by_cases h : leWRow x y
      · simp [insertSortedW, h]
      · simp [insertSortedW, h]
        exact (List.Perm.cons y (insertSortedW_perm x ys)).trans (List.Perm.swap x y ys)

private theorem sortW_perm : ∀ l : List WRow, (sortW l).Perm l
  | [] => by simp [sortW]
  | x :: xs => by
      simp [sortW]
      exact (insertSortedW_perm x (sortW xs)).trans (List.Perm.cons x (sortW_perm xs))

-- ------------------------------------------------------------ rank machinery

private def rankGo (base : Nat) (t : Int) : List WRow → Nat
  | [] => 0
  | r :: rest => if r.ord == t then base + 1 else rankGo (base + 1) t rest

private theorem rankGo_eq (base : Nat) (t : Int) : ∀ l : List WRow,
    rankGo base t l =
      match l.findIdx? (fun r => r.ord == t) with
      | some i => base + i + 1
      | none => 0
  | [] => by simp [rankGo]
  | r :: rest => by
      have ih := rankGo_eq (base + 1) t rest
      by_cases ht : r.ord == t
      · simp [rankGo, ht, List.findIdx?_cons]
      · simp [rankGo, ht, List.findIdx?_cons, ih]
        cases hfi : rest.findIdx? (fun r => r.ord == t) with
        | none => simp [hfi]
        | some i => simp [hfi]; omega

private def partRankGo (t : Int) (idx prevRank : Nat) (prevOrd : Option Int) : List WRow → Nat
  | [] => 0
  | r :: rest =>
      let rn :=
        match prevOrd with
        | none => 1
        | some po => if po == r.ord then prevRank else idx + 1
      if r.ord == t then rn else partRankGo t (idx + 1) rn (some r.ord) rest

private theorem go_eq_partRankGo (l : List WRow) (t : Int) (idx prevRank : Nat) (prevOrd : Option Int) :
    partRank.go t idx prevRank prevOrd l = partRankGo t idx prevRank prevOrd l := by
  induction l generalizing t idx prevRank prevOrd with
  | nil => rfl
  | cons r rest ih =>
      simp [partRank.go, partRankGo, ih]
      rfl

private theorem partRank_eq_go (l : List WRow) (t : Int) :
    partRank l t = partRankGo t 0 0 none l := by
  rw [show partRank l t = partRank.go t 0 0 none l by rfl]
  exact go_eq_partRankGo l t 0 0 none

private theorem partRankGo_index (t : Int) (idx prevRank : Nat) (prevOrd : Option Int) :
    ∀ l : List WRow,
      (l.map WRow.ord).Nodup →
      (∀ r ∈ l, ∀ po : Int, prevOrd = some po → po ≠ r.ord) →
      (match prevOrd with
        | none => idx = 0 ∧ prevRank = 0
        | some _ => prevRank = idx) →
      partRankGo t idx prevRank prevOrd l =
        match l.findIdx? (fun r => r.ord == t) with
        | some i => idx + i + 1
        | none => 0
  | [], hnodup, hdisj, hprev => by simp [partRankGo]
  | r :: rest, hnodup, hdisj, hprev => by
      have htail : (rest.map WRow.ord).Nodup := by
        simpa [List.Nodup] using (List.Pairwise.of_cons hnodup)
      have hnothead : ∀ x ∈ rest, x.ord ≠ r.ord := by
        intro x hx hxeq
        exact (List.pairwise_cons.mp hnodup).1 x.ord (List.mem_map.mpr ⟨x, hx, rfl⟩) hxeq.symm
      have hdisj_tail : ∀ r' ∈ rest, ∀ po : Int, some r.ord = some po → po ≠ r'.ord := by
        intro r' hr' po hpo
        have hpo' : r.ord = po := by simpa using hpo
        subst po
        exact fun h => hnothead r' hr' h.symm
      cases hprevOrd : prevOrd with
      | none =>
          have hp' : idx = 0 ∧ prevRank = 0 := by simpa [hprevOrd] using hprev
          have hi : idx = 0 := hp'.1
          have hpr : prevRank = 0 := hp'.2
          by_cases ht : r.ord == t
          · simp [partRankGo, hprevOrd, hi, hpr, ht, List.findIdx?_cons]
          · have hrec := partRankGo_index t (idx + 1) (idx + 1) (some r.ord) rest
              htail hdisj_tail (by simp)
            have hrec' : partRankGo t 1 1 (some r.ord) rest =
                match rest.findIdx? (fun r => r.ord == t) with
                | some i => 1 + i + 1
                | none => 0 := by simpa [hi] using hrec
            rw [partRankGo]
            simp [hi, ht, List.findIdx?_cons, hrec']
            cases hfi : rest.findIdx? (fun r => r.ord == t) with
            | none => simp [hfi]
            | some i => simp [hfi]; omega
      | some po =>
          have hne : po ≠ r.ord := hdisj r (by simp) po (by simpa using hprevOrd)
          have hbe : (po == r.ord) = false := by simpa [hne]
          have hpr : prevRank = idx := by simpa [hprevOrd] using hprev
          by_cases ht : r.ord == t
          · simp [partRankGo, hprevOrd, hbe, hpr, ht, List.findIdx?_cons]
          · have hrec := partRankGo_index t (idx + 1) (idx + 1) (some r.ord) rest
              htail hdisj_tail (by simp)
            simp [partRankGo, hprevOrd, hbe, hpr, ht, List.findIdx?_cons, hrec]
            cases hfi : rest.findIdx? (fun r => r.ord == t) with
            | none => simp [hfi]
            | some i => simp [hfi]; omega

private theorem partRank_eq_rankGo {l : List WRow} (hl : (l.map WRow.ord).Nodup) (t : Int) :
    partRank l t = rankGo 0 t l := by
  rw [partRank_eq_go]
  rw [partRankGo_index t 0 0 none l hl (by simp) (by simp)]
  rw [rankGo_eq]

private theorem mem_findIdx? {l : List WRow} {r : WRow} (hr : r ∈ l) :
    ∃ i, l.findIdx? (fun x => x.ord == r.ord) = some i := by
  by_cases hex : ∃ i, l.findIdx? (fun x => x.ord == r.ord) = some i
  · exact hex
  have hnone : l.findIdx? (fun x => x.ord == r.ord) = none := by
    cases hfi : l.findIdx? (fun x => x.ord == r.ord) with
    | none => rfl
    | some i => exact False.elim (hex ⟨i, hfi⟩)
  have hbad := (List.findIdx?_eq_none_iff.mp hnone) r hr
  simp at hbad

private theorem rankGo_ge_one {l : List WRow} {r : WRow} (hr : r ∈ l) (base : Nat) :
    base + 1 ≤ rankGo base r.ord l := by
  rw [rankGo_eq]
  obtain ⟨i, hi⟩ := mem_findIdx? hr
  simp [hi]

private theorem rankGo_of_mem_eq {l : List WRow} {r : WRow} (hr : r ∈ l) (base : Nat) :
    ∃ i, rankGo base r.ord l = base + i + 1 := by
  rw [rankGo_eq]
  obtain ⟨i, hi⟩ := mem_findIdx? hr
  exact ⟨i, by simp [hi]⟩

private theorem eq_of_mem_of_ord_eq : ∀ {l : List WRow}, (l.map WRow.ord).Nodup →
    ∀ {x y : WRow}, x ∈ l → y ∈ l → x.ord = y.ord → x = y
  | [], h, x, y, hx, hy, hord => by simp at hx
  | a :: l, h, x, y, hx, hy, hord => by
      simp at hx hy
      rcases hx with hxa | hx
      · subst x
        rcases hy with hya | hy
        · subst y
          rfl
        · have hne : a.ord ≠ y.ord := (List.pairwise_cons.mp h).1 y.ord (List.mem_map.mpr ⟨y, hy, rfl⟩)
          exact (hne hord).elim
      · rcases hy with hya | hy
        · subst y
          have hne : a.ord ≠ x.ord := (List.pairwise_cons.mp h).1 x.ord (List.mem_map.mpr ⟨x, hx, rfl⟩)
          exact (hne hord.symm).elim
        · exact eq_of_mem_of_ord_eq (List.Pairwise.of_cons h) hx hy hord

private theorem find?_congr_on {α : Type} (p q : α → Bool) : ∀ l : List α,
    (∀ x ∈ l, p x = q x) → l.find? p = l.find? q
  | [], h => by simp
  | a :: l, h => by
      have ha := h a (by simp)
      have ht : ∀ x ∈ l, p x = q x := by intro x hx; exact h x (by simp [hx])
      simp [List.find?_cons, ha, find?_congr_on p q l ht]

private theorem filterMap_congr_on {α β : Type} (f g : α → Option β) : ∀ l : List α,
    (∀ x ∈ l, f x = g x) → l.filterMap f = l.filterMap g
  | [], h => by simp
  | a :: l, h => by
      have ha := h a (by simp)
      have ht : ∀ x ∈ l, f x = g x := by intro x hx; exact h x (by simp [hx])
      simp [List.filterMap_cons, ha, filterMap_congr_on f g l ht]

-- ------------------------------------------------------------- group sweep

private def lagPred (base : Nat) (g : List WRow) (cur : WRow) : WRow → Bool :=
  fun r => rankGo base r.ord g + 1 == rankGo base cur.ord g

private def leadPred (base : Nat) (g : List WRow) (cur : WRow) : WRow → Bool :=
  fun r => rankGo base cur.ord g + 1 == rankGo base r.ord g

private def groupStep (base : Nat) (g : List WRow) (cur : WRow) : Option LagLeadTriple :=
  match g.find? (lagPred base g cur), g.find? (leadPred base g cur) with
  | some lg, some ld => some (cur, lg.val, ld.val)
  | _, _ => none

private def groupRef (base : Nat) (g : List WRow) : List LagLeadTriple :=
  g.filterMap (groupStep base g)

private theorem rankGo_head_two {a b : WRow} {rest : List WRow} (hg : ((a :: b :: rest).map WRow.ord).Nodup)
    (base : Nat) :
    rankGo base a.ord (a :: b :: rest) = base + 1 ∧
      rankGo base b.ord (a :: b :: rest) = base + 2 := by
  have hab : a.ord ≠ b.ord := by
    intro h
    exact (List.pairwise_cons.mp hg).1 b.ord (by simp) h
  have habB : (a.ord == b.ord) = false := by simpa [hab]
  constructor
  · simp [rankGo]
  · simp [rankGo, habB]

private theorem rankGo_tail2_ge {a b r : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (hr : r ∈ rest) (base : Nat) :
    base + 3 ≤ rankGo base r.ord (a :: b :: rest) := by
  have hgTail : ((b :: rest).map WRow.ord).Nodup := by
    simpa [List.Nodup] using (List.Pairwise.of_cons hg)
  have hane : a.ord ≠ r.ord := by
    intro h
    exact (List.pairwise_cons.mp hg).1 r.ord (List.mem_map.mpr ⟨r, by simp [hr], rfl⟩) h
  have hbne : b.ord ≠ r.ord := by
    intro h
    exact (List.pairwise_cons.mp hgTail).1 r.ord (List.mem_map.mpr ⟨r, hr, rfl⟩) h
  have haneB : (a.ord == r.ord) = false := by simpa [hane]
  have hbneB : (b.ord == r.ord) = false := by simpa [hbne]
  rw [rankGo_eq]
  obtain ⟨j, hj⟩ := mem_findIdx? hr
  have hfi : (a :: b :: rest).findIdx? (fun x => x.ord == r.ord) = some (j + 2) := by
    simp [List.findIdx?_cons, haneB, hbneB, hj, Nat.add_assoc]
  simpa [hfi]

private theorem rankGo_shift_tail {a : WRow} {l : List WRow} {r : WRow}
    (hr : r ∈ l) (hg : ((a :: l).map WRow.ord).Nodup) (base : Nat) :
    rankGo base r.ord (a :: l) = rankGo (base + 1) r.ord l := by
  have hane : a.ord ≠ r.ord := by
    intro h
    exact (List.pairwise_cons.mp hg).1 r.ord (List.mem_map.mpr ⟨r, hr, rfl⟩) h
  have haneB : (a.ord == r.ord) = false := by simpa [hane]
  simp [rankGo, haneB]

private theorem bool_eq_false_of_ne {n m : Nat} (h : n ≠ m) : (n == m) = false := by
  simpa [h]

private theorem nat_eq_of_beq_true {n m : Nat} (h : (n == m) = true) : n = m := by
  simpa using h

private theorem lagPred_head_false {a b cur : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (hcur : cur ∈ rest) (base : Nat) :
    lagPred base (a :: b :: rest) cur a = false := by
  unfold lagPred
  have ha : rankGo base a.ord (a :: b :: rest) = base + 1 := (rankGo_head_two hg base).1
  have hge := rankGo_tail2_ge hg hcur base
  have hne : base + 2 ≠ rankGo base cur.ord (a :: b :: rest) := by omega
  simpa [ha] using bool_eq_false_of_ne hne

private theorem leadPred_head_false {a b cur : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (hcur : cur ∈ rest) (base : Nat) :
    leadPred base (a :: b :: rest) cur a = false := by
  unfold leadPred
  have ha : rankGo base a.ord (a :: b :: rest) = base + 1 := (rankGo_head_two hg base).1
  have hge := rankGo_tail2_ge hg hcur base
  have hne : rankGo base cur.ord (a :: b :: rest) + 1 ≠ base + 1 := by omega
  simpa [ha] using bool_eq_false_of_ne hne

private theorem lagPred_shift {a b cur : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (hcur : cur ∈ rest) (base : Nat) :
    (a :: b :: rest).find? (lagPred base (a :: b :: rest) cur) =
      (b :: rest).find? (lagPred (base + 1) (b :: rest) cur) := by
  have hgTail : ((b :: rest).map WRow.ord).Nodup := by
    simpa [List.Nodup] using (List.Pairwise.of_cons hg)
  have hhead := lagPred_head_false hg hcur base
  rw [List.find?_cons]
  simp [hhead]
  apply find?_congr_on
  intro r hr
  unfold lagPred
  have hrank := rankGo_shift_tail (a := a) (l := b :: rest) (r := r) (by simp [hr]) hg base
  have hcurRank := rankGo_shift_tail (a := a) (l := b :: rest) (r := cur) (by simp [hcur]) hg base
  simp [hrank, hcurRank]

private theorem leadPred_shift {a b cur : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (hcur : cur ∈ rest) (base : Nat) :
    (a :: b :: rest).find? (leadPred base (a :: b :: rest) cur) =
      (b :: rest).find? (leadPred (base + 1) (b :: rest) cur) := by
  have hhead := leadPred_head_false hg hcur base
  rw [List.find?_cons]
  simp [hhead]
  apply find?_congr_on
  intro r hr
  unfold leadPred
  have hrank := rankGo_shift_tail (a := a) (l := b :: rest) (r := r) (by simp [hr]) hg base
  have hcurRank := rankGo_shift_tail (a := a) (l := b :: rest) (r := cur) (by simp [hcur]) hg base
  simp [hrank, hcurRank]

private theorem lagPred_none_for_head {a b : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (base : Nat) :
    (a :: b :: rest).find? (lagPred base (a :: b :: rest) a) = none := by
  apply List.find?_eq_none.mpr
  intro r hr
  unfold lagPred
  have hge := rankGo_ge_one hr base
  have ha : rankGo base a.ord (a :: b :: rest) = base + 1 := (rankGo_head_two hg base).1
  have hne : rankGo base r.ord (a :: b :: rest) + 1 ≠ base + 1 := by omega
  have hfalse : (rankGo base r.ord (a :: b :: rest) + 1 == base + 1) = false :=
    bool_eq_false_of_ne hne
  simpa [ha, hfalse]

private theorem lagPred_none_for_head1 {b : WRow} {rest : List WRow}
    (hg : ((b :: rest).map WRow.ord).Nodup) (base : Nat) :
    (b :: rest).find? (lagPred base (b :: rest) b) = none := by
  apply List.find?_eq_none.mpr
  intro r hr
  unfold lagPred
  have hge := rankGo_ge_one hr base
  have hb : rankGo base b.ord (b :: rest) = base + 1 := by simp [rankGo]
  have hne : rankGo base r.ord (b :: rest) + 1 ≠ base + 1 := by omega
  have hfalse : (rankGo base r.ord (b :: rest) + 1 == base + 1) = false :=
    bool_eq_false_of_ne hne
  simpa [hb, hfalse]

private theorem lagPred_b_at_a {a b : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (base : Nat) :
    lagPred base (a :: b :: rest) b a = true := by
  unfold lagPred
  rcases rankGo_head_two hg base with ⟨ha, hb⟩
  simp [ha, hb]

private theorem leadPred_b_false_ab {a b : WRow} {rest : List WRow}
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) (base : Nat) :
    leadPred base (a :: b :: rest) b a = false ∧
      leadPred base (a :: b :: rest) b b = false := by
  rcases rankGo_head_two hg base with ⟨ha, hb⟩
  constructor <;> unfold leadPred <;> simp [ha, hb]

private theorem leadPred_b_at_c {a b c : WRow} {rest : List WRow}
    (hg : ((a :: b :: c :: rest).map WRow.ord).Nodup) (base : Nat) :
    leadPred base (a :: b :: c :: rest) b c = true := by
  unfold leadPred
  rcases rankGo_head_two hg base with ⟨ha, hb⟩
  have hane : a.ord ≠ c.ord := by
    intro h
    exact (List.pairwise_cons.mp hg).1 c.ord (List.mem_map.mpr ⟨c, by simp, rfl⟩) h
  have hgTail : ((b :: c :: rest).map WRow.ord).Nodup := by
    simpa [List.Nodup] using (List.Pairwise.of_cons hg)
  have hbne : b.ord ≠ c.ord := by
    intro h
    exact (List.pairwise_cons.mp hgTail).1 c.ord (List.mem_map.mpr ⟨c, by simp, rfl⟩) h
  have haneB : (a.ord == c.ord) = false := by simpa [hane]
  have hbneB : (b.ord == c.ord) = false := by simpa [hbne]
  have hc : rankGo base c.ord (a :: b :: c :: rest) = base + 3 := by
    simp [rankGo, haneB, hbneB]
  simp [hb, hc]

private theorem leadPred_none_two {a b : WRow} (hg : ([a, b].map WRow.ord).Nodup) (base : Nat) :
    [a, b].find? (leadPred base [a, b] b) = none := by
  apply List.find?_eq_none.mpr
  intro r hr
  simp at hr
  have hab : a.ord ≠ b.ord := (List.pairwise_cons.mp hg).1 b.ord (by simp)
  have habB : (a.ord == b.ord) = false := by simpa [hab]
  have hbRank : rankGo base b.ord [a, b] = base + 2 := by simp [rankGo, habB]
  rcases hr with hra | hrb
  · subst r
    unfold leadPred
    have haRank : rankGo base a.ord [a, b] = base + 1 := by simp [rankGo]
    have hne : base + 2 + 1 ≠ base + 1 := by omega
    simpa [hbRank, haRank] using bool_eq_false_of_ne hne
  · subst r
    unfold leadPred
    have hne : base + 2 + 1 ≠ base + 2 := by omega
    simpa [hbRank] using bool_eq_false_of_ne hne

private theorem groupRef_shift (base : Nat) (a b : WRow) (rest : List WRow)
    (hg : ((a :: b :: rest).map WRow.ord).Nodup) :
    groupRef base (a :: b :: rest) =
      (match rest with | c :: _ => [(b, a.val, c.val)] | [] => []) ++
        groupRef (base + 1) (b :: rest) := by
  cases rest with
  | nil =>
      have hgTail : ([b].map WRow.ord).Nodup := by
        simpa [List.Nodup] using (List.Pairwise.of_cons hg)
      have hnoneA := lagPred_none_for_head hg base
      have hlagA := lagPred_b_at_a hg base
      have hleadNone : [a, b].find? (leadPred base [a, b] b) = none := by
        simpa using leadPred_none_two (by simpa using hg) base
      have hnoneB := lagPred_none_for_head1 hgTail (base + 1)
      have hstepA : groupStep base [a, b] a = none := by simp [groupStep, hnoneA]
      have hstepB : groupStep base [a, b] b = none := by simp [groupStep, hlagA, hleadNone]
      have hstepB' : groupStep (base + 1) [b] b = none := by simp [groupStep, hnoneB]
      simp [groupRef, List.filterMap_cons, hstepA, hstepB, hstepB']
  | cons c rest' =>
      have hgTail : ((b :: c :: rest').map WRow.ord).Nodup := by
        simpa [List.Nodup] using (List.Pairwise.of_cons hg)
      have hnoneA := lagPred_none_for_head hg base
      have hlagA := lagPred_b_at_a hg base
      have hleadA := (leadPred_b_false_ab hg base).1
      have hleadB := (leadPred_b_false_ab hg base).2
      have hleadC := leadPred_b_at_c hg base
      have hnoneBtail := lagPred_none_for_head1 hgTail (base + 1)
      let g : List WRow := a :: b :: c :: rest'
      let g' : List WRow := b :: c :: rest'
      have hlag : g.find? (lagPred base g b) = some a := by
        change (a :: b :: c :: rest').find? (lagPred base (a :: b :: c :: rest') b) = some a
        rw [List.find?_cons, hlagA]
      have hlead : g.find? (leadPred base g b) = some c := by
        change (a :: b :: c :: rest').find? (leadPred base (a :: b :: c :: rest') b) = some c
        rw [List.find?_cons, hleadA, List.find?_cons, hleadB, List.find?_cons, hleadC]
      have hstepA : groupStep base g a = none := by simp [groupStep, g, hnoneA]
      have hstepB : groupStep base g b = some (b, a.val, c.val) := by
        simp [groupStep, g, hlag, hlead]
      have hstepB' : groupStep (base + 1) g' b = none := by
        simp [groupStep, g', hnoneBtail]
      have hstepTail : ∀ cur ∈ c :: rest', groupStep base g cur = groupStep (base + 1) g' cur := by
        intro cur hcur
        change groupStep base (a :: b :: c :: rest') cur = groupStep (base + 1) (b :: c :: rest') cur
        simp only [groupStep]
        rw [lagPred_shift hg hcur base, leadPred_shift hg hcur base]
      change g.filterMap (groupStep base g) = [(b, a.val, c.val)] ++ g'.filterMap (groupStep (base + 1) g')
      rw [List.filterMap_cons, hstepA]
      simp only [Option.none]
      rw [List.filterMap_cons, hstepB]
      simp only [Option.some, List.cons_append]
      congr 1
      have hdrop : g'.filterMap (groupStep (base + 1) g') =
          (c :: rest').filterMap (groupStep (base + 1) g') := by
        change (b :: c :: rest').filterMap (groupStep (base + 1) g') =
          (c :: rest').filterMap (groupStep (base + 1) g')
        rw [List.filterMap_cons, hstepB']
      rw [hdrop]
      apply filterMap_congr_on
      intro cur hcur
      exact hstepTail cur (by simp [hcur])

private theorem groupRef_eq_adj (base : Nat) : ∀ g : List WRow,
    (g.map WRow.ord).Nodup → groupRef base g = adjTriplesForPart g
  | [], hg => by simp [groupRef, adjTriplesForPart]
  | [a], hg => by
      have hlagNone : [a].find? (lagPred base [a] a) = none := by
        apply List.find?_eq_none.mpr
        intro r hr
        simp at hr
        subst r
        unfold lagPred
        have hb : (a.ord == a.ord) = true := by simp
        have hne : base + 1 + 1 ≠ base + 1 := by omega
        simpa [rankGo, hb] using bool_eq_false_of_ne hne
      have hstep : groupStep base [a] a = none := by simp [groupStep, hlagNone]
      simp [groupRef, adjTriplesForPart, hstep]
  | a :: b :: rest, hg => by
      rw [groupRef_shift base a b rest hg]
      rw [groupRef_eq_adj (base + 1) (b :: rest) (by simpa [List.Nodup] using (List.Pairwise.of_cons hg))]
      cases rest with
      | nil => rfl
      | cons c rest' => rfl

-- ------------------------------------------------ whole-list decomposition

private def addUnique (acc : List Int) (p : Int) : List Int :=
  if p ∈ acc then acc else acc ++ [p]

private theorem foldl_contains_eq_addUnique (acc : List Int) (ps : List Int) :
    ps.foldl (fun a p => if a.contains p then a else a ++ [p]) acc = ps.foldl addUnique acc := by
  induction ps generalizing acc with
  | nil => rfl
  | cons p ps ih =>
      simp only [List.foldl_cons]
      rw [ih]
      congr 1
      simp [addUnique]

private theorem uniqueParts_eq_fold (s : List WRow) :
    uniqueParts s = (s.map WRow.part).foldl addUnique [] := by
  unfold uniqueParts
  exact foldl_contains_eq_addUnique [] (s.map WRow.part)

private theorem foldl_addUnique_prepend (p : Int) (acc : List Int) (ps : List Int) :
    ps.foldl addUnique (p :: acc) = p :: (ps.filter (fun x => x != p)).foldl addUnique acc := by
  induction ps generalizing acc with
  | nil => rfl
  | cons q qs ih =>
      by_cases hq : q == p
      · have hqp : q = p := by simpa using hq
        subst q
        have hadd : addUnique (p :: acc) p = p :: acc := by simp [addUnique]
        rw [List.foldl_cons, hadd]
        have hpne : (p != p) = false := by simp
        simp [List.filter_cons, hpne]
        exact ih acc
      · have hqp : q ≠ p := by
          intro h
          apply hq
          simp [h]
        have hadd : addUnique (p :: acc) q = p :: addUnique acc q := by
          unfold addUnique
          by_cases hc : q ∈ acc <;> simp [hc, hqp]
        have hqne : (q != p) = true := by simpa [hqp]
        rw [List.foldl_cons, hadd, List.filter_cons, hqne]
        exact ih (addUnique acc q)

private theorem foldl_addUnique_cons (p : Int) (ps : List Int) :
    ps.foldl addUnique [p] = p :: (ps.filter (fun q => q != p)).foldl addUnique [] := by
  simpa using foldl_addUnique_prepend p [] ps

private theorem map_filter_part (a : WRow) : ∀ s : List WRow,
    (s.map WRow.part).filter (fun q => q != a.part) =
      (s.filter (fun r => r.part != a.part)).map WRow.part
  | [] => rfl
  | r :: rs => by
      by_cases h : (r.part != a.part) = true
      · simp [h, map_filter_part a rs]
      · simp [h, map_filter_part a rs]

private theorem uniqueParts_cons_filter (a : WRow) (s : List WRow) :
    uniqueParts (a :: s) = a.part :: uniqueParts (s.filter (fun r => r.part != a.part)) := by
  rw [uniqueParts_eq_fold, uniqueParts_eq_fold]
  simp only [List.map_cons, List.foldl_cons]
  have hinit : addUnique [] a.part = [a.part] := by simp [addUnique]
  rw [hinit, foldl_addUnique_cons]
  rw [map_filter_part a s]

private def groupOf (s : List WRow) (pk : Int) : List WRow := s.filter (fun r => r.part == pk)

private def refOn (s : List WRow) : List LagLeadTriple :=
  s.filterMap fun cur => groupStep 0 (groupOf s cur.part) cur

private def optOn (s : List WRow) : List LagLeadTriple :=
  (uniqueParts s).flatMap fun pk => groupRef 0 (groupOf s pk)

private theorem groupOf_cons_self (a : WRow) (t : List WRow) :
    groupOf (a :: t) a.part = a :: groupOf t a.part := by
  simp [groupOf]

private theorem groupOf_cons_ne {a cur : WRow} {t : List WRow} (hne : a.part ≠ cur.part) :
    groupOf (a :: t) cur.part = groupOf t cur.part := by
  have hb : (a.part == cur.part) = false := by simpa [hne]
  simp [groupOf, hb]

private theorem groupOf_filter_ne {pk : Int} {cur : WRow} {t : List WRow}
    (hne : cur.part ≠ pk) :
    groupOf (t.filter (fun r => r.part != pk)) cur.part = groupOf t cur.part := by
  unfold groupOf
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases hrc : r.part == cur.part
  · have hrp : r.part = cur.part := by simpa using hrc
    have hbool : (cur.part != pk) = true := by simpa [hne]
    have hrne : (r.part != pk) = true := by simpa [hrp] using hbool
    simp [hrc, hrne]
  · simp [hrc]

private def SortedW : List WRow → Prop
| [] => True
| a :: l => (∀ b ∈ l, leWRow a b = true) ∧ SortedW l

private theorem leWRow_part_le {u v : WRow} (h : leWRow u v = true) : u.part ≤ v.part := by
  unfold leWRow at h
  by_cases hp : (u.part != v.part) = true
  · simpa [hp] using h
  · have hpfalse : (u.part != v.part) = false := by cases hv : (u.part != v.part) <;> simp_all
    have heq : u.part = v.part := by simpa using hpfalse
    omega

private theorem leWRow_trans {u v w : WRow} (huv : leWRow u v = true) (hvw : leWRow v w = true) :
    leWRow u w = true := by
  have huvle : u.part ≤ v.part := leWRow_part_le huv
  have hvwle : v.part ≤ w.part := leWRow_part_le hvw
  by_cases huw : u.part = w.part
  · have hvu : v.part = u.part := by omega
    have hvw' : v.part = w.part := by omega
    have hpfalse1 : (u.part != v.part) = false := by simp [hvu]
    have hpfalse2 : (v.part != w.part) = false := by simp [hvw']
    have huord : u.ord ≤ v.ord := by
      have h := huv
      unfold leWRow at h
      simp [hpfalse1] at h
      exact h
    have hvord : v.ord ≤ w.ord := by
      have h := hvw
      unfold leWRow at h
      simp [hpfalse2] at h
      exact h
    have huword : u.ord ≤ w.ord := by omega
    have hbool : (u.part != w.part) = false := by simp [huw]
    simp [leWRow, hbool, decide_eq_true huword]
  · have hbool : (u.part != w.part) = true := by simpa [huw]
    have hle : u.part ≤ w.part := by omega
    simp [leWRow, hbool, decide_eq_true hle]

private theorem leWRow_total (u v : WRow) : leWRow u v = true ∨ leWRow v u = true := by
  unfold leWRow
  by_cases hp : (u.part != v.part) = true
  · have hne : u.part ≠ v.part := by simpa using hp
    by_cases hlt : u.part < v.part
    · left
      have hle : u.part ≤ v.part := by omega
      simp [hp, decide_eq_true hle]
    · right
      have hle : v.part ≤ u.part := by omega
      have hne2 : v.part ≠ u.part := fun h => hne h.symm
      have hp2 : (v.part != u.part) = true := by simpa [hne2]
      simp [hp2, decide_eq_true hle]
  · have heq : u.part = v.part := by simpa using hp
    by_cases ho : u.ord ≤ v.ord
    · left
      simp [hp, decide_eq_true ho]
    · right
      have hov : v.ord ≤ u.ord := by omega
      have hp2 : (v.part != u.part) = false := by simp [heq]
      simp [hp2, decide_eq_true hov]

private theorem insertSortedW_sorted (x : WRow) : ∀ l : List WRow, SortedW l → SortedW (insertSortedW x l)
  | [], hl => by simp [insertSortedW, SortedW, leWRow]
  | y :: ys, hl => by
      by_cases hxy : leWRow x y = true
      · have hxys : ∀ b ∈ ys, leWRow x b = true := by
          intro b hb
          exact leWRow_trans hxy (hl.1 b hb)
        simp [insertSortedW, hxy, SortedW, hxys, hl.1, hl.2]
        exact ⟨hxys, hl.1⟩
      · have hyx : leWRow y x = true := by
          rcases leWRow_total x y with h | h
          · exact False.elim (hxy h)
          · exact h
        have hperm := insertSortedW_perm x ys
        have hyins : ∀ b ∈ insertSortedW x ys, leWRow y b = true := by
          intro b hb
          have hb' : b ∈ x :: ys := hperm.mem_iff.mp hb
          simp at hb'
          rcases hb' with rfl | hb'
          · exact hyx
          · exact hl.1 b hb'
        have htailSorted := insertSortedW_sorted x ys hl.2
        simp [insertSortedW, hxy]
        exact ⟨hyins, htailSorted⟩

private theorem sortW_sorted : ∀ l : List WRow, SortedW (sortW l)
  | [] => by simp [sortW, SortedW]
  | x :: xs => insertSortedW_sorted x (sortW xs) (sortW_sorted xs)

private theorem sorted_no_more_part (a b : WRow) (t : List WRow) (hs : SortedW (a :: b :: t))
    (hne : (b.part == a.part) = false) : ∀ r ∈ b :: t, (r.part == a.part) = false := by
  have htail : SortedW (b :: t) := hs.2
  have hab : a.part ≤ b.part := leWRow_part_le (hs.1 b (by simp))
  have hbane : b.part ≠ a.part := by simpa using hne
  have habne : a.part ≠ b.part := fun h => hbane h.symm
  have hablt : a.part < b.part := by omega
  intro r hr
  simp at hr
  rcases hr with rfl | hr
  · exact hne
  · have hbr : b.part ≤ r.part := leWRow_part_le (htail.1 r hr)
    have hrne : r.part ≠ a.part := by omega
    simpa [hrne]

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) : ∀ l : List α,
    (∀ x ∈ l, p x = false) → l.filter p = []
  | [], h => rfl
  | x :: xs, h => by
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := by intro y hy; exact h y (by simp [hy])
      simp [hx, filter_eq_nil_of_all_false p xs ht]

private theorem dropWhile_sorted : ∀ (a : WRow) (t : List WRow), SortedW (a :: t) →
    SortedW ((a :: t).dropWhile (fun r => r.part == a.part))
  | a, [], hs => by simp [SortedW]
  | a, b :: t, hs => by
      by_cases hb : (b.part == a.part) = true
      · have heq : b.part = a.part := by simpa using hb
        simpa [List.dropWhile, hb, heq] using (dropWhile_sorted b t hs.2)
      · simpa [List.dropWhile, hb] using hs.2

private theorem takeWhile_dropWhile_eq : ∀ (a : WRow) (t : List WRow), SortedW (a :: t) →
    (a :: t).takeWhile (fun r => r.part == a.part) = (a :: t).filter (fun r => r.part == a.part) ∧
      (a :: t).dropWhile (fun r => r.part == a.part) =
        (a :: t).filter (fun r => r.part != a.part)
  | a, [], hs => by simp
  | a, b :: t, hs => by
      have htail : SortedW (b :: t) := hs.2
      by_cases hb : (b.part == a.part) = true
      · have heq : b.part = a.part := by simpa using hb
        have iht := takeWhile_dropWhile_eq b t htail
        constructor
        · have h1 : (b :: t).takeWhile (fun r => r.part == a.part) =
              (b :: t).filter (fun r => r.part == a.part) := by simpa [heq] using iht.1
          simpa [List.takeWhile, List.filter_cons, hb] using h1
        · have h1 : (b :: t).dropWhile (fun r => r.part == a.part) =
              (b :: t).filter (fun r => r.part != a.part) := by simpa [heq] using iht.2
          simpa [List.dropWhile, List.filter_cons, hb] using h1
      · have hbfalse : (b.part == a.part) = false := by
          cases hv : (b.part == a.part) <;> simp_all
        have hnone := sorted_no_more_part a b t hs hbfalse
        constructor
        · simp [List.takeWhile, List.filter_cons, hb,
            filter_eq_nil_of_all_false (fun r : WRow => r.part == a.part) (b :: t) (by
              intro r hr; exact hnone r (by simp [hr]))]
        · have hfilterAll : (b :: t).filter (fun r => r.part != a.part) = b :: t := by
            apply List.filter_eq_self.mpr
            intro r hr
            have h := hnone r (by simp [hr])
            simpa using h
          simp [List.dropWhile, List.filter_cons, hb, hfilterAll]

private theorem mem_takeWhile_pred {α : Type} (p : α → Bool) : ∀ {x : α} {l : List α},
    x ∈ l.takeWhile p → p x = true
  | x, [], h => by simp at h
  | x, a :: as, h => by
      by_cases hp : p a = true
      · simp [List.takeWhile, hp] at h
        rcases h with rfl | h
        · exact hp
        · exact mem_takeWhile_pred p h
      · simp [List.takeWhile, hp] at h

private theorem mem_uniqueParts_mem : ∀ (s : List WRow) (pk : Int),
    pk ∈ uniqueParts s → ∃ r ∈ s, r.part = pk
  | [], pk, h => by simp [uniqueParts] at h
  | a :: t, pk, h => by
      rw [uniqueParts_cons_filter] at h
      simp at h
      rcases h with hpkeq | hpkin
      · exact ⟨a, by simp, hpkeq.symm⟩
      · obtain ⟨r, hr, hpart⟩ := mem_uniqueParts_mem (t.filter (fun r => r.part != a.part)) pk hpkin
        have hrT : r ∈ t := (List.mem_filter.mp hr).1
        exact ⟨r, by simp [hrT], hpart⟩
termination_by s => s.length
decreasing_by
  have hlen : (List.filter (fun r => r.part != a.part) t).length ≤ t.length := List.length_filter_le _ _
  have hlen2 : (a :: t).length = t.length + 1 := by simp
  omega

private theorem groupOf_filter_ne_pk {pk excl : Int} {t : List WRow} (hne : pk ≠ excl) :
    groupOf (t.filter (fun r => r.part != excl)) pk = groupOf t pk := by
  unfold groupOf
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases hrc : (r.part == pk) = true
  · have hrpk : r.part = pk := by simpa using hrc
    have hrne : (r.part != excl) = true := by simpa [hrpk, hne]
    simp [hrc, hrne]
  · simp [hrc]

private theorem flatMap_congr_on {α β : Type} (f g : α → List β) : ∀ l : List α,
    (∀ x ∈ l, f x = g x) → l.flatMap f = l.flatMap g
  | [], h => rfl
  | x :: xs, h => by
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = g y := by intro y hy; exact h y (by simp [hy])
      simp [hx, flatMap_congr_on f g xs ht]

private theorem refOn_decomp : ∀ s : List WRow, SortedW s →
    refOn s = (uniqueParts s).flatMap fun pk => groupRef 0 (groupOf s pk)
  | [], hs => by simp [refOn, uniqueParts, groupOf]
  | a :: t, hs => by
      have htw := takeWhile_dropWhile_eq a t hs
      let g := groupOf (a :: t) a.part
      let rest := (a :: t).dropWhile (fun r => r.part == a.part)
      have hg_take : g = (a :: t).takeWhile (fun r => r.part == a.part) := by
        dsimp [g]
        exact htw.1.symm
      have hrest_filter : rest = (a :: t).filter (fun r => r.part != a.part) := by
        dsimp [rest]
        exact htw.2
      have hsplit : a :: t = g ++ rest := by
        rw [hg_take]
        dsimp [rest]
        exact (List.takeWhile_append_dropWhile).symm
      have hrestSorted : SortedW rest := by
        dsimp [rest]
        exact dropWhile_sorted a t hs
      have hgrp : ∀ cur ∈ g, groupOf (a :: t) cur.part = g := by
        intro cur hcur
        have hcurpart : cur.part = a.part := by
          have hm : cur ∈ (a :: t).takeWhile (fun r => r.part == a.part) := by
            rw [← hg_take]
            exact hcur
          have hm' : cur = a ∨ cur ∈ t.takeWhile (fun r => r.part == a.part) := by
            simpa [List.takeWhile] using hm
          rcases hm' with rfl | htail
          · rfl
          · have hb := mem_takeWhile_pred (fun r : WRow => r.part == a.part) htail
            simpa using hb
        dsimp [g]
        rw [← hcurpart]
      have hrest_filter_tail : rest = t.filter (fun r => r.part != a.part) := by
        rw [hrest_filter]
        simp
      have hrestgrp : ∀ cur ∈ rest, groupOf (a :: t) cur.part = groupOf rest cur.part := by
        intro cur hcur
        have hmemt : cur ∈ t.filter (fun r => r.part != a.part) := by
          rw [← hrest_filter_tail]
          exact hcur
        have hne0 : cur.part ≠ a.part := by
          have hm := (List.mem_filter.mp hmemt).2
          simpa using hm
        -- groupOf (a::t) cur.part = groupOf rest cur.part because rest is filter !a.part
        rw [hrest_filter]
        rw [groupOf_filter_ne hne0]
      have hup : uniqueParts (a :: t) = a.part :: uniqueParts rest := by
        rw [hrest_filter_tail]
        exact uniqueParts_cons_filter a t
      rw [hup]
      simp only [List.flatMap_cons]
      have hfilterApp : refOn (a :: t) =
          g.filterMap (fun cur => groupStep 0 (groupOf (a :: t) cur.part) cur) ++
            rest.filterMap (fun cur => groupStep 0 (groupOf (a :: t) cur.part) cur) := by
        rw [hsplit]
        simp [refOn]
      rw [hfilterApp]
      have hleft : g.filterMap (fun cur => groupStep 0 (groupOf (a :: t) cur.part) cur) = groupRef 0 g := by
        unfold groupRef
        apply filterMap_congr_on
        intro cur hcur
        rw [hgrp cur hcur]
      rw [hleft]
      have hright0 : rest.filterMap (fun cur => groupStep 0 (groupOf (a :: t) cur.part) cur) = refOn rest := by
        unfold refOn
        apply filterMap_congr_on
        intro cur hcur
        rw [hrestgrp cur hcur]
      rw [hright0, refOn_decomp rest hrestSorted]
      have hpkdiff : ∀ pk ∈ uniqueParts rest, pk ≠ a.part := by
        intro pk hp
        obtain ⟨r, hr, hpart⟩ := mem_uniqueParts_mem rest pk hp
        have hmemt : r ∈ t.filter (fun r => r.part != a.part) := by
          rw [← hrest_filter_tail]
          exact hr
        have hrne : r.part ≠ a.part := by simpa using (List.mem_filter.mp hmemt).2
        exact fun h => hrne (by omega)
      have hpkrew : (uniqueParts rest).flatMap (fun pk => groupRef 0 (groupOf (a :: t) pk)) =
          (uniqueParts rest).flatMap (fun pk => groupRef 0 (groupOf rest pk)) := by
        apply flatMap_congr_on
        intro pk hp
        have hne := hpkdiff pk hp
        have hne2 : a.part ≠ pk := fun h => hne h.symm
        have hhab : (a.part == pk) = false := by simpa [hne2]
        have h1 : groupOf (a :: t) pk = groupOf t pk := by
          simp [groupOf, hhab]
        rw [h1, hrest_filter_tail]
        rw [groupOf_filter_ne_pk hne]
      rw [hpkrew]
termination_by s => s.length
decreasing_by
  have hlen : ((a :: t).dropWhile (fun r => r.part == a.part)).length ≤ t.length := by
    change rest.length ≤ t.length
    rw [hrest_filter_tail]
    exact List.length_filter_le _ _
  have hlen2 : (a :: t).length = t.length + 1 := by simp
  omega


-- --------------------------------------------------------------- final parts

private theorem nodup_ord_filter : ∀ (s : List WRow),
    (s.map (fun r => (r.part, r.ord))).Nodup →
      ∀ pk, ((s.filter (fun r => r.part == pk)).map WRow.ord).Nodup
  | [], h, pk => by simp
  | a :: t, h, pk => by
      have hhead' : ∀ r ∈ t, a.part = r.part → a.ord ≠ r.ord := by
        intro r hr hpart hordEq
        have hneq := (List.pairwise_cons.mp h).1 (r.part, r.ord) (List.mem_map.mpr ⟨r, hr, rfl⟩)
        apply hneq
        simp [hpart, hordEq]
      have htail : (t.map (fun r => (r.part, r.ord))).Nodup := by
        simpa [List.Nodup] using (List.Pairwise.of_cons h)
      have ihres := nodup_ord_filter t htail pk
      by_cases hp : (a.part == pk) = true
      · have haPart : a.part = pk := by simpa using hp
        have hnot : a.ord ∉ ((t.filter (fun r => r.part == pk)).map WRow.ord) := by
          intro hm
          obtain ⟨r, hr, hord⟩ := List.mem_map.mp hm
          have hfp : r.part = pk := by simpa using (List.mem_filter.mp hr).2
          have hpartEq : a.part = r.part := by omega
          exact (hhead' r (List.mem_filter.mp hr).1 hpartEq) hord.symm
        have hnod : (a.ord :: ((t.filter (fun r => r.part == pk)).map WRow.ord)).Nodup := by
          constructor
          · intro x hx hEq
            apply hnot
            simpa [hEq] using hx
          · exact ihres
        simpa [List.filter_cons, hp] using hnod
      · simpa [List.filter_cons, hp] using ihres

private theorem partOne : ∀ (rows : List WRow), UniquePartOrd rows →
    optLagLeadSweep rows = refRankSelfJoin rows := by
  intro rows hrows
  let s := sortW rows
  have hsnodup : (s.map (fun r => (r.part, r.ord))).Nodup := by
    dsimp [s]
    have hmapped : ((sortW rows).map (fun r : WRow => (r.part, r.ord))).Perm
        (rows.map (fun r : WRow => (r.part, r.ord))) := (sortW_perm rows).map _
    exact hmapped.symm.nodup (by simpa [UniquePartOrd] using hrows)
  have hsSorted : SortedW s := by
    dsimp [s]
    exact sortW_sorted rows
  have href : refRankSelfJoin rows = refOn s := by
    unfold refRankSelfJoin refOn groupStep
    dsimp [s]
    apply filterMap_congr_on
    intro cur hcur
    have hnod := nodup_ord_filter s hsnodup cur.part
    dsimp [s] at hnod
    simp [groupOf, lagPred, leadPred, partRank_eq_rankGo hnod]
    rfl
  rw [href]
  unfold optLagLeadSweep
  change (uniqueParts s).flatMap (fun pk => adjTriplesForPart (groupOf s pk)) = refOn s
  rw [refOn_decomp s hsSorted]
  apply flatMap_congr_on
  intro pk hp
  exact (groupRef_eq_adj 0 (groupOf s pk) (nodup_ord_filter s hsnodup pk)).symm

-- ---------------------------------------------------- hash aggregation part

private def hashAcc (s : List WRow) (pk : Int) : Prod Int Nat :=
  s.foldl (fun (acc : Prod Int Nat) r => if r.part = pk then (acc.1 + r.val, acc.2 + 1) else acc) (0, 0)

private theorem foldl_add_aux (l : List WRow) (a : Int) :
    l.foldl (fun s r => s + r.val) a = a + l.foldl (fun s r => s + r.val) 0 := by
  induction l generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp [List.foldl_cons]
      rw [ih (a + x.val), ih x.val]
      omega

private theorem hashAcc_aux (s : List WRow) (pk : Int) (s0 : Int) (c0 : Nat) :
    s.foldl (fun (acc : Prod Int Nat) r => if r.part = pk then (acc.1 + r.val, acc.2 + 1) else acc) (s0, c0) =
      let pRows := s.filter (fun r => r.part == pk)
      (s0 + pRows.foldl (fun a r => a + r.val) 0, c0 + pRows.length) := by
  induction s generalizing s0 c0 with
  | nil => simp
  | cons a t ih =>
      simp only [List.foldl_cons]
      by_cases hp : a.part = pk
      · simp [hp]
        rw [ih (s0 + a.val) (c0 + 1)]
        rw [foldl_add_aux]
        dsimp
        apply Prod.ext <;> omega
      · simp [hp]
        rw [ih s0 c0]

private theorem hashAcc_eq (s : List WRow) (pk : Int) :
    hashAcc s pk =
      ((s.filter (fun r => r.part == pk)).foldl (fun a r => a + r.val) 0,
        (s.filter (fun r => r.part == pk)).length) := by
  unfold hashAcc
  simpa using (hashAcc_aux s pk 0 0)

private theorem foldl_add_perm {l₁ l₂ : List WRow} (hp : l₁.Perm l₂) :
    l₁.foldl (fun s r => s + r.val) 0 = l₂.foldl (fun s r => s + r.val) 0 := by
  induction hp with
  | nil => rfl
  | cons x hp ih =>
      simp only [List.foldl_cons]
      rw [foldl_add_aux _ (0 + x.val), foldl_add_aux _ (0 + x.val), ih]
  | swap x y l =>
      simp only [List.foldl_cons]
      rw [foldl_add_aux l (0 + y.val + x.val), foldl_add_aux l (0 + x.val + y.val)]
      omega
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem partTwo : ∀ (rows : List WRow) (pk : Int),
    orderlessSumCntHash rows pk = orderlessSumCntSorted rows pk := by
  intro rows pk
  have hperm := (sortW_perm rows).filter (fun r => r.part == pk)
  rw [show orderlessSumCntHash rows pk = hashAcc rows pk by simp [orderlessSumCntHash, hashAcc]]
  rw [hashAcc_eq]
  unfold orderlessSumCntSorted
  dsimp
  have hsum := foldl_add_perm hperm
  have hlen := hperm.length_eq
  simp [hperm.length_eq, hsum]

private theorem partThree : ∃ rows : List WRow, optLagLeadSweep rows ≠ refRankSelfJoin rows := by
  refine ⟨[ { part := 1, ord := 10, val := 100 }
          , { part := 1, ord := 10, val := 200 }
          , { part := 1, ord := 20, val := 300 } ], ?_⟩
  decide

theorem sound : TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow.Goal := by
  exact ⟨partOne, partTwo, partThree⟩

end TinylambOpt.ResearchProofs.RankSelfJoinToLagLeadAndHashWindow
