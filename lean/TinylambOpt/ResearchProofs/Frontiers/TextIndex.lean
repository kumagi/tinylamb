import TinylambOpt.ResearchGoals.Frontiers.TextIndex

namespace TinylambOpt.ResearchProofs.Frontiers.TextIndex

open TinylambOpt.ResearchGoals.Frontiers.TextIndex

/-! ## Basic list / prefix helpers -/

private theorem prefix_append_iff_drop {α : Type} (p q t : List α) :
    p ++ q <+: t ↔ p <+: t ∧ q <+: t.drop p.length := by
  constructor
  · intro h
    rcases h with ⟨u, hu⟩
    constructor
    · exact ⟨q ++ u, by simpa [List.append_assoc] using hu⟩
    · refine ⟨u, ?_⟩
      rw [← hu, List.drop_append]
      simp
  · rintro ⟨⟨v, hv⟩, ⟨u, hu⟩⟩
    have hu' : q ++ u = v := by
      rw [← hv] at hu
      simpa using hu
    refine ⟨u, ?_⟩
    rw [← hv, List.append_assoc, hu']

private theorem prefix_length_le_take {α : Type} {p t : List α} (h : p <+: t) :
    t.take p.length = p := by
  rcases h with ⟨u, rfl⟩
  simp

private theorem drop_of_prefix {α : Type} {p t u : List α} (h : t = p ++ u) :
    t.drop p.length = u := by
  rw [h, List.drop_append]
  simp

private theorem mem_range_of_le {i n : Nat} (h : i ≤ n) : i ∈ List.range (n+1) := by
  rw [List.mem_range]
  omega

private theorem mem_sa_iff {s : Word} {i : Nat} : i ∈ sa s ↔ i ≤ s.length := by
  unfold sa
  rw [List.mem_mergeSort]
  rw [List.mem_range]
  omega

private theorem sa_nodup (s : Word) : (sa s).Nodup := by
  unfold sa
  have hp := List.mergeSort_perm (List.range (s.length+1))
    (fun i j : Nat => decide (s.drop i ≤ s.drop j))
  exact hp.symm.nodup (List.nodup_range)

private theorem sa_pairwise (s : Word) :
    (sa s).Pairwise (fun i j => (decide (s.drop i ≤ s.drop j) : Bool) = true) := by
  unfold sa
  apply List.pairwise_mergeSort
  · intro a b c hab hbc
    have hab' : s.drop a ≤ s.drop b := of_decide_eq_true hab
    have hbc' : s.drop b ≤ s.drop c := of_decide_eq_true hbc
    exact decide_eq_true_eq.mpr (List.le_trans hab' hbc')
  · intro a b
    rcases List.le_total (s.drop a) (s.drop b) with h | h
    · simp [h]
    · simp [h]

private theorem pairwise_drop_lt_of_le_of_nodup (s : Word) {l : List Nat}
    (hle : l.Pairwise (fun i j => (decide (s.drop i ≤ s.drop j) : Bool) = true)) (hnd : l.Nodup)
    (hall : ∀ i ∈ l, i ≤ s.length) :
    l.Pairwise (fun i j => s.drop i < s.drop j) := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a t ih =>
      rw [List.pairwise_cons] at hle ⊢
      have hnd' := (List.nodup_cons.mp hnd).2
      have hanot : a ∉ t := (List.nodup_cons.mp hnd).1
      have hall' : ∀ i ∈ t, i ≤ s.length := by
        intro i hi
        exact hall i (by simp [hi])
      refine ⟨?_, ih hle.2 hnd' hall'⟩
      intro b hb
      have hleab : (decide (s.drop a ≤ s.drop b) : Bool) = true := hle.1 b hb
      have hle' : s.drop a ≤ s.drop b := of_decide_eq_true hleab
      have hab : a ≠ b := by
        intro heq
        subst b
        exact hanot hb
      have hne : s.drop a ≠ s.drop b := by
        intro hd
        have hleneq := congrArg List.length hd
        simp only [List.length_drop] at hleneq
        have ha := hall a (by simp)
        have hbnd := hall b (by simp [hb])
        omega
      rcases List.le_iff_lt_or_eq.mp hle' with hlt | heq
      · exact hlt
      · exact False.elim (hne heq)

private theorem sa_sorted_lt (s : Word) :
    (sa s).Pairwise (fun i j => s.drop i < s.drop j) := by
  exact pairwise_drop_lt_of_le_of_nodup s (sa_pairwise s) (sa_nodup s)
    (by intro i hi; exact mem_sa_iff.mp hi)

/-! ## Lexicographic interval lemmas -/

private theorem not_prefix_of_lt (p x : Word) (h : x < p) : ¬ p <+: x := by
  intro hp
  exact (List.not_lt.mpr hp.le) h

private theorem lt_of_not_lt_not_prefix (p x : Word) (hn : ¬ x < p) (hnp : ¬ p <+: x) :
    p < x := by
  have hle : p ≤ x := List.not_lt.mp hn
  rcases List.le_iff_lt_or_eq.mp hle with hlt | heq
  · exact hlt
  · subst x
    exact False.elim (hnp ⟨[], by simp⟩)

private theorem not_prefix_of_between {p x y : Word} (hpx : p < x)
    (hnp : ¬ p <+: x) (hxy : x < y) : ¬ p <+: y := by
  intro hpy
  rcases hpy with ⟨u, hu⟩
  have hlt : p ++ u < x := by
    simpa using (List.append_lt_append_of_lt_of_not_prefix hpx hnp (l₄ := []))
  rw [hu] at hlt
  exact (List.not_lt.mpr (List.le_of_lt hxy)) hlt

private theorem not_lt_of_prefix_of_lt {p x y : Word} (hpx : p <+: x) (hxy : x < y) :
    ¬ y < p := by
  have hp_lt_y : p < y := List.lt_of_le_of_lt hpx.le hxy
  exact List.not_lt.mpr (List.le_of_lt hp_lt_y)

/-! ## Contiguous -/

private theorem sorted_block_aux (s p : Word) : ∀ L : List Nat,
    L.Pairwise (fun i j => s.drop i < s.drop j) →
    let lo := L.countP (fun i => decide (s.drop i < p))
    let blk := L.filter (fun i => p.isPrefixOf (s.drop i))
    (L.drop lo).take blk.length = blk := by
  intro L
  induction L with
  | nil => intro h; simp
  | cons a t ih =>
      intro hlt
      rw [List.pairwise_cons] at hlt
      dsimp
      have hrel : ∀ b ∈ t, s.drop a < s.drop b := hlt.1
      have ih' := ih hlt.2
      dsimp at ih'
      by_cases hltap : s.drop a < p
      · have hblk : (a :: t).filter (fun i => p.isPrefixOf (s.drop i)) =
            t.filter (fun i => p.isPrefixOf (s.drop i)) := by
          have hnpfx : ¬ p <+: s.drop a := not_prefix_of_lt _ _ hltap
          have hb' : p.isPrefixOf (s.drop a) = false := by
            exact Bool.eq_false_of_ne_true (fun ht => hnpfx (List.isPrefixOf_iff_prefix.mp ht))
          simp [hb']
        have hcnt : (a :: t).countP (fun i => decide (s.drop i < p)) =
            t.countP (fun i => decide (s.drop i < p)) + 1 := by
          simp [hltap]
        rw [hblk, hcnt]
        simpa using ih'
      · by_cases hprefa : p.isPrefixOf (s.drop a) = true
        · have hpfa : p <+: s.drop a := List.isPrefixOf_iff_prefix.mp hprefa
          have hblk : (a :: t).filter (fun i => p.isPrefixOf (s.drop i)) =
              a :: t.filter (fun i => p.isPrefixOf (s.drop i)) := by simp [hprefa]
          have hcnt : (a :: t).countP (fun i => decide (s.drop i < p)) =
              t.countP (fun i => decide (s.drop i < p)) := by simp [hltap]
          have hless : t.countP (fun i => decide (s.drop i < p)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro b hb
            have hbne : ¬ s.drop b < p := not_lt_of_prefix_of_lt hpfa (hrel b hb)
            simpa using hbne
          rw [hless] at ih'
          rw [hblk, hcnt, hless]
          simpa using ih'
        · have hnpfx : ¬ p <+: s.drop a := by
            intro hp
            exact hprefa (List.isPrefixOf_iff_prefix.mpr hp)
          have hblk : (a :: t).filter (fun i => p.isPrefixOf (s.drop i)) =
              t.filter (fun i => p.isPrefixOf (s.drop i)) := by simp [hprefa]
          have hcnt : (a :: t).countP (fun i => decide (s.drop i < p)) =
              t.countP (fun i => decide (s.drop i < p)) := by simp [hltap]
          have hplt : p < s.drop a := lt_of_not_lt_not_prefix _ _ hltap hnpfx
          have hless : t.countP (fun i => decide (s.drop i < p)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro b hb
            have hpb : p < s.drop b := List.lt_trans hplt (hrel b hb)
            simpa using (List.not_lt.mpr (List.le_of_lt hpb) : ¬ s.drop b < p)
          have hblocknil : t.filter (fun i => p.isPrefixOf (s.drop i)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro b hb hpfx
            have hpfx' : p <+: s.drop b := List.isPrefixOf_iff_prefix.mp hpfx
            exact not_prefix_of_between hplt hnpfx (hrel b hb) hpfx'
          rw [hblk, hcnt, hless, hblocknil]
          simp

theorem Contiguous_sound : TinylambOpt.ResearchGoals.Frontiers.TextIndex.Contiguous.Goal := by
  intro s p
  dsimp [lo, hits]
  exact sorted_block_aux s p (sa s) (sa_sorted_lt s)

/-! ## PositionalIntersect -/

private theorem hits_mem {s p : Word} {j : Nat} :
    j ∈ hits s p ↔ j ∈ sa s ∧ p <+: s.drop j := by
  rw [hits, List.mem_filter]
  simp only [List.isPrefixOf_iff_prefix]

private theorem drop_add_eq (s : Word) (i c : Nat) :
    (s.drop i).drop c = s.drop (i + c) := by
  simpa [Nat.add_comm] using (List.drop_drop (l := s) (i := c) (j := i))

private theorem phrase_prefix_iff (s left right : Word) {i : Nat} (hi : i ∈ sa s) :
    left ++ right <+: s.drop i ↔
      left <+: s.drop i ∧ i + left.length ∈ hits s right := by
  constructor
  · intro h
    rcases prefix_append_iff_drop left right (s.drop i) |>.mp h with ⟨hl, hr⟩
    refine ⟨hl, ?_⟩
    rw [hits_mem]
    constructor
    · apply mem_sa_iff.mpr
      have hlenpre : (left ++ right).length ≤ (s.drop i).length := h.length_le
      have hilen : i ≤ s.length := mem_sa_iff.mp hi
      simp only [List.length_append, List.length_drop] at hlenpre ⊢
      omega
    · rw [← drop_add_eq]
      exact hr
  · rintro ⟨hl, hm⟩
    have hr0 := (hits_mem.mp hm).2
    rw [← drop_add_eq] at hr0
    exact prefix_append_iff_drop left right (s.drop i) |>.mpr ⟨hl, hr0⟩

theorem PositionalIntersect_sound :
    TinylambOpt.ResearchGoals.Frontiers.TextIndex.PositionalIntersect.Goal := by
  intro s left right
  unfold hits
  rw [List.filter_filter]
  apply List.filter_congr
  intro i hi
  have hpi := phrase_prefix_iff s left right hi
  rw [Bool.and_comm]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hpfx : left ++ right <+: s.drop i := List.isPrefixOf_iff_prefix.mp h
    rcases hpi.mp hpfx with ⟨hl, hm⟩
    have hbl : left.isPrefixOf (s.drop i) = true := List.isPrefixOf_iff_prefix.mpr hl
    have hc : ((sa s).filter (fun j => right.isPrefixOf (s.drop j))).contains
        (i + left.length) = true := List.contains_iff.mpr hm
    rw [Bool.and_eq_true]
    exact ⟨hbl, hc⟩
  · intro h
    rcases (Bool.and_eq_true _ _).mp h with ⟨hbl, hc⟩
    have hl : left <+: s.drop i := List.isPrefixOf_iff_prefix.mp hbl
    have hm : i + left.length ∈ (sa s).filter (fun j => right.isPrefixOf (s.drop j)) :=
      List.contains_iff.mp hc
    have hm' : i + left.length ∈ hits s right := by
      simpa [hits, List.isPrefixOf_iff_prefix] using hm
    exact List.isPrefixOf_iff_prefix.mpr (hpi.mpr ⟨hl, hm'⟩)

/-! ## PhraseBound -/

private theorem nodup_map_add (l : List Nat) (c : Nat) (h : l.Nodup) :
    (l.map (fun i => i + c)).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.nodup_cons] at h
      simp only [List.map_cons]
      rw [List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, heq⟩
        have : b = a := by omega
        subst b
        exact h.1 hb
      · exact ih h.2

theorem PhraseBound_sound : TinylambOpt.ResearchGoals.Frontiers.TextIndex.PhraseBound.Goal := by
  intro s left right
  have hpi := PositionalIntersect_sound s left right
  rw [hpi]
  apply Nat.le_min.mpr
  constructor
  · exact List.length_filter_le _ _
  · let l0 := (hits s left).filter (fun i => (hits s right).contains (i + left.length))
    let mapped := l0.map (fun i => i + left.length)
    have hnd_hits_left : (hits s left).Nodup :=
      (List.filter_sublist).nodup (sa_nodup s)
    have hnd0 : l0.Nodup := (List.filter_sublist).nodup hnd_hits_left
    have hndm : mapped.Nodup := nodup_map_add l0 left.length hnd0
    have hsub : mapped ⊆ hits s right := by
      intro x hx
      rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
      have hc : (hits s right).contains (i + left.length) = true :=
        (List.mem_filter.mp hi).2
      exact List.contains_iff_mem.mp hc
    have hle := List.Nodup.length_le_of_subset hndm hsub
    simpa [mapped, l0] using hle

/-! ## Unseparated (false) -/

/-- Counterexample: documents `[[1], [2]]` and pattern `[1, 2]`.
    Left side: `hits [1,2] [1,2] = [0]` so `!isEmpty = true`.
    Right side: `hits [1] [1,2] = []` and `hits [2] [1,2] = []` so `any = false`. -/
theorem Unseparated_disproof :
    ¬ TinylambOpt.ResearchGoals.Frontiers.TextIndex.Unseparated.Goal := by
  intro h
  have hx := h [[1], [2]] [1, 2] (by decide)
  have hbad : ¬ ((!(hits [[1], [2]].flatten [1, 2]).isEmpty) =
      (([[1], [2]].any (fun s => !(hits s [1, 2]).isEmpty)))) := by
    native_decide
  exact hbad hx

/-! ## DocumentSum -/

private theorem length_ne_zero_eq_not_isEmpty (l : List Nat) :
    (l.length != 0) = (l.isEmpty = false) := by
  cases l <;> simp

private theorem map_const_eq_replicate {α : Type} (l : List α) (i : Nat) :
    l.map (fun _ => i) = List.replicate l.length i := by
  induction l with
  | nil => simp
  | cons _ t ih => simp [ih, List.replicate_succ]

private theorem eraseDups_replicate_prefix (c i : Nat) (l : List Nat) :
    (List.replicate c i ++ l).eraseDups =
      if c = 0 then l.eraseDups else i :: (l.filter (fun x => x != i)).eraseDups := by
  cases c with
  | zero => simp
  | succ c =>
      rw [List.replicate_succ]
      simp only [List.cons_append, List.eraseDups_cons]
      simp only [ite_false, Nat.succ_ne_zero]
      have hf : (List.replicate c i ++ l).filter (fun b => !b == i) =
          l.filter (fun b => !b == i) := by
        rw [List.filter_append]
        have hrep : (List.replicate c i).filter (fun b => !b == i) = [] := by
          induction c with
          | zero => simp
          | succ c ih =>
              simp [ih]
        rw [hrep, List.nil_append]
      rw [hf]
      congr 1

private theorem eraseDups_flatMap_replicate_of {α : Type} (ys : List α) (f : α → Nat × Nat)
    (hnd : (ys.map (fun y => (f y).2)).Nodup) :
    (ys.flatMap (fun y => List.replicate (f y).1 (f y).2)).eraseDups =
      (ys.filter (fun y => (f y).1 != 0)).map (fun y => (f y).2) := by
  induction ys with
  | nil => simp
  | cons a ys ih =>
      simp only [List.flatMap_cons, List.map_cons] at hnd ⊢
      let c := (f a).1
      let i := (f a).2
      have hi_not : ∀ j ∈ ys.map (fun y => (f y).2), j ≠ i := by
        intro j hj heq
        have hmem : i ∈ ys.map (fun y => (f y).2) := heq ▸ hj
        exact (List.nodup_cons.mp hnd).1 hmem
      have hfilter : (ys.flatMap (fun y => List.replicate (f y).1 (f y).2)).filter
          (fun x => x != i) = ys.flatMap (fun y => List.replicate (f y).1 (f y).2) := by
        apply List.filter_eq_self.mpr
        intro y hy
        rcases List.mem_flatMap.mp hy with ⟨z, hz, hyz⟩
        have hyeq : y = (f z).2 := (List.mem_replicate.mp hyz).2
        have hjne : (f z).2 ≠ i := hi_not (f z).2 (List.mem_map.mpr ⟨z, hz, rfl⟩)
        rw [hyeq]
        simp [hjne]
      have hnd' : (ys.map (fun y => (f y).2)).Nodup := (List.nodup_cons.mp hnd).2
      have ih' := ih hnd'
      change (List.replicate c i ++
          ys.flatMap (fun y => List.replicate (f y).1 (f y).2)).eraseDups = _
      cases hc : c with
      | zero =>
          have hc' : (f a).1 = 0 := by simpa [c] using hc
          simp [hc', ih']
      | succ c' =>
          have hc' : (f a).1 = c' + 1 := by simpa [c] using hc
          rw [eraseDups_replicate_prefix]
          rw [hfilter, ih']
          simp [hc', i]

private theorem hits_nonempty_iff_any_range (s p : Word) :
    (hits s p).isEmpty = false ↔
      (List.range (s.length+1)).any (fun i => p.isPrefixOf (s.drop i)) = true := by
  constructor
  · intro hne
    have hnemp : hits s p ≠ [] := by
      intro hnil
      rw [hnil] at hne
      simp at hne
    rcases List.exists_mem_of_ne_nil (hits s p) hnemp with ⟨i, hi⟩
    apply List.any_eq_true.mpr
    have hile : i ≤ s.length := mem_sa_iff.mp (hits_mem.mp hi).1
    exact ⟨i, mem_range_of_le hile,
      List.isPrefixOf_iff_prefix.mpr (hits_mem.mp hi).2⟩
  · intro hany
    apply Bool.eq_false_of_ne_true
    intro hempty
    have hnemp : hits s p = [] := List.isEmpty_iff.mp hempty
    rcases List.any_eq_true.mp hany with ⟨i, hiR, hpfx⟩
    have hile : i ≤ s.length := by
      have := List.mem_range.mp hiR
      omega
    have hisa : i ∈ sa s := mem_sa_iff.mpr hile
    have him : i ∈ hits s p := hits_mem.mpr ⟨hisa, List.isPrefixOf_iff_prefix.mp hpfx⟩
    rw [hnemp] at him
    simp at him

theorem DocumentSum_sound : TinylambOpt.ResearchGoals.Frontiers.TextIndex.DocumentSum.Goal := by
  intro docs p weight
  have hnd : (docs.zipIdx.map Prod.snd).Nodup := by
    simpa [List.range_eq_range'] using (List.nodup_range' (s := 0) (n := docs.length) (step := 1))
  have hmain :
      (owners docs p).eraseDups =
        (docs.zipIdx.filter (fun (s, _) => (hits s p).isEmpty = false)).map Prod.snd := by
    unfold owners
    have hflat : docs.zipIdx.flatMap (fun (s, i) => (hits s p).map (fun _ => i)) =
        docs.zipIdx.flatMap (fun (s, i) => List.replicate (hits s p).length i) := by
      simp only [map_const_eq_replicate]
    rw [hflat]
    rw [eraseDups_flatMap_replicate_of (ys := docs.zipIdx)
      (f := fun x => ((hits x.1 p).length, x.2))]
    · change List.map Prod.snd
          (List.filter (fun y : Word × Nat => (hits y.1 p).length != 0) docs.zipIdx) =
        List.map Prod.snd
          (docs.zipIdx.filter (fun (s, _) => decide ((hits s p).isEmpty = false)))
      apply congrArg (fun l : List (Word × Nat) => l.map Prod.snd)
      apply List.filter_congr
      intro x hx
      rcases x with ⟨s, i⟩
      cases hl : hits s p with
      | nil => simp [hl]
      | cons a t => simp [hl]
    · simpa [List.range_eq_range'] using
        (List.nodup_range' (s := 0) (n := docs.length) (step := 1))
  rw [hmain]
  have hfilter :
      (docs.zipIdx.filter (fun (s, _) => decide ((hits s p).isEmpty = false))) =
        (docs.zipIdx.filter (fun (s, _) =>
          (List.range (s.length+1)).any (fun i => p.isPrefixOf (s.drop i)))) := by
    apply List.filter_congr
    intro x hx
    rcases x with ⟨s, i⟩
    simp only
    apply Bool.eq_iff_iff.mpr
    rw [decide_eq_true_eq]
    exact hits_nonempty_iff_any_range s p
  rw [hfilter]
  simp [Function.comp_def]

/-! ## SeparatedCount -/

private theorem hits_length_eq_count_range (s p : Word) :
    (hits s p).length = (List.range (s.length+1)).countP (fun i => p.isPrefixOf (s.drop i)) := by
  unfold hits
  rw [← List.countP_eq_length_filter]
  unfold sa
  exact (List.mergeSort_perm (List.range (s.length+1))
    (fun i j : Nat => decide (s.drop i ≤ s.drop j))).countP_eq
      (fun i => p.isPrefixOf (s.drop i))

private theorem map_succ_prefix_iff (p t : Word) :
    p.map Nat.succ <+: t.map Nat.succ ↔ p <+: t := by
  induction p generalizing t with
  | nil => simp
  | cons a ps ih =>
      cases t with
      | nil => simp
      | cons b bs =>
          by_cases hab : a = b
          · subst b
            simpa using ih bs
          · have hs : Nat.succ a ≠ Nat.succ b := by omega
            simp [List.prefix_cons_inj, hab, hs]

private theorem nozero_map_succ (p : Word) : ∀ x ∈ p.map Nat.succ, x ≠ 0 := by
  intro x hx
  rcases List.mem_map.mp hx with ⟨y, hy, rfl⟩
  simp

private theorem take_succ_append_singleton (b : Word) :
    (b ++ [0]).take (b.length + 1) = b ++ [0] := by
  induction b with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem prefix_append_singleton_of_nozero {a b : Word}
    (hno : ∀ x ∈ a, x ≠ 0) (h : a <+: b ++ [0]) : a <+: b := by
  have hle0 : a.length ≤ b.length + 1 := by simpa using h.length_le
  have hatake : a = (b ++ [0]).take a.length := by
    rcases h with ⟨u, hu⟩
    rw [← hu]
    simp
  by_cases hle : a.length ≤ b.length
  · refine ⟨b.drop a.length, ?_⟩
    have htakesucc : (b ++ [0]).take a.length = b.take a.length := by
      rw [List.take_append_of_le_length hle]
    have ha' : a = b.take a.length := hatake.trans htakesucc
    have hlen_take : (b.take a.length).length = a.length := by
      rw [List.length_take, Nat.min_eq_left hle]
    rw [ha', hlen_take]
    exact List.take_append_drop a.length b
  · have hlen : a.length = b.length + 1 := by omega
    have htakeall : (b ++ [0]).take a.length = b ++ [0] := by
      rw [hlen]
      exact take_succ_append_singleton b
    have haeq : a = b ++ [0] := hatake.trans htakeall
    have hz : 0 ∈ a := by rw [haeq]; simp
    exact False.elim (hno 0 hz rfl)

private theorem prefix_append_of_nozero {a b t : Word}
    (hno : ∀ x ∈ a, x ≠ 0) (hb0 : ∃ c, b = c ++ [0]) (h : a <+: b ++ t) : a <+: b := by
  rcases hb0 with ⟨c, rfl⟩
  have hatake : a = ((c ++ [0]) ++ t).take a.length := by
    rcases h with ⟨u, hu⟩
    rw [← hu]
    simp
  have hbpref : c ++ [0] <+: (c ++ [0]) ++ t := List.prefix_append _ _
  by_cases hle : a.length ≤ c.length + 1
  · refine ⟨(c ++ [0]).drop a.length, ?_⟩
    have hle' : a.length ≤ (c ++ [0]).length := by simpa using hle
    have htakeshort : ((c ++ [0]) ++ t).take a.length = (c ++ [0]).take a.length := by
      rw [List.take_append_of_le_length hle']
    have hlen_take : ((c ++ [0]).take a.length).length = a.length := by
      rw [List.length_take, Nat.min_eq_left hle']
    rw [hatake, htakeshort, hlen_take]
    exact List.take_append_drop a.length (c ++ [0])
  · have hbsub : c ++ [0] <+: a :=
      List.prefix_of_prefix_length_le hbpref (by
        rcases h with ⟨u, hu⟩
        exact ⟨u, by simpa [List.append_assoc] using hu⟩) (by
        have := List.IsPrefix.length_le h
        simp [List.append_assoc] at this ⊢
        omega)
    rcases hbsub with ⟨u, hu⟩
    have hz : 0 ∈ a := by
      rw [← hu]
      simp
    exact False.elim (hno 0 hz rfl)

private theorem succ_prefix_encode_iff (s p : Word) (hnp : p ≠ []) {i : Nat} (hi : i ≤ s.length) :
    (p.map Nat.succ).isPrefixOf ((markerEncode s).drop i) = p.isPrefixOf (s.drop i) := by
  apply Bool.eq_iff_iff.mpr
  rw [List.isPrefixOf_iff_prefix, List.isPrefixOf_iff_prefix]
  have hdrop : (markerEncode s).drop i = (s.drop i).map Nat.succ ++ [0] := by
    unfold markerEncode
    rw [List.drop_append]
    have hsub : i - s.length = 0 := Nat.sub_eq_zero_of_le hi
    simp [hsub, List.map_drop]
  rw [hdrop]
  constructor
  · intro h
    have h' : p.map Nat.succ <+: (s.drop i).map Nat.succ ++ [0] := h
    have h'' : p.map Nat.succ <+: (s.drop i).map Nat.succ :=
      prefix_append_singleton_of_nozero (nozero_map_succ p) h'
    exact (map_succ_prefix_iff p (s.drop i)).mp h''
  · intro h
    rw [← map_succ_prefix_iff] at h
    exact List.prefix_append_of_prefix h

private theorem countP_map_add_range (f : Nat → Bool) (a b : Nat) :
    (List.range (a + b)).countP f =
      (List.range a).countP f + (List.range b).countP (fun i => f (a + i)) := by
  rw [List.range_add, List.countP_append, List.countP_map]
  rfl

private theorem separated_count_aux (docs : List Word) (p : Word) (hnp : p ≠ []) :
    (List.range ((docs.flatMap markerEncode).length + 1)).countP
        (fun i => (p.map Nat.succ).isPrefixOf ((docs.flatMap markerEncode).drop i)) =
      (docs.map (fun s => (List.range (s.length+1)).countP (fun i => p.isPrefixOf (s.drop i)))).sum := by
  induction docs with
  | nil => simp [hnp]
  | cons s rest ih =>
      let enc := markerEncode s
      let tail := rest.flatMap markerEncode
      have hconcat : (s :: rest).flatMap markerEncode = enc ++ tail := by rfl
      rw [hconcat]
      have hlenenc : enc.length = s.length + 1 := by simp [enc, markerEncode]
      simp only [List.map_cons, List.sum_cons]
      rw [show (enc ++ tail).length + 1 = enc.length + (tail.length + 1) by
        simp [hlenenc]; omega]
      rw [countP_map_add_range]
      congr 1
      · -- positions inside the first encoded document
        rw [hlenenc]
        apply List.countP_congr
        intro i hi
        have hile : i ≤ s.length := by
          have := List.mem_range.mp hi
          omega
        have hdrop : (enc ++ tail).drop i = enc.drop i ++ tail := by
          rw [List.drop_append]
          have hsub : i - enc.length = 0 := Nat.sub_eq_zero_of_le (by omega)
          simp [hsub]
        rw [hdrop]
        have hdropenc : enc.drop i = (s.drop i).map Nat.succ ++ [0] := by
          simpa [enc] using (show (markerEncode s).drop i = (s.drop i).map Nat.succ ++ [0] from by
            unfold markerEncode
            rw [List.drop_append]
            have hsub : i - s.length = 0 := Nat.sub_eq_zero_of_le hile
            simp [hsub, List.map_drop])
        constructor
        · intro hb
          have hpre : p.map Nat.succ <+: enc.drop i ++ tail := List.isPrefixOf_iff_prefix.mp hb
          have hpre' : p.map Nat.succ <+: enc.drop i :=
            prefix_append_of_nozero (nozero_map_succ p)
              ⟨(s.drop i).map Nat.succ, hdropenc⟩ hpre
          have heq := succ_prefix_encode_iff s p hnp hile
          exact (Bool.eq_iff_iff.mp (by simpa [enc] using heq)).mp
            (List.isPrefixOf_iff_prefix.mpr hpre')
        · intro hb
          have hp' : p <+: s.drop i := List.isPrefixOf_iff_prefix.mp hb
          have heq := succ_prefix_encode_iff s p hnp hile
          have hs : (p.map Nat.succ).isPrefixOf (enc.drop i) = true := by
            apply List.isPrefixOf_iff_prefix.mpr
            rw [hdropenc]
            exact List.prefix_append_of_prefix (l₃ := [0])
              ((map_succ_prefix_iff p (s.drop i)).mpr hp')
          exact List.isPrefixOf_iff_prefix.mpr
            (List.prefix_append_of_prefix (List.isPrefixOf_iff_prefix.mp hs))
      · -- positions after the first encoded document
        have hshift : (List.range (tail.length + 1)).countP
            (fun i => (p.map Nat.succ).isPrefixOf ((enc ++ tail).drop (enc.length + i))) =
            (List.range (tail.length + 1)).countP
              (fun i => (p.map Nat.succ).isPrefixOf (tail.drop i)) := by
          apply List.countP_congr
          intro i hi
          have hile : i ≤ tail.length := by
            have := List.mem_range.mp hi
            omega
          rw [List.drop_append]
          have hsub : enc.length + i - enc.length = i := by omega
          simp [hsub]
          have hle : enc.length + i ≤ enc.length + tail.length := by omega
          have hdropenc : enc.drop (enc.length + i) = [] := by
            apply List.drop_eq_nil_of_le
            omega
          simp [hdropenc]
        rw [hshift]
        simpa [tail] using ih

theorem SeparatedCount_sound : TinylambOpt.ResearchGoals.Frontiers.TextIndex.SeparatedCount.Goal := by
  intro docs p hnp
  rw [hits_length_eq_count_range]
  rw [separated_count_aux docs p hnp]
  apply congrArg List.sum
  apply List.map_congr_left
  intro s hs
  exact (hits_length_eq_count_range s p).symm

/-! ## DistinctMarks -/

private theorem nodup_eraseDups_nat : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Nat), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length hlen _ rfl)
            have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
            rw [Nat.not_beq_eq_true_eq] at hb'
            exact fun heq => hb' heq.symm
  exact key l.length l rfl

private theorem nodup_length_eq_of_mem_iff {l₁ l₂ : List Nat}
    (d₁ : l₁.Nodup) (d₂ : l₂.Nodup) (h : ∀ a, a ∈ l₁ ↔ a ∈ l₂) :
    l₁.length = l₂.length := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp List.mem_cons_self
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact (hca rfl).elim
          · exact hr
      have hrlen := ih hnd hd hiff
      have hlen2 := List.length_erase_of_mem ha
      have hpos : 0 < l₂.length := List.length_pos_of_mem ha
      have hlen2' : (l₂.erase a).length + 1 = l₂.length := by omega
      simp only [List.length_cons]
      omega

private theorem eraseDups_cons_length (a : Nat) (t : List Nat) :
    (a :: t).eraseDups.length = t.eraseDups.length + (if a ∈ t then 0 else 1) := by
  by_cases hat : a ∈ t
  · rw [List.eraseDups_cons]
    simp only [List.length_cons]
    have hfnd : ((t.filter (fun b => !b == a)).eraseDups).Nodup :=
      nodup_eraseDups_nat _
    have htnd : t.eraseDups.Nodup := nodup_eraseDups_nat _
    have hlen : (t.filter (fun b => !b == a)).eraseDups.length =
        (t.eraseDups.erase a).length := by
      apply nodup_length_eq_of_mem_iff hfnd (htnd.erase a)
      intro z
      rw [List.mem_eraseDups, List.mem_filter]
      rw [List.Nodup.mem_erase_iff htnd]
      constructor
      · rintro ⟨hz, hza⟩
        have hza' : z ≠ a := by
          intro heq
          subst z
          simp at hza
        exact ⟨hza', List.mem_eraseDups.mpr hz⟩
      · rintro ⟨hza, hz⟩
        refine ⟨List.mem_eraseDups.mp hz, ?_⟩
        by_cases heq : z == a
        · exact (hza (beq_iff_eq.mp heq)).elim
        · simpa using heq
    have hlen2 := List.length_erase_of_mem (List.mem_eraseDups.mpr hat : a ∈ t.eraseDups)
    have hpos : 0 < t.eraseDups.length := List.length_pos_of_mem (List.mem_eraseDups.mpr hat)
    simp [hat, hlen]
    omega
  · rw [List.eraseDups_cons]
    have hf : (t.filter (fun b => !b == a)) = t := by
      apply List.filter_eq_self.mpr
      intro b hb
      by_cases hba : b == a
      · have heq : b = a := beq_iff_eq.mp hba
        subst b
        exact (hat hb).elim
      · simp [hba]
    simp [hf, hat]

private theorem filter_map_snd_add (doc : Nat) (l : List (Nat × Nat)) :
    (l.map (Prod.map id (fun i => i + 1))).filter (fun e => e.1 == doc) =
      (l.filter (fun e => e.1 == doc)).map (Prod.map id (fun i => i + 1)) := by
  induction l with
  | nil => rfl
  | cons e es ih =>
      by_cases h : e.1 == doc <;> simp [h, ih]

private theorem previous_cons_succ (a doc : Nat) (t : List Nat) (pos : Nat) :
    previous (a :: t) doc (pos + 1) =
      match previous t doc pos with
      | some p => some (p + 1)
      | none => if a == doc then some 0 else none := by
  unfold previous
  rw [List.take_succ_cons]
  rw [List.zipIdx_cons']
  simp only [List.filter_cons]
  rw [filter_map_snd_add]
  let F := (List.filter (fun e : Nat × Nat => e.1 == doc) (List.take pos t).zipIdx)
  change Option.map Prod.snd ((if a == doc then (a, 0) :: F.map (Prod.map id fun i => i + 1)
      else F.map (Prod.map id fun i => i + 1)).getLast?) =
    match Option.map Prod.snd F.getLast? with
    | some p => some (p + 1)
    | none => if a == doc then some 0 else none
  by_cases had : a == doc
  · simp only [if_pos had]
    rw [List.getLast?_cons, List.getLast?_map]
    cases hgl : F.getLast? with
    | none => simp
    | some e =>
        have he : e.1 == doc := by
          rcases List.getLast?_eq_some_iff.mp hgl with ⟨ys, hys⟩
          have hm : e ∈ F := by rw [hys]; simp
          exact (List.mem_filter.mp hm).2
        rcases e with ⟨v, i⟩
        simp at he ⊢
  · simp only [if_neg had]
    rw [List.getLast?_map]
    cases hgl : F.getLast? with
    | none => simp
    | some e =>
        rcases e with ⟨v, i⟩
        simp

private theorem countP_and_le {α : Type} (l : List α) (p q : α → Bool) :
    l.countP (fun x => p x && q x) ≤ l.countP p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.countP_cons]
      by_cases hp : p a = true <;>
      by_cases hq : q a = true <;> simp [hp, hq] <;> omega

private theorem countP_and_not {α : Type} (l : List α) (p q : α → Bool) :
    l.countP (fun x => p x && Bool.not (q x)) =
      l.countP p - l.countP (fun x => p x && q x) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.countP_cons]
      have hle := countP_and_le as p q
      by_cases hp : p a = true
      · by_cases hq : q a = true
        · simp [hp, hq, ih]
        · simp [hp, hq, ih]
          omega
      · simp [hp, ih]

private theorem first_count_of_eq (t : List Nat) (a : Nat) :
    t.zipIdx.countP (fun e => e.1 == a && (previous t e.1 e.2 == none)) =
      (if a ∈ t then 1 else 0) := by
  induction t with
  | nil => simp [previous]
  | cons b bs ih =>
      rw [List.zipIdx_cons']
      simp only [List.countP_cons, List.countP_map, Function.comp_apply]
      have hhead : (if b == a && (previous (b :: bs) b 0 == none) then 1 else 0) =
          (if b == a then 1 else 0) := by
        simp [previous]
      rw [hhead]
      have htail : bs.zipIdx.countP (fun e =>
          e.1 == a && (previous (b :: bs) e.1 (e.2 + 1) == none)) =
          (if b == a then 0 else bs.zipIdx.countP (fun e => e.1 == a && (previous bs e.1 e.2 == none))) := by
        by_cases hba : b == a
        · rw [if_pos hba]
          apply List.countP_eq_zero.mpr
          intro e he
          rw [previous_cons_succ]
          cases hp : previous bs e.1 e.2 with
          | none =>
              by_cases hea : a == e.1
              · have hbeq : b == e.1 := by
                  apply beq_iff_eq.mpr
                  exact (beq_iff_eq.mp hba).trans (beq_iff_eq.mp hea)
                simp [hp, hbeq]
              · have hea' : (e.1 == a) = false := by
                  by_cases h : e.1 == a
                  · have h' : a = e.1 := (beq_iff_eq.mp h).symm
                    exact (hea (by simpa [h'])).elim
                  · exact Bool.eq_false_of_ne_true h
                simp [hp, hea']
          | some p => simp [hp]
        · rw [if_neg hba]
          have hba' : (b == a) = false := Bool.eq_false_of_ne_true hba
          apply List.countP_congr
          intro e he
          by_cases hea : e.1 == a
          · have hbe : (b == e.1) = false := by
              by_cases h : b == e.1
              · have h1 : b = e.1 := beq_iff_eq.mp h
                have h2 : e.1 = a := beq_iff_eq.mp hea
                have hbatrue : (b == a) = true := by rw [h1, h2]; simp
                rw [hba'] at hbatrue
                contradiction
              · exact Bool.eq_false_of_ne_true h
            rw [previous_cons_succ]
            cases hp : previous bs e.1 e.2 with
            | none => simp [hp, hbe, hea, hba']
            | some p => simp [hp, hea, hba']
          · simp [hea]
      change List.countP (fun e : Nat × Nat =>
          e.1 == a && previous (b :: bs) e.1 (e.2 + 1) == none) bs.zipIdx +
          (if b == a then 1 else 0) = (if a ∈ b :: bs then 1 else 0)
      rw [htail, ih]
      by_cases hba : b == a
      · have hbaeq : b = a := beq_iff_eq.mp hba
        subst b
        simp
      · have hba' : (b == a) = false := Bool.eq_false_of_ne_true hba
        have hbane : a ≠ b := by
          intro h
          have ht : (b == a) = true := by rw [h]; simp
          rw [hba'] at ht
          contradiction
        by_cases habs : a ∈ bs <;> simp [hbane, hba', habs]

private theorem previous_take_eq (t : List Nat) {w j x : Nat} (hj : j ≤ w) :
    previous (t.take w) x j = previous t x j := by
  unfold previous
  rw [List.take_take, Nat.min_eq_left hj]

private theorem distinct_first_zero (ids : List Nat) (w : Nat) :
    (ids.take w).eraseDups.length =
      (ids.take w).zipIdx.countP (fun e => previous ids e.1 e.2 == none) := by
  induction ids generalizing w with
  | nil => simp [previous]
  | cons a t ih =>
      cases w with
      | zero => simp
      | succ w =>
          rw [List.take_succ_cons, List.zipIdx_cons', List.countP_cons]
          simp only [Function.comp_apply]
          have hhead : (previous (a :: t) a 0 == none) = true := by simp [previous]
          rw [hhead]
          simp only [if_true, Nat.add_zero]
          rw [eraseDups_cons_length]
          have hcond : ∀ e : Nat × Nat,
              (previous (a :: t) e.1 (e.2 + 1) == none) =
                (previous t e.1 e.2 == none && Bool.not (e.1 == a)) := by
            intro e
            rw [previous_cons_succ]
            cases hp : previous t e.1 e.2 with
            | none =>
                by_cases hea : a == e.1
                · have hea' : (e.1 == a) = true := by
                    apply beq_iff_eq.mpr
                    exact (beq_iff_eq.mp hea).symm
                  simp [hp, hea', (beq_iff_eq.mp hea').symm]
                · have hea' : (e.1 == a) = false := by
                    by_cases h : e.1 == a
                    · have h' : a = e.1 := (beq_iff_eq.mp h).symm
                      exact (hea (by simpa [h'])).elim
                    · exact Bool.eq_false_of_ne_true h
                  have hne : a ≠ e.1 := by
                    intro h
                    have ht : (e.1 == a) = true := by rw [h]; simp
                    rw [hea'] at ht
                    contradiction
                  simp [hp, hea', hne]
            | some p => simp [hp]
          have htail : (t.take w).zipIdx.countP (fun e =>
              previous (a :: t) e.1 (e.2 + 1) == none) =
              (t.take w).zipIdx.countP (fun e => previous t e.1 e.2 == none) -
                (t.take w).zipIdx.countP (fun e => e.1 == a && previous t e.1 e.2 == none) := by
            rw [show (t.take w).zipIdx.countP (fun e =>
                previous (a :: t) e.1 (e.2 + 1) == none) =
                (t.take w).zipIdx.countP (fun e =>
                  previous t e.1 e.2 == none && Bool.not (e.1 == a)) by
              apply List.countP_congr
              intro e he
              exact Bool.eq_iff_iff.mp (hcond e)]
            simpa [Bool.and_comm] using
              (countP_and_not (t.take w).zipIdx
                (fun e => previous t e.1 e.2 == none) (fun e => e.1 == a))
          have hmapcount : List.countP (fun e : Nat × Nat => previous (a :: t) e.1 e.2 == none)
              (List.map (Prod.map id (fun x => x + 1)) (t.take w).zipIdx) =
              List.countP (fun e : Nat × Nat => previous (a :: t) e.1 (e.2 + 1) == none)
                (t.take w).zipIdx := by
            rw [List.countP_map]
            rfl
          rw [hmapcount, htail]
          have hIH := ih w
          have hfc0 := first_count_of_eq (t.take w) a
          have hfc : (t.take w).zipIdx.countP
              (fun e => e.1 == a && previous t e.1 e.2 == none) =
              (if a ∈ t.take w then 1 else 0) := by
            rw [← hfc0]
            apply List.countP_congr
            intro e he
            have hj : e.2 ≤ w := by
              have hlt := (List.mem_zipIdx he).2.1
              simp at hlt
              omega
            rw [previous_take_eq t hj]
          have hle : (if a ∈ t.take w then 1 else 0) ≤
              (t.take w).eraseDups.length := by
            by_cases ha : a ∈ t.take w
            · have hp : 0 < (t.take w).eraseDups.length :=
                List.length_pos_of_mem (List.mem_eraseDups.mpr ha)
              simp [ha]
              omega
            · simp [ha]
          rw [hfc]
          have hdn : (t.take w).zipIdx.countP (fun e => (previous t e.1 e.2).isNone) =
              (t.take w).zipIdx.countP (fun e => previous t e.1 e.2 == none) := by
            apply List.countP_congr
            intro e he
            simp
          have hdn' : (t.take w).zipIdx.countP (fun e => previous t e.1 e.2 == none) =
              (t.take w).zipIdx.countP (fun e => (previous t e.1 e.2).isNone) := hdn.symm
          rw [hIH]
          by_cases hat : a ∈ t.take w
          · simp [hat]
            have hp : 0 < (t.take w).zipIdx.countP
                (fun e => previous t e.1 e.2 == none) := by
              rw [← hIH]
              exact List.length_pos_of_mem (List.mem_eraseDups.mpr hat)
            omega
          · simp [hat]

theorem DistinctMarks_sound : TinylambOpt.ResearchGoals.Frontiers.TextIndex.DistinctMarks.Goal := by
  intro ids l w
  induction ids generalizing l w with
  | nil => simp [previous]
  | cons a t ih =>
      cases l with
      | succ l =>
          let slice := (t.drop l).take w
          have hIH := ih l w
          have heq : slice.zipIdx.countP (fun e => (previous t e.1 (l + e.2)).all (fun p => p < l)) =
              slice.zipIdx.countP (fun e =>
                (previous (a :: t) e.1 (l + 1 + e.2)).all (fun p => p < l + 1)) := by
            apply List.countP_congr
            intro e he
            have hpos : l + 1 + e.2 = (l + e.2) + 1 := by omega
            rw [hpos, previous_cons_succ]
            cases hprev : previous t e.1 (l + e.2) with
            | none =>
                by_cases hea : a == e.1 <;> simp [hprev, hea]
            | some p =>
                have hlt : p + 1 < l + 1 ↔ p < l := by omega
                simp [hprev, hlt]
          exact hIH.trans heq
      | zero =>
          simp only [List.drop_zero, Nat.zero_add]
          have hzero := distinct_first_zero (a :: t) w
          have heq : ((a :: t).take w).zipIdx.countP (fun e =>
              (previous (a :: t) e.1 e.2).all (fun p => p < 0)) =
              ((a :: t).take w).zipIdx.countP (fun e => previous (a :: t) e.1 e.2 == none) := by
            apply List.countP_congr
            intro e he
            cases hp : previous (a :: t) e.1 e.2 with
            | none => simp [hp]
            | some p => simp [hp]
          exact hzero.trans heq.symm

/-! ## FM-index: shift-by-one prefix extension (LocateExtension) -/

private theorem drop_pred_of_preceding {s : Word} {i a : Nat}
    (h : preceding s i = some a) : s.drop (i - 1) = a :: s.drop i := by
  unfold preceding at h
  by_cases hi0 : i == 0
  · simp [hi0] at h
  · have hget : s[i - 1]? = some a := by simpa [hi0] using h
    have hpos : 0 < i := by
      by_cases hi : i = 0
      · simp [hi] at hi0
      · omega
    have htake : s.take i = s.take (i - 1) ++ [a] := by
      rw [← Nat.sub_add_cancel (Nat.succ_le_of_lt hpos), List.take_succ]
      simp [hget]
    have h1 := List.take_append_drop (i - 1) s
    have h2 := List.take_append_drop i s
    rw [htake] at h2
    have h3 : s.take (i - 1) ++ (a :: s.drop i) = s.take (i - 1) ++ s.drop (i - 1) := by
      simpa [List.append_assoc] using h2.trans h1.symm
    exact List.append_cancel_left h3.symm

private theorem preceding_succ_of_getElem {s : Word} {i a : Nat}
    (h : s[i]? = some a) : preceding s (i + 1) = some a := by
  unfold preceding
  simpa using h

private theorem getElem_preceding_succ {s : Word} {i a : Nat}
    (h : preceding s (i + 1) = some a) : s[i]? = some a := by
  unfold preceding at h
  simpa using h

private theorem drop_succ_of_preceding {s : Word} {i a : Nat}
    (h : preceding s (i + 1) = some a) : s.drop i = a :: s.drop (i + 1) := by
  have hget : s[i]? = some a := getElem_preceding_succ h
  rcases List.getElem?_eq_some_iff.mp hget with ⟨hi, hia⟩
  have htake : s.take (i + 1) = s.take i ++ [a] := by
    rw [List.take_succ]
    simp [hget]
  have h1 := List.take_append_drop (i + 1) s
  have h2 := List.take_append_drop i s
  rw [htake] at h1
  have h3 : s.take i ++ (a :: s.drop (i + 1)) = s.take i ++ s.drop i := by
    simpa [List.append_assoc] using h1.trans h2.symm
  exact (List.append_cancel_left h3).symm

private theorem prefix_ext_mem (s p : Word) {i a : Nat} (hi : i ∈ sa s) :
    a :: p <+: s.drop i ↔ preceding s (i + 1) = some a ∧ p <+: s.drop (i + 1) := by
  constructor
  · intro h
    rcases h with ⟨u, hu⟩
    cases hdi : s.drop i with
    | nil =>
        rw [hdi] at hu
        simp at hu
    | cons b bs =>
        rw [hdi] at hu
        have hcons : a :: (p ++ u) = b :: bs := by simpa [List.append_assoc] using hu
        have hb : a = b := by injection hcons
        have hp_u : p ++ u = bs := by injection hcons
        have hdrop : s.drop i = a :: bs := by simpa [hb] using hdi
        have hlen : 0 < (s.drop i).length := by rw [hdi]; simp
        have hget : s[i]? = some a := by
          have hg : (s.drop i)[0]? = some a := by rw [hdrop]; simp
          simpa [List.getElem?_drop] using hg
        have hbs : bs = s.drop (i + 1) := by
          have h1 : (s.drop i).drop 1 = bs := by rw [hdrop]; simp
          have h2 : (s.drop i).drop 1 = s.drop (i + 1) := by
            simpa [Nat.add_comm] using (List.drop_drop (l := s) (i := 1) (j := i))
          exact h1.symm.trans h2
        refine ⟨preceding_succ_of_getElem hget, ?_⟩
        rw [← hbs]
        exact ⟨u, by simpa [List.append_assoc] using hp_u⟩
  · rintro ⟨hprec, hp⟩
    have hd := drop_succ_of_preceding hprec
    rw [hd]
    exact (List.prefix_cons_inj a).mpr hp

private theorem pairwise_map_pred_aux (s : Word) (a : Nat) (l : List Nat)
    (hl : l.Pairwise (fun i j => s.drop i < s.drop j))
    (hpre : ∀ i ∈ l, preceding s i = some a) :
    (l.map (fun j => j - 1)).Pairwise (fun i j => s.drop i < s.drop j) := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons i t ih =>
      rw [List.map_cons, List.pairwise_cons]
      constructor
      · intro j hj
        rcases List.mem_map.mp hj with ⟨k, hk, rfl⟩
        have hik : s.drop i < s.drop k := (List.pairwise_cons.mp hl).1 k hk
        have hip : preceding s i = some a := hpre i (by simp)
        have hkp : preceding s k = some a := hpre k (by simp [hk])
        rw [drop_pred_of_preceding hip, drop_pred_of_preceding hkp]
        simpa using (List.append_left_lt (l₁ := [a]) hik)
      · exact ih (List.pairwise_cons.mp hl).2 (by
          intro x hx
          exact hpre x (by simp [hx]))

private theorem nodup_of_drop_pairwise (s : Word) {l : List Nat}
    (h : l.Pairwise (fun i j => s.drop i < s.drop j)) : l.Nodup := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a t ih =>
      rw [List.nodup_cons]
      constructor
      · intro ha
        exact (List.lt_irrefl (s.drop a)) ((List.pairwise_cons.mp h).1 a ha)
      · exact ih (List.pairwise_cons.mp h).2

private theorem perm_of_nodup_of_mem_iff {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (d₁ : l₁.Nodup) (d₂ : l₂.Nodup)
    (h : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => exact List.Perm.refl []
      | cons b bs =>
          have hb := (h b).mpr (by simp)
          simp at hb
  | cons a t ih =>
      have ha : a ∈ l₂ := (h a).mp (by simp)
      have hnd₂' := d₂.erase a
      have hnd₁' := (List.nodup_cons.mp d₁).2
      have hiff : ∀ x, x ∈ t ↔ x ∈ l₂.erase a := by
        intro x
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hx
          exact ⟨fun hxa => (List.nodup_cons.mp d₁).1 (hxa ▸ hx), (h x).mp (by simp [hx])⟩
        · rintro ⟨hxa, hx⟩
          rcases List.mem_cons.mp ((h x).mpr hx) with rfl | hx'
          · exact (hxa rfl).elim
          · exact hx'
      exact (List.Perm.cons a (ih hnd₁' hnd₂' hiff)).trans
        (List.perm_cons_erase ha).symm

private theorem eq_of_perm_of_drop_pairwise (s : Word) {l₁ l₂ : List Nat}
    (hp : l₁.Perm l₂)
    (h₁ : l₁.Pairwise (fun i j => s.drop i < s.drop j))
    (h₂ : l₂.Pairwise (fun i j => s.drop i < s.drop j)) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := List.Perm.eq_nil (List.Perm.symm hp)
      subst l₂
      rfl
  | cons a t ih =>
      have ha_mem : a ∈ l₂ := hp.mem_iff.mp (by exact List.mem_cons_self)
      have hamin : ∀ x ∈ l₂, x = a ∨ s.drop a < s.drop x := by
        intro x hx
        have hxmem : x ∈ a :: t := hp.mem_iff.mpr hx
        rcases List.mem_cons.mp hxmem with rfl | hxt
        · exact Or.inl rfl
        · exact Or.inr ((List.pairwise_cons.mp h₁).1 x hxt)
      cases l₂ with
      | nil => simp at ha_mem
      | cons b u =>
          have hbmin := hamin b (by simp)
          rcases hbmin with rfl | hlt
          · congr 1
            exact ih (List.Perm.cons_inv hp) (List.pairwise_cons.mp h₁).2
              (List.pairwise_cons.mp h₂).2
          · have hau : a ∈ u := by
              rcases List.mem_cons.mp ha_mem with rfl | hau
              · exact False.elim ((List.lt_irrefl (s.drop a)) hlt)
              · exact hau
            have hba : s.drop b < s.drop a := (List.pairwise_cons.mp h₂).1 a hau
            exact False.elim ((List.not_lt.mpr (List.le_of_lt hlt)) hba)

theorem LocateExtension_sound :
    TinylambOpt.ResearchGoals.Frontiers.TextIndex.LocateExtension.Goal := by
  intro s p a
  let L := (sa s).filter (fun j => p.isPrefixOf (s.drop j) && preceding s j == some a)
  let R := L.map (fun j => j - 1)
  let H := hits s (a :: p)
  have hLpair : L.Pairwise (fun i j => s.drop i < s.drop j) :=
    List.Pairwise.sublist List.filter_sublist (sa_sorted_lt s)
  have hRpair : R.Pairwise (fun i j => s.drop i < s.drop j) := by
    dsimp [R]
    apply pairwise_map_pred_aux s a
    · exact hLpair
    · intro i hi
      exact beq_iff_eq.mp (Bool.and_eq_true _ _ |>.mp (List.mem_filter.mp hi).2).2
  have hHpair : H.Pairwise (fun i j => s.drop i < s.drop j) := by
    unfold H hits
    exact List.Pairwise.sublist List.filter_sublist (sa_sorted_lt s)
  have hnodH : H.Nodup := nodup_of_drop_pairwise s hHpair
  have hnodR : R.Nodup := nodup_of_drop_pairwise s hRpair
  have hmemiff : ∀ i, i ∈ H ↔ i ∈ R := by
    intro i
    constructor
    · intro hi
      have hisa : i ∈ sa s := (hits_mem.mp hi).1
      have hpfx : a :: p <+: s.drop i := (hits_mem.mp hi).2
      have heq := (prefix_ext_mem s p (a := a) hisa).mp hpfx
      have hprec : preceding s (i + 1) = some a := heq.1
      have hile : i + 1 ≤ s.length := by
        have hget := getElem_preceding_succ hprec
        rcases List.getElem?_eq_some_iff.mp hget with ⟨hlt, _⟩
        omega
      apply List.mem_map.mpr
      refine ⟨i + 1, ?_, by omega⟩
      apply List.mem_filter.mpr
      constructor
      · exact mem_sa_iff.mpr hile
      · rw [Bool.and_eq_true]
        exact ⟨List.isPrefixOf_iff_prefix.mpr heq.2, beq_iff_eq.mpr hprec⟩
    · intro hi
      rcases List.mem_map.mp hi with ⟨j, hj, hjEq⟩
      rw [← hjEq]
      have hjmem : j ∈ sa s := (List.mem_filter.mp hj).1
      have hjprec : preceding s j = some a :=
        beq_iff_eq.mp (Bool.and_eq_true _ _ |>.mp (List.mem_filter.mp hj).2).2
      have hjpos : 0 < j := by
        by_cases hj0 : j = 0
        · subst j
          unfold preceding at hjprec
          simp at hjprec
        · omega
      have hpfxp : p <+: s.drop j := List.isPrefixOf_iff_prefix.mp
        (Bool.and_eq_true _ _ |>.mp (List.mem_filter.mp hj).2).1
      have hjpred : j - 1 + 1 = j := Nat.sub_add_cancel (Nat.succ_le_of_lt hjpos)
      have himem : j - 1 ∈ sa s := mem_sa_iff.mpr (by
        have := mem_sa_iff.mp hjmem
        omega)
      have hemp := (prefix_ext_mem s p (a := a) himem).mpr (by
        constructor
        · rwa [hjpred]
        · rwa [hjpred])
      apply hits_mem.mpr
      constructor
      · rw [hjEq]
        apply mem_sa_iff.mpr
        have := mem_sa_iff.mp hjmem
        omega
      · simpa [List.isPrefixOf_iff_prefix] using hemp
  have hperm : H.Perm R := perm_of_nodup_of_mem_iff hnodH hnodR hmemiff
  change hits s (a :: p) =
    ((hits s p).filter (fun i => preceding s i == some a)).map (fun i => i - 1)
  have hRL : R = ((hits s p).filter (fun i => preceding s i == some a)).map
      (fun i => i - 1) := by
    dsimp [R, L, hits]
    rw [List.filter_filter]
    apply congrArg (fun l : List Nat => l.map (fun i => i - 1))
    apply List.filter_congr
    intro i hi
    rw [Bool.and_comm]
  rw [← hRL]
  exact eq_of_perm_of_drop_pairwise s hperm hHpair hRpair

end TinylambOpt.ResearchProofs.Frontiers.TextIndex
