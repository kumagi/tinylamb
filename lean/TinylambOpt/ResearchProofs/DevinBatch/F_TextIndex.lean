import TinylambOpt.ResearchGoals.Frontiers.TextIndex

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.F_TextIndex

open TinylambOpt.ResearchGoals.Frontiers.TextIndex

/-! ## Suffix-array basics -/

private theorem mem_sa_iff {s : Word} {i : Nat} : i ∈ sa s ↔ i ≤ s.length := by
  unfold sa
  rw [List.mem_mergeSort, List.mem_range]
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

private theorem hits_mem {s p : Word} {j : Nat} :
    j ∈ hits s p ↔ j ∈ sa s ∧ p <+: s.drop j := by
  rw [hits, List.mem_filter]
  simp only [List.isPrefixOf_iff_prefix]

private theorem hits_length_eq_count_range (s p : Word) :
    (hits s p).length =
      (List.range (s.length+1)).countP (fun i => p.isPrefixOf (s.drop i)) := by
  unfold hits
  rw [← List.countP_eq_length_filter]
  unfold sa
  exact (List.mergeSort_perm (List.range (s.length+1))
    (fun i j : Nat => decide (s.drop i ≤ s.drop j))).countP_eq
      (fun i => p.isPrefixOf (s.drop i))

private theorem lo_eq_countP_range (s p : Word) :
    lo s p = (List.range (s.length+1)).countP (fun i => decide (s.drop i < p)) := by
  unfold lo sa
  exact (List.mergeSort_perm (List.range (s.length+1))
    (fun i j : Nat => decide (s.drop i ≤ s.drop j))).countP_eq _

/-- Counting a list's elements via positions: `countP` over `l` equals `countP`
over `range l.length` of the element at each index (out-of-range maps to
`false`, which never occurs for `i ∈ range l.length`). -/
private theorem countP_getElem? {α : Type} (l : List α) (p : α → Bool) :
    l.countP p =
      (List.range l.length).countP (fun i => (l[i]?).any p) := by
  induction l with
  | nil => simp
  | cons a t ih =>
    cases t with
    | nil =>
      simp [List.countP_cons, List.getElem?_cons_zero, Option.any_some]
    | cons b u =>
      -- `t = b :: u`, so `1 ≤ t.length`.
      have ht : 1 ≤ (b :: u).length := by simp
      have hlen : (a :: b :: u).length = (b :: u).length + 1 := rfl
      rw [List.countP_cons, ih, hlen]
      -- Split `range (t.length + 1) = range t.length ++ [t.length]`.
      have hrw : List.range ((b :: u).length + 1) =
          List.range (b :: u).length ++ [(b :: u).length] := by
        have h := List.range_succ (n := (b :: u).length)
        rwa [Nat.succ_eq_add_one] at h
      rw [hrw, List.countP_append, List.countP_singleton]
      -- Split `range t.length = 0 :: map succ (range (t.length - 1))`.
      have htm1 : (b :: u).length - 1 + 1 = (b :: u).length := by omega
      have hrw2 : List.range (b :: u).length =
          0 :: List.map Nat.succ (List.range ((b :: u).length - 1)) := by
        have h := List.range_succ_eq_map (n := (b :: u).length - 1)
        rwa [htm1] at h
      rw [hrw2, List.countP_cons, List.countP_map]
      -- Now relate `countP (Q ∘ succ)` to `countP Qt` pointwise, and evaluate
      -- the boundary `if`s.
      have hQ0 : ((a :: b :: u)[0]?).any p = p a := by
        rw [List.getElem?_cons_zero, Option.any_some]
      have hQs : ∀ i : Nat, ((a :: b :: u)[i + 1]?).any p =
          ((b :: u)[i]?).any p := fun i => rfl
      have hQlen : ((a :: b :: u)[(b :: u).length]?).any p =
          ((b :: u)[(b :: u).length - 1]?).any p := by
        have hidx : (b :: u).length = (b :: u).length - 1 + 1 := by omega
        conv_lhs => rw [hidx]
        rw [List.getElem?_cons_succ]
      have hQtlen : ((b :: u)[(b :: u).length - 1]?).any p =
          p ((b :: u)[(b :: u).length - 1]) := by
        rw [List.getElem?_eq_getElem (by omega), Option.any_some]
      rw [hQ0, hQlen, hQtlen]
      -- `countP (Q ∘ succ) X = countP Qt X` via pointwise equality.
      have hcomp : ∀ i : Nat,
          ((fun j => ((a :: b :: u)[j]?).any p) ∘ Nat.succ) i =
            ((b :: u)[i]?).any p := by
        intro i
        show ((a :: b :: u)[i + 1]?).any p = _
        exact hQs i
      rw [List.countP_congr (fun i _ => by rw [hcomp i])]
      -- Reassemble `countP Qt (range (t.length-1)) + if Qt(t.length-1)` into
      -- `countP Qt (range t.length)`.
      have hrw3 : List.range (b :: u).length =
          List.range ((b :: u).length - 1) ++ [(b :: u).length - 1] := by
        have h := List.range_succ (n := (b :: u).length - 1)
        rwa [htm1] at h
      rw [hrw3, List.countP_append, List.countP_singleton]
      rw [show ((b :: u)[(b :: u).length - 1]?).any p =
          p ((b :: u)[(b :: u).length - 1]) from hQtlen]
      -- Goal is now a Nat equation among `countP`-terms and two `if`s; close by
      -- commutativity/associativity of `+`.
      omega

/-! ## Prefix / lexicographic order lemmas -/

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

/-! ## Sorted-block lemmas (take/drop of a sorted list) -/

/-- In a list sorted by `s.drop`, the elements whose drop is `< p` form an
initial segment: `take (countP) = filter`. -/
private theorem take_countP_lt_eq_filter (s p : Word) : ∀ L : List Nat,
    L.Pairwise (fun i j => s.drop i < s.drop j) →
    L.take (L.countP (fun i => decide (s.drop i < p))) =
      L.filter (fun i => decide (s.drop i < p)) := by
  intro L
  induction L with
  | nil => intro h; simp
  | cons a t ih =>
      intro hlt
      rw [List.pairwise_cons] at hlt
      have hilt := ih hlt.2
      by_cases ha : s.drop a < p
      · have hcnt : (a :: t).countP (fun i => decide (s.drop i < p)) =
            t.countP (fun i => decide (s.drop i < p)) + 1 := by
          simp [List.countP_cons, ha]
        have hflt : (a :: t).filter (fun i => decide (s.drop i < p)) =
            a :: t.filter (fun i => decide (s.drop i < p)) := by
          simp [List.filter_cons, ha]
        rw [hflt, hcnt, List.take_succ_cons, hilt]
      · have hnt : ∀ b ∈ t, ¬ s.drop b < p := by
          intro b hb hbp
          have hab : s.drop a < s.drop b := hlt.1 b hb
          exact ha (List.lt_trans hab hbp)
        have hct : t.countP (fun i => decide (s.drop i < p)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          simpa using hnt b hb
        have hft : t.filter (fun i => decide (s.drop i < p)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro b hb
          simp [hnt b hb]
        have hcnt : (a :: t).countP (fun i => decide (s.drop i < p)) =
            t.countP (fun i => decide (s.drop i < p)) := by
          simp [List.countP_cons, ha]
        have hflt : (a :: t).filter (fun i => decide (s.drop i < p)) =
            t.filter (fun i => decide (s.drop i < p)) := by
          simp [List.filter_cons, ha]
        rw [hflt, hcnt, hct, hft]
        simp

/-- The hits of `p` inside a sorted list form a contiguous block immediately
after the suffixes smaller than `p`. -/
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

/-- `Contiguous.Goal` restated for `sa s`: the hits form a contiguous block. -/
private theorem contiguous (s p : Word) :
    ((sa s).drop (lo s p)).take (hits s p).length = hits s p := by
  dsimp [lo, hits]
  exact sorted_block_aux s p (sa s) (sa_sorted_lt s)

/-! ## LocateExtension -/

private theorem hits_pairwise_lt (s q : Word) :
    (hits s q).Pairwise (fun i j => s.drop i < s.drop j) :=
  List.Pairwise.filter _ (sa_sorted_lt s)

/-- Shape of a hit of `a :: p`: the position is `< length`, its head char is
`a`, and the tail still has `p` as a prefix. -/
private theorem hit_succ_shape {s : Word} {p : Word} {a : Nat} {i : Nat}
    (hi : i ∈ hits s (a :: p)) :
    i < s.length ∧ s[i]? = some a ∧ s.drop i = a :: s.drop (i + 1) ∧
      p <+: s.drop (i + 1) := by
  rw [hits_mem] at hi
  obtain ⟨hisa, hpre⟩ := hi
  have hilt : i < s.length := by
    by_cases h : i < s.length
    · exact h
    · have hge : s.length ≤ i := by omega
      have hd : s.drop i = [] := List.drop_eq_nil_iff.mpr hge
      rw [hd] at hpre
      rcases hpre with ⟨u, hu⟩
      simp at hu
  have hdec := List.drop_eq_getElem_cons hilt
  rw [hdec] at hpre
  obtain ⟨ha_eq, hp⟩ := List.cons_prefix_cons.mp hpre
  refine ⟨hilt, ?_, ?_, hp⟩
  · rw [List.getElem?_eq_getElem hilt]
    exact congrArg some ha_eq.symm
  · rw [hdec, ha_eq.symm]

/-- A hit of `p` whose preceding char is `a`: position `j ≥ 1`, `s[j-1] = a`,
and `drop (j-1) = a :: drop j`. -/
private theorem hit_pred_shape {s : Word} {p : Word} {a j : Nat}
    (hj : j ∈ hits s p) (hpa : preceding s j == some a) :
    1 ≤ j ∧ j ≤ s.length ∧ s[j-1]? = some a ∧ s.drop (j-1) = a :: s.drop j ∧
      p <+: s.drop j := by
  rw [hits_mem] at hj
  obtain ⟨hjsa, hp⟩ := hj
  have hjle : j ≤ s.length := mem_sa_iff.mp hjsa
  have hj0 : j ≠ 0 := by
    intro h0
    subst j
    simp [preceding] at hpa
  have hj1 : 1 ≤ j := by omega
  have hjm1 : j - 1 < s.length := by omega
  have hbeq : (j == 0) = false :=
    Bool.eq_false_of_ne_true (fun ht => hj0 (beq_iff_eq.mp ht))
  have hprev : preceding s j = some s[j-1] := by
    unfold preceding
    rw [ite_eq_right (by simp [hbeq])]
    exact List.getElem?_eq_getElem hjm1
  rw [hprev] at hpa
  have hjeq : s[j-1] = a := by
    have h2 : some s[j-1] = some a := beq_iff_eq.mp hpa
    exact Option.some.inj h2
  refine ⟨hj1, hjle, ?_, ?_, hp⟩
  · rw [List.getElem?_eq_getElem hjm1]
    exact congrArg some hjeq
  · have hdec := List.drop_eq_getElem_cons hjm1
    have hjm1' : j - 1 + 1 = j := by omega
    rw [hjm1'] at hdec
    exact hdec.trans (congrArg (fun x => x :: s.drop j) hjeq)

/-- The `i ↦ i+1` correspondence: hits of `a :: p` at `i` correspond exactly to
hits of `p` at `i+1` whose preceding char is `a`. -/
private theorem hit_succ_iff {s : Word} {p : Word} {a i : Nat} (hi : i < s.length) :
    i ∈ hits s (a :: p) ↔
      i + 1 ∈ hits s p ∧ preceding s (i + 1) == some a := by
  have hsa : i ∈ sa s := mem_sa_iff.mpr (by omega)
  have hsa1 : i + 1 ∈ sa s := mem_sa_iff.mpr (by omega)
  have hdec : s.drop i = s[i] :: s.drop (i + 1) := List.drop_eq_getElem_cons hi
  have hprev : preceding s (i + 1) = some s[i] := by
    unfold preceding
    have h1 : ((i + 1) == 0) = false :=
      Bool.eq_false_of_ne_true (fun ht => by
        have : i + 1 = 0 := beq_iff_eq.mp ht
        omega)
    rw [ite_eq_right (by simp [h1])]
    have h2 : i + 1 - 1 = i := by omega
    rw [h2]
    exact List.getElem?_eq_getElem hi
  rw [hits_mem, hits_mem]
  simp only [hsa, hsa1, true_and]
  rw [hdec, hprev]
  simp only [List.cons_prefix_cons, Option.some_beq_some, beq_iff_eq]
  constructor
  · rintro ⟨ha_eq, hp⟩
    exact ⟨hp, ha_eq.symm⟩
  · rintro ⟨hp, hpa⟩
    exact ⟨hpa.symm, hp⟩

theorem LocateExtension_sound : LocateExtension.Goal := by
  intro s p a
  -- Prove the `+1` form, then convert to the `-1` form of the goal.
  have hmain : (hits s (a :: p)).map (· + 1) =
      (hits s p).filter (fun i => preceding s i == some a) := by
    apply List.Perm.eq_of_pairwise (le := fun i j => s.drop i < s.drop j)
    · intro x y _ _ hxy hyx
      exact absurd hxy (List.lt_asymm hyx)
    · -- pairwise of the mapped list
      rw [List.pairwise_map]
      have hpair := hits_pairwise_lt s (a :: p)
      exact List.Pairwise.imp_of_mem (fun hu hv huvlt => by
        obtain ⟨-, -, hudu, -⟩ := hit_succ_shape hu
        obtain ⟨-, -, hdvd, -⟩ := hit_succ_shape hv
        rw [hudu, hdvd] at huvlt
        exact List.cons_lt_cons_self.mp huvlt) hpair
    · -- pairwise of the filtered list
      exact List.Pairwise.filter _ (hits_pairwise_lt s p)
    · -- permutation
      have hndh : (hits s (a :: p)).Nodup := (List.filter_sublist).nodup (sa_nodup s)
      have hndh' : (hits s p).Nodup := (List.filter_sublist).nodup (sa_nodup s)
      have hnd1 : ((hits s (a :: p)).map (· + 1)).Nodup := by
        rw [List.nodup_iff_pairwise_ne, List.pairwise_map]
        exact List.Pairwise.imp (fun h heq => h (by omega)) hndh
      have hnd2 : ((hits s p).filter (fun i => preceding s i == some a)).Nodup :=
        (List.filter_sublist).nodup hndh'
      apply (List.perm_ext_iff_of_nodup hnd1 hnd2).mpr
      intro x
      constructor
      · -- `x ∈ map (+1) hits(a::p)` implies `x ∈ filter P hits p`
        intro hx
        rw [List.mem_map] at hx
        obtain ⟨i, hi, hix⟩ := hx
        have hilt : i < s.length := (hit_succ_shape hi).1
        have hb := (hit_succ_iff hilt).mp hi
        rw [List.mem_filter, ← hix]
        exact ⟨hb.1, hb.2⟩
      · -- `x ∈ filter P hits p` implies `x ∈ map (+1) hits(a::p)`
        intro hx
        rw [List.mem_filter] at hx
        obtain ⟨hxp, hpa⟩ := hx
        have hjle : x ≤ s.length := mem_sa_iff.mp ((hits_mem).mp hxp).1
        have hx0 : x ≠ 0 := by
          intro h0
          subst x
          simp [preceding] at hpa
        have hx1 : 1 ≤ x := by omega
        have hxm1 : x - 1 < s.length := by omega
        have hx' : x - 1 + 1 = x := by omega
        have harg : x - 1 + 1 ∈ hits s p ∧ preceding s (x - 1 + 1) == some a := by
          rw [hx']
          exact ⟨hxp, hpa⟩
        have hb := (hit_succ_iff hxm1).mpr harg
        rw [List.mem_map]
        exact ⟨x - 1, hb, by omega⟩
  -- convert the `+1` form to the `-1` form of the goal
  have hconv := congrArg (·.map (· - 1)) hmain
  rw [List.map_map] at hconv
  have hfun : ∀ x : Nat, ((· - 1) ∘ (· + 1)) x = x := by
    intro x
    simp only [Function.comp_apply]
    omega
  rw [List.map_id'' hfun] at hconv
  exact hconv

end TinylambOpt.ResearchProofs.DevinBatch.F_TextIndex
