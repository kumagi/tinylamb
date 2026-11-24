import TinylambOpt.ResearchGoals.ExecutionStrategies.OrderedStreams

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.OrderedStreams

open TinylambOpt.ResearchGoals.OrderedStreams

/-!
# Ordered-stream execution strategies

Proofs for the eight research propositions in
`TinylambOpt.ResearchGoals.OrderedStreams`.
-/

/-- `getLast?` of a cons with nonempty tail equals `getLast?` of the tail. -/
private theorem getLast?_cons_cons {α : Type} (a b : α) (u : List α) :
    (a :: b :: u).getLast? = (b :: u).getLast? := by
  rw [List.getLast?_cons, List.getLast?_cons]
  rfl

/-- In a sorted row list, the head key is at most the last key. -/
private theorem sorted_head_key_le_last_key {l : List Row}
    (hs : l.Pairwise (fun a b => a.1 ≤ b.1))
    {h v : Row} (hh : l.head? = some h) (hv : l.getLast? = some v) : h.1 ≤ v.1 := by
  obtain ⟨t, rfl⟩ := List.head?_eq_some_iff.mp hh
  cases t with
  | nil =>
      have hv' : ([h] : List Row).getLast? = some h := by
        rw [List.getLast?_cons]
        rfl
      rw [hv'] at hv
      injection hv with hv'
      subst hv'
      exact Int.le_rfl
  | cons b u =>
      have htv : (b :: u).getLast? = some v := by
        have h2 := hv
        rw [getLast?_cons_cons h b u] at h2
        exact h2
      obtain ⟨ys, hys⟩ := List.getLast?_eq_some_iff.mp htv
      have hvmem : v ∈ b :: u := by
        rw [hys]
        simp
      exact (List.pairwise_cons.mp hs).1 v hvmem

theorem sound_SpliceBlock : SpliceBlock.Goal := by
  intro block tail right hs hr hlast
  revert hs hlast
  induction block with
  | nil =>
      intro hs hlast
      rfl
  | cons x xs ih =>
      intro hs hlast
      cases right with
      | nil => simp
      | cons y ys =>
          -- every element of `x :: xs` has key ≤ y.1; in particular x.1 ≤ y.1
          obtain ⟨v, hv⟩ : ∃ v, (x :: xs).getLast? = some v :=
            ⟨xs.getLast?.getD x, List.getLast?_cons⟩
          have hxv : x.1 ≤ v.1 :=
            sorted_head_key_le_last_key hs List.head?_cons hv
          have hvy : v.1 ≤ y.1 := by
            apply hlast v _ y _
            · rw [hv]
              exact Option.mem_def.mpr rfl
            · rw [List.head?_cons]
              exact Option.mem_def.mpr rfl
          have hxy : x.1 ≤ y.1 := Int.le_trans hxv hvy
          have hle : leKey x y = true := decide_eq_true hxy
          rw [List.cons_append, List.cons_merge_cons, ite_eq_left hle]
          congr 1
          apply ih
          · exact (List.pairwise_cons.mp hs).2
          · intro x' hx' y' hy'
            rw [List.head?_cons, Option.mem_some_iff] at hy'
            subst hy'
            cases xs with
            | nil => simp at hx'
            | cons b u =>
                have hx'' : x' ∈ (x :: b :: u).getLast? := by
                  rw [getLast?_cons_cons x b u]
                  exact hx'
                apply hlast x' hx'' y
                · rw [List.head?_cons]
                  exact Option.mem_def.mpr rfl

/-! ### MonotoneProbes helpers -/

/-- Elements kept by `takeWhile` satisfy the predicate. -/
private theorem mem_takeWhile {α : Type} {p : α → Bool} {l : List α} {x : α}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => simp at h
  | cons a l ih =>
      rw [List.takeWhile_cons] at h
      split at h
      · rename_i hp
        rcases List.mem_cons.mp h with rfl | h'
        · exact hp
        · exact ih h'
      · simp at h

/-- Composing two `dropWhile`s collapses when the first predicate implies the second. -/
private theorem dropWhile_dropWhile {α : Type} {p q : α → Bool} {l : List α}
    (hpq : ∀ x, p x = true → q x = true) :
    (l.dropWhile p).dropWhile q = l.dropWhile q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.dropWhile_cons]
      by_cases hp : p x = true
      · rw [ite_eq_left hp, List.dropWhile_cons, ite_eq_left (hpq x hp)]
        exact ih
      · rw [ite_eq_right hp]

/-- `find?` is unaffected by a `dropWhile` prefix whose elements all fail `q`. -/
private theorem find?_dropWhile_of {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ x ∈ l.takeWhile p, q x = false) :
    (l.dropWhile p).find? q = l.find? q := by
  have hnone : (l.takeWhile p).find? q = none := by
    rw [List.find?_eq_none]
    intro x hx
    exact Bool.eq_false_iff.mp (h x hx)
  have key : l.find? q = (l.takeWhile p ++ l.dropWhile p).find? q := by
    rw [List.takeWhile_append_dropWhile]
  rw [key, List.find?_append, hnone, Option.none_or]

/-- The head of a `dropWhile` result fails the predicate. -/
private theorem head_dropWhile {α : Type} {p : α → Bool} {l : List α} {r : α}
    (h : (l.dropWhile p).head? = some r) : p r ≠ true := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      rw [List.dropWhile_cons] at h
      by_cases hp : p x = true
      · rw [ite_eq_left hp] at h
        exact ih h
      · rw [ite_eq_right hp] at h
        rw [List.head?_cons] at h
        injection h with h'
        subst h'
        exact hp

/-- Every row surviving `dropWhile (< k)` in a sorted run has key `≥ k`. -/
private theorem dropWhile_key_ge {rows : List Row} (k : Int) :
    sorted rows → ∀ {r : Row}, r ∈ rows.dropWhile (fun r => r.1 < k) → k ≤ r.1 := by
  induction rows with
  | nil => intro _ hr hmem; simp at hmem
  | cons a t ih =>
      intro hs hr hmem
      have hts : sorted t := (List.pairwise_cons.mp hs).2
      have hat : ∀ b ∈ t, a.1 ≤ b.1 := (List.pairwise_cons.mp hs).1
      rw [List.dropWhile_cons] at hmem
      by_cases ha : decide (a.1 < k) = true
      · rw [ite_eq_left ha] at hmem
        exact ih hts hmem
      · rw [ite_eq_right ha] at hmem
        have hak : k ≤ a.1 := by
          have h2 : ¬ a.1 < k := by
            intro hlt
            exact ha (decide_eq_true hlt)
          exact Int.not_lt.mp h2
        rcases List.mem_cons.mp hmem with rfl | hrt
        · exact hak
        · exact Int.le_trans hak (hat _ hrt)

/-- In a sorted row list whose keys are all `≥ k`, `find? (· == k)` inspects
only the head: it returns the head iff the head's key is `k`, else `none`. -/
private theorem find?_eq_head_filter_of_forall_ge {l : List Row} (hs : sorted l) {k : Int}
    (hge : ∀ r ∈ l, k ≤ r.1) :
    l.find? (fun r => r.1 == k) = l.head?.filter (fun r => r.1 == k) := by
  cases l with
  | nil => rfl
  | cons a t =>
      have hat : ∀ b ∈ t, a.1 ≤ b.1 := (List.pairwise_cons.mp hs).1
      rw [List.head?_cons, Option.filter_some, List.find?_cons]
      by_cases hq : (a.1 == k) = true
      · simp [hq]
      · have hqf : (a.1 == k) = false := Bool.eq_false_iff.mpr hq
        have haq : a.1 ≠ k := fun h => hq (beq_iff_eq.mpr h)
        have hak : k ≤ a.1 := hge a (List.mem_cons_self)
        have hak' : k < a.1 := by omega
        simp [hqf]
        intro i j hx
        have hxi : a.1 ≤ i := hat (i, j) hx
        omega

/-- For sorted rows, probing the `dropWhile (< k)` staircase for `== k`
agrees with filtering its head. -/
private theorem find?_eq_head?_filter {rows : List Row} (hs : sorted rows) (k : Int) :
    (rows.dropWhile fun r => r.1 < k).find? (fun r => r.1 == k) =
      (rows.dropWhile fun r => r.1 < k).head?.filter (fun r => r.1 == k) := by
  apply find?_eq_head_filter_of_forall_ge
  · exact List.Pairwise.sublist (List.dropWhile_sublist _) hs
  · intro r hr
    exact dropWhile_key_ge k hs hr

/-- Staircase probes equal per-key `find?` results. -/
private theorem seekMany_aux (ks : List Int) :
    ∀ (rows₀ : List Row) (k₀ : Int), ks.Pairwise (· ≤ ·) → sorted rows₀ →
      (∀ k ∈ ks, k₀ ≤ k) →
      seekMany ks (rows₀.dropWhile fun r => r.1 < k₀) =
        ks.map (fun k => rows₀.find? fun r => r.1 == k) := by
  induction ks with
  | nil => intro _ _ _ _ _; rfl
  | cons k ks ih =>
      intro rows₀ k₀ hks hrows hk₀
      have hkk : k₀ ≤ k := hk₀ k (List.mem_cons_self)
      have hk_tail : ∀ k' ∈ ks, k ≤ k' := (List.pairwise_cons.mp hks).1
      have hks_tail : ks.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hks).2
      -- unfold one seekMany step, then collapse the two nested dropWhiles
      show ((rows₀.dropWhile fun r => r.1 < k₀).dropWhile fun r => r.1 < k).head?.filter
            (fun r => r.1 == k) ::
          seekMany ks ((rows₀.dropWhile fun r => r.1 < k₀).dropWhile fun r => r.1 < k) =
        (k :: ks).map (fun k => rows₀.find? fun r => r.1 == k)
      rw [dropWhile_dropWhile (p := fun r => r.1 < k₀) (q := fun r => r.1 < k) (l := rows₀)
        (fun x hx => by
          have hx' : x.1 < k₀ := of_decide_eq_true hx
          have : x.1 < k := by omega
          exact decide_eq_true this)]
      rw [List.map_cons]
      congr 1
      · exact ((find?_eq_head?_filter hrows k).symm.trans
          (find?_dropWhile_of (l := rows₀) (p := fun r => r.1 < k) (q := fun r => r.1 == k)
            (fun x hx => by
              have hpx := mem_takeWhile hx
              have hxk : x.1 < k := of_decide_eq_true hpx
              have hxk' : x.1 ≠ k := by omega
              exact Bool.eq_false_iff.mpr (fun h => hxk' (beq_iff_eq.mp h)))))
      · exact ih rows₀ k hks_tail hrows hk_tail

theorem sound_MonotoneProbes : MonotoneProbes.Goal := by
  intro ks rows hks hrows
  cases ks with
  | nil => rfl
  | cons k ks =>
      have hks' : ks.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hks).2
      have hk_tail : ∀ k' ∈ ks, k ≤ k' := (List.pairwise_cons.mp hks).1
      -- unfold one step and apply the auxiliary staircase lemma
      show ((rows.dropWhile fun r => r.1 < k).head?.filter (fun r => r.1 == k)) ::
          seekMany ks (rows.dropWhile fun r => r.1 < k) =
        (k :: ks).map (fun k => rows.find? fun r => r.1 == k)
      rw [List.map_cons]
      congr 1
      · exact ((find?_eq_head?_filter hrows k).symm.trans
          (find?_dropWhile_of (l := rows) (p := fun r => r.1 < k) (q := fun r => r.1 == k)
            (fun x hx => by
              have hpx := mem_takeWhile hx
              have hxk : x.1 < k := of_decide_eq_true hpx
              have hxk' : x.1 ≠ k := by omega
              exact Bool.eq_false_iff.mpr (fun h => hxk' (beq_iff_eq.mp h)))))
      · exact seekMany_aux ks rows k hks' hrows hk_tail


/-! ### ReversePeerRuns helpers -/

private abbrev peerExpand (rs : List (Int × List Nat)) : List Row :=
  rs.flatMap fun (k, vs) => vs.map (k, ·)

private def candW : List (Int × List Nat) → Nat → List (Row × Nat)
  | [], _ => []
  | (k, vs) :: rs, off => candW rs (off + vs.length) ++ (vs.map (k, ·)).zipIdx off

private theorem peerExpand_cons (k : Int) (vs : List Nat) (rs : List (Int × List Nat)) :
    peerExpand ((k, vs) :: rs) = vs.map (k, ·) ++ peerExpand rs := by
  show ((k, vs) :: rs).flatMap _ = _
  rw [List.flatMap_cons]

private theorem peerExpand_append (l₁ l₂ : List (Int × List Nat)) :
    peerExpand (l₁ ++ l₂) = peerExpand l₁ ++ peerExpand l₂ := by
  show (l₁ ++ l₂).flatMap _ = _
  rw [List.flatMap_append]

private theorem peerExpand_singleton (k : Int) (vs : List Nat) :
    peerExpand [(k, vs)] = vs.map (k, ·) := by
  show [(k, vs)].flatMap _ = _
  rw [List.flatMap_cons]
  simp

private theorem peerExpand_reverse_append (rs : List (Int × List Nat)) (k : Int) (vs : List Nat) :
    peerExpand (rs.reverse ++ [(k, vs)]) = peerExpand rs.reverse ++ vs.map (k, ·) := by
  rw [peerExpand_append, peerExpand_singleton]

private theorem map_fst_zipIdx (l : List α) (i : Nat) :
    (l.zipIdx i).map Prod.fst = l := by
  induction l generalizing i with
  | nil => rfl
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.map_cons, ih]

private theorem candW_fst : ∀ (runs : List (Int × List Nat)) (off : Nat),
    (candW runs off).map Prod.fst = peerExpand runs.reverse := by
  intro runs
  induction runs with
  | nil => intro off; rfl
  | cons r rs ih =>
      obtain ⟨k, vs⟩ := r
      intro off
      show (candW rs (off + vs.length) ++ (vs.map (k, ·)).zipIdx off).map Prod.fst = _
      rw [List.map_append, map_fst_zipIdx, ih]
      show peerExpand rs.reverse ++ vs.map (k, ·) = peerExpand ((k, vs) :: rs).reverse
      rw [List.reverse_cons, peerExpand_reverse_append]

private theorem candW_perm : ∀ (runs : List (Int × List Nat)) (off : Nat),
    (candW runs off).Perm ((peerExpand runs).zipIdx off) := by
  intro runs
  induction runs with
  | nil => intro off; rfl
  | cons r rs ih =>
      obtain ⟨k, vs⟩ := r
      intro off
      show (candW rs (off + vs.length) ++ (vs.map (k, ·)).zipIdx off).Perm _
      have hpe : peerExpand ((k, vs) :: rs) = vs.map (k, ·) ++ peerExpand rs :=
        peerExpand_cons k vs rs
      rw [hpe, List.zipIdx_append, List.length_map]
      exact (List.perm_append_comm).trans
        (List.Perm.append (List.Perm.refl _) (ih (off + vs.length)))

private theorem mem_map_pair_fst {k : Int} {vs : List Nat} {x : Int × Nat}
    (h : x ∈ vs.map (k, ·)) : x.1 = k := by
  obtain ⟨v, -, hv⟩ := List.mem_map.mp h
  have hx : x = (k, v) := hv.symm
  rw [hx]

private theorem candW_key_mem : ∀ (runs : List (Int × List Nat)) (off : Nat)
    (p : Row × Nat), p ∈ candW runs off → p.1.1 ∈ runs.map Prod.fst := by
  intro runs
  induction runs with
  | nil => intro off p h; simp [candW] at h
  | cons r rs ih =>
      obtain ⟨k, vs⟩ := r
      intro off p h
      have h' : p ∈ candW rs (off + vs.length) ++ (vs.map (k, ·)).zipIdx off := h
      rw [List.mem_append] at h'
      rw [List.map_cons]
      rcases h' with h | h
      · have := ih (off + vs.length) p h
        exact List.mem_cons_of_mem _ this
      · have hfst : p.1 ∈ vs.map (k, ·) := by
          have hm := List.mem_zipIdx h
          obtain ⟨-, -, heq⟩ := hm
          rw [heq]
          apply List.getElem_mem
        have hkey : p.1.1 = k := mem_map_pair_fst hfst
        rw [hkey]
        exact List.mem_cons_self

private theorem block_pairwise (k : Int) : ∀ (vs : List Nat) (off : Nat),
    ((vs.map (k, ·)).zipIdx off).Pairwise
      (fun p q => List.zipIdxLE leKey p q = true) := by
  intro vs
  induction vs with
  | nil => intro off; simp
  | cons v vs ih =>
      intro off
      show (((v :: vs).map (k, ·)).zipIdx off).Pairwise _
      rw [List.map_cons, List.zipIdx_cons, List.pairwise_cons]
      refine ⟨?_, ih (off + 1)⟩
      intro q hq
      have hm := List.mem_zipIdx hq
      obtain ⟨hge, hlt, heq⟩ := hm
      have hq1 : q.1 ∈ vs.map (k, ·) := by
        rw [heq]
        apply List.getElem_mem
      have hq1k : q.1.1 = k := mem_map_pair_fst hq1
      have hq2 : off ≤ q.2 := by omega
      show List.zipIdxLE leKey ((k, v), off) q = true
      rw [List.zipIdxLE]
      have h1 : leKey (k, v) q.1 = true := by
        show decide ((k, v).1 ≤ q.1.1) = true
        rw [hq1k]
        show decide (k ≤ k) = true
        exact decide_eq_true (Int.le_refl k)
      have h2 : leKey q.1 (k, v) = true := by
        show decide (q.1.1 ≤ (k, v).1) = true
        rw [hq1k]
        show decide (k ≤ k) = true
        exact decide_eq_true (Int.le_refl k)
      rw [h1, h2]
      show decide (off ≤ q.2) = true
      exact decide_eq_true hq2

private theorem candW_pairwise : ∀ (runs : List (Int × List Nat)) (off : Nat),
    (runs.map Prod.fst).Pairwise (· > ·) →
    (candW runs off).Pairwise (fun p q => List.zipIdxLE leKey p q = true) := by
  intro runs
  induction runs with
  | nil => intro off _; simp [candW]
  | cons r rs ih =>
      obtain ⟨k, vs⟩ := r
      intro off h
      rw [List.map_cons, List.pairwise_cons] at h
      obtain ⟨hk, hrs⟩ := h
      show (candW rs (off + vs.length) ++ (vs.map (k, ·)).zipIdx off).Pairwise _
      rw [List.pairwise_append]
      refine ⟨?_, block_pairwise k vs off, ?_⟩
      · exact ih (off + vs.length) hrs
      · intro a ha b hb
        have hb1 : b.1 ∈ vs.map (k, ·) := by
          have hm := List.mem_zipIdx hb
          obtain ⟨-, -, heq⟩ := hm
          rw [heq]
          apply List.getElem_mem
        have hbk : b.1.1 = k := mem_map_pair_fst hb1
        have hak : a.1.1 ∈ rs.map Prod.fst := candW_key_mem rs (off + vs.length) a ha
        have hak' : a.1.1 < k := hk _ hak
        have hle1 : leKey a.1 b.1 = true := by
          show decide (a.1.1 ≤ b.1.1) = true
          rw [hbk]
          exact decide_eq_true (by omega : a.1.1 ≤ k)
        have hle2 : leKey b.1 a.1 = false := by
          show decide (b.1.1 ≤ a.1.1) = false
          rw [hbk]
          exact decide_eq_false (by omega : ¬ k ≤ a.1.1)
        show List.zipIdxLE leKey a b = true
        rw [List.zipIdxLE, hle1, hle2]
        rfl

-- leKey transitivity and totality for pairwise_mergeSort
private theorem leKey_trans (a b c : Row) :
    leKey a b = true → leKey b c = true → leKey a c = true := by
  intro h1 h2
  have h1' : a.1 ≤ b.1 := of_decide_eq_true h1
  have h2' : b.1 ≤ c.1 := of_decide_eq_true h2
  show decide (a.1 ≤ c.1) = true
  exact decide_eq_true (Int.le_trans h1' h2')

private theorem leKey_total (a b : Row) : (leKey a b || leKey b a) = true := by
  show (decide (a.1 ≤ b.1) || decide (b.1 ≤ a.1)) = true
  rw [Bool.or_eq_true]
  rcases Int.le_total a.1 b.1 with h | h
  · exact Or.inl (decide_eq_true h)
  · exact Or.inr (decide_eq_true h)

private theorem leKey_eq_true_iff_key {p q : Row × Nat} :
    leKey p.1 q.1 = true ↔ p.1.1 ≤ q.1.1 := by
  show decide (p.1.1 ≤ q.1.1) = true ↔ p.1.1 ≤ q.1.1
  exact decide_eq_true_iff

theorem sound_ReversePeerRuns : ReversePeerRuns.Goal := by
  intro runs h
  show peerExpand runs.reverse = (peerExpand runs).mergeSort leKey
  have hSZ : ((peerExpand runs).zipIdx.mergeSort (List.zipIdxLE leKey)).Perm ((peerExpand runs).zipIdx) :=
    List.mergeSort_perm _ _
  have hWZ : (candW runs 0).Perm ((peerExpand runs).zipIdx) := candW_perm runs 0
  have hSW : ((peerExpand runs).zipIdx.mergeSort (List.zipIdxLE leKey)).Perm (candW runs 0) :=
    hSZ.trans hWZ.symm
  have hSP : ((peerExpand runs).zipIdx.mergeSort (List.zipIdxLE leKey)).Pairwise
      (fun p q => List.zipIdxLE leKey p q = true) :=
    List.pairwise_mergeSort (fun a b c => List.zipIdxLE_trans leKey_trans a b c) (fun a b => List.zipIdxLE_total leKey_total a b) _
  have hWP : (candW runs 0).Pairwise (fun p q => List.zipIdxLE leKey p q = true) :=
    candW_pairwise runs 0 h
  have hEq : (peerExpand runs).zipIdx.mergeSort (List.zipIdxLE leKey) = candW runs 0 := by
    apply List.Perm.eq_of_pairwise (le := fun p q : Row × Nat => List.zipIdxLE leKey p q = true)
    · intro p q hp hq hpq hqp
      have hpZ : p ∈ (peerExpand runs).zipIdx := List.mem_mergeSort.mp hp
      have hqZ : q ∈ (peerExpand runs).zipIdx := (List.Perm.mem_iff hWZ).mp hq
      obtain ⟨hp1, hp2, hpe⟩ := List.mem_zipIdx hpZ
      obtain ⟨hq1, hq2, hqe⟩ := List.mem_zipIdx hqZ
      have hle1 : leKey p.1 q.1 = true := by
        by_cases hh : leKey p.1 q.1 = true
        · exact hh
        · have hfalse : leKey p.1 q.1 = false := by cases hb : leKey p.1 q.1 <;> simp_all
          have hz : List.zipIdxLE leKey p q = false := by
            show (if leKey p.1 q.1 then (if leKey q.1 p.1 then decide (p.2 ≤ q.2) else true) else false) = false
            rw [hfalse]
            rfl
          rw [hz] at hpq
          exact Bool.noConfusion hpq
      have hle2 : leKey q.1 p.1 = true := by
        by_cases hh : leKey q.1 p.1 = true
        · exact hh
        · have hfalse : leKey q.1 p.1 = false := by cases hb : leKey q.1 p.1 <;> simp_all
          have hz : List.zipIdxLE leKey q p = false := by
            show (if leKey q.1 p.1 then (if leKey p.1 q.1 then decide (q.2 ≤ p.2) else true) else false) = false
            rw [hfalse]
            rfl
          rw [hz] at hqp
          exact Bool.noConfusion hqp
      have hkey : p.1.1 = q.1.1 := by
        have h1 : p.1.1 ≤ q.1.1 := (leKey_eq_true_iff_key).mp hle1
        have h2 : q.1.1 ≤ p.1.1 := (leKey_eq_true_iff_key).mp hle2
        exact Int.le_antisymm h1 h2
      have hidx1 : p.2 ≤ q.2 := by
        rw [List.zipIdxLE] at hpq
        rw [hle1, hle2] at hpq
        simp only [Bool.cond_true, Bool.cond_false] at hpq
        exact of_decide_eq_true hpq
      have hidx2 : q.2 ≤ p.2 := by
        rw [List.zipIdxLE] at hqp
        rw [hle2, hle1] at hqp
        simp only [Bool.cond_true, Bool.cond_false] at hqp
        exact of_decide_eq_true hqp
      have hidx : p.2 = q.2 := Nat.le_antisymm hidx1 hidx2
      have hidx0 : p.snd - 0 = q.snd - 0 := by rw [hidx]
      have hpBound : p.snd - 0 < (peerExpand runs).length := by omega
      have hqBound : q.snd - 0 < (peerExpand runs).length := by omega
      have hpOpt : (peerExpand runs)[p.snd - 0]? = some p.fst :=
        (List.getElem?_eq_getElem hpBound).trans (congrArg some hpe.symm)
      have hqOpt : (peerExpand runs)[q.snd - 0]? = some q.fst :=
        (List.getElem?_eq_getElem hqBound).trans (congrArg some hqe.symm)
      have hcongr : (peerExpand runs)[p.snd - 0]? = (peerExpand runs)[q.snd - 0]? :=
        congrArg (fun i => (peerExpand runs)[i]?) hidx0
      have hsome : some p.fst = some q.fst := hpOpt.symm.trans (hcongr.trans hqOpt)
      have hrow : p.fst = q.fst := Option.some.inj hsome
      exact Prod.ext hrow hidx
    · exact hSP
    · exact hWP
    · exact hSW
  have hmain : (peerExpand runs).mergeSort leKey = peerExpand runs.reverse := by
    rw [← List.mergeSort_zipIdx]
    rw [hEq]
    exact candW_fst runs 0
  exact hmain.symm

/-! ### WeightedOffset helpers -/

private theorem sub_div (k b : Nat) (hb : 0 < b) (hbk : b ≤ k) :
    (k - b) / b = k / b - 1 := by
  have hdm := Nat.div_add_mod k b
  have hr : k % b < b := Nat.mod_lt k hb
  have hq : 1 ≤ k / b := (Nat.le_div_iff_mul_le hb).mpr (by simpa using hbk)
  apply Nat.div_eq_of_lt_le
  · have h1 : (k / b - 1) * b = k / b * b - b := by
      rw [Nat.sub_mul, Nat.one_mul]
    rw [h1]
    have hqb0 : b * (k / b) ≤ k := by omega
    have hqb : k / b * b ≤ k := by
      rw [Nat.mul_comm b (k / b)] at hqb0
      exact hqb0
    exact Nat.sub_le_sub_right hqb b
  · have h2 : k / b - 1 + 1 = k / b := Nat.sub_add_cancel hq
    rw [h2, Nat.sub_lt_iff_lt_add hbk, Nat.mul_comm (k / b) b]
    calc k = b * (k / b) + k % b := hdm.symm
      _ < b * (k / b) + b := by omega

private theorem sub_mod (k b : Nat) (hbk : b ≤ k) :
    (k - b) % b = k % b := by
  have := Nat.mod_eq_sub_mod hbk
  exact this.symm

private theorem flatMap_pair_getElem? (a b : List Nat) {k : Nat}
    (hk : k < a.length * b.length) :
    (a.flatMap fun x => b.map fun y => (x, y))[k]? =
      (do return (← a[k / b.length]?, ← b[k % b.length]?)) := by
  induction a generalizing k with
  | nil =>
      simp at hk
  | cons x xs ih =>
      have hbpos : 0 < b.length := by
        rcases Nat.eq_zero_or_pos b.length with h0 | hpos
        · rw [h0, Nat.mul_zero] at hk
          omega
        · exact hpos
      rw [List.flatMap_cons, List.getElem?_append, List.length_map]
      by_cases hk2 : k < b.length
      · rw [ite_eq_left hk2, List.getElem?_map]
        have hdiv0 : k / b.length = 0 := Nat.div_eq_of_lt hk2
        have hmod0 : k % b.length = k := Nat.mod_eq_of_lt hk2
        rw [hdiv0, hmod0, List.getElem?_cons, ite_eq_left rfl]
        cases hb : b[k]? <;> rfl
      · rw [ite_eq_right hk2]
        have hbk : b.length ≤ k := Nat.le_of_not_lt hk2
        have hkdiv : 1 ≤ k / b.length :=
          (Nat.le_div_iff_mul_le hbpos).mpr (by simpa using hbk)
        have hget : (x :: xs)[k / b.length]? = xs[(k - b.length) / b.length]? := by
          have hsub_div := sub_div k b.length hbpos hbk
          have hdiv_eq : k / b.length = (k - b.length) / b.length + 1 := by omega
          rw [hdiv_eq, List.getElem?_cons_succ]
        have hsub_mod : k % b.length = (k - b.length) % b.length :=
          (sub_mod k b.length hbk).symm
        rw [hget, hsub_mod]
        have hk' : k < xs.length * b.length + b.length := by
          have hlen : (x :: xs).length * b.length = xs.length * b.length + b.length := by
            rw [List.length_cons, Nat.add_mul, Nat.one_mul]
          rw [hlen] at hk
          exact hk
        have hk_sub : k - b.length < xs.length * b.length :=
          (Nat.sub_lt_iff_lt_add hbk).mpr hk'
        exact ih (k := k - b.length) hk_sub


private theorem flatMap_pair_length (a b : List Nat) :
    (a.flatMap fun x => b.map fun y => (x, y)).length = a.length * b.length := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      rw [List.flatMap_cons, List.length_append, ih, List.length_map, List.length_cons,
        Nat.add_mul, Nat.one_mul, Nat.add_comm]

theorem sound_WeightedOffset : WeightedOffset.Goal := by
  intro rs k
  induction rs generalizing k with
  | nil => rfl
  | cons ab rs ih =>
      cases ab with
      | mk a b =>
          simp only [seekPair, List.flatMap_cons, List.getElem?_append, flatMap_pair_length]
          by_cases hk : k < a.length * b.length
          · rw [ite_eq_left hk, ite_eq_left hk]
            exact (flatMap_pair_getElem? a b hk).symm
          · rw [ite_eq_right hk, ite_eq_right hk]
            exact ih (k - a.length * b.length)


def istep (g : Nat) (acc : List (Nat × Nat)) (x : Nat) : List (Nat × Nat) :=
  match acc with
  | [] => [(x, x)]
  | (l, r) :: rest => if x ≤ r + g + 1 then (l, x) :: rest else (x, x) :: acc

private theorem islands_eq_foldl (xs : List Nat) (g : Nat) :
    islands xs g = (List.foldl (istep g) [] xs).reverse := rfl

/-- Step preserves separation, validity, and `hi ≤ x` for all resulting islands. -/
private theorem istep_spec (g : Nat) (acc : List (Nat × Nat)) (x : Nat)
    (hsep : acc.Pairwise (fun a b => b.2 + g + 1 < a.1))
    (hvalid : ∀ p ∈ acc, p.1 ≤ p.2)
    (hx : ∀ p ∈ acc, p.2 ≤ x) :
    (istep g acc x).Pairwise (fun a b => b.2 + g + 1 < a.1) ∧
    (∀ p ∈ istep g acc x, p.1 ≤ p.2) ∧
    (∀ p ∈ istep g acc x, p.2 ≤ x) := by
  cases acc with
  | nil =>
      refine ⟨?_, ?_, ?_⟩
      · simp [istep]
      · intro p hp; simp [istep] at hp; rcases hp with rfl; omega
      · intro p hp; simp [istep] at hp; rcases hp with rfl; omega
  | cons lr rest =>
      obtain ⟨l, r⟩ := lr
      simp only [istep]
      by_cases hmerge : x ≤ r + g + 1
      · rw [ite_eq_left hmerge]
        obtain ⟨hhead, htail⟩ := List.pairwise_cons.mp hsep
        refine ⟨?_, ?_, ?_⟩
        · rw [List.pairwise_cons]
          refine ⟨?_, htail⟩
          intro b hb
          exact hhead b hb
        · intro p hp
          rcases List.mem_cons.mp hp with rfl | hpr
          · have hlr : l ≤ r := hvalid (l, r) (List.mem_cons_self)
            have hrx : r ≤ x := hx (l, r) (List.mem_cons_self)
            exact Nat.le_trans hlr hrx
          · exact hvalid p (List.mem_cons.mpr (Or.inr hpr))
        · intro p hp
          rcases List.mem_cons.mp hp with rfl | hpr
          · omega
          · exact hx p (List.mem_cons.mpr (Or.inr hpr))
      · rw [ite_eq_right hmerge]
        have hrx : r < x := by
          have : r + g + 1 < x := by omega
          omega
        have hlx : l < x := by
          have hlr : l ≤ r := hvalid (l, r) (List.mem_cons_self)
          exact Nat.lt_of_le_of_lt hlr hrx
        obtain ⟨hhead, htail⟩ := List.pairwise_cons.mp hsep
        refine ⟨?_, ?_, ?_⟩
        · rw [List.pairwise_cons]
          refine ⟨?_, hsep⟩
          intro b hb
          rcases List.mem_cons.mp hb with rfl | hbrest
          · show r + g + 1 < x
            omega
          · have hbl : b.2 + g + 1 < l := hhead b hbrest
            have hlex : l ≤ x := Nat.le_of_lt hlx
            exact Nat.lt_of_lt_of_le hbl hlex
        · intro p hp
          rcases List.mem_cons.mp hp with rfl | hpr
          · omega
          · rcases List.mem_cons.mp hpr with rfl | hprest
            · exact hvalid (l, r) (List.mem_cons_self)
            · exact hvalid p (List.mem_cons.mpr (Or.inr hprest))
        · intro p hp
          rcases List.mem_cons.mp hp with rfl | hpr
          · omega
          · exact hx p hpr

/-- The full fold preserves separation and validity. -/
private theorem foldl_sep (g : Nat) :
    ∀ (acc : List (Nat × Nat)) (xs : List Nat),
      acc.Pairwise (fun a b => b.2 + g + 1 < a.1) →
      (∀ p ∈ acc, p.1 ≤ p.2) →
      xs.Pairwise (· ≤ ·) →
      (∀ x' ∈ xs, ∀ p ∈ acc, p.2 ≤ x') →
      let res := List.foldl (istep g) acc xs
      res.Pairwise (fun a b => b.2 + g + 1 < a.1) ∧
      (∀ p ∈ res, p.1 ≤ p.2) := by
  intro acc xs
  induction xs generalizing acc with
  | nil =>
      intro hsep hvalid _ _
      rw [List.foldl_nil]
      exact ⟨hsep, hvalid⟩
  | cons y ys ih =>
      intro hsep hvalid hsorted hbnd
      rw [List.foldl_cons]
      have hspec := istep_spec g acc y hsep hvalid (fun p hp => hbnd y (List.mem_cons_self) p hp)
      have hysorted : ys.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hsorted).2
      have hy_le : ∀ z ∈ ys, y ≤ z := (List.pairwise_cons.mp hsorted).1
      have hbnd' : ∀ x' ∈ ys, ∀ p ∈ istep g acc y, p.2 ≤ x' := by
        intro x' hx' p hp
        have hpy : p.2 ≤ y := hspec.2.2 p hp
        exact Nat.le_trans hpy (hy_le x' hx')
      exact ih (istep g acc y) hspec.1 hspec.2.1 hysorted hbnd'

/-- An element covered by some island of `acc` stays covered after folding `ys`,
provided it is `≤` every element of `ys`. -/
private theorem cover_persist (g : Nat) :
    ∀ (acc : List (Nat × Nat)) (ys : List Nat) (y : Nat),
      (∃ p ∈ acc, p.1 ≤ y ∧ y ≤ p.2) →
      (∀ z ∈ ys, y ≤ z) →
      ∃ p ∈ List.foldl (istep g) acc ys, p.1 ≤ y ∧ y ≤ p.2 := by
  intro acc ys
  induction ys generalizing acc with
  | nil =>
      intro y hcov _
      rw [List.foldl_nil]
      exact hcov
  | cons z zs ih =>
      intro y hcov hyz
      rw [List.foldl_cons]
      have hyz' : ∀ w ∈ zs, y ≤ w := fun w hw => hyz w (List.mem_cons_of_mem z hw)
      -- one-step persistence: y covered in istep g acc z
      have hone : ∃ p ∈ istep g acc z, p.1 ≤ y ∧ y ≤ p.2 := by
        obtain ⟨p, hp, hply, hpy2⟩ := hcov
        have hzy : y ≤ z := hyz z (List.mem_cons_self)
        cases acc with
        | nil => simp [istep] at hp
        | cons lr rest =>
            obtain ⟨l, r⟩ := lr
            simp only [istep]
            by_cases hmerge : z ≤ r + g + 1
            · rw [ite_eq_left hmerge]
              rcases List.mem_cons.mp hp with rfl | hprest
              · -- p = (l,r): y ∈ [l,r]; new head (l,z) covers since l≤y≤z
                exact ⟨(l, z), List.mem_cons_self, hply, hzy⟩
              · -- p ∈ rest: unchanged
                exact ⟨p, List.mem_cons_of_mem _ hprest, hply, hpy2⟩
            · rw [ite_eq_right hmerge]
              -- result (z,z)::(l,r)::rest; p still in tail
              exact ⟨p, List.mem_cons_of_mem _ hp, hply, hpy2⟩
      exact ih (istep g acc z) y hone hyz'

/-- Every element of the folded list `xs` is covered by some island of the result. -/
private theorem cover_foldl (g : Nat) :
    ∀ (acc : List (Nat × Nat)) (xs : List Nat) (y : Nat),
      acc.Pairwise (fun a b => b.2 + g + 1 < a.1) →
      (∀ p ∈ acc, p.1 ≤ p.2) →
      xs.Pairwise (· ≤ ·) →
      (∀ x' ∈ xs, ∀ p ∈ acc, p.2 ≤ x') →
      y ∈ xs →
      ∃ p ∈ List.foldl (istep g) acc xs, p.1 ≤ y ∧ y ≤ p.2 := by
  intro acc xs
  induction xs generalizing acc with
  | nil => intro y _ _ _ _ hy; simp at hy
  | cons hd tl ih =>
      intro y hsep hvalid hsorted hbnd hymem
      rw [List.foldl_cons]
      have htl_sorted : tl.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hsorted).2
      have hhd_le : ∀ z ∈ tl, hd ≤ z := (List.pairwise_cons.mp hsorted).1
      have hspec := istep_spec g acc hd hsep hvalid
        (fun p hp => hbnd hd (List.mem_cons_self) p hp)
      have hbnd' : ∀ x' ∈ tl, ∀ p ∈ istep g acc hd, p.2 ≤ x' := by
        intro x' hx' p hp
        exact Nat.le_trans (hspec.2.2 p hp) (hhd_le x' hx')
      rcases List.mem_cons.mp hymem with heq | hy_tl
      · subst y
        -- y = hd: covered by istep g acc hd, persists through tl
        have hhd_cov : ∃ p ∈ istep g acc hd, p.1 ≤ hd ∧ hd ≤ p.2 := by
          cases acc with
          | nil =>
              exact ⟨(hd, hd), by simp [istep], Nat.le_refl _, Nat.le_refl _⟩
          | cons lr rest =>
              obtain ⟨l, r⟩ := lr
              simp only [istep]
              by_cases hmerge : hd ≤ r + g + 1
              · rw [ite_eq_left hmerge]
                have hlr : l ≤ r := hvalid (l, r) (List.mem_cons_self)
                have hry : r ≤ hd := hbnd hd (List.mem_cons_self) (l, r) (List.mem_cons_self)
                exact ⟨(l, hd), List.mem_cons_self, Nat.le_trans hlr hry, Nat.le_refl _⟩
              · rw [ite_eq_right hmerge]
                exact ⟨(hd, hd), by simp, Nat.le_refl _, Nat.le_refl _⟩
        exact cover_persist g (istep g acc hd) tl hd hhd_cov hhd_le
      · exact ih (istep g acc hd) y hspec.1 hspec.2.1 htl_sorted hbnd' hy_tl

/-- The `mergeSort` of `keys` is sorted. -/
private theorem mergeSort_sorted (keys : List Nat) :
    keys.mergeSort.Pairwise (· ≤ ·) := by
  have htrans : ∀ (a b c : Nat), decide (a ≤ b) = true → decide (b ≤ c) = true →
      decide (a ≤ c) = true := by
    intro a b c hab hbc
    have ha : a ≤ b := of_decide_eq_true hab
    have hb : b ≤ c := of_decide_eq_true hbc
    exact decide_eq_true_eq.mpr (Nat.le_trans ha hb)
  have htotal : ∀ (a b : Nat), (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    have := Nat.le_total a b
    cases this with
    | inl h => simp [h]
    | inr h => simp [h]
  have h := List.pairwise_mergeSort htrans htotal keys
  exact List.Pairwise.imp (fun {a b} hh => of_decide_eq_true hh) h

/-- Spans (= `islands keys.mergeSort g`) are pairwise separated. -/
private theorem spans_pairwise (keys : List Nat) (g : Nat) :
    (islands keys.mergeSort g).Pairwise (fun a b => a.2 + g + 1 < b.1) := by
  rw [islands_eq_foldl]
  have hspec := foldl_sep g [] keys.mergeSort
    (by simp) (by simp) (mergeSort_sorted keys) (by simp)
  exact (List.pairwise_reverse).mpr hspec.1

/-- Bool "value `x` lies in span `s`'s `[lo,hi]` range". -/
private def covB (x : Nat) (s : Nat × Nat) : Bool :=
  decide (s.1 ≤ x) && decide (x ≤ s.2)

private theorem covB_eq_true {x : Nat} {s : Nat × Nat} (h : s.1 ≤ x ∧ x ≤ s.2) :
    covB x s = true := by
  unfold covB
  rw [Bool.and_eq_true]
  exact ⟨by rw [decide_eq_true_eq]; exact h.1, by rw [decide_eq_true_eq]; exact h.2⟩

/-- Exactly one span covers a key value `x`. -/
private theorem covering_filter_length (keys : List Nat) (g : Nat) (x : Nat)
    (hx : keys.contains x = true) :
    ((islands keys.mergeSort g).filter (covB x)).length = 1 := by
  have hxmem : x ∈ keys := (List.contains_iff).mp hx
  have hxms : x ∈ keys.mergeSort := (List.mem_mergeSort).mpr hxmem
  -- lower bound: coverage gives a covering span
  have hcov := cover_foldl g [] keys.mergeSort x
    (by simp) (by simp) (mergeSort_sorted keys) (by simp) hxms
  obtain ⟨p, hp, hp1, hp2⟩ := hcov
  have hp_spans : p ∈ islands keys.mergeSort g := by
    rw [islands_eq_foldl]
    exact (List.mem_reverse).mpr hp
  have hpcov : covB x p = true := covB_eq_true ⟨hp1, hp2⟩
  have hpF : p ∈ (islands keys.mergeSort g).filter (covB x) :=
    (List.mem_filter).mpr ⟨hp_spans, hpcov⟩
  have hge : 1 ≤ ((islands keys.mergeSort g).filter (covB x)).length := by
    have : ((islands keys.mergeSort g).filter (covB x)) ≠ [] :=
      fun h => by rw [h] at hpF; simp at hpF
    have h0 : 0 < ((islands keys.mergeSort g).filter (covB x)).length :=
      List.length_pos_iff.mpr this
    omega
  -- upper bound: separation forbids two covering spans
  have hle : ((islands keys.mergeSort g).filter (covB x)).length ≤ 1 := by
    by_cases h : ((islands keys.mergeSort g).filter (covB x)).length ≤ 1
    · exact h
    · have hgt : 2 ≤ ((islands keys.mergeSort g).filter (covB x)).length := by omega
      have h01 : (0 : Nat) < ((islands keys.mergeSort g).filter (covB x)).length := by omega
      have h11 : (1 : Nat) < ((islands keys.mergeSort g).filter (covB x)).length := by omega
      have h0mem : ((islands keys.mergeSort g).filter (covB x))[0]'h01 ∈
          (islands keys.mergeSort g).filter (covB x) := List.getElem_mem h01
      have h1mem : ((islands keys.mergeSort g).filter (covB x))[1]'h11 ∈
          (islands keys.mergeSort g).filter (covB x) := List.getElem_mem h11
      have hR := (List.pairwise_iff_getElem.mp
        (List.Pairwise.filter _ (spans_pairwise keys g))) 0 1 h01 h11 (by omega)
      have hc0 : covB x (((islands keys.mergeSort g).filter (covB x))[0]'h01) = true :=
        ((List.mem_filter).mp h0mem).2
      have hc1 : covB x (((islands keys.mergeSort g).filter (covB x))[1]'h11) = true :=
        ((List.mem_filter).mp h1mem).2
      have hb0 : (((islands keys.mergeSort g).filter (covB x))[0]'h01).1 ≤ x ∧
          x ≤ (((islands keys.mergeSort g).filter (covB x))[0]'h01).2 := by
        unfold covB at hc0; rw [Bool.and_eq_true] at hc0
        exact ⟨of_decide_eq_true hc0.1, of_decide_eq_true hc0.2⟩
      have hb1 : (((islands keys.mergeSort g).filter (covB x))[1]'h11).1 ≤ x ∧
          x ≤ (((islands keys.mergeSort g).filter (covB x))[1]'h11).2 := by
        unfold covB at hc1; rw [Bool.and_eq_true] at hc1
        exact ⟨of_decide_eq_true hc1.1, of_decide_eq_true hc1.2⟩
      omega
  omega

/-- Per-span count of value `x` in the residual row filter, when `x` is a key. -/
private theorem count_span_key (keys : List Nat) (rows : List Nat) (x : Nat)
    (s : Nat × Nat) (hx : keys.contains x = true) :
    List.count x (rows.filter fun v => s.1 ≤ v && v ≤ s.2 && keys.contains v)
      = if covB x s = true then List.count x rows else 0 := by
  have hpx : (fun v => s.1 ≤ v && v ≤ s.2 && keys.contains v) x = covB x s := by
    simp only [hx, Bool.and_true, covB]
  by_cases hc : covB x s = true
  · rw [if_pos hc]
    have hpxtrue : (fun v => s.1 ≤ v && v ≤ s.2 && keys.contains v) x = true :=
      hpx.trans hc
    exact List.count_filter hpxtrue
  · rw [if_neg hc]
    rw [List.count_eq_zero]
    intro hmemF
    have hmem := (List.mem_filter).mp hmemF
    have hcov : covB x s = true := hpx.symm.trans hmem.2
    exact hc hcov

/-- Sum of `if p a then c else 0` over a list equals `c` times the filtered length. -/
private theorem sum_map_ite {α : Type} (l : List α) (p : α → Bool) (c : Nat) :
    (l.map fun a => if p a = true then c else 0).sum = c * (l.filter p).length := by
  induction l with
  | nil => simp
  | cons a l ih =>
    rw [List.map_cons, List.sum_cons, ih]
    by_cases hp : p a = true
    · rw [List.filter_cons]
      simp only [hp, if_true, List.length_cons]
      rw [Nat.mul_succ]
      omega
    · rw [List.filter_cons]
      simp only [hp, if_false, List.length_cons]
      simp

/-- Sum of a constant-zero mapped list is zero. -/
private theorem sum_map_zero {α : Type} (l : List α) :
    (l.map fun _ => (0 : Nat)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => rw [List.map_cons, List.sum_cons, ih]

/-- Count of `x` in `rows.filter keys.contains` when `x` is a key. -/
private theorem count_rows_key (keys rows : List Nat) (x : Nat)
    (hx : keys.contains x = true) :
    List.count x (rows.filter keys.contains) = List.count x rows := by
  exact List.count_filter hx

/-- Count of `x` in `rows.filter keys.contains` when `x` is not a key. -/
private theorem count_rows_nonkey (keys rows : List Nat) (x : Nat)
    (hx : keys.contains x = false) :
    List.count x (rows.filter keys.contains) = 0 := by
  rw [List.count_eq_zero]
  intro hmemF
  have hmem := (List.mem_filter).mp hmemF
  have hcx : keys.contains x = true := hmem.2
  rw [hx] at hcx
  exact Bool.noConfusion hcx

/-- Count of `x` in a single span's row filter when `x` is not a key: always zero. -/
private theorem count_span_zero (keys : List Nat) (rows : List Nat) (x : Nat)
    (s : Nat × Nat) (hx : keys.contains x = false) :
    List.count x (rows.filter fun v => s.1 ≤ v && v ≤ s.2 && keys.contains v) = 0 := by
  rw [List.count_eq_zero]
  intro hmemF
  have hmem := (List.mem_filter).mp hmemF
  have hpxf : (fun v => s.1 ≤ v && v ≤ s.2 && keys.contains v) x = false := by
    simp only [hx, Bool.and_false]
  have hbad : false = true := hpxf.symm.trans hmem.2
  exact Bool.noConfusion hbad

/-- Count of `x` in the span-expanded output when `x` is a key. -/
private theorem count_lhs_key (keys rows : List Nat) (g : Nat) (x : Nat)
    (hx : keys.contains x = true) :
    List.count x ((islands keys.mergeSort g).flatMap
      fun (lo,hi) => rows.filter fun v => lo ≤ v && v ≤ hi && keys.contains v)
      = List.count x rows := by
  rw [List.count_flatMap]
  have hcongr : (islands keys.mergeSort g).map (List.count x ∘
      fun (lo,hi) => rows.filter fun v => lo ≤ v && v ≤ hi && keys.contains v)
      = (islands keys.mergeSort g).map
        fun s => if covB x s = true then List.count x rows else 0 := by
    apply List.map_congr_left
    intro s _
    exact count_span_key keys rows x s hx
  rw [hcongr, sum_map_ite, covering_filter_length keys g x hx]
  simp

/-- Count of `x` in the span-expanded output when `x` is not a key: always zero. -/
private theorem count_lhs_nonkey (keys rows : List Nat) (g : Nat) (x : Nat)
    (hx : keys.contains x = false) :
    List.count x ((islands keys.mergeSort g).flatMap
      fun (lo,hi) => rows.filter fun v => lo ≤ v && v ≤ hi && keys.contains v)
      = 0 := by
  rw [List.count_flatMap]
  have hcongr : (islands keys.mergeSort g).map (List.count x ∘
      fun (lo,hi) => rows.filter fun v => lo ≤ v && v ≤ hi && keys.contains v)
      = (islands keys.mergeSort g).map fun _ => (0 : Nat) := by
    apply List.map_congr_left
    intro s _
    exact count_span_zero keys rows x s hx
  rw [hcongr, sum_map_zero]

/-- The span-expanded output is a permutation of the key-filtered rows. -/
private theorem range_perm (keys rows : List Nat) (g : Nat) :
    ((islands keys.mergeSort g).flatMap
      fun (lo,hi) => rows.filter fun v => lo ≤ v && v ≤ hi && keys.contains v).Perm
      (rows.filter keys.contains) := by
  rw [List.perm_iff_count]
  intro a
  by_cases h : keys.contains a = true
  · rw [count_lhs_key keys rows g a h, count_rows_key keys rows a h]
  · have hf : keys.contains a = false := Bool.eq_false_of_ne_true h
    rw [count_lhs_nonkey keys rows g a hf, count_rows_nonkey keys rows a hf]

/-- Range coalescing: span-expansion is a permutation of the key-filtered rows,
and the resulting spans are pairwise `g`-separated. -/
theorem sound_RangeCoalescing : RangeCoalescing.Goal := by
  intro keys g rows
  dsimp only
  exact ⟨range_perm keys rows g, spans_pairwise keys g⟩

private theorem expand_cons (k : Nat) (n : Nat) (xs : Rle) :
    expand ((k,n)::xs) = List.replicate n k ++ expand xs := by
  simp only [expand, List.flatMap_cons]

private theorem count_expand_cons (k x : Nat) (n : Nat) (xs : Rle) :
    (expand ((x,n)::xs)).count k = (if x == k then n else 0) + (expand xs).count k := by
  rw [expand_cons, List.count_append, List.count_replicate]

private theorem count_expand_eq_zero {l : Rle} {k : Nat}
    (h : ∀ e ∈ l, e.1 ≠ k) : (expand l).count k = 0 := by
  rw [List.count_eq_zero]
  intro hmem
  have hmem' : k ∈ l.flatMap fun (a,b) => List.replicate b a := hmem
  rw [List.mem_flatMap] at hmem'
  obtain ⟨e, he, hke⟩ := hmem'
  rw [List.mem_replicate] at hke
  obtain ⟨_, hkeq⟩ := hke
  have h1 : e.1 = k := hkeq.symm
  exact h e he h1

private theorem key_ne_of_lt {x y : Nat} (h : x < y) : y ≠ x := fun heq => by omega

private theorem pairwise_cons_head {x n : Nat} {xs : Rle}
    (hp : (((x,n)::xs).map Prod.fst).Pairwise (· < ·)) :
    (∀ e ∈ xs, x < e.1) ∧ (xs.map Prod.fst).Pairwise (· < ·) := by
  have hmap : ((x,n)::xs).map Prod.fst = x :: xs.map Prod.fst := by
    rw [List.map_cons]
  rw [hmap] at hp
  obtain ⟨h1, h2⟩ := List.pairwise_cons.mp hp
  refine ⟨?_, h2⟩
  intro e he
  have hmem : e.1 ∈ xs.map Prod.fst := List.mem_map.mpr ⟨e, he, rfl⟩
  exact h1 _ hmem

private theorem sorted_all_gt {h n : Nat} {t : Rle} {x : Nat}
    (hp : (((h,n)::t).map Prod.fst).Pairwise (· < ·)) (xh : x < h) :
    ∀ e ∈ (h,n)::t, x < e.1 := by
  have hmap : ((h,n)::t).map Prod.fst = h :: t.map Prod.fst := by
    rw [List.map_cons]
  rw [hmap] at hp
  obtain ⟨h1, -⟩ := List.pairwise_cons.mp hp
  intro e he
  rw [List.mem_cons] at he
  cases he with
  | inl heq =>
      subst heq
      exact xh
  | inr het =>
      have hmem : e.1 ∈ t.map Prod.fst := List.mem_map.mpr ⟨e, het, rfl⟩
      exact Nat.lt_trans xh (h1 _ hmem)

-- count of head key in the tail expansion is zero (tail keys all exceed it)

/-- Dropping the left head (smaller key `x < y`) preserves the min-count. -/
private theorem min_count_cons_left {x n y m : Nat} {xs ys : Rle} {k : Nat}
    (ha : (((x,n)::xs).map Prod.fst).Pairwise (· < ·))
    (hb : (((y,m)::ys).map Prod.fst).Pairwise (· < ·))
    (hxy : x < y) :
    min ((expand ((x,n)::xs)).count k) ((expand ((y,m)::ys)).count k)
      = min ((expand xs).count k) ((expand ((y,m)::ys)).count k) := by
  rw [count_expand_cons]
  by_cases hk : (x == k) = true
  · have hkx : x = k := (beq_iff_eq).mp hk
    have hxs0 : (expand xs).count k = 0 := by
      have hne : ∀ e ∈ xs, e.1 ≠ k := by
        intro e he
        have hlt : x < e.1 := (pairwise_cons_head ha).1 e he
        rw [← hkx]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    have hb0 : (expand ((y,m)::ys)).count k = 0 := by
      have hne : ∀ e ∈ (y,m)::ys, e.1 ≠ k := by
        intro e he
        have hlt : x < e.1 := sorted_all_gt hb hxy e he
        rw [← hkx]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    simp [hk, hxs0, hb0]
  · simp [hk]

/-- Dropping the right head (smaller key `y < x`) preserves the min-count. -/
private theorem min_count_cons_right {x n y m : Nat} {xs ys : Rle} {k : Nat}
    (ha : (((x,n)::xs).map Prod.fst).Pairwise (· < ·))
    (hb : (((y,m)::ys).map Prod.fst).Pairwise (· < ·))
    (hyx : y < x) :
    min ((expand ((x,n)::xs)).count k) ((expand ((y,m)::ys)).count k)
      = min ((expand ((x,n)::xs)).count k) ((expand ys).count k) := by
  rw [count_expand_cons k y m ys]
  by_cases hk : (y == k) = true
  · have hky : y = k := (beq_iff_eq).mp hk
    have hys0 : (expand ys).count k = 0 := by
      have hne : ∀ e ∈ ys, e.1 ≠ k := by
        intro e he
        have hlt : y < e.1 := (pairwise_cons_head hb).1 e he
        rw [← hky]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    have ha0 : (expand ((x,n)::xs)).count k = 0 := by
      have hne : ∀ e ∈ (x,n)::xs, e.1 ≠ k := by
        intro e he
        have hlt : y < e.1 := sorted_all_gt ha hyx e he
        rw [← hky]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    simp [hk, hys0, ha0]
  · simp [hk]

/-- Merging equal heads `(x,min n m)` preserves the min-count. -/
private theorem min_count_cons_eq {x n m : Nat} {xs ys : Rle} {k : Nat}
    (ha : (((x,n)::xs).map Prod.fst).Pairwise (· < ·))
    (hb : (((x,m)::ys).map Prod.fst).Pairwise (· < ·)) :
    (if x == k then min n m else 0) + min ((expand xs).count k) ((expand ys).count k)
      = min ((expand ((x,n)::xs)).count k) ((expand ((x,m)::ys)).count k) := by
  rw [count_expand_cons k x n xs, count_expand_cons k x m ys]
  by_cases hk : (x == k) = true
  · have hkx : x = k := (beq_iff_eq).mp hk
    have hxs0 : (expand xs).count k = 0 := by
      have hne : ∀ e ∈ xs, e.1 ≠ k := by
        intro e he
        have hlt : x < e.1 := (pairwise_cons_head ha).1 e he
        rw [← hkx]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    have hys0 : (expand ys).count k = 0 := by
      have hne : ∀ e ∈ ys, e.1 ≠ k := by
        intro e he
        have hlt : x < e.1 := (pairwise_cons_head hb).1 e he
        rw [← hkx]
        exact key_ne_of_lt hlt
      exact count_expand_eq_zero hne
    simp [hk, hxs0, hys0]
  · simp [hk]

private theorem intersect_count_aux : ∀ (fuel : Nat) (a b : Rle),
    a.length + b.length ≤ fuel →
    (a.map Prod.fst).Pairwise (· < ·) → (b.map Prod.fst).Pairwise (· < ·) →
    ∀ k, (expand (intersectRuns fuel a b)).count k =
      min ((expand a).count k) ((expand b).count k) := by
  intro fuel
  induction fuel with
  | zero =>
    intro a b hsum ha hb k
    have ha0 : a.length = 0 := by omega
    have hb0 : b.length = 0 := by omega
    rw [List.eq_nil_of_length_eq_zero ha0, List.eq_nil_of_length_eq_zero hb0]
    simp [intersectRuns, expand]
  | succ fuel ih =>
    intro a b hsum ha hb k
    cases a with
    | nil =>
      have hres : intersectRuns (fuel+1) [] b = [] := by simp [intersectRuns]
      rw [hres]
      simp [expand]
    | cons hnx xs =>
      obtain ⟨x, n⟩ := hnx
      cases b with
      | nil =>
        have hres : intersectRuns (fuel+1) ((x,n)::xs) [] = [] := by simp [intersectRuns]
        rw [hres]
        simp [expand]
      | cons hmy ys =>
        obtain ⟨y, m⟩ := hmy
        have htri : x < y ∨ x = y ∨ y < x := by omega
        obtain hxy | hxeq | hyx := htri
        · have hres : intersectRuns (fuel+1) ((x,n)::xs) ((y,m)::ys)
              = intersectRuns fuel xs ((y,m)::ys) := by
            simp only [intersectRuns]
            rw [if_pos hxy]
          rw [hres]
          have hsum' : xs.length + ((y,m)::ys).length ≤ fuel := by
            simp only [List.length_cons] at hsum ⊢
            omega
          have hpa := (pairwise_cons_head ha).2
          have ih' := ih xs ((y,m)::ys) hsum' hpa hb k
          rw [ih']
          exact (min_count_cons_left ha hb hxy).symm
        · subst y
          have hres : intersectRuns (fuel+1) ((x,n)::xs) ((x,m)::ys)
              = (x, min n m) :: intersectRuns fuel xs ys := by
            simp only [intersectRuns]
            have hxx : ¬ x < x := by omega
            rw [if_neg hxx, if_neg hxx]
          rw [hres, expand_cons, List.count_append, List.count_replicate]
          have hsum' : xs.length + ys.length ≤ fuel := by
            simp only [List.length_cons] at hsum ⊢
            omega
          have hpa := (pairwise_cons_head ha).2
          have hpb := (pairwise_cons_head hb).2
          have ih' := ih xs ys hsum' hpa hpb k
          rw [ih']
          exact min_count_cons_eq ha hb
        · have hxy' : ¬ x < y := by omega
          have hres : intersectRuns (fuel+1) ((x,n)::xs) ((y,m)::ys)
              = intersectRuns fuel ((x,n)::xs) ys := by
            simp only [intersectRuns]
            rw [if_neg hxy', if_pos hyx]
          rw [hres]
          have hsum' : ((x,n)::xs).length + ys.length ≤ fuel := by
            simp only [List.length_cons] at hsum ⊢
            omega
          have hpb := (pairwise_cons_head hb).2
          have ih' := ih ((x,n)::xs) ys hsum' ha hpb k
          rw [ih']
          exact (min_count_cons_right ha hb hyx).symm

theorem sound_RunIntersection : RunIntersection.Goal := by
  intro a b ha hb k
  rw [intersect_count_aux (a.length + b.length) a b (Nat.le_refl _)
    ha hb k]

end TinylambOpt.ResearchProofs.ExecutionStrategies.OrderedStreams
