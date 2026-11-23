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

end TinylambOpt.ResearchProofs.ExecutionStrategies.OrderedStreams

/-! ### WeightedOffset helpers -/

/-- Length of the Cartesian-product expansion of one run pair. -/
private theorem flatMap_pair_length (a b : List Nat) :
    (a.flatMap fun x => b.map fun y => (x, y)).length = a.length * b.length := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      rw [List.flatMap_cons, List.length_append, ih, List.length_map, List.length_cons,
        Nat.add_mul, Nat.one_mul, Nat.add_comm]

/-- Indexing the Cartesian-product expansion of a single run pair returns the
`k / b.length`-th left and `k % b.length`-th right element. -/
private theorem flatMap_pair_getElem? (a b : List Nat) {k : Nat}
    (hk : k < a.length * b.length) :
    (a.flatMap fun x => b.map fun y => (x, y))[k]? =
      (do return (← a[k / b.length]?, ← b[k % b.length]?)) := by
  induction a generalizing k with
  | nil => simp at hk
  | cons x xs ih =>
      have hlen : (x :: xs).length = xs.length + 1 := List.length_cons
      rw [List.flatMap_cons, List.getElem?_append, List.length_map]
      by_cases hk2 : k < b.length
      · rw [ite_eq_left hk2, List.getElem?_map]
        have hdiv : k / b.length = 0 := Nat.div_eq_of_lt hk2
        have hmod : k % b.length = k := Nat.mod_eq_of_lt hk2
        rw [hdiv, List.getElem?_cons, ite_eq_left rfl, hmod]
        -- LHS: Option.map (fun y => (x,y)) (b[k]?)
        -- RHS: do return (← some x, ← b[k]?)
        sorry
      · rw [ite_eq_right hk2]
        sorry

theorem sound_WeightedOffset : WeightedOffset.Goal := by
  intro rs k
  induction rs generalizing k with
  | nil => rfl
  | cons ab rs ih =>
      cases ab with
      | mk a b =>
          rw [List.flatMap_cons, List.getElem?_append, flatMap_pair_length]
          by_cases hk : k < a.length * b.length
          · rw [ite_eq_left hk, ite_eq_left hk]
            exact (flatMap_pair_getElem? a b hk).symm
          · rw [ite_eq_right hk, ite_eq_right hk]
            exact ih (k - a.length * b.length)
