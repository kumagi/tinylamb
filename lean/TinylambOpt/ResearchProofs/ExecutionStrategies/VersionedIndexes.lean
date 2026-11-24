import TinylambOpt.ResearchGoals.ExecutionStrategies.VersionedIndexes

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.VersionedIndexes

open TinylambOpt.ResearchGoals.VersionedIndexes
open List

/-!
# Versioned-index execution strategies

Proofs for the eight research propositions in
`TinylambOpt.ResearchGoals.VersionedIndexes`: snapshot anchors, sparse pins,
range tombstones, covered predicates, batched lookup, dominated runs,
bottom tombstones and overlay aggregates over immutable, committed
versioned entries.
-/

/-! ### Batched lookup -/

/-- Folding `insert k (valueAt xs k t)` over `L` leaves `e`'s slot untouched
when `e ∉ L`. -/
private theorem probe_foldl_getD_of_not_mem (xs : List Entry) (t : Nat)
    (m : Std.HashMap Nat (Option Int)) (e : Nat) :
    ∀ (L : List Nat), e ∉ L →
      (L.foldl (fun m k => m.insert k (valueAt xs k t)) m).getD e none
        = m.getD e none := by
  intro L
  induction L generalizing m with
  | nil => intro _; rfl
  | cons g L ih =>
      intro hne
      rw [List.foldl_cons]
      rw [ih _ (fun hg => hne (List.mem_cons_of_mem g hg))]
      rw [Std.HashMap.getD_insert]
      have hge : (g == e) = false := by
        rw [beq_eq_false_iff_ne]
        exact fun h => hne (h ▸ List.mem_cons_self)
      rw [hge]
      simp

/-- After folding the per-key probe inserts over `L`, a key `k ∈ L` reads back
`valueAt xs k t`. -/
private theorem probe_foldl_getD_of_mem (xs : List Entry) (t : Nat)
    (m : Std.HashMap Nat (Option Int)) :
    ∀ (L : List Nat) (k : Nat), k ∈ L →
      (L.foldl (fun m k => m.insert k (valueAt xs k t)) m).getD k none
        = valueAt xs k t := by
  intro L
  induction L generalizing m with
  | nil => intro k hk; simp at hk
  | cons e L ih =>
      intro k hk
      rw [List.mem_cons] at hk
      rw [List.foldl_cons]
      rcases hk with hke | hkL
      · rw [hke]
        by_cases hel : e ∈ L
        · rw [ih _ _ hel]
        · rw [probe_foldl_getD_of_not_mem xs t (m.insert e (valueAt xs e t)) e L hel,
            Std.HashMap.getD_insert_self]
      · rw [ih _ _ hkL]

theorem sound_BatchedLookup : BatchedLookup.Goal := by
  intro xs ks t
  unfold probeBatch
  apply List.map_congr_left
  intro k hk
  exact probe_foldl_getD_of_mem xs t {} ks.eraseDups k
    ((List.mem_eraseDups).mpr hk)

/-! ### Overlay aggregate -/

/-- `(l.filterMap f).sum` equals summing `(f x).getD 0` over `l`. -/
private theorem sum_filterMap_getD {α : Type} (l : List α) (f : α → Option Int) :
    (l.filterMap f).sum = (l.map fun x => (f x).getD 0).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hx : f x with
      | none => simp [hx, ih, List.sum_cons]
      | some v => simp [hx, ih, List.sum_cons]

/-- The visible entry of `a ++ b` is `a`'s visible entry when present,
otherwise `b`'s. -/
private theorem visible_append (a b : List Entry) (k t : Nat) :
    visible (a ++ b) k t = (visible a k t).or (visible b k t) := by
  unfold visible
  rw [List.find?_append]

/-- `valueAt` on the merged run resolves to `a`'s version when visible,
else `b`'s. -/
private theorem valueAt_append (a b : List Entry) (k t : Nat) :
    valueAt (a ++ b) k t
      = if (visible a k t).isSome then valueAt a k t else valueAt b k t := by
  unfold valueAt
  rw [visible_append]
  cases h : visible a k t with
  | none => simp
  | some e => simp

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun x => f x + g x).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_map_ite_filter {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    (l.map fun x => if p x then f x else 0).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases hp : p x with
      | true => simp [hp, ih, List.sum_cons]
      | false => simp [hp, ih, List.sum_cons]

theorem sound_OverlayAggregate : OverlayAggregate.Goal := by
  intro a b ks t
  unfold total
  simp only [sum_filterMap_getD]
  -- pointwise relation between the three `valueAt` contributions
  have hpt : ∀ k : Nat,
      (valueAt (a ++ b) k t).getD 0
        = (valueAt a k t).getD 0
          + (if (visible a k t).isNone then (valueAt b k t).getD 0 else 0) := by
    intro k
    cases h : visible a k t with
    | none =>
        have hv : visible a k t = none := h
        have h3 : valueAt a k t = none := by
          unfold valueAt; rw [hv]; rfl
        rw [valueAt_append, h3]
        simp [hv]
    | some e =>
        have hv : visible a k t = some e := h
        rw [valueAt_append]
        simp [hv]
  have hmap : ks.map (fun k => (valueAt (a ++ b) k t).getD 0)
      = ks.map (fun k => (valueAt a k t).getD 0
          + (if (visible a k t).isNone then (valueAt b k t).getD 0 else 0)) :=
    List.map_congr_left (fun k _ => hpt k)
  rw [hmap, sum_map_add, sum_map_ite_filter]

/-! ### Snapshot anchor -/

/-- The fold step used by `compact`, factored to take the state pair. -/
private def cfold (f : Nat) (s : List Nat × List Entry) (e : Entry) :
    List Nat × List Entry :=
  if f < e.seq then (s.1, e :: s.2)
  else if s.1.contains e.key then s
  else (e.key :: s.1, e :: s.2)

private theorem cfold_new (f : Nat) (sn : List Nat) (out : List Entry) (e : Entry)
    (h : f < e.seq) : cfold f (sn, out) e = (sn, e :: out) := by
  unfold cfold; rw [if_pos h]

private theorem cfold_drop (f : Nat) (sn : List Nat) (out : List Entry) (e : Entry)
    (h1 : ¬ f < e.seq) (h2 : sn.contains e.key) : cfold f (sn, out) e = (sn, out) := by
  unfold cfold; rw [if_neg h1, if_pos h2]

private theorem cfold_keep (f : Nat) (sn : List Nat) (out : List Entry) (e : Entry)
    (h1 : ¬ f < e.seq) (h2 : ¬ sn.contains e.key) :
    cfold f (sn, out) e = (e.key :: sn, e :: out) := by
  unfold cfold; rw [if_neg h1, if_neg h2]

private def seenInv (f : Nat) (seen : List Nat) (out : List Entry) : Prop :=
  ∀ key ∈ seen, ∃ e ∈ out, e.key = key ∧ e.seq ≤ f

private theorem fold_find?_inv (xs : List Entry) (seen₀ : List Nat) (out₀ : List Entry)
    (k t f : Nat) (hf : f ≤ t) (hinv : seenInv f seen₀ out₀) :
    (((xs.foldl (fun s e => cfold f s e) (seen₀, out₀)).2).reverse).find?
        (fun e => e.key == k && e.seq ≤ t)
      = (out₀.reverse ++ xs).find? (fun e => e.key == k && e.seq ≤ t) := by
  revert hinv hf
  induction xs generalizing seen₀ out₀ with
  | nil =>
      intro hf hinv
      simp
  | cons e xs ih =>
      intro hf hinv
      rw [List.foldl_cons]
      by_cases h1 : f < e.seq
      · rw [cfold_new f seen₀ out₀ e h1]
        have hinv' : seenInv f seen₀ (e :: out₀) := by
          intro key hk
          obtain ⟨w, hw, hkw, hws⟩ := hinv key hk
          exact ⟨w, List.mem_cons_of_mem e hw, hkw, hws⟩
        rw [ih seen₀ (e :: out₀) hf hinv']
        have hlist : (e :: out₀).reverse ++ xs = out₀.reverse ++ e :: xs := by
          simp [List.reverse_cons, List.append_assoc]
        rw [hlist]
      · have hle : e.seq ≤ f := Nat.le_of_not_lt h1
        by_cases h2 : seen₀.contains e.key
        · rw [cfold_drop f seen₀ out₀ e h1 h2]
          rw [ih seen₀ out₀ hf hinv]
          have hmem : e.key ∈ seen₀ := by
            have h2' : seen₀.contains e.key = true := h2
            rwa [List.contains_iff_mem] at h2'
          obtain ⟨w, hw, hkw, hws⟩ := hinv e.key hmem
          have hw' : w ∈ out₀.reverse := List.mem_reverse.mpr hw
          have hwt : w.seq ≤ t := Nat.le_trans hws hf
          by_cases hP : (e.key == k && decide (e.seq ≤ t)) = true
          · have hek : e.key = k := by
              have h1' : (e.key == k) = true := (Bool.and_eq_true_iff.mp hP).1
              exact beq_iff_eq.mp h1'
            have hwk : w.key = k := hkw.trans hek
            have hwP : (w.key == k && decide (w.seq ≤ t)) = true := by
              have h3 : (w.key == k) = true := beq_iff_eq.mpr hwk
              have h4 : decide (w.seq ≤ t) = true := decide_eq_true hwt
              exact Bool.and_eq_true_iff.mpr ⟨h3, h4⟩
            have hsome : (out₀.reverse.find? (fun e => e.key == k && e.seq ≤ t)).isSome = true := by
              rw [List.find?_isSome]
              exact ⟨w, hw', hwP⟩
            rw [List.find?_append, List.find?_append,
              Option.or_eq_left_of_isSome hsome, Option.or_eq_left_of_isSome hsome]
          · have hP' : (e.key == k && decide (e.seq ≤ t)) = false := by
              cases hb : (e.key == k && decide (e.seq ≤ t)) with
              | true => rw [hb] at hP; simp at hP
              | false => rfl
            have hcons : (e :: xs).find? (fun e => e.key == k && e.seq ≤ t)
                = xs.find? (fun e => e.key == k && e.seq ≤ t) := by
              apply List.find?_cons_of_neg
              intro h
              rw [hP'] at h
              exact Bool.noConfusion h
            rw [List.find?_append, List.find?_append, hcons]
        · rw [cfold_keep f seen₀ out₀ e h1 h2]
          have hinv' : seenInv f (e.key :: seen₀) (e :: out₀) := by
            intro key hk
            rw [List.mem_cons] at hk
            rcases hk with rfl | hk
            · exact ⟨e, List.mem_cons_self, rfl, hle⟩
            · obtain ⟨w, hw, hkw, hws⟩ := hinv key hk
              exact ⟨w, List.mem_cons_of_mem e hw, hkw, hws⟩
          rw [ih (e.key :: seen₀) (e :: out₀) hf hinv']
          have hlist : (e :: out₀).reverse ++ xs = out₀.reverse ++ e :: xs := by
            simp [List.reverse_cons, List.append_assoc]
          rw [hlist]

theorem sound_SnapshotAnchor : SnapshotAnchor.Goal := by
  intro xs oldest k t _hord hord
  exact fold_find?_inv xs [] [] k t oldest hord (by simp [seenInv])

/-! ### Sparse pins -/

private theorem entry_beq_self (e : Entry) : (e == e) = true := by
  obtain ⟨k, s, v⟩ := e
  change instBEqEntry.beq _ _ = true
  unfold instBEqEntry.beq
  simp

/-- If the first `p`-match `e` of `xs` satisfies `q`, filtering by `q`
preserves the first `p`-match. -/
private theorem find?_filter_of_first_mem {α : Type} (xs : List α) (p q : α → Bool) {e : α}
    (h : xs.find? p = some e) (he : q e = true) :
    (xs.filter q).find? p = xs.find? p := by
  rw [List.find?_eq_some_iff_append] at h
  obtain ⟨hp, as, bs, hxs, has⟩ := h
  subst hxs
  have hR : (as ++ e :: bs).find? p = some e :=
    List.find?_eq_some_iff_append.mpr ⟨hp, as, bs, rfl, has⟩
  have hL : ((as ++ e :: bs).filter q).find? p = some e := by
    have hq : (as ++ e :: bs).filter q = as.filter q ++ e :: bs.filter q := by
      rw [List.filter_append, List.filter_cons_of_pos he]
    rw [hq]
    apply List.find?_eq_some_iff_append.mpr
    refine ⟨hp, as.filter q, bs.filter q, rfl, ?_⟩
    intro x hx
    rw [List.mem_filter] at hx
    have h2 := has x hx.1
    simpa using h2
  rw [hL, hR]

/-- `keepPinned`'s filter predicate, named for readability. -/
private def keepPred (xs : List Entry) (pins : List Nat) (latest : Nat) (e : Entry) : Bool :=
  latest < e.seq || (latest :: pins).any fun t => visible xs e.key t == some e

theorem sound_SparsePins : SparsePins.Goal := by
  intro xs pins latest k t hord hmem
  show ((xs.filter fun e => keepPred xs pins latest e).find?
        (fun e => e.key == k && e.seq ≤ t))
      = xs.find? (fun e => e.key == k && e.seq ≤ t)
  by_cases hfind : xs.find? (fun e => e.key == k && e.seq ≤ t) = none
  · have hnone : (xs.filter fun e => keepPred xs pins latest e).find?
        (fun e => e.key == k && e.seq ≤ t) = none := by
      unfold keepPred
      exact List.Sublist.find?_eq_none List.filter_sublist hfind
    rw [hnone, hfind]
  · obtain ⟨e, he⟩ := Option.ne_none_iff_exists'.mp hfind
    have heP : (e.key == k && decide (e.seq ≤ t)) = true := by
      have h := List.find?_some he
      simpa using h
    have hek : e.key = k := beq_iff_eq.mp (Bool.and_eq_true_iff.mp heP).1
    have hest : e.seq ≤ t := of_decide_eq_true (Bool.and_eq_true_iff.mp heP).2
    -- the winner is kept by keepPinned's filter
    have hkeep : keepPred xs pins latest e = true := by
      unfold keepPred
      rcases hmem with htp | hlt
      · -- t ∈ pins: pin t witnesses e
        have hany : ((latest :: pins).any fun s => visible xs e.key s == some e) = true := by
          rw [List.any_eq_true]
          refine ⟨t, List.mem_cons.mpr (Or.inr htp), ?_⟩
          rw [hek]
          have hvis : visible xs k t = some e := he
          rw [hvis]
          exact entry_beq_self e
        exact Bool.or_eq_true_iff.mpr (Or.inr hany)
      · by_cases hseq : latest < e.seq
        · -- newer than latest: first disjunct
          exact Bool.or_eq_true_iff.mpr (Or.inl (decide_eq_true hseq))
        · have helse : e.seq ≤ latest := Nat.le_of_not_lt hseq
          have hvis : visible xs k latest = some e := by
            unfold visible
            apply List.find?_eq_some_iff_append.mpr
            obtain ⟨_, as, bs, hxs, has⟩ := List.find?_eq_some_iff_append.mp he
            have hPle : (e.key == k && decide (e.seq ≤ latest)) = true := by
              have h1 : (e.key == k) = true := (Bool.and_eq_true_iff.mp heP).1
              have h2 : decide (e.seq ≤ latest) = true := decide_eq_true helse
              exact Bool.and_eq_true_iff.mpr ⟨h1, h2⟩
            refine ⟨hPle, as, bs, hxs, ?_⟩
            intro a ha
            by_cases hpl : (a.key == k && decide (a.seq ≤ latest)) = true
            · have hak : (a.key == k) = true := (Bool.and_eq_true_iff.mp hpl).1
              have has_ : a.seq ≤ latest := of_decide_eq_true (Bool.and_eq_true_iff.mp hpl).2
              have has_t : a.seq ≤ t := Nat.le_trans has_ hlt
              have hpt : (a.key == k && decide (a.seq ≤ t)) = true :=
                Bool.and_eq_true_iff.mpr ⟨hak, decide_eq_true has_t⟩
              have h4 := has a ha
              rw [hpt] at h4
              simp at h4
            · have hfa : (a.key == k && decide (a.seq ≤ latest)) = false :=
                Bool.eq_false_iff.mpr hpl
              rw [hfa]
              rfl
          have hany : ((latest :: pins).any fun s => visible xs e.key s == some e) = true := by
            rw [List.any_eq_true]
            refine ⟨latest, List.mem_cons_self, ?_⟩
            rw [hek, hvis]
            exact entry_beq_self e
          exact Bool.or_eq_true_iff.mpr (Or.inr hany)
    have hfil := find?_filter_of_first_mem xs
      (fun e => e.key == k && e.seq ≤ t) (fun e => keepPred xs pins latest e)
      he hkeep
    rw [hfil, he]

/-! ### Bottom tombstone -/

/-- The compaction fold emits a sublist of the processed prefix (reversed). -/
private theorem foldl_sublist (f : Nat) (xs : List Entry) :
    ∀ (sn : List Nat) (out : List Entry),
      (((xs.foldl (fun s e => cfold f s e) (sn, out)).2).reverse) <+ out.reverse ++ xs := by
  induction xs with
  | nil => intro sn out; simp
  | cons e xs ih =>
      intro sn out
      rw [List.foldl_cons]
      by_cases h1 : f < e.seq
      · rw [cfold_new f sn out e h1]
        have hrev : (e :: out).reverse ++ xs = out.reverse ++ e :: xs := by
          simp [List.reverse_cons, List.append_assoc]
        have h := ih sn (e :: out)
        rwa [hrev] at h
      · have hle : e.seq ≤ f := Nat.le_of_not_lt h1
        by_cases h2 : sn.contains e.key
        · rw [cfold_drop f sn out e h1 h2]
          have h := ih sn out
          refine h.trans ?_
          exact (List.sublist_cons_self e xs).append_left (out.reverse)
        · rw [cfold_keep f sn out e h1 h2]
          have hrev : (e :: out).reverse ++ xs = out.reverse ++ e :: xs := by
            simp [List.reverse_cons, List.append_assoc]
          have h := ih (e.key :: sn) (e :: out)
          rwa [hrev] at h

/-- `compact xs f` is a sublist of `xs`. -/
private theorem compact_sublist (xs : List Entry) (f : Nat) :
    compact xs f <+ xs := by
  have h := foldl_sublist f xs [] []
  have h2 : [].reverse ++ xs = xs := by simp
  rw [h2] at h
  show (((xs.foldl (fun (seen, out) e =>
      if f < e.seq then (seen, e :: out)
      else if seen.contains e.key then (seen, out)
      else (e.key :: seen, e :: out)) ([], [])).2).reverse) <+ xs
  exact h

/-- Invariant: keys of kept old entries are recorded in `seen`, and old kept
entries have pairwise-distinct keys. -/
private theorem foldl_pw_inv (f : Nat) (xs : List Entry) :
    ∀ (sn : List Nat) (out : List Entry),
      (∀ e ∈ out, e.seq ≤ f → e.key ∈ sn) →
      (out.filter fun e => e.seq ≤ f).Pairwise (fun a b => a.key ≠ b.key) →
      (∀ e ∈ (xs.foldl (fun s e => cfold f s e) (sn, out)).2,
          e.seq ≤ f → e.key ∈ (xs.foldl (fun s e => cfold f s e) (sn, out)).1) ∧
      (((xs.foldl (fun s e => cfold f s e) (sn, out)).2).filter fun e => e.seq ≤ f).Pairwise
        (fun a b => a.key ≠ b.key) := by
  induction xs with
  | nil => intro sn out h1 h2; exact ⟨h1, h2⟩
  | cons e xs ih =>
      intro sn out h1 h2
      rw [List.foldl_cons]
      by_cases hnew : f < e.seq
      · rw [cfold_new f sn out e hnew]
        refine ih sn (e :: out) ?_ ?_
        · intro e' he' hle'
          rw [List.mem_cons] at he'
          rcases he' with rfl | he''
          · exact absurd hle' (Nat.not_le_of_gt hnew)
          · exact h1 e' he'' hle'
        · have hneg : ¬ decide (e.seq ≤ f) = true := by
            intro hdec
            exact absurd (of_decide_eq_true hdec) (Nat.not_le_of_gt hnew)
          have hfilter : ((e :: out).filter fun e => e.seq ≤ f)
              = out.filter fun e => e.seq ≤ f := by
            apply List.filter_cons_of_neg
            exact hneg
          rw [hfilter]; exact h2
      · have hle : e.seq ≤ f := Nat.le_of_not_lt hnew
        by_cases h2' : sn.contains e.key
        · rw [cfold_drop f sn out e hnew h2']
          exact ih sn out h1 h2
        · rw [cfold_keep f sn out e hnew h2']
          refine ih (e.key :: sn) (e :: out) ?_ ?_
          · intro e' he' hle'
            rw [List.mem_cons] at he'
            rcases he' with rfl | he''
            · exact List.mem_cons_self
            · have hk := h1 e' he'' hle'
              exact List.mem_cons_of_mem e.key hk
          · have hpos : decide (e.seq ≤ f) = true := decide_eq_true hle
            have hfilter : ((e :: out).filter fun e => e.seq ≤ f)
                = e :: out.filter fun e => e.seq ≤ f :=
              List.filter_cons_of_pos hpos
            rw [hfilter, List.pairwise_cons]
            refine ⟨?_, h2⟩
            intro b hb
            rw [List.mem_filter] at hb
            have hbkey : b.key ∈ sn := h1 b hb.1 (of_decide_eq_true hb.2)
            have hnek : e.key ∉ sn := by
              intro hmem
              apply h2'
              rw [List.contains_iff_mem]
              exact hmem
            intro hbe
            apply hnek
            simpa [hbe] using hbkey

/-- Old (≤ f) kept entries of `compact xs f` have pairwise-distinct keys. -/
private theorem compact_old_pw (xs : List Entry) (f : Nat) :
    ((compact xs f).filter fun e => e.seq ≤ f).Pairwise (fun a b => a.key ≠ b.key) := by
  have h := foldl_pw_inv f xs [] [] (by simp) (by simp)
  have h2 := h.2
  show (((xs.foldl (fun (seen, out) e =>
      if f < e.seq then (seen, e :: out)
      else if seen.contains e.key then (seen, out)
      else (e.key :: seen, e :: out)) ([], [])).2).reverse.filter
        fun e => e.seq ≤ f).Pairwise (fun a b => a.key ≠ b.key)
  rw [List.filter_reverse]
  apply List.pairwise_reverse.mpr
  exact (List.Pairwise.iff (fun a b => ne_comm)).mpr h2

theorem sound_BottomTombstone : BottomTombstone.Goal := by
  intro xs oldest k t hord hge
  have hSA : visible (compact xs oldest) k t = visible xs k t :=
    sound_SnapshotAnchor xs oldest k t hord hge
  have hVC : valueAt (compact xs oldest) k t = valueAt xs k t := by
    unfold valueAt
    rw [hSA]
  rw [← hVC]
  unfold valueAt
  by_cases hfind : visible (compact xs oldest) k t = none
  · have hnone : visible ((compact xs oldest).filter
        fun e => oldest < e.seq || e.value.isSome) k t = none := by
      show ((compact xs oldest).filter fun e => oldest < e.seq || e.value.isSome).find?
        (fun e => e.key == k && e.seq ≤ t) = none
      exact List.Sublist.find?_eq_none List.filter_sublist hfind
    rw [hnone, hfind]
  · obtain ⟨w, hvis⟩ := Option.ne_none_iff_exists'.mp hfind
    by_cases hFw : oldest < w.seq || w.value.isSome
    · -- w survives the filter: the first match is preserved
      have hfil := find?_filter_of_first_mem (compact xs oldest)
        (fun e => e.key == k && e.seq ≤ t)
        (fun e => oldest < e.seq || e.value.isSome) hvis hFw
      have hEq : visible ((compact xs oldest).filter
            fun e => oldest < e.seq || e.value.isSome) k t
          = visible (compact xs oldest) k t := by
        show ((compact xs oldest).filter fun e => oldest < e.seq || e.value.isSome).find?
            (fun e => e.key == k && e.seq ≤ t)
          = (compact xs oldest).find? (fun e => e.key == k && e.seq ≤ t)
        exact hfil
      rw [hEq]
    · -- w is an old tombstone anchor that gets dropped
      have hFw' : (oldest < w.seq || w.value.isSome) = false :=
        Bool.eq_false_iff.mpr hFw
      have hparts := Bool.or_eq_false_iff.mp hFw'
      have hseqle : ¬ oldest < w.seq := of_decide_eq_false hparts.1
      have hwseq : w.seq ≤ oldest := Nat.le_of_not_lt hseqle
      have hwval : w.value = none := by
        cases hv : w.value with
        | none => rfl
        | some v =>
          rw [hv] at hparts
          simp at hparts
      have hPw : (w.key == k && decide (w.seq ≤ t)) = true := by
        have h := List.find?_some hvis
        simpa using h
      have hwk : w.key = k := beq_iff_eq.mp (Bool.and_eq_true_iff.mp hPw).1
      have hwt : w.seq ≤ t := of_decide_eq_true (Bool.and_eq_true_iff.mp hPw).2
      have hdec := List.find?_eq_some_iff_append.mp hvis
      obtain ⟨_, as, bs, hcs, has⟩ := hdec
      have hnone' : visible ((compact xs oldest).filter
            fun e => oldest < e.seq || e.value.isSome) k t = none := by
        show ((compact xs oldest).filter fun e => oldest < e.seq || e.value.isSome).find?
          (fun e => e.key == k && e.seq ≤ t) = none
        have hBsNone : ∀ x ∈ bs.filter (fun e => oldest < e.seq || e.value.isSome),
            ¬ (x.key == k && decide (x.seq ≤ t)) := by
          intro x hx hPx
          rw [List.mem_filter] at hx
          obtain ⟨hxb, _⟩ := hx
          have hxk : x.key = k := beq_iff_eq.mp (Bool.and_eq_true_iff.mp hPx).1
          have hxt : x.seq ≤ t := of_decide_eq_true (Bool.and_eq_true_iff.mp hPx).2
          have hordc : (compact xs oldest).Pairwise (fun a b => b.seq ≤ a.seq) := by
            exact List.Pairwise.sublist (compact_sublist xs oldest) hord
          have hcs' : compact xs oldest = (as ++ [w]) ++ bs := by
            rw [hcs, List.append_assoc, List.singleton_append]
          rw [hcs'] at hordc
          have hxseq : x.seq ≤ w.seq := by
            have h := List.Pairwise.rel_of_mem_append hordc
              (List.mem_append_right as (List.mem_singleton_self w)) hxb
            exact h
          have hxold : x.seq ≤ oldest := Nat.le_trans hxseq hwseq
          have hcF : (compact xs oldest).filter (fun e => e.seq ≤ oldest)
              = (as.filter (fun e => e.seq ≤ oldest) ++ [w])
                ++ bs.filter (fun e => e.seq ≤ oldest) := by
            rw [hcs']
            have hwf : [w].filter (fun e => e.seq ≤ oldest) = [w] :=
              List.filter_cons_of_pos (decide_eq_true hwseq)
            rw [List.filter_append, List.filter_append, hwf]
          have hpw := compact_old_pw xs oldest
          rw [hcF] at hpw
          have hxF' : x ∈ bs.filter (fun e => e.seq ≤ oldest) :=
            List.mem_filter.mpr ⟨hxb, decide_eq_true hxold⟩
          have hne := List.Pairwise.rel_of_mem_append hpw
            (List.mem_append_right _ (List.mem_singleton_self w)) hxF'
          have hwe : w.key = x.key := hwk.trans hxk.symm
          exact hne hwe
        have hAsNone : (as.filter fun e => oldest < e.seq || e.value.isSome).find?
            (fun e => e.key == k && e.seq ≤ t) = none := by
          apply List.Sublist.find?_eq_none List.filter_sublist
          apply List.find?_eq_none.mpr
          intro a ha
          have h4 := has a ha
          intro hPa
          rw [hPa] at h4
          simp at h4
        have hfilter : (compact xs oldest).filter
              (fun e => oldest < e.seq || e.value.isSome)
            = as.filter (fun e => oldest < e.seq || e.value.isSome)
              ++ bs.filter (fun e => oldest < e.seq || e.value.isSome) := by
          rw [hcs, List.filter_append]
          have hwf : (w :: bs).filter (fun e => oldest < e.seq || e.value.isSome)
              = bs.filter (fun e => oldest < e.seq || e.value.isSome) :=
            List.filter_cons_of_neg hFw
          rw [hwf]
        rw [hfilter, List.find?_append, hAsNone]
        show (bs.filter fun e => oldest < e.seq || e.value.isSome).find?
          (fun e => e.key == k && e.seq ≤ t) = none
        apply List.find?_eq_none.mpr
        intro x hx
        exact hBsNone x hx
      rw [hnone', hvis]
      show (none : Option Int) = w.value
      exact hwval.symm

/-! ### Covered predicate -/

/-- Under `ordered`, the `find?` winner for key `k` at `t` has maximal `seq`
among all key-`k` entries visible at `t`. -/
private theorem visible_max_seq {xs : List Entry} {k t : Nat} {e : Entry}
    (hord : ordered xs) (hvis : visible xs k t = some e) :
    ∀ f ∈ xs, f.key = k → f.seq ≤ t → f.seq ≤ e.seq := by
  unfold visible at hvis
  obtain ⟨hPe, as, bs, hxs, has⟩ := List.find?_eq_some_iff_append.mp hvis
  intro f hf hfk hft
  rw [hxs] at hf
  rw [List.mem_append] at hf
  rcases hf with hfa | hfb
  · exfalso
    have hPf : (f.key == k && decide (f.seq ≤ t)) = true := by
      have h1 : (f.key == k) = true := beq_iff_eq.mpr hfk
      have h2 : decide (f.seq ≤ t) = true := decide_eq_true hft
      exact Bool.and_eq_true_iff.mpr ⟨h1, h2⟩
    have h4 := has f hfa
    exact (Bool.not_eq.mp h4) hPf
  · rw [List.mem_cons] at hfb
    rcases hfb with rfl | hfs
    · exact Nat.le_refl _
    · have hsub : e :: bs <+ xs := by
        rw [hxs]
        exact List.sublist_append_right as (e :: bs)
      have hpw : (e :: bs).Pairwise (fun a b => b.seq ≤ a.seq) :=
        List.Pairwise.sublist hsub hord
      have hpw' : ([e] ++ bs).Pairwise (fun a b => b.seq ≤ a.seq) := hpw
      have h := List.Pairwise.rel_of_mem_append hpw' (List.mem_singleton_self e) hfs
      exact h

/-- The `find?` winner has no strictly newer key-`k` version still `≤ t`. -/
private theorem visible_no_newer {xs : List Entry} {k t : Nat} {e : Entry}
    (hord : ordered xs) (hvis : visible xs k t = some e) :
    (xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t) = false := by
  apply Bool.eq_false_iff.mpr
  intro hany
  rw [List.any_eq_true] at hany
  obtain ⟨f, hfm, hfP⟩ := hany
  simp only [Bool.and_eq_true_iff] at hfP
  obtain ⟨⟨hf1, hf2⟩, hf3⟩ := hfP
  have hfk : f.key = k := beq_iff_eq.mp hf1
  have hgt : e.seq < f.seq := of_decide_eq_true hf2
  have hft : f.seq ≤ t := of_decide_eq_true hf3
  have hle := visible_max_seq hord hvis f hfm hfk hft
  omega

/-- `uniqueSeq` makes `(key, seq)` pairs unique, hence entries with equal
key and sequence are equal. -/
private theorem uniqueSeq_inj {xs : List Entry} (huniq : uniqueSeq xs)
    {e e' : Entry} (he : e ∈ xs) (he' : e' ∈ xs)
    (hkey : e.key = e'.key) (hseq : e.seq = e'.seq) : e = e' := by
  obtain ⟨i, hi, hie⟩ := List.getElem_of_mem he
  obtain ⟨j, hj, hje⟩ := List.getElem_of_mem he'
  have hiM : i < (xs.map fun e => (e.key, e.seq)).length := by
    rw [List.length_map]; exact hi
  have hjM : j < (xs.map fun e => (e.key, e.seq)).length := by
    rw [List.length_map]; exact hj
  have hi? : xs[i]? = some e := by
    rw [List.getElem?_eq_getElem hi, hie]
  have hj? : xs[j]? = some e' := by
    rw [List.getElem?_eq_getElem hj, hje]
  have hmi? : (xs.map fun e => (e.key, e.seq))[i]?
      = some (e.key, e.seq) := by
    rw [List.getElem?_map, hi?]
    rfl
  have hmj? : (xs.map fun e => (e.key, e.seq))[j]?
      = some (e'.key, e'.seq) := by
    rw [List.getElem?_map, hj?]
    rfl
  have hpair : (e'.key, e'.seq) = (e.key, e.seq) := by
    apply Prod.ext_iff.mpr
    exact ⟨hkey.symm, hseq.symm⟩
  rw [hpair] at hmj?
  have hij : i = j := (List.Nodup.getElem?_inj hiM huniq).mp (hmi?.trans hmj?.symm)
  subst j
  have hsome : some e = some e' := hi?.symm.trans hj?
  exact Option.some.inj hsome

/-- An entry satisfying the right-hand side of `CoveredPredicate` must be the
`find?` winner: its sequence is maximal and `uniqueSeq` pins it down. -/
private theorem rhs_unique {xs : List Entry} {k t : Nat} {e e' : Entry}
    (hord : ordered xs) (huniq : uniqueSeq xs)
    (hvis : visible xs k t = some e)
    (he'mem : e' ∈ xs) (he'key : e'.key = k) (he'seq : e'.seq ≤ t)
    (hnewer : (xs.any fun f => f.key == k && e'.seq < f.seq && f.seq ≤ t) = false) :
    e' = e := by
  have hek : e.key = k := by
    have hPe := List.find?_some hvis
    exact beq_iff_eq.mp (Bool.and_eq_true_iff.mp hPe).1
  have het : e.seq ≤ t := by
    have hPe := List.find?_some hvis
    exact of_decide_eq_true (Bool.and_eq_true_iff.mp hPe).2
  have he_mem : e ∈ xs := by
    obtain ⟨_, as, bs, hxs, _⟩ := List.find?_eq_some_iff_append.mp hvis
    rw [hxs]
    exact List.mem_append_right as List.mem_cons_self
  have hle1 : e'.seq ≤ e.seq :=
    visible_max_seq hord hvis e' he'mem he'key he'seq
  have hle2 : e.seq ≤ e'.seq := by
    apply Nat.le_of_not_lt
    intro hlt'
    have hPe' : (e.key == k && decide (e'.seq < e.seq) && decide (e.seq ≤ t)) = true := by
      have h1 : (e.key == k) = true := beq_iff_eq.mpr hek
      have h2 : decide (e'.seq < e.seq) = true := decide_eq_true hlt'
      have h3 : decide (e.seq ≤ t) = true := decide_eq_true het
      exact Bool.and_eq_true_iff.mpr ⟨Bool.and_eq_true_iff.mpr ⟨h1, h2⟩, h3⟩
    have htrue : (xs.any fun f => f.key == k && e'.seq < f.seq && f.seq ≤ t) = true := by
      rw [List.any_eq_true]
      exact ⟨e, he_mem, hPe'⟩
    rw [htrue] at hnewer
    simp at hnewer
  have hseq : e'.seq = e.seq := Nat.le_antisymm hle1 hle2
  exact uniqueSeq_inj huniq he'mem he_mem (he'key.trans hek.symm) hseq

theorem sound_CoveredPredicate : CoveredPredicate.Goal := by
  intro xs p k t hord huniq
  rw [Bool.eq_iff_iff]
  constructor
  · -- LHS → RHS: a satisfying visible value yields the newest matching entry
    intro hL
    unfold valueAt at hL
    cases hvis : visible xs k t with
    | none =>
        rw [hvis] at hL
        simp at hL
    | some e =>
        have hbind : (visible xs k t).bind Entry.value = e.value := by
          rw [hvis]; rfl
        rw [hbind] at hL
        cases hv : e.value with
        | none =>
            rw [hv] at hL
            simp at hL
        | some v =>
            have hpv : p v = true := by
              rw [hv] at hL
              exact hL
            have hPe := List.find?_some hvis
            have hek : e.key = k := beq_iff_eq.mp (Bool.and_eq_true_iff.mp hPe).1
            have het : e.seq ≤ t := of_decide_eq_true (Bool.and_eq_true_iff.mp hPe).2
            have he_mem : e ∈ xs := by
              obtain ⟨_, as, bs, hxs, _⟩ := List.find?_eq_some_iff_append.mp hvis
              rw [hxs]
              exact List.mem_append_right as List.mem_cons_self
            have hpred : (e.key == k && decide (e.seq ≤ t) && Option.any p e.value
                && !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t)) = true := by
              have h1 : (e.key == k) = true := beq_iff_eq.mpr hek
              have h2 : decide (e.seq ≤ t) = true := decide_eq_true het
              have h3 : Option.any p e.value = true := by
                rw [hv]
                exact hpv
              have h4 : !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t) = true :=
                Bool.not_eq.mpr (Bool.ne_true_of_eq_false (visible_no_newer hord hvis))
              exact Bool.and_eq_true_iff.mpr ⟨Bool.and_eq_true_iff.mpr
                ⟨Bool.and_eq_true_iff.mpr ⟨h1, h2⟩, h3⟩, h4⟩
            rw [List.any_eq_true]
            exact ⟨e, he_mem, hpred⟩
  · -- RHS → LHS: the unique newest matching entry is the visible winner
    intro hR
    rw [List.any_eq_true] at hR
    obtain ⟨e', he'mem, he'P⟩ := hR
    simp only [Bool.and_eq_true_iff] at he'P
    obtain ⟨⟨⟨hk, ht⟩, hv⟩, hn⟩ := he'P
    have he'key : e'.key = k := beq_iff_eq.mp hk
    have he'seq : e'.seq ≤ t := of_decide_eq_true ht
    have hany' : (xs.any fun f => f.key == k && e'.seq < f.seq && f.seq ≤ t) = false := by
      apply Bool.eq_false_iff.mpr
      intro h
      rw [h] at hn
      simp at hn
    -- `visible` cannot be `none`: `e'` itself matches the `find?` predicate
    have hvis : ∃ e, visible xs k t = some e := by
      cases hvc : visible xs k t with
      | none =>
          unfold visible at hvc
          rw [List.find?_eq_none] at hvc
          have hPe' : (e'.key == k && decide (e'.seq ≤ t)) = true := by
            exact Bool.and_eq_true_iff.mpr ⟨hk, ht⟩
          exact absurd hPe' (hvc e' he'mem)
      | some e => exact ⟨e, rfl⟩
    obtain ⟨e, hvis⟩ := hvis
    have he'e : e' = e := rhs_unique hord huniq hvis he'mem he'key he'seq hany'
    rw [he'e] at hv
    unfold valueAt
    rw [hvis]
    show Option.any p e.value = true
    exact hv

/-! ### Dominated run -/

/-- Transitivity of the sequence comparator underlying `zipIdxLE`. -/
private theorem domLe_trans (a b c : Entry) :
    decide (b.seq ≤ a.seq) = true → decide (c.seq ≤ b.seq) = true →
    decide (c.seq ≤ a.seq) = true := by
  intro hab hbc
  exact decide_eq_true (Nat.le_trans (of_decide_eq_true hbc) (of_decide_eq_true hab))

private theorem domLe_total (a b : Entry) :
    (decide (b.seq ≤ a.seq) || decide (a.seq ≤ b.seq)) = true := by
  rcases Nat.le_total a.seq b.seq with h | h
  · rw [decide_eq_true h]; simp
  · rw [decide_eq_true h]; simp

private theorem zipIdxLE_refl (x : Entry × Nat) :
    zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x x = true := by
  obtain ⟨xf, xi⟩ := x
  simp [zipIdxLE]

/-- A strictly smaller sequence always loses to a larger one. -/
private theorem zipIdxLE_of_seq_lt {x y : Entry × Nat} (h : y.fst.seq < x.fst.seq) :
    zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true := by
  obtain ⟨xf, xi⟩ := x; obtain ⟨yf, yi⟩ := y
  have h' : yf.seq < xf.seq := h
  have h1 : yf.seq ≤ xf.seq := Nat.le_of_lt h'
  have h2 : ¬ xf.seq ≤ yf.seq := by omega
  simp [zipIdxLE, h1, h2]

private theorem zipIdxLE_iff {x y : Entry × Nat} :
    zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true ↔
      y.fst.seq < x.fst.seq ∨ (x.fst.seq = y.fst.seq ∧ x.snd ≤ y.snd) := by
  obtain ⟨xf, xi⟩ := x; obtain ⟨yf, yi⟩ := y
  by_cases h1 : yf.seq ≤ xf.seq <;> by_cases h2 : xf.seq ≤ yf.seq <;>
    simp [zipIdxLE, h1, h2] <;> omega

/-- `zipIdxLE` is antisymmetric on elements drawn from one `zipIdx`,
thanks to the index tie-break. -/
private theorem zipIdxLE_antisymm {xs : List Entry} {x y : Entry × Nat}
    (hx : x ∈ xs.zipIdx) (hy : y ∈ xs.zipIdx)
    (hxy : zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true)
    (hyx : zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) y x = true) : x = y := by
  obtain ⟨xf, xi⟩ := x; obtain ⟨yf, yi⟩ := y
  rw [zipIdxLE_iff] at hxy hyx
  dsimp only at hxy hyx
  rcases hxy with hlt | ⟨hseq, hle⟩ <;> rcases hyx with hlt' | ⟨hseq', hle'⟩
  · omega
  · omega
  · omega
  · have hsnd : xi = yi := Nat.le_antisymm hle hle'
    have hxs := (List.mem_zipIdx' hx).2
    have hys := (List.mem_zipIdx' hy).2
    cases hsnd
    exact Prod.ext (hxs.trans hys.symm) rfl

private theorem nodup_zipIdx_gen (l : List Entry) (i : Nat) : (l.zipIdx i).Nodup := by
  induction l generalizing i with
  | nil => simp
  | cons x xs ih =>
    rw [List.zipIdx_cons, List.nodup_cons]
    constructor
    · intro hmem
      obtain ⟨h1, -, -⟩ := List.mem_zipIdx hmem
      omega
    · exact ih (i + 1)

theorem sound_DominatedRun : DominatedRun.Goal := by
  intro a b k t e hvis hb
  unfold visible at hvis
  unfold newest at hvis
  rw [← List.mergeSort_zipIdx] at hvis
  rw [List.find?_map] at hvis
  obtain ⟨⟨vf, i0⟩, hzfind, hzsnd⟩ := Option.map_eq_some_iff.mp hvis
  have hvf : vf = e := hzsnd
  subst vf
  -- hzfind : Z_a.find? q = some (e, i0)
  obtain ⟨hq_ei0, asA, bsA, hZA, hasA⟩ := List.find?_eq_some_iff_append.mp hzfind
  have hq' : (e.key == k && decide (e.seq ≤ t)) = true := hq_ei0
  rw [Bool.and_eq_true_iff] at hq'
  obtain ⟨hkeyb, hseqb⟩ := hq'
  have hek : e.key = k := beq_iff_eq.mp hkeyb
  have hest : e.seq ≤ t := of_decide_eq_true hseqb
  have hei0_Za : (e, i0) ∈ (a.zipIdx).mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)) := by
    rw [hZA]
    simp
  have hei0_Wa : (e, i0) ∈ a.zipIdx := List.mem_mergeSort.mp hei0_Za
  obtain ⟨hi0len, hie0⟩ := List.mem_zipIdx' hei0_Wa
  have hea : e ∈ a := List.mem_iff_getElem.mpr ⟨i0, hi0len, hie0.symm⟩
  have htransLE : ∀ (x y z : Entry × Nat),
      zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true →
      zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) y z = true →
      zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x z = true :=
    fun x y z h1 h2 => List.zipIdxLE_trans (fun a b c h1 h2 => domLe_trans a b c h1 h2) x y z h1 h2
  have htotalLE : ∀ (x y : Entry × Nat),
      (zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y ||
        zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) y x) = true :=
    fun x y => List.zipIdxLE_total (fun a b => domLe_total a b) x y
  have hPairA : ((a.zipIdx).mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq))).Pairwise
      (fun x y => zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true) :=
    List.pairwise_mergeSort htransLE htotalLE _
  -- Every qualifying element of the `a`-sorted list sits at-or-after `(e, i0)`.
  have hminA : ∀ u ∈ (a.zipIdx).mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)),
      ((fun e => e.key == k && decide (e.seq ≤ t)) ∘ fun x => x.fst) u = true →
      zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) (e, i0) u = true := by
    intro u huZ huq
    rw [hZA] at huZ
    rw [List.mem_append] at huZ
    rcases huZ with hua | hub
    · have hnu := hasA u hua
      rw [Bool.not_eq] at hnu
      exact absurd huq hnu
    · rw [List.mem_cons] at hub
      rcases hub with rfl | hubs
      · exact zipIdxLE_refl _
      · have hZeq : (a.zipIdx).mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)) =
            (asA ++ [(e, i0)]) ++ bsA := by
          rw [hZA]; simp [List.append_assoc]
        have hpair : ((asA ++ [(e, i0)]) ++ bsA).Pairwise
            (fun x y => zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true) := by
          rw [← hZeq]; exact hPairA
        rw [List.pairwise_append] at hpair
        exact hpair.2.2 (e, i0)
          (List.mem_append_right asA (List.mem_singleton_self (e, i0))) u hubs
  -- W-side
  have hWlen : i0 < (a ++ b).length := by
    rw [List.length_append]; omega
  have he_ab : e = (a ++ b)[i0]'hWlen := by
    rw [List.getElem_append_left hi0len (h' := hWlen)]
    exact hie0
  have heW : (e, i0) ∈ (a ++ b).zipIdx := by
    rw [List.mem_zipIdx_iff_getElem?]
    show (a ++ b)[i0]? = some e
    exact List.getElem?_eq_some_iff.mpr ⟨hWlen, he_ab.symm⟩
  have hminW : ∀ z ∈ (a ++ b).zipIdx,
      ((fun e => e.key == k && decide (e.seq ≤ t)) ∘ fun x => x.fst) z = true →
      zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) (e, i0) z = true := by
    intro z hzW hzq
    obtain ⟨zf, zi⟩ := z
    by_cases hlt : zf.seq < e.seq
    · exact zipIdxLE_of_seq_lt hlt
    · have hge : e.seq ≤ zf.seq := Nat.le_of_not_lt hlt
      obtain ⟨hzWlen, hzWval⟩ := List.mem_zipIdx' hzW
      rw [List.length_append] at hzWlen
      by_cases hzi : zi < a.length
      · have hzAval : zf = a[zi]'hzi := hzWval.trans (List.getElem_append_left hzi)
        have hzA : (zf, zi) ∈ a.zipIdx := by
          rw [List.mem_zipIdx_iff_getElem?]
          show a[zi]? = some zf
          exact List.getElem?_eq_some_iff.mpr ⟨hzi, hzAval.symm⟩
        have hzZa : (zf, zi) ∈ (a.zipIdx).mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)) :=
          List.mem_mergeSort.mpr hzA
        exact hminA (zf, zi) hzZa hzq
      · have hzBval : zf = b[zi - a.length]'(by omega : zi - a.length < b.length) := by
          exact hzWval.trans (List.getElem_append_right (by omega : a.length ≤ zi))
        have hzb : zf ∈ b := by
          rw [hzBval]; exact List.getElem_mem _
        have hlt2 := hb zf hzb
        exfalso; omega
  have hPairW : ((a ++ b).zipIdx.mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq))).Pairwise
      (fun x y => zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true) :=
    List.pairwise_mergeSort htransLE htotalLE _
  have heZ : (e, i0) ∈ (a ++ b).zipIdx.mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)) :=
    List.mem_mergeSort.mpr heW
  obtain ⟨asZ, bsZ, hZdec⟩ := List.append_of_mem heZ
  -- No element before `(e, i0)` in the merged sort satisfies the predicate.
  have hprefix : ∀ z ∈ asZ,
      (!((fun e => e.key == k && decide (e.seq ≤ t)) ∘ fun x => x.fst) z) = true := by
    intro z hz
    have hrel : zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) z (e, i0) = true := by
      have hpair : (asZ ++ ((e, i0) :: bsZ)).Pairwise
          (fun x y => zipIdxLE (fun a b => decide (b.seq ≤ a.seq)) x y = true) := by
        rw [← hZdec]; exact hPairW
      rw [List.pairwise_append] at hpair
      exact hpair.2.2 z hz (e, i0) (List.mem_cons_self)
    rw [Bool.not_eq]
    by_cases hqz : ((fun e => e.key == k && decide (e.seq ≤ t)) ∘ fun x => x.fst) z = true
    · exfalso
      have hzW : z ∈ (a ++ b).zipIdx := by
        have hzZ : z ∈ (a ++ b).zipIdx.mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq)) := by
          rw [hZdec]
          exact List.mem_append_left _ hz
        exact List.mem_mergeSort.mp hzZ
      have hmin := hminW z hzW hqz
      have hzeq : z = (e, i0) := zipIdxLE_antisymm hzW heW hrel hmin
      rw [hzeq] at hz
      have hZN : ((a ++ b).zipIdx.mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq))).Nodup :=
        (List.Perm.nodup_iff (List.mergeSort_perm _ _)).mpr (nodup_zipIdx_gen _ 0)
      rw [hZdec, List.nodup_append] at hZN
      have hne := hZN.2.2 (e, i0) hz (e, i0) (List.mem_cons_self)
      exact hne rfl
    · exact hqz
  unfold visible
  unfold newest
  rw [← List.mergeSort_zipIdx, List.find?_map]
  have hZfind : ((a ++ b).zipIdx.mergeSort (zipIdxLE fun a b => decide (b.seq ≤ a.seq))).find?
      ((fun e => e.key == k && decide (e.seq ≤ t)) ∘ fun x => x.fst) = some (e, i0) :=
    List.find?_eq_some_iff_append.mpr ⟨hq_ei0, asZ, bsZ, hZdec, hprefix⟩
  rw [hZfind]
  rfl


/-! ### Range tombstone -/

private def rcmp (a b : Entry) : Bool :=
  decide (a.seq > b.seq) || (a.seq == b.seq && (a.value.isNone || b.value.isSome))

private theorem rcmp_iff (a b : Entry) :
    rcmp a b = true ↔ a.seq > b.seq ∨
      (a.seq = b.seq ∧ (a.value = none ∨ b.value ≠ none)) := by
  unfold rcmp
  rw [Bool.or_eq_true_iff, Bool.and_eq_true_iff]
  constructor
  · rintro (h | ⟨h1, h2⟩)
    · exact Or.inl (of_decide_eq_true h)
    · rw [beq_iff_eq] at h1
      rw [Bool.or_eq_true_iff] at h2
      rcases h2 with h2 | h2
      · exact Or.inr ⟨h1, Or.inl (Option.isNone_iff_eq_none.mp h2)⟩
      · exact Or.inr ⟨h1, Or.inr (Option.isSome_iff_ne_none.mp h2)⟩
  · rintro (h | ⟨h1, h2⟩)
    · exact Or.inl (decide_eq_true h)
    · apply Or.inr
      constructor
      · exact beq_iff_eq.mpr h1
      · rcases h2 with h2 | h2
        · exact Bool.or_eq_true_iff.mpr (Or.inl
            (Option.isNone_iff_eq_none.mpr h2))
        · exact Bool.or_eq_true_iff.mpr (Or.inr
            (Option.isSome_iff_ne_none.mpr h2))

private theorem rcmp_refl (a : Entry) : rcmp a a = true := by
  apply (rcmp_iff a a).mpr
  right
  refine ⟨rfl, ?_⟩
  by_cases hav : a.value = none
  · exact Or.inl hav
  · exact Or.inr hav

private theorem rcmp_trans (a b c : Entry)
    (hab : rcmp a b = true) (hbc : rcmp b c = true) : rcmp a c = true := by
  rw [rcmp_iff] at hab hbc ⊢
  rcases hab with h1 | ⟨h2, h3⟩ <;> rcases hbc with h4 | ⟨h5, h6⟩
  · left; omega
  · left; omega
  · left; omega
  · right
    refine ⟨?_, ?_⟩
    · omega
    · rcases h3 with h3 | h3 <;> rcases h6 with h6 | h6
      · exact Or.inl h3
      · exact Or.inl h3
      · exact (h3 h6).elim
      · exact Or.inr h6

private theorem rcmp_total (a b : Entry) : (rcmp a b || rcmp b a) = true := by
  rw [Bool.or_eq_true_iff]
  by_cases hseq : a.seq = b.seq
  · by_cases hav : a.value = none
    · left
      apply (rcmp_iff a b).mpr
      right
      exact ⟨hseq, Or.inl hav⟩
    · right
      apply (rcmp_iff b a).mpr
      right
      exact ⟨hseq.symm, Or.inr hav⟩
  · rcases Nat.lt_trichotomy a.seq b.seq with hlt | heq | hgt
    · right
      apply (rcmp_iff b a).mpr
      left
      omega
    · exact absurd heq hseq
    · left
      apply (rcmp_iff a b).mpr
      left
      omega


/-- On a `rcmp`-sorted list, the `find?` winner is `rcmp`-minimal among
all elements satisfying the predicate. -/
private theorem visible_sorted_min {xs : List Entry} {k t : Nat} {w : Entry}
    (hsorted : xs.Pairwise (fun a b => rcmp a b = true))
    (hfind : visible xs k t = some w) :
    ∀ y ∈ xs, (y.key == k && decide (y.seq ≤ t)) = true → rcmp w y = true := by
  unfold visible at hfind
  obtain ⟨hPw, pre, post, hl, hasPre⟩ := List.find?_eq_some_iff_append.mp hfind
  intro y hy hp
  rw [hl] at hy hsorted
  rw [List.mem_append] at hy
  rcases hy with hyp | hyp
  · have hnp := hasPre y hyp
    rw [hp] at hnp
    simp at hnp
  · rw [List.mem_cons] at hyp
    rcases hyp with rfl | hpost
    · exact rcmp_refl _
    · have hsub : w :: post <+ pre ++ w :: post :=
        List.sublist_append_right pre (w :: post)
      have hpw := List.Pairwise.sublist hsub hsorted
      exact (List.pairwise_cons.mp hpw).1 y hpost

/-- A tombstone entry produced by `applyMarkers` comes from a marker that
covers `k` spatially (`covers m k m.seq` ignores the time bound). -/
private theorem tomb_mem {ms : List Marker} {k : Nat} {e : Entry}
    (he : e ∈ (ms.filter fun m => covers m k m.seq).map
      fun m => Entry.mk k m.seq none) :
    ∃ m ∈ ms, covers m k m.seq = true ∧ e = Entry.mk k m.seq none := by
  rw [List.mem_map] at he
  obtain ⟨m, hm, hfm⟩ := he
  rw [List.mem_filter] at hm
  exact ⟨m, hm.1, hm.2, hfm.symm⟩

private theorem tomb_mem' {ms : List Marker} {k : Nat} {m : Marker}
    (hm : m ∈ ms.filter fun m => covers m k m.seq) :
    Entry.mk k m.seq none ∈
      (ms.filter fun m => covers m k m.seq).map
        fun m => Entry.mk k m.seq none := by
  rw [List.mem_map]
  exact ⟨m, hm, rfl⟩

/-- `covers m k t` implies the spatial part needed for `covers m k m.seq`. -/
private theorem covers_mseq_of_covers {m : Marker} {k t : Nat}
    (h : covers m k t = true) : covers m k m.seq = true := by
  unfold covers at h ⊢
  simp only [Bool.and_eq_true_iff] at h ⊢
  obtain ⟨⟨h1, h2⟩, h3⟩ := h
  exact ⟨⟨h1, h2⟩, decide_eq_true (Nat.le_refl _)⟩

/-- Spatial coverage gives the concrete bounds on `lo`, `hi`. -/
private theorem covers_bounds {m : Marker} {k : Nat}
    (h : covers m k m.seq = true) : m.lo ≤ k ∧ k < m.hi := by
  unfold covers at h
  simp only [Bool.and_eq_true_iff] at h
  obtain ⟨⟨h1, h2⟩, _⟩ := h
  exact ⟨of_decide_eq_true h1, of_decide_eq_true h2⟩

/-- A `some`-valued entry never precedes a `none`-valued entry of sequence
at least its own: the tombstone wins ties and higher sequences. -/
private theorem rcmp_false {a b : Entry} (hle : a.seq ≤ b.seq)
    (ha : a.value.isSome = true) (hb : b.value.isNone = true) :
    rcmp a b = false := by
  rw [Bool.eq_false_iff]
  intro hrcmp
  rw [rcmp_iff] at hrcmp
  rcases hrcmp with hgt | ⟨heq, hval⟩
  · omega
  · rcases hval with hnone | hsome
    · rw [hnone] at ha
      simp at ha
    · have hbv : b.value = none := Option.isNone_iff_eq_none.mp hb
      exact hsome hbv

/-- A strictly older sequence never precedes a newer one. -/
private theorem rcmp_false_of_seq_lt {a b : Entry} (hlt : a.seq < b.seq) :
    rcmp a b = false := by
  rw [Bool.eq_false_iff]
  intro h
  rw [rcmp_iff] at h
  rcases h with hgt | ⟨heq, _⟩ <;> omega

theorem sound_RangeTombstone : RangeTombstone.Goal := by
  intro xs ms k t hord huniq
  -- sortedness of the merged list produced by `applyMarkers`
  have hSsorted : (((xs ++ ((ms.filter fun m => covers m k m.seq).map
        fun m => Entry.mk k m.seq none)).mergeSort
        (fun a b => a.seq > b.seq ||
          (a.seq == b.seq && (a.value.isNone || b.value.isSome))))).Pairwise
      (fun a b => rcmp a b = true) := by
    have h := List.pairwise_mergeSort (le := rcmp) rcmp_trans rcmp_total
      (xs ++ ((ms.filter fun m => covers m k m.seq).map
        fun m => Entry.mk k m.seq none))
    exact h
  unfold valueAt
  cases hvis : visible xs k t with
  | none =>
      cases hSv : visible (applyMarkers xs ms k) k t with
      | none => rfl
      | some w =>
          -- LHS = w.value; RHS = none → need w.value = none
          unfold applyMarkers at hSv
          unfold visible at hSv
          obtain ⟨hPw, pre, post, hl, hasPre⟩ :=
            List.find?_eq_some_iff_append.mp hSv
          have hwS : w ∈ (xs ++ ((ms.filter fun m => covers m k m.seq).map
              fun m => Entry.mk k m.seq none)).mergeSort
              (fun a b => a.seq > b.seq ||
                (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
            rw [hl]
            exact List.mem_append_right pre List.mem_cons_self
          rw [List.mem_mergeSort] at hwS
          rw [List.mem_append] at hwS
          rcases hwS with hwxs | hwt
          · -- w ∈ xs → contradicts visible xs k t = none
            have hPw := List.find?_some hSv
            -- hPw : (w.key == k && decide (w.seq ≤ t)) = true
            unfold visible at hvis
            rw [List.find?_eq_none] at hvis
            exact absurd hPw (hvis w hwxs)
          · obtain ⟨m, hmms, hmcov, rfl⟩ := tomb_mem hwt
            rfl
  | some e₀ =>
      simp only [Option.bind_some]
      by_cases hany : ((ms.filter fun m => covers m k t).any
          fun m => decide (e₀.seq ≤ m.seq)) = true
      · rw [ite_eq_left hany]
        cases hSv : visible (applyMarkers xs ms k) k t with
        | none => rfl
        | some w =>
            simp only [Option.bind_some]
            -- goal: w.value = none; derive via tombstone τ₀
            obtain ⟨m₀, hm₀filt, hm₀ge⟩ := List.any_eq_true.mp hany
            rw [List.mem_filter] at hm₀filt
            obtain ⟨hm₀ms, hm₀cov⟩ := hm₀filt
            have hm₀seq : e₀.seq ≤ m₀.seq := of_decide_eq_true hm₀ge
            have hm₀covseq : covers m₀ k m₀.seq = true :=
              covers_mseq_of_covers hm₀cov
            have hm₀tseq : m₀.seq ≤ t := by
              unfold covers at hm₀cov
              simp only [Bool.and_eq_true_iff] at hm₀cov
              obtain ⟨⟨_, _⟩, h3⟩ := hm₀cov
              exact of_decide_eq_true h3
            have hm₀filt' : m₀ ∈ ms.filter (fun m => covers m k m.seq) := by
              rw [List.mem_filter]
              exact ⟨hm₀ms, hm₀covseq⟩
            have hτ₀T' : Entry.mk k m₀.seq none ∈
                (ms.filter fun m => covers m k m.seq).map
                  fun m => Entry.mk k m.seq none := tomb_mem' hm₀filt'
            have hτ₀S : Entry.mk k m₀.seq none ∈
                (xs ++ ((ms.filter fun m => covers m k m.seq).map
                  fun m => Entry.mk k m.seq none)).mergeSort
                  (fun a b => a.seq > b.seq ||
                    (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
              rw [List.mem_mergeSort]
              exact List.mem_append_right xs hτ₀T'
            have hτ₀q : ((Entry.mk k m₀.seq none).key == k &&
                decide ((Entry.mk k m₀.seq none).seq ≤ t)) = true := by
              have hk : (Entry.mk k m₀.seq none).key = k := rfl
              have hs : (Entry.mk k m₀.seq none).seq = m₀.seq := rfl
              rw [hk, hs]
              exact Bool.and_eq_true_iff.mpr
                ⟨beq_iff_eq.mpr rfl, decide_eq_true hm₀tseq⟩
            unfold applyMarkers at hSv
            unfold visible at hSv
            have hτ₀S' : Entry.mk k m₀.seq none ∈
                applyMarkers xs ms k := hτ₀S
            have hmin := visible_sorted_min hSsorted hSv
              (Entry.mk k m₀.seq none) hτ₀S' hτ₀q
            -- hmin : rcmp w (Entry.mk k m₀.seq none) = true
            obtain ⟨hPw, pre, post, hl, hasPre⟩ :=
              List.find?_eq_some_iff_append.mp hSv
            have hwS : w ∈ (xs ++ ((ms.filter fun m => covers m k m.seq).map
                fun m => Entry.mk k m.seq none)).mergeSort
                (fun a b => a.seq > b.seq ||
                  (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
              rw [hl]
              exact List.mem_append_right pre List.mem_cons_self
            rw [List.mem_mergeSort, List.mem_append] at hwS
            rcases hwS with hwxs | hwt
            · -- w ∈ xs: w.seq ≤ e₀.seq; τ₀.seq ≥ e₀.seq → τ₀.seq ≥ w.seq
              have hPw' := hPw
              have hkey : w.key = k := beq_iff_eq.mp
                (Bool.and_eq_true_iff.mp hPw').1
              have hseq : w.seq ≤ t := of_decide_eq_true
                (Bool.and_eq_true_iff.mp hPw').2
              have hwle := visible_max_seq hord hvis w hwxs hkey hseq
              -- hwle : w.seq ≤ e₀.seq
              cases hwv : w.value with
              | none => rfl
              | some hv =>
                  exfalso
                  have hle : w.seq ≤ (Entry.mk k m₀.seq none).seq := by
                    have : (Entry.mk k m₀.seq none).seq = m₀.seq := rfl
                    rw [this]
                    omega
                  have his : w.value.isSome = true := by
                    rw [hwv]; rfl
                  have hnis : (Entry.mk k m₀.seq none).value.isNone = true := rfl
                  have hfalse := rcmp_false hle his hnis
                  rw [hfalse] at hmin
                  exact Bool.noConfusion hmin
            · obtain ⟨m, hmms, hmcov, rfl⟩ := tomb_mem hwt
              rfl
      · rw [ite_eq_right hany]
        cases hSv : visible (applyMarkers xs ms k) k t with
        | none =>
            exfalso
            unfold visible at hvis
            obtain ⟨hPe₀, as, bs, hxs, _⟩ :=
              List.find?_eq_some_iff_append.mp hvis
            have he₀xs : e₀ ∈ xs := by
              rw [hxs]
              exact List.mem_append_right as List.mem_cons_self
            have he₀S : e₀ ∈ (xs ++ ((ms.filter fun m => covers m k m.seq).map
                fun m => Entry.mk k m.seq none)).mergeSort
                (fun a b => a.seq > b.seq ||
                  (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
              rw [List.mem_mergeSort]
              exact List.mem_append_left _ he₀xs
            unfold applyMarkers at hSv
            unfold visible at hSv
            rw [List.find?_eq_none] at hSv
            exact (hSv e₀ he₀S) hPe₀
        | some w =>
            simp only [Option.bind_some]
            unfold applyMarkers at hSv
            unfold visible at hSv
            obtain ⟨hPw, pre, post, hl, hasPre⟩ :=
              List.find?_eq_some_iff_append.mp hSv
            have hwS : w ∈ (xs ++ ((ms.filter fun m => covers m k m.seq).map
                fun m => Entry.mk k m.seq none)).mergeSort
                (fun a b => a.seq > b.seq ||
                  (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
              rw [hl]
              exact List.mem_append_right pre List.mem_cons_self
            rw [List.mem_mergeSort, List.mem_append] at hwS
            have hPw' := hPw
            have hkeyW : w.key = k := beq_iff_eq.mp
              (Bool.and_eq_true_iff.mp hPw').1
            have hseqW : w.seq ≤ t := of_decide_eq_true
              (Bool.and_eq_true_iff.mp hPw').2
            -- e₀ facts
            unfold visible at hvis
            obtain ⟨hPe₀, _, _, _, _⟩ := List.find?_eq_some_iff_append.mp hvis
            have he₀xs : e₀ ∈ xs := by
              obtain ⟨_, as, bs, hxs, _⟩ :=
                List.find?_eq_some_iff_append.mp hvis
              rw [hxs]
              exact List.mem_append_right as List.mem_cons_self
            have he₀key : e₀.key = k := beq_iff_eq.mp
              (Bool.and_eq_true_iff.mp hPe₀).1
            have he₀S : e₀ ∈ (xs ++ ((ms.filter fun m => covers m k m.seq).map
                fun m => Entry.mk k m.seq none)).mergeSort
                (fun a b => a.seq > b.seq ||
                  (a.seq == b.seq && (a.value.isNone || b.value.isSome))) := by
              rw [List.mem_mergeSort]
              exact List.mem_append_left _ he₀xs
            have hmin := visible_sorted_min hSsorted hSv
            have hrw_e₀ : rcmp w e₀ = true := hmin e₀ he₀S hPe₀
            rcases hwS with hwxs | hwt
            · -- w ∈ xs: show w.seq = e₀.seq then uniqueSeq gives w = e₀
              have hwle := visible_max_seq hord hvis w hwxs hkeyW hseqW
              rw [rcmp_iff] at hrw_e₀
              rcases hrw_e₀ with hgt | ⟨heq, _⟩
              · omega
              · have hseq : w.seq = e₀.seq := heq
                have hwe : w = e₀ :=
                  uniqueSeq_inj huniq hwxs he₀xs
                    (hkeyW.trans he₀key.symm) hseq
                rw [hwe]
            · -- w ∈ T': a tombstone; derive w.seq < e₀.seq, contradicting min
              exfalso
              obtain ⟨m, hmms, hmcov, rfl⟩ := tomb_mem hwt
              have hms_le : m.seq ≤ t := by
                have hPwτ := hPw
                -- pvis (Entry.mk k m.seq none) = true → seq ≤ t
                rw [show (Entry.mk k m.seq none).key = k from rfl,
                    show (Entry.mk k m.seq none).seq = m.seq from rfl] at hPwτ
                exact of_decide_eq_true (Bool.and_eq_true_iff.mp hPwτ).2
              have hbounds := covers_bounds hmcov
              have hcovt : covers m k t = true := by
                unfold covers
                simp only [Bool.and_eq_true_iff]
                refine ⟨⟨?_, ?_⟩, ?_⟩
                · exact decide_eq_true hbounds.1
                · exact decide_eq_true hbounds.2
                · exact decide_eq_true hms_le
              have hm_filt : m ∈ ms.filter (fun m => covers m k t) := by
                rw [List.mem_filter]
                exact ⟨hmms, hcovt⟩
              have hnotle : ¬(e₀.seq ≤ m.seq) := by
                intro hle
                have htrue : ((ms.filter fun m => covers m k t).any
                    fun m => decide (e₀.seq ≤ m.seq)) = true := by
                  rw [List.any_eq_true]
                  exact ⟨m, hm_filt, decide_eq_true hle⟩
                exact hany htrue
              have hlt : m.seq < e₀.seq := Nat.lt_of_not_ge hnotle
              have hfalse := rcmp_false_of_seq_lt (a := Entry.mk k m.seq none) (b := e₀) hlt
              rw [hfalse] at hrw_e₀
              simp at hrw_e₀

end TinylambOpt.ResearchProofs.ExecutionStrategies.VersionedIndexes
