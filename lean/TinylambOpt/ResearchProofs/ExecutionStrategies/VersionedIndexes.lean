import TinylambOpt.ResearchGoals.ExecutionStrategies.VersionedIndexes

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.VersionedIndexes

open TinylambOpt.ResearchGoals.VersionedIndexes

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

end TinylambOpt.ResearchProofs.ExecutionStrategies.VersionedIndexes
