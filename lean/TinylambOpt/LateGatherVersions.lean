import Std

/-!
  Late materialization: gathering a saved row reference must reproduce the
  values captured by an eager projection (C++:
  `executor/late_materialization_join.cpp`, `common/late_materialization.hpp`).

  Model. A physical slot holds a chain of `Version`s. Each version carries a
  logical `key`, a `value`, a validity interval `[from, to)` in commit-timestamp
  space and a unique `id`. A snapshot is a single read timestamp `s`; the
  version visible for that snapshot is the unique chain element whose interval
  contains `s` (`visibleVersion`). An UPDATE appends a version with
  `from = s + 1`, so the version visible at the *old* snapshot `s` is unchanged;
  a slot reuse installs a chain whose versions were born after `s`, so again
  the snapshot-`s` image is unchanged. What would change the projection is
  re-probing at the *current* timestamp (an "unpinned" re-search), or an
  in-place rewrite / backdated version with `from ≤ s` — both excluded by the
  hypotheses below.

  The selection step saves row references (physical slot numbers) and, for the
  eager baseline, the values read at snapshot `s`. The gather step re-reads
  each saved reference. Soundness holds under either of two disciplines:

    * **Pinned** — gather uses the same snapshot `s`, and for each saved slot
      the part of the chain visible at `s` is unchanged
      (`hstable`: `upTo heap₁ s = upTo heap₀ s`). This is the main theorem
      `late_gather_versions_sound`.
    * **Revalidated / fallback** — gather compares the id of the freshly
      resolved version with the id of the captured version and, on any
      mismatch (including "no version"), falls back to the eager value. This
      reproduces the eager result unconditionally, as
      `late_gather_versions_sound_revalidated` shows.

  Reordering the saved references before gather is harmless because both the
  eager and the late list are obtained by applying the same map to the same
  saved-reference list; hence a permutation of that list permutes both results
  identically.
-/

namespace TinylambOpt.LateGatherVersions

/-- One version stored in a physical slot. -/
structure Version (K V : Type) where
  key   : K
  value : V
  born  : Nat
  expiry : Nat
  id    : Nat
  deriving DecidableEq, Repr

/-- The version visible at snapshot `s`. `find?` returns the first matching
    version; well-formedness below makes it unique. -/
def visibleVersion {K V : Type} (chain : List (Version K V)) (s : Nat) :
    Option (Version K V) :=
  chain.find? (fun v => v.born ≤ s && s < v.expiry)

/-- The part of a chain that could be visible at or before `s`: versions born
    after `s` can never be seen by snapshot `s`. -/
def upTo {K V : Type} (chain : List (Version K V)) (s : Nat) : List (Version K V) :=
  chain.filter (fun v => v.born ≤ s)

/-- At most one version of a chain is visible at any snapshot. -/
def ChainWellFormed {K V : Type} (chain : List (Version K V)) : Prop :=
  ∀ v ∈ chain, ∀ w ∈ chain, ∀ s,
    v.born ≤ s ∧ s < v.expiry → w.born ≤ s ∧ s < w.expiry → v = w

/-- Version ids identify a version everywhere in the two heaps. -/
def IdsUnique {K V : Type} (chain : List (Version K V)) : Prop :=
  ∀ v ∈ chain, ∀ w ∈ chain, v.id = w.id → v = w

/-- `List.find?` result membership / predicate satisfaction. -/
theorem find_some {α : Type} (p : α → Bool) : ∀ (l : List α) {a},
    l.find? p = some a → a ∈ l ∧ p a = true
  | [], a, h => by simp at h
  | x :: xs, a, h => by
      by_cases hx : p x = true
      · simp [List.find?, hx] at h
        subst a
        exact ⟨by simp, hx⟩
      · have hx' : p x = false := by cases hpx : p x <;> simp_all
        simp [List.find?, hx'] at h
        have ih := find_some p xs h
        exact ⟨List.mem_cons_of_mem _ ih.1, ih.2⟩

theorem visibleVersion_some {K V : Type} {chain : List (Version K V)} {s : Nat}
    {v : Version K V} (h : visibleVersion chain s = some v) :
    v ∈ chain ∧ v.born ≤ s ∧ s < v.expiry := by
  have h' := find_some (fun (w : Version K V) => w.born ≤ s && w.expiry > s) chain h
  exact ⟨h'.1, by simpa using h'.2⟩

/-- Filtering away versions born after `s` cannot change what snapshot `s`
    sees: those versions never satisfy the visibility test. -/
theorem find_filter_of_imp {α : Type} (p q : α → Bool)
    (himp : ∀ a, q a = true → p a = true) :
    ∀ l : List α, l.find? q = (l.filter p).find? q := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hq : q x = true
      · have hp : p x = true := himp x hq
        simp [List.filter, hp, hq]
      · have hq' : q x = false := by cases hqx : q x <;> simp_all
        by_cases hp : p x = true
        · simp [List.filter, hp, hq', ih]
        · have hp' : p x = false := by cases hpx : p x <;> simp_all
          simp [List.filter, hp', hq', ih]

theorem visibleVersion_upTo {K V : Type} (chain : List (Version K V)) (s : Nat) :
    visibleVersion chain s = visibleVersion (upTo chain s) s := by
  unfold visibleVersion upTo
  apply find_filter_of_imp (fun v : Version K V => v.born ≤ s)
    (fun v : Version K V => v.born ≤ s && s < v.expiry)
  intro v hv
  by_cases hb : v.born ≤ s
  · simpa using hb
  · simp [hb] at hv

/-- If the visible prefixes of two chains coincide, snapshot `s` resolves the
    same version in both. This is the pinned-snapshot condition. -/
theorem visibleVersion_eq_of_upTo_eq {K V : Type}
    {c₀ c₁ : List (Version K V)} {s : Nat}
    (h : upTo c₁ s = upTo c₀ s) :
    visibleVersion c₁ s = visibleVersion c₀ s := by
  rw [visibleVersion_upTo c₁ s, visibleVersion_upTo c₀ s, h]

/-- The eager projection: values captured while the rows were selected. -/
def eagerValues {K V : Type} (heap : Nat → List (Version K V))
    (saved : List Nat) (s : Nat) : List (Option V) :=
  saved.map (fun slot => (visibleVersion (heap slot) s).map Version.value)

/-- The late gather: re-read every saved reference at snapshot `s`. -/
def gatherPinned {K V : Type} (heap : Nat → List (Version K V))
    (saved : List Nat) (s : Nat) : List (Option V) :=
  saved.map (fun slot => (visibleVersion (heap slot) s).map Version.value)

/-- MAIN THEOREM (pinned snapshot). Re-reading the saved row references at the
    pinned snapshot reproduces the eager projection exactly, provided each
    saved slot's snapshot-`s` visible prefix is unchanged. -/
theorem late_gather_versions_sound {K V : Type}
    (heap₀ heap₁ : Nat → List (Version K V)) (saved : List Nat) (s : Nat)
    (hstable : ∀ slot ∈ saved, upTo (heap₁ slot) s = upTo (heap₀ slot) s) :
    gatherPinned heap₁ saved s = eagerValues heap₀ saved s := by
  unfold gatherPinned eagerValues
  apply List.map_congr_left
  intro slot hslot
  exact congrArg (Option.map Version.value) (visibleVersion_eq_of_upTo_eq (hstable slot hslot))

/-- Gather with id revalidation and eager fallback. A freshly resolved version
    is accepted only when its id matches the captured version's id; otherwise
    the eagerly captured value is used. -/
def gatherRevalidated {K V : Type} (heap₀ heap₁ : Nat → List (Version K V))
    (saved : List Nat) (s : Nat) : List (Option V) :=
  saved.map (fun slot =>
    match visibleVersion (heap₁ slot) s with
    | none => (visibleVersion (heap₀ slot) s).map Version.value
    | some v₁ =>
        match visibleVersion (heap₀ slot) s with
        | none => none
        | some v₀ => if v₀.id = v₁.id then some v₁.value else some v₀.value)

/-- Revalidated gather always equals the eager projection: when ids match,
    uniqueness makes the versions (hence values) equal; when they differ (or
    a version vanished) the eager value is the fallback. -/
theorem late_gather_versions_sound_revalidated {K V : Type}
    (heap₀ heap₁ : Nat → List (Version K V)) (saved : List Nat) (s : Nat)
    (huniq : ∀ slot ∈ saved, IdsUnique (heap₀ slot ++ heap₁ slot)) :
    gatherRevalidated heap₀ heap₁ saved s = eagerValues heap₀ saved s := by
  unfold gatherRevalidated eagerValues
  apply List.map_congr_left
  intro slot hslot
  cases h₀ : visibleVersion (heap₀ slot) s with
  | none =>
      cases h₁ : visibleVersion (heap₁ slot) s <;> rfl
  | some v₀ =>
      cases h₁ : visibleVersion (heap₁ slot) s with
      | none => rfl
      | some v₁ =>
          by_cases hid : v₀.id = v₁.id
          · have hv₀ := visibleVersion_some h₀
            have hv₁ := visibleVersion_some h₁
            have hv₀app : v₀ ∈ heap₀ slot ++ heap₁ slot := List.mem_append_left _ hv₀.1
            have hv₁app : v₁ ∈ heap₀ slot ++ heap₁ slot := List.mem_append_right _ hv₁.1
            have heq : v₀ = v₁ := huniq slot hslot v₀ hv₀app v₁ hv₁app hid
            rw [← heq]
            simp [hid]
          · simp [hid]

/-- Boundary example: an unpinned re-probe at the current timestamp reads a
    newer version and therefore changes the projected value. Pinning the
    snapshot (the main theorem) is what prevents this. -/
theorem unpinned_reprobe_can_change_value :
    let chain : List (Version Nat Int) :=
      [ { key := 1, value := 10, born := 0, expiry := 5,  id := 0 },
        { key := 1, value := 99, born := 5, expiry := 100, id := 1 } ]
    (visibleVersion chain 0).map Version.value = some 10 ∧
    (visibleVersion chain 6).map Version.value = some 99 := by
  native_decide

end TinylambOpt.LateGatherVersions
