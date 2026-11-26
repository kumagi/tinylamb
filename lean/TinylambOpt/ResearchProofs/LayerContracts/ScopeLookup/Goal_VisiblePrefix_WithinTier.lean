import TinylambOpt.ResearchGoals.LayerContracts.ScopeLookup

/-!
SQL binding and compilation depend on preserving the chosen name, overload, or ordinal
when narrowing the lookup.

Proved

- `VisiblePrefix.WithinTier` — Permuting a tier changes neither successful binding nor
  ambiguity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ScopeLookup.Goal_VisiblePrefix_WithinTier

open TinylambOpt.ResearchGoals.LayerContracts.ScopeLookup
open TinylambOpt.ResearchGoals.LayerContracts.ScopeLookup.VisiblePrefix

/-! Generic name-resolution facts shared by every `ScopeLookup` model.
They depend only on `lookup`/`classify` and never on the acceptance predicate. -/
theorem classify_perm {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : classify l₁ = classify l₂ := by
  cases l₁ with
  | nil =>
      have hl : l₂.length = 0 := by simpa using h.length_eq.symm
      have hnil : l₂ = [] := List.length_eq_zero_iff.mp hl
      subst hnil
      rfl
  | cons x xs =>
      cases xs with
      | nil =>
          have hl : l₂.length = 1 := by simpa using h.length_eq.symm
          rcases List.length_eq_one_iff.mp hl with ⟨y, rfl⟩
          have hxy : x = y := by
            have hxmem : x ∈ [y] := h.mem_iff.mp (by simp)
            simpa using hxmem
          subst y
          rfl
      | cons y ys =>
          have hl : 2 ≤ l₂.length := by
            have heq := h.length_eq
            simp at heq ⊢
            omega
          cases l₂ with
          | nil => simp at hl
          | cons z zs =>
              cases zs with
              | nil => simp at hl
              | cons w ws => rfl
theorem hits_perm (m : Model) (q : Name) {a b : List Entry} (h : a.Perm b) :
    (hits m q a).Perm (hits m q b) := by
  unfold hits
  exact (h.filter (fun e => m.accept q e.toName)).map Entry.id
/-! Appending tiers after a scope that already resolves changes nothing. -/
theorem lookup_append_suffix (m : Model) (q : Name) (a b : Env)
    (h : lookup m q a ≠ .missing) : lookup m q (a ++ b) = lookup m q a := by
  induction a with
  | nil =>
      simp [lookup, classify] at h
  | cons t ts ih =>
      by_cases ht : (hits m q t).isEmpty = true
      · have heq : lookup m q (t :: ts) = lookup m q ts := by simp [lookup, ht]
        have h' : lookup m q ts ≠ .missing := by rwa [heq] at h
        have heq' : lookup m q ((t :: ts) ++ b) = lookup m q (ts ++ b) := by
          simp only [List.cons_append]
          simp [lookup, ht]
        rw [heq', heq]
        exact ih h'
      · simp [lookup, ht]
theorem shadowCut_of_model (m : Model) : shadowCut m := by
  intro q a b h
  exact lookup_append_suffix m q a b h
theorem incrementalInvalidate_of_model (m : Model) : incrementalInvalidate m := by
  intro q inner tier e rest h
  rw [lookup_append_suffix m q inner ((e :: tier) :: rest) h,
      lookup_append_suffix m q inner (tier :: rest) h]
theorem withinTier_of_model (m : Model) : withinTier m := by
  intro q a b rest hp
  have hp' := hits_perm m q hp
  have hc := classify_perm hp'
  cases ha : hits m q a with
  | nil =>
      have hb : hits m q b = [] := by
        have hl := hp'.length_eq
        simp [ha] at hl
        cases hb : hits m q b with
        | nil => rfl
        | cons x xs => simp [hb] at hl
      simp [lookup, ha, hb]
  | cons x xs =>
      cases hb : hits m q b with
      | nil =>
          have hl := hp'.length_eq
          simp [ha, hb] at hl
      | cons y ys =>
          have hc' : classify (x :: xs) = classify (y :: ys) := by
            simpa [ha, hb] using hc
          simp [lookup, ha, hb, hc']
theorem lookup_of_hitmaps_eq (m : Model) (q : Name) (e₁ e₂ : Env)
    (h : e₁.map (hits m q) = e₂.map (hits m q)) : lookup m q e₁ = lookup m q e₂ := by
  unfold lookup
  rw [h]
/-! If a generated posting key is sound, filtering by it first is lossless. -/
theorem indexedScope_of_posting (m : Model) (h : posting m) : indexedScope m := by
  intro q env
  have hfilter : ∀ tier : List Entry,
      hits m q (tier.filter (fun e => m.key e.toName == m.key q)) = hits m q tier := by
    intro tier
    unfold hits
    rw [List.filter_filter]
    congr 1
    apply List.filter_congr
    intro e he
    by_cases hac : m.accept q e.toName = true
    · have hk := h q e.toName hac
      simp [hac, hk]
    · simp [hac]
  have hmap : env.map (fun tier => hits m q (tier.filter (fun e => m.key e.toName == m.key q))) =
      env.map (hits m q) := by
    induction env with
    | nil => rfl
    | cons t ts ih => simp [hfilter]
  apply lookup_of_hitmaps_eq
  unfold posted
  rw [List.map_map]
  exact hmap
/-! Equal acceptance predicates give equal lookups. -/
theorem lookup_eq_of_accept_eq (m : Model) (p q : Name)
    (h : ∀ e : Name, m.accept p e = m.accept q e) (env : Env) :
    lookup m p env = lookup m q env := by
  unfold lookup
  have hmap : env.map (hits m p) = env.map (hits m q) := by
    induction env with
    | nil => rfl
    | cons t ts ih => simp [hits, h, ih]
  rw [hmap]
theorem nat_eq_of_toString_eq {a b : Nat} (h : toString a = toString b) : a = b := by
  have h1 : (toString a).toNat? = some a := by simp
  have h2 : (toString b).toNat? = some b := by simp
  have hs : (toString b).toNat? = some a := by simpa [h] using h1
  exact Option.some.inj (hs.symm.trans h2)
theorem sound : VisiblePrefix.WithinTier.Goal := by
  exact withinTier_of_model VisiblePrefix.model

end TinylambOpt.ResearchProofs.LayerContracts.ScopeLookup.Goal_VisiblePrefix_WithinTier
