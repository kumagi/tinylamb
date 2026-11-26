import TinylambOpt.ResearchGoals.RestartRaces.Catalog

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `ShadowBind` — Optimistic cache reuse from partial revalidation: checking the old
  search prefix suffices if it still resolves to the old descriptor. If its winner
  vanishes, require the unread suffix to be empty or otherwise certified. Here
  quantify over every possible suffix binding on fresh namespace keys.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_ShadowBind
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.ShadowBind

theorem prefix_subset (view : Nat → Option Nat) (path : List Nat) :
    ∀ x ∈ prefixThrough view path, x ∈ path := by
  induction path with
  | nil => simp [prefixThrough]
  | cons k ks ih =>
      intro x hx
      by_cases hv : (view k).isSome
      · simp [prefixThrough, hv] at hx
        subst x
        simp
      · simp [prefixThrough, hv] at hx ⊢
        rcases hx with hxk | hx
        · exact Or.inl hxk
        · exact Or.inr (ih x hx)

theorem prefix_eq_path_of_resolve_none (view : Nat → Option Nat) (path : List Nat)
    (h : resolve view path = none) : prefixThrough view path = path := by
  induction path with
  | nil => simp [prefixThrough]
  | cons k ks ih =>
      have hvk : view k = none := by
        cases hk : view k with
        | none => rfl
        | some v => simp [resolve, hk] at h
      have hks : resolve view ks = none := by simpa [resolve, hvk] using h
      simp [prefixThrough, hvk, ih hks]

theorem disj_trivial (view : Nat → Option Nat) (path : List Nat) :
    (resolve view path).isSome ∨ prefixThrough view path = path := by
  by_cases h : (resolve view path).isSome
  · exact Or.inl h
  · right
    apply prefix_eq_path_of_resolve_none
    cases hres : resolve view path <;> simp_all

theorem resolve_none (l : List Nat) : resolve (fun _ : Nat => none) l = none := by
  simp [resolve, List.findSome?_eq_none_iff]

theorem resolve_eq_of_eq_on (f g : Nat → Option Nat) (l : List Nat)
    (h : ∀ x ∈ l, f x = g x) : resolve f l = resolve g l := by
  induction l with
  | nil => simp [resolve]
  | cons a as ih =>
      by_cases hfa : (f a).isSome
      · have hga : g a = f a := (h a (by simp)).symm
        simp [resolve, hfa, hga]
      · have hfa' : f a = none := by cases hres : f a <;> simp_all
        have hga' : g a = none := by rw [← h a (by simp)]; exact hfa'
        have ht : ∀ x ∈ as, f x = g x := by
          intro x hx
          exact h x (by simp [hx])
        rw [show resolve f (a :: as) = resolve f as by simp [resolve, hfa']]
        rw [show resolve g (a :: as) = resolve g as by simp [resolve, hga']]
        exact ih ht

theorem sound : Goal := by
  intro old live path hnodup
  induction path generalizing old live with
  | nil => simp [prefixThrough, resolve]
  | cons k ks ih =>
      have ih' :
          (resolve live (prefixThrough old ks) = resolve old ks) ↔
            (∀ completion : Catalog,
              (∀ x ∈ prefixThrough old ks, live x = completion x) →
                resolve completion ks = resolve old ks) := by
        have h := ih old live hnodup.tail
        have hd := disj_trivial old ks
        simpa [hd] using h
      have hnd : ¬ k ∈ ks ∧ ks.Nodup := by simpa using hnodup
      have hk_notin : k ∉ ks := hnd.1
      have hD' : (resolve old ks).isSome ∨ k :: prefixThrough old ks = k :: ks := by
        rcases disj_trivial old ks with hpos | heq
        · exact Or.inl hpos
        · exact Or.inr (by simp [heq])
      cases hOk : old k with
      | some v =>
          have hChecked : prefixThrough old (k :: ks) = [k] := by
            simp [prefixThrough, hOk]
          have hOldAns : resolve old (k :: ks) = some v := by
            simp [resolve, hOk]
          rw [hChecked, hOldAns] at ⊢
          constructor
          · intro h
            have hres : live k = some v := by
              simpa [resolve] using h.1
            intro completion hc
            have hck : completion k = live k := (hc k (by simp)).symm
            simpa [resolve, hck, hres]
          · intro hR
            have hres : live k = some v := by
              let completion : Catalog := fun j => if j == k then live k else none
              have hc : ∀ x ∈ [k], live x = completion x := by
                intro x hx
                simp at hx
                subst x
                simp [completion]
              have hr := hR completion hc
              by_cases hl : live k = none
              · have hnone : resolve completion ks = none := by
                  simp [resolve, completion, hl]
                have htmp : resolve (fun _ : Nat => none) ks = some v := by
                  simpa [resolve, completion, hl, hnone] using hr
                rw [resolve_none] at htmp
                cases htmp
              · rcases hlive : live k with _ | w
                · exact False.elim (hl hlive)
                · have hrescomp : resolve completion (k :: ks) = some w := by
                    simp [resolve, completion, hlive]
                  rw [hrescomp] at hr
                  injection hr with hwv
                  simpa [hwv] using hlive
            exact ⟨by simpa [resolve, hres], Or.inl (by simp)⟩
      | none =>
          have hChecked : prefixThrough old (k :: ks) = k :: prefixThrough old ks := by
            simp [prefixThrough, hOk]
          have hOldAns : resolve old (k :: ks) = resolve old ks := by
            simp [resolve, hOk]
          rw [hChecked, hOldAns] at ⊢
          cases hLive : live k with
          | some w =>
              constructor
              · intro h
                have hw : some w = resolve old ks := by
                  simpa [resolve, hLive] using h.1
                intro completion hc
                have hck : completion k = some w := by
                  have := (hc k (by simp)).symm
                  simpa [hLive] using this
                simpa [resolve, hck, hw.symm]
              · intro hR
                have hw : some w = resolve old ks := by
                  have hr := hR live (by intro x hx; rfl)
                  simpa [resolve, hLive] using hr
                exact ⟨by simpa [resolve, hLive, hw.symm], hD'⟩
          | none =>
              have convert :
                  (∀ completion : Catalog,
                    (∀ x ∈ prefixThrough old ks, live x = completion x) →
                      resolve completion ks = resolve old ks) →
                  (∀ completion : Catalog,
                    (∀ x ∈ k :: prefixThrough old ks, live x = completion x) →
                      resolve completion (k :: ks) = resolve old ks) := by
                intro hRbase completion hc
                let comp' : Catalog := fun x => if x == k then live x else completion x
                have hc' : ∀ x ∈ k :: prefixThrough old ks, live x = comp' x := by
                  intro x hx
                  simp at hx
                  rcases hx with hxk | hx
                  · subst x
                    simp [comp', hLive]
                  · have hxne : x ≠ k := by
                      intro heq
                      exact hk_notin (by simpa [heq] using (prefix_subset old ks x hx))
                    simpa [comp', hxne] using hc x (by simp [hx])
                have hc'' : ∀ x ∈ prefixThrough old ks, live x = comp' x := by
                  intro x hx
                  exact hc' x (by simp [hx])
                have hr := hRbase comp' hc''
                have hcomp'ks : resolve comp' ks = resolve completion ks := by
                  apply resolve_eq_of_eq_on
                  intro x hx
                  have hxne : x ≠ k := by
                    intro heq
                    exact hk_notin (by simpa [heq] using hx)
                  simp [comp', hxne]
                rw [hcomp'ks] at hr
                have hck : completion k = none := by
                  have := (hc k (by simp)).symm
                  simpa [hLive] using this
                simpa [resolve, hck] using hr
              constructor
              · intro h
                have hL : resolve live (prefixThrough old ks) = resolve old ks := by
                  simpa [resolve, hLive] using h.1
                exact convert (ih'.1 hL)
              · intro hR
                have hR' : ∀ completion : Catalog,
                    (∀ x ∈ prefixThrough old ks, live x = completion x) →
                      resolve completion ks = resolve old ks := by
                  intro completion hc
                  let comp' : Catalog := fun x => if x == k then live x else completion x
                  have hc' : ∀ x ∈ k :: prefixThrough old ks, live x = comp' x := by
                    intro x hx
                    simp at hx
                    rcases hx with hxk | hx
                    · subst x
                      simp [comp', hLive]
                    · have hxne : x ≠ k := by
                        intro heq
                        exact hk_notin (by simpa [heq] using (prefix_subset old ks x hx))
                      simpa [comp', hxne] using hc x hx
                  have hr := hR comp' hc'
                  have hcomp'ks : resolve comp' ks = resolve completion ks := by
                    apply resolve_eq_of_eq_on
                    intro x hx
                    have hxne : x ≠ k := by
                      intro heq
                      exact hk_notin (by simpa [heq] using hx)
                    simp [comp', hxne]
                  have hr' : resolve comp' ks = resolve old ks := by
                    simpa [resolve, comp', hLive] using hr
                  rw [hcomp'ks] at hr'
                  exact hr'
                exact ⟨by simpa [resolve, hLive] using ih'.2 hR', hD'⟩
