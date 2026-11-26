import TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

/-! Draining a permutation of the outstanding leases releases exactly one lease
    per acquisition, so the closing table reaches the absent generation with no
    dangling foreign references. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_QuiescentDropDrainsExactMultiset

open TinylambOpt.ResearchGoals.DatabaseCorrectness.CatalogDrop

private theorem foldl_erase_perm (order : List Nat) :
    ∀ acc : List Nat, order.Perm acc → order.foldl (fun a x => a.erase x) acc = [] := by
  induction order with
  | nil => intro acc h; simpa using h
  | cons a tl ih =>
      intro acc h
      simp only [List.foldl_cons]
      exact ih (acc.erase a) (by simpa using h.erase a)

private theorem foldl_release_eq (order : List Nat) (c : Catalog) (gen : Nat)
    (hgen : c.generation = gen) :
    order.foldl (fun t id => release t id gen) c =
      {c with leases := order.foldl (fun ls id => ls.erase id) c.leases} := by
  induction order generalizing c with
  | nil => rfl
  | cons a tl ih =>
      have hfirst : release c a gen = {c with leases := c.leases.erase a} := by
        simp [release, hgen]
      rw [List.foldl_cons, hfirst]
      have hcgen : ({c with leases := c.leases.erase a} : Catalog).generation = gen := by
        simpa using hgen
      simpa using (ih {c with leases := c.leases.erase a} hcgen)

theorem sound : QuiescentDropDrainsExactMultiset.Goal := by
  intro c q order hbegin hperm
  rcases c with ⟨mode, gen, leases, children⟩
  cases mode <;> simp [beginDrop] at hbegin
  rcases hbegin with ⟨_, rfl⟩
  subst children
  have hleases : order.foldl (fun ls id => ls.erase id) leases = [] :=
    foldl_erase_perm order leases hperm
  have hdrained : order.foldl (fun t id => release t id gen)
      { mode := .closing, generation := gen, leases := leases, children := [] } =
      { mode := .closing, generation := gen, leases := [], children := [] } := by
    rw [foldl_release_eq]
    · simp [hleases]
    · rfl
  refine ⟨{ mode := .absent, generation := gen, leases := [], children := [] }, ?_, ?_, ?_⟩
  · simp [finishDrop, hdrained]
  · simp
  · simp

end TinylambOpt.ResearchProofs.DatabaseCorrectness.CatalogDrop.Goal_QuiescentDropDrainsExactMultiset
