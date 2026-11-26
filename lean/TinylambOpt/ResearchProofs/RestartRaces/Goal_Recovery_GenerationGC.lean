import TinylambOpt.ResearchGoals.RestartRaces.Recovery
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_GenerationGC

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.GenerationGC

private def eligible (s : Cut) (g : Gen) : Bool :=
  s.manifests.contains g.id && s.walStart ≤ g.anchor && g.anchor ≤ s.walEnd &&
    g.files.all s.durableFiles.contains

private theorem find?_mem {α : Type} (l : List α) (p : α → Bool) {a : α}
    (h : l.find? p = some a) : a ∈ l := by
  induction l with
  | nil => simp [List.find?] at h
  | cons x xs ih =>
      simp only [List.find?_cons] at h
      by_cases hp : p x = true
      · simp [hp] at h
        simp [h]
      · simp [hp] at h
        simp [ih h]

private theorem nodup_id_eq (gs : List Gen) (hnod : (gs.map Gen.id).Nodup)
    {g g' : Gen} (hg : g ∈ gs) (hg' : g' ∈ gs) (hid : g.id = g'.id) : g = g' := by
  induction gs with
  | nil => simp at hg
  | cons a as ih =>
      change List.Pairwise (fun x y : Nat => x ≠ y) (a.id :: List.map Gen.id as) at hnod
      rw [List.pairwise_cons] at hnod
      simp only [List.mem_cons] at hg hg'
      rcases hg with hga | hg
      · subst g
        rcases hg' with hg'a | hg'
        · subst g'
          rfl
        · have hne : a.id ≠ g'.id := hnod.1 g'.id (List.mem_map_of_mem hg')
          exact (hne hid).elim
      · rcases hg' with hg'a | hg'
        · subst g'
          have hne : a.id ≠ g.id := hnod.1 g.id (List.mem_map_of_mem hg)
          exact (hne hid.symm).elim
        · exact ih hnod.2 hg hg'

private theorem chosen_mem (gs : List Gen) (s : Cut) {g : Gen} (h : chosen gs s = some g) : g ∈ gs := by
  exact find?_mem gs (eligible s) h

private theorem chosen_filter_same (gs : List Gen) (s : Cut) (remove : List Nat)
    (hrem : ∀ g, chosen gs s = some g → ∀ f ∈ g.files, f ∉ remove) :
    chosen gs {s with durableFiles := s.durableFiles.filter (fun f => !remove.contains f)} = chosen gs s := by
  let s' : Cut := {s with durableFiles := s.durableFiles.filter (fun f => !remove.contains f)}
  induction gs with
  | nil => rfl
  | cons g gs' ih =>
      change (g :: gs').find? (eligible s') = (g :: gs').find? (eligible s)
      by_cases hp : eligible s g = true
      · have hch : chosen (g :: gs') s = some g := by
          change (g :: gs').find? (eligible s) = some g
          simp [List.find?_cons, hp]
        have hfiles := hrem g hch
        have hpProp : ((g.id ∈ s.manifests ∧ s.walStart ≤ g.anchor) ∧ g.anchor ≤ s.walEnd) ∧
            ∀ f ∈ g.files, f ∈ s.durableFiles := by
          simpa [eligible] using hp
        have hq : eligible s' g = true := by
          simp [eligible, s']
          exact ⟨hpProp.1, fun f hf => ⟨hpProp.2 f hf, hfiles f hf⟩⟩
        simp [List.find?_cons, hp, hq]
      · have hq_imp_p : ∀ x, eligible s' x = true → eligible s x = true := by
          intro x hx
          have hxProp : ((x.id ∈ s.manifests ∧ s.walStart ≤ x.anchor) ∧ x.anchor ≤ s.walEnd) ∧
              ∀ f ∈ x.files, f ∈ s.durableFiles.filter (fun f => !remove.contains f) := by
            simpa [eligible, s'] using hx
          have : eligible s x = true := by
            simp [eligible]
            exact ⟨hxProp.1, fun f hf => (List.mem_filter.mp (hxProp.2 f hf)).1⟩
          exact this
        have hqg : eligible s' g ≠ true := by intro hq; exact hp (hq_imp_p g hq)
        have hch_eq : chosen gs' s = chosen (g :: gs') s := by
          change gs'.find? (eligible s) = (g :: gs').find? (eligible s)
          simp [List.find?_cons, hp]
        have hrem' : ∀ x, chosen gs' s = some x → ∀ f ∈ x.files, f ∉ remove := by
          intro x hx f hf
          exact (hrem x (by rwa [← hch_eq])) f hf
        have ih' := ih hrem'
        change (g :: gs').find? (eligible s') = (g :: gs').find? (eligible s)
        simp only [List.find?_cons, hqg, hp]
        change gs'.find? (eligible s') = gs'.find? (eligible s) at ih'
        exact ih'

theorem sound : Goal := by
  intro gs cuts remove hnod
  constructor
  · intro hrem s hs
    have h := chosen_filter_same gs s remove (hrem s hs)
    simpa only [h]
  · intro H s hs g hch
    let s' : Cut := {s with durableFiles := s.durableFiles.filter (fun f => !remove.contains f)}
    have hEq : (chosen gs s).map Gen.id = (chosen gs s').map Gen.id := by
      simpa [s'] using H s hs
    have hmap : some g.id = (chosen gs s').map Gen.id := by simpa [hch] using hEq
    have hch' : chosen gs s' = some g := by
      cases hcs' : chosen gs s' with
      | none => simp [hcs'] at hmap
      | some g' =>
          have hid : g.id = g'.id := by simpa [hcs'] using hmap
          have heq : g' = g := nodup_id_eq gs hnod (chosen_mem gs s' hcs') (chosen_mem gs s hch) hid.symm
          simpa [heq] using hcs'
    intro f hf
    have hcfiles' : ∀ x ∈ g.files, x ∈ s'.durableFiles := by
      have hp := List.find?_some hch'
      have hpProp : ((g.id ∈ s'.manifests ∧ s'.walStart ≤ g.anchor) ∧ g.anchor ≤ s'.walEnd) ∧
          ∀ x ∈ g.files, x ∈ s'.durableFiles := by
        simpa [eligible] using hp
      exact hpProp.2
    have hmem_filt : f ∈ s.durableFiles.filter (fun x => !remove.contains x) := by
      simpa [s'] using hcfiles' f hf
    have hnot : remove.contains f = false := by
      have := (List.mem_filter.mp hmem_filt).2
      simpa using this
    intro hfrem
    have hct : remove.contains f = true := by simpa using hfrem
    rw [hct] at hnot
    contradiction

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_GenerationGC
