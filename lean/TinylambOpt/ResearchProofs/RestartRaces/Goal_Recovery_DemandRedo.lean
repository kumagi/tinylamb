import TinylambOpt.ResearchGoals.RestartRaces.Recovery

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `DemandRedo` — Instant restart from a sliced checkpoint and WAL: the surviving base
  keys are precisely the initial values that can influence the requested output.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_DemandRedo

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.DemandRedo

private def applyLog (l : List Edit) (s : Nat → Nat) : Nat → Nat := l.foldl step s

private theorem applyLog_single (e : Edit) (s : Nat → Nat) : applyLog [e] s = step s e := rfl

private theorem applyLog_append (l1 l2 : List Edit) (s : Nat → Nat) :
    applyLog (l1 ++ l2) s = applyLog l2 (applyLog l1 s) := by
  unfold applyLog
  rw [List.foldl_append]

private theorem step_ne (s : Nat → Nat) (e : Edit) (k : Nat) (h : k ≠ e.dst) :
    step s e k = s k := by
  simp [step, h]

private theorem step_lit (s : Nat → Nat) (dst n : Nat) :
    step s ⟨dst, .literal n⟩ dst = n := by
  simp [step]

private theorem step_copy (s : Nat → Nat) (dst src : Nat) :
    step s ⟨dst, .copy src⟩ dst = s src := by
  simp [step]


private theorem applyLog_append_single (l : List Edit) (e : Edit) (s : Nat → Nat) :
    applyLog (l ++ [e]) s = step (applyLog l s) e := by
  rw [applyLog_append, applyLog_single]

private theorem slice_properties (r : List Edit) :
    ∀ (K : List Nat) (s t : Nat → Nat) (d : Nat),
      let (deps, sel) := slice K r
      (∀ k ∈ K, applyLog r.reverse s k = applyLog sel.reverse s k) ∧
      (∀ k ∈ K, (∀ x ∈ deps, s x = t x) → applyLog sel.reverse s k = applyLog sel.reverse t k) ∧
      (d ∈ deps → (∀ j, j ≠ d → s j = t j) → s d ≠ t d →
        ∃ k ∈ K, applyLog r.reverse s k ≠ applyLog r.reverse t k) := by
  induction r with
  | nil =>
      intro K s t d
      simp [slice, applyLog]
      constructor
      · intro k hk hagree
        exact hagree k hk
      · intro hdeps hother hdiff
        exact ⟨d, hdeps, hdiff⟩
  | cons e er ih =>
      intro K s t d
      cases e with
      | mk dst val =>
          by_cases hdst : dst ∈ K
          · cases hval : val with
            | literal n =>
                let rest := K.filter (fun k => k != dst)
                have hrest : ∀ k, k ∈ rest ↔ k ∈ K ∧ k ≠ dst := by intro k; simp [rest]
                have hslice : slice K (⟨dst, .literal n⟩ :: er) =
                    (let pair := slice rest er; (pair.1, ⟨dst, .literal n⟩ :: pair.2)) := by
                  simp [slice, hdst, rest]
                rw [hslice]
                dsimp
                dsimp [rest]
                have ihrest := ih rest s t d
                dsimp at ihrest
                rcases ihrest with ⟨ih1, ih2, ih3⟩
                have hrev : (⟨dst, .literal n⟩ :: er).reverse = er.reverse ++ [⟨dst, .literal n⟩] := by simp
                have hselrev : (⟨dst, .literal n⟩ :: (slice rest er).2).reverse =
                    (slice rest er).2.reverse ++ [⟨dst, .literal n⟩] := by simp
                constructor
                · intro k hk
                  rw [hrev, hselrev]
                  repeat rw [applyLog_append_single]
                  by_cases hke : k = dst
                  · subst k
                    simp [step]
                  · have hkrest : k ∈ rest := (hrest k).mpr ⟨hk, hke⟩
                    rw [step_ne _ _ k hke, step_ne _ _ k hke]
                    exact ih1 k hkrest
                constructor
                · intro k hk hagree
                  rw [hselrev]
                  repeat rw [applyLog_append_single]
                  by_cases hke : k = dst
                  · subst k
                    simp [step]
                  · have hkrest : k ∈ rest := (hrest k).mpr ⟨hk, hke⟩
                    rw [step_ne _ _ k hke, step_ne _ _ k hke]
                    exact ih2 k hkrest hagree
                · intro hdeps hother hdiff
                  rcases ih3 hdeps hother hdiff with ⟨k, hkrest, hkdiff⟩
                  refine ⟨k, (hrest k).mp hkrest |>.1, ?_⟩
                  have hke : k ≠ dst := (hrest k).mp hkrest |>.2
                  rw [hrev]
                  repeat rw [applyLog_append_single]
                  rw [step_ne _ _ k hke, step_ne _ _ k hke]
                  exact hkdiff
            | copy src =>
                let rest := K.filter (fun k => k != dst)
                let before := src :: rest
                have hbefore : ∀ k, k ∈ before ↔ k = src ∨ (k ∈ K ∧ k ≠ dst) := by
                  intro k
                  simp [before, rest]
                have hslice : slice K (⟨dst, .copy src⟩ :: er) =
                    (let pair := slice before er; (pair.1, ⟨dst, .copy src⟩ :: pair.2)) := by
                  simp [slice, hdst, before, rest]
                rw [hslice]
                dsimp
                dsimp [before, rest]
                have ihbefore := ih before s t d
                dsimp at ihbefore
                rcases ihbefore with ⟨ih1, ih2, ih3⟩
                have hrev : (⟨dst, .copy src⟩ :: er).reverse = er.reverse ++ [⟨dst, .copy src⟩] := by simp
                have hselrev : (⟨dst, .copy src⟩ :: (slice before er).2).reverse =
                    (slice before er).2.reverse ++ [⟨dst, .copy src⟩] := by simp
                constructor
                · intro k hk
                  rw [hrev, hselrev]
                  repeat rw [applyLog_append_single]
                  by_cases hke : k = dst
                  · subst k
                    simp [step]
                    exact ih1 src ((hbefore src).mpr (Or.inl rfl))
                  · rw [step_ne _ _ k hke, step_ne _ _ k hke]
                    exact ih1 k ((hbefore k).mpr (Or.inr ⟨hk, hke⟩))
                constructor
                · intro k hk hagree
                  rw [hselrev]
                  repeat rw [applyLog_append_single]
                  by_cases hke : k = dst
                  · subst k
                    simp [step]
                    exact ih2 src ((hbefore src).mpr (Or.inl rfl)) hagree
                  · rw [step_ne _ _ k hke, step_ne _ _ k hke]
                    exact ih2 k ((hbefore k).mpr (Or.inr ⟨hk, hke⟩)) hagree
                · intro hdeps hother hdiff
                  rcases ih3 hdeps hother hdiff with ⟨k, hkbefore, hkdiff⟩
                  rcases (hbefore k).mp hkbefore with hksrc | hkrest
                  · subst k
                    refine ⟨dst, hdst, ?_⟩
                    rw [hrev]
                    repeat rw [applyLog_append_single]
                    simp [step]
                    exact hkdiff
                  · refine ⟨k, hkrest.1, ?_⟩
                    rw [hrev]
                    repeat rw [applyLog_append_single]
                    rw [step_ne _ _ k hkrest.2, step_ne _ _ k hkrest.2]
                    exact hkdiff
          · have hslice : slice K (⟨dst, val⟩ :: er) = slice K er := by
              simp [slice, hdst]
            rw [hslice]
            dsimp
            have ihK := ih K s t d
            dsimp at ihK
            rcases ihK with ⟨ih1, ih2, ih3⟩
            have hrev : (⟨dst, val⟩ :: er).reverse = er.reverse ++ [⟨dst, val⟩] := by simp
            constructor
            · intro k hk
              rw [hrev]
              repeat rw [applyLog_append_single]
              have hke : k ≠ dst := by intro hkeq; apply hdst; simpa [hkeq] using hk
              rw [step_ne _ _ k hke]
              exact ih1 k hk
            constructor
            · exact ih2
            · intro hdeps hother hdiff
              rcases ih3 hdeps hother hdiff with ⟨k, hk, hkdiff⟩
              refine ⟨k, hk, ?_⟩
              rw [hrev]
              repeat rw [applyLog_append_single]
              have hke : k ≠ dst := by intro hkeq; apply hdst; simpa [hkeq] using hk
              rw [step_ne _ _ k hke, step_ne _ _ k hke]
              exact hkdiff

private theorem map_eq_pointwise {α β : Type} (l : List α) (f g : α → β)
    (h : l.map f = l.map g) : ∀ x ∈ l, f x = g x := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      intro x hx
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact h.1
      · exact ih h.2 x hx

theorem sound : Goal := by
  intro log wanted
  let rev := log.reverse
  let pair := slice wanted rev
  let deps := pair.1
  let sel := pair.2
  constructor
  · intro s
    have ps := slice_properties rev wanted s s 0
    dsimp [deps, sel, pair] at ps ⊢
    rcases ps with ⟨ps1, _, _⟩
    apply List.map_congr_left
    intro k hk
    have h := ps1 k hk
    simpa [rev, applyLog] using h.symm
  · intro loaded
    constructor
    · intro hload s t hst
      have ps := slice_properties rev wanted s s 0
      have pt := slice_properties rev wanted t t 0
      have pst := slice_properties rev wanted s t 0
      dsimp [deps, sel, pair] at ps pt pst ⊢
      rcases ps with ⟨ps1, _, _⟩
      rcases pt with ⟨pt1, _, _⟩
      rcases pst with ⟨_, pst2, _⟩
      apply List.map_congr_left
      intro k hk
      have hsel := pst2 k hk (by
        intro x hx
        exact hst x (hload x hx))
      have hs : applyLog log s k = applyLog sel.reverse s k := by
        simpa [rev, applyLog] using ps1 k hk
      have ht : applyLog log t k = applyLog sel.reverse t k := by
        simpa [rev, applyLog] using pt1 k hk
      change applyLog log s k = applyLog log t k
      rw [hs, ht]
      exact hsel
    · intro hEq k hkdep
      by_cases hkload : k ∈ loaded
      · exact hkload
      · exfalso
        let s : Nat → Nat := fun _ => 0
        let t : Nat → Nat := fun j => if j == k then 1 else 0
        have hagree : ∀ j ∈ loaded, s j = t j := by
          intro j hj
          by_cases hjk : j = k
          · subst j
            exact (hkload hj).elim
          · simp [s, t, hjk]
        have hout := hEq s t hagree
        have pst := slice_properties rev wanted s t k
        dsimp [deps, sel, pair] at pst
        rcases pst with ⟨_, _, pst3⟩
        have hdiffk : s k ≠ t k := by simp [s, t]
        rcases pst3 hkdep (by
          intro j hjne
          by_cases hjk : j = k
          · exact (hjne hjk).elim
          · simp [s, t, hjk]) hdiffk with ⟨w, hw, hwdiff⟩
        have hwEq : applyLog log s w = applyLog log t w := by
          have hpm := map_eq_pointwise wanted (applyLog log s) (applyLog log t) (by
            simpa [applyLog] using hout) w hw
          exact hpm
        have hwEq' : applyLog rev.reverse s w = applyLog rev.reverse t w := by
          have hrevlog : rev.reverse = log := by simp [rev]
          simpa [hrevlog] using hwEq
        exact hwdiff hwEq'

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_DemandRedo
