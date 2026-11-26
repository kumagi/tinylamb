import TinylambOpt.ResearchGoals.JitFrontiers

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Generic contract implications for the WordPromises model. Every model in
`TinylambOpt.ResearchGoals.JitFrontiers.WordPromises` instantiates the shared
`Op` contract; these lemmas turn a model's `guardExact` into the seven derived
goals and give a counterexample template for `unguarded`. -/

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Common

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-- Bool equality as an `Iff` (coercion-safe bridge). -/
theorem beq_iff (a b : Bool) : (a = b) ↔ (a ↔ b) := by cases a <;> cases b <;> simp

theorem not_eq_true' {b : Bool} : ¬(b = true) → b = false := by
  cases h : b <;> simp_all

/-- From `guardExact`, the IR result exposes the raw word exactly on passing
inputs, and is poison otherwise. -/
theorem ir_eq_some (o : Op) (hgo : guardExact o) (p : Args) (hp : passes o p) :
    ir o p = some (o.raw p.1 p.2) := by
  have hd : o.defined p.1 p.2 = true := by
    unfold passes at hp
    rw [hgo p.1 p.2] at hp
    exact hp
  show ir o p = some (o.raw p.1 p.2)
  unfold ir
  rw [if_pos hd]

theorem ir_eq_none (o : Op) (hgo : guardExact o) (p : Args) (hp : ¬ passes o p) :
    ir o p = none := by
  have hd : ¬(o.defined p.1 p.2 = true) := by
    unfold passes at hp
    exact fun hde => hp ((hgo p.1 p.2).symm ▸ hde)
  show ir o p = none
  unfold ir
  rw [if_neg hd]

theorem passes_eq_defined (o : Op) (hgo : guardExact o) (p : Args) :
    passes o p = o.defined p.1 p.2 := by
  unfold passes
  exact hgo p.1 p.2

theorem dropFlag_of_guardExact (o : Op) : guardExact o → dropFlag o := by
  intro hgo p hp
  show ir o p = some (o.raw p.1 p.2)
  exact ir_eq_some o hgo p hp

theorem speculateSelect_of_guardExact (o : Op) : guardExact o → speculateSelect o := by
  intro hgo active p defval H
  cases active with
  | false => unfold takeBranch; simp
  | true =>
    have hdp : ir o p = some (o.raw p.1 p.2) :=
      ir_eq_some o hgo p (H rfl)
    unfold takeBranch
    rw [hdp]
    simp

theorem firstPoison_of_guardExact (o : Op) : guardExact o → firstPoison o := by
  intro hgo ps live
  refine congrArg (fun f => List.findIdx f ps.zipIdx) (funext fun x => ?_)
  rcases x with ⟨p, i⟩
  have key : (ir o p).isNone = !(passes o p) := by
    rw [beq_iff]
    unfold ir passes
    by_cases hd : o.defined p.1 p.2 = true
    · rw [if_pos hd]
      have ht : o.test p.1 p.2 = true := by
        have := hgo p.1 p.2
        rwa [this]
      rw [ht]
      simp
    · rw [if_neg hd]
      have hgo' := hgo p.1 p.2
      have ht : ¬(o.test p.1 p.2 = true) := fun h => hd (hgo' ▸ h)
      refine ⟨fun _ => by simpa [Bool.not_eq_true] using ht, fun _ => by simp⟩
  show (live i && (ir o p).isNone) = (live i && !(passes o p))
  rw [key]

theorem hoistAcrossPhi_of_guardExact (o : Op) : guardExact o → hoistAcrossPhi o := by
  intro hgo pick a b hp
  cases pick with
  | false =>
    unfold takeBranch
    simp
    exact ir_eq_some o hgo b hp
  | true =>
    unfold takeBranch
    simp
    exact ir_eq_some o hgo a hp

theorem freezeInvariant_of_guardExact (o : Op) : guardExact o → freezeInvariant o := by
  intro hgo p
  by_cases hd : o.defined p.1 p.2 = true
  · have hp : passes o p = true := by
      rw [passes_eq_defined o hgo p]
      exact hd
    have hir : ir o p = some (o.raw p.1 p.2) :=
      ir_eq_some o hgo p hp
    constructor
    · intro _
      exact hp
    · intro _ a b
      simp [freeze, hir]
  · have hdf : o.defined p.1 p.2 = false := not_eq_true' hd
    have hnone : ir o p = none :=
      ir_eq_none o hgo p (fun hh => hd (passes_eq_defined o hgo p ▸ hh))
    constructor
    · intro h
      have h01 := h 0 1
      rw [hnone] at h01
      exact absurd h01 (by simp [freeze])
    · intro hh
      exact absurd hh (fun heq => hd (passes_eq_defined o hgo p ▸ heq))

theorem deadInputs_of_guardExact (o : Op) : guardExact o → deadInputs o := by
  intro hgo a b ids hall hpass
  induction ids with
  | nil => rfl
  | cons i rest ih =>
    simp only [List.mem_cons, forall_eq_or_imp, List.map_cons] at hall hpass ⊢
    obtain ⟨hp, passR⟩ := hpass
    obtain ⟨heq, hallR⟩ := hall
    have hir : ir o (a i) = some (o.raw (a i).1 (a i).2) :=
      ir_eq_some o hgo (a i) hp
    rw [hir, heq, ih hallR passR]

theorem not_unguarded_of (o : Op) (p : Args) (h : o.defined p.1 p.2 = false) :
    ¬ unguarded o := by
  intro hU
  have := hU p
  unfold ir at this
  rw [if_neg (by rw [h]; exact Bool.false_ne_true)] at this
  exact absurd this (by simp)

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Common
