import TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

/-!
Concurrency control must account for the modeled wait dependencies of SQL transactions.

Proved

- `LocalDetection` — A fresh wait must belong to any cycle newly created by installing
  it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_LocalDetection

open TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

private theorem find_filter_ne (g : Graph) (w v : Nat) (hw : w ≠ v) :
    (g.filter (fun f => f.waiter != v)).find? (fun f => f.waiter == w) =
      g.find? (fun f => f.waiter == w) := by
  induction g with
  | nil => simp
  | cons f fs ih =>
      by_cases hfv : (f.waiter == v) = true
      · have hpv : (f.waiter != v) = false := by
          simpa using (beq_iff_eq.mp hfv)
        have hneq : f.waiter ≠ w := by
          intro h
          exact hw (h.symm.trans (beq_iff_eq.mp hfv))
        have hfw : (f.waiter == w) = false := beq_eq_false_iff_ne.mpr hneq
        simp [hpv, hfw, ih]
      · have hfvf : (f.waiter == v) = false := by
          cases hb : f.waiter == v <;> simp_all
        have hpv : (f.waiter != v) = true := by
          simpa using congrArg Bool.not hfvf
        by_cases hfw : (f.waiter == w) = true
        · simp [hpv, hfw]
        · have hfwf : (f.waiter == w) = false := by
            cases hb : f.waiter == w <;> simp_all
          simp [hpv, hfwf, ih]

private theorem edge_install_ne (g : Graph) (e : Edge) (w : Nat) (h : w ≠ e.waiter) :
    edge (install g e) w = edge g w := by
  unfold install edge
  have heq : (e.waiter == w) = false := beq_eq_false_iff_ne.mpr (fun hh => h hh.symm)
  simp only [heq, Bool.false_eq_true, List.find?_cons]
  have hff := find_filter_ne g w e.waiter h
  rw [List.find?_filter] at hff
  simpa [beq_iff_eq, Bool.and_eq_true] using hff

private theorem mem_zip_fst {α β} {xs : List α} {ys : List β} {a : α} {b : β}
    (h : (a,b) ∈ xs.zip ys) : a ∈ xs := by
  induction xs generalizing ys with
  | nil => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp at h
          rcases h with h | h
          · rcases h with ⟨_, _⟩
            subst a
            simp
          · exact List.mem_cons_of_mem x (ih h)

theorem sound : LocalDetection.Goal := by
  intro g e xs hnew hold
  by_cases hmem : e.waiter ∈ xs
  · exact hmem
  · exfalso
    apply hold
    rcases hnew with ⟨hne, hnod, hchain⟩
    refine ⟨hne, hnod, ?_⟩
    unfold chain at hchain ⊢
    apply List.all_eq_true.mpr
    intro p hp
    rcases p with ⟨w,b⟩
    have hw : w ∈ xs := mem_zip_fst hp
    have hwne : w ≠ e.waiter := by
      intro hwe
      have : e.waiter ∈ xs := by simpa [hwe] using hw
      exact hmem this
    have hedge := edge_install_ne g e w hwne
    have hpnew := (List.all_eq_true.mp hchain) (w,b) hp
    simpa [hedge] using hpnew

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_LocalDetection
