import TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_RetireLocal

open TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

private theorem edge_retire_self (g : Graph) (v : Nat) : edge (retire g v) v = none := by
  let P : Edge → Bool := fun e => (e.waiter != v && e.holder != v) && e.waiter == v
  have hP : ∀ e, P e = false := by
    intro e
    by_cases hw : e.waiter == v
    · have heq : e.waiter = v := beq_iff_eq.mp hw
      simp [P, heq]
    · simp [P, hw]
  have hfind : ∀ l : Graph, l.find? P = none := by
    intro l
    induction l with
    | nil => simp
    | cons f fs ih => simp [hP f, ih]
  simpa [edge, retire, List.find?_filter, P] using hfind g

private theorem mem_zip_of_length {xs ys : List Nat} (hlen : xs.length = ys.length)
    {v : Nat} (hv : v ∈ xs) : ∃ b, (v, b) ∈ xs.zip ys := by
  induction xs generalizing ys with
  | nil => simp at hv
  | cons x xs ih =>
      cases ys with
      | nil => simp at hlen
      | cons y ys =>
          simp only [List.length_cons] at hlen
          have htail : xs.length = ys.length := by omega
          simp at hv
          rcases hv with rfl | hv
          · exact ⟨y, by simp⟩
          · rcases ih htail hv with ⟨b, hb⟩
            exact ⟨b, by simp [hb]⟩

private theorem length_drop_take (xs : List Nat) (hne : xs ≠ []) :
    (xs.drop 1 ++ xs.take 1).length = xs.length := by
  cases xs with
  | nil => contradiction
  | cons x xs => simp

private theorem mem_pair_of_mem_cycle {xs : List Nat} {v : Nat} (hne : xs ≠ [])
    (hv : v ∈ xs) : ∃ b, (v, b) ∈ xs.zip (xs.drop 1 ++ xs.take 1) :=
  mem_zip_of_length (length_drop_take xs hne).symm hv

private theorem mem_zip_fst {α β} {xs : List α} {ys : List β} {a : α} {b : β}
    (h : (a, b) ∈ xs.zip ys) : a ∈ xs := by
  induction xs generalizing ys with
  | nil => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp at h
          rcases h with ⟨_, _⟩ | h
          · subst a
            simp
          · exact List.mem_cons_of_mem x (ih h)

private theorem mem_zip_snd {α β} {xs : List α} {ys : List β} {a : α} {b : β}
    (h : (a, b) ∈ xs.zip ys) : b ∈ ys := by
  induction ys generalizing xs with
  | nil => simp at h
  | cons y ys ih =>
      cases xs with
      | nil => simp at h
      | cons x xs =>
          simp at h
          rcases h with ⟨_, _⟩ | h
          · subst b
            simp
          · exact List.mem_cons_of_mem y (ih h)

private theorem mem_drop_take {xs : List Nat} {b : Nat}
    (h : b ∈ xs.drop 1 ++ xs.take 1) : b ∈ xs := by
  cases xs with
  | nil => simp at h
  | cons x xs =>
      simp at h ⊢
      simpa [or_comm] using h

private theorem edge_retire_eq (g : Graph) (a v : Nat) (e : Edge)
    (hfind : edge g a = some e) (hew : e.waiter ≠ v) (heh : e.holder ≠ v) :
    edge (retire g v) a = some e := by
  rw [edge, retire, List.find?_filter]
  apply List.find?_eq_some_iff_append.mpr
  have hfind' : g.find? (fun f => f.waiter == a) = some e := by
    simpa [edge] using hfind
  rcases (List.find?_eq_some_iff_append.mp hfind') with ⟨hpred, as, bs, hg, hprefix⟩
  refine ⟨?_, as, bs, hg, ?_⟩
  · simp [hew, heh, hpred]
  · intro f hf
    have hfwa : (f.waiter == a) = false := by
      have h := hprefix f hf
      cases hb : f.waiter == a <;> simp_all
    simp [hfwa]

theorem sound : RetireLocal.Goal := by
  intro g xs v hnod hcyc
  rcases hcyc with ⟨hne, hnd, hchain⟩
  constructor
  · intro hsurvive hvxs
    rcases hsurvive with ⟨_, _, hchain'⟩
    rcases mem_pair_of_mem_cycle hne hvxs with ⟨b, hpair⟩
    have hall := List.all_eq_true.mp hchain'
    have hany : (edge (retire g v) v).any (fun e => e.holder == b) = true := by
      simpa using hall (v, b) hpair
    have hnone := edge_retire_self g v
    rw [hnone] at hany
    simp at hany
  · intro hvnot
    refine ⟨hne, hnd, ?_⟩
    apply List.all_eq_true.mpr
    intro p hp
    rcases p with ⟨a, b⟩
    have ha_in : a ∈ xs := mem_zip_fst hp
    have hb_in : b ∈ xs := mem_drop_take (mem_zip_snd hp)
    have hane : a ≠ v := by
      intro h
      exact hvnot (h ▸ ha_in)
    have hbne : b ≠ v := by
      intro h
      exact hvnot (h ▸ hb_in)
    have hall := List.all_eq_true.mp hchain
    have hany : (edge g a).any (fun e => e.holder == b) = true := by
      simpa using hall (a, b) hp
    rcases (Option.any_eq_true (fun e : Edge => e.holder == b) (edge g a)).mp hany with
      ⟨e, hfind, hehb⟩
    have hwa : (e.waiter == a) = true := by
      have hf : g.find? (fun f => f.waiter == a) = some e := by simpa [edge] using hfind
      exact (List.find?_eq_some_iff_append.mp hf).1
    have hew : e.waiter ≠ v := by
      intro hh
      apply hane
      exact (beq_iff_eq.mp hwa).symm.trans hh
    have heh : e.holder ≠ v := by
      intro hh
      apply hbne
      exact (beq_iff_eq.mp hehb).symm.trans hh
    have hfind' := edge_retire_eq g a v e hfind hew heh
    simp [hfind', hehb]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_RetireLocal
