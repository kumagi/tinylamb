import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CancelMonotoneWait

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! Cancellation deletes tickets from the queue.  The flags ahead of a surviving
ticket form a sublist of the original flags, and counting reader cohorts before
subtracting the surviving ticket's own cohort is monotone under deletion.  This
lets SQL admission reuse a shorter queue without recalculating a larger delay. -/

namespace Scratch.CancelMonotoneWait

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

set_option maxHeartbeats 0

def fm (r : Bool) (flags : List Bool) : Nat :=
  (flags.filter id).length + (flags.zip (true :: flags)).countP (fun p => !p.1 && p.2) -
    (if r = true ∧ flags.getLast? == some false then 1 else 0)

def cohortsP (r prev : Bool) (flags : List Bool) : Nat :=
  (flags.filter id).length + (flags.zip (prev :: flags)).countP (fun p => !p.1 && p.2) +
    (if r = false ∨ flags.getLast?.getD prev = true then 1 else 0)

theorem waitTurns_eq_fm : ∀ (q : List Ticket) (t : Ticket),
    waitTurns q t = fm (!t.writer) ((q.takeWhile (fun x => x != t)).map Ticket.writer) := by
  intro q t
  simp [waitTurns, fm]

theorem fm_eq_cohortsP_sub : ∀ (r : Bool) (flags : List Bool),
    fm r flags = cohortsP r true flags - 1 := by
  intro r flags
  cases flags with
  | nil =>
      cases r <;> simp [fm, cohortsP]
  | cons a l =>
      have hgl : (a :: l).getLast? = some (l.getLast?.getD a) := List.getLast?_cons
      cases hlast : l.getLast?.getD a
      · cases r <;> simp [fm, cohortsP, hgl, hlast]
      · cases r <;> simp [fm, cohortsP, hgl, hlast] <;> omega

theorem waitTurns_eq_cohortsP_sub : ∀ (q : List Ticket) (t : Ticket),
    waitTurns q t =
      cohortsP (!t.writer) true ((q.takeWhile (fun x => x != t)).map Ticket.writer) - 1 := by
  intro q t
  rw [waitTurns_eq_fm, fm_eq_cohortsP_sub]

theorem cohortsP_cons : ∀ (r prev a : Bool) (flags : List Bool),
    cohortsP r prev (a :: flags) =
      (if a = true then 1 else 0) + (if a = false ∧ prev = true then 1 else 0) +
        cohortsP r a flags := by
  intro r prev a flags
  cases flags with
  | nil =>
      cases a <;> cases prev <;> cases r <;> simp [cohortsP, List.getLast?_cons] <;> omega
  | cons b bs =>
      cases a <;> cases prev <;> cases r <;> simp [cohortsP, List.getLast?_cons] <;> omega

theorem cohortsP_cons_ge : ∀ (r a : Bool) (flags : List Bool) (prev : Bool),
    cohortsP r prev flags ≤ cohortsP r prev (a :: flags) := by
  intro r a flags
  cases flags with
  | nil =>
      intro prev
      cases a <;> cases prev <;> cases r <;> simp [cohortsP, List.getLast?_cons] <;> omega
  | cons b bs =>
      cases bs with
      | nil =>
          intro prev
          cases a <;> cases b <;> cases prev <;> cases r <;>
            simp [cohortsP, List.getLast?_cons] <;> omega
      | cons c cs =>
          intro prev
          cases a <;> cases b <;> cases prev <;> cases r <;>
            simp [cohortsP, List.getLast?_cons] <;> omega

theorem cohortsP_sublist : ∀ (r : Bool) {f g : List Bool}, g.Sublist f →
    ∀ (prev : Bool), cohortsP r prev g ≤ cohortsP r prev f := by
  intro r f g h
  induction h with
  | slnil =>
      intro prev
      cases r <;> cases prev <;> simp [cohortsP] <;> omega
  | cons a h ih =>
      rename_i l₁ l₂
      intro prev
      have h1 := ih prev
      have h2 := cohortsP_cons_ge r a l₂ prev
      omega
  | cons_cons a h ih =>
      rename_i g f'
      intro prev
      rw [cohortsP_cons r prev a g, cohortsP_cons r prev a f']
      have hih := ih a
      omega

theorem filteredFlags_sublist : ∀ (p : Ticket → Bool) (t : Ticket), p t = true →
    ∀ (q : List Ticket),
      (((q.filter p).takeWhile (fun x => x != t)).map Ticket.writer).Sublist
        ((q.takeWhile (fun x => x != t)).map Ticket.writer) := by
  intro p t hpt q
  induction q with
  | nil => simp
  | cons x xs ih =>
      by_cases hxt : x = t
      · subst x
        simp [hpt]
      · by_cases hpx : p x = true
        · simp [hpx, hxt]
          exact ih
        · have hpfx : p x = false := by cases h : p x <;> simp_all
          simp [hpfx, hxt]
          exact List.Sublist.cons x.writer ih

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CancelMonotoneWait.Goal := by
  intro q owner gen t ht
  let p : Ticket → Bool := fun x => !(x.owner == owner && x.generation == gen)
  change waitTurns (q.filter p) t ≤ waitTurns q t
  change t ∈ q.filter p at ht
  have hpt : p t = true := (List.mem_filter.mp ht).2
  have hsub := filteredFlags_sublist p t hpt q
  have hmono := cohortsP_sublist (!t.writer) hsub true
  rw [waitTurns_eq_cohortsP_sub, waitTurns_eq_cohortsP_sub]
  exact Nat.sub_le_sub_right hmono 1

end Scratch.CancelMonotoneWait
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CancelMonotoneWait
