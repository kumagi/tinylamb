import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CohortWaitCountdown

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! A full coordinator turn removes the head of a queue before the next ticket
is counted.  The remaining wait is exactly one scheduler turn less, which keeps
admission estimates aligned with the tickets that actually remain queued. -/

set_option maxHeartbeats 0
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

def fm (r : Bool) (flags : List Bool) : Nat :=
  (flags.filter id).length + (flags.zip (true::flags)).countP (fun p => !p.1 && p.2) -
    (if r = true ∧ flags.getLast? == some false then 1 else 0)

theorem fm_bound : ∀ (r : Bool) (flags : List Bool),
    (if r = true ∧ flags.getLast? == some false then 1 else 0) ≤
      (flags.filter id).length + (flags.zip (true::flags)).countP (fun p => !p.1 && p.2) := by
  intro r flags
  cases flags with
  | nil => simp
  | cons f fs =>
      cases f with
      | false =>
          by_cases hc : r = true ∧ (false :: fs).getLast? = some false <;> simp [hc] <;> omega
      | true =>
          cases fs with
          | nil => simp
          | cons g gs =>
              have hih := fm_bound r (g :: gs)
              by_cases hc1 : r = true ∧ (true :: g :: gs).getLast? = some false <;>
                by_cases hc2 : r = true ∧ (g :: gs).getLast? = some false <;>
                simp [hc1, hc2] at hih ⊢ <;> omega

theorem fm_true_cons : ∀ (r : Bool) (flags : List Bool), fm r (true :: flags) = fm r flags + 1 := by
  intro r flags
  cases flags with
  | nil => simp [fm]
  | cons f fs =>
      have hb := fm_bound r (f :: fs)
      by_cases hc : r = true ∧ (f :: fs).getLast? = some false <;>
        simp [fm, hc] at hb ⊢ <;> omega

theorem fm_false_false : ∀ (r : Bool) (flags : List Bool),
    (flags = [] ∧ r = true) ∨ flags.head? == some false →
    fm r (false :: flags) = fm r flags := by
  intro r flags h
  cases flags with
  | nil =>
      have hr : r = true := by simpa using h
      subst r
      simp [fm]
  | cons f fs =>
      have hf : f = false := by simpa using h
      subst f
      cases fs with
      | nil => cases r <;> simp [fm]
      | cons g gs =>
          cases r <;> simp [fm]

theorem fm_false_true : ∀ (r : Bool) (flags : List Bool), fm r (false :: true :: flags) = fm r (true :: flags) + 1 := by
  intro r flags
  cases flags with
  | nil => cases r <;> simp [fm]
  | cons f fs =>
      have hb := fm_bound r (f :: fs)
      by_cases hc : r = true ∧ (f :: fs).getLast? = some false <;>
        simp [fm, hc] at hb ⊢ <;> omega

theorem waitTurns_eq_fm : ∀ (q : List Ticket) (t : Ticket),
    waitTurns q t = fm (!t.writer) ((q.takeWhile (fun x => x != t)).map Ticket.writer) := by
  intro q t
  simp [waitTurns, fm]

theorem waitTurns_writer_head : ∀ (x t : Ticket) (ts : List Ticket),
    x.writer = true → x ≠ t → waitTurns (x :: ts) t = waitTurns ts t + 1 := by
  intro x t ts hx hne
  have hne' : (x != t) = true := by simp [hne]
  rw [waitTurns_eq_fm, waitTurns_eq_fm]
  simp [List.takeWhile, hne', hx]
  exact fm_true_cons (!t.writer) _

theorem waitTurns_reader_reader : ∀ (x y t : Ticket) (ts : List Ticket),
    x.writer = false → y.writer = false → x ≠ t → t ∈ y :: ts →
    waitTurns (x :: y :: ts) t = waitTurns (y :: ts) t := by
  intro x y t ts hx hy hne ht
  have hne' : (x != t) = true := by simp [hne]
  by_cases hyt : y = t
  · subst y
    simp [waitTurns_eq_fm, fm, hne', hx, hy]
  · have hyne' : (y != t) = true := by simp [hyt]
    rw [waitTurns_eq_fm, waitTurns_eq_fm]
    simp [List.takeWhile, hne', hx, hyne', hy]
    apply fm_false_false
    right
    simp

theorem waitTurns_reader_writer : ∀ (x y t : Ticket) (ts : List Ticket),
    x.writer = false → y.writer = true → x ≠ t → t ∈ y :: ts →
    waitTurns (x :: y :: ts) t = waitTurns (y :: ts) t + 1 := by
  intro x y t ts hx hy hne ht
  have hne' : (x != t) = true := by simp [hne]
  by_cases hyt : y = t
  · subst y
    simp [waitTurns_eq_fm, fm, hne', hx, hy]
  · have hyne' : (y != t) = true := by simp [hyt]
    rw [waitTurns_eq_fm, waitTurns_eq_fm]
    simp [List.takeWhile, hne', hx, hyne', hy]
    exact fm_false_true (!t.writer) _

theorem not_mem_after_head : ∀ (x : Ticket) (xs : List Ticket),
    (x :: xs).Nodup → x ∉ after (x :: xs) := by
  intro x xs hnd hmem
  have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
  have hkpos : 0 < (turn (x :: xs)).length := by
    by_cases hxw : x.writer = true
    · simp [turn, hxw]
    · have hxf : x.writer = false := by cases h : x.writer <;> simp_all
      simp [turn, hxf]
  cases hk : (turn (x :: xs)).length with
  | zero => omega
  | succ k' =>
      have hmemxs : x ∈ xs.drop k' := by
        simpa [after, hk] using hmem
      exact hxnot (List.mem_of_mem_drop hmemxs)

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CohortWaitCountdown.Goal := by
  intro q t hnodup ht
  induction q generalizing t with
  | nil => simp [after] at ht
  | cons x xs ih =>
      cases xs with
      | nil =>
          simp [after, turn] at ht
      | cons y ys =>
          have htq : t ∈ x :: y :: ys := List.mem_of_mem_drop ht
          have hxne : x ≠ t := by
            intro hxt
            subst t
            exact not_mem_after_head x (y :: ys) hnodup ht
          have htTail : t ∈ y :: ys := by
            simp at htq
            cases htq with
            | inl htx => exact False.elim (hxne htx.symm)
            | inr htail => simpa using htail
          by_cases hxw : x.writer = true
          · have hafter : after (x :: y :: ys) = y :: ys := by
              simp [after, turn, hxw]
            rw [hafter]
            exact waitTurns_writer_head x t (y :: ys) hxw hxne
          · have hxf : x.writer = false := by cases h : x.writer <;> simp_all
            by_cases hyw : y.writer = true
            · have hafter : after (x :: y :: ys) = y :: ys := by
                simp [after, turn, hxf, hyw]
              rw [hafter]
              exact waitTurns_reader_writer x y t ys hxf hyw hxne htTail
            · have hyf : y.writer = false := by cases h : y.writer <;> simp_all
              have hafter : after (x :: y :: ys) = after (y :: ys) := by
                simp [after, turn, hxf, hyf]
              rw [hafter] at ht ⊢
              have ihres := ih t (List.nodup_cons.mp hnodup).2 ht
              have hrr := waitTurns_reader_reader x y t ys hxf hyf hxne htTail
              rw [hrr, ihres]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CohortWaitCountdown
