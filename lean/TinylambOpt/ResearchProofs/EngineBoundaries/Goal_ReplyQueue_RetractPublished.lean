import TinylambOpt.ResearchGoals.EngineBoundaries.ReplyQueue

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_ReplyQueue_RetractPublished

open TinylambOpt.ResearchGoals.EngineBoundaries.ReplyQueue

private theorem take_drop_append (a b : Nat) (p : List UInt8) :
    p.take a ++ (p.drop a).take b = p.take (a+b) := by
  induction a generalizing p with
  | zero => simp
  | succ a ih =>
      cases p with
      | nil => simp
      | cons x xs =>
          simp
          rw [ih xs]
          rw [show a + 1 + b = (a+b)+1 by omega, List.take_succ_cons]

private theorem take_all {α} (l : List α) (n : Nat) (h : l.length ≤ n) : l.take n = l := by
  induction l generalizing n with
  | nil => cases n <;> rfl
  | cons x xs ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          simp only [List.take_succ_cons]
          congr 1
          apply ih
          simp at h
          omega

private theorem drop_all {α} (l : List α) (n : Nat) (h : l.length ≤ n) : l.drop n = [] := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          have h' : xs.length ≤ n := by
            simp at h
            omega
          simp [ih n h']

private theorem isEmpty_eq (l : List UInt8) : l.isEmpty = decide (l.length = 0) := by
  cases l <;> simp

private theorem foldl_append_eq (q : Queue) (chunks : List Bytes) :
    chunks.foldl append q = {q with pending := q.pending ++ chunks.flatten} := by
  induction chunks generalizing q with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons, ih]
      simp [append, List.flatten_cons, List.append_assoc]

private theorem drain_pending_length (q : Queue) (cs : List Nat) :
    (cs.foldl drain q).pending.length = q.pending.length - cs.sum := by
  induction cs generalizing q with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons, ih]
      rcases q with ⟨s,p⟩
      simp [drain, List.length_drop, Nat.sub_sub]

theorem refute : ¬ RetractPublished.Goal := by
  intro h
  have hh := h [1] [10] [20] 2
  have hbad : ¬ stream (retract (drain (start [1] [10] [20]) 2) (1-2)) = [1] ++ [10] := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_ReplyQueue_RetractPublished
