import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery

/-!
Shared lemmas for the Simple Query message model. Writes only stage rows, so a
writes prefix composes with the rest of one message without triggering the
end-of-message flush; the flush happens only at the real ReadyForQuery boundary.
The wire lemma records that a partial send retires exactly the bytes reported
as already sent by the frame model.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow (Frames remaining)

theorem message_writes_then (committed staged xs : List Nat) (rest : List Cmd) :
    message ⟨committed, staged, .idle⟩ (writes xs ++ rest) =
      message ⟨committed, staged ++ xs, .idle⟩ rest := by
  induction xs generalizing staged with
  | nil => simp [writes]
  | cons x xs ih =>
      change message ⟨committed, staged, .idle⟩ (Cmd.write x :: (writes xs ++ rest)) =
        message ⟨committed, staged ++ (x :: xs), .idle⟩ rest
      simp only [message]
      rw [ih (staged ++ [x])]
      simp [List.append_assoc]

theorem message_writes_then_explicit (committed staged xs : List Nat) (rest : List Cmd) :
    message ⟨committed, staged, .explicitTxn⟩ (writes xs ++ rest) =
      message ⟨committed, staged ++ xs, .explicitTxn⟩ rest := by
  induction xs generalizing staged with
  | nil => simp [writes]
  | cons x xs ih =>
      change message ⟨committed, staged, .explicitTxn⟩ (Cmd.write x :: (writes xs ++ rest)) =
        message ⟨committed, staged ++ (x :: xs), .explicitTxn⟩ rest
      simp only [message]
      rw [ih (staged ++ [x])]
      simp [List.append_assoc]

theorem message_writes_idle (committed staged xs : List Nat) :
    message ⟨committed, staged, .idle⟩ (writes xs) =
      ⟨committed ++ staged ++ xs, [], .idle⟩ := by
  simpa [message, flush, clear, List.append_assoc] using message_writes_then committed staged xs []

theorem message_writes_explicit (committed staged xs : List Nat) :
    message ⟨committed, staged, .explicitTxn⟩ (writes xs) =
      ⟨committed, staged ++ xs, .explicitTxn⟩ := by
  simpa [message] using message_writes_then_explicit committed staged xs []

theorem remaining_flatten : ∀ (fs : Frames) (n : Nat),
    (remaining n fs).flatten = fs.flatten.drop n
  | [], _ => by simp [remaining]
  | f :: fs, n => by
      by_cases h : n < f.length
      · have hle : n ≤ f.length := Nat.le_of_lt h
        simp [remaining, h, List.drop_append, hle]
      · have hle : f.length ≤ n := Nat.le_of_not_gt h
        have ih := remaining_flatten fs (n - f.length)
        simp [remaining, h, List.drop_append, ih, hle]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon
