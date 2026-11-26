import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

/-! A truncated field is never decoded as an empty field.

Any proper prefix of an encoding leaves either an incomplete block or a
continuation block with no successor, so it cannot satisfy the final-block
checks. SQL execution may treat partial WAL or page bytes as a failed field
decode rather than a valid empty key. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_IncompleteFieldIsNotEmpty

open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

private theorem groups_short_none (initial : Bool) (l : Bytes) (h : l.length < 9) : groups initial l = none := by
  cases l with
  | nil => simp [groups]
  | cons a l =>
    cases l with
    | nil => simp [groups]
    | cons b l =>
      cases l with
      | nil => simp [groups]
      | cons c l =>
        cases l with
        | nil => simp [groups]
        | cons d l =>
          cases l with
          | nil => simp [groups]
          | cons e l =>
            cases l with
            | nil => simp [groups]
            | cons f l =>
              cases l with
              | nil => simp [groups]
              | cons g l =>
                cases l with
                | nil => simp [groups]
                | cons h8 l =>
                  cases l with
                  | nil => simp [groups]
                  | cons i l =>
                    simp at h
                    omega

private theorem groups_cont (initial : Bool) (a b c d e f g h : UInt8) (rest : Bytes) :
    groups initial ([a,b,c,d,e,f,g,h,9] ++ rest) =
      (groups false rest).bind (fun p => some ([a,b,c,d,e,f,g,h] ++ p.1, p.2)) := by
  simp [groups]

private theorem groups_take_none_aux (fuel : Nat) : ∀ (s : Bytes), s.length = fuel →
    ∀ (initial : Bool) n, n < (body s).length → groups initial ((body s).take n) = none := by
  induction fuel using Nat.strongRecOn with
  | ind fuel ih =>
      intro s hlen initial n hn
      by_cases hf0 : fuel = 0
      · have hlen0 : s.length = 0 := by omega
        have hs : s = [] := List.length_eq_zero_iff.mp hlen0
        subst s
        apply groups_short_none
        have hn9small : n < 9 := by simpa [body] using hn
        rw [List.length_take]
        simpa [body, Nat.min_eq_left (Nat.le_of_lt hn9small)] using hn9small
      · cases s with
        | nil =>
          apply groups_short_none
          simp [body] at hn ⊢
          omega
        | cons a s =>
          cases s with
          | nil =>
            apply groups_short_none
            simp [body] at hn ⊢
            omega
          | cons b s =>
            cases s with
            | nil =>
              apply groups_short_none
              simp [body] at hn ⊢
              omega
            | cons c s =>
              cases s with
              | nil =>
                apply groups_short_none
                simp [body] at hn ⊢
                omega
              | cons d s =>
                cases s with
                | nil =>
                  apply groups_short_none
                  simp [body] at hn ⊢
                  omega
                | cons e s =>
                  cases s with
                  | nil =>
                    apply groups_short_none
                    simp [body] at hn ⊢
                    omega
                  | cons f s =>
                    cases s with
                    | nil =>
                      apply groups_short_none
                      simp [body] at hn ⊢
                      omega
                    | cons g s =>
                      cases s with
                      | nil =>
                        apply groups_short_none
                        simp [body] at hn ⊢
                        omega
                      | cons h s =>
                        cases s with
                        | nil =>
                          apply groups_short_none
                          simp [body] at hn ⊢
                          omega
                        | cons i rest =>
                          have htail_lt : (i :: rest).length < fuel := by
                            rw [show (i :: rest).length = rest.length + 1 by simp [Nat.add_assoc]]
                            rw [show (a :: b :: c :: d :: e :: f :: g :: h :: i :: rest).length = rest.length + 9 by simp [Nat.add_assoc]] at hlen
                            omega
                          simp only [body] at hn ⊢
                          by_cases hn9 : n < 9
                          · apply groups_short_none
                            rw [List.length_take]
                            omega
                          · rw [List.take_append]
                            have hlen9 : ([a,b,c,d,e,f,g,h,9] : Bytes).length = 9 := by simp
                            rw [List.take_of_length_le (by omega : 9 ≤ n)]
                            change groups initial ([a,b,c,d,e,f,g,h,9] ++ List.take (n - 9) (body (i :: rest))) = none
                            rw [groups_cont]
                            have hlt : n - 9 < (body (i :: rest)).length := by
                              simp only [body, List.length_append, List.length_cons] at hn
                              rw [Nat.sub_lt_iff_lt_add (Nat.le_of_not_gt hn9)]
                              simpa [Nat.add_comm] using hn
                            have hrec := ih (i :: rest).length htail_lt (i :: rest) rfl false (n - 9) hlt
                            rw [hrec]
                            simp

theorem sound : IncompleteFieldIsNotEmpty.Goal := by
  intro s n hn
  cases n with
  | zero => simp [decode]
  | succ n' =>
      have hnbody : n' < (body s).length := by
        simpa [encode] using hn
      simp [encode, decode]
      exact groups_take_none_aux s.length s rfl true n' hnbody
end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_IncompleteFieldIsNotEmpty
