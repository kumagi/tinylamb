import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

/-! Concatenated components identify the tuple exactly.

Decoding the first field of a concatenated byte string recovers the leading
component and the remaining encoded suffix; equality of two concatenations
therefore forces equality component by component. SQL execution may encode
compound VARCHAR keys by simple concatenation without losing tuple identity. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_CompoundKeyComponentIdentity

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


private theorem groups_body_aux (fuel : Nat) : ∀ (initial : Bool) (s : Bytes), s.length = fuel →
    (initial = false → s ≠ []) → ∀ tail : Bytes, groups initial (body s ++ tail) = some (s, tail) := by
  induction fuel using Nat.strongRecOn with
  | ind fuel ih =>
      intro initial s hlen hne tail
      by_cases hf0 : fuel = 0
      · have hlen0 : s.length = 0 := by omega
        have hs : s = [] := List.length_eq_zero_iff.mp hlen0
        subst s
        have hinit : initial = true := by cases initial <;> simp_all
        simp [body, groups, hinit]
      · cases s with
        | nil =>
          simp at hlen
          omega
        | cons a s =>
          cases s with
          | nil =>
            simp [body, groups]
          | cons b s =>
            cases s with
            | nil =>
              simp [body, groups]
            | cons c s =>
              cases s with
              | nil =>
                simp [body, groups]
              | cons d s =>
                cases s with
                | nil =>
                  simp [body, groups]
                | cons e s =>
                  cases s with
                  | nil =>
                    simp [body, groups]
                  | cons f s =>
                    cases s with
                    | nil =>
                      simp [body, groups]
                    | cons g s =>
                      cases s with
                      | nil =>
                        simp [body, groups]
                      | cons h s =>
                        cases s with
                        | nil =>
                          cases initial <;> simp [body, groups] at hne ⊢
                        | cons i rest =>
                          have htail_lt : (i :: rest).length < fuel := by
                            rw [show (i :: rest).length = rest.length + 1 by simp [Nat.add_assoc]]
                            rw [show (a :: b :: c :: d :: e :: f :: g :: h :: i :: rest).length = rest.length + 9 by simp [Nat.add_assoc]] at hlen
                            omega
                          rw [show body (a :: b :: c :: d :: e :: f :: g :: h :: i :: rest) =
                                [a,b,c,d,e,f,g,h,9] ++ body (i :: rest) by rfl]
                          rw [List.append_assoc]
                          rw [groups_cont]
                          have hrec := ih (i :: rest).length htail_lt false (i :: rest) rfl (by intro hf; simp) tail
                          rw [hrec]
                          simp

example : BinaryKeySelfDelimiting.Goal := by
  intro s tail
  dsimp [decode, encode]
  exact groups_body_aux s.length true s rfl (by intro h; cases h) tail

private theorem decode_encode_append (s : Bytes) (tail : Bytes) :
    decode (encode s ++ tail) = some (s, tail) := by
  dsimp [decode, encode]
  exact groups_body_aux s.length true s rfl (by intro h; cases h) tail

theorem sound : CompoundKeyComponentIdentity.Goal := by
  intro a b
  constructor
  · intro h
    induction a generalizing b with
    | nil =>
        cases b with
        | nil => rfl
        | cons b0 btl =>
            have hlen := congrArg List.length h
            simp [List.flatMap, encode, body] at hlen
    | cons a0 atl ih =>
        cases b with
        | nil =>
            have hlen := congrArg List.length h
            simp [List.flatMap, encode, body] at hlen
        | cons b0 btl =>
            have hdec := congrArg decode h
            simp only [List.flatMap_cons] at hdec
            rw [decode_encode_append, decode_encode_append] at hdec
            have hp : (a0, List.flatMap encode atl) = (b0, List.flatMap encode btl) := Option.some.inj hdec
            have hhead : a0 = b0 := congrArg Prod.fst hp
            have htail : List.flatMap encode atl = List.flatMap encode btl := congrArg Prod.snd hp
            subst b0
            have htail' := ih btl htail
            subst btl
            rfl
  · intro h
    subst b
    rfl
end TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharKeys_CompoundKeyComponentIdentity
