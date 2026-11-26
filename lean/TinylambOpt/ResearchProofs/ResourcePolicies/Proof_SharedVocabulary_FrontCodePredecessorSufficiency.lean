import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

/-!
`FrontCodePredecessorSufficiency`: decoding a front-coded dictionary entry only
needs the immediately preceding byte string: `expand prev (front prev s)` clips the
shared prefix off `prev` and appends the tail, which is exactly `s`, including the
empty-symbol and full-prefix cases.  A decoder may therefore store only `(prefix
length, suffix)` pairs against the previous entry instead of absolute offsets.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_FrontCodePredecessorSufficiency
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

/-- On a matching head the shared prefix grows by one. -/
private theorem common_cons (p : Nat) (ps : Symbol) (x : Nat) (xs : Symbol) (h : p == x) :
    common (p :: ps) (x :: xs) = common ps xs + 1 := by
  show (if p == x then 1 + common ps xs else 0) = common ps xs + 1
  rw [if_pos h]
  omega

/-- The two clipped prefixes of a pair of symbols agree. -/
private theorem take_common_eq : ∀ (prev s : Symbol),
    prev.take (common prev s) = s.take (common prev s) := by
  intro prev
  induction prev with
  | nil => intro s; rfl
  | cons p ps ih =>
      intro s
      cases s with
      | nil => rfl
      | cons x xs =>
          by_cases h : p == x
          · rw [common_cons p ps x xs h, List.take_succ_cons, List.take_succ_cons]
            have hpx : p = x := eq_of_beq h
            subst hpx
            exact congrArg (fun t => p :: t) (ih xs)
          · have hc : common (p :: ps) (x :: xs) = 0 := by
              show (if p == x then 1 + common ps xs else 0) = 0
              rw [if_neg h]
            rw [hc]
            rfl

theorem sound : FrontCodePredecessorSufficiency.Goal := by
  intro prev s
  show prev.take (common prev s) ++ s.drop (common prev s) = s
  rw [take_common_eq]
  exact List.take_append_drop (common prev s) s

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_FrontCodePredecessorSufficiency
