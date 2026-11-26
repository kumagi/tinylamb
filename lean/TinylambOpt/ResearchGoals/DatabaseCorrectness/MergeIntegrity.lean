import Std

/-! MERGE candidate discovery uses the original target and source bags. Keys are
nullable and equality is SQL ON equality. The FK model uses non-null parent keys,
nullable references and physical row identities for in-statement deletes. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

abbrev Key := Option Nat
def eqOn (a b : Key) := a.isSome && a == b
def sourceIds (src : List Key) (k : Key) := (src.zipIdx.filter (fun s => eqOn k s.1)).map Prod.snd
def unmatched (target src : List Key) := src.zipIdx.filter (fun s => !(target.any (eqOn s.1)))
inductive Act where | update | delete | skip
  deriving DecidableEq, Repr
def decideMatched (hits : List Nat) (clauses : List (Bool × Act)) : Except Unit Act := do
  if hits.isEmpty then return .skip
  let action := ((clauses.find? Prod.fst).map Prod.snd).getD .skip
  if hits.length > 1 && action != .skip then throw () else return action

/-- Equal source payloads still have separate ordinals for the one-action-per-target check. -/
def DuplicateSourceIdentityConflict.Goal : Prop := ∀ src k, k.isSome → 1 < src.count k →
  decideMatched (sourceIds src k) [(true,.update)] = .error ()

/-- A false earlier clause must not suppress the first later true clause; unreachable trailing clauses cannot affect its action. -/
def FirstMatchingMergeAction.Goal : Prop := ∀ before after a i,
  (∀ c ∈ before, c.1 = false) → decideMatched [i] (before++(true,a)::after) = .ok a

/-- A source row with a match remains matched even if every WHEN MATCHED condition was false. -/
def NoActionCannotBecomeInsert.Goal : Prop := ∀ target src k i, src[i]? = some k → k.isSome → k ∈ target →
  (k,i) ∉ unmatched target src

/-- Every NULL source key is unmatched even in a target containing NULLs; two NULL source rows stay separate. -/
def NullMergeAntiRowsRetainMultiplicity.Goal : Prop := ∀ target src,
  ((unmatched target src).map Prod.fst).count none = src.count none

structure Row where
  id : Nat
  key : Nat
  parent : Key
  deriving DecidableEq, Repr
def valid (rows : List Row) := ∀ r ∈ rows, ∀ p, r.parent = some p → p ∈ rows.map Row.key
def survivors (rows : List Row) (deleted : List Nat) := rows.filter (fun r => !deleted.contains r.id)
def afterRekey (rows : List Row) (id replacement : Nat) :=
  rows.map (fun r => if r.id == id then {r with key := replacement} else r)

/-- Check reverse references against surviving children and remaining parent multiplicities, including self-references. -/
def DeleteSetForeignKeyCriterion.Goal : Prop := ∀ rows gone, valid rows →
  (valid (survivors rows gone) ↔ ∀ r ∈ survivors rows gone, ∀ p, r.parent = some p →
    ((rows.map Row.key).count p > ((rows.filter (fun x => gone.contains x.id)).map Row.key).count p))

/-- Reject a parent rekey precisely when it removes the last provider of a still-referenced key. -/
def RekeyLastReferencedParent.Goal : Prop := ∀ rows r k, valid rows → r ∈ rows → (rows.map Row.id).Nodup → k ≠ r.key →
  (valid (afterRekey rows r.id k) ↔ (rows.map Row.key).count r.key > 1 ∨ ∀ x ∈ rows, x.parent ≠ some r.key)

def acceptInserts (base : List Nat) : List Row → Bool
  | [] => true
  | r::rs => (r.parent.all base.contains) && acceptInserts (r.key::base) rs

/-- Requiring each parent to exist before its child is inserted suffices for final referential integrity, even when both arrive in this statement. -/
def InsertPhaseOwnEarlierParent.Goal : Prop := ∀ base fresh, valid base →
  acceptInserts (base.map Row.key) fresh → valid (base++fresh)

structure Session where
  committed : List Row
  staged : List Row
  failed : Bool
  deriving DecidableEq, Repr
def publish (s : Session) := if s.failed then s.committed else s.staged
def failAfterDelete (s : Session) (ids : List Nat) := {s with staged := survivors s.staged ids, failed := true}

/-- An FK error after a successful delete prevents precommit from exposing the already staged deletion. -/
def FailedMergePhaseCannotPublishPrefix.Goal : Prop := ∀ s gone, publish (failAfterDelete s gone) = s.committed

#guard (sourceIds [some 2,none,some 2] (some 2)) == [0,2]
#guard acceptInserts [1] [⟨2,2,some 1⟩,⟨3,3,some 2⟩]

end TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity
