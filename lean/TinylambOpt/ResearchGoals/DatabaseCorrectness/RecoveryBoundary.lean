import Std

/-! Recovery chooses an anchor before applying any redo. For a damaged suffix,
classification and the ability to actually truncate are independent. The model
takes validated framing, footer and I/O observations as inputs, not raw WAL bytes. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

def anchor (requested : Nat) (loaded : Option Nat) :=
  match loaded with
  | none => requested
  | some replay => if requested == 0 then replay else min requested replay
def startAt (first requested : Nat) (loaded : Option Nat) :=
  let cut := anchor requested loaded
  if cut < first then none else some (max first cut)

structure Tail where
  fixedFooter : Bool
  finalSegment : Bool
  readable : Bool
  allZero : Bool
  inSegment : Bool
  shortRecord : Bool
  withinRecordLimit : Bool
  laterFrame : Bool
  deriving DecidableEq, Repr
def benign (t : Tail) := !t.fixedFooter && t.finalSegment && t.readable &&
  (if t.allZero then t.inSegment else t.shortRecord && t.withinRecordLimit && !t.laterFrame)
inductive EndStatus where | clean | repaired | corrupt | ioError
  deriving DecidableEq, Repr
def finish (validEnd fileEnd : Nat) (t : Tail) (force hasWriter truncateOK : Bool) :=
  if fileEnd ≤ validEnd then EndStatus.clean else
  if (!force && !benign t) || !hasWriter then .corrupt else
  if truncateOK then .repaired else .ioError

/-- Combining two checkpoint anchors may require an earlier retained WAL segment than either caller first selected. -/
def EarliestUsableCheckpointAnchor.Goal : Prop := ∀ first req snapshot, 0 < req →
  startAt first req (some snapshot) = if first ≤ req && first ≤ snapshot then some (min req snapshot) else none

/-- Without an explicit replay anchor, recycled prefix bytes cannot be treated as an empty database. -/
def RecycledWalNeedsAnchor.Goal : Prop := ∀ first, 0 < first → startAt first 0 none = none

/-- Invalid bytes inside a validated fixed-size footer range are suspect even if they look like a short final record. -/
def FooterEndCannotMaskLostCommit.Goal : Prop := ∀ v n t writer ok, v < n → t.fixedFooter →
  finish v n t false writer ok = .corrupt

/-- A later valid record following a nonzero ambiguous short record blocks normal tail repair. -/
def LaterValidFramePreventsTailTruncation.Goal : Prop := ∀ v n t, v < n → !t.allZero → t.laterFrame →
  finish v n t false true true = .corrupt

/-- The recovery completion flag can request a replacement checkpoint only after a writer actually shortened the log. -/
def RepairFlagRequiresSuccessfulTruncate.Goal : Prop := ∀ v n t forced writer ok,
  (finish v n t forced writer ok = .repaired ↔ v < n ∧ writer ∧ ok ∧ (forced ∨ benign t))

/-- Force mode relaxes classification only; inability to rewrite the backing file still prevents a repaired result. -/
def ForcedRepairWithoutWriterFails.Goal : Prop := ∀ v n t ok, v < n → finish v n t true false ok = .corrupt

/-- Once repaired to the validated boundary, another recovery reports an intact clean end and requires no writer. -/
def ReopenClearsPriorRepairFlag.Goal : Prop := ∀ v n t force writer ok t',
  finish v n t force writer ok = .repaired → finish v v t' false false false = .clean

end TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary
