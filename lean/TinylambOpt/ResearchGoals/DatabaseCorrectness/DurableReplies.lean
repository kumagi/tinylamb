import Std

/-! Irrevocable publication and a successful durability reply are distinct.
Ends are exclusive WAL offsets. A read or chain dependency contributes its
producer's required end. The modeled writer has a page manager. An interrupted
installation can leave visibility uncertain while the transaction is irrevocable. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

structure Ticket where
  ownEnd : Nat
  sync : Bool
  observed : List Nat
  deriving DecidableEq, Repr

def target (t : Ticket) := max (if t.sync then t.ownEnd else 0) (t.observed.max?.getD 0)
def sequentialWaits (t : Ticket) (durable : Nat) :=
  (!t.sync || t.ownEnd ≤ durable) && t.observed.all (· ≤ durable)

/-- Moving two synchronous barriers to one deferred reply ticket preserves the set of admissible acknowledgements. -/
def InlineAndDeferredBarrierAgreement.Goal : Prop := ∀ t d, sequentialWaits t d = decide (target t ≤ d)

/-- An asynchronous writer may skip its own wait but cannot acknowledge a result that depends on an undurable observed writer. -/
def AsyncCommitKeepsReadBarrier.Goal : Prop := ∀ (t : Ticket) d x, x ∈ t.observed → d < x →
  sequentialWaits {t with sync := false} d = false

/-- A read-only ticket waits for exactly the maximum of the ends it observed, including an empty dependency set. -/
def EmptyWriterDependencyCut.Goal : Prop := ∀ seen d sync,
  (target ⟨0,sync,seen⟩ ≤ d ↔ ∀ endPos ∈ seen, endPos ≤ d)

/-- When a dependency already subsumes its predecessors, flattening their transitive ends cannot delay the reply. -/
def ChainedObservationBarrier.Goal : Prop := ∀ (t : Ticket) producer (inherited : List Nat), producer ∈ t.observed →
  (∀ x ∈ inherited, x ≤ producer) →
  target {t with observed := inherited++t.observed} = target t

inductive Phase where | running | aborted | interruptedInstall | published
  deriving DecidableEq, Repr
inductive Reply where | success | failure | waiting
  deriving DecidableEq, Repr
structure Outcome where
  phase : Phase
  managerStopped : Bool
  reply : Reply
  deriving DecidableEq, Repr
def launch (appendOK installOK : Bool) : Outcome :=
  if !appendOK then ⟨.aborted,true,.failure⟩ else
  if !installOK then ⟨.interruptedInstall,true,.failure⟩ else ⟨.published,false,.waiting⟩
def complete (s : Outcome) (t : Ticket) (durable : Nat) (ioOK : Bool) :=
  if s.reply != .waiting then s else
  if !ioOK then {s with reply := .failure} else
  if target t ≤ durable then {s with reply := .success} else s
def abort (s : Outcome) := if s.phase == .running then {s with phase := .aborted} else s

/-- An observed success identifies a published writer and a durable prefix covering its required ends. -/
def WriteReplyRequiresPublicationAndBarrier.Goal : Prop := ∀ a p t d ok,
  (complete (launch a p) t d ok).reply = .success →
  a = true ∧ p = true ∧ ok = true ∧ sequentialWaits t d = true

/-- Abort after a failed post-publication wait cannot rewind data already visible to another transaction. -/
def AbortAfterDurabilityFailureIsIrrevocable.Goal : Prop := ∀ t d,
  let s := abort (complete (launch true true) t d false)
  s.phase = .published ∧ s.reply = .failure

/-- A late successful wakeup cannot turn a previously failed write into a success notification. -/
def TerminalReplyCannotBeUpgraded.Goal : Prop := ∀ a p t d future,
  let s := complete (launch a p) t d false
  (complete s t future true).reply = .failure

/-- A gap between the commit record's first and last bytes must still block its synchronous reply. -/
def PartialCommitRecordCannotAcknowledge.Goal : Prop := ∀ start size observed d, 0 < size →
  start ≤ d → d < start+size → (complete (launch true true) ⟨start+size,true,observed⟩ d true).reply = .waiting

#guard (complete (launch true true) ⟨10,false,[14]⟩ 10 true).reply == .waiting

end TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies
