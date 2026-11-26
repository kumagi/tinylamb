import Std

/-! Temporary grants for operators that already hold memory, need scratch to finish,
and return their original holdings on completion. A second model makes grant identity
and pool attribution explicit during reclamation and cancellation. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

structure Job where
  held : Nat
  scratch : Nat
  deriving DecidableEq, Repr

/-- Minimum initial free bytes for this completion order, with scratch returned after each job. -/
def headroom : List Job → Nat
  | [] => 0
  | j::js => max j.scratch (headroom js - j.held)

/-- First job at which a serial completion order would stall. -/
def stalled (free : Nat) : List Job → Option Job
  | [] => none
  | j::js => if j.scratch ≤ free then stalled (free+j.held) js else some j

/-- A scalar reservation certificate predicts whether any job stalls before all holders release. -/
def CompletionOrderCertificate.Goal : Prop := ∀ jobs free,
  (stalled free jobs = none ↔ headroom jobs ≤ free)

/-- Serving the smallest scratch request first needs no more reserve than any serial order. -/
def LeastScratchCompletionOrder.Goal : Prop := ∀ (jobs order : List Job), jobs.Perm order →
  headroom (jobs.mergeSort (fun a b => decide (a.scratch ≤ b.scratch))) ≤ headroom order

/-- Aggressive alternative: releasing the largest holder first is also optimal when scratch is heterogeneous. -/
def LargestHolderAdmission.Goal : Prop := ∀ (jobs order : List Job), jobs.Perm order →
  headroom (jobs.mergeSort (fun a b => decide (b.held ≤ a.held))) ≤ headroom order

/-- A queued suffix can be admitted against headroom released by the prefix without recomputing that suffix. -/
def QueueSegmentReserveComposition.Goal : Prop := ∀ (front back : List Job),
  headroom (front++back) = max (headroom front) (headroom back - (front.map Job.held).sum)

/-- Marginal scratch savings for the whole queue cannot exceed the largest per-job reduction if holdings stay fixed. -/
def ScratchCompressionReserveSensitivity.Goal : Prop := ∀ (jobs : List Job) cut,
  headroom jobs ≤ headroom (jobs.map (fun j => {j with scratch := j.scratch-cut})) + cut

/-- Testing just total scratch less total holdings would permit bypassing serial reserve admission. -/
def NetScratchAdmission.Goal : Prop := ∀ (jobs : List Job) free,
  (jobs.map Job.scratch).sum ≤ free+(jobs.map Job.held).sum → stalled free jobs = none

structure Ticket where
  owner : Nat
  generation : Nat
  pool : Nat
  bytes : Nat
  deriving DecidableEq, Repr

def liveBytes (ts : List Ticket) := (ts.map Ticket.bytes).sum
def revoke (owner gen : Nat) (ts : List Ticket) :=
  ts.filter (fun t => !(t.owner == owner && t.generation == gen))

/-- A late release for an old query generation preserves every byte of a replacement query's grants. -/
def GenerationScopedLateRelease.Goal : Prop := ∀ ts owner old fresh, old ≠ fresh →
  ((revoke owner old ts).filter (fun t => t.owner == owner && t.generation == fresh)) =
    ts.filter (fun t => t.owner == owner && t.generation == fresh)

/-- Closing a generation twice must return exactly the bytes it owned once, even when split across pools. -/
def CanceledGrantByteConservation.Goal : Prop := ∀ ts owner gen,
  let kept := revoke owner gen ts
  (liveBytes ts-liveBytes kept) + (liveBytes kept-liveBytes (revoke owner gen kept)) =
    liveBytes (ts.filter (fun t => t.owner == owner && t.generation == gen))

/-- A scratch allocation moved between pools leaves process occupancy invariant while changing the destination cap test. -/
def GrantTransferDestinationCharge.Goal : Prop := ∀ ts fromPool toPool owner cap, fromPool ≠ toPool →
  let moving := ts.filter (fun t => t.owner == owner && t.pool == fromPool)
  let transferred := ts.map (fun t => if t.owner == owner && t.pool == fromPool then {t with pool := toPool} else t)
  liveBytes transferred = liveBytes ts ∧
    (liveBytes (transferred.filter (fun t => t.pool == toPool)) ≤ cap ↔
     liveBytes (ts.filter (fun t => t.pool == toPool)) + liveBytes moving ≤ cap)

/-- Per-worker high-water marks do not necessarily coincide; taking their sum as an admission requirement is a strong hypothesis. -/
def WorkerPeaksAreSimultaneous.Goal : Prop := ∀ (samples : List (Nat × Nat)),
  ((samples.map (fun (a,b) => a+b)).max?).getD 0 =
    ((samples.map Prod.fst).max?).getD 0 + ((samples.map Prod.snd).max?).getD 0

/-- Once independently charged emergency buffers have a per-client upper bound, connection admission covers their simultaneous peak. -/
def EmergencyFloorAcrossClients.Goal : Prop := ∀ (buffers : List Nat) floor maxClients,
  buffers.length ≤ maxClients → (∀ b ∈ buffers, b ≤ floor) → buffers.sum ≤ maxClients*floor

/-- Stage-local callbacks may share the same reclaimable object. Their reported successes add exactly only for disjoint releases. -/
def DisjointVictimAccounting.Goal : Prop := ∀ (a b : List Nat) (bytes : Nat → Nat), a.Nodup → b.Nodup →
  (∀ id ∈ a, ¬ id ∈ b) → (((a++b).eraseDups).map bytes).sum = (a.map bytes).sum + (b.map bytes).sum

#guard headroom [⟨7,1⟩,⟨10,7⟩] == 1 && headroom [⟨10,7⟩,⟨7,1⟩] == 7

end TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
