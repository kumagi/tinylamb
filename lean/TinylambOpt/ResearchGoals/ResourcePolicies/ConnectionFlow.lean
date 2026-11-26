import Std

/-! Partial sends of pre-serialized response frames and per-connection queue
watermarks. An acknowledged byte has been accepted by the socket. No fairness
or bound on socket service is implicit. The parser model splits zero-delimited
fields of a PostgreSQL simple-query payload after framing. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

abbrev Frames := List (List Nat)
def remaining : Nat → Frames → Frames
  | _, [] => []
  | n, f::fs => if n < f.length then (f.drop n)::fs else remaining (n-f.length) fs

/-- A partial write may retire complete frames and trim exactly one leading frame. -/
def PartialSendWireSuffix.Goal : Prop := ∀ fs n, (remaining n fs).flatten = fs.flatten.drop n

/-- Arbitrary socket progress callbacks can be coalesced while preserving the unwritten reply. -/
def WriteCreditComposition.Goal : Prop := ∀ fs a b, remaining b (remaining a fs) = remaining (a+b) fs

/-- Appending a later SQL reply while a write is pending preserves FIFO, including the ReadyForQuery suffix. -/
def PipelinedReplyFifo.Goal : Prop := ∀ (a b : Frames) n, n ≤ a.flatten.length →
  (remaining n (a++b)).flatten = (remaining n a).flatten ++ b.flatten

/-- Queue headroom checked before admission bounds overshoot by the largest single admitted frame. -/
def EnqueueHighWaterSlack.Goal : Prop := ∀ (fs : Frames) high frame maxFrame,
  fs.flatten.length < high → frame.length ≤ maxFrame →
  (fs++[frame]).flatten.length < high+maxFrame

/-- Logical bytes left to send cap physical memory if a partially consumed frame retains its complete backing allocation. -/
def UnsentBytesBoundPinnedAllocation.Goal : Prop := ∀ (frame : List Nat) off cap,
  off < frame.length → (frame.drop off).length ≤ cap → frame.length ≤ cap

def blocked (lo hi : Nat) (paused : Bool) (bytes : Nat) :=
  if paused then decide (lo < bytes) else decide (hi ≤ bytes)

/-- Distinct high and low watermarks prevent an immediate pause/resume flip without any byte progress. -/
def BackpressureStableObservation.Goal : Prop := ∀ lo hi paused bytes, lo < hi →
  blocked lo hi (blocked lo hi paused bytes) bytes = blocked lo hi paused bytes

/-- A single shared watermark preserves independent per-connection read admission for every distribution of queued bytes. -/
def GlobalWatermarkClientIsolation.Goal : Prop := ∀ (other : List Nat) client lo hi, lo < hi →
  blocked lo hi false client = blocked lo hi false (client+other.sum)

/-- Summing each connection's post-admission slack bounds all pending output, including a reserved control frame each. -/
def ConcurrentReplyReserve.Goal : Prop := ∀ (loads : List Nat) hi maxFrame control,
  (∀ n ∈ loads, n ≤ hi+maxFrame) →
  (loads.map (·+control)).sum ≤ loads.length*(hi+maxFrame+control)

def beforeZero (xs : List Nat) := xs.takeWhile (· != 0)

/-- A fragmented text field can accumulate delimiter-free chunks without rescanning their bytes. -/
def DelimiterFreeChunkContinuation.Goal : Prop := ∀ a b, ¬ 0 ∈ a →
  beforeZero (a++b) = a++beforeZero b

/-- Once a terminator was seen, scanning can stop even when more already-buffered SQL payload follows. -/
def TerminatedFieldEarlyStop.Goal : Prop := ∀ a b, 0 ∈ a → beforeZero (a++b) = beforeZero a

/-- Emitting completed fields per chunk and retaining its final unterminated
field allows the next read to resume across any TCP fragmentation. -/
def FieldDecoderChunkCarry.Goal : Prop := ∀ (a b : List Nat),
  let pieces := a.splitOn 0
  pieces.dropLast ++ ((pieces.getLast?.getD [] ++ b).splitOn 0) = (a++b).splitOn 0

#guard remaining 3 [[1,2],[],[3,4],[5]] == [[4],[5]]

end TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow
