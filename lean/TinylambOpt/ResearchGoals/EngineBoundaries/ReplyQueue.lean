import Std

namespace TinylambOpt.ResearchGoals.EngineBoundaries.ReplyQueue

/-! One Simple-Query response under construction behind an older queued prefix.
The sink observes every byte sent. Retraction can remove only pending bytes.
The model covers an abstract byte queue, without sockets or SQL transactions. -/
abbrev Bytes := List UInt8
structure Queue where
  sent : Bytes
  pending : Bytes
  deriving BEq, DecidableEq

def drain (q : Queue) (quota : Nat) : Queue :=
  ⟨q.sent ++ q.pending.take quota, q.pending.drop quota⟩
def append (q : Queue) (bs : Bytes) : Queue := {q with pending := q.pending ++ bs}
def retract (q : Queue) (olderLeft : Nat) : Queue := {q with pending := q.pending.take olderLeft}
def stream (q : Queue) := q.sent ++ q.pending
def start (past older reply : Bytes) : Queue := ⟨past, older ++ reply⟩

/-- A burst of short writes is observationally one write with the combined quota. -/
def CombineDrains.Goal : Prop := ∀ q a b, drain (drain q a) b = drain q (a+b)

/-- Compaction after a partial write preserves the final byte stream. -/
def PreserveBytes.Goal : Prop := ∀ q n, stream (drain q n) = stream q

/-- If no current-reply byte escaped, a failed response can be replaced exactly. -/
def UnpublishedError.Goal : Prop := ∀ past older reply err n, n ≤ older.length →
  let q := drain (start past older reply) n
  stream (append (retract q (older.length-n)) err) = past ++ older ++ err

/-- The observable residue of a failed reply is exactly the prefix already sent. -/
def PublishedPrefix.Goal : Prop := ∀ past older reply err n,
  let q := drain (start past older reply) n
  stream (append (retract q (older.length-n)) err) =
    past ++ older ++ reply.take (n-older.length) ++ err

/-- Aggressive: reuse the original saved older-byte count after a short write. -/
def StaleCheckpoint.Goal : Prop := ∀ past older reply n, n ≤ older.length →
  stream (retract (drain (start past older reply) n) older.length) = past ++ older

/-- Moving enqueue past an already capped write is safe if that write cannot reach new bytes. -/
def DelayEnqueue.Goal : Prop := ∀ q bs n, n ≤ q.pending.length →
  drain (append q bs) n = append (drain q n) bs

/-- A writable callback can honor a queue high-water mark using just remaining unsent bytes. -/
def Backpressure.Goal : Prop := ∀ q n bs limit,
  ((append (drain q n) bs).pending.length ≤ limit ↔ q.pending.length - n + bs.length ≤ limit)

/-- Before the first new byte is exposed, sending the old prefix may overlap response assembly. -/
def PipelineOldPrefix.Goal : Prop := ∀ past older chunks n, n ≤ older.length →
  let q := (chunks : List Bytes).foldl append (⟨past,older⟩ : Queue)
  drain q n = chunks.foldl append (drain ⟨past,older⟩ n)

/-- A full abort after exposing some reply bytes can recover the old stream for arbitrary replies. -/
def RetractPublished.Goal : Prop := ∀ past older reply n,
  stream (retract (drain (start past older reply) n) (older.length-n)) = past ++ older

/-- A credit-limited writer can drain the pending response in a finite list of writes. -/
def DrainCredits.Goal : Prop := ∀ q credits,
  let out := (credits : List Nat).foldl drain q
  out.pending.isEmpty = decide (q.pending.length ≤ credits.sum)

private def partiallySent := drain (start [] [10,11] [20,21]) 1
#guard stream (retract partiallySent 2) == [10,11,20]
#guard stream (retract partiallySent 1) == [10,11]

end TinylambOpt.ResearchGoals.EngineBoundaries.ReplyQueue
