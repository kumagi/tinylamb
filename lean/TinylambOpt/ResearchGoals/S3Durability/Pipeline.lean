/-!
Throughput model of the WAL uploader in `docs/primary-s3.md`. A PUT of `C`
bytes takes `a + C / b`: a fixed latency `a` plus the transfer at `b` bytes per
time unit on one connection. With `W` PUTs in flight, throughput is
`W * C / (a + C / b) = W * C * b / (a * b + C)`. Inequalities are cross
multiplied so that every quantity stays a natural number. The goals show that
the sustainable WAL rate depends only on `W * b`, not on the PUT latency, when
chunks may grow with the backlog without a cap; that with a cap the ceiling is
reached at the cap and depends on the latency; how large chunks must be to
reach a fraction of the bandwidth; and that bounded chunks with one PUT in
flight are limited by latency. They size the uploader so that SQL commit throughput is bounded by
bandwidth rather than by S3 round trips.
-/

namespace TinylambOpt.ResearchGoals.S3Durability.Pipeline

namespace CapacityBelowBandwidth
/-- Throughput stays strictly below `W * b`, whatever the chunk size. -/
def Goal : Prop :=
  ∀ W a b C : Nat, 0 < W → 0 < a → 0 < b → W * C * b < W * b * (a * b + C)
end CapacityBelowBandwidth

namespace ChunkSizing
/-- To reach the fraction `p / q` of `W * b`, a chunk needs at least
`p / (q - p) * a * b` bytes, and that size suffices. -/
def Goal : Prop :=
  ∀ W a b C p q : Nat, 0 < W → 0 < b → p ≤ q →
    (p * (W * b) * (a * b + C) ≤ W * C * b * q ↔ p * (a * b) ≤ C * (q - p))
end ChunkSizing

namespace SerialBoundedIsLatencyBound
/-- One PUT in flight with chunks of at most `Cmax` bytes moves at most
`Cmax / a`: that design is limited by latency, not bandwidth. -/
def Goal : Prop :=
  ∀ a b C Cmax : Nat, C ≤ Cmax → C * b * a ≤ Cmax * (a * b + C)
end SerialBoundedIsLatencyBound

namespace EagerCapacity
/-- Eager cutting: a slot that frees takes the whole backlog. A rate `lam`
is sustainable with some finite chunk size exactly when `lam < W * b`. The
latency `a` changes only the chunk size and the commit delay. -/
def Goal : Prop :=
  ∀ lam W a b : Nat, 0 < W → 0 < a → 0 < b →
    (lam < W * b ↔ ∃ s, lam * (a * b + s) ≤ W * s * b)
end EagerCapacity

namespace BoundedChunkCapacity
/-- With chunks capped at `Cmax`, throughput `W * C * b / (a * b + C)` grows
with `C`, so the cap gives the ceiling `W * Cmax * b / (a * b + Cmax)`. That
ceiling falls as the latency `a` grows: a fixed cap makes the uploader
latency-dependent, and the cap must be sized from `a * b` (`ChunkSizing`). -/
def Goal : Prop :=
  ∀ W a b C Cmax : Nat, C ≤ Cmax →
    W * C * b * (a * b + Cmax) ≤ W * Cmax * b * (a * b + C)
end BoundedChunkCapacity

end TinylambOpt.ResearchGoals.S3Durability.Pipeline
