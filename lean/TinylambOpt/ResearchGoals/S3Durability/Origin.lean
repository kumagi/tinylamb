import TinylambOpt.ResearchGoals.S3Durability.History

/-!
Starting the restore walk at a retained origin instead of sequence 1
(`docs/primary-s3.md`, sections 9 and 13). A manifest records the key and the
expected first record of a chunk inside the writer's confirmed contiguous
prefix. These goals state that the walk from the start of the epoch reads the
confirmed chain and then exactly the walk from that origin, and that the walk
from the origin never reads a key below it. So the chunks below the origin can
be deleted, and a restore or the seal scan of a long epoch can start at the
origin, without changing the official history.
-/

namespace TinylambOpt.ResearchGoals.S3Durability.Origin

open TinylambOpt.ResearchGoals.S3Durability.History

/-- The record position a chain expects next, starting from `lo`. -/
def nextLo : List Chunk → Nat → Nat
  | [], lo => lo
  | c :: cs, _ => nextLo cs (c.hi + 1)

namespace WalkFromOrigin
/-- When a chain `cs` is stored at consecutive keys from `s`, the walk from `s`
reads its records followed by the walk from the key after the chain. -/
def Goal : Prop :=
  ∀ (cs : List Chunk) (st : Store) (s lo fuel : Nat), Chained cs lo → Placed st s cs →
    walk (fuel + cs.length) st s lo =
      cs.flatMap Chunk.recs ++ walk fuel st (s + cs.length) (nextLo cs lo)
end WalkFromOrigin

namespace WalkIgnoresBelow
/-- The walk from key `s` reads only keys at or above `s`: deleting the chunks
below an origin does not change the walk from the origin. -/
def Goal : Prop :=
  ∀ (fuel : Nat) (st st' : Store) (s lo : Nat), (∀ j, s ≤ j → st' j = st j) →
    walk fuel st' s lo = walk fuel st s lo
end WalkIgnoresBelow

end TinylambOpt.ResearchGoals.S3Durability.Origin
