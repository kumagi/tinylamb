import Std

/-!
These claims recover XOR aggregates for segments. A grouped or windowed SQL operator can
reuse segment summaries under the specified boundaries.

Modeled claims

- `Goal` — `∀ carry t, parallel carry t = stream carry (flatten t)`
-/

namespace TinylambOpt.ResearchGoals.SegmentedXor

inductive Token where
  | null | base (x : UInt64) | delta (bits : UInt64)
  deriving DecidableEq

abbrev Out := List (Option UInt64) × UInt64

def stream (carry : UInt64) : List Token → Out
  | [] => ([], carry)
  | token :: rest =>
    let (value, next) := match token with
      | .null => (none, carry)
      | .base x => (some x, x)
      | .delta d => (some (carry ^^^ d), carry ^^^ d)
    let (tail, last) := stream next rest
    (value :: tail, last)

inductive Tree where
  | empty | leaf (t : Token) | node (l r : Tree)

def flatten : Tree → List Token
  | .empty => []
  | .leaf t => [t]
  | .node l r => flatten l ++ flatten r

structure Summary where
  reset : Bool
  bits : UInt64

def applySummary (s : Summary) (x : UInt64) :=
  if s.reset then s.bits else x ^^^ s.bits

-- Cache the left summary on the upward pass for the downward carry pass.
inductive Cached where
  | empty | leaf (t : Token) | node (leftSum : Summary) (l r : Cached)

def up : Tree → Summary × Cached
  | .empty => (⟨false, 0⟩, .empty)
  | .leaf t =>
    let s := match t with
      | .null => ⟨false, 0⟩
      | .base x => ⟨true, x⟩
      | .delta d => ⟨false, d⟩
    (s, .leaf t)
  | .node l r =>
    let (a, lc) := up l
    let (b, rc) := up r
    (⟨a.reset || b.reset, applySummary b a.bits⟩, .node a lc rc)

def down (carry : UInt64) : Cached → List (Option UInt64)
  | .empty => []
  | .leaf .null => [none]
  | .leaf (.base x) => [some x]
  | .leaf (.delta d) => [some (carry ^^^ d)]
  | .node a l r => down carry l ++ down (applySummary a carry) r

def parallel (carry : UInt64) (t : Tree) : Out :=
  let (s, c) := up t
  (down carry c, applySummary s carry)

def Goal : Prop := ∀ carry t, parallel carry t = stream carry (flatten t)

private def sample : Tree :=
  .node (.node (.leaf (.delta 3)) (.leaf .null))
        (.node (.leaf (.base 8)) (.node .empty (.leaf (.delta 1))))
example : parallel 4 sample = ([some 7, none, some 8, some 9], 9) := by decide
example : stream 4 (flatten sample) = ([some 7, none, some 8, some 9], 9) := by decide

end TinylambOpt.ResearchGoals.SegmentedXor
