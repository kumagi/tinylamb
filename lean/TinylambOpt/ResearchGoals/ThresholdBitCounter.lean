import TinylambOpt.ThreeValued

namespace TinylambOpt.ResearchGoals.ThresholdBitCounter

abbrev W := BitVec 64
structure Term where
  yes : W
  unknown : W

-- Carry bit of a three-input binary sum, evaluated for all lanes at once.
def atLeastTwo (a b c : W) := (a &&& b) ||| ((a ^^^ b) &&& c)

def masks (active : W) (a b c : Term) : W × W :=
  let sure := atLeastTwo a.yes b.yes c.yes
  let maybe := atLeastTwo (a.yes ||| a.unknown) (b.yes ||| b.unknown) (c.yes ||| c.unknown)
  (active &&& sure, active &&& maybe &&& ~~~sure)

-- SQL (A AND B) OR (A AND C) OR (B AND C), including UNKNOWN in projection.
def atom (t : Term) (i : Nat) : SqlBool :=
  if t.yes.getLsbD i then .tTrue else if t.unknown.getLsbD i then .tNull else .tFalse

def reference (a b c : SqlBool) := ((a.and b).or (a.and c)).or (b.and c)

def Goal : Prop := ∀ active a b c, ∀ i < 64,
  let (yes,unknown) := masks active a b c
  let r := reference (atom a i) (atom b i) (atom c i)
  yes.getLsbD i = (active.getLsbD i && decide (r = .tTrue)) ∧
  unknown.getLsbD i = (active.getLsbD i && decide (r = .tNull))

example : masks 7 ⟨3,0⟩ ⟨1,2⟩ ⟨0,4⟩ = (1,2) := by decide

end TinylambOpt.ResearchGoals.ThresholdBitCounter
