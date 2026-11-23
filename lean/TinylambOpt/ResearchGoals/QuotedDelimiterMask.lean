import Std

namespace TinylambOpt.ResearchGoals.QuotedDelimiterMask

abbrev W := BitVec 64
-- Inclusive prefix parity, with XOR propagation in six fixed stages.
def parity (w : W) :=
  ([1,2,4,8,16,32] : List Nat).foldl (fun x n => x ^^^ (x <<< n)) w

def mask (len : Nat) : W := ((1 : W) <<< len) - 1

def chunk (prior : Bool) (len : Nat) (quotes commas : W) : W × Bool :=
  let q := quotes &&& mask len
  let inside := parity q ^^^ (if prior then ~~~(0 : W) else 0)
  (commas &&& mask len &&& ~~~inside, inside.getLsbD 63)

def scalar (prior : Bool) (len : Nat) (quotes commas : W) :=
  (List.range len).foldl (fun (out,state) i =>
    let next := state != quotes.getLsbD i
    let emit := commas.getLsbD i && !next
    (out ||| ((BitVec.ofNat 64 emit.toNat) <<< i), next)) ((0 : W),prior)

def Goal : Prop := ∀ prior len q c, len ≤ 64 → q &&& c = 0 →
  chunk prior len q c = scalar prior len q c

example : chunk false 4 2 12 = (0,true) := by decide
example : chunk true 4 2 12 = (12,false) := by decide
example : chunk true 0 3 3 = (0,true) := by decide

end TinylambOpt.ResearchGoals.QuotedDelimiterMask
