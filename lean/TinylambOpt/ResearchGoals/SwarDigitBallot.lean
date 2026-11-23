import Std

namespace TinylambOpt.ResearchGoals.SwarDigitBallot

abbrev W := BitVec 64
def high : W := 0x8080808080808080
def low7 : W := 0x7f7f7f7f7f7f7f7f
-- One high bit per byte. Addition cannot carry across a byte boundary.
def zeroBytes (z : W) : W := ~~~(((z &&& low7) + low7) ||| z ||| low7)
def ballot (x : W) : W :=
  let hiMatch := zeroBytes ((x &&& 0xf0f0f0f0f0f0f0f0) ^^^ 0x3030303030303030)
  let bad := (((x &&& 0x0f0f0f0f0f0f0f0f) + 0x0606060606060606) &&& 0x1010101010101010) <<< 3
  hiMatch &&& ~~~bad &&& high

def scalar (x : W) (i : Nat) :=
  let byte := (x.extractLsb' (8*i) 8).toNat
  decide (48 ≤ byte ∧ byte ≤ 57)

def Goal : Prop := ∀ x i, i < 64 →
  (ballot x).getLsbD i = (i % 8 == 7 && scalar x (i/8))

example : ballot 0x803a39302f310030 = 0x0000808000800080 := by decide
example : ballot 0x3030303030303030 = high := by decide

end TinylambOpt.ResearchGoals.SwarDigitBallot
