import Std

/-! Tier transitions between stack bytecode and a register kernel. A stack map reconstructs logical slots including duplicate operands and recoverable constants. Dead slots may be ignored only at observations that do not read them. -/
namespace TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

inductive Recipe where
  | reg (id : Nat) | lit (v : Int) | plus (a b : Recipe) | neg (a : Recipe)
  deriving DecidableEq, Repr
abbrev Regs := Nat → Int
def eval (s : Regs) : Recipe → Int
  | .reg i => s i | .lit v => v | .plus a b => eval s a + eval s b | .neg a => -(eval s a)
def uses : Recipe → List Nat
  | .reg i => [i] | .lit _ => [] | .plus a b => uses a ++ uses b | .neg a => uses a
def subst (f : Nat → Recipe) : Recipe → Recipe
  | .reg i => f i | .lit v => .lit v | .plus a b => .plus (subst f a) (subst f b) | .neg a => .neg (subst f a)
def widthOK (w : Nat) (x : Int) := decide (-(2^w:Int) ≤ x ∧ x < (2^w:Int))
def checked (w : Nat) (s : Regs) : Recipe → Option Int
  | .reg i => some (s i) | .lit v => some v
  | .plus a b => do
    let x ← checked w s a
    let y ← checked w s b
    if widthOK w (x+y) then some (x+y) else none
  | .neg a => do
    let x ← checked w s a
    if widthOK w (-x) then some (-x) else none

/-- Compose two tier transitions by substituting rematerialization recipes instead of materializing their intermediate frame. -/
def StackMapCompositionInlining.Goal : Prop := ∀ regs f (stack : List Recipe), (stack.map (subst f)).map (eval regs) = stack.map (eval (fun i => eval regs (f i)))

/-- Evacuate only registers mentioned by recipes reachable from the side-exit's logical stack slots. -/
def StackMapLiveRegisterClosure.Goal : Prop := ∀ (a b : Regs) (stack : List Recipe), (∀ r ∈ stack.flatMap uses, a r = b r) → stack.map (eval a) = stack.map (eval b)

/-- Spilling only slots represented directly by a register is sufficient when all other slots are rematerialized expressions. -/
def RootRegisterOnlySpill.Goal : Prop := ∀ a b (stack : List Recipe), (∀ i, .reg i ∈ stack → a i = b i) → stack.map (eval a) = stack.map (eval b)

/-- Share duplicate recipes at an exit and expand their results back to the original positional stack. -/
def StackRecipeDedupExpand.Goal : Prop := ∀ regs (stack : List Recipe), let compact := stack.eraseDups
  stack.map (fun r => (compact.map (eval regs))[compact.idxOf r]?) = (stack.map (eval regs)).map some

/-- Exact-integer equality across all register files licenses merging two fallible deopt recipes. -/
def ArithmeticEquivalenceStackMerge.Goal : Prop := ∀ a b, (∀ regs, eval regs a = eval regs b) → ∀ w regs, checked w regs a = checked w regs b

/-- An in-range final stack slot implies rematerializing its checked intermediate operations cannot fault. -/
def TerminalRangeOnlyRematerialization.Goal : Prop := ∀ w regs r, widthOK w (eval regs r) → checked w regs r = some (eval regs r)

/-- A successful checked recipe yields the same rematerialized logical value as unbounded arithmetic. -/
def RecipeCheckedSuccessfulValue.Goal : Prop := ∀ w regs r v, checked w regs r = some v → v = eval regs r

/-- Gather only demanded logical slots directly from the recipe map, including repeated stack positions. -/
def DeadFrameSlotCompression.Goal : Prop := ∀ regs (stack : List Recipe) (wanted : List Nat),
  wanted.filterMap (fun i => (stack.map (eval regs))[i]?) = (wanted.filterMap (fun i => stack[i]?)).map (eval regs)

/-- Remove paired negations in the stack map whenever the outer value satisfies the target signed-width guard. -/
def SignedZeroCostDoubleNegation.Goal : Prop := ∀ w regs r, checked w regs r = some (eval regs r) → widthOK w (eval regs r) →
  checked w regs (.neg (.neg r)) = checked w regs r

end TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps
