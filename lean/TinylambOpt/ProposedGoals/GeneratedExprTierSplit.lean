import Std

namespace TinylambOpt.ProposedGoals.GeneratedExprTierSplit

inductive Val where
  | null | num (x : Int) | err (tag : String)
  deriving DecidableEq

inductive Expr where
  | col (i : Nat)
  | konst (v : Option Int)
  | add (a b : Expr)
  | host (tag : String) (a : Expr)

/-- A deterministic stand-in for a Tier-1 host call (VARCHAR / JSON / UDF). -/
def hostFn (tag : String) (x : Int) : Val :=
  if tag == "neg" then .num (-x) else .err tag

def addVals : Val → Val → Val
  | .err t, _ => .err t
  | _, .err t => .err t
  | .null, _ => .null
  | _, .null => .null
  | .num x, .num y => .num (x + y)

def toVal : Option Int → Val
  | none => .null | some x => .num x

/-- Reference: tree-walking AST evaluator (semantic ground truth). -/
def eval : Expr → List (Option Int) → Val
  | .col i, row => match row[i]? with
      | none => .err "oob" | some v => toVal v
  | .konst v, _ => toVal v
  | .add a b, row =>
      match eval a row with
      | .err t => .err t
      | va => match eval b row with
          | .err t => .err t
          | vb => addVals va vb
  | .host tag a, row =>
      match eval a row with
      | .num x => hostFn tag x
      | v => v

/-- Generated code for the fast Tier-0 engine; unsupported nodes become host calls. -/
inductive Op where
  | push (v : Option Int) | load (i : Nat) | add | host (tag : String)

def compile : Expr → List Op
  | .col i => [.load i]
  | .konst v => [.push v]
  | .add a b => compile b ++ compile a ++ [.add]
  | .host tag a => compile a ++ [.host tag]

def runOps : List Op → List (Option Int) → List Val → Val
  | [], _, st => match st with | v :: _ => v | [] => .err "empty"
  | .push v :: ops, row, st => runOps ops row (toVal v :: st)
  | .load i :: ops, row, st =>
      let v := match row[i]? with | none => .err "oob" | some x => toVal x
      runOps ops row (v :: st)
  | .add :: ops, row, va :: vb :: st => runOps ops row (addVals va vb :: st)
  | .add :: _, _, _ => .err "stack"
  | .host tag :: ops, row, v :: st =>
      let out := match v with | .num x => hostFn tag x | other => other
      runOps ops row (out :: st)
  | .host _ :: _, _, [] => .err "stack"

/-- Staging an unsupported expression into a bytecode pipeline plus host calls
    preserves values, NULL and the first error. -/
def Goal : Prop := ∀ (e : Expr) (row : List (Option Int)),
  runOps (compile e) row [] = eval e row

example : eval (.add (.konst (some 2)) (.host "neg" (.konst (some 5)))) [] = .num (-3) := by decide
example : runOps (compile (.add (.konst (some 2)) (.host "neg" (.konst (some 5))))) [] [] = .num (-3) := by decide
example : eval (.host "upper" (.konst (some 5))) [] = .err "upper" := by decide

end TinylambOpt.ProposedGoals.GeneratedExprTierSplit
