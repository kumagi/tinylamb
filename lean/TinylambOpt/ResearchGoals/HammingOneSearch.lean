import Std

namespace TinylambOpt.ResearchGoals.HammingOneSearch

abbrev W := BitVec 64
abbrev Bytes := List UInt8

def reference (pat text : Bytes) : List Nat :=
  (List.range (text.length+1)).filter fun i =>
    i+pat.length ≤ text.length &&
    ((pat.zip (text.drop i)).filter fun (a,b) => a != b).length ≤ 1

def eqMask (pat : Bytes) (c : UInt8) : W :=
  pat.zipIdx.foldl (fun w (b,i) =>
    w ||| ((BitVec.ofNat 64 (b == c).toNat) <<< i)) 0

def compile (pat : Bytes) : Array W :=
  Array.ofFn (fun (i : Fin 256) => eqMask pat i.val.toUInt8)

def search (pat text : Bytes) : List Nat := Id.run do
  if pat.isEmpty then return List.range (text.length+1)
  let masks := compile pat
  let mut exact : W := 0
  let mut one : W := 0
  let mut hits := []
  for (c,i) in text.zipIdx do
    let eq := masks[c.toNat]?.getD 0
    let extend := (exact <<< 1) ||| 1
    one := (((one <<< 1) ||| 1) &&& eq) ||| extend
    exact := extend &&& eq
    if one.getLsbD (pat.length-1) then hits := (i+1-pat.length) :: hits
  return hits.reverse

def Goal : Prop := ∀ pat text, pat.length ≤ 64 → search pat text = reference pat text

example : search [97,98,97] [97,97,97,98,97] = [0,2] := by native_decide
example : search [9] [7,8] = [0,1] := by native_decide
example : search [] [7] = [0,1] := by decide

example : search (List.replicate 64 1) (List.replicate 63 1 ++ [2]) = [0] := by native_decide
example : search [1,2] [1] = [] := by native_decide

end TinylambOpt.ResearchGoals.HammingOneSearch
