import TinylambOpt.ResearchGoals.BlockCountModel

namespace TinylambOpt.ResearchGoals.BigramChainSkip

open BlockCountModel
abbrev Bytes := List UInt8
abbrev W := BitVec 64

def grams (s : Bytes) := s.zip (s.drop 1)
def flag (p : UInt8 × UInt8) : W := (1 : W) <<< ((17*p.1.toNat+p.2.toNat)%64)
def sketch (s : Bytes) : W := (grams s).foldl (fun a p => a ||| flag p) 0
def summary (xs : List (Option Bytes)) : W :=
  xs.foldl (fun a x => a ||| sketch (x.getD [])) 0

-- Ordered literal fragments separated by SQL %, under binary byte semantics.
def chain (s : Bytes) : List Bytes → Bool
  | [] => true
  | t :: ts => (List.range (s.length+1)).any fun i =>
    decide (t <+: s.drop i) && chain (s.drop (i+t.length)) ts

def candidate (frags : List Bytes) (s : W) :=
  frags.all fun f => (sketch f &&& s) == sketch f

def Goal : Prop := ∀ frags (pages : List (Block (Option Bytes) W)),
  (∀ p ∈ pages, p.header = summary p.data) →
  let test := fun (row : Option Bytes) => row.any (chain · frags)
  let r := execute (fun m => if candidate frags m then .inspect else .skip) test pages
  r.count = ((pages.flatMap Block.data).filter test).length ∧
  ∀ p ∈ pages.zipIdx, p.2 ∉ reads r →
    ∃ f ∈ frags, ∃ g ∈ grams f, (flag g &&& p.1.header) = 0

private def page (xs : List (Option Bytes)) : Block (Option Bytes) W := ⟨summary xs,xs,xs.length⟩
example : execute (fun m => if candidate [[0,0]] m then .inspect else .skip)
    (fun row => row.any (chain · [[0,0]]))
    [page [some [0,64]], page [some [0], some [0]], page [some [0,0]]] = ⟨1,[2,0]⟩ := by decide

end TinylambOpt.ResearchGoals.BigramChainSkip
