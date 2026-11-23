import TinylambOpt.ResearchGoals.BlockCountModel

namespace TinylambOpt.ResearchGoals.CongruenceCount

open BlockCountModel

structure Meta where
  anchor : Nat
  stride : Nat

def summary (xs : List Nat) : Meta :=
  let a := xs.headD 0
  ⟨a, xs.foldl (fun d x => Nat.gcd d ((x-a)+(a-x))) 0⟩

def classify (m r : Nat) (s : Meta) : Mode :=
  let g := Nat.gcd m s.stride
  if s.anchor % g != r % g then .skip
  else if s.stride % m = 0 then .all else .inspect

def test (m r x : Nat) := x % m == r % m

def Goal : Prop := ∀ m r (pages : List (Block Nat Meta)), 0 < m →
  (∀ b ∈ pages, b.header = summary b.data ∧ b.size = b.data.length) →
  let res := execute (classify m r) (test m r) pages
  res.count = ((pages.flatMap Block.data).filter (test m r)).length ∧
  ∀ b ∈ pages.zipIdx, b.2 ∈ reads res →
    let s := b.1.header
    (∃ k, test m r (s.anchor + k*s.stride) = true) ∧
    (∃ k, test m r (s.anchor + k*s.stride) = false)

private def page (xs : List Nat) : Block Nat Meta := ⟨summary xs,xs,xs.length⟩
example : execute (classify 4 1) (test 4 1)
    [page [11,3], page [9,1,5], page [7,1,3], page []] = ⟨4,[2]⟩ := by decide

end TinylambOpt.ResearchGoals.CongruenceCount
