import TinylambOpt.ResearchGoals.BlockCountModel

/-!
These claims use compact bit signatures to count matching keys. A scan must retain any
information needed to distinguish keys sharing the same signature.

Modeled claims

- `Goal` — `∀ mask want (pages : List (Block W Meta)), (∀ b ∈ pages, b.header = summary
  b.data ∧ b.size = b.data.length) → let r := execute (classify mask want) (test mask
  want) pages r.count = ((pages.flatMap Block.data).filter (test mask want)).length ∧
  ∀ b ∈ pages.zipIdx, b.2 ∈ reads r → ∃ i < 64, mask.getLsbD i = true ∧ (∃ x ∈
  b.1.data, x.getLsbD i = true) ∧ (∃ y ∈ b.1.data, y.getLsbD i = false)`
-/

namespace TinylambOpt.ResearchGoals.BitSignatureCount

open BlockCountModel
abbrev W := BitVec 64
structure Meta where
  must : W
  may : W

def summary (xs : List W) : Meta :=
  ⟨xs.foldl (· &&& ·) (~~~0), xs.foldl (· ||| ·) 0⟩

def test (mask want x : W) := (x &&& mask) == (want &&& mask)
def classify (mask want : W) (s : Meta) : Mode :=
  let ones := mask &&& want
  let zeros := mask &&& ~~~want
  if (ones &&& ~~~s.may) != 0 || (zeros &&& s.must) != 0 then .skip
  else if ((s.may ^^^ s.must) &&& mask) = 0 then .all else .inspect

def Goal : Prop := ∀ mask want (pages : List (Block W Meta)),
  (∀ b ∈ pages, b.header = summary b.data ∧ b.size = b.data.length) →
  let r := execute (classify mask want) (test mask want) pages
  r.count = ((pages.flatMap Block.data).filter (test mask want)).length ∧
  ∀ b ∈ pages.zipIdx, b.2 ∈ reads r →
    ∃ i < 64, mask.getLsbD i = true ∧
      (∃ x ∈ b.1.data, x.getLsbD i = true) ∧ (∃ y ∈ b.1.data, y.getLsbD i = false)

private def page (xs : List W) : Block W Meta := ⟨summary xs,xs,xs.length⟩
example : execute (classify 3 1) (test 3 1) [page [1,5], page [3,7], page [1,2]] =
    ⟨3,[2]⟩ := by decide

end TinylambOpt.ResearchGoals.BitSignatureCount
