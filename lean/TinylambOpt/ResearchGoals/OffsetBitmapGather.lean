import Std

/-!
These claims relate offsets and selection bits to a reference gather. A scan can read
the selected payloads late while preserving their positions and order.

Modeled claims

- `Goal` — `∀ off k (pages : List Page), let r := run off k pages.zipIdx let rows :=
  pages.zipIdx.flatMap fun (p,i) => (selected p).map fun lane => ((i,lane),p.payload
  lane) let slice := (rows.drop off).take k r.values = slice.map Prod.snd ∧ r.reads =
  slice.map Prod.fst ∧ r.opened = (slice.map (fun x => x.1.1)).eraseDups`
-/

namespace TinylambOpt.ResearchGoals.OffsetBitmapGather

abbrev W := BitVec 64
structure Page where
  live : W
  payload : Nat → Int

def selected (p : Page) := (List.range 64).filter p.live.getLsbD
abbrev Addr := Nat × Nat
structure Out where
  values : List Int := []
  reads : List Addr := []
  opened : List Nat := []
  deriving DecidableEq

def run (off need : Nat) : List (Page × Nat) → Out
  | [] => {}
  | (p,i)::tail =>
    if need = 0 then {} else
    let lanes := selected p
    if lanes.length ≤ off then run (off-lanes.length) need tail else
    let kept := (lanes.drop off).take need
    let rest := run 0 (need-kept.length) tail
    ⟨kept.map p.payload ++ rest.values, kept.map (i,·) ++ rest.reads, i :: rest.opened⟩

def Goal : Prop := ∀ off k (pages : List Page),
  let r := run off k pages.zipIdx
  let rows := pages.zipIdx.flatMap fun (p,i) =>
    (selected p).map fun lane => ((i,lane),p.payload lane)
  let slice := (rows.drop off).take k
  r.values = slice.map Prod.snd ∧ r.reads = slice.map Prod.fst ∧
    r.opened = (slice.map (fun x => x.1.1)).eraseDups

private def page (w : W) : Page := ⟨w,Int.ofNat⟩
example : run 2 1 [page 9,page 0,page 32,page 7].zipIdx = ⟨[5],[(2,5)],[2]⟩ := by cbv
example : run 0 0 [page 1].zipIdx = {} := by decide

end TinylambOpt.ResearchGoals.OffsetBitmapGather
