import TinylambOpt.ResearchGoals.Atlas.EncodedPages

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

def mkC (n split param : Nat) (base step bias : Int) : Config := ⟨n, split, param, base, step, bias, []⟩

def configs : List Config :=
  (List.range 5).flatMap fun n =>
  (List.range 6).flatMap fun sp =>
  (List.range 3).flatMap fun p =>
  [-1,0,1].flatMap fun b =>
  [-2,-1,0,1,2].map fun s => mkC n sp p b s 0

def lanes : List Lane :=
  (List.range 3).flatMap fun st =>
  (List.range 3).flatMap fun str =>
  (List.range 3).flatMap fun n =>
  [-1,0,1].flatMap fun b =>
  [-1,0,1].map fun s => ⟨st,str,n,b,s⟩

def masks : List Progression :=
  [-2,0,1].flatMap fun lo =>
  (List.range 3).flatMap fun str =>
  (List.range 3).map fun n => ⟨lo,str,n⟩

def checkResidue (L : Layout) (c : Config) (b : Lane) : Bool :=
  ((rows L c).map (fun r => (List.range b.n).countP
    (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum ==
  ((L.lanes c).map (joinCount · b)).sum

def checkDelete (L : Layout) (c : Config) (m : Progression) : Bool :=
  (((rows L c).filter (fun r => !contains m r.1)).map Prod.snd).sum ==
  ((L.lanes c).map (fun a => partialSum a a.n - deletedSum a m)).sum

def checkAligned (L : Layout) (c : Config) (b : Lane) : Bool :=
  (((rows L c).map (fun r => ((List.range b.n).map (fun (i : Nat) =>
    if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum) ==
  ((L.lanes c).map (aligned · b)).sum

def checkSlice (L : Layout) (c : Config) (lo hi : Nat) : Bool :=
  let selected := (rows L c).filter (fun r => lo ≤ r.1 && r.1 < hi)
  let ls := L.lanes c
  (selected.length,(selected.map Prod.snd).sum) =
    ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
     (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

def pairs := configs.flatMap fun c => lanes.map fun b => (c,b)
def c_masks := configs.flatMap fun c => masks.map fun m => (c,m)
def slices := configs.flatMap fun c => (List.range 6).flatMap fun lo => (List.range 6).map fun hi => (c,lo,hi)

def findFirst (name : String) {α : Type} (xs : List α) (f : α → Bool) : IO Unit := do
  match xs.findIdx? (fun x => ! f x) with
  | none => IO.println (name ++ ": ALL OK (" ++ toString xs.length ++ " cases)")
  | some i => IO.println (name ++ ": FAIL at index " ++ toString i ++ " of " ++ toString xs.length)

def checkAll (name : String) (L : Layout) : IO Unit := do
  findFirst (name ++ ".res") pairs (fun p => checkResidue L p.1 p.2)
  findFirst (name ++ ".del") c_masks (fun p => checkDelete L p.1 p.2)
  findFirst (name ++ ".ali") pairs (fun p => if p.2.stride > 0 then checkAligned L p.1 p.2 else true)
  findFirst (name ++ ".sli") slices (fun p => if p.2.1 ≤ p.2.2 then checkSlice L p.1 p.2.1 p.2.2 else true)

def main : IO Unit := do
  checkAll "LinearOrdinal" TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout
  checkAll "SaturatingCounter" TinylambOpt.ResearchGoals.Atlas.EncodedPages.SaturatingCounter.layout
  checkAll "SegmentedSlope" TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope.layout
  checkAll "MirroredTail" TinylambOpt.ResearchGoals.Atlas.EncodedPages.MirroredTail.layout
  checkAll "BucketRuns" TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns.layout

#eval main
