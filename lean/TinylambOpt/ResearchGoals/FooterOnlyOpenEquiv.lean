import Std

namespace TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv

structure ChunkEntry where
  offset : Nat
  len : Nat
  deriving DecidableEq

def buildDir (chunks : List (List UInt8)) : List ChunkEntry :=
  let rec go (pos : Nat) : List (List UInt8) → List ChunkEntry
    | [] => []
    | c :: rest => ⟨pos, c.length⟩ :: go (pos + c.length) rest
  go 0 chunks

structure EagerFile where
  chunks : List (List UInt8)
  bytesRead : Nat
  deriving DecidableEq

structure LazyHandle where
  buf : List UInt8
  dataEnd : Nat
  dir : List ChunkEntry
  bytesRead : Nat
  deriving DecidableEq

def serialize (chunks : List (List UInt8)) (footer garbage : List UInt8) : List UInt8 :=
  chunks.flatten ++ footer ++ garbage

def eagerOpen (chunks : List (List UInt8)) (footer garbage : List UInt8) : EagerFile :=
  let _ := serialize chunks footer garbage
  ⟨chunks, chunks.flatten.length + footer.length⟩

def footerOpen (chunks : List (List UInt8)) (footer garbage : List UInt8) : LazyHandle :=
  let buf := serialize chunks footer garbage
  ⟨buf, chunks.flatten.length, buildDir chunks, footer.length⟩

def lazyReadCol (h : LazyHandle) (col : Nat) : Option (List UInt8) × LazyHandle :=
  match h.dir[col]? with
  | none => (none, h)
  | some entry =>
      let slice := (h.buf.drop entry.offset).take entry.len
      (some slice, { h with bytesRead := h.bytesRead + entry.len })

def lazyReadAll (h : LazyHandle) : List Nat → List (Option (List UInt8)) × Nat
  | [] => ([], h.bytesRead)
  | c :: cs =>
      let (res, h') := lazyReadCol h c
      let (rest, totalBytes) := lazyReadAll h' cs
      (res :: rest, totalBytes)

/-- Footer-only TLCS v3 open (`table/tlcs_file.cpp`): opening reads only the
    footer directory regardless of total column payload size or trailing bytes,
    and subsequent on-demand column reads recover exact column byte slices. -/
def Goal : Prop :=
  ∀ (chunks : List (List UInt8)) (footer garbage : List UInt8) (cols : List Nat),
    let ef := eagerOpen chunks footer garbage
    let lh := footerOpen chunks footer garbage
    let (lazyRes, totalLazyBytes) := lazyReadAll lh cols
    lazyRes = cols.map (fun c => ef.chunks[c]?) ∧
    lh.bytesRead = footer.length ∧
    totalLazyBytes = footer.length +
      ((cols.filterMap (fun c => lh.dir[c]?)).map ChunkEntry.len).sum

private def c0 : List UInt8 := [1, 2, 3]
private def c1 : List UInt8 := [4, 5]
private def c2 : List UInt8 := [6, 7, 8, 9]
private def ftr : List UInt8 := [0xAA, 0xBB]
private def junk : List UInt8 := [0xFF, 0xEE, 0xDD]

example :
    let lh := footerOpen [c0, c1, c2] ftr junk
    lazyReadAll lh [2, 0, 5] = ([some c2, some c0, none], ftr.length + 7) ∧
    lh.bytesRead = ftr.length := by
  decide

example :
    (footerOpen [c0, c1, c2] ftr junk).bytesRead =
      (footerOpen [c0, c1, c2] ftr []).bytesRead := by
  decide

end TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv
