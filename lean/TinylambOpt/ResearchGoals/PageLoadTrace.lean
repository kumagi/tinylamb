import Std

/-!
These claims compare page-load traces for alternative read paths. A SQL scan can see the
same data under the modeled conditions while changing its page-load work.

Modeled claims

- `Goal` — `∀ pages needle width, 0 < width → let r := scan pages needle width r.hits =
  reference needle (pages.flatMap Page.bytes) ∧ r.reads = (if needle.isEmpty then []
  else pages.flatMap fun p => (List.range p.bytes.length).map (p.base + ·))`
-/

namespace TinylambOpt.ResearchGoals.PageLoadTrace

structure Page where
  base : Nat
  bytes : List UInt8

-- Masked lanes never invoke a read. Each successful read returns its address.
def load (p : Page) (offset width : Nat) : List (Nat × UInt8) :=
  (List.range width).filterMap fun lane =>
    (p.bytes[offset + lane]?).map (fun b => (p.base + offset + lane, b))

def reference (needle data : List UInt8) : List Nat :=
  (List.range (data.length + 1)).filter fun i => decide (needle <+: data.drop i)

structure Scan where
  pos : Nat := 0
  carry : List UInt8 := []
  hits : List Nat := []
  reads : List Nat := []
  deriving Inhabited

def scanChunk (s : Scan) (needle : List UInt8) (chunk : List (Nat × UInt8)) : Scan :=
  let window := s.carry ++ chunk.map Prod.snd
  let offset := s.pos - s.carry.length
  let hits := (reference needle window).filter (fun i => s.carry.length < i + needle.length)
  { pos := s.pos + chunk.length
    carry := window.drop (window.length - (needle.length - 1))
    hits := s.hits ++ hits.map (offset + ·)
    reads := s.reads ++ chunk.map Prod.fst }

def scan (pages : List Page) (needle : List UInt8) (width : Nat) : Scan :=
  if needle.isEmpty then
    { hits := List.range ((pages.map (fun p => p.bytes.length)).sum + 1) }
  else pages.foldl (fun s p =>
    (List.range ((p.bytes.length + width - 1) / width)).foldl
      (fun a i => scanChunk a needle (load p (i * width) width)) s) {}

def Goal : Prop := ∀ pages needle width, 0 < width →
  let r := scan pages needle width
  r.hits = reference needle (pages.flatMap Page.bytes) ∧
  r.reads = (if needle.isEmpty then [] else pages.flatMap fun p =>
    (List.range p.bytes.length).map (p.base + ·))

private def pages : List Page := [⟨10,[97,98]⟩, ⟨80,[]⟩, ⟨90,[97,98,97]⟩]
example : (scan pages [97,98,97] 2).hits = [0,2] := by decide
example : (scan pages [120] 4).reads = [10,11,90,91,92] := by decide
example : (scan [] [] 8).hits = [0] := by decide

end TinylambOpt.ResearchGoals.PageLoadTrace
