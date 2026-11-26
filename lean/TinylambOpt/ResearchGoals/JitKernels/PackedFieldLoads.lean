import TinylambOpt.ResearchGoals.Compression.Wire

/-! Compile per-width loads and comparisons directly against the existing little-endian bitstream model. Natural bytes here represent canonical wire bytes; bounds and tail overreads are separate obligations. -/
namespace TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

open TinylambOpt.ResearchGoals.Compression.Wire
def span (w i : Nat) := byteCount (i*w%8+w)
def within (bs : Bytes) (w i readWidth : Nat) := i*w/8 + readWidth ≤ bs.length
def load (bs : Bytes) (w i n : Nat) := (fromLittle ((bs.drop (i*w/8)).take n) / 2^(i*w%8)) % 2^w
def bitOffset (w i : Nat) := w*i/8

/-- At width up to 64, two adjacent machine words contain any single unaligned packed field starting at its first byte. -/
def TwoWordCrossingDecode.Goal : Prop := ∀ bs w i, w ≤ 64 → (∀ b ∈ bs, b < 256) → load bs w i 16 = takeAt bs w i

/-- A single eight-byte load suffices for every packed value no wider than 63 bits, independent of phase. -/
def SingleWordUnalignedWidth.Goal : Prop := ∀ bs w i, w ≤ 63 → (∀ b ∈ bs, b < 256) → load bs w i 8 = takeAt bs w i

/-- Specialize shift and byte stride once per repeating packing phase. -/
def BytePhaseLoopVersion.Goal : Prop := ∀ w i period, 8 ∣ period*w →
  ((i+period)*w%8,bitOffset w (i+period)) = (i*w%8,bitOffset w i+period*w/8)

/-- A valid logical lane guarantees the entire speculative eight-byte read is inside the compressed allocation. -/
def LastFieldReadFootprint.Goal : Prop := ∀ w xs i, 0 < w → w ≤ 64 → i < xs.length → within (pack w xs) w i 8

/-- Guard the final lane's fixed-width footprint once to license all earlier lane loads. -/
def GroupLastLaneBoundsHoist.Goal : Prop := ∀ bs w last bytes, within bs w last bytes → ∀ i ≤ last, within bs w i bytes

/-- Physical zero padding can preserve decoded output through the last field without changing its stored length. -/
def ZeroPaddingMaskedWord.Goal : Prop := ∀ w xs i, i < xs.length → load (pack w xs ++ List.replicate 8 0) w i 8 = load (pack w xs) w i 8

/-- Generate a single decoder for two concatenated packed pages when the first one ends at a byte boundary. -/
def ByteAlignedFrameConcatenation.Goal : Prop := ∀ w a b, 8 ∣ a.length*w → pack w (a++b) = pack w a ++ pack w b

/-- Unpack two adjacent fields by reading one twice-wide field at their shared bit offset. -/
def ExtractAdjacentWidePair.Goal : Prop := ∀ bs w j, (∀ b ∈ bs, b < 256) →
  takeAt bs (2*w) j = takeAt bs w (2*j) + 2^w * takeAt bs w (2*j+1)

end TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
