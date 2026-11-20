import Std.Tactic

/-!
# Literal search and row selection over a TLCS string buffer

The ORC kernel tests the first and last bytes in 32 adjacent start
positions, then verifies the earliest candidate byte-for-byte. The TLCS
consumer maps each exact hit to a row using its exclusive end offset. The
model covers those transformations and their bounds; LLVM, C++ pointer
validity and the TLCS decoder are outside the model.
-/

namespace TinylambOpt.PhysicalJitFusion

abbrev Bytes := Nat → UInt8

def matchesAt (bytes needle : Bytes) (length start : Nat) : Prop :=
  ∀ i < length, bytes (start + i) = needle i

def anchorsMatch (bytes needle : Bytes) (last start : Nat) : Prop :=
  bytes start = needle 0 ∧ bytes (start + last) = needle last

theorem anchor_search_sound (bytes needle : Bytes) (last start : Nat)
    (h : matchesAt bytes needle (last + 1) start) :
    anchorsMatch bytes needle last start :=
  ⟨by simpa using h 0 (by omega), h last (by omega)⟩

/-- Each bit of the vector AND is the conjunction of the two anchor tests
    at the same lane. -/
theorem anchor_mask_lane (first last : BitVec 32) (lane : Nat) :
    (first &&& last).getLsbD lane = (first.getLsbD lane && last.getLsbD lane) := by
  simp

theorem first_mask_lane (mask : BitVec 32) (nonzero : mask ≠ 0) :
    mask.ctz.toNat < 32 ∧ mask.getLsbD mask.ctz.toNat = true ∧
      ∀ lane < mask.ctz.toNat, mask.getLsbD lane = false := by
  have bound := (BitVec.ctz_lt_iff_ne_zero (x := mask)).mpr nonzero
  exact ⟨by simpa [BitVec.lt_def] using bound, BitVec.getLsbD_true_ctz_of_ne_zero nonzero,
    fun _ h => BitVec.getLsbD_false_of_lt_ctz h⟩

/-- Every byte examined by either anchor load or by exact verification of
    any candidate lane is strictly inside the byte buffer. -/
theorem vector_window_load_in_bounds
    (size offset last lane delta : Nat)
    (hWindow : offset + 32 + last ≤ size) (hLane : lane < 32)
    (hDelta : delta ≤ last) :
    offset + lane + delta < size := by omega

theorem scalar_load_in_bounds (size start last delta : Nat)
    (hStart : start + last < size) (hDelta : delta ≤ last) :
    start + delta < size := by omega

/-- A zero mask can skip the whole window. Otherwise the first set bit
    allows skipping its predecessors, and a failed exact comparison
    allows skipping that start as well. -/
theorem skip_anchor_misses (bytes needle : Bytes) (last begin stop : Nat)
    (absent : ∀ j, begin ≤ j → j < stop → ¬ anchorsMatch bytes needle last j) :
    ∀ j, begin ≤ j → j < stop → ¬ matchesAt bytes needle (last + 1) j := by
  intro j lo hi h
  exact absent j lo hi (anchor_search_sound bytes needle last j h)

/-- For a window in start-offset order, exact verification of only the
    anchor hits returns the same first match as exhaustive verification. -/
theorem anchored_first_match (starts : List Nat) (exactMatch anchors : Nat → Bool)
    (sound : ∀ j, exactMatch j = true → anchors j = true) :
    (starts.filter anchors).find? exactMatch = starts.find? exactMatch := by
  induction starts with
  | nil => rfl
  | cons start rest ih =>
    cases hm : exactMatch start with
    | false => cases ha : anchors start <;> simp [hm, ha, ih]
    | true => simp [hm, sound start hm]

def rowMatches (bytes needle : Bytes) (length begin endOffset : Nat) : Prop :=
  ∃ start, begin ≤ start ∧ start + length ≤ endOffset ∧
    matchesAt bytes needle length start

/-- After an exact search starting at the row's beginning (or earlier),
    the first hit decides the row. If it crosses the exclusive end, a
    later start cannot fit either. -/
theorem first_hit_decides_row (bytes needle : Bytes)
    (length cursor begin endOffset hit : Nat)
    (hBegin : cursor ≤ begin) (hHit : begin ≤ hit)
    (hMatch : matchesAt bytes needle length hit)
    (hFirst : ∀ j, cursor ≤ j → j < hit → ¬ matchesAt bytes needle length j) :
    rowMatches bytes needle length begin endOffset ↔ hit + length ≤ endOffset := by
  constructor
  · rintro ⟨start, lower, upper, matchAt⟩
    by_cases fits : hit + length ≤ endOffset
    · exact fits
    · exact False.elim (hFirst start (by omega) (by omega) matchAt)
  · exact fun fits => ⟨hit, hHit, fits, hMatch⟩

/-- Rows preceding the hit cannot contain an occurrence, including empty
    rows with duplicate end offsets. -/
theorem row_before_first_hit (bytes needle : Bytes)
    (length cursor begin endOffset hit : Nat)
    (hLen : 0 < length) (hBegin : cursor ≤ begin) (hEnd : endOffset ≤ hit)
    (hFirst : ∀ j, cursor ≤ j → j < hit → ¬ matchesAt bytes needle length j) :
    ¬ rowMatches bytes needle length begin endOffset := by
  rintro ⟨start, lower, upper, matchAt⟩
  exact hFirst start (by omega) (by omega) matchAt

/-- Intersecting a previous candidate list with the exact literal result
    and residual predicates keeps order and multiplicities. A NULL flag
    makes the literal fail at the WHERE boundary. -/
theorem refine_candidates_sound {Row : Type} (rows : List Row)
    (previous nullFlag literal residual : Row → Bool) :
    ((rows.filter previous).filter (fun r => !nullFlag r && literal r)).filter residual =
      rows.filter (fun r => previous r && (!nullFlag r && literal r) && residual r) := by
  simp [Bool.and_comm, Bool.and_left_comm]

/-- Final overlapping vector. Bits below the current cursor are ignored,
    and the byte loads still end within the row. -/
theorem overlapping_tail_safe (limit cursor : Nat)
    (width : 32 ≤ limit) (pending : cursor < limit) :
    let base := min cursor (limit - 32)
    base ≤ cursor ∧ cursor - base < 32 ∧ base + 32 ≤ limit := by
  dsimp
  omega

def shiftedLiteralMask : List (Nat × BitVec 64) → BitVec 64
  | [] => ~~~0
  | (offset, equals) :: rest => (equals >>> offset) &&& shiftedLiteralMask rest

/-- Each character comparison is shifted to its candidate start lane.
    Intersection leaves precisely the starts satisfying every comparison. -/
theorem shifted_literal_mask_exact (equals : List (Nat × BitVec 64))
    (start : Nat) (valid : start < 64) :
    (shiftedLiteralMask equals).getLsbD start =
      equals.all (fun (offset, mask) => mask.getLsbD (start + offset)) := by
  induction equals with
  | nil =>
    change (~~~(0 : BitVec 64)).getLsbD start = true
    rw [BitVec.getLsbD_not]
    simp [valid]
  | cons entry rest ih =>
    rcases entry with ⟨offset, mask⟩
    simp [shiftedLiteralMask, ih, Nat.add_comm]

theorem literal_mask_tail_safe (size last cursor : Nat)
    (wide : 64 ≤ size) (length : last < 64) (pending : cursor + last < size) :
    let base := min cursor (size - 64)
    base + 64 ≤ size ∧ base ≤ cursor ∧ cursor - base < 64 - last := by
  dsimp
  omega

theorem literal_before_length_gate {Row : Type} (rows : List Row)
    (literal lengthGate : Row → Bool) :
    (rows.filter lengthGate).filter literal =
      (rows.filter literal).filter lengthGate := by
  simp [List.filter_filter, Bool.and_comm]

theorem zero_ends_give_empty_gate (starts ends : List Nat)
    (zero : ∀ e ∈ ends, e = 0) :
    (starts.zip ends).filter (fun (start, finish) => finish - start != 0) = [] := by
  induction starts generalizing ends with
  | nil => simp
  | cons start starts ih =>
    cases ends with
    | nil => simp
    | cons finish ends =>
      have head := zero finish (by simp)
      have tail : ∀ e ∈ ends, e = 0 := by
        intro e mem
        exact zero e (by simp [mem])
      simp [head, ih ends tail]

/-- Selected strings can follow different prefixes in the source and packed
    buffers. Slicing at each corresponding prefix length returns the same bytes. -/
theorem sparse_varchar_projection (source packed row sourceRest packedRest : List UInt8) :
    (((source ++ row ++ sourceRest).drop source.length).take row.length) =
      (((packed ++ row ++ packedRest).drop packed.length).take row.length) := by
  simp only [List.append_assoc, List.drop_left, List.take_left]

end TinylambOpt.PhysicalJitFusion
