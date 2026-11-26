import Std.Tactic

/-!
Addresses read by the scalar reference: `base + i` for `i < r`.

Addresses read by the lowered short masked-load sequence. Each chunk contributes only
lanes that are still live; out-of-tail lanes are absent.

Named results include `chunk_events_eq_ref`, `masked_load_tail_sound`,
`full_width_load_reads_unassigned`.
-/

namespace TinylambOpt.MaskedLoadTail

/-
# Masked tail load: values and access range together

A batch has `W` lanes and only `r ≤ W` live input elements starting at `base`
inside an allocation `[0, alloc)`.  The **reference** reads the `r` live
elements scalarly, in lane order (`refEvents` / `refLoad`).  The **optimized**
lowering is a sequence of short masked loads: each step loads `c = min C r`
lanes (with `C = 0` meaning "one whole-tail chunk"); lanes beyond `r` are
masked off and emit *no* access event (`chunkEvents`).  `loadFrom` turns the
recorded addresses into values through the memory function.

Soundness compares two observations simultaneously:

1. the values of the `r` live lanes (in lane order) agree with scalar reads;
2. every recorded address lies in `[base, base + r)`, hence inside the
   allocation when `base + r ≤ alloc`.

This models an LLVM masked load or an explicit copy into a temporary buffer;
it deliberately does **not** model "full-width load then `select`": that form
still touched `off + r` and fails the access condition when `r < W`
(`full_width_load_reads_unassigned`).  The case `r = 0` emits no event at all.

## C++ realization

`expression/jit.cpp` tail path: lower the tail into short loads with a
lane predicate `lane < remaining`, advance `base` by the number of *live*
lanes of each chunk, and keep the address list (or prove the loop bound
`addr < base + r`) next to the value code.  Guard with `base + r ≤ alloc`.
-/

/-- Addresses read by the scalar reference: `base + i` for `i < r`. -/
def refEvents (base r : Nat) : List Nat := (List.range r).map (fun i => base + i)

/-- Addresses read by the lowered short masked-load sequence. Each chunk
    contributes only lanes that are still live; out-of-tail lanes are absent. -/
def chunkEvents (base r C : Nat) : List Nat :=
  if r = 0 then [] else
    let c := if C = 0 then r else min C r
    (List.range c).map (fun j => base + j) ++ chunkEvents (base + c) (r - c) C
termination_by r
decreasing_by
  by_cases hC : C = 0
  · simp [hC]
    omega
  · simp [hC]
    omega

/-- Values obtained from a recorded access list. -/
def loadFrom {α : Type} (mem : Nat → α) (events : List Nat) : List α := events.map mem

/-- The lowered access list is exactly the scalar one, in lane order. -/
theorem chunk_events_eq_ref (base r C : Nat) : chunkEvents base r C = refEvents base r := by
  induction r using Nat.strongRecOn generalizing base with
  | ind r ih =>
    by_cases hr : r = 0
    · simp [chunkEvents, refEvents, hr]
    · let c := if C = 0 then r else min C r
      have hcpos : 0 < c := by
        dsimp [c]
        by_cases hC : C = 0 <;> simp [hC] <;> omega
      have hcle : c ≤ r := by
        dsimp [c]
        by_cases hC : C = 0 <;> simp [hC] <;> omega
      have hsmall : r - c < r := by omega
      have ih' := ih (r - c) hsmall (base + c)
      rw [chunkEvents]
      simp only [hr, ↓reduceIte]
      change (List.range c).map (fun j => base + j) ++ chunkEvents (base + c) (r - c) C =
        refEvents base r
      rw [ih']
      dsimp [refEvents]
      have hsum : c + (r - c) = r := Nat.add_sub_of_le hcle
      have hsplit : ∀ (a b : Nat), List.range (a + b) =
          List.range a ++ (List.range b).map (fun j => a + j) := by
        intro a b
        rw [List.range_add]
      have hrange : List.range r = List.range c ++ (List.range (r - c)).map (fun j => c + j) := by
        have hs := hsplit c (r - c)
        rw [hsum] at hs
        exact hs
      rw [hrange, List.map_append]
      simp only [List.map_map]
      congr 2
      funext j
      change base + c + j = base + (c + j)
      omega

/-- **Soundness**: live-lane values match scalar reads, every recorded
    address is inside the live tail, and therefore inside the allocation. -/
theorem masked_load_tail_sound {α : Type} (mem : Nat → α) (base r C alloc : Nat)
    (tailFits : base + r ≤ alloc) :
    let evs := chunkEvents base r C
    loadFrom mem evs = (List.range r).map (fun i => mem (base + i)) ∧
    (∀ a ∈ evs, base ≤ a ∧ a < base + r) ∧
    (∀ a ∈ evs, a < alloc) := by
  dsimp
  have hev := chunk_events_eq_ref base r C
  rw [hev]
  constructor
  · simp [loadFrom, refEvents]
  constructor
  · intro a ha
    simp [refEvents, List.mem_map] at ha
    rcases ha with ⟨i, hi, rfl⟩
    constructor <;> omega
  · intro a ha
    simp [refEvents, List.mem_map] at ha
    rcases ha with ⟨i, hi, rfl⟩
    omega

/-- Boundary trap: a full-width load then `select` still *reads* the first
    out-of-tail address `base + r`; it cannot satisfy the access condition. -/
theorem full_width_load_reads_unassigned (base r W : Nat) (hr : r < W) :
    base + r ∈ (List.range W).map (fun i => base + i) ∧ ¬ (base + r < base + r) := by
  constructor
  · rw [List.mem_map]
    exact ⟨r, by simpa using hr, rfl⟩
  · omega

/-- Boundary case `r = 0`: the masked lowering performs no memory access. -/
theorem empty_tail_has_no_events (base C : Nat) : chunkEvents base 0 C = [] := by
  simp [chunkEvents]

/-- Boundary case: only one byte remains at the end of a page.  The masked
    load reads exactly that byte, and it lies strictly inside the allocation. -/
theorem masked_load_tail_one_byte {α : Type} (_mem : Nat → α) (base C alloc : Nat)
    (fits : base + 1 ≤ alloc) :
    let evs := chunkEvents base 1 C
    evs = [base] ∧ base < alloc := by
  dsimp
  have hev := chunk_events_eq_ref base 1 C
  rw [hev]
  dsimp [refEvents]
  constructor
  · simp
  · omega

/-- Output lanes of a full-width load followed by a `select`: valid lanes keep
    the loaded value, invalid lanes are zeroed.  The select changes only the
    output values; the W memory accesses have already happened. -/
def fullWidthSelect {α : Type} [Zero α] (mem : Nat → α) (base r _W : Nat) : Nat → α :=
  fun i => if i < r then mem (base + i) else 0

/-- Counterexample with an explicit allocation bound.  A full-width load
    reads address `total` itself when `base ≤ total < base + W`; that address
    is outside the allocation `[0, total)`, even though every invalid output
    lane is zeroed by the following select.  This is why the masked lowering
    must be distinguished from full-width-load-then-select. -/
theorem full_width_load_violates_tail_access {α : Type} [Zero α] (mem : Nat → α)
    (base r W total : Nat) (_hr : r < W) (hle : base ≤ total)
    (hgt : total < base + W) :
    (∀ i, r ≤ i → i < W → fullWidthSelect mem base r W i = 0) ∧
      ∃ a ∈ (List.range W).map (fun i => base + i), total ≤ a := by
  constructor
  · intro i hri hiW
    have hnot : ¬ i < r := by omega
    simp [fullWidthSelect, hnot]
  · have hWpos : 0 < W := by omega
    refine ⟨base + (W - 1), ?_, ?_⟩
    · rw [List.mem_map]
      exact ⟨W - 1, List.mem_range.mpr (Nat.sub_one_lt (Nat.ne_of_gt hWpos)), rfl⟩
    · omega

/-- Operational read trace of a masked instruction. Inactive lanes emit no
    memory event. Mask construction and decoding use opposite representations. -/
def maskedEvents (base : Nat) (mask : BitVec w) :=
  ((List.range w).filter mask.getLsbD).map (base + ·)

theorem generated_tail_load (base live width : Nat) (fits : live ≤ width) :
    let mask := BitVec.ofBoolListLE ((List.range width).map (fun i => decide (i < live)))
    maskedEvents base mask = refEvents base live := by
  intro mask
  unfold maskedEvents refEvents
  simp only [List.length_map,List.length_range]
  congr 1
  have heq : (List.range width).filter mask.getLsbD =
      (List.range width).filter (fun i => decide (i < live)) := by
    apply List.filter_congr
    intro i hi
    unfold mask
    rw [BitVec.getLsbD_ofBoolListLE,List.getD_eq_getElem?_getD]
    simp [List.mem_range.mp hi]
  rw [heq]
  have split : List.range width = List.range live ++
      (List.range (width-live)).map (live+·) := by rw [← List.range_add]; congr 1; omega
  rw [split,List.filter_append]
  have yes : (List.range live).filter (fun i => decide (i < live)) = List.range live :=
    List.filter_eq_self.mpr (fun i hi => decide_eq_true (List.mem_range.mp hi))
  have no : ((List.range (width-live)).map (live+·)).filter (fun i => decide (i < live)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro i hi
    obtain ⟨j,_,rfl⟩ := List.mem_map.mp hi
    simp
  simp [yes,no]

end TinylambOpt.MaskedLoadTail
