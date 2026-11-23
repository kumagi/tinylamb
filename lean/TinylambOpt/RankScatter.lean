import Std.Tactic
import TinylambOpt.AdaptiveSelection

/-! Soundness of stable SIMD compression by prefix popcount ("rank scatter").

    Reference computation: `reference` scans the W lanes in order and keeps
    the row of every selected lane.
    Optimized computation: lane `i` is selected iff `mask[i] = true`; its
    output slot is `writtenBefore + countTrue (mask.take i)` (the popcount of
    the lower lanes). `scatterWrites` records the resulting writes as
    `(slot, lane, row)`, and `readBack` reads slots in increasing order from
    `writtenBefore` onward.

    Hypotheses realised by the caller (see `executor/simd_comparison.cpp`):
      * `mask.length = rows.length = W`, the valid lane count of the vector;
      * `base` is the number of rows already written by earlier vectors;
      * `base + countTrue mask ≤ capacity`, so every generated slot is inside
        the output buffer (the generated shift/popcount expression uses a
        lane count below the LLVM vector width).

    The theorem states both that the packed values equal the sequential
    filter result and that all written slots lie in `[base, capacity)` for
    the next vector, so successive vector writes never overlap. -/

namespace TinylambOpt.RankScatter

/-- Number of selected lanes in the whole mask (the batch's popcount). -/
def countTrue : List Bool → Nat
  | [] => 0
  | false :: xs => countTrue xs
  | true :: xs => countTrue xs + 1

/-- Reference: sequential filter of the rows by their selection lanes. -/
def reference : List Bool → List α → List α
  | [], _ => []
  | _, [] => []
  | true :: bs, row :: rs => row :: reference bs rs
  | false :: bs, _ :: rs => reference bs rs

/-- A scatter write: output slot, source lane, and row value. Lane `i`
    writes to slot `base + r` where `r` is the popcount of lower lanes. -/
abbrev Write (α : Type u) := Nat × Nat × α

/-- Generate all scatter writes in increasing lane order. Unequal lengths
    are outside the caller's invariant and produce no writes. -/
def scatterWrites (base : Nat) : Nat → Nat → List Bool → List α → List (Write α)
  | _, _, [], [] => []
  | i, r, b :: bs, row :: rs =>
      (if b then [(base + r, i, row)] else []) ++
        scatterWrites base (i + 1) (if b then r + 1 else r) bs rs
  | _, _, _, _ => []

/-- Value stored at a slot (first matching write), if the slot was written. -/
def readSlot (writes : List (Write α)) (slot : Nat) : Option α :=
  match writes.find? (fun w => w.1 == slot) with
  | some (_, _, v) => some v
  | none => none

/-- Read back `total` consecutive slots starting at `base`. -/
def readBack : Nat → Nat → List (Write α) → List α
  | 0, _, _ => []
  | n + 1, slot, ws => (readSlot ws slot).toList ++ readBack n (slot + 1) ws

theorem readBack_ignore_before (n s t : Nat) (h : s < t) (lane : Nat) (v : α)
    (rest : List (Write α)) :
    readBack n t ((s, lane, v) :: rest) = readBack n t rest := by
  induction n generalizing t with
  | zero => simp [readBack]
  | succ n ih =>
      have hne : s ≠ t := by omega
      simp [readBack, readSlot, hne]
      exact ih (t + 1) (by omega)

theorem readBack_cons_match (v : α) (n slot lane : Nat) (rest : List (Write α)) :
    readBack (n + 1) slot ((slot, lane, v) :: rest) = v :: readBack n (slot + 1) rest := by
  simp [readBack, readSlot]
  exact readBack_ignore_before n slot (slot + 1) (by omega) lane v rest

theorem readBack_eq_aux (base : Nat) (mask : List Bool) :
    ∀ (rows : List α) (i r : Nat), mask.length = rows.length →
    readBack (countTrue mask) (base + r) (scatterWrites base i r mask rows) =
      reference mask rows := by
  induction mask with
  | nil =>
      intro rows i r hlen
      cases rows with
      | nil => simp [readBack, countTrue, reference, scatterWrites]
      | cons row rs => simp at hlen
  | cons b bs ih =>
      intro rows i r hlen
      cases rows with
      | nil => simp at hlen
      | cons row rs =>
          have hlen' : bs.length = rs.length := by simpa using hlen
          cases b with
          | false =>
              simpa [scatterWrites, reference, countTrue] using ih rs (i + 1) r hlen'
          | true =>
              simp only [scatterWrites, ↓reduceIte, List.singleton_append, reference, countTrue]
              rw [readBack_cons_match]
              have h := ih rs (i + 1) (r + 1) hlen'
              rw [show base + r + 1 = base + (r + 1) by omega]
              rw [h]

/-- Every write produced from rank `r` lies in
    `[base + r, base + r + countTrue mask)`: the ranks are contiguous and
    cover each selected lane exactly once. -/
theorem scatterWrites_slot_bounds (base : Nat) (mask : List Bool) :
    ∀ (rows : List α) (i r : Nat), mask.length = rows.length →
    ∀ w ∈ scatterWrites base i r mask rows,
      base + r ≤ w.1 ∧ w.1 < base + r + countTrue mask := by
  induction mask with
  | nil =>
      intro rows i r hlen w hw
      cases rows with
      | nil => simp [scatterWrites] at hw
      | cons row rs => simp at hlen
  | cons b bs ih =>
      intro rows i r hlen w hw
      cases rows with
      | nil => simp at hlen
      | cons row rs =>
          have hlen' : bs.length = rs.length := by simpa using hlen
          rcases w with ⟨slot, lane, val⟩
          cases b with
          | false =>
              simp only [scatterWrites, List.mem_append] at hw
              rcases hw with hhead | htail
              · simp at hhead
              · have hb := ih rs (i + 1) r hlen' (slot, lane, val) htail
                simpa [countTrue] using hb
          | true =>
              simp only [scatterWrites, List.mem_append] at hw
              rcases hw with hhead | htail
              · simp at hhead
                rcases hhead with ⟨rfl, rfl, rfl⟩
                constructor <;> simp [countTrue] <;> omega
              · have hb := ih rs (i + 1) (r + 1) hlen' (slot, lane, val) htail
                constructor
                · omega
                · simp [countTrue]
                  omega

theorem scatterWrites_within_capacity (mask : List Bool) (rows : List α)
    (base capacity : Nat) (hlen : mask.length = rows.length)
    (hcap : base + countTrue mask ≤ capacity) :
    ∀ w ∈ scatterWrites base 0 0 mask rows, base ≤ w.1 ∧ w.1 < capacity := by
  intro w hw
  have hb := scatterWrites_slot_bounds base mask rows 0 0 hlen w hw
  constructor <;> omega

theorem rank_scatter_sound (mask : List Bool) (rows : List α) (base capacity : Nat)
    (hlen : mask.length = rows.length)
    (hcap : base + countTrue mask ≤ capacity) :
    readBack (countTrue mask) base (scatterWrites base 0 0 mask rows) =
        reference mask rows ∧
      (∀ w ∈ scatterWrites base 0 0 mask rows, base ≤ w.1 ∧ w.1 < capacity) := by
  exact ⟨readBack_eq_aux base mask rows 0 0 hlen,
    scatterWrites_within_capacity mask rows base capacity hlen hcap⟩

private theorem count_snoc (xs : List Bool) (b : Bool) :
    countTrue (xs ++ [b]) = countTrue xs + b.toNat := by
  induction xs with
  | nil => cases b <;> rfl
  | cons x xs ih => cases x <;> simp [countTrue, ih] <;> omega

theorem population_rank (word : BitVec w) (n acc : Nat) :
    word.cpopNatRec n acc = acc + countTrue ((List.range n).map word.getLsbD) := by
  induction n generalizing acc with
  | zero => simp [BitVec.cpopNatRec, countTrue]
  | succ n ih =>
    rw [BitVec.cpopNatRec, ih, List.range_succ, List.map_append]
    simp [count_snoc]
    omega

/-- Stop before the next selected lane when the destination is full. The
    physical offset suffices to resume without losing or duplicating a row. -/
theorem bounded_scatter (mask : List Bool) (rows : List α) (cap : Nat)
    (hlen : mask.length = rows.length) :
    let n := AdaptiveSelection.consumedLanes cap mask
    let part := mask.take n
    countTrue part ≤ cap ∧
    readBack (countTrue part) 0 (scatterWrites 0 0 0 part (rows.take n)) ++
      reference (mask.drop n) (rows.drop n) = reference mask rows := by
  dsimp only
  have heq : ∀ n, (mask.take n).length = (rows.take n).length := by simp [hlen]
  rw [(readBack_eq_aux 0 _ _ 0 0 (heq _))]
  clear heq
  induction mask generalizing rows cap with
  | nil => cases cap <;> simp [AdaptiveSelection.consumedLanes, countTrue, reference]
  | cons b bs ih =>
    cases rows with
    | nil => simp at hlen
    | cons x xs =>
      simp at hlen
      cases cap with
      | zero => simp [AdaptiveSelection.consumedLanes, countTrue, reference]
      | succ k =>
        cases b <;> simp [AdaptiveSelection.consumedLanes, Nat.add_comm 1,
          List.take_succ_cons, List.drop_succ_cons, reference, countTrue] <;>
          first | exact ih xs _ hlen | simpa using ih xs _ hlen

/-- Hardware population count on the low bits. Extracting zero bits and all
    bits are both defined; no shift by the lane width is required. -/
theorem lower_bits_popcount (word : BitVec w) (i : Nat) :
    (word.extractLsb' 0 i).cpop.toNat = countTrue ((List.range i).map word.getLsbD) := by
  rw [BitVec.toNat_cpop, population_rank, Nat.zero_add]
  congr 1
  apply List.map_congr_left
  intro j hj
  simp [List.mem_range.mp hj]

private theorem selected_write (mask : List Bool) (rows : List α) (base lane rank i : Nat)
    (hlen : mask.length = rows.length) (sel : mask[i]? = some true) (value : rows[i]? = some v) :
    (base + (rank + countTrue (mask.take i)),lane+i,v) ∈
      scatterWrites base lane rank mask rows := by
  induction mask generalizing rows lane rank i with
  | nil => simp at sel
  | cons b bs ih =>
    cases rows with
    | nil => simp at hlen
    | cons a xs =>
      simp at hlen
      cases i with
      | zero =>
        simp at sel value
        subst b; subst a
        simp [countTrue,scatterWrites]
      | succ i =>
        simp at sel value
        cases b with
        | false =>
          simpa [scatterWrites,countTrue,Nat.add_assoc,Nat.add_comm 1] using
            ih xs (lane+1) rank i hlen sel value
        | true =>
          apply List.mem_cons_of_mem
          simpa [countTrue,Nat.add_assoc,Nat.add_comm 1] using
            ih xs (lane+1) (rank+1) i hlen sel value

theorem packed_scatter_slot (word : BitVec w) (rows : List α) (base i : Nat)
    (hlen : rows.length = w) (hi : i < w) (sel : word.getLsbD i = true)
    (value : rows[i]? = some v) :
    (base + (word.extractLsb' 0 i).cpop.toNat,i,v) ∈
      scatterWrites base 0 0 ((List.range w).map word.getLsbD) rows := by
  rw [lower_bits_popcount]
  simpa [← List.map_take, Nat.min_eq_left (Nat.le_of_lt hi)] using
    selected_write ((List.range w).map word.getLsbD) rows base 0 0 i
      (by simp [hlen]) (by simp [hi,sel]) value

end TinylambOpt.RankScatter
