import Std.Tactic

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

end TinylambOpt.RankScatter
