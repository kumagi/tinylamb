import Std.Tactic

/-! Soundness of decoding a selection bit mask into contiguous runs of set
    bits ("mask to ranges") and gathering only those runs.

    Reference computation: `selectedRows` walks the mask lane by lane and
    records the physical row number of every set bit, in order.
    Optimized computation: `runs` compresses consecutive set bits into
    maximal `[start, length)` ranges; `decode` expands those ranges back into
    physical row numbers. The theorem shows both produce the *same sequence
    of physical row numbers*, so a subsequent gather of row values agrees
    too (`mask_to_ranges_sound_values`).

    Hypotheses realised by the caller (see `executor/selection_vector.cpp`):
      * `base` is the physical row number of mask lane 0 (the physical row
        origin is known);
      * `mask` has already been truncated to the valid row count of the
        final word, so padding bits are not present (a padded word of all
        ones would otherwise select phantom rows);
      * word boundaries are invisible to `runs`, hence the bit-63/bit-0
        adjacency case is merged automatically. `mask_to_ranges_merge_words`
        states that decoding per word and concatenating equals decoding the
        concatenated mask, which is the adjacent-range merge used by the
        C++ decoder. -/

namespace TinylambOpt.MaskToRanges

/-- A maximal run of set lanes: physical start row and run length. -/
abbrev Range := Nat × Nat

/-- Expand `[start, start + length)` into physical row numbers. -/
def expandRange (start : Nat) : Nat → List Nat
  | 0 => []
  | n + 1 => start :: expandRange (start + 1) n

theorem expandRange_succ (start len : Nat) :
    expandRange start (len + 1) = expandRange start len ++ [start + len] := by
  induction len generalizing start with
  | zero => simp [expandRange]
  | succ k ih =>
      change start :: expandRange (start + 1) (k + 1) =
        (start :: expandRange (start + 1) k) ++ [start + (k + 1)]
      rw [ih (start + 1)]
      simp only [List.cons_append]
      rw [show start + 1 + k = start + (k + 1) by omega]

theorem expandRange_append_one (start len : Nat) :
    expandRange start (len + 1) =
      (if len = 0 then [] else expandRange start len) ++ [start + len] := by
  cases len with
  | zero => simp [expandRange]
  | succ k => simpa using expandRange_succ start (k + 1)

/-- Streaming decoder. `pos` is the next physical row, `start`/`len`
    describe the currently open run (invariant `start + len = pos`). -/
def runsGo (pos : Nat) (start : Nat) (len : Nat) : List Bool → List Range
  | [] => if len = 0 then [] else [(start, len)]
  | b :: xs =>
      if b then runsGo (pos + 1) start (len + 1) xs
      else (if len = 0 then [] else [(start, len)]) ++ runsGo (pos + 1) (pos + 1) 0 xs

/-- Decode the set lanes of `mask` into maximal contiguous ranges, expressed
    as physical row numbers starting at `base`. -/
def runs (base : Nat) (mask : List Bool) : List Range := runsGo base base 0 mask

/-- Expand decoded ranges into the sequence of physical row numbers. -/
def decode (ranges : List Range) : List Nat :=
  ranges.flatMap (fun (start, len) => expandRange start len)

/-- Reference: sequential scan of the selection mask. -/
def selectedRows (base : Nat) : List Bool → List Nat
  | [] => []
  | b :: xs => (if b then [base] else []) ++ selectedRows (base + 1) xs

theorem runsGo_sound (pos start len : Nat) (mask : List Bool)
    (hopen : start + len = pos) :
    decode (runsGo pos start len mask) =
      (if len = 0 then [] else expandRange start len) ++ selectedRows pos mask := by
  induction mask generalizing pos start len with
  | nil =>
      by_cases hl : len = 0 <;> simp [runsGo, decode, selectedRows, hl]
  | cons b xs ih =>
      cases b with
      | false =>
          calc
            decode (runsGo pos start len (false :: xs))
                = (if len = 0 then [] else expandRange start len) ++
                    decode (runsGo (pos + 1) (pos + 1) 0 xs) := by
                    by_cases hl : len = 0 <;> simp [runsGo, decode, hl]
            _ = (if len = 0 then [] else expandRange start len) ++ selectedRows (pos + 1) xs := by
                    rw [ih (pos + 1) (pos + 1) 0 rfl]
                    simp
            _ = (if len = 0 then [] else expandRange start len) ++ selectedRows pos (false :: xs) := by
                    simp [selectedRows]
      | true =>
          have hopen' : start + (len + 1) = pos + 1 := by omega
          calc
            decode (runsGo pos start len (true :: xs))
                = decode (runsGo (pos + 1) start (len + 1) xs) := by simp [runsGo]
            _ = expandRange start (len + 1) ++ selectedRows (pos + 1) xs := by
                    simpa using ih (pos + 1) start (len + 1) hopen'
            _ = ((if len = 0 then [] else expandRange start len) ++ [pos]) ++
                    selectedRows (pos + 1) xs := by
                    rw [expandRange_append_one]
                    simp [hopen]
            _ = (if len = 0 then [] else expandRange start len) ++ selectedRows pos (true :: xs) := by
                    simp [selectedRows, List.append_assoc]

theorem selectedRows_append (base : Nat) (xs ys : List Bool) :
    selectedRows base (xs ++ ys) =
      selectedRows base xs ++ selectedRows (base + xs.length) ys := by
  induction xs generalizing base with
  | nil => simp [selectedRows]
  | cons b xs ih =>
      have h := ih (base + 1)
      have harith : base + 1 + xs.length = base + (xs.length + 1) := by omega
      cases b with
      | false => simpa [selectedRows, harith] using h
      | true => simpa [selectedRows, harith] using congrArg (fun l => base :: l) h

theorem selectedRows_replicate (base n : Nat) (xs : List Bool) :
    selectedRows base (List.replicate n true ++ xs) =
      expandRange base n ++ selectedRows (base + n) xs := by
  induction n generalizing base with
  | zero => simp [expandRange]
  | succ n ih =>
      rw [List.replicate_succ]
      change base :: selectedRows (base + 1) (List.replicate n true ++ xs) =
        (base :: expandRange (base + 1) n) ++ selectedRows (base + (n + 1)) xs
      rw [ih (base + 1)]
      simp only [List.cons_append]
      congr 1
      rw [show base + 1 + n = base + (n + 1) by omega]

theorem mask_to_ranges_sound (base : Nat) (mask : List Bool) :
    decode (runs base mask) = selectedRows base mask := by
  simpa [runs] using runsGo_sound base base 0 mask (by omega)

/-- Row values are also identical: the optimized decoder reads exactly the
    rows that the reference scan selects. -/
theorem mask_to_ranges_sound_values (base : Nat) (mask : List Bool)
    (rowAt : Nat → α) :
    (decode (runs base mask)).map rowAt = (selectedRows base mask).map rowAt := by
  rw [mask_to_ranges_sound]

/-- Decoding two words separately and concatenating equals decoding their
    concatenation: the run crossing a word boundary (bit 63 of word `k` and
    bit 0 of word `k+1`) is merged into a single range. -/
theorem mask_to_ranges_merge_words (base : Nat) (m₁ m₂ : List Bool) :
    decode (runs base m₁) ++ decode (runs (base + m₁.length) m₂) =
      decode (runs base (m₁ ++ m₂)) := by
  rw [mask_to_ranges_sound, mask_to_ranges_sound, mask_to_ranges_sound]
  exact (selectedRows_append base m₁ m₂).symm

/-- LSB-first physical words; truncate before run detection so high padding
    bits in the last word can never join a visible range. -/
def wordMask (words : List (BitVec w)) (valid : Nat) : List Bool :=
  (words.flatMap (fun word => (List.range w).map word.getLsbD)).take valid

def SeparatedFrom (lower : Nat) : List Range → Prop
  | [] => True
  | (s,n) :: rs => lower ≤ s ∧ 0 < n ∧ SeparatedFrom (s+n+1) rs

private theorem runs_separated (mask : List Bool) (s n : Nat) :
    SeparatedFrom s (runsGo (s+n) s n mask) := by
  induction mask generalizing s n with
  | nil => cases n <;> simp [runsGo, SeparatedFrom]
  | cons b bs ih =>
    cases b with
    | true => exact ih s (n+1)
    | false =>
      have h := ih (s+n+1) 0
      cases n with
      | succ n => simpa [runsGo, SeparatedFrom] using h
      | zero =>
        simp only [runsGo, Bool.false_eq_true, ↓reduceIte, List.nil_append, Nat.add_zero]
        simp only [Nat.add_zero] at h
        generalize runsGo (s+1) (s+1) 0 bs = rs at *
        cases rs <;> simp_all [SeparatedFrom] <;> omega

theorem physical_ranges_sound (words : List (BitVec w)) (valid base : Nat) :
    let mask := wordMask words valid
    decode (runs base mask) = selectedRows base mask ∧ SeparatedFrom base (runs base mask) :=
  ⟨mask_to_ranges_sound .., runs_separated _ base 0⟩

example : runs 0 (wordMask [0x8000000000000000#64, 0xffffffffffffffff#64] 65) =
    [(63,2)] := by decide

end TinylambOpt.MaskToRanges

-- C++ references to this model or its proof.
-- executor/selection_vector.hpp
