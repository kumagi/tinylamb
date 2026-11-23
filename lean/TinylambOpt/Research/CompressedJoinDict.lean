import Std.Tactic

/-!
# Decode-free equi-join across dictionary generations

Two compressed columns carry dictionary *codes* rather than values.  An
equi-join under a collation must compare the decoded SQL values (NULL never
equals anything, including NULL).  A partial translation table keyed by
`(generation, code, generation, code)` may certify the collation comparison
of a code pair *without decoding*; on a miss (or when the collation is not
safe for the table) the engine must fall back to decoding both real values.

Reference computation: `refDec` decides a pair by decoding both values and
applying the collation; `joinD` is the bag equi-join built from that decision.
Optimized computation: `optDec` probes the partial translation table first
and only decodes on a miss (or when `safe = false`).

Hypotheses (realised by
`executor/detail/pipeline/ooc_hash_join_sink.cpp`):
  * `TransSound` — every table entry agrees with the real collation result;
  * `safe = true` only for collations for which the table was built;
  * the generation ids are part of the table key, so a dictionary update
    (a new generation) cannot reuse a stale entry.

`compressed_join_dict_sound` shows the two joins produce the identical bag
of matched row-id pairs.  `compressed_join_dict_zero_decodes_on_hits`
shows that when every pair is a (same-generation) table hit the optimized
join performs **zero** decodes.  The counterexample theorems show that
comparing codes directly is wrong in both directions, and that an unsafe
collation must fall back.
-/

namespace TinylambOpt.Research.CompressedJoinDict

/-- A dictionary: a generation id and a code-to-value map.
    `none` denotes SQL NULL. -/
structure Dict where
  gen : Nat
  val : Nat → Option Int

/-- SQL equality under a collation: NULL never compares equal. -/
def sqlEq (coll : Int → Int → Bool) : Option Int → Option Int → Bool
  | none, _ => false
  | _, none => false
  | some x, some y => coll x y

/-- A partial translation table: `some b` certifies that the code pair is
    collation-equal (`b = true`) or not (`b = false`); `none` is a miss. -/
abbrev TransTable := Nat → Nat → Nat → Nat → Option Bool

/-- Every table entry agrees with the decoded collation result. -/
def TransSound (coll : Int → Int → Bool) (dA dB : Dict) (t : TransTable) : Prop :=
  ∀ cA cB b, t dA.gen cA dB.gen cB = some b →
    sqlEq coll (dA.val cA) (dB.val cB) = b

/-- A per-pair decision: the boolean match result plus the number of real
    value decodes charged to this pair. -/
abbrev PairDecision := Nat → Nat → Bool × Nat

/-- Reference decision: always decode both values. -/
def refDec (coll : Int → Int → Bool) (dA dB : Dict) : PairDecision :=
  fun cA cB => (sqlEq coll (dA.val cA) (dB.val cB), 2)

/-- Probe the partial table; on a hit no decode is needed. -/
def probe (coll : Int → Int → Bool) (safe : Bool) (dA dB : Dict)
    (t : TransTable) (cA cB : Nat) : Bool × Nat :=
  if safe then
    match t dA.gen cA dB.gen cB with
    | some b => (b, 0)
    | none => (sqlEq coll (dA.val cA) (dB.val cB), 2)
  else
    (sqlEq coll (dA.val cA) (dB.val cB), 2)

/-- Optimized decision. -/
def optDec (coll : Int → Int → Bool) (safe : Bool) (dA dB : Dict)
    (t : TransTable) : PairDecision :=
  probe coll safe dA dB t

/-- A table hit (with a safe collation) returns the collation result. -/
theorem probe_eq_ref (coll : Int → Int → Bool) (safe : Bool) (dA dB : Dict)
    (t : TransTable) (hs : TransSound coll dA dB t) (cA cB : Nat) :
    (probe coll safe dA dB t cA cB).1 =
      sqlEq coll (dA.val cA) (dB.val cB) := by
  cases safe with
  | false => rfl
  | true =>
      cases hhit : t dA.gen cA dB.gen cB with
      | none => simp [probe, hhit]
      | some b =>
          have hb := hs cA cB b hhit
          simpa [probe, hhit] using hb.symm

theorem probe_hit_zero_decodes (coll : Int → Int → Bool) (dA dB : Dict)
    (t : TransTable) (cA cB : Nat) (b : Bool)
    (hhit : t dA.gen cA dB.gen cB = some b) :
    (probe coll true dA dB t cA cB).2 = 0 := by
  simp [probe, hhit]

theorem probe_unsafe_decodes (coll : Int → Int → Bool) (dA dB : Dict)
    (t : TransTable) (cA cB : Nat) :
    (probe coll false dA dB t cA cB).2 = 2 := by
  rfl

/- ------------------------------------------------------------------------ -
    Bag equi-join driven by an abstract pair decision
    ------------------------------------------------------------------------ -/

/-- A row is `(row-id, dictionary code)`. -/
abbrev Row := Nat × Nat

/-- Matched row-id pairs of `ra` against a list of right rows. -/
def matchesForD (dec : PairDecision) (ra : Row) : List Row → List (Nat × Nat) × Nat
  | [] => ([], 0)
  | rb :: rbs =>
      let d := dec ra.2 rb.2
      let tl := matchesForD dec ra rbs
      if d.1 then ((ra.1, rb.1) :: tl.1, d.2 + tl.2) else (tl.1, d.2 + tl.2)

/-- Bag equi-join: output pairs plus the total decode count. -/
def joinD (dec : PairDecision) : List Row → List Row → List (Nat × Nat) × Nat
  | [], _ => ([], 0)
  | ra :: ras, rbs =>
      let ms := matchesForD dec ra rbs
      let rest := joinD dec ras rbs
      (ms.1 ++ rest.1, ms.2 + rest.2)

theorem matchesForD_fst_eq (d1 d2 : PairDecision)
    (h : ∀ cA cB, (d1 cA cB).1 = (d2 cA cB).1) (ra : Row) (rbs : List Row) :
    (matchesForD d1 ra rbs).1 = (matchesForD d2 ra rbs).1 := by
  induction rbs with
  | nil => rfl
  | cons rb rbs ih =>
      dsimp [matchesForD]
      rw [h ra.2 rb.2]
      cases (d2 ra.2 rb.2).1 <;> simp [ih]

theorem joinD_fst_eq (d1 d2 : PairDecision)
    (h : ∀ cA cB, (d1 cA cB).1 = (d2 cA cB).1)
    (ras rbs : List Row) :
    (joinD d1 ras rbs).1 = (joinD d2 ras rbs).1 := by
  induction ras with
  | nil => rfl
  | cons ra ras ih =>
      dsimp [joinD]
      rw [matchesForD_fst_eq d1 d2 h ra rbs]
      rw [ih]

theorem matchesForD_cost_zero (dec : PairDecision)
    (hzero : ∀ cA cB, (dec cA cB).2 = 0) (ra : Row) (rbs : List Row) :
    (matchesForD dec ra rbs).2 = 0 := by
  induction rbs with
  | nil => rfl
  | cons rb rbs ih =>
      dsimp [matchesForD]
      rw [hzero ra.2 rb.2, ih]
      split <;> rfl

theorem joinD_cost_zero (dec : PairDecision)
    (hzero : ∀ cA cB, (dec cA cB).2 = 0) (ras rbs : List Row) :
    (joinD dec ras rbs).2 = 0 := by
  induction ras with
  | nil => rfl
  | cons ra ras ih =>
      simp [joinD, matchesForD_cost_zero dec hzero ra rbs, ih]

/- ------------------------------------------------------------------------ -
    Main theorems
    ------------------------------------------------------------------------ -/

/-- **Main theorem.** Under a sound partial translation table the optimized
    join preserves the full bag of matches (and its multiplicity) of the
    reference collation join. -/
theorem compressed_join_dict_sound (coll : Int → Int → Bool) (safe : Bool)
    (dA dB : Dict) (t : TransTable) (hs : TransSound coll dA dB t)
    (rowsA rowsB : List Row) :
    (joinD (optDec coll safe dA dB t) rowsA rowsB).1 =
      (joinD (refDec coll dA dB) rowsA rowsB).1 := by
  apply joinD_fst_eq
  intro cA cB
  exact probe_eq_ref coll safe dA dB t hs cA cB

/-- If every code pair is a table hit, the optimized join performs no
    decodes at all. -/
theorem compressed_join_dict_zero_decodes_on_hits (coll : Int → Int → Bool)
    (dA dB : Dict) (t : TransTable)
    (htotal : ∀ cA cB, ∃ b, t dA.gen cA dB.gen cB = some b)
    (rowsA rowsB : List Row) :
    (joinD (optDec coll true dA dB t) rowsA rowsB).2 = 0 := by
  apply joinD_cost_zero
  intro cA cB
  rcases htotal cA cB with ⟨b, hb⟩
  exact probe_hit_zero_decodes coll dA dB t cA cB b hb

/-- All code-pair equivalence classes are compared correctly: a safe hit
    returns exactly the decoded collation class. -/
theorem all_code_pairs_classes_preserved (coll : Int → Int → Bool) (dA dB : Dict)
    (t : TransTable) (hs : TransSound coll dA dB t) (cA cB : Nat) :
    (probe coll true dA dB t cA cB).1 = sqlEq coll (dA.val cA) (dB.val cB) :=
  probe_eq_ref coll true dA dB t hs cA cB

/- ------------------------------------------------------------------------ -
    Concrete regression instances and counterexamples
    ------------------------------------------------------------------------ -/

/-- Parity collation: two non-NULL values are equal iff they have the same
    parity.  Different integers can therefore be collation-equal. -/
def collParity (x y : Int) : Bool := x % 2 == y % 2

def dictA : Dict :=
  { gen := 0, val := fun c => if c = 1 then some 10 else if c = 2 then some 4 else none }

def dictB : Dict :=
  { gen := 1, val := fun c => if c = 1 then some 21 else if c = 3 then some 6 else none }

/-- The naive (unsafe) optimization: compare codes, ignoring generation and
    collation. -/
def naiveDec : PairDecision := fun cA cB => (decide (cA = cB), 0)

/-- **Counterexample:** the same code in two generations denotes different
    values (10 vs 21), which are not parity-equal.  Comparing codes directly
    produces a spurious join pair. -/
theorem same_code_different_generation_unsound :
    (joinD naiveDec [(0, 1)] [(7, 1)]).1 = [(0, 7)] ∧
    (joinD (refDec collParity dictA dictB) [(0, 1)] [(7, 1)]).1 = [] := by
  native_decide

/-- **Counterexample:** different codes (2 and 3) denote collation-equal
    values (4 and 6, both even).  Comparing codes directly loses the match. -/
theorem different_codes_collation_equal_counterexample :
    (joinD naiveDec [(0, 2)] [(7, 3)]).1 = [] ∧
    (joinD (refDec collParity dictA dictB) [(0, 2)] [(7, 3)]).1 = [(0, 7)] := by
  native_decide

/-- A deliberately stale table entry claiming a match. -/
def bogusTable : TransTable := fun _ _ _ _ => some true

/-- **Counterexample:** with an unsafe collation a stale table entry must be
    ignored; trusting it (the `safe = true` path) gives the wrong answer,
    while the fallback path agrees with the real value comparison. -/
theorem unsafe_collation_must_fallback_counterexample :
    (optDec collParity true dictA dictB bogusTable 1 1).1 = true ∧
    (optDec collParity false dictA dictB bogusTable 1 1).1 = false ∧
    (refDec collParity dictA dictB 1 1).1 = false := by
  native_decide

/-- A table that only knows about the original dictionary generation. -/
def genTable : TransTable := fun gA cA gB cB =>
  if gA = 0 && gB = 1 && cA = 1 && cB = 1 then some false else none

def dictBv2 : Dict := { dictB with gen := 2 }

/-- A dictionary update produces a new generation: the old translation
    entry is a miss for the new generation, so a fresh decode is forced. -/
theorem dict_update_new_generation_is_miss :
    genTable dictA.gen 1 dictB.gen 1 = some false ∧
    genTable dictA.gen 1 dictBv2.gen 1 = none ∧
    (probe collParity true dictA dictBv2 genTable 1 1).2 = 2 := by
  native_decide

end TinylambOpt.Research.CompressedJoinDict
