import TinylambOpt.ResearchGoals.RowInMask

namespace TinylambOpt.ResearchProofs.RowInMask

open TinylambOpt.ResearchGoals.RowInMask

/-!
Soundness of the compound-`IN` candidate-mask evaluation: the pair of masks
(`sure`, `possible`) accumulated by a `List.foldl` over the columns of the
right-hand table agrees with the scalar per-row three-valued evaluation
`scalar`.  Both mask definitions are characterized bit-wise via
`BitVec.getLsbD`, the three-way `if`-chains of `vector` and `scalar` are
rewritten into propositions about rows, and the two chains are then seen to
branch on the same propositions.
-/

private theorem lt64_of_bit {x : BitVec 64} {i : Nat} (h : x.getLsbD i) : i < 64 := by
  by_cases hi : i < 64
  · exact hi
  · have := BitVec.getLsbD_of_ge x i (by omega)
    rw [this] at h
    cases h

private theorem nz (w : BitVec 64) : w ≠ 0 ↔ ∃ i, w.getLsbD i := by
  constructor
  · intro h
    by_cases hex : ∃ i, w.getLsbD i
    · exact hex
    · exfalso
      apply h
      apply BitVec.eq_of_getLsbD_eq
      intro i _
      cases e : w.getLsbD i with
      | false => exact (BitVec.getLsbD_zero (w := 64) (i := i)).symm
      | true => exact (hex ⟨i, e⟩).elim
  · rintro ⟨i, hi⟩ h
    rw [h] at hi
    exact absurd hi (by simp [BitVec.getLsbD_zero])

private theorem nz64 (w : BitVec 64) : w ≠ 0 ↔ ∃ i, i < 64 ∧ w.getLsbD i := by
  rw [nz]
  constructor
  · rintro ⟨i, hi⟩
    exact ⟨i, lt64_of_bit hi, hi⟩
  · rintro ⟨i, _, hi⟩
    exact ⟨i, hi⟩

/-! ### bit extraction from the `bits` fold -/

private theorem foldl_or_bits (g : Nat → Bool) (w : BitVec 64) (n i : Nat) :
    ((List.range n).foldl (fun w i => w ||| (BitVec.ofNat 64 (g i).toNat <<< i)) w).getLsbD i =
      (w.getLsbD i || (decide (i < n) && decide (i < 64) && g i)) := by
  induction n generalizing w with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil,
        BitVec.getLsbD_or, ih, BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat,
        Nat.testBit_bool_toNat]
      by_cases hn : i < n
      · rw [show (decide (i < n)) = true from decide_eq_true hn,
          show (decide (i < n + 1)) = true from decide_eq_true (by omega),
          show (decide (i - n = 0)) = true from decide_eq_true (by omega)]
        simp
      · by_cases hd : i - n = 0
        · have hsub : i = n := by omega
          rw [hsub]
          simp
        · rw [show (decide (i < n)) = false from decide_eq_false hn,
            show (decide (i - n = 0)) = false from decide_eq_false hd,
            show (decide (i < n + 1)) = false from decide_eq_false (by omega)]
          simp

private theorem getLsbD_bits_eq (g : Nat → Bool) (i : Nat) :
    (bits g).getLsbD i = (decide (i < 64) && g i) := by
  show ((List.range 64).foldl (fun w i => w ||| (BitVec.ofNat 64 (g i).toNat <<< i)) 0).getLsbD i
      = (decide (i < 64) && g i)
  rw [foldl_or_bits]
  by_cases h : i < 64 <;> simp [h]

/-! ### the two folded masks -/

private def sureBit (x : Cell) (f : Nat → Cell) : Nat → Bool := fun i => x.isSome && x == f i

private def possBit (x : Cell) (f : Nat → Cell) : Nat → Bool := fun i =>
  x.isNone || (f i).isNone || x == f i

private def sureMask (enabled : W) (cs : List (Cell × (Nat → Cell))) : W :=
  cs.foldl (fun s p => s &&& bits (sureBit p.1 p.2)) enabled

private def possMask (enabled : W) (cs : List (Cell × (Nat → Cell))) : W :=
  cs.foldl (fun b p => b &&& bits (possBit p.1 p.2)) enabled

private theorem fold_pair (cs : List (Cell × (Nat → Cell))) (a b : W) :
    (cs.foldl (fun (s, p) (x, f) =>
        (s &&& bits (fun i => x.isSome && x == f i),
          p &&& bits (fun i => x.isNone || (f i).isNone || x == f i))) (a, b)) =
      (sureMask a cs, possMask b cs) := by
  induction cs generalizing a b with
  | nil => rfl
  | cons p t ih =>
      cases p with
      | mk x f =>
          simp only [List.foldl_cons]
          rw [ih]
          rfl

private theorem vector_eq (enabled : W) (cols : List (Cell × (Nat → Cell))) :
    vector enabled cols =
      if sureMask enabled cols != 0 then Tri.yes
      else if possMask enabled cols != 0 then Tri.unknown
      else Tri.no := by
  have hkey (a b : W) :
      (match (a, b) with
        | (sure, possible) =>
            if sure != 0 then Tri.yes
            else if possible != 0 then Tri.unknown
            else Tri.no) =
        if a != 0 then Tri.yes else if b != 0 then Tri.unknown else Tri.no := rfl
  have h1 : vector enabled cols =
      (match (sureMask enabled cols, possMask enabled cols) with
        | (sure, possible) =>
            if sure != 0 then Tri.yes
            else if possible != 0 then Tri.unknown
            else Tri.no) := by
    unfold vector
    rw [fold_pair]
  rw [h1, hkey]

/-! ### bit-level descriptions of the masks -/

private theorem sureMask_bit (cs : List (Cell × (Nat → Cell))) (enabled : W) (i : Nat) :
    (sureMask enabled cs).getLsbD i ↔
      (enabled.getLsbD i ∧ i < 64 ∧ ∀ p ∈ cs, p.1.isSome ∧ (p.1 == p.2 i) = true) := by
  induction cs generalizing enabled with
  | nil =>
      simp only [sureMask, List.foldl_nil]
      constructor
      · intro h
        exact ⟨h, lt64_of_bit h, fun p hp => absurd hp (by simp)⟩
      · intro h
        exact h.1
  | cons p t ih =>
      cases p with
      | mk x f =>
          have hstep : sureMask enabled ((x, f) :: t)
              = sureMask (enabled &&& bits (sureBit x f)) t := rfl
          rw [hstep, ih, BitVec.getLsbD_and, getLsbD_bits_eq, List.forall_mem_cons]
          by_cases hE : enabled.getLsbD i <;> by_cases hi : i < 64 <;>
            by_cases hS : x.isSome <;> by_cases hB : (x == f i) = true <;>
              by_cases hT : ∀ q ∈ t, q.1.isSome ∧ (q.1 == q.2 i) = true <;>
                simp [hE, hi, hS, hB, hT, sureBit]

private theorem possMask_bit (cs : List (Cell × (Nat → Cell))) (enabled : W) (i : Nat) :
    (possMask enabled cs).getLsbD i ↔
      (enabled.getLsbD i ∧ i < 64 ∧
        ∀ p ∈ cs, p.1.isNone ∨ (p.2 i).isNone ∨ (p.1 == p.2 i) = true) := by
  induction cs generalizing enabled with
  | nil =>
      simp only [possMask, List.foldl_nil]
      constructor
      · intro h
        exact ⟨h, lt64_of_bit h, fun p hp => absurd hp (by simp)⟩
      · intro h
        exact h.1
  | cons p t ih =>
      cases p with
      | mk x f =>
          have hstep : possMask enabled ((x, f) :: t)
              = possMask (enabled &&& bits (possBit x f)) t := rfl
          rw [hstep, ih, BitVec.getLsbD_and, getLsbD_bits_eq, List.forall_mem_cons]
          by_cases hE : enabled.getLsbD i <;> by_cases hi : i < 64 <;>
            by_cases hN : x.isNone <;> by_cases hM : (f i).isNone <;>
              by_cases hB : (x == f i) = true <;>
                by_cases hT : ∀ q ∈ t, q.1.isNone ∨ (q.2 i).isNone ∨ (q.1 == q.2 i) = true <;>
                  simp [hE, hi, hN, hM, hB, hT, possBit]

/-! ### three-valued comparison facts -/

private theorem eqCell_yes_iff (x y : Cell) :
    eqCell x y = Tri.yes ↔ (x.isSome && x == y) = true := by
  rcases x with _|⟨a⟩ <;> rcases y with _|⟨b⟩ <;> simp [eqCell]

private theorem eqCell_not_no_iff (x y : Cell) :
    eqCell x y ≠ Tri.no ↔ (x.isNone || y.isNone || x == y) = true := by
  rcases x with _|⟨a⟩ <;> rcases y with _|⟨b⟩ <;> simp [eqCell]

private theorem rowEq_no_of (fields : List (Cell × Cell))
    (h : .no ∈ fields.map (fun p => eqCell p.1 p.2)) : rowEq fields = Tri.no := by
  simp only [rowEq]
  rw [if_pos h]

private theorem rowEq_unknown_of (fields : List (Cell × Cell))
    (h1 : .no ∉ fields.map (fun p => eqCell p.1 p.2))
    (h2 : .unknown ∈ fields.map (fun p => eqCell p.1 p.2)) : rowEq fields = Tri.unknown := by
  simp only [rowEq]
  rw [if_neg h1, if_pos h2]

private theorem rowEq_yes_of (fields : List (Cell × Cell))
    (h1 : .no ∉ fields.map (fun p => eqCell p.1 p.2))
    (h2 : .unknown ∉ fields.map (fun p => eqCell p.1 p.2)) : rowEq fields = Tri.yes := by
  simp only [rowEq]
  rw [if_neg h1, if_neg h2]

private theorem rowEq_yes_iff (fields : List (Cell × Cell)) :
    rowEq fields = Tri.yes ↔
      .no ∉ fields.map (fun p => eqCell p.1 p.2) ∧
        .unknown ∉ fields.map (fun p => eqCell p.1 p.2) := by
  by_cases h1 : .no ∈ fields.map (fun p => eqCell p.1 p.2)
  · rw [rowEq_no_of fields h1]
    exact ⟨fun heq => absurd heq (by decide), fun hh => absurd h1 hh.1⟩
  · by_cases h2 : .unknown ∈ fields.map (fun p => eqCell p.1 p.2)
    · rw [rowEq_unknown_of fields h1 h2]
      exact ⟨fun heq => absurd heq (by decide), fun hh => absurd h2 hh.2⟩
    · rw [rowEq_yes_of fields h1 h2]
      exact ⟨fun _ => ⟨h1, h2⟩, fun _ => rfl⟩

private theorem rowEq_not_no_iff (fields : List (Cell × Cell)) :
    rowEq fields ≠ Tri.no ↔ .no ∉ fields.map (fun p => eqCell p.1 p.2) := by
  constructor
  · intro hh hn
    exact hh (rowEq_no_of fields hn)
  · intro hh heq
    by_cases h2 : .unknown ∈ fields.map (fun p => eqCell p.1 p.2)
    · rw [rowEq_unknown_of fields hh h2] at heq
      exact absurd heq (by decide)
    · rw [rowEq_yes_of fields hh h2] at heq
      exact absurd heq (by decide)

private theorem map_eqCell (cs : List (Cell × (Nat → Cell))) (i : Nat) :
    (cs.map fun (x, f) => (x, f i)).map (fun p => eqCell p.1 p.2)
      = cs.map (fun p => eqCell p.1 (p.2 i)) := by
  rw [List.map_map]
  rfl

private theorem rowEq_map_yes (cs : List (Cell × (Nat → Cell))) (i : Nat) :
    rowEq (cs.map fun (x, f) => (x, f i)) = Tri.yes ↔ ∀ p ∈ cs, eqCell p.1 (p.2 i) = Tri.yes := by
  rw [rowEq_yes_iff, map_eqCell]
  simp only [List.mem_map]
  constructor
  · rintro ⟨h1, h2⟩ p hp
    cases e : eqCell p.1 (p.2 i) with
    | yes => rfl
    | no => exact False.elim (h1 ⟨p, hp, e⟩)
    | unknown => exact False.elim (h2 ⟨p, hp, e⟩)
  · intro h
    constructor
    · rintro ⟨a, ha, he⟩
      rw [h a ha] at he
      exact absurd he (by decide)
    · rintro ⟨a, ha, he⟩
      rw [h a ha] at he
      exact absurd he (by decide)

private theorem rowEq_map_not_no (cs : List (Cell × (Nat → Cell))) (i : Nat) :
    rowEq (cs.map fun (x, f) => (x, f i)) ≠ Tri.no ↔ ∀ p ∈ cs, eqCell p.1 (p.2 i) ≠ Tri.no := by
  rw [rowEq_not_no_iff, map_eqCell]
  simp only [List.mem_map]
  constructor
  · intro h p hp he
    exact h ⟨p, hp, he⟩
  · intro h ⟨a, ha, he⟩
    exact absurd he (h a ha)

/-! ### mask non-emptiness vs. scalar truth values -/

private theorem sure_nonzero (enabled : W) (cs : List (Cell × (Nat → Cell))) :
    (sureMask enabled cs != 0) = true ↔
      ∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cs.map fun (x, f) => (x, f i)) = Tri.yes := by
  rw [show (sureMask enabled cs != 0) = true ↔ sureMask enabled cs ≠ 0 from by simp]
  rw [nz64]
  constructor
  · rintro ⟨i, hi, hb⟩
    rw [sureMask_bit] at hb
    have hrow :=
      (rowEq_map_yes cs i).mpr fun p hp =>
        (eqCell_yes_iff p.1 (p.2 i)).mpr (by simpa using hb.2.2 p hp)
    exact ⟨i, hi, hb.1, hrow⟩
  · rintro ⟨i, hi, hb, hr⟩
    refine ⟨i, hi, ?_⟩
    rw [sureMask_bit]
    exact ⟨hb, hi, fun p hp => by
      have h := (eqCell_yes_iff p.1 (p.2 i)).mp ((rowEq_map_yes cs i).mp hr p hp)
      rw [Bool.and_eq_true] at h
      exact h⟩

private theorem poss_nonzero (enabled : W) (cs : List (Cell × (Nat → Cell))) :
    (possMask enabled cs != 0) = true ↔
      ∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cs.map fun (x, f) => (x, f i)) ≠ Tri.no := by
  rw [show (possMask enabled cs != 0) = true ↔ possMask enabled cs ≠ 0 from by simp]
  rw [nz64]
  constructor
  · rintro ⟨i, hi, hb⟩
    rw [possMask_bit] at hb
    have hrow :=
      (rowEq_map_not_no cs i).mpr fun p hp =>
        (eqCell_not_no_iff p.1 (p.2 i)).mpr (by simpa [or_assoc] using hb.2.2 p hp)
    exact ⟨i, hi, hb.1, hrow⟩
  · rintro ⟨i, hi, hb, hr⟩
    refine ⟨i, hi, ?_⟩
    rw [possMask_bit]
    exact ⟨hb, hi, fun p hp => by
      have h := (eqCell_not_no_iff p.1 (p.2 i)).mp ((rowEq_map_not_no cs i).mp hr p hp)
      rw [Bool.or_eq_true, Bool.or_eq_true, or_assoc] at h
      exact h⟩

/-! ### scalar case analysis -/

private theorem yes_mem (enabled : W) (cols : List (Cell × (Nat → Cell))) :
    .yes ∈ ((List.range 64).filter enabled.getLsbD).map
        (fun i => rowEq (cols.map fun (x, f) => (x, f i))) ↔
      ∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cols.map fun (x, f) => (x, f i)) = Tri.yes := by
  simp only [List.mem_map, List.mem_filter, List.mem_range, and_assoc]

private theorem unknown_mem (enabled : W) (cols : List (Cell × (Nat → Cell))) :
    .unknown ∈ ((List.range 64).filter enabled.getLsbD).map
        (fun i => rowEq (cols.map fun (x, f) => (x, f i))) ↔
      ∃ i, i < 64 ∧ enabled.getLsbD i ∧
        rowEq (cols.map fun (x, f) => (x, f i)) = Tri.unknown := by
  simp only [List.mem_map, List.mem_filter, List.mem_range, and_assoc]

private theorem maybe_iff (enabled : W) (cols : List (Cell × (Nat → Cell))) :
    (∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cols.map fun (x, f) => (x, f i)) ≠ Tri.no) ↔
      (∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cols.map fun (x, f) => (x, f i)) = Tri.yes) ∨
        (∃ i, i < 64 ∧ enabled.getLsbD i ∧
          rowEq (cols.map fun (x, f) => (x, f i)) = Tri.unknown) := by
  constructor
  · rintro ⟨i, hi, hb, hn⟩
    cases e : rowEq (cols.map fun (x, f) => (x, f i)) with
    | yes => exact Or.inl ⟨i, hi, hb, e⟩
    | no => exact absurd e hn
    | unknown => exact Or.inr ⟨i, hi, hb, e⟩
  · rintro (⟨i, hi, hb, e⟩ | ⟨i, hi, hb, e⟩)
    · exact ⟨i, hi, hb, by rw [e]; decide⟩
    · exact ⟨i, hi, hb, by rw [e]; decide⟩

open Classical in
theorem sound : TinylambOpt.ResearchGoals.RowInMask.Goal := by
  intro enabled cols
  rw [vector_eq]
  have hs : scalar enabled cols =
      if .yes ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i))) then Tri.yes
      else if .unknown ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i))) then Tri.unknown
      else Tri.no := rfl
  rw [hs]
  rcases Classical.em
      (∃ i, i < 64 ∧ enabled.getLsbD i ∧ rowEq (cols.map fun (x, f) => (x, f i)) = Tri.yes) with
    hy | hy
  · rcases Classical.em
      (∃ i, i < 64 ∧ enabled.getLsbD i ∧
        rowEq (cols.map fun (x, f) => (x, f i)) = Tri.unknown) with hu | hu
    · have h1 : sureMask enabled cols != 0 := by
        rw [sure_nonzero]
        exact hy
      have h3 : .yes ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i))) := by
        rw [yes_mem]
        exact hy
      rw [if_pos h1, if_pos h3]
    · have h1 : sureMask enabled cols != 0 := by
        rw [sure_nonzero]
        exact hy
      have h3 : .yes ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i))) := by
        rw [yes_mem]
        exact hy
      rw [if_pos h1, if_pos h3]
  · rcases Classical.em
      (∃ i, i < 64 ∧ enabled.getLsbD i ∧
        rowEq (cols.map fun (x, f) => (x, f i)) = Tri.unknown) with hu | hu
    · have h1 : ¬ (sureMask enabled cols != 0) := by
        rw [sure_nonzero]
        exact hy
      have h2 : possMask enabled cols != 0 := by
        rw [poss_nonzero, maybe_iff]
        exact Or.inr hu
      have h3 : ¬ (.yes ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i)))) := by
        rw [yes_mem]
        exact hy
      have h4 : .unknown ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i))) := by
        rw [unknown_mem]
        exact hu
      rw [if_neg h1, if_pos h2, if_neg h3, if_pos h4]
    · have h1 : ¬ (sureMask enabled cols != 0) := by
        rw [sure_nonzero]
        exact hy
      have h2 : ¬ (possMask enabled cols != 0) := by
        rw [poss_nonzero, maybe_iff]
        rintro (h | h)
        · exact hy h
        · exact hu h
      have h3 : ¬ (.yes ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i)))) := by
        rw [yes_mem]
        exact hy
      have h4 : ¬ (.unknown ∈ ((List.range 64).filter enabled.getLsbD).map
          (fun i => rowEq (cols.map fun (x, f) => (x, f i)))) := by
        rw [unknown_mem]
        exact hu
      rw [if_neg h1, if_neg h2, if_neg h3, if_neg h4]

end TinylambOpt.ResearchProofs.RowInMask
