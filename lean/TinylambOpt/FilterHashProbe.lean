import Std.Tactic.BVDecide

/-! The resident probe paths share an immutable linear-probing index and
the same duplicate chains. The direct cursor advances to the first match.
The compact cursor keeps candidate ordinals in input order and prefetches
their home slots. The cost decision is irrelevant to the joined stream. -/

namespace TinylambOpt.FilterHashProbe

abbrev Slot := Option (Int × Nat)

/-- Slots in linear-probing order, starting at the home bucket. An empty
    slot terminates the search, even if later slots contain the key. -/
def lookup (key : Int) : List Slot → Option Nat
  | [] | none :: _ => none
  | some (k, head) :: rest => if key == k then some head else lookup key rest

theorem lookup_at_first_equal (key : Int) (before : List (Int × Nat))
    (head : Nat) (after : List Slot) (hne : ∀ p ∈ before, key ≠ p.1) :
    lookup key (before.map some ++ some (key, head) :: after) = some head := by
  induction before with
  | nil => simp [lookup]
  | cons pair rest ih =>
    obtain ⟨k, idx⟩ := pair
    have neq := hne (k, idx) (by simp)
    simp [lookup, neq, ih (fun p hp => hne p (by simp [hp]))]

theorem lookup_stops_at_empty (key : Int) (before : List (Int × Nat))
    (after : List Slot) (hne : ∀ p ∈ before, key ≠ p.1) :
    lookup key (before.map some ++ none :: after) = none := by
  induction before with
  | nil => simp [lookup]
  | cons pair rest ih =>
    obtain ⟨k, idx⟩ := pair
    have neq := hne (k, idx) (by simp)
    simp [lookup, neq, ih (fun p hp => hne p (by simp [hp]))]

def hits (probe : α → Option β) (rows : List α) : List (α × β) :=
  rows.filterMap (fun row => (probe row).map (row, ·))

def fusedHits (keep : α → Bool) (probe : α → Option β)
    (rows : List α) : List (α × β) :=
  rows.filterMap (fun row => if keep row then (probe row).map (row, ·) else none)

theorem fused_matches_compact (keep : α → Bool) (probe : α → Option β)
    (rows : List α) :
    fusedHits keep probe rows = hits probe (rows.filter keep) := by
  induction rows with
  | nil => rfl
  | cons row rest ih =>
    cases h : keep row <;> cases p : probe row <;>
      simp [fusedHits, hits, h, p, fusedHits, hits] at ih ⊢ <;> exact ih

/-- Number of accepted rows traversed and ordered suffix when the direct
    cursor stops at its first match or consumes the input. -/
def next (keep : α → Bool) (probe : α → Option β) :
    List α → Nat × Option (α × β × List α)
  | [] => (0, none)
  | row :: rest =>
    if keep row then
      match probe row with
      | some head => (1, some (row, head, rest))
      | none => let (count, stop) := next keep probe rest; (count + 1, stop)
    else next keep probe rest

theorem next_matches_stream (keep : α → Bool) (probe : α → Option β)
    (rows : List α) :
    fusedHits keep probe rows =
      match (next keep probe rows).2 with
      | none => []
      | some (row, head, rest) => (row, head) :: fusedHits keep probe rest := by
  induction rows with
  | nil => rfl
  | cons row rest ih =>
    cases h : keep row <;> cases p : probe row <;>
      simp [next, fusedHits, h, p] at ih ⊢ <;> exact ih

theorem switched_join_stream (useCompact : Bool) (keep : α → Bool)
    (probe : α → Option β) (chain : β → List γ) (project : α → δ)
    (rows : List α) :
    ((if useCompact then hits probe (rows.filter keep)
       else fusedHits keep probe rows).flatMap
       (fun (row, head) => (chain head).map (project row, ·))) =
    (hits probe (rows.filter keep)).flatMap
       (fun (row, head) => (chain head).map (project row, ·)) := by
  cases useCompact <;> simp [fused_matches_compact]

theorem remove_nonnull_conjunct (keep nonnull : α → Bool) (rows : List α)
    (nonnullBatch : ∀ row ∈ rows, nonnull row = true) :
    rows.filter (fun row => keep row && nonnull row) = rows.filter keep := by
  apply List.filter_congr
  intro row mem
  simp [nonnullBatch row mem]

/-- Exact membership for the same build keys is already decided by lookup. -/
theorem skip_exact_membership (keep : α → Bool) (probe : α → Option β)
    (rows : List α) :
    fusedHits (fun row => keep row && (probe row).isSome) probe rows =
      fusedHits keep probe rows := by
  induction rows with
  | nil => rfl
  | cons row rest ih =>
    cases h : keep row <;> cases p : probe row <;>
      simp [fusedHits, h, p, fusedHits] at ih ⊢ <;> exact ih

/-- LLVM's plain multiply and logical xor-shift implement unsigned wrapping
    hash arithmetic. Both multipliers and both shifts use the same rule. -/
theorem hash_stage (key multiplier : BitVec 64) (shift : Nat) :
    let product := key.toNat * multiplier.toNat % 2^64
    ((key * multiplier) ^^^ ((key * multiplier) >>> shift)).toNat =
      product ^^^ (product >>> shift) := by
  simp only [BitVec.toNat_xor, BitVec.toNat_ushiftRight, BitVec.toNat_mul]

theorem slot_in_range (hash mask : BitVec 64) :
    hash &&& mask ≤ mask ∧ ((hash + 1) &&& mask) ≤ mask := by
  bv_decide

/-- Removing the result of the first-set-lane operation leaves every other
    lane unchanged. The runtime calls cttz only when the mask is nonzero. -/
theorem pop_first_lane (mask : BitVec 8) (nonempty : mask ≠ 0) :
    (mask &&& (mask - 1)) ^^^ (1#8 <<< mask.ctz) = mask := by
  bv_decide

end TinylambOpt.FilterHashProbe
