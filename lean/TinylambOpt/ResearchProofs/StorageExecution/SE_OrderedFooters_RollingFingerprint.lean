import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved (10 claims, including the following)

- `RollingFingerprint.AmendLeaf` — Update one cached block using the left and right
  partial states.
- `RollingFingerprint.CollisionBoundary` — `agg.reference [0] = agg.reference [65521]`
- `RollingFingerprint.EqualWindowResidues` — `∀ a b pre z, a.length=b.length →
  (agg.reference a).2=(agg.reference b).2 → agg.reference (pre++a++z)=agg.reference
  (pre++b++z)`
-/
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.RollingFingerprint
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

theorem ref_eq (xs : List Nat) :
    RollingFingerprint.agg.reference xs =
      (257 ^ xs.length % 65521, xs.foldl (fun h x => (257 * h + x) % 65521) 0) := rfl

theorem join_eq :
    RollingFingerprint.agg.join =
      fun (p, h) (q, j) => (p * q % 65521, (h * q + j) % 65521) := rfl

/-- The fold stays below the modulus. -/
private theorem F_lt : ∀ (xs : List Nat) (acc : Nat),
    acc < 65521 → xs.foldl (fun h x => (257 * h + x) % 65521) acc < 65521 := by
  intro xs
  induction xs with
  | nil => intro acc hacc; exact hacc
  | cons y ys ih =>
      intro acc hacc
      exact ih ((257 * acc + y) % 65521) (Nat.mod_lt _ (by decide))

theorem H_lt (xs : List Nat) :
    xs.foldl (fun h x => (257 * h + x) % 65521) 0 < 65521 :=
  F_lt xs 0 (by decide)

/-- Modding one factor is immaterial. -/
private theorem M1 : ∀ (a b : Nat), (a % 65521 * b) % 65521 = (a * b) % 65521 := by
  intro a b
  rw [Nat.mul_mod, Nat.mod_mod, ← Nat.mul_mod]

private theorem M1' (a b : Nat) : (a * (b % 65521)) % 65521 = a * b % 65521 := by
  rw [Nat.mul_comm a (b % 65521), M1, Nat.mul_comm]

/-- Congruent summands may be swapped inside a modulus. -/
private theorem MODCONG : ∀ (x y z : Nat),
    x % 65521 = y % 65521 → (x + z) % 65521 = (y + z) % 65521 := by
  intro x y z h
  rw [← Nat.mod_add_mod, h, Nat.mod_add_mod]

private theorem MODCONG2 : ∀ (x y z : Nat),
    x % 65521 = y % 65521 → (z + x) % 65521 = (z + y) % 65521 := by
  intro x y z h
  rw [Nat.add_comm z x, Nat.add_comm z y, MODCONG x y z h]

/-- Closed form of the rolling fold from an arbitrary starting residue. -/
private theorem foldl_F : ∀ (xs : List Nat) (acc : Nat),
    acc < 65521 →
      xs.foldl (fun h x => (257 * h + x) % 65521) acc
        = (acc * 257 ^ xs.length + xs.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521 := by
  intro xs
  induction xs with
  | nil =>
      intro acc hacc
      rw [List.foldl_nil, List.length_nil, Nat.pow_zero, List.foldl_nil, Nat.mul_one,
        Nat.add_zero, Nat.mod_eq_of_lt hacc]
  | cons y ys ih =>
      intro acc hacc
      rw [List.foldl_cons, List.foldl_cons, List.length_cons, Nat.mul_zero, Nat.zero_add,
        Nat.pow_succ]
      have e1 := ih ((257 * acc + y) % 65521) (Nat.mod_lt _ (by decide))
      have e2 := ih (y % 65521) (Nat.mod_lt _ (by decide))
      rw [e1, e2]
      have c1 :
          ((257 * acc + y) % 65521 * 257 ^ ys.length
              + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521
            = ((257 * acc + y) * 257 ^ ys.length
                + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521 :=
        MODCONG _ _ _ (M1 (257 * acc + y) (257 ^ ys.length))
      have c2 :
          (y % 65521 * 257 ^ ys.length + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521
            = (y * 257 ^ ys.length + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521 :=
        MODCONG _ _ _ (M1 y (257 ^ ys.length))
      rw [c1, c2]
      have c3 :
          (acc * (257 ^ ys.length * 257)
              + (y * 257 ^ ys.length + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521)
              % 65521
            = (acc * (257 ^ ys.length * 257)
                + (y * 257 ^ ys.length + ys.foldl (fun h x => (257 * h + x) % 65521) 0))
              % 65521 :=
        MODCONG2 _ _ _ (Nat.mod_mod _ _)
      rw [c3]
      exact congrArg (fun z => z % 65521)
        (by simp only [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm,
            Nat.add_assoc, Nat.add_comm, Nat.add_left_comm])

/-- Reference polynomial multiplier over an append. -/
private theorem P_ap : ∀ (xs ys : List Nat),
    257 ^ (xs ++ ys).length % 65521
      = (257 ^ xs.length % 65521 * (257 ^ ys.length % 65521)) % 65521 := by
  intro xs ys
  rw [List.length_append, Nat.pow_add, Nat.mul_mod]

/-- Reference residue over an append. -/
private theorem H_ap : ∀ (xs ys : List Nat),
    (xs ++ ys).foldl (fun h x => (257 * h + x) % 65521) 0
      = ((xs.foldl (fun h x => (257 * h + x) % 65521) 0 % 65521 * 257 ^ ys.length % 65521
            + ys.foldl (fun h x => (257 * h + x) % 65521) 0) % 65521) := by
  intro xs ys
  rw [List.foldl_append, foldl_F ys _ (H_lt xs), Nat.mod_eq_of_lt (H_lt xs), Nat.mod_add_mod]

theorem stitch_agg : stitch RollingFingerprint.agg := by
  intro a b
  rw [ref_eq, ref_eq, ref_eq, join_eq]
  rw [Prod.mk.injEq]
  refine ⟨?_, ?_⟩
  · rw [P_ap]
  · rw [H_ap a b, Nat.mod_eq_of_lt (H_lt a)]
    refine MODCONG _ _ _ ?_
    rw [M1', Nat.mod_mod]

theorem sound_Stitch : RollingFingerprint.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : RollingFingerprint.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : RollingFingerprint.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : RollingFingerprint.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : RollingFingerprint.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : RollingFingerprint.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : RollingFingerprint.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : RollingFingerprint.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sound_EqualWindowResidues : RollingFingerprint.EqualWindowResidues.Goal := by
  intro a b pre z hlen hres
  rw [ref_eq, ref_eq]
  rw [Prod.mk.injEq]
  refine ⟨?_, ?_⟩
  · rw [show (pre ++ a ++ z).length = pre.length + a.length + z.length from by
      rw [List.length_append, List.length_append],
      show (pre ++ b ++ z).length = pre.length + b.length + z.length from by
      rw [List.length_append, List.length_append]]
    rw [hlen]
  · rw [ref_eq, ref_eq] at hres
    simp at hres
    rw [H_ap (pre ++ a) z, H_ap (pre ++ b) z, H_ap pre a, H_ap pre b]
    rw [hres, hlen]

theorem sound_CollisionBoundary : RollingFingerprint.CollisionBoundary.Goal := by
  show (257 ^ [0].length % 65521, List.foldl (fun h x => (257 * h + x) % 65521) 0 [0])
      = (257 ^ [65521].length % 65521, List.foldl (fun h x => (257 * h + x) % 65521) 0 [65521])
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.RollingFingerprint
