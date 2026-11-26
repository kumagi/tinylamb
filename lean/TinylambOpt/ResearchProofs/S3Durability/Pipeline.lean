import TinylambOpt.ResearchGoals.S3Durability.Pipeline

/-!
Proofs for the uploader throughput goals. Each proof rewrites products into a
common order and lets `omega` treat the remaining products as atoms. The
results size the chunk limit and the PUT window so that SQL commit throughput
is limited by bandwidth, not by S3 round trips.
-/

namespace TinylambOpt.ResearchProofs.S3Durability.Pipeline

open TinylambOpt.ResearchGoals.S3Durability.Pipeline

theorem capacity_below_bandwidth : CapacityBelowBandwidth.Goal := by
  intro W a b C hW ha hb
  have hpos : 0 < W * b * (a * b) :=
    Nat.mul_pos (Nat.mul_pos hW hb) (Nat.mul_pos ha hb)
  have e1 : W * C * b = W * b * C := Nat.mul_right_comm W C b
  have e2 : W * b * (a * b + C) = W * b * (a * b) + W * b * C := Nat.mul_add _ _ _
  omega

theorem chunk_sizing : ChunkSizing.Goal := by
  intro W a b C p q hW hb hpq
  have hK : 0 < W * b := Nat.mul_pos hW hb
  -- Both sides carry the factor K = W * b.
  have eL : p * (W * b) * (a * b + C) = W * b * (p * (a * b + C)) := by
    rw [Nat.mul_comm p (W * b), Nat.mul_assoc]
  have eR : W * C * b * q = W * b * (C * q) := by
    rw [Nat.mul_right_comm W C b, Nat.mul_assoc]
  rw [eL, eR]
  have cancel : W * b * (p * (a * b + C)) ≤ W * b * (C * q) ↔ p * (a * b + C) ≤ C * q :=
    ⟨fun h => Nat.le_of_mul_le_mul_left h hK, fun h => Nat.mul_le_mul_left _ h⟩
  rw [cancel]
  have e3 : p * (a * b + C) = p * (a * b) + p * C := Nat.mul_add _ _ _
  have e4 : C * (q - p) = C * q - C * p := Nat.mul_sub _ _ _
  have e5 : p * C = C * p := Nat.mul_comm p C
  have e6 : C * p ≤ C * q := Nat.mul_le_mul_left C hpq
  omega

theorem serial_bounded_is_latency_bound : SerialBoundedIsLatencyBound.Goal := by
  intro a b C Cmax hC
  have e1 : C * b * a = C * (a * b) := by
    rw [Nat.mul_assoc, Nat.mul_comm b a]
  have h1 : C * (a * b) ≤ Cmax * (a * b) := Nat.mul_le_mul_right _ hC
  have e2 : Cmax * (a * b + C) = Cmax * (a * b) + Cmax * C := Nat.mul_add _ _ _
  omega

theorem eager_capacity : EagerCapacity.Goal := by
  intro lam W a b hW ha hb
  constructor
  · intro hlt
    -- Take the backlog of one fixed latency, s = lam * (a * b).
    refine ⟨lam * (a * b), ?_⟩
    have h1 : lam * (a * b) * (lam + 1) ≤ lam * (a * b) * (W * b) :=
      Nat.mul_le_mul_left _ hlt
    have e1 : lam * (a * b + lam * (a * b)) = lam * (a * b) + lam * (lam * (a * b)) :=
      Nat.mul_add _ _ _
    have e2 : lam * (a * b) * (lam + 1) = lam * (a * b) * lam + lam * (a * b) :=
      Nat.mul_succ _ _
    have e3 : lam * (lam * (a * b)) = lam * (a * b) * lam := Nat.mul_comm _ _
    have e4 : W * (lam * (a * b)) * b = lam * (a * b) * (W * b) := by
      rw [Nat.mul_right_comm, Nat.mul_comm]
    omega
  · rintro ⟨s, hs⟩
    apply Nat.lt_of_not_le
    intro hge
    have hab : 0 < a * b := Nat.mul_pos ha hb
    have hlam : 0 < lam := Nat.lt_of_lt_of_le (Nat.mul_pos hW hb) hge
    have hpos : 0 < lam * (a * b) := Nat.mul_pos hlam hab
    have h1 : W * b * s ≤ lam * s := Nat.mul_le_mul_right s hge
    have e1 : lam * (a * b + s) = lam * (a * b) + lam * s := Nat.mul_add _ _ _
    have e2 : W * s * b = W * b * s := Nat.mul_right_comm _ _ _
    omega

theorem bounded_chunk_capacity : BoundedChunkCapacity.Goal := by
  intro W a b C Cmax hC
  have e1 : W * C * b * (a * b + Cmax) = W * b * (C * (a * b) + C * Cmax) := by
    rw [Nat.mul_right_comm W C b, Nat.mul_assoc (W * b) C, Nat.mul_add]
  have e2 : W * Cmax * b * (a * b + C) = W * b * (Cmax * (a * b) + Cmax * C) := by
    rw [Nat.mul_right_comm W Cmax b, Nat.mul_assoc (W * b) Cmax, Nat.mul_add]
  rw [e1, e2]
  apply Nat.mul_le_mul_left
  have h1 : C * (a * b) ≤ Cmax * (a * b) := Nat.mul_le_mul_right _ hC
  have h2 : C * Cmax = Cmax * C := Nat.mul_comm _ _
  omega

end TinylambOpt.ResearchProofs.S3Durability.Pipeline
