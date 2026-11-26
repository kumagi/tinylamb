import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `PatchedOctet.MandatoryCuts` — A page budget must cover at least the required number
  of tags and restart entries.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_MandatoryCuts

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

private theorem uleb_length_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem parts_sum_length (xs : List Nat) : ∀ ps, ps ∈ parts xs → xs.length = (ps.map List.length).sum := by
  induction xs with
  | nil =>
      intro ps h
      simp [parts] at h
      subst ps
      simp
  | cons x xs ih =>
      intro ps h
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨qs, hqs, hmem⟩
      have hsum := ih qs hqs
      cases qs with
      | nil =>
          simp at hmem
          subst ps
          simp [hsum]
      | cons a rest =>
          simp at hmem
          rcases hmem with hps | hps
          · subst ps
            simp [hsum] <;> omega
          · subst ps
            simp [hsum] <;> omega

private theorem sum_le_card_mul (ps : List (List Nat)) (cap : Nat) :
    (∀ p ∈ ps, p.length ≤ cap) → (ps.map List.length).sum ≤ ps.length * cap := by
  induction ps with
  | nil =>
      intro h
      simp
  | cons p ps ih =>
      intro h
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.add_one, Nat.succ_mul]
      have hp : p.length ≤ cap := h p (by simp)
      have htail : ∀ q ∈ ps, q.length ≤ cap := by
        intro q hq
        exact h q (by simp [hq])
      have iht := ih htail
      omega

private theorem chosen_ge_two (f : List Nat → Bytes) (p : List Nat) : 2 ≤ chosen f p := by
  have huleb : 0 < (uleb p.length).length := uleb_length_pos p.length
  unfold chosen
  apply Nat.le_min.mpr
  constructor
  · simp [frame]
    omega
  · simp [raw]
    omega

private theorem charge_ge (f : List Nat → Bytes) (fee : Nat) (ps : List (List Nat)) :
    ps.length * (fee + 2) ≤ charge f fee ps := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.add_one, Nat.succ_mul]
      have hp : 2 ≤ chosen f p := chosen_ge_two f p
      omega

theorem sound_MandatoryCuts : PatchedOctet.MandatoryCuts.Goal := by
  intro xs cap fee ps hcap hparts hlen
  have hsum := parts_sum_length xs ps hparts
  have hxn : xs.length ≤ ps.length * cap := by
    rw [hsum]
    exact sum_le_card_mul ps cap hlen
  have hpage : pageLimit xs.length cap ≤ ps.length := by
    unfold pageLimit
    rw [Nat.div_le_iff_le_mul_add_pred hcap]
    rw [Nat.mul_comm cap ps.length]
    omega
  have hc : ps.length * (fee + 2) ≤ charge write fee ps := charge_ge write fee ps
  have hmul : (fee + 2) * pageLimit xs.length cap ≤ ps.length * (fee + 2) := by
    calc
      (fee + 2) * pageLimit xs.length cap ≤ (fee + 2) * ps.length :=
        Nat.mul_le_mul_left (fee + 2) hpage
      _ = ps.length * (fee + 2) := Nat.mul_comm (fee + 2) ps.length
  exact Nat.le_trans hmul hc

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_MandatoryCuts
