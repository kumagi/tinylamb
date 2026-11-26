import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_MandatoryCuts

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem parts_flatten (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : ps.flatten = xs := by
  induction xs generalizing ps with
  | nil =>
      simp [parts] at h
      simp [h]
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      have hflat := ih ps' hps'
      match ps', hmem with
      | [], hmem =>
          simp at hmem
          subst hmem
          simp [hflat]
      | a :: rest, hmem =>
          rw [List.flatten_cons] at hflat
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> simp [hflat]

private theorem parts_length_sum (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : (ps.map List.length).sum = xs.length := by
  have hf := parts_flatten xs ps h
  have := congrArg List.length hf
  simpa [List.length_flatten] using this

private theorem raw_len_ge_two (xs : List Nat) : 2 ≤ (raw xs).length := by
  simp [raw, uleb_len_eq_ubytes]
  have : 1 ≤ ubytes xs.length := Nat.le_max_left _ _
  omega

private theorem frame_len_ge_two (f : Writer) (xs : List Nat) :
    2 ≤ (frame f xs).length := by
  simp [frame, uleb_len_eq_ubytes]
  have : 1 ≤ ubytes xs.length := Nat.le_max_left _ _
  omega

private theorem chosen_ge_two (f : Writer) (xs : List Nat) : 2 ≤ chosen f xs := by
  exact Nat.le_min.mpr ⟨frame_len_ge_two f xs, raw_len_ge_two xs⟩

private theorem pageLimit_le_of_cover (n cap k : Nat) (hcap : 0 < cap)
    (hk : n ≤ k * cap) : pageLimit n cap ≤ k := by
  unfold pageLimit
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  have hm : cap * k = k * cap := Nat.mul_comm _ _
  omega

private theorem charge_ge_mul (f : Writer) (fee : Nat) (ps : List (List Nat))
    (h : ∀ p ∈ ps, 2 ≤ chosen f p) :
    (fee + 2) * ps.length ≤ charge f fee ps := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp : 2 ≤ chosen f p := h p (by simp)
      have ih' : (fee + 2) * ps.length ≤
          (ps.map (fun p => fee + chosen f p)).sum := by
        apply ih
        intro q hq
        exact h q (by simp [hq])
      have hmul : (fee + 2) * (ps.length + 1) =
          (fee + 2) * ps.length + (fee + 2) := by
        rw [Nat.mul_add, Nat.mul_one]
      omega

/-- The mandatory-cut bound is a property of any frame writer, since each
    frame pays for its tag/length header and each raw cell is four bytes. -/
theorem sound_MandatoryCuts : OrdinalResidual.MandatoryCuts.Goal := by
  intro xs cap fee ps hcap hps hall
  have hsum : (ps.map List.length).sum = xs.length := parts_length_sum xs ps hps
  have hle : xs.length ≤ ps.length * cap := by
    rw [← hsum]
    clear hsum hps
    induction ps with
    | nil => simp
    | cons p ps ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        have hp := hall p (by simp)
        have ih' := ih (fun q hq => hall q (by simp [hq]))
        have hmul : (ps.length + 1) * cap = ps.length * cap + cap := by
          simp [Nat.add_mul, Nat.one_mul]
        omega
  have hpl : pageLimit xs.length cap ≤ ps.length :=
    pageLimit_le_of_cover xs.length cap ps.length hcap hle
  have hch : (fee + 2) * ps.length ≤ charge write fee ps :=
    charge_ge_mul write fee ps (fun p _ => chosen_ge_two write p)
  exact Nat.le_trans (Nat.mul_le_mul_left _ hpl) hch

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_MandatoryCuts
