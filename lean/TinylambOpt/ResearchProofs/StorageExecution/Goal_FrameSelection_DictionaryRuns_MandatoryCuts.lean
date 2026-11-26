import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `DictionaryRuns.MandatoryCuts` — A page budget must cover at least the required
  number of tags and restart entries.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_MandatoryCuts

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

-- Every ULEB encoding is nonempty.
private theorem uleb_length_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

-- A frame and the raw fallback both start with a tag byte and a row count,
-- hence every chosen payload costs at least two bytes (for any writer).
private theorem chosen_ge_two (f : Writer) (p : List Nat) : 2 ≤ chosen f p := by
  unfold chosen frame raw
  have h := uleb_length_pos p.length
  simp only [List.length_append, List.length_cons, List.length_nil]
  rw [Nat.le_min]
  constructor <;> omega

-- The partitions produced by `parts` cover exactly the input rows.
private theorem parts_sum_length (xs : List Nat) :
    ∀ ps ∈ parts xs, (ps.map List.length).sum = xs.length := by
  induction xs with
  | nil =>
      intro ps hps
      simp [parts] at hps
      subst ps
      simp
  | cons x xs ih =>
      intro ps hps
      rw [parts] at hps
      simp only [List.mem_flatMap] at hps
      rcases hps with ⟨p0, hp0, hmem⟩
      have hsum : (p0.map List.length).sum = xs.length := ih p0 hp0
      change ps ∈ (match p0 with
        | [] => [[[x]]]
        | a::rest => [[x]::a::rest, (x::a)::rest]) at hmem
      cases p0 with
      | nil =>
          simp at hmem
          subst ps
          simp_all
      | cons a rest =>
          simp at hmem
          rcases hmem with hmem | hmem
          · subst ps
            simp_all
            omega
          · subst ps
            simp_all
            omega

-- Pages of at most `cap` rows cannot cover more than `ps.length * cap` rows.
private theorem sum_lengths_le (ps : List (List Nat)) (cap : Nat)
    (h : ∀ p ∈ ps, p.length ≤ cap) :
    (ps.map List.length).sum ≤ ps.length * cap := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, List.mem_cons] at h ⊢
      have hp : p.length ≤ cap := h p (by simp)
      have hrest : (ps.map List.length).sum ≤ ps.length * cap := ih (by
        intro q hq
        exact h q (by simp [hq]))
      rw [Nat.succ_mul]
      omega

-- Ceiling division: if n ≤ k*cap then ceil(n/cap) ≤ k.
private theorem pageLimit_le_pages (n cap k : Nat) (hcap : 0 < cap)
    (hbound : n ≤ k * cap) : pageLimit n cap ≤ k := by
  unfold pageLimit
  have h1 : n + cap - 1 ≤ k * cap + cap - 1 := by omega
  have hk : k * cap + cap = (k + 1) * cap := by simp [Nat.add_mul]
  have h2 : k * cap + cap - 1 < (k + 1) * cap := by
    rw [← hk]
    omega
  have h3 : n + cap - 1 < (k + 1) * cap := by omega
  have h4 : (n + cap - 1) / cap < k + 1 := (Nat.div_lt_iff_lt_mul hcap).2 h3
  omega

-- Fixed per-page fee plus at least two bytes of framing per page.
private theorem charge_ge (f : Writer) (fee : Nat) (ps : List (List Nat)) :
    (fee + 2) * ps.length ≤ charge f fee ps := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.mul_succ]
      have hp := chosen_ge_two f p
      omega

-- The claim is writer-independent: mandatory cuts come from the framing
-- overhead alone, so it holds in particular for the dictionary-runs writer.
theorem sound_MandatoryCuts : DictionaryRuns.MandatoryCuts.Goal := by
  unfold MandatoryCuts.Goal mandatoryCuts
  intro xs cap fee ps hcap hpsparts hpages
  have hsum := parts_sum_length xs ps hpsparts
  have hcapsum := sum_lengths_le ps cap hpages
  have hn : xs.length ≤ ps.length * cap := by omega
  have hpl : pageLimit xs.length cap ≤ ps.length :=
    pageLimit_le_pages xs.length cap ps.length hcap hn
  exact Nat.le_trans (Nat.mul_le_mul_left (fee + 2) hpl) (charge_ge write fee ps)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_MandatoryCuts
