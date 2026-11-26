import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `DictionaryRuns.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.DictionaryRuns

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  have hk : 0 < ubytes m := by
    unfold ubytes
    omega
  exact (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff n (ubytes m) hk).mpr
    (Nat.lt_of_le_of_lt h
      ((TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff m (ubytes m) hk).mp (Nat.le_refl _)))

private theorem little_len (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem fixed_len (xs : List Nat) : (fixed xs).length = 4 * xs.length := by
  induction xs with
  | nil => simp [fixed]
  | cons x xs ih =>
      rw [show fixed (x :: xs) = little x 4 ++ fixed xs by rfl]
      simp [little_len, ih]
      omega

private theorem raw_len_eq (xs : List Nat) :
    (raw xs).length = 1 + ubytes xs.length + 4 * xs.length := by
  simp [raw, fixed_len, uleb_len]
  omega

private theorem chosen_bound (f : Writer) (xs : List Nat) (cap : Nat) (h : xs.length ≤ cap) :
    chosen f xs ≤ 1 + ubytes cap + 4 * xs.length := by
  unfold chosen
  calc
    min (frame f xs).length (raw xs).length ≤ (raw xs).length := Nat.min_le_right _ _
    _ = 1 + ubytes xs.length + 4 * xs.length := raw_len_eq xs
    _ ≤ 1 + ubytes cap + 4 * xs.length := by
      have hm := ubytes_mono h
      omega

/-- Greedy right-to-left chunking of a list into pages of length at most `cap`. -/
private def chunkPart : List Nat → Nat → List (List Nat)
  | [], _ => []
  | x :: xs, cap =>
      match chunkPart xs cap with
      | [] => [[x]]
      | p :: ps => if p.length < cap then (x :: p) :: ps else [x] :: p :: ps

private theorem sum_lengths_full (ps : List (List Nat)) (cap : Nat)
    (hfull : ∀ p ∈ ps, p.length = cap) :
    (ps.map List.length).sum = ps.length * cap := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp [hfull p (by simp), ih (fun q hq => hfull q (by simp [hq]))]
      rw [Nat.add_mul]
      simp
      omega

private theorem pageLimit_mono (n cap : Nat) :
    pageLimit n cap ≤ pageLimit (n + 1) cap := by
  unfold pageLimit
  exact Nat.div_le_div_right (by omega : n + cap - 1 ≤ n + 1 + cap - 1)

private theorem chunkPart_spec (xs : List Nat) (cap : Nat) (hcap : 0 < cap) :
    (chunkPart xs cap).flatten = xs ∧
    (∀ p ∈ chunkPart xs cap, p.length ≤ cap) ∧
    (∀ p ∈ (chunkPart xs cap).tail, p.length = cap) ∧
    (chunkPart xs cap).length ≤ pageLimit xs.length cap := by
  induction xs with
  | nil =>
      simp [chunkPart, pageLimit]
  | cons x xs ih =>
      rcases ih with ⟨hflat, hpage, hfull, hnum⟩
      rw [chunkPart.eq_2]
      by_cases hempty : chunkPart xs cap = []
      · rw [hempty]
        simp only
        have hxs : xs = [] := by
          rw [hempty] at hflat
          simpa using hflat.symm
        subst xs
        refine ⟨?_, ?_, ?_, ?_⟩
        · rfl
        · intro q hqmem
          simp only [List.mem_singleton] at hqmem
          subst q
          change 1 ≤ cap
          exact hcap
        · intro q hqmem
          simp at hqmem
        · change 1 ≤ (1 + cap - 1) / cap
          rw [show 1 + cap - 1 = cap by omega]
          rw [Nat.div_self hcap]
          omega
      · have hcons : ∃ hd tl, chunkPart xs cap = hd :: tl :=
          List.exists_cons_of_ne_nil hempty
        rcases hcons with ⟨hd, tl, hq⟩
        rw [hq]
        simp only
        by_cases hlt : hd.length < cap
        · rw [ite_eq_left hlt]
          refine ⟨?_, ?_, ?_, ?_⟩
          · simpa [hq] using hflat
          · intro q hmem
            simp only [List.mem_cons] at hmem
            rcases hmem with rfl | hmem
            · simpa [List.length_cons] using (show hd.length + 1 ≤ cap by omega)
            · exact hpage q (by rw [hq]; simp [hmem])
          · intro q hmem
            exact hfull q (by rw [hq]; simpa using hmem)
          · have hnum' : (hd :: tl).length ≤ pageLimit xs.length cap := by
              rw [← hq]
              exact hnum
            have hmono := pageLimit_mono xs.length cap
            have hle : (hd :: tl).length ≤ pageLimit (xs.length + 1) cap := Nat.le_trans hnum' hmono
            simpa [List.length_cons] using hle
        · have hp_le : hd.length ≤ cap := hpage hd (by rw [hq]; simp)
          have hp_eq : hd.length = cap := by omega
          have hsum : (tl.map List.length).sum = tl.length * cap :=
            sum_lengths_full tl cap (fun q hmem => hfull q (by rw [hq]; simp [hmem]))
          have hc : hd.length + (tl.map List.length).sum = xs.length := by
            have hc' := congrArg List.length hflat
            rw [hq] at hc'
            simpa [List.length_flatten] using hc'
          rw [ite_eq_right hlt]
          refine ⟨?_, ?_, ?_, ?_⟩
          · simpa [hq] using hflat
          · intro q hmem
            simp only [List.mem_cons] at hmem
            rcases hmem with rfl | rfl | hmem
            · change 1 ≤ cap
              exact hcap
            · exact hp_le
            · exact hpage q (by rw [hq]; simp [hmem])
          · intro q hmem
            by_cases hqhd : q = hd
            · subst q
              exact hp_eq
            · have hmem_tl : q ∈ tl := by simpa [hqhd] using hmem
              exact hfull q (by rw [hq]; simpa using hmem_tl)
          · unfold pageLimit
            rw [Nat.le_div_iff_mul_le hcap]
            rw [show (x :: xs).length + cap - 1 = xs.length + cap by
              simp only [List.length_cons]
              omega]
            simp [Nat.add_mul]
            omega

private theorem chunkPart_mem_parts (xs : List Nat) (cap : Nat) :
    chunkPart xs cap ∈ parts xs := by
  induction xs with
  | nil => simp [chunkPart, parts]
  | cons x xs ih =>
      rw [parts, List.mem_flatMap]
      refine ⟨chunkPart xs cap, ih, ?_⟩
      rw [chunkPart.eq_2]
      by_cases hempty : chunkPart xs cap = []
      · rw [hempty]
        simp
      · have hcons : ∃ hd tl, chunkPart xs cap = hd :: tl :=
          List.exists_cons_of_ne_nil hempty
        rcases hcons with ⟨hd, tl, hq⟩
        rw [hq]
        simp only
        by_cases hlt : hd.length < cap
        · rw [ite_eq_left hlt]; simp
        · rw [ite_eq_right hlt]; simp

private theorem sum_chosen_bound (f : Writer) (ps : List (List Nat)) (cap : Nat)
    (hpage : ∀ p ∈ ps, p.length ≤ cap) :
    (ps.map (fun p => chosen f p)).sum ≤
      4 * (ps.map List.length).sum + ps.length * (1 + ubytes cap) := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp : chosen f p ≤ (1 + ubytes cap) + 4 * p.length :=
        chosen_bound f p cap (hpage p (by simp))
      have hps : ∀ q ∈ ps, q.length ≤ cap := fun q hmem => hpage q (by simp [hmem])
      have ih' := ih hps
      have hmul : (ps.length + 1) * (1 + ubytes cap) =
          ps.length * (1 + ubytes cap) + (1 + ubytes cap) := by
        rw [Nat.add_mul]
        simp
      rw [hmul]
      omega

theorem sound_BoundedRaw : DictionaryRuns.BoundedRaw.Goal := by
  unfold DictionaryRuns.BoundedRaw.Goal boundedRaw
  intro xs cap s hcap hb
  let part := chunkPart xs cap
  have hspec := chunkPart_spec xs cap hcap
  rcases hspec with ⟨hflat, hpage, _hfull, hnum⟩
  have hmems : part ∈ parts xs := by
    simpa [part] using chunkPart_mem_parts xs cap
  have hfiltered : part ∈ (parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap)) := by
    rw [List.mem_filter]
    refine ⟨hmems, ?_⟩
    rw [List.all_eq_true]
    intro p hp
    exact decide_eq_true (hpage p (by simpa [part] using hp))
  have hmemcharge : charge write 0 part ∈
      ((parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap))).map (charge write 0) :=
    List.mem_map.mpr ⟨part, hfiltered, rfl⟩
  have hb' : (((parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap))).map
      (charge write 0)).min? = some s := by
    simpa [budget] using hb
  have hs_le : s ≤ charge write 0 part :=
    (List.min?_eq_some_iff.mp hb').2 (charge write 0 part) hmemcharge
  have hcharge : charge write 0 part ≤
      4 * (part.map List.length).sum + part.length * (1 + ubytes cap) := by
    simpa [charge] using sum_chosen_bound write part cap (fun p hp => hpage p (by simpa [part] using hp))
  have hsum : (part.map List.length).sum = xs.length := by
    have hc := congrArg List.length hflat
    simpa [part, List.length_flatten] using hc
  have hnum' : part.length ≤ pageLimit xs.length cap := by simpa [part] using hnum
  have hmul : part.length * (1 + ubytes cap) ≤ pageLimit xs.length cap * (1 + ubytes cap) :=
    Nat.mul_le_mul_right (1 + ubytes cap) hnum'
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_DictionaryRuns_BoundedRaw
