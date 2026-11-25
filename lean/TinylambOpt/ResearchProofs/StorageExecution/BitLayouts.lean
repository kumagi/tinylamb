import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.BitLayouts

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-! Proofs/disproofs for all 100 BitLayouts goals. -/


/-! ## Generic lemmas -/

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private theorem footprint_mem (l : Layout) (n w i j : Nat) (hj : j < w) :
    l.bit n w i j / 8 ∈ footprint l n w i := by
  unfold footprint
  exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨j, by simp [lanes, List.mem_range, hj], rfl⟩)

private theorem addressSet_mem (l : Layout) (n w : Nat) (rows : List Nat) {i j : Nat}
    (hi : i ∈ rows) (hj : j < w) : l.bit n w i j / 8 ∈ addressSet l n w rows := by
  unfold addressSet
  refine List.mem_eraseDups.mpr ?_
  refine List.mem_flatMap.mpr ⟨i, hi, footprint_mem l n w i j hj⟩

private theorem fetch_eq_of_agree (l : Layout) (n w i : Nat) (a b : Nat → Nat)
    (h : ∀ j, j < w → a (l.bit n w i j / 8) = b (l.bit n w i j / 8)) :
    fetch l n w i a = fetch l n w i b := by
  unfold fetch lanes
  congr 1
  apply List.map_congr_left
  intro j hj
  have hjw : j < w := by simpa [List.mem_range] using hj
  simp [h j hjw]

theorem sparseGather_of_def (l : Layout) : sparseGather l := by
  intro n w rows a b h
  apply List.map_congr_left
  intro i hi
  exact fetch_eq_of_agree l n w i a b (fun j hj => h _ (addressSet_mem l n w rows hi hj))


private theorem word_congr_mod (w : Nat) (xs ys : List Nat)
    (h : List.map (· % 2 ^ w) xs = List.map (· % 2 ^ w) ys) :
    word w xs = word w ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          simp only [List.map_cons, List.cons.injEq] at h
          simp [word, h.1, ih ys h.2]

private theorem pack_congr_mod (w : Nat) (xs ys : List Nat)
    (hlen : xs.length = ys.length)
    (h : List.map (· % 2 ^ w) xs = List.map (· % 2 ^ w) ys) :
    pack w xs = pack w ys := by
  unfold pack
  rw [word_congr_mod w xs ys h, hlen]

private theorem getD_range_map (xs : List Nat) (f : Nat → Nat) :
    (List.range xs.length).map (fun i => f (xs[i]?.getD 0)) = xs.map f := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [List.range_succ, Function.comp, ih]

theorem transcode_of_lossless (l : Layout) (hl : lossless l) : transcode l := by
  intro w xs
  unfold transcode
  have hfetch : (List.range xs.length).map (fun i => fetch l xs.length w i (atByte (file l w xs))) =
      xs.map (· % 2 ^ w) := by
    rw [← getD_range_map xs (· % 2 ^ w)]
    apply List.map_congr_left
    intro i hi
    have hiw : i < xs.length := by simpa [List.mem_range] using hi
    exact hl w xs i hiw
  rw [hfetch]
  unfold pack
  simp [word_congr_mod w (xs.map (· % 2 ^ w)) xs (by simp [Nat.mod_mod])]

theorem dictionaryProbe_of_lossless (l : Layout) (hl : lossless l) : dictionaryProbe l := by
  intro w xs dict keys _hkeys
  apply List.map_congr_left
  intro i hi
  have hki := _hkeys i hi
  simp [hl w xs i hki]

