import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SplitHigh.BoundedRaw` — A forced frame cap still limits worst-case expansion to raw
  cells and per-frame overhead.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  have hb : b < 128 ^ ubytes b :=
    (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff b (ubytes b) hbpos).mp (Nat.le_refl _)
  exact (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff a (ubytes b) hbpos).mpr
    (Nat.lt_of_le_of_lt h hb)

private theorem little4_len (n : Nat) : (little n 4).length = 4 := by simp [little]

private theorem raw_len (xs : List Nat) :
    (raw xs).length = 1 + ubytes xs.length + 4 * xs.length := by
  have hfixed : (fixed xs).length = 4 * xs.length := by
    unfold fixed
    simp only [List.length_flatMap, little4_len]
    induction xs with
    | nil => simp
    | cons x xs ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons, ih]
        omega
  simp [raw, uleb_len, hfixed, Nat.mul_comm, Nat.add_assoc]
  omega

private theorem sum_le_sum_pointwise {α : Type} (f g : α → Nat) (l : List α)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (by simp)) (ih (by intro y hy; exact h y (by simp [hy])))

/-! Greedy partitioning into runs of at most `cap` rows, built from the tail so
    that it traces the defining recursion of `parts`. -/

private def optParts (cap : Nat) (hcap : 0 < cap) : List Nat → List (List Nat)
  | [] => []
  | x :: xs =>
      match optParts cap hcap xs with
      | [] => [[x]]
      | a :: rest => if a.length < cap then (x :: a) :: rest else [x] :: a :: rest

private theorem optParts_mem_parts (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    optParts cap hcap xs ∈ parts xs := by
  induction xs with
  | nil => simp [parts, optParts]
  | cons x xs ih =>
      rw [parts]
      simp only [List.mem_flatMap]
      refine ⟨optParts cap hcap xs, ih, ?_⟩
      rw [optParts]
      generalize hps : optParts cap hcap xs = ps
      cases ps with
      | nil => simp
      | cons a rest =>
          by_cases hlt : a.length < cap <;> simp [hlt]

private theorem optParts_length_bounds (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    ∀ p ∈ optParts cap hcap xs, 0 < p.length ∧ p.length ≤ cap := by
  induction xs with
  | nil => simp [optParts]
  | cons x xs ih =>
      simp only [optParts]
      generalize hps : optParts cap hcap xs = ps
      cases ps with
      | nil =>
          simp
          omega
      | cons a rest =>
          have ha := ih a (by simp [hps])
          have hrest : ∀ p ∈ rest, 0 < p.length ∧ p.length ≤ cap := by
            intro p hp
            exact ih p (by simp [hps, hp])
          by_cases hlt : a.length < cap
          · simp [hlt]
            constructor
            · omega
            · exact hrest
          · simp [hlt]
            constructor
            · omega
            · constructor
              · exact ha
              · exact hrest

private theorem optParts_flatten (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    (optParts cap hcap xs).flatten = xs := by
  induction xs with
  | nil => simp [optParts]
  | cons x xs ih =>
      simp only [optParts]
      generalize hps : optParts cap hcap xs = ps
      have ih' : ps.flatten = xs := by simpa [hps] using ih
      cases ps with
      | nil => simp [ih']
      | cons a rest =>
          by_cases hlt : a.length < cap
          · simpa [hlt] using ih'
          · simpa [hlt] using ih'

private theorem optParts_len_sum (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    ((optParts cap hcap xs).map List.length).sum = xs.length := by
  have h := congrArg List.length (optParts_flatten cap hcap xs)
  simpa [List.length_flatten] using h

private def allFull (cap : Nat) : List (List Nat) → Prop
  | [] => True
  | _ :: rest => ∀ p ∈ rest, p.length = cap

private theorem optParts_allFull (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    allFull cap (optParts cap hcap xs) := by
  induction xs with
  | nil => simp [optParts, allFull]
  | cons x xs ih =>
      simp only [optParts]
      generalize hps : optParts cap hcap xs = ps
      cases ps with
      | nil => simp [allFull]
      | cons a rest =>
          have hrest : ∀ p ∈ rest, p.length = cap := by
            simpa [allFull, hps] using ih
          by_cases hlt : a.length < cap
          · simp [hlt]
            dsimp [allFull]
            exact hrest
          · have ha_le := (optParts_length_bounds cap hcap xs a (by simp [hps])).2
            have ha : a.length = cap := by omega
            simp [hlt]
            dsimp [allFull]
            intro p hp
            simp only [List.mem_cons] at hp
            rcases hp with rfl | hp
            · exact ha
            · exact hrest p hp

private theorem sum_lengths_full (cap : Nat) (rest : List (List Nat))
    (h : ∀ p ∈ rest, p.length = cap) :
    (rest.map List.length).sum = rest.length * cap := by
  induction rest with
  | nil => simp
  | cons p ps ih =>
      have hp : p.length = cap := h p (by simp)
      have hps : ∀ q ∈ ps, q.length = cap := by
        intro q hq
        exact h q (by simp [hq])
      simp [hp, ih hps, Nat.succ_mul, Nat.add_comm]

private theorem optParts_count_le (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    (optParts cap hcap xs).length ≤ pageLimit xs.length cap := by
  cases xs with
  | nil => simp [optParts, pageLimit]
  | cons x xs =>
      generalize hps : optParts cap hcap (x :: xs) = ps
      cases ps with
      | nil =>
          have hflat := optParts_flatten cap hcap (x :: xs)
          simp [hps] at hflat
      | cons a rest =>
          have hsum := optParts_len_sum cap hcap (x :: xs)
          have hn : a.length + rest.length * cap = (x :: xs).length := by
            have hfull : ∀ p ∈ rest, p.length = cap := by
              have hf := optParts_allFull cap hcap (x :: xs)
              simpa [hps, allFull] using hf
            have hsumrest := sum_lengths_full cap rest hfull
            simpa [hps, hsumrest] using hsum
          have ha_pos := (optParts_length_bounds cap hcap (x :: xs) a (by simp [hps])).1
          unfold pageLimit
          rw [← hn]
          simp only [List.length_cons]
          rw [Nat.le_div_iff_mul_le hcap]
          rw [Nat.succ_mul]
          omega

theorem sound_BoundedRaw : SplitHigh.BoundedRaw.Goal := by
  intro xs cap s hcap hb
  let ps := optParts cap hcap xs
  have hmem : ps ∈ parts xs := by simpa [ps] using optParts_mem_parts cap hcap xs
  have hle_cap : ∀ p ∈ ps, p.length ≤ cap := by
    intro p hp
    exact (optParts_length_bounds cap hcap xs p (by simpa [ps] using hp)).2
  have hfilter : ps ∈ (parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap)) := by
    simpa [hmem, hle_cap]
  have hcharge_mem : charge write 0 ps ∈
      (((parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap))).map (charge write 0)) := by
    exact List.mem_map.mpr ⟨ps, hfilter, rfl⟩
  have hmin_le : s ≤ charge write 0 ps := by
    unfold budget at hb
    exact (List.min?_eq_some_iff.mp hb).2 (charge write 0 ps) hcharge_mem
  have hcharge_le : charge write 0 ps ≤ (ps.map (fun p => (raw p).length)).sum := by
    unfold charge
    simp only [Nat.zero_add]
    exact sum_le_sum_pointwise (fun p => chosen write p) (fun p => (raw p).length) ps
      (by intro p hp; exact Nat.min_le_right _ _)
  have hraw_le : ∀ p ∈ ps, (raw p).length ≤ p.length * 4 + (1 + ubytes cap) := by
    intro p hp
    have hlen := hle_cap p hp
    have hub := ubytes_mono hlen
    rw [raw_len]
    omega
  have hsumlens : (ps.map List.length).sum = xs.length := by
    simpa [ps] using optParts_len_sum cap hcap xs
  have hsum4 : (ps.map (fun p => p.length * 4)).sum = xs.length * 4 := by
    calc
      (ps.map (fun p => p.length * 4)).sum = 4 * (ps.map List.length).sum := by
        induction ps with
        | nil => simp
        | cons p ps ih =>
            simp only [List.map_cons, List.sum_cons]
            rw [ih, Nat.mul_add, Nat.mul_comm p.length 4]
      _ = xs.length * 4 := by rw [hsumlens, Nat.mul_comm]
  have hsum_add : (ps.map (fun p => p.length * 4 + (1 + ubytes cap))).sum =
      (ps.map (fun p => p.length * 4)).sum + ps.length * (1 + ubytes cap) := by
    induction ps with
    | nil => simp
    | cons p ps ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        rw [ih]
        simp [Nat.succ_mul, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
  have hrawsum_le : (ps.map (fun p => (raw p).length)).sum ≤
      xs.length * 4 + ps.length * (1 + ubytes cap) := by
    calc
      (ps.map (fun p => (raw p).length)).sum ≤
          (ps.map (fun p => p.length * 4 + (1 + ubytes cap))).sum :=
        sum_le_sum_pointwise (fun p => (raw p).length)
          (fun p => p.length * 4 + (1 + ubytes cap)) ps hraw_le
      _ = xs.length * 4 + ps.length * (1 + ubytes cap) := by rw [hsum_add, hsum4]
  have hcount : ps.length ≤ pageLimit xs.length cap := by
    simpa [ps] using optParts_count_le cap hcap xs
  have hmul : ps.length * (1 + ubytes cap) ≤ pageLimit xs.length cap * (1 + ubytes cap) :=
    Nat.mul_le_mul_right (1 + ubytes cap) hcount
  calc
    s ≤ charge write 0 ps := hmin_le
    _ ≤ (ps.map (fun p => (raw p).length)).sum := hcharge_le
    _ ≤ xs.length * 4 + ps.length * (1 + ubytes cap) := hrawsum_le
    _ ≤ xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) :=
      Nat.add_le_add_left hmul _

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_BoundedRaw
