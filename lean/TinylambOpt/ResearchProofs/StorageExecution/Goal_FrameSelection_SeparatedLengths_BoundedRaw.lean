import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  exact (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff a (ubytes b) hbpos).mpr
    (Nat.lt_of_le_of_lt h
      ((TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff b (ubytes b) hbpos).mp
        (Nat.le_refl _)))

private theorem raw_len (xs : List Nat) :
    (raw xs).length = 1 + ubytes xs.length + 4 * xs.length := by
  simp [raw, fixed, little, uleb_len_eq_ubytes]
  have hsum : (List.map (fun _ : Nat => 4) xs).sum = 4 * xs.length := by
    induction xs with
    | nil => simp
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        omega
  omega

private theorem chosen_write_le (xs : List Nat) :
    chosen write xs ≤ 1 + ubytes xs.length + 4 * xs.length := by
  rw [← raw_len]
  exact Nat.min_le_right _ _

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
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          simp [hflat]
      | cons a rest =>
          dsimp only at hmem
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl <;>
            simpa [List.flatten_cons] using congrArg (List.cons x) hflat

private theorem parts_prepend (xs : List Nat) (k : Nat) (tail : List (List Nat))
    (hk : 0 < k) (hklen : k ≤ xs.length) (htail : tail ∈ parts (xs.drop k)) :
    xs.take k :: tail ∈ parts xs := by
  induction k generalizing xs with
  | zero => omega
  | succ k ih =>
      cases xs with
      | nil => simp at hklen
      | cons x xs =>
          simp only [List.length_cons] at hklen
          cases k with
          | zero =>
              cases tail with
              | nil =>
                  have hnil : xs = [] := by simpa using parts_flatten xs [] htail
                  subst xs
                  simp [parts]
              | cons a rest =>
                  rw [parts]
                  exact List.mem_flatMap.mpr ⟨a :: rest, htail, by simp⟩
          | succ k' =>
              rw [List.take_succ_cons]
              rw [List.drop_succ_cons] at htail
              rw [parts]
              exact List.mem_flatMap.mpr ⟨xs.take (k' + 1) :: tail,
                ih xs (by omega) (by omega) htail, by simp⟩

private theorem single_part_mem (xs : List Nat) (hxs : xs ≠ []) : [xs] ∈ parts xs := by
  cases xs with
  | nil => exact (hxs rfl).elim
  | cons x xs' =>
      induction xs' generalizing x with
      | nil =>
          simp [parts]
      | cons y ys ih =>
          change [x :: y :: ys] ∈ (parts (y :: ys)).flatMap
            (fun ps => match ps with
              | [] => [[[x]]]
              | a :: rest => [[x] :: a :: rest, (x :: a) :: rest])
          exact List.mem_flatMap.mpr ⟨[y :: ys], ih y (by simp), by simp⟩

private theorem pageLimit_zero (cap : Nat) (hcap : 0 < cap) : pageLimit 0 cap = 0 := by
  unfold pageLimit
  rw [Nat.div_eq_of_lt]
  omega

private theorem pageLimit_succ_chunk (n cap : Nat) (hcap : 0 < cap) (hgt : cap < n) :
    pageLimit n cap = 1 + pageLimit (n - cap) cap := by
  unfold pageLimit
  have hsub : n - cap + cap - 1 = n - 1 := by omega
  rw [hsub]
  have hn1 : n + cap - 1 = (n - 1) + cap := by omega
  rw [hn1]
  have hdiv := Nat.add_mul_div_right (n - 1) 1 hcap
  simpa [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc, Nat.one_mul] using hdiv

private theorem exists_bounded (n : Nat) :
    ∀ xs : List Nat, xs.length = n → ∀ cap : Nat, 0 < cap →
      ∃ ps ∈ parts xs, (∀ p ∈ ps, p.length ≤ cap) ∧
        charge write 0 ps ≤ xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hxlen cap hcap
      cases xs with
      | nil =>
          refine ⟨[], ?_, ?_, ?_⟩
          · simp [parts]
          · simp
          · simp [charge, pageLimit_zero cap hcap]
      | cons x xs' =>
          by_cases hle : (x :: xs').length ≤ cap
          · refine ⟨[x :: xs'], single_part_mem _ (by simp), ?_, ?_⟩
            · intro p hp
              simp only [List.mem_singleton] at hp
              subst p
              exact hle
            · simp only [charge, List.map_cons, List.sum_cons, List.map_nil, List.sum_nil, Nat.add_zero]
              have hc := chosen_write_le (x :: xs')
              have hub : ubytes (x :: xs').length ≤ ubytes cap := ubytes_mono hle
              have hlenpos : 0 < (x :: xs').length := by simp
              have hpl : 1 ≤ pageLimit (x :: xs').length cap := by
                unfold pageLimit
                rw [Nat.le_div_iff_mul_le hcap]
                simp only [Nat.one_mul]
                omega
              have hone : 1 + ubytes (x :: xs').length ≤
                  pageLimit (x :: xs').length cap * (1 + ubytes cap) := by
                have hmul : 1 * (1 + ubytes cap) ≤
                    pageLimit (x :: xs').length cap * (1 + ubytes cap) :=
                  Nat.mul_le_mul_right (1 + ubytes cap) hpl
                omega
              omega
          · have hgt : cap < (x :: xs').length := by omega
            let rest := (x :: xs').drop cap
            have hrestlen : rest.length < n := by
              dsimp [rest]
              rw [List.length_drop]
              rw [← hxlen]
              omega
            obtain ⟨ps', hps', hcaprest, hchargerest⟩ :=
              ih rest.length hrestlen rest rfl cap hcap
            refine ⟨(x :: xs').take cap :: ps', ?_, ?_, ?_⟩
            · exact parts_prepend (x :: xs') cap ps' (by omega) (by omega) hps'
            · intro p hp
              simp only [List.mem_cons] at hp
              rcases hp with rfl | hp
              · rw [List.length_take]
                exact Nat.min_le_left _ _
              · exact hcaprest p hp
            · unfold charge at hchargerest ⊢
              simp only [List.map_cons, List.sum_cons]
              have htake_len : ((x :: xs').take cap).length = cap := by
                rw [List.length_take]
                exact Nat.min_eq_left (by omega)
              have hctake : chosen write ((x :: xs').take cap) ≤ 1 + ubytes cap + 4 * cap := by
                have hc := chosen_write_le ((x :: xs').take cap)
                have hub : ubytes ((x :: xs').take cap).length ≤ ubytes cap :=
                  ubytes_mono (by rw [List.length_take]; exact Nat.min_le_left _ _)
                omega
              have hrest_len : rest.length = (x :: xs').length - cap := by
                simp [rest, List.length_drop]
              have hpage : pageLimit (x :: xs').length cap = 1 + pageLimit rest.length cap := by
                rw [hrest_len]
                exact pageLimit_succ_chunk (x :: xs').length cap hcap hgt
              have hpage_mul : pageLimit (x :: xs').length cap * (1 + ubytes cap) =
                  (1 + ubytes cap) + pageLimit rest.length cap * (1 + ubytes cap) := by
                rw [hpage, Nat.add_mul, Nat.one_mul]
              rw [hpage_mul]
              have hsubadd : (x :: xs').length - cap + cap = (x :: xs').length := by omega
              omega

theorem sound_BoundedRaw : SeparatedLengths.BoundedRaw.Goal := by
  intro xs cap s hcap hb
  unfold budget at hb
  obtain ⟨ps, hps, hcapps, hcharge⟩ := exists_bounded xs.length xs rfl cap hcap
  have hmem : charge write 0 ps ∈
      (((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ cap))).map (charge write 0)) := by
    apply List.mem_map.mpr
    refine ⟨ps, ?_, rfl⟩
    apply List.mem_filter.mpr
    refine ⟨hps, ?_⟩
    simpa using hcapps
  exact Nat.le_trans ((List.min?_eq_some_iff.mp hb).2 (charge write 0 ps) hmem) hcharge

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_BoundedRaw
