import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_TurnExtrema

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.SegmentedSlope

/-- The scalar carried by the two segments of `SegmentedSlope.layout`. -/
private def val (c : Config) (i : Nat) : Int :=
  if i < c.split then c.base + c.step * (i : Int)
  else c.base + c.step * (c.split : Int) + c.bias * ((i - c.split : Nat) : Int)

private theorem read_eq (c : Config) (i : Nat) : layout.read c i = some (val c i) := by
  by_cases h : i < c.split <;> simp [layout, val, h]

private theorem rows_snd (c : Config) :
    (rows layout c).map Prod.snd = (List.range c.n).map (fun i => val c i) := by
  simp [rows, read_eq]

/-- Physical indices that can be extremal: both ends of each segment. -/
private def cand (c : Config) : List Nat :=
  [0, c.split - 1, c.split, c.n - 1].filter (fun x => x < c.n)

private theorem cand_lt (c : Config) {i : Nat} (hi : i ∈ cand c) : i < c.n := by
  have h : i ∈ [0, c.split - 1, c.split, c.n - 1] ∧ i < c.n := by
    simpa [cand] using hi
  exact h.2

private theorem mem_cand_val (c : Config) {i : Nat} (hi : i ∈ cand c) :
    val c i ∈ (cand c).filterMap (layout.read c) := by
  rw [List.mem_filterMap]
  exact ⟨i, hi, read_eq c i⟩

private theorem memV_of_memC (c : Config) {b : Int}
    (hb : b ∈ (cand c).filterMap (layout.read c)) :
    b ∈ (rows layout c).map Prod.snd := by
  rw [List.mem_filterMap] at hb
  rcases hb with ⟨i, hi, hread⟩
  rw [read_eq] at hread
  have hread' : val c i = b := by simpa using hread
  rw [rows_snd]
  exact List.mem_map.mpr ⟨i, List.mem_range.mpr (cand_lt c hi), hread'⟩

/-- Every row value is bounded by a value at one of the four candidate indices. -/
private theorem le_candidate (c : Config) (i : Nat) (hi : i < c.n) :
    ∃ j : Nat, j ∈ cand c ∧ val c i ≤ val c j := by
  by_cases his : i < c.split
  · by_cases hstep : 0 ≤ c.step
    · let j := min c.split c.n - 1
      have hij : i ≤ j := by
        have hmin : i < min c.split c.n := Nat.lt_min.mpr ⟨his, hi⟩
        omega
      have hjlt : j < c.n := by
        have hmin : min c.split c.n ≤ c.n := Nat.min_le_right _ _
        omega
      have hjlt_split : j < c.split := by
        have hmin : min c.split c.n ≤ c.split := Nat.min_le_left _ _
        omega
      have hjmem : j ∈ cand c := by
        rw [cand, List.mem_filter]
        constructor
        · by_cases hsn : c.split ≤ c.n
          · simp [j, Nat.min_eq_left hsn]
          · have hns : c.n ≤ c.split := by omega
            simp [j, Nat.min_eq_right hns]
        · simpa using hjlt
      refine ⟨j, hjmem, ?_⟩
      have hijI : (i : Int) ≤ (j : Int) := by exact_mod_cast hij
      have hmul : c.step * (i : Int) ≤ c.step * (j : Int) :=
        Int.mul_le_mul_of_nonneg_left hijI hstep
      simp [val, his, hjlt_split]
      omega
    · have hstepneg : c.step < 0 := by omega
      have hsplitpos : 0 < c.split := by omega
      have hnpos : 0 < c.n := by omega
      refine ⟨0, ?_, ?_⟩
      · simp [cand, hnpos]
      · have hmul : c.step * (i : Int) ≤ 0 :=
          Int.mul_nonpos_of_nonpos_of_nonneg (by omega) (Int.natCast_nonneg i)
        simp [val, his, hsplitpos]
        omega
  · have hsplitn : c.split < c.n := by omega
    by_cases hbias : 0 ≤ c.bias
    · refine ⟨c.n - 1, ?_, ?_⟩
      · simp [cand] <;> omega
      · have hle : i ≤ c.n - 1 := Nat.le_sub_one_of_lt hi
        have hle' : i - c.split ≤ (c.n - 1) - c.split := Nat.sub_le_sub_right hle _
        have hleI : ((i - c.split : Nat) : Int) ≤ (((c.n - 1) - c.split : Nat) : Int) := by
          exact_mod_cast hle'
        have hmul : c.bias * ((i - c.split : Nat) : Int) ≤
            c.bias * (((c.n - 1) - c.split : Nat) : Int) :=
          Int.mul_le_mul_of_nonneg_left hleI hbias
        have hnot_j : ¬ c.n - 1 < c.split := by omega
        simp [val, his, hnot_j]
        omega
    · have hbiasneg : c.bias < 0 := by omega
      refine ⟨c.split, ?_, ?_⟩
      · simp [cand, hsplitn]
      · have hmul : c.bias * ((i - c.split : Nat) : Int) ≤ 0 :=
          Int.mul_nonpos_of_nonpos_of_nonneg (by omega) (Int.natCast_nonneg (i - c.split))
        simp [val, his]
        omega

theorem sound_SegmentedSlope_TurnExtrema : TurnExtrema.Goal := by
  intro c
  change ((rows layout c).map Prod.snd).max? = ((cand c).filterMap (layout.read c)).max?
  by_cases hn : c.n = 0
  · simp [rows, layout, cand, hn]
  · have hnpos : 0 < c.n := Nat.pos_of_ne_zero hn
    have hVne : (rows layout c).map Prod.snd ≠ [] := by
      rw [rows_snd]
      simp [List.range_eq_nil, hn]
    have hVnone : ((rows layout c).map Prod.snd).max? ≠ none := by
      intro h
      have hnil : (rows layout c).map Prod.snd = [] := (List.max?_eq_none_iff).mp h
      exact hVne hnil
    cases hmaxV : ((rows layout c).map Prod.snd).max? with
    | none => exact False.elim (hVnone hmaxV)
    | some a =>
      have ha_mem : a ∈ (rows layout c).map Prod.snd := List.max?_mem hmaxV
      rw [rows_snd] at ha_mem
      rcases List.mem_map.mp ha_mem with ⟨i, hi_range, hia⟩
      have hi_lt : i < c.n := List.mem_range.mp hi_range
      rcases le_candidate c i hi_lt with ⟨j, hjmem, hle⟩
      have hjval_mem : val c j ∈ (cand c).filterMap (layout.read c) := mem_cand_val c hjmem
      have hjval_memV : val c j ∈ (rows layout c).map Prod.snd := memV_of_memC c hjval_mem
      have hvalj_le_a : val c j ≤ a := (List.max?_eq_some_iff.mp hmaxV).2 (val c j) hjval_memV
      have ha_le_valj : a ≤ val c j := by rw [← hia]; exact hle
      have hvalj_eq : val c j = a := by omega
      have hCsome : ((cand c).filterMap (layout.read c)).max? = some a := by
        rw [List.max?_eq_some_iff]
        constructor
        · rw [← hvalj_eq]
          exact hjval_mem
        · intro b hb
          exact (List.max?_eq_some_iff.mp hmaxV).2 b (memV_of_memC c hb)
      rw [hCsome]

end TinylambOpt.ResearchProofs.Atlas.Goal_SegmentedSlope_TurnExtrema
