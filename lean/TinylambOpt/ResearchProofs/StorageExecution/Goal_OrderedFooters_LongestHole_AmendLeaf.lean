import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole

private def zpref (xs : List Nat) := (xs.takeWhile (fun x => x == 0)).length
private def zsuf (xs : List Nat) := (xs.reverse.takeWhile (fun x => x == 0)).length
private def runs (xs : List Nat) := (List.range (xs.length + 1)).map (fun i => zpref (xs.drop i))

private theorem longestZero_eq (xs : List Nat) : longestZero xs = (runs xs).max?.getD 0 := by
  rfl

private theorem max_le_of_mem_le {l : List Nat} {M : Nat} (h : ∀ v ∈ l, v ≤ M) :
    l.max?.getD 0 ≤ M := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.max?_cons]
      simp only [Option.getD, Option.elim]
      cases hm : l.max? with
      | none =>
          simpa using h a (by simp)
      | some m =>
          have ha : a ≤ M := h a (by simp)
          have hmle : m ≤ M := by
            have h' := ih (by intro v hv; exact h v (by simp [hv]))
            simpa [hm] using h'
          exact Nat.max_le.mpr ⟨ha, hmle⟩

private theorem le_max_of_mem {l : List Nat} {v : Nat} (h : v ∈ l) : v ≤ l.max?.getD 0 := by
  induction l with
  | nil => simp at h
  | cons a l ih =>
      rw [List.max?_cons]
      simp only [Option.getD, Option.elim]
      rcases List.mem_cons.mp h with rfl | hv
      · cases hm : l.max? with
        | none => simp
        | some m => exact Nat.le_max_left _ _
      · cases hm : l.max? with
        | none =>
            have hvle : v ≤ 0 := by
              have h' := ih hv
              simpa [hm] using h'
            omega
        | some m =>
            have hvle : v ≤ m := by
              have h' := ih hv
              simpa [hm] using h'
            exact Nat.le_trans hvle (Nat.le_max_right a m)

private theorem zpref_allzero (xs : List Nat) (h : ∀ x ∈ xs, x == 0) : zpref xs = xs.length := by
  induction xs with
  | nil => simp [zpref]
  | cons x xs ih =>
      have hx : x == 0 := h x (by simp)
      have hs : ∀ y ∈ xs, y == 0 := by intro y hy; exact h y (by simp [hy])
      simp [zpref, hx]
      change zpref xs = xs.length
      exact ih hs

private theorem zpref_eq_length_iff (xs : List Nat) : zpref xs = xs.length ↔ ∀ x ∈ xs, x == 0 := by
  induction xs with
  | nil => simp [zpref]
  | cons y ys ih =>
      by_cases hy : y == 0
      · have hy0 : y = 0 := by simpa using hy
        simp [zpref, hy0]
        change zpref ys = ys.length ↔ ∀ x ∈ ys, x = 0
        simpa using ih
      · have hy0 : y ≠ 0 := by simpa using hy
        simp [zpref, hy, hy0]

private theorem zpref_le_length (xs : List Nat) : zpref xs ≤ xs.length := by
  induction xs with
  | nil => simp [zpref]
  | cons x xs ih =>
      by_cases hx : x == 0
      · simp [zpref, hx]
        exact ih
      · simp [zpref, hx]

private theorem zpref_append (xs ys : List Nat) :
    zpref (xs ++ ys) = if zpref xs == xs.length then xs.length + zpref ys else zpref xs := by
  induction xs with
  | nil => simp [zpref]
  | cons x xs ih =>
      by_cases hx : x == 0
      · simp [zpref, hx]
        change zpref (xs ++ ys) + 1 = if zpref xs = xs.length then xs.length + 1 + zpref ys else zpref xs + 1
        rw [ih]
        by_cases hzz : zpref xs = xs.length <;> simp [hzz] <;> omega
      · simp [zpref, hx]

private theorem zsuf_eq (xs : List Nat) : zsuf xs = zpref xs.reverse := by
  rfl

private theorem zsuf_append (xs ys : List Nat) :
    zsuf (xs ++ ys) = if zsuf ys == ys.length then ys.length + zsuf xs else zsuf ys := by
  rw [zsuf_eq, List.reverse_append, zpref_append]
  simp [zsuf_eq]

private theorem zsuf_allzero (xs : List Nat) (h : ∀ x ∈ xs, x == 0) : zsuf xs = xs.length := by
  rw [zsuf_eq]
  have hp := zpref_allzero xs.reverse (by
    intro y hy
    have hyxs : y ∈ xs := by simpa using hy
    simp [h y hyxs])
  simpa using hp

private theorem zsuf_eq_length_iff (xs : List Nat) : zsuf xs = xs.length ↔ ∀ x ∈ xs, x == 0 := by
  constructor
  · intro h y hy
    have hyrev : y ∈ xs.reverse := by simpa using hy
    have hall := (zpref_eq_length_iff xs.reverse).mp (by simpa [zsuf_eq] using h)
    have hy0 := hall y hyrev
    simp [hy0]
  · intro h
    exact zsuf_allzero xs h

private theorem zsuf_le_length (xs : List Nat) : zsuf xs ≤ xs.length := by
  rw [zsuf_eq]
  simpa using zpref_le_length xs.reverse

private theorem zsuf_cons_ge (x : Nat) (xs : List Nat) : zsuf xs ≤ zsuf (x :: xs) := by
  rw [zsuf_eq xs, zsuf_eq (x :: xs), List.reverse_cons]
  rw [zpref_append, List.length_reverse]
  change zsuf xs ≤ if zsuf xs == xs.length then xs.length + zpref [x] else zsuf xs
  by_cases h : zsuf xs == xs.length
  · have heq : zsuf xs = xs.length := by simpa using h
    simp [h]
    omega
  · simp [h]

private theorem drop_allzero_le_zsuf : ∀ (xs : List Nat) i,
    (∀ x ∈ xs.drop i, x == 0) → (xs.drop i).length ≤ zsuf xs
  | [], i, h => by simp [zsuf]
  | x :: xs, 0, h => by
      have hall : ∀ y ∈ x :: xs, y == 0 := by simpa using h
      rw [zsuf_allzero (x :: xs) hall]
      simp
  | x :: xs, i + 1, h => by
      have h' : ∀ y ∈ xs.drop i, y == 0 := by simpa using h
      have hih := drop_allzero_le_zsuf xs i h'
      have hge := zsuf_cons_ge x xs
      simpa using Nat.le_trans hih hge

private theorem lz_drop_le (xs : List Nat) (i : Nat) (hi : i ≤ xs.length) :
    (runs (xs.drop i)).max?.getD 0 ≤ (runs xs).max?.getD 0 := by
  apply max_le_of_mem_le
  intro v hv
  rcases List.mem_map.mp hv with ⟨j, hj, rfl⟩
  simp at hj
  have hidx : i + j < xs.length + 1 := by omega
  apply le_max_of_mem
  rw [runs]
  rw [List.mem_map]
  refine ⟨i + j, ?_, ?_⟩
  · simp [hidx]
  · rw [← List.drop_drop]

private theorem cross_le_lz_append : ∀ (xs ys : List Nat),
    zsuf xs + zpref ys ≤ (runs (xs ++ ys)).max?.getD 0
  | [], ys => by
      simp [zsuf]
      have hmem : zpref ys ∈ runs ys := by
        rw [runs, List.mem_map]
        refine ⟨0, ?_, ?_⟩
        · simp
        · simp
      exact le_max_of_mem hmem
  | x :: xs, ys => by
      rw [zsuf_eq (x :: xs), List.reverse_cons, zpref_append, List.length_reverse]
      change (if zsuf xs == xs.length then xs.length + zpref [x] else zsuf xs) + zpref ys ≤
        (runs ((x :: xs) ++ ys)).max?.getD 0
      by_cases h : zsuf xs == xs.length
      · have heq : zsuf xs = xs.length := by simpa using h
        simp [h]
        have hallxs : ∀ y ∈ xs, y == 0 := (zsuf_eq_length_iff xs).mp heq
        have hpxs : zpref xs = xs.length := zpref_allzero xs hallxs
        have hz : zpref (xs ++ ys) = xs.length + zpref ys := by
          rw [zpref_append]
          simp [hpxs]
        by_cases hx : x == 0
        · simp [zpref, hx]
          change xs.length + 1 + zpref ys ≤ (runs ((x :: xs) ++ ys)).max?.getD 0
          have hallcons : ∀ y ∈ x :: xs, y == 0 := by
            intro y hy
            rcases List.mem_cons.mp hy with rfl | hy
            · simp [hx]
            · exact hallxs y hy
          have hp : zpref (x :: xs) = (x :: xs).length := zpref_allzero (x :: xs) hallcons
          have hz2 : zpref ((x :: xs) ++ ys) = (x :: xs).length + zpref ys := by
            rw [zpref_append]
            simp [hp]
          have hmem : zpref ((x :: xs) ++ ys) ∈ runs ((x :: xs) ++ ys) := by
            rw [runs, List.mem_map]
            refine ⟨0, ?_, ?_⟩
            · simp
            · simp
          have hle := le_max_of_mem hmem
          rw [hz2] at hle
          simpa using hle
        · simp [zpref, hx]
          change xs.length + zpref ys ≤ (runs ((x :: xs) ++ ys)).max?.getD 0
          have hmem : zpref (((x :: xs) ++ ys).drop 1) ∈ runs ((x :: xs) ++ ys) := by
            rw [runs, List.mem_map]
            refine ⟨1, ?_, ?_⟩
            · simp
            · simp
          have hle := le_max_of_mem hmem
          simp at hle
          rw [hz] at hle
          exact hle
      · simp [h]
        have hih := cross_le_lz_append xs ys
        have hle := lz_drop_le ((x :: xs) ++ ys) 1 (by simp)
        simpa using Nat.le_trans hih hle

private theorem runs_append_max (xs ys : List Nat) :
    (runs (xs ++ ys)).max?.getD 0 =
      Nat.max (Nat.max ((runs xs).max?.getD 0) ((runs ys).max?.getD 0)) (zsuf xs + zpref ys) := by
  apply Nat.le_antisymm
  · apply max_le_of_mem_le
    intro v hv
    rcases List.mem_map.mp hv with ⟨i, hi, rfl⟩
    rw [List.length_append] at hi
    simp at hi
    by_cases hixs : i ≤ xs.length
    · have hd : (xs ++ ys).drop i = xs.drop i ++ ys := List.drop_append_of_le_length hixs
      rw [hd]
      rw [zpref_append]
      by_cases hz : zpref (xs.drop i) == (xs.drop i).length
      · rw [ite_eq_left hz]
        have hdlen : (xs.drop i).length ≤ zsuf xs := by
          apply drop_allzero_le_zsuf
          intro x hx
          exact (zpref_eq_length_iff (xs.drop i)).mp (by simpa using hz) x hx
        have hsum : (xs.drop i).length + zpref ys ≤ zsuf xs + zpref ys :=
          Nat.add_le_add_right hdlen _
        exact Nat.le_trans hsum (Nat.le_max_right _ _)
      · rw [ite_eq_right hz]
        have hvle : zpref (xs.drop i) ≤ (runs xs).max?.getD 0 := by
          apply le_max_of_mem
          rw [runs, List.mem_map]
          refine ⟨i, ?_, ?_⟩
          · simp
            omega
          · simp
        exact Nat.le_trans hvle (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hxlt : xs.length < i := Nat.lt_of_not_ge hixs
      rw [List.drop_append]
      rw [List.drop_eq_nil_of_le (Nat.le_of_lt hxlt)]
      rw [List.nil_append]
      let j := i - xs.length
      have hj : j < ys.length + 1 := by omega
      have hvle : zpref (ys.drop j) ≤ (runs ys).max?.getD 0 := by
        apply le_max_of_mem
        rw [runs, List.mem_map]
        refine ⟨j, ?_, ?_⟩
        · simpa [j] using hj
        · simp
      exact Nat.le_trans hvle (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · apply max_le_of_mem_le
        intro v hv
        rcases List.mem_map.mp hv with ⟨i, hi, rfl⟩
        simp at hi
        have hidx : i < (xs ++ ys).length + 1 := by
          rw [List.length_append]
          omega
        have hmem : zpref ((xs ++ ys).drop i) ∈ runs (xs ++ ys) := by
          rw [runs, List.mem_map]
          refine ⟨i, ?_, ?_⟩
          · simpa using hidx
          · simp
        have hle := le_max_of_mem hmem
        have hd : (xs ++ ys).drop i = xs.drop i ++ ys :=
          List.drop_append_of_le_length (Nat.le_of_lt_succ hi)
        rw [hd] at hle
        rw [zpref_append] at hle
        by_cases hz : zpref (xs.drop i) == (xs.drop i).length
        · rw [ite_eq_left hz] at hle
          have hzi : zpref (xs.drop i) = (xs.drop i).length := by simpa using hz
          omega
        · rw [ite_eq_right hz] at hle
          exact hle
      · apply max_le_of_mem_le
        intro v hv
        rcases List.mem_map.mp hv with ⟨j, hj, rfl⟩
        simp at hj
        let i := xs.length + j
        have hidx : i < (xs ++ ys).length + 1 := by
          rw [List.length_append]
          dsimp [i]
          omega
        have hmem : zpref ((xs ++ ys).drop i) ∈ runs (xs ++ ys) := by
          rw [runs, List.mem_map]
          refine ⟨i, ?_, ?_⟩
          · simpa using hidx
          · simp
        have hle := le_max_of_mem hmem
        have hdrop : (xs ++ ys).drop i = ys.drop j := by
          dsimp [i]
          rw [List.drop_append]
          rw [List.drop_eq_nil_of_le (by omega)]
          simp
        rw [hdrop] at hle
        exact hle
    · exact cross_le_lz_append xs ys

private theorem agg_stitch : stitch agg := by
  intro a b
  simp [agg, longestZero_eq, runs_append_max]
  constructor
  · simpa [zpref] using (zpref_append a b).symm
  · constructor
    · simpa [zpref, List.length_reverse] using (zpref_append b.reverse a.reverse).symm
    · simp [zsuf, zpref]

private theorem foldl_ref_append (ps : List (List Nat)) (acc : List Nat) :
    (ps.map agg.reference).foldl agg.join (agg.reference acc) =
      agg.reference (acc ++ ps.flatten) := by
  induction ps generalizing acc with
  | nil => simp
  | cons p ps ih =>
      rw [List.map_cons, List.foldl_cons]
      rw [agg_stitch acc p]
      simpa using ih (acc ++ p)

private theorem combine_eq_reference_flatten (ps : List (List Nat)) :
    combine agg ps = agg.reference ps.flatten := by
  simpa [combine] using foldl_ref_append ps []

private theorem flatten_set (ps : List (List Nat)) (i : Nat) (xs : List Nat) (hi : i < ps.length) :
    (ps.set i xs).flatten = (ps.take i).flatten ++ (xs ++ (ps.drop (i + 1)).flatten) := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp at hi ⊢
          simpa [List.append_assoc] using ih i hi

theorem sound_AmendLeaf : LongestHole.AmendLeaf.Goal := by
  intro ps i xs hi
  rw [combine_eq_reference_flatten (ps.set i xs)]
  rw [combine_eq_reference_flatten (ps.take i)]
  rw [combine_eq_reference_flatten (ps.drop (i + 1))]
  rw [flatten_set ps i xs hi]
  rw [agg_stitch xs (ps.drop (i + 1)).flatten]
  rw [agg_stitch (ps.take i).flatten (xs ++ (ps.drop (i + 1)).flatten)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_AmendLeaf
