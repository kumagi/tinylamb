import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.ReorderNeighbors` — Two neighboring states may be scheduled in reverse
  if they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

private def z (x : Nat) : Bool := x == 0
private def suf (xs : List Nat) : Nat := (xs.reverse.takeWhile z).length
private def run (xs : List Nat) (i : Nat) : Nat := ((xs.drop i).takeWhile z).length
private def d (x : Nat) : Nat := if z x then 1 else 0

private def step (st : Nat × Nat × Nat × Nat) (x : Nat) : Nat × Nat × Nat × Nat :=
  match st with
  | (n, p, t, l) =>
      (n + 1, if p == n then n + d x else p,
        if z x then t + 1 else 0, max l (if z x then t + 1 else 0))

private theorem tw_len_eq_iff_all (p : Nat → Bool) (l : List Nat) :
    (l.takeWhile p).length = l.length ↔ l.all p = true := by
  induction l with
  | nil => simp
  | cons y ys ih => by_cases h : p y <;> simp [h, ih]

private theorem takeWhile_length_le (p : Nat → Bool) (l : List Nat) :
    (l.takeWhile p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons y ys ih => by_cases h : p y <;> simp [h] <;> omega

private theorem takeWhile_eq_take_length (p : Nat → Bool) (l : List Nat) :
    l.takeWhile p = l.take (l.takeWhile p).length := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases h : p y
      · simp only [List.takeWhile_cons, h, ↓reduceIte, List.length_cons]
        rw [List.take_succ_cons]
        congr 1
      · simp [h]

private theorem tw_singleton_len (x : Nat) : (List.takeWhile z [x]).length = d x := by
  cases h : z x <;> simp [h, d]

private theorem suf_eq_length_iff_all (xs : List Nat) :
    suf xs = xs.length ↔ xs.all z = true := by
  unfold suf
  rw [← List.length_reverse (as := xs)]
  simpa [List.all_reverse] using (tw_len_eq_iff_all z xs.reverse)

private theorem suf_le_length (xs : List Nat) : suf xs ≤ xs.length := by
  unfold suf
  rw [← List.length_reverse (as := xs)]
  exact takeWhile_length_le z xs.reverse

private theorem max?_none_iff_nil (l : List Nat) : l.max? = none ↔ l = [] := by
  cases l with
  | nil => simp
  | cons y ys => simp [List.max?_cons]

private theorem peak_mem_le (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ peak l := by
  unfold peak
  cases hm : l.max? with
  | none =>
      have hnil : l = [] := (max?_none_iff_nil l).mp hm
      simp [hnil] at hx
  | some m =>
      have hall : ∀ b ∈ l, b ≤ m := (List.max?_le_iff hm (x := m)).mp (Nat.le_refl m)
      simpa [hm] using hall x hx

private theorem peak_le_of_all_le (l : List Nat) (x : Nat) (h : ∀ b ∈ l, b ≤ x) :
    peak l ≤ x := by
  cases hm : l.max? with
  | none =>
      have hnil : l = [] := (max?_none_iff_nil l).mp hm
      simp [hnil, peak]
  | some m =>
      have hmle : m ≤ x := (List.max?_le_iff hm (x := x)).mpr h
      simpa [peak, hm] using hmle

private theorem peak_cons (y : Nat) (ys : List Nat) : peak (y :: ys) = max y (peak ys) := by
  cases h : ys.max? with
  | none =>
      have hnil : ys = [] := (max?_none_iff_nil ys).mp h
      simp [hnil, peak]
  | some m => simp [peak, List.max?_cons, h]

private theorem peak_append_singleton (l : List Nat) (x : Nat) :
    peak (l ++ [x]) = max (peak l) x := by
  induction l with
  | nil => simp [peak]
  | cons y ys ih =>
      rw [List.cons_append, peak_cons, ih, peak_cons]
      simp [Nat.max_assoc]

private theorem peak_map_le (n : Nat) (f g : Nat → Nat)
    (h : ∀ i, i ∈ List.range n → f i ≤ g i) :
    peak ((List.range n).map f) ≤ peak ((List.range n).map g) := by
  induction n with
  | zero => simp [peak]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_append]
      simp only [List.map_singleton]
      rw [peak_append_singleton, peak_append_singleton]
      exact Nat.max_le.mpr ⟨Nat.le_trans (ih (by intro i hi; exact h i (by simp [List.mem_range] at hi ⊢; omega))) (Nat.le_max_left _ _),
        Nat.le_trans (h n (by simp)) (Nat.le_max_right _ _)⟩

private theorem rev_tw_rev (xs : List Nat) :
    (xs.reverse.takeWhile z).reverse = xs.drop (xs.length - suf xs) := by
  unfold suf
  rw [takeWhile_eq_take_length z xs.reverse]
  rw [List.reverse_take]
  have hle : (xs.reverse.takeWhile z).length ≤ xs.length := by
    rw [← List.length_reverse (as := xs)]
    exact takeWhile_length_le z xs.reverse
  simp [List.length_reverse, Nat.min_eq_left hle]

private theorem all_drop_sub_suf (xs : List Nat) : (xs.drop (xs.length - suf xs)).all z = true := by
  rw [← rev_tw_rev xs]
  rw [List.all_reverse]
  exact List.all_takeWhile

private theorem run_suffix (xs : List Nat) : run xs (xs.length - suf xs) = suf xs := by
  have hall := all_drop_sub_suf xs
  have hc : ((xs.drop (xs.length - suf xs)).takeWhile z).length =
      (xs.drop (xs.length - suf xs)).length := (tw_len_eq_iff_all z _).mpr hall
  have htw : (xs.drop (xs.length - suf xs)).takeWhile z = xs.drop (xs.length - suf xs) :=
    List.Sublist.eq_of_length (List.takeWhile_sublist (p := z) (l := xs.drop (xs.length - suf xs))) hc
  unfold run
  rw [htw]
  simp [List.length_drop]
  have hle := suf_le_length xs
  omega

private theorem suf_cons_ge (y : Nat) (ys : List Nat) : suf ys ≤ suf (y :: ys) := by
  unfold suf
  rw [List.reverse_cons, List.takeWhile_append]
  by_cases h : (ys.reverse.takeWhile z).length = ys.reverse.length
  · rw [if_pos h]
    simp only [List.length_append, tw_singleton_len]
    rw [h]
    omega
  · rw [if_neg h]
    simp

private theorem suf_drop_le (xs : List Nat) (i : Nat) : suf (xs.drop i) ≤ suf xs := by
  induction xs generalizing i with
  | nil => simp [suf]
  | cons y ys ih =>
      cases i with
      | zero => simpa using (suf_cons_ge y ys)
      | succ j => simpa using (Nat.le_trans (ih j) (suf_cons_ge y ys))

private theorem len_sub_le_suf (a : List Nat) (i : Nat) (hi : i ≤ a.length)
    (hall : (a.drop i).all z = true) : a.length - i ≤ suf a := by
  have hs : suf (a.drop i) = (a.drop i).length := (suf_eq_length_iff_all _).mpr hall
  have hle := suf_drop_le a i
  rw [List.length_drop] at hs
  omega

private theorem suf_le_lz (xs : List Nat) : suf xs ≤ longestZero xs := by
  change suf xs ≤ peak ((List.range (xs.length + 1)).map (run xs))
  apply peak_mem_le
  rw [List.mem_map]
  refine ⟨xs.length - suf xs, ?_, ?_⟩
  · simp [List.mem_range]
    omega
  · exact run_suffix xs

private theorem run_append_singleton (a : List Nat) (x : Nat) (i : Nat) (hi : i ≤ a.length) :
    run (a ++ [x]) i = run a i + (if (a.drop i).all z then d x else 0) := by
  unfold run
  rw [List.drop_append_of_le_length hi]
  rw [List.takeWhile_append]
  by_cases h : (a.drop i).all z = true
  · have hc : ((a.drop i).takeWhile z).length = (a.drop i).length :=
      (tw_len_eq_iff_all z (a.drop i)).mpr h
    rw [if_pos hc]
    simp only [h, d, ↓reduceIte, List.length_append, tw_singleton_len]
    rw [hc]
  · have hc : ((a.drop i).takeWhile z).length ≠ (a.drop i).length := by
      intro hc
      exact h ((tw_len_eq_iff_all z (a.drop i)).mp hc)
    rw [if_neg hc]
    simp [h]

private theorem suf_snoc (a : List Nat) (x : Nat) :
    suf (a ++ [x]) = if z x then suf a + 1 else 0 := by
  unfold suf
  rw [List.reverse_append]
  cases h : z x <;> simp [h, List.takeWhile_cons]

private theorem lz_snoc (a : List Nat) (x : Nat) :
    longestZero (a ++ [x]) = max (longestZero a) (if z x then suf a + 1 else 0) := by
  change peak ((List.range ((a ++ [x]).length + 1)).map (run (a ++ [x]))) =
    max (longestZero a) (if z x then suf a + 1 else 0)
  have hlen : (a ++ [x]).length + 1 = (a.length + 1) + 1 := by simp
  rw [hlen, List.range_succ, List.map_append]
  simp only [List.map_singleton]
  rw [peak_append_singleton]
  have hlast : run (a ++ [x]) (a.length + 1) = 0 := by
    unfold run
    simp
  rw [hlast]
  rw [Nat.max_eq_left (Nat.zero_le _)]
  apply Nat.le_antisymm
  · apply peak_le_of_all_le
    intro v hv
    rw [List.mem_map] at hv
    obtain ⟨i, hi, rfl⟩ := hv
    rw [List.mem_range] at hi
    have hi' : i ≤ a.length := by omega
    rw [run_append_singleton a x i hi']
    by_cases hall : (a.drop i).all z = true
    · simp [hall]
      have hrun_eq : run a i = a.length - i := by
        unfold run
        have hc := (tw_len_eq_iff_all z (a.drop i)).mpr hall
        rw [hc]
        simp [List.length_drop]
      have hle : a.length - i ≤ suf a := len_sub_le_suf a i hi' hall
      have hsl := suf_le_lz a
      cases hx : z x <;> simp [hx, d] <;> omega
    · simp [hall]
      have hmem : run a i ∈ (List.range (a.length + 1)).map (run a) := by
        rw [List.mem_map]
        exact ⟨i, by simp [List.mem_range, hi], rfl⟩
      have hle : run a i ≤ longestZero a := by
        unfold longestZero
        exact peak_mem_le _ _ hmem
      omega
  · apply Nat.max_le.mpr
    constructor
    · unfold longestZero
      apply peak_map_le
      intro i hi
      rw [List.mem_range] at hi
      have hi' : i ≤ a.length := by omega
      rw [run_append_singleton a x i hi']
      by_cases hall : (a.drop i).all z = true
      · simp [hall]
        change ((a.drop i).takeWhile z).length ≤ ((a.drop i).takeWhile z).length + d x
        omega
      · simp [hall]
        change ((a.drop i).takeWhile z).length ≤ ((a.drop i).takeWhile z).length
        omega
    · by_cases hx : z x
      · simp [hx]
        have hi : a.length - suf a < a.length + 1 := by omega
        have hmem : suf a + 1 ∈ (List.range (a.length + 1)).map (run (a ++ [x])) := by
          rw [List.mem_map]
          refine ⟨a.length - suf a, by simpa [List.mem_range] using hi, ?_⟩
          rw [run_append_singleton a x (a.length - suf a) (by omega), run_suffix]
          have hall := all_drop_sub_suf a
          simp [hall, hx, d]
        exact peak_mem_le _ _ hmem
      · simp [hx]

private theorem ref_snoc (a : List Nat) (x : Nat) :
    agg.reference (a ++ [x]) = step (agg.reference a) x := by
  simp only [agg, step]
  apply Prod.ext
  · simp
  apply Prod.ext
  · change ((a ++ [x]).takeWhile z).length =
      (if (a.takeWhile z).length == a.length then a.length + d x else (a.takeWhile z).length)
    rw [List.takeWhile_append]
    by_cases hp : (a.takeWhile z).length = a.length
    · rw [if_pos hp]
      simp [hp, tw_singleton_len, List.length_append]
    · rw [if_neg hp]
      simp [hp]
  apply Prod.ext
  · change suf (a ++ [x]) = if z x then suf a + 1 else 0
    exact suf_snoc a x
  · change longestZero (a ++ [x]) = max (longestZero a) (if z x then suf a + 1 else 0)
    exact lz_snoc a x

private theorem join_step_aux (n p l s m q t w : Nat) (hp : p ≤ n) (hq : q ≤ m) (ht : t ≤ m)
    (hqt : (q == m) = (t == m)) (x : Nat) :
    agg.join (n, p, l, s) (step (m, q, t, w) x) =
      step (agg.join (n, p, l, s) (m, q, t, w)) x := by
  have hqtIff : (q = m) ↔ (t = m) := by
    simpa [beq_iff_eq] using (Bool.eq_iff_iff.mp hqt)
  by_cases hpN : p = n <;> by_cases hqM : q = m <;> by_cases htM : t = m <;>
    cases hzx : z x <;>
    simp [agg, step, d, beq_iff_eq, hpN, hqM, htM, hzx] <;>
    first | omega | (repeat' constructor <;> first | omega | ac_rfl)

private theorem join_step (a b : List Nat) (x : Nat) :
    agg.join (agg.reference a) (step (agg.reference b) x) =
      step (agg.join (agg.reference a) (agg.reference b)) x := by
  have hp : (agg.reference a).2.1 ≤ (agg.reference a).1 := by
    simp [agg]
    exact takeWhile_length_le z a
  have hq : (agg.reference b).2.1 ≤ (agg.reference b).1 := by
    simp [agg]
    exact takeWhile_length_le z b
  have ht : (agg.reference b).2.2.1 ≤ (agg.reference b).1 := by
    simp [agg]
    exact suf_le_length b
  have hqt : ((agg.reference b).2.1 == (agg.reference b).1) =
      ((agg.reference b).2.2.1 == (agg.reference b).1) := by
    change ((b.takeWhile z).length == b.length) = (suf b == b.length)
    have hall := suf_eq_length_iff_all b
    have hpre := tw_len_eq_iff_all z b
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      exact beq_iff_eq.mpr (hall.mpr (hpre.mp (beq_iff_eq.mp h)))
    · intro h
      exact beq_iff_eq.mpr (hpre.mpr (hall.mp (beq_iff_eq.mp h)))
  have ha := join_step_aux (agg.reference a).1 (agg.reference a).2.1
    (agg.reference a).2.2.1 (agg.reference a).2.2.2
    (agg.reference b).1 (agg.reference b).2.1 (agg.reference b).2.2.1
    (agg.reference b).2.2.2 hp hq ht hqt x
  simpa [agg] using ha

private theorem stitch_agg : stitch agg := by
  intro a b
  have hrev : ∀ bs : List Nat, agg.join (agg.reference a) (agg.reference bs.reverse) =
      agg.reference (a ++ bs.reverse) := by
    intro bs
    induction bs with
    | nil =>
        simp [agg]
        constructor
        · intro h; exact h.symm
        · have hsl := suf_le_lz a
          change max (longestZero a) (suf a) = longestZero a
          exact Nat.max_eq_left hsl
    | cons x xs ih =>
        rw [List.reverse_cons]
        rw [ref_snoc xs.reverse x]
        rw [show a ++ (xs.reverse ++ [x]) = (a ++ xs.reverse) ++ [x] by simp [List.append_assoc]]
        rw [ref_snoc (a ++ xs.reverse) x]
        rw [join_step a xs.reverse x]
        rw [ih]
  simpa using hrev b.reverse

private theorem reorderNeighbors_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    reorderNeighbors f := by
  intro pre a b suf hcomm
  have stepCtx : ∀ x y : List Nat,
      f.reference (pre ++ x ++ y ++ suf) =
        f.join (f.join (f.reference pre) (f.join (f.reference x) (f.reference y)))
          (f.reference suf) := by
    intro x y
    rw [show pre ++ x ++ y ++ suf = (pre ++ (x ++ y)) ++ suf by simp [List.append_assoc]]
    rw [← h, ← h, ← h]
  rw [stepCtx a b, stepCtx b a, hcomm]

theorem sound_ReorderNeighbors : LongestHole.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_ReorderNeighbors
