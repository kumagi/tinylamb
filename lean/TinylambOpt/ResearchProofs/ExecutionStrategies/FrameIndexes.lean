import TinylambOpt.ResearchGoals.ExecutionStrategies.FrameIndexes

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.FrameIndexes

open TinylambOpt.ResearchGoals.FrameIndexes

/-!
# Window/frame index lemmas

`frame xs l r = (xs.take r).drop l` is the half-open slice `[l, r)`.
The proofs below only need elementary `List.take`/`drop`/`getElem?` facts.
-/

private theorem length_take_of_le {α : Type} (xs : List α) {a : Nat} (h : a ≤ xs.length) :
    (xs.take a).length = a := by
  rw [List.length_take, Nat.min_eq_left h]

/-- `EXCLUDE GROUP`: deleting the contiguous peer interval `[a, b)` shifts the
requested surviving rank `k` by the removed width whenever `k ≥ a`. -/
theorem sound_ExcludePeerSelect : ExcludePeerSelect.Goal := by
  intro xs a b k hab hb
  have ha_len : (xs.take a).length = a := length_take_of_le xs (by omega)
  have hcat_len : (xs.take a ++ xs.drop b).length = xs.length - (b - a) := by
    rw [List.length_append, ha_len, List.length_drop]
    omega
  by_cases hk : k < a
  · -- Before the deleted interval: index unchanged.
    rw [ite_eq_left hk, List.getElem?_append,
      ite_eq_left (by rw [ha_len]; exact hk), List.getElem?_take, ite_eq_left hk]
  · -- At/after the deleted interval: shift the index by `b - a`.
    rw [ite_eq_right hk]
    by_cases hk2 : k < (xs.take a ++ xs.drop b).length
    · have htake : ¬ k < (xs.take a).length := by omega
      rw [List.getElem?_append, ite_eq_right htake, List.getElem?_drop]
      congr 1
      omega
    · rw [List.getElem?_eq_none (Nat.le_of_not_lt hk2)]
      apply List.getElem?_eq_none
      rw [hcat_len] at hk2
      omega

/-! ### Session boundary reconciliation

`sessions gap xs` counts `1` plus the number of adjacent pairs `(a, b)` with
`a + gap < b`. Splitting `xs = a ++ b` only changes the count through the
single boundary pair `(last a, head b)`, which contributes `1` exactly when
`head b ≤ last a + gap`. -/

private def apairs {α : Type} (xs : List α) : List (α × α) := xs.zip xs.tail

private def cpair {α : Type} (x : α) (w : List α) : List (α × α) :=
  match w.head? with | some y => [(x, y)] | none => []

private def bpair {α : Type} (u v : List α) : List (α × α) :=
  match u.getLast?, v.head? with
  | some x, some y => [(x, y)]
  | _, _ => []

private theorem apairs_cons {α : Type} (x : α) (w : List α) :
    apairs (x :: w) = cpair x w ++ apairs w := by
  cases w <;> rfl

private theorem bpair_singleton {α : Type} (x : α) (v : List α) :
    bpair [x] v = cpair x v := by
  cases v <;> rfl

private theorem bpair_cons_cons {α : Type} (x y : α) (ys : List α) (v : List α) :
    bpair (x :: y :: ys) v = bpair (y :: ys) v := by
  simp [bpair, List.getLast?_cons]

private theorem apairs_append {α : Type} (u v : List α) :
    apairs (u ++ v) = apairs u ++ bpair u v ++ apairs v := by
  induction u with
  | nil => rfl
  | cons x xs ih =>
    cases xs with
    | nil =>
      have hxs : apairs [x] = ([] : List (α × α)) := rfl
      show apairs (x :: v) = apairs [x] ++ bpair [x] v ++ apairs v
      rw [apairs_cons, bpair_singleton, hxs]
      simp
    | cons y ys =>
      rw [List.cons_append, apairs_cons, ih]
      rw [show cpair x ((y :: ys) ++ v) = [(x, y)] from rfl]
      rw [apairs_cons x (y :: ys), bpair_cons_cons]
      rw [show cpair x (y :: ys) = [(x, y)] from rfl]
      simp only [apairs_cons, List.append_assoc]

private theorem sessions_eq_countP {gap : Nat} {xs : List Nat} (h : xs ≠ []) :
    sessions gap xs = 1 + List.countP (fun p => match p with | (a, b) => decide (a + gap < b)) (apairs xs) := by
  cases xs with
  | nil => exact absurd rfl h
  | cons x xs' => rfl

private theorem countP_boundary_singleton {gap s y : Nat} :
    List.countP (fun p => match p with | (a, b) => decide (a + gap < b)) [(s, y)] +
      (decide (y ≤ s + gap)).toNat = 1 := by
  rw [List.countP_singleton]
  by_cases h : s + gap < y
  · have h2 : ¬ y ≤ s + gap := by omega
    simp [h, h2]
  · have h2 : y ≤ s + gap := by omega
    simp [h, h2]

/-- Only the boundary pair can change the session count when concatenating. -/
theorem sound_SessionBoundary : SessionBoundary.Goal := by
  intro gap a b
  cases a with
  | nil => simp [sessions]
  | cons x as =>
    cases b with
    | nil => simp [sessions]
    | cons y ys =>
      have hne1 : ((x :: as) ++ (y :: ys)) ≠ [] := by simp
      have hne2 : (x :: as) ≠ [] := by simp
      have hne3 : (y :: ys) ≠ [] := by simp
      rw [sessions_eq_countP hne1, sessions_eq_countP hne2, sessions_eq_countP hne3]
      rw [apairs_append, List.countP_append, List.countP_append]
      have hbpair : bpair (x :: as) (y :: ys) = [((as.getLast?).getD x, y)] := by
        simp only [bpair]
        rw [List.getLast?_cons]
        simp only [List.head?]
      rw [hbpair]
      rw [List.getLast?_cons]
      simp only [List.head?]
      generalize hB : List.countP (fun p => match p with | (a, b) => decide (a + gap < b))
          [((as.getLast?).getD x, y)] = B
      generalize hC : (decide (y ≤ (as.getLast?).getD x + gap)).toNat = C
      have key : B + C = 1 := by
        rw [← hB, ← hC]
        exact countP_boundary_singleton (gap := gap) (s := (as.getLast?).getD x) (y := y)
      omega

/-! ### Prefix nullable sums

`sums xs` is the `scanl` of `(non-null count, sum)` pairs. The `l`-th and `r`-th
prefix entries differ exactly by the frame's non-null count and value sum; the
count difference is `0` iff `filterMap id` of the frame is empty. -/

private def stepF (q : Nat × Int) (x : Option Int) : Nat × Int :=
  (q.1 + x.isSome.toNat, q.2 + x.getD 0)

private theorem stepF_none (q : Nat × Int) : stepF q none = q := by
  apply Prod.ext <;> simp [stepF] <;> omega

private theorem stepF_some (q : Nat × Int) (v : Int) : stepF q (some v) = (q.1 + 1, q.2 + v) := by
  apply Prod.ext <;> simp [stepF] <;> omega

private theorem foldl_stepF {xs : List (Option Int)} (q : Nat × Int) :
    xs.foldl stepF q =
      (q.1 + xs.countP (·.isSome), q.2 + (xs.map (·.getD 0)).sum) := by
  induction xs generalizing q with
  | nil => simp [stepF]
  | cons x xs ih =>
    rw [List.foldl_cons, ih]
    cases x with
    | none =>
      rw [stepF_none]
      apply Prod.ext
      · rw [List.countP_cons_of_neg (by simp [Option.isSome_none])]
      · simp [List.map_cons, List.sum_cons, Option.getD_none]
    | some v =>
      rw [stepF_some]
      apply Prod.ext
      · rw [List.countP_cons_of_pos (by simp [Option.isSome_some])]
        omega
      · simp [List.map_cons, List.sum_cons, Option.getD_some]
        omega

private theorem foldl_pattern_eq_stepF (xs : List (Option Int)) (q : Nat × Int) :
    xs.foldl (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) q =
      xs.foldl stepF q := by
  have hfun : (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) = stepF := by
    funext q x; cases q with | mk n s => rfl
  rw [hfun]

private theorem foldl_pattern (xs : List (Option Int)) (q : Nat × Int) :
    xs.foldl (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) q =
      (q.1 + xs.countP (·.isSome), q.2 + (xs.map (·.getD 0)).sum) := by
  rw [foldl_pattern_eq_stepF, foldl_stepF]

private theorem filterMap_id_sum (xs : List (Option Int)) :
    (xs.filterMap id).sum = (xs.map (·.getD 0)).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    cases x with
    | none => simp [List.map_cons, List.sum_cons, ih]
    | some v => simp [List.map_cons, List.sum_cons, ih]

private theorem filterMap_id_isEmpty_iff (xs : List (Option Int)) :
    (xs.filterMap id).isEmpty = true ↔ xs.countP (·.isSome) = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    cases x with
    | none =>
      have hfm : List.filterMap id (none :: xs) = List.filterMap id xs := by
        simp
      have hcp : List.countP (·.isSome) (none :: xs) = List.countP (·.isSome) xs :=
        List.countP_cons_of_neg (by simp [Option.isSome_none])
      rw [hfm, hcp]
      exact ih
    | some v =>
      have hfm : List.filterMap id (some v :: xs) = v :: List.filterMap id xs := by
        simp
      have hcp : List.countP (·.isSome) (some v :: xs) = List.countP (·.isSome) xs + 1 :=
        List.countP_cons_of_pos (by simp [Option.isSome_some])
      rw [hfm, hcp]
      exact iff_of_false (by simp [List.isEmpty_cons]) (by omega)

/-- Prefix `(count, sum)` pairs distinguish "no non-null values" from "sum is 0". -/
theorem sound_PrefixNullableSum : PrefixNullableSum.Goal := by
  intro xs l r hlr hr
  have hlen_l : l ≤ xs.length := Nat.le_trans hlr hr
  have ha1 : ((sums xs)[l]?.getD (0,0)).1 = (xs.take l).countP (·.isSome) := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hlen_l,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have ha2 : ((sums xs)[l]?.getD (0,0)).2 = ((xs.take l).map (·.getD 0)).sum := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hlen_l,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have hb1 : ((sums xs)[r]?.getD (0,0)).1 = (xs.take r).countP (·.isSome) := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hr,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have hb2 : ((sums xs)[r]?.getD (0,0)).2 = ((xs.take r).map (·.getD 0)).sum := by
    unfold sums
    rw [List.getElem?_toArray, List.getElem?_scanl, ite_eq_left hr,
      Option.getD_some, foldl_pattern]
    simp [Nat.zero_add, Int.zero_add]
  have htake : xs.take r = xs.take l ++ frame xs l r := by
    have h1 : xs.take r = List.take l (xs.take r) ++ List.drop l (xs.take r) :=
      (List.take_append_drop l (xs.take r)).symm
    rw [h1, List.take_take, Nat.min_eq_left hlr]
    rfl
  have hcntR : (xs.take r).countP (·.isSome) =
      (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome) := by
    rw [htake, List.countP_append]
  have hsmR : ((xs.take r).map (·.getD 0)).sum =
      ((xs.take l).map (·.getD 0)).sum + ((frame xs l r).map (·.getD 0)).sum := by
    rw [htake, List.map_append, List.sum_append]
  show (if (((sums xs)[l]?.getD (0,0)).1 == ((sums xs)[r]?.getD (0,0)).1) = true
        then none
        else some (((sums xs)[r]?.getD (0,0)).2 - ((sums xs)[l]?.getD (0,0)).2)) =
       (if ((frame xs l r).filterMap id).isEmpty = true then none
        else some ((frame xs l r).filterMap id).sum)
  rw [ha1, ha2, hb1, hb2, hcntR, hsmR, filterMap_id_sum]
  by_cases hF : (frame xs l r).countP (·.isSome) = 0
  · have hLc : ((xs.take l).countP (·.isSome) ==
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome)) = true := by
      apply beq_iff_eq.mpr
      rw [hF, Nat.add_zero]
    have hRc : ((frame xs l r).filterMap id).isEmpty = true :=
      (filterMap_id_isEmpty_iff (frame xs l r)).mpr hF
    rw [ite_eq_left hLc, ite_eq_left hRc]
  · have hne : (xs.take l).countP (·.isSome) ≠
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome) := by
      intro h
      apply hF
      exact Nat.add_eq_left.mp h.symm
    have hLc : ¬(((xs.take l).countP (·.isSome) ==
        (xs.take l).countP (·.isSome) + (frame xs l r).countP (·.isSome)) = true) := by
      rw [beq_iff_eq]
      exact hne
    have hRc : ¬(((frame xs l r).filterMap id).isEmpty = true) := by
      rw [filterMap_id_isEmpty_iff]
      exact hF
    rw [ite_eq_right hLc, ite_eq_right hRc]
    apply congrArg
    omega



private theorem frame_eq_drop_take {α : Type} (xs : List α) (l r : Nat) :
    frame xs l r = (xs.drop l).take (r - l) := by
  unfold frame
  rw [List.drop_take]

private theorem frame_append {α : Type} (xs : List α) {a b c : Nat} (hab : a ≤ b) (hbc : b ≤ c) :
    frame xs a c = frame xs a b ++ frame xs b c := by
  have hca : c - a = (b - a) + (c - b) := by omega
  have hmid : List.drop (b - a) (List.drop a xs) = List.drop b xs := by
    rw [List.drop_drop, Nat.add_sub_of_le hab]
  unfold frame
  rw [List.drop_take, List.drop_take, List.drop_take, hca, List.take_add, hmid]

private theorem frame_ne_nil {α : Type} (xs : List α) {a b : Nat} (hab : a < b) (hb : b ≤ xs.length) :
    frame xs a b ≠ [] := by
  intro h
  have hlen : (frame xs a b).length = b - a := by
    rw [frame_eq_drop_take, List.length_take, List.length_drop,
      Nat.min_eq_left (by omega : b - a ≤ xs.length - a)]
  rw [h] at hlen
  simp at hlen
  omega

private theorem min?_append_some {l1 l2 : List Int} {a b : Int}
    (h1 : l1.min? = some a) (h2 : l2.min? = some b) :
    (l1 ++ l2).min? = some (min a b) := by
  rw [List.min?_eq_some_iff]
  obtain ⟨ha_mem, ha_le⟩ := List.min?_eq_some_iff.mp h1
  obtain ⟨hb_mem, hb_le⟩ := List.min?_eq_some_iff.mp h2
  constructor
  · by_cases hle : a ≤ b
    · have : min a b = a := Int.min_eq_left hle
      rw [this]
      exact List.mem_append.mpr (Or.inl ha_mem)
    · have hba : b ≤ a := Int.le_of_lt (Int.not_le.mp hle)
      have : min a b = b := Int.min_eq_right hba
      rw [this]
      exact List.mem_append.mpr (Or.inr hb_mem)
  · intro c hc
    rw [List.mem_append] at hc
    cases hc with
    | inl hcl =>
      exact Int.le_trans (Int.min_le_left a b) (ha_le c hcl)
    | inr hcl =>
      exact Int.le_trans (Int.min_le_right a b) (hb_le c hcl)

theorem sound_SparseMinimum : SparseMinimum.Goal := by
  intro xs l r hlr hr
  show min ((frame xs l (l + 2 ^ (r - l).log2)).min?.getD 0)
      ((frame xs (r - 2 ^ (r - l).log2) r).min?.getD 0) =
    (frame xs l r).min?.getD 0
  have hn : 0 < r - l := Nat.sub_pos_of_lt hlr
  have hw_le : 2 ^ (r - l).log2 ≤ r - l := Nat.log2_self_le (Nat.ne_of_gt hn)
  have hw_lt : r - l < 2 * 2 ^ (r - l).log2 := by
    have h := Nat.lt_log2_self (n := r - l)
    rwa [Nat.pow_add_one'] at h
  have hw1 : 1 ≤ 2 ^ (r - l).log2 := Nat.one_le_two_pow
  have hlw : l + 2 ^ (r - l).log2 ≤ r := by omega
  have hrw : l ≤ r - 2 ^ (r - l).log2 := by omega
  have hrwl : r - 2 ^ (r - l).log2 ≤ l + 2 ^ (r - l).log2 := by omega
  have hrwl' : r - 2 ^ (r - l).log2 < l + 2 ^ (r - l).log2 := by omega
  by_cases hw_eq : 2 ^ (r - l).log2 = r - l
  · have hlweq : l + 2 ^ (r - l).log2 = r := by omega
    have hrweq : r - 2 ^ (r - l).log2 = l := by omega
    rw [hlweq, hrweq]
    exact Int.min_self _
  · have hwlt : 2 ^ (r - l).log2 < r - l := Nat.lt_of_le_of_ne hw_le hw_eq
    have hlwr : l + 2 ^ (r - l).log2 < r := by omega
    have hlr' : l < r - 2 ^ (r - l).log2 := by omega
    have hPne : frame xs l (r - 2 ^ (r - l).log2) ≠ [] :=
      frame_ne_nil xs hlr' (by omega)
    have hQne : frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2) ≠ [] :=
      frame_ne_nil xs hrwl' (by omega)
    have hRne : frame xs (l + 2 ^ (r - l).log2) r ≠ [] :=
      frame_ne_nil xs hlwr hr
    have hW1ne : frame xs l (l + 2 ^ (r - l).log2) ≠ [] :=
      frame_ne_nil xs (by omega) (by omega)
    have hW2ne : frame xs (r - 2 ^ (r - l).log2) r ≠ [] :=
      frame_ne_nil xs (by omega) hr
    have hFne : frame xs l r ≠ [] := frame_ne_nil xs hlr hr
    have hP : (frame xs l (r - 2 ^ (r - l).log2)).min? =
        some ((frame xs l (r - 2 ^ (r - l).log2)).min hPne) :=
      List.min?_eq_some_min hPne
    have hQ : (frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2)).min? =
        some ((frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2)).min hQne) :=
      List.min?_eq_some_min hQne
    have hR : (frame xs (l + 2 ^ (r - l).log2) r).min? =
        some ((frame xs (l + 2 ^ (r - l).log2) r).min hRne) :=
      List.min?_eq_some_min hRne
    have hW1eq : frame xs l (l + 2 ^ (r - l).log2) =
        frame xs l (r - 2 ^ (r - l).log2) ++ frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2) :=
      frame_append xs hrw hrwl
    have hW2eq : frame xs (r - 2 ^ (r - l).log2) r =
        frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2) ++ frame xs (l + 2 ^ (r - l).log2) r :=
      frame_append xs hrwl hlw
    have hFeq : frame xs l r =
        frame xs l (r - 2 ^ (r - l).log2) ++ frame xs (r - 2 ^ (r - l).log2) r :=
      frame_append xs hrw (by omega)
    have hW1 : (frame xs l (l + 2 ^ (r - l).log2)).min? =
        some (min ((frame xs l (r - 2 ^ (r - l).log2)).min hPne)
          ((frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2)).min hQne)) := by
      rw [hW1eq]
      exact min?_append_some hP hQ
    have hW2 : (frame xs (r - 2 ^ (r - l).log2) r).min? =
        some (min ((frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2)).min hQne)
          ((frame xs (l + 2 ^ (r - l).log2) r).min hRne)) := by
      rw [hW2eq]
      exact min?_append_some hQ hR
    have hF : (frame xs l r).min? =
        some (min ((frame xs l (r - 2 ^ (r - l).log2)).min hPne)
          (min ((frame xs (r - 2 ^ (r - l).log2) (l + 2 ^ (r - l).log2)).min hQne)
            ((frame xs (l + 2 ^ (r - l).log2) r).min hRne))) := by
      rw [hFeq]
      exact min?_append_some hP hW2
    rw [hW1, hW2, hF]
    simp only [Option.getD_some]
    omega

private def rrf (st : Nat × List (List (Nat × Nat))) (p : Int × Nat) :
    Nat × List (List (Nat × Nat)) :=
  (st.1 + p.2, List.replicate p.2 (st.1 + 1, st.1 + p.2) :: st.2)

private def rankRunsFrom (n0 : Nat) (runs : List (Int × Nat)) : List (Nat × Nat) :=
  (runs.foldl rrf (n0, [])).2.reverse.flatten

private def xsOf (runs : List (Int × Nat)) : List Int :=
  runs.flatMap fun (k,m) => List.replicate m k

private theorem foldl_eq (init : Nat × List (List (Nat × Nat))) (runs : List (Int × Nat)) :
    runs.foldl (fun (n,out) (_,m) => (n+m, List.replicate m (n+1,n+m) :: out)) init =
    runs.foldl rrf init := by
  induction runs generalizing init with
  | nil => rfl
  | cons p rest ih =>
    obtain ⟨n, out⟩ := init
    obtain ⟨k, m⟩ := p
    rw [List.foldl_cons, List.foldl_cons]
    exact ih _

private theorem rankRuns_eq_rankRunsFrom (runs : List (Int × Nat)) :
    rankRuns runs = rankRunsFrom 0 runs := by
  have h := foldl_eq (0, []) runs
  unfold rankRuns rankRunsFrom
  rw [h]

private theorem foldl_snd_append (n : Nat) (out0 : List (List (Nat × Nat)))
    (rest : List (Int × Nat)) :
    (rest.foldl rrf (n, out0)).2 = (rest.foldl rrf (n, [])).2 ++ out0 := by
  induction rest generalizing n out0 with
  | nil => rfl
  | cons p rest ih =>
    rw [List.foldl_cons, List.foldl_cons]
    have h1 : rrf (n, out0) p = (n + p.2, List.replicate p.2 (n + 1, n + p.2) :: out0) := rfl
    have h2 : rrf (n, []) p = (n + p.2, [List.replicate p.2 (n + 1, n + p.2)]) := rfl
    rw [h1, h2]
    rw [ih (n + p.2) (List.replicate p.2 (n + 1, n + p.2) :: out0),
        ih (n + p.2) [List.replicate p.2 (n + 1, n + p.2)]]
    simp only [List.cons_append, List.nil_append, List.append_assoc]

private theorem rankRunsFrom_cons (n : Nat) (k : Int) (m : Nat) (rest : List (Int × Nat)) :
    rankRunsFrom n ((k,m) :: rest) =
      List.replicate m (n+1, n+m) ++ rankRunsFrom (n+m) rest := by
  unfold rankRunsFrom
  rw [List.foldl_cons]
  have hstep : rrf (n, []) (k,m) = (n + m, [List.replicate m (n+1, n+m)]) := rfl
  rw [hstep, foldl_snd_append, List.reverse_append]
  simp only [List.reverse_cons, List.reverse_nil, List.nil_append, List.cons_append]
  rw [List.flatten_cons]

private theorem mem_xsOf_gt (k : Int) (rest : List (Int × Nat))
    (h : ∀ b ∈ rest.map Prod.fst, k < b) :
    ∀ y ∈ xsOf rest, k < y := by
  intro y hy
  rw [xsOf, List.mem_flatMap] at hy
  obtain ⟨⟨k', m'⟩, hmem, hyrep⟩ := hy
  rw [List.mem_replicate] at hyrep
  obtain ⟨-, hyk⟩ := hyrep
  rw [hyk]
  apply h k'
  rw [List.mem_map]
  exact ⟨⟨k', m'⟩, hmem, rfl⟩

private theorem filter_lt_append (m : Nat) (k x : Int) (B : List Int)
    (hx : k < x) :
    (List.replicate m k ++ B).filter (· < x) =
      List.replicate m k ++ B.filter (· < x) := by
  rw [List.filter_append, List.filter_replicate]
  have hdec : decide (k < x) = true := decide_eq_true hx
  rw [hdec]
  simp

private theorem filter_le_append (m : Nat) (k x : Int) (B : List Int)
    (hx : k ≤ x) :
    (List.replicate m k ++ B).filter (· ≤ x) =
      List.replicate m k ++ B.filter (· ≤ x) := by
  rw [List.filter_append, List.filter_replicate]
  have hdec : decide (k ≤ x) = true := decide_eq_true hx
  rw [hdec]
  simp

-- the x = k filter lengths
private theorem filter_lt_k_len (m : Nat) (k : Int) (B : List Int)
    (hB : ∀ y ∈ B, k < y) :
    ((List.replicate m k ++ B).filter (· < k)).length = 0 := by
  rw [List.filter_append, List.length_append]
  have e1 : (List.replicate m k).filter (· < k) = [] := by
    rw [List.filter_replicate]
    have hd : decide (k < k) = false := decide_eq_false (Int.lt_irrefl k)
    rw [hd]
    simp
  have e2 : B.filter (· < k) = [] := by
    rw [List.filter_eq_nil_iff]
    intro a ha
    have hak : k < a := hB a ha
    have hna : ¬ a < k := Int.lt_asymm hak
    rw [decide_eq_false hna]
    simp
  rw [e1, e2]
  simp

private theorem filter_le_k_len (m : Nat) (k : Int) (B : List Int)
    (hB : ∀ y ∈ B, k < y) :
    ((List.replicate m k ++ B).filter (· ≤ k)).length = m := by
  rw [List.filter_append, List.length_append]
  have e1 : (List.replicate m k).filter (· ≤ k) = List.replicate m k := by
    rw [List.filter_replicate]
    have hd : decide (k ≤ k) = true := decide_eq_true (Int.le_refl k)
    rw [hd]
    simp
  have e2 : B.filter (· ≤ k) = [] := by
    rw [List.filter_eq_nil_iff]
    intro a ha
    have hak : k < a := hB a ha
    have hna : ¬ a ≤ k := Int.not_le.mpr hak
    rw [decide_eq_false hna]
    simp
  rw [e1, e2]
  simp [List.length_replicate]



private theorem rankRunsFrom_spec (n : Nat) (runs : List (Int × Nat))
    (h : (runs.map Prod.fst).Pairwise (· < ·)) :
    rankRunsFrom n runs =
      (xsOf runs).map fun x =>
        (n + 1 + ((xsOf runs).filter (· < x)).length,
         n + ((xsOf runs).filter (· ≤ x)).length) := by
  induction runs generalizing n with
  | nil => rfl
  | cons p rest ih =>
    obtain ⟨k, m⟩ := p
    rw [List.map_cons, List.pairwise_cons] at h
    obtain ⟨hhead, htail⟩ := h
    have hgt : ∀ y ∈ xsOf rest, k < y := mem_xsOf_gt k rest hhead
    have hxs : xsOf ((k,m) :: rest) = List.replicate m k ++ xsOf rest := rfl
    rw [rankRunsFrom_cons, hxs, List.map_append, List.map_replicate]
    have hspeck :
        (n + 1 + ((List.replicate m k ++ xsOf rest).filter (· < k)).length,
         n + ((List.replicate m k ++ xsOf rest).filter (· ≤ k)).length) =
        (n+1, n+m) := by
      have h1 := filter_lt_k_len m k (xsOf rest) hgt
      have h2 := filter_le_k_len m k (xsOf rest) hgt
      rw [h1, h2]
    have hmap :
        (xsOf rest).map (fun x =>
          (n + 1 + ((List.replicate m k ++ xsOf rest).filter (· < x)).length,
           n + ((List.replicate m k ++ xsOf rest).filter (· ≤ x)).length)) =
        rankRunsFrom (n+m) rest := by
      rw [ih (n+m) htail]
      apply List.map_congr_left
      intro x hx
      have hkx : k < x := hgt x hx
      have hlt : ((List.replicate m k ++ xsOf rest).filter (· < x)).length =
                 m + ((xsOf rest).filter (· < x)).length := by
        rw [filter_lt_append m k x (xsOf rest) hkx, List.length_append, List.length_replicate]
      have hle : ((List.replicate m k ++ xsOf rest).filter (· ≤ x)).length =
                 m + ((xsOf rest).filter (· ≤ x)).length := by
        rw [filter_le_append m k x (xsOf rest) (Int.le_of_lt hkx), List.length_append, List.length_replicate]
      rw [hlt, hle]
      exact Prod.ext_iff.mpr ⟨by omega, by omega⟩
    rw [hspeck, hmap]



theorem sound_PeerRankRuns : PeerRankRuns.Goal := by
  intro runs hpair
  intro xs
  have hxs : xs = xsOf runs := rfl
  rw [hxs, rankRuns_eq_rankRunsFrom, rankRunsFrom_spec 0 runs hpair]
  apply List.map_congr_left
  intro x hx
  exact Prod.ext_iff.mpr ⟨by omega, by omega⟩

private def szf (n m j : Nat) : Nat := n/m + (decide (j < n%m)).toNat

private def blkPref (s : Nat → Nat) (b : Nat) : Nat := ((List.range b).map s).sum

private theorem blkPref_succ (s : Nat → Nat) (b : Nat) :
    blkPref s (b+1) = blkPref s b + s b := by
  show ((List.range (b+1)).map s).sum = ((List.range b).map s).sum + s b
  rw [List.range_succ, List.map_append, List.sum_append]
  have hm : List.map s [b] = [s b] := by simp
  rw [hm]
  simp

private theorem blkPref_mono (s : Nat → Nat) {a b : Nat} (h : a ≤ b) :
    blkPref s a ≤ blkPref s b := by
  obtain ⟨c, rfl⟩ := Nat.exists_eq_add_of_le h
  induction c with
  | zero => simp [blkPref]
  | succ c ih =>
    have : a + (c+1) = (a+c)+1 := by omega
    rw [this, blkPref_succ]
    omega

private theorem min_succ (b r : Nat) :
    min (b+1) r = min b r + (decide (b < r)).toNat := by
  by_cases hb : b < r
  · have hbr : b ≤ r := Nat.le_of_lt hb
    have hb1r : b + 1 ≤ r := Nat.lt_iff_add_one_le.mp hb
    rw [Nat.min_eq_left hb1r, Nat.min_eq_left hbr, decide_eq_true hb]
    simp
  · have hge : r ≤ b := Nat.le_of_not_lt hb
    have hrb1 : r ≤ b + 1 := by omega
    rw [Nat.min_eq_right hrb1, Nat.min_eq_right hge, decide_eq_false hb]
    simp

private theorem blkPref_sizes (n m b : Nat) :
    blkPref (fun i => n/m + (decide (i < n%m)).toNat) b =
      b * (n/m) + min b (n%m) := by
  induction b with
  | zero => simp [blkPref]
  | succ b ih =>
    rw [blkPref_succ, ih]
    show b * (n/m) + min b (n%m) + (n/m + (decide (b < n%m)).toNat) =
         (b+1) * (n/m) + min (b+1) (n%m)
    rw [min_succ, Nat.succ_mul]
    omega

private theorem blkPref_szf (n m b : Nat) :
    blkPref (szf n m) b = b * (n/m) + min b (n%m) := by
  show blkPref (fun j => n/m + (decide (j < n%m)).toNat) b = _
  exact blkPref_sizes n m b

private theorem szf_eq (n m j : Nat) : szf n m j = n/m + (decide (j < n%m)).toNat := rfl

private theorem flatMap_replicate_length (s : Nat → Nat) (m : Nat) :
    ((List.range m).flatMap fun b => List.replicate (s b) (b+1)).length = blkPref s m := by
  rw [List.length_flatMap]
  have hmap : List.map (fun b => (List.replicate (s b) (b+1)).length) (List.range m) =
             List.map s (List.range m) := by
    apply List.map_congr_left
    intro b hb
    rw [List.length_replicate]
  rw [hmap]
  rfl

private theorem getElem?_flatMap_replicate (s : Nat → Nat) (m i v : Nat)
    (h : ∃ b, b < m ∧ blkPref s b ≤ i ∧ i < blkPref s b + s b ∧ v = b + 1) :
    ((List.range m).flatMap fun b => List.replicate (s b) (b+1))[i]? = some v := by
  induction m with
  | zero =>
    obtain ⟨b, hb, -, -, -⟩ := h
    omega
  | succ m ih =>
    obtain ⟨b, hb, hP1, hP2, hv⟩ := h
    rw [List.range_succ, List.flatMap_append]
    have hlen : ((List.range m).flatMap fun b => List.replicate (s b) (b+1)).length = blkPref s m :=
      flatMap_replicate_length s m
    rw [List.getElem?_append, hlen]
    by_cases hbm : b < m
    · have hi : i < blkPref s m := by
        have h2 : blkPref s (b+1) ≤ blkPref s m := blkPref_mono s (by omega)
        rw [blkPref_succ] at h2
        omega
      rw [if_pos hi]
      exact ih ⟨b, hbm, hP1, hP2, hv⟩
    · have hbeq : b = m := by omega
      rw [hbeq] at hP1 hP2 hv
      have hi : ¬ i < blkPref s m := by omega
      rw [if_neg hi]
      have hfm : List.flatMap (fun b => List.replicate (s b) (b+1)) [m] =
                 List.replicate (s m) (m+1) := by
        rw [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [hfm, List.getElem?_replicate]
      have hsub : i - blkPref s m < s m := by omega
      rw [if_pos hsub, hv]

private theorem tile_of_lt (n m i : Nat) (h : i < n%m*(n/m+1)) :
    tile n m i = i/(n/m+1)+1 := by
  show (if i < n%m*(n/m+1) then i/(n/m+1)+1 else n%m+(i-n%m*(n/m+1))/(n/m)+1) = i/(n/m+1)+1
  exact ite_eq_left h

private theorem tile_of_ge (n m i : Nat) (h : ¬ i < n%m*(n/m+1)) :
    tile n m i = n%m+(i-n%m*(n/m+1))/(n/m)+1 := by
  show (if i < n%m*(n/m+1) then i/(n/m+1)+1 else n%m+(i-n%m*(n/m+1))/(n/m)+1) = n%m+(i-n%m*(n/m+1))/(n/m)+1
  exact ite_eq_right h



theorem sound_TileArithmetic : TileArithmetic.Goal := by
  intro n m i hm hi
  symm
  show ((List.range m).flatMap fun j => List.replicate (n/m + (decide (j < n%m)).toNat) (j+1))[i]? = some (tile n m i)
  apply getElem?_flatMap_replicate (szf n m) m i (tile n m i)
  by_cases hbig : i < n%m * (n/m + 1)
  · -- big buckets: position i lies in bucket i/(n/m+1)
    have hq1 : 0 < n/m + 1 := Nat.succ_pos _
    have hrm : n%m < m := Nat.mod_lt _ hm
    have hb_lt_r : i / (n/m + 1) < n%m := (Nat.div_lt_iff_lt_mul hq1).mpr hbig
    have hb1_le_r : i / (n/m + 1) + 1 ≤ n%m := Nat.lt_iff_add_one_le.mp hb_lt_r
    refine ⟨i / (n/m + 1), ?_, ?_, ?_, tile_of_lt n m i hbig⟩
    · -- b < m
      have him : i < m * (n/m + 1) := by
        have hle : n%m * (n/m + 1) ≤ m * (n/m + 1) :=
          Nat.mul_le_mul_right (n/m + 1) (Nat.le_of_lt hrm)
        exact Nat.lt_of_lt_of_le hbig hle
      exact (Nat.div_lt_iff_lt_mul hq1).mpr him
    · -- blkPref s b ≤ i
      have hP : blkPref (szf n m) (i / (n/m + 1)) = i / (n/m + 1) * (n/m + 1) := by
        rw [blkPref_szf, Nat.min_eq_left (Nat.le_of_lt hb_lt_r)]
        rw [Nat.mul_add, Nat.mul_one]
      rw [hP]
      exact Nat.div_mul_le_self i (n/m + 1)
    · -- i < blkPref s b + s b
      have hPs : blkPref (szf n m) (i / (n/m + 1)) + szf n m (i / (n/m + 1)) =
          (i / (n/m + 1) + 1) * (n/m + 1) := by
        rw [← blkPref_succ, blkPref_szf, Nat.min_eq_left hb1_le_r]
        rw [Nat.mul_add, Nat.mul_one]
      rw [hPs]
      exact (Nat.div_lt_iff_lt_mul hq1).mp (by omega)
  · -- small buckets
    have hbig' : n%m * (n/m + 1) ≤ i := Nat.le_of_not_lt hbig
    have hrm : n%m < m := Nat.mod_lt _ hm
    have hq : 0 < n/m := by
      by_cases hq0 : n/m = 0
      · have hnm : n < m := by
          cases (Nat.div_eq_zero_iff).mp hq0 with
          | inl h => omega
          | inr h => exact h
        have hrn : n%m = n := Nat.mod_eq_of_lt hnm
        rw [hrn, hq0] at hbig'
        simp at hbig'
        omega
      · exact Nat.pos_of_ne_zero hq0
    refine ⟨n%m + (i - n%m*(n/m+1))/(n/m), ?_, ?_, ?_, tile_of_ge n m i hbig⟩
    · -- b < m
      have ht_lt : (i - n%m*(n/m+1))/(n/m) < m - n%m := by
        apply (Nat.div_lt_iff_lt_mul hq).mpr
        have hnB : n - n%m*(n/m+1) = (m - n%m)*(n/m) := by
          have hdiv : m * (n/m) + n%m = n := Nat.div_add_mod n m
          have hB' : n%m * (n/m + 1) = n%m * (n/m) + n%m := by
            rw [Nat.mul_add, Nat.mul_one]
          have hsub : (m - n%m) * (n/m) = m*(n/m) - n%m*(n/m) := Nat.sub_mul _ _ _
          rw [hB', hsub]
          have hle : n%m * (n/m) ≤ m * (n/m) :=
            Nat.mul_le_mul_right (n/m) (Nat.le_of_lt hrm)
          omega
        have hiB : i - n%m*(n/m+1) < n - n%m*(n/m+1) := by omega
        exact Nat.lt_of_lt_of_eq hiB hnB
      omega
    · -- blkPref s b ≤ i
      have hP : blkPref (szf n m) (n%m + (i - n%m*(n/m+1))/(n/m)) =
          (n%m + (i - n%m*(n/m+1))/(n/m)) * (n/m) + n%m := by
        rw [blkPref_szf]
        have hmin : min (n%m + (i - n%m*(n/m+1))/(n/m)) (n%m) = n%m :=
          Nat.min_eq_right (Nat.le_add_right _ _)
        rw [hmin]
      rw [hP, Nat.add_mul]
      have hB' : n%m * (n/m) + n%m = n%m * (n/m + 1) := by
        rw [Nat.mul_add, Nat.mul_one]
      have htq : ((i - n%m*(n/m+1))/(n/m)) * (n/m) ≤ i - n%m*(n/m+1) :=
        Nat.div_mul_le_self _ _
      have hadd : n%m*(n/m+1) + (i - n%m*(n/m+1)) = i := Nat.add_sub_of_le hbig'
      omega
    · -- i < blkPref s b + s b
      have hPs : blkPref (szf n m) (n%m + (i - n%m*(n/m+1))/(n/m)) +
            szf n m (n%m + (i - n%m*(n/m+1))/(n/m)) =
          n%m*(n/m+1) + ((i - n%m*(n/m+1))/(n/m) + 1)*(n/m) := by
        rw [← blkPref_succ, blkPref_szf]
        have hmin : min (n%m + (i - n%m*(n/m+1))/(n/m) + 1) (n%m) = n%m :=
          Nat.min_eq_right (Nat.le_trans (Nat.le_add_right _ _) (Nat.le_add_right _ _))
        rw [hmin]
        simp only [Nat.add_mul, Nat.mul_add, Nat.one_mul, Nat.mul_one]
        omega
      rw [hPs]
      have h2 : (i - n%m*(n/m+1))/(n/m) < (i - n%m*(n/m+1))/(n/m) + 1 := by omega
      have hlt : i - n%m*(n/m+1) < ((i - n%m*(n/m+1))/(n/m) + 1)*(n/m) :=
        (Nat.div_lt_iff_lt_mul hq).mp h2
      have hadd : n%m*(n/m+1) + (i - n%m*(n/m+1)) = i := Nat.add_sub_of_le hbig'
      omega

private theorem pow2_pos (n : Nat) : 0 < 2^n := Nat.pow_pos (by decide : (0:Nat) < 2)

private theorem testBit_zero' (i : Nat) : (0 : Nat).testBit i = false :=
  Nat.testBit_lt_two_pow (pow2_pos i)

private theorem xor_ne_zero {l r : Nat} (h : l < r - 1) : l ^^^ (r-1) ≠ 0 := by
  intro hx
  have hbits : ∀ i, l.testBit i = (r-1).testBit i := by
    intro i
    have hxi := congrArg (fun n => n.testBit i) hx
    rw [Nat.testBit_xor, testBit_zero'] at hxi
    cases a : l.testBit i <;> cases b : (r-1).testBit i <;> simp_all [a, b]
  have heq : l = r - 1 := Nat.eq_of_testBit_eq hbits
  omega

private theorem log2_bit_facts {x : Nat} (hx : x ≠ 0) :
    x.testBit x.log2 = true ∧ ∀ i, x.log2 < i → x.testBit i = false := by
  have hge : 2^(x.log2) ≤ x := by
    apply Nat.le_of_not_lt
    intro hlt
    have := (Nat.log2_lt hx).mpr hlt
    exact absurd this (Nat.lt_irrefl _)
  have hlt1 : x < 2^(x.log2 + 1) := (Nat.log2_lt hx).mp (by omega : x.log2 < x.log2 + 1)
  have hdiv1 : 1 ≤ x / 2^(x.log2) := by
    apply (Nat.le_div_iff_mul_le (pow2_pos _)).mpr
    rw [Nat.one_mul]; exact hge
  have hdiv2 : x / 2^(x.log2) < 2 := by
    apply (Nat.div_lt_iff_lt_mul (pow2_pos _)).mpr
    have h2 : 2 * 2^(x.log2) = 2^(x.log2 + 1) := by rw [Nat.pow_succ, Nat.mul_comm]
    rw [h2]; exact hlt1
  have hdiv : x / 2^(x.log2) = 1 := by omega
  have htb : x.testBit x.log2 = (x / 2^(x.log2)).testBit 0 := by
    have h := Nat.testBit_add x 0 (x.log2)
    rw [Nat.zero_add] at h
    exact h
  rw [htb, hdiv]
  constructor
  · show (2^0).testBit 0 = true
    rw [Nat.testBit_two_pow]
    rfl
  · intro i hi
    have h2i : 2^(x.log2 + 1) ≤ 2^i := Nat.pow_le_pow_right (by decide : (0:Nat) < 2) (by omega)
    have hxi : x < 2^i := Nat.lt_of_lt_of_le hlt1 h2i
    exact Nat.testBit_lt_two_pow hxi

-- a.testBit p = true → 2^p ≤ a
private theorem tb_ge {a p : Nat} (h : a.testBit p = true) : 2^p ≤ a := by
  have htb := Nat.testBit_add a 0 p
  rw [Nat.zero_add] at htb
  have h0 : (a/2^p).testBit 0 = true := htb ▸ h
  rw [Nat.testBit_zero] at h0
  have hmod : (a/2^p) % 2 = 1 := of_decide_eq_true h0
  have hne : a/2^p ≠ 0 := by
    intro hz
    rw [hz] at hmod
    simp at hmod
  have hpos : 0 < a/2^p := Nat.pos_of_ne_zero hne
  have := (Nat.le_div_iff_mul_le (pow2_pos p)).mp hpos
  rwa [Nat.one_mul] at this

-- a < 2^(p+1) → a.testBit p = false → a < 2^p
private theorem tb_lt {a p : Nat} (ha : a < 2^(p+1)) (h : a.testBit p = false) : a < 2^p := by
  have htb := Nat.testBit_add a 0 p
  rw [Nat.zero_add] at htb
  have h0 : (a/2^p).testBit 0 = false := htb ▸ h
  rw [Nat.testBit_zero] at h0
  have hmod_ne : (a/2^p) % 2 ≠ 1 := by
    intro hm
    rw [hm] at h0
    simp at h0
  have hdiv : a / 2^p < 2 := by
    apply (Nat.div_lt_iff_lt_mul (pow2_pos p)).mpr
    have h2 : 2 * 2^p = 2^(p+1) := by rw [Nat.pow_succ, Nat.mul_comm]
    rw [h2]; exact ha
  have hq0 : a / 2^p = 0 := by
    by_cases h1 : a / 2^p = 1
    · rw [h1] at h0
      simp at h0
    · have hle : a / 2^p ≤ 1 := Nat.le_of_lt_succ hdiv
      have hlt1 : a / 2^p < 1 := Nat.lt_of_le_of_ne hle h1
      exact Nat.lt_one_iff.mp hlt1
  have := (Nat.div_eq_zero_iff).mp hq0
  cases this with
  | inl h2p0 =>
    exact absurd h2p0 (Nat.pos_iff_ne_zero.mp (pow2_pos p))
  | inr hlt => exact hlt

-- mod preserves bit p when modulus is 2^(p+1)
private theorem mod_bit {a p : Nat} :
    (a % 2^(p+1)).testBit p = a.testBit p := by
  have h := Nat.testBit_mod_two_pow a (p+1) p
  have hdec : decide (p < p+1) = true := by
    rw [decide_eq_true_iff]; exact Nat.lt_succ_self p
  rw [hdec] at h
  simpa using h

private theorem bxor_self (b : Bool) : (b ^^ b) = false := by
  cases b <;> rfl

private theorem bool_eq_false_of_ne_true {b : Bool} (h : b ≠ true) : b = false := by
  cases b <;> simp_all

private theorem bool_eq_true_of_ne_false {b : Bool} (h : b ≠ false) : b = true := by
  cases b <;> simp_all

-- main bit analysis
private theorem drf_bits {l r : Nat} (hlr : l + 1 < r) :
    l / 2^((l ^^^ (r-1)).log2 + 1) = (r-1) / 2^((l ^^^ (r-1)).log2 + 1) ∧
    l.testBit (l ^^^ (r-1)).log2 = false ∧
    (r-1).testBit (l ^^^ (r-1)).log2 = true := by
  have hlrr : l < r - 1 := by omega
  have hx : l ^^^ (r-1) ≠ 0 := xor_ne_zero hlrr
  obtain ⟨hxbit, hxhigh⟩ := log2_bit_facts hx
  have hxor : ∀ i, (l ^^^ (r-1)).testBit i = (l.testBit i ^^ (r-1).testBit i) :=
    fun i => Nat.testBit_xor l (r-1) i
  have hF3 : ∀ i, (l ^^^ (r-1)).log2 < i → l.testBit i = (r-1).testBit i := by
    intro i hi
    have hxi : (l ^^^ (r-1)).testBit i = false := hxhigh i hi
    rw [hxor i] at hxi
    cases a : l.testBit i <;> cases b : (r-1).testBit i <;> simp_all [a, b]
  have hlq : l / 2^((l ^^^ (r-1)).log2 + 1) = (r-1) / 2^((l ^^^ (r-1)).log2 + 1) := by
    apply Nat.eq_of_testBit_eq
    intro j
    have e1 : (l / 2^((l ^^^ (r-1)).log2 + 1)).testBit j = l.testBit (j + ((l ^^^ (r-1)).log2 + 1)) := by
      have h := Nat.testBit_add l j ((l ^^^ (r-1)).log2 + 1)
      exact h.symm
    have e2 : ((r-1) / 2^((l ^^^ (r-1)).log2 + 1)).testBit j = (r-1).testBit (j + ((l ^^^ (r-1)).log2 + 1)) := by
      have h := Nat.testBit_add (r-1) j ((l ^^^ (r-1)).log2 + 1)
      exact h.symm
    rw [e1, e2]
    apply hF3
    omega
  have hne : l.testBit (l ^^^ (r-1)).log2 ≠ (r-1).testBit (l ^^^ (r-1)).log2 := by
    intro heq
    have hxi := hxor ((l ^^^ (r-1)).log2)
    rw [hxbit, heq, bxor_self] at hxi
    simp at hxi
  have hprod : 2^((l ^^^ (r-1)).log2 + 1) * (l / 2^((l ^^^ (r-1)).log2 + 1)) =
      2^((l ^^^ (r-1)).log2 + 1) * ((r-1) / 2^((l ^^^ (r-1)).log2 + 1)) := by
    rw [hlq]
  have hla : l = 2^((l ^^^ (r-1)).log2 + 1) * (l / 2^((l ^^^ (r-1)).log2 + 1)) + l % 2^((l ^^^ (r-1)).log2 + 1) :=
    (Nat.div_add_mod l _).symm
  have hlb : r - 1 = 2^((l ^^^ (r-1)).log2 + 1) * ((r-1) / 2^((l ^^^ (r-1)).log2 + 1)) + (r-1) % 2^((l ^^^ (r-1)).log2 + 1) :=
    (Nat.div_add_mod (r-1) _).symm
  have hla_mod : l % 2^((l ^^^ (r-1)).log2 + 1) < 2^((l ^^^ (r-1)).log2 + 1) :=
    Nat.mod_lt _ (pow2_pos _)
  have hlb_mod : (r-1) % 2^((l ^^^ (r-1)).log2 + 1) < 2^((l ^^^ (r-1)).log2 + 1) :=
    Nat.mod_lt _ (pow2_pos _)
  have hla_lb : l % 2^((l ^^^ (r-1)).log2 + 1) < (r-1) % 2^((l ^^^ (r-1)).log2 + 1) := by
    omega
  have htb_ne : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 ≠
      ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 := by
    have h1 := mod_bit (a := l) (p := (l ^^^ (r-1)).log2)
    have h2 := mod_bit (a := r-1) (p := (l ^^^ (r-1)).log2)
    rw [h1, h2]
    exact hne
  have key : ¬ ((l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = true ∧
      ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = false) := by
    intro h
    have h1 : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = true := h.1
    have h2 : ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = false := h.2
    have hge := tb_ge h1
    have hlt := tb_lt hlb_mod h2
    have hcontra : 2^((l ^^^ (r-1)).log2) < 2^((l ^^^ (r-1)).log2) := by omega
    exact absurd hcontra (Nat.lt_irrefl _)
  have hla_false : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = false := by
    by_cases hcase : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = true
    · have hlb_false : ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = false := by
        apply bool_eq_false_of_ne_true
        intro hb
        have heq : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 =
            ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 := by
          rw [hcase, hb]
        exact absurd heq htb_ne
      have hF := key ⟨hcase, hlb_false⟩
      cases hF
    · exact bool_eq_false_of_ne_true hcase
  have hlb_true : ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = true := by
    by_cases hcase : ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = false
    · have hla_true : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 = true := by
        apply bool_eq_true_of_ne_false
        intro hb
        have heq : (l % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 =
            ((r-1) % 2^((l ^^^ (r-1)).log2 + 1)).testBit (l ^^^ (r-1)).log2 := by
          rw [hb, hcase]
        exact absurd heq htb_ne
      have hF := key ⟨hla_true, hcase⟩
      cases hF
    · exact bool_eq_true_of_ne_false hcase
  have hl_false : l.testBit (l ^^^ (r-1)).log2 = false := by
    have h1 := mod_bit (a := l) (p := (l ^^^ (r-1)).log2)
    rw [hla_false] at h1
    exact h1.symm
  have hr_true : (r-1).testBit (l ^^^ (r-1)).log2 = true := by
    have h2 := mod_bit (a := r-1) (p := (l ^^^ (r-1)).log2)
    rw [hlb_true] at h2
    exact h2.symm
  exact ⟨hlq, hl_false, hr_true⟩



theorem sound_DisjointRangeFold : DisjointRangeFold.Goal := by
  intro xs l r hlr hr w cut
  have hw : w = 2^((l ^^^ (r-1)).log2) := rfl
  have hcut : cut = l / (2*w) * (2*w) + w := rfl
  obtain ⟨hlq, hl_false, hr_true⟩ := drf_bits hlr
  have hD : 2 * w = 2^((l ^^^ (r-1)).log2 + 1) := by
    rw [hw, Nat.pow_succ, Nat.mul_comm]
  have hcutD : cut = 2^((l ^^^ (r-1)).log2 + 1) * (l / 2^((l ^^^ (r-1)).log2 + 1)) + 2^((l ^^^ (r-1)).log2) := by
    rw [hcut, hD, hw, Nat.mul_comm]
  have hla_lt : l % 2^((l ^^^ (r-1)).log2 + 1) < 2^((l ^^^ (r-1)).log2) := by
    apply tb_lt (Nat.mod_lt _ (pow2_pos _))
    have h1 := mod_bit (a := l) (p := (l ^^^ (r-1)).log2)
    rw [hl_false] at h1
    exact h1
  have hlb_ge : 2^((l ^^^ (r-1)).log2) ≤ (r-1) % 2^((l ^^^ (r-1)).log2 + 1) := by
    apply tb_ge
    have h2 := mod_bit (a := r-1) (p := (l ^^^ (r-1)).log2)
    rw [hr_true] at h2
    exact h2
  have hla : l = 2^((l ^^^ (r-1)).log2 + 1) * (l / 2^((l ^^^ (r-1)).log2 + 1)) + l % 2^((l ^^^ (r-1)).log2 + 1) :=
    (Nat.div_add_mod l _).symm
  have hlb : r - 1 = 2^((l ^^^ (r-1)).log2 + 1) * ((r-1) / 2^((l ^^^ (r-1)).log2 + 1)) + (r-1) % 2^((l ^^^ (r-1)).log2 + 1) :=
    (Nat.div_add_mod (r-1) _).symm
  have hprod : 2^((l ^^^ (r-1)).log2 + 1) * (l / 2^((l ^^^ (r-1)).log2 + 1)) =
      2^((l ^^^ (r-1)).log2 + 1) * ((r-1) / 2^((l ^^^ (r-1)).log2 + 1)) := by
    rw [hlq]
  refine ⟨?_, ?_, ?_⟩
  · rw [hcutD]; omega
  · rw [hcutD]; omega
  · have hle1 : l ≤ cut := by rw [hcutD]; omega
    have hle2 : cut ≤ r := by rw [hcutD]; omega
    have happ := frame_append xs hle1 hle2
    rw [← String.join_append, ← happ]

end TinylambOpt.ResearchProofs.ExecutionStrategies.FrameIndexes
