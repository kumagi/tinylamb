import TinylambOpt.ResearchGoals.Frontiers.RankedJoins

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `BoxCertificate` — `∀ (boxes : List (Int × List Int)) (seed : List Int) k cutoff, k ≤
  seed.countP (· ≤ cutoff) → (∀ b ∈ boxes, ∀ v ∈ b.2, b.1 ≤ v) → best k (seed ++
  boxes.flatMap Prod.snd) = best k (seed ++ (boxes.filter (·.1 ≤ cutoff)).flatMap
  Prod.snd)`
- `DistinctOverfetch` — `∀ (xs : List Int) k cap, (∀ x ∈ xs, xs.count x ≤ cap) → ((best
  (k*cap) xs).eraseDups.take k) = best k xs.eraseDups`
- `IntervalWinners` — `∀ (winners rest : List Nat) (lo hi score : Nat → Int), (∀ x ∈
  winners ++ rest, lo x ≤ score x ∧ score x ≤ hi x) → (∀ x ∈ winners, ∀ y ∈ rest, hi
  x < lo y) → ((winners ++ rest).mergeSort (fun x y => score x ≤ score y)).take
  winners.length = winners.mergeSort (fun x y => score x ≤ score y)`
- `RankByCut` — `∀ a b k (bound : Int), k < a.length*b.length → ((sums a
  b).mergeSort[k]?.any (· ≤ bound) = true ↔ k < (a.map (fun x =>
  (b.mergeSort.takeWhile (· ≤ bound-x)).length)).sum)`
- `TieMass` — `∀ (rows : List (Int × Nat)) k cutoff, pick (rows.mergeSort (fun a b =>
  a.1 ≤ b.1)) k = some cutoff → let xs := rows.flatMap (fun (v,w) => List.replicate w
  v) let lt := ((rows.filter (·.1 < cutoff)).map Prod.snd).sum let eq :=
  ((rows.filter (·.1 == cutoff)).map Prod.snd).sum lt ≤ k ∧ k < lt+eq ∧
  (xs.mergeSort.takeWhile (· ≤ cutoff)).length = lt+eq`
- `WeightedSeek` — `∀ (rows : List (Int × Nat)) k, pick (rows.mergeSort (fun a b => a.1
  ≤ b.1)) k = (rows.flatMap (fun (v,w) => List.replicate w v)).mergeSort[k]?`

Refuted by counterexample

- `MeanHoleBudget` — `∀ a b ok k h, (a.map (fun x => b.countP (fun y => !ok x y))).sum
  ≤ h * min a.length b.length → best k (qualified a b ok) = best k (qualified (best
  (k+h) a) (best (k+h) b) ok)`
-/

namespace TinylambOpt.ResearchProofs.Frontiers.RankedJoins

open TinylambOpt.ResearchGoals.Frontiers.RankedJoins

private theorem sortedInt (xs : List Int) : xs.mergeSort.Pairwise (fun a b : Int => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
    (by intro a b c hab hbc; by_cases h1 : a ≤ b <;> by_cases h2 : b ≤ c <;> simp_all; omega)
    (by intro a b; by_cases h : a ≤ b <;> simp [h] <;> omega) xs
  exact h.imp (by intro a b hab; simpa using hab)

private theorem keySorted (rows : List (Int × Nat)) :
    (rows.mergeSort (fun a b => a.1 ≤ b.1)).Pairwise (fun a b : Int × Nat => a.1 ≤ b.1) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int × Nat => decide (a.1 ≤ b.1))
    (by intro a b c hab hbc; by_cases h1 : a.1 ≤ b.1 <;> by_cases h2 : b.1 ≤ c.1 <;> simp_all; omega)
    (by intro a b; by_cases h : a.1 ≤ b.1 <;> simp [h] <;> omega) rows
  exact h.imp (by intro a b hab; simpa using hab)

private def expanded : List (Int × Nat) → List Int
  | [] => []
  | (v,w)::rs => List.replicate w v ++ expanded rs

private theorem expanded_eq (rows) : rows.flatMap (fun (v,w) => List.replicate w v) = expanded rows := by
  induction rows with
  | nil => simp [expanded]
  | cons r rs ih => rcases r with ⟨v,w⟩; simp [expanded, ih]

private theorem repSorted (v : Int) (w : Nat) :
    (List.replicate w v).Pairwise (fun a b : Int => a ≤ b) := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.replicate_succ]
      exact ih.cons (by
        intro y hy
        rcases List.mem_replicate.mp hy with ⟨_, rfl⟩
        omega)

private theorem expanded_sorted {rows : List (Int × Nat)}
    (hs : rows.Pairwise (fun a b : Int × Nat => a.1 ≤ b.1)) :
    (expanded rows).Pairwise (fun a b : Int => a ≤ b) := by
  induction rows with
  | nil => simp [expanded]
  | cons r rs ih =>
      rcases r with ⟨v,w⟩
      have hs' := List.Pairwise.of_cons hs
      have hle : ∀ y ∈ expanded rs, v ≤ y := by
        intro y hy
        have hy'' : y ∈ rs.flatMap (fun (v,w) => List.replicate w v) := by simpa [expanded_eq] using hy
        rcases List.mem_flatMap.mp hy'' with ⟨⟨v',w'⟩, hv'mem, hyrep⟩
        change y ∈ List.replicate w' v' at hyrep
        rcases List.mem_replicate.mp hyrep with ⟨_, rfl⟩
        exact (List.pairwise_cons.mp hs).1 _ hv'mem
      change List.Pairwise (fun a b : Int => a ≤ b) (List.replicate w v ++ expanded rs)
      rw [List.pairwise_append]
      refine ⟨repSorted v w, ih hs', ?_⟩
      intro x hx y hy
      rcases List.mem_replicate.mp hx with ⟨_, rfl⟩
      exact hle y hy

private theorem pick_expanded : ∀ (rows : List (Int × Nat)) (k : Nat),
    rows.Pairwise (fun a b : Int × Nat => a.1 ≤ b.1) →
    pick rows k = (expanded rows)[k]? := by
  intro rows k
  induction rows generalizing k with
  | nil => simp [pick, expanded]
  | cons r rs ih =>
      rcases r with ⟨v,w⟩
      intro hs
      have hs' := List.Pairwise.of_cons hs
      by_cases hk : k < w
      · have hkw : k < (List.replicate w v).length := by simpa using hk
        simp [pick, hk, expanded, List.getElem?_append_left hkw]
      · have hkw : w ≤ k := by omega
        have hkn : (List.replicate w v).length ≤ k := by simpa using hkw
        rw [show expanded ((v,w)::rs) = List.replicate w v ++ expanded rs from rfl]
        rw [List.getElem?_append_right hkn]
        simp [pick, hk]
        simpa using ih (k - w) hs'

theorem WeightedSeek_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.WeightedSeek.Goal := by
  intro rows k
  let rs := rows.mergeSort (fun a b => a.1 ≤ b.1)
  have hrsP : rs.Perm rows := by simpa [rs] using List.mergeSort_perm rows (fun a b : Int × Nat => decide (a.1 ≤ b.1))
  have hrsSort := keySorted rows
  have hsel : pick rs k = (expanded rs)[k]? := pick_expanded rs k hrsSort
  have hexpandP : (expanded rs).Perm (expanded rows) := by
    rw [← expanded_eq, ← expanded_eq]
    exact hrsP.flatMap_right (fun (v,w) => List.replicate w v)
  have hsort_eq : (expanded rows).mergeSort = expanded rs := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    · intro a b ha hb hab hba; omega
    · exact sortedInt (expanded rows)
    · simpa using expanded_sorted hrsSort
    · exact (List.mergeSort_perm (expanded rows) (fun a b : Int => decide (a ≤ b))).trans hexpandP.symm
  simpa [expanded_eq, hsort_eq, rs] using hsel


private theorem countP_expanded (p : Int → Bool) (rows : List (Int × Nat)) :
    (expanded rows).countP p =
      ((rows.filter (fun r : Int × Nat => p r.1)).map Prod.snd).sum := by
  induction rows with
  | nil => simp [expanded]
  | cons r rs ih =>
      rcases r with ⟨v,w⟩
      change (List.replicate w v ++ expanded rs).countP p =
        ((List.filter (fun t : Int × Nat => p t.1) ((v,w)::rs)).map Prod.snd).sum
      rw [List.countP_append, List.countP_replicate]
      by_cases hv : p v <;> simp [hv, ih]

private theorem countP_take_add_drop (p : Int → Bool) (s : List Int) (n : Nat) :
    (s.take n).countP p + (s.drop n).countP p = s.countP p := by
  rw [← List.countP_append, List.take_append_drop]

private theorem takeWhile_length_count {s : List Int} (hs : s.Pairwise (fun a b => a ≤ b)) (c : Int) :
    (s.takeWhile (fun y => y ≤ c)).length = s.countP (fun y => y ≤ c) := by
  induction s with
  | nil => simp
  | cons a s ih =>
      have hs' := List.Pairwise.of_cons hs
      have hall : ∀ y ∈ s, a ≤ y := (List.pairwise_cons.mp hs).1
      by_cases ha : a ≤ c
      · simp [ha, ih hs']
      · have hnone : ∀ y ∈ s, ¬ (decide (y ≤ c)) = true := by
          intro y hy hbt
          have hay := hall y hy
          have hyc : y ≤ c := by simpa using hbt
          omega
        have hcnt0 : s.countP (fun y => y ≤ c) = 0 :=
          List.countP_eq_zero.mpr hnone
        simp [ha, hcnt0]

private theorem mem_drop_index {l : List Int} {n : Nat} {y : Int} :
    y ∈ l.drop n → ∃ j, n + j < l.length ∧ l[n+j]? = some y := by
  induction n generalizing l with
  | zero =>
      intro hy
      rcases List.mem_iff_getElem?.mp hy with ⟨j, hj⟩
      rcases List.getElem?_eq_some_iff.mp hj with ⟨hjlt, _⟩
      have hj' : (0 + j) < l.length ∧ l[0+j]? = some y := by simpa using And.intro hjlt hj
      exact ⟨j, hj'.1, hj'.2⟩
  | succ n ih =>
      intro hy
      cases l with
      | nil => simp at hy
      | cons a l =>
          change y ∈ List.drop n l at hy
          rcases ih hy with ⟨j, hjlt, hjval⟩
          refine ⟨j, ?_, ?_⟩
          · simpa [Nat.succ_add] using hjlt
          · simpa [Nat.succ_add] using hjval

private theorem getElem_sorted_bounds {s : List Int} (hs : s.Pairwise (fun a b => a ≤ b))
    {k : Nat} {c : Int} (hk : s[k]? = some c) :
    s.countP (fun y => y < c) ≤ k ∧ k < s.countP (fun y => y ≤ c) := by
  rcases List.getElem?_eq_some_iff.mp hk with ⟨hkl, hget⟩
  have htake : (s.take (k+1)).countP (fun y => y ≤ c) = k+1 := by
    have hlen : (s.take (k+1)).length = k+1 := List.length_take_of_le (by omega)
    have hcntlen : (s.take (k+1)).countP (fun y => y ≤ c) = (s.take (k+1)).length := by
      rw [List.countP_eq_length]
      intro y hy
      rcases List.mem_iff_getElem?.mp hy with ⟨j, hjopt⟩
      rw [List.getElem?_take] at hjopt
      by_cases hjlt : j < k+1
      · simp [hjlt] at hjopt
        rcases List.getElem?_eq_some_iff.mp hjopt with ⟨hjlen, hjval⟩
        by_cases hjk : j = k
        · subst j
          have hyc : y = c := hjval.symm.trans hget
          simp [hyc]
        · have hjltk : j < k := by omega
          have hrel := hs.rel_getElem_of_lt hjlen hkl hjltk
          have hyc : y ≤ c := by simpa [hjval.symm, hget] using hrel
          simp [hyc]
      · simp [hjlt] at hjopt
    rw [hcntlen, hlen]
  have hdrop : (s.drop k).countP (fun y => y < c) = 0 := by
    rw [List.countP_eq_zero]
    intro y hy
    rcases mem_drop_index hy with ⟨j, hjlen, hjval⟩
    rcases List.getElem?_eq_some_iff.mp hjval with ⟨_, hjval'⟩
    by_cases hj0 : j = 0
    · subst j
      have hyc : y = c := by simpa [hget] using hjval'.symm
      simp [hyc]
    · have hklt : k < k+j := by omega
      have hrel := hs.rel_getElem_of_lt hkl hjlen hklt
      have hcyle : c ≤ y := by simpa [hjval', hget] using hrel
      simp [show ¬ y < c by omega]
  constructor
  · have hsum := countP_take_add_drop (fun y => y < c) s k
    rw [hdrop, Nat.add_zero] at hsum
    rw [← hsum]
    have hlelen := List.countP_le_length (p := fun y => y < c) (l := s.take k)
    rw [List.length_take] at hlelen
    omega
  · have hsum := countP_take_add_drop (fun y => y ≤ c) s (k+1)
    rw [htake] at hsum
    omega

private theorem sum_filter_weights_perm {p : Int → Bool} {rs rows : List (Int × Nat)}
    (h : rs.Perm rows) :
    ((rs.filter (fun r => p r.1)).map Prod.snd).sum =
      ((rows.filter (fun r => p r.1)).map Prod.snd).sum := by
  exact ((h.filter (fun r => p r.1)).map Prod.snd).sum_nat

private theorem countP_le_eq_sum (s : List Int) (c : Int) :
    s.countP (fun y => y ≤ c) = s.countP (fun y => y < c) + s.countP (fun y => y == c) := by
  induction s with
  | nil => simp
  | cons a s ih =>
      by_cases hac : a ≤ c
      · by_cases hal : a < c
        · have haeq : (a == c) = false := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
        · have haeq : (a == c) = true := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
      · have hal : ¬ a < c := by omega
        have haeq : (a == c) = false := by simp; omega
        simp [hac, hal, haeq, ih]

theorem TieMass_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.TieMass.Goal := by
  intro rows k cutoff hpick
  let rs := rows.mergeSort (fun a b => a.1 ≤ b.1)
  have hrsP : rs.Perm rows := by simpa [rs] using List.mergeSort_perm rows (fun a b : Int × Nat => decide (a.1 ≤ b.1))
  have hrsSort := keySorted rows
  have hexpP : (expanded rs).Perm (expanded rows) := by
    rw [← expanded_eq, ← expanded_eq]
    exact hrsP.flatMap_right (fun (v,w) => List.replicate w v)
  have hsortEq : (expanded rows).mergeSort = expanded rs := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    · intro a b ha hb hab hba; omega
    · exact sortedInt (expanded rows)
    · simpa using expanded_sorted hrsSort
    · exact (List.mergeSort_perm (expanded rows) (fun a b : Int => decide (a ≤ b))).trans hexpP.symm
  let xs := rows.flatMap (fun (v,w) => List.replicate w v)
  have hxs : xs.mergeSort = expanded rs := by simp [xs, expanded_eq, hsortEq]
  have hpick' : (expanded rs)[k]? = some cutoff := by
    rw [← pick_expanded rs k hrsSort]
    simpa [rs] using hpick
  have hb := getElem_sorted_bounds (expanded_sorted hrsSort) hpick'
  have hlt : (expanded rs).countP (fun y => y < cutoff) =
      ((rows.filter (fun r : Int × Nat => r.1 < cutoff)).map Prod.snd).sum := by
    rw [countP_expanded]
    exact sum_filter_weights_perm (p := fun y => y < cutoff) hrsP
  have heq : (expanded rs).countP (fun y => y == cutoff) =
      ((rows.filter (fun r : Int × Nat => r.1 == cutoff)).map Prod.snd).sum := by
    rw [countP_expanded]
    exact sum_filter_weights_perm (p := fun y => y == cutoff) hrsP
  have hle : (expanded rs).countP (fun y => y ≤ cutoff) =
      (expanded rs).countP (fun y => y < cutoff) + (expanded rs).countP (fun y => y == cutoff) :=
    countP_le_eq_sum (expanded rs) cutoff
  have htw : (xs.mergeSort.takeWhile (fun y => y ≤ cutoff)).length =
      (expanded rs).countP (fun y => y ≤ cutoff) := by
    rw [hxs]
    exact takeWhile_length_count (expanded_sorted hrsSort) cutoff
  rw [hlt] at hb hle
  rw [heq] at hle
  rw [hle] at hb htw
  dsimp only
  constructor
  · omega
  constructor
  · omega
  · omega


private theorem count_le_getElem {s : List Int} (hs : s.Pairwise (fun a b => a ≤ b))
    {k : Nat} (hk : k < s.length) (bound : Int) :
    s[k] ≤ bound ↔ k < s.countP (fun y => y ≤ bound) := by
  constructor
  · intro hle
    have hcntlen : (s.take (k+1)).countP (fun y => y ≤ bound) = (s.take (k+1)).length := by
      rw [List.countP_eq_length]
      intro y hy
      rcases List.mem_iff_getElem?.mp hy with ⟨j, hjopt⟩
      rw [List.getElem?_take] at hjopt
      by_cases hjlt : j < k+1
      · simp [hjlt] at hjopt
        rcases List.getElem?_eq_some_iff.mp hjopt with ⟨hjlen, hjval⟩
        by_cases hjk : j = k
        · subst j
          have hyc : y = s[k] := hjval.symm
          simp [hyc, hle]
        · have hjltk : j < k := by omega
          have hrel := hs.rel_getElem_of_lt hjlen hk hjltk
          have hyc : y ≤ bound := by
            have hsjk : s[j] ≤ s[k] := hrel
            have hybound : y ≤ bound := by omega
            simp [hybound]
          simp [hyc]
      · simp [hjlt] at hjopt
    rw [List.length_take_of_le (by omega)] at hcntlen
    have hsum := countP_take_add_drop (fun y => y ≤ bound) s (k+1)
    rw [hcntlen] at hsum
    omega
  · intro hkcnt
    by_cases hle : s[k] ≤ bound
    · exact hle
    · have hgt : bound < s[k] := by omega
      have hcnt0 : (s.drop k).countP (fun y => y ≤ bound) = 0 := by
        rw [List.countP_eq_zero]
        intro y hy
        rcases mem_drop_index hy with ⟨j, hjlen, hjval⟩
        rcases List.getElem?_eq_some_iff.mp hjval with ⟨_, hjval'⟩
        have hyge : s[k] ≤ y := by
          by_cases hj0 : j = 0
          · subst j
            have hky : s[k] = y := by simpa using hjval'
            rw [← hky]
            omega
          · have hklt : k < k+j := by omega
            have hrel := hs.rel_getElem_of_lt hk hjlen hklt
            rw [← hjval']
            exact hrel
        simp [show ¬ y ≤ bound by omega]
      have hsum := countP_take_add_drop (fun y => y ≤ bound) s k
      rw [hcnt0, Nat.add_zero] at hsum
      have hlelen := List.countP_le_length (p := fun y => y ≤ bound) (l := s.take k)
      rw [List.length_take] at hlelen
      exfalso
      omega

private theorem sum_const_length (xs : List α) (n : Nat) : (xs.map fun _ => n).sum = xs.length * n := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.succ_mul]; omega

private theorem length_sums (a b : List Int) : (sums a b).length = a.length * b.length := by
  induction a with
  | nil => simp [sums]
  | cons x xs ih =>
      simp [sums, List.length_flatMap, Nat.succ_mul, sum_const_length]
      omega

private theorem count_sums (p : Int → Bool) (a b : List Int) :
    (sums a b).countP p = (a.map fun x => b.countP (fun y => p (x+y))).sum := by
  simp only [sums, List.countP_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [Function.comp_apply, List.countP_map]
  apply List.countP_congr
  intro y hy
  rfl

private theorem takeWhile_count_sort (b : List Int) (c : Int) :
    (b.mergeSort.takeWhile (fun y => y ≤ c)).length = b.countP (fun y => y ≤ c) := by
  rw [takeWhile_length_count (sortedInt b) c]
  exact (List.mergeSort_perm b (fun a b : Int => decide (a ≤ b))).countP_eq (fun y => y ≤ c)

theorem RankByCut_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.RankByCut.Goal := by
  intro a b k bound hklen
  let q := (sums a b).mergeSort
  have hqsort := sortedInt (sums a b)
  have hqlen : q.length = a.length * b.length := by simp [q, length_sums]
  have hkq : k < q.length := by omega
  have hleft : ((sums a b).mergeSort[k]?.any (fun y => y ≤ bound) = true) ↔
      k < (sums a b).countP (fun y => y ≤ bound) := by
    change (q[k]?.any (fun y => y ≤ bound) = true) ↔ _
    rw [List.getElem?_eq_getElem hkq]
    simp
    have hcg : q.countP (fun y => y ≤ bound) = (sums a b).countP (fun y => y ≤ bound) := by
      exact (List.mergeSort_perm (sums a b) (fun x y : Int => decide (x ≤ y))).countP_eq (fun y => y ≤ bound)
    rw [← hcg]
    exact count_le_getElem hqsort hkq bound
  have hright : k < (a.map (fun x => (b.mergeSort.takeWhile (fun y => y ≤ bound - x)).length)).sum ↔
      k < (sums a b).countP (fun y => y ≤ bound) := by
    have hsum : (a.map (fun x => (b.mergeSort.takeWhile (fun y => y ≤ bound - x)).length)).sum =
        (sums a b).countP (fun y => y ≤ bound) := by
      rw [count_sums]
      apply congrArg List.sum
      apply List.map_congr_left
      intro x hx
      rw [takeWhile_count_sort]
      apply List.countP_congr
      intro y hy
      simp
      omega
    rw [hsum]
  exact hleft.trans hright.symm



private theorem sort_eq_of_perm {xs ys : List Int} (h : xs.Perm ys) : xs.mergeSort = ys.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    (by intro a b ha hb hab hba; omega)
  · exact sortedInt xs
  · exact sortedInt ys
  · exact (List.mergeSort_perm xs (fun a b : Int => decide (a ≤ b))).trans
      (h.trans (List.mergeSort_perm ys (fun a b : Int => decide (a ≤ b))).symm)

private theorem best_perm {xs ys : List Int} (h : xs.Perm ys) (k : Nat) : best k xs = best k ys := by
  simp [best, sort_eq_of_perm h]

private theorem best_sorted {xs : List Int} (hs : xs.Pairwise (fun a b => a ≤ b)) (k : Nat) :
    best k xs = xs.take k := by
  have hb : xs.mergeSort (fun a b : Int => decide (a ≤ b)) = xs := by
    apply List.mergeSort_of_pairwise
    exact hs.imp (by intro a b h; simp [h])
  simp [best, hb]

private theorem eraseDups_nodup : ∀ (l : List Int), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Int), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons ?_ (ih _ hlen _ rfl)
            intro b hb
            have hbmem : b ∈ as.filter (fun z => !z == a) := List.mem_eraseDups.mp hb
            have hb' := (List.mem_filter.mp hbmem).2
            have hbne : b ≠ a := by
              intro hba
              subst b
              simp at hb'
            exact fun heq => hbne heq.symm
  exact key l.length l rfl

private theorem count_eq_zero_of_not_mem {l : List Int} {v : Int} (hv : v ∉ l) : l.count v = 0 := by
  rw [List.count_eq_countP]
  exact List.countP_eq_zero.mpr (by
    intro a ha hbeq
    have haeq : a = v := by simpa using hbeq
    exact hv (by simpa [haeq] using ha))

private theorem count_eq_one_of_nodup_mem {l : List Int} (hnd : l.Nodup) {v : Int} (hv : v ∈ l) :
    l.count v = 1 := by
  induction l with
  | nil => simp at hv
  | cons a l ih =>
      rcases List.mem_cons.mp hv with hva | hv
      · subst v
        have hnot : a ∉ l := by
          intro ha
          exact (List.pairwise_cons.mp hnd).1 a ha rfl
        rw [List.count_cons_self, count_eq_zero_of_not_mem hnot]
      · have hne : a ≠ v := by
          intro heq
          exact (List.pairwise_cons.mp hnd).1 v hv heq
        rw [List.count_cons_of_ne hne]
        exact ih (List.pairwise_cons.mp hnd).2 hv

private theorem count_eraseDups (v : Int) (l : List Int) :
    l.eraseDups.count v = if v ∈ l then 1 else 0 := by
  by_cases hv : v ∈ l
  · have hm : v ∈ l.eraseDups := List.mem_eraseDups.mpr hv
    rw [count_eq_one_of_nodup_mem (eraseDups_nodup l) hm]
    simp [hv]
  · have hm : v ∉ l.eraseDups := by intro hm; exact hv (List.mem_eraseDups.mp hm)
    rw [count_eq_zero_of_not_mem hm]
    simp [hv]

private theorem eraseDups_perm {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  apply List.perm_iff_count.mpr
  intro v
  rw [count_eraseDups, count_eraseDups]
  by_cases hv : v ∈ l₁
  · have hv2 : v ∈ l₂ := h.mem_iff.mp hv
    simp [hv, hv2]
  · have hv2 : v ∉ l₂ := by intro hv2; exact hv (h.mem_iff.mpr hv2)
    simp [hv, hv2]

private theorem takeWhile_eq_replicate (a : Int) (l : List Int) (h : ∀ y ∈ l, y == a) :
    l = List.replicate l.length a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hb : b == a := h b (by simp)
      have hba : b = a := by simpa using hb
      subst b
      have htail : ∀ y ∈ bs, y == a := by intro y hy; exact h y (by simp [hy])
      rw [show (a :: bs).length = bs.length + 1 by simp, List.replicate_succ]
      congr
      exact ih htail

private theorem no_head_in_dropWhile {a : Int} {xs : List Int}
    (hs : (a :: xs).Pairwise (fun x y : Int => x ≤ y)) :
    ∀ y ∈ xs.dropWhile (fun z => z == a), y ≠ a := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : b == a
      · have hba : b = a := by simpa using hb
        subst b
        have hs' : (a :: bs).Pairwise (fun x y : Int => x ≤ y) := (List.pairwise_cons.mp hs).2
        simpa [hb] using (ih hs')
      · have hrel : ∀ y ∈ bs, b ≤ y := (List.pairwise_cons.mp (List.pairwise_cons.mp hs).2).1
        have hab : a ≤ b := (List.pairwise_cons.mp hs).1 b (by simp)
        intro y hy
        simp [hb] at hy
        rcases hy with rfl | hy
        · intro heq; apply hb; simp [heq]
        · have hby : b ≤ y := hrel y hy
          intro hya
          subst y
          have hba : b = a := by omega
          apply hb
          simp [hba]

private theorem take_append_eq (tw dw : List Int) : ∀ n, tw.length ≤ n →
    (tw ++ dw).take n = tw ++ dw.take (n - tw.length) := by
  induction tw with
  | nil => intro n h; simp
  | cons a tw ih =>
      intro n h
      cases n with
      | zero => simp at h
      | succ n =>
          have hlen : (a :: tw).length = tw.length + 1 := by simp
          simp only [List.cons_append]
          rw [List.take_succ_cons]
          rw [ih n (by omega)]
          congr 1
          rw [hlen]
          have heq : n + 1 - (tw.length + 1) = n - tw.length := by omega
          rw [heq]

private theorem removeAll_replicate_no {a : Int} {l : List Int}
    (h : ∀ y ∈ l, y ≠ a) : ∀ n : Nat, l.removeAll (List.replicate n a) = l := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.removeAll_cons]
      have hf : l.filter (fun x : Int => !x == a) = l := by
        rw [List.filter_eq_self]
        intro x hx
        simp [h x hx]
      rw [hf]
      exact ih

private theorem mem_takeWhile_eq {p : Int → Bool} {l : List Int} {y : Int}
    (hy : y ∈ l.takeWhile p) : p y := by
  induction l with
  | nil => simp at hy
  | cons a l ih =>
      by_cases ha : p a
      · simp [ha] at hy
        rcases hy with rfl | hy
        · exact ha
        · exact ih hy
      · simp [ha] at hy

private theorem eraseDups_replicate_pos (a : Int) : ∀ n : Nat, 0 < n →
    (List.replicate n a).eraseDups = [a]
  | 0, h => by omega
  | n+1, h => by
      cases n with
      | zero => simp [List.eraseDups_cons]
      | succ m =>
          rw [List.replicate_succ, List.eraseDups_cons]
          simp

private theorem prefix_erase : ∀ (s : List Int) (cap : Nat),
    s.Pairwise (fun x y : Int => x ≤ y) →
    (∀ x ∈ s, s.count x ≤ cap) →
    ∀ k n, k * cap ≤ n → ((s.take n).eraseDups).take k = (s.eraseDups).take k := by
  intro s cap hs hcap
  have key : ∀ (len : Nat) (s : List Int), s.length = len →
      s.Pairwise (fun x y : Int => x ≤ y) →
      (∀ x ∈ s, s.count x ≤ cap) →
      ∀ k n, k * cap ≤ n → ((s.take n).eraseDups).take k = (s.eraseDups).take k := by
    intro len
    induction len using Nat.strongRecOn with
    | ind len ih =>
        intro s hslen hs hcap k n hle
        cases s with
        | nil => simp
        | cons a as =>
            by_cases hcap0 : cap = 0
            · subst cap
              have hc := hcap a (by simp)
              have hpos : 0 < (a :: as).count a := by simp
              omega
            · have hcapPos : 0 < cap := Nat.pos_of_ne_zero hcap0
              cases k with
              | zero => simp [List.eraseDups_cons]
              | succ k' =>
                  let tw := (a :: as).takeWhile (fun z => z == a)
                  let dw := (a :: as).dropWhile (fun z => z == a)
                  have hsplit : tw ++ dw = a :: as := List.takeWhile_append_dropWhile
                  have htwmem : ∀ y ∈ tw, y == a := by
                    intro y hy
                    exact mem_takeWhile_eq (p := fun z : Int => z == a) hy
                  have htwrep : tw = List.replicate tw.length a :=
                    takeWhile_eq_replicate a tw htwmem
                  have hdwnol : ∀ y ∈ dw, y ≠ a := by
                    simpa [dw] using (no_head_in_dropWhile hs)
                  have hsub : List.Sublist dw (a :: as) := by
                    simpa [dw] using (List.dropWhile_sublist (p := fun z : Int => z == a) (l := a :: as))
                  have hdwsort : dw.Pairwise (fun x y : Int => x ≤ y) := List.Pairwise.sublist hsub hs
                  have hdcap : ∀ x ∈ dw, dw.count x ≤ cap := by
                    intro x hx
                    have hle' : dw.countP (fun y => y == x) ≤ (a :: as).countP (fun y => y == x) := hsub.countP_le
                    have hmem : x ∈ a :: as := hsub.subset hx
                    exact Nat.le_trans (by simpa [List.count_eq_countP] using hle') (hcap x hmem)
                  have hcnta : (a :: as).count a = tw.length := by
                    rw [← hsplit, List.count_append]
                    have hctw : tw.count a = tw.length := by rw [htwrep]; simp
                    have hcdw : dw.count a = 0 := count_eq_zero_of_not_mem (by intro ha; exact hdwnol a ha rfl)
                    rw [hctw, hcdw, Nat.add_zero]
                  have htwlen_le_cap : tw.length ≤ cap := by
                    rw [← hcnta]
                    exact hcap a (by simp)
                  have htwpos : 0 < tw.length := by simp [tw]
                  have hcap_le_n : cap ≤ n := by
                    rw [Nat.succ_mul] at hle
                    omega
                  have htwlen_le_n : tw.length ≤ n := by omega
                  have htake : (a :: as).take n = tw ++ dw.take (n - tw.length) := by
                    rw [← hsplit]
                    exact take_append_eq tw dw n htwlen_le_n
                  have htwED : tw.eraseDups = [a] := by
                    rw [htwrep]
                    exact eraseDups_replicate_pos a tw.length htwpos
                  have hremove : dw.removeAll tw = dw := by
                    rw [htwrep]
                    exact removeAll_replicate_no hdwnol tw.length
                  have htakeNo : ∀ y ∈ dw.take (n - tw.length), y ≠ a := by
                    intro y hy
                    exact hdwnol y ((List.take_sublist _ _).subset hy)
                  have hremoveTake : (dw.take (n - tw.length)).removeAll tw = dw.take (n - tw.length) := by
                    rw [htwrep]
                    simpa only [List.length_replicate] using (removeAll_replicate_no htakeNo tw.length)
                  have heraseTake : ((a :: as).take n).eraseDups =
                      a :: (dw.take (n - tw.length)).eraseDups := by
                    rw [htake, List.eraseDups_append, htwED, hremoveTake]
                    simp
                  have heraseFull : (a :: as).eraseDups = a :: dw.eraseDups := by
                    rw [← hsplit, List.eraseDups_append, htwED, hremove]
                    simp
                  have hdlen : dw.length < len := by
                    have hlen_split : tw.length + dw.length = len := by
                      rw [← hslen, ← hsplit, List.length_append]
                    omega
                  have hle'' : k' * cap + cap ≤ n := by
                    simpa [Nat.succ_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using hle
                  have hle' : k' * cap ≤ n - tw.length := by omega
                  have hrec := ih dw.length hdlen dw rfl hdwsort hdcap k' (n - tw.length) hle'
                  rw [heraseTake, heraseFull]
                  simp
                  exact hrec
  exact key s.length s rfl hs hcap

private theorem eraseDups_sorted : ∀ (l : List Int),
    l.Pairwise (fun a b : Int => a ≤ b) → l.eraseDups.Pairwise (fun a b : Int => a ≤ b) := by
  intro l
  have key : ∀ (n : Nat) (l : List Int), l.length = n →
      l.Pairwise (fun a b : Int => a ≤ b) → l.eraseDups.Pairwise (fun a b : Int => a ≤ b) := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro l hlen hs
        cases l with
        | nil => simp
        | cons a as =>
            rw [List.eraseDups_cons]
            have hfiltsort : (as.filter (fun b : Int => !b == a)).Pairwise (fun x y : Int => x ≤ y) :=
              List.Pairwise.filter _ (List.Pairwise.of_cons hs)
            have hlenf : (as.filter (fun b : Int => !b == a)).length < n := by
              rw [List.length_cons] at hlen
              have hle := List.length_filter_le (fun b : Int => !b == a) as
              omega
            refine List.Pairwise.cons ?_ (ih _ hlenf _ rfl hfiltsort)
            intro y hy
            exact (List.pairwise_cons.mp hs).1 y (List.mem_filter.mp (List.mem_eraseDups.mp hy)).1
  exact key l.length l rfl

theorem DistinctOverfetch_sound :
    TinylambOpt.ResearchGoals.Frontiers.RankedJoins.DistinctOverfetch.Goal := by
  intro xs k cap hcap
  let s := xs.mergeSort
  have hsSort : s.Pairwise (fun a b : Int => a ≤ b) := by simpa [s] using sortedInt xs
  have hsPerm : s.Perm xs := by simpa [s] using List.mergeSort_perm xs (fun a b : Int => decide (a ≤ b))
  have hcapS : ∀ x ∈ s, s.count x ≤ cap := by
    intro x hx
    rw [hsPerm.count_eq x]
    exact hcap x ((List.mem_mergeSort).mp hx)
  have hprefix := prefix_erase s cap hsSort hcapS k (k*cap) (by omega)
  have hse : xs.eraseDups.mergeSort = s.eraseDups := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    · intro a b ha hb hab hba; omega
    · exact sortedInt xs.eraseDups
    · exact eraseDups_sorted s hsSort
    · exact (List.mergeSort_perm xs.eraseDups (fun a b : Int => decide (a ≤ b))).trans
        (eraseDups_perm hsPerm.symm)
  simpa [best, s, hse] using hprefix




private theorem countLE_take : ∀ (s : List Int), s.Pairwise (fun a b => a ≤ b) →
    ∀ (v : Int) (k : Nat),
    (s.take k).countP (fun y => y ≤ v) = min k (s.countP (fun y => y ≤ v)) := by
  intro s
  induction s with
  | nil => intro hs v k; simp
  | cons a s ih =>
      intro hs v k
      have hs' := List.Pairwise.of_cons hs
      cases k with
      | zero => simp
      | succ k =>
          by_cases ha : a ≤ v
          · have hrel := ih hs' v k
            simp [ha, hrel]
          · have hnone : ∀ y ∈ s, ¬ (decide (y ≤ v)) = true := by
              intro y hy hbt
              have hay := (List.pairwise_cons.mp hs).1 y hy
              have hyv : y ≤ v := by simpa using hbt
              omega
            have hcnt0 : s.countP (fun y => y ≤ v) = 0 := List.countP_eq_zero.mpr hnone
            have hcntTake0 : (s.take k).countP (fun y => y ≤ v) = 0 :=
              List.countP_eq_zero.mpr (by
                intro y hy hbt
                exact hnone y ((List.take_sublist k s).subset hy) hbt)
            rw [List.take_succ_cons]
            simp [ha, hcnt0, hcntTake0]

private theorem countP_lt_eq_pred (s : List Int) (c : Int) :
    s.countP (fun y => y < c) = s.countP (fun y => y ≤ c - 1) := by
  apply List.countP_congr
  intro y hy
  simp
  omega

private theorem countP_le_eq_sum_do (s : List Int) (c : Int) :
    s.countP (fun y => y ≤ c) = s.countP (fun y => y < c) + s.countP (fun y => y == c) := by
  induction s with
  | nil => simp
  | cons a s ih =>
      by_cases hac : a ≤ c
      · by_cases hal : a < c
        · have haeq : (a == c) = false := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
        · have haeq : (a == c) = true := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
      · have hal : ¬ a < c := by omega
        have haeq : (a == c) = false := by simp; omega
        simp [hac, hal, haeq, ih]

private theorem sum_filter_of_zero {α : Type} (f : α → Nat) (p : α → Bool) (l : List α)
    (hz : ∀ x ∈ l, ¬ p x → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x
      · simp [hx, ih (by intro y hy; exact hz y (by simp [hy]))]
      · have hz0 := hz x (by simp) hx
        simp [hx, hz0, ih (by intro y hy; exact hz y (by simp [hy]))]

private theorem count_eq_of_countLE {A B : List Int}
    (hcnt : ∀ v, A.countP (fun y => y ≤ v) = B.countP (fun y => y ≤ v)) :
    A.Perm B := by
  apply List.perm_iff_count.mpr
  intro c
  have hleA := countP_le_eq_sum_do A c
  have hleB := countP_le_eq_sum_do B c
  rw [countP_lt_eq_pred] at hleA
  rw [countP_lt_eq_pred] at hleB
  have hleA' := hleA
  rw [hcnt c, hcnt (c-1)] at hleA'
  have heqA : A.countP (fun y => y == c) = B.countP (fun y => y == c) := by omega
  simpa [List.count_eq_countP] using heqA

private theorem take_eq_of_countLE {S T : List Int}
    (hs : S.Pairwise (fun a b => a ≤ b)) (ht : T.Pairwise (fun a b => a ≤ b))
    (k : Nat)
    (hcnt : ∀ v, (S.take k).countP (fun y => y ≤ v) = (T.take k).countP (fun y => y ≤ v)) :
    S.take k = T.take k := by
  have hperm := count_eq_of_countLE hcnt
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
  · intro a b ha hb hab hba; omega
  · exact List.Pairwise.take hs
  · exact List.Pairwise.take ht
  · exact hperm

theorem BoxCertificate_sound :
    TinylambOpt.ResearchGoals.Frontiers.RankedJoins.BoxCertificate.Goal := by
  intro boxes seed k cutoff hseed hbox
  let allv := boxes.flatMap Prod.snd
  let openv := (boxes.filter (fun b : Int × List Int => b.1 ≤ cutoff)).flatMap Prod.snd
  let S := (seed ++ allv).mergeSort
  let T := (seed ++ openv).mergeSort
  have hSsort : S.Pairwise (fun a b : Int => a ≤ b) := by simpa [S] using sortedInt (seed ++ allv)
  have hTsort : T.Pairwise (fun a b : Int => a ≤ b) := by simpa [T] using sortedInt (seed ++ openv)
  have hSPerm : S.Perm (seed ++ allv) := by simpa [S] using List.mergeSort_perm (seed ++ allv) (fun a b : Int => decide (a ≤ b))
  have hTPerm : T.Perm (seed ++ openv) := by simpa [T] using List.mergeSort_perm (seed ++ openv) (fun a b : Int => decide (a ≤ b))
  have hcutT : k ≤ T.countP (fun y => y ≤ cutoff) := by
    rw [hTPerm.countP_eq (fun y => y ≤ cutoff)]
    simp [openv]
    have hs : k ≤ seed.countP (fun y => y ≤ cutoff) := hseed
    omega
  have hcutS : k ≤ S.countP (fun y => y ≤ cutoff) := by
    rw [hSPerm.countP_eq (fun y => y ≤ cutoff)]
    simp [allv]
    omega
  have hwhole_le : ∀ v, v ≤ cutoff → S.countP (fun y => y ≤ v) = T.countP (fun y => y ≤ v) := by
    intro v hv
    rw [hSPerm.countP_eq (fun y => y ≤ v), hTPerm.countP_eq (fun y => y ≤ v)]
    simp only [allv, openv, List.countP_append, List.countP_flatMap]
    have hzero : ∀ b ∈ boxes, ¬ b.1 ≤ cutoff → b.2.countP (fun y => y ≤ v) = 0 := by
      intro b hb hbc
      rw [List.countP_eq_zero]
      intro y hy hylv
      have hyv : y ≤ v := by simpa using hylv
      have hby := hbox b hb y hy
      omega
    have hsum : (boxes.map fun b : Int × List Int => b.2.countP (fun y => y ≤ v)).sum =
        ((boxes.filter (fun b : Int × List Int => b.1 ≤ cutoff)).map
          fun b : Int × List Int => b.2.countP (fun y => y ≤ v)).sum := by
      apply sum_filter_of_zero
      intro b hb hbc
      have hbc' : ¬ b.1 ≤ cutoff := by
        intro hle
        exact hbc (by simpa using hle)
      exact hzero b hb hbc' 
    exact congrArg (fun n : Nat => (seed.countP (fun y => y ≤ v)) + n) hsum
  have hcnt : ∀ v, (S.take k).countP (fun y => y ≤ v) = (T.take k).countP (fun y => y ≤ v) := by
    intro v
    rw [countLE_take _ hSsort v k, countLE_take _ hTsort v k]
    by_cases hv : v ≤ cutoff
    · rw [hwhole_le v hv]
    · have hSv : k ≤ S.countP (fun y => y ≤ v) := by
        have hmono : S.countP (fun y => y ≤ cutoff) ≤ S.countP (fun y => y ≤ v) := by
          apply List.countP_mono_left
          intro y hy hyle
          have hyc : y ≤ cutoff := by simpa using hyle
          have hyv : y ≤ v := by omega
          simp [hyv]
        omega
      have hTv : k ≤ T.countP (fun y => y ≤ v) := by
        have hmono : T.countP (fun y => y ≤ cutoff) ≤ T.countP (fun y => y ≤ v) := by
          apply List.countP_mono_left
          intro y hy hyle
          have hyc : y ≤ cutoff := by simpa using hyle
          have hyv : y ≤ v := by omega
          simp [hyv]
        omega
      rw [Nat.min_eq_left hSv, Nat.min_eq_left hTv]
  have htake := take_eq_of_countLE hSsort hTsort k hcnt
  simpa [best, S, T] using htake


private theorem sum_le_of_sublist {l₁ l₂ : List Nat} (h : l₁.Sublist l₂) : l₁.sum ≤ l₂.sum := by
  induction h with
  | slnil => simp
  | cons a h ih => simp [List.sum_cons]; omega
  | cons_cons a h ih => simp [List.sum_cons]; omega

private theorem map_sum_le_of_sublist {α} (l₁ l₂ : List α) (f g : α → Nat)
    (hfg : ∀ x ∈ l₂, f x ≤ g x) (h : l₁.Sublist l₂) :
    (l₁.map f).sum ≤ (l₂.map g).sum := by
  induction h with
  | slnil => simp
  | cons a h ih =>
      simp only [List.map_cons, List.sum_cons]
      have := ih (by intro x hx; exact hfg x (by simp [hx]))
      omega
  | cons_cons a h ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := hfg a (by simp)
      have := ih (by intro x hx; exact hfg x (by simp [hx]))
      omega

private theorem countP_take_add_drop_iw {α} (p : α → Bool) (s : List α) (n : Nat) :
    (s.take n).countP p + (s.drop n).countP p = s.countP p := by
  rw [← List.countP_append, List.take_append_drop]

private theorem perm_of_nodup_subset_length_eq {α} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hsub : l₁ ⊆ l₂) (hlen : l₁.length = l₂.length) :
    l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have : l₂ = [] := by
        have := hlen
        cases l₂ with
        | nil => rfl
        | cons a t => simp at this
      simp [this]
  | cons a t ih =>
      have ha : a ∈ l₂ := hsub (by simp)
      have hnd₁' := List.pairwise_cons.mp hnd₁
      have htsub : t ⊆ l₂.erase a := by
        intro x hx
        have hxa : x ∈ l₂ := hsub (by simp [hx])
        have hxa_ne : x ≠ a := by
          intro heq
          exact hnd₁'.1 x hx heq.symm
        exact (List.mem_erase_of_ne hxa_ne).2 hxa
      have hlen' : t.length = (l₂.erase a).length := by
        have hlen0 : t.length + 1 = l₂.length := by simpa using hlen
        rw [List.length_erase]
        simp [ha]
        omega
      have hperm := ih hnd₁'.2 (hnd₂.erase a) htsub hlen'
      exact (hperm.cons a).trans (List.perm_cons_erase ha).symm

private theorem zipIdx_nodup {α} (l : List α) (i : Nat) : (l.zipIdx i).Nodup := by
  induction l generalizing i with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons]
      refine List.Pairwise.cons ?_ (ih (i+1))
      intro p hp hpeq
      rcases List.mem_iff_getElem?.mp hp with ⟨j, hj⟩
      rw [List.getElem?_zipIdx] at hj
      rcases Option.map_eq_some_iff.mp hj with ⟨y, hy, hpval⟩
      have hp2 : p.2 = i + 1 + j := (congrArg Prod.snd hpval).symm
      rw [← hpeq] at hp2
      simp at hp2
      omega

private theorem leKey_char (score : Nat → Int) (a b : Nat × Nat) :
    (List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) a b) = true ↔
      score a.1 < score b.1 ∨ (score a.1 == score b.1 ∧ a.2 ≤ b.2) := by
  simp only [List.zipIdxLE]
  by_cases hab : score a.1 ≤ score b.1
  · by_cases hba : score b.1 ≤ score a.1
    · have heq : score a.1 = score b.1 := by omega
      simp [heq]
    · have hlt : score a.1 < score b.1 := by omega
      simp [hab, hba, hlt]
  · have hlt : score b.1 ≤ score a.1 := by omega
    simp [hab, hlt]
    intro heq
    omega

private theorem countLE_take_key (score : Nat → Int) : ∀ (s : List (Nat × Nat)),
    s.Pairwise (fun a b => List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) a b) →
    ∀ (c : Nat × Nat) (k : Nat),
    (s.take k).countP (fun y => List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) y c) =
      min k (s.countP (fun y => List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) y c)) := by
  intro s hs c k
  let leK := List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y))
  have htrans : ∀ a b d, leK a b → leK b d → leK a d := by
    intro a b d hab hbd
    have ht := List.zipIdxLE_trans
      (le := fun x y : Nat => decide (score x ≤ score y))
      (by intro x y z hxy hyz; by_cases h1 : score x ≤ score y <;> by_cases h2 : score y ≤ score z <;> simp_all; omega)
    exact ht a b d hab hbd
  induction s generalizing c k with
  | nil => simp
  | cons a s ih =>
      have hs' := List.Pairwise.of_cons hs
      cases k with
      | zero => simp
      | succ k =>
          by_cases ha : leK a c
          · have hrel := ih hs' c k
            have ha' : (fun y => leK y c) a := by simpa using ha
            change List.countP (fun y => leK y c) (a :: List.take k s) =
              min (k+1) (List.countP (fun y => leK y c) (a :: s))
            rw [List.countP_cons_of_pos (p := fun y => leK y c) ha']
            change List.countP (fun y => leK y c) (List.take k s) =
              min k (List.countP (fun y => leK y c) s) at hrel
            have hcons : List.countP (fun y => leK y c) (a :: s) =
                List.countP (fun y => leK y c) s + 1 := by
              rw [List.countP_cons_of_pos (p := fun y => leK y c) ha']
            rw [hcons, hrel]
            omega
          · have hnone : ∀ y ∈ s, ¬ leK y c = true := by
              intro y hy hbt
              have hay := (List.pairwise_cons.mp hs).1 y hy
              exact ha (htrans a y c hay hbt)
            have hcnt0 : s.countP (fun y => leK y c) = 0 := List.countP_eq_zero.mpr hnone
            have hcntTake0 : (s.take k).countP (fun y => leK y c) = 0 :=
              List.countP_eq_zero.mpr (by
                intro y hy hbt
                exact hnone y ((List.take_sublist k s).subset hy) hbt)
            rw [List.take_succ_cons]
            change List.countP (fun y => leK y c) (a :: List.take k s) =
              min (k+1) (List.countP (fun y => leK y c) (a :: s))
            rw [List.countP_cons_of_neg (p := fun y => leK y c) ha]
            have hcntCons0 : List.countP (fun y => leK y c) (a :: s) = 0 := by
              rw [List.countP_cons_of_neg (p := fun y => leK y c) ha, hcnt0]
            rw [hcntTake0, hcntCons0]
            simp

private theorem zipIdx_fst_mem {α} {l : List α} {i : Nat} {p : α × Nat}
    (hp : p ∈ l.zipIdx i) : p.1 ∈ l := by
  have hmap : p.1 ∈ (l.zipIdx i).map Prod.fst := List.mem_map.mpr ⟨p, hp, rfl⟩
  simpa using hmap

private theorem zipIdx_snd_lt {α} {l : List α} {i : Nat} {p : α × Nat}
    (hp : p ∈ l.zipIdx i) : p.2 < i + l.length := by
  rcases List.mem_iff_getElem?.mp hp with ⟨j, hj⟩
  rw [List.getElem?_zipIdx] at hj
  rcases Option.map_eq_some_iff.mp hj with ⟨y, hy, hpval⟩
  have hp2 : p.2 = i + j := by simpa using (congrArg Prod.snd hpval).symm
  have hjlen : j < l.length := (List.getElem?_eq_some_iff.mp hy).1
  omega

private theorem zipIdx_snd_ge {α} {l : List α} {i : Nat} {p : α × Nat}
    (hp : p ∈ l.zipIdx i) : i ≤ p.2 := by
  rcases List.mem_iff_getElem?.mp hp with ⟨j, hj⟩
  rw [List.getElem?_zipIdx] at hj
  rcases Option.map_eq_some_iff.mp hj with ⟨y, hy, hpval⟩
  have hp2 : p.2 = i + j := by simpa using (congrArg Prod.snd hpval).symm
  omega

private theorem leKey_idx_eq (score : Nat → Int) (a b : Nat × Nat) :
    List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) a b →
    List.zipIdxLE (fun x y : Nat => decide (score x ≤ score y)) b a → a.2 = b.2 := by
  intro hab hba
  have hab' := (leKey_char score a b).mp hab
  have hba' := (leKey_char score b a).mp hba
  rcases hab' with hlt | ⟨heq, hi⟩
  · rcases hba' with hlt2 | ⟨heq2, hi2⟩
    · omega
    · have hbaeq : score b.1 = score a.1 := by simpa using heq2
      omega
  · have heq1 : score a.1 = score b.1 := by simpa using heq
    rcases hba' with hlt2 | ⟨heq2, hi2⟩
    · omega
    · omega

private theorem zipIdx_same_index_eq {α} {xs ys : List α}
    {a b : α × Nat} (ha : a ∈ (xs ++ ys).zipIdx) (hb : b ∈ xs.zipIdx) (hidx : a.2 = b.2) : a = b := by
  rcases List.mem_iff_getElem?.mp ha with ⟨ja, hja⟩
  rw [List.getElem?_zipIdx] at hja
  rcases Option.map_eq_some_iff.mp hja with ⟨va, hva, haeq⟩
  have ha2 : a.2 = ja := by simpa using (congrArg Prod.snd haeq).symm
  rcases List.mem_iff_getElem?.mp hb with ⟨jb, hjb⟩
  rw [List.getElem?_zipIdx] at hjb
  rcases Option.map_eq_some_iff.mp hjb with ⟨vb, hvb, hbeq⟩
  have hb2 : b.2 = jb := by simpa using (congrArg Prod.snd hbeq).symm
  have hj : ja = jb := by omega
  have hjblt : jb < xs.length := (List.getElem?_eq_some_iff.mp hvb).1
  have hjal : ja < xs.length := by omega
  have hva2 : xs[ja]? = some va := by
    rw [← List.getElem?_append_left hjal]
    exact hva
  have hva' : xs[ja] = va := (List.getElem?_eq_some_iff.mp hva2).2
  have hvb' : xs[jb] = vb := (List.getElem?_eq_some_iff.mp hvb).2
  rw [← haeq, ← hbeq]
  congr
  · have hopt : some va = some vb := by
      rw [← hva2, ← hvb, hj]
    exact Option.some.inj hopt

theorem IntervalWinners_sound :
    TinylambOpt.ResearchGoals.Frontiers.RankedJoins.IntervalWinners.Goal := by
  intro winners rest lo hi score hbound hsep
  let n := winners.length
  let cmp := fun x y : Nat => decide (score x ≤ score y)
  let leK := List.zipIdxLE cmp
  let U := (winners ++ rest).zipIdx
  let Wk := winners.zipIdx
  let Rk := rest.zipIdx n
  let SK := U.mergeSort leK
  let WK := Wk.mergeSort leK
  have htransBool : ∀ a b c : Nat, cmp a b → cmp b c → cmp a c := by
    intro a b c hab hbc
    by_cases h1 : score a ≤ score b <;> by_cases h2 : score b ≤ score c <;> simp_all [cmp] <;> omega
  have htotalBool : ∀ a b : Nat, cmp a b || cmp b a := by
    intro a b
    by_cases h : score a ≤ score b <;> simp [cmp, h] <;> omega
  have hSKsort : SK.Pairwise (fun a b : Nat × Nat => leK a b) := by
    have h := List.pairwise_mergeSort (le := leK) (List.zipIdxLE_trans htransBool) (List.zipIdxLE_total htotalBool) U
    have h' : SK.Pairwise (fun a b : Nat × Nat => leK a b) := by simpa [SK, leK, cmp] using h
    exact h'.imp (by intro a b hab; simpa using hab)
  have hWKsort : WK.Pairwise (fun a b : Nat × Nat => leK a b) := by
    have h := List.pairwise_mergeSort (le := leK) (List.zipIdxLE_trans htransBool) (List.zipIdxLE_total htotalBool) Wk
    have h' : WK.Pairwise (fun a b : Nat × Nat => leK a b) := by simpa [WK, leK, cmp] using h
    exact h'.imp (by intro a b hab; simpa using hab)
  have hSKperm : SK.Perm U := by simpa [SK, leK, cmp] using List.mergeSort_perm U leK
  have hWKperm : WK.Perm Wk := by simpa [WK, leK, cmp] using List.mergeSort_perm Wk leK
  have hUzip : U = Wk ++ Rk := by
    simp [U, Wk, Rk, n, List.zipIdx_append]
  have hnodupWk : Wk.Nodup := by simpa [Wk] using zipIdx_nodup winners 0
  have hnodupRk : Rk.Nodup := by simpa [Rk, n] using zipIdx_nodup rest winners.length
  have hdisj : ∀ r ∈ Rk, r ∉ Wk := by
    intro r hr hrW
    have hrge : winners.length ≤ r.2 := by
      have hsnd := zipIdx_snd_lt (l := rest) (i := winners.length) hr
      rcases List.mem_iff_getElem?.mp hr with ⟨j, hj⟩
      rw [List.getElem?_zipIdx] at hj
      rcases Option.map_eq_some_iff.mp hj with ⟨y, hy, hrval⟩
      have hr2 : r.2 = winners.length + j := by simpa using (congrArg Prod.snd hrval).symm
      omega
    have hwlt := zipIdx_snd_lt hrW
    omega
  have hcntW : SK.countP (fun q : Nat × Nat => decide (q ∈ Wk)) = n := by
    rw [hSKperm.countP_eq (fun q : Nat × Nat => decide (q ∈ Wk))]
    rw [hUzip]
    simp only [List.countP_append]
    have hwall : Wk.countP (fun q : Nat × Nat => decide (q ∈ Wk)) = Wk.length :=
      List.countP_eq_length.mpr (by intro q hq; simp [hq])
    have hrnone : Rk.countP (fun q : Nat × Nat => decide (q ∈ Wk)) = 0 :=
      List.countP_eq_zero.mpr (by intro q hq hmem; exact hdisj q hq (by simpa using hmem))
    rw [hwall, hrnone]
    simp [Wk, n]
  let A := SK.take n
  let D := SK.drop n
  have hAnodup : A.Nodup := by
    have hnodupSK : SK.Nodup := hSKperm.symm.nodup (by simpa [U] using zipIdx_nodup (winners ++ rest) 0)
    exact List.Pairwise.take hnodupSK
  have hnSK : n ≤ SK.length := by
    have hlen := hSKperm.length_eq
    simp [U] at hlen
    omega
  have hAnolen : A.length = n := by simp [A, List.length_take_of_le hnSK]
  have hnoRest : ∀ r ∈ Rk, r ∉ A := by
    intro r hr hrA
    have htake := countP_take_add_drop_iw (fun q : Nat × Nat => decide (q ∈ Wk)) SK n
    rw [hcntW] at htake
    change A.countP (fun q : Nat × Nat => decide (q ∈ Wk)) + D.countP (fun q : Nat × Nat => decide (q ∈ Wk)) = n at htake
    have hnotall : ¬ ∀ q ∈ A, q ∈ Wk := by
      intro hall
      exact hdisj r hr (by simpa using hall r hrA)
    have hcountAlt : A.countP (fun q : Nat × Nat => decide (q ∈ Wk)) < n := by
      by_cases he : A.countP (fun q : Nat × Nat => decide (q ∈ Wk)) = A.length
      · have hall' : ∀ q ∈ A, q ∈ Wk := by
          intro q hq
          exact by simpa using (List.countP_eq_length.mp he q hq)
        exact False.elim (hnotall hall')
      · have hle := List.countP_le_length (p := fun q : Nat × Nat => decide (q ∈ Wk)) (l := A)
        omega
    have hcountDpos : 0 < D.countP (fun q : Nat × Nat => decide (q ∈ Wk)) := by omega
    rcases List.countP_pos_iff.mp hcountDpos with ⟨w, hwD, hwWb⟩
    have hwW : w ∈ Wk := by simpa using hwWb
    have hrel := List.Pairwise.rel_of_mem_take_of_mem_drop hSKsort hrA hwD
    have hrel' := (leKey_char score r w).mp (by simpa [leK, cmp] using hrel)
    have hwmem : w.1 ∈ winners := by
      exact zipIdx_fst_mem hwW
    have hrmem : r.1 ∈ rest := by
      exact zipIdx_fst_mem hr
    have hwbAll : w.1 ∈ winners ++ rest := by simp [hwmem]
    have hrbAll : r.1 ∈ winners ++ rest := by simp [hrmem]
    have hwb := hbound w.1 hwbAll
    have hrb := hbound r.1 hrbAll
    have hwup : score w.1 ≤ hi w.1 := hwb.2
    have hrlo : lo r.1 ≤ score r.1 := hrb.1
    have hltwr0 : hi w.1 < lo r.1 := hsep w.1 hwmem r.1 hrmem
    have hltwr : score w.1 < score r.1 := by omega
    rcases hrel' with hlt | ⟨heq, hi⟩
    · omega
    · have heq' : score r.1 = score w.1 := by simpa using heq
      omega
  have hAsub : A ⊆ Wk := by
    intro q hq
    have hqSK : q ∈ SK := (List.take_subset n SK) hq
    have hqU : q ∈ U := hSKperm.mem_iff.mp hqSK
    rw [hUzip] at hqU
    rcases List.mem_append.mp hqU with hqW | hqR
    · exact hqW
    · exact False.elim (hnoRest q hqR hq)
  have hlenAW : A.length = Wk.length := by
    rw [hAnolen]
    simp [Wk, n]
  have hpermAW : A.Perm Wk := perm_of_nodup_subset_length_eq hAnodup hnodupWk hAsub hlenAW
  have hAeq : A = WK := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat × Nat => leK a b)
    · intro a b ha hb hab hba
      have hidx := leKey_idx_eq score a b (by simpa [leK, cmp] using hab) (by simpa [leK, cmp] using hba)
      have haU : a ∈ U := hSKperm.mem_iff.mp ((List.take_subset n SK) ha)
      have hbW : b ∈ Wk := hWKperm.mem_iff.mp hb
      rw [hUzip] at haU
      rcases List.mem_append.mp haU with haW | haR
      · have haW' : a ∈ (winners ++ ([] : List Nat)).zipIdx := by simpa using haW
        exact zipIdx_same_index_eq haW' hbW hidx
      · have hbIdx := zipIdx_snd_lt hbW
        have haIdx := zipIdx_snd_ge haR
        omega
    · exact List.Pairwise.take hSKsort
    · exact hWKsort
    · exact hpermAW.trans hWKperm.symm
  have hmapA : (A.map Prod.fst) = WK.map Prod.fst := congrArg (fun l : List (Nat × Nat) => l.map Prod.fst) hAeq
  have hmapSK : SK.map Prod.fst = (winners ++ rest).mergeSort cmp := by
    simpa [SK, U, leK, cmp] using (List.mergeSort_zipIdx (l := winners ++ rest) (le := cmp))
  have hmapWK : WK.map Prod.fst = winners.mergeSort cmp := by
    simpa [WK, Wk, leK, cmp] using (List.mergeSort_zipIdx (l := winners) (le := cmp))
  have htakeMap : (SK.take n).map Prod.fst = (SK.map Prod.fst).take n := by simp
  change ((winners ++ rest).mergeSort cmp).take n = winners.mergeSort cmp
  rw [← hmapSK, ← hmapWK, ← htakeMap, hmapA]



/-! Counterexample for `MeanHoleBudget` (from `issues/research/frontiers.md`):
    `a = b = [0,1,...,8]`, `ok x y = !(x < 2 && y < 2)`, `k = 1`, `h = 1`.
    Total holes = 4 ≤ 1 * min 9 9 = 9, so the premise holds.
    `best 1 (qualified a b ok) = [2]`, while
    `best 1 (qualified (best 2 a) (best 2 b) ok) = []` — the two sides differ. -/
theorem refutation_MeanHoleBudget :
    ¬ TinylambOpt.ResearchGoals.Frontiers.RankedJoins.MeanHoleBudget.Goal := by
  intro h
  let axis : List Int := (List.range 9).map Int.ofNat
  let allowed : Int → Int → Bool := fun x y => !(x < 2 && y < 2)
  have hmean : (axis.map (fun x => axis.countP (fun y => !allowed x y))).sum ≤
      1 * min axis.length axis.length := by
    native_decide
  have heq := h axis axis allowed 1 1 hmean
  have hneq : best 1 (qualified axis axis allowed) ≠
      best 1 (qualified (best 2 axis) (best 2 axis) allowed) := by
    native_decide
  exact hneq heq


end TinylambOpt.ResearchProofs.Frontiers.RankedJoins

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
