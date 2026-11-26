import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_PairHistogramByRadixRuns. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_PairHistogramByRadixRuns

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# PairHistogramByRadixRuns

Disjoint radix lanes partition composite-key rows: every row lands in the single
lane determined by its first coordinate. For each histogram entry, at most one
lane contributes a count, and that lane sees exactly the same multiplicity as a
global scan. Summing local `frequency` maps therefore preserves every key/count
pair, including tied groups and duplicate rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites (frequency)
open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping (nodup_eraseDups)

theorem krs_nodup_map_inj {α β : Type} (f : α → β) (hf : Function.Injective f) :
    ∀ {l : List α}, l.Nodup → (l.map f).Nodup := by
  intro l hl
  induction l with
  | nil => simp
  | cons a l ih =>
      have hhead : a ∉ l := (List.nodup_cons.mp hl).1
      have htail : l.Nodup := (List.nodup_cons.mp hl).2
      change (f a :: l.map f).Nodup
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨x, hx, hfx⟩
        have hxa : x = a := hf hfx
        exact hhead (by simpa [hxa] using hx)
      · exact ih htail

theorem krs_count_eq_one_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} (hl : l.Nodup) {a : α} (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      have hhead : x ∉ xs := (List.nodup_cons.mp hl).1
      have htail : xs.Nodup := (List.nodup_cons.mp hl).2
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · have h0 : xs.count a = 0 := List.count_eq_zero.mpr hhead
        simp [h0]
      · have hxa : x ≠ a := by
          intro hxa
          subst x
          exact hhead ha
        have hia := ih htail ha
        simp [hxa, hia]

theorem krs_count_frequency (xs : List (Nat × Nat)) (a : Nat × Nat) (n : Nat) :
    (frequency xs).count (a, n) = if a ∈ xs ∧ xs.count a = n then 1 else 0 := by
  unfold frequency
  by_cases hmem : a ∈ xs
  · have hnd : (xs.eraseDups.map (fun x : Nat × Nat => (x, xs.count x))).Nodup :=
      krs_nodup_map_inj (fun x : Nat × Nat => (x, xs.count x)) (by
        intro x y h
        exact congrArg Prod.fst h) (nodup_eraseDups xs)
    have hpairmem :
        (a, n) ∈ xs.eraseDups.map (fun x : Nat × Nat => (x, xs.count x)) ↔ xs.count a = n := by
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨x, hx, hpx⟩
        have hxa : x = a := congrArg Prod.fst hpx
        subst x
        simpa using congrArg Prod.snd hpx
      · intro hn
        have hmEd : a ∈ xs.eraseDups := by simpa using hmem
        exact List.mem_map.mpr ⟨a, hmEd, by simp [hn]⟩
    by_cases hn : xs.count a = n
    · have hp := hpairmem.mpr hn
      have hc := krs_count_eq_one_of_nodup_mem hnd hp
      simp [hmem, hn, hc]
    · have hpnot : (a, n) ∉ xs.eraseDups.map (fun x : Nat × Nat => (x, xs.count x)) := by
        intro hp
        exact hn (hpairmem.mp hp)
      have hc := List.count_eq_zero.mpr hpnot
      simp [hmem, hn, hc]
  · have hnotEd : a ∉ xs.eraseDups := by simpa using hmem
    have hpnot : (a, n) ∉ xs.eraseDups.map (fun x : Nat × Nat => (x, xs.count x)) := by
      intro hp
      rcases List.mem_map.mp hp with ⟨x, hx, hpx⟩
      have hxa : x = a := congrArg Prod.fst hpx
      subst x
      exact hnotEd hx
    have hc := List.count_eq_zero.mpr hpnot
    simp [hmem, hc]

theorem krs_count_flatMap {α β : Type} [BEq β] [LawfulBEq β]
    (l : List α) (f : α → List β) (b : β) :
    (l.flatMap f).count b = (l.map (fun i => (f i).count b)).sum := by
  induction l with
  | nil => simp
  | cons i l ih => simp [ih, List.count_append]

theorem krs_sum_zero_map (span : Nat) : ((List.range span).map (fun _ : Nat => 0)).sum = 0 := by
  induction span with
  | zero => simp
  | succ span ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp [ih]

theorem krs_range_pick (d span v : Nat) (hd : d < span) :
    ((List.range span).map (fun i => if i = d then v else 0)).sum = v := by
  induction span with
  | zero => omega
  | succ span ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      by_cases hlt : d < span
      · have ih' := ih hlt
        have hne : span ≠ d := by omega
        simp [hne, ih']
      · have hdeq : d = span := by omega
        have hzero : ((List.range span).map (fun i => if i = span then v else 0)).sum = 0 := by
          have hm : (List.range span).map (fun i => if i = span then v else 0) =
              (List.range span).map (fun _ : Nat => 0) := by
            apply List.map_congr_left
            intro i hi
            have hi_lt : i < span := by simpa using hi
            simp [show i ≠ span by omega]
          rw [hm]
          exact krs_sum_zero_map span
        simp [hdeq, hzero]

theorem krs_range_pick_beq (d span v : Nat) (hd : d < span) :
    ((List.range span).map (fun i => if d == i then v else 0)).sum = v := by
  have hm : (List.range span).map (fun i => if d == i then v else 0) =
      (List.range span).map (fun i => if i = d then v else 0) := by
    apply List.map_congr_left
    intro i hi
    by_cases hdi : d == i
    · have hi_eq : i = d := (beq_iff_eq.mp hdi).symm
      simp [hdi, hi_eq]
    · have hd_ne : d ≠ i := by
        intro hde
        exact hdi (by simpa [hde])
      have hi_ne : i ≠ d := by
        intro hid
        exact hd_ne hid.symm
      simp [hdi, hi_ne]
  rw [hm]
  exact krs_range_pick d span v hd

theorem krs_frequency_partition_perm (rows : List (Nat × Nat)) (radix : Nat) (hpos : 0 < radix) :
    ((List.range radix).flatMap
        (fun i => frequency (rows.filter (fun r : Nat × Nat => r.1 % radix == i)))).Perm
      (frequency rows) := by
  apply List.perm_iff_count.mpr
  intro q
  rcases q with ⟨a, n⟩
  rw [krs_count_flatMap]
  have hcnt :
      (List.range radix).map
          (fun i => (frequency (rows.filter (fun r : Nat × Nat => r.1 % radix == i))).count (a, n)) =
        (List.range radix).map
          (fun i => if a.1 % radix == i then (if a ∈ rows ∧ rows.count a = n then 1 else 0) else 0) := by
    apply List.map_congr_left
    intro i hi
    by_cases hiEq : a.1 % radix == i
    · have hiTrue : (fun r : Nat × Nat => r.1 % radix == i) a = true := by simpa using hiEq
      have hcntL : (rows.filter (fun r : Nat × Nat => r.1 % radix == i)).count a = rows.count a := by
        exact List.count_filter (p := fun r : Nat × Nat => r.1 % radix == i)
          (a := a) (l := rows) hiTrue
      have hmemL : a ∈ rows.filter (fun r : Nat × Nat => r.1 % radix == i) ↔ a ∈ rows := by
        constructor
        · intro h
          exact (List.mem_filter.mp h).1
        · intro h
          exact List.mem_filter.mpr ⟨h, hiTrue⟩
      rw [krs_count_frequency (rows.filter (fun r : Nat × Nat => r.1 % radix == i)) a n]
      simp [hmemL, hcntL, hiEq]
    · have hmemLfalse : a ∉ rows.filter (fun r : Nat × Nat => r.1 % radix == i) := by
        intro h
        exact hiEq (List.mem_filter.mp h).2
      have hcntL0 : (rows.filter (fun r : Nat × Nat => r.1 % radix == i)).count a = 0 :=
        List.count_eq_zero.mpr hmemLfalse
      rw [krs_count_frequency (rows.filter (fun r : Nat × Nat => r.1 % radix == i)) a n]
      simp [hmemLfalse, hcntL0, hiEq]
  rw [hcnt]
  have hlt : a.1 % radix < radix := Nat.mod_lt a.1 hpos
  rw [krs_range_pick_beq (a.1 % radix) radix (if a ∈ rows ∧ rows.count a = n then 1 else 0) hlt]
  rw [krs_count_frequency rows a n]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PairHistogramByRadixRuns.Goal := by
  intro rows radix hpos
  dsimp
  exact krs_frequency_partition_perm rows radix hpos
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_PairHistogramByRadixRuns
