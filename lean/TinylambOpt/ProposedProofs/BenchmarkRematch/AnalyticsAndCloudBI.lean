import TinylambOpt.ProposedGoals.BenchmarkRematch.AnalyticsAndCloudBI
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# Analytics and Cloud/BI benchmark rematch proofs

This module proves the fourteen executable equivalences proposed for the losing
Analytics-5 and Cloud/BI benchmark queries.  The results permit pre-aggregation,
dictionary-grid accumulation, predicate reordering, block-extrema reduction, and
positional-delete subtraction while preserving the modeled SQL sums and counts.
No floating-point reassociation or change to SQL NULL semantics is modeled here.
-/

namespace TinylambOpt.ProposedProofs.BenchmarkRematch.AnalyticsAndCloudBI

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.AnalyticsAndCloudBI

/-- Mapping a constant over a list scales its length in exact integer arithmetic. -/
private theorem sum_map_const {α : Type} (xs : List α) (z : Int) :
    (xs.map (fun _ => z)).sum = (xs.length : Int) * z := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul, Int.add_comm]

/-- Summing a right-side payload over a Cartesian product factors out the left
multiplicity. -/
private theorem sum_flatMap_right_payload {α β : Type} (xs : List α)
    (ys : List β) (f : β → Int) :
    (xs.flatMap (fun _ => ys.map f)).sum = (xs.length : Int) * (ys.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add, Int.add_mul, Int.add_comm]

/-- Summing a left-side payload over a Cartesian product factors out the right
multiplicity. -/
private theorem sum_flatMap_left_payload {α β : Type} (xs : List α)
    (ys : List β) (f : α → Int) :
    (xs.flatMap (fun x => ys.map (fun _ => f x))).sum =
      (xs.map f).sum * (ys.length : Int) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, sum_map_const, Int.add_mul, Int.mul_comm]

/-- The sum of a flattened list is the sum of the sums of its mapped blocks. -/
private theorem sum_flatMap_blocks {α : Type} (xs : List α) (f : α → List Int) :
    (xs.flatMap f).sum = (xs.map (fun x => (f x).sum)).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

/-- List permutation preserves an exact integer sum. -/
private theorem sum_eq_of_perm {xs ys : List Int} (h : xs.Perm ys) :
    xs.sum = ys.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simp [ih]
  | swap x y xs => simp [Int.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

/-- A conjunction filter is the same as filtering by its right component and then
its left component. -/
private theorem filter_and_order {α : Type} (xs : List α) (p q : α → Bool) :
    xs.filter (fun x => p x && q x) = (xs.filter q).filter p := by
  exact (List.filter_filter (p := p) (q := q) (l := xs)).symm

/-- Two Boolean filters can be evaluated in either order, provided their
conjunction remains unchanged. -/
private theorem filter_and_comm {α : Type} (xs : List α) (p q : α → Bool) :
    xs.filter (fun x => p x && q x) = (xs.filter p).filter q := by
  calc
    xs.filter (fun x => p x && q x) = xs.filter (fun x => q x && p x) := by
      have hpred : (fun x : α => p x && q x) = (fun x => q x && p x) := by
        funext x
        exact Bool.and_comm _ _
      rw [hpred]
    _ = (xs.filter p).filter q :=
      (List.filter_filter (p := q) (q := p) (l := xs)).symm

/-- For a fixed movie identifier, optional production years sum to the movie's
multiplicity times that year, with zero contribution for a missing year. -/
private theorem sum_filterMap_year_bucket (casts : List (Nat × Nat))
    (yearOf : Nat → Option Int) (mid : Nat) :
    ((casts.filter (fun c => c.1 == mid)).filterMap (fun c => yearOf c.1)).sum =
      ((casts.filter (fun c => c.1 == mid)).length : Int) * (yearOf mid).getD 0 := by
  induction casts with
  | nil => simp
  | cons c cs ih =>
      by_cases hc : c.1 == mid
      · have heq : c.1 = mid := beq_iff_eq.mp hc
        rw [List.filter_cons_of_pos (p := fun c : Nat × Nat => c.1 == mid)
          (a := c) (l := cs) hc]
        rw [List.filterMap_cons, heq]
        cases hy : yearOf mid <;>
          simp [hy, ih, Int.natCast_add, Int.add_mul, Int.add_comm]
      · rw [List.filter_cons_of_neg (p := fun c : Nat × Nat => c.1 == mid)
          (a := c) (l := cs) hc]
        simpa using ih

/-- A grouped movie bucket has exactly the weighted optional-year contribution
used by the gender aggregation. -/
private theorem sum_filterMap_key_bucket (casts : List (Nat × Nat))
    (yearOf : Nat → Option Int) (mid : Nat) :
    (((casts.filter (fun c => c.1 == mid)).map Prod.fst).filterMap yearOf).sum =
      ((yearOf mid).map (fun y => ((casts.map Prod.fst).count mid : Int) * y)).getD 0 := by
  have h := sum_filterMap_year_bucket casts yearOf mid
  have hleft :
      (((casts.filter (fun c => c.1 == mid)).map Prod.fst).filterMap yearOf).sum =
      ((casts.filter (fun c => c.1 == mid)).filterMap (fun c => yearOf c.1)).sum := by
    simp [Function.comp_def]
  rw [hleft]
  cases hy : yearOf mid with
  | none => simpa [hy] using h
  | some y =>
      rw [hy] at h
      simp only [Option.map_some, Option.getD_some, Option.getD_some] at h ⊢
      rw [List.count_eq_length_filter]
      simpa [List.filter_map, Function.comp_def] using h

/-- Summing filtered mapped optional values equals summing their zero-defaulted
values over all keys. -/
private theorem sum_filterMap_option_map {α β : Type} (xs : List α)
    (f : α → Option β) (g : α → β → Int) :
    (xs.filterMap (fun x => (f x).map (g x))).sum =
      (xs.map (fun x => ((f x).map (g x)).getD 0)).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases hx : f x <;> simp [hx, ih]

/-- Weighting distinct movie identifiers by their cast multiplicity reproduces
the ungrouped optional-year sum. -/
private theorem sum_filterMap_eraseDups_weighted (casts : List (Nat × Nat))
    (yearOf : Nat → Option Int) :
    (((casts.map Prod.fst).eraseDups).filterMap (fun mid =>
      (yearOf mid).map (fun y => ((casts.map Prod.fst).count mid : Int) * y))).sum =
      (casts.filterMap (fun c => yearOf c.1)).sum := by
  let rows := casts.map (fun c => (c.1, c.1))
  have hp := TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping.forKey_filter_perm
    rows (fun _ => true)
  have hp' : (casts.map Prod.fst).Perm
      (((casts.map Prod.fst).eraseDups).flatMap
        (fun mid => (casts.filter (fun c => c.1 == mid)).map Prod.fst)) := by
    have htrue {α : Type} (l : List α) : l.filter (fun _ => true) = l :=
      List.filter_eq_self.mpr (by intro a _; rfl)
    simp only [rows] at hp
    unfold forKey at hp
    simp only [htrue, List.map_map, Function.comp_def, List.filter_map] at hp
    exact hp
  have hs := sum_eq_of_perm (hp'.filterMap (fun mid => yearOf mid))
  rw [List.filterMap_flatMap, sum_flatMap_blocks] at hs
  have hbuckets :
      (((casts.map Prod.fst).eraseDups).map (fun mid =>
        (((casts.filter (fun c => c.1 == mid)).map Prod.fst).filterMap yearOf).sum)) =
      (((casts.map Prod.fst).eraseDups).map (fun mid =>
        ((yearOf mid).map (fun y => ((casts.map Prod.fst).count mid : Int) * y)).getD 0)) := by
    apply List.map_congr_left
    intro mid _
    exact sum_filterMap_key_bucket casts yearOf mid
  rw [hbuckets] at hs
  rw [sum_filterMap_option_map]
  simpa [List.filterMap_map, Function.comp_def] using hs.symm

/-- A bounded second dictionary coordinate makes the flat grid key injective:
equality of encoded keys forces equality of both original coordinates. -/
private theorem grid_left_eq_of_encoded_eq {d2 c1 c2 r1 r2 : Nat}
    (hr2 : r2 < d2) (hc2 : c2 < d2)
    (h : r1 * d2 + r2 = c1 * d2 + c2) : r1 = c1 := by
  by_cases heq : r1 = c1
  · exact heq
  · rcases Nat.lt_or_gt_of_ne heq with hlt | hgt
    · have hmul : (r1 + 1) * d2 ≤ c1 * d2 := Nat.mul_le_mul_right d2 hlt
      have hstrict : r1 * d2 + r2 < c1 * d2 + c2 := by
        calc
          r1 * d2 + r2 < r1 * d2 + d2 := Nat.add_lt_add_left hr2 _
          _ = (r1 + 1) * d2 := by rw [Nat.add_mul]; simp
          _ ≤ c1 * d2 := hmul
          _ ≤ c1 * d2 + c2 := Nat.le_add_right _ _
      omega
    · have hmul : (c1 + 1) * d2 ≤ r1 * d2 := Nat.mul_le_mul_right d2 hgt
      have hstrict : c1 * d2 + c2 < r1 * d2 + r2 := by
        calc
          c1 * d2 + c2 < c1 * d2 + d2 := Nat.add_lt_add_left hc2 _
          _ = (c1 + 1) * d2 := by rw [Nat.add_mul]; simp
          _ ≤ r1 * d2 := hmul
          _ ≤ r1 * d2 + r2 := Nat.le_add_right _ _
      omega

/-- Folding blocks from left to right agrees with flattening them first for any
fold operation, including the block-footer `max` reduction. -/
private theorem foldl_blocks {α β : Type} (f : β → α → β) (init : β)
    (blocks : List (List α)) :
    blocks.flatten.foldl f init = blocks.foldl (fun acc b => b.foldl f acc) init := by
  induction blocks generalizing init with
  | nil => rfl
  | cons b bs ih =>
      rw [List.flatten_cons, List.foldl_append]
      simpa only [List.foldl_cons] using ih (List.foldl f init b)

/-- Removing a duplicate-free bounded position list from a duplicate-free range
leaves a complement whose concatenation with the removed list is a permutation. -/
private theorem perm_filter_not_contains_append (base del : List Nat)
    (hbase : base.Nodup) (hdel : del.Nodup) (hsub : ∀ i ∈ del, i ∈ base) :
    (base.filter (fun i => !del.contains i) ++ del).Perm base := by
  induction base generalizing del with
  | nil =>
      have hnil : del = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro i hi
        exact List.not_mem_nil (hsub i hi)
      simp [hnil]
  | cons a base ih =>
      have hbase' := List.nodup_cons.mp hbase
      by_cases ha : a ∈ del
      · let rest := del.erase a
        have hdelperm : del.Perm (a :: rest) := List.perm_cons_erase ha
        have hnothead : ¬(!del.contains a) = true := by simp [ha]
        have hfilter :
            (a :: base).filter (fun i => !del.contains i) =
              base.filter (fun i => !rest.contains i) := by
          rw [List.filter_cons_of_neg (p := fun i => !del.contains i)
            (a := a) (l := base) hnothead]
          apply List.filter_congr
          intro i hi
          have hne : i ≠ a := by
            intro heq
            subst i
            exact hbase'.1 hi
          simp [rest, List.mem_erase_of_ne hne]
        have hrestnodup : rest.Nodup := by
          have h := hdelperm.nodup_iff.mp hdel
          exact (List.nodup_cons.mp h).2
        have hrestsub : ∀ i ∈ rest, i ∈ base := by
          intro i hi
          rcases List.mem_cons.mp (hsub i (List.mem_of_mem_erase hi)) with heq | hibase
          · subst i
            exact False.elim ((hdel.not_mem_erase) hi)
          · exact hibase
        have hih := ih rest hbase'.2 hrestnodup hrestsub
        rw [hfilter]
        have happend : (base.filter (fun i => !rest.contains i) ++ del).Perm
            (base.filter (fun i => !rest.contains i) ++ a :: rest) :=
          List.Perm.append_left _ hdelperm
        exact happend.trans (List.perm_middle.trans (List.Perm.cons a hih))
      · have hfilter : (a :: base).filter (fun i => !del.contains i) =
            a :: base.filter (fun i => !del.contains i) := by
          apply List.filter_cons_of_pos
          simp [ha]
        have hdel' : ∀ i ∈ del, i ∈ base := by
          intro i hi
          exact (List.mem_cons.mp (hsub i hi)).resolve_left (by
            intro heq
            subst i
            exact ha hi)
        rw [hfilter]
        exact List.Perm.cons a (ih del hbase'.2 hdel hdel')

/-- Gathering every position of a list returns the original list. -/
private theorem valuesAt_range {α : Type} (xs : List α) :
    valuesAt xs (List.range xs.length) = xs := by
  have h : ∀ (xs : List α) (s n : Nat),
      valuesAt xs (List.range' s n) = (xs.drop s).take n := by
    intro xs s n
    induction n generalizing s with
    | zero => simp [valuesAt]
    | succ n ih =>
        rw [List.range'_succ]
        change (match xs[s]? with
          | none => List.filterMap (fun i => xs[i]?) (List.range' (s + 1) n)
          | some b => b :: List.filterMap (fun i => xs[i]?) (List.range' (s + 1) n)) =
          (xs.drop s).take (n + 1)
        have htail : List.filterMap (fun i => xs[i]?) (List.range' (s + 1) n) =
            (xs.drop (s + 1)).take n := ih (s + 1)
        cases hdrop : xs.drop s with
        | nil =>
            have hs : xs.length ≤ s := List.drop_eq_nil_iff.mp hdrop
            have hget : xs[s]? = none := List.getElem?_eq_none (by omega)
            rw [hget, htail]
            simp [List.drop_eq_nil_iff.mpr (by omega : xs.length ≤ s + 1)]
        | cons y ys =>
            have hs : s < xs.length := by
              by_cases hs : s < xs.length
              · exact hs
              · have hn := List.drop_eq_nil_iff.mpr (by omega : xs.length ≤ s)
                rw [hn] at hdrop
                cases hdrop
            have hget : xs[s]? = some y := by
              rw [List.drop_eq_getElem_cons hs] at hdrop
              cases hdrop
              exact List.getElem?_eq_getElem hs
            have htaildrop : xs.drop (s + 1) = ys := by
              simp [← List.tail_drop, hdrop]
            rw [List.take_succ_cons, hget, htail, htaildrop]
  rw [List.range_eq_range']
  simpa using h xs 0 xs.length

/-- JOB J1 may replace the company-multiplicity join expansion by a count-weighted
sum of title years; every qualifying company match contributes exactly once. -/
theorem JOB_J1_CompanyMultiplicityWeightedTitleTotals :
    JOB_J1_CompanyMultiplicityWeightedTitleTotalsGoal := by
  intro titles mcMovieIds
  induction titles with
  | nil => simp
  | cons t ts ih =>
      rw [List.flatMap_cons, List.sum_append, List.map_cons, List.sum_cons, ih]
      have hhead := sum_map_const (mcMovieIds.filter (fun m => m == t.1)) t.2
      have hc := List.count_eq_length_filter (a := t.1) (l := mcMovieIds)
      rw [hhead, hc]

/-- JOB J3 may first count cast rows per distinct movie and gender, then weight
each non-NULL title year by that count without changing the gender sum. -/
theorem JOB_J3_GenderCodeCountWeightedYearAvg :
    JOB_J3_GenderCodeCountWeightedYearAvgGoal := by
  intro casts yearOf g
  dsimp only
  exact (sum_filterMap_eraseDups_weighted (casts.filter (fun c => c.2 == g)) yearOf).symm

/-- STATS-CEB C2 may count comments per post and sum those frequencies per owner;
the owner-level COUNT is unchanged. -/
theorem STATS_C2_PostCommentFreqOwnerSum :
    STATS_C2_PostCommentFreqOwnerSumGoal := by
  intro posts comments owner
  dsimp only
  induction posts with
  | nil => simp
  | cons p ps ih =>
      by_cases hp : p.1 == owner
      · rw [List.filter_cons_of_pos (p := fun p => p.1 == owner)
          (a := p) (l := ps) hp, List.flatMap_cons, List.length_append,
          List.map_cons, List.sum_cons, ih]
        have hc := List.count_eq_length_filter (a := p.2) (l := comments)
        rw [hc]
      · rw [List.filter_cons_of_neg (p := fun p => p.1 == owner)
          (a := p) (l := ps) hp]
        exact ih

/-- STATS-CEB C3 may apply the related-post validity test before or after the
link-type test; filtering order does not change the selected score sum. -/
theorem STATS_C3_RelatedPostBitsetAndDenseScoreLookup :
    STATS_C3_RelatedPostBitsetAndDenseScoreLookupGoal := by
  intro links validRelated scoreOf lt
  unfold total
  rw [filter_and_order]

/-- H2O H2's flat dictionary-grid count equals pair-key grouping when the second
coordinate is bounded by the grid width and the probed coordinate is valid. -/
theorem H2O_H2_BoundedDictGridDirectCount :
    H2O_H2_BoundedDictGridDirectCountGoal := by
  intro rows d2 c1 c2 hbound hc2
  unfold countWhere
  congr 1
  apply List.filter_congr
  intro r hr
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hp := Bool.and_eq_true_iff.mp h
    have hleft : r.1 = c1 := beq_iff_eq.mp hp.1
    have hright : r.2 = c2 := beq_iff_eq.mp hp.2
    simp [hleft, hright]
  · intro h
    have heq : r.1 * d2 + r.2 = c1 * d2 + c2 := beq_iff_eq.mp h
    have hleft := grid_left_eq_of_encoded_eq (hbound r hr) hc2 heq
    have hright : r.2 = c2 := Nat.add_left_cancel (n := r.1 * d2)
      (by simpa [hleft] using heq)
    simp [hleft, hright]

/-- Fusing a selection with a subsequent group predicate preserves the group
count. -/
private theorem countWhere_filter {α : Type} (xs : List α) (p q : α → Bool) :
    countWhere (xs.filter p) q = countWhere xs (fun x => p x && q x) := by
  unfold countWhere
  rw [filter_and_comm]

/-- Fusing a selection with a subsequent aggregate predicate preserves the exact
integer sum. -/
private theorem total_filter {α : Type} (xs : List α) (p q : α → Bool)
    (f : α → Int) :
    total ((xs.filter p).filter q) f = total (xs.filter (fun x => p x && q x)) f := by
  unfold total
  rw [filter_and_comm]

/-- H2O H4 may materialize a sparse selection and then group, or fuse the selection
with the group predicate; both count and sum see exactly the same rows. -/
theorem H2O_H4_SparseSelectedRunsGroupAvg :
    H2O_H4_SparseSelectedRunsGroupAvgGoal := by
  intro rows bound k
  dsimp only
  apply Prod.ext
  · exact countWhere_filter rows (fun r => decide (r.1 < bound))
      (fun r => r.2.1 == k)
  · exact total_filter rows (fun r => decide (r.1 < bound))
      (fun r => r.2.1 == k) (fun r => r.2.2)

/-- H2O H5 may factor the many-to-many join sums into one total and the opposite
side's multiplicity, avoiding materialization of the Cartesian product. -/
theorem H2O_H5_FactorizedManyToManyJoinSums :
    H2O_H5_FactorizedManyToManyJoinSumsGoal := by
  intro xs ys k
  dsimp only
  constructor
  · exact sum_flatMap_left_payload (xs.filter (fun r => r.1 == k))
      (ys.filter (fun r => r.1 == k)) Prod.snd
  · exact sum_flatMap_right_payload (xs.filter (fun r => r.1 == k))
      (ys.filter (fun r => r.1 == k)) Prod.snd

/-- LDBC-SNB L2 may multiply each forum's message sum by its qualifying member
count instead of expanding every message once per member. -/
theorem LDBC_L2_ForumMemberWeightFactoring :
    LDBC_L2_ForumMemberWeightFactoringGoal := by
  intro msgs members fid
  dsimp only
  have h := sum_flatMap_left_payload
    (msgs.filter (fun m => m.1 == fid))
    (members.filter (fun m => m == fid)) (fun m => m.2)
  simpa [total, Int.mul_comm] using h

/-- LDBC-SNB L4 may combine year and parent-validity tests before gathering forum
and score values, or append the forum test afterward, with the same score sum. -/
theorem LDBC_L4_NullBitmapAndYearZoneFilter :
    LDBC_L4_NullBitmapAndYearZoneFilterGoal := by
  intro msgs yr fid
  dsimp only
  unfold total
  rw [filter_and_comm]

/-- BigBench B3 may compare a pair of direct dimension lookup results or test the
two components separately; revenue selection is identical. -/
theorem BigBench_B3_DirectDimLookupGridSum :
    BigBench_B3_DirectDimLookupGridSumGoal := by
  intro sales regOf catOf rg ct
  unfold total
  rfl

/-- BigBench B4 may short-circuit substring matching for US rows by partitioning
the selected rows into non-US matches and US rows; their revenue sums add exactly. -/
theorem BigBench_B4_CountryShortCircuitBeforeSubstring :
    BigBench_B4_CountryShortCircuitBeforeSubstringGoal := by
  intro rows
  induction rows with
  | nil => simp [total]
  | cons r rs ih =>
      simp only [total] at ih ⊢
      rcases r with ⟨us, matchesWord, revenue⟩
      cases us <;> cases matchesWord <;>
        simp [ih, Int.add_assoc, Int.add_left_comm]

/-- BigBench B5 may fuse the expensive-item bitset test with the year test or
apply the year test after bitmap selection; the direct year sum is unchanged. -/
theorem BigBench_B5_ExpensiveItemBitsetDirectYearSum :
    BigBench_B5_ExpensiveItemBitsetDirectYearSumGoal := by
  intro sales expensive yr
  unfold total
  rw [filter_and_comm]

/-- Public BI MLB Q108 may reduce exact block-level maxima instead of scanning
rows, because a left fold distributes over the concatenation of all blocks. -/
theorem PublicBI_MLBQ108_BlockFooterExtremaReduction :
    PublicBI_MLBQ108_BlockFooterExtremaReductionGoal := by
  intro blocks init
  exact foldl_blocks max init blocks

/-- Iceberg positional deletes may subtract the sum at deleted positions from the
file sum: under the stated bounds and duplicate-freedom, deleted and surviving
positions form a permutation of every valid file position. -/
theorem Iceberg_Base_ComplementDeleteSum :
    Iceberg_Base_ComplementDeleteSumGoal := by
  intro vals del hnodup hbound
  let live := (List.range vals.length).filter (fun i => !del.contains i)
  have hdelrange : ∀ i ∈ del, i ∈ List.range vals.length := by
    intro i hi
    exact List.mem_range.mpr (hbound i hi)
  have hpos := perm_filter_not_contains_append (List.range vals.length) del
    (List.nodup_range (n := vals.length)) hnodup hdelrange
  change total (valuesAt vals live) id + total (valuesAt vals del) id = total vals id
  calc
    total (valuesAt vals live) id + total (valuesAt vals del) id =
        total (valuesAt vals (live ++ del)) id := by
      unfold total
      rw [← List.sum_append]
      congr 1
      simp [valuesAt, List.filterMap_append]
    _ = total (valuesAt vals (List.range vals.length)) id := by
      unfold total
      exact sum_eq_of_perm ((hpos.filterMap (fun i => vals[i]?)).map id)
    _ = total vals id := by rw [valuesAt_range]

end TinylambOpt.ProposedProofs.BenchmarkRematch.AnalyticsAndCloudBI
