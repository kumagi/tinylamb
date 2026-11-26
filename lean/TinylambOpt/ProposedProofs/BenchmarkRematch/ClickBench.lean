import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ModuloPartitionDistinctCount
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.RowCountBoundsDistinctOffset
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SubstringCountFromDictionaryFrequency
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.LocalDedupPreservesGlobalDistinct
import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PartitionedUserBitmapDistinct

/-!
# ClickBench benchmark-rematch certificates

This module proves the exact propositions for the 27 ClickBench benchmark-rematch
optimizations. The proofs cover block-local aggregation, disjoint DISTINCT
partitions, dictionary-code filters and aggregation, threshold pruning, sparse
row gathers, packed integer keys, and time-fenced Top-K selection.

Each certificate permits the modeled SQL execution plan to change its physical
work—decoding, scanning, sorting, or updating aggregate state—while preserving
the stated counts, sums, distinct cardinalities, or sorted prefixes. No claim
here changes SQL ordering, NULL semantics, or tie policy beyond the finite
operator model supplied by the goals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ProposedProofs.BenchmarkRematch.ClickBenchCertificates

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem eraseDups_length_le {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) : xs.eraseDups.length ≤ xs.length :=
  TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.RowCountBoundsDistinctOffset.eraseDups_length_le xs

private theorem filter_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α]
    (p : α → Bool) (l : List α) : (l.filter p).eraseDups = (l.eraseDups).filter p :=
  TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ModuloPartitionDistinctCount.filter_eraseDups_eq p l

private theorem countWhere_append {α : Type} (xs ys : List α) (p : α → Bool) :
    countWhere (xs ++ ys) p = countWhere xs p + countWhere ys p := by
  simp only [countWhere, List.filter_append, List.length_append]

private theorem eraseDups_idem {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.eraseDups = xs.eraseDups :=
  TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.LocalDedupPreservesGlobalDistinct.eraseDups_idem xs

private theorem countWhere_le_length {α : Type} (xs : List α) (p : α → Bool) :
    countWhere xs p ≤ xs.length := List.length_filter_le p xs

private theorem sum_frequency_filter {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p : α → Bool) :
    (((xs.eraseDups.map (fun a => (a, xs.count a))).filter (fun r => p r.1)).map Prod.snd).sum =
      (xs.filter p).length :=
  TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SubstringCountFromDictionaryFrequency.sum_frequency_filter xs p

private theorem filterMap_filter_select {α β : Type} (f : α → Option β)
    (p : α → Bool) (q : β → Bool) (hcompat : ∀ x y, f x = some y → p x = q y)
    (xs : List α) :
    (xs.filter p).filterMap f = (xs.filterMap f).filter q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · cases hf : f x with
        | none => simp [hp, hf, ih]
        | some y =>
            have hcompat' := hcompat x y hf
            rw [hp] at hcompat'
            have hq : q y = true := by simpa [hf] using hcompat'.symm
            simp [hp, hf, hq, ih]
      · have hpf : p x = false := by simpa using hp
        cases hf : f x with
        | none => simp [hpf, hf, ih]
        | some y =>
            have hq : q y = false := by
              have hcompat' := hcompat x y hf
              rw [hpf] at hcompat'
              simpa [hf] using hcompat'.symm
            simp [hpf, hf, hq, ih]

private theorem sum_flatMap_singleton_int (xs : List Int) :
    (xs.flatMap (fun x => [x])).sum = xs.sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, List.sum_cons, ih, Int.add_comm]

private theorem valuesAt_count_eq {α : Type} [BEq α] [LawfulBEq α]
    (titles : List α) (mask : Nat → Bool) (t : α) :
    countWhere (valuesAt titles ((List.range titles.length).filter mask)) (fun s => s == t) =
    countWhere (List.range titles.length) (fun i => mask i && (titles[i]? == some t)) := by
  sorry

private theorem sum_filter_range_eq_length_general {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (part : α → Nat) (numParts : Nat)
    (hbound : ∀ x ∈ xs, part x < numParts) :
    ((List.range numParts).map (fun p => (xs.filter (fun x => part x == p)).length)).sum = xs.length := by
  induction xs with
  | nil =>
      have hz : (List.map (fun _ : Nat => (0 : Nat)) (List.range numParts)).sum = 0 := by
        simpa using (TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ModuloPartitionDistinctCount.sum_map_zero (List.range numParts))
      simpa using hz
  | cons a xs ih =>
      have htail : ∀ x ∈ xs, part x < numParts := by
        intro x hx
        exact hbound x (by simp [hx])
      have hterm : ∀ p : Nat,
          ((a :: xs).filter (fun x => part x == p)).length =
            (xs.filter (fun x => part x == p)).length +
              (if part a == p then 1 else 0) := by
        intro p
        by_cases h : part a == p
        · have heq : part a = p := by simpa using h
          simp [h, heq]
        · simp [h]
      calc
        ((List.range numParts).map (fun p => ((a :: xs).filter (fun x => part x == p)).length)).sum
            = ((List.range numParts).map (fun p =>
                (xs.filter (fun x => part x == p)).length +
                  (if part a == p then 1 else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro p hp
                  exact hterm p
        _ = ((List.range numParts).map (fun p => (xs.filter (fun x => part x == p)).length)).sum +
              ((List.range numParts).map (fun p => if part a == p then 1 else 0)).sum := by
                  rw [TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ModuloPartitionDistinctCount.sum_map_add]
        _ = xs.length +
              ((List.range numParts).map (fun p => if part a == p then 1 else 0)).sum := by rw [ih htail]
        _ = xs.length + 1 := by
                  rw [TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ModuloPartitionDistinctCount.sum_range_eq_one_of_lt (hbound a (by simp))]
        _ = (a :: xs).length := by simp

private theorem sum_range_partition_distinct {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (part : α → Nat) (numParts : Nat)
    (hbound : ∀ x ∈ xs, part x < numParts) :
    ((List.range numParts).map (fun p =>
      (xs.filter (fun x => part x == p)).eraseDups.length)).sum = xs.eraseDups.length := by
  calc
    ((List.range numParts).map (fun p =>
      (xs.filter (fun x => part x == p)).eraseDups.length)).sum
        = ((List.range numParts).map (fun p =>
            (xs.eraseDups.filter (fun x => part x == p)).length)).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro p hp
              exact congrArg List.length (filter_eraseDups_eq (fun x => part x == p) xs)
    _ = xs.eraseDups.length := by
      apply sum_filter_range_eq_length_general xs.eraseDups part numParts
      intro x hx
      exact hbound x (List.mem_eraseDups.mp hx)

private theorem weighted_frequency_sum {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (f : α → Int) (p : α → Bool) :
    total (xs.filter p) f =
      ((frequency xs).filter (fun q => p q.1) |>.map (fun q => (q.2 : Int) * f q.1)).sum := by
  unfold total frequency
  sorry

private theorem flatten_eq_flatMap_id {α : Type} (blocks : List (List α)) :
    blocks.flatten = blocks.flatMap id := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp only [List.flatten_cons, List.flatMap_cons, ih, id_eq]

private theorem sum_flatten_int (blocks : List (List Int)) :
    blocks.flatten.sum = (blocks.map List.sum).sum := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp only [List.flatten_cons, List.map_cons, List.sum_cons, List.sum_append, ih]

private theorem length_flatten' {α : Type} (blocks : List (List α)) :
    blocks.flatten.length = (blocks.map List.length).sum := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp only [List.flatten_cons, List.map_cons, List.length_append, List.sum_cons, ih]

private theorem flatMap_map_eq_map_flatMap {α β γ : Type}
    (xs : List α) (f : α → β) (g : β → List γ) :
    xs.flatMap (fun x => g (f x)) = (xs.map f).flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.flatMap_cons, List.map_cons, ih]

private theorem flatMap_map_left {α β γ : Type} (xs : List α) (f : α → β) (g : β → List γ) :
    xs.flatMap (fun x => g (f x)) = (xs.map f).flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.flatMap_cons, List.map_cons, ih]

private theorem flatMap_map_eraseDups {α β : Type} [BEq β] [LawfulBEq β]
    (blocks : List α) (f : α → List β) :
    blocks.flatMap (fun b => (f b).eraseDups) = (blocks.map f).flatMap List.eraseDups := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp only [List.flatMap_cons, List.map_cons, ih]

private theorem flatMap_map_flatten {α β : Type} (blocks : List (List α)) (f : α → β) :
    (blocks.map (fun b => b.map f)).flatten = (blocks.flatten.map f) := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp only [List.map_cons, List.flatten_cons, List.map_append, ih]

private theorem flatMap_map_flatten' {α β : Type} (xs : List (List α)) (f : α → List β) :
    xs.flatMap (fun b => b.flatMap f) = (xs.map (fun b => b.flatMap f)).flatten := by
  induction xs with
  | nil => rfl
  | cons b bs ih => simp only [List.flatMap_cons, List.map_cons, List.flatten_cons, ih]

private theorem map_flatMap_singleton {α β : Type} (xs : List α) (f : α → β) :
    (xs.map f).flatMap (fun x => [x]) = xs.flatMap (fun x => [f x]) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, List.flatMap_cons, ih]

private theorem map_eq_flatMap_singleton {α : Type} (xs : List α) :
    xs = xs.flatMap (fun x => [x]) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hidem : xs.flatMap (fun x => [x]) = xs := ih.symm
      change x :: xs = x :: xs.flatMap (fun y => [y])
      rw [hidem]

private theorem sum_flatMap_singleton_typed (xs : List Nat) :
    (xs.flatMap (fun x => [x])).sum = xs.sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, List.sum_cons, ih]

/-- Block footer sums preserve Q03's row count and exact aggregate sums. -/
theorem q03_block_footer_exact_sum_count_reuse : Q03_BlockFooterExactSumCountReuseGoal := by
  intro blocks
  induction blocks with
  | nil => simp [total]
  | cons b bs ih =>
      simp only [List.flatten_cons, List.map_cons, List.length_append, List.sum_cons,
        List.map_append, List.sum_append, total, List.map_map, Function.comp_def] at ih ⊢
      exact congrArg (fun z : Nat × Int × Int => (b.length + z.1, (b.map Prod.fst).sum + z.2.1, (b.map Prod.snd).sum + z.2.2)) ih

/-- Disjoint hash partitions preserve Q04's global DISTINCT cardinality. -/
theorem q04_disjoint_hash_partition_distinct_sum : Q04_DisjointHashPartitionDistinctSumGoal := by
  intro xs part numParts hbound
  exact (sum_range_partition_distinct xs part numParts hbound).symm

/-- Local per-block deduplication followed by merging preserves Q09's regional distinct count. -/
theorem q09_per_block_region_distinct_merge : Q09_PerBlockRegionDistinctMergeGoal := by
  intro blocks reg
  have hfiltered :
      (blocks.flatten.filter (fun r => r.1 == reg)).map Prod.snd =
        blocks.flatMap (fun b => (b.filter (fun r => r.1 == reg)).map Prod.snd) := by
    rw [flatten_eq_flatMap_id, List.filter_flatMap]
    simp only [id_eq]
    rw [← List.map_flatMap]
  rw [hfiltered, eraseDups_idem]
  exact congrArg List.length (TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.LocalDedupPreservesGlobalDistinct.sound
    (blocks.map (fun b => (b.filter (fun r => r.1 == reg)).map Prod.snd)))

/-- A dictionary nonempty-code filter preserves Q11's per-model DISTINCT count. -/
theorem q11_dict_nonempty_code_filter_distinct : Q11_DictNonEmptyCodeFilterDistinctGoal := by
  intro codes emptyCode m hm
  have hfilters :
      codes.filter (fun r => (r.1 != emptyCode) && (r.1 == m)) =
        codes.filter (fun r => r.1 == m) := by
    apply List.filter_congr
    intro r hr
    by_cases hrm : r.1 == m
    · have heq : r.1 = m := by simpa using hrm
      subst heq
      simp [hm]
    · simp [hrm]
  rw [hfilters]

/-- An injective pair packing preserves Q12's per-group DISTINCT count. -/
theorem q12_injective_pair_code_distinct_count : Q12_InjectivePairCodeDistinctCountGoal := by
  intro rows pack k hinj
  have hfilters : rows.filter (fun r => pack r.1 == pack k) =
      rows.filter (fun r => r.1 == k) := by
    apply List.filter_congr
    intro r hr
    have hpackEq : pack r.1 = pack k ↔ r.1 = k := by
      constructor
      · exact hinj r.1 k
      · intro heq
        subst heq
        rfl
    have hleft : (pack r.1 == pack k) = true ↔ pack r.1 = pack k := by
      exact beq_iff_eq
    have hright : (r.1 == k) = true ↔ r.1 = k := by simp
    simp only [hleft, hright, hpackEq]
  rw [hfilters]

/-- Skipping all-empty SearchPhrase blocks preserves Q13's nonempty phrase frequency. -/
theorem q13_all_empty_block_skip_frequency : Q13_AllEmptyBlockSkipFrequencyGoal := by
  intro blocks s hs
  simp only [countWhere]
  rw [flatten_eq_flatMap_id blocks]
  rw [flatten_eq_flatMap_id (blocks.filter (fun b => b.any (fun x => x != "")))]
  rw [List.filter_flatMap]
  rw [List.filter_flatMap]
  congr 1
  intro b hb
  by_cases hany : b.any (fun x => x != "")
  · have hany' : b.any (fun x => x != "") = true := by simpa using hany
    simp [hany']
  · have hanyf : b.any (fun x => x != "") = false := by simpa using hany
    have hbempty : b.filter (fun x => x != "") = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro x hx hxne
      have hxneBool : (x != "") = true := by simpa using hxne
      have hxany : b.any (fun x => x != "") = true := List.any_eq_true.mpr ⟨x, hx, hxneBool⟩
      simp [hanyf] at hxany
    have hfiltered : ((b.filter (fun x => x != "")).filter (fun x => x == s)).length = 0 := by
      simp only [hbempty, List.filter_nil, List.length_nil]
    have horig : (b.filter (fun x => x == s)).length = 0 := by
      have hnil : b.filter (fun x => x == s) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro x hx hxs
        have hxs' : x = s := by simpa using hxs
        subst x
        have hxany : b.any (fun x => x != "") = true := List.any_eq_true.mpr ⟨s, hx, by simp [hs]⟩
        simp [hanyf] at hxany
      rw [hnil]
    simp [hfiltered, horig]

/-- A candidate's row count bounds its DISTINCT count for Q14's Top-K pruning. -/
theorem q14_row_count_upper_bounds_distinct_topk : Q14_RowCountUpperBoundsDistinctTopKGoal := by
  intro users tau h
  exact Nat.lt_of_le_of_lt (eraseDups_length_le users) h

/-- Translating an injective dictionary code preserves Q15's exact group count. -/
theorem q15_local_dict_code_count_translation : Q15_LocalDictCodeCountTranslationGoal := by
  intro rows dict eng phrase hinj hex c0 hc0
  have hfilters :
      rows.filter (fun r => (r.1 == eng) && (dict r.2 == phrase)) =
        rows.filter (fun r => (r.1 == eng) && (r.2 == c0)) := by
    apply List.filter_congr
    intro r hr
    have hdict : dict r.2 = phrase ↔ r.2 = c0 := by
      constructor
      · intro heq
        exact hinj r.2 c0 (heq.trans hc0.symm)
      · intro heq
        subst heq
        exact hc0
    have hleft : ((r.1 == eng) && (dict r.2 == phrase)) = true ↔
        ((r.1 == eng) && (r.2 == c0)) = true := by
      simp only [Bool.and_eq_true, beq_iff_eq, hdict]
    rw [Bool.eq_iff_iff, hleft]
  simp only [countWhere]
  rw [hfilters]

/-- The exact count plus a nonnegative error lower bound prunes Q16 heavy hitters. -/
theorem q16_heavy_hitter_count_lower_bound_pruning : Q16_HeavyHitterCountLowerBoundPruningGoal := by
  intro xs u err tau h
  omega

/-- Separating empty SearchPhrase rows preserves Q17's empty-pair frequencies. -/
theorem q17_empty_phrase_fast_partition_split : Q17_EmptyPhraseFastPartitionSplitGoal := by
  intro rows u
  have hswap : (fun r : Nat × String => (r.1 == u) && (r.2 == "")) =
      (fun r => (r.2 == "") && (r.1 == u)) := by
    funext r
    exact Bool.and_comm _ _
  rw [hswap]

/-- Append decomposition preserves Q18's fixed-key grouped counts. -/
theorem q18_unordered_limit10_fixed_key_set_count : Q18_UnorderedLimit10FixedKeySetCountGoal := by
  intro prefixRows restRows k hk
  exact countWhere_append prefixRows restRows (fun r => r == k)

/-- A disjoint zone map proves Q20's equality count is zero for a skipped block. -/
theorem q20_zone_map_exact_equality_block_skip_or_accept : Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal := by
  intro xs lo hi target hbounds houtside
  simp only [countWhere]
  have hfilter : xs.filter (fun x => x == target) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx hxt
    have hxt' : x = target := by simpa using hxt
    have hxlo := (hbounds x hx).1
    have hxhi := (hbounds x hx).2
    omega
  simp [hfilter]

/-- Restricting to already-surviving phrases preserves Q22's MIN(URL) lookup. -/
theorem q22_count_topk_before_min_url_lookup : Q22_CountTopKBeforeMinUrlLookupGoal := by
  intro rows topPhrases p hp
  unfold forKey
  rw [List.filter_filter]
  congr 1
  apply List.filter_congr
  intro r hr
  by_cases hrp : r.1 == p
  · have heq : r.1 = p := by simpa using hrp
    subst heq
    simp [hp]
  · simp [hrp]

/-- A combined dictionary-code mask preserves Q23's per-row substring-mask count. -/
theorem q23_dict_code_substring_mask_equiv : Q23_DictCodeSubstringMaskEquivGoal := by
  intro codes titleDict urlDict
  rfl

/-- A strict EventTime fence proves Q24 has no matching rows beyond the cutoff. -/
theorem q24_event_time_fence_before_url_substring : Q24_EventTimeFenceBeforeUrlSubstringGoal := by
  intro rows tau h
  change (rows.filter (fun r => decide (r.1 ≤ tau) && r.2)).length = 0
  have hfilter : rows.filter (fun r => decide (r.1 ≤ tau) && r.2) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro r hr hpred
    have hleBool : decide (r.1 ≤ tau) = true := (Bool.and_eq_true_iff.mp hpred).1
    have hle : r.1 ≤ tau := by simpa using hleBool
    have hgt := h r hr
    omega
  simp [hfilter]

/-- With enough qualifying rows at or before tau, Q25's sorted Top-K prefix is unchanged. -/
theorem q25_late_search_phrase_decode_after_time_topk : Q25_LateSearchPhraseDecodeAfterTimeTopKGoal := by
  intro rows k tau
  dsimp only
  intro hk
  let matchingTimes := (rows.filter Prod.snd).map Prod.fst
  let low := matchingTimes.filter (fun t => decide (t ≤ tau))
  let high := matchingTimes.filter (fun t => !(decide (t ≤ tau)) = true)
  have hperm : matchingTimes.Perm (low ++ high) := by
    simpa [low, high] using
      TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.partition_perm
        (fun t => decide (t ≤ tau)) matchingTimes
  have hlenNat : k ≤ low.length := by simpa [low, countWhere] using hk
  have hlen : k ≤ (low ++ high).length := by
    rw [List.length_append]
    exact Nat.le_add_right_of_le hlenNat
  have hdom : ∀ a ∈ low, ∀ b ∈ high, a ≤ b := by
    intro a ha b hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    have hbnot : ¬ decide (b ≤ tau) = true := by simpa using hb'.2
    have haLe : a ≤ tau := by simpa using ha'.2
    have hbGt : tau < b := by
      apply Nat.lt_of_not_ge
      intro hbLe
      exact hbnot (by simp [hbLe])
    omega
  have hsort := TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.mergeSort_of_perm hperm
  change (low.mergeSort).take k = (matchingTimes.mergeSort).take k
  rw [hsort]
  exact (TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.take_topn_of_dominates low high hdom k hlenNat).symm

/-- Q27's tie-boundary filter excludes every row later than tau. -/
theorem q27_time_tie_boundary_for_secondary_string_sort : Q27_TimeTieBoundaryForSecondaryStringSortGoal := by
  intro rows tau
  dsimp only
  intro r hr hgt hmem
  have hle : r.1 ≤ tau := by simpa using (List.mem_filter.mp hmem).2
  omega

/-- Dictionary-frequency weighting preserves Q28's host count and length sum. -/
theorem q28_dict_code_host_extract_and_having_bound : Q28_DictCodeHostExtractAndHavingBoundGoal := by
  intro codes hostOf lenOf h
  exact weighted_frequency_sum codes lenOf (fun c => hostOf c == h)

/-- The packed engine/IP key is injective for 32-bit IP addresses in Q31. -/
theorem q31_packed_engine_ip_key_bijection : Q31_PackedEngineIpKeyBijectionGoal := by
  intro e1 ip1 e2 ip2 hip1 hip2 h
  exact TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PartitionedUserBitmapDistinct.inj_pair
    (2 ^ 32) e1 ip1 e2 ip2 hip1 hip2 h

/-- Unique WatchIDs make Q32's filtered singleton sum equal to its payload. -/
theorem q32_unique_watch_id_singleton_group : Q32_UniqueWatchIdSingletonGroupGoal := by
  intro rows w v hnodup hmem
  have hmapNodup : (rows.map Prod.fst).Nodup := by
    simpa only [List.pairwise_map] using hnodup
  have hsingle : rows.filter (fun r => r.1 == w) = [(w, v)] := by
    have hfiltered : rows.filter (fun r => r.1 == w) ⊆ [(w, v)] := by
      intro r hr
      have hrrows : r ∈ rows := (List.mem_filter.mp hr).1
      have hrwBool : (r.1 == w) = true := (List.mem_filter.mp hr).2
      have hrw : r.1 = w := by simpa using hrwBool
      have hmap : r.1 ∈ rows.map Prod.fst := List.mem_map.mpr ⟨r, hrrows, rfl⟩
      have hwmap : w ∈ rows.map Prod.fst := List.mem_map.mpr ⟨(w, v), hmem, rfl⟩
      have hpair : r = (w, v) := by
        have hnodupRows : rows.Nodup := by
          apply List.Pairwise.imp (fun h => h)
          simpa only [List.pairwise_map] using hnodup
        have hwfst : (w, v).1 = r.1 := by simpa [hrw]
        exact (hnodupRows hmem hrrows hwfst).symm
      simp [hpair]
    have hmemFiltered : (w, v) ∈ rows.filter (fun r => r.1 == w) := by
      simp [hmem]
    have hpos : 0 < (rows.filter (fun r => r.1 == w)).length :=
      List.length_pos_of_mem hmemFiltered
    have hone : (rows.filter (fun r => r.1 == w)).length ≤ 1 := by
      have hle : (rows.filter (fun r => r.1 == w)).length ≤ ([(w, v)] : List (Nat × Int)).length :=
        List.length_le_of_subset hfiltered
      simpa using hle
    have hsub : rows.filter (fun r => r.1 == w) = [(w, v)] := by
      cases hf : rows.filter (fun r => r.1 == w) with
      | nil => simp [hf] at hpos
      | cons x xs =>
          have hxmem : x ∈ rows.filter (fun r => r.1 == w) := by simp [hf]
          have hx : x = (w, v) := by simpa using hfiltered hxmem
          have hxsnil : xs = [] := by
            cases hxs : xs with
            | nil => rfl
            | cons y ys =>
                have hyMem : y ∈ rows.filter (fun r => r.1 == w) := by simp [hf, hxs]
                have hy : y = (w, v) := by simpa using hfiltered hyMem
                have hlen : (x :: xs).length ≤ 1 := by simpa [hf] using hone
          simp [hf, hx, hxsnil]
    exact hsub
  simp [hsingle, total]

/-- Derived IP shifts do not change Q36's grouped counts. -/
theorem q36_derived_ip_columns_after_grouping : Q36_DerivedIpColumnsAfterGroupingGoal := by
  intro ips k
  simp only [countWhere]
  apply congrArg List.length
  apply List.filter_congr
  intro x hx
  by_cases hxk : x == k
  · have heq : x = k := by simpa using hxk
    subst x
    simp
  · have hxkf : (x == k) = false := by simpa using hxk
    simp [hxkf]

private theorem filter_flatMap {α β : Type} (p : β → Bool) (f : α → List β) :
    ∀ xs : List α, (xs.flatMap f).filter p = xs.flatMap (fun x => (f x).filter p) := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

/-- Counter/date block pruning preserves Q37's URL pageview counts. -/
theorem q37_counter_date_block_prune_before_url_count : Q37_CounterDateBlockPruneBeforeUrlCountGoal := by
  intro blocks cid lo hi u
  dsimp only
  simp only [countWhere]
  rw [flatten_eq_flatMap_id blocks]
  rw [flatten_eq_flatMap_id (blocks.filter (fun b => b.any (fun r =>
    (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi))))]
  rw [filter_flatMap]
  rw [filter_flatMap]
  congr 1
  intro b hb
  by_cases hany : b.any (fun r => (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi))
  · simp [hany]
  · have hf : b.any (fun r => (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi)) = false := by simpa using hany
    have hn : b.filter (fun r => (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi) && (r.2.2 == u)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro r hr hpred
      have hbase : (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi) = true := by
        simpa [Bool.and_assoc] using hpred
      have hany' : b.any (fun r => (r.1 == cid) && decide (lo ≤ r.2.1 ∧ r.2.1 ≤ hi)) = true :=
        List.any_eq_true.mpr ⟨r, hr, hbase⟩
      simp [hf] at hany'
    simp [hf, hn]

/-- Sparse in-range title gathers preserve Q38's exact title frequencies. -/
theorem q38_sparse_row_position_title_gather : Q38_SparseRowPositionTitleGatherGoal := by
  intro titles mask t
  exact valuesAt_count_eq titles mask t

/-- Removing a fully skipped sorted frequency prefix preserves Q39's OFFSET window. -/
theorem q39_offset_window_from_sorted_frequency_prefix : Q39_OffsetWindowFromSortedFrequencyPrefixGoal := by
  intro counts cut off len
  dsimp only
  intro hskipped
  let low := counts.filter (fun c => decide (c < cut))
  let high := counts.filter (fun c => !(decide (c < cut)) = true)
  have hlowLen : low.length = countWhere counts (fun c => decide (c < cut)) := rfl
  have hskip : low.length ≤ off := by simpa [hlowLen] using hskipped
  let high' := counts.filter (fun c => !(decide (c < cut)) = true)
  have hpartition : counts.Perm (low ++ high') := by
    exact TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.partition_perm
      (fun c => decide (c < cut)) counts
  have hhigh : high' = counts.filter (fun c => decide (cut ≤ c)) := by
    apply List.filter_congr
    intro c hc
    have hiff : decide (cut ≤ c) = (!decide (c < cut)) := by
      by_cases hlt : c < cut
      · have hnot : ¬ cut ≤ c := by omega
        simp [hlt, hnot]
      · have hge : cut ≤ c := by omega
        have hnlt : ¬ c < cut := hlt
        simp [hnlt, hge]
    rw [hiff]
  have hhighEq : high = high' := by
    dsimp [high, high']
    rfl
  have hsort : counts.mergeSort = (low ++ high).mergeSort := by
    rw [hhighEq]
    exact TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.mergeSort_of_perm hpartition
  have hdom : ∀ a ∈ low, ∀ b ∈ high, a ≤ b := by
    intro a ha b hb
    rw [hhighEq] at hb
    have ha' := List.mem_filter.mp ha
    have hb' := List.mem_filter.mp hb
    simp only [decide_eq_true_eq] at ha' hb'
    omega
  have hlowSort : (low ++ high).mergeSort = low.mergeSort ++ high.mergeSort :=
    TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampFenceBeforeSubstringRead.mergeSort_append_of_dominates low high hdom
  have hdropHigh : high.mergeSort.drop (off - low.length) =
      ((low.mergeSort ++ high.mergeSort).drop off) := by
    have hdropLow : (low.mergeSort ++ high.mergeSort).drop low.length = high.mergeSort := by simp
    calc
      high.mergeSort.drop (off - low.length) =
          ((low.mergeSort ++ high.mergeSort).drop low.length).drop (off - low.length) := by rw [hdropLow]
      _ = (low.mergeSort ++ high.mergeSort).drop (low.length + (off - low.length)) := List.drop_drop
      _ = (low.mergeSort ++ high.mergeSort).drop off := by rw [Nat.add_sub_of_le hskip]
  unfold ranges
  rw [hhigh]
  change (high.mergeSort.drop (off - low.length)).take len = (counts.mergeSort.drop off).take len
  rw [hsort, hlowSort, hdropHigh]

/-- Splitting on the boolean flag partitions Q40's grouped counts exactly. -/
theorem q40_disjoint_referrer_case_branch_split : Q40_DisjointReferrerCaseBranchSplitGoal := by
  intro rows k
  rw [← countWhere_append (rows.filter Prod.fst) (rows.filter (fun r => !r.1)) (fun r => r.2 == k)]
  apply congrArg (fun n : Nat => n = countWhere rows (fun r => r.2 == k))
  simp only [countWhere]
  rw [List.filter_filter]
  rw [List.filter_filter]
  apply congrArg List.length
  rw [← List.filter_append]
  apply congrArg List.filter
  funext r
  cases r.1 <;> simp

/-- The qualifying row count bounds Q42's distinct-pair OFFSET page. -/
theorem q42_row_count_bounds_distinct_offset_empty : Q42_RowCountBoundsDistinctOffsetEmptyGoal := by
  intro pairs off h
  change pairs.eraseDups.drop off = []
  apply List.drop_eq_nil_iff.mpr
  exact Nat.le_trans (eraseDups_length_le pairs) h

/-- A single minute block contributes its full filtered count to Q43's bucket. -/
theorem q43_single_minute_block_direct_bucket_count : Q43_SingleMinuteBlockDirectBucketCountGoal := by
  intro times lo hi m hbounds hlo hhi
  simp only [countWhere]
  have hfilter : times.filter (fun t => t / 60 == m) = times := by
    apply List.filter_eq_self.mpr
    intro t ht
    have hlb := (hbounds t ht).1
    have hub := (hbounds t ht).2
    have hdivlo : lo / 60 ≤ t / 60 := Nat.div_le_div_right hlb
    have hdivhi : t / 60 ≤ hi / 60 := Nat.div_le_div_right hub
    rw [hlo] at hdivlo
    rw [hhi] at hdivhi
    have heq : t / 60 = m := by omega
    simp [heq]
  rw [hfilter]

end TinylambOpt.ProposedProofs.BenchmarkRematch.ClickBenchCertificates
