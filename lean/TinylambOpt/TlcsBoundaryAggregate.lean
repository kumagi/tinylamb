import TinylambOpt.CountBoundsAndBlockAggregates
import TinylambOpt.QuotientFusion

/-! The boundary tracker stores exact counts for its surviving values.
Reservoir estimates never enter these guards. NULL rows are removed before
ordinary numeric comparison and extrema. -/

namespace TinylambOpt.TlcsBoundaryAggregate

theorem boundary_min (rows : List (Option Int)) (lo : Int)
    (present : lo ∈ rows.filterMap id)
    (bound : ∀ x ∈ rows.filterMap id, lo ≤ x) :
    (rows.filterMap id).min? = some lo :=
  List.min?_eq_some_iff.mpr ⟨present, bound⟩

theorem boundary_max (rows : List (Option Int)) (hi : Int)
    (present : hi ∈ rows.filterMap id)
    (bound : ∀ x ∈ rows.filterMap id, x ≤ hi) :
    (rows.filterMap id).max? = some hi :=
  List.max?_eq_some_iff.mpr ⟨present, bound⟩

theorem equality_complement (rows : List Nat) (key : Nat) :
    (rows.filter (· != key)).length =
      rows.length - (rows.filter (· == key)).length := by
  induction rows with
  | nil => simp
  | cons row rest ih =>
    have le := List.length_filter_le (· == key) rest
    by_cases same : row = key
    · subst row
      simpa using ih
    · simp [same, ih]
      omega

theorem coverage_by_count (rows domain : List Nat)
    (nodup : domain.Nodup)
    (mass : (domain.map (fun k => (rows.filter (· == k)).length)).sum = rows.length) :
    ∀ k ∈ rows, k ∈ domain := by
  let known := rows.filter (fun k => decide (k ∈ domain))
  have cover : ∀ k ∈ known, k ∈ domain := by
    intro k mem
    simpa [known] using (List.mem_filter.mp mem).2
  have freq := dict_exact_freq_filtered_count_sound id (fun _ => true)
      known domain nodup cover
  have eqCount : ∀ k ∈ domain,
      (known.filter (· == k)).length = (rows.filter (· == k)).length := by
    intro k mem
    unfold known
    rw [List.filter_filter]
    congr 1
    apply List.filter_congr
    intro v _
    by_cases h : v = k <;> simp [h, mem]
  have eqMass : (domain.map (fun k => (known.filter (· == k)).length)) =
      domain.map (fun k => (rows.filter (· == k)).length) :=
    List.map_congr_left eqCount
  have allKnown : known.filter (fun _ => true) = known :=
    List.filter_eq_self.mpr (by simp)
  have allDomain : domain.filter (fun _ => true) = domain :=
    List.filter_eq_self.mpr (by simp)
  rw [allKnown, allDomain] at freq
  rw [eqMass, mass] at freq
  intro k mem
  exact of_decide_eq_true ((List.length_filter_eq_length_iff.mp
    (show (rows.filter (fun k => decide (k ∈ domain))).length = rows.length
      from freq)) k mem)

theorem exact_grouped_counts (rows domain : List Nat) (keep : Nat → Bool)
    (nodup : domain.Nodup)
    (mass : (domain.map (fun k => (rows.filter (· == k)).length)).sum = rows.length) :
    (rows.filter keep).length =
      ((domain.filter keep).map (fun k => (rows.filter (· == k)).length)).sum :=
  dict_exact_freq_filtered_count_sound id keep rows domain nodup
    (coverage_by_count rows domain nodup mass)

theorem exact_grouped_null_count (rows : List (Option Int)) :
    (rows.filter Option.isNone).length = rows.length - (rows.filterMap id).length := by
  suffices (rows.filter Option.isNone).length + (rows.filterMap id).length =
      rows.length by omega
  induction rows with
  | nil => simp
  | cons row rows ih =>
    cases row <;> simp at * <;> omega

theorem dense_frequency_offset (lo hi value capacity : Int)
    (inRange : lo ≤ value ∧ value ≤ hi) (fits : hi - lo < capacity) :
    0 ≤ value - lo ∧ value - lo < capacity ∧ lo + (value - lo) = value := by
  omega

theorem dense_frequency_word_reconstructs (start value : BitVec 64) :
    start + (value - start) = value := by
  bv_decide

theorem shortlist_interval_cannot_match (lo hi value : Int)
    (shortlist : List Int) (inBlock : lo ≤ value ∧ value ≤ hi)
    (disjoint : ∀ x ∈ shortlist, x < lo ∨ hi < x) :
    value ∉ shortlist := by
  intro mem
  have outside := disjoint value mem
  omega

theorem shortlist_fence_sound (key : Int) (shortlist : List Int)
    (fingerprint : Int → Nat) (matchKey : key ∈ shortlist) :
    fingerprint key ∈ shortlist.map fingerprint :=
  List.mem_map.mpr ⟨key, matchKey, rfl⟩

theorem duplicate_key_retained {Key Bucket : Type}
    [DecidableEq Key] [DecidableEq Bucket] (rows : List Key)
    (fingerprint : Key → Bucket) (key : Key)
    (duplicate : 2 ≤ groupCount id rows key) :
    2 ≤ bucketCount fingerprint id rows (fingerprint key) :=
  Nat.le_trans duplicate
    (quotient_bucket_count_ge_group_count fingerprint id rows key)

/-- Other words can interleave between the two sightings of a key. OR
    never clears its bits and the second sighting publishes all of them. -/
theorem second_sighting_marks_all_bits
    (seen mask other repeated : BitVec 64) :
    ((repeated ||| (((seen ||| mask) ||| other) &&& mask)) &&& mask) = mask := by
  bv_decide

theorem replay_saved_selection {Row : Type} (rows saved : List Row)
    (passes duplicate : Row → Bool) (savedExactly : saved = rows.filter passes) :
    saved.filter duplicate = rows.filter (fun row => passes row && duplicate row) := by
  rw [savedExactly, List.filter_filter]
  simp [Bool.and_comm]

theorem dropped_singleton_has_enough_peers (cap seen : Nat)
    (drop : cap ≤ seen) : (List.range seen |>.take cap).length = cap := by
  simp [Nat.min_eq_left drop]

theorem bounded_compare_true (lo hi value bound : Int)
    (inBlock : lo ≤ value ∧ value ≤ hi) :
    ((lo = bound ∧ hi = bound) → value = bound) ∧
    ((hi < bound ∨ bound < lo) → value ≠ bound) ∧
    (hi < bound → value < bound) ∧ (hi ≤ bound → value ≤ bound) ∧
    (lo > bound → value > bound) ∧ (lo ≥ bound → value ≥ bound) := by
  omega

end TinylambOpt.TlcsBoundaryAggregate
