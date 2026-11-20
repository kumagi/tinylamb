import TinylambOpt.ClickBenchRound2
import Std.Tactic

/-! A single ordered sweep maintains both the original bucket histogram and
exact group counts in a mask chosen before that sweep. The mask may be
arbitrary, including an inaccurate sample. The full histogram certifies the
top-k cutoff or supplies the original replay unchanged. -/

namespace TinylambOpt.QuotientFusion

structure Counts (Key Bucket : Type) where
  buckets : Bucket → Nat
  groups : Key → Nat

def bump {α : Type} [DecidableEq α] (counts : α → Nat) (value : α) : α → Nat :=
  fun k => counts k + if value = k then 1 else 0

def step {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool)
    (counts : Counts Key Bucket) (key : Key) : Counts Key Bucket :=
  ⟨bump counts.buckets (bucket key),
    if selected (bucket key) then bump counts.groups key else counts.groups⟩

def sweep {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool)
    (keys : List Key) : Counts Key Bucket :=
  keys.foldl (step bucket selected) ⟨fun _ => 0, fun _ => 0⟩

private theorem bucket_fold {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (keys : List Key)
    (initial : Counts Key Bucket) (b : Bucket) :
    (keys.foldl (step bucket selected) initial).buckets b =
      initial.buckets b + bucketCount bucket id keys b := by
  induction keys generalizing initial with
  | nil => simp [bucketCount]
  | cons key tail ih =>
    rw [List.foldl_cons, ih]
    simp only [step, bump, bucketCount, id_eq, List.filter_cons, beq_iff_eq]
    split <;> simp_all <;> omega

private theorem group_fold {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (keys : List Key)
    (initial : Counts Key Bucket) (k : Key) (included : selected (bucket k) = true) :
    (keys.foldl (step bucket selected) initial).groups k =
      initial.groups k + groupCount id keys k := by
  induction keys generalizing initial with
  | nil => simp [groupCount]
  | cons key tail ih =>
    rw [List.foldl_cons, ih]
    by_cases same : key = k
    · subst key
      simp [step, bump, included, groupCount, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
    · cases chosen : selected (bucket key) <;>
        simp [step, bump, chosen, groupCount, same]

theorem fused_bucket_count {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (keys : List Key) (b : Bucket) :
    (sweep bucket selected keys).buckets b = bucketCount bucket id keys b := by
  simp [sweep, bucket_fold]

theorem fused_group_count {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (keys : List Key)
    (k : Key) (included : selected (bucket k) = true) :
    (sweep bucket selected keys).groups k = groupCount id keys k := by
  simp [sweep, group_fold, included]

theorem untracked_group_fails_count_having {Key Bucket : Type}
    [DecidableEq Key] [DecidableEq Bucket] (bucket : Key → Bucket)
    (selected : Bucket → Bool) (keys : List Key) (key : Key)
    (required qualifying : Nat) (subsetCount : qualifying ≤ groupCount id keys key)
    (under : (sweep bucket selected keys).buckets (bucket key) < required) :
    qualifying < required := by
  rw [fused_bucket_count] at under
  have upper := quotient_bucket_count_ge_group_count bucket id keys key
  omega

theorem covered_having_payload {Row Key Bucket : Type} [DecidableEq Key]
    (rows : List Row) (keyOf : Row → Key) (bucket : Key → Bucket)
    (selected : Bucket → Bool) (key : Key) (covered : selected (bucket key) = true) :
    ((rows.filter (fun row => selected (bucket (keyOf row)))).filter
      (fun row => decide (keyOf row = key))) =
    rows.filter (fun row => decide (keyOf row = key)) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro row _
  by_cases same : keyOf row = key <;> simp [same, covered]

/-- The parallel merge adds the same exact counts as scanning the concatenation.
    The bucket identity and mask are common to all workers. -/
theorem merge_exact_counts {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (xs ys : List Key)
    (b : Bucket) (k : Key) (included : selected (bucket k) = true) :
    (sweep bucket selected (xs ++ ys)).buckets b =
      (sweep bucket selected xs).buckets b + (sweep bucket selected ys).buckets b ∧
    (sweep bucket selected (xs ++ ys)).groups k =
      (sweep bucket selected xs).groups k + (sweep bucket selected ys).groups k := by
  simp [fused_bucket_count, fused_group_count, included, bucketCount, groupCount]

/-- An unfiltered block can drive the sweep by row index, omitting the
    original identity selection vector and the intermediate decoded key list. -/
theorem sweep_without_identity_selection
    {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool)
    (decode : Nat → Key) (length : Nat) :
    sweep bucket selected ((List.range length).map decode) =
      (List.range length).foldl (fun state i => step bucket selected state (decode i))
        ⟨fun _ => 0, fun _ => 0⟩ := by
  simp [sweep, List.foldl_map]

/-- The first and only VARCHAR key contributes no positional salt. -/
theorem single_varchar_hash
    (mix : BitVec 64 → BitVec 64) (seed valueHash salt : BitVec 64) :
    mix (seed ^^^ (valueHash + 0 * salt)) = mix (seed ^^^ valueHash) := by
  simp

/-- Reading just a projected stream gives the same bytes as slicing
    the coalesced block window, when the stream lies within that window. -/
theorem direct_projection_read {Byte : Type} (file : List Byte)
    (base window offset length : Nat) (inside : offset + length ≤ window) :
    ((((file.drop base).take window).drop offset).take length) =
      (file.drop (base + offset)).take length := by
  rw [List.drop_take, List.take_take, List.drop_drop,
    Nat.min_eq_left (show length ≤ window - offset by omega)]

/-- If the kth *exact* group has count `cutoff`, preserve every group
    that can tie or beat it, including all groups considered by secondary
    ORDER BY keys or by taking LIMIT + OFFSET. -/
theorem fused_topk_certificate {Key Bucket : Type} [DecidableEq Key] [DecidableEq Bucket]
    (bucket : Key → Bucket) (selected : Bucket → Bool) (keys : List Key)
    (cutoff : Nat)
    (outside : ∀ b, selected b = false → (sweep bucket selected keys).buckets b < cutoff)
    (k : Key) (competitive : cutoff ≤ groupCount id keys k) :
    selected (bucket k) = true ∧
      (sweep bucket selected keys).groups k = groupCount id keys k := by
  have included : selected (bucket k) = true := by
    cases h : selected (bucket k) with
    | true => rfl
    | false =>
      have upper := quotient_bucket_count_ge_group_count bucket id keys k
      have lower := outside (bucket k) h
      rw [fused_bucket_count] at lower
      omega
  exact ⟨included, fused_group_count bucket selected keys k included⟩

/-- A strict cutoff matters. An untracked bucket with the same count can
    contain a peer that comes first by a secondary ordering key. -/
theorem nonstrict_cutoff_loses_peer :
    let counts := sweep (fun k : Nat => k) (fun k => k == 1) [1, 0]
    counts.groups 1 = counts.buckets 0 ∧ groupCount id [1, 0] 0 = 1 ∧ 0 < 1 := by
  decide

theorem bucket_increment_fits (count rows : Nat)
    (increment : count < rows) (bounded : rows < 2 ^ 32) :
    (BitVec.ofNat 32 count + 1).toNat = count + 1 := by
  have before : count < 2 ^ 32 := by omega
  have after : count + 1 < 2 ^ 32 := by omega
  simp [BitVec.toNat_add, Nat.mod_eq_of_lt before, Nat.mod_eq_of_lt after]

end TinylambOpt.QuotientFusion
