import Std.Tactic
-- C++ counterpart: expression/jit.cpp (see issues/README.md).

/-! Soundness of SIMD grouped aggregation in the presence of hash-slot
    conflicts.

    Each lane carries a grouping key, a hash slot, and a nullable INT value.
    The aggregator state of a group is `(cnt, sum)`: `cnt` counts non-NULL
    inputs and `sum` is `none` until the first non-NULL input arrives, so an
    all-NULL group keeps `sum = none` and is never turned into `0`.

    Reference computation (`seqAgg`): a sequential fold over all lanes,
    merging each lane's contribution into the group state.

    Optimized computation (`batchAgg`): lanes are split into buckets by
    hash slot, then (inside a slot) by key equality.  All lanes of one key
    are merged into a *single* pending aggregate; the batch performs exactly
    one scatter per representative key.  Because equal keys always share a
    slot, splitting by key refines the slot partition, so distinct keys in
    the same slot are never merged.

    Overflow: this model uses mathematical integers (`Int`), so SUM has no
    checked overflow and the "every prefix of the group is safe" side
    condition of a fixed-width build is vacuous.  A checked-width model would
    add the hypothesis that every prefix sum fits in the target width and
    replace `mergeSum` by a checked addition; we deliberately keep unbounded
    `Int` to match the AST ground truth (`Expression::Evaluate`). -/

namespace TinylambOpt.SimdGroupConflicts

/-- SUM merge for nullable values: NULL is the identity, and an all-NULL
    group stays NULL (never `some 0`). -/
def mergeSum : Option Int → Option Int → Option Int
  | none, b => b
  | a, none => a
  | some a, some b => some (a + b)

theorem mergeSum_assoc (a b c : Option Int) :
    mergeSum (mergeSum a b) c = mergeSum a (mergeSum b c) := by
  cases a <;> cases b <;> cases c <;> simp [mergeSum, Int.add_assoc]

theorem mergeSum_comm (a b : Option Int) : mergeSum a b = mergeSum b a := by
  cases a <;> cases b <;> simp [mergeSum, Int.add_comm]

theorem mergeSum_none_left (a : Option Int) : mergeSum none a = a := by
  cases a <;> rfl

theorem mergeSum_none_right (a : Option Int) : mergeSum a none = a := by
  cases a <;> rfl

/-- Aggregator state: COUNT of non-NULL lanes, and nullable SUM. -/
structure Agg where
  cnt : Nat
  sum : Option Int
  deriving DecidableEq, Repr

def aggZero : Agg := ⟨0, none⟩

def aggMerge : Agg → Agg → Agg
  | ⟨c₁, s₁⟩, ⟨c₂, s₂⟩ => ⟨c₁ + c₂, mergeSum s₁ s₂⟩

theorem aggMerge_assoc (a b c : Agg) :
    aggMerge (aggMerge a b) c = aggMerge a (aggMerge b c) := by
  obtain ⟨a₁, a₂⟩ := a
  obtain ⟨b₁, b₂⟩ := b
  obtain ⟨c₁, c₂⟩ := c
  simp [aggMerge, Nat.add_assoc, mergeSum_assoc]

theorem aggMerge_comm (a b : Agg) : aggMerge a b = aggMerge b a := by
  obtain ⟨a₁, a₂⟩ := a
  obtain ⟨b₁, b₂⟩ := b
  simp [aggMerge, Nat.add_comm, mergeSum_comm]

theorem aggMerge_zero_left (a : Agg) : aggMerge aggZero a = a := by
  obtain ⟨c, s⟩ := a
  simp [aggMerge, aggZero, mergeSum_none_left]

theorem aggMerge_zero_right (a : Agg) : aggMerge a aggZero = a := by
  obtain ⟨c, s⟩ := a
  simp [aggMerge, aggZero, mergeSum_none_right]

/-- One lane of the vector: grouping key, hash slot owner, nullable value. -/
structure Lane (K : Type) where
  key : K
  val : Option Int
  deriving DecidableEq, Repr

/-- Contribution of a single lane: NULL contributes `⟨0, none⟩`, a non-NULL
    value contributes `⟨1, some x⟩`. -/
def laneAgg {K : Type} (lane : Lane K) : Agg :=
  match lane.val with
  | some x => ⟨1, some x⟩
  | none => ⟨0, none⟩

/-! ## Reference: sequential fold -/

def aggStep {K : Type} (acc : Agg) (lane : Lane K) : Agg :=
  aggMerge acc (laneAgg lane)

def seqAgg {K : Type} (lanes : List (Lane K)) : Agg :=
  lanes.foldl aggStep aggZero

theorem aggStep_zero {K : Type} (lane : Lane K) : aggStep aggZero lane = laneAgg lane := by
  simp [aggStep, aggMerge_zero_left]

theorem foldl_aggStep_eq {K : Type} (lanes : List (Lane K)) (acc : Agg) :
    lanes.foldl aggStep acc = aggMerge acc (seqAgg lanes) := by
  induction lanes generalizing acc with
  | nil => simp [seqAgg, aggMerge_zero_right]
  | cons lane lanes ih =>
      simp only [List.foldl_cons]
      rw [ih (aggStep acc lane)]
      have hseq : seqAgg (lane :: lanes) = aggMerge (laneAgg lane) (seqAgg lanes) := by
        unfold seqAgg
        rw [List.foldl_cons, aggStep_zero]
        exact ih (laneAgg lane)
      rw [hseq]
      simp [aggStep, aggMerge_assoc]

theorem seqAgg_append {K : Type} (l₁ l₂ : List (Lane K)) :
    seqAgg (l₁ ++ l₂) = aggMerge (seqAgg l₁) (seqAgg l₂) := by
  unfold seqAgg
  rw [List.foldl_append]
  exact foldl_aggStep_eq l₂ (l₁.foldl aggStep aggZero)

/-! ## Optimized: one scatter per representative key -/

/-- Insert a lane into the representative table.  If a representative with
    the same key already exists, only its accumulated state is updated — no
    new write is generated.  The key comparison (not the hash slot) decides
    equality, so colliding keys stay distinct. -/
def addToReps {K : Type} [DecidableEq K] : List (K × Agg) → Lane K → List (K × Agg)
  | [], lane => [(lane.key, laneAgg lane)]
  | (k, a) :: rest, lane =>
      if lane.key == k then (k, aggMerge a (laneAgg lane)) :: rest
      else (k, a) :: addToReps rest lane

def representatives {K : Type} [DecidableEq K] (lanes : List (Lane K)) : List (K × Agg) :=
  lanes.foldl addToReps []

def repStep {K : Type} (acc : Agg) (e : K × Agg) : Agg := aggMerge acc e.2

def repsAgg {K : Type} (reps : List (K × Agg)) : Agg :=
  reps.foldl repStep aggZero

/-- The batch update: one combined write per representative key. -/
def batchAgg {K : Type} [DecidableEq K] (lanes : List (Lane K)) : Agg :=
  repsAgg (representatives lanes)

theorem repsAgg_cons {K : Type} (k : K) (a : Agg) (rest : List (K × Agg)) :
    repsAgg ((k, a) :: rest) = rest.foldl repStep a := by
  simp [repsAgg, repStep, aggMerge_zero_left]

theorem repFoldl_merge {K : Type} (rest : List (K × Agg)) (a x : Agg) :
    rest.foldl repStep (aggMerge a x) = aggMerge (rest.foldl repStep a) x := by
  induction rest generalizing a with
  | nil => rfl
  | cons e rest ih =>
      simp only [List.foldl_cons]
      have hacc : repStep (aggMerge a x) e = aggMerge (aggMerge a e.2) x := by
        simp [repStep]
        rw [aggMerge_assoc, aggMerge_comm x e.2, ← aggMerge_assoc]
      change rest.foldl repStep (repStep (aggMerge a x) e) =
        aggMerge (rest.foldl repStep (repStep a e)) x
      rw [hacc]
      change rest.foldl repStep (aggMerge (aggMerge a e.2) x) =
        aggMerge (rest.foldl repStep (aggMerge a e.2)) x
      exact ih (aggMerge a e.2)

theorem repFoldl_merge_from_zero {K : Type} (rest : List (K × Agg)) (a : Agg) :
    rest.foldl repStep a = aggMerge (repsAgg rest) a := by
  simpa [repsAgg, aggMerge_zero_left] using repFoldl_merge rest aggZero a

theorem repsAgg_add {K : Type} [DecidableEq K] (reps : List (K × Agg)) (lane : Lane K) :
    repsAgg (addToReps reps lane) = aggMerge (repsAgg reps) (laneAgg lane) := by
  induction reps with
  | nil => simp [addToReps, repsAgg, repStep, aggMerge_zero_left]
  | cons e rest ih =>
      obtain ⟨k, a⟩ := e
      by_cases h : lane.key = k
      · subst k
        simp [addToReps]
        rw [repsAgg_cons, repsAgg_cons]
        rw [repFoldl_merge_from_zero rest (aggMerge a (laneAgg lane))]
        rw [repFoldl_merge_from_zero rest a]
        rw [aggMerge_assoc]
      · have hb : (lane.key == k) = false := by simp [h]
        simp [addToReps, hb]
        rw [repsAgg_cons, repsAgg_cons]
        rw [repFoldl_merge_from_zero (addToReps rest lane) a]
        rw [ih]
        rw [repFoldl_merge_from_zero rest a]
        rw [aggMerge_assoc, aggMerge_comm (laneAgg lane) a, ← aggMerge_assoc]

theorem repsAgg_foldl {K : Type} [DecidableEq K]
    (lanes : List (Lane K)) (reps : List (K × Agg)) :
    repsAgg (lanes.foldl addToReps reps) = aggMerge (repsAgg reps) (seqAgg lanes) := by
  induction lanes generalizing reps with
  | nil => simp [seqAgg, aggMerge_zero_right]
  | cons lane lanes ih =>
      rw [List.foldl_cons, ih, repsAgg_add]
      have hseq : seqAgg (lane :: lanes) = aggMerge (laneAgg lane) (seqAgg lanes) := by
        unfold seqAgg
        rw [List.foldl_cons, aggStep_zero]
        exact foldl_aggStep_eq lanes (laneAgg lane)
      rw [hseq, aggMerge_assoc]

/-- **Reference = batch**: grouping lanes by key and emitting one scatter per
    representative reproduces the sequential nullable COUNT/SUM fold. -/
theorem simd_group_conflicts_sound {K : Type} [DecidableEq K]
    (_slot : K → Nat) (lanes : List (Lane K)) :
    batchAgg lanes = seqAgg lanes := by
  unfold batchAgg representatives
  rw [repsAgg_foldl]
  simp [repsAgg, aggMerge_zero_left]

/-! ## Lane splitting by equal keys — collisions are not merged -/

theorem addToReps_mem_key {K : Type} [DecidableEq K]
    (reps : List (K × Agg)) (lane : Lane K) :
    lane.key ∈ (addToReps reps lane).map Prod.fst := by
  induction reps with
  | nil => simp [addToReps]
  | cons e rest ih =>
      obtain ⟨k, a⟩ := e
      by_cases h : lane.key = k
      · subst h; simp [addToReps]
      · have hb : (lane.key == k) = false := by simp [h]
        simp [addToReps, hb, ih]

/-- Inserting a lane leaves the key list unchanged when its key already has a
    representative, and otherwise appends exactly that key. -/
theorem addToReps_keys {K : Type} [DecidableEq K]
    (reps : List (K × Agg)) (lane : Lane K) :
    (lane.key ∈ reps.map Prod.fst ∧ (addToReps reps lane).map Prod.fst = reps.map Prod.fst) ∨
    (lane.key ∉ reps.map Prod.fst ∧
      (addToReps reps lane).map Prod.fst = reps.map Prod.fst ++ [lane.key]) := by
  induction reps with
  | nil => simp [addToReps]
  | cons e rest ih =>
      obtain ⟨k, a⟩ := e
      by_cases h : lane.key = k
      · subst k
        left
        simp [addToReps]
      · have hb : (lane.key == k) = false := by simp [h]
        simp only [addToReps, hb, List.map_cons]
        rcases ih with ⟨hmem, hsame⟩ | ⟨hnotmem, happ⟩
        · left
          simp [h, hmem, hsame]
        · right
          simp [h, hnotmem, happ, List.cons_append]

theorem addToReps_nodup {K : Type} [DecidableEq K]
    (reps : List (K × Agg)) (lane : Lane K)
    (hnd : (reps.map Prod.fst).Nodup) :
    ((addToReps reps lane).map Prod.fst).Nodup := by
  rcases addToReps_keys reps lane with ⟨_, hsame⟩ | ⟨hnotmem, happ⟩
  · rwa [hsame]
  · rw [happ, List.nodup_append]
    refine ⟨hnd, by simp, ?_⟩
    intro a ha b hb
    have hbEq : b = lane.key := by simpa using hb
    intro hab
    exact hnotmem ((hab.trans hbEq) ▸ ha)

theorem foldl_addToReps_nodup {K : Type} [DecidableEq K]
    (lanes : List (Lane K)) (reps : List (K × Agg))
    (hnd : (reps.map Prod.fst).Nodup) :
    ((lanes.foldl addToReps reps).map Prod.fst).Nodup := by
  induction lanes generalizing reps with
  | nil => exact hnd
  | cons lane lanes ih =>
      exact ih (addToReps reps lane) (addToReps_nodup reps lane hnd)

theorem representatives_nodup {K : Type} [DecidableEq K] (lanes : List (Lane K)) :
    ((representatives lanes).map Prod.fst).Nodup := by
  unfold representatives
  exact foldl_addToReps_nodup lanes [] (by simp)

theorem mem_map_fst_exists {K V : Type} {k : K} {l : List (K × V)} :
    k ∈ l.map Prod.fst ↔ ∃ v, (k, v) ∈ l := by
  constructor
  · intro h
    obtain ⟨e, hel, heq⟩ := List.mem_map.mp h
    obtain ⟨k', v⟩ := e
    simp at heq
    subst k'
    exact ⟨v, hel⟩
  · rintro ⟨v, hv⟩
    exact List.mem_map.mpr ⟨(k, v), hv, rfl⟩

theorem addToReps_preserves_keys {K : Type} [DecidableEq K]
    (reps : List (K × Agg)) (lane : Lane K) :
    ∀ k, k ∈ reps.map Prod.fst → k ∈ (addToReps reps lane).map Prod.fst := by
  intro k hk
  induction reps with
  | nil => cases hk
  | cons e rest ih =>
      obtain ⟨k', a⟩ := e
      simp only [List.map_cons, List.mem_cons] at hk
      cases hk with
      | inl heq =>
          rw [heq]
          by_cases h : lane.key = k'
          · have hb : (lane.key == k') = true := by simp [h]
            simp [addToReps, hb]
          · have hb : (lane.key == k') = false := by simp [h]
            simp [addToReps, hb]
      | inr hkrest =>
          by_cases h : lane.key = k'
          · have hb : (lane.key == k') = true := by simp [h]
            simp [addToReps, hb, hkrest]
          · have hb : (lane.key == k') = false := by simp [h]
            simp [addToReps, hb, ih hkrest]

theorem foldl_addToReps_preserves_keys {K : Type} [DecidableEq K]
    (lanes : List (Lane K)) (reps : List (K × Agg)) :
    ∀ k, k ∈ reps.map Prod.fst → k ∈ ((lanes.foldl addToReps reps).map Prod.fst) := by
  induction lanes generalizing reps with
  | nil => intro k hk; simpa using hk
  | cons lane lanes ih =>
      intro k hk
      exact ih (addToReps reps lane) k (addToReps_preserves_keys reps lane k hk)

theorem representatives_covers_aux {K : Type} [DecidableEq K]
    (lanes : List (Lane K)) (reps : List (K × Agg)) :
    ∀ x ∈ lanes, x.key ∈ ((lanes.foldl addToReps reps).map Prod.fst) := by
  induction lanes generalizing reps with
  | nil => simp
  | cons lane lanes ih =>
      intro x hx
      rw [List.foldl_cons]
      rw [List.mem_cons] at hx
      cases hx with
      | inl hxeq =>
          subst x
          exact foldl_addToReps_preserves_keys lanes (addToReps reps lane) lane.key
            (addToReps_mem_key reps lane)
      | inr hxmem =>
          exact ih (addToReps reps lane) x hxmem

theorem representatives_covers {K : Type} [DecidableEq K] (lanes : List (Lane K)) :
    ∀ lane ∈ lanes, lane.key ∈ (representatives lanes).map Prod.fst := by
  intro lane hlane
  exact representatives_covers_aux lanes [] lane hlane

/-- Equal keys always land in the same hash slot, so grouping by key refines
    the "partition by slot, then by key" schedule. -/
theorem same_key_same_slot {K : Type} (slot : K → Nat) (a b : K) (h : a = b) :
    slot a = slot b := by
  subst h
  rfl

/-- A hash collision (different keys, same slot) produces two distinct
    representatives: the colliding keys are never folded into one group. -/
theorem slot_collision_distinct_reps {K : Type} [DecidableEq K]
    (slot : K → Nat) (lanes : List (Lane K)) (a b : K)
    (_hslot : slot a = slot b) (hne : a ≠ b)
    (ha : a ∈ lanes.map (fun l => l.key)) (hb : b ∈ lanes.map (fun l => l.key)) :
    ∃ ra ∈ representatives lanes, ∃ rb ∈ representatives lanes,
      ra.1 = a ∧ rb.1 = b ∧ ra ≠ rb := by
  obtain ⟨la, hla, hakey⟩ := List.mem_map.mp ha
  obtain ⟨lb, hlb, hbkey⟩ := List.mem_map.mp hb
  have hcov := representatives_covers lanes
  have ha' : a ∈ (representatives lanes).map Prod.fst := by
    rw [← hakey]
    exact hcov la hla
  have hb' : b ∈ (representatives lanes).map Prod.fst := by
    rw [← hbkey]
    exact hcov lb hlb
  obtain ⟨ra, hra, hrae⟩ := List.mem_map.mp ha'
  obtain ⟨rb, hrb, hrbe⟩ := List.mem_map.mp hb'
  refine ⟨ra, hra, rb, hrb, hrae, hrbe, ?_⟩
  intro heq
  apply hne
  rw [← hrae, ← hrbe, heq]

/-- The keys of the representative table are distinct, so for a given key the
    batch performs exactly one write (the merged contribution). -/
theorem nodup_fst_unique {K V : Type} [DecidableEq K] :
    ∀ (reps : List (K × V)), (reps.map Prod.fst).Nodup →
      ∀ r₁ ∈ reps, ∀ r₂ ∈ reps, r₁.1 = r₂.1 → r₁ = r₂
  | [], _, r₁, hr₁, _, _, _ => by cases hr₁
  | (k, v) :: rest, hnd, r₁, hr₁, r₂, hr₂, heq => by
      rw [List.map_cons, List.nodup_cons] at hnd
      rw [List.mem_cons] at hr₁ hr₂
      rcases hr₁ with rfl | hr₁ <;> rcases hr₂ with rfl | hr₂
      · rfl
      · have hm : r₂.1 ∈ rest.map Prod.fst := List.mem_map.mpr ⟨r₂, hr₂, rfl⟩
        exact False.elim (hnd.1 (heq ▸ hm))
      · have hm : r₁.1 ∈ rest.map Prod.fst := List.mem_map.mpr ⟨r₁, hr₁, rfl⟩
        exact False.elim (hnd.1 (heq.symm ▸ hm))
      · exact nodup_fst_unique rest hnd.2 r₁ hr₁ r₂ hr₂ heq

theorem same_key_combined_once {K : Type} [DecidableEq K]
    (lanes : List (Lane K)) (k : K)
    (hk : k ∈ lanes.map (fun l => l.key)) :
    ∃ rep ∈ representatives lanes, rep.1 = k ∧
      ∀ other ∈ representatives lanes, other.1 = k → other = rep := by
  obtain ⟨lane, hlane, hkey⟩ := List.mem_map.mp hk
  have hcov := representatives_covers lanes
  have hk' : k ∈ (representatives lanes).map Prod.fst := by
    rw [← hkey]
    exact hcov lane hlane
  obtain ⟨rep, hrep, hrepkey⟩ := List.mem_map.mp hk'
  refine ⟨rep, hrep, hrepkey, ?_⟩
  intro other hother hotherkey
  apply nodup_fst_unique (representatives lanes) (representatives_nodup lanes)
  · exact hother
  · exact hrep
  · rw [hotherkey, hrepkey]

/-! ## Boundary example: naive per-slot single-write loses increments -/

/-- Buggy (SIMD-shaped) update: per hash slot, gather a single representative
    lane's value, add once, and scatter once.  All other lanes of the slot are
    ignored, so duplicate keys in the same slot lose their increments. -/
def naiveSlotWrite {K : Type} [DecidableEq K] (slot : K → Nat) (lanes : List (Lane K)) : Agg :=
  let reps : List (Nat × Agg) :=
    lanes.foldl (fun acc lane =>
      if (acc.map Prod.fst).contains (slot lane.key) then acc
      else acc ++ [(slot lane.key, laneAgg lane)]) []
  reps.foldl (fun acc (_, a) => aggMerge acc a) aggZero

def exampleLanes : List (Lane Nat) :=
  [⟨0, some 1⟩, ⟨1, some 1⟩, ⟨0, some 1⟩, ⟨0, some 1⟩]

/-- Keys `[A, B, A, A]` (A = 0, B = 1): the sequential fold records three
    increments for A (COUNT 4, SUM 4), while the naive gather-add-scatter
    records only one (COUNT 2, SUM 2). -/
theorem naive_gather_add_scatter_loses_increments :
    seqAgg exampleLanes = ⟨4, some (4 : Int)⟩ ∧
    naiveSlotWrite (fun k : Nat => k) exampleLanes = ⟨2, some (2 : Int)⟩ ∧
    naiveSlotWrite (fun k : Nat => k) exampleLanes ≠ seqAgg exampleLanes := by
  decide

def allNullLanes : List (Lane Nat) :=
  [⟨0, none⟩, ⟨0, none⟩]

/-- An all-NULL group keeps `sum = none` (never `0`) in both computations. -/
theorem all_null_group_sum_is_not_zero :
    seqAgg allNullLanes = ⟨0, none⟩ ∧
    batchAgg allNullLanes = ⟨0, none⟩ := by
  decide

def lookupAgg [DecidableEq K] (k : K) : List (K × Agg) → Agg
  | [] => aggZero
  | (j,a) :: rest => if j = k then a else lookupAgg k rest

private theorem lookup_insert [DecidableEq K] (k : K) (rs : List (K × Agg)) (l : Lane K) :
    lookupAgg k (addToReps rs l) =
    if l.key = k then aggStep (lookupAgg k rs) l else lookupAgg k rs := by
  induction rs with
  | nil => simp [lookupAgg, addToReps, aggStep_zero]
  | cons p rs ih =>
    obtain ⟨j,a⟩ := p
    by_cases h : l.key = j <;> by_cases e : j = k <;>
      simp_all [addToReps, lookupAgg, aggStep]

/-- Every key sees exactly its sequential nullable COUNT and SUM, including
    keys absent from the batch. The lookup uses the actual representative table. -/
theorem representatives_per_key [DecidableEq K] (lanes : List (Lane K)) (k : K) :
    lookupAgg k (representatives lanes) = seqAgg (lanes.filter (fun l => l.key == k)) := by
  suffices h : ∀ rs : List (K × Agg), lookupAgg k (lanes.foldl addToReps rs) =
      (lanes.filter (fun l => l.key == k)).foldl aggStep (lookupAgg k rs) from h []
  induction lanes with
  | nil => intro rs; rfl
  | cons l ls ih =>
    intro rs
    by_cases h : l.key = k <;> simp [h, ih, lookup_insert]

def fits (a : Agg) : Bool :=
  a.sum.all fun v => decide (-(2^63) ≤ v ∧ v < 2^63)

def sequentialChecked : List (Lane K) → Agg → Option Agg
  | [], a => some a
  | l :: ls, a =>
    let b := aggStep a l
    if fits b then sequentialChecked ls b else none

def safePrefixes : List (Lane K) → Agg → Prop
  | [], _ => True
  | l :: ls, a => fits (aggStep a l) = true ∧ safePrefixes ls (aggStep a l)

private theorem checked_fold (ls : List (Lane K)) (a : Agg) (hs : safePrefixes ls a) :
    sequentialChecked ls a = some (ls.foldl aggStep a) := by
  induction ls generalizing a with
  | nil => rfl
  | cons l ls ih => simpa [sequentialChecked, hs.1] using ih _ hs.2

theorem checked_scatter_per_key [DecidableEq K] (lanes : List (Lane K)) (k : K)
    (initial : Agg)
    (safe : safePrefixes (lanes.filter (fun l => l.key == k)) initial)
    (partialSafe : safePrefixes (lanes.filter (fun l => l.key == k)) aggZero) :
    sequentialChecked (lanes.filter (fun l => l.key == k)) aggZero =
      some (lookupAgg k (representatives lanes)) ∧
    sequentialChecked (lanes.filter (fun l => l.key == k)) initial =
      some (aggMerge initial (lookupAgg k (representatives lanes))) := by
  rw [checked_fold _ _ partialSafe, checked_fold _ _ safe, representatives_per_key]
  exact ⟨rfl, congrArg some (foldl_aggStep_eq _ _)⟩

/-- The reduction actually aborts before storing an out-of-range partial
    state. Independent keys can be interleaved in any order within a batch. -/
def checkedReps [DecidableEq K] (table : List (K × Agg)) :
    List (Lane K) → Option (List (K × Agg))
  | [] => some table
  | l :: ls =>
    if fits (aggStep (lookupAgg l.key table) l) then checkedReps (addToReps table l) ls
    else none

theorem checked_table_sound [DecidableEq K] (table : List (K × Agg)) (ls : List (Lane K))
    (safe : ∀ k, safePrefixes (ls.filter (fun l => l.key == k)) (lookupAgg k table)) :
    checkedReps table ls = some (ls.foldl addToReps table) := by
  induction ls generalizing table with
  | nil => rfl
  | cons l ls ih =>
    have atKey := safe l.key
    simp only [List.filter_cons,beq_self_eq_true,↓reduceIte,safePrefixes] at atKey
    rw [checkedReps,if_pos atKey.1,List.foldl_cons]
    apply ih
    intro k
    rw [lookup_insert]
    by_cases hk : l.key = k
    · subst k
      simpa using atKey.2
    · simpa [hk] using safe k

theorem checked_batch_scatter [DecidableEq K] (ls : List (Lane K))
    (initial : K → Agg) (safe : ∀ k,
      safePrefixes (ls.filter (fun l => l.key == k)) (initial k))
    (partials : ∀ k, safePrefixes (ls.filter (fun l => l.key == k)) aggZero) :
    ∃ table, checkedReps [] ls = some table ∧ ∀ k,
      sequentialChecked (ls.filter (fun l => l.key == k)) (initial k) =
        some (aggMerge (initial k) (lookupAgg k table)) := by
  exact ⟨representatives ls, checked_table_sound [] ls partials,
    fun k => (checked_scatter_per_key ls k _ (safe k) (partials k)).2⟩

end TinylambOpt.SimdGroupConflicts
