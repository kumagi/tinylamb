import Std.Tactic

/-!
# Adaptive radix split with replay: consume every occurrence exactly once

C++ counterpart: `executor/detail/pipeline/ooc_group_agg_sink.cpp`.
The `budget` and `bucketOf` parameters stand for the memory pressure and the
radix ownership function; the theorem is uniform in both, i.e. it holds for
every split history and budget.

The **reference** folds all input occurrences in id order into a per-group
nullable state `(rows, nonNull, sum)`; NULL contributes a row but no sum.

The **optimized** plan is a list of events.  Each event carries a generation,
the bucket it owns, a list of occurrence ids and a kind (`partial`, `sealed`,
`replay`, `spill`).  Only non-`spill` events consume ids; consuming an id adds
its contribution to its group.  Ownership requires a consumed id to belong to
the event's bucket at that generation.  Sealed buckets are represented by a
single `sealed` event and must not be replayed again at the same generation.
The plan is correct exactly when every occurrence is consumed once
(`hCoverage`).

`adaptive_radix_replay_sound` proves that, for any split history and budget
satisfying coverage, every group's nullable state equals the sequential
reference.  `consumption_count_eq_one` records the exactly-once property.
The counterexample is the regression case: after partially merging the head
of a parent bucket, replaying the *whole* parent through its children adds the
head twice.
-/

namespace TinylambOpt.Research.AdaptiveRadixReplay

abbrev Val := Option Int

structure Occ where
  id : Nat
  key : Nat
  val : Val
  deriving DecidableEq, Repr, BEq

inductive EKind where
  | part | sealed | replay | spill
  deriving DecidableEq, Repr, BEq

structure Event where
  gen : Nat
  bucket : Nat
  ids : List Nat
  kind : EKind
  deriving DecidableEq, Repr, BEq

structure Agg where
  rows : Nat
  nonNull : Nat
  sum : Int
  deriving DecidableEq, Repr

@[ext] theorem Agg.ext {a b : Agg} (h1 : a.rows = b.rows)
    (h2 : a.nonNull = b.nonNull) (h3 : a.sum = b.sum) : a = b := by
  cases a
  cases b
  simp_all

def addAgg (a : Agg) (v : Val) : Agg :=
  match v with
  | none => { a with rows := a.rows + 1 }
  | some z => { rows := a.rows + 1, nonNull := a.nonNull + 1, sum := a.sum + z }

theorem addAgg_comm (a : Agg) (v w : Val) :
    addAgg (addAgg a v) w = addAgg (addAgg a w) v := by
  cases v <;> cases w <;> ext <;> simp [addAgg] <;> omega

/-! ## Occurrences, events, consumption counts -/

def occIds (occs : List Occ) : List Nat := occs.map Occ.id

def consumedIds (events : List Event) : List Nat :=
  (events.filter (fun e => e.kind != EKind.spill)).flatMap Event.ids

def consumptionCount (events : List Event) (id : Nat) : Nat :=
  List.count id (consumedIds events)

def lookupOcc (occs : List Occ) (id : Nat) : Option Occ :=
  occs.find? (fun o => o.id == id)

theorem mem_occIds {o : Occ} {occs : List Occ} (h : o ∈ occs) : o.id ∈ occIds occs := by
  unfold occIds
  exact List.mem_map.mpr ⟨o, h, rfl⟩

theorem lookupOcc_cons_of_ne (x : Occ) (xs : List Occ) (id : Nat) (hne : x.id ≠ id) :
    lookupOcc (x :: xs) id = lookupOcc xs id := by
  have hb : (x.id == id) = false := by simpa [hne]
  simp [lookupOcc, hb]

theorem lookup_self (occs : List Occ) (hnd : (occIds occs).Nodup)
    (o : Occ) (ho : o ∈ occs) : lookupOcc occs o.id = some o := by
  induction occs with
  | nil => simp at ho
  | cons x xs ih =>
      have hndc : x.id ∉ occIds xs ∧ (occIds xs).Nodup := by
        have : (x.id :: occIds xs).Nodup := by simpa [occIds] using hnd
        exact List.nodup_cons.mp this
      rcases List.mem_cons.mp ho with rfl | hoxs
      · simp [lookupOcc]
      · have hne : x.id ≠ o.id := by
          intro hxid
          have hmem : x.id ∈ occIds xs := by
            rw [hxid]
            exact mem_occIds hoxs
          exact hndc.1 hmem
        rw [lookupOcc_cons_of_ne x xs o.id hne]
        exact ih hndc.2 hoxs

theorem count_eq_one_of_mem_nodup {a : Nat} {l : List Nat}
    (hnd : l.Nodup) (h : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp [List.nodup_cons] at hnd
      simp at h
      rcases h with rfl | h
      · rw [List.count_cons]
        simp [(List.count_eq_zero).mpr hnd.1]
      · have hne : x ≠ a := by intro hxa; exact hnd.1 (hxa ▸ h)
        simp [List.count_cons, hne, ih hnd.2 h]

/-! ## Permutation of consumed ids under coverage -/

theorem consumedIds_perm (occs : List Occ) (events : List Event)
    (hnd : (occIds occs).Nodup)
    (hin : ∀ e ∈ events, ∀ id ∈ e.ids, id ∈ occIds occs)
    (hcov : ∀ o ∈ occs, consumptionCount events o.id = 1) :
    (consumedIds events).Perm (occIds occs) := by
  apply List.perm_iff_count.mpr
  intro a
  by_cases ha : a ∈ occIds occs
  · have hcountOcc : (occIds occs).count a = 1 := count_eq_one_of_mem_nodup hnd ha
    have hex : ∃ o ∈ occs, o.id = a := by
      simpa [occIds] using ha
    obtain ⟨o, ho, rfl⟩ := hex
    have hc := hcov o ho
    change List.count o.id (consumedIds events) = 1 at hc
    rw [hcountOcc, hc]
  · have hcountOcc : (occIds occs).count a = 0 := (List.count_eq_zero).mpr ha
    rw [hcountOcc]
    have hnone : a ∉ consumedIds events := by
      intro hconsumed
      rcases List.mem_flatMap.mp hconsumed with ⟨e, hef, haids⟩
      exact ha (hin e (List.mem_filter.mp hef).1 a haids)
    exact (List.count_eq_zero).mpr hnone

/-! ## Group aggregation and permutation-invariant folds -/

def stepK (occs : List Occ) (k : Nat) (a : Agg) (id : Nat) : Agg :=
  match lookupOcc occs id with
  | some o => if o.key == k then addAgg a o.val else a
  | none => a

theorem stepK_comm (occs : List Occ) (k : Nat) (a : Agg) (i j : Nat) :
    stepK occs k (stepK occs k a i) j = stepK occs k (stepK occs k a j) i := by
  unfold stepK
  cases hoi : lookupOcc occs i with
  | none =>
      cases hoj : lookupOcc occs j with
      | none => simp [hoi, hoj]
      | some oj => by_cases h : oj.key == k <;> simp [hoi, hoj, h]
  | some oi =>
      cases hoj : lookupOcc occs j with
      | none => by_cases h : oi.key == k <;> simp [hoi, hoj, h]
      | some oj =>
          by_cases hki : oi.key == k <;> by_cases hkj : oj.key == k <;>
            simp [hoi, hoj, hki, hkj, addAgg_comm]

theorem perm_fold_comm {α : Type} (step : Agg → α → Agg)
    (hcomm : ∀ a x y, step (step a x) y = step (step a y) x)
    {l₁ l₂ : List α} (h : l₁.Perm l₂) (a : Agg) :
    l₁.foldl step a = l₂.foldl step a := by
  induction h generalizing a with
  | nil => rfl
  | cons x h ih => simpa using ih (step a x)
  | swap x y l =>
      simp only [List.foldl_cons]
      rw [hcomm]
  | trans h₁ h₂ ih₁ ih₂ => exact (ih₁ a).trans (ih₂ a)

def refAgg (occs : List Occ) (k : Nat) : Agg :=
  (occs.filter (fun o => o.key == k)).foldl (fun a o => addAgg a o.val) ⟨0, 0, 0⟩

theorem foldl_congr {α β : Type} (f g : β → α → β) (l : List α) (b : β)
    (h : ∀ x ∈ l, ∀ y, f y x = g y x) : l.foldl f b = l.foldl g b := by
  induction l generalizing b with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      rw [h x (by simp) b]
      exact ih (g b x) (by intro y hy; exact h y (by simp [hy]))

theorem stepK_cons_eq (x : Occ) (xs : List Occ) (hnd : (occIds (x :: xs)).Nodup)
    (k : Nat) (a : Agg) (id : Nat) (hid : id ∈ occIds xs) :
    stepK (x :: xs) k a id = stepK xs k a id := by
  have hndc : x.id ∉ occIds xs ∧ (occIds xs).Nodup := by
    have : (x.id :: occIds xs).Nodup := by simpa [occIds] using hnd
    exact List.nodup_cons.mp this
  have hne : x.id ≠ id := by
    intro hxid
    subst id
    exact hndc.1 hid
  unfold stepK
  rw [lookupOcc_cons_of_ne x xs id hne]

theorem fold_stepK_occIds (occs : List Occ) (hnd : (occIds occs).Nodup)
    (k : Nat) (a : Agg) :
    (occIds occs).foldl (stepK occs k) a =
      (occs.filter (fun o => o.key == k)).foldl (fun b o => addAgg b o.val) a := by
  induction occs generalizing a with
  | nil => simp [occIds, stepK]
  | cons x xs ih =>
      have hndc : x.id ∉ occIds xs ∧ (occIds xs).Nodup := by
        have : (x.id :: occIds xs).Nodup := by simpa [occIds] using hnd
        exact List.nodup_cons.mp this
      have hself : lookupOcc (x :: xs) x.id = some x := by
        simp [lookupOcc]
      simp only [occIds, List.map_cons, List.foldl_cons, List.filter_cons]
      rw [show stepK (x :: xs) k a x.id =
            (if x.key == k then addAgg a x.val else a) by simp [stepK, hself]]
      have hfold : ∀ b : Agg, (List.map Occ.id xs).foldl (stepK (x :: xs) k) b =
          (List.map Occ.id xs).foldl (stepK xs k) b := by
        intro b
        apply foldl_congr
        intro id hid y
        exact stepK_cons_eq x xs hnd k y id hid
      by_cases hk : x.key == k
      · simp [hk]
        rw [hfold (addAgg a x.val)]
        exact ih hndc.2 (addAgg a x.val)
      · simp [hk]
        rw [hfold a]
        exact ih hndc.2 a

/-! ## Running the event plan -/

def runEvents (occs : List Occ) (st : Nat → Agg) : List Event → Nat → Agg
  | [], k => st k
  | e :: es, k =>
      if e.kind == EKind.spill then runEvents occs st es k
      else
        let st' : Nat → Agg := fun kk => e.ids.foldl (stepK occs kk) (st kk)
        runEvents occs st' es k

def optAgg (occs : List Occ) (events : List Event) (k : Nat) : Agg :=
  runEvents occs (fun _ => ⟨0, 0, 0⟩) events k

theorem runEvents_eq_fold (occs : List Occ) (st : Nat → Agg)
    (events : List Event) (k : Nat) :
    runEvents occs st events k =
      (consumedIds events).foldl (stepK occs k) (st k) := by
  induction events generalizing st with
  | nil => rfl
  | cons e es ih =>
      by_cases hspill : e.kind == EKind.spill
      · have hnot : (e.kind != EKind.spill) = false := by
          cases hk : e.kind
          · have hf : (EKind.part == EKind.spill) = false := by native_decide
            rw [hk, hf] at hspill
            contradiction
          · have hf : (EKind.sealed == EKind.spill) = false := by native_decide
            rw [hk, hf] at hspill
            contradiction
          · have hf : (EKind.replay == EKind.spill) = false := by native_decide
            rw [hk, hf] at hspill
            contradiction
          · rfl
        simp [runEvents, hspill, hnot, consumedIds, ih]
      · have hnot : (e.kind != EKind.spill) = true := by
          cases hk : e.kind
          · native_decide
          · native_decide
          · native_decide
          · have hf : (EKind.spill == EKind.spill) = true := by native_decide
            rw [hk, hf] at hspill
            contradiction
        simp [runEvents, hspill, hnot, consumedIds, ih, List.foldl_append]

theorem adaptive_radix_replay_sound
    (occs : List Occ) (events : List Event)
    (bucketOf : Nat → Nat → Nat) (budget : Nat)
    (hnd : (occIds occs).Nodup)
    (hin : ∀ e ∈ events, ∀ id ∈ e.ids, id ∈ occIds occs)
    (hown : ∀ e ∈ events, ∀ id ∈ e.ids, e.kind ≠ EKind.spill →
      bucketOf e.gen id = e.bucket)
    (hsealed : ∀ e ∈ events, e.kind = EKind.sealed →
      ∀ e' ∈ events, e' ≠ e → e'.gen = e.gen → e'.bucket = e.bucket →
        e'.kind ≠ EKind.part ∧ e'.kind ≠ EKind.replay)
    (hcov : ∀ o ∈ occs, consumptionCount events o.id = 1) :
    ∀ k, optAgg occs events k = refAgg occs k := by
  intro k
  unfold optAgg
  rw [runEvents_eq_fold]
  rw [perm_fold_comm (stepK occs k) (stepK_comm occs k)
        (consumedIds_perm occs events hnd hin hcov) (⟨0, 0, 0⟩ : Agg)]
  unfold refAgg
  exact fold_stepK_occIds occs hnd k ⟨0, 0, 0⟩

theorem consumption_count_eq_one (occs : List Occ) (events : List Event)
    (hnd : (occIds occs).Nodup)
    (hin : ∀ e ∈ events, ∀ id ∈ e.ids, id ∈ occIds occs)
    (hcov : ∀ o ∈ occs, consumptionCount events o.id = 1) :
    ∀ o ∈ occs, consumptionCount events o.id = 1 := hcov

/-! ## Counterexample: replaying the whole parent double-adds the head -/

def regressionOccs : List Occ :=
  [{ id := 0, key := 0, val := some 5 },
   { id := 1, key := 0, val := some 7 },
   { id := 2, key := 0, val := none }]

def regressionNaiveEvents : List Event :=
  [{ gen := 0, bucket := 0, ids := [0], kind := EKind.part },
   { gen := 1, bucket := 0, ids := [0], kind := EKind.replay },
   { gen := 1, bucket := 1, ids := [1], kind := EKind.replay },
   { gen := 1, bucket := 2, ids := [2], kind := EKind.replay }]

def regressionCorrectEvents : List Event :=
  [{ gen := 0, bucket := 0, ids := [0], kind := EKind.part },
   { gen := 1, bucket := 1, ids := [1], kind := EKind.replay },
   { gen := 1, bucket := 2, ids := [2], kind := EKind.replay }]

theorem whole_parent_replay_double_adds_counterexample :
    consumptionCount regressionNaiveEvents 0 = 2 ∧
    consumptionCount regressionCorrectEvents 0 = 1 ∧
    (optAgg regressionOccs regressionNaiveEvents 0).sum = 17 ∧
    (optAgg regressionOccs regressionCorrectEvents 0).sum = 12 ∧
    (optAgg regressionOccs regressionNaiveEvents 0).rows = 4 ∧
    (refAgg regressionOccs 0).rows = 3 := by
  decide

end TinylambOpt.Research.AdaptiveRadixReplay
