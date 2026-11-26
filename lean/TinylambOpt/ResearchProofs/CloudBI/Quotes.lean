import TinylambOpt.ResearchGoals.CloudBI.Quotes
import TinylambOpt.ResearchProofs.CloudBI.Common

/-!
These models describe SQL scans and aggregates over file metadata and selected column
data.

Proved (10 claims, including the following)

- `CandleFringe` — Answer arbitrary half-open time windows from whole fixed-width
  candles and the two partial edges. Each event belongs to a tile by its event time,
  including late arrivals.
- `CloseTournament` — `∀ (runs : List (List Tick)) (input : List Tick),
  runs.flatten.Perm input → (∀ a ∈ runs.flatten, ∀ b ∈ runs.flatten, a.ts = b.ts →
  a.tie = b.tie → a.bid = b.bid) → latest (runs.filterMap lastBid) = lastBid input`
- `FenceRead` — Prune a physically clustered run using its symbol fence. An overlapping
  fence can have holes; rows still undergo the exact symbol and time predicate.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.CloudBI.Quotes

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.CloudBI.Quotes
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-!
# Proof strategy overview

We prove the ten Quotes propositions from `ResearchGoals/CloudBI/Quotes.lean`:

* `StaleRollup`: the two watermark half-open batch slices of the hit set are a
  disjoint partition of the `≤ snapshot` slice; `filter_disjoint_perm` plus
  `summarize_append`/`summarize_perm` finish.
* `SparseRefresh`: `foldl_bucket_partition` over the sealed distinct batches,
  then the unsealed remainder is the complementary filter.
* `CloseTournament`: `latest` of a list with functional dependency
  `(ts, tie) → bid` is characterized by a fold invariant (the accumulator always
  dominates everything seen so far); permutation transfers the winner.
* `CandleFringe`: bucket ids present in the rows satisfy `ids.contains (bucket r)`
  iff `whole (bucket r)`; complete buckets and fringe rows partition the window.
* `SpreadMoments`: componentwise `summarize_cnt/nn/sum` identities.
* `FenceRead`: a run with an eligible row has its symbol inside the run's
  `[min?, max?]` fence, so every eligible row of every run survives in `opened`;
  `opened` is a sublist of `runs`.
* `ReclusterSwap`: the read window only sees multiplicities, so permuting the
  `old`/`fresh` part before `++ rest` is invisible; `pin < cut` decides which
  side of the `ite` is used.
* `RetryRefresh`: fold invariant: the deduplicated key list (in first-seen
  order) stays `Nodup` with exactly the keys seen; `perm_of_nodup_mem` turns it
  into a permutation of `deliveries.eraseDups`, and `hflat` folds the per-batch
  stats into `stats` of the flat-mapped payload.
* `ReclusterBudget`: exchange argument — for a descending-sorted list, any nodup
  selection of `m` indices has saving-sum at most the sum of the top `m`;
  `take` of a shorter prefix is bounded because savings are nonnegative.
* `MaintenanceBreakEven`: set `d = time - s`, expand with distributivity and
  cancel `d * (fresh + query)`.
-/

private theorem contains_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.eraseDups.contains a = l.contains a := by
  cases h1 : l.contains a with
  | true =>
      exact List.contains_iff_mem.mpr (List.mem_eraseDups.mpr (List.contains_iff_mem.mp h1))
  | false =>
      cases h2 : l.eraseDups.contains a with
      | true =>
          have : l.contains a = true :=
            List.contains_iff_mem.mpr (List.mem_eraseDups.mp (List.contains_iff_mem.mp h2))
          exact absurd this (fun hc => Bool.false_ne_true (h1 ▸ hc))
      | false => exact rfl

private theorem stats_perm {vs ws : List Tick} (h : List.Perm vs ws) : stats vs = stats ws :=
  summarize_perm (h.map Tick.bid)

private theorem stats_append (xs ys : List Tick) :
    stats (xs ++ ys) = plus (stats xs) (stats ys) := by
  unfold stats
  rw [List.map_append, summarize_append]

/-! ### CloseTournament -/

/-- Natural (uncoerced) form of the goal's tournament comparison. -/
private def beatsN (x y : Close) : Prop := x.1 > y.1 ∨ (x.1 = y.1 ∧ x.2.1 > y.2.1)

private instance : DecidableRel beatsN := fun x y => by unfold beatsN; exact inferInstance

/-- `x` is at most `y` in the (event time, feed sequence) lexicographic order. -/
private def lexLe (x y : Close) : Prop := x.1 < y.1 ∨ (x.1 = y.1 ∧ x.2.1 ≤ y.2.1)

private theorem lexLe_le {x y : Close} (h : lexLe x y) : x.1 ≤ y.1 := by
  cases h with
  | inl h => exact Nat.le_of_lt h
  | inr h => exact h.1 ▸ Nat.le_refl _

private theorem lexLe_trans {x y z : Close} (h1 : lexLe x y) (h2 : lexLe y z) : lexLe x z := by
  unfold lexLe at *
  omega

private theorem lexLe_total (x y : Close) : lexLe x y ∨ lexLe y x := by
  unfold lexLe
  omega

private theorem lexLe_antisym {x y : Close} (h1 : lexLe x y) (h2 : lexLe y x) :
    x.1 = y.1 ∧ x.2.1 = y.2.1 := by
  unfold lexLe at h1 h2
  omega

private theorem beatsN_iff_not_lexLe (x y : Close) : beatsN x y ↔ ¬ lexLe x y := by
  unfold beatsN lexLe
  omega

private theorem beatsN_irrefl (x : Close) : ¬ beatsN x x := by
  intro h
  have hn := (beatsN_iff_not_lexLe x x).mp h
  exact hn (Or.inr (And.intro rfl (Nat.le_refl _)))

private theorem beatsN_trans {x y z : Close} (h1 : beatsN x y) (h2 : beatsN y z) : beatsN x z := by
  refine (beatsN_iff_not_lexLe x z).mpr ?_
  intro hxz
  have hyx : lexLe y x :=
    (lexLe_total y x).resolve_right ((beatsN_iff_not_lexLe x y).mp h1)
  exact ((beatsN_iff_not_lexLe y z).mp h2) (lexLe_trans hyx hxz)

/-- Not being a strict winner means being lexicographically at most. -/
private theorem lexLe_of_not_beatsN {x y : Close} (h : ¬ beatsN x y) : lexLe x y := by
  by_cases hl : lexLe x y
  · exact hl
  · exact absurd ((beatsN_iff_not_lexLe x y).mpr hl) h

/-- The goal's coerced condition agrees with `beatsN`. -/
private theorem C_iff (x y : Close) :
    (x.1 > y.1 || (x.1 == y.1 && x.2.1 > y.2.1)) ↔ beatsN x y := by
  by_cases h1 : x.1 > y.1
  · refine ⟨fun _ => Or.inl h1, fun _ => ?_⟩
    have hd : decide (x.1 > y.1) = true := decide_eq_true h1
    show (decide (x.1 > y.1) || (x.1 == y.1 && decide (x.2.1 > y.2.1))) = true
    rw [hd]
    exact rfl
  · have hd : decide (x.1 > y.1) = false := decide_eq_false h1
    refine ⟨fun h => ?_, fun h => ?_⟩
    · rw [hd] at h
      simp at h
      exact Or.inr h
    · rcases h with hlt | ⟨heq, hgt⟩
      · exact absurd hlt h1
      · show (decide (x.1 > y.1) || (x.1 == y.1 && decide (x.2.1 > y.2.1))) = true
        rw [hd]
        simp
        exact ⟨heq, hgt⟩

private def step (best : Option Close) (x : Close) : Option Close :=
  match best with
  | none => some x
  | some y => if x.1 > y.1 || (x.1 == y.1 && x.2.1 > y.2.1) then some x else best

private theorem step_some (a x : Close) :
    step (some a) x = if beatsN x a then some x else some a := by
  show (if x.1 > a.1 || (x.1 == a.1 && x.2.1 > a.2.1) then some x else some a) = _
  by_cases h : x.1 > a.1 || (x.1 == a.1 && x.2.1 > a.2.1)
  · rw [ite_eq_left h, ite_eq_left ((C_iff x a).mp h)]
  · rw [ite_eq_right h, ite_eq_right (fun hb => h ((C_iff x a).mpr hb))]

private theorem step_ne_none (a x : Close) : step (some a) x ≠ none := by
  rw [step_some]
  split
  · exact fun h => Option.some_ne_none _ h
  · exact fun h => Option.some_ne_none _ h

private theorem foldl_some :
    ∀ (t : List Close) (a : Close), ∃ w, t.foldl step (some a) = some w := by
  intro t
  induction t with
  | nil => intro a; exact ⟨_, rfl⟩
  | cons x ds ih =>
      intro a
      rw [List.foldl_cons]
      cases hstep : step (some a) x with
      | none => exact absurd hstep (step_ne_none a x)
      | some b =>
          obtain ⟨w, hw⟩ := ih b
          exact ⟨w, hw⟩

/-- If the accumulator already dominates the whole remaining list, the fold never moves. -/
private theorem foldl_stays : ∀ (t : List Close) (a : Close),
    (∀ x ∈ t, lexLe x a) → t.foldl step (some a) = some a := by
  intro t
  induction t with
  | nil => intro a _; rfl
  | cons x ds ih =>
      intro a h
      rw [List.foldl_cons, step_some,
        ite_eq_right (fun hb => (beatsN_iff_not_lexLe x a).mp hb (h x (List.Mem.head _)))]
      exact ih a (fun y hy => h y (List.Mem.tail _ hy))

/-- Running accumulator facts: the value dominates everything seen and is a member. -/
private theorem foldl_dom : ∀ (t : List Close) (a w : Close),
    t.foldl step (some a) = some w →
      (∀ x ∈ t, lexLe x w) ∧ lexLe a w ∧ w ∈ a :: t := by
  intro t
  induction t with
  | nil =>
      intro a w h
      simp only [List.foldl_nil] at h
      have : w = a := (Option.some.inj h).symm
      subst this
      exact ⟨fun x hx => (by cases hx), Or.inr ⟨rfl, Nat.le_refl _⟩, List.Mem.head []⟩
  | cons x ds ih =>
      intro a w h
      rw [List.foldl_cons] at h
      by_cases hb : beatsN x a
      · -- the accumulator moved to `x`
        rw [step_some, ite_eq_left hb] at h
        obtain ⟨hd, hxw, hm⟩ := ih x w h
        refine ⟨fun y hy => by
          cases hy with
          | head _ => exact hxw
          | tail _ hy => exact hd y hy,
          lexLe_trans
            ((lexLe_total a x).resolve_right (fun hn => (beatsN_iff_not_lexLe x a).mp hb hn))
            hxw,
          ?_⟩
        cases hm with
        | head _ => exact List.Mem.tail a (List.Mem.head ds)
        | tail _ hm2 => exact List.Mem.tail a (List.Mem.tail x hm2)
      · -- the accumulator stayed at `a`
        rw [step_some, ite_eq_right hb] at h
        obtain ⟨hd, haw, hm⟩ := ih a w h
        refine ⟨fun y hy => by
          cases hy with
          | head _ => exact lexLe_trans (lexLe_of_not_beatsN hb) haw
          | tail _ hy => exact hd y hy,
          haw, ?_⟩
        cases hm with
        | head _ => exact List.Mem.head (x :: ds)
        | tail _ hm2 => exact List.Mem.tail a (List.Mem.tail x hm2)

private theorem latest_def (xs : List Close) : latest xs = xs.foldl step none := rfl

private theorem latest_mem_some {xs : List Close} (z : Close) (hz : z ∈ xs) :
    ∃ w, latest xs = some w := by
  rcases xs with _ | ⟨x, t⟩
  · cases hz
  · obtain ⟨w, hw⟩ := foldl_some t x
    exact ⟨w, hw⟩

private theorem latest_some_dom {xs : List Close} {z : Close} (hz : latest xs = some z) :
    ∀ x ∈ xs, lexLe x z := by
  rcases xs with _ | ⟨x, t⟩
  · exact absurd hz.symm (Option.some_ne_none z)
  · have hz' : List.foldl step (some x) t = some z := hz
    obtain ⟨hd, hx, _⟩ := foldl_dom t x z hz'
    intro p hp
    cases hp with
    | head _ => exact hx
    | tail _ hp => exact hd p hp

private theorem latest_some_mem {xs : List Close} {z : Close} (hz : latest xs = some z) :
    z ∈ xs := by
  rcases xs with _ | ⟨x, t⟩
  · exact absurd hz.symm (Option.some_ne_none z)
  · have hz' : List.foldl step (some x) t = some z := hz
    exact (foldl_dom t x z hz').2.2

private def fdPts (xs : List Close) : Prop :=
  ∀ x ∈ xs, ∀ y ∈ xs, x.1 = y.1 → x.2.1 = y.2.1 → x.2.2 = y.2.2

private theorem latest_eq_of_dom {xs : List Close} {z : Close}
    (hmem : z ∈ xs) (hdom : ∀ x ∈ xs, lexLe x z) (hfd : fdPts xs) :
    latest xs = some z := by
  obtain ⟨w, hw⟩ := latest_mem_some z hmem
  have h1 : lexLe w z := hdom w (latest_some_mem hw)
  have h2 : lexLe z w := latest_some_dom hw z hmem
  obtain ⟨e1, e2⟩ := lexLe_antisym h1 h2
  have hew : w = z := by
    apply Prod.ext
    · exact e1
    · apply Prod.ext
      · exact e2
      · exact hfd w (latest_some_mem hw) z hmem e1 e2
  rw [hew] at hw
  exact hw

private theorem fdPts_mono {xs ys : List Close} (h : ∀ c ∈ xs, c ∈ ys) (H : fdPts ys) :
    fdPts xs :=
  fun x hx y hy exx eyy => H x (h x hx) y (h y hy) exx eyy

private theorem latest_perm {xs ys : List Close} (hp : xs.Perm ys) (hfd : fdPts xs) :
    latest xs = latest ys := by
  cases h : latest xs with
  | none =>
      have hxs : xs = [] := by
        refine List.eq_nil_iff_forall_not_mem.mpr ?_
        intro x hx
        obtain ⟨v, hv⟩ := latest_mem_some x hx
        exact absurd (hv ▸ h) (Option.some_ne_none v)
      have h2 : xs.length = 0 := by rw [hxs]; rfl
      have hl : ys.length = 0 := hp.symm.length_eq.trans h2
      have hys : ys = [] := List.length_eq_zero_iff.mp hl
      rw [hys, latest_def, List.foldl_nil]
  | some w =>
      exact (latest_eq_of_dom (hp.mem_iff.mp (latest_some_mem h))
        (fun y hy => latest_some_dom h y (hp.mem_iff.mpr hy))
        (fdPts_mono (fun c hc => hp.symm.mem_iff.mp hc) hfd)).symm

private def cand (t : List Tick) : List Close :=
  t.filterMap (fun r => r.bid.map (fun p => (r.ts, r.tie, p)))

private theorem lastBid_def (t : List Tick) : lastBid t = latest (cand t) := rfl

private theorem mem_cand {t : List Tick} {r : Tick} {p : Int}
    (hr : r ∈ t) (hb : r.bid = some p) : (r.ts, r.tie, p) ∈ cand t :=
  List.mem_filterMap.mpr ⟨r, hr, by rw [hb]; rfl⟩

private theorem exists_of_mem_cand {t : List Tick} {c : Close} (hc : c ∈ cand t) :
    ∃ r ∈ t, ∃ p : Int, r.bid = some p ∧ c = (r.ts, r.tie, p) := by
  obtain ⟨r, hr, hmap⟩ := List.mem_filterMap.mp hc
  cases hb : r.bid with
  | none => simp [hb] at hmap
  | some p =>
      rw [hb] at hmap
      have h : (r.ts, r.tie, p) = c := Option.some.inj hmap
      exact ⟨r, hr, p, hb, h.symm⟩

private theorem cand_mono {t u : List Tick} (h : ∀ r ∈ t, r ∈ u) :
    ∀ c ∈ cand t, c ∈ cand u := by
  intro c hc
  obtain ⟨r, hr, p, hp, rfl⟩ := exists_of_mem_cand hc
  exact mem_cand (h r hr) hp

private theorem cand_flatten {runs : List (List Tick)} (rs : List Tick) (hrs : rs ∈ runs) :
    ∀ c ∈ cand rs, c ∈ cand (runs.flatten) :=
  cand_mono (fun _r hr => List.mem_flatten.mpr ⟨rs, hrs, hr⟩)

private def wins (runs : List (List Tick)) : List Close := runs.filterMap lastBid

private theorem mem_wins {runs : List (List Tick)} {rs : List Tick} (hrs : rs ∈ runs)
    (hw : lastBid rs = some w) : w ∈ wins runs :=
  List.mem_filterMap.mpr ⟨rs, hrs, hw⟩

private theorem exists_of_mem_wins {runs : List (List Tick)} {w : Close}
    (hw : w ∈ wins runs) : ∃ rs ∈ runs, lastBid rs = some w := List.mem_filterMap.mp hw

private theorem fd_cand {input : List Tick}
    (fd : ∀ a ∈ input, ∀ b ∈ input, a.ts = b.ts → a.tie = b.tie → a.bid = b.bid) :
    fdPts (cand input) := by
  intro x hx y hy exx eyy
  obtain ⟨r, hr, p, hp, hx'⟩ := exists_of_mem_cand hx
  obtain ⟨q, hq, o, hq', hy'⟩ := exists_of_mem_cand hy
  cases hx'
  cases hy'
  have f1 : r.ts = q.ts := exx
  have f2 : r.tie = q.tie := eyy
  have ebid : r.bid = q.bid := fd r hr q hq f1 f2
  rw [hp, hq'] at ebid
  exact Option.some.inj ebid

private theorem qt_main (runs : List (List Tick)) (input : List Tick)
    (hp : runs.flatten.Perm input)
    (fd : ∀ a ∈ runs.flatten, ∀ b ∈ runs.flatten, a.ts = b.ts → a.tie = b.tie → a.bid = b.bid) :
    latest (wins runs) = lastBid input := by
  have fd' : ∀ a ∈ input, ∀ b ∈ input, a.ts = b.ts → a.tie = b.tie → a.bid = b.bid := by
    intro a ha b hb
    exact fd a (hp.mem_iff.mpr ha) b (hp.mem_iff.mpr hb)
  have hfdin : fdPts (cand input) := fd_cand fd'
  have xfer : ∀ c ∈ cand runs.flatten, c ∈ cand input :=
    cand_mono fun _ hr => hp.mem_iff.mp hr
  cases h : latest (cand input) with
  | none =>
      have hcand : cand input = [] := by
        refine List.eq_nil_iff_forall_not_mem.mpr ?_
        intro c hc
        obtain ⟨v, hv⟩ := latest_mem_some c hc
        exact absurd (hv ▸ h) (Option.some_ne_none v)
      have hwins : wins runs = [] := by
        refine List.eq_nil_iff_forall_not_mem.mpr ?_
        intro w hw
        obtain ⟨rs, hrs, hlast⟩ := exists_of_mem_wins hw
        have hin : w ∈ cand input := xfer w (cand_flatten rs hrs w (latest_some_mem hlast))
        rw [hcand] at hin
        cases hin
      rw [lastBid_def, hwins]
      exact (by rw [latest_def, List.foldl_nil] : latest ([] : List Close) = none).trans h.symm
  | some z =>
      have rhs : lastBid input = some z := by rw [lastBid_def, h]
      have hz_mem : z ∈ cand input := latest_some_mem h
      have hz_dom : ∀ c ∈ cand input, lexLe c z := latest_some_dom h
      have win : z ∈ wins runs := by
        obtain ⟨r, hr, p, hp', rfl⟩ := exists_of_mem_cand hz_mem
        obtain ⟨rs, hrs, hr'⟩ := List.mem_flatten.mp (hp.symm.mem_iff.mp hr)
        have hz' : (r.ts, r.tie, p) ∈ cand rs := mem_cand hr' hp'
        have hlast : lastBid rs = some (r.ts, r.tie, p) := by
          rw [lastBid_def]
          refine latest_eq_of_dom hz'
            (fun c hc => hz_dom c (xfer c (cand_flatten rs hrs c hc))) ?_
          exact fdPts_mono (fun c hc => xfer c (cand_flatten rs hrs c hc)) hfdin
        exact mem_wins hrs hlast
      have wins_dom : ∀ w ∈ wins runs, lexLe w z := by
        intro w hw
        obtain ⟨rs, hrs, hlast⟩ := exists_of_mem_wins hw
        exact hz_dom w (xfer w (cand_flatten rs hrs w (latest_some_mem hlast)))
      have wins_fd : fdPts (wins runs) :=
        fdPts_mono (fun w hw => by
          obtain ⟨rs, hrs, hlast⟩ := exists_of_mem_wins hw
          exact xfer w (cand_flatten rs hrs w (latest_some_mem hlast))) hfdin
      rw [rhs, latest_eq_of_dom win wins_dom wins_fd]

theorem sound_CloseTournament : CloseTournament.Goal := qt_main

/-! ### StaleRollup -/

theorem sound_StaleRollup : StaleRollup.Goal := by
  intro rows sym lo hi watermark snapshot hw
  dsimp only
  let hits := rows.filter (eligible sym lo hi)
  let p : Tick → Bool := fun r => r.batch ≤ watermark
  let q : Tick → Bool := fun r => watermark < r.batch && r.batch ≤ snapshot
  have hdis : ∀ x : Tick, (p x && q x) = false := by
    intro x
    simp only [p, q]
    cases h1 : (decide (x.batch ≤ watermark) : Bool) with
    | true =>
        cases h2 : (decide (watermark < x.batch) : Bool) with
        | true =>
            exact absurd (Nat.lt_of_le_of_lt (of_decide_eq_true h1) (of_decide_eq_true h2))
              (Nat.lt_irrefl x.batch)
        | false => exact Bool.and_false _
    | false => exact Bool.false_and _
  have htor : List.filter (fun x => p x || q x) hits
      = List.filter (fun r => r.batch ≤ snapshot) hits := by
    apply List.filter_congr
    intro x _
    simp only [p, q]
    cases h1 : (decide (x.batch ≤ watermark) : Bool) with
    | true =>
        cases h3 : (decide (x.batch ≤ snapshot) : Bool) with
        | true => cases h2 : (decide (watermark < x.batch) : Bool) <;> simp
        | false =>
            exfalso
            have ha := of_decide_eq_true h1
            have hb := of_decide_eq_false h3
            omega
    | false =>
        have h2 : decide (watermark < x.batch) = true :=
          decide_eq_true (Nat.lt_of_not_le (of_decide_eq_false h1))
        cases h3 : (decide (x.batch ≤ snapshot) : Bool) <;> simp [h2]
  have hstep : plus (stats (hits.filter p)) (stats (hits.filter q))
      = stats (hits.filter (fun x => p x || q x)) := by
    show plus (summarize ((hits.filter p).map Tick.bid))
        (summarize ((hits.filter q).map Tick.bid)) = _
    rw [← summarize_append, ← List.map_append]
    exact summarize_perm
      ((filter_disjoint_perm p q hits (fun x _ => hdis x)).map Tick.bid)
  rw [htor] at hstep
  exact hstep

/-! ### SparseRefresh -/

theorem sound_SparseRefresh : SparseRefresh.Goal := by
  intro rows sealed snapshot sym
  dsimp only
  let live := rows.filter (fun r => r.batch ≤ snapshot && r.sym == sym)
  let bs := sealed.eraseDups
  let p : Tick → Bool := fun r => bs.contains r.batch
  let q : Tick → Bool := fun r => !bs.contains r.batch
  have hfold :
      bs.foldl (fun a b => plus a (stats (live.filter (fun r => r.batch == b)))) zero
        = stats (live.filter p) := by
    have h := foldl_bucket_partition (fun r : Tick => r.batch) Tick.bid bs
      (nodup_eraseDups sealed) zero live
    rw [plus_zero'] at h
    exact h
  have hq : List.filter (fun r => !sealed.contains r.batch) live = List.filter q live := by
    apply List.filter_congr
    intro r _
    exact congrArg (fun z => !z) (contains_eraseDups sealed r.batch).symm
  have hdis : ∀ x ∈ live, (p x && q x) = false := by
    intro x _
    simp only [p, q]
    cases h : bs.contains x.batch <;> simp
  have hself : List.filter (fun x => p x || q x) live = live := by
    rw [List.filter_eq_self]
    intro x _
    simp only [p, q]
    cases h : bs.contains x.batch <;> simp
  have hstep : plus (stats (live.filter p)) (stats (live.filter q)) = stats live := by
    have h2 : plus (stats (live.filter p)) (stats (live.filter q))
        = stats (live.filter (fun x => p x || q x)) := by
      show plus (summarize ((live.filter p).map Tick.bid))
          (summarize ((live.filter q).map Tick.bid)) = _
      rw [← summarize_append, ← List.map_append]
      exact summarize_perm ((filter_disjoint_perm p q live hdis).map Tick.bid)
    rw [h2, hself]
  rw [hfold, hq]
  exact hstep

/-! ### CandleFringe -/

private theorem nodup_filter {α : Type} (p : α → Bool) :
    ∀ (l : List α), l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro _; exact List.Pairwise.nil
  | cons x l ih =>
      intro h
      have hx : x ∉ l := (List.nodup_cons.mp h).1
      have hl : l.Nodup := (List.nodup_cons.mp h).2
      cases g : p x with
      | true =>
          rw [List.filter_cons_of_pos g]
          refine List.Pairwise.cons (fun b hb => ?_) (ih hl)
          intro hbx
          exact absurd (hbx ▸ (List.mem_filter.mp hb).1) hx
      | false =>
          rw [List.filter_cons_of_neg (by simpa using g)]
          exact ih hl

theorem sound_CandleFringe : CandleFringe.Goal := by
  intro rows width lo hi hw
  dsimp only
  let bucket : Tick → Nat := fun r => r.ts / width
  let whole : Nat → Bool := fun k => decide (lo ≤ k * width ∧ (k + 1) * width ≤ hi)
  let ids := (rows.map bucket).eraseDups.filter whole
  let p : Tick → Bool := fun r => ids.contains (bucket r)
  let q : Tick → Bool := fun r => lo ≤ r.ts && r.ts < hi && !whole (bucket r)
  have hnd : ids.Nodup :=
    nodup_filter whole _ (nodup_eraseDups (rows.map bucket))
  have hkey : ∀ k : Nat, whole k = true → lo ≤ k * width ∧ (k + 1) * width ≤ hi := by
    intro k hk
    exact of_decide_eq_true hk
  have hlb : ∀ (r : Tick), bucket r * width ≤ r.ts := fun r => Nat.div_mul_le_self r.ts width
  have hut : ∀ (r : Tick), r.ts < (bucket r + 1) * width := by
    intro r
    have h3 := Nat.add_lt_add_left (Nat.mod_lt r.ts hw) (r.ts / width * width)
    have h2 : r.ts / width * width + r.ts % width = r.ts := Nat.div_add_mod' r.ts width
    rw [h2] at h3
    show r.ts < (r.ts / width + 1) * width
    rw [Nat.add_mul, Nat.one_mul]
    exact h3
  have hpE : ∀ r ∈ rows, p r = whole (bucket r) := by
    intro r hr
    have hin : bucket r ∈ rows.map bucket := List.mem_map.mpr ⟨r, hr, rfl⟩
    cases h : whole (bucket r) with
    | true =>
        exact List.contains_iff_mem.mpr
          (List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hin, h⟩)
    | false =>
        cases hc : ids.contains (bucket r) with
        | true =>
            have hm := List.contains_iff_mem.mp hc
            rw [List.mem_filter, List.mem_eraseDups] at hm
            exact absurd hm.2 (by rw [h]; exact Bool.false_ne_true)
        | false => exact hc
  have hdis : ∀ r ∈ rows, (p r && q r) = false := by
    intro r hr
    rw [hpE r hr]
    simp only [q]
    cases h : whole (bucket r) <;> simp
  have hcov : List.filter (fun x => p x || q x) rows
      = List.filter (fun r => lo ≤ r.ts && r.ts < hi) rows := by
    apply List.filter_congr
    intro r hr
    rw [hpE r hr]
    simp only [q]
    by_cases h1 : whole (bucket r) = true
    · have ⟨ha, hb⟩ := hkey (bucket r) h1
      have hlo : lo ≤ r.ts := Nat.le_trans ha (hlb r)
      have hhi : r.ts < hi := Nat.lt_of_lt_of_le (hut r) hb
      simp [h1, decide_eq_true hlo, decide_eq_true hhi]
    · have hwf : whole (bucket r) = false := by
        cases h : whole (bucket r) with
        | true => exact absurd h h1
        | false => exact rfl
      by_cases hlo : lo ≤ r.ts
      · by_cases hhi : r.ts < hi
        · simp [hwf, decide_eq_true hlo, decide_eq_true hhi]
        · simp [hwf, decide_eq_true hlo, decide_eq_false hhi]
      · by_cases hhi : r.ts < hi
        · simp [hwf, decide_eq_false hlo, decide_eq_true hhi]
        · simp [hwf, decide_eq_false hlo, decide_eq_false hhi]
  have hfold :
      ids.foldl (fun a k => plus a (stats (rows.filter (fun r => bucket r == k)))) zero
        = stats (rows.filter p) := by
    have h := foldl_bucket_partition bucket Tick.bid ids hnd zero rows
    rw [plus_zero'] at h
    exact h
  have hstep : plus (stats (rows.filter p)) (stats (rows.filter q))
      = stats (rows.filter (fun r => lo ≤ r.ts && r.ts < hi)) := by
    have h2 : plus (stats (rows.filter p)) (stats (rows.filter q))
        = stats (rows.filter (fun x => p x || q x)) := by
      show plus (summarize ((rows.filter p).map Tick.bid))
          (summarize ((rows.filter q).map Tick.bid)) = _
      rw [← summarize_append, ← List.map_append]
      exact summarize_perm
        ((filter_disjoint_perm p q rows (fun x hx => hdis x hx)).map Tick.bid)
    rw [h2, hcov]
  rw [hfold]
  exact hstep

/-! ### MaintenanceBreakEven -/

theorem sound_MaintenanceBreakEven : MaintenanceBreakEven.Goal := by
  intro fresh query time m s hle
  obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hle
  rw [Nat.add_sub_cancel_left, Nat.add_mul, Nat.mul_add,
    Nat.add_comm ((fresh + query) * s) ((fresh + query) * d), Nat.add_lt_add_iff_left]

/-! ### SpreadMoments -/

/-- One row's paired quote is present iff both legs are, and the difference is
extracted exactly then. -/
private theorem paired_filterMap (rows : List Tick) :
    (rows.map (fun r => do return (← r.ask) - (← r.bid))).filterMap id =
      (rows.filterMap (fun r => do return (← r.bid, ← r.ask))).map (fun p => p.2 - p.1) := by
  induction rows with
  | nil => rfl
  | cons r t ih =>
      rw [List.map_cons, List.filterMap_cons, ih, List.filterMap_cons]
      cases hb : r.bid with
      | none => simp
      | some b =>
          cases ha : r.ask with
          | none => simp
          | some a => rfl

private theorem sum_getD_eq_filterMap (ws : List (Option Int)) :
    (ws.map (fun v => v.getD 0)).sum = (ws.filterMap id).sum := by
  induction ws with
  | nil => rfl
  | cons v t ih =>
      cases v with
      | none => simp [ih]
      | some x => simp [ih]

private theorem sum_sub_distrib (xs : List (Int × Int)) :
    (xs.map (fun p => p.2 - p.1)).sum = (xs.map Prod.snd).sum - (xs.map Prod.fst).sum := by
  induction xs with
  | nil => rfl
  | cons p t ih =>
      rw [List.map_cons, List.map_cons, List.map_cons, List.sum_cons, List.sum_cons,
        List.sum_cons, ih]
      omega

theorem sound_SpreadMoments : SpreadMoments.Goal := by
  intro rows
  dsimp only
  refine ⟨?_, ?_, ?_⟩
  · rw [summarize_cnt, List.length_map]
  · rw [summarize_nn, paired_filterMap rows, List.length_map]
  · rw [summarize_sum, sum_getD_eq_filterMap (rows.map (fun r => do return (← r.ask) - (← r.bid)))]
    rw [paired_filterMap rows, sum_sub_distrib
      (rows.filterMap (fun r => do return (← r.bid, ← r.ask)))]

/-! ### FenceRead -/

private theorem foldl_min_le : ∀ (as : List Nat) (acc : Nat),
    (as.foldl min acc) ≤ acc ∧ ∀ b ∈ as, (as.foldl min acc) ≤ b := by
  intro as
  induction as with
  | nil => intro acc; exact ⟨Nat.le_refl _, fun _ h => by cases h⟩
  | cons x xs ih =>
      intro acc
      have h := ih (min acc x)
      refine ⟨Nat.le_trans h.1 (by omega), fun b hb => ?_⟩
      cases List.mem_cons.mp hb with
      | inl e => subst e; exact Nat.le_trans h.1 (by omega)
      | inr m => exact h.2 b m

private theorem foldl_max_ge : ∀ (as : List Nat) (acc : Nat),
    acc ≤ (as.foldl max acc) ∧ ∀ b ∈ as, b ≤ (as.foldl max acc) := by
  intro as
  induction as with
  | nil => intro acc; exact ⟨Nat.le_refl _, fun _ h => by cases h⟩
  | cons x xs ih =>
      intro acc
      have h := ih (max acc x)
      refine ⟨Nat.le_trans (by omega) h.1, fun b hb => ?_⟩
      cases List.mem_cons.mp hb with
      | inl e => subst e; exact Nat.le_trans (by omega) h.1
      | inr m => exact h.2 b m

private theorem min?_le_of_mem {l : List Nat} :
    ∀ (a : Nat), a ∈ l → (l.min?.any (· ≤ a)) = true := by
  intro a
  induction l with
  | nil => intro h; cases h
  | cons x xs ih =>
      intro h
      have key : xs.foldl min x ≤ a := by
        cases List.mem_cons.mp h with
        | inl e => rw [← e]; exact (foldl_min_le xs a).1
        | inr m => exact (foldl_min_le xs x).2 a m
      show decide (xs.foldl min x ≤ a) = true
      exact decide_eq_true key

private theorem max?_ge_of_mem {l : List Nat} :
    ∀ (a : Nat), a ∈ l → (l.max?.any (a ≤ ·)) = true := by
  intro a
  induction l with
  | nil => intro h; cases h
  | cons x xs ih =>
      intro h
      have key : a ≤ xs.foldl max x := by
        cases List.mem_cons.mp h with
        | inl e => rw [← e]; exact (foldl_max_ge xs a).1
        | inr m => exact (foldl_max_ge xs x).2 a m
      show decide (a ≤ xs.foldl max x) = true
      exact decide_eq_true key

theorem sound_FenceRead : FenceRead.Goal := by
  intro runs sym lo hi
  dsimp only
  let F := fun (rs : List Tick) =>
    (rs.map Tick.sym).min?.any (fun s => s ≤ sym) && (rs.map Tick.sym).max?.any (fun s => sym ≤ s)
  induction runs with
  | nil => exact ⟨rfl, Nat.le_refl _⟩
  | cons rs rest ih =>
      cases hg : F rs with
      | true =>
          constructor
          · rw [List.filter_cons_of_pos hg, List.flatten_cons, List.filter_append,
              List.flatten_cons, List.filter_append, ih.1]
          · rw [List.filter_cons_of_pos hg, List.length_cons, List.length_cons]
            exact Nat.succ_le_succ ih.2
      | false =>
          have hne : ¬(F rs = true) := fun hh => by
            rw [hh] at hg
            exact absurd hg (by decide)
          have hnil : rs.filter (eligible sym lo hi) = [] := by
            apply filter_eq_nil_of_all_false
            intro r hr
            cases hz : eligible sym lo hi r with
            | false => rfl
            | true =>
                exfalso
                have hs : r.sym = sym := by
                  cases hb : r.sym == sym with
                  | true => exact beq_iff_eq.mp hb
                  | false => simp [hb, eligible] at hz
                have hmin : (rs.map Tick.sym).min?.any (fun s => s ≤ sym) = true :=
                  min?_le_of_mem sym (List.mem_map.mpr ⟨r, hr, hs⟩)
                have hmax : (rs.map Tick.sym).max?.any (fun s => sym ≤ s) = true :=
                  max?_ge_of_mem sym (List.mem_map.mpr ⟨r, hr, hs⟩)
                have hf : F rs = true := by
                  change Option.any (fun s => decide (s ≤ sym)) (List.map Tick.sym rs).min? &&
                    Option.any (fun s => decide (sym ≤ s)) (List.map Tick.sym rs).max? = true
                  simp [hmin, hmax]
                exact hne hf
          rw [List.filter_cons_of_neg hne]
          constructor
          · rw [List.flatten_cons, List.filter_append, hnil, List.nil_append, ih.1]
          · exact Nat.le_trans (List.length_filter_le F rest) (Nat.le_succ _)

/-! ### ReclusterSwap -/

private theorem swap_read (W : Tick → Bool) (old fresh rest : List (List Tick))
    (h : old.flatten.Perm fresh.flatten) :
    stats ((fresh ++ rest).flatten.filter W) = stats ((old ++ rest).flatten.filter W) := by
  rw [List.flatten_append, List.flatten_append, List.filter_append, List.filter_append,
    stats_append, stats_append]
  rw [stats_perm (h.filter W)]

theorem sound_ReclusterSwap : ReclusterSwap.Goal := by
  intro old fresh rest cut pin sym lo hi hperm
  dsimp only
  by_cases hpc : pin < cut
  · rw [ite_eq_left hpc]
  · rw [ite_eq_right (by omega)]
    exact swap_read (fun r => r.batch ≤ pin && eligible sym lo hi r) old fresh rest hperm


/-! ### RetryRefresh -/

private theorem hstats_flat (payload : Nat → List Tick) :
    ∀ (us : List Nat) (z : Totals),
      us.foldl (fun a k => plus a (stats (payload k))) z
        = plus z (stats (us.flatMap payload)) := by
  intro us
  induction us with
  | nil => intro z; exact (plus_zero z).symm
  | cons k us ih =>
      intro z
      show us.foldl (fun a x => plus a (stats (payload x))) (plus z (stats (payload k)))
          = plus z (stats (payload k ++ us.flatMap payload))
      rw [ih, stats_append, ← plus_assoc]

private theorem retry_fold (payload : Nat → List Tick) : ∀ (ds : List Nat),
    ∀ (u : List Nat) (z : Totals),
      ∃ v : List Nat,
        ds.foldl (fun (seen,acc) k =>
          if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k)))) (u, z) =
          (v.reverse ++ u, v.foldl (fun a k => plus a (stats (payload k))) z) ∧
          v.Nodup ∧ ∀ k, k ∈ v ↔ k ∈ ds ∧ ¬ k ∈ u := by
  intro ds
  induction ds with
  | nil =>
      intro u z
      exact ⟨[], ⟨rfl, ⟨List.nodup_nil, fun k => by simp⟩⟩⟩
  | cons d ds ih =>
      intro u z
      cases hc : u.contains d with
      | true =>
          obtain ⟨v, hev, hvn, hvm⟩ := ih u z
          have hd : d ∈ u := List.contains_iff_mem.mp hc
          have hstep1 : (fun (seen,acc) k =>
                if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                (u, z) d = (u, z) := by
            show (if u.contains d = true then (u, z)
                  else (d :: u, plus z (stats (payload d)))) = (u, z)
            rw [hc]
            rfl
          refine ⟨v, ⟨?_, ⟨hvn, fun k => ?_⟩⟩⟩
          · show ds.foldl (fun (seen,acc) k =>
                if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                ((fun (seen,acc) k =>
                  if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                  (u, z) d) = (v.reverse ++ u, v.foldl (fun a k => plus a (stats (payload k))) z)
            rw [hstep1]
            exact hev
          · constructor
            · intro hkv
              obtain ⟨hds, hnu⟩ := (hvm k).mp hkv
              exact ⟨List.mem_cons.mpr (Or.inr hds), hnu⟩
            · intro ⟨hdd, hnu⟩
              cases List.mem_cons.mp hdd with
              | inl e => rw [e] at hnu ⊢; exact absurd hd hnu
              | inr hds => exact (hvm k).mpr ⟨hds, hnu⟩
      | false =>
          obtain ⟨v, hev, hvn, hvm⟩ := ih (d :: u) (plus z (stats (payload d)))
          have hdnot : ¬d ∈ u := fun hh =>
            absurd (hc.symm.trans (List.contains_iff_mem.mpr hh)) (by decide)
          have hstep0 : (fun (seen,acc) k =>
                if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                (u, z) d = (d :: u, plus z (stats (payload d))) := by
            show (if u.contains d = true then (u, z)
                  else (d :: u, plus z (stats (payload d)))) = (d :: u, plus z (stats (payload d)))
            rw [hc]
            rfl
          refine ⟨d :: v, ⟨?_, ⟨?_, fun k => ?_⟩⟩⟩
          · show ds.foldl (fun (seen,acc) k =>
                if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                ((fun (seen,acc) k =>
                  if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
                  (u, z) d) =
                ((d :: v).reverse ++ u,
                  (d :: v).foldl (fun a k => plus a (stats (payload k))) z)
            rw [hstep0, hev]
            refine Prod.ext ?_ ?_
            · simp [List.reverse_cons, List.append_assoc]
            · rfl
          · refine List.nodup_cons.mpr ⟨fun h => ?_, hvn⟩
            have hdn := ((hvm d).mp h).2
            exact hdn List.mem_cons_self
          · constructor
            · intro hk
              cases List.mem_cons.mp hk with
              | inl e =>
                  exact ⟨List.mem_cons.mpr (Or.inl e), fun hu => hdnot (by rw [← e]; exact hu)⟩
              | inr hkv =>
                  obtain ⟨hds, hdnu⟩ := (hvm k).mp hkv
                  exact ⟨List.mem_cons.mpr (Or.inr hds),
                    fun hku => hdnu (List.mem_cons.mpr (Or.inr hku))⟩
            · intro ⟨hdd, hnu⟩
              cases List.mem_cons.mp hdd with
              | inl e => exact List.mem_cons.mpr (Or.inl e)
              | inr hds =>
                  by_cases hkd : k = d
                  · exact List.mem_cons.mpr (Or.inl hkd)
                  · exact List.mem_cons.mpr (Or.inr
                      ((hvm k).mpr ⟨hds, fun hdu =>
                        Or.elim (List.mem_cons.mp hdu)
                          (fun e2 => absurd e2 hkd)
                          (fun hku => hnu hku)⟩))

theorem sound_RetryRefresh : RetryRefresh.Goal := by
  intro deliveries payload
  dsimp only
  obtain ⟨v, hev, hvn, hvm⟩ := retry_fold payload deliveries [] zero
  have hp : List.Perm v (deliveries.eraseDups) :=
    perm_of_nodup_mem hvn (deliveries.eraseDups) (nodup_eraseDups _) (fun a => by
      rw [hvm a, List.mem_eraseDups]
      simp)
  rw [hev]
  constructor
  · rw [hstats_flat payload v, plus_zero', stats_perm (hp.flatMap_right payload)]
  · rw [List.append_nil, List.length_reverse]
    exact hp.length_eq

/-! ### ReclusterBudget -/

private theorem pw_decide {α : Type} (r : α → α → Prop) [DecidableRel r] {l : List α}
    (h : l.Pairwise (fun a b => decide (r a b) = true)) : l.Pairwise r := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a as ih =>
      obtain ⟨h1, h2⟩ := List.pairwise_cons.mp h
      exact List.Pairwise.cons (fun b hb => of_decide_eq_true (h1 b hb)) (ih h2)

private theorem budget_aux (f : Nat → Nat) : ∀ (s : List Nat),
    s.Pairwise (fun i j => f i ≥ f j) →
    ∀ (other : List Nat) (k : Nat), other.Nodup → (∀ i ∈ other, i ∈ s) →
      other.length ≤ k → (other.map f).sum ≤ ((s.take k).map f).sum := by
  intro s
  induction s with
  | nil =>
      intro _ other k _ hsub _
      have ho : other = [] := by
        cases other with
        | nil => rfl
        | cons a t => exact absurd (hsub a List.mem_cons_self) (fun h => by cases h)
      rw [ho]
      exact Nat.zero_le _
  | cons x xs ih =>
      intro hpair other k hnod hsub hl
      obtain ⟨hgt, hpair'⟩ := List.pairwise_cons.mp hpair
      cases k with
      | zero =>
          have ho : other = [] := by
            cases other with
            | nil => rfl
            | cons a t => exact absurd hl (by simp)
          rw [ho]
          exact Nat.zero_le _
      | succ m =>
          by_cases hx : x ∈ other
          · obtain ⟨p, q, hpq⟩ := exists_decomp_of_mem other x hx
            have hperm : other.Perm (x :: p ++ q) := by rw [hpq]; exact perm_move_front p q x
            have hmid := nodup_mid_not_mem (hpq ▸ hnod)
            have hnod' : (x :: p ++ q).Nodup := hperm.nodup_iff.mp hnod
            have hlen : (p ++ q).length ≤ m := by
              have h1 : other.length = (p ++ x :: q).length := congrArg List.length hpq
              simp only [List.length_append, List.length_cons] at h1 ⊢
              omega
            have h2 : ∀ y ∈ p ++ q, y ∈ xs := by
              intro y hy
              have hne : y ≠ x := fun e => hmid (by rw [← e]; exact hy)
              cases List.mem_append.mp hy with
              | inl hp' =>
                  have hyo : y ∈ other := by
                    rw [hpq]; exact List.mem_append.mpr (Or.inl hp')
                  cases List.mem_cons.mp (hsub y hyo) with
                  | inl e => exact absurd e hne
                  | inr h => exact h
              | inr hq' =>
                  have hyo : y ∈ other := by
                    rw [hpq]
                    exact List.mem_append.mpr (Or.inr (List.mem_cons.mpr (Or.inr hq')))
                  cases List.mem_cons.mp (hsub y hyo) with
                  | inl e => exact absurd e hne
                  | inr h => exact h
            rw [map_sum_perm f hperm]
            exact Nat.add_le_add_left
              (ih hpair' (p ++ q) m (List.nodup_cons.mp hnod').2 h2 hlen) (f x)
          · cases other with
            | nil => exact Nat.zero_le _
            | cons a tl =>
                have ha : a ∈ xs := by
                  cases List.mem_cons.mp (hsub a List.mem_cons_self) with
                  | inl e => exact absurd (by rw [← e]; exact List.mem_cons_self) hx
                  | inr h => exact h
                by_cases hok : (a :: tl).length ≤ m
                · refine Nat.le_trans (ih hpair' (a :: tl) m hnod ?_ hok) (Nat.le_add_left _ _)
                  intro y hy
                  cases List.mem_cons.mp (hsub y hy) with
                  | inl e => exact absurd (by rw [← e]; exact hy) hx
                  | inr h => exact h
                · have h1 : (a :: tl).length = tl.length + 1 := by simp only [List.length_cons]
                  have hlt' : tl.length ≤ m := by omega
                  refine Nat.add_le_add (hgt a ha) (ih hpair' tl m ?_ ?_ hlt')
                  · exact (List.nodup_cons.mp hnod).2
                  · intro y hy
                    cases List.mem_cons.mp (hsub y (List.mem_cons.mpr (Or.inr hy))) with
                    | inl e =>
                        exact absurd (by rw [← e]; exact List.mem_cons.mpr (Or.inr hy)) hx
                    | inr h => exact h

theorem sound_ReclusterBudget : ReclusterBudget.Goal := by
  intro runs queries k replacement other _
  dsimp only
  intro hnod hlt hlen
  let S := fun i => readWork (runs[i]?.getD []) queries -
    ((replacement i).map (fun r => readWork r queries)).sum
  have htotal : ∀ a b : Nat,
      ((fun i j => decide (S i ≥ S j)) a b || (fun i j => decide (S i ≥ S j)) b a) = true := by
    intro a b
    by_cases h : S b ≤ S a
    · have e : (fun i j => decide (S i ≥ S j)) a b = true := decide_eq_true h
      rw [e]
      simp
    · have e : (fun i j => decide (S i ≥ S j)) b a = true :=
        decide_eq_true (by omega : S a ≤ S b)
      rw [e]
      simp
  have htrans : ∀ a b c : Nat, (fun i j => decide (S i ≥ S j)) a b = true →
      (fun i j => decide (S i ≥ S j)) b c = true →
      (fun i j => decide (S i ≥ S j)) a c = true := by
    intro a b c hab hbc
    show decide (S a ≥ S c) = true
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hbc) (of_decide_eq_true hab))
  refine budget_aux S ((List.range runs.length).mergeSort (fun i j => decide (S i ≥ S j)))
    (pw_decide (fun i j => S i ≥ S j)
      (mergeSort_pairwise (fun i j => decide (S i ≥ S j)) htrans htotal
        (List.range runs.length)))
    other k hnod ?_ hlen
  · intro i hi
    exact List.mem_mergeSort.mpr (List.mem_range.mpr (hlt i hi))

end TinylambOpt.ResearchProofs.CloudBI.Quotes
