import TinylambOpt.ResearchGoals.DuckDBGaps.Refresh
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DuckDBGaps.Refresh

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.CloudBI.Quotes
open TinylambOpt.ResearchGoals.DuckDBGaps.Refresh
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-! ### Basic `Totals` / `atKey` facts -/

private theorem stats_append (xs ys : List Tick) :
    stats (xs ++ ys) = plus (stats xs) (stats ys) := by
  unfold stats
  rw [List.map_append, summarize_append]

private theorem stats_nil : stats ([] : List Tick) = zero := by
  rfl

private theorem stats_cnt (xs : List Tick) : (stats xs).cnt = xs.length := by
  unfold stats
  rw [summarize_cnt, List.length_map]

private theorem stats_singleton (r : Tick) : stats [r] = weighted 1 r.bid := by
  unfold stats
  rw [List.map_cons, List.map_nil, summarize_cons, summarize_nil, plus_zero']

private theorem stats_cons (r : Tick) (rs : List Tick) :
    stats (r :: rs) = plus (weighted 1 r.bid) (stats rs) := by
  change stats ([r] ++ rs) = _
  rw [stats_append, stats_singleton]

private theorem atKey_append (xs ys : List Tick) (k : Key) :
    atKey (xs ++ ys) k = plus (atKey xs k) (atKey ys k) := by
  unfold atKey
  rw [List.filter_append, stats_append]

private theorem atKey_eq_zero_of_cnt_zero (xs : List Tick) (k : Key)
    (h : (atKey xs k).cnt == 0) : atKey xs k = zero := by
  unfold atKey at h ⊢
  let ys := List.filter (fun r : Tick => ((r.batch, r.sym) == k)) xs
  have hc : (stats ys).cnt = ys.length := stats_cnt ys
  have hl : ys.length = 0 := by
    have heq : (stats ys).cnt = 0 := (beq_iff_eq (α := Nat)).mp h
    omega
  rw [show List.filter (fun r : Tick => ((r.batch, r.sym) == k)) xs = [] from
    List.length_eq_zero_iff.mp hl]
  exact stats_nil

private theorem atKey_singleton (r : Tick) (k : Key) :
    atKey [r] k = if ((r.batch, r.sym) == k) then weighted 1 r.bid else zero := by
  unfold atKey
  change stats (List.filter (fun x : Tick => ((x.batch, x.sym) == k)) (r :: [])) = _
  by_cases h : ((r.batch, r.sym) == k)
  · rw [List.filter_cons_of_pos (p := fun x : Tick => ((x.batch, x.sym) == k))
        (by simpa using h)]
    simp [h, stats_singleton]
  · rw [List.filter_cons_of_neg (p := fun x : Tick => ((x.batch, x.sym) == k))
        (by simpa using h)]
    simp [h, stats_nil]

private theorem atKey_nil_of_not_mem (xs : List Tick) (k : Key)
    (h : ∀ r ∈ xs, (r.batch, r.sym) ≠ k) : atKey xs k = zero := by
  induction xs with
  | nil => rfl
  | cons r rs ih =>
      have hr : (r.batch, r.sym) ≠ k := h r (by simp)
      have hk : ((r.batch, r.sym) == k) = false := by
        exact Bool.eq_false_of_ne_true (fun hb => hr ((beq_iff_eq (α := Key)).mp hb))
      have h' : ∀ z ∈ rs, (z.batch, z.sym) ≠ k := by
        intro z hz; exact h z (by simp [hz])
      unfold atKey
      rw [List.filter_cons_of_neg (by simpa using hk)]
      simpa [atKey] using ih h'

private theorem stats_singleton_cnt_pos (r : Tick) : (weighted 1 r.bid).cnt ≠ 0 := by
  cases r.bid <;> simp [weighted]

/-! ### HashMap helper facts -/

private theorem getD_of_getElem? (m : Groups) (k : Key) :
    m.getD k zero = match m[k]? with | none => zero | some v => v := by
  rw [Std.HashMap.getD_eq_getD_getElem?]
  cases h : m[k]? <;> rfl

private theorem getD_insert_same (m : Groups) (k : Key) (v : Totals) :
    (m.insert k v).getD k zero = v := by
  rw [Std.HashMap.getD_insert]
  simp

private theorem getElem?_insert_same (m : Groups) (k : Key) (v : Totals) :
    (m.insert k v)[k]? = some v := by
  rw [Std.HashMap.getElem?_insert]
  simp

private theorem push_getD (m : Groups) (xs : List Tick) (k : Key) :
    (xs.foldl push m).getD k zero = plus (m.getD k zero) (atKey xs k) := by
  induction xs generalizing m with
  | nil =>
      rw [List.foldl_nil, atKey, List.filter_nil, stats_nil, plus_zero]
  | cons r rs ih =>
      rw [List.foldl_cons, ih (push m r)]
      unfold push
      by_cases hk : (r.batch, r.sym) == k
      · have heq : (r.batch, r.sym) = k := (beq_iff_eq (α := Key)).mp hk
        subst heq
        rw [getD_insert_same]
        unfold atKey
        simp [stats_cons, plus_assoc]
      · have hget : (m.insert (r.batch, r.sym)
              (plus (m.getD (r.batch, r.sym) zero) (weighted 1 r.bid))).getD k zero =
              m.getD k zero := by
          rw [Std.HashMap.getD_insert]
          simp [hk]
        rw [hget]
        have hat : atKey (r :: rs) k = atKey rs k := by
          unfold atKey
          rw [List.filter_cons_of_neg (p := fun x : Tick => ((x.batch, x.sym) == k))
            (by simpa using hk)]
        rw [hat]

/-! ### InsertSideState -/

private theorem push_spec (m : Groups)
    (hspec : ∀ k, m[k]? = if (m.getD k zero).cnt == 0 then none else some (m.getD k zero))
    (r : Tick) : ∀ k, (push m r)[k]? =
      if ((push m r).getD k zero).cnt == 0 then none else some ((push m r).getD k zero) := by
  intro k
  unfold push
  rw [Std.HashMap.getElem?_insert, Std.HashMap.getD_insert]
  by_cases hk : (r.batch, r.sym) == k
  · have heq : (r.batch, r.sym) = k := (beq_iff_eq (α := Key)).mp hk
    subst heq
    have hpos : (plus (m.getD (r.batch, r.sym) zero) (weighted 1 r.bid)).cnt ≠ 0 := by
      simp only [plus]
      have hw : (weighted 1 r.bid).cnt = 1 := by cases r.bid <;> rfl
      omega
    have hb : ((plus (m.getD (r.batch, r.sym) zero) (weighted 1 r.bid)).cnt == 0) = false :=
      Bool.eq_false_of_ne_true (fun hh => hpos ((beq_iff_eq (α := Nat)).mp hh))
    simp [hb]
  · simpa [hk] using hspec k

private theorem foldl_push_spec (m : Groups)
    (hspec : ∀ k, m[k]? = if (m.getD k zero).cnt == 0 then none else some (m.getD k zero)) :
    ∀ (xs : List Tick) (k : Key), (xs.foldl push m)[k]? =
      if ((xs.foldl push m).getD k zero).cnt == 0 then none
      else some ((xs.foldl push m).getD k zero) := by
  intro xs
  induction xs generalizing m with
  | nil => intro k; simpa using hspec k
  | cons r rs ih =>
      intro k
      have hs' := push_spec m hspec r
      have h := ih (push m r) hs' k
      simpa [List.foldl_cons] using h

private theorem getD_eq_atKey_of_hm (old : List Tick) (m : Groups)
    (hm : ∀ k, m[k]? = (if (atKey old k).cnt == 0 then none else some (atKey old k))) :
    ∀ k, m.getD k zero = atKey old k := by
  intro k
  rw [getD_of_getElem?, hm k]
  by_cases hc : (atKey old k).cnt == 0
  · have hz := atKey_eq_zero_of_cnt_zero old k hc
    simp [hz, zero]
  · simp [hc]

theorem InsertSideState_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.InsertSideState.Goal := by
  intro old fresh m hm
  have hget_old := getD_eq_atKey_of_hm old m hm
  have hspec : ∀ k, m[k]? =
      if (m.getD k zero).cnt == 0 then none else some (m.getD k zero) := by
    intro k
    rw [hm k, ← hget_old k]
  intro k
  let a := atKey (old ++ fresh) k
  have hfinal := foldl_push_spec m hspec fresh k
  have hget : (fresh.foldl push m).getD k zero = a := by
    dsimp [a]
    rw [push_getD, hget_old k, atKey_append]
  rw [hfinal, hget]

/-! ### ChangedSymbols -/

private theorem replace_plus (U B A : Totals) :
    replaceState (plus U B) B A =
      (if (plus U A).cnt == 0 then none else some (plus U A)) := by
  unfold replaceState
  have hPA : plus U A = ⟨U.cnt + A.cnt, U.nn + A.nn, U.sum + A.sum⟩ := rfl
  rw [hPA]
  simp only [plus_cnt, plus_nn, plus_sum]
  by_cases h : U.cnt + A.cnt == 0
  · have heq : U.cnt + A.cnt = 0 := beq_iff_eq.mp h
    have hn : U.cnt + B.cnt - B.cnt + A.cnt = 0 := by omega
    simp [h]
  · have hn : U.cnt + B.cnt - B.cnt + A.cnt = U.cnt + A.cnt := by omega
    have hn0 : (U.cnt + B.cnt - B.cnt + A.cnt == 0) = false := by
      rw [hn]
      exact Bool.eq_false_of_ne_true h
    simp [h]

theorem ChangedSymbols_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.ChangedSymbols.Goal := by
  intro before after unaffected k
  dsimp only
  rw [atKey_append, atKey_append]
  exact replace_plus (atKey unaffected k) (atKey before k) (atKey after k)

/-! ### OverlapReplay -/

private theorem overlap_tail (source : List Tick) (o a s : Nat) (hoa : o ≤ a) (has : a ≤ o + s) :
    ((source.drop o).take s).drop (a - o) = (source.drop a).take (o + s - a) := by
  rw [List.drop_take, List.drop_drop]
  have h1 : o + (a - o) = a := Nat.add_sub_of_le hoa
  rw [h1]
  have h2 : s - (a - o) = o + s - a := by omega
  rw [h2]

private theorem overlap_split (source : List Tick) (a t : Nat) (hat : a ≤ t) :
    source.take t = source.take a ++ (source.drop a).take (t - a) := by
  calc
    source.take t = List.take a (source.take t) ++ List.drop a (source.take t) :=
      (List.take_append_drop a (source.take t)).symm
    _ = source.take a ++ List.drop a (source.take t) := by
      rw [List.take_take, Nat.min_eq_left hat]
    _ = source.take a ++ (source.drop a).take (t - a) := by rw [List.drop_take]

theorem OverlapReplay_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.OverlapReplay.Goal := by
  intro source offset acknowledged size k hoa has
  rw [overlap_tail source offset acknowledged size hoa has]
  rw [overlap_split source acknowledged (offset + size) has]
  rw [atKey_append]

/-! ### LaneOrSpill -/

private theorem atKey_filter_batch (xs : List Tick) (batch sym : Nat) :
    atKey (xs.filter (fun r => r.batch == batch)) (batch, sym) = atKey xs (batch, sym) := by
  unfold atKey
  rw [List.filter_filter]
  apply congrArg stats
  apply List.filter_congr
  intro r _
  by_cases hkey : ((r.batch, r.sym) == (batch, sym)) = true
  · have heq : (r.batch, r.sym) = (batch, sym) := (beq_iff_eq (α := Key)).mp hkey
    have hb : r.batch == batch := by
      exact (beq_iff_eq (α := Nat)).mpr (congrArg Prod.fst heq)
    simp [hkey, hb]
  · have hkf : ((r.batch, r.sym) == (batch, sym)) = false :=
      Bool.eq_false_of_ne_true hkey
    simp [hkey]

private theorem atKey_singleton_zero (r : Tick) (k : Key)
    (h : (r.batch, r.sym) ≠ k) : atKey [r] k = zero := by
  rw [atKey_singleton]
  have hk : ((r.batch, r.sym) == k) = false :=
    Bool.eq_false_of_ne_true (fun hb => h ((beq_iff_eq (α := Key)).mp hb))
  simp [hk]

private theorem atKey_singleton_match (base batch j : Nat) (r : Tick)
    (hbatch : r.batch == batch) (hsym : base + j = r.sym) :
    atKey [r] (batch, base + j) = weighted 1 r.bid := by
  rw [atKey_singleton]
  have hk : ((r.batch, r.sym) == (batch, base + j)) = true := by
    apply (beq_iff_eq (α := Key)).mpr
    exact Prod.ext ((beq_iff_eq (α := Nat)).mp hbatch) hsym.symm
  simp [hk]

private theorem atKey_singleton_self (batch : Nat) (r : Tick) (hbatch : r.batch == batch) :
    atKey [r] (batch, r.sym) = weighted 1 r.bid := by
  rw [atKey_singleton]
  have hk : ((r.batch, r.sym) == (batch, r.sym)) = true := by
    exact (beq_iff_eq (α := Key)).mpr
      (Prod.ext ((beq_iff_eq (α := Nat)).mp hbatch) rfl)
  simp [hk]

private def outLane (base width k : Nat) : Prop := ¬ (base ≤ k ∧ k < base + width)

private theorem outLane_of_false {base width sym : Nat}
    (h : (base ≤ sym && sym < base + width) = false) : outLane base width sym := by
  intro hb
  have htrue : (base ≤ sym && sym < base + width) = true := by
    simp [decide_eq_true hb.1, decide_eq_true hb.2]
  exact absurd h (by simpa using htrue)

private def laneStep (base width : Nat)
    (st : Array Totals × Std.HashMap Nat Totals) (r : Tick) :
    Array Totals × Std.HashMap Nat Totals :=
  let v := weighted 1 r.bid
  match lane base width r.sym with
  | .inl i => (st.1.modify i (fun t => plus t v), st.2)
  | .inr k => (st.1, st.2.insert k (plus (st.2.getD k zero) v))

private theorem laneStep_inv (base width batch : Nat) (r : Tick) (hr : r.batch == batch)
    (pre : List Tick) (lanes : Array Totals) (spill : Std.HashMap Nat Totals)
    (hL : ∀ j, j < width → lanes[j]? = some (atKey pre (batch, base + j)))
    (hS : ∀ k, outLane base width k → spill.getD k zero = atKey pre (batch, k)) :
    let st := laneStep base width (lanes, spill) r
    (∀ j, j < width → st.1[j]? = some (atKey (pre ++ [r]) (batch, base + j))) ∧
    (∀ k, outLane base width k → st.2.getD k zero = atKey (pre ++ [r]) (batch, k)) := by
  by_cases hcond : (base ≤ r.sym && r.sym < base + width) = true
  · have hblo : base ≤ r.sym := of_decide_eq_true (Bool.and_eq_true_iff.mp hcond).1
    have bhhi : r.sym < base + width := of_decide_eq_true (Bool.and_eq_true_iff.mp hcond).2
    let i := r.sym - base
    have hiw : i < width := by dsimp [i]; omega
    have heq : base + i = r.sym := by dsimp [i]; omega
    have hLane : lane base width r.sym = .inl i := by simp [lane, hcond, i]
    constructor
    · intro j hj
      dsimp [laneStep]
      rw [hLane]
      change (lanes.modify i (fun t => plus t (weighted 1 r.bid)))[j]? =
        some (atKey (pre ++ [r]) (batch, base + j))
      rw [Array.getElem?_modify, hL j hj]
      by_cases hij : i = j
      · rw [ite_eq_left hij]
        simp only [Option.map_some]
        congr 1
        rw [atKey_append pre [r] (batch, base + j)]
        have hs : base + j = r.sym := by omega
        rw [atKey_singleton_match base batch j r hr hs]
      · rw [ite_eq_right hij]
        congr 1
        rw [atKey_append pre [r] (batch, base + j)]
        have hne : (r.batch, r.sym) ≠ (batch, base + j) := by
          intro hp
          have hs : r.sym = base + j := congrArg Prod.snd hp
          have : i = j := by dsimp [i] at *; omega
          exact hij this
        rw [atKey_singleton_zero r (batch, base + j) hne, plus_zero]
    · intro k hk
      dsimp [laneStep]
      rw [hLane]
      change spill.getD k zero = atKey (pre ++ [r]) (batch, k)
      rw [hS k hk]
      rw [atKey_append pre [r] (batch, k)]
      have hne : (r.batch, r.sym) ≠ (batch, k) := by
        intro hp
        have hksym : r.sym = k := congrArg Prod.snd hp
        apply hk
        constructor
        · omega
        · rw [← hksym]
          exact bhhi
      rw [atKey_singleton_zero r (batch, k) hne, plus_zero]
  · have hcondf : (base ≤ r.sym && r.sym < base + width) = false :=
      Bool.eq_false_of_ne_true hcond
    have hout : outLane base width r.sym := outLane_of_false hcondf
    have hLane : lane base width r.sym = .inr r.sym := by simp [lane, hcondf]
    constructor
    · intro j hj
      dsimp [laneStep]
      rw [hLane]
      change lanes[j]? = some (atKey (pre ++ [r]) (batch, base + j))
      rw [hL j hj]
      congr 1
      rw [atKey_append pre [r] (batch, base + j)]
      have hne : (r.batch, r.sym) ≠ (batch, base + j) := by
        intro hp
        have hs : r.sym = base + j := congrArg Prod.snd hp
        apply hout
        constructor
        · omega
        · rw [hs]
          omega
      rw [atKey_singleton_zero r (batch, base + j) hne, plus_zero]
    · intro k hk
      dsimp [laneStep]
      rw [hLane]
      change (spill.insert r.sym (plus (spill.getD r.sym zero) (weighted 1 r.bid))).getD k zero =
        atKey (pre ++ [r]) (batch, k)
      by_cases hkr : r.sym == k
      · have heq : r.sym = k := (beq_iff_eq (α := Nat)).mp hkr
        subst heq
        rw [Std.HashMap.getD_insert]
        simp
        rw [hS r.sym hk]
        rw [atKey_append pre [r] (batch, r.sym)]
        rw [atKey_singleton_self batch r hr]
      · rw [Std.HashMap.getD_insert]
        simp [hkr]
        rw [hS k hk]
        rw [atKey_append pre [r] (batch, k)]
        have hne : (r.batch, r.sym) ≠ (batch, k) := by
          intro hp
          exact hkr ((beq_iff_eq (α := Nat)).mpr (congrArg Prod.snd hp))
        rw [atKey_singleton_zero r (batch, k) hne, plus_zero]

private theorem lane_fold_spec_aux (base width batch : Nat) :
    ∀ (rs pre : List Tick) (lanes : Array Totals) (spill : Std.HashMap Nat Totals),
      (∀ r ∈ rs, r.batch == batch) →
      (∀ j, j < width → lanes[j]? = some (atKey pre (batch, base + j))) →
      (∀ k, outLane base width k → spill.getD k zero = atKey pre (batch, k)) →
      let st := rs.foldl (laneStep base width) (lanes, spill)
      (∀ j, j < width → st.1[j]? = some (atKey (pre ++ rs) (batch, base + j))) ∧
      (∀ k, outLane base width k → st.2.getD k zero = atKey (pre ++ rs) (batch, k)) := by
  intro rs
  induction rs with
  | nil =>
      intro pre lanes spill _ hL hS
      constructor
      · intro j hj
        simpa using hL j hj
      · intro k hk
        simpa using hS k hk
  | cons r rs ih =>
      intro pre lanes spill hmem hL hS
      rw [List.foldl_cons]
      have hr : r.batch == batch := hmem r (by simp)
      have hmem' : ∀ z ∈ rs, z.batch == batch := by
        intro z hz; exact hmem z (by simp [hz])
      have hstep := laneStep_inv base width batch r hr pre lanes spill hL hS
      dsimp only at hstep
      have hL' : ∀ j, j < width →
          (laneStep base width (lanes, spill) r).1[j]? = some (atKey (pre ++ [r]) (batch, base + j)) :=
        hstep.1
      have hS' : ∀ k, outLane base width k →
          (laneStep base width (lanes, spill) r).2.getD k zero = atKey (pre ++ [r]) (batch, k) :=
        hstep.2
      have h := ih (pre ++ [r]) (laneStep base width (lanes, spill) r).1
          (laneStep base width (lanes, spill) r).2 hmem' hL' hS'
      simpa [List.append_assoc] using h

theorem LaneOrSpill_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.LaneOrSpill.Goal := by
  intro xs batch base width sym
  dsimp only
  let target := xs.filter (fun r => r.batch == batch)
  let init : Array Totals × Std.HashMap Nat Totals :=
    (Array.replicate width zero, ({} : Std.HashMap Nat Totals))
  have hmem : ∀ r ∈ target, r.batch == batch := by
    intro r hr
    exact (List.mem_filter.mp hr).2
  have hL0 : ∀ j, j < width → init.1[j]? = some (atKey [] (batch, base + j)) := by
    intro j hj
    dsimp [init]
    rw [Array.getElem?_replicate]
    simp [hj, atKey, stats_nil]
  have hS0 : ∀ k, outLane base width k → init.2.getD k zero = atKey [] (batch, k) := by
    intro k hk
    dsimp [init]
    simp [atKey, stats_nil]
  have hres := lane_fold_spec_aux base width batch target [] init.1 init.2 hmem hL0 hS0
  dsimp only at hres
  change (match lane base width sym with
    | .inl i => ((target.foldl (laneStep base width) init).1[i]?.getD zero)
    | .inr k => ((target.foldl (laneStep base width) init).2.getD k zero)) =
    atKey xs (batch, sym)
  dsimp [init] at hres ⊢
  by_cases hcond : (base ≤ sym && sym < base + width) = true
  · have hblo : base ≤ sym := of_decide_eq_true (Bool.and_eq_true_iff.mp hcond).1
    have bhhi : sym < base + width := of_decide_eq_true (Bool.and_eq_true_iff.mp hcond).2
    let i := sym - base
    have hiw : i < width := by dsimp [i]; omega
    have heq : base + i = sym := by dsimp [i]; omega
    rw [show lane base width sym = .inl i by simp [lane, hcond, i]]
    dsimp
    rw [hres.1 i hiw]
    simp only [Option.getD_some]
    rw [heq, atKey_filter_batch]
  · have hcondf : (base ≤ sym && sym < base + width) = false :=
      Bool.eq_false_of_ne_true hcond
    have hout : outLane base width sym := outLane_of_false hcondf
    rw [show lane base width sym = .inr sym by simp [lane, hcondf]]
    dsimp
    rw [hres.2 sym hout]
    rw [atKey_filter_batch]

/-! ### VisibleFlush -/

private def flushRHS (chunks : List (Nat × List Tick)) (pin sym : Nat) : Totals :=
  stats ((chunks.flatMap (fun (version, rs) => rs.map (fun r => (version, r)))).filterMap
    (fun (version, r) => if version ≤ pin && r.sym == sym then some r else none))

private theorem filterMap_version_rows (rs : List Tick) (version pin sym : Nat)
    (hv : version ≤ pin) :
    (rs.map (fun r => (version, r))).filterMap
      (fun p : Nat × Tick => if p.1 ≤ pin && p.2.sym == sym then some p.2 else none) =
      rs.filter (fun r => r.sym == sym) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [List.map_cons, List.filterMap_cons, ih]
      by_cases hs : r.sym == sym
      · simp [hv, hs]
      · simp [hv, hs]

private theorem filterMap_version_rows_off (rs : List Tick) (version pin sym : Nat)
    (hv : ¬ version ≤ pin) :
    (rs.map (fun r => (version, r))).filterMap
      (fun p : Nat × Tick => if p.1 ≤ pin && p.2.sym == sym then some p.2 else none) = [] := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [List.map_cons, List.filterMap_cons, ih]
      simp [hv]

private theorem flushRHS_cons (version : Nat) (rs : List Tick)
    (rest : List (Nat × List Tick)) (pin sym : Nat) :
    flushRHS ((version, rs) :: rest) pin sym =
      if version ≤ pin then plus (stats (rs.filter (fun r => r.sym == sym)))
        (flushRHS rest pin sym) else flushRHS rest pin sym := by
  unfold flushRHS
  simp only [List.flatMap_cons]
  rw [List.filterMap_append]
  by_cases hv : version ≤ pin
  · rw [filterMap_version_rows rs version pin sym hv]
    simp [hv, stats_append]
  · rw [filterMap_version_rows_off rs version pin sym hv]
    simp [hv]

private theorem visible_main : ∀ (chunks : List (Nat × List Tick)) (pin sym : Nat),
    TSum ((chunks.filter (fun c => c.1 ≤ pin)).map
      (fun c => stats (c.2.filter (fun r => r.sym == sym)))) = flushRHS chunks pin sym := by
  intro chunks
  induction chunks with
  | nil =>
      intro pin sym
      rfl
  | cons c rest ih =>
      intro pin sym
      rcases c with ⟨version, rs⟩
      rw [flushRHS_cons]
      by_cases hv : version ≤ pin
      · rw [ite_eq_left hv]
        simp [hv]
        change plus (stats (rs.filter (fun r => r.sym == sym)))
            (TSum ((rest.filter (fun c => c.1 ≤ pin)).map
              (fun c => stats (c.2.filter (fun r => r.sym == sym))))) =
          plus (stats (rs.filter (fun r => r.sym == sym))) (flushRHS rest pin sym)
        rw [ih pin sym]
      · simp [hv, ih pin sym]

theorem VisibleFlush_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.VisibleFlush.Goal := by
  intro chunks pin sym
  dsimp only
  rw [foldl_plus, plus_zero']
  exact visible_main chunks pin sym

/-! ### DirtyFlush -/

private theorem lookup_map_fst (k : Key) (f : Key → Totals) :
    ∀ (l : List Key), l.Nodup →
      List.lookup k (l.map (fun x => (x, f x))) = if k ∈ l then some (f k) else none := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      intro hnd
      have hx : x ∉ xs := (List.nodup_cons.mp hnd).1
      have hxs : xs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hkx : k == x
      · have heq : k = x := (beq_iff_eq (α := Key)).mp hkx
        subst heq
        simp
      · have hne : k ≠ x := fun h => hkx (by simp [h])
        rw [List.map_cons, List.lookup_cons]
        simp [hkx, hne, ih hxs]

private theorem length_eraseDups_le [BEq α] : ∀ l : List α, l.eraseDups.length ≤ l.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      have hf : (List.filter (fun b : α => ! b == a) as).length ≤ as.length :=
        List.length_filter_le _ _
      have her := length_eraseDups_le (List.filter (fun b : α => ! b == a) as)
      exact Nat.succ_le_succ (Nat.le_trans her hf)
termination_by l => l.length
decreasing_by
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : α => ! b == a) as)
    (Nat.lt_succ_self as.length)

theorem DirtyFlush_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Refresh.DirtyFlush.Goal := by
  intro old xs k
  dsimp only [flush]
  let finished := xs.foldl push old
  let touched := (xs.map (fun r => (r.batch, r.sym))).eraseDups
  let changes := touched.map (fun k => (k, finished.getD k zero))
  have hnodT : touched.Nodup := nodup_eraseDups _
  have hlookup : List.lookup k changes =
      if k ∈ touched then some (finished.getD k zero) else none := by
    dsimp [changes]
    exact lookup_map_fst k (fun k => finished.getD k zero) touched hnodT
  have hfin : finished.getD k zero = plus (old.getD k zero) (atKey xs k) := by
    dsimp [finished]
    exact push_getD old xs k
  have hfst : changes.map Prod.fst = touched := by
    dsimp [changes]
    induction touched with
    | nil => rfl
    | cons a t ih => simp [ih]
  have hlen : changes.length ≤ xs.length := by
    dsimp [changes, touched]
    simpa using length_eraseDups_le (xs.map (fun r => (r.batch, r.sym)))
  constructor
  · by_cases hk : k ∈ touched
    · rw [hlookup]
      simp [hk, hfin]
    · have hnotmap : k ∉ xs.map (fun r => (r.batch, r.sym)) := by
        intro hm
        exact hk ((List.mem_eraseDups).mpr hm)
      have hat : atKey xs k = zero := atKey_nil_of_not_mem xs k (by
        intro r hr heq
        exact hnotmap (List.mem_map.mpr ⟨r, hr, heq⟩))
      rw [hlookup]
      simp [hk, hat, plus_zero]
  · constructor
    · rw [hfst]
      exact hnodT
    · exact hlen

end TinylambOpt.ResearchProofs.DuckDBGaps.Refresh
