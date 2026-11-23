import TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown

namespace TinylambOpt.ResearchProofs.InnerJoinUnreferencedSideWeightPushdown

open TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown

/-- One more unit-weight contribution is the same as bumping the weight by one.
    `COUNT(*)/COUNT(v)/SUM(v)` are additive (hence the weighted sum), while
    `MIN(v)/MAX(v)` are idempotent (hence the extra copy changes nothing). -/
private theorem addValWeighted_succ (a : Agg5) (w : Nat) (ov : Option Int) :
    addValWeighted (addValWeighted a w ov) 1 ov = addValWeighted a (Nat.succ w) ov := by
  cases w with
  | zero =>
      simp [addValWeighted]
  | succ w =>
      cases ov with
      | none =>
          simp [addValWeighted]
          omega
      | some x =>
          cases a with
          | mk cntStar cntCol sumOpt minOpt maxOpt =>
              cases sumOpt <;> cases minOpt <;> cases maxOpt <;>
                simp [addValWeighted, Int.add_comm, Int.add_mul] <;>
                omega <;>
                simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]

/-- Two successive contributions of the same value with weights `w1` and `w2`
    equal a single contribution with weight `w1 + w2`. -/
private theorem addValWeighted_add_weight (a : Agg5) (w1 w2 : Nat) (ov : Option Int) :
    addValWeighted (addValWeighted a w1 ov) w2 ov = addValWeighted a (w1 + w2) ov := by
  induction w2 generalizing a with
  | zero => simp [addValWeighted]
  | succ w2 ih =>
      rw [Nat.add_succ]
      change addValWeighted (addValWeighted a w1 ov) (Nat.succ w2) ov =
        addValWeighted a (Nat.succ (w1 + w2)) ov
      calc
        addValWeighted (addValWeighted a w1 ov) (Nat.succ w2) ov
            = addValWeighted (addValWeighted (addValWeighted a w1 ov) w2 ov) 1 ov :=
                (addValWeighted_succ (addValWeighted a w1 ov) w2 ov).symm
        _ = addValWeighted (addValWeighted a (w1 + w2) ov) 1 ov := by rw [ih]
        _ = addValWeighted a (Nat.succ (w1 + w2)) ov :=
                addValWeighted_succ a (w1 + w2) ov

/-- Folding `w` unit-weight copies is one `w`-weighted step. -/
private theorem fold_replicate_addValWeighted (a : Agg5) (w : Nat) (ov : Option Int) :
    (List.replicate w ov).foldl (fun acc ov => addValWeighted acc 1 ov) a =
      addValWeighted a w ov := by
  induction w generalizing a with
  | zero =>
      simp [addValWeighted]
  | succ w ih =>
      simp only [List.replicate_succ, List.foldl_cons]
      rw [ih]
      simpa [Nat.add_comm] using addValWeighted_add_weight a 1 w ov

/-- The right-hand rows matching a given left key are exactly `rightWeight`
    many, so the join's `filterMap` emits that many copies of `l.v` (and none
    at all for a NULL left key or a zero weight). -/
private theorem filterMap_join_one (l : LRow) (rsFilt : List RRow) :
    rsFilt.filterMap (fun r =>
      match l.k, r.k with
      | some lk, some rk => if lk = rk then some l.v else none
      | _, _ => none) =
    match l.k with
    | none => []
    | some lk => List.replicate (rightWeight rsFilt lk) l.v := by
  cases l with
  | mk g k v =>
      cases k with
      | none =>
          induction rsFilt with
          | nil => rfl
          | cons r rs ih => simp [ih]
      | some lk =>
          induction rsFilt with
          | nil => simp [rightWeight]
          | cons r rs ih =>
              cases r with
              | mk rk pred =>
                  cases rk with
                  | none =>
                      simpa [rightWeight] using ih
                  | some rk =>
                      by_cases h : lk = rk
                      · subst rk
                        simp [rightWeight, ih, List.replicate_succ]
                      · have h' : rk ≠ lk := by
                          intro e
                          exact h e.symm
                        simp [rightWeight, h, h', ih]

/-- Folding the replicated join output equals the weighted single scan. -/
private theorem fold_flatMap_replicate (ls : List LRow) (rsFilt : List RRow) (a : Agg5) :
    (ls.flatMap (fun l =>
      match l.k with
      | none => []
      | some lk => List.replicate (rightWeight rsFilt lk) l.v)).foldl
        (fun acc ov => addValWeighted acc 1 ov) a =
    ls.foldl (fun acc l =>
      match l.k with
      | none => acc
      | some lk => addValWeighted acc (rightWeight rsFilt lk) l.v) a := by
  induction ls generalizing a with
  | nil =>
      rfl
  | cons l ls ih =>
      cases l with
      | mk g k v =>
          cases k with
          | none =>
              simp [ih]
          | some lk =>
              simp only [List.flatMap_cons, List.foldl_cons]
              rw [List.foldl_append]
              rw [fold_replicate_addValWeighted]
              rw [ih]

/-- Flat join plus unit-weight fold is precisely the weighted scan. -/
private theorem ref_eq_weighted (lsFilt : List LRow) (rsFilt : List RRow) :
    (lsFilt.flatMap (fun l =>
      rsFilt.filterMap (fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none))).foldl
        (fun acc ov => addValWeighted acc 1 ov) emptyAgg5 =
    lsFilt.foldl (fun acc l =>
      match l.k with
      | none => acc
      | some lk => addValWeighted acc (rightWeight rsFilt lk) l.v) emptyAgg5 := by
  rw [show lsFilt.flatMap (fun l =>
        rsFilt.filterMap (fun r =>
          match l.k, r.k with
          | some lk, some rk => if lk == rk then some l.v else none
          | _, _ => none)) =
      lsFilt.flatMap (fun l =>
        match l.k with
        | none => []
        | some lk => List.replicate (rightWeight rsFilt lk) l.v) by
    induction lsFilt with
    | nil => rfl
    | cons l ls ih => simp [filterMap_join_one]]
  exact fold_flatMap_replicate lsFilt rsFilt emptyAgg5

theorem sound : TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown.Goal := by
  intro ls rs g
  unfold optAgg5 refAgg5
  symm
  exact ref_eq_weighted (ls.filter (fun l => l.g == g)) (rs.filter RRow.pred)

end TinylambOpt.ResearchProofs.InnerJoinUnreferencedSideWeightPushdown
