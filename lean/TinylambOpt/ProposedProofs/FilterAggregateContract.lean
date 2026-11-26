import TinylambOpt.ProposedGoals.FilterAggregateContract

/-!
Proved

- `Goal` — Staged evaluation preserves value, NULL and first error; the FILTER
  aggregate is chunk-mergeable under the row-order contract.

The reference model is defined in `TinylambOpt.ProposedGoals.FilterAggregateContract`.
-/

namespace TinylambOpt.ProposedProofs.FilterAggregateContract

open TinylambOpt.ProposedGoals.FilterAggregateContract

private def errPred (p : Pred) (v : Option Int) : Bool := (p v).isNone

private theorem addVal_none (s : Option Int) : addVal none s = s := by
  cases s <;> simp [addVal]

private theorem addVal_comm_apply (a x y : Option Int) :
    addVal (addVal a x) y = addVal (addVal a y) x := by
  cases a <;> cases x <;> cases y <;> simp [addVal] <;> ac_rfl

private theorem addVal_assoc_comm (a b c : Option Int) :
    addVal a (addVal b c) = addVal (addVal a c) b := by
  cases a <;> cases b <;> cases c <;> simp [addVal] <;> ac_rfl

private theorem foldl_addVal_init (s : Option Int) (B : Rows) :
    B.foldl addVal s = addVal (B.foldl addVal none) s := by
  induction B generalizing s with
  | nil => simp [addVal_none]
  | cons v vs ih =>
      simp only [List.foldl_cons]
      rw [ih (addVal s v), ih (addVal none v)]
      rw [addVal_none]
      exact addVal_assoc_comm _ _ _

private def scanAcc (p : Pred) (rows : Rows) (i : Nat) (s : Option Int) : Result :=
  match rows with
  | [] => .ok s
  | v :: vs =>
    match p v with
    | none => .failed i
    | some TV.t => scanAcc p vs (i + 1) (addVal s v)
    | _ => scanAcc p vs (i + 1) s

private theorem scanAcc_tack (p : Pred) (rows : Rows) (i : Nat) (s : Option Int) (v : Option Int) :
    (match scanAcc p rows i s with | .ok t => .ok (addVal t v) | e => e) =
      scanAcc p rows i (addVal s v) := by
  induction rows generalizing i s with
  | nil => rfl
  | cons w ws ih =>
      cases hpw : p w
      · simp [scanAcc, hpw]
      · case some tv =>
          cases tv
          · simp only [scanAcc, hpw]
            rw [addVal_comm_apply s v w]
            exact ih (i + 1) (addVal s w)
          · simp only [scanAcc, hpw]
            exact ih (i + 1) s
          · simp only [scanAcc, hpw]
            exact ih (i + 1) s

private theorem scan_eq_scanAcc (p : Pred) (rows : Rows) (i : Nat) :
    scan p rows i = scanAcc p rows i none := by
  induction rows generalizing i with
  | nil => rfl
  | cons v vs ih =>
      cases hpv : p v
      · simp [scan, scanAcc, hpv]
      · case some tv =>
          cases tv
          · simp only [scan, scanAcc, hpv, ih]
            exact scanAcc_tack p vs (i + 1) none v
          · simp [scan, scanAcc, hpv, ih]
          · simp [scan, scanAcc, hpv, ih]

private theorem scanAcc_spec (p : Pred) (rows : Rows) (i : Nat) (s : Option Int) :
    (match rows.findIdx? (errPred p) with
     | some j => scanAcc p rows i s = .failed (i + j)
     | none => scanAcc p rows i s =
         .ok ((rows.filter (fun v => p v == some TV.t)).foldl addVal s)) := by
  induction rows generalizing i s with
  | nil => simp [scanAcc]
  | cons v vs ih =>
      cases hpv : p v
      · simp [scanAcc, hpv, List.findIdx?_cons, errPred]
      · case some tv =>
          cases tv
          · cases hfind : vs.findIdx? (errPred p) with
            | none =>
                have ih' := ih (i + 1) (addVal s v)
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind] using ih'
            | some j =>
                have ih' := ih (i + 1) (addVal s v)
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih'
          · cases hfind : vs.findIdx? (errPred p) with
            | none =>
                have ih' := ih (i + 1) s
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind] using ih'
            | some j =>
                have ih' := ih (i + 1) s
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih'
          · cases hfind : vs.findIdx? (errPred p) with
            | none =>
                have ih' := ih (i + 1) s
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind] using ih'
            | some j =>
                have ih' := ih (i + 1) s
                rw [hfind] at ih'
                simpa [scanAcc, hpv, List.findIdx?_cons, errPred, hfind, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih'

private theorem scan_closed (p : Pred) (rows : Rows) (i : Nat) :
    scan p rows i =
      match rows.findIdx? (errPred p) with
      | some j => .failed (i + j)
      | none => .ok ((rows.filter (fun v => p v == some TV.t)).foldl addVal none) := by
  rw [scan_eq_scanAcc]
  cases h : rows.findIdx? (errPred p) with
  | none => simpa [h] using (scanAcc_spec p rows i none)
  | some j => simpa [h] using (scanAcc_spec p rows i none)

private theorem staged_closed (p : Pred) (rows : Rows) :
    staged p rows =
      match rows.findIdx? (errPred p) with
      | some j => .failed j
      | none => .ok ((rows.filter (fun v => p v == some TV.t)).foldl addVal none) := by
  unfold staged
  dsimp only
  have hfind : (rows.map (fun v => (v, p v))).findIdx? (fun x => x.2 == none) =
      rows.findIdx? (errPred p) := by
    rw [List.findIdx?_map]
    congr 1
    funext v
    dsimp [Function.comp, errPred]
    cases p v <;> rfl
  have hfilter : ((rows.map (fun v => (v, p v))).filter (fun x => x.2 == some TV.t)).map Prod.fst =
      rows.filter (fun v => p v == some TV.t) := by
    rw [List.filter_map]
    have hc : (fun x : Option Int × Option TV => x.2 == some TV.t) ∘ (fun v => (v, p v)) =
        fun v => p v == some TV.t := by rfl
    rw [hc]
    rw [List.map_map]
    have hf : Prod.fst ∘ (fun v : Option Int => (v, p v)) = id := by funext v; rfl
    rw [hf, List.map_id]
  rw [hfind, hfilter]
  rfl

private theorem merged_scan (p : Pred) (pre suf : Rows) :
    merged p pre suf = scan p (pre ++ suf) 0 := by
  unfold merged
  rw [scan_closed p pre 0, scan_closed p suf 0, scan_closed p (pre ++ suf) 0]
  rw [List.findIdx?_append]
  cases hpre : pre.findIdx? (errPred p) with
  | none =>
      cases hsuf : suf.findIdx? (errPred p) with
      | none =>
          have hfold := foldl_addVal_init
            (List.foldl addVal none (pre.filter (fun v => p v == some TV.t)))
            (suf.filter (fun v => p v == some TV.t))
          simpa [hpre, hsuf, List.filter_append, List.foldl_append] using hfold.symm
      | some j =>
          simp
  | some i =>
      simp

theorem sound : Goal := by
  intro p rows
  constructor
  · rw [staged_closed, scan_closed]
    cases rows.findIdx? (errPred p) <;> simp
  · intro pre suf hps
    rw [← hps]
    exact merged_scan p pre suf

end TinylambOpt.ProposedProofs.FilterAggregateContract
