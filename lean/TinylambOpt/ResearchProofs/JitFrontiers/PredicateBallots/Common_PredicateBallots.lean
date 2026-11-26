import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Named results include `bitvec_eq_of_lanes`, `ballotAux_getLsbD`, `ballot_getLsbD`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-! ## Bit-vector / ballot infrastructure -/

theorem bitvec_eq_of_lanes (x y : W) (h : ∀ i, i < 64 → x.getLsbD i = y.getLsbD i) : x = y := by
  ext i
  exact h i (by omega)

def ballotStep (f : Nat → Bool) (w : W) (j : Nat) : W :=
  if f j then w ||| ((1 : W) <<< j) else w

def ballotAux (f : Nat → Bool) (n : Nat) : W :=
  (List.range n).foldl (ballotStep f) 0

theorem ballotAux_getLsbD (f : Nat → Bool) (n i : Nat) (hi : i < 64) :
    (ballotAux f n).getLsbD i = (decide (i < n) && f i) := by
  induction n with
  | zero => simp [ballotAux]
  | succ n ih =>
      simp [ballotAux, List.range_succ] at ih ⊢
      by_cases hfi : f n
      · simp [ballotStep, hfi, ih, BitVec.getLsbD_or, BitVec.getLsbD_shiftLeft]
        by_cases hin : i < n
        · have hin' : i < n + 1 := Nat.lt_succ_of_lt hin
          simp [hin, hin']
        · by_cases heq : i = n
          · subst i
            simpa [hfi]
          · have hsub : i - n ≠ 0 := by omega
            have hin' : ¬ i < n + 1 := by omega
            simp [hin, hin', hsub]
      · simp [ballotStep, hfi, ih]
        by_cases heq : i = n
        · subst i
          simp [hfi]
        · have hcases : i < n ∨ n < i := by omega
          cases hcases with
          | inl hlt =>
              have hlt' : i < n + 1 := Nat.lt_succ_of_lt hlt
              simp [hlt, hlt']
          | inr hgt =>
              have hnlt : ¬ i < n := by omega
              have hnlt' : ¬ i < n + 1 := by omega
              simp [hnlt, hnlt']

theorem ballot_getLsbD (f : Nat → Bool) (i : Nat) (hi : i < 64) :
    (ballot f).getLsbD i = f i := by
  unfold ballot
  change (ballotAux f 64).getLsbD i = f i
  rw [ballotAux_getLsbD f 64 i hi]
  simp [hi]

theorem rows_ballot (f : Nat → Bool) : rows (ballot f) = (List.range 64).filter f := by
  unfold rows
  apply List.filter_congr
  intro i hi
  rw [ballot_getLsbD f i (by simpa using (List.mem_range.mp hi))]

theorem zeros_getLsbD (p : Bits) (i : Nat) (hi : i < 64) :
    (zeros p).getLsbD i = !(p.yes.getLsbD i || p.nul.getLsbD i || p.err.getLsbD i) := by
  simp [zeros, BitVec.getLsbD_not, BitVec.getLsbD_or, hi]

theorem zeros_getElem (p : Bits) (i : Nat) (hi : i < 64) :
    (zeros p)[i] = !(p.yes[i] || p.nul[i] || p.err[i]) := by
  rw [← BitVec.getLsbD_eq_getElem hi, ← BitVec.getLsbD_eq_getElem hi,
      ← BitVec.getLsbD_eq_getElem hi, ← BitVec.getLsbD_eq_getElem hi]
  exact zeros_getLsbD p i hi

/-! ## Generic contracts from per-lane identities -/

theorem maskedTruth_of (k : Test)
    (hyes : ∀ a b, well a → well b → ∀ i, i < 64 →
      (k.emit a b).yes.getLsbD i = decide (observe k a b i == .yes)) :
    maskedTruth k := by
  intro m a b ha hb
  apply bitvec_eq_of_lanes
  intro i hi
  simp [gate, ballot_getLsbD _ i hi, hyes a b ha hb i hi, BitVec.getLsbD_and]

theorem nullOutput_of (k : Test)
    (hnul : ∀ a b, well a → well b → ∀ i, i < 64 →
      (k.emit a b).nul.getLsbD i = decide (observe k a b i == .null)) :
    nullOutput k := by
  intro m a b ha hb
  apply bitvec_eq_of_lanes
  intro i hi
  simp [gate, ballot_getLsbD _ i hi, hnul a b ha hb i hi, BitVec.getLsbD_and]

theorem trapBallot_of (k : Test)
    (herr : ∀ a b, well a → well b → ∀ i, i < 64 →
      (k.emit a b).err.getLsbD i = decide (observe k a b i == .fault)) :
    trapBallot k := by
  intro m a b ha hb
  let P : Nat → Bool := fun i => observe k a b i == .fault
  have hmask : (gate m (k.emit a b)).err = ballot (fun i => m.getLsbD i && P i) := by
    apply bitvec_eq_of_lanes
    intro i hi
    simp [gate, ballot_getLsbD _ i hi, herr a b ha hb i hi, P, BitVec.getLsbD_and]
  change (rows ((gate m (k.emit a b)).err)).head? = (rows m).find? P
  rw [hmask]
  change (rows (ballot (fun i => m.getLsbD i && P i))).head? =
    ((List.range 64).filter m.getLsbD).find? P
  rw [rows_ballot]
  rw [show (List.range 64).filter (fun i => m.getLsbD i && P i) =
      ((List.range 64).filter m.getLsbD).filter P by
        rw [List.filter_filter]
        apply List.filter_congr
        intro i hi
        simp [Bool.and_comm]]
  rw [List.head?_filter]

theorem limitBeforeFault_of (k : Test)
    (hyes : ∀ a b, well a → well b → ∀ i, i < 64 →
      (k.emit a b).yes.getLsbD i = decide (observe k a b i == .yes))
    (herr : ∀ a b, well a → well b → ∀ i, i < 64 →
      (k.emit a b).err.getLsbD i = decide (observe k a b i == .fault)) :
    limitBeforeFault k := by
  intro m a b ha hb
  let out := gate m (k.emit a b)
  have hmask : out.yes ||| out.err =
      ballot (fun i => m.getLsbD i && firstEvent (observe k a b i)) := by
    apply bitvec_eq_of_lanes
    intro i hi
    simp [out, gate, ballot_getLsbD _ i hi, hyes a b ha hb i hi,
      herr a b ha hb i hi, firstEvent, BitVec.getLsbD_and, BitVec.getLsbD_or]
    by_cases hm : m.getLsbD i <;> simp [hm]
  change (rows (out.yes ||| out.err)).head? = (rows m).find? (fun i => firstEvent (observe k a b i))
  rw [hmask]
  change (rows (ballot (fun i => m.getLsbD i && firstEvent (observe k a b i)))).head? =
    ((List.range 64).filter m.getLsbD).find? (fun i => firstEvent (observe k a b i))
  rw [rows_ballot]
  rw [show (List.range 64).filter (fun i => m.getLsbD i && firstEvent (observe k a b i)) =
      ((List.range 64).filter m.getLsbD).filter (fun i => firstEvent (observe k a b i)) by
        rw [List.filter_filter]
        apply List.filter_congr
        intro i hi
        simp [Bool.and_comm]]
  rw [List.head?_filter]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
