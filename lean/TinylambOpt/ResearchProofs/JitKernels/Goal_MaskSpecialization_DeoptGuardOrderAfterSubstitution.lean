import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_DeoptGuardOrderAfterSubstitution

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA


private theorem vbeq (a b : V) : (a == b) = decide (a = b) := by
  change TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.instBEqV.beq a b = decide (a = b)
  cases a <;> cases b <;> simp [TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.instBEqV.beq]
  rename_i s t
  by_cases h : s == t
  · simp [h]
    exact beq_iff_eq.mp h
  · simp [h]
    intro heq
    simp [heq] at h

private theorem after_prefix (t : Trace) (k : V → Trace) : t.2 <+: (after t k).2 := by
  cases t with | mk v r =>
  cases v <;> simp [after]

theorem subst_run (env : List V) (e : E) (i : Nat) (v : V)
    (hguard : i ∈ reads env e → slot env i = v) :
    run env (specialize i v e) = ((run env e).1, (reads env e).filter (fun j => j != i)) := by
  induction e with
  | arg j =>
      by_cases hji : j == i
      · have hjeqi : j = i := by simpa using hji
        subst j
        have hg : slot env i = v := hguard (by simp [reads, run])
        unfold slot at hg
        simpa [specialize, run, reads] using hg.symm
      · have hji' : j ≠ i := by
          intro hj; apply hji; simp [hj]
        simp [specialize, hji, hji', reads, run]
  | lit w => simp [specialize, run, reads]
  | neg a ih =>
      have hga : i ∈ reads env a → slot env i = v := by
        intro hia; apply hguard; simpa [reads, run] using hia
      specialize ih hga
      simp [specialize, run, ih, reads]
  | isTrue a ih =>
      have hga : i ∈ reads env a → slot env i = v := by
        intro hia; apply hguard; simpa [reads, run] using hia
      specialize ih hga
      simp [specialize, run, ih, reads]
  | missing a ih =>
      have hga : i ∈ reads env a → slot env i = v := by
        intro hia; apply hguard; simpa [reads, run] using hia
      specialize ih hga
      simp [specialize, run, ih, reads]
  | conj a b iha ihb =>
      cases hrunA : run env a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | no =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            have hpref : ra <+: (reads env (E.conj a b)) := by
              simp [reads, run, hrunA]
              exact after_prefix (V.yes, ra) _
            exact List.IsPrefix.mem hia' hpref
          have ihaR := iha hga
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | no =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | null =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
      | null =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            have hpref : ra <+: (reads env (E.conj a b)) := by
              simp [reads, run, hrunA]
              exact after_prefix (V.null, ra) _
            exact List.IsPrefix.mem hia' hpref
          have ihaR := iha hga
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | no =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | null =>
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (List.mem_append_right ra hib' : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
  | disj a b iha ihb =>
      cases hrunA : run env a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | no =>
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | no =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | null =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
      | null =>
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | no =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | null =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
  | co a b iha ihb =>
      cases hrunA : run env a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | no =>
          have hga : i ∈ reads env a → slot env i = v := by
            intro hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply hguard
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [specialize, run, reads, after, hrunA, ihaR, vbeq]
      | null =>
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | no =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
          | null =>
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                have hpref : ra <+: (reads env (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunA, hrunB, ihaR, ihbR, List.filter_append, vbeq]
  | branch p a b ihp iha ihb =>
      cases hrunP : run env p with | mk vp rp =>
      cases vp with
      | fault s =>
          have hgp : i ∈ reads env p → slot env i = v := by
            intro hip
            have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
            apply hguard
            simpa [reads, run, after, hrunP, vbeq] using hip'
          have ihpR := ihp hgp
          simp [specialize, run, reads, after, hrunP, ihpR, vbeq]
      | yes =>
          cases hrunA : run env a with | mk va ra =>
          cases va with
          | fault s =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [specialize, run, reads, after, hrunP, hrunA, ihpR, ihaR, List.filter_append, vbeq]
          | no =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [specialize, run, reads, after, hrunP, hrunA, ihpR, ihaR, List.filter_append, vbeq]
          | yes =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [specialize, run, reads, after, hrunP, hrunA, ihpR, ihaR, List.filter_append, vbeq]
          | null =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : i ∈ reads env a → slot env i = v := by
                intro hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply hguard
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [specialize, run, reads, after, hrunP, hrunA, ihpR, ihaR, List.filter_append, vbeq]
      | no =>
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | no =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | null =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
      | null =>
          cases hrunB : run env b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | yes =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | no =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]
          | null =>
              have hgp : i ∈ reads env p → slot env i = v := by
                intro hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply hguard
                have hpref : rp <+: (reads env (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : i ∈ reads env b → slot env i = v := by
                intro hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply hguard
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [specialize, run, reads, after, hrunP, hrunB, ihpR, ihbR, List.filter_append, vbeq]


theorem sound : DeoptGuardOrderAfterSubstitution.Goal := by
  intro env e i j x y hs hg2
  have h1 := subst_run env e i x (fun _ => hs)
  have h2 := subst_run env (specialize i x e) j y hg2
  have ha1 : answer env (specialize i x e) = answer env e := by
    simpa [answer] using congrArg Prod.fst h1
  have ha1' : (run env (specialize i x e)).1 = (run env e).1 := by
    simpa [answer] using ha1
  have h2v := congrArg Prod.fst h2
  rw [ha1'] at h2v
  simpa [answer] using h2v

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_DeoptGuardOrderAfterSubstitution
