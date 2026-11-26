import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.DictionaryByTraceValue research goal: the proposed rewrite is sound and preserves the modeled result. -/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_DictionaryByTraceValue
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.DictionaryByTraceValue

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

theorem run_congr (prev fresh : List V) (e : E)
    (h : ∀ i ∈ reads prev e, slot prev i = slot fresh i) :
    run fresh e = run prev e := by
  induction e with
  | arg j =>
      have hj : slot prev j = slot fresh j := h j (by simp [reads, run])
      unfold slot at hj
      simpa [run] using hj.symm
  | lit w => simp [run]
  | neg a ih =>
      have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
        intro i hi; apply h; simpa [reads, run] using hi
      have iha := ih hga
      simp [run, iha]
  | isTrue a ih =>
      have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
        intro i hi; apply h; simpa [reads, run] using hi
      have iha := ih hga
      simp [run, iha]
  | missing a ih =>
      have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
        intro i hi; apply h; simpa [reads, run] using hi
      have iha := ih hga
      simp [run, iha]
  | conj a b iha ihb =>
      cases hrunA : run prev a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hi
            have hi' : i ∈ ra := by simpa [reads, hrunA] using hi
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hi'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | no =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hi
            have hi' : i ∈ ra := by simpa [reads, hrunA] using hi
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hi'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hi
            have hi' : i ∈ ra := by simpa [reads, hrunA] using hi
            apply h
            have hpref : ra <+: (reads prev (E.conj a b)) := by
              simp [reads, run, hrunA]
              exact after_prefix (V.yes, ra) _
            exact List.IsPrefix.mem hi' hpref
          have ihaR := iha hga
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | no =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | yes =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | null =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
      | null =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hi
            have hi' : i ∈ ra := by simpa [reads, hrunA] using hi
            apply h
            have hpref : ra <+: (reads prev (E.conj a b)) := by
              simp [reads, run, hrunA]
              exact after_prefix (V.null, ra) _
            exact List.IsPrefix.mem hi' hpref
          have ihaR := iha hga
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | no =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | yes =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | null =>
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hi
                have hi' : i ∈ rb := by simpa [reads, hrunB] using hi
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using (by simp [hi'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
  | disj a b iha ihb =>
      cases hrunA : run prev a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | no =>
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | yes =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | no =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | null =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.no, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
      | null =>
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | yes =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | no =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | null =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.disj a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
  | co a b iha ihb =>
      cases hrunA : run prev a with | mk va ra =>
      cases va with
      | fault s =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | yes =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | no =>
          have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
            intro i hia
            have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
            apply h
            simpa [reads, run, after, hrunA, vbeq] using hia'
          have ihaR := iha hga
          simp [run, after, hrunA, ihaR, vbeq]
      | null =>
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | yes =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | no =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
          | null =>
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                have hpref : ra <+: (reads prev (E.co a b)) := by
                  simp [reads, run, hrunA]
                  exact after_prefix (V.null, ra) _
                exact List.IsPrefix.mem hia' hpref
              have ihaR := iha hga
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunA, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ ra ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunA, hrunB, ihaR, ihbR, vbeq]
  | branch p a b ihp iha ihb =>
      cases hrunP : run prev p with | mk vp rp =>
      cases vp with
      | fault s =>
          have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
            intro i hip
            have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
            apply h
            simpa [reads, run, after, hrunP, vbeq] using hip'
          have ihpR := ihp hgp
          simp [run, after, hrunP, ihpR, vbeq]
      | yes =>
          cases hrunA : run prev a with | mk va ra =>
          cases va with
          | fault s =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [run, after, hrunP, hrunA, ihpR, ihaR, vbeq]
          | no =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [run, after, hrunP, hrunA, ihpR, ihaR, vbeq]
          | yes =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [run, after, hrunP, hrunA, ihpR, ihaR, vbeq]
          | null =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.yes, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hga : ∀ i ∈ reads prev a, slot prev i = slot fresh i := by
                intro i hia
                have hia' : i ∈ ra := by simpa [reads, hrunA] using hia
                apply h
                simpa [reads, run, after, hrunP, hrunA, vbeq] using
                  (by simp [hia'] : i ∈ rp ++ ra)
              have ihaR := iha hga
              simp [run, after, hrunP, hrunA, ihpR, ihaR, vbeq]
      | no =>
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | yes =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | no =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | null =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.no, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
      | null =>
          cases hrunB : run prev b with | mk vb rb =>
          cases vb with
          | fault s =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | yes =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | no =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]
          | null =>
              have hgp : ∀ i ∈ reads prev p, slot prev i = slot fresh i := by
                intro i hip
                have hip' : i ∈ rp := by simpa [reads, hrunP] using hip
                apply h
                have hpref : rp <+: (reads prev (E.branch p a b)) := by
                  simp [reads, run, hrunP]
                  exact after_prefix (V.null, rp) _
                exact List.IsPrefix.mem hip' hpref
              have ihpR := ihp hgp
              have hgb : ∀ i ∈ reads prev b, slot prev i = slot fresh i := by
                intro i hib
                have hib' : i ∈ rb := by simpa [reads, hrunB] using hib
                apply h
                simpa [reads, run, after, hrunP, hrunB, vbeq] using
                  (by simp [hib'] : i ∈ rp ++ rb)
              have ihbR := ihb hgb
              simp [run, after, hrunP, hrunB, ihpR, ihbR, vbeq]



theorem sound : Goal := by
  intro prev fresh e h
  exact run_congr prev fresh e h

end Nov26.Proof_MaskSpecialization_DictionaryByTraceValue
