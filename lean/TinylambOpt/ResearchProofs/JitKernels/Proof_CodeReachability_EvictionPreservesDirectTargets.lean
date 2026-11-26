import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# EvictionPreservesDirectTargets (proved)

After a mark-sweep keeps the set `live = mark heap roots heap.length`, an
evictor may drop every object whose key is not live.  The claim says that
resident objects' direct-call tables are unchanged by this eviction: for any
live key `k`, `edges (retain heap live) k = edges heap k`.  So an executor may
keep dispatching through cached direct-call tables after reclaiming thunks
without re-marking.

Proof: `retain` is `filter (live.contains ·.key)`.  Show by induction over the
heap list that, whenever `k ∈ live`, filtering only removes objects that
already fail the predicate `o.key == k`: if the head survives the filter and
matches the key, it stays the first match on both sides; if the head fails the
key predicate, `find?` skips it on both sides and the induction hypothesis
applies, whether or not the filter kept it.  Head-key equality is turned into
propositional equality by the lawful `BEq` on `Nat × Nat`.  `edges` then
follows by congruence through `map calls` and `getD []`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_EvictionPreservesDirectTargets
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem contains_of_key_mem (live : List Key) (o : Obj)
    (h : o.key ∈ live) : live.contains o.key = true := by
  rw [List.contains_eq_mem]; exact decide_eq_true h

private theorem find?_retain (live : List Key) (p : Obj → Bool)
    (h : ∀ o, p o = true → o.key ∈ live) :
    ∀ heap : List Obj, (heap.filter (fun o => live.contains o.key)).find? p = heap.find? p
  | [] => rfl
  | o :: os => by
      by_cases hc : live.contains o.key = true
      · rw [List.filter_cons, if_pos hc, List.find?_cons, List.find?_cons]
        by_cases hp : p o = true
        · rw [hp]
        · rw [not_true_eq_false hp, find?_retain live p h os]
      · have hpnf : p o = false := not_true_eq_false
          (fun hp => hc (contains_of_key_mem live o (h o hp)))
        rw [List.filter_cons, if_neg hc, find?_retain live p h os, List.find?_cons, hpnf]

theorem sound : EvictionPreservesDirectTargets.Goal := by
  intro heap roots live k hk
  have hf : ((heap.filter (fun o => live.contains o.key)).find? (fun o => o.key == k))
      = heap.find? (fun o => o.key == k) :=
    find?_retain live (fun o => o.key == k)
      (fun o ho => show o.key ∈ live from by rw [LawfulBEq.eq_of_beq ho]; exact hk) heap
  simp only [edges, retain]
  rw [hf]

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_EvictionPreservesDirectTargets
