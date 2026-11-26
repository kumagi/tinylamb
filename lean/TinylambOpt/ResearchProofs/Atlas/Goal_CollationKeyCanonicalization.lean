import Std
import TinylambOpt.ProposedGoals.CollationKeyCanonicalization

/-!
Proved

- `Goal` — Key-based grouping and ordering may replace raw grouping and ordering as
  long as the key does not merge distinct raw values and preserves order.

The reference model is defined in
`TinylambOpt.ProposedGoals.CollationKeyCanonicalization`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CollationKeyCanonicalization

open TinylambOpt.ProposedGoals.CollationKeyCanonicalization

/-- Brute-force sanity check (finite domain of 5 values, 7 key codes,
    all row lists of length at most 2): every key passing both guards also
    passes the grouping identity.  Verified separately with `#eval`. -/
theorem sound_CollationKeyCanonicalization : Goal := by
  intro key hkey _ rows v
  have hpred : (fun r : Row => key r.1 == key v) = (fun r : Row => rawEq r.1 v) := by
    funext r
    by_cases h : r.1 = v
    · subst v
      simp [rawEq]
    · have hkfalse : (key r.1 == key v) = false := by
        cases hkv : key r.1 == key v
        · rfl
        · exfalso
          have hraw : rawEq r.1 v = true := hkey r.1 v hkv
          unfold rawEq at hraw
          exact h (beq_iff_eq.mp hraw)
      have hrawfalse : rawEq r.1 v = false := by
        simp [rawEq, h]
      simp [hkfalse, hrawfalse]
  simp [groupByKey, groupRaw, hpred]

end TinylambOpt.ResearchProofs.Atlas.Goal_CollationKeyCanonicalization
