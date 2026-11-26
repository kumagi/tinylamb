import TinylambOpt.ProposedGoals.CollationKeyCanonicalization

/-!
Proved

- `Goal` — Key-based grouping and ordering may replace raw grouping and ordering as
  long as the key does not merge distinct raw values and preserves order.

The reference model is defined in
`TinylambOpt.ProposedGoals.CollationKeyCanonicalization`.
-/

namespace TinylambOpt.ProposedProofs.CollationKeyCanonicalization

open TinylambOpt.ProposedGoals.CollationKeyCanonicalization

theorem sound : Goal := by
  intro key hkey horder rows v
  have hfilter : rows.filter (fun r => key r.1 == key v) =
      rows.filter (fun r => rawEq r.1 v) := by
    apply List.filter_congr
    intro r hr
    by_cases h : r.1 = v
    · subst v
      simp [rawEq]
    · have hkfalse : (key r.1 == key v) = false := by
        cases hkv : key r.1 == key v
        · rfl
        · exfalso
          have hr_eq : rawEq r.1 v = true := hkey r.1 v hkv
          unfold rawEq at hr_eq
          exact h (beq_iff_eq.mp hr_eq)
      have hrawfalse : rawEq r.1 v = false := by
        unfold rawEq
        simp [h]
      rw [hkfalse, hrawfalse]
  simpa [groupByKey, groupRaw] using
    congrArg (fun l : List Row => (l.map Prod.snd).sum) hfilter

end TinylambOpt.ProposedProofs.CollationKeyCanonicalization
