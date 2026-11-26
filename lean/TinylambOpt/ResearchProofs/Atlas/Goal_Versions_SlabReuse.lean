import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `Versions.SlabReuse` — `∀ es k t l h, l ≤ h → (∀ e ∈ es, e.key = k → e.tx ≤ t →
  (e.since ≤ l ∨ h < e.since) ∧ (e.stop ≤ l ∨ h < e.stop)) → ∀ v, l ≤ v → v ≤ h →
  winner es k t v = winner es k t l`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Versions_SlabReuse

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions

/-! Exhaustive small-input sanity check: enumerate every sub-log of six
hand-picked events and every small `(k, t, l, h, v)`; whenever the slab
hypothesis holds, the winner at the left endpoint must equal the winner at
any interior point. The result is `true` iff no counterexample exists. -/

def baseEvents : List Event :=
  [ ⟨0, 0, 0, 0, 3, some 1⟩
  , ⟨0, 1, 0, 1, 4, none⟩
  , ⟨0, 1, 1, 0, 2, some 5⟩
  , ⟨1, 0, 0, 0, 4, some 2⟩
  , ⟨1, 1, 0, 2, 5, none⟩
  , ⟨1, 2, 0, 1, 3, some 7⟩ ]

def subsets : List Event → List (List Event)
  | [] => [[]]
  | e :: es => (subsets es).flatMap (fun s => [s, e :: s])

def slabCond (es : List Event) (k t l h : Nat) : Bool :=
  es.all (fun e => e.key != k || t < e.tx ||
    ((e.since ≤ l || h < e.since) && (e.stop ≤ l || h < e.stop)))

#eval (subsets baseEvents).all (fun es =>
  (List.range 2).all (fun k =>
  (List.range 3).all (fun t =>
  (List.range 4).all (fun l =>
  (List.range 4).all (fun h =>
    if l ≤ h && slabCond es k t l h then
      (List.range 5).all (fun v =>
        if l ≤ v && v ≤ h then winner es k t v == winner es k t l else true)
    else true)))))

/-- Between the endpoints of a slab, eligibility of every relevant event is
constant, so the filtered log — and hence its newest winner — is unchanged. -/
theorem sound_Versions_SlabReuse : Versions.SlabReuse.Goal := by
  intro es k t l h hle hcond v hlv hvh
  unfold winner
  have hfilter : es.filter (eligible k t v) = es.filter (eligible k t l) := by
    apply List.filter_congr
    intro e he
    unfold eligible
    by_cases hkey : (e.key == k) = true
    · by_cases htx : e.tx ≤ t
      · have hspec := hcond e he (beq_iff_eq.mp hkey) htx
        rcases hspec with ⟨hsince, hstop⟩
        by_cases hsl : e.since ≤ l
        · have hsv : e.since ≤ v := Int.le_trans hsl hlv
          by_cases hst : e.stop ≤ l
          · have hvs : ¬ v < e.stop := by omega
            have hls : ¬ l < e.stop := by omega
            simp [hkey, htx, hsl, hsv, hvs, hls]
          · have hsh : h < e.stop := hstop.resolve_left hst
            have hvs : v < e.stop := Int.lt_of_le_of_lt hvh hsh
            have hls : l < e.stop := Int.lt_of_le_of_lt hle hsh
            simp [hkey, htx, hsl, hsv, hvs, hls]
        · have hsh : h < e.since := hsince.resolve_left hsl
          have hsv : ¬ e.since ≤ v := by omega
          by_cases hst : e.stop ≤ l
          · have hvs : ¬ v < e.stop := by omega
            have hls : ¬ l < e.stop := by omega
            simp [hkey, htx, hsl, hsv, hvs, hls]
          · have hstop' : h < e.stop := hstop.resolve_left hst
            have hvs : v < e.stop := Int.lt_of_le_of_lt hvh hstop'
            have hls : l < e.stop := Int.lt_of_le_of_lt hle hstop'
            simp [hkey, htx, hsl, hsv, hvs, hls]
      · simp [hkey, htx]
    · simp [hkey]
  rw [hfilter]

end TinylambOpt.ResearchProofs.Atlas.Goal_Versions_SlabReuse
