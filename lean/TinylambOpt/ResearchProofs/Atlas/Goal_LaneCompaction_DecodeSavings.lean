import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `LaneCompaction.DecodeSavings` — `∀ m (widths : List Nat), m.length = widths.length →
  widths.sum - (pack m widths).sum = (pack (m.map (!·)) widths).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_DecodeSavings

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

/-! 総当たり確認: 長さ 3 以下の全マスク × 全幅列に反例なし。 -/
def masksUpTo : Nat → List (List Bool)
  | 0 => [[]]
  | n + 1 => (masksUpTo n).flatMap (fun m => [true :: m, false :: m])

def widthsUpTo : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (widthsUpTo n).flatMap (fun ws => [0 :: ws, 1 :: ws, 2 :: ws])

#eval (masksUpTo 3).filterMap (fun m =>
  ((widthsUpTo 3).find? (fun w =>
    m.length == w.length &&
      w.sum - (pack m w).sum != (pack (m.map (!·)) w).sum)).map (fun w => (m, w)))

private theorem pack_sum_le (m : List Bool) (ws : List Nat) : (pack m ws).sum ≤ ws.sum := by
  induction m generalizing ws with
  | nil => simp [pack]
  | cons ok m ih =>
      cases ws with
      | nil => simp [pack]
      | cons w ws =>
          have h := ih ws
          by_cases hok : ok
          · simp [pack, hok] at h ⊢
            omega
          · simp [pack, hok] at h ⊢
            omega

theorem sound_LaneCompaction_DecodeSavings : DecodeSavings.Goal := by
  intro m
  induction m with
  | nil =>
      intro widths hlen
      cases widths with
      | nil => simp [pack]
      | cons w ws => simp at hlen
  | cons ok m ih =>
      intro widths hlen
      cases widths with
      | nil => simp at hlen
      | cons w ws =>
          simp at hlen
          have hi := ih ws hlen
          have hle := pack_sum_le m ws
          by_cases hok : ok
          · simp [pack, hok] at hi hle ⊢
            omega
          · simp [pack, hok] at hi hle ⊢
            omega

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_DecodeSavings
