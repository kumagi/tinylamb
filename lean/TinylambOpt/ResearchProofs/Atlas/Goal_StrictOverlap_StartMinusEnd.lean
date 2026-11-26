import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `StrictOverlap.StartMinusEnd` — `∀ p rs, p.x < p.y → (∀ r ∈ rs, r.1.x < r.1.y) → scan
  shape p rs = ((rs.filter (fun r => r.1.x < p.y)).map Prod.snd).sum - ((rs.filter
  (fun r => r.1.y ≤ p.x)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_StartMinusEnd

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StrictOverlap

-- scratch brute-force truth check
private def pts : List Point := (List.range 7).flatMap (fun i => (List.range 7).map (fun j => ⟨Int.ofNat i - 3, Int.ofNat j - 3⟩))
private def rows1 : List (Point × Int) := pts.flatMap (fun p => [(p, -2), (p, 0), (p, 1)])
private def rows2 : List (List (Point × Int)) := rows1.flatMap (fun r => rows1.map (fun s => [r, s]))
private def probes : List Point := pts.filter (fun p => p.x < p.y)
private def check (p : Point) (rs : List (Point × Int)) : Bool :=
  (rs.all (fun r => r.1.x < r.1.y)) →
  scan shape p rs =
    ((rs.filter (fun r => r.1.x < p.y)).map Prod.snd).sum -
      ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum
#eval probes.all (fun p => rows2.all (fun rs => check p rs))

private theorem scan_cons (p : Point) (a : Point × Int) (rs : Rows) :
    scan shape p (a :: rs) = (if shape.qualifies p a.1 then a.2 else 0) + scan shape p rs := by
  by_cases h : shape.qualifies p a.1 <;> simp [scan, h]

private theorem filterSum_cons (f : Point → Bool) (a : Point × Int) (rs : Rows) :
    ((List.filter (fun r => f r.1) (a :: rs)).map Prod.snd).sum =
      (if f a.1 then a.2 else 0) + ((rs.filter (fun r => f r.1)).map Prod.snd).sum := by
  by_cases h : f a.1 <;> simp [h]

private theorem row_eq (p : Point) (a : Point × Int) (hp : p.x < p.y) (ha : a.1.x < a.1.y) :
    (if shape.qualifies p a.1 then a.2 else 0) =
      (if a.1.x < p.y then a.2 else 0) - (if a.1.y ≤ p.x then a.2 else 0) := by
  have hqiff : shape.qualifies p a.1 ↔ a.1.x < p.y ∧ p.x < a.1.y := by
    simp [shape]
    constructor <;> intro h <;> omega
  by_cases hf : a.1.x < p.y
  · by_cases hg : a.1.y ≤ p.x
    · have hqf : ¬ shape.qualifies p a.1 := by
        rw [hqiff]
        intro h
        omega
      simp [hqf, hf, hg]
    · have hg' : p.x < a.1.y := by omega
      have hqt : shape.qualifies p a.1 := by
        rw [hqiff]
        exact ⟨hf, hg'⟩
      simp [hqt, hf, hg]
  · by_cases hg : a.1.y ≤ p.x
    · exfalso
      omega
    · have hqf : ¬ shape.qualifies p a.1 := by
        rw [hqiff]
        intro h
        omega
      simp [hqf, hf, hg]

theorem sound_StrictOverlap_StartMinusEnd : StrictOverlap.StartMinusEnd.Goal := by
  intro p rs hp hrs
  induction rs with
  | nil => simp [scan]
  | cons a rs ih =>
      have ha : a.1.x < a.1.y := hrs a (by simp)
      have hrs' : ∀ r ∈ rs, r.1.x < r.1.y := by
        intro r hr
        exact hrs r (by simp [hr])
      have ih' := ih hrs'
      rw [scan_cons]
      rw [filterSum_cons (fun v => v.x < p.y)]
      rw [filterSum_cons (fun v => v.y ≤ p.x)]
      rw [ih']
      have hrow := row_eq p a hp ha
      rw [hrow]
      simp only [decide_eq_true_eq]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_StrictOverlap_StartMinusEnd
