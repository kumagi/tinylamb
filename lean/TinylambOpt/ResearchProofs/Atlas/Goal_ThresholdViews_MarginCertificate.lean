import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `ThresholdViews.MarginCertificate` — `∀ old patch k, (weight old k).natAbs >
  ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)).sum → active old k =
  active (old++patch) k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_MarginCertificate

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews

/-! 小さい入力（要素を 7 通り、old/patch を長さ 2 以下、k を 0/1）の総当たりで
    符号が反転しないことを確認したうえで、一般命題を証明する。 -/

private def elems : List (Nat × Int) :=
  [(0,-2),(0,-1),(0,0),(0,1),(0,2),(1,-1),(1,1)]

private def bagsUpTo : Nat → List Bag
  | 0 => [[]]
  | n+1 => bagsUpTo n ++ (bagsUpTo n).flatMap (fun xs => elems.map (fun e => e :: xs))

private def hcheck (old patch : Bag) (k : Nat) : Bool :=
  let w := ((old.filter (fun r => r.1 == k)).map Prod.snd).sum
  let p := ((patch.filter (fun r => r.1 == k)).map Prod.snd).sum
  let act (xs : Bag) := decide (0 < ((xs.filter (fun r => r.1 == k)).map Prod.snd).sum)
  decide ((p.natAbs : Int) < w.natAbs) → act old == act (old ++ patch)

private def checkAll : Bool :=
  (bagsUpTo 2).all (fun old => (bagsUpTo 2).all (fun patch =>
    [0,1].all (fun k => hcheck old patch k)))

#guard checkAll

private theorem natAbs_sum_le_sum_natAbs (xs : List Int) :
    xs.sum.natAbs ≤ (xs.map Int.natAbs).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (Int.natAbs_add_le x xs.sum) (Nat.add_le_add_left ih x.natAbs)

private theorem weight_append (old patch : Bag) (k : Nat) :
    weight (old ++ patch) k = weight old k + weight patch k := by
  simp [weight, List.filter_append, List.map_append, List.sum_append]

theorem sound_ThresholdViews_MarginCertificate : MarginCertificate.Goal := by
  intro old patch k h
  have hsum : (weight patch k).natAbs ≤
      ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)).sum := by
    have hs := natAbs_sum_le_sum_natAbs ((patch.filter (fun r => r.1 == k)).map Prod.snd)
    have hmap : (((patch.filter (fun r => r.1 == k)).map Prod.snd).map Int.natAbs) =
        ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)) := by
      rw [List.map_map]
      rfl
    simpa [weight, hmap] using hs
  have hlt : (weight patch k).natAbs < (weight old k).natAbs := by omega
  have hltI : ((weight patch k).natAbs : Int) < ((weight old k).natAbs : Int) :=
    Int.ofNat_lt.mpr hlt
  by_cases hpos : 0 < weight old k
  · have hold : ((weight old k).natAbs : Int) = weight old k :=
      Int.natAbs_of_nonneg (by omega)
    have hp_le : weight patch k ≤ ((weight patch k).natAbs : Int) := Int.le_natAbs
    have hnew : 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]
  · have hne : weight old k ≠ 0 := by
      intro hz
      rw [hz] at hlt
      have hznonneg : 0 ≤ (weight patch k).natAbs := Nat.zero_le _
      omega
    have holdneg : weight old k < 0 := by omega
    have hold : ((weight old k).natAbs : Int) = -(weight old k) := by
      rw [← Int.natAbs_neg]
      exact Int.natAbs_of_nonneg (by omega)
    have hp_ge : -(weight patch k) ≤ ((weight patch k).natAbs : Int) := by
      have ht : -weight patch k ≤ (((-weight patch k).natAbs : Int)) := Int.le_natAbs
      rwa [Int.natAbs_neg] at ht
    have hnew : ¬ 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]

end TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_MarginCertificate
