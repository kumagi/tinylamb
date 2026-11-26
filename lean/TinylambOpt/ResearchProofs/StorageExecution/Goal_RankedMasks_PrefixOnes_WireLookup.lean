import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `PrefixOnes.WireLookup` — A rank packed into whole bytes decodes despite byte
  padding.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_WireLookup

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

/-! A rank always fits the code width derived from `count`, so the byte-padded
wire round trip is the identity on that rank.  Combined with injectivity of
`unrank` (a generic property of the rank/unrank pair), this proves the goal. -/

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases hs : m.step q b with
  | none => simp [rank, hs]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, hs, hr]
    | some r => simp [rank, hs, hr]

private theorem rank_bound (m : Machine) (q : Nat) (xs : List Bool) :
    ∀ r, rank m q xs = some r → r < count m xs.length q := by
  induction xs generalizing q with
  | nil =>
    intro r h
    cases ha : m.accept q <;> simp [rank, count, ha] at h ⊢ <;> omega
  | cons b bs ih =>
    intro r h
    rw [rank_cons] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp only [hs, Option.bind_some] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some rt =>
        simp [hr] at h
        have hrt := ih q' rt hr
        change r < count m (bs.length + 1) q
        rw [count_succ]
        cases b with
        | false =>
          simp only [Bool.false_eq_true, ite_false, Nat.add_zero] at h
          subst r
          have hlow : lowCount m bs.length q = count m bs.length q' := by
            simp [lowCount, hs]
          rw [hlow]
          omega
        | true =>
          simp only [Bool.true_eq_false, ite_true] at h
          subst r
          have hhi : hiCount m bs.length q = count m bs.length q' := by
            simp [hiCount, hs]
          rw [hhi]
          omega

private theorem decoder_generic (m : Machine) : decoder m := by
  intro q xs r hr
  induction xs generalizing q r with
  | nil =>
    simp [rank] at hr
    rcases hr with ⟨hacc, rfl⟩
    simp [unrank, hacc]
  | cons b bs ih =>
    rw [rank_cons] at hr
    cases hs : m.step q b with
    | none => simp [hs] at hr
    | some q' =>
      simp only [hs, Option.bind_some] at hr
      cases hrt : rank m q' bs with
      | none => simp [hrt] at hr
      | some rt =>
        simp [hrt] at hr
        have hrt_bound := rank_bound m q' bs rt hrt
        cases b with
        | false =>
          simp only [Bool.false_eq_true, ite_false, Nat.add_zero] at hr
          subst r
          have hlow : lowCount m bs.length q = count m bs.length q' := by
            simp [lowCount, hs]
          have hnot : ¬ lowCount m bs.length q ≤ rt := by
            rw [hlow]
            exact Nat.not_le.mpr hrt_bound
          simp [unrank, hnot, hs, ih q' rt hrt]
        | true =>
          simp only [Bool.true_eq_false, ite_true] at hr
          subst r
          have hle : lowCount m bs.length q ≤ rt + lowCount m bs.length q :=
            Nat.le_add_left _ _
          have hsub : rt + lowCount m bs.length q - lowCount m bs.length q = rt := by
            omega
          simp [unrank, hle, hs, hsub, ih q' rt hrt]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · rw [width]
    simp [hn]
    exact Nat.lt_log2_self

private theorem get_pack_single (w r : Nat) (hr : r < 2 ^ w) :
    TinylambOpt.ResearchGoals.Compression.Wire.get (pack w [r]) w 0 = r := by
  have h := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse w [r] (by simp [hr])
  simpa using h

theorem sound_WireLookup : WireLookup.Goal := by
  intro q xs r hr
  have hbound := rank_bound model q xs r hr
  have hdec := decoder_generic model q xs r hr
  have hget : TinylambOpt.ResearchGoals.Compression.Wire.get
      (pack (codeBits model xs.length q) [r]) (codeBits model xs.length q) 0 = r := by
    apply get_pack_single
    dsimp [codeBits]
    have hlt := lt_two_pow_width (count model xs.length q - 1)
    omega
  rw [hget]
  exact hdec

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_WireLookup
