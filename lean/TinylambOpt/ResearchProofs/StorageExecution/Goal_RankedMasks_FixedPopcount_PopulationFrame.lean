import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_FixedPopcount_PopulationFrame

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples
open TinylambOpt.ResearchGoals.Compression.Wire

-- Truth check on all bitmaps of length < 10 (every rank, including rank zero).
private def checkOne (xs : List Bool) : Bool :=
  let n := xs.length
  let k := xs.count true
  let r := (rank model k xs).getD 0
  let bits := width (choose n k - 1)
  let packet := [k] ++ little r (byteCount bits)
  decide (unrank model n (packet.head?.getD 0) (fromLittle (packet.drop 1)) = some xs ∧
    packet.length = 1 + byteCount (codeBits model n k))

private def check : Bool :=
  (List.range 10).all fun n =>
  (List.range (2 ^ n)).all fun i =>
    checkOne ((List.range n).map fun j => (i / 2 ^ j) % 2 == 1)

#eval check

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

/-! A legal word always has a rank (the run never gets stuck). -/
private theorem rank_some_of_legal (m : Machine) : ∀ (xs : List Bool) (q : Nat),
    legal m q xs = true → ∃ r, rank m q xs = some r
  | [], q, h => by
      have hacc : m.accept q = true := by simpa [legal, run] using h
      exact ⟨0, by simp [rank, hacc]⟩
  | b :: bs, q, h => by
      cases hs : m.step q b with
      | none => simp [legal, run, hs] at h
      | some q' =>
        have hleg' : legal m q' bs = true := by
          simpa [legal, run, hs] using h
        obtain ⟨r, hr⟩ := rank_some_of_legal m bs q' hleg'
        exact ⟨r + (if b then lowCount m bs.length q else 0), by simp [rank, hs, hr]⟩

/-! The rank of an accepted word is strictly below the number of accepted
words of that length, i.e. below the number of available codes. -/
private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
      unfold rank at h
      by_cases hacc : m.accept q = true
      · simp [hacc] at h
        subst h
        simp [count, hacc]
      · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
        simp [hacc'] at h
  | b :: bs, q, r, h => by
      have hlen : (b :: bs).length = bs.length + 1 := by simp
      rw [hlen, count_succ]
      unfold rank at h
      cases hs : m.step q b with
      | none => simp [hs] at h
      | some q' =>
        simp [hs] at h
        cases hr : rank m q' bs with
        | none => simp [hr] at h
        | some tail =>
          simp [hr, Option.bind_some] at h
          subst h
          have ih := rank_bound m bs q' tail hr
          cases b with
          | false =>
            simp [lowCount, hs] at ih ⊢
            omega
          | true =>
            simp [lowCount, hiCount, hs] at ih ⊢
            omega

/-! Ranking then unranking recovers the original bitmap. -/
private theorem decoder_all (m : Machine) : decoder m := by
  intro q xs r hrank
  induction xs generalizing q r with
  | nil =>
    unfold rank at hrank
    by_cases hacc : m.accept q = true
    · simp [hacc] at hrank
      subst hrank
      unfold unrank
      simp [hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at hrank
  | cons b bs ih =>
    unfold rank at hrank
    cases hs : m.step q b with
    | none => simp [hs] at hrank
    | some q' =>
      simp [hs] at hrank
      cases ht : rank m q' bs with
      | none => simp [ht] at hrank
      | some tail =>
        simp [ht, Option.bind_some] at hrank
        subst hrank
        have ih' := ih q' tail ht
        have hbound := rank_bound m bs q' tail ht
        have hlen : (b :: bs).length = bs.length + 1 := by simp
        unfold unrank
        simp only [hlen]
        cases b with
        | false =>
          have hlt : ¬ (lowCount m bs.length q ≤ tail) := by
            simp [lowCount, hs] at hbound ⊢
            omega
          have hdec : decide (lowCount m bs.length q ≤ tail) = false := by simp [hlt]
          simp [hdec, hs, ih', Option.bind_some]
        | true =>
          have hle : lowCount m bs.length q ≤ tail + lowCount m bs.length q := by omega
          have hdec : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by simp [hle]
          simp [hdec, hs, Option.bind_some]
          have : tail + lowCount m bs.length q - lowCount m bs.length q = tail := by omega
          simp [this, ih']

private theorem fromLittle_append_singleton (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + 256 ^ xs.length * b := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  unfold little
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 n (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h; simp at h; subst h; decide
  · rename_i h
    have _hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    exact Nat.lt_log2_self

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  exact Nat.lt_of_le_of_lt (Nat.le_pred_of_lt hr) hlt

private theorem fp_count_succ (n q : Nat) :
    count model (n + 1) q = count model n q + if q = 0 then 0 else count model n (q - 1) := by
  rw [count_succ]
  by_cases hq : q = 0 <;> simp [lowCount, hiCount, model, hq]

private theorem fp_count_zero_right (n : Nat) : count model n 0 = 1 := by
  induction n with
  | zero => simp [count, model]
  | succ n ih => rw [fp_count_succ, ih]; simp

private theorem fp_count_eq_choose (n k : Nat) :
    count model n k = choose n k := by
  induction n generalizing k with
  | zero =>
    cases k with
    | zero => simp [count, model, choose]
    | succ k => simp [count, model, choose]
  | succ n ih =>
    cases k with
    | zero =>
      rw [fp_count_succ]
      simp [ih, choose, fp_count_zero_right]
    | succ k =>
      rw [fp_count_succ]
      simp [ih, choose]
      omega

private theorem fp_run_count (xs : List Bool) (q : Nat) (h : xs.count true ≤ q) :
    run model q xs = some (q - xs.count true) := by
  induction xs generalizing q with
  | nil => simp [run]
  | cons b bs ih =>
      cases b with
      | false =>
        have hstep : model.step q false = some q := by simp [model]
        rw [run_cons, hstep]
        simp only [Option.bind_some]
        simpa [List.count_cons] using ih q (by simpa [List.count_cons] using h)
      | true =>
        have hq : q ≠ 0 := by
          simp [List.count_cons] at h
          omega
        have hstep : model.step q true = some (q - 1) := by simp [model, hq]
        rw [run_cons, hstep]
        simp only [Option.bind_some]
        have h' : bs.count true ≤ q - 1 := by
          simp [List.count_cons] at h
          omega
        rw [ih (q - 1) h']
        simp [List.count_cons]
        omega

private theorem fp_legal_count (xs : List Bool) :
    legal model (xs.count true) xs = true := by
  have hr := fp_run_count xs (xs.count true) (Nat.le_refl _)
  have hzero : xs.count true - xs.count true = 0 := by omega
  simp only [legal, hr]
  simp [model, hzero]

theorem sound_PopulationFrame : FixedPopcount.PopulationFrame.Goal := by
  intro xs _hlen
  have hleg : legal model (xs.count true) xs = true := fp_legal_count xs
  obtain ⟨r, hr⟩ := rank_some_of_legal model xs (xs.count true) hleg
  simp [hr]
  have hdec := decoder_all model (xs.count true) xs r hr
  have hcount : count model xs.length (xs.count true) =
      choose xs.length (xs.count true) := fp_count_eq_choose xs.length (xs.count true)
  have hcb : codeBits model xs.length (xs.count true) =
      width (choose xs.length (xs.count true) - 1) := by
    simp [codeBits, hcount]
  constructor
  · rw [fromLittle_little]
    have hrb : r < count model xs.length (xs.count true) := rank_bound model xs (xs.count true) r hr
    have hcbound : r < 2 ^ codeBits model xs.length (xs.count true) :=
      codeBits_bound model xs.length (xs.count true) r hrb
    have hbc : codeBits model xs.length (xs.count true) ≤
        8 * byteCount (codeBits model xs.length (xs.count true)) := by
      simp [byteCount]; omega
    have hpow : 256 ^ byteCount (codeBits model xs.length (xs.count true)) =
        2 ^ (8 * byteCount (codeBits model xs.length (xs.count true))) := by
      have : (256 : Nat) = 2 ^ 8 := by decide
      rw [this, ← Nat.pow_mul]
    have hmod : r % 256 ^ byteCount (codeBits model xs.length (xs.count true)) = r := by
      rw [hpow]
      exact Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le hcbound (Nat.pow_le_pow_right (by decide) hbc))
    have hmod' : r % 256 ^ byteCount (width (choose xs.length (xs.count true) - 1)) = r := by
      simpa [hcb] using hmod
    rw [hmod']
    exact hdec
  · simp [little, hcb, Nat.add_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_FixedPopcount_PopulationFrame
