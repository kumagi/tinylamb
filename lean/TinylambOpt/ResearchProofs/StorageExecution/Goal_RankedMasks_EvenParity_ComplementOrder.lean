import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `EvenParity.ComplementOrder` — `∀ xs, xs.length%2=0 → legal model 0 xs → (rank model
  0 xs).getD 0 + (rank model 0 (xs.map (!·))).getD 0 = count model xs.length 0-1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_ComplementOrder

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.EvenParity

/-! The complement of a legal even-length page is legal again and sits at the
    opposite end of the lexicographic ranking. The proof works with a
    state-dependent involution `alt`: after `n` bits the complement is ranked
    from state `alt n q` (`q` when `n` is even, `1-q` when `n` is odd). -/

private def alt (n q : Nat) : Nat := if n % 2 = 0 then q else 1 - q

private theorem alt_lt (n q : Nat) (hq : q < 2) : alt n q < 2 := by
  simp [alt]
  split <;> omega

private theorem alt_step (m q : Nat) (b : Bool) (hq : q < 2) :
    alt (m + 1) q = (alt m ((q + b.toNat) % 2) + (1 - b.toNat)) % 2 := by
  by_cases hm : m % 2 = 0
  · have hm1 : (m + 1) % 2 ≠ 0 := by omega
    cases b <;> rcases (show q = 0 ∨ q = 1 by omega) with rfl | rfl <;>
      simp [alt, hm, hm1]
  · have hm1 : (m + 1) % 2 = 0 := by omega
    cases b <;> rcases (show q = 0 ∨ q = 1 by omega) with rfl | rfl <;>
      simp [alt, hm, hm1]

private theorem run_eq : ∀ (q : Nat) (xs : List Bool), q < 2 →
    run model q xs = some ((q + xs.count true) % 2) := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro hq
    simp [run, Nat.mod_eq_of_lt hq]
  | cons b bs ih =>
    intro hq
    simp only [run, List.foldlM_cons]
    change run model ((q + b.toNat) % 2) bs = some ((q + (b :: bs).count true) % 2)
    rw [ih ((q + b.toNat) % 2) (by omega)]
    have hcnt : (b :: bs).count true = b.toNat + bs.count true := by
      cases b <;> simp <;> omega
    congr 1
    calc
      ((q + b.toNat) % 2 + bs.count true) % 2 =
          (q + b.toNat + bs.count true) % 2 := by omega
      _ = (q + (b :: bs).count true) % 2 := by rw [hcnt]; omega

private theorem accept_eq (s : Nat) : model.accept s = decide (s = 0) := by
  by_cases h : s = 0 <;> simp [model, h]

private theorem legal_iff (q : Nat) (xs : List Bool) (hq : q < 2) :
    legal model q xs = decide ((q + xs.count true) % 2 = 0) := by
  rw [legal, run_eq q xs hq]
  simp [accept_eq]

private theorem count_true_le_length : ∀ xs : List Bool, xs.count true ≤ xs.length := by
  intro xs
  induction xs with
  | nil => simp
  | cons b bs ih => cases b <;> simp [ih] <;> omega

private theorem count_true_map_not : ∀ xs : List Bool,
    (xs.map (!·)).count true = xs.length - xs.count true := by
  intro xs
  induction xs with
  | nil => simp
  | cons b bs ih =>
    have hle := count_true_le_length bs
    cases b <;> simp [ih] <;> omega

private theorem legal_comp_alt (n q : Nat) (xs : List Bool) (hq : q < 2)
    (hlen : xs.length = n) (hlegal : legal model q xs) :
    legal model (alt n q) (xs.map (!·)) := by
  subst n
  have hleg : (q + xs.count true) % 2 = 0 := by
    simpa [legal_iff q xs hq] using hlegal
  have hle := count_true_le_length xs
  have hgoal : (alt xs.length q + (xs.length - xs.count true)) % 2 = 0 := by
    by_cases hn : xs.length % 2 = 0
    · have halt : alt xs.length q = q := by simp [alt, hn]
      rw [halt]
      omega
    · have hn1 : xs.length % 2 = 1 := by omega
      have halt : alt xs.length q = 1 - q := by simp [alt, hn]
      rw [halt]
      omega
  simpa [legal_iff (alt xs.length q) (xs.map (!·)) (alt_lt xs.length q hq),
    count_true_map_not] using hgoal

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases h : m.step q b with
  | none => simp [rank, h]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, h, hr]
    | some r => simp [rank, h, hr]

private theorem rank_model_cons (q : Nat) (b : Bool) (bs : List Bool) :
    rank model q (b :: bs) =
      (rank model ((q + b.toNat) % 2) bs).map
        (fun r => r + if b then lowCount model bs.length q else 0) := by
  rw [rank_cons]
  rfl

private theorem rank_some_of_legal : ∀ (q : Nat) (xs : List Bool),
    legal model q xs → ∃ r, rank model q xs = some r := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro h
    have hq0 : q = 0 := by simpa [legal, run, model] using h
    subst q
    exact ⟨0, by simp [rank, model]⟩
  | cons b bs ih =>
    intro h
    have h1 : legal model ((q + b.toNat) % 2) bs := by
      simpa [legal, run, model] using h
    rcases ih ((q + b.toNat) % 2) h1 with ⟨r, hr⟩
    refine ⟨r + if b then lowCount model bs.length q else 0, ?_⟩
    rw [rank_model_cons, hr]
    simp

private theorem count_succ_model (n q : Nat) (hq : q < 2) :
    count model (n + 1) q = count model n q + count model n (1 - q) := by
  have h1 : q % 2 = q := Nat.mod_eq_of_lt hq
  have h2 : (q + 1) % 2 = 1 - q := by omega
  simp [count, lowCount, model, h1, h2]

private theorem count_sym : ∀ n, 0 < n → count model n 0 = count model n 1 := by
  intro n hn
  induction n with
  | zero => omega
  | succ n ih =>
    cases n with
    | zero => decide
    | succ k =>
      rw [count_succ_model (k + 1) 0 (by omega),
        count_succ_model (k + 1) 1 (by omega), ih (by omega)]

private theorem count_flip (n q : Nat) (hn : 0 < n) (hq : q < 2) :
    count model n q = count model n (1 - q) := by
  rcases (show q = 0 ∨ q = 1 by omega) with rfl | rfl
  · simpa using count_sym n hn
  · simpa using (count_sym n hn).symm

private theorem count_pos_of_legal : ∀ (q : Nat) (xs : List Bool),
    q < 2 → legal model q xs → 0 < count model xs.length q := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro hq h
    have hq0 : q = 0 := by simpa [legal, run, model] using h
    subst q
    simp [count, model]
  | cons b bs ih =>
    intro hq h
    have h1 : legal model ((q + b.toNat) % 2) bs := by
      simpa [legal, run, model] using h
    have hpos := ih ((q + b.toNat) % 2) (by omega) h1
    rw [show (b :: bs).length = bs.length + 1 by rfl,
      count_succ_model bs.length q hq]
    have hq1eq : (q + b.toNat) % 2 = q ∨ (q + b.toNat) % 2 = 1 - q := by
      cases b <;> omega
    rcases hq1eq with h | h <;> rw [h] at hpos <;> omega

private theorem add_two_eq (k : Nat) : k + 1 + 1 = k + 2 := by omega

private theorem arith_final (rb rc c1 c0 ctot off1 off2 k : Nat)
    (hs : rb + rc = c1 - 1) (hp : 0 < c1) (ho : off1 + off2 = c0)
    (ht : ctot = c0 + c1) (hk : k = ctot) :
    (rb + off1) + (rc + off2) = k - 1 := by
  omega

private theorem main : ∀ (n q : Nat) (xs : List Bool), q < 2 → xs.length = n →
    legal model q xs →
    (rank model q xs).getD 0 + (rank model (alt n q) (xs.map (!·))).getD 0 =
      count model n q - 1 := by
  intro n
  induction n with
  | zero =>
    intro q xs hq hlen hlegal
    have hxs : xs = [] := by cases xs <;> simp_all
    subst xs
    have hq0 : q = 0 := by simpa [legal, run, model] using hlegal
    subst q
    simp [alt, rank, count, model]
  | succ n ih =>
    cases n with
    | zero =>
      intro q xs hq hlen hlegal
      rcases (show q = 0 ∨ q = 1 by omega) with rfl | rfl <;>
        cases xs with
        | nil => simp_all
        | cons b bs =>
          cases bs with
          | nil =>
            cases b <;> simp_all [legal, run, model, alt, rank, count, lowCount]
          | cons _ _ => simp_all
    | succ k =>
      intro q xs hq hlen hlegal
      cases xs with
      | nil => simp_all
      | cons b bs =>
        have hbslen : bs.length = k + 1 := by simp at hlen; omega
        let q1 : Nat := (q + b.toNat) % 2
        have hq1 : q1 < 2 := by dsimp [q1]; omega
        have hbslegal : legal model q1 bs := by
          simpa [legal, run, model, q1] using hlegal
        have hih := ih q1 bs hq1 hbslen hbslegal
        have hcompbslegal : legal model (alt (k + 1) q1) (bs.map (!·)) :=
          legal_comp_alt (k + 1) q1 bs hq1 hbslen hbslegal
        rcases rank_some_of_legal q1 bs hbslegal with ⟨rb, hrb⟩
        rcases rank_some_of_legal (alt (k + 1) q1) (bs.map (!·)) hcompbslegal with
          ⟨rc, hrc⟩
        have hsum : rb + rc = count model (k + 1) q1 - 1 := by
          simpa [hrb, hrc] using hih
        have hpos : 0 < count model (k + 1) q1 := by
          simpa [hbslen] using count_pos_of_legal q1 bs hq1 hbslegal
        have hrx : rank model q (b :: bs) =
            some (rb + if b then lowCount model bs.length q else 0) := by
          rw [rank_model_cons]
          change (rank model q1 bs).map
            (fun r => r + if b then lowCount model bs.length q else 0) = _
          rw [hrb]
          simp
        have hstate : alt (k + 2) q = (alt (k + 1) q1 + (1 - b.toNat)) % 2 := by
          dsimp [q1]
          simpa using alt_step (k + 1) q b hq
        have hstate2 : (alt (k + 2) q + (!b).toNat) % 2 = alt (k + 1) q1 := by
          have hbnot : (!b).toNat = 1 - b.toNat := by cases b <;> rfl
          rw [hstate, hbnot]
          have haltlt : alt (k + 1) q1 < 2 := alt_lt (k + 1) q1 hq1
          omega
        have hrcx : rank model (alt (k + 2) q) ((b :: bs).map (!·)) =
            some (rc + if (!b) then lowCount model bs.length (alt (k + 2) q) else 0) := by
          change rank model (alt (k + 2) q) ((!b) :: bs.map (!·)) = _
          rw [rank_model_cons, hstate2]
          simp only [List.length_map]
          change (rank model (alt (k + 1) q1) (bs.map (!·))).map
            (fun r => r + if (!b) then lowCount model bs.length (alt (k + 2) q) else 0) = _
          rw [hrc]
          simp
        have hlow1 : lowCount model bs.length q = count model bs.length q := by
          have hm : q % 2 = q := Nat.mod_eq_of_lt hq
          simp [lowCount, model, hm]
        have hlow2 : lowCount model bs.length (alt (k + 2) q) =
            count model bs.length (alt (k + 2) q) := by
          have hm : alt (k + 2) q % 2 = alt (k + 2) q :=
            Nat.mod_eq_of_lt (alt_lt (k + 2) q hq)
          simp [lowCount, model, hm]
        have halt : alt (k + 2) q = if bs.length % 2 = 0 then 1 - q else q := by
          have hk : k + 2 = bs.length + 1 := by omega
          rw [hk]
          simp [alt]
          split <;> split <;> omega
        have hcounts : count model bs.length q = count model bs.length (alt (k + 2) q) := by
          by_cases hp : bs.length % 2 = 0
          · simpa [halt, hp] using count_flip bs.length q (by omega) hq
          · simp [halt, hp]
        have hoff : (if b then lowCount model bs.length q else 0) +
            (if (!b) then lowCount model bs.length (alt (k + 2) q) else 0) =
              count model bs.length q := by
          cases b <;> simp [hlow1, hlow2, hcounts]
        have htotal : count model (k + 2) q =
            count model bs.length q + count model bs.length q1 := by
          have hk : k + 2 = bs.length + 1 := by omega
          rw [hk, count_succ_model bs.length q hq]
          dsimp [q1]
          cases b with
          | false =>
            have hm : q % 2 = q := Nat.mod_eq_of_lt hq
            simp [hm]
            rw [← count_flip bs.length q (by omega) hq]
          | true =>
            have hm : (q + 1) % 2 = 1 - q := by omega
            simp [hm]
        have hsum' : rb + rc = count model bs.length q1 - 1 := by
          simpa [hbslen] using hsum
        have hpos' : 0 < count model bs.length q1 := by
          simpa [hbslen] using hpos
        have hkw : count model (k + 1 + 1) q = count model (k + 2) q := by
          rw [add_two_eq]
        simp only [hrx, hrcx, Option.getD_some]
        exact arith_final rb rc (count model bs.length q1) (count model bs.length q)
          (count model (k + 2) q)
          (if b then lowCount model bs.length q else 0)
          (if (!b) then lowCount model bs.length (alt (k + 2) q) else 0)
          (count model (k + 1 + 1) q) hsum' hpos' hoff htotal hkw

theorem sound_ComplementOrder : ComplementOrder.Goal := by
  intro xs hlen hlegal
  have h := main xs.length 0 xs (by omega) rfl hlegal
  simpa [alt, hlen] using h

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_ComplementOrder
