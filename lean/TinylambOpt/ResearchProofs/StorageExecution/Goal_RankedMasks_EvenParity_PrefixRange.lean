import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_PrefixRange

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.EvenParity

-- Truth check on small inputs (parity states 0/1, all prefixes up to length 6).
private def bits : Nat → List (List Bool)
  | 0 => [[]]
  | n + 1 => (bits n).map (false :: ·) ++ (bits n).map (true :: ·)

private def checkOne (n q : Nat) (pre : List Bool) (r : Nat) : Bool :=
  match descend model n q pre with
  | none => true
  | some (q', off) =>
      if r < count model n q then
        decide (((unrank model n q r).any (fun bs => pre.isPrefixOf bs)) =
          decide (off ≤ r ∧ r < off + count model (n - pre.length) q'))
      else true

private def check : Bool :=
  (List.range 7).all fun n =>
    (List.range 2).all fun q =>
      (bits n).all fun pre =>
        (List.range (count model n q + 2)).all fun r =>
          checkOne n q pre r

#eval check

private theorem filter_false {α : Type} (xs : List α) :
    xs.filter (fun _ => false) = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.filter_cons, ih]

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem unrank_total (m : Machine) : ∀ (n q r : Nat),
    r < count m n q → ∃ xs, unrank m n q r = some xs
  | 0, q, r, h => by
    cases ha : m.accept q with
    | false => simp [count, ha] at h
    | true =>
      simp [count, ha] at h
      have hr : r = 0 := by omega
      subst r
      exact ⟨[], by simp [unrank, ha]⟩
  | n + 1, q, r, h => by
    rw [count_succ] at h
    by_cases hl : r < lowCount m n q
    · cases hs : m.step q false with
      | none => simp [lowCount, hs, filter_false] at hl
      | some qf =>
        obtain ⟨bs, hbs⟩ := unrank_total m n qf r (by simpa [lowCount, hs] using hl)
        have hb : decide (lowCount m n q ≤ r) = false := by simp [hl]
        exact ⟨false :: bs, by simp [unrank, hb, hs, hbs]⟩
    · have hle : lowCount m n q ≤ r := by omega
      cases hs : m.step q true with
      | none => simp [hiCount, hs] at h; omega
      | some qt =>
        simp [hiCount, hs] at h
        obtain ⟨bs, hbs⟩ := unrank_total m n qt (r - lowCount m n q) (by omega)
        exact ⟨true :: bs, by simp [unrank, hle, hs, hbs]⟩

private theorem unrank_any_cons (m : Machine) (n q r : Nat) (b : Bool) (bs : List Bool) :
    Option.any (fun xs => (b :: bs).isPrefixOf xs) (unrank m (n + 1) q r) =
      if decide (lowCount m n q ≤ r) then
        (if b then ((m.step q true).map (fun q' =>
          Option.any (fun xs => bs.isPrefixOf xs)
            (unrank m n q' (r - lowCount m n q)))).getD false else false)
      else
        (if b then false else ((m.step q false).map (fun q' =>
          Option.any (fun xs => bs.isPrefixOf xs) (unrank m n q' r))).getD false) := by
  cases b with
  | false =>
    by_cases hle : lowCount m n q ≤ r
    · have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
      cases hs : m.step q true with
      | none => simp [unrank, hd, hs]
      | some qt =>
        cases hu : unrank m n qt (r - lowCount m n q) with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
    · have hd : decide (lowCount m n q ≤ r) = false := by simp [hle]
      cases hs : m.step q false with
      | none => simp [unrank, hd, hs]
      | some qf =>
        cases hu : unrank m n qf r with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
  | true =>
    by_cases hle : lowCount m n q ≤ r
    · have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
      cases hs : m.step q true with
      | none => simp [unrank, hd, hs]
      | some qt =>
        cases hu : unrank m n qt (r - lowCount m n q) with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
    · have hd : decide (lowCount m n q ≤ r) = false := by simp [hle]
      cases hs : m.step q false with
      | none => simp [unrank, hd, hs]
      | some qf =>
        cases hu : unrank m n qf r with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]

private theorem descend_bound (m : Machine) : ∀ (pre : List Bool) (n q q' off : Nat),
    descend m n q pre = some (q', off) →
    pre.length ≤ n ∧ off + count m (n - pre.length) q' ≤ count m n q
  | [], n, q, q', off, h => by
    simp [descend] at h
    rcases h with ⟨rfl, rfl⟩
    simp
  | b :: bs, 0, q, q', off, h => by simp [descend] at h
  | b :: bs, n + 1, q, q', off, h => by
    simp only [descend] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q1 =>
      simp only [hs, Option.bind_some] at h
      cases hd : descend m n q1 bs with
      | none => simp [hd] at h
      | some p =>
        rcases p with ⟨last, off1⟩
        simp [hd] at h
        rcases h with ⟨rfl, rfl⟩
        have ih := descend_bound m bs n q1 last off1 hd
        rcases ih with ⟨hlen, hbound⟩
        constructor
        · simp; omega
        · have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp
          rw [hsub, count_succ]
          cases b with
          | false => simp [lowCount, hs] at hbound ⊢; omega
          | true => simp [lowCount, hiCount, hs] at hbound ⊢; omega

private theorem prefixRange_generic (m : Machine) : prefixRange m := by
  intro n q pre q' off r hdesc hr
  induction pre generalizing n q q' off r with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    obtain ⟨xs, hx⟩ := unrank_total m n q r hr
    simp [hx, hr, List.isPrefixOf]
  | cons b bs ih =>
    cases n with
    | zero => simp [descend] at hdesc
    | succ n =>
      simp only [descend] at hdesc
      cases hs : m.step q b with
      | none => simp [hs] at hdesc
      | some q1 =>
        simp only [hs, Option.bind_some] at hdesc
        cases hd : descend m n q1 bs with
        | none => simp [hd] at hdesc
        | some p =>
          rcases p with ⟨last, off1⟩
          simp [hd] at hdesc
          have hdb := descend_bound m bs n q1 last off1 hd
          have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp
          rw [count_succ] at hr
          rw [hsub]
          cases b with
          | false =>
            simp only [Bool.false_eq_true, ite_false, Nat.add_zero] at hdesc
            rcases hdesc with ⟨rfl, rfl⟩
            by_cases hl : r < lowCount m n q
            · have hddec : decide (lowCount m n q ≤ r) = false := by simp [hl]
              rw [unrank_any_cons]
              simp only [hddec, Bool.false_eq_true, ite_false, hs,
                Option.map_some, Option.getD_some]
              have hr' : r < count m n q1 := by
                simpa [lowCount, hs] using hl
              exact ih n q1 last off1 r hd hr'
            · have hle : lowCount m n q ≤ r := by omega
              have hout : ¬ (off1 ≤ r ∧ r < off1 + count m (n - bs.length) last) := by
                have hb := hdb.2
                have hlow : lowCount m n q = count m n q1 := by simp [lowCount, hs]
                omega
              rw [unrank_any_cons]
              simp [hle, hout]
          | true =>
            simp only [ite_true] at hdesc
            rcases hdesc with ⟨rfl, rfl⟩
            by_cases hle : lowCount m n q ≤ r
            · have hddec : decide (lowCount m n q ≤ r) = true := by simp [hle]
              rw [unrank_any_cons]
              simp only [hddec, ite_true, hs, Option.map_some, Option.getD_some]
              have hr' : r - lowCount m n q < count m n q1 := by
                simp [hiCount, hs] at hr
                omega
              have hcond :
                  (off1 + lowCount m n q ≤ r ∧
                    r < off1 + lowCount m n q + count m (n - bs.length) last) ↔
                  (off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                constructor <;> rintro ⟨h1, h2⟩ <;> constructor <;> omega
              have hdec : decide
                  (off1 + lowCount m n q ≤ r ∧
                    r < off1 + lowCount m n q + count m (n - bs.length) last) =
                  decide
                  (off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                by_cases hh : off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last
                · have hh2 := hcond.mpr hh
                  simp [hh, hh2]
                · have hh2 : ¬ (off1 + lowCount m n q ≤ r ∧
                      r < off1 + lowCount m n q + count m (n - bs.length) last) := by
                    intro hc
                    exact hh (hcond.mp hc)
                  simp [hh, hh2]
              rw [hdec]
              exact ih n q1 last off1 (r - lowCount m n q) hd hr'
            · have hl : r < lowCount m n q := by omega
              have hout : ¬ (off1 + lowCount m n q ≤ r ∧
                  r < off1 + lowCount m n q + count m (n - bs.length) last) := by omega
              rw [unrank_any_cons]
              have hddec : decide (lowCount m n q ≤ r) = false := by simp [hl]
              simp [hddec, hout]

theorem sound_PrefixRange : EvenParity.PrefixRange.Goal := by
  exact prefixRange_generic model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_EvenParity_PrefixRange
