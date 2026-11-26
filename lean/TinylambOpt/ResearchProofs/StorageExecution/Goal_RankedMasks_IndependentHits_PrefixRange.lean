import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `IndependentHits.PrefixRange` — Accepted completions of a fixed bitmap prefix occupy
  a contiguous interval of ranks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_PrefixRange

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

/-- For every rank below the total count there is a bitmap of the expected
    length that `unrank` returns. -/
private theorem unrank_some_eq (m : Machine) : ∀ n q r, r < count m n q →
    ∃ bs, unrank m n q r = some bs ∧ bs.length = n := by
  intro n
  induction n with
  | zero =>
      intro q r h
      simp [count] at h
      by_cases ha : m.accept q
      · simp [ha] at h
        have hr0 : r = 0 := by omega
        refine ⟨[], ?_, rfl⟩
        simp [unrank, ha, hr0]
      · simp [ha] at h
  | succ n ih =>
      intro q r h
      by_cases hle : lowCount m n q ≤ r
      · have hsp : ∃ q2, m.step q true = some q2 := by
          cases hq : m.step q true with
          | none =>
              have h' : r < lowCount m n q := by simpa [count, lowCount, hq] using h
              omega
          | some q2 => exact ⟨q2, rfl⟩
        rcases hsp with ⟨q2, hq2⟩
        have hhigh : r - lowCount m n q < count m n q2 := by
          have h' : r < lowCount m n q + count m n q2 := by simpa [count, lowCount, hq2] using h
          omega
        rcases ih q2 (r - lowCount m n q) hhigh with ⟨bs, hbs, hlen⟩
        refine ⟨true :: bs, ?_, ?_⟩
        · simp [unrank, hle, hq2, hbs]
        · simp [hlen]
      · have hlt : r < lowCount m n q := by omega
        have hsp : ∃ q1, m.step q false = some q1 := by
          cases hq : m.step q false with
          | none =>
              have hz : lowCount m n q = 0 := by simp [lowCount, hq]
              omega
          | some q1 => exact ⟨q1, rfl⟩
        rcases hsp with ⟨q1, hq1⟩
        have hlow : r < count m n q1 := by simpa [lowCount, hq1] using hlt
        rcases ih q1 r hlow with ⟨bs, hbs, hlen⟩
        refine ⟨false :: bs, ?_, ?_⟩
        · simp [unrank, hle, hq1, hbs]
        · simp [hlen]

/-- The rank interval of a prefix stays inside the interval of its parent. -/
private theorem descend_bounded (m : Machine) : ∀ n q pre q' off,
    descend m n q pre = some (q', off) → off + count m (n - pre.length) q' ≤ count m n q := by
  intro n
  induction n with
  | zero =>
      intro q pre q' off h
      cases pre with
      | nil =>
          simp [descend] at h
          rcases h with ⟨rfl, rfl⟩
          simp
      | cons b bs => simp [descend] at h
  | succ n ih =>
      intro q pre q' off h
      cases pre with
      | nil =>
          simp [descend] at h
          rcases h with ⟨rfl, rfl⟩
          simp
      | cons b bs =>
          simp only [descend, List.length_cons, Nat.succ_sub_succ_eq_sub] at h ⊢
          obtain ⟨q1, hstep, h2⟩ := Option.bind_eq_some_iff.mp h
          obtain ⟨p, hdesc, hp⟩ := Option.bind_eq_some_iff.mp h2
          have hp' : (p.1, p.2 + (if b then lowCount m n q else 0)) = (q', off) := by simpa using hp
          have hq' : p.1 = q' := congrArg Prod.fst hp'
          have ho : p.2 + (if b then lowCount m n q else 0) = off := congrArg Prod.snd hp'
          have ih' := ih q1 bs p.1 p.2 hdesc
          cases hb : b with
          | false =>
              subst b
              simp at ho
              have hcnt : count m n q1 = lowCount m n q := by simp [lowCount, hstep]
              have htot : count m (n + 1) q = lowCount m n q + ((m.step q true).map (count m n)).getD 0 := by simp [count, lowCount]
              subst off
              subst q'
              omega
          | true =>
              subst b
              simp at ho
              have hcnt : count m n q1 = ((m.step q true).map (count m n)).getD 0 := by simp [hstep]
              have htot : count m (n + 1) q = lowCount m n q + ((m.step q true).map (count m n)).getD 0 := by simp [count, lowCount]
              subst off
              subst q'
              omega

/-- The combinatorial rank interval of any fixed prefix is contiguous.
    This holds for every machine whose `count`, `unrank` and `descend` are
    defined as in `RankedMasks`, so it applies in particular to the
    independent-hits automaton. -/
private theorem prefixRange_gen (m : Machine) : prefixRange m := by
  intro n
  induction n with
  | zero =>
      intro q pre q' off r hdesc hr
      cases pre with
      | nil =>
          simp [descend] at hdesc
          rcases hdesc with ⟨rfl, rfl⟩
          rcases unrank_some_eq m 0 q r hr with ⟨bs, hbs, hlen⟩
          simp [hbs, hr]
      | cons b bs => simp [descend] at hdesc
  | succ n ih =>
      intro q pre q' off r hdesc hr
      cases pre with
      | nil =>
          simp [descend] at hdesc
          rcases hdesc with ⟨rfl, rfl⟩
          rcases unrank_some_eq m (n + 1) q r hr with ⟨bs, hbs, hlen⟩
          simp [hbs, hr]
      | cons b bs =>
          simp only [descend, List.length_cons, Nat.succ_sub_succ_eq_sub] at hdesc ⊢
          obtain ⟨q1, hstep, h2⟩ := Option.bind_eq_some_iff.mp hdesc
          obtain ⟨p, hpd, hp⟩ := Option.bind_eq_some_iff.mp h2
          have hp' : (p.1, p.2 + (if b then lowCount m n q else 0)) = (q', off) := by simpa using hp
          have hq' : p.1 = q' := congrArg Prod.fst hp'
          have ho : p.2 + (if b then lowCount m n q else 0) = off := congrArg Prod.snd hp'
          cases hb : b with
          | false =>
              subst b
              simp at ho
              by_cases hle : lowCount m n q ≤ r
              · -- the decoded bitmap starts with `true`, so `false :: bs` is not a prefix
                have hsp : ∃ q2, m.step q true = some q2 := by
                  cases ht : m.step q true with
                  | none =>
                      have h' : r < lowCount m n q := by simpa [count, lowCount, ht] using hr
                      omega
                  | some q2 => exact ⟨q2, rfl⟩
                rcases hsp with ⟨q2, hq2⟩
                have hhigh : r - lowCount m n q < count m n q2 := by
                  have h' : r < lowCount m n q + count m n q2 := by simpa [count, lowCount, hq2] using hr
                  omega
                rcases unrank_some_eq m n q2 (r - lowCount m n q) hhigh with ⟨bs', hbs', hlen'⟩
                have hbnd := descend_bounded m n q1 bs p.1 p.2 hpd
                have hlc : count m n q1 = lowCount m n q := by simp [lowCount, hstep]
                have hnot : ¬ (p.2 ≤ r ∧ r < p.2 + count m (n - bs.length) p.1) := by omega
                subst off
                subst q'
                simp [unrank, hle, hq2, hbs', List.isPrefixOf, hnot]
              · -- both the prefix and the decoded bitmap take the `false` branch
                have hr' : r < count m n q1 := by simpa [lowCount, hstep] using (show r < lowCount m n q by omega)
                rcases unrank_some_eq m n q1 r hr' with ⟨bs', hbs', hlen'⟩
                have ihres := ih q1 bs p.1 p.2 r hpd hr'
                simp [hbs'] at ihres
                subst off
                subst q'
                simp [unrank, hle, hstep, hbs', List.isPrefixOf]
                exact ihres
          | true =>
              subst b
              simp at ho
              by_cases hle : lowCount m n q ≤ r
              · -- both the prefix and the decoded bitmap take the `true` branch;
                -- the rank shifts by the size of the `false` branch
                have hr' : r - lowCount m n q < count m n q1 := by
                  have h' : r < lowCount m n q + count m n q1 := by simpa [count, lowCount, hstep] using hr
                  omega
                rcases unrank_some_eq m n q1 (r - lowCount m n q) hr' with ⟨bs', hbs', hlen'⟩
                have ihres := ih q1 bs p.1 p.2 (r - lowCount m n q) hpd hr'
                simp [hbs'] at ihres
                have hA : (p.2 + lowCount m n q ≤ r) ↔ (p.2 ≤ r - lowCount m n q) := by omega
                have hB : (r < p.2 + lowCount m n q + count m (n - bs.length) p.1) ↔
                    (r - lowCount m n q < p.2 + count m (n - bs.length) p.1) := by omega
                subst off
                subst q'
                simpa [unrank, hle, hstep, hbs', List.isPrefixOf, hA, hB] using ihres
              · -- the decoded bitmap starts with `false`, so `true :: bs` is not a prefix
                have hlt : r < lowCount m n q := by omega
                have hsp : ∃ q2, m.step q false = some q2 := by
                  cases hf : m.step q false with
                  | none =>
                      have hz : lowCount m n q = 0 := by simp [lowCount, hf]
                      omega
                  | some q2 => exact ⟨q2, rfl⟩
                rcases hsp with ⟨q2, hq2⟩
                have hr' : r < count m n q2 := by simpa [lowCount, hq2] using hlt
                rcases unrank_some_eq m n q2 r hr' with ⟨bs', hbs', hlen'⟩
                have hnot : ¬ (p.2 + lowCount m n q ≤ r ∧
                    r < p.2 + lowCount m n q + count m (n - bs.length) p.1) := by omega
                subst off
                subst q'
                simp [unrank, hle, hq2, hbs', List.isPrefixOf, hnot]

theorem sound_PrefixRange : TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.PrefixRange.Goal := by
  exact prefixRange_gen model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_PrefixRange
