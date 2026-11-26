import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_PublicationHoles

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.PublicationHoles

private theorem winner_lt (txs : List Tx) (snap k bound : Nat) (f : Nat → Bool)
    (hall : ∀ u ∈ txs, u.stamp < bound) :
    (winner txs snap k f).all (fun s => s < bound) = true := by
  unfold winner
  cases hfind : txs.find? (fun t => t.stamp ≤ snap && t.keys.contains k && (t.published || f t.stamp)) with
  | none => simp [hfind]
  | some v =>
      have hmem := List.mem_of_find?_eq_some hfind
      have hlt := hall v hmem
      simp [hfind, hlt]

private theorem key_equiv (txs : List Tx) (snap k : Nat)
    (hpair : txs.Pairwise fun a b => b.stamp < a.stamp) :
    (txs.all fun t => t.published || snap < t.stamp || !t.keys.contains k ||
        (winner txs snap k (fun _ => false)).any (t.stamp < ·)) = true ↔
      ∀ finishes, winner txs snap k finishes = winner txs snap k (fun _ => false) := by
  induction txs with
  | nil => simp [winner]
  | cons t rest ih =>
      have htail : rest.Pairwise (fun a b : Tx => b.stamp < a.stamp) := (List.pairwise_cons.mp hpair).2
      have hhead : ∀ u ∈ rest, u.stamp < t.stamp := (List.pairwise_cons.mp hpair).1
      by_cases hcand : t.stamp ≤ snap && k ∈ t.keys
      · by_cases hp : t.published = true
        · constructor
          · intro _
            intro f
            simp [winner, hcand, hp]
          · intro _
            rw [List.all_eq_true]
            intro u hu
            rcases List.mem_cons.mp hu with rfl | hu
            · simp [hcand, hp]
            · have hlt := hhead u hu
              have hwpub : winner (t :: rest) snap k (fun _ => false) = some t.stamp := by
                simp [winner, hcand, hp]
              rw [hwpub]
              simp [hlt]
        · have hwlt := winner_lt rest snap k t.stamp (fun _ => false) hhead
          have hpfalse : t.published = false := by cases h : t.published <;> simp [h] at hp ⊢
          have hc1 : t.stamp ≤ snap := by
            by_cases h1 : t.stamp ≤ snap
            · exact h1
            · have hzero : (decide (t.stamp ≤ snap) && decide (k ∈ t.keys)) = false := by simp [h1]
              simp [hzero] at hcand
          have hc2 : k ∈ t.keys := by
            by_cases h2 : k ∈ t.keys
            · exact h2
            · have hzero : (decide (t.stamp ≤ snap) && decide (k ∈ t.keys)) = false := by simp [hc1, h2]
              simp [hzero] at hcand
          constructor
          · intro hcert
            rw [List.all_eq_true] at hcert
            have hbad := hcert t (by simp)
            have hwfalse : winner (t :: rest) snap k (fun _ => false) = winner rest snap k (fun _ => false) := by
              simp [winner, hcand, hpfalse]
            have hnval : (winner (t :: rest) snap k (fun _ => false)).any (fun s => t.stamp < s) = false := by
              rw [hwfalse]
              cases hf : winner rest snap k (fun _ => false) with
              | none => simp [hf]
              | some s =>
                  have hslt : s < t.stamp := by
                    exact by simpa using (Option.all_eq_true (fun s : Nat => s < t.stamp) (winner rest snap k (fun _ => false))).mp hwlt s (by simp [hf])
                  have hnlt : ¬ t.stamp < s := Nat.not_lt.mpr (Nat.le_of_lt hslt)
                  simp [hf, hnlt]
            have hnle : ¬ snap < t.stamp := Nat.not_lt.mpr hc1
            have hcont : t.keys.contains k = true := by simpa [hc2]
            have hnleBool : (snap < t.stamp) = false := by simpa [hnle]
            simp [hpfalse, hcont, hnval, hnleBool] at hbad
            exact False.elim (hbad hc2)
          · intro h
            exfalso
            have heq := h (fun _ => true)
            have hl : winner (t :: rest) snap k (fun _ => true) = some t.stamp := by
              simp [winner, hcand, hpfalse]
            have hr : winner (t :: rest) snap k (fun _ => false) = winner rest snap k (fun _ => false) := by
              simp [winner, hcand, hpfalse]
            rw [hl, hr] at heq
            cases hw : winner rest snap k (fun _ => false) with
            | none => simp [hw] at heq
            | some s =>
                have hslt : s < t.stamp := by simpa using (Option.all_eq_true (fun s : Nat => s < t.stamp) (winner rest snap k (fun _ => false))).mp hwlt s (by simp [hw])
                rw [hw] at heq
                injection heq with heq'
                omega
      · have hskip (f : Nat → Bool) : winner (t :: rest) snap k f = winner rest snap k f := by
          by_cases hts : t.stamp ≤ snap
          · have hkmem : k ∉ t.keys := by
              intro hk
              apply hcand
              simp [hts, hk]
            simp [winner, hts, hkmem]
          · simp [winner, hts]
        constructor
        · intro hcert
          rw [List.all_eq_true] at hcert
          have hrest : (rest.all fun u => u.published || snap < u.stamp || !u.keys.contains k ||
              (winner rest snap k (fun _ => false)).any (u.stamp < ·)) = true := by
            rw [List.all_eq_true]
            intro u hu
            have hcond := hcert u (by simp [hu])
            have hskipF := hskip (fun _ => false)
            rw [hskipF] at hcond
            exact hcond
          intro f
          have hf' := (ih htail).mp hrest f
          calc
            winner (t :: rest) snap k f = winner rest snap k f := hskip f
            _ = winner rest snap k (fun _ => false) := hf'
            _ = winner (t :: rest) snap k (fun _ => false) := (hskip (fun _ => false)).symm
        · intro heq
          have hheadcond : (t.published || snap < t.stamp || !t.keys.contains k ||
              (winner (t :: rest) snap k (fun _ => false)).any (t.stamp < ·)) = true := by
            by_cases hts : t.stamp ≤ snap
            · have hkmem : k ∉ t.keys := by
                intro hk
                have hbool : (decide (t.stamp ≤ snap) && decide (k ∈ t.keys)) = true := by simp [hts, hk]
                exact hcand hbool
              have hnotmem : t.keys.contains k = false := by simpa [hkmem]
              simp [hnotmem, hkmem]
            · simp [show snap < t.stamp by omega]
          rw [List.all_eq_true]
          intro u hu
          rcases List.mem_cons.mp hu with hEq | hu
          · subst u
            exact hheadcond
          · have hrest_eq : ∀ f, winner rest snap k f = winner rest snap k (fun _ => false) := by
              intro f
              have hf' := heq f
              rw [hskip f, hskip (fun _ => false)] at hf'
              exact hf'
            have hrestcert := (ih htail).mpr hrest_eq
            have hcond := (List.all_eq_true.mp hrestcert) u hu
            have hskipF := hskip (fun _ => false)
            rw [hskipF]
            exact hcond

theorem sound : Goal := by
  intro txs snap keys hpair
  rw [certified]
  simp only [List.all_eq_true]
  constructor
  · intro hcert finishes k hk
    have h := (List.all_eq_true.mpr (hcert k hk))
    exact (key_equiv txs snap k hpair).mp h finishes
  · intro h k hk
    exact List.all_eq_true.mp ((key_equiv txs snap k hpair).mpr (fun finishes => h finishes k hk))

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_PublicationHoles
