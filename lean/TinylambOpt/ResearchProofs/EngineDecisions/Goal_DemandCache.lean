import TinylambOpt.ResearchGoals.EngineDecisions.Executor
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Ex_DemandCache

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.DemandCache

private def demanded (test : Nat → Verdict) : Nat → Nat → List Nat → List Nat
  | 0, _, _ => []
  | _+1, _, [] => []
  | n+1, i, k::ks =>
      k :: match test k with
      | .error _ => []
      | .ok false => demanded test (n+1) (i+1) ks
      | .ok true => demanded test n (i+1) ks

private theorem lookup_correct_cons {test : Nat → Verdict} {memo : List (Nat × Verdict)} {k : Nat}
    (hcorrect : ∀ x v, memo.lookup x = some v → v = test x) :
    ∀ x v, ((k, test k) :: memo).lookup x = some v → v = test x := by
  intro x v h
  by_cases hx : x = k
  · subst x
    have h' : test k = v := by simpa using h
    exact h'.symm
  · by_cases hbeq : x == k
    · have hxk : x = k := beq_iff_eq.mp hbeq
      exact False.elim (hx hxk)
    · have h' : memo.lookup x = some v := by simpa [List.lookup_cons, hbeq] using h
      exact hcorrect x v h'

private theorem cached_eq_take (test : Nat → Verdict) :
    ∀ (memo : List (Nat × Verdict)),
      (∀ k v, memo.lookup k = some v → v = test k) →
      ∀ n i ks, (cached test memo n i ks).1 = takePassing test n i ks := by
  intro memo hcorrect n i ks
  induction ks generalizing n i memo with
  | nil =>
      cases n <;> simp [cached, takePassing]
  | cons k ks ih =>
      cases n with
      | zero =>
          simp [cached, takePassing]
      | succ n =>
          cases hlk : memo.lookup k with
          | none =>
              simp [cached, takePassing, hlk]
              cases htest : test k with
              | error e => simp [htest]
              | ok pass =>
                  by_cases hp : pass = true
                  · subst pass
                    have htail := ih ((k, Except.ok true)::memo)
                      (by simpa [htest] using (lookup_correct_cons (test := test) (memo := memo) (k := k) hcorrect)) n (i+1)
                    have hem := congrArg (fun r : Result => r.emitted) htail
                    have hst := congrArg (fun r : Result => r.stop) htail
                    simpa [htest] using And.intro hem hst
                  · have hp' : pass = false := by cases pass <;> simp_all
                    subst pass
                    have htail := ih ((k, Except.ok false)::memo)
                      (by simpa [htest] using (lookup_correct_cons (test := test) (memo := memo) (k := k) hcorrect)) (n+1) (i+1)
                    simpa [htest] using htail
          | some v =>
              have hv := hcorrect k v hlk
              subst v
              simp [cached, takePassing, hlk]
              cases htest : test k with
              | error e => simp [htest]
              | ok pass =>
                  by_cases hp : pass = true
                  · subst pass
                    have htail := ih memo hcorrect n (i+1)
                    have hem := congrArg (fun r : Result => r.emitted) htail
                    have hst := congrArg (fun r : Result => r.stop) htail
                    simpa [htest] using And.intro hem hst
                  · have hp' : pass = false := by cases pass <;> simp_all
                    subst pass
                    have htail := ih memo hcorrect (n+1) (i+1)
                    simpa [htest] using htail

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · have h' := List.nodup_cons.mp h
        have hnot : ¬ a ∈ List.filter p as := by
          intro hm
          exact h'.1 (List.mem_filter.mp hm).1
        simpa [List.filter_cons_of_pos hp, List.nodup_cons] using And.intro hnot (ih h'.2)
      · simpa [List.filter_cons_of_neg hp] using ih (List.nodup_cons.mp h).2

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        rw [List.mem_eraseDups] at ha
        simp at ha
      · exact nodup_eraseDups (as.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => ! b == a) as
  omega

private theorem not_mem_erase_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.1
      · have hne : a ≠ b := by
          intro h
          subst b
          simp at hba
        have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba]
        intro hmem
        rcases List.mem_cons.mp hmem with hab | hmem
        · exact hne hab
        · exact ih hn'.2 hmem

private theorem nodup_erase {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : (l.erase a).Nodup := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.2
      · have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba, List.nodup_cons]
        constructor
        · intro hbmem
          exact hn'.1 (List.mem_of_mem_erase hbmem)
        · exact ih hn'.2

private theorem nodup_perm_ext {α : Type} [BEq α] [LawfulBEq α] (l₁ l₂ : List α)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hl2 : l₂ = [] := by
        cases l₂ with
        | nil => rfl
        | cons b bs =>
            have : b ∈ b :: bs := by simp
            have : b ∈ ([] : List α) := (hmem b).mpr this
            simp at this
      subst l₂
      exact List.Perm.nil
  | cons a as ih =>
      rw [List.cons_perm_iff_perm_erase]
      constructor
      · exact (hmem a).mp (by simp)
      · have hnd' := List.nodup_cons.mp hnd₁
        apply ih (l₂.erase a) hnd'.2
        · exact nodup_erase hnd₂
        · intro x
          constructor
          · intro hx
            have hxa : x ≠ a := by
              intro h; subst x; exact hnd'.1 hx
            have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx])
            simpa [List.mem_erase_of_ne hxa] using hx2
          · intro hx
            have hxa : x ≠ a := by
              intro h; subst x
              exact not_mem_erase_self_of_nodup hnd₂ hx
            have hx2 : x ∈ l₂ := List.mem_of_mem_erase hx
            have hx1 : x ∈ a :: as := (hmem x).mpr hx2
            rcases List.mem_cons.mp hx1 with rfl | hxas
            · contradiction
            · exact hxas

private theorem eraseDups_cons_filter_ne {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (L : List α) (p : α → Bool) :
    (k :: L.filter p).eraseDups.length =
      1 + ((L.filter p).filter (fun x => ! x == k)).eraseDups.length := by
  have hperm : (k :: L.filter p).eraseDups.Perm
      (k :: (L.filter p).filter (fun x => ! x == k)).eraseDups := by
    apply nodup_perm_ext (k :: L.filter p).eraseDups
      (k :: (L.filter p).filter (fun x => ! x == k)).eraseDups
      (nodup_eraseDups (k :: L.filter p))
      (nodup_eraseDups (k :: (L.filter p).filter (fun x => ! x == k)))
    intro x
    rw [List.mem_eraseDups, List.mem_eraseDups]
    simp only [List.mem_cons, List.mem_filter]
    constructor
    · intro hx
      rcases hx with hxk | hxmem
      · subst x
        simp
      · by_cases hxeq : x = k
        · subst x
          simp
        · have hbeq : (! x == k) = true := by
            have : (x == k) = false := by
              cases h : x == k
              · rfl
              · have : x = k := beq_iff_eq.mp h
                exact False.elim (hxeq this)
            simpa using this
          right
          exact ⟨hxmem, hbeq⟩
    · intro hx
      rcases hx with hxk | hxmem
      · subst x
        simp
      · exact Or.inr hxmem.1
  have hlen := hperm.length_eq
  simp [List.eraseDups_cons] at hlen ⊢
  omega

private theorem filter_and {α : Type} (p q : α → Bool) (l : List α) :
    l.filter (fun x => p x && q x) = (l.filter p).filter q := by
  rw [show (fun x => p x && q x) = (fun x => q x && p x) by
    funext x
    cases p x <;> cases q x <;> rfl]
  exact (List.filter_filter (p := q) (q := p) (l := l)).symm

private theorem lookup_none_cons_bool {memo : List (Nat × Verdict)} {k x : Nat} {v : Verdict} :
    (decide (((k, v) :: memo).lookup x = none)) = true ↔
      (decide (memo.lookup x = none)) = true ∧ (! (x == k)) = true := by
  by_cases hxk : x = k
  · subst x
    simp [List.lookup_cons]
  · by_cases hbeq : x == k
    · have hx : x = k := beq_iff_eq.mp hbeq
      exact False.elim (hxk hx)
    · have hbeqfalse : (x == k) = false := by cases h : x == k <;> simp_all
      rw [List.lookup_cons, hbeqfalse]
      simp [hxk]

private theorem cached_work_eq_demanded (test : Nat → Verdict) :
    ∀ (memo : List (Nat × Verdict)),
      (∀ k v, memo.lookup k = some v → v = test k) →
      ∀ n i ks, (cached test memo n i ks).2 =
        ((demanded test n i ks).filter (fun k => decide (memo.lookup k = none))).eraseDups.length := by
  intro memo hcorrect n i ks
  induction ks generalizing memo n i with
  | nil =>
      cases n <;> simp [cached, demanded]
  | cons k ks ih =>
      cases n with
      | zero =>
          simp [cached, demanded]
      | succ n =>
          cases hlk : memo.lookup k with
          | none =>
              cases htest : test k with
              | error e =>
                  simp [cached, demanded, hlk, htest, List.eraseDups_cons]
              | ok pass =>
                  by_cases hp : pass = true
                  · subst pass
                    let memo' := (k, Except.ok true) :: memo
                    have hcorrect' : ∀ x v, memo'.lookup x = some v → v = test x := by
                      dsimp [memo']
                      simpa [htest] using (lookup_correct_cons (test := test) (memo := memo) (k := k) hcorrect)
                    have htail := ih memo' hcorrect' n (i+1)
                    have hpred : (fun x => decide (memo'.lookup x = none)) =
                        (fun x => (decide (memo.lookup x = none)) && (! (x == k))) := by
                      funext x
                      apply Bool.eq_iff_iff.mpr
                      by_cases hxk : x = k
                      · subst x
                        simp [memo']
                      · by_cases hbeq : x == k
                        · have : x = k := beq_iff_eq.mp hbeq
                          exact False.elim (hxk this)
                        · have hbeqfalse : (x == k) = false := by cases h : x == k <;> simp_all
                          dsimp [memo']
                          rw [List.lookup_cons, hbeqfalse]
                          simp [hxk]
                    have hfilt : (demanded test n (i+1) ks).filter (fun x => decide (memo'.lookup x = none)) =
                        ((demanded test n (i+1) ks).filter (fun x => decide (memo.lookup x = none))).filter
                          (fun x => ! (x == k)) := by
                      rw [hpred]
                      exact filter_and _ _ _
                    dsimp [memo'] at htail
                    rw [hfilt] at htail
                    have hrew := eraseDups_cons_filter_ne k (demanded test n (i+1) ks)
                      (fun x => decide (memo.lookup x = none))
                    simp [cached, hlk, htest, demanded, htail, hrew]
                  · have hp' : pass = false := by cases pass <;> simp_all
                    subst pass
                    let memo' := (k, Except.ok false) :: memo
                    have hcorrect' : ∀ x v, memo'.lookup x = some v → v = test x := by
                      dsimp [memo']
                      simpa [htest] using (lookup_correct_cons (test := test) (memo := memo) (k := k) hcorrect)
                    have htail := ih memo' hcorrect' (n+1) (i+1)
                    have hpred : (fun x => decide (memo'.lookup x = none)) =
                        (fun x => (decide (memo.lookup x = none)) && (! (x == k))) := by
                      funext x
                      apply Bool.eq_iff_iff.mpr
                      by_cases hxk : x = k
                      · subst x
                        simp [memo']
                      · by_cases hbeq : x == k
                        · have : x = k := beq_iff_eq.mp hbeq
                          exact False.elim (hxk this)
                        · have hbeqfalse : (x == k) = false := by cases h : x == k <;> simp_all
                          dsimp [memo']
                          rw [List.lookup_cons, hbeqfalse]
                          simp [hxk]
                    have hfilt : (demanded test (n+1) (i+1) ks).filter (fun x => decide (memo'.lookup x = none)) =
                        ((demanded test (n+1) (i+1) ks).filter (fun x => decide (memo.lookup x = none))).filter
                          (fun x => ! (x == k)) := by
                      rw [hpred]
                      exact filter_and _ _ _
                    dsimp [memo'] at htail
                    rw [hfilt] at htail
                    have hrew := eraseDups_cons_filter_ne k (demanded test (n+1) (i+1) ks)
                      (fun x => decide (memo.lookup x = none))
                    simp [cached, hlk, htest, demanded, htail, hrew]
          | some v =>
              have hv := hcorrect k v hlk
              subst v
              cases htest : test k with
              | error e =>
                  simp [cached, demanded, hlk, htest]
              | ok pass =>
                  by_cases hp : pass = true
                  · subst pass
                    have htail := ih memo hcorrect n (i+1)
                    have hkstale : (decide (memo.lookup k = none)) = false := by simp [hlk]
                    simp [cached, hlk, htest, demanded, htail, hkstale]
                  · have hp' : pass = false := by cases pass <;> simp_all
                    subst pass
                    have htail := ih memo hcorrect (n+1) (i+1)
                    have hkstale : (decide (memo.lookup k = none)) = false := by simp [hlk]
                    simp [cached, hlk, htest, demanded, htail, hkstale]



private def bumpStop : Stop → Stop
  | .fault j e => .fault (j+1) e
  | s => s

private theorem bumpStop_eq_enough (s : Stop) : bumpStop s = .enough ↔ s = .enough := by
  cases s <;> simp [bumpStop]

private def scan (test : Nat → Verdict) : Nat → List Nat → Result
  | 0, _ => ⟨[], .enough⟩
  | _+1, [] => ⟨[], .eof⟩
  | n+1, k::ks =>
      match test k with
      | .error e => ⟨[], .fault 0 e⟩
      | .ok false => let r := scan test (n+1) ks; {r with stop := bumpStop r.stop}
      | .ok true => let r := scan test n ks; {r with emitted := k::r.emitted, stop := bumpStop r.stop}
termination_by n ks => (n, ks.length)

private def passIdx (test : Nat → Verdict) (l : List (Nat × Nat)) : List (Nat × Nat) :=
  l.filter (fun item => test item.1 matches .ok true)

private def nthCut (test : Nat → Verdict) (n : Nat) (ks : List Nat) : Nat :=
  if n == 0 then 0 else
    ((passIdx test ks.zipIdx)[n-1]?.map (fun item => item.2 + 1)).getD 0

private def visitedRel (test : Nat → Verdict) (n : Nat) (ks : List Nat) : List Nat :=
  match (scan test n ks).stop with
  | .eof => ks
  | .fault j _ => ks.take (j+1)
  | .enough => if n == 0 then [] else ks.take (nthCut test n ks)

private theorem passIdx_true_cons (test : Nat → Verdict) (k i : Nat) (ks : List (Nat × Nat))
    (ht : (test k matches .ok true) = true) :
    passIdx test ((k, i) :: ks) = (k, i) :: passIdx test ks := by
  simp [passIdx, ht]

private theorem passIdx_false_cons (test : Nat → Verdict) (k i : Nat) (ks : List (Nat × Nat))
    (ht : (test k matches .ok true) = false) :
    passIdx test ((k, i) :: ks) = passIdx test ks := by
  simp [passIdx, ht]

private theorem zipIdx_add_eq_map_add (ks : List Nat) (i : Nat) :
    ks.zipIdx (i+1) = (ks.zipIdx i).map (fun p : Nat × Nat => (p.1, p.2 + 1)) := by
  induction ks generalizing i with
  | nil => simp
  | cons a as ih =>
      simp [List.zipIdx_cons, ih (i+1)]

private theorem passIdx_map_snd_add (test : Nat → Verdict) (l : List (Nat × Nat)) :
    passIdx test (l.map (fun p : Nat × Nat => (p.1, p.2 + 1))) =
      (passIdx test l).map (fun p : Nat × Nat => (p.1, p.2 + 1)) := by
  induction l with
  | nil => simp [passIdx]
  | cons a as ih =>
      by_cases hp : ((test a.1 matches .ok true) = true)
      · simpa [passIdx, hp] using ih
      · simpa [passIdx, hp] using ih

private theorem nthCut_false_cons_succ (test : Nat → Verdict) (k : Nat) (ks : List Nat) (m : Nat)
    (hex : (passIdx test ks.zipIdx)[m]?.isSome = true)
    (ht : (test k matches .ok true) = false) :
    nthCut test (m+1) (k :: ks) = nthCut test (m+1) ks + 1 := by
  simp [nthCut]
  rw [passIdx_false_cons test k 0 (ks.zipIdx 1) ht]
  rw [zipIdx_add_eq_map_add ks 0]
  rw [passIdx_map_snd_add, List.getElem?_map]
  cases h : (passIdx test ks.zipIdx)[m]? with
  | none => simp [h] at hex
  | some item => simp

private theorem nthCut_true_cons_one (test : Nat → Verdict) (k : Nat) (ks : List Nat)
    (ht : (test k matches .ok true) = true) :
    nthCut test 1 (k :: ks) = 1 := by
  simp [nthCut, passIdx, List.zipIdx_cons, ht]

private theorem nthCut_true_cons_succ (test : Nat → Verdict) (k : Nat) (ks : List Nat) (m : Nat)
    (hex : (passIdx test ks.zipIdx)[m]?.isSome = true)
    (ht : (test k matches .ok true) = true) :
    nthCut test (m+2) (k :: ks) = nthCut test (m+1) ks + 1 := by
  have hz : 0 < m + 2 := by omega
  simp [nthCut, hz]
  rw [passIdx_true_cons test k 0 (ks.zipIdx 1) ht]
  rw [zipIdx_add_eq_map_add ks 0]
  rw [passIdx_map_snd_add]
  rw [List.getElem?_cons_succ]
  rw [List.getElem?_map]
  cases h : (passIdx test ks.zipIdx)[m]? with
  | none => simp [h] at hex
  | some item => simp

private theorem scan_enough_nth_some (test : Nat → Verdict) :
    ∀ n ks, (scan test n ks).stop = .enough → 0 < n →
      (passIdx test ks.zipIdx)[n-1]?.isSome = true := by
  intro n ks hstop hn
  induction ks generalizing n with
  | nil =>
      cases n with
      | zero => omega
      | succ n => simp [scan] at hstop
  | cons k ks ih =>
      cases n with
      | zero => omega
      | succ n =>
          cases htest : test k with
          | error e =>
              simp [scan, htest, bumpStop] at hstop
          | ok pass =>
              by_cases hp : pass = true
              · subst pass
                cases n with
                | zero =>
                    have ht : (test k matches .ok true) = true := by simp [htest]
                    simp [passIdx, List.zipIdx_cons, ht]
                | succ m =>
                    have ht : (test k matches .ok true) = true := by simp [htest]
                    have hs : bumpStop (scan test (m+1) ks).stop = .enough := by
                      simpa [scan, htest] using hstop
                    have htail : (scan test (m+1) ks).stop = .enough :=
                      (bumpStop_eq_enough _).mp hs
                    have htailnth : (passIdx test ks.zipIdx)[m]?.isSome = true := by
                      have := ih (m+1) htail (by omega)
                      simpa using this
                    change (passIdx test ((k, 0) :: ks.zipIdx 1))[m+1]?.isSome = true
                    rw [passIdx_true_cons test k 0 (ks.zipIdx 1) ht]
                    rw [List.getElem?_cons_succ]
                    rw [zipIdx_add_eq_map_add ks 0]
                    rw [passIdx_map_snd_add, List.getElem?_map]
                    cases h : (passIdx test ks.zipIdx)[m]? with
                    | none => simp [h] at htailnth
                    | some item => simp
              · have hp' : pass = false := by
                  cases pass <;> simp at hp ⊢
                subst pass
                have ht : (test k matches .ok true) = false := by simp [htest]
                have hs : bumpStop (scan test (n+1) ks).stop = .enough := by
                  simpa [scan, htest] using hstop
                have htail : (scan test (n+1) ks).stop = .enough :=
                  (bumpStop_eq_enough _).mp hs
                have htailnth := ih (n+1) htail (by omega)
                change (passIdx test ((k, 0) :: ks.zipIdx 1))[n]?.isSome = true
                rw [passIdx_false_cons test k 0 (ks.zipIdx 1) ht]
                rw [zipIdx_add_eq_map_add ks 0]
                rw [passIdx_map_snd_add, List.getElem?_map]
                cases h : (passIdx test ks.zipIdx)[n]? with
                | none => simp [h] at htailnth
                | some item => simp


private def shiftStop (i : Nat) : Stop → Stop
  | .fault j e => .fault (i+j) e
  | s => s

private theorem shift_bump (i : Nat) (s : Stop) : shiftStop i (bumpStop s) = shiftStop (i+1) s := by
  cases s <;> simp [shiftStop, bumpStop, Nat.add_assoc] <;> omega

private theorem shiftStop_zero (s : Stop) : shiftStop 0 s = s := by
  cases s <;> simp [shiftStop]

private theorem shift_zero_result (r : Result) :
    ({ r with stop := shiftStop 0 r.stop } : Result) = r := by
  cases r with
  | mk emitted stop =>
      cases stop <;> simp [shiftStop]

private theorem takePassing_eq_shift (test : Nat → Verdict) :
    ∀ n i ks, takePassing test n i ks =
      let r := scan test n ks
      { r with stop := shiftStop i r.stop } := by
  intro n i ks
  induction ks generalizing n i with
  | nil =>
      cases n <;> simp [takePassing, scan, shiftStop]
  | cons k ks ih =>
      cases n with
      | zero => simp [takePassing, scan, shiftStop]
      | succ n =>
          cases htest : test k with
          | error e => simp [takePassing, scan, htest, shiftStop]
          | ok pass =>
              by_cases hp : pass = true
              · subst pass
                have htail := ih n (i+1)
                simp [takePassing, scan, htest, shift_bump] at htail ⊢
                rw [htail]
                simp
              · have hp' : pass = false := by cases pass <;> simp_all
                subst pass
                have htail := ih (n+1) (i+1)
                simpa [takePassing, scan, htest, shift_bump] using htail


private theorem demanded_eq_visitedRel (test : Nat → Verdict) :
    ∀ n i ks, demanded test n i ks = visitedRel test n ks := by
  intro n i ks
  induction ks generalizing n i with
  | nil =>
      cases n <;> simp [demanded, visitedRel, scan]
  | cons k ks ih =>
      cases n with
      | zero =>
          simp [demanded, visitedRel, scan]
      | succ n =>
          cases htest : test k with
          | error e =>
              simp [demanded, visitedRel, scan, htest]
          | ok pass =>
              by_cases hp : pass = true
              · subst pass
                have ht : (test k matches .ok true) = true := by simp [htest]
                cases n with
                | zero =>
                    simp [demanded, visitedRel, scan, htest, bumpStop,
                      nthCut_true_cons_one test k ks ht]
                | succ m =>
                    have htail := ih (m+1) (i+1)
                    cases hstop : (scan test (m+1) ks).stop with
                    | eof =>
                        simp [demanded, visitedRel, scan, htest, hstop, htail, bumpStop]
                    | fault j e =>
                        simp [demanded, visitedRel, scan, htest, hstop, htail, bumpStop]
                    | enough =>
                        have hex : (passIdx test ks.zipIdx)[m]?.isSome = true := by
                          have := scan_enough_nth_some test (m+1) ks hstop (by omega)
                          simpa using this
                        have hcut := nthCut_true_cons_succ test k ks m hex ht
                        simp [demanded, visitedRel, scan, htest, hstop, hcut, htail, bumpStop]
              · have hp' : pass = false := by
                  cases pass <;> simp at hp ⊢
                subst pass
                have ht : (test k matches .ok true) = false := by simp [htest]
                have htail := ih (n+1) (i+1)
                cases hstop : (scan test (n+1) ks).stop with
                | eof =>
                    simp [demanded, visitedRel, scan, htest, hstop, htail, bumpStop]
                | fault j e =>
                    simp [demanded, visitedRel, scan, htest, hstop, htail, bumpStop]
                | enough =>
                    have hex : (passIdx test ks.zipIdx)[n]?.isSome = true := by
                      have := scan_enough_nth_some test (n+1) ks hstop (by omega)
                      simpa using this
                    have hcut := nthCut_false_cons_succ test k ks n hex ht
                    simp [demanded, visitedRel, scan, htest, hstop, hcut, htail, bumpStop]


theorem sound : Goal := by
  intro test ks n
  let ref := takePassing test n 0 ks
  let result := cached test [] n 0 ks
  let visited := match ref.stop with
    | .eof => ks
    | .fault i _ => ks.take (i+1)
    | .enough => if n == 0 then [] else ks.take ((ks.zipIdx.filter
        (fun (item : Nat × Nat) => test item.1 matches .ok true))[n-1]?.map (fun (item : Nat × Nat) => item.2+1) |>.getD 0)
  change result.1 = ref ∧ result.2 = visited.eraseDups.length
  have hout : result.1 = ref := by
    dsimp [result, ref]
    exact cached_eq_take test [] (by simp) n 0 ks
  have hwork0 := cached_work_eq_demanded test [] (by simp) n 0 ks
  have hfilt : (demanded test n 0 ks).filter
      (fun k => decide (List.lookup k ([] : List (Nat × Verdict)) = none)) = demanded test n 0 ks := by
    apply List.filter_eq_self.mpr
    intro k hk
    simp
  have hwork : result.2 = (demanded test n 0 ks).eraseDups.length := by
    dsimp [result]
    rw [← hfilt]
    exact hwork0
  have htake : ref = scan test n ks := by
    dsimp [ref]
    have h := takePassing_eq_shift test n 0 ks
    rw [shift_zero_result] at h
    exact h
  have hvisited : visited = visitedRel test n ks := by
    change (match ref.stop with
      | .eof => ks
      | .fault i _ => ks.take (i+1)
      | .enough => if n == 0 then [] else ks.take ((ks.zipIdx.filter
          (fun (item : Nat × Nat) => test item.1 matches .ok true))[n-1]?.map (fun (item : Nat × Nat) => item.2+1) |>.getD 0)) =
      visitedRel test n ks
    rw [htake]
    by_cases hn : n == 0 <;> simp [visitedRel, nthCut, passIdx, hn]
  have hdem : demanded test n 0 ks = visited := by
    exact (demanded_eq_visitedRel test n 0 ks).trans hvisited.symm
  constructor
  · exact hout
  · rw [hwork, hdem]

end TinylambOpt.ResearchProofs.EngineDecisions.Ex_DemandCache
