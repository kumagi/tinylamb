import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-!
Committing the materialized output prefix at the first fault is sound: the
output prefix ends exactly where the first failing physical row lies, so all
earlier successful values are preserved and no later value is published.
SQL execution may therefore commit only the compacted prefix and replay the
remaining rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ScatterEarlierSuccessesOnly

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

private theorem zipIdx_fst_aux (xs : List Cell) (k : Nat) (l : List (Cell × Nat)) (h : xs.zipIdx k = l) :
    xs = l.map Prod.fst := by
  induction xs generalizing k l with
  | nil => simp at h; simp [h]
  | cons x xs' ih =>
      cases l with
      | nil => simp at h
      | cons p ps =>
          simp [List.zipIdx_cons] at h
          rcases h with ⟨h1, h2⟩
          cases h1
          simp [ih (k+1) ps h2]

private theorem zipIdx_split_aux (xs : List Cell) (k : Nat) (l₁ l₂ : List (Cell × Nat)) (c : Cell) (i : Nat)
    (h : xs.zipIdx k = l₁ ++ (c,i)::l₂) :
    xs = l₁.map Prod.fst ++ c :: l₂.map Prod.fst ∧ i = k + l₁.length := by
  induction l₁ generalizing xs k with
  | nil =>
      simp at h
      cases xs with
      | nil => simp at h
      | cons x xs' =>
          simp [List.zipIdx_cons] at h
          rcases h with ⟨h1, h2⟩
          rcases h1 with ⟨rfl, hk⟩
          subst i
          constructor
          · simp [zipIdx_fst_aux xs' (k+1) l₂ h2]
          · simp
  | cons p ps ih =>
      cases xs with
      | nil => simp at h
      | cons x xs' =>
          simp [List.zipIdx_cons] at h
          rcases h with ⟨h1, h2⟩
          cases h1
          have hsplit := ih xs' (k+1) h2
          rcases hsplit with ⟨hxs, hi⟩
          constructor
          · simp [hxs]
          · simp at hi ⊢
            omega

private theorem takeWhile_all_not_bad_aux (l : List Cell) (h : ∀ c ∈ l, bad c = false) (e : Nat) (b : List Cell) :
    (l ++ (.error e :: b)).takeWhile (fun x => !bad x) = l := by
  induction l with
  | nil => simp [bad]
  | cons c cs ih =>
      have hc : bad c = false := h c (by simp)
      have hcs : ∀ z ∈ cs, bad z = false := by intro z hz; exact h z (by simp [hz])
      simp [hc, ih hcs]

private theorem prefixOut_eq_take_aux (xs : List Cell) (idx e : Nat) (h : first xs = some (idx,e)) :
    prefixOut xs = (xs.take idx).filterMap (fun c => c.toOption.join) := by
  unfold first at h
  rcases List.findSome?_eq_some_iff.mp h with ⟨l₁, a, l₂, hzip, hfa, hpref⟩
  rcases a with ⟨c,i⟩
  cases c with
  | error e0 =>
      simp at hfa
      rcases hfa with ⟨hi, he⟩
      subst i
      subst e0
      have hsplit := zipIdx_split_aux xs 0 l₁ l₂ (.error e) idx hzip
      rcases hsplit with ⟨hxs, hilen⟩
      have hi_len : idx = l₁.length := by omega
      have hall : ∀ z ∈ l₁.map Prod.fst, bad z = false := by
        intro z hz
        rcases List.mem_map.mp hz with ⟨p, hp, rfl⟩
        rcases p with ⟨pc, pn⟩
        have hpnone := hpref (pc,pn) hp
        cases pc with
        | error ee => simp at hpnone
        | ok v => simp [bad]
      have htw : xs.takeWhile (fun x => !bad x) = l₁.map Prod.fst := by
        rw [hxs]
        exact takeWhile_all_not_bad_aux (l₁.map Prod.fst) hall e (l₂.map Prod.fst)
      rw [prefixOut, htw]
      simp [hi_len, hxs]
  | ok v => simp at hfa

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ScatterEarlierSuccessesOnly.Goal := by
  intro xs i e h
  exact prefixOut_eq_take_aux xs i e h

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ScatterEarlierSuccessesOnly
