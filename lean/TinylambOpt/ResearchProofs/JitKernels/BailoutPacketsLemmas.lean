import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-! Shared list lemmas for the JitKernels.BailoutPackets audit. They relate the
first fault reported by a compacted packet to the original ordinal, and relate
prefix output to the physical prefix preceding that fault. These lemmas let SQL
execution replay or commit a prefix without rescanning earlier packets. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.BailoutPacketsLemmas

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets


def firstAt (l : List Cell) (k : Nat) : Option (Nat × Nat) :=
  (l.zipIdx k).findSome? (fun (c,i) => match c with | .error e => some (i,e) | _ => none)

theorem firstAt_index (l : List Cell) (k j e : Nat) (h : firstAt l k = some (j,e)) :
    ∃ d, j = k+d ∧ l[d]? = some (.error e) := by
  induction l generalizing k with
  | nil => simp [firstAt] at h
  | cons c cs ih =>
      cases c with
      | error e0 =>
          simp [firstAt, List.zipIdx_cons] at h
          rcases h with ⟨hk, he⟩
          subst j
          refine ⟨0, by omega, ?_⟩
          simp [he]
      | ok v =>
          simp [firstAt, List.zipIdx_cons] at h
          obtain ⟨d, hd, hcell⟩ := ih (k+1) h
          exact ⟨d+1, by omega, by simpa using hcell⟩

theorem firstAt_drop (l : List Cell) (k i e : Nat) (h : firstAt l k = some (k+i,e)) :
    firstAt (l.drop i) 0 = some (0,e) := by
  induction i generalizing l k with
  | zero =>
      cases l with
      | nil => simp [firstAt] at h
      | cons c cs =>
          cases c with
          | error e0 =>
              simp [firstAt, List.zipIdx_cons] at h ⊢
              omega
          | ok v =>
              simp [firstAt, List.zipIdx_cons] at h
              obtain ⟨d, hd, _⟩ := firstAt_index cs (k+1) k e h
              omega
  | succ i ih =>
      cases l with
      | nil => simp [firstAt] at h
      | cons c cs =>
          cases c with
          | error e0 =>
              simp [firstAt, List.zipIdx_cons] at h
          | ok v =>
              have hh : firstAt cs (k+1) = some ((k+1)+i,e) := by
                unfold firstAt at h ⊢
                rw [List.zipIdx_cons, List.findSome?_cons] at h
                simpa [show k + (i+1) = (k+1)+i by omega] using h
              simpa using ih cs (k+1) hh

theorem first_error_at (l : List Cell) (idx e : Nat) (h : first l = some (idx,e)) :
    l[idx]? = some (.error e) := by
  obtain ⟨d, hd, hcell⟩ := firstAt_index l 0 idx e h
  simpa [hd] using hcell

theorem first_none_no_bad (l : List Cell) (k : Nat) (h : firstAt l k = none) :
    ∀ c ∈ l, bad c = false := by
  induction l generalizing k with
  | nil => simp
  | cons c cs ih =>
      cases c with
      | error e0 => simp [firstAt, List.zipIdx_cons] at h
      | ok v =>
          simp [firstAt, List.zipIdx_cons] at h
          have hcs : firstAt cs (k+1) = none := by
            unfold firstAt
            apply List.findSome?_eq_none_iff.mpr
            intro x hx
            simpa using h x.1 x.2 hx
          intro z hz
          simp at hz
          rcases hz with rfl | hz
          · simp [bad]
          · exact ih (k+1) hcs z hz

theorem first_any_bad (l : List Cell) (k : Nat) : (firstAt l k).isSome = l.any bad := by
  induction l generalizing k with
  | nil => simp [firstAt]
  | cons c cs ih =>
      cases c with
      | error e0 =>
          rw [firstAt, List.zipIdx_cons, List.findSome?_cons, List.any_cons]
          simp [bad]
      | ok v =>
          change (firstAt cs (k+1)).isSome = (bad (.ok v) || cs.any bad)
          simp only [bad, Bool.false_or]
          exact ih (k+1)

theorem prefixOut_append_of_nobad (a b : List Cell) (h : ∀ c ∈ a, bad c = false) :
    prefixOut (a++b) = prefixOut a ++ prefixOut b := by
  induction a with
  | nil => simp [prefixOut]
  | cons c cs ih =>
      have hc : bad c = false := h c (by simp)
      have hcs : ∀ z ∈ cs, bad z = false := by intro z hz; exact h z (by simp [hz])
      have ih' : ((cs++b).takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join) =
          ((cs.takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join)) ++
          ((b.takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join)) := by
        simpa [prefixOut] using ih hcs
      change ((c :: (cs ++ b)).takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join) =
        (((c :: cs).takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join)) ++
          ((b.takeWhile (fun x => !bad x)).filterMap (fun x => x.toOption.join))
      simp only [List.takeWhile_cons, hc, Bool.not_false, ite_true]
      cases hcj : c.toOption.join with
      | none => simpa [hcj] using ih'
      | some v => simpa [hcj] using congrArg (List.cons v) ih'

theorem zipIdx_fst (xs : List Cell) (k : Nat) (l : List (Cell × Nat)) (h : xs.zipIdx k = l) :
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

theorem zipIdx_split (xs : List Cell) (k : Nat) (l₁ l₂ : List (Cell × Nat)) (c : Cell) (i : Nat)
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
          · simp [zipIdx_fst xs' (k+1) l₂ h2]
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

theorem takeWhile_of_all_notbad (l : List Cell) (h : ∀ c ∈ l, bad c = false) (e : Nat) (b : List Cell) :
    (l ++ (.error e :: b)).takeWhile (fun x => !bad x) = l := by
  induction l with
  | nil => simp [bad]
  | cons c cs ih =>
      have hc : bad c = false := h c (by simp)
      have hcs : ∀ z ∈ cs, bad z = false := by intro z hz; exact h z (by simp [hz])
      simp [hc, ih hcs]

theorem prefixOut_eq_take (xs : List Cell) (idx e : Nat) (h : first xs = some (idx,e)) :
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
      have hsplit := zipIdx_split xs 0 l₁ l₂ (.error e) idx hzip
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
        exact takeWhile_of_all_notbad (l₁.map Prod.fst) hall e (l₂.map Prod.fst)
      rw [prefixOut, htw]
      simpa [hi_len, hxs]
  | ok v => simp at hfa

theorem gather_getElem? (xs : List Cell) (sel : List Nat) (hb : ∀ i ∈ sel, i < xs.length) (j : Nat) :
    (gather xs sel)[j]? = (sel[j]?).bind (fun i => xs[i]?) := by
  induction sel generalizing j with
  | nil => simp [gather]
  | cons i is ih =>
      have hi : i < xs.length := hb i (by simp)
      have his : ∀ z ∈ is, z < xs.length := by intro z hz; exact hb z (by simp [hz])
      cases j with
      | zero => simp [gather, hi]
      | succ j => simpa [gather, hi] using ih his j

theorem gather_any (xs : List Cell) (sel : List Nat) (hb : ∀ i ∈ sel, i < xs.length) :
    (gather xs sel).any bad = sel.any (fun i => xs[i]?.any bad) := by
  induction sel with
  | nil => simp [gather]
  | cons i is ih =>
      have hi : i < xs.length := hb i (by simp)
      have his : ∀ z ∈ is, z < xs.length := by intro z hz; exact hb z (by simp [hz])
      simp [gather, hi]
      exact congrArg (fun b => bad xs[i] || b) (by simpa [gather] using ih his)

end Nov26.BailoutPacketsLemmas
