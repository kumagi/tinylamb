import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-
Proves `CompactedBailoutOrdinal`: when every selected ordinal is in range,
the compacted packet's first fault maps back to the selected row that carries
the reported error. A SQL kernel may replay at that source ordinal.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_CompactedBailoutOrdinal

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

def firstF (n : Nat) (xs : List Cell) : Option (Nat × Nat) :=
  (xs.zipIdx n).findSome? (fun (c, i) => match c with | .error e => some (i, e) | _ => none)

theorem firstF_add (a b : Nat) (xs : List Cell) :
    firstF (a + b) xs = (firstF a xs).map (fun p : Nat × Nat => (p.1 + b, p.2)) := by
  induction xs generalizing a b with
  | nil => simp [firstF]
  | cons c cs ih =>
      cases c with
      | error e => simp [firstF]
      | ok v =>
          change firstF (a + b) (Except.ok v :: cs) = _
          simp [firstF]
          rw [← List.map_findSome?]
          change firstF (a + b + 1) cs = (firstF (a + 1) cs).map (fun p : Nat × Nat => (p.1 + b, p.2))
          rw [show a + b + 1 = a + 1 + b by omega]
          exact ih (a + 1) b

theorem firstF_none_iff (n : Nat) (xs : List Cell) : firstF n xs = none ↔ ∀ c ∈ xs, bad c = false := by
  unfold firstF
  rw [List.findSome?_eq_none_iff]
  constructor
  · intro h c hc
    rcases (List.mem_iff_getElem?.mp hc) with ⟨i, hi⟩
    have hmem : (c, n + i) ∈ xs.zipIdx n := by
      rw [List.mem_iff_getElem?]
      refine ⟨i, ?_⟩
      rw [List.getElem?_zipIdx]
      simp [hi]
    have hf := h (c, n + i) hmem
    cases c <;> simp [bad] at hf ⊢
  · intro h p hp
    rcases p with ⟨c, i⟩
    have hpz := List.mem_zipIdx hp
    have hc : c ∈ xs := by
      have hlt : i - n < xs.length := by omega
      simpa [hpz.2.2] using (List.getElem_mem hlt : xs[i - n] ∈ xs)
    have hf := h c hc
    cases c <;> simp [bad] at hf ⊢

theorem first_none_iff (xs : List Cell) : first xs = none ↔ ∀ c ∈ xs, bad c = false := by
  change firstF 0 xs = none ↔ ∀ c ∈ xs, bad c = false
  exact firstF_none_iff 0 xs

theorem first_cons_of_not_bad (c : Cell) (cs : List Cell) (hc : bad c = false) :
    first (c :: cs) = (first cs).map (fun p : Nat × Nat => (p.1 + 1, p.2)) := by
  cases c with
  | error e => simp [bad] at hc
  | ok v =>
      change firstF 0 (Except.ok v :: cs) = _
      simp [firstF]
      change firstF 1 cs = (first cs).map (fun p : Nat × Nat => (p.1 + 1, p.2))
      have h := firstF_add 0 1 cs
      change firstF 1 cs = (firstF 0 cs).map (fun p : Nat × Nat => (p.1 + 1, p.2)) at h
      exact h

theorem takeWhile_notBad_eq_self (xs : List Cell) (h : ∀ c ∈ xs, bad c = false) :
    xs.takeWhile (fun c => !(bad c)) = xs := by
  induction xs with
  | nil => simp
  | cons c cs ih =>
      simp [h c (by simp), ih (by intro x hx; exact h x (by simp [hx]))]

theorem cell_join_ok (v : Option Int) : (Except.ok v : Cell).toOption.join = v := by
  cases v <;> rfl

theorem prefixOut_cons_not_bad (c : Cell) (cs : List Cell) (hc : bad c = false) :
    prefixOut (c :: cs) = (match c.toOption.join with | none => prefixOut cs | some v => v :: prefixOut cs) := by
  cases c with
  | error e => simp [bad] at hc
  | ok v =>
      rw [prefixOut, List.takeWhile_cons]
      simp [bad]
      rw [List.filterMap_cons]
      rw [cell_join_ok]
      cases v <;> rfl

theorem first_get : ∀ (xs : List Cell) (j e : Nat), first xs = some (j, e) →
    xs[j]? = some (.error e)
| [], j, e, h => by simp [first] at h
| c :: cs, j, e, h => by
    cases c with
    | error e' =>
        simp [first] at h
        rcases h with ⟨rfl, rfl⟩
        simp
    | ok v =>
        have hmap := first_cons_of_not_bad (Except.ok v) cs rfl
        rw [hmap] at h
        cases hcs : first cs with
        | none => simp [hcs] at h
        | some p =>
            simp [hcs] at h
            rcases p with ⟨k, z⟩
            simp at h
            rcases h with ⟨rfl, rfl⟩
            have ihres := first_get cs k z (by simp [hcs])
            simpa using ihres

theorem takeWhile_of_first : ∀ (xs : List Cell) (i e : Nat), first xs = some (i, e) →
    xs.takeWhile (fun c => !(bad c)) = xs.take i := by
  intro xs
  induction xs with
  | nil => intro i e h; simp [first] at h
  | cons c cs ih =>
      intro i e h
      cases c with
      | error e' =>
          simp [first] at h
          rcases h with ⟨rfl, rfl⟩
          simp [bad]
      | ok v =>
          have hmap := first_cons_of_not_bad (Except.ok v) cs rfl
          rw [hmap] at h
          cases hcs : first cs with
          | none => simp [hcs] at h
          | some p =>
              rcases p with ⟨k, z⟩
              simp [hcs] at h
              rcases h with ⟨rfl, rfl⟩
              have ihres := ih k z (by simp [hcs])
              simp [bad] at ihres ⊢
              exact ihres

theorem first_isSome_eq_any (ys : List Cell) : (first ys).isSome = ys.any bad := by
  induction ys with
  | nil => simp [first, bad]
  | cons c cs ih =>
      cases c with
      | error e => simp [first, bad]
      | ok v =>
          rw [first_cons_of_not_bad (Except.ok v) cs rfl]
          simp [bad, ih]

theorem gather_any {xs : List Cell} {sel : List Nat} (h : ∀ i ∈ sel, i < xs.length) :
    (gather xs sel).any bad = sel.any (fun i => xs[i]?.any bad) := by
  induction sel with
  | nil => simp [gather]
  | cons i is ih =>
      have hi : i < xs.length := h i (by simp)
      have h' : ∀ j ∈ is, j < xs.length := by intro j hj; exact h j (by simp [hj])
      have hg : gather xs (i :: is) = xs[i] :: gather xs is := by
        simp [gather, hi, List.filterMap_cons]
      rw [hg]
      simp
      rw [← ih h']
      congr 1
      rw [List.getElem?_eq_getElem hi]
      rfl

theorem gather_get {xs : List Cell} {sel : List Nat} (h : ∀ i ∈ sel, i < xs.length) (j : Nat) :
    (gather xs sel)[j]? = (sel[j]?).bind (fun i => xs[i]?) := by
  induction sel generalizing j with
  | nil => simp [gather]
  | cons i is ih =>
      have hi : i < xs.length := h i (by simp)
      have h' : ∀ k ∈ is, k < xs.length := by intro k hk; exact h k (by simp [hk])
      have hg : gather xs (i :: is) = xs[i] :: gather xs is := by
        simp [gather, hi, List.filterMap_cons]
      cases j with
      | zero => simp [hg]
      | succ j => simp [hg, ih h' j]

theorem proof : CompactedBailoutOrdinal.Goal := by
  intro xs sel j e hrange hfirst
  have hg := first_get (gather xs sel) j e hfirst
  have hgg := gather_get hrange j
  rw [hgg] at hg
  have hsSome : (sel[j]?).isSome = true := by
    cases hsel : sel[j]? <;> simp [hsel] at hg ⊢
  rcases (Option.isSome_iff_exists.mp hsSome) with ⟨i, hi⟩
  refine ⟨i, hi, ?_⟩
  simpa [hi] using hg

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_CompactedBailoutOrdinal
