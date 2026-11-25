import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_TransportLower

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings

private def pc : List Int → List Int → Nat
  | [], _ => 0
  | _ :: _, [] => 0
  | x :: xs, y :: ys => (x - y).natAbs + pc xs ys

private theorem pc_eq_zip (xs ys : List Int) :
    pc xs ys = ((xs.zip ys).map (fun p => (p.1 - p.2).natAbs)).sum := by
  induction xs generalizing ys with
  | nil => simp [pc]
  | cons x xs ih =>
      cases ys with
      | nil => simp [pc]
      | cons y ys => simp [pc, ih]

private theorem zip_map (rs : Rows) :
    (rs.map Prod.fst).zip (rs.map Prod.snd) = rs := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      cases r
      simp [ih]

private theorem cost_eq_pc (rs : Rows) :
    cost rs = pc (rs.map Prod.fst) (rs.map Prod.snd) := by
  rw [pc_eq_zip, zip_map]
  rfl

private theorem cost_mono_eq_pc (rs : Rows) :
    cost (monotone rs) = pc ((rs.map Prod.fst).mergeSort) ((rs.map Prod.snd).mergeSort) := by
  rw [pc_eq_zip]
  simp [monotone, cost]

private def sorted (l : List Int) : Prop := l.Pairwise (fun a b => a ≤ b)

private theorem sorted_mergeSort (l : List Int) : sorted l.mergeSort := by
  apply (List.Pairwise.imp (fun {a b} h => of_decide_eq_true h))
  apply List.pairwise_mergeSort
  · intro a b c hab hbc
    apply decide_eq_true
    have ha : a ≤ b := of_decide_eq_true hab
    have hb : b ≤ c := of_decide_eq_true hbc
    omega
  · intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have hba : b ≤ a := by omega
      simp [hba]

private def insAt : Nat → Int → List Int → List Int
  | 0, b, ys => b :: ys
  | _ + 1, b, [] => [b]
  | n + 1, b, y :: ys => y :: insAt n b ys

private def insSorted (b : Int) : List Int → List Int
  | [] => [b]
  | y :: ys => if y < b then y :: insSorted b ys else b :: y :: ys

private theorem insAt_length (p : Nat) (b : Int) (ys : List Int) :
    (insAt p b ys).length = ys.length + 1 := by
  induction p generalizing ys with
  | zero => simp [insAt]
  | succ n ih =>
      cases ys with
      | nil => simp [insAt]
      | cons y ys => simp [insAt, ih]

private theorem insSorted_length (b : Int) (ys : List Int) :
    (insSorted b ys).length = ys.length + 1 := by
  induction ys with
  | nil => simp [insSorted]
  | cons y ys ih =>
      by_cases h : y < b <;> simp [insSorted, h, ih]

private theorem insSorted_eq_cons {b : Int} {ys : List Int} (h : ∀ z ∈ ys, b ≤ z) :
    insSorted b ys = b :: ys := by
  cases ys with
  | nil => simp [insSorted]
  | cons y ys =>
      have hy : b ≤ y := h y (by simp)
      have hny : ¬ y < b := by omega
      simp [insSorted, hny]

private theorem countP_eq_zero {α : Type} (p : α → Bool) {l : List α}
    (h : ∀ x ∈ l, p x = false) : l.countP p = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem insSorted_eq_insAt {xs : List Int} (hs : sorted xs) (a : Int) :
    insSorted a xs = insAt (xs.countP (fun z => z < a)) a xs := by
  induction xs with
  | nil => simp [insSorted, insAt]
  | cons x xs ih =>
      have htail : sorted xs := hs.tail
      have hxmin : ∀ z ∈ xs, x ≤ z := (List.pairwise_cons.mp hs).1
      by_cases hx : x < a
      · simp [insSorted, hx, insAt, ih htail]
      · have hnone : ∀ z ∈ xs, decide (z < a) = false := by
          intro z hz
          have hxz : x ≤ z := hxmin z hz
          exact decide_eq_false (by omega)
        have hcnt : xs.countP (fun z => z < a) = 0 :=
          countP_eq_zero (fun z => decide (z < a)) hnone
        simp [insSorted, hx, insAt, hcnt]

private theorem mem_insSorted {b z : Int} {ys : List Int} :
    z ∈ insSorted b ys ↔ z = b ∨ z ∈ ys := by
  induction ys with
  | nil => simp [insSorted]
  | cons y ys ih =>
      by_cases hy : y < b <;> simp [insSorted, hy, ih, or_left_comm]

private theorem le_all_insSorted {x a : Int} {xs : List Int}
    (hxall : ∀ z ∈ xs, x ≤ z) (hxa : x ≤ a) :
    ∀ z ∈ insSorted a xs, x ≤ z := by
  intro z hz
  rcases (mem_insSorted.mp hz) with rfl | hz
  · exact hxa
  · exact hxall z hz

private theorem insSorted_sorted {xs : List Int} (h : sorted xs) (a : Int) :
    sorted (insSorted a xs) := by
  induction xs with
  | nil => simp [sorted, insSorted]
  | cons x xs ih =>
      have htail : sorted xs := h.tail
      have hxmin : ∀ z ∈ xs, x ≤ z := (List.pairwise_cons.mp h).1
      by_cases hx : x < a
      · rw [show insSorted a (x :: xs) = x :: insSorted a xs by simp [insSorted, hx]]
        apply List.pairwise_cons.mpr
        constructor
        · intro z hz
          exact le_all_insSorted hxmin (by omega) z hz
        · exact ih htail
      · have hax : a ≤ x := by omega
        rw [show insSorted a (x :: xs) = a :: x :: xs by simp [insSorted, hx]]
        apply List.pairwise_cons.mpr
        constructor
        · intro z hz
          rcases List.mem_cons.mp hz with rfl | hz
          · exact hax
          · have haz : a ≤ z := by have := hxmin z hz; omega
            exact haz
        · exact h

private theorem mergeSort_cons_eq_insSorted (a : Int) (xs : List Int) :
    (a :: xs).mergeSort = insSorted a xs.mergeSort := by
  have trans : ∀ (p q r : Int), decide (p ≤ q) = true → decide (q ≤ r) = true →
      decide (p ≤ r) = true := by
    intro p q r hpq hqr
    apply decide_eq_true
    have hpq' : p ≤ q := of_decide_eq_true hpq
    have hqr' : q ≤ r := of_decide_eq_true hqr
    omega
  have total : ∀ (p q : Int), (decide (p ≤ q) || decide (q ≤ p)) = true := by
    intro p q
    by_cases h : p ≤ q
    · simp [h]
    · have hqp : q ≤ p := by omega
      simp [hqp]
  obtain ⟨l1, l2, hfull, hsplit, hl1⟩ := List.mergeSort_cons trans total a xs
  rw [hfull, hsplit]
  have hl1lt : ∀ z ∈ l1, z < a := by
    intro z hz
    have hnz : (!decide (a ≤ z)) = true := hl1 z hz
    by_cases haz : a ≤ z <;> simp [haz] at hnz <;> omega
  have le_suf {a : Int} {pre suf : List Int} (hpair : sorted (pre ++ a :: suf)) :
      ∀ z ∈ suf, a ≤ z := by
    induction pre with
    | nil => exact (List.pairwise_cons.mp hpair).1
    | cons w ws ih => exact ih (List.pairwise_cons.mp hpair).2
  have hl2ge : ∀ z ∈ l2, a ≤ z := by
    apply le_suf
    rw [← hfull]
    exact sorted_mergeSort (a :: xs)
  have hins : ∀ (pre suf : List Int), (∀ z ∈ pre, z < a) → (∀ z ∈ suf, a ≤ z) →
      insSorted a (pre ++ suf) = pre ++ a :: suf := by
    intro pre
    induction pre with
    | nil =>
        intro suf _ hsuf
        simp [insSorted_eq_cons hsuf]
    | cons z zs ih =>
        intro suf hpre hsuf
        have hz : z < a := hpre z (by simp)
        simp [insSorted, hz, ih suf (fun y hy => hpre y (by simp [hy])) hsuf]
  exact (hins l1 l2 hl1lt hl2ge).symm

private theorem insAt_nil (p : Nat) (b : Int) : insAt p b [] = [b] := by
  induction p with
  | zero => rfl
  | succ n ih => simp [insAt]

private theorem pc_insAt_insAt (p : Nat) (a b : Int) (xs ys : List Int)
    (hlen : xs.length = ys.length) (hp : p ≤ xs.length) :
    pc (insAt p a xs) (insAt p b ys) = (a - b).natAbs + pc xs ys := by
  induction p generalizing xs ys with
  | zero => simp [insAt, pc]
  | succ n ih =>
      cases xs with
      | nil =>
          simp at hp
      | cons x xs' =>
          cases ys with
          | nil =>
              simp at hlen
          | cons y ys' =>
              have hlen' : xs'.length = ys'.length := by simp at hlen; omega
              have hp' : n ≤ xs'.length := by simpa using hp
              simp only [insAt, pc]
              rw [ih xs' ys' hlen' hp']
              omega

private theorem core {xs ys : List Int} (hxs : sorted xs) (hys : sorted ys)
    (hlen : xs.length = ys.length + 1) (b : Int) (p : Nat) :
    pc xs (insSorted b ys) ≤ pc xs (insAt p b ys) := by
  induction ys generalizing xs p with
  | nil =>
      simp [insSorted, insAt_nil]
  | cons y ys ih =>
      have hys' : sorted ys := hys.tail
      have hy_le : ∀ z ∈ ys, y ≤ z := (List.pairwise_cons.mp hys).1
      cases xs with
      | nil => simp at hlen
      | cons x xs' =>
          have hxs' : sorted xs' := hxs.tail
          have hx_le : ∀ z ∈ xs', x ≤ z := (List.pairwise_cons.mp hxs).1
          have hlen' : xs'.length = ys.length + 1 := by simp at hlen; omega
          cases p with
          | zero =>
              by_cases hyb : y < b
              · have hWW : insSorted b (y :: ys) = y :: insSorted b ys := by
                  simp [insSorted, hyb]
                rw [hWW]
                simp only [insAt, pc]
                have ih0 := ih (xs := xs') hxs' hys' hlen' (p := 0)
                change pc xs' (insSorted b ys) ≤ pc xs' (b :: ys) at ih0
                cases xs' with
                | nil => simp at hlen'
                | cons x2 xs'' =>
                    have hxx2 : x ≤ x2 := hx_le x2 (by simp)
                    simp [pc] at ih0 ⊢
                    omega
              · have hby : b ≤ y := by omega
                have hWW : insSorted b (y :: ys) = b :: y :: ys := by
                  simp [insSorted, show ¬ y < b by omega]
                rw [hWW]
                simp [insAt]
          | succ n =>
              by_cases hyb : y < b
              · have hWW : insSorted b (y :: ys) = y :: insSorted b ys := by
                  simp [insSorted, hyb]
                rw [hWW]
                simp only [insAt, pc]
                have ihN := ih (xs := xs') hxs' hys' hlen' (p := n)
                omega
              · have hby : b ≤ y := by omega
                have hall : ∀ z ∈ ys, b ≤ z := by
                  intro z hz
                  have : y ≤ z := hy_le z hz
                  omega
                have hins : insSorted b ys = b :: ys := insSorted_eq_cons hall
                have ihN := ih (xs := xs') hxs' hys' hlen' (p := n)
                rw [hins] at ihN
                have hWW : insSorted b (y :: ys) = b :: y :: ys := by
                  simp [insSorted, show ¬ y < b by omega]
                rw [hWW]
                simp only [insAt, pc]
                cases xs' with
                | nil => simp at hlen'
                | cons x2 xs'' =>
                    have hxx2 : x ≤ x2 := hx_le x2 (by simp)
                    simp [pc] at ihN ⊢
                    omega

private theorem two_insert (xs ys : List Int) (hxs : sorted xs) (hys : sorted ys)
    (hlen : xs.length = ys.length) (a b : Int) :
    pc (insSorted a xs) (insSorted b ys) ≤ (a - b).natAbs + pc xs ys := by
  let p := xs.countP (fun z => z < a)
  have hp : p ≤ xs.length := List.countP_le_length
  have hinsa : insSorted a xs = insAt p a xs := insSorted_eq_insAt hxs a
  have hsia : sorted (insSorted a xs) := insSorted_sorted hxs a
  have hlen1 : (insSorted a xs).length = ys.length + 1 := by
    rw [insSorted_length]
    omega
  have h1 := core hsia hys hlen1 b p
  rw [hinsa] at h1 ⊢
  have h2 := pc_insAt_insAt p a b xs ys hlen hp
  omega

private theorem transport : ∀ (xs ys : List Int), xs.length = ys.length →
    pc xs.mergeSort ys.mergeSort ≤ pc xs ys := by
  intro xs
  induction xs with
  | nil =>
      intro ys hlen
      cases ys with
      | nil => simp [pc]
      | cons b ys => simp at hlen
  | cons a xs ih =>
      intro ys hlen
      cases ys with
      | nil => simp at hlen
      | cons b ys =>
          have hlen' : xs.length = ys.length := by simp at hlen; omega
          have ih' := ih ys hlen'
          rw [mergeSort_cons_eq_insSorted a xs, mergeSort_cons_eq_insSorted b ys]
          have hstep := two_insert xs.mergeSort ys.mergeSort
            (sorted_mergeSort xs) (sorted_mergeSort ys)
            (by simpa [List.length_mergeSort] using hlen') a b
          simp [pc] at ih' ⊢
          omega

theorem sound_Couplings_TransportLower : Couplings.TransportLower.Goal := by
  intro rs
  rw [cost_mono_eq_pc]
  rw [cost_eq_pc]
  exact transport (rs.map Prod.fst) (rs.map Prod.snd) (by simp)

end TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_TransportLower
