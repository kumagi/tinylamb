import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `PredicateOrder.RatioSort` — `∀ d ts alt, 0 < d → valid d ts → (∀ t ∈ ts, 0 < t.price
  ∨ t.pass < d) → ts.Perm alt → bill d (ts.mergeSort (byRatio d)) ≤ bill d alt`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

/-- Tests that are admissible inputs of the Smith-ratio argument. -/
private def good (d : Nat) (t : Test) : Prop := t.pass ≤ d ∧ (0 < t.price ∨ t.pass < d)

/-- A globally well-behaved ratio key: clamped survival count, and no degenerate zero/zero key. -/
private def cpass (d : Nat) (t : Test) : Nat := min t.pass d

private def cprice (d : Nat) (t : Test) : Nat :=
  if t.price = 0 ∧ cpass d t = d then 1 else t.price

private def cle (d : Nat) (a b : Test) : Bool :=
  decide (cprice d a * (d - cpass d b) ≤ cprice d b * (d - cpass d a))

/-- Transitivity of comparison of ratios w/p, assuming only the two first keys are non-degenerate. -/
private theorem ratio_trans {α : Type} (w p : α → Nat) (a b c : α)
    (hnz_a : 0 < w a ∨ 0 < p a)
    (hnz_b : 0 < w b ∨ 0 < p b)
    (h1 : w a * p b ≤ w b * p a)
    (h2 : w b * p c ≤ w c * p b) :
    w a * p c ≤ w c * p a := by
  by_cases hpc : p c = 0
  · simp [hpc]
  · have hpcpos : 0 < p c := Nat.pos_of_ne_zero hpc
    by_cases hpb : p b = 0
    · have hwb : 0 < w b := by omega
      have hz : w b * p c = 0 := by
        have hh : w b * p c ≤ 0 := by simpa [hpb] using h2
        exact Nat.eq_zero_of_le_zero hh
      have hpos : 0 < w b * p c := Nat.mul_pos hwb hpcpos
      omega
    · have hpbpos : 0 < p b := Nat.pos_of_ne_zero hpb
      by_cases hpa : p a = 0
      · have hwa : 0 < w a := by omega
        have hz : w a * p b = 0 := by
          have hh : w a * p b ≤ 0 := by simpa [hpa] using h1
          exact Nat.eq_zero_of_le_zero hh
        have hpos : 0 < w a * p b := Nat.mul_pos hwa hpbpos
        omega
      · have hpapos : 0 < p a := Nat.pos_of_ne_zero hpa
        by_cases hwb0 : w b = 0
        · have hh : w a * p b ≤ 0 := by simpa [hwb0] using h1
          have hz : w a * p b = 0 := Nat.eq_zero_of_le_zero hh
          have hwazero : w a = 0 := by
            apply Nat.eq_of_mul_eq_mul_right hpbpos
            simpa [hwb0] using hz
          simp [hwazero]
        · have hwbpos : 0 < w b := Nat.pos_of_ne_zero hwb0
          have h1' : w a * p b * p c ≤ w b * p a * p c := Nat.mul_le_mul_right (p c) h1
          have h2' : w b * p c * p a ≤ w c * p b * p a := Nat.mul_le_mul_right (p a) h2
          have h2'' : w b * p a * p c ≤ w c * p b * p a := by
            simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using h2'
          have hchain := Nat.le_trans h1' h2''
          have hcancel : (w a * p c) * p b ≤ (w c * p a) * p b := by
            simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using hchain
          exact Nat.le_of_mul_le_mul_right hcancel hpbpos

private theorem cpass_eq (d : Nat) (t : Test) (h : t.pass ≤ d) : cpass d t = t.pass := by
  simp [cpass, Nat.min_eq_left h]

private theorem cprice_eq (d : Nat) (t : Test) (hg : good d t) : cprice d t = t.price := by
  unfold good at hg
  unfold cprice
  by_cases h : t.price = 0 ∧ cpass d t = d
  · exfalso
    rcases h with ⟨hp0, hpd⟩
    have hcp : cpass d t = t.pass := cpass_eq d t hg.1
    rcases hg.2 with hpos | hlt <;> omega
  · simp [h]

private theorem agree (d : Nat) {a b : Test} (ha : good d a) (hb : good d b) :
    byRatio d a b = cle d a b := by
  simp [byRatio, cle, cpass_eq d a ha.1, cpass_eq d b hb.1,
        cprice_eq d a ha, cprice_eq d b hb]

private theorem cle_nonzero (d : Nat) (t : Test) :
    0 < cprice d t ∨ 0 < d - cpass d t := by
  unfold cprice
  by_cases h : t.price = 0 ∧ cpass d t = d
  · simp [h]
  · simp [h]
    by_cases hp : t.price = 0
    · right
      have hle : cpass d t ≤ d := Nat.min_le_right _ _
      have hlt : cpass d t < d := by omega
      omega
    · left
      omega

private theorem cle_trans (d : Nat) : ∀ a b c : Test,
    cle d a b = true → cle d b c = true → cle d a c = true := by
  intro a b c hab hbc
  have h1 : cprice d a * (d - cpass d b) ≤ cprice d b * (d - cpass d a) := by
    simpa [cle] using hab
  have h2 : cprice d b * (d - cpass d c) ≤ cprice d c * (d - cpass d b) := by
    simpa [cle] using hbc
  simpa [cle] using
    ratio_trans (cprice d) (fun t => d - cpass d t) a b c
      (cle_nonzero d a) (cle_nonzero d b) h1 h2

private theorem cle_total (d : Nat) : ∀ a b : Test, (cle d a b || cle d b a) = true := by
  intro a b
  have h := Nat.le_total (cprice d a * (d - cpass d b)) (cprice d b * (d - cpass d a))
  simpa [cle] using h

/-- The two comparators coincide on all admissible tests. -/
private theorem merge_congr_aux {α : Type} (le₁ le₂ : α → α → Bool) :
    ∀ n, ∀ xs ys : List α, xs.length + ys.length = n →
      (∀ a ∈ xs, ∀ b ∈ ys, le₁ a b = le₂ a b) →
      List.merge xs ys le₁ = List.merge xs ys le₂ := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro xs ys hn h
    cases xs with
    | nil => simp
    | cons x xs =>
      cases ys with
      | nil => simp
      | cons y ys =>
        simp only [List.merge]
        have hn0 : xs.length + 1 + (ys.length + 1) = n := by simpa using hn
        have hn2 : xs.length + 1 + ys.length + 1 = n := by omega
        by_cases hxy : le₁ x y = true
        · have hxy2 : le₂ x y = true := by
            rw [← h x (by simp) y (by simp)]
            exact hxy
          simp [hxy, hxy2]
          have hlt : xs.length + 1 + ys.length < n := by omega
          apply ih (xs.length + 1 + ys.length) hlt
          · simp [Nat.add_comm, Nat.add_left_comm]
          · intro a ha b hb
            exact h a (by simp [ha]) b (by simp [hb])
        · have hxy2 : le₂ x y ≠ true := by
            intro hh
            apply hxy
            rwa [h x (by simp) y (by simp)]
          simp [hxy, hxy2]
          have hlt : xs.length + 1 + ys.length < n := by omega
          apply ih (xs.length + 1 + ys.length) hlt
          · simp [Nat.add_comm, Nat.add_left_comm]
          · intro a ha b hb
            exact h a (by simp [ha]) b (by simp [hb])

private theorem merge_congr {α : Type} (le₁ le₂ : α → α → Bool) (xs ys : List α)
    (h : ∀ a ∈ xs, ∀ b ∈ ys, le₁ a b = le₂ a b) :
    List.merge xs ys le₁ = List.merge xs ys le₂ :=
  merge_congr_aux le₁ le₂ (xs.length + ys.length) xs ys rfl h

private theorem mergeSort_unfold {α : Type} (a b : α) (xs : List α) (le : α → α → Bool) :
    (a :: b :: xs).mergeSort le =
      List.merge (((a :: b :: xs).take ((xs.length + 3) / 2)).mergeSort le)
        (((a :: b :: xs).drop ((xs.length + 3) / 2)).mergeSort le) le := by
  rw [List.mergeSort]
  rw [List.MergeSort.Internal.splitInTwo_fst]
  rw [List.MergeSort.Internal.splitInTwo_snd]
  rfl

private theorem mergeSort_congr_aux (d : Nat) : ∀ n, ∀ ts : List Test, ts.length = n →
    (∀ t ∈ ts, good d t) → ts.mergeSort (byRatio d) = ts.mergeSort (cle d) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro ts hlen hg
    cases ts with
    | nil => simp
    | cons a tl =>
      cases tl with
      | nil => simp
      | cons b rest =>
        let k := (rest.length + 3) / 2
        let l := (a :: b :: rest).take k
        let r := (a :: b :: rest).drop k
        have hlenS : (a :: b :: rest).length = n := hlen
        have hlenR : rest.length + 2 = n := by simpa using hlen
        have hlgood : ∀ t ∈ l, good d t := by
          intro t ht
          exact hg t (List.mem_of_mem_take ht)
        have hrgood : ∀ t ∈ r, good d t := by
          intro t ht
          exact hg t (List.mem_of_mem_drop ht)
        have hkpos : 0 < k := by dsimp [k]; omega
        have hklt : k < n := by dsimp [k]; omega
        have hltl : l.length < n := by
          dsimp [l]
          rw [List.length_take]
          have hmin : min k (a :: b :: rest).length ≤ k := Nat.min_le_left _ _
          exact Nat.lt_of_le_of_lt hmin hklt
        have hltr : r.length < n := by
          dsimp [r]
          rw [List.length_drop]
          omega
        have ihl : l.mergeSort (byRatio d) = l.mergeSort (cle d) := ih l.length hltl l rfl hlgood
        have ihr : r.mergeSort (byRatio d) = r.mergeSort (cle d) := ih r.length hltr r rfl hrgood
        rw [mergeSort_unfold a b rest (byRatio d), mergeSort_unfold a b rest (cle d)]
        dsimp [l, r, k] at ihl ihr ⊢
        rw [ihl, ihr]
        apply merge_congr
        intro x hx y hy
        have hxg : good d x := by
          have hxl : x ∈ (a :: b :: rest).take ((rest.length + 3) / 2) := by
            simpa using (List.mem_mergeSort.mp hx)
          exact hlgood x hxl
        have hyg : good d y := by
          have hyr : y ∈ (a :: b :: rest).drop ((rest.length + 3) / 2) := by
            simpa using (List.mem_mergeSort.mp hy)
          exact hrgood y hyr
        exact agree d hxg hyg

private theorem mergeSort_congr (d : Nat) (ts : List Test) (hg : ∀ t ∈ ts, good d t) :
    ts.mergeSort (byRatio d) = ts.mergeSort (cle d) :=
  mergeSort_congr_aux d ts.length ts rfl hg

private theorem bill_cons_cons (d : Nat) (a b : Test) (r : List Test) :
    bill d (a :: b :: r) = (a.price * d + a.pass * b.price) * d ^ r.length +
      a.pass * b.pass * bill d r := by
  simp [bill, Nat.pow_succ, Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm,
    Nat.mul_left_comm, Nat.add_assoc]

private theorem bill_cons_le (d : Nat) (a : Test) {x y : List Test}
    (hlen : x.length = y.length) (hxy : bill d x ≤ bill d y) :
    bill d (a :: x) ≤ bill d (a :: y) := by
  rw [bill, bill, hlen]
  exact Nat.add_le_add_left (Nat.mul_le_mul_left _ hxy) _

/-- Smith's adjacent-exchange inequality. -/
private theorem bill_swap (d : Nat) (a b : Test) (ha : a.pass ≤ d) (hb : b.pass ≤ d)
    (hab : byRatio d a b = true) (r : List Test) :
    bill d (a :: b :: r) ≤ bill d (b :: a :: r) := by
  have hcond : a.price * (d - b.pass) ≤ b.price * (d - a.pass) := by
    simpa [byRatio] using hab
  have hsub : a.price * d - a.price * b.pass ≤ b.price * d - b.price * a.pass := by
    simpa [Nat.mul_sub_left_distrib] using hcond
  have hX : a.price * b.pass ≤ a.price * d := Nat.mul_le_mul_left a.price hb
  have hY : b.price * a.pass ≤ b.price * d := Nat.mul_le_mul_left b.price ha
  have hcore : a.price * d + b.price * a.pass ≤ b.price * d + a.price * b.pass := by omega
  rw [bill_cons_cons, bill_cons_cons]
  have hcore' : a.price * d + a.pass * b.price ≤ b.price * d + b.pass * a.price := by
    simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc, Nat.add_comm, Nat.add_left_comm,
      Nat.add_assoc] using hcore
  have hmul : (a.price * d + a.pass * b.price) * d ^ r.length ≤
      (b.price * d + b.pass * a.price) * d ^ r.length := Nat.mul_le_mul_right _ hcore'
  simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc, Nat.add_comm, Nat.add_left_comm,
    Nat.add_assoc] using Nat.add_le_add_right hmul (a.pass * b.pass * bill d r)

/-- Moving an element to the front never increases the cost if it is no worse than everything it crosses. -/
private theorem bill_move_front (d : Nat) (a : Test) (p q : List Test)
    (hle : ∀ z ∈ p, byRatio d a z = true) (hpa : a.pass ≤ d)
    (hpp : ∀ z ∈ p, z.pass ≤ d) :
    bill d (a :: p ++ q) ≤ bill d (p ++ a :: q) := by
  induction p with
  | nil => simp
  | cons z p ih =>
    have hzle : byRatio d a z = true := hle z (by simp)
    have hzpass : z.pass ≤ d := hpp z (by simp)
    have h1 : bill d (a :: z :: (p ++ q)) ≤ bill d (z :: a :: (p ++ q)) :=
      bill_swap d a z hpa hzpass hzle (p ++ q)
    have ih' : bill d (a :: (p ++ q)) ≤ bill d (p ++ a :: q) := by
      apply ih
      · intro y hy
        exact hle y (by simp [hy])
      · intro y hy
        exact hpp y (by simp [hy])
    have hlen : (a :: (p ++ q)).length = (p ++ a :: q).length := by
      simp
      omega
    have h2 : bill d (z :: a :: (p ++ q)) ≤ bill d (z :: (p ++ a :: q)) :=
      bill_cons_le d z hlen ih'
    simpa using Nat.le_trans h1 h2

private theorem perm_move_front (a : Test) (p q : List Test) :
    (p ++ a :: q).Perm (a :: (p ++ q)) := by
  induction p with
  | nil => simp
  | cons b p ih =>
    have h1 : (b :: p ++ a :: q).Perm (b :: a :: (p ++ q)) := List.Perm.cons b ih
    have h2 : (b :: a :: (p ++ q)).Perm (a :: b :: (p ++ q)) := List.Perm.swap a b (p ++ q)
    exact h1.trans h2

private theorem main (d : Nat) : ∀ n, ∀ ts : List Test, ts.length = n →
    (∀ t ∈ ts, good d t) → ∀ alt : List Test, ts.Perm alt →
    bill d (ts.mergeSort (cle d)) ≤ bill d alt := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro ts hlen hgood alt hperm
    cases ts with
    | nil =>
        have halt : alt = [] := by simpa using hperm
        subst alt
        simp [bill]
    | cons t0 tl =>
      let s := (t0 :: tl).mergeSort (cle d)
      change bill d s ≤ bill d alt
      have hpair : List.Pairwise (fun x y : Test => cle d x y = true) s := by
        simpa [s] using List.pairwise_mergeSort (cle_trans d) (cle_total d) (t0 :: tl)
      have hsperm : s.Perm (t0 :: tl) := by
        simpa [s] using List.mergeSort_perm (t0 :: tl) (cle d)
      cases hs : s with
      | nil =>
        have hz := hsperm.length_eq
        simp [hs] at hz
      | cons b s' =>
        change bill d (b :: s') ≤ bill d alt
        have hpair' : List.Pairwise (fun x y : Test => cle d x y = true) (b :: s') := by
          simpa [hs] using hpair
        have hbts : b ∈ t0 :: tl := hsperm.mem_iff.mp (by simp [hs])
        have hbgood : good d b := hgood b hbts
        have hs'good : ∀ t ∈ s', good d t := by
          intro t ht
          exact hgood t (hsperm.mem_iff.mp (by simp [hs, ht]))
        have hbin_alt : b ∈ alt := hperm.mem_iff.mp hbts
        obtain ⟨p, q, halt, _⟩ := List.eq_append_cons_of_mem hbin_alt
        subst alt
        have hpgood : ∀ z ∈ p, good d z := by
          intro z hz
          exact hgood z (hperm.mem_iff.mpr (by simp [hz]))
        have hple : ∀ z ∈ p, byRatio d b z = true := by
          intro z hz
          have hzts : z ∈ t0 :: tl := hperm.mem_iff.mpr (by simp [hz])
          have hzs : z ∈ s := hsperm.mem_iff.mpr hzts
          have hzcons : z ∈ b :: s' := by simpa [hs] using hzs
          by_cases hzb : z = b
          · subst z
            simp [byRatio]
          · have hzs' : z ∈ s' := by simpa [hzb] using hzcons
            have hcle : cle d b z = true := (List.pairwise_cons.mp hpair').1 z hzs'
            rwa [← agree d hbgood (hpgood z hz)] at hcle
        have hp_pass : ∀ z ∈ p, z.pass ≤ d := fun z hz => (hpgood z hz).1
        have hstep1 : bill d (b :: (p ++ q)) ≤ bill d (p ++ b :: q) :=
          bill_move_front d b p q hple hbgood.1 hp_pass
        have hpf : (p ++ b :: q).Perm (b :: (p ++ q)) := perm_move_front b p q
        have h1 : (b :: (p ++ q)).Perm (t0 :: tl) := hpf.symm.trans hperm.symm
        have hsback : (t0 :: tl).Perm (b :: s') := by simpa [hs] using hsperm.symm
        have h2 : (b :: (p ++ q)).Perm (b :: s') := h1.trans hsback
        have hps : (p ++ q).Perm s' := List.Perm.cons_inv h2
        have hslen0 : s.length = n := by
          have hlm : ((t0 :: tl).mergeSort (cle d)).length = (t0 :: tl).length :=
            List.length_mergeSort (t0 :: tl) (le := cle d)
          simp [s, hlen] at hlm ⊢
        have hslen : (b :: s').length = n := by simpa [hs] using hslen0
        have hslen' : s'.length + 1 = n := by simpa using hslen
        have hs'len : s'.length < n := by omega
        have ihs' := ih s'.length hs'len s' rfl hs'good (p ++ q) hps.symm
        have hs'sorted : s'.mergeSort (cle d) = s' :=
          List.mergeSort_of_pairwise (List.pairwise_cons.mp hpair').2
        have hstep2 : bill d s' ≤ bill d (p ++ q) := by
          simpa [hs'sorted] using ihs'
        have hstep3 : bill d (b :: s') ≤ bill d (b :: (p ++ q)) :=
          bill_cons_le d b hps.length_eq.symm hstep2
        exact Nat.le_trans hstep3 hstep1

theorem sound_RatioSort : PredicateOrder.RatioSort.Goal := by
  intro d ts alt hd hvalid hnd hperm
  have hg : ∀ t ∈ ts, good d t := by
    intro t ht
    exact ⟨hvalid t ht, hnd t ht⟩
  rw [mergeSort_congr d ts hg]
  exact main d ts.length ts rfl hg alt hperm

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort
