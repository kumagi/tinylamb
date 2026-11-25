import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

universe u

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope


private def good (d : Nat) (t : Test) : Prop := t.pass ≤ d ∧ (0 < t.price ∨ t.pass < d)

private def passProd : List Test → Nat
  | [] => 1
  | t :: ts => t.pass * passProd ts

private theorem pow_add_nat (d a b : Nat) : d ^ (a + b) = d ^ a * d ^ b := by
  induction b with
  | zero => simp
  | succ b ih =>
      rw [Nat.add_succ, Nat.pow_succ, ih, Nat.pow_succ]
      ac_rfl

private theorem bill_append (d : Nat) (p q : List Test) :
    bill d (p ++ q) = d ^ q.length * bill d p + passProd p * bill d q := by
  induction p with
  | nil => simp [bill, passProd]
  | cons t ts ih =>
      simp only [List.cons_append, bill]
      rw [List.length_append, pow_add_nat, ih]
      simp [passProd, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
      omega

private theorem local_swap {d : Nat} {a b : Test}
    (ha : good d a) (hb : good d b)
    (hle : byRatio d a b = true) (rest : List Test) :
    bill d (a :: b :: rest) ≤ bill d (b :: a :: rest) := by
  have hab : a.price * (d - b.pass) ≤ b.price * (d - a.pass) := of_decide_eq_true hle
  have ha' : a.pass ≤ d := ha.1
  have hb' : b.pass ≤ d := hb.1
  have hcore : a.price * d + a.pass * b.price ≤ b.price * d + b.pass * a.price := by
    rw [Nat.mul_sub_left_distrib, Nat.mul_sub_left_distrib] at hab
    have hAQ : a.price * b.pass ≤ a.price * d := Nat.mul_le_mul_left a.price hb'
    have hXP : b.price * a.pass ≤ b.price * d := Nat.mul_le_mul_left b.price ha'
    have habZ : ((a.price*d - a.price*b.pass : Nat) : Int) ≤
        ((b.price*d - b.price*a.pass : Nat) : Int) := by exact_mod_cast hab
    rw [Int.ofNat_sub hAQ, Int.ofNat_sub hXP] at habZ
    rw [Nat.mul_comm a.pass b.price, Nat.mul_comm b.pass a.price]
    omega
  have hmul := Nat.mul_le_mul_right (d ^ rest.length) hcore
  change a.price * d ^ (rest.length + 1) + a.pass * (b.price * d ^ rest.length + b.pass * bill d rest) ≤
      b.price * d ^ (rest.length + 1) + b.pass * (a.price * d ^ rest.length + a.pass * bill d rest)
  rw [Nat.pow_succ]
  simp only [Nat.mul_add, Nat.add_mul] at hmul ⊢
  have h1 : a.price * (d ^ rest.length * d) = a.price * d * d ^ rest.length := by ac_rfl
  have h2 : a.pass * (b.price * d ^ rest.length) = a.pass * b.price * d ^ rest.length := by ac_rfl
  have h3 : b.price * (d ^ rest.length * d) = b.price * d * d ^ rest.length := by ac_rfl
  have h4 : b.pass * (a.price * d ^ rest.length) = b.pass * a.price * d ^ rest.length := by ac_rfl
  have h5 : a.pass * (b.pass * bill d rest) = b.pass * (a.pass * bill d rest) := by ac_rfl
  rw [h1, h2, h3, h4, h5]
  omega

private theorem bill_cons_mono {d : Nat} {t : Test} {L M : List Test}
    (hlen : L.length = M.length) (h : bill d L ≤ bill d M) :
    bill d (t :: L) ≤ bill d (t :: M) := by
  change t.price * d ^ L.length + t.pass * bill d L ≤
    t.price * d ^ M.length + t.pass * bill d M
  rw [hlen]
  exact Nat.add_le_add_left (Nat.mul_le_mul_left t.pass h) _

private theorem bill_swap_prefix {d : Nat} {a b : Test}
    (ha : good d a) (hb : good d b) (hle : byRatio d a b = true)
    (p rest : List Test) :
    bill d (p ++ a :: b :: rest) ≤ bill d (p ++ b :: a :: rest) := by
  rw [bill_append, bill_append]
  have hlen : (a :: b :: rest).length = (b :: a :: rest).length := by simp
  rw [hlen]
  have hscaled := Nat.mul_le_mul_left (passProd p) (local_swap ha hb hle rest)
  omega

private theorem bill_move_front (d : Nat) {a : Test} (ha : good d a) (u v : List Test)
    (hgood : ∀ t ∈ u ++ v, good d t)
    (hle : ∀ x ∈ u, byRatio d a x = true) :
    bill d (a :: (u ++ v)) ≤ bill d (u ++ a :: v) := by
  induction u with
  | nil => simp
  | cons x u' ih =>
      have hxle : byRatio d a x = true := hle x (by simp)
      have hxu : ∀ y ∈ u', byRatio d a y = true := by
        intro y hy; exact hle y (by simp [hy])
      have hgoodx : good d x := hgood x (by simp)
      have hgoodu : ∀ t ∈ u' ++ v, good d t := by
        intro t ht; exact hgood t (by simp [ht])
      have ih' := ih hgoodu hxu
      have hlocal := bill_swap_prefix (d:=d) ha hgoodx hxle ([] : List Test) (u' ++ v)
      simp only [List.nil_append] at hlocal
      have hlen : (a :: (u' ++ v)).length = (u' ++ a :: v).length := by
        simp [List.length_append]
        omega
      have hscaled := bill_cons_mono (t:=x) hlen ih'
      exact Nat.le_trans hlocal hscaled

private theorem perm_append_cons {α : Type} (u : List α) (a : α) (v : List α) :
    (u ++ a :: v).Perm (a :: (u ++ v)) := by
  induction u with
  | nil => simp
  | cons x xs ih =>
      have h1 : (x :: (xs ++ a :: v)).Perm (x :: (a :: (xs ++ v))) := ih.cons x
      have h2 : (x :: (a :: (xs ++ v))).Perm (a :: (x :: (xs ++ v))) :=
        List.Perm.swap a x (xs ++ v)
      simpa using h1.trans h2

private def key (d : Nat) (t : Test) : Nat × Nat :=
  if t.pass < d then (t.price, d - t.pass)
  else if t.price == 0 then (0, 1) else (1, 0)

private def leExt (d : Nat) (a b : Test) : Bool :=
  decide ((key d a).1 * (key d b).2 ≤ (key d b).1 * (key d a).2)

private theorem key_spec (d : Nat) (t : Test) :
    (key d t).2 = 0 → (key d t).1 = 1 := by
  unfold key
  by_cases h : t.pass < d
  · simp [h]
    omega
  · by_cases h0 : t.price == 0
    · simp [h, h0]
    · simp [h, h0]

private theorem key_spec_zero (d : Nat) (t : Test) :
    (key d t).1 = 0 → 0 < (key d t).2 := by
  unfold key
  by_cases h : t.pass < d
  · simp [h]
    omega
  · by_cases h0 : t.price == 0
    · simp [h, h0]
    · simp [h, h0]

private theorem cross_trans {n1 n2 n3 d1 d2 d3 : Nat} (hd2 : 0 < d2)
    (h12 : n1 * d2 ≤ n2 * d1) (h23 : n2 * d3 ≤ n3 * d2) : n1 * d3 ≤ n3 * d1 := by
  have h12' := Nat.mul_le_mul_right d3 h12
  have h23' := Nat.mul_le_mul_left d1 h23
  have hA : (n1 * d2) * d3 = (n1 * d3) * d2 := by ac_rfl
  have hB : (n2 * d1) * d3 = (n2 * d3) * d1 := by ac_rfl
  have hC : d1 * (n2 * d3) = (n2 * d3) * d1 := by ac_rfl
  have hD : d1 * (n3 * d2) = (n3 * d1) * d2 := by ac_rfl
  rw [hA, hB] at h12'
  rw [hC, hD] at h23'
  exact Nat.le_of_mul_le_mul_right (Nat.le_trans h12' h23') hd2

private theorem leExt_trans (d : Nat) (a b c : Test) :
    leExt d a b = true → leExt d b c = true → leExt d a c = true := by
  intro hab hbc
  unfold leExt at hab hbc ⊢
  have hab' : (key d a).1 * (key d b).2 ≤ (key d b).1 * (key d a).2 := of_decide_eq_true hab
  have hbc' : (key d b).1 * (key d c).2 ≤ (key d c).1 * (key d b).2 := of_decide_eq_true hbc
  apply decide_eq_true
  let ka := key d a
  let kb := key d b
  let kc := key d c
  change ka.1 * kb.2 ≤ kb.1 * ka.2 at hab'
  change kb.1 * kc.2 ≤ kc.1 * kb.2 at hbc'
  change ka.1 * kc.2 ≤ kc.1 * ka.2
  have hspecb : kb.2 = 0 → kb.1 = 1 := by dsimp [kb]; exact key_spec d b
  have hspecc : kc.2 = 0 → kc.1 = 1 := by dsimp [kc]; exact key_spec d c
  by_cases hb0 : kb.2 = 0
  · have hbnum : kb.1 = 1 := hspecb hb0
    rw [hb0, hbnum] at hab' hbc'
    simp at hab'
    have hc0 : kc.2 = 0 := by omega
    rw [hc0, hspecc hc0]
    simp
  · have hbpos : 0 < kb.2 := Nat.pos_of_ne_zero hb0
    by_cases ha0 : ka.2 = 0
    · have hanum : ka.1 = 1 := by dsimp [ka]; exact key_spec d a ha0
      rw [ha0, hanum] at hab'
      simp at hab'
      omega
    · have hapos : 0 < ka.2 := Nat.pos_of_ne_zero ha0
      by_cases hc0 : kc.2 = 0
      · rw [hc0, hspecc hc0]
        simp
      · have hcpos : 0 < kc.2 := Nat.pos_of_ne_zero hc0
        exact cross_trans hbpos hab' hbc'

private theorem leExt_total (d : Nat) (a b : Test) :
    (leExt d a b || leExt d b a) = true := by
  unfold leExt
  let ka := key d a
  let kb := key d b
  change (decide (ka.1 * kb.2 ≤ kb.1 * ka.2) ||
    decide (kb.1 * ka.2 ≤ ka.1 * kb.2)) = true
  by_cases hp : ka.1 * kb.2 ≤ kb.1 * ka.2
  · simp [hp]
  · have hq : kb.1 * ka.2 ≤ ka.1 * kb.2 := by
      by_cases ha0 : ka.2 = 0
      · have hanum : ka.1 = 1 := by dsimp [ka]; exact key_spec d a ha0
        rw [ha0, hanum]
        simp
      · have hapos : 0 < ka.2 := Nat.pos_of_ne_zero ha0
        by_cases hb0 : kb.2 = 0
        · have hbnum : kb.1 = 1 := by dsimp [kb]; exact key_spec d b hb0
          rw [hb0, hbnum] at hp ⊢
          simp at hp
        · have hbpos : 0 < kb.2 := Nat.pos_of_ne_zero hb0
          exact Nat.le_of_not_ge hp
    simp [hp, decide_eq_true hq]

private theorem leExt_eq_byRatio {d : Nat} {a b : Test} (ha : good d a) (hb : good d b) :
    leExt d a b = byRatio d a b := by
  unfold leExt byRatio key
  have hapos_of_not_lt (t : Test) (ht : good d t) (h : ¬ t.pass < d) : 0 < t.price := by
    have hpdt : t.pass = d := by have hle := ht.1; omega
    rcases ht.2 with hpos | hlt
    · exact hpos
    · omega
  by_cases hap : a.pass < d
  · by_cases hbp : b.pass < d
    · simp [hap, hbp]
    · have hbpd : b.pass = d := by have hle := hb.1; omega
      have hbne : (b.price == 0) = false := by
        simp [Nat.ne_of_gt (hapos_of_not_lt b hb hbp)]
      simp [hap, hbp, hbpd, hbne]
  · have hapd : a.pass = d := by have hle := ha.1; omega
    have hane : (a.price == 0) = false := by
      simp [Nat.ne_of_gt (hapos_of_not_lt a ha hap)]
    by_cases hbp : b.pass < d
    · simp [hap, hbp, hane, hapd]
      have hdiff : 0 < d - b.pass := by omega
      have hleft : 0 < a.price * (d - b.pass) := Nat.mul_pos (hapos_of_not_lt a ha hap) hdiff
      omega
    · have hbpd : b.pass = d := by have hle := hb.1; omega
      have hbne : (b.price == 0) = false := by
        simp [Nat.ne_of_gt (hapos_of_not_lt b hb hbp)]
      simp [hap, hbp, hane, hbne, hapd, hbpd]


private theorem bill_min (d : Nat) {s alt : List Test}
    (hgood : ∀ t ∈ s, good d t)
    (hs : List.Pairwise (fun a b => byRatio d a b = true) s)
    (hperm : s.Perm alt) : bill d s ≤ bill d alt := by
  induction s generalizing alt with
  | nil =>
      have halt : alt = [] := List.perm_nil.mp hperm.symm
      simp [halt]
  | cons a s' ih =>
      have hga : good d a := hgood a (by simp)
      have hgs : ∀ t ∈ s', good d t := by
        intro t ht; exact hgood t (by simp [ht])
      have hpw := List.pairwise_cons.mp hs
      have hpair' := hpw.2
      have hle_all : ∀ x ∈ s', byRatio d a x = true := hpw.1
      have haAlt : a ∈ alt := (hperm.mem_iff).mp (by simp)
      obtain ⟨u, v, halt, hu⟩ := List.eq_append_cons_of_mem haAlt
      subst alt
      have h1 : (u ++ a :: v).Perm (a :: (u ++ v)) := perm_append_cons u a v
      have h2 : (a :: (u ++ v)).Perm (a :: s') := h1.symm.trans hperm.symm
      have hperm' : (u ++ v).Perm s' := (List.perm_cons a).mp h2
      have ih' := ih hgs hpair' hperm'.symm
      have hlen := hperm'.length_eq
      have hfirst : bill d (a :: s') ≤ bill d (a :: (u ++ v)) := by
        change a.price * d ^ s'.length + a.pass * bill d s' ≤
          a.price * d ^ (u ++ v).length + a.pass * bill d (u ++ v)
        rw [hlen]
        exact Nat.add_le_add_left (Nat.mul_le_mul_left a.pass ih') _
      have hrest : bill d (a :: (u ++ v)) ≤ bill d (u ++ a :: v) := by
        apply bill_move_front d hga u v
        · intro t ht; exact hgs t ((hperm'.mem_iff).mp ht)
        · intro x hx
          exact hle_all x ((hperm'.mem_iff).mp (by simp [hx]))
      exact Nat.le_trans hfirst hrest

private def Good (d : Nat) := {t : Test // good d t}

private def leG (d : Nat) (a b : Good d) : Bool := byRatio d a.val b.val

private theorem leG_trans (d : Nat) : ∀ a b c : Good d,
    leG d a b = true → leG d b c = true → leG d a c = true := by
  intro a b c hab hbc
  unfold leG at hab hbc ⊢
  rw [← leExt_eq_byRatio a.property b.property] at hab
  rw [← leExt_eq_byRatio b.property c.property] at hbc
  rw [← leExt_eq_byRatio a.property c.property]
  exact leExt_trans d a.val b.val c.val hab hbc

private theorem leG_total (d : Nat) : ∀ a b : Good d, (leG d a b || leG d b a) = true := by
  intro a b
  unfold leG
  rw [← leExt_eq_byRatio a.property b.property, ← leExt_eq_byRatio b.property a.property]
  exact leExt_total d a.val b.val

private theorem map_pmap_val {α : Type} {p : α → Prop} (l : List α) (H : ∀ a ∈ l, p a) :
    (l.pmap (P := fun a => a ∈ l ∧ p a) (fun a ht => (⟨a, ht.2⟩ : {x : α // p x}))
      (fun a ha => ⟨ha, H a ha⟩)).map Subtype.val = l := by
  rw [List.map_pmap]
  simpa using (List.pmap_eq_map (p := fun a => a ∈ l ∧ p a) (f := fun a : α => a)
    (l := l) (H := fun a ha => ⟨ha, H a ha⟩))

theorem RatioSort_sound : TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder.RatioSort.Goal := by
  intro d ts alt hd hv hpos hperm
  let H : ∀ t ∈ ts, good d t := by
    intro t ht
    exact ⟨hv t ht, hpos t ht⟩
  let tsG : List (Good d) := ts.pmap (P := fun t => t ∈ ts ∧ good d t)
    (fun t ht => (⟨t, ht.2⟩ : Good d)) (fun t ht => ⟨ht, H t ht⟩)
  have hval : tsG.map Subtype.val = ts := by
    dsimp [tsG]
    exact map_pmap_val ts H
  let sortedG := tsG.mergeSort (leG d)
  have hpwG : List.Pairwise (fun a b => leG d a b = true) sortedG := by
    dsimp [sortedG]
    exact List.pairwise_mergeSort (leG_trans d) (leG_total d) tsG
  have hpwVal : List.Pairwise (fun a b => byRatio d a b = true) (sortedG.map Subtype.val) := by
    exact hpwG.map Subtype.val (by intro a b h; exact h)
  have hmapmerge : sortedG.map Subtype.val = (tsG.map Subtype.val).mergeSort (byRatio d) := by
    dsimp [sortedG]
    exact List.map_mergeSort (r := leG d) (s := byRatio d) (f := Subtype.val) (l := tsG) (by intro a ha b hb; rfl)
  have hsortedEq : sortedG.map Subtype.val = ts.mergeSort (byRatio d) := by
    rw [hmapmerge, hval]
  have hgoodSorted : ∀ t ∈ sortedG.map Subtype.val, good d t := by
    intro t ht
    obtain ⟨g, hg, rfl⟩ := List.mem_map.mp ht
    exact g.property
  have hspermG : (sortedG.map Subtype.val).Perm ts := by
    have hpm : (sortedG.map Subtype.val).Perm (tsG.map Subtype.val) := by
      dsimp [sortedG]
      exact (List.mergeSort_perm tsG (leG d)).map Subtype.val
    rw [hval] at hpm
    exact hpm
  have hsperm : (sortedG.map Subtype.val).Perm alt := hspermG.trans hperm
  have hmin := bill_min d hgoodSorted hpwVal hsperm
  rwa [hsortedEq] at hmin

private theorem actual_append (ts1 ts2 : List (Nat × (Nat → Bool))) (rows : List Nat) :
    PredicateOrder.actual (ts1 ++ ts2) rows =
      PredicateOrder.actual ts1 rows + PredicateOrder.actual ts2 (ts1.foldl (fun rs t => rs.filter t.2) rows) := by
  induction ts1 generalizing rows with
  | nil => simp [PredicateOrder.actual]
  | cons x xs ih =>
      rcases x with ⟨c, p⟩
      simp [List.cons_append, PredicateOrder.actual]
      rw [ih (rows.filter p)]
      omega

private theorem filteredBy_subset (ts : List (Nat × (Nat → Bool))) (rows : List Nat) {x : Nat}
    (hx : x ∈ ts.foldl (fun rs t => rs.filter t.2) rows) : x ∈ rows := by
  induction ts generalizing rows with
  | nil => simpa using hx
  | cons y ys ih =>
      rcases y with ⟨c, p⟩
      simp at hx
      have hx' := ih (rows.filter p) hx
      exact (List.mem_filter.mp hx').1

private theorem filteredBy_eq_filter (ts : List (Nat × (Nat → Bool))) (rows : List Nat) :
    ts.foldl (fun rs t => rs.filter t.2) rows = rows.filter (fun r => ts.all (fun t => t.2 r)) := by
  induction ts generalizing rows with
  | nil =>
      symm
      apply List.filter_eq_self.mpr
      intro r hr
      rfl
  | cons x xs ih =>
      rcases x with ⟨c, p⟩
      change xs.foldl (fun rs t => rs.filter t.2) (rows.filter p) =
        rows.filter (fun r => p r && xs.all (fun t => t.2 r))
      rw [ih (rows.filter p), List.filter_filter]
      apply List.filter_congr
      intro r hr
      exact Bool.and_comm _ _

private theorem filteredBy_length_le (ts : List (Nat × (Nat → Bool))) (rows : List Nat) :
    (ts.foldl (fun rs t => rs.filter t.2) rows).length ≤ rows.length := by
  induction ts generalizing rows with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨c, p⟩
      simp
      exact Nat.le_trans (ih (rows.filter p)) (List.length_filter_le _ _)

theorem ConditionalSwap_sound :
    PredicateOrder.ConditionalSwap.Goal := by
  intro ca cb pa pb rest rows
  have hA : PredicateOrder.actual ((ca,pa)::(cb,pb)::rest) rows
      = ca * rows.length + cb * (rows.filter pa).length +
        PredicateOrder.actual rest ((rows.filter pa).filter pb) := by
    simp [PredicateOrder.actual]
    omega
  have hB : PredicateOrder.actual ((cb,pb)::(ca,pa)::rest) rows
      = cb * rows.length + ca * (rows.filter pb).length +
        PredicateOrder.actual rest ((rows.filter pb).filter pa) := by
    simp [PredicateOrder.actual]
    omega
  have hfilt : (rows.filter pa).filter pb = (rows.filter pb).filter pa := by
    rw [List.filter_filter, List.filter_filter]
    apply List.filter_congr
    intro x _
    simp [Bool.and_comm]
  rw [hA, hB, hfilt]
  have hcomp (p : Nat → Bool) :
      rows.length - (rows.filter p).length = (rows.filter (fun x => !p x)).length := by
    have h := List.length_eq_countP_add_countP (p := p) (l := rows)
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at h
    have heq : (rows.filter (fun a => decide (¬ p a = true))) = rows.filter (fun x => !p x) := by
      apply List.filter_congr
      intro x hx
      cases p x <;> rfl
    rw [heq] at h
    omega
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  rw [← hcomp pb, ← hcomp pa]
  rw [Int.ofNat_sub (List.length_filter_le pb rows),
      Int.ofNat_sub (List.length_filter_le pa rows)]
  rw [Int.mul_sub, Int.mul_sub]
  push_cast
  omega

theorem RepeatedGuard_sound :
    PredicateOrder.RepeatedGuard.Goal := by
  intro c p middle tail rows
  let A := (c, p) :: middle
  have horig := actual_append A ((c, p) :: tail) rows
  have hopt := actual_append A tail rows
  have hp : (A.foldl (fun rs t => rs.filter t.2) rows).filter p =
      A.foldl (fun rs t => rs.filter t.2) rows := by
    apply List.filter_eq_self.mpr
    intro x hx
    change x ∈ middle.foldl (fun rs t => rs.filter t.2) (rows.filter p) at hx
    exact (List.mem_filter.mp (filteredBy_subset middle (rows.filter p) hx)).2
  have hcomm : (rows.filter p).filter (fun r => middle.all (fun t => t.2 r)) =
      (rows.filter (fun r => middle.all (fun t => t.2 r))).filter p := by
    rw [List.filter_filter, List.filter_filter]
    apply List.filter_congr
    intro r hr
    exact Bool.and_comm _ _
  have hlen : (A.foldl (fun rs t => rs.filter t.2) rows).length =
      (rows.filter (fun r => p r && middle.all (fun t => t.2 r))).length := by
    have heq := filteredBy_eq_filter middle (rows.filter p)
    change (middle.foldl (fun rs t => rs.filter t.2) (rows.filter p)).length =
      (rows.filter (fun r => p r && middle.all (fun t => t.2 r))).length
    rw [show rows.filter (fun r => p r && middle.all (fun t => t.2 r)) =
        (rows.filter p).filter (fun r => middle.all (fun t => t.2 r)) by
      rw [List.filter_filter]
      apply List.filter_congr
      intro r hr
      exact Bool.and_comm _ _]
    exact congrArg List.length heq

  have hdup : PredicateOrder.actual ((c, p) :: tail) (A.foldl (fun rs t => rs.filter t.2) rows) =
      c * (A.foldl (fun rs t => rs.filter t.2) rows).length +
        PredicateOrder.actual tail ((A.foldl (fun rs t => rs.filter t.2) rows).filter p) := by
    simp [PredicateOrder.actual]
  rw [hp] at hdup
  rw [horig, hopt, hdup, hlen]
  omega

theorem DeferredPass_sound :
    PredicateOrder.DeferredPass.Goal := by
  intro c tests rows
  have h1 : PredicateOrder.actual (tests ++ [(c, fun _ => true)]) rows
      = PredicateOrder.actual tests rows + c * (tests.foldl (fun rs t => rs.filter t.2) rows).length := by
    induction tests generalizing rows with
    | nil => simp [PredicateOrder.actual]
    | cons x xs ih =>
        rcases x with ⟨cc, pp⟩
        simp [PredicateOrder.actual]
        rw [ih (rows.filter pp)]
        omega
  have h2 : PredicateOrder.actual ((c, fun _ => true) :: tests) rows
      = c * rows.length + PredicateOrder.actual tests rows := by
    change c * rows.length + PredicateOrder.actual tests (rows.filter (fun _ => true)) =
      c * rows.length + PredicateOrder.actual tests rows
    rw [show rows.filter (fun _ : Nat => true) = rows by
      apply List.filter_eq_self.mpr
      intro r hr
      rfl]
  rw [h1, h2]
  have hle := filteredBy_length_le tests rows
  have hmul := Nat.mul_le_mul_left c hle
  omega

theorem MarginalTie_disproof :
    ¬ PredicateOrder.MarginalTie.Goal := by
  intro h
  -- Counterexample: rows = [0,1], gate/a keep only 0, b keeps only 1.
  -- Both a and b pass exactly one row, yet [gate,a,b] costs 4 while [gate,b,a] costs 3.
  let rows := [0, 1]
  let gate := fun (x : Nat) => x == 0
  let a := fun (x : Nat) => x == 0
  let b := fun (x : Nat) => x == 1
  have hcount : rows.countP a = rows.countP b := by decide
  have hne : PredicateOrder.actual [(1, gate), (1, a), (1, b)] rows ≠
      PredicateOrder.actual [(1, gate), (1, b), (1, a)] rows := by decide
  exact hne (h rows a b gate hcount)

private def step (cap : Nat) (st : Nat × Nat) (w : Nat) : Nat × Nat :=
  if st.2 + w ≤ cap then (st.1, st.2 + w) else (st.1 + 1, w)

private def packFrom (cap : Nat) (st : Nat × Nat) (xs : List Nat) : Nat × Nat :=
  xs.foldl (step cap) st

private def need (cap cur : Nat) : List Nat → Nat
  | [] => 0
  | w :: ws => if cur + w ≤ cap then need cap (cur + w) ws else 1 + need cap w ws

private theorem packFrom_fst (cap b cur : Nat) (xs : List Nat) :
    (packFrom cap (b, cur) xs).1 = b + need cap cur xs := by
  induction xs generalizing b cur with
  | nil => simp [packFrom, need]
  | cons w ws ih =>
      dsimp [packFrom]
      by_cases h : cur + w ≤ cap
      · simp [step, h, need]
        simpa [packFrom] using (ih (b := b) (cur := cur + w))
      · simp [step, h, need]
        have hi := ih (b := b + 1) (cur := w)
        simp [packFrom] at hi
        omega

private theorem packFrom_snd_le (cap cur : Nat) (xs : List Nat)
    (hcur : cur ≤ cap) (hw : ∀ w ∈ xs, w ≤ cap) : (packFrom cap (b, cur) xs).2 ≤ cap := by
  induction xs generalizing b cur with
  | nil => simpa [packFrom] using hcur
  | cons w ws ih =>
      have hwle : w ≤ cap := hw w (by simp)
      dsimp [packFrom]
      by_cases h : cur + w ≤ cap
      · simpa [step, h, packFrom] using
          (ih (b := b) (cur := cur + w) h (by intro y hy; exact hw y (by simp [hy])))
      · simpa [step, h, packFrom] using
          (ih (b := b + 1) (cur := w) hwle (by intro y hy; exact hw y (by simp [hy])))

private theorem need_props (cap : Nat) : ∀ xs : List Nat, (∀ x ∈ xs, x ≤ cap) →
    (∀ c1 c2, c1 ≤ c2 → c2 ≤ cap → need cap c1 xs ≤ need cap c2 xs) ∧
    (∀ c, c ≤ cap → need cap c xs ≤ 1 + need cap 0 xs) := by
  intro xs
  induction xs with
  | nil =>
      intro _
      simp [need]
  | cons x xs ih =>
      intro hxs
      have hx : x ≤ cap := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact hxs y (by simp [hy])
      have ih' := ih hxs'
      constructor
      · intro c1 c2 h12 h2
        by_cases h2x : c2 + x ≤ cap
        · have h1x : c1 + x ≤ cap := by omega
          simp only [need, if_pos h2x, if_pos h1x]
          exact ih'.1 (c1 + x) (c2 + x) (by omega) (by omega)
        · by_cases h1x : c1 + x ≤ cap
          · simp only [need, if_neg h2x, if_pos h1x]
            have hc : c1 + x ≤ cap := h1x
            have hd := ih'.2 (c1 + x) hc
            have hm := ih'.1 0 x (Nat.zero_le _) hx
            omega
          · simp [need, h2x, h1x]
      · intro c hc
        by_cases hcx : c + x ≤ cap
        · have h0x : 0 + x ≤ cap := by simpa using hx
          simp [need, hx, hcx]
          have hmono := ih'.1 0 x (Nat.zero_le _) hx
          have hdiff := ih'.2 (c + x) hcx
          omega
        · have h0x : 0 + x ≤ cap := by simpa using hx
          simp [need, hcx, h0x, hx]

private theorem need_lower_aux (cap cur : Nat) (xs : List Nat)
    (hcur : cur ≤ cap) (hxs : ∀ x ∈ xs, x ≤ cap)
    (blocks : List (List Nat)) (hflat : blocks.flatten = xs)
    (hsums : ∀ b ∈ blocks, b.sum ≤ cap)
    (hfirst : blocks = [] ∨ (xs ≠ [] → cur + (blocks.head?.getD []).sum ≤ cap))
    (hne : ∀ b ∈ blocks, b ≠ []) :
    need cap cur xs ≤ blocks.length := by
  induction xs generalizing cur blocks with
  | nil => simp [need]
  | cons x xs ih =>
      have hx : x ≤ cap := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact hxs y (by simp [hy])
      have hbne : blocks ≠ [] := by
        intro hb
        rw [hb] at hflat
        simp at hflat
      obtain ⟨b, bs, hb_eq⟩ := List.exists_cons_of_ne_nil hbne
      rw [hb_eq] at hflat hsums hfirst hne ⊢
      have hb0 : b ≠ [] := hne b (by simp)
      obtain ⟨y, b', hb_eq'⟩ := List.exists_cons_of_ne_nil hb0
      rw [hb_eq'] at hflat hsums hfirst hne ⊢
      injection hflat with hy htail
      subst y
      have hflat' : b' ++ bs.flatten = xs := htail
      have hsums' : ∀ z ∈ bs, z.sum ≤ cap := by
        intro z hz; exact hsums z (by simp [hz])
      have hne' : ∀ z ∈ bs, z ≠ [] := by
        intro z hz; exact hne z (by simp [hz])
      have hfirstsum : cur + (x + b'.sum) ≤ cap := by
        have ht := hfirst
        simp only [ne_eq, List.cons_ne_nil, false_or] at ht
        have ht' := ht (by simp)
        simpa [List.sum_cons, Nat.add_assoc] using ht'
      by_cases hc : cur + x ≤ cap
      · simp [need, hc]
        by_cases hb' : b' = []
        · subst b'
          have hcompat0 : bs = [] ∨ (xs ≠ [] → 0 + (bs.head?.getD []).sum ≤ cap) := by
            by_cases hbs : bs = []
            · exact Or.inl hbs
            · right
              intro _
              obtain ⟨z, zs, hz⟩ := List.exists_cons_of_ne_nil hbs
              rw [hz] at hsums'
              rw [hz]
              simpa using hsums' z (by simp)
          have ih0 := ih (cur := 0) (Nat.zero_le _) hxs' bs (by simpa using hflat')
            hsums' hcompat0 hne'
          have hd := (need_props cap xs hxs').2 (cur + x) hc
          omega
        · have hsumB : b'.sum ≤ cap := by omega
          have hcompat' : b' :: bs = [] ∨
              (xs ≠ [] → (cur + x) + ((b' :: bs).head?.getD []).sum ≤ cap) := by
            right
            intro _
            change (cur + x) + b'.sum ≤ cap
            simpa [Nat.add_assoc] using hfirstsum
          have ih' := ih (cur := cur + x) hc hxs' (b' :: bs)
            (by simpa using hflat') (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hsumB
              · exact hsums' z hz)
            hcompat' (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hb'
              · exact hne' z hz)
          omega
      · have : cur + x ≤ cap := by omega
        exact (hc this).elim


private theorem need_lower_strong (cap cur : Nat) (xs : List Nat)
    (hcur : cur ≤ cap) (hxs : ∀ x ∈ xs, x ≤ cap)
    (blocks : List (List Nat)) (hflat : blocks.flatten = xs)
    (hsums : ∀ b ∈ blocks, b.sum ≤ cap)
    (hfirst : blocks = [] ∨ (xs ≠ [] → cur + (blocks.head?.getD []).sum ≤ cap))
    (hne : ∀ b ∈ blocks, b ≠ []) :
    need cap cur xs + (if xs.isEmpty then 0 else 1) ≤ blocks.length := by
  induction xs generalizing cur blocks with
  | nil => simp [need]
  | cons x xs ih =>
      have hx : x ≤ cap := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact hxs y (by simp [hy])
      have hbne : blocks ≠ [] := by
        intro hb
        rw [hb] at hflat
        simp at hflat
      obtain ⟨b, bs, hb_eq⟩ := List.exists_cons_of_ne_nil hbne
      rw [hb_eq] at hflat hsums hfirst hne ⊢
      have hb0 : b ≠ [] := hne b (by simp)
      obtain ⟨y, b', hb_eq'⟩ := List.exists_cons_of_ne_nil hb0
      rw [hb_eq'] at hflat hsums hfirst hne ⊢
      injection hflat with hy htail
      subst y
      have hflat' : b' ++ bs.flatten = xs := htail
      have hsums' : ∀ z ∈ bs, z.sum ≤ cap := by
        intro z hz; exact hsums z (by simp [hz])
      have hne' : ∀ z ∈ bs, z ≠ [] := by
        intro z hz; exact hne z (by simp [hz])
      have hfirstsum : cur + (x + b'.sum) ≤ cap := by
        have ht := hfirst
        simp only [ne_eq, List.cons_ne_nil, false_or] at ht
        have ht' := ht (by simp)
        simpa [List.sum_cons, Nat.add_assoc] using ht'
      by_cases hc : cur + x ≤ cap
      · simp [need, hc]
        by_cases hb' : b' = []
        · subst b'
          have hcompat0 : bs = [] ∨ (xs ≠ [] → 0 + (bs.head?.getD []).sum ≤ cap) := by
            by_cases hbs : bs = []
            · exact Or.inl hbs
            · right
              intro _
              obtain ⟨z, zs, hz⟩ := List.exists_cons_of_ne_nil hbs
              rw [hz] at hsums'
              rw [hz]
              simpa using hsums' z (by simp)
          have ih0 := ih (cur := 0) (Nat.zero_le _) hxs' bs (by simpa using hflat')
            hsums' hcompat0 hne'
          have hd := (need_props cap xs hxs').2 (cur + x) hc
          have ih0' : need cap 0 xs + (if xs.isEmpty then 0 else 1) ≤ bs.length := by
            simpa [hflat'] using ih0
          by_cases hxs0 : xs = []
          · rw [hxs0]
            simp [need]
          · have ih1 : need cap 0 xs + 1 ≤ bs.length := by simpa [hxs0] using ih0'
            omega
        · have hsumB : b'.sum ≤ cap := by omega
          have hcompat' : b' :: bs = [] ∨
              (xs ≠ [] → (cur + x) + ((b' :: bs).head?.getD []).sum ≤ cap) := by
            right
            intro _
            change (cur + x) + b'.sum ≤ cap
            simpa [Nat.add_assoc] using hfirstsum
          have ih' := ih (cur := cur + x) hc hxs' (b' :: bs)
            (by simpa using hflat') (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hsumB
              · exact hsums' z hz)
            hcompat' (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hb'
              · exact hne' z hz)
          have ih'' : need cap (cur + x) xs + (if xs.isEmpty then 0 else 1) ≤ bs.length + 1 := by
            simpa [hflat'] using ih'
          by_cases hxs0 : xs = []
          · have hbnil : b' = [] := (by
                simpa [hxs0] using hflat' : b' = [] ∧ ∀ l ∈ bs, l = []).1
            exact (hb' hbnil).elim
          · have ihend : need cap (cur + x) xs + 1 ≤ bs.length + 1 := by simpa [hxs0] using ih''
            omega
      · have : cur + x ≤ cap := by omega
        exact (hc this).elim

private structure PackState where
  b : Nat
  cur : Nat
  done : List Nat
  blocks : List (List Nat)

private def prependHead (w : Nat) : List (List Nat) → List (List Nat)
  | [] => [[w]]
  | b :: bs => (w :: b) :: bs

private def pstep (cap : Nat) (st : PackState) (w : Nat) : PackState :=
  if st.cur + w ≤ cap then
    { st with cur := st.cur + w, done := st.done ++ [w], blocks := prependHead w st.blocks }
  else
    { b := st.b + 1, cur := w, done := st.done ++ [w], blocks := [w] :: st.blocks }

private theorem prependHead_rev (w : Nat) (bs : List (List Nat)) :
    (prependHead w bs).flatten.reverse = bs.flatten.reverse ++ [w] := by
  cases bs with
  | nil => simp [prependHead]
  | cons b bs => simp [prependHead, List.reverse_append]

private theorem prependHead_len (w : Nat) (bs : List (List Nat)) :
    (prependHead w bs).length = if bs.isEmpty then 1 else bs.length := by
  cases bs <;> simp [prependHead]

private structure PInv (cap cur0 : Nat) (st : PackState) : Prop where
  cur_le : st.cur ≤ cap
  pack_eq : packFrom cap (0, cur0) st.done = (st.b, st.cur)
  flat : st.blocks.flatten.reverse = st.done
  sums : ∀ b ∈ st.blocks, b ≠ [] ∧ b.sum ≤ cap
  head : st.blocks ≠ [] → (st.blocks.head?.getD []).sum = st.cur
  len : st.blocks.length = st.b + (if st.blocks.isEmpty then 0 else 1)
  empty_cur : st.blocks = [] → st.cur = 0

private theorem packInv_step (cap cur0 w : Nat) (st : PackState)
    (hinv : PInv cap cur0 st) (hw : w ≤ cap) :
    PInv cap cur0 (pstep cap st w) := by
  unfold pstep
  by_cases h : st.cur + w ≤ cap
  · rw [if_pos h]
    have hpack0 : packFrom cap (0, cur0) (st.done ++ [w]) =
        step cap (packFrom cap (0, cur0) st.done) w := by
      dsimp [packFrom]
      rw [List.foldl_append]
      rfl
    have hpack : packFrom cap (0, cur0) (st.done ++ [w]) = (st.b, st.cur + w) := by
      rw [hpack0, hinv.pack_eq]
      simp [step, h]
    refine ⟨by omega, hpack, ?_, ?_, ?_, ?_, ?_⟩
    · rw [prependHead_rev, ← hinv.flat]
    · intro b hb
      cases hbs : st.blocks with
      | nil =>
          simp [prependHead, hbs] at hb
          rcases hb with rfl | hfalse
          · constructor
            · simp
            · simpa using hw
      | cons z zs =>
          simp [prependHead, hbs] at hb
          rcases hb with hhead | htail
          · rcases hhead with rfl
            constructor
            · simp
            · have hzsum : z.sum = st.cur := by
                have hh := hinv.head (by simp [hbs])
                simpa [hbs] using hh
              rw [show (w :: z).sum = w + z.sum by rfl, hzsum]
              omega
          · have hz := hinv.sums b (by simp [hbs, htail])
            simpa [hbs] using hz
    · intro hne
      cases hbs : st.blocks with
      | nil =>
          have hc0 := hinv.empty_cur hbs
          simp [prependHead, hbs, hc0]
      | cons z zs =>
          have hsum := hinv.head (by simp [hbs])
          rw [hbs] at hsum
          simp [prependHead, hbs] at hsum ⊢
          omega
    · cases hbl : st.blocks with
      | nil =>
          have hc0 := hinv.empty_cur hbl
          have hbcount := hinv.len
          simp [hbl] at hbcount
          simp [prependHead, hbl, hc0, hbcount]
      | cons z zs =>
          have hlen := hinv.len
          simp [hbl, prependHead] at hlen ⊢
          omega
    · cases hbl : st.blocks <;> simp [prependHead, hbl]
  · rw [if_neg h]
    have hpack0 : packFrom cap (0, cur0) (st.done ++ [w]) =
        step cap (packFrom cap (0, cur0) st.done) w := by
      dsimp [packFrom]
      rw [List.foldl_append]
      rfl
    have hpack : packFrom cap (0, cur0) (st.done ++ [w]) = (st.b + 1, w) := by
      rw [hpack0, hinv.pack_eq]
      simp [step, h]
    refine ⟨by omega, hpack, ?_, ?_, ?_, ?_, ?_⟩
    · simp [List.reverse_cons]
      simpa using hinv.flat
    · intro b hb
      simp at hb
      rcases hb with rfl | hb
      · constructor
        · simp
        · simpa using hw
      · have hz := hinv.sums b hb
        simpa using hz
    · simp
    · cases hbl : st.blocks with
      | nil =>
          have hc0 := hinv.empty_cur hbl
          have hbcount := hinv.len
          simp [hbl] at hbcount
          simp [hbl, hc0]
          omega
      | cons z zs =>
          have hlen := hinv.len
          simp [hbl] at hlen ⊢
          omega
    · simp

private theorem packInv_fold (cap cur0 : Nat) (xs : List Nat) (st : PackState)
    (hw : ∀ w ∈ xs, w ≤ cap) (hinv : PInv cap cur0 st) :
    PInv cap cur0 (xs.foldl (pstep cap) st) := by
  induction xs generalizing st with
  | nil => simpa using hinv
  | cons w ws ih =>
      rw [List.foldl_cons]
      apply ih
      · intro x hx; exact hw x (by simp [hx])
      · exact packInv_step cap cur0 w st hinv (hw w (by simp))

private theorem packFull_inv (cap : Nat) (xs : List Nat)
    (hw : ∀ w ∈ xs, w ≤ cap) :
    PInv cap 0 (xs.foldl (pstep cap) { b := 0, cur := 0, done := [], blocks := [] }) := by
  apply packInv_fold
  · exact hw
  · refine ⟨Nat.zero_le _, ?_, rfl, ?_, ?_, ?_, ?_⟩
    · simp [packFrom]
    · intro b hb; simp at hb
    · intro h; simp at h
    · simp
    · simp


private theorem pstep_done_append (cap : Nat) (xs : List Nat) (st : PackState) :
    (xs.foldl (pstep cap) st).done = st.done ++ xs := by
  induction xs generalizing st with
  | nil => simp
  | cons w ws ih =>
      rw [List.foldl_cons]
      by_cases h : st.cur + w ≤ cap
      · rw [show pstep cap st w = ⟨st.b, st.cur + w, st.done ++ [w], prependHead w st.blocks⟩ by simp [pstep, h]]
        simpa [List.append_assoc] using ih ⟨st.b, st.cur + w, st.done ++ [w], prependHead w st.blocks⟩
      · rw [show pstep cap st w = ⟨st.b + 1, w, st.done ++ [w], [w] :: st.blocks⟩ by simp [pstep, h]]
        simpa [List.append_assoc] using ih ⟨st.b + 1, w, st.done ++ [w], [w] :: st.blocks⟩

private theorem pack_blocks_flatten (blocks : List (List Nat)) :
    (blocks.reverse.map List.reverse).flatten = blocks.flatten.reverse := by
  induction blocks with
  | nil => rfl
  | cons b bs ih =>
      rw [List.reverse_cons, List.map_append, List.map_cons, List.map_nil, List.flatten_append, ih]
      simp [List.reverse_append]

private theorem filter_nonempty_flatten (blocks : List (List Nat)) :
    (blocks.filter (fun b : List Nat => !b.isEmpty)).flatten = blocks.flatten := by
  induction blocks with
  | nil => rfl
  | cons b bs ih =>
      by_cases hb : b.isEmpty
      · have hb0 : b = [] := by cases b <;> simp at hb ⊢
        simp [hb0, ih]
      · simp [hb, ih]

private theorem mem_of_mem_reverse {α : Type} {a : α} {l : List α} (h : a ∈ l.reverse) : a ∈ l := by
  simpa using (List.mem_reverse.mp h)

private theorem max?_getD_le (l : List Nat) (cap : Nat) (h : ∀ x ∈ l, x ≤ cap) :
    l.max?.getD 0 ≤ cap := by
  by_cases hnil : l = []
  · simp [hnil]
  · have his := List.isSome_max?_of_ne_nil hnil
    cases hm : l.max? with
    | none => simp [hm] at his
    | some a =>
        have hiff : ∀ {x : Nat}, a ≤ x ↔ ∀ b ∈ l, b ≤ x := List.max?_le_iff hm
        have hall : a ≤ cap := hiff.mpr h
        simpa [hm] using hall

private theorem peak_le_of_sums {cap : Nat} (blocks : List (List Nat))
    (h : ∀ b ∈ blocks, b.sum ≤ cap) : peak blocks ≤ cap := by
  unfold peak
  apply max?_getD_le
  intro x hx
  obtain ⟨b, hb, rfl⟩ := List.mem_map.mp hx
  exact h b hb

private theorem sums_of_peak_le {cap : Nat} (blocks : List (List Nat))
    (h : peak blocks ≤ cap) : ∀ b ∈ blocks, b.sum ≤ cap := by
  intro b hb
  unfold peak at h
  have hbmem : b.sum ∈ blocks.map List.sum := List.mem_map.mpr ⟨b, hb, rfl⟩
  exact Nat.le_trans (List.le_max?_getD_of_mem hbmem) h

private theorem opt_lower (cap : Nat) (xs : List Nat)
    (hxs : ∀ x ∈ xs, x ≤ cap)
    (blocks : List (List Nat)) (hflat : blocks.flatten = xs)
    (hsums : ∀ b ∈ blocks, b.sum ≤ cap) :
    need cap 0 xs + (if xs.isEmpty then 0 else 1) ≤ blocks.length := by
  induction xs generalizing blocks with
  | nil => simp [need]
  | cons x xs ih =>
      let bs := blocks.filter (fun b : List Nat => !b.isEmpty)
      have hlenfilter : bs.length ≤ blocks.length := by
        dsimp [bs]
        exact List.length_filter_le _ _
      have hflatbs : bs.flatten = x :: xs := by
        dsimp [bs]
        rw [filter_nonempty_flatten, hflat]
      have hsumsbs : ∀ b ∈ bs, b.sum ≤ cap := by
        intro b hb
        exact hsums b (List.mem_filter.mp hb).1
      have hbsne : bs ≠ [] := by
        intro h
        rw [h] at hflatbs
        simp at hflatbs
      obtain ⟨b, rest, hbs⟩ := List.exists_cons_of_ne_nil hbsne
      have hbne : b ≠ [] := by
        have hm : b ∈ bs := by simp [hbs]
        have hm' : b ∈ blocks.filter (fun b : List Nat => !b.isEmpty) := by simpa [bs] using hm
        simpa using (List.mem_filter.mp hm').2
      rw [hbs] at hflatbs hsumsbs
      obtain ⟨y, b', hb⟩ := List.exists_cons_of_ne_nil hbne
      rw [hb] at hflatbs hsumsbs
      injection hflatbs with hy htail
      subst y
      have hx : x ≤ cap := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact hxs y (by simp [hy])
      have hsums_rest : ∀ z ∈ rest, z.sum ≤ cap := by
        intro z hz; exact hsumsbs z (by simp [hbs, hz])
      have hlow : need cap 0 (x :: xs) + 1 ≤ bs.length := by
        rw [hbs]
        by_cases hb' : b' = []
        · subst b'
          have ih' := ih hxs' rest (by simpa using htail) hsums_rest
          have ih2p : need cap 0 xs + (if xs.isEmpty then 0 else 1) ≤ rest.length := ih'
          by_cases hxs0 : xs = []
          · rw [hxs0]
            simp [need, hx]
          · have ih'' : need cap 0 xs + 1 ≤ rest.length := by simpa [hxs0] using ih2p
            have hd := (need_props cap xs hxs').2 x hx
            simp [need, hx] at hd ⊢
            omega
        · have hfirstsum : x + b'.sum ≤ cap := by
            have hs := hsumsbs (x :: b') (by simp)
            simpa [List.sum_cons] using hs
          have hcompat : b' :: rest = [] ∨
              (xs ≠ [] → x + ((b' :: rest).head?.getD []).sum ≤ cap) := by
            right
            intro _
            change x + b'.sum ≤ cap
            exact hfirstsum
          have hl := need_lower_strong cap x xs hx hxs' (b' :: rest)
            (by simpa using htail) (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · omega
              · exact hsums_rest z hz)
            hcompat (by
              intro z hz
              simp at hz
              rcases hz with rfl | hz
              · exact hb'
              · have hm : z ∈ bs := by simp [hbs, hz]
                have hm' : z ∈ blocks.filter (fun b : List Nat => !b.isEmpty) := by simpa [bs] using hm
                simpa using (List.mem_filter.mp hm').2)
          have hbslen : bs.length = rest.length + 1 := by rw [hbs]; rfl
          have hl2 : need cap x xs + (if xs.isEmpty then 0 else 1) ≤ (b' :: rest).length := by
            simpa [htail] using hl
          by_cases hxs0 : xs = []
          · have hbnil : b' = [] := (by
                simpa [hxs0] using htail : b' = [] ∧ ∀ l ∈ rest, l = []).1
            exact (hb' hbnil).elim
          · have hl' : need cap x xs + 1 ≤ rest.length + 1 := by
              simpa [hxs0] using hl2
            simp [need, hx]
            omega
      have hres : need cap 0 (x :: xs) + (if (x :: xs).isEmpty then 0 else 1) ≤ bs.length := by
        simpa using hlow
      exact Nat.le_trans hres hlenfilter

theorem Feasible_sound : ParallelCuts.Feasible.Goal := by
  intro cap xs k hw
  constructor
  · intro hreq
    by_cases hxnil : xs.isEmpty
    · have hxnil' : xs = [] := by cases xs <;> simp at hxnil ⊢
      subst xs
      refine ⟨[], by simp, by simp, ?_⟩
      simp [required, peak]
    · let st := xs.foldl (pstep cap) { b := 0, cur := 0, done := [], blocks := [] }
      have hinv : PInv cap 0 st := by
        dsimp [st]
        exact packFull_inv cap xs hw
      let bl := st.blocks.reverse.map List.reverse
      have hdone : st.done = xs := by
        dsimp [st]
        rw [pstep_done_append]
        simp
      have hblocksne : st.blocks ≠ [] := by
        intro hb
        have hf := hinv.flat
        rw [hb] at hf
        simp at hf
        have hempty : xs.isEmpty = true := by
          rw [← hdone]
          simpa [hf]
        exact hxnil hempty
      have hfl : bl.flatten = xs := by
        dsimp [bl]
        rw [pack_blocks_flatten, hinv.flat, hdone]
      have hsums : ∀ b ∈ bl, b.sum ≤ cap := by
        intro b hb
        obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hb
        have hz' : z ∈ st.blocks := mem_of_mem_reverse hz
        simpa [List.sum_reverse] using (hinv.sums z hz').2
      have hlen : bl.length = required cap xs := by
        dsimp [bl, required]
        rw [if_neg hxnil]
        simp only [List.length_map, List.length_reverse]
        have hp := congrArg Prod.fst hinv.pack_eq
        rw [hdone] at hp
        have hcount : (packFrom cap (0, 0) xs).1 = (pack cap xs).1 := by rfl
        rw [hcount] at hp
        have hlen' := hinv.len
        simp [hblocksne] at hlen'
        omega
      refine ⟨bl, hfl, ?_, peak_le_of_sums bl hsums⟩
      rw [hlen]
      exact hreq
  · rintro ⟨blocks, hflat, hlen, hpeak⟩
    have hxs' : ∀ x ∈ xs, x ≤ cap := hw
    have hsums := sums_of_peak_le blocks hpeak
    have hlower := opt_lower cap xs hxs' blocks hflat hsums
    by_cases hxnil : xs.isEmpty
    · simp [required, hxnil]
    · have hneed : required cap xs = need cap 0 xs + 1 := by
        dsimp [required]
        rw [if_neg hxnil]
        have hpf := packFrom_fst cap 0 0 xs
        simp at hpf
        have hcount : (packFrom cap (0, 0) xs).1 = (pack cap xs).1 := by rfl
        rw [hcount] at hpf
        omega
      rw [hneed]
      have hlow' : need cap 0 xs + 1 ≤ blocks.length := by simpa [hxnil] using hlower
      exact Nat.le_trans hlow' hlen


theorem CapacitySearch_sound : ParallelCuts.CapacitySearch.Goal := by
  intro a b xs hab hw
  have hwa : ∀ w ∈ xs, w ≤ a := hw
  have hwb : ∀ w ∈ xs, w ≤ b := by intro w hw'; exact Nat.le_trans (hw w hw') hab
  have hex : ∃ blocks : List (List Nat), blocks.flatten = xs ∧
      blocks.length ≤ required a xs ∧ peak blocks ≤ a :=
    (Feasible_sound a xs (required a xs) hw).mp (Nat.le_refl _)
  obtain ⟨blocks, hflat, hlen, hpeak⟩ := hex
  have hsumb : ∀ z ∈ blocks, z.sum ≤ b := by
    intro z hz
    exact Nat.le_trans (sums_of_peak_le blocks hpeak z hz) hab
  exact (Feasible_sound b xs (required a xs) hwb).mpr
    ⟨blocks, hflat, hlen, peak_le_of_sums blocks hsumb⟩

theorem ReverseScan_sound : ParallelCuts.ReverseScan.Goal := by
  intro cap xs hw
  apply Nat.le_antisymm
  · have hex : ∃ blocks : List (List Nat), blocks.flatten = xs ∧
        blocks.length ≤ required cap xs ∧ peak blocks ≤ cap :=
      (Feasible_sound cap xs (required cap xs) hw).mp (Nat.le_refl _)
    obtain ⟨blocks, hflat, hlen, hpeak⟩ := hex
    let rblocks := blocks.reverse.map List.reverse
    have hrflat : rblocks.flatten = xs.reverse := by
      dsimp [rblocks]
      rw [pack_blocks_flatten, ← hflat]
    have hrsums : ∀ z ∈ rblocks, z.sum ≤ cap := by
      intro z hz
      obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hz
      have hy' : y ∈ blocks := by simpa using hy
      have hysum := sums_of_peak_le blocks hpeak y hy'
      simpa [List.sum_reverse] using hysum
    have hrlen : rblocks.length ≤ required cap xs := by
      dsimp [rblocks]
      simpa [List.length_reverse] using hlen
    exact (Feasible_sound cap xs.reverse (required cap xs) (by
      intro w hw'
      have hw'' : w ∈ xs := by simpa using hw'
      exact hw w hw'')).mpr ⟨rblocks, hrflat, hrlen, peak_le_of_sums rblocks hrsums⟩
  · -- apply the same argument to xs.reverse
    have hwr : ∀ w ∈ xs.reverse, w ≤ cap := by
      intro w hw'
      have hw'' : w ∈ xs := by simpa using hw'
      exact hw w hw''
    have hex : ∃ blocks : List (List Nat), blocks.flatten = xs.reverse ∧
        blocks.length ≤ required cap xs.reverse ∧ peak blocks ≤ cap :=
      (Feasible_sound cap xs.reverse (required cap xs.reverse) hwr).mp (Nat.le_refl _)
    obtain ⟨blocks, hflat, hlen, hpeak⟩ := hex
    let rblocks := blocks.reverse.map List.reverse
    have hrflat : rblocks.flatten = xs.reverse.reverse := by
      dsimp [rblocks]
      rw [pack_blocks_flatten, ← hflat]
    have hrsums : ∀ z ∈ rblocks, z.sum ≤ cap := by
      intro z hz
      obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hz
      have hy' : y ∈ blocks := by simpa using hy
      have hysum := sums_of_peak_le blocks hpeak y hy'
      simpa [List.sum_reverse] using hysum
    have hrlen : rblocks.length ≤ required cap xs.reverse := by
      dsimp [rblocks]
      simpa [List.length_reverse] using hlen
    have hres := (Feasible_sound cap xs.reverse.reverse (required cap xs.reverse) (by
      intro w hw'
      have hw'' : w ∈ xs.reverse := by simpa using hw'
      have hw''' : w ∈ xs := by simpa using hw''
      exact hw w hw''')).mpr ⟨rblocks, hrflat, hrlen, peak_le_of_sums rblocks hrsums⟩
    simpa using hres


private theorem sum_take_le (n : Nat) (l : List Nat) : (l.take n).sum ≤ l.sum := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp
      | succ n =>
          change x + (xs.take n).sum ≤ x + xs.sum
          exact Nat.add_le_add_left (ih n) x

private theorem sum_drop_le (n : Nat) (l : List Nat) : (l.drop n).sum ≤ l.sum := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp
      | succ n =>
          change (xs.drop n).sum ≤ x + xs.sum
          exact Nat.le_trans (ih n) (Nat.le_add_left _ _)

private def splitBlocks (n : Nat) : List (List Nat) → List (List Nat) × List (List Nat)
  | [] => ([], [])
  | b :: bs =>
      if b.length ≤ n then
        let (l, r) := splitBlocks (n - b.length) bs
        (b :: l, r)
      else ([b.take n], b.drop n :: bs)

private theorem splitBlocks_spec (n : Nat) (blocks : List (List Nat))
    (xs ys : List Nat) (hflat : blocks.flatten = xs ++ ys) (hn : n = xs.length) :
    (splitBlocks n blocks).1.flatten = xs ∧
    (splitBlocks n blocks).2.flatten = ys ∧
    (splitBlocks n blocks).1.length + (splitBlocks n blocks).2.length ≤ blocks.length + 1 ∧
    (∀ z ∈ (splitBlocks n blocks).1, ∃ w ∈ blocks, z.sum ≤ w.sum) ∧
    (∀ z ∈ (splitBlocks n blocks).2, ∃ w ∈ blocks, z.sum ≤ w.sum) := by
  induction blocks generalizing n xs ys with
  | nil =>
      simp at hflat
      rcases hflat with ⟨rfl, rfl⟩
      subst n
      simp [splitBlocks]
  | cons blk bs ih =>
      simp only [List.flatten_cons] at hflat
      dsimp [splitBlocks]
      by_cases hb : blk.length ≤ n
      · rw [if_pos hb]
        have hbxs : blk.length ≤ xs.length := by omega
        have hbflat : blk = xs.take blk.length := by
          have ht := congrArg (fun l : List Nat => l.take blk.length) hflat
          rw [List.take_append_of_le_length hbxs] at ht
          simpa using ht
        have hrest : bs.flatten = xs.drop blk.length ++ ys := by
          have ht := congrArg (fun l : List Nat => l.drop blk.length) hflat
          rw [List.drop_append_of_le_length hbxs] at ht
          simpa using ht
        have hn' : n - blk.length = (xs.drop blk.length).length := by rw [hn, List.length_drop]
        have hs := ih (n - blk.length) (xs.drop blk.length) ys hrest hn'
        rcases hs with ⟨hl, hr, hlen, hlsum, hrsum⟩
        refine ⟨?_, hr, ?_, ?_, ?_⟩
        · change blk ++ (splitBlocks (n - blk.length) bs).1.flatten = xs
          rw [hl]
          have htlen : (xs.take blk.length).length = blk.length := by simp [hbxs]
          rw [hbflat, htlen]
          exact List.take_append_drop _ _
        · simp only [List.length_cons]
          omega
        · exact fun elt hmem => by
            rcases List.mem_cons.mp hmem with heq | hmem
            · subst elt
              exact ⟨blk, by simp, Nat.le_refl _⟩
            · obtain ⟨v, hv, hzv⟩ := hlsum elt hmem
              exact ⟨v, by simp [hv], hzv⟩
        · exact fun elt hmem => by
            obtain ⟨v, hv, hvle⟩ := hrsum elt hmem
            exact ⟨v, by simp [hv], hvle⟩
      · rw [if_neg hb]
        have hnle : n ≤ blk.length := by omega
        have hxsle : xs.length ≤ blk.length := by omega
        have hbtake : blk.take n = xs := by
          have ht := congrArg (fun l : List Nat => l.take xs.length) hflat
          rw [List.take_append_of_le_length hxsle] at ht
          simpa [hn] using ht
        have hdrop : blk.drop n ++ bs.flatten = ys := by
          have ht := congrArg (fun l : List Nat => l.drop n) hflat
          rw [List.drop_append_of_le_length hnle] at ht
          simpa [hn] using ht
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · simpa [hbtake]
        · simpa [hdrop]
        · simp only [List.length_cons, List.length_singleton, List.length_nil, Nat.zero_add]
          omega
        · exact fun elt hmem => by
            simp at hmem
            subst elt
            exact ⟨blk, by simp, sum_take_le n blk⟩
        · exact fun elt hmem => by
            rcases List.mem_cons.mp hmem with heq | hmem
            · subst elt
              exact ⟨blk, by simp, sum_drop_le n blk⟩
            · exact ⟨elt, by simp [hmem], Nat.le_refl _⟩

theorem BoundarySlack_sound : ParallelCuts.BoundarySlack.Goal := by
  intro cap a b hw
  have hwa : ∀ w ∈ a, w ≤ cap := by intro w hw'; exact hw w (by simp [hw'])
  have hwb : ∀ w ∈ b, w ≤ cap := by intro w hw'; exact hw w (by simp [hw'])
  have hwab : ∀ w ∈ a ++ b, w ≤ cap := hw
  constructor
  · have hex : ∃ blocks : List (List Nat), blocks.flatten = a ++ b ∧
        blocks.length ≤ required cap (a ++ b) ∧ peak blocks ≤ cap :=
      (Feasible_sound cap (a ++ b) (required cap (a ++ b)) hwab).mp (Nat.le_refl _)
    obtain ⟨blocks, hflat, hlen, hpeak⟩ := hex
    have hsums := sums_of_peak_le blocks hpeak
    obtain ⟨hba, hbb, hlen', hsuma, hsumb⟩ :=
      splitBlocks_spec a.length blocks a b hflat rfl
    have hsuma' : ∀ z ∈ (splitBlocks a.length blocks).1, z.sum ≤ cap := by
      intro z hz
      obtain ⟨w, hw', hzw⟩ := hsuma z hz
      exact Nat.le_trans hzw (hsums w hw')
    have hsumb' : ∀ z ∈ (splitBlocks a.length blocks).2, z.sum ≤ cap := by
      intro z hz
      obtain ⟨w, hw', hzw⟩ := hsumb z hz
      exact Nat.le_trans hzw (hsums w hw')
    have hla : required cap a ≤ (splitBlocks a.length blocks).1.length :=
      (Feasible_sound cap a (splitBlocks a.length blocks).1.length hwa).mpr ⟨(splitBlocks a.length blocks).1, hba, Nat.le_refl _, peak_le_of_sums _ hsuma'⟩
    have hlb : required cap b ≤ (splitBlocks a.length blocks).2.length :=
      (Feasible_sound cap b (splitBlocks a.length blocks).2.length hwb).mpr ⟨(splitBlocks a.length blocks).2, hbb, Nat.le_refl _, peak_le_of_sums _ hsumb'⟩
    omega
  · have hexa : ∃ blocks : List (List Nat), blocks.flatten = a ∧
        blocks.length ≤ required cap a ∧ peak blocks ≤ cap :=
      (Feasible_sound cap a (required cap a) hwa).mp (Nat.le_refl _)
    obtain ⟨ba, hfa, hla, hpa⟩ := hexa
    have hexb : ∃ blocks : List (List Nat), blocks.flatten = b ∧
        blocks.length ≤ required cap b ∧ peak blocks ≤ cap :=
      (Feasible_sound cap b (required cap b) hwb).mp (Nat.le_refl _)
    obtain ⟨bb, hfb, hlb, hpb⟩ := hexb
    have hcatflat : (ba ++ bb).flatten = a ++ b := by simp [hfa, hfb]
    have hcatsums : ∀ z ∈ ba ++ bb, z.sum ≤ cap := by
      intro z hz
      simp at hz
      rcases hz with hz | hz
      · exact sums_of_peak_le ba hpa z hz
      · exact sums_of_peak_le bb hpb z hz
    have hj : (ba ++ bb).length = ba.length + bb.length := by simp
    have hlenTotal : (ba ++ bb).length ≤ required cap a + required cap b := by
      rw [hj]
      omega
    exact (Feasible_sound cap (a ++ b) (required cap a + required cap b) hwab).mpr
      ⟨ba ++ bb, hcatflat, hlenTotal, peak_le_of_sums (ba ++ bb) hcatsums⟩

private theorem sum_modify_le (xs : List Nat) (i d : Nat) :
    (xs.modify i (fun x => x + d)).sum ≤ xs.sum + d := by
  induction xs generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero =>
          simp
          omega
      | succ i =>
          simp
          have := ih i
          omega

private theorem take_modify_of_lt (xs : List Nat) (i c : Nat) (hic : i < c) (f : Nat → Nat) :
    (xs.modify i f).take c = (xs.take c).modify i f := by
  induction xs generalizing i c with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => omega
      | succ c =>
          cases i with
          | zero => simp
          | succ i =>
              simp
              exact ih i c (by omega)

private theorem drop_modify_of_lt (xs : List Nat) (i c : Nat) (hic : i < c) (f : Nat → Nat) :
    (xs.modify i f).drop c = xs.drop c := by
  induction xs generalizing i c with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => omega
      | succ c =>
          cases i with
          | zero => simp
          | succ i =>
              simp
              exact ih i c (by omega)

private theorem drop_modify_of_le (xs : List Nat) (i c : Nat) (hci : c ≤ i) (f : Nat → Nat) :
    (xs.modify i f).drop c = (xs.drop c).modify (i - c) f := by
  induction xs generalizing i c with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => simp
      | succ c =>
          cases i with
          | zero => omega
          | succ i =>
              simp
              exact ih i c (by omega)

private theorem take_modify_of_le (xs : List Nat) (i c : Nat) (hci : c ≤ i) (f : Nat → Nat) :
    (xs.modify i f).take c = xs.take c := by
  induction xs generalizing i c with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => simp
      | succ c =>
          cases i with
          | zero => omega
          | succ i =>
              simp
              exact ih i c (by omega)

private theorem LocalCorrection_sound : ParallelCuts.LocalCorrection.Goal := by
  intro xs cs i d
  induction cs generalizing xs i with
  | nil =>
      apply peak_le_of_sums
      intro z hz
      simp [groups] at hz
      subst z
      change (xs.modify i (fun x => x + d)).sum ≤ (xs : List Nat).sum + d
      exact sum_modify_le xs i d
  | cons c cs ih =>
      apply peak_le_of_sums
      intro z hz
      simp [groups] at hz
      by_cases hic : i < c
      · rcases hz with rfl | hz
        · rw [take_modify_of_lt xs i c hic]
          have hle := sum_modify_le (xs.take c) i d
          have hold : (xs.take c).sum ≤ peak (groups xs (c :: cs)) :=
            sums_of_peak_le (groups xs (c :: cs)) (Nat.le_refl _) (xs.take c) (by simp [groups])
          omega
        · have hdrop : (xs.modify i (fun x => x + d)).drop c = xs.drop c :=
            drop_modify_of_lt xs i c hic _
          rw [hdrop] at hz
          have hold : z.sum ≤ peak (groups (xs.drop c) cs) :=
            sums_of_peak_le (groups (xs.drop c) cs) (Nat.le_refl _) z hz
          have hpw : peak (groups (xs.drop c) cs) ≤ peak (groups xs (c :: cs)) :=
            peak_le_of_sums (groups (xs.drop c) cs) (by
              intro w hw
              exact sums_of_peak_le (groups xs (c :: cs)) (Nat.le_refl _) w (by simp [groups, hw]))
          omega
      · rcases hz with rfl | hz
        · rw [take_modify_of_le xs i c (by omega)]
          have hold : (xs.take c).sum ≤ peak (groups xs (c :: cs)) :=
            sums_of_peak_le (groups xs (c :: cs)) (Nat.le_refl _) (xs.take c) (by simp [groups])
          omega
        · have hdrop : (xs.modify i (fun x => x + d)).drop c =
              (xs.drop c).modify (i - c) (fun x => x + d) :=
            drop_modify_of_le xs i c (by omega) _
          rw [hdrop] at hz
          have hih := ih (xs.drop c) (i - c)
          have hzle : z.sum ≤ peak (groups ((xs.drop c).modify (i - c) (fun x => x + d)) cs) :=
            sums_of_peak_le _ (Nat.le_refl _) z hz
          have hpw : peak (groups (xs.drop c) cs) ≤ peak (groups xs (c :: cs)) :=
            peak_le_of_sums (groups (xs.drop c) cs) (by
              intro w hw
              exact sums_of_peak_le (groups xs (c :: cs)) (Nat.le_refl _) w (by simp [groups, hw]))
          omega



private theorem toNat_of_nonneg_eq (d : Int) (h : 0 ≤ d) : ((d.toNat : Nat) : Int) = d :=
  Int.toNat_of_nonneg h

private theorem toNat_of_nonpos_eq (d : Int) (h : d ≤ 0) : (d.toNat : Nat) = 0 :=
  Int.toNat_of_nonpos h

-- Single-row finite difference of the asymmetric absolute loss.
private theorem loss_one_step (x z : Int) (a b w : Nat) :
    ((w * (a * (x - (z + 1)).toNat + b * ((z + 1) - x).toNat) : Nat) : Int) -
      ((w * (a * (x - z).toNat + b * (z - x).toNat) : Nat) : Int) =
    (if x ≤ z then (b * w : Int) else -(a * w : Int)) := by
  by_cases hxz : x ≤ z
  · have hn1 : x - z ≤ 0 := by omega
    have hn2 : x - (z + 1) ≤ 0 := by omega
    have hz1 : ((z + 1 - x).toNat : Int) = z + 1 - x := Int.toNat_of_nonneg (by omega)
    have hz0 : ((z - x).toNat : Int) = z - x := Int.toNat_of_nonneg (by omega)
    have hx1 : (x - (z + 1)).toNat = 0 := Int.toNat_of_nonpos hn2
    have hx0 : (x - z).toNat = 0 := Int.toNat_of_nonpos hn1
    simp [hxz, hx1, hx0, hz1, hz0]
    push_cast
    have hd : z + 1 - x = (z - x) + 1 := by omega
    rw [hd]
    rw [Int.mul_add]
    have hb1 : (b : Int) * 1 = b := by omega
    rw [hb1]
    rw [Int.mul_add]
    have hcomm : (w : Int) * (b : Int) = (b : Int) * (w : Int) := by ac_rfl
    rw [hcomm]
    omega
  · have h1 : 0 ≤ x - z := by omega
    have h2 : 0 ≤ x - (z + 1) := by omega
    have hz1 : (z - x).toNat = 0 := Int.toNat_of_nonpos (by omega)
    have hz0 : (z + 1 - x).toNat = 0 := Int.toNat_of_nonpos (by omega)
    have hx1 : ((x - (z + 1)).toNat : Int) = x - (z + 1) := Int.toNat_of_nonneg h2
    have hx0 : ((x - z).toNat : Int) = x - z := Int.toNat_of_nonneg h1
    simp [hxz, hz1, hz0, hx1, hx0]
    push_cast
    have hd : x - z = (x - (z + 1)) + 1 := by omega
    rw [hd]
    rw [Int.mul_add]
    have ha1 : (a : Int) * 1 = a := by omega
    rw [ha1]
    rw [Int.mul_add]
    have hcomm : (w : Int) * (a : Int) = (a : Int) * (w : Int) := by ac_rfl
    rw [hcomm]
    omega

theorem ReservationSlope_sound : RiskEnvelope.ReservationSlope.Goal := by
  intro xs z a b
  induction xs with
  | nil => simp [loss, mass]
  | cons row xs ih =>
      rcases row with ⟨x, w⟩
      simp only [loss, List.map_cons, List.sum_cons, mass, List.filter_cons] at ih ⊢
      by_cases hxz : x ≤ z
      · have hle : decide (x ≤ z) = true := decide_eq_true hxz
        have hgt : decide (x > z) = false := by simp [hxz]
        simp [hle, hgt]
        simp only [gt_iff_lt] at ih ⊢
        have hx0 : (x - z).toNat = 0 := Int.toNat_of_nonpos (by omega)
        have hx1 : (x - (z + 1)).toNat = 0 := Int.toNat_of_nonpos (by omega)
        have hrow := loss_one_step x z a b w
        simp [hxz, hx0, hx1] at hrow
        push_cast at hrow ⊢
        simp only [Int.mul_add]
        have hm1 : max (x - (z + 1)) 0 = 0 := by omega
        have hm0 : max (x - z) 0 = 0 := by omega
        rw [hm1, hm0]
        omega
      · have hle : decide (x ≤ z) = false := by simp [hxz]
        have hzx : z < x := by omega
        have hgt : decide (x > z) = true := by simp [hzx]
        simp [hle, hgt]
        simp only [gt_iff_lt] at ih ⊢
        have hz0 : (z - x).toNat = 0 := Int.toNat_of_nonpos (by omega)
        have hz1 : (z + 1 - x).toNat = 0 := Int.toNat_of_nonpos (by omega)
        have hrow := loss_one_step x z a b w
        simp [hxz, hz0, hz1] at hrow
        push_cast at hrow ⊢
        simp only [Int.mul_add]
        have hm1 : max (z + 1 - x) 0 = 0 := by omega
        have hm0 : max (z - x) 0 = 0 := by omega
        rw [hm1, hm0]
        omega


theorem MeanReserve_disproof : ¬ RiskEnvelope.MeanReserve.Goal := by
  intro h
  -- Counterexample: loads 0 with weight 3 and 8 with weight 1.
  -- The integer mean is 2; loss at 2 is 12 but reserving 0 loses only 8.
  let xs : Costs := [(0, 3), (8, 1)]
  have hpos : 0 < mass xs (fun _ => true) := by native_decide
  let avg : Int := (xs.map (fun (v, w) => v * (w : Int))).sum / (mass xs (fun _ => true) : Int)
  have hbad := (h xs hpos) 0
  have hne : ¬ loss xs avg 1 1 ≤ loss xs 0 1 1 := by native_decide
  exact hne hbad

private theorem loss_step_one (x : Int) (w : Nat) (z : Int) (a b : Nat) :
    ((w * (a * (x - (z+1)).toNat + b * ((z+1) - x).toNat) : Nat) : Int) -
      ((w * (a * (x - z).toNat + b * (z - x).toNat) : Nat) : Int)
      = (b:Int) * (if x ≤ z then w else 0) - (a:Int) * (if z < x then w else 0) := by
  by_cases hxz : x ≤ z
  · have h1 : x - (z+1) ≤ 0 := by omega
    have h2 : x - z ≤ 0 := by omega
    have h3 : 0 ≤ (z+1) - x := by omega
    have h4 : 0 ≤ z - x := by omega
    have hzx : ¬ z < x := by omega
    simp [hxz, hzx, Int.toNat_of_nonpos h1, Int.toNat_of_nonpos h2,
      Int.toNat_of_nonneg h3, Int.toNat_of_nonneg h4]
    grind
  · have h1 : 0 ≤ x - (z+1) := by omega
    have h2 : 0 ≤ x - z := by omega
    have h3 : (z+1) - x ≤ 0 := by omega
    have h4 : z - x ≤ 0 := by omega
    have hzx : z < x := by omega
    simp [hxz, hzx, Int.toNat_of_nonpos h3, Int.toNat_of_nonpos h4,
      Int.toNat_of_nonneg h1, Int.toNat_of_nonneg h2]
    grind

private theorem step_eq (xs : RiskEnvelope.Costs) (z : Int) (a b : Nat) :
    (RiskEnvelope.loss xs (z+1) a b : Int) - RiskEnvelope.loss xs z a b =
      (b:Int)*RiskEnvelope.mass xs (fun x => decide (x ≤ z)) -
        (a:Int)*RiskEnvelope.mass xs (fun x => decide (z < x)) := by
  induction xs with
  | nil => simp [RiskEnvelope.loss, RiskEnvelope.mass]
  | cons x xs ih =>
      rcases x with ⟨xv, xw⟩
      dsimp [RiskEnvelope.loss, RiskEnvelope.mass]
      dsimp [RiskEnvelope.loss, RiskEnvelope.mass] at ih
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases hle : xv ≤ z
      · have hgt : ¬ xv > z := by omega
        simp [hle, hgt]
        have hstep := loss_step_one xv xw z a b
        simp [hle, hgt] at hstep
        grind
      · have hgt : xv > z := by omega
        have hnle : ¬ xv ≤ z := by omega
        simp [hnle, hgt]
        have hstep := loss_step_one xv xw z a b
        simp [hnle, hgt] at hstep
        grind

private theorem mass_le_add_gt (xs : RiskEnvelope.Costs) (z : Int) :
    RiskEnvelope.mass xs (fun x => decide (x ≤ z)) +
      RiskEnvelope.mass xs (fun x => decide (z < x)) =
        RiskEnvelope.mass xs (fun _ => true) := by
  induction xs with
  | nil => simp [RiskEnvelope.mass]
  | cons p xs ih =>
      rcases p with ⟨x, w⟩
      dsimp [RiskEnvelope.mass] at ih
      by_cases h : x ≤ z
      · have hgt : ¬ z < x := by omega
        simp [RiskEnvelope.mass, h, hgt]
        omega
      · have hgt : z < x := by omega
        simp [RiskEnvelope.mass, h, hgt]
        omega

private theorem mass_le_pred (xs : RiskEnvelope.Costs) (z : Int) :
    RiskEnvelope.mass xs (fun x => decide (x ≤ z - 1)) =
      RiskEnvelope.mass xs (fun x => decide (x < z)) := by
  unfold RiskEnvelope.mass
  apply congrArg (fun l : List (Int × Nat) => (l.map Prod.snd).sum)
  apply List.filter_congr
  intro p hp
  rcases p with ⟨x, w⟩
  by_cases h : x < z
  · have h' : x ≤ z - 1 := by omega
    simp [h, h']
  · have h' : ¬ x ≤ z - 1 := by omega
    simp [h, h']

private theorem mass_gt_pred (xs : RiskEnvelope.Costs) (z : Int) :
    RiskEnvelope.mass xs (fun x => decide (x > z - 1)) =
      RiskEnvelope.mass xs (fun x => decide (z ≤ x)) := by
  unfold RiskEnvelope.mass
  apply congrArg (fun l : List (Int × Nat) => (l.map Prod.snd).sum)
  apply List.filter_congr
  intro p hp
  rcases p with ⟨x, w⟩
  by_cases h : z ≤ x
  · have h' : x > z - 1 := by omega
    simp [h, h']
  · have h' : ¬ x > z - 1 := by omega
    simp [h, h']

private def slope (xs : RiskEnvelope.Costs) (a b : Nat) (z : Int) : Int :=
  (b:Int) * (RiskEnvelope.mass xs (fun x => decide (x ≤ z)) : Int) -
    (a:Int) * (RiskEnvelope.mass xs (fun x => decide (z < x)) : Int)

private theorem slope_mono (xs : RiskEnvelope.Costs) (a b : Nat) {y y' : Int}
    (hyy : y ≤ y') : slope xs a b y ≤ slope xs a b y' := by
  have hmono :
      (RiskEnvelope.mass xs (fun x => decide (x ≤ y)) ≤
        RiskEnvelope.mass xs (fun x => decide (x ≤ y'))) ∧
      (RiskEnvelope.mass xs (fun x => decide (y' < x)) ≤
        RiskEnvelope.mass xs (fun x => decide (y < x))) := by
    induction xs with
    | nil => simp [RiskEnvelope.mass]
    | cons p xs ih =>
        rcases p with ⟨x, w⟩
        dsimp [RiskEnvelope.mass] at ih
        dsimp [RiskEnvelope.mass]
        constructor
        · by_cases h : x ≤ y
          · have h' : x ≤ y' := by omega
            simp [h, h']
            omega
          · by_cases h' : x ≤ y'
            · simp [h, h']
              omega
            · simp [h, h']
              exact ih.1
        · by_cases h : y' < x
          · have h' : y < x := by omega
            simp [h, h']
            omega
          · by_cases h' : y < x
            · simp [h, h']
              omega
            · simp [h, h']
              exact ih.2
  have hle := hmono.1
  have hgt := hmono.2
  have h1 := Nat.mul_le_mul_left b hle
  have h2 := Nat.mul_le_mul_left a hgt
  dsimp [slope]
  omega


private theorem filter_true_gen {α : Type} (xs : List α) : xs.filter (fun _ => true) = xs := by
  exact List.filter_eq_self.mpr (fun _ _ => rfl)
private theorem natAbs_sub_comm (z y : Int) : (z-y).natAbs = (y-z).natAbs := by
  rw [show z - y = -(y-z) by omega, Int.natAbs_neg]
private theorem toNat_sub_le_of_le (y z x : Int) (h : y ≤ z) : (x-z).toNat ≤ (x-y).toNat := by
  exact Int.toNat_le_toNat (by omega)
private theorem toNat_sub_le_of_le_left (a b x : Int) (h : a ≤ b) : (a-x).toNat ≤ (b-x).toNat := by
  exact Int.toNat_le_toNat (by omega)
private theorem toNat_add_gap (y z x : Int) (h : y ≤ z) : (z-x).toNat ≤ (y-x).toNat + (z-y).natAbs := by
  rw [← Int.ofNat_le]
  simp [Int.ofNat_toNat, Int.natAbs_of_nonneg (by omega : 0 ≤ z-y)]
  omega
private theorem toNat_add_gap' (y z x : Int) (h : z ≤ y) : (x-z).toNat ≤ (x-y).toNat + (z-y).natAbs := by
  rw [← Int.ofNat_le]
  rw [natAbs_sub_comm z y]
  simp [Int.ofNat_toNat, Int.natAbs_of_nonneg (by omega : 0 ≤ y-z)]
  omega
private theorem loss_elem_lipschitz (x z y : Int) (w a b : Nat) :
    w * (a * (x-z).toNat + b * (z-x).toNat) ≤
      w * (a * (x-y).toNat + b * (y-x).toNat) + max a b * (z-y).natAbs * w := by
  by_cases hyz : y ≤ z
  · have h1 := toNat_sub_le_of_le y z x hyz
    have h2 := toNat_add_gap y z x hyz
    have ha : a * (x-z).toNat ≤ a * (x-y).toNat := Nat.mul_le_mul_left a h1
    have hb : b * (z-x).toNat ≤ b * ((y-x).toNat + (z-y).natAbs) := Nat.mul_le_mul_left b h2
    rw [Nat.mul_add] at hb
    have hmaxa : a ≤ max a b := Nat.le_max_left _ _
    have hmaxb : b ≤ max a b := Nat.le_max_right _ _
    have hm : a*(z-y).natAbs ≤ max a b*(z-y).natAbs := Nat.mul_le_mul_right _ hmaxa
    have hm' : b*(z-y).natAbs ≤ max a b*(z-y).natAbs := Nat.mul_le_mul_right _ hmaxb
    have hinner : a * (x-z).toNat + b * (z-x).toNat ≤
        a * (x-y).toNat + b * (y-x).toNat + max a b*(z-y).natAbs := by omega
    calc
      w * (a * (x-z).toNat + b * (z-x).toNat) ≤
          w * (a * (x-y).toNat + b * (y-x).toNat + max a b*(z-y).natAbs) :=
        Nat.mul_le_mul_left w hinner
      _ = w * (a * (x-y).toNat + b * (y-x).toNat) + max a b*(z-y).natAbs*w := by
        rw [Nat.mul_add, Nat.mul_comm w (max a b * (z-y).natAbs)]
  · have hzy : z ≤ y := by omega
    have h1 := toNat_sub_le_of_le_left z y x hzy
    have h2 := toNat_add_gap' y z x hzy
    have ha : a * (x-z).toNat ≤ a * ((x-y).toNat + (z-y).natAbs) := Nat.mul_le_mul_left a h2
    rw [Nat.mul_add] at ha
    have hb : b * (z-x).toNat ≤ b * (y-x).toNat := Nat.mul_le_mul_left b h1
    have hmaxa : a ≤ max a b := Nat.le_max_left _ _
    have hmaxb : b ≤ max a b := Nat.le_max_right _ _
    have hm : a*(z-y).natAbs ≤ max a b*(z-y).natAbs := Nat.mul_le_mul_right _ hmaxa
    have hm' : b*(z-y).natAbs ≤ max a b*(z-y).natAbs := Nat.mul_le_mul_right _ hmaxb
    have hinner : a * (x-z).toNat + b * (z-x).toNat ≤
        a * (x-y).toNat + b * (y-x).toNat + max a b*(z-y).natAbs := by omega
    calc
      w * (a * (x-z).toNat + b * (z-x).toNat) ≤
          w * (a * (x-y).toNat + b * (y-x).toNat + max a b*(z-y).natAbs) :=
        Nat.mul_le_mul_left w hinner
      _ = w * (a * (x-y).toNat + b * (y-x).toNat) + max a b*(z-y).natAbs*w := by
        rw [Nat.mul_add, Nat.mul_comm w (max a b * (z-y).natAbs)]

private theorem loss_extra_lipschitz (extra : RiskEnvelope.Costs) (z y : Int) (a b : Nat) :
    RiskEnvelope.loss extra z a b ≤
      RiskEnvelope.loss extra y a b + max a b * (z-y).natAbs *
        RiskEnvelope.mass extra (fun _ => true) := by
  induction extra with
  | nil => simp [RiskEnvelope.loss, RiskEnvelope.mass]
  | cons x xs ih =>
      rcases x with ⟨xv, xw⟩
      dsimp [RiskEnvelope.loss, RiskEnvelope.mass]
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      dsimp [RiskEnvelope.loss, RiskEnvelope.mass] at ih
      simp [filter_true_gen, Nat.mul_add] at ih ⊢
      have hlem := loss_elem_lipschitz xv z y xw a b
      simp [Nat.mul_add] at hlem
      omega


private theorem filter_map_fst_ne (xs : RiskEnvelope.Costs) (x : Int) :
    (xs.filter (fun p => !(p.1 == x))).map Prod.fst =
      (xs.map Prod.fst).filter (fun b => !b == x) := by
  induction xs with
  | nil => simp
  | cons p xs ih =>
      rcases p with ⟨v, w⟩
      by_cases h : v = x
      · subst v
        simp [ih]
      · have hb : (v == x) = false := by simp [h]
        simp [hb, ih]

private theorem sum_filter_eq_val (xs : RiskEnvelope.Costs) (x : Int) (f : Int → Nat) :
    ((xs.filter (fun p => p.1 == x)).map (fun p => p.2 * f p.1)).sum =
      RiskEnvelope.mass xs (fun y => y == x) * f x := by
  induction xs with
  | nil => simp [RiskEnvelope.mass]
  | cons p xs ih =>
      rcases p with ⟨v, w⟩
      dsimp [RiskEnvelope.mass] at ih ⊢
      by_cases h : v = x
      · subst v
        simp [ih]
        rw [Nat.add_mul]
      · have hb : (v == x) = false := by simp [h]
        simp [hb, ih]

private theorem sum_filter_partition (xs : RiskEnvelope.Costs) (x : Int) (f : Int → Nat) :
    (xs.map (fun p => p.2 * f p.1)).sum =
      ((xs.filter (fun p => p.1 == x)).map (fun p => p.2 * f p.1)).sum +
      ((xs.filter (fun p => !(p.1 == x))).map (fun p => p.2 * f p.1)).sum := by
  induction xs with
  | nil => simp
  | cons p xs ih =>
      rcases p with ⟨v, w⟩
      by_cases h : v = x
      · subst v
        simp [ih]
        omega
      · have hb : (v == x) = false := by simp [h]
        simp [hb, ih]
        omega

private theorem mass_head_eq (xs : RiskEnvelope.Costs) (w : Nat) (x : Int) :
    RiskEnvelope.mass ((x, w) :: xs) (fun y => y == x) =
      w + RiskEnvelope.mass xs (fun y => y == x) := by
  simp [RiskEnvelope.mass]

private theorem mass_head_ne (xs : RiskEnvelope.Costs) (w : Nat) (x v : Int) (h : v ≠ x) :
    RiskEnvelope.mass ((x, w) :: xs) (fun y => y == v) =
      RiskEnvelope.mass xs (fun y => y == v) := by
  have h' : x ≠ v := by omega
  have hb : (x == v) = false := by simp [h']
  simp [RiskEnvelope.mass, hb]

private theorem mass_filter_ne (xs : RiskEnvelope.Costs) (x v : Int) (h : v ≠ x) :
    RiskEnvelope.mass xs (fun y => y == v) =
      RiskEnvelope.mass (xs.filter (fun q => !(q.1 == x))) (fun y => y == v) := by
  induction xs with
  | nil => simp [RiskEnvelope.mass]
  | cons p xs ih =>
      rcases p with ⟨y, wt⟩
      dsimp [RiskEnvelope.mass] at ih ⊢
      by_cases hyv : y = v
      · subst y
        have h' : v ≠ x := h
        have hb : (v == x) = false := by simp [h']
        simp [hb]
        apply congrArg (fun l : List (Int × Nat) => (l.map Prod.snd).sum)
        apply List.filter_congr
        intro p hp
        by_cases hbv : p.fst == v
        · have hpv : p.fst = v := beq_iff_eq.mp hbv
          have hpx : ¬ p.fst = x := by omega
          simp [hbv, hpx]
        · simp [hbv]
      · have hb : (y == v) = false := by simp [hyv]
        simp [hb, ih]

private theorem mem_dedup_ne (xs : RiskEnvelope.Costs) (x v : Int) :
    v ∈ ((xs.filter (fun q => !(q.1 == x))).map Prod.fst).eraseDups → v ≠ x := by
  intro hv
  rw [List.mem_eraseDups] at hv
  rcases List.mem_map.mp hv with ⟨p, hp, rfl⟩
  have hb := (List.mem_filter.mp hp).2
  intro heq
  have : (p.1 == x) = true := by simp [heq]
  simp [this] at hb

private theorem sum_eraseDups (f : Int → Nat) : ∀ xs : RiskEnvelope.Costs,
    (xs.map (fun p => p.2 * f p.1)).sum =
      (((xs.map Prod.fst).eraseDups).map (fun v =>
        RiskEnvelope.mass xs (fun y => y == v) * f v)).sum := by
  intro xs
  induction hlen : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      cases xs with
      | nil => simp [RiskEnvelope.mass]
      | cons p xs =>
          rcases p with ⟨x, w⟩
          let xs_ne : RiskEnvelope.Costs := xs.filter (fun q => !(q.1 == x))
          have hlen_ne : xs_ne.length < n := by
            have hle : xs_ne.length ≤ xs.length := by
              dsimp [xs_ne]
              exact List.length_filter_le _ _
            simp at hlen
            omega
          have ih_ne := ih xs_ne.length hlen_ne xs_ne rfl
          have hdedup : ((x :: xs.map Prod.fst).eraseDups) =
              x :: ((xs_ne.map Prod.fst).eraseDups) := by
            rw [List.eraseDups_cons]
            congr 1
            dsimp [xs_ne]
            rw [filter_map_fst_ne]
          simp only [Prod.fst, Prod.snd, List.map_cons, List.sum_cons]
          rw [hdedup]
          simp only [List.map_cons, List.sum_cons]
          rw [sum_filter_partition xs x f]
          rw [sum_filter_eq_val xs x f]
          rw [mass_head_eq xs w x]
          rw [ih_ne]
          have hmass : ∀ v ∈ (xs_ne.map Prod.fst).eraseDups,
              RiskEnvelope.mass ((x, w) :: xs) (fun y => y == v) * f v =
                RiskEnvelope.mass xs_ne (fun y => y == v) * f v := by
            intro v hv
            have hvne : v ≠ x := mem_dedup_ne xs x v hv
            rw [mass_head_ne xs w x v hvne]
            dsimp [xs_ne]
            rw [mass_filter_ne xs x v hvne]
          rw [List.map_congr_left hmass]
          rw [Nat.add_mul]
          omega


private theorem weights_length_pos (t : AlphabeticMerge.Tree) : 0 < (AlphabeticMerge.weights t).length := by
  induction t with
  | leaf w => simp [AlphabeticMerge.weights]
  | fork l r ihl ihr => simp [AlphabeticMerge.weights]; omega

private theorem weights_ne_nil (t : AlphabeticMerge.Tree) : AlphabeticMerge.weights t ≠ [] := by
  have hp := weights_length_pos t
  intro h
  rw [h] at hp
  simp at hp

private theorem sum_take_add_sum_drop (ws : List Nat) (k : Nat) :
    (ws.take k).sum + (ws.drop k).sum = ws.sum := by
  rw [← List.sum_append]
  rw [List.take_append_drop]

private theorem minList_mem (L : List Nat) (hne : L ≠ []) :
    ∃ m, L.min? = some m ∧ m ∈ L ∧ ∀ b ∈ L, m ≤ b := by
  cases L with
  | nil => contradiction
  | cons x xs =>
      have hs : (x :: xs).min? = some ((x :: xs).min?.getD 0) := by
        cases h : (x :: xs).min? with
        | none =>
            have hnil : x :: xs = [] := List.min?_eq_none_iff.mp h
            simp at hnil
        | some m => simp [h]
      refine ⟨(x :: xs).min?.getD 0, hs, ?_, ?_⟩
      · exact (List.min?_eq_some_iff.mp hs).1
      · exact (List.min?_eq_some_iff.mp hs).2

private theorem min_le_of_mem (L : List Nat) (a : Nat) (ha : a ∈ L) : L.min?.getD 0 ≤ a := by
  by_cases h : L = []
  · subst L; simp at ha
  · rcases minList_mem L h with ⟨m, hm, hmem, hle⟩
    rw [hm]
    simp
    exact hle a ha

private theorem opt_bound_aux (n : Nat) : ∀ (ws : List Nat) (fuel : Nat),
    ws.length = n → ws ≠ [] → n ≤ fuel →
    (∃ t, AlphabeticMerge.weights t = ws ∧ AlphabeticMerge.work t = AlphabeticMerge.dp fuel ws) ∧
      ∀ t, AlphabeticMerge.weights t = ws → AlphabeticMerge.dp fuel ws ≤ AlphabeticMerge.work t := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ws fuel hlen hne hfuel
      cases ws with
      | nil => contradiction
      | cons x xs =>
          cases xs with
          | nil =>
              constructor
              · refine ⟨.leaf x, ?_, ?_⟩
                · simp [AlphabeticMerge.weights]
                · cases fuel <;> simp [AlphabeticMerge.dp, AlphabeticMerge.work]
              · intro t ht
                cases t with
                | leaf w =>
                    simp [AlphabeticMerge.weights] at ht
                    subst w
                    cases fuel <;> simp [AlphabeticMerge.dp, AlphabeticMerge.work]
                | fork l r =>
                    have hl := weights_length_pos l
                    have hr := weights_length_pos r
                    have hsum : (AlphabeticMerge.weights l).length + (AlphabeticMerge.weights r).length = 1 := by
                      have := congrArg List.length ht
                      simpa [AlphabeticMerge.weights] using this
                    omega
          | cons y ys =>
              let ww : List Nat := x :: y :: ys
              let f : Nat := fuel - 1
              let L : List Nat := (List.range (ww.length - 1)).map
                (fun i => AlphabeticMerge.dp f (ww.take (i+1)) + AlphabeticMerge.dp f (ww.drop (i+1)))
              have hfuelpos : 0 < fuel := by dsimp [ww] at hlen; omega
              have hf_eq : fuel = f + 1 := by dsimp [f]; omega
              have hwwn : ww.length = n := by dsimp [ww]; omega
              have hwwfuel : ww.length ≤ fuel := by omega
              have hwwlen2 : 2 ≤ ww.length := by dsimp [ww]; simp
              have hLne : L ≠ [] := by
                dsimp [L]
                have hpos : 0 < ww.length - 1 := by omega
                simp [List.range_eq_nil]
                omega
              have hsub_fuel (i : Nat) (hi : i ∈ List.range (ww.length-1)) :
                  (ww.take (i+1)).length ≤ f ∧ (ww.drop (i+1)).length ≤ f := by
                rw [List.mem_range] at hi
                have ht : (ww.take (i+1)).length = i+1 := List.length_take_of_le (by omega)
                rw [ht, List.length_drop]
                dsimp [f]
                omega
              have hex_min : ∃ i ∈ List.range (ww.length-1),
                  AlphabeticMerge.dp f (ww.take (i+1)) + AlphabeticMerge.dp f (ww.drop (i+1)) = L.min?.getD 0 := by
                rcases minList_mem L hLne with ⟨m, hm, hmem, hleall⟩
                rw [hm]
                dsimp [L] at hmem
                rcases List.mem_map.mp hmem with ⟨i, hi, hival⟩
                refine ⟨i, hi, ?_⟩
                simpa using hival
              constructor
              · rcases hex_min with ⟨i, hi, hmin⟩
                have hi_lt : i < ww.length - 1 := by simpa [List.mem_range] using hi
                have htk_len : (ww.take (i+1)).length = i+1 := by
                  apply List.length_take_of_le
                  omega
                have hdr_len : (ww.drop (i+1)).length = ww.length - (i+1) := List.length_drop
                have htk_ne : ww.take (i+1) ≠ [] := by
                  intro h
                  have hh := congrArg List.length h
                  rw [htk_len] at hh
                  simp at hh
                have hdr_ne : ww.drop (i+1) ≠ [] := by
                  intro h
                  have hh := congrArg List.length h
                  rw [hdr_len] at hh
                  simp at hh
                  omega
                have hsf := hsub_fuel i hi
                have hlt1 : (ww.take (i+1)).length < n := by rw [htk_len]; omega
                have hlt2 : (ww.drop (i+1)).length < n := by rw [hdr_len]; omega
                have ht1 := ih _ hlt1 (ww.take (i+1)) f rfl htk_ne hsf.1
                have ht2 := ih _ hlt2 (ww.drop (i+1)) f rfl hdr_ne hsf.2
                rcases ht1.1 with ⟨t1, hw1, hc1⟩
                rcases ht2.1 with ⟨t2, hw2, hc2⟩
                refine ⟨.fork t1 t2, ?_, ?_⟩
                · rw [AlphabeticMerge.weights, hw1, hw2]
                  exact List.take_append_drop (i+1) ww
                · have hnotle : ¬ ww.length ≤ 1 := by omega
                  have hdp : AlphabeticMerge.dp fuel ww = ww.sum + L.min?.getD 0 := by
                    rw [hf_eq]
                    simp [AlphabeticMerge.dp, hnotle, f, L]
                  change AlphabeticMerge.work t1 + AlphabeticMerge.work t2 +
                      (AlphabeticMerge.weights t1).sum + (AlphabeticMerge.weights t2).sum =
                        AlphabeticMerge.dp fuel ww
                  rw [hw1, hw2, hc1, hc2, hdp]
                  have hs := sum_take_add_sum_drop ww (i+1)
                  omega
              · intro t ht
                cases t with
                | leaf w => simp [AlphabeticMerge.weights] at ht
                | fork l r =>
                    let k := (AlphabeticMerge.weights l).length
                    have hkpos : 0 < k := weights_length_pos l
                    have hsplit0 : AlphabeticMerge.weights l ++ AlphabeticMerge.weights r = x :: y :: ys := by
                      simpa [AlphabeticMerge.weights] using ht
                    have hsplit : ww = AlphabeticMerge.weights l ++ AlphabeticMerge.weights r := by
                      dsimp [ww]
                      exact hsplit0.symm
                    have hklt : k < ww.length := by
                      rw [hsplit]
                      simp [k]
                      have hr := weights_length_pos r
                      omega
                    have htake : ww.take k = AlphabeticMerge.weights l := by
                      rw [hsplit]
                      simp [k]
                    have hdrop : ww.drop k = AlphabeticMerge.weights r := by
                      rw [hsplit]
                      simp [k]
                    have htk_ne : ww.take k ≠ [] := by simpa [htake] using weights_ne_nil l
                    have hdr_ne : ww.drop k ≠ [] := by simpa [hdrop] using weights_ne_nil r
                    have hktk : (ww.take k).length = k := by rw [htake]
                    have hkdk : (ww.drop k).length = ww.length - k := List.length_drop
                    have hsf1 : (ww.take k).length ≤ f := by rw [hktk]; dsimp [f]; omega
                    have hsf2 : (ww.drop k).length ≤ f := by rw [hkdk]; dsimp [f]; omega
                    have hlt1 : (ww.take k).length < n := by rw [hktk]; omega
                    have hlt2 : (ww.drop k).length < n := by rw [hkdk]; omega
                    have ht1 := ih _ hlt1 (ww.take k) f rfl htk_ne hsf1
                    have ht2 := ih _ hlt2 (ww.drop k) f rfl hdr_ne hsf2
                    have hle1 : AlphabeticMerge.dp f (ww.take k) ≤ AlphabeticMerge.work l :=
                      ht1.2 l htake.symm
                    have hle2 : AlphabeticMerge.dp f (ww.drop k) ≤ AlphabeticMerge.work r :=
                      ht2.2 r hdrop.symm
                    have hkidx : k - 1 ∈ List.range (ww.length-1) := by
                      rw [List.mem_range]
                      omega
                    have hmemin : AlphabeticMerge.dp f (ww.take ((k-1)+1)) +
                        AlphabeticMerge.dp f (ww.drop ((k-1)+1)) ∈ L := by
                      dsimp [L]
                      apply List.mem_map.mpr
                      exact ⟨k-1, hkidx, rfl⟩
                    have hminle : L.min?.getD 0 ≤
                        AlphabeticMerge.dp f (ww.take ((k-1)+1)) + AlphabeticMerge.dp f (ww.drop ((k-1)+1)) :=
                      min_le_of_mem L _ hmemin
                    have hk1 : k - 1 + 1 = k := by omega
                    rw [hk1] at hminle
                    have hnotle : ¬ ww.length ≤ 1 := by omega
                    have hdp : AlphabeticMerge.dp fuel ww = ww.sum + L.min?.getD 0 := by
                      rw [hf_eq]
                      simp [AlphabeticMerge.dp, hnotle, f, L]
                    rw [hdp]
                    change ww.sum + L.min?.getD 0 ≤
                      AlphabeticMerge.work l + AlphabeticMerge.work r +
                        (AlphabeticMerge.weights l).sum + (AlphabeticMerge.weights r).sum
                    have hs2 : ww.sum = (AlphabeticMerge.weights l).sum +
                        (AlphabeticMerge.weights r).sum := by
                      rw [hsplit]
                      simp
                    rw [hs2]
                    omega


private theorem modify_append_left (l1 l2 : List Nat) (i d : Nat) (h : i < l1.length) :
    (l1 ++ l2).modify i (·+d) = l1.modify i (·+d) ++ l2 := by
  induction l1 generalizing i with
  | nil => simp at h
  | cons x xs ih =>
      cases i with
      | zero => simp [List.modify_cons]
      | succ i =>
          simp [List.modify_cons]
          apply ih i
          simp at h ⊢
          omega

private theorem modify_append_right (l1 l2 : List Nat) (i d : Nat) :
    (l1 ++ l2).modify (l1.length + i) (·+d) = l1 ++ l2.modify i (·+d) := by
  induction l1 generalizing i with
  | nil => simp
  | cons x xs ih =>
      simp [List.modify_cons]
      rw [ih i]

private theorem sum_modify_add (l : List Nat) (i d : Nat) (hi : i < l.length) :
    (l.modify i (·+d)).sum = l.sum + d := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero =>
          simp [List.modify_cons]
          omega
      | succ i =>
          simp [List.modify_cons]
          rw [ih i (by simp at hi; omega)]
          omega

private def bumpLeaf : AlphabeticMerge.Tree → Nat → Nat → AlphabeticMerge.Tree
  | .leaf w, _, d => .leaf (w+d)
  | .fork l r, i, d =>
      if i < (AlphabeticMerge.weights l).length then
        .fork (bumpLeaf l i d) r
      else
        .fork l (bumpLeaf r (i - (AlphabeticMerge.weights l).length) d)

private theorem weights_bumpLeaf (t : AlphabeticMerge.Tree) (i d : Nat)
    (hi : i < (AlphabeticMerge.weights t).length) :
    AlphabeticMerge.weights (bumpLeaf t i d) = (AlphabeticMerge.weights t).modify i (·+d) := by
  induction t generalizing i with
  | leaf w =>
      simp [AlphabeticMerge.weights] at hi
      subst i
      simp [bumpLeaf, AlphabeticMerge.weights, List.modify_cons]
  | fork l r ihl ihr =>
      by_cases h : i < (AlphabeticMerge.weights l).length
      · simp only [bumpLeaf, h]
        change AlphabeticMerge.weights (bumpLeaf l i d) ++ AlphabeticMerge.weights r =
          (AlphabeticMerge.weights l ++ AlphabeticMerge.weights r).modify i (·+d)
        rw [ihl i h]
        exact (modify_append_left (AlphabeticMerge.weights l) (AlphabeticMerge.weights r) i d h).symm
      · have hle : (AlphabeticMerge.weights l).length ≤ i := by omega
        have hir : i - (AlphabeticMerge.weights l).length < (AlphabeticMerge.weights r).length := by
          simp [AlphabeticMerge.weights] at hi
          omega
        simp only [bumpLeaf, h]
        change AlphabeticMerge.weights l ++
            AlphabeticMerge.weights (bumpLeaf r (i - (AlphabeticMerge.weights l).length) d) =
          (AlphabeticMerge.weights l ++ AlphabeticMerge.weights r).modify i (·+d)
        rw [ihr (i - (AlphabeticMerge.weights l).length) hir]
        have hidx : (AlphabeticMerge.weights l).length + (i - (AlphabeticMerge.weights l).length) = i := by omega
        rw [← hidx]
        have hsub : ((AlphabeticMerge.weights l).length +
            (i - (AlphabeticMerge.weights l).length)) - (AlphabeticMerge.weights l).length =
            i - (AlphabeticMerge.weights l).length := by omega
        rw [hsub]
        exact (modify_append_right (AlphabeticMerge.weights l) (AlphabeticMerge.weights r)
          (i - (AlphabeticMerge.weights l).length) d).symm

private theorem sums_bumpLeaf (t : AlphabeticMerge.Tree) (i d : Nat)
    (hi : i < (AlphabeticMerge.weights t).length) :
    (AlphabeticMerge.weights (bumpLeaf t i d)).sum =
      (AlphabeticMerge.weights t).sum + d := by
  rw [weights_bumpLeaf t i d hi]
  exact sum_modify_add _ _ _ hi

private theorem work_bumpLeaf (t : AlphabeticMerge.Tree) (i d : Nat)
    (hi : i < (AlphabeticMerge.weights t).length) :
    AlphabeticMerge.work (bumpLeaf t i d) = AlphabeticMerge.work t + d * AlphabeticMerge.depth t i := by
  induction t generalizing i with
  | leaf w =>
      simp [AlphabeticMerge.weights] at hi
      subst i
      simp [bumpLeaf, AlphabeticMerge.work, AlphabeticMerge.depth]
  | fork l r ihl ihr =>
      by_cases h : i < (AlphabeticMerge.weights l).length
      · simp [bumpLeaf, h, AlphabeticMerge.work, AlphabeticMerge.depth]
        rw [ihl i h]
        rw [sums_bumpLeaf l i d h]
        simp [Nat.mul_add]
        omega
      · have hir : i - (AlphabeticMerge.weights l).length < (AlphabeticMerge.weights r).length := by
          simp [AlphabeticMerge.weights] at hi
          omega
        simp [bumpLeaf, h, AlphabeticMerge.work, AlphabeticMerge.depth]
        rw [ihr (i - (AlphabeticMerge.weights l).length) hir]
        rw [sums_bumpLeaf r (i - (AlphabeticMerge.weights l).length) d hir]
        simp [Nat.mul_add]
        omega


private theorem sum_map_mul (c : Nat) (ws : List Nat) : (ws.map (c*·)).sum = c * ws.sum := by
  induction ws with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.mul_add]

private theorem min?_map_mul (c : Nat) (L : List Nat) :
    (L.map (c*·)).min?.getD 0 = c * L.min?.getD 0 := by
  cases L with
  | nil => simp
  | cons x xs =>
      have hs : (x :: xs).min? = some ((x :: xs).min?.getD 0) := by
        cases h : (x :: xs).min? with
        | none => have hn : x :: xs = [] := List.min?_eq_none_iff.mp h; simp at hn
        | some m => simp [h]
      have hsmap : ((x :: xs).map (c*·)).min? = some (c * (x :: xs).min?.getD 0) := by
        apply List.min?_eq_some_iff.mpr
        constructor
        · have hm := (List.min?_eq_some_iff.mp hs).1
          rw [List.mem_map]
          exact ⟨(x :: xs).min?.getD 0, hm, rfl⟩
        · intro b hb
          rcases List.mem_map.mp hb with ⟨y, hy, rfl⟩
          exact Nat.mul_le_mul_left c ((List.min?_eq_some_iff.mp hs).2 y hy)
      rw [hs, hsmap]
      simp

private theorem dp_scale (c fuel : Nat) : ∀ ws : List Nat,
    AlphabeticMerge.dp fuel (ws.map (c*·)) = c * AlphabeticMerge.dp fuel ws := by
  induction fuel with
  | zero => intro ws; simp [AlphabeticMerge.dp]
  | succ f ih =>
      intro ws
      by_cases hlen : ws.length ≤ 1
      · have hlen' : (ws.map (c*·)).length ≤ 1 := by simpa using hlen
        simp [AlphabeticMerge.dp, hlen, hlen']
      · have hlen' : ¬ (ws.map (c*·)).length ≤ 1 := by simpa using hlen
        simp [AlphabeticMerge.dp, hlen, hlen']
        have hmap (i : Nat) :
            (ws.map (c*·)).take (i+1) = (ws.take (i+1)).map (c*·) := by
          exact (List.map_take (f := fun x => c*x) (i := i+1) (l := ws)).symm
        have hdrop (i : Nat) :
            (ws.map (c*·)).drop (i+1) = (ws.drop (i+1)).map (c*·) := by
          exact (List.map_drop (f := fun x => c*x) (i := i+1) (l := ws)).symm
        simp only [hmap, hdrop, ih]
        rw [sum_map_mul]
        have hmins :
            (List.map (fun i => c * AlphabeticMerge.dp f (List.take (i+1) ws) +
                c * AlphabeticMerge.dp f (List.drop (i+1) ws))
                (List.range (ws.length - 1))).min?.getD 0 =
              c * (List.map (fun i => AlphabeticMerge.dp f (List.take (i+1) ws) +
                AlphabeticMerge.dp f (List.drop (i+1) ws))
                (List.range (ws.length - 1))).min?.getD 0 := by
          have heq : List.map (fun i => c * AlphabeticMerge.dp f (List.take (i+1) ws) +
                c * AlphabeticMerge.dp f (List.drop (i+1) ws))
                (List.range (ws.length - 1)) =
              (List.map (fun i => AlphabeticMerge.dp f (List.take (i+1) ws) +
                AlphabeticMerge.dp f (List.drop (i+1) ws))
                (List.range (ws.length - 1))).map (c*·) := by
            rw [List.map_map]
            apply List.map_congr_left
            intro i hi
            rw [← Nat.mul_add]
            rfl
          rw [heq, min?_map_mul]
        rw [hmins, Nat.mul_add]

private theorem best_scale (c : Nat) (ws : List Nat) :
    AlphabeticMerge.best (ws.map (c*·)) = c * AlphabeticMerge.best ws := by
  dsimp [AlphabeticMerge.best]
  rw [dp_scale]
  simp

private theorem merge_congr_aux (n : Nat) : ∀ {α : Type u} (le1 le2 : α → α → Bool)
    (l₁ l₂ : List α), l₁.length + l₂.length = n →
    (∀ a ∈ l₁ ++ l₂, ∀ b ∈ l₁ ++ l₂, le1 a b = le2 a b) →
    List.merge l₁ l₂ le1 = List.merge l₁ l₂ le2 := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro α le1 le2 l₁ l₂ hlen h
      cases l₁ with
      | nil => simp
      | cons x xs =>
          cases l₂ with
          | nil => simp
          | cons y ys =>
              simp only [List.merge]
              rw [h x (by simp) y (by simp)]
              by_cases h2 : le2 x y
              · simp [h2]
                apply ih (xs.length + (y :: ys).length) (by simp at hlen ⊢; omega) le1 le2 xs (y :: ys) rfl
                intro a ha b hb
                exact h a (List.mem_cons_of_mem x ha) b (List.mem_cons_of_mem x hb)
              · simp [h2]
                apply ih ((x :: xs).length + ys.length) (by simp at hlen ⊢; omega) le1 le2 (x :: xs) ys rfl
                intro a ha b hb
                have hsub : ∀ {z : α}, z ∈ (x :: xs) ++ ys → z ∈ (x :: xs) ++ (y :: ys) := by
                  intro z hz
                  rw [List.mem_append] at hz ⊢
                  rcases hz with hleft | hright
                  · exact Or.inl hleft
                  · exact Or.inr (List.mem_cons_of_mem y hright)
                exact h a (hsub ha) b (hsub hb)

private theorem merge_congr {α : Type u} (le1 le2 : α → α → Bool)
    (l₁ l₂ : List α)
    (h : ∀ a ∈ l₁ ++ l₂, ∀ b ∈ l₁ ++ l₂, le1 a b = le2 a b) :
    List.merge l₁ l₂ le1 = List.merge l₁ l₂ le2 :=
  merge_congr_aux (l₁.length + l₂.length) le1 le2 l₁ l₂ rfl h

private theorem mergeSort_congr_aux {α : Type u} (n : Nat) : ∀ (xs : List α) (le1 le2 : α → α → Bool),
    xs.length = n →
    (∀ a ∈ xs, ∀ b ∈ xs, le1 a b = le2 a b) →
    xs.mergeSort le1 = xs.mergeSort le2 := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs le1 le2 hlen h
      cases xs with
      | nil => simp
      | cons x xs1 =>
          cases xs1 with
          | nil => simp
          | cons y ys =>
              rw [List.mergeSort, List.mergeSort]
              let lr := List.MergeSort.Internal.splitInTwo ⟨x :: y :: ys, rfl⟩
              have happ : lr.1.1 ++ lr.2.1 = x :: y :: ys :=
                List.MergeSort.Internal.splitInTwo_fst_append_splitInTwo_snd _
              have hl1 : lr.1.1.length < n := by
                dsimp [lr] at hlen ⊢
                simp [List.MergeSort.Internal.splitInTwo_fst]
                omega
              have hl2 : lr.2.1.length < n := by
                dsimp [lr] at hlen ⊢
                simp [List.MergeSort.Internal.splitInTwo_snd]
                omega
              have hm1 : ∀ a ∈ lr.1.1, ∀ b ∈ lr.1.1, le1 a b = le2 a b := by
                intro a ha b hb
                exact h a (by rw [← happ]; exact List.mem_append.mpr (Or.inl ha))
                  b (by rw [← happ]; exact List.mem_append.mpr (Or.inl hb))
              have hm2 : ∀ a ∈ lr.2.1, ∀ b ∈ lr.2.1, le1 a b = le2 a b := by
                intro a ha b hb
                exact h a (by rw [← happ]; exact List.mem_append.mpr (Or.inr ha))
                  b (by rw [← happ]; exact List.mem_append.mpr (Or.inr hb))
              rw [ih _ hl1 lr.1.1 le1 le2 rfl hm1, ih _ hl2 lr.2.1 le1 le2 rfl hm2]
              change (lr.1.1.mergeSort le2).merge (lr.2.1.mergeSort le2) le1 =
                (lr.1.1.mergeSort le2).merge (lr.2.1.mergeSort le2) le2
              apply merge_congr
              intro a ha b hb
              have ha' : a ∈ lr.1.1 ∨ a ∈ lr.2.1 := by
                rw [List.mem_append] at ha
                rcases ha with ha | ha
                · exact Or.inl (List.mem_mergeSort.mp ha)
                · exact Or.inr (List.mem_mergeSort.mp ha)
              have hb' : b ∈ lr.1.1 ∨ b ∈ lr.2.1 := by
                rw [List.mem_append] at hb
                rcases hb with hb | hb
                · exact Or.inl (List.mem_mergeSort.mp hb)
                · exact Or.inr (List.mem_mergeSort.mp hb)
              exact h a (by rw [← happ]; exact List.mem_append.mpr ha')
                b (by rw [← happ]; exact List.mem_append.mpr hb')

private theorem mergeSort_congr {α : Type u} (xs : List α) (le1 le2 : α → α → Bool)
    (h : ∀ a ∈ xs, ∀ b ∈ xs, le1 a b = le2 a b) :
    xs.mergeSort le1 = xs.mergeSort le2 :=
  mergeSort_congr_aux xs.length xs le1 le2 rfl h


private theorem t01_sum_modify_le (xs : List Nat) (i d : Nat) :
    (xs.modify i (·+d)).sum ≤ xs.sum + d := by
  induction xs generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp [List.modify_cons]; omega
      | succ i =>
          simp [List.modify_cons]
          have h := ih i
          omega

private theorem drop_modify_ge (xs : List Nat) (c i d : Nat) (hci : c ≤ i) :
    (xs.modify i (·+d)).drop c = (xs.drop c).modify (i-c) (·+d) := by
  induction xs generalizing c i with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => simp
      | succ c =>
          cases i with
          | zero => omega
          | succ i =>
              simp [List.modify_cons]
              apply ih c i
              omega

private theorem drop_modify_lt (xs : List Nat) (c i d : Nat) (hic : i < c) :
    (xs.modify i (·+d)).drop c = xs.drop c := by
  induction xs generalizing c i with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => omega
      | succ c =>
          cases i with
          | zero =>
              simp [List.modify_cons]
          | succ i =>
              simp [List.modify_cons]
              apply ih c i
              omega

private theorem take_modify_le (xs : List Nat) (c i d : Nat) :
    ((xs.modify i (·+d)).take c).sum ≤ (xs.take c).sum + d := by
  induction xs generalizing c i with
  | nil => simp
  | cons x xs ih =>
      cases c with
      | zero => simp
      | succ c =>
          cases i with
          | zero =>
              simp [List.modify_cons]
              omega
          | succ i =>
              simp [List.modify_cons]
              have h := ih c i
              omega

private theorem peak_cons (g : List Nat) (gs : List (List Nat)) :
    ParallelCuts.peak (g :: gs) = max g.sum (ParallelCuts.peak gs) := by
  unfold ParallelCuts.peak
  cases h : (gs.map List.sum).max? with
  | none => simp [h]
  | some m => simp [h]

private theorem local_correction_aux (xs : List Nat) (cs : List Nat) (i d : Nat) :
    ParallelCuts.peak (ParallelCuts.groups (xs.modify i (·+d)) cs) ≤
      ParallelCuts.peak (ParallelCuts.groups xs cs) + d := by
  induction cs generalizing xs i with
  | nil =>
      simp [ParallelCuts.groups, ParallelCuts.peak]
      exact t01_sum_modify_le xs i d
  | cons c cs ih =>
      simp only [ParallelCuts.groups]
      have hfirst : ((xs.modify i (·+d)).take c).sum ≤ (xs.take c).sum + d :=
        take_modify_le xs c i d
      -- tail: xs.drop c, index i-c if c ≤ i else unchanged
      by_cases hci : c ≤ i
      · have hdrop := drop_modify_ge xs c i d hci
        have htail := ih (xs.drop c) (i-c)
        have htail' : ParallelCuts.peak (ParallelCuts.groups ((xs.drop c).modify (i-c) (·+d)) cs) ≤
            ParallelCuts.peak (ParallelCuts.groups (xs.drop c) cs) + d := htail
        rw [← hdrop] at htail'
        rw [peak_cons, peak_cons]
        apply Nat.max_le.mpr
        constructor <;> omega
      · have hic : i < c := by omega
        have hdrop := drop_modify_lt xs c i d hic
        have htail := ih (xs.drop c) 0
        -- modify with index 0 out of range when tail empty? t01_sum_modify_le still holds
        have htail' : ParallelCuts.peak (ParallelCuts.groups (xs.drop c) cs) ≤
            ParallelCuts.peak (ParallelCuts.groups (xs.drop c) cs) + d := by omega
        rw [hdrop]
        rw [peak_cons, peak_cons]
        apply Nat.max_le.mpr
        constructor <;> omega


theorem sound_RatioSort : PredicateOrder.RatioSort.Goal := RatioSort_sound
theorem sound_OptimalTree : AlphabeticMerge.OptimalTree.Goal := by
  intro ws hne
  have h := opt_bound_aux ws.length ws ws.length rfl hne (by omega)
  constructor
  · exact h.1
  · intro t ht
    exact h.2 t ht

theorem sound_ReweightBound : AlphabeticMerge.ReweightBound.Goal := by
  intro t i d hi hopt
  have hw := work_bumpLeaf t i d hi
  have hweights := weights_bumpLeaf t i d hi
  rw [← hweights]
  dsimp [AlphabeticMerge.best]
  have hle := (opt_bound_aux (AlphabeticMerge.weights (bumpLeaf t i d)).length
      (AlphabeticMerge.weights (bumpLeaf t i d)) (AlphabeticMerge.weights (bumpLeaf t i d)).length
      rfl (weights_ne_nil _) (by omega)).2 (bumpLeaf t i d) rfl
  calc
    AlphabeticMerge.dp (AlphabeticMerge.weights (bumpLeaf t i d)).length
        (AlphabeticMerge.weights (bumpLeaf t i d)) ≤ AlphabeticMerge.work (bumpLeaf t i d) := hle
    _ = AlphabeticMerge.work t + d * AlphabeticMerge.depth t i := hw

theorem sound_ScaleReuse : AlphabeticMerge.ScaleReuse.Goal := by
  intro ws c hc
  unfold AlphabeticMerge.cut
  have hlen : (ws.map (c*·)).length = ws.length := by simp
  have hcmp : ∀ a ∈ (List.range (ws.length-1)).map (·+1),
      ∀ b ∈ (List.range (ws.length-1)).map (·+1),
      (decide (AlphabeticMerge.best ((ws.map (c*·)).take a) + AlphabeticMerge.best ((ws.map (c*·)).drop a) ≤
       AlphabeticMerge.best ((ws.map (c*·)).take b) + AlphabeticMerge.best ((ws.map (c*·)).drop b))) =
      (decide (AlphabeticMerge.best (ws.take a) + AlphabeticMerge.best (ws.drop a) ≤
       AlphabeticMerge.best (ws.take b) + AlphabeticMerge.best (ws.drop b))) := by
    intro a ha b hb
    have hta : (ws.map (c*·)).take a = (ws.take a).map (c*·) :=
      (List.map_take (f := fun x => c*x) (i := a) (l := ws)).symm
    have hda : (ws.map (c*·)).drop a = (ws.drop a).map (c*·) :=
      (List.map_drop (f := fun x => c*x) (i := a) (l := ws)).symm
    have htb : (ws.map (c*·)).take b = (ws.take b).map (c*·) :=
      (List.map_take (f := fun x => c*x) (i := b) (l := ws)).symm
    have hdb : (ws.map (c*·)).drop b = (ws.drop b).map (c*·) :=
      (List.map_drop (f := fun x => c*x) (i := b) (l := ws)).symm
    rw [hta, hda, htb, hdb, best_scale c, best_scale c, best_scale c, best_scale c]
    rw [← Nat.mul_add, ← Nat.mul_add]
    by_cases hab : AlphabeticMerge.best (List.take a ws) + AlphabeticMerge.best (List.drop a ws) ≤
        AlphabeticMerge.best (List.take b ws) + AlphabeticMerge.best (List.drop b ws)
    · have hc' : c * (AlphabeticMerge.best (List.take a ws) + AlphabeticMerge.best (List.drop a ws)) ≤
          c * (AlphabeticMerge.best (List.take b ws) + AlphabeticMerge.best (List.drop b ws)) :=
        Nat.mul_le_mul_left c hab
      simp [hab, hc']
    · have hc' : ¬ c * (AlphabeticMerge.best (List.take a ws) + AlphabeticMerge.best (List.drop a ws)) ≤
          c * (AlphabeticMerge.best (List.take b ws) + AlphabeticMerge.best (List.drop b ws)) := by
        intro hh
        exact hab (Nat.le_of_mul_le_mul_left hh hc)
      simp [hab, hc']
  rw [hlen]
  rw [mergeSort_congr _ _ _ hcmp]

theorem sound_Feasible : ParallelCuts.Feasible.Goal := Feasible_sound
theorem sound_CapacitySearch : ParallelCuts.CapacitySearch.Goal := CapacitySearch_sound
theorem sound_ReverseScan : ParallelCuts.ReverseScan.Goal := ReverseScan_sound
theorem sound_BoundarySlack : ParallelCuts.BoundarySlack.Goal := BoundarySlack_sound
theorem sound_LocalCorrection : ParallelCuts.LocalCorrection.Goal := by
  intro xs cs i d
  exact local_correction_aux xs cs i d

theorem sound_QuantileReserve : RiskEnvelope.QuantileReserve.Goal := by
  intro xs z a b h1 h2 y
  have hpartz := mass_le_add_gt xs z
  have hpartp := mass_le_add_gt xs (z-1)
  rw [mass_le_pred xs z] at hpartp
  rw [mass_gt_pred xs z] at hpartp
  have hab : ((a + b : Nat) : Int) = (a:Int) + (b:Int) := by omega
  have h2c : ((a : Int) * (RiskEnvelope.mass xs (fun _ => true) : Int) ≤
      ((a : Int) + (b : Int)) * (RiskEnvelope.mass xs (fun x => decide (x ≤ z)) : Int)) := by
    have hh := Int.ofNat_le.mpr h2
    simp at hh
    exact hh
  have h1c : (((a : Int) + (b : Int)) * (RiskEnvelope.mass xs (fun x => decide (x < z)) : Int) ≤
      (a : Int) * (RiskEnvelope.mass xs (fun _ => true) : Int)) := by
    have hh := Int.ofNat_le.mpr h1
    simp at hh
    exact hh
  have hpartzI : (RiskEnvelope.mass xs (fun x => decide (x ≤ z)) : Int) +
      (RiskEnvelope.mass xs (fun x => decide (z < x)) : Int) =
        (RiskEnvelope.mass xs (fun _ => true) : Int) := by
    have := congrArg (fun n : Nat => (n : Int)) hpartz
    simpa using this
  have hpartpI : (RiskEnvelope.mass xs (fun x => decide (x < z)) : Int) +
      (RiskEnvelope.mass xs (fun x => decide (z ≤ x)) : Int) =
        (RiskEnvelope.mass xs (fun _ => true) : Int) := by
    have := congrArg (fun n : Nat => (n : Int)) hpartp
    simpa using this
  have h2e := h2c
  rw [← hpartzI] at h2e
  simp [Int.mul_add, Int.add_mul] at h2e
  have h1e := h1c
  rw [← hpartpI] at h1e
  simp [Int.mul_add, Int.add_mul] at h1e
  have hslope_z : 0 ≤ slope xs a b z := by
    dsimp [slope]
    omega
  have hslope_pred : slope xs a b (z-1) ≤ 0 := by
    dsimp [slope]
    rw [mass_le_pred xs z, mass_gt_pred xs z]
    omega
  by_cases hyz : z ≤ y
  · let k : Nat := (y-z).toNat
    have hyk : y = z + (k : Int) := by
      dsimp [k]
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ y-z)]
      omega
    rw [hyk]
    induction k with
    | zero => simp
    | succ k ih =>
        have hcast : ((k+1 : Nat) : Int) = (k:Int) + 1 := by omega
        rw [hcast]
        rw [show z + ((k:Int)+1) = z + (k:Int) + 1 by omega]
        have hsl : 0 ≤ slope xs a b (z + (k:Int)) := by
          have hm := slope_mono xs a b (show z ≤ z + (k:Int) by omega)
          omega
        have hs := step_eq xs (z + (k:Int)) a b
        have hs' : (RiskEnvelope.loss xs (z + (k:Int) + 1) a b : Int) -
            RiskEnvelope.loss xs (z + (k:Int)) a b =
            slope xs a b (z + (k:Int)) := by
          simpa [slope, Int.add_assoc] using hs
        omega
  · have hyz' : y ≤ z := by omega
    let k : Nat := (z-y).toNat
    have hyk : y = z - (k : Int) := by
      dsimp [k]
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ z-y)]
      omega
    rw [hyk]
    induction k with
    | zero => simp
    | succ k ih =>
        have hcast : ((k+1 : Nat) : Int) = (k:Int) + 1 := by omega
        rw [hcast]
        rw [show z - ((k:Int)+1) = z - (k:Int) - 1 by omega]
        have hsl : slope xs a b (z - (k:Int) - 1) ≤ 0 := by
          have hm := slope_mono xs a b (show z - (k:Int) - 1 ≤ z - 1 by omega)
          omega
        have hs := step_eq xs (z - (k:Int) - 1) a b
        have heq : z - (k:Int) - 1 + 1 = z - (k:Int) := by omega
        rw [heq] at hs
        have hs' : (RiskEnvelope.loss xs (z - (k:Int)) a b : Int) -
            RiskEnvelope.loss xs (z - (k:Int) - 1) a b =
            slope xs a b (z - (k:Int) - 1) := by
          simpa [slope, Int.add_assoc] using hs
        omega

theorem sound_ReservationSlope : RiskEnvelope.ReservationSlope.Goal := ReservationSlope_sound
theorem sound_HistogramFold : RiskEnvelope.HistogramFold.Goal := by
  intro xs z a b
  unfold RiskEnvelope.loss
  simp only [List.map_map, Function.comp_apply]
  exact sum_eraseDups (fun x => a * (x-z).toNat + b * (z-x).toNat) xs

theorem sound_RareWorkload : RiskEnvelope.RareWorkload.Goal := by
  intro xs extra z a b hopt y
  have happ : ∀ t : Int, RiskEnvelope.loss (xs++extra) t a b =
      RiskEnvelope.loss xs t a b + RiskEnvelope.loss extra t a b := by
    intro t
    simp [RiskEnvelope.loss, List.map_append, List.sum_append]
  rw [happ z, happ y]
  have hopt' := hopt y
  have hextra := loss_extra_lipschitz extra z y a b
  omega

theorem disproof_MeanReserve : ¬ RiskEnvelope.MeanReserve.Goal := MeanReserve_disproof
theorem sound_MeanReserve : ¬ RiskEnvelope.MeanReserve.Goal := disproof_MeanReserve
theorem disproof_MarginalTie : ¬ PredicateOrder.MarginalTie.Goal := MarginalTie_disproof
end TinylambOpt.ResearchProofs.Atlas.PlanSearch
