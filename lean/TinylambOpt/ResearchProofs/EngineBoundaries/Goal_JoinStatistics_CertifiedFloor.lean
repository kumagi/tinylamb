import TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics

/-!
Join summaries and statistics must retain the multiplicity or bounds that the SQL
planner derives from them.

Proved

- `CertifiedFloor` — Common residual support gives a lower bound even when all residual
  frequencies are unknown.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_CertifiedFloor

open TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics
open TinylambOpt.ResearchGoals.SearchBounds (fanout)


private theorem le_sum_of_mem {l : List Nat} {v : Nat} (h : v ∈ l) : v ≤ l.sum := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp only [List.mem_cons] at h
      rcases h with rfl | hx
      · exact Nat.le_add_right _ _
      · exact Nat.le_trans (ih hx) (Nat.le_add_left _ _)

private theorem le_max_getD {l : List Nat} {v : Nat} (h : v ∈ l) : v ≤ l.max?.getD 0 := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      rw [List.max?_cons]
      simp only [Option.getD_some]
      cases hxmax : xs.max? with
      | none =>
          have hxs : xs = [] := List.max?_eq_none_iff.mp hxmax
          subst xs
          simp at h ⊢
          omega
      | some m =>
          simp only [Option.elim_some]
          simp only [List.mem_cons] at h
          rcases h with rfl | hv
          · exact Nat.le_max_left _ _
          · exact Nat.le_trans (by simpa [hxmax] using ih hv) (Nat.le_max_right _ _)

private theorem max_getD_le {l : List Nat} {C : Nat} (h : ∀ v ∈ l, v ≤ C) : l.max?.getD 0 ≤ C := by
  cases l with
  | nil => simp
  | cons x xs =>
      rw [List.max?_cons]
      simp only [Option.getD_some]
      cases hxmax : xs.max? with
      | none =>
          simp [hxmax]
          exact h x (by simp)
      | some m =>
          simp only [Option.elim_some]
          exact Nat.max_le.mpr ⟨h x (by simp), by
            have hxs : ∀ v ∈ xs, v ≤ C := fun v hv => h v (by simp [hv])
            have ih := max_getD_le hxs
            simpa [hxmax] using ih⟩

private theorem count_le_cap (l : List Nat) (k : Nat) : l.count k ≤ (l.map l.count).max?.getD 0 := by
  by_cases hk : k ∈ l
  · apply le_max_getD
    exact List.mem_map.mpr ⟨k, hk, rfl⟩
  · rw [List.count_eq_zero.mpr hk]
    exact Nat.zero_le _

private theorem cap_filter_le (xs : List Nat) (p : Nat → Bool) :
    ((xs.filter p).map (xs.filter p).count).max?.getD 0 ≤
      (xs.map xs.count).max?.getD 0 := by
  apply max_getD_le
  intro v hv
  rcases List.mem_map.mp hv with ⟨k, hk, rfl⟩
  have hle : (xs.filter p).count k ≤ xs.count k := by
    simpa [List.count_eq_countP] using
      ((List.filter_sublist (p := p) (l := xs)).countP_le (p := fun y => y == k))
  exact Nat.le_trans hle (count_le_cap xs k)

private theorem sum_tailKeys_count (hot : List Nat) (b : List Nat) (k : Nat) :
    (b.filter (fun x => !hot.contains x)).count k = if k ∈ hot then 0 else b.count k := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x ∈ hot <;> by_cases hk : k ∈ hot
      · simpa [List.contains_eq_mem, hx, hk] using ih
      · have hxne : x ≠ k := by intro h; subst x; exact hk hx
        simp [hx, hk, hxne]
      · have hxne : x ≠ k := by intro h; subst x; exact hx hk
        simpa [List.contains_eq_mem, hx, hk, hxne, List.count_cons] using ih
      · simp [List.contains_eq_mem, hk, hx, List.count_cons]

private theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons]
      exact List.nodup_cons.mpr ⟨by simp, eraseDups_nodup (xs.filter (fun y => !y == x))⟩
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_of_le_of_lt (List.length_filter_le _ xs) (Nat.lt_succ_self _)

private theorem sum_map_eraseP_of_mem (D : List Nat) (hD : D.Nodup) (x : Nat)
    (hx : x ∈ D) (f : Nat → Nat) :
    (D.map f).sum = f x + ((D.eraseP (fun y => y == x)).map f).sum := by
  induction D with
  | nil => simp at hx
  | cons y ys ih =>
      have hyny : y ∉ ys := (List.nodup_cons.mp hD).1
      have hysnd : ys.Nodup := (List.nodup_cons.mp hD).2
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hxys
      · rw [List.eraseP_cons_of_pos]
        · simp
        · simp
      · have hyne : y ≠ x := by intro h; subst x; exact hyny hxys
        have hbe : (y == x) = false := beq_eq_false_iff_ne.mpr hyne
        rw [List.eraseP_cons_of_neg]
        · simp only [List.map_cons, List.sum_cons]
          rw [ih hysnd hxys]
          omega
        · simp [hbe]

private theorem nodup_sum_eq_of_mem (D1 D2 : List Nat) (h1 : D1.Nodup) (h2 : D2.Nodup)
    (hmem : ∀ x, x ∈ D1 ↔ x ∈ D2) (f : Nat → Nat) :
    (D1.map f).sum = (D2.map f).sum := by
  induction D1 generalizing D2 with
  | nil =>
      have hD2 : D2 = [] := by
        cases D2 with
        | nil => rfl
        | cons y ys =>
            have : y ∈ ([] : List Nat) := (hmem y).2 (by simp)
            simp at this
      subst D2
      simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h1).1
      have hxsnd : xs.Nodup := (List.nodup_cons.mp h1).2
      have hx2 : x ∈ D2 := (hmem x).1 (by simp)
      have hD2' : (D2.eraseP (fun y => y == x)).Nodup := h2.eraseP _
      have hmem' : ∀ y, y ∈ xs ↔ y ∈ D2.eraseP (fun z => z == x) := by
        intro y
        by_cases hyx : y = x
        · subst y
          constructor
          · intro h
            exact False.elim (hxnot h)
          · intro h
            have hcD2 : D2.count x = 1 := List.Nodup.count_of_mem h2 hx2
            have hcnt : (D2.eraseP (fun z => z == x)).count x = 0 := by
              rw [← List.erase_eq_eraseP']
              simp [hcD2]
            have hpos : 0 < (D2.eraseP (fun z => z == x)).count x :=
              Nat.pos_of_ne_zero (by intro hz; exact (List.count_eq_zero.mp hz) h)
            omega
        · have hbe : (y == x) = false := beq_eq_false_iff_ne.mpr hyx
          simpa [hbe, hyx] using (hmem y)
      have ih' := ih _ hxsnd hD2' hmem'
      have hs2 := sum_map_eraseP_of_mem D2 h2 x hx2 f
      simp only [List.map_cons, List.sum_cons]
      rw [hs2, ih']

private theorem sum_map_if_eq_zero_of_not_mem (l : List Nat) (k : Nat) (f : Nat → Nat) :
    (l.map (fun y => if y = k then 0 else f y)).sum =
      ((l.filter (fun y => !y == k)).map f).sum := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases hyk : y = k
      · subst y
        simp [ih]
      · simp [hyk, ih]

private theorem eraseDups_filter_erase_sum (l : List Nat) (k : Nat) (f : Nat → Nat) :
    ((l.filter (fun y => !y == k)).eraseDups.map f).sum =
      (l.eraseDups.map (fun y => if y = k then 0 else f y)).sum := by
  rw [sum_map_if_eq_zero_of_not_mem l.eraseDups k f]
  symm
  apply nodup_sum_eq_of_mem
  · exact (eraseDups_nodup l).filter _
  · exact eraseDups_nodup (l.filter (fun y => !y == k))
  · intro x
    simp [List.mem_filter]

private theorem nodup_add_point (D : List Nat) (hD : D.Nodup) (k : Nat) (f : Nat → Nat) :
    (D.map f).sum =
      (D.map (fun y => if y = k then 0 else f y)).sum + if k ∈ D then f k else 0 := by
  induction D with
  | nil => simp
  | cons y ys ih =>
      have hyny : y ∉ ys := (List.nodup_cons.mp hD).1
      have hysnd : ys.Nodup := (List.nodup_cons.mp hD).2
      simp only [List.map_cons, List.sum_cons]
      rw [ih hysnd]
      by_cases hyk : y = k
      · subst y
        simp [hyny]
        omega
      · have hky : k ≠ y := by intro h; exact hyk h.symm
        have hmem : k ∈ y :: ys ↔ k ∈ ys := by simp [hky]
        simp [hyk, hky, hmem]
        rw [Nat.add_assoc]

private theorem nodup_sum_update (D : List Nat) (hD : D.Nodup) (x : Nat) (a b : List Nat) :
    (D.map (fun k => (x :: a).count k * b.count k)).sum =
      (D.map (fun k => a.count k * b.count k)).sum + if x ∈ D then b.count x else 0 := by
  induction D with
  | nil => simp
  | cons k ks ih =>
      have hkks : k ∉ ks := (List.nodup_cons.mp hD).1
      have hksnd : ks.Nodup := (List.nodup_cons.mp hD).2
      simp only [List.map_cons, List.sum_cons]
      rw [ih hksnd]
      simp only [List.count_cons]
      by_cases hxk : x = k
      · subst k
        have hxks : x ∉ ks := hkks
        simp [hxks]
        rw [Nat.add_mul]
        simp [Nat.one_mul]
        ac_rfl
      · have hky : k ≠ x := by intro h; exact hxk h.symm
        have hbe : (x == k) = false := beq_eq_false_iff_ne.mpr hxk
        have hmem : x ∈ k :: ks ↔ x ∈ ks := by
          constructor
          · intro hx
            simp only [List.mem_cons] at hx
            rcases hx with rfl | hx
            · exact False.elim (hxk rfl)
            · exact hx
          · intro hx
            exact List.mem_cons_of_mem k hx
        simp [hxk, hbe, hmem]
        rw [Nat.add_assoc]

private theorem matched_cons (hot a b : List Nat) (x : Nat) :
    (hot.eraseDups.map (fun k => (x :: a).count k * b.count k)).sum =
      (hot.eraseDups.map (fun k => a.count k * b.count k)).sum +
        if x ∈ hot then b.count x else 0 := by
  rw [nodup_sum_update hot.eraseDups (eraseDups_nodup hot) x a b]
  simp [List.mem_eraseDups]

private theorem matched_cons_mem (hot a b : List Nat) (k : Nat) (hk : k ∈ hot) :
    (hot.eraseDups.map (fun y => a.count y * b.count y)).sum =
      ((k :: hot).eraseDups.map (fun y => a.count y * b.count y)).sum := by
  apply nodup_sum_eq_of_mem
  · exact eraseDups_nodup hot
  · exact eraseDups_nodup (k :: hot)
  · intro x
    constructor
    · intro hx
      apply List.mem_eraseDups.mpr
      have hx' : x ∈ hot := List.mem_eraseDups.mp hx
      exact List.mem_cons_of_mem k hx'
    · intro hx
      apply List.mem_eraseDups.mpr
      have hx' : x ∈ k :: hot := List.mem_eraseDups.mp hx
      simp only [List.mem_cons] at hx'
      rcases hx' with rfl | hx'
      · exact hk
      · exact hx' 

private theorem matched_cons_not_mem (hot a b : List Nat) (k : Nat) (hk : k ∉ hot) :
    ((k :: hot).eraseDups.map (fun y => a.count y * b.count y)).sum =
      (hot.eraseDups.map (fun y => a.count y * b.count y)).sum + a.count k * b.count k := by
  rw [List.eraseDups_cons]
  simp only [List.map_cons, List.sum_cons]
  have hfilt : hot.filter (fun y => !y == k) = hot := by
    apply List.filter_eq_self.mpr
    intro y hy
    by_cases hyk : y = k
    · subst y
      simp [hk hy]
    · simp [beq_eq_false_iff_ne.mpr hyk]
  rw [hfilt]
  omega

private theorem split_heavy (hot a b : List Nat) :
    (a.map b.count).sum =
      (hot.eraseDups.map (fun k => a.count k * b.count k)).sum +
        ((a.filter (fun x => !hot.contains x)).map
          (fun x => (b.filter (fun y => !hot.contains y)).count x)).sum := by
  induction a with
  | nil =>
      simp only [List.map_nil, List.sum_nil, List.count_nil, Nat.zero_mul, List.filter_nil]
      clear b
      induction hot.eraseDups with
      | nil => rfl
      | cons k ks ih =>
          simpa using ih
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [matched_cons hot xs b x]
      by_cases hx : x ∈ hot
      · have htail : (x :: xs).filter (fun y => !hot.contains y) =
            xs.filter (fun y => !hot.contains y) := by
          simp [List.contains_eq_mem, hx]
        rw [htail]
        rw [ih]
        simp [hx]
        omega
      · have htail : (x :: xs).filter (fun y => !hot.contains y) =
            x :: xs.filter (fun y => !hot.contains y) := by
          simp [List.contains_eq_mem, hx]
        rw [htail]
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        have hcount : (b.filter (fun y => !hot.contains y)).count x = b.count x := by
          simpa [List.contains_eq_mem, hx] using sum_tailKeys_count hot b x
        rw [hcount]
        simp [hx]
        omega

private theorem tailKeys_cons_mem (hot xs : List Nat) (k : Nat) (hk : k ∈ hot) :
    xs.filter (fun x => !(k :: hot).contains x) = xs.filter (fun x => !hot.contains x) := by
  apply List.filter_congr
  intro x hx
  by_cases hxk : x = k
  · subst x
    simp [List.contains_eq_mem, hk]
  · simp [List.contains_eq_mem, hxk]

private theorem tailKeys_cons_not_mem (hot xs : List Nat) (k : Nat) (hk : k ∉ hot) :
    xs.filter (fun x => !(k :: hot).contains x) =
      (xs.filter (fun x => !hot.contains x)).filter (fun x => !x == k) := by
  rw [show xs.filter (fun x => !(k :: hot).contains x) =
      xs.filter (fun x => !hot.contains x && !x == k) by
        apply List.filter_congr
        intro x hx
        by_cases hxk : x = k <;> by_cases hxh : x ∈ hot <;>
          simp [List.contains_eq_mem, hxk, hxh, hk]]
  simpa [Bool.and_comm] using
    (List.filter_filter (p := fun x => !x == k) (q := fun x => !hot.contains x) (l := xs)).symm

private theorem filter_not_count_length (l : List Nat) (k : Nat) :
    (l.filter (fun y => !y == k)).length + l.count k = l.length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x = k
      · subst x
        simp [List.count_cons, ih]
        omega
      · have hbe : (x == k) = false := beq_eq_false_iff_ne.mpr hx
        simp [List.count_cons, hbe]
        omega

private theorem eraseDups_sublist (l : List Nat) : List.Sublist l.eraseDups l := by
  match l with
  | [] => exact List.Sublist.refl []
  | x :: xs =>
      rw [List.eraseDups_cons]
      exact ((eraseDups_sublist (xs.filter (fun y => !y == x))).trans
        (List.filter_sublist (p := fun y => !y == x) (l := xs))).cons_cons x
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_of_le_of_lt (List.length_filter_le _ xs) (Nat.lt_succ_self _)

private theorem contained_len_le_fanout (a b : List Nat) :
    (a.filter (b.contains ·)).length ≤ (a.map b.count).sum := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      have ih' : (xs.filter (fun y => decide (y ∈ b))).length ≤ (xs.map b.count).sum := by
        simpa [List.contains_eq_mem] using ih
      by_cases hx : b.contains x
      · have hxmem : x ∈ b := by
          have : decide (x ∈ b) = true := by simpa [List.contains_eq_mem] using hx
          exact of_decide_eq_true this
        have hpos : 0 < b.count x := by
          exact Nat.pos_of_ne_zero (by
            intro hz
            exact (List.count_eq_zero.mp hz) hxmem)
        simp [hx, hxmem]
        omega
      · have hxnot : x ∉ b := by
          intro hm
          apply hx
          simpa [List.contains_eq_mem] using hm
        simp [hx, hxnot]
        omega

private theorem support_le_fanout (a b : List Nat) :
    (a.eraseDups.filter (b.contains ·)).length ≤ (a.map b.count).sum := by
  exact Nat.le_trans ((eraseDups_sublist a).filter (b.contains ·)).length_le
    (contained_len_le_fanout a b)

private theorem sum_map_add (xs : List Nat) (f g : Nat → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      ac_rfl

private theorem count_eq_sum_boole (xs : List Nat) (y : Nat) :
    xs.count y = (xs.map (fun a => if y == a then 1 else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.count_cons, List.map_cons, List.sum_cons]
      rw [ih]
      by_cases h : y == x
      · have hxy : x = y := (beq_iff_eq.mp h).symm
        simp [hxy]
        omega
      · have hxy : x ≠ y := by intro h'; apply h; simp [h']
        have hyx : y ≠ x := by intro h'; exact hxy h'.symm
        simp [hxy, hyx]

private theorem sum_map_zero (a : List Nat) : (a.map (fun _ => 0)).sum = 0 := by
  induction a with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem fanout_cons_right (a : List Nat) (y : Nat) (ys : List Nat) :
    (a.map (y :: ys).count).sum = (a.map ys.count).sum + a.count y := by
  induction a with
  | nil => simp [sum_map_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp only [List.count_cons]
      by_cases hxy : x = y
      · subst x
        simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have h1 : (y == x) = false := beq_eq_false_iff_ne.mpr (by intro h; exact hxy h.symm)
        have h2 : (x == y) = false := beq_eq_false_iff_ne.mpr hxy
        simp [h1, h2, Nat.add_comm, Nat.add_left_comm]

private theorem fanout_le_len_cap_right (a b : List Nat) :
    (a.map b.count).sum ≤ a.length * (b.map b.count).max?.getD 0 := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hx := count_le_cap b x
      rw [Nat.succ_mul]
      omega

private theorem fanout_le_len_cap_left (a b : List Nat) :
    (a.map b.count).sum ≤ b.length * (a.map a.count).max?.getD 0 := by
  induction b with
  | nil => simp [sum_map_zero]
  | cons y ys ih =>
      rw [fanout_cons_right a y ys]
      simp only [List.length_cons]
      have hy := count_le_cap a y
      rw [Nat.succ_mul]
      omega

private theorem fanout_le_residualBound (a b : List Nat) :
    (a.map b.count).sum ≤
      min (a.length * (b.map b.count).max?.getD 0)
          (b.length * (a.map a.count).max?.getD 0) := by
  exact (Nat.le_min).2 ⟨fanout_le_len_cap_right a b, fanout_le_len_cap_left a b⟩

private theorem sum_map_count_cons_eq_of_ne (l : List Nat) (xs : List Nat) (x : Nat) (f : Nat → Nat)
    (h : ∀ k ∈ l, k ≠ x) :
    (l.map (fun k => (x :: xs).count k * f k)).sum =
      (l.map (fun k => xs.count k * f k)).sum := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      have hyne : y ≠ x := h y (by simp)
      have hys : ∀ k ∈ ys, k ≠ x := fun k hk => h k (by simp [hk])
      simp only [List.map_cons, List.sum_cons]
      rw [ih hys]
      rw [List.count_cons]
      have hbe : (x == y) = false := beq_eq_false_iff_ne.mpr (by intro hh; exact hyne hh.symm)
      simp [hbe, hyne]

private theorem group_by (l : List Nat) (f : Nat → Nat) :
    (l.map f).sum = (l.eraseDups.map (fun k => l.count k * f k)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [show ((xs.filter (fun y => !y == x)).eraseDups.map
            (fun k => (x :: xs).count k * f k)).sum =
            ((xs.filter (fun y => !y == x)).eraseDups.map
            (fun k => xs.count k * f k)).sum by
          apply sum_map_count_cons_eq_of_ne
          intro k hk
          have hkf : k ∈ xs.filter (fun y => !y == x) := List.mem_eraseDups.mp hk
          have hb : (!k == x) = true := (List.mem_filter.mp hkf).2
          intro hkx
          subst k
          simp at hb]
      rw [eraseDups_filter_erase_sum xs x (fun k => xs.count k * f k)]
      have hpoint := nodup_add_point xs.eraseDups (eraseDups_nodup xs) x (fun k => xs.count k * f k)
      rw [hpoint]
      rw [List.count_cons]
      by_cases hmem : x ∈ xs.eraseDups
      · have hxmem : x ∈ xs := List.mem_eraseDups.mp hmem
        have hcnt : 0 < xs.count x := by
          exact Nat.pos_of_ne_zero (by
            intro hz
            exact (List.count_eq_zero.mp hz) hxmem)
        simp [hmem]
        rw [Nat.add_mul]
        simp [Nat.one_mul]
        omega
      · have hxnot : x ∉ xs := by simpa using hmem
        have hcnt : xs.count x = 0 := List.count_eq_zero.mpr hxnot
        simp [hmem, hcnt]


theorem floor : CertifiedFloor.Goal := by
  intro hot a b
  have h := split_heavy hot a b
  have hs := support_le_fanout (tailKeys hot a) (tailKeys hot b)
  simp [fanout, matched, tailKeys, keySupport] at h hs ⊢
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_CertifiedFloor
