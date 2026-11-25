import TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

namespace TinylambOpt.ResearchProofs.Claude1125.HuffmanWrites
open TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

/-- dot product of weights with depths -/
def dot : List Nat → List Nat → Nat
  | a::as, d::ds => a*d + dot as ds
  | _, _ => 0

/-- leaf depths of a merge tree (left to right) -/
def dps : Runs → List Nat
  | .run _ => [0]
  | .merge a b => (dps a ++ dps b).map (· + 1)

theorem len_dps : ∀ t : Runs, (dps t).length = (leaves t).length
  | .run _ => rfl
  | .merge a b => by simp [dps, leaves, len_dps a, len_dps b]

theorem dps_pos : ∀ t : Runs, 0 < (dps t).length
  | .run _ => by simp [dps]
  | .merge a b => by have := dps_pos a; simp [dps]; omega

theorem dps_one : ∀ t : Runs, (dps t).length = 1 → dps t = [0]
  | .run _, _ => rfl
  | .merge a b, h => by
    have h1 := dps_pos a; have h2 := dps_pos b
    simp [dps] at h; omega

theorem dot_append : ∀ (l1 d1 l2 d2 : List Nat), l1.length = d1.length →
    dot (l1 ++ l2) (d1 ++ d2) = dot l1 d1 + dot l2 d2
  | [], d1, l2, d2, h => by
    have : d1 = [] := List.eq_nil_of_length_eq_zero (by simpa using h.symm)
    subst this; simp [dot]
  | a::l1, [], l2, d2, h => by simp at h
  | a::l1, d::d1, l2, d2, h => by
    simp only [List.cons_append, dot]
    rw [dot_append l1 d1 l2 d2 (by simpa using h)]; omega

theorem dot_succ : ∀ (l d : List Nat), l.length = d.length →
    dot l (d.map (· + 1)) = dot l d + l.sum
  | [], d, h => by
    have : d = [] := List.eq_nil_of_length_eq_zero (by simpa using h.symm)
    subst this; simp [dot]
  | a::l, [], h => by simp at h
  | a::l, x::d, h => by
    simp only [List.map_cons, dot, List.sum_cons]
    rw [dot_succ l d (by simpa using h), Nat.mul_add, Nat.mul_one]; omega

theorem writes_eq : ∀ t : Runs, writes t = dot (leaves t) (dps t)
  | .run n => by simp [writes, leaves, dps, dot]
  | .merge a b => by
    have ha := writes_eq a; have hb := writes_eq b
    have la := len_dps a
    simp only [writes, leaves, dps]
    rw [dot_succ _ _ (by simp [la, len_dps b, List.length_append]),
      dot_append _ _ _ _ la.symm, ← ha, ← hb]
    simp [List.sum_append]; omega

/-! sorting facts -/
theorem ms_sorted (l : List Nat) : (l.mergeSort).Pairwise (fun a b => a ≤ b) := by
  have := List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c h1 h2; simp at *; omega) (by intro a b; simp; omega) l
  exact this.imp (by intro a b h; simpa using h)

theorem ms_eq {l1 l2 : List Nat} (h : l1.Perm l2) : l1.mergeSort = l2.mergeSort :=
  List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
    (by intro a b _ _ h1 h2; omega) (ms_sorted l1) (ms_sorted l2)
    ((List.mergeSort_perm _ _).trans (h.trans (List.mergeSort_perm _ _).symm))

theorem huffman_perm (n : Nat) {l1 l2 : List Nat} (h : l1.Perm l2) : huffman n l1 = huffman n l2 := by
  cases n with
  | zero => rfl
  | succ n => simp only [huffman]; rw [ms_eq h]

theorem huffman_cons2 (m1 m2 : Nat) (r : List Nat) (h12 : m1 ≤ m2) (hr : ∀ x ∈ r, m2 ≤ x) :
    huffman (r.length + 2) (m1::m2::r) = m1 + m2 + huffman (r.length + 1) ((m1+m2)::r) := by
  have hs : (m1::m2::r).mergeSort = m1::m2::r.mergeSort := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
      (by intro a b _ _ h1 h2; omega) (ms_sorted _)
    · refine List.Pairwise.cons ?_ (List.Pairwise.cons ?_ (ms_sorted r))
      · intro x hx
        simp only [List.mem_cons, List.mem_mergeSort] at hx
        rcases hx with rfl | hx
        · exact h12
        · have := hr x hx; omega
      · intro x hx
        rw [List.mem_mergeSort] at hx
        exact hr x hx
    · exact (List.mergeSort_perm _ _).trans
        ((List.Perm.cons _ (List.Perm.cons _ (List.mergeSort_perm _ _).symm)))
  show huffman (r.length + 1 + 1) _ = _
  rw [huffman, hs]
  show m1 + m2 + huffman (r.length + 1) ((m1+m2)::r.mergeSort) = _
  have : huffman (r.length + 1) ((m1+m2)::r.mergeSort) = huffman (r.length + 1) ((m1+m2)::r) :=
    huffman_perm _ (List.Perm.cons _ (List.mergeSort_perm _ _))
  rw [this]

/-! zipping with a permuted depth list -/
theorem dot_perm_ds {ds ds2 : List Nat} (h : ds.Perm ds2) :
    ∀ l : List Nat, l.length = ds.length → ∃ l2, l2.Perm l ∧ dot l2 ds2 = dot l ds := by
  induction h with
  | nil => intro l hl; exact ⟨[], by
      have : l = [] := List.eq_nil_of_length_eq_zero (by simpa using hl)
      subst this; exact .refl _, by simp [dot]⟩
  | cons x h ih =>
    intro l hl
    cases l with
    | nil => simp at hl
    | cons a l' =>
      obtain ⟨l2, hp, he⟩ := ih l' (by simpa using hl)
      exact ⟨a::l2, hp.cons a, by simp only [dot]; omega⟩
  | swap x y s =>
    intro l hl
    match l, hl with
    | a::b::l', _ => exact ⟨b::a::l', List.Perm.swap _ _ _, by simp only [dot]; omega⟩
  | trans h1 h2 ih1 ih2 =>
    intro l hl
    obtain ⟨l2, hp, he⟩ := ih1 l hl
    obtain ⟨l3, hp3, he3⟩ := ih2 l2 (by rw [hp.length_eq, hl]; exact h1.length_eq)
    exact ⟨l3, hp3.trans hp, by rw [he3, he]⟩

theorem swap_ineq {a b d D : Nat} (hab : b ≤ a) (hdD : d ≤ D) : b*D + a*d ≤ a*D + b*d := by
  obtain ⟨c, rfl⟩ := Nat.exists_eq_add_of_le hab
  obtain ⟨e, rfl⟩ := Nat.exists_eq_add_of_le hdD
  simp only [Nat.add_mul, Nat.mul_add]
  omega

theorem front_min (D : Nat) : ∀ (l ds : List Nat) (a : Nat), l.length = ds.length →
    (∀ d ∈ ds, d ≤ D) →
    ∃ a' l', (a'::l').Perm (a::l) ∧ (∀ x ∈ l', a' ≤ x) ∧ a'*D + dot l' ds ≤ a*D + dot l ds
  | [], ds, a, hl, _ => by
    have : ds = [] := List.eq_nil_of_length_eq_zero (by simpa using hl.symm)
    subst this
    exact ⟨a, [], .refl _, by simp, by simp [dot]⟩
  | b::bs, [], a, hl, _ => by simp at hl
  | b::bs, d::ds1, a, hl, hD => by
    have hd : d ≤ D := hD d (by simp)
    obtain ⟨a1, l1, hp, hmin, hc⟩ := front_min D bs ds1 a (by simpa using hl)
      (fun x hx => hD x (by simp [hx]))
    by_cases hab : a1 ≤ b
    · refine ⟨a1, b::l1, ?_, ?_, ?_⟩
      · exact (List.Perm.swap a1 b l1).symm.trans (((hp.cons b)).trans (List.Perm.swap a b bs))
      · intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact hab
        · exact hmin x hx
      · simp only [dot]; omega
    · have hba : b ≤ a1 := by omega
      refine ⟨b, a1::l1, ?_, ?_, ?_⟩
      · exact ((hp.cons b)).trans (List.Perm.swap a b bs)
      · intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact hba
        · have := hmin x hx; omega
      · have := swap_ineq hba hd
        simp only [dot]; omega

/-! cherry lemma -/
theorem lift_left (a b a' : Runs) (k : Nat) (e : List Nat)
    (ha : (dps a).Perm ((k+1)::(k+1)::e)) (ha' : (dps a').Perm (k::e))
    (he : ∀ d ∈ e, d ≤ k+1) (hb : ∀ d ∈ dps b, d ≤ k+1) :
    ∃ k' e' t', (dps (Runs.merge a b)).Perm ((k'+1)::(k'+1)::e') ∧
      (dps t').Perm (k'::e') ∧ ∀ d ∈ e', d ≤ k'+1 := by
  refine ⟨k+1, (e ++ dps b).map (· + 1), Runs.merge a' b, ?_, ?_, ?_⟩
  · have := (ha.append_right (dps b)).map (· + 1)
    simpa [dps] using this
  · have := (ha'.append_right (dps b)).map (· + 1)
    simpa [dps] using this
  · intro d hd
    simp only [List.mem_map, List.mem_append] at hd
    obtain ⟨x, hx | hx, rfl⟩ := hd
    · have := he x hx; omega
    · have := hb x hx; omega

theorem lift_right (a b b' : Runs) (k : Nat) (e : List Nat)
    (hb : (dps b).Perm ((k+1)::(k+1)::e)) (hb' : (dps b').Perm (k::e))
    (he : ∀ d ∈ e, d ≤ k+1) (ha : ∀ d ∈ dps a, d ≤ k+1) :
    ∃ k' e' t', (dps (Runs.merge a b)).Perm ((k'+1)::(k'+1)::e') ∧
      (dps t').Perm (k'::e') ∧ ∀ d ∈ e', d ≤ k'+1 := by
  obtain ⟨k', e', t', h1, h2, h3⟩ := lift_left b a b' k e hb hb' he ha
  refine ⟨k', e', t', ?_, h2, h3⟩
  have : (dps (Runs.merge a b)).Perm (dps (Runs.merge b a)) := by
    simp only [dps]
    exact (List.perm_append_comm).map _
  exact this.trans h1

theorem bound_of {t : Runs} {k : Nat} {e : List Nat}
    (h : (dps t).Perm ((k+1)::(k+1)::e)) (he : ∀ d ∈ e, d ≤ k+1) : ∀ d ∈ dps t, d ≤ k+1 := by
  intro d hd
  have := h.subset hd
  simp only [List.mem_cons] at this
  rcases this with rfl | rfl | hd
  · exact Nat.le_refl _
  · exact Nat.le_refl _
  · exact he d hd

theorem cherry : ∀ t : Runs, 2 ≤ (dps t).length →
    ∃ k e t', (dps t).Perm ((k+1)::(k+1)::e) ∧ (dps t').Perm (k::e) ∧ ∀ d ∈ e, d ≤ k+1
  | .run _, h => by simp [dps] at h
  | .merge a b, _ => by
    by_cases ha : 2 ≤ (dps a).length
    · obtain ⟨ka, ea, a', h1, h2, h3⟩ := cherry a ha
      by_cases hb : 2 ≤ (dps b).length
      · obtain ⟨kb, eb, b', g1, g2, g3⟩ := cherry b hb
        by_cases hk : kb ≤ ka
        · refine lift_left a b a' ka ea h1 h2 h3 ?_
          intro d hd
          have := bound_of g1 g3 d hd; omega
        · refine lift_right a b b' kb eb g1 g2 g3 ?_
          intro d hd
          have := bound_of h1 h3 d hd; omega
      · have hb1 : dps b = [0] := dps_one b (by have := dps_pos b; omega)
        refine lift_left a b a' ka ea h1 h2 h3 ?_
        intro d hd; rw [hb1] at hd; simp at hd; omega
    · have ha1 : dps a = [0] := dps_one a (by have := dps_pos a; omega)
      by_cases hb : 2 ≤ (dps b).length
      · obtain ⟨kb, eb, b', g1, g2, g3⟩ := cherry b hb
        refine lift_right a b b' kb eb g1 g2 g3 ?_
        intro d hd; rw [ha1] at hd; simp at hd; omega
      · have hb1 : dps b = [0] := dps_one b (by have := dps_pos b; omega)
        refine ⟨0, [], Runs.run 0, ?_, ?_, by simp⟩
        · simp [dps, ha1, hb1]
        · simp [dps]

/-- main inequality -/
theorem main : ∀ n : Nat, ∀ (t : Runs) (ds l : List Nat), ds.Perm (dps t) → ds.length = n →
    l.length = n → huffman n l ≤ dot l ds := by
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
  intro t ds l hds hdl hl
  by_cases hn : n ≤ 1
  · have : huffman n l = 0 := by
      rcases (by omega : n = 0 ∨ n = 1) with rfl | rfl
      · rfl
      · cases l with
        | nil => simp at hl
        | cons w l0 =>
          cases l0 with
          | nil => simp [huffman, List.mergeSort]
          | cons _ _ => simp at hl
    omega
  · obtain ⟨m, rfl⟩ := Nat.exists_eq_add_of_le (by omega : 2 ≤ n)
    obtain ⟨k, e, t', hp, hp', he⟩ := cherry t (by have := hds.length_eq; omega)
    have hds2 : ds.Perm ((k+1)::(k+1)::e) := hds.trans hp
    obtain ⟨l2, hl2p, hl2e⟩ := dot_perm_ds hds2 l (by rw [hl, hdl])
    have hlen2 : l2.length = 2 + m := by rw [hl2p.length_eq, hl]
    have helen : e.length = m := by have := hds2.length_eq; simp at this; omega
    cases l2 with
    | nil => exact absurd hlen2 (by simp <;> omega)
    | cons a l2' =>
      have hl2' : l2'.length = ((k+1)::e).length := by
        simp only [List.length_cons] at hlen2 ⊢; omega
      obtain ⟨a', l', hpa, hmina, hca⟩ := front_min (k+1) l2' ((k+1)::e) a hl2'
        (by
          intro d hd
          simp only [List.mem_cons] at hd
          rcases hd with rfl | hd
          · exact Nat.le_refl _
          · exact he d hd)
      have hl'len : l'.length = m + 1 := by
        have := hpa.length_eq; simp at this hlen2; omega
      cases l' with
      | nil => simp at hl'len
      | cons b r' =>
        have hr' : r'.length = e.length := by simp at hl'len; omega
        obtain ⟨b', r'', hpb, hminb, hcb⟩ := front_min (k+1) r' e b hr' he
        have hr''len : r''.length = m := by
          have := hpb.length_eq; simp at this; omega
        have hperm : (a'::b'::r'').Perm l := ((hpb.cons a').trans hpa).trans hl2p
        have hab : a' ≤ b' := hmina b' (hpb.subset (List.mem_cons_self))
        have hr : ∀ x ∈ r'', b' ≤ x := hminb
        have hr2 : ∀ x ∈ r'', a' ≤ x := fun x hx => hmina x (hpb.subset (List.mem_cons_of_mem _ hx))
        have key := huffman_cons2 a' b' r'' hab hr
        rw [hr''len] at key
        have IH := ih (m+1) (by omega) t' (k::e) ((a'+b')::r'') hp'.symm
          (by simp [helen]) (by simp [hr''len])
        have hcost : a'*(k+1) + b'*(k+1) + dot r'' e ≤ dot (a::l2') ((k+1)::(k+1)::e) := by
          simp only [dot] at hca hcb ⊢
          omega
        have hIH2 : dot ((a'+b')::r'') (k::e) = (a'+b')*k + dot r'' e := by simp [dot]
        rw [Nat.add_comm 2 m]
        calc huffman (m+2) l = huffman (m+2) (a'::b'::r'') := huffman_perm _ hperm.symm
          _ = a' + b' + huffman (m+1) ((a'+b')::r'') := by
              have := key; simpa [Nat.add_comm] using this
          _ ≤ a' + b' + ((a'+b')*k + dot r'' e) := by rw [← hIH2]; omega
          _ ≤ a'*(k+1) + b'*(k+1) + dot r'' e := by
              simp only [Nat.add_mul, Nat.mul_add, Nat.mul_one]; omega
          _ ≤ dot (a::l2') ((k+1)::(k+1)::e) := hcost
          _ = dot l ds := hl2e

theorem sound : MinimumWrites.Goal := by
  intro t
  rw [writes_eq t]
  exact main _ t (dps t) (leaves t) (List.Perm.refl _) (len_dps t) rfl


end TinylambOpt.ResearchProofs.Claude1125.HuffmanWrites
