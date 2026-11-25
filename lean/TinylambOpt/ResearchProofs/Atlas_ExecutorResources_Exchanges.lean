import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

namespace TinylambOpt.ResearchProofs.Atlas.ExecutorResources.Exchanges

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons a l =>
          rw [List.eraseDups_cons]
          refine List.nodup_cons.mpr ⟨?_, ?_⟩
          · simp
          · apply ih ((l.filter (fun b => !b == a)).length)
            · have hle := List.length_filter_le (l := l) (p := fun b => !b == a)
              have hn : n = l.length + 1 := by simpa using h.symm
              omega
            · rfl

private theorem sum_map_eq_zero_of_all_zero {α : Type} (l : List α) (f : α → Nat)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = 0 := by intro y hy; exact h y (by simp [hy])
      simp only [List.map_cons, List.sum_cons]
      rw [hx]
      simpa using ih ht

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_le_sum_map {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      exact Nat.add_le_add hx (ih ht)

private theorem sum_map_mul_left {α : Type} (l : List α) (f : α → Nat) (n : Nat) :
    (l.map (fun x => n * f x)).sum = n * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [Nat.mul_add]

private theorem sum_map_ite_countP (l : List Nat) (p : Nat → Bool) :
    (l.map (fun d => if p d then 1 else 0)).sum = l.countP p := by
  induction l with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      by_cases h : p d = true
      · simp [h, ih]
        omega
      · simp [h, ih]

private theorem countP_eq_le_one {ds : List Nat} (hnd : ds.Nodup) (x : Nat) :
    ds.countP (fun d => d == x) ≤ 1 := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      rw [List.countP_cons]
      by_cases hd : d == x
      · have hne : ∀ y ∈ ds, y ≠ x := by
          intro y hy hyx
          have hdx : d = x := by simpa using hd
          have : d = y := by omega
          exact (List.nodup_cons.mp hnd).1 (by simpa [this] using hy)
        have hzero : ds.countP (fun y => y == x) = 0 := by
          rw [List.countP_eq_zero]
          intro y hy htrue
          have hyx : y = x := by simpa using htrue
          exact hne y hy hyx
        simp [hd, hzero]
      · have hnd' := (List.nodup_cons.mp hnd).2
        have hi := ih hnd'
        simp [hd]
        omega

private theorem nodup_decomp {l : List Nat} (hnd : l.Nodup) {a : Nat} (ha : a ∈ l) :
    ∃ l', List.Perm l (a :: l') ∧ a ∉ l' ∧ l'.Nodup := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        exact ⟨bs, List.Perm.refl _, (List.nodup_cons.mp hnd).1, (List.nodup_cons.mp hnd).2⟩
      · have habs : a ∈ bs := by
          rcases List.mem_cons.mp ha with h | h
          · exact False.elim (hba h.symm)
          · exact h
        rcases ih (List.nodup_cons.mp hnd).2 habs with ⟨l', hp, hnot, hnd'⟩
        refine ⟨b :: l', ?_, ?_, ?_⟩
        · exact (List.Perm.cons b hp).trans (List.Perm.swap a b l')
        · have hba' : a ≠ b := by intro h; exact hba h.symm
          simp [hba', hnot]
        · refine List.nodup_cons.mpr ⟨?_, hnd'⟩
          intro hbin
          exact (List.nodup_cons.mp hnd).1 ((hp.mem_iff).mpr (by simp [hbin]))

private theorem sum_group_by_key {α : Type} (key : α → Nat) (f : α → Nat)
    (l : List α) (rs : List Nat) (hrnd : rs.Nodup)
    (hcover : ∀ a ∈ l, key a ∈ rs) :
    (l.map f).sum =
      (rs.map (fun r => ((l.filter (fun a => key a == r)).map f).sum)).sum := by
  induction l with
  | nil =>
      symm
      apply sum_map_eq_zero_of_all_zero
      intro r hr
      simp
  | cons a l ih =>
      have hcover_l : ∀ x ∈ l, key x ∈ rs := by intro x hx; exact hcover x (by simp [hx])
      have ihres := ih hcover_l
      let rkey := key a
      rcases nodup_decomp hrnd (hcover a (by simp)) with ⟨rs', hperm, hnot, hnd'⟩
      have hperm1 :
          (rs.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)).sum =
          ((rkey :: rs').map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)).sum := by
        exact List.Perm.sum_nat (hperm.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum))
      have hperm2 :
          (rs.map (fun t => ((l.filter (fun x => key x == t)).map f).sum)).sum =
          ((rkey :: rs').map (fun t => ((l.filter (fun x => key x == t)).map f).sum)).sum := by
        exact List.Perm.sum_nat (hperm.map (fun t => ((l.filter (fun x => key x == t)).map f).sum))
      have hgroupr :
          (((a :: l).filter (fun x => key x == rkey)).map f).sum =
            f a + ((l.filter (fun x => key x == rkey)).map f).sum := by
        simp [rkey]
      have hgroups' :
          (rs'.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)) =
          (rs'.map (fun t => ((l.filter (fun x => key x == t)).map f).sum)) := by
        apply List.map_congr_left
        intro t ht
        have hne : rkey ≠ t := by
          intro heq
          exact hnot (by simpa [rkey, heq] using ht)
        have hbad : (key a == t) = false := by
          by_cases h : (key a == t) = true
          · have : rkey = t := by simpa [rkey] using h
            exact False.elim (hne this)
          · exact Bool.eq_false_of_ne_true h
        simp [hbad]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres, hperm1, hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [hgroupr, hgroups']
      omega

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        have ht : ∀ y ∈ xs, p y = false → f y = 0 := by
          intro y hy hpy; exact hz y (by simp [hy]) hpy
        rw [ih ht]
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem sum_eraseDups_eq_of_mem (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro d hd
        cases ((hmem d).mpr (by simpa using hd))
      subst l₂
      simp
  | cons a l₁ ih =>
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      rcases nodup_decomp hnd₂ ha2 with ⟨l₂', hp, hnot, hnd₂'⟩
      have hmem' : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
        intro d
        constructor
        · intro hd
          have hd2 : d ∈ l₂ := (hmem d).mp (by simp [hd])
          have hda : d ∈ a :: l₂' := hp.mem_iff.mp hd2
          rcases List.mem_cons.mp hda with h | h
          · subst h
            exact False.elim ((List.nodup_cons.mp hnd₁).1 hd)
          · exact h
        · intro hd
          have hd2 : d ∈ a :: l₂' := by simp [hd]
          have hd1 : d ∈ a :: l₁ := (hmem d).mpr (hp.mem_iff.mpr hd2)
          rcases List.mem_cons.mp hd1 with h | h
          · subst h
            exact False.elim (hnot hd)
          · exact h
      have ihres := ih l₂' (List.nodup_cons.mp hnd₁).2 hnd₂' hmem'
      have hperm2 : (l₂.map f).sum = ((a :: l₂').map f).sum :=
        List.Perm.sum_nat (hp.map f)
      simp only [List.map_cons, List.sum_cons]
      rw [hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres]

private theorem sum_eraseDups_eq_of_mem_zero (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hsub : ∀ d ∈ l₁, d ∈ l₂) (hzero : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let l₂' := l₂.filter (fun d => d ∈ l₁)
  have hnd' : l₂'.Nodup := by
    dsimp [l₂']
    exact hnd₂.filter _
  have hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
    intro d
    constructor
    · intro hd
      exact List.mem_filter.mpr ⟨hsub d hd, by simp [hd]⟩
    · intro hd
      exact of_decide_eq_true (List.mem_filter.mp hd).2
  have heq1 : (l₁.map f).sum = (l₂'.map f).sum := sum_eraseDups_eq_of_mem f l₁ l₂' hnd₁ hnd' hmem
  have heq2 : (l₂.map f).sum = (l₂'.map f).sum := by
    dsimp [l₂']
    apply sum_map_filter_eq
    intro d hd hpd
    exact hzero d hd (of_decide_eq_false hpd)
  omega

private theorem sum_eraseDups_append_left (f : Nat → Nat) (la lb : List Nat)
    (hz : ∀ d ∈ lb, d ∉ la → f d = 0) :
    (la.eraseDups.map f).sum = ((la ++ lb).eraseDups.map f).sum := by
  apply sum_eraseDups_eq_of_mem_zero f la.eraseDups (la ++ lb).eraseDups
      (nodup_eraseDups la) (nodup_eraseDups (la ++ lb))
  · intro d hd
    exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_eraseDups.mp hd))
  · intro d hd hnot
    apply hz d
    · have hdab := List.mem_eraseDups.mp hd
      rcases List.mem_append.mp hdab with h | h
      · exact False.elim (hnot (List.mem_eraseDups.mpr h))
      · exact h
    · intro hdla
      exact hnot (List.mem_eraseDups.mpr hdla)

private theorem sum_eraseDups_append_right (f : Nat → Nat) (la lb : List Nat)
    (hz : ∀ d ∈ la, d ∉ lb → f d = 0) :
    (lb.eraseDups.map f).sum = ((la ++ lb).eraseDups.map f).sum := by
  apply sum_eraseDups_eq_of_mem_zero f lb.eraseDups (la ++ lb).eraseDups
      (nodup_eraseDups lb) (nodup_eraseDups (la ++ lb))
  · intro d hd
    exact List.mem_eraseDups.mpr (List.mem_append_right _ (List.mem_eraseDups.mp hd))
  · intro d hd hnot
    apply hz d
    · have hdab := List.mem_eraseDups.mp hd
      rcases List.mem_append.mp hdab with h | h
      · exact h
      · exact False.elim (hnot (List.mem_eraseDups.mpr h))
    · intro hdmem
      exact hnot (List.mem_eraseDups.mpr hdmem)

private theorem sum_filter_map_eq_sum_ite {α : Type} (l : List α) (f : α → Nat) (p : α → Bool) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      by_cases h : p x = true <;> simp [h, ih]

private theorem packets_zero (cap : Nat) : packets cap 0 = 0 := by
  unfold packets
  cases cap <;> simp

private theorem le_packets_mul (cap n : Nat) (hcap : 0 < cap) :
    n ≤ packets cap n * cap := by
  unfold packets
  have hdecomp0 : cap * ((n + cap - 1) / cap) + (n + cap - 1) % cap = n + cap - 1 :=
    Nat.div_add_mod (n + cap - 1) cap
  have hdecomp : ((n + cap - 1) / cap) * cap + (n + cap - 1) % cap = n + cap - 1 := by
    simpa [Nat.mul_comm] using hdecomp0
  have hmod : (n + cap - 1) % cap < cap := Nat.mod_lt _ hcap
  omega

private theorem packets_le_of_le_mul (cap n k : Nat) (hcap : 0 < cap)
    (h : n ≤ k * cap) : packets cap n ≤ k := by
  unfold packets
  apply Nat.le_of_lt_succ
  apply (Nat.div_lt_iff_lt_mul hcap).mpr
  have hlt : n + cap - 1 < k * cap + cap := by omega
  have heq : (k + 1) * cap = k * cap + cap := by simp [Nat.add_mul]
  rwa [heq]

private theorem packets_add_le_one (cap x y : Nat) (hcap : 0 < cap) :
    packets cap x + packets cap y ≤ packets cap (x + y) + 1 := by
  unfold packets
  let a := x + cap - 1
  let b := y + cap - 1
  have hq : a / cap + b / cap ≤ (a + b) / cap := by
    rw [Nat.add_div hcap]
    omega
  have hle : a + b ≤ (x + y + cap - 1) + cap := by
    dsimp [a, b]
    omega
  have hd := Nat.div_le_div_right hle (c := cap)
  have hadd : ((x + y + cap - 1) + cap) / cap = (x + y + cap - 1) / cap + 1 := by
    rw [show (x + y + cap - 1) + cap = (x + y + cap - 1) + cap * 1 by simp]
    rw [Nat.add_mul_div_left (x + y + cap - 1) 1 hcap]
  dsimp [a, b] at hq hd
  omega

private theorem packets_add (cap x y : Nat) (hcap : 0 < cap) :
    packets cap (x + y) ≤ packets cap x + packets cap y := by
  have hx := le_packets_mul cap x hcap
  have hy := le_packets_mul cap y hcap
  have hxy : x + y ≤ (packets cap x + packets cap y) * cap := by
    rw [Nat.add_mul]
    omega
  exact packets_le_of_le_mul cap (x + y) (packets cap x + packets cap y) hcap hxy

private theorem packets_sum_le (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    packets cap xs.sum ≤ (xs.map (packets cap)).sum := by
  induction xs with
  | nil => simp [packets_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (packets_add cap x xs.sum hcap)
        (Nat.add_le_add_left ih (packets cap x))

private theorem volume_append (a b : List Bucket) (d : Nat) :
    volume (a ++ b) d = volume a d + volume b d := by
  unfold volume
  rw [List.filter_append, List.map_append, List.sum_append]

private theorem volume_not_mem (xs : List Bucket) (d : Nat)
    (hd : d ∉ xs.map Prod.fst) : volume xs d = 0 := by
  unfold volume
  have hf : xs.filter (fun p => p.1 == d) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro p hp
    have hm := List.mem_filter.mp hp
    have hpd : p.1 = d := by simpa using hm.2
    exact hd (List.mem_map.mpr ⟨p, hm.1, hpd⟩)
  rw [hf]
  simp

private theorem sum_le_length_mul {α : Type} (l : List α) (f : α → Nat) (cap : Nat)
    (h : ∀ x ∈ l, f x ≤ cap) : (l.map f).sum ≤ l.length * cap := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ cap := by intro y hy; exact h y (by simp [hy])
      have hi := ih ht
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.succ_mul]
      omega

private theorem volume_le_count_mul (xs : List Bucket) (d cap : Nat)
    (h : ∀ p ∈ xs, p.2 ≤ cap) :
    volume xs d ≤ (xs.filter (fun p => p.1 == d)).length * cap := by
  unfold volume
  apply sum_le_length_mul
  intro p hp
  exact h p (List.mem_filter.mp hp).1

private theorem packets_volume_le_count (xs : List Bucket) (d cap : Nat) (hcap : 0 < cap)
    (h : ∀ p ∈ xs, p.2 ≤ cap) :
    packets cap (volume xs d) ≤ (xs.filter (fun p => p.1 == d)).length := by
  exact packets_le_of_le_mul cap (volume xs d) _ hcap (volume_le_count_mul xs d cap h)

private theorem sum_countDest_le_length (ds : List Nat) (hnd : ds.Nodup) (wire : List Bucket) :
    (ds.map (fun d => (wire.filter (fun p => p.1 == d)).length)).sum ≤ wire.length := by
  induction wire with
  | nil =>
      simpa using sum_map_eq_zero_of_all_zero ds (fun _ => 0) (by simp)
  | cons p wire ih =>
      simp only [List.filter_cons]
      have hstep :
          (ds.map (fun d => (if p.1 == d then p :: wire.filter (fun q => q.1 == d)
              else wire.filter (fun q => q.1 == d)).length)).sum =
            (ds.map (fun d => (wire.filter (fun q => q.1 == d)).length)).sum +
              ds.countP (fun d => p.1 == d) := by
        rw [← sum_map_ite_countP ds (fun d => p.1 == d)]
        rw [← sum_map_add]
        have hmap :
            (ds.map (fun d => (if p.1 == d then p :: wire.filter (fun q => q.1 == d)
                else wire.filter (fun q => q.1 == d)).length)) =
              ds.map (fun d => (wire.filter (fun q => q.1 == d)).length +
                if p.1 == d then 1 else 0) := by
          apply List.map_congr_left
          intro d hdmem
          by_cases h : p.1 == d <;> simp [h]
        rw [hmap]
      rw [hstep]
      have hc : ds.countP (fun d => p.1 == d) ≤ 1 := by
        have hc' := countP_eq_le_one hnd p.1
        have hfuneq : (fun d => p.1 == d) = (fun d => d == p.1) := by
          funext d
          exact Bool.beq_comm
        simpa [hfuneq] using hc'
      rw [List.length_cons]
      omega

private theorem packets_mono_cap (cap k n : Nat) (hcap : 0 < cap) (hck : cap ≤ k) :
    packets k n ≤ packets cap n := by
  have hkpos : 0 < k := Nat.lt_of_lt_of_le hcap hck
  have hn := le_packets_mul cap n hcap
  have hmul : packets cap n * cap ≤ packets cap n * k := Nat.mul_le_mul_left _ hck
  exact packets_le_of_le_mul k n (packets cap n) hkpos (Nat.le_trans hn hmul)

private theorem packets_cap_scale (cap n v : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    packets cap v ≤ n * packets (cap * n) v := by
  have hmpos : 0 < cap * n := Nat.mul_pos hcap hn
  have hv := le_packets_mul (cap * n) v hmpos
  have hre : packets (cap * n) v * (cap * n) = (n * packets (cap * n) v) * cap := by
    simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  have hle : v ≤ (n * packets (cap * n) v) * cap := by simpa [hre] using hv
  exact packets_le_of_le_mul cap v (n * packets (cap * n) v) hcap hle

private theorem volume_route (xs : List Bucket) (route : Nat → Nat) (r : Nat) :
    volume (xs.map (fun (d, w) => (route d, w))) r =
      ((xs.filter (fun p => route p.1 == r)).map Prod.snd).sum := by
  unfold volume
  rw [List.filter_map]
  simp only [Function.comp_apply]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨d, w⟩
  simp

theorem sound_PacketLowerBound : PacketLowerBound.Goal := by
  intro cap xs wire hcap hvol hbound
  let ds := (xs.map Prod.fst).eraseDups
  have hnd : ds.Nodup := nodup_eraseDups _
  have htraffic :
      traffic cap xs ≤ (ds.map (fun d => (wire.filter (fun p => p.1 == d)).length)).sum := by
    dsimp [traffic, ds]
    apply sum_map_le_sum_map
    intro d hd
    rw [← hvol d]
    exact packets_volume_le_count wire d cap hcap hbound
  exact Nat.le_trans htraffic (sum_countDest_le_length ds hnd wire)

theorem sound_FlushDebt : FlushDebt.Goal := by
  intro cap a b hcap
  let la := a.map Prod.fst
  let lb := b.map Prod.fst
  let U := (la ++ lb).eraseDups
  have htrA : traffic cap a = (U.map (fun d => packets cap (volume a d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    apply sum_eraseDups_append_left
    intro d hd hnot
    have hnota : d ∉ a.map Prod.fst := by simpa [la] using hnot
    rw [volume_not_mem a d hnota, packets_zero]
  have htrB : traffic cap b = (U.map (fun d => packets cap (volume b d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    apply sum_eraseDups_append_right
    intro d hd hnot
    have hnotb : d ∉ b.map Prod.fst := by simpa [lb] using hnot
    rw [volume_not_mem b d hnotb, packets_zero]
  have htrAB : traffic cap (a ++ b) = (U.map (fun d => packets cap (volume (a ++ b) d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    rw [List.map_append]
  rw [htrA, htrB, htrAB]
  apply Nat.sub_le_iff_le_add.mpr
  have hper : ∀ d ∈ U,
      packets cap (volume a d) + packets cap (volume b d) ≤
        packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0) := by
    intro d hd
    by_cases hdA : d ∈ la
    · by_cases hdT : d ∈ lb
      · simp [hdA, hdT, volume_append]
        exact packets_add_le_one cap (volume a d) (volume b d) hcap
      · have hnotb : d ∉ b.map Prod.fst := by simpa [lb] using hdT
        simp [hdA, hdT, hnotb, volume_append, volume_not_mem b d hnotb, packets_zero]
    · have hnota : d ∉ a.map Prod.fst := by simpa [la] using hdA
      simp [hdA, hnota, volume_append, volume_not_mem a d hnota, packets_zero]
  have hsum := sum_map_le_sum_map U
      (fun d => packets cap (volume a d) + packets cap (volume b d))
      (fun d => packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0)) hper
  have hleft :
      (U.map (fun d => packets cap (volume a d) + packets cap (volume b d))).sum =
        (U.map (fun d => packets cap (volume a d))).sum +
          (U.map (fun d => packets cap (volume b d))).sum := sum_map_add _ _ _
  have hright :
      (U.map (fun d => packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0))).sum =
        (U.map (fun d => packets cap (volume (a ++ b) d))).sum +
          (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum := sum_map_add _ _ _
  have hind :
      (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
        la.eraseDups.countP (fun d => lb.contains d) := by
    have h1 :
        (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
          (la.eraseDups.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum := by
      symm
      apply sum_eraseDups_append_left
      intro d hd hnot
      simp [hnot]
    have h2 :
        (la.eraseDups.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
          (la.eraseDups.map (fun d => if lb.contains d then 1 else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hdmem
      have hdla : d ∈ la := List.mem_eraseDups.mp hdmem
      simp [hdla]
    rw [h1, h2, sum_map_ite_countP]
  rw [hleft, hright, hind] at hsum
  dsimp [la, lb] at hsum ⊢
  omega

theorem sound_FrameScale : FrameScale.Goal := by
  intro cap xs n hcap hn
  let ds := (xs.map Prod.fst).eraseDups
  have hlecap : cap ≤ cap * n := by
    have h1n : 1 ≤ n := Nat.succ_le_of_lt hn
    simpa using Nat.mul_le_mul_left cap h1n
  constructor
  · dsimp [traffic, ds]
    apply sum_map_le_sum_map
    intro d hd
    exact packets_mono_cap cap (cap * n) (volume xs d) hcap hlecap
  · dsimp [traffic, ds]
    have hper : ∀ d ∈ ds,
        packets cap (volume xs d) ≤ n * packets (cap * n) (volume xs d) := by
      intro d hd
      exact packets_cap_scale cap n (volume xs d) hcap hn
    have hsum := sum_map_le_sum_map ds (fun d => packets cap (volume xs d))
        (fun d => n * packets (cap * n) (volume xs d)) hper
    rw [sum_map_mul_left] at hsum
    exact hsum

theorem sound_ReceiverCoalesce : ReceiverCoalesce.Goal := by
  intro cap xs route hcap
  let routed := xs.map (fun (d, w) => (route d, w))
  let dests := (xs.map Prod.fst).eraseDups
  let rdests := (routed.map Prod.fst).eraseDups
  have hndD : dests.Nodup := nodup_eraseDups _
  have hndR : rdests.Nodup := nodup_eraseDups _
  have hcoverD : ∀ p ∈ xs, p.1 ∈ dests := by
    intro p hp
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨p, hp, rfl⟩)
  have hcoverR : ∀ d ∈ dests, route d ∈ rdests := by
    intro d hd
    have hdmem := List.mem_eraseDups.mp hd
    rcases List.mem_map.mp hdmem with ⟨p, hp, hpd⟩
    apply List.mem_eraseDups.mpr
    rw [List.mem_map]
    refine ⟨(route d, p.2), ?_, rfl⟩
    have hpin : (route d, p.2) ∈ routed := by
      dsimp [routed]
      rw [List.mem_map]
      exact ⟨p, hp, by simp [hpd]⟩
    exact hpin
  have hvolroute : ∀ r, volume routed r =
      ((dests.filter (fun d => route d == r)).map (fun d => volume xs d)).sum := by
    intro r
    let lr := xs.filter (fun p => route p.1 == r)
    have hcoverLR : ∀ p ∈ lr, p.1 ∈ dests := by
      intro p hp
      exact hcoverD p (List.mem_filter.mp hp).1
    have hgrp := sum_group_by_key (fun p : Bucket => p.1) Prod.snd lr dests hndD hcoverLR
    have hL : (lr.map Prod.snd).sum = volume routed r := by
      dsimp [lr]
      rw [volume_route]
    have hinner : ∀ d ∈ dests,
        ((lr.filter (fun p => p.1 == d)).map Prod.snd).sum =
          if route d == r then volume xs d else 0 := by
      intro d hd
      by_cases hdr : (route d == r) = true
      · have hfilter : lr.filter (fun p => p.1 == d) = xs.filter (fun p => p.1 == d) := by
          dsimp [lr]
          rw [List.filter_filter]
          apply List.filter_congr
          intro p hp
          by_cases hpd : (p.1 == d) = true
          · have hpd' : p.1 = d := by simpa using hpd
            have hrt : (route p.1 == r) = true := by simpa [hpd'] using hdr
            simp [hrt, hpd]
          · have hpdb : (p.1 == d) = false := Bool.eq_false_of_ne_true hpd
            simp [hpdb]
        rw [hfilter]
        simp [hdr, volume]
      · have hfilter : lr.filter (fun p => p.1 == d) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro p hp
          have hp1 := List.mem_filter.mp (List.mem_filter.mp hp).1
          have hpd : p.1 = d := by simpa using (List.mem_filter.mp hp).2
          have hrt : (route p.1 == r) = true := by simpa using hp1.2
          have hdr' : (route d == r) = true := by simpa [hpd] using hrt
          exact hdr hdr'
        have hdrf : (route d == r) = false := Bool.eq_false_of_ne_true hdr
        rw [hfilter]
        simp [hdrf]
    have hstep :
        (dests.map (fun d => ((lr.filter (fun p => p.1 == d)).map Prod.snd).sum)).sum =
          (dests.map (fun d => if route d == r then volume xs d else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hd
      exact hinner d hd
    have hfiltered :
        (dests.map (fun d => if route d == r then volume xs d else 0)).sum =
          ((dests.filter (fun d => route d == r)).map (fun d => volume xs d)).sum := by
      exact (sum_filter_map_eq_sum_ite dests (fun d => volume xs d) (fun d => route d == r)).symm
    rw [← hL, hgrp, hstep, hfiltered]
  have hper : ∀ r ∈ rdests,
      packets cap (volume routed r) ≤
        ((dests.filter (fun d => route d == r)).map (fun d => packets cap (volume xs d))).sum := by
    intro r hr
    rw [hvolroute r]
    have hp := packets_sum_le cap hcap
        ((dests.filter (fun d => route d == r)).map (fun d => volume xs d))
    rw [List.map_map] at hp
    simpa [Function.comp_def] using hp
  have hsum := sum_map_le_sum_map rdests
      (fun r => packets cap (volume routed r))
      (fun r => ((dests.filter (fun d => route d == r)).map (fun d => packets cap (volume xs d))).sum) hper
  have hgrp := sum_group_by_key (fun d : Nat => route d) (fun d => packets cap (volume xs d))
      dests rdests hndR hcoverR
  rw [← hgrp] at hsum
  dsimp [traffic, dests, routed] at hsum ⊢
  exact hsum

end TinylambOpt.ResearchProofs.Atlas.ExecutorResources.Exchanges
