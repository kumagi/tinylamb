import TinylambOpt.ResearchGoals.DuckDBGaps.Hits

namespace TinylambOpt.ResearchProofs.DuckDBGaps.Hits

open TinylambOpt.ResearchGoals.DuckDBGaps.Hits
open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.SearchBounds

/-! ## Generic list helpers -/

private theorem nodup_eraseDups_nat : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Nat), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length hlen _ rfl)
            have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
            rw [Nat.not_beq_eq_true_eq] at hb'
            exact fun heq => hb' heq.symm
  exact key l.length l rfl

private theorem nodup_length_eq_of_mem_iff' {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact (hca rfl).elim
          · exact hr
      have hrlen := ih hnd (l₂.erase a) hd hiff
      have hlen2 := List.length_erase_of_mem ha
      have hlpos : 0 < l₂.length := List.length_pos_of_mem ha
      have hlen2' : (l₂.erase a).length + 1 = l₂.length := by omega
      simp only [List.length_cons]
      omega

private theorem eraseDups_cons_length_nat (ys : List Nat) (x : Nat) :
    (x :: ys).eraseDups.length = ys.eraseDups.length + (if x ∈ ys then 0 else 1) := by
  by_cases hxy : x ∈ ys
  · have hlen := nodup_length_eq_of_mem_iff' (nodup_eraseDups_nat (x :: ys)) ys.eraseDups
      (nodup_eraseDups_nat ys) (fun z => by
        constructor
        · intro hz
          rcases List.mem_cons.mp ((List.mem_eraseDups).mp hz) with rfl | hzys
          · exact (List.mem_eraseDups).mpr hxy
          · exact (List.mem_eraseDups).mpr hzys
        · intro hz
          apply (List.mem_eraseDups).mpr
          exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hz)))
    rw [hlen, if_pos hxy, Nat.add_zero]
  · have hnody : (x :: ys.eraseDups).Nodup := by
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups_nat ys⟩
      intro hm
      exact hxy ((List.mem_eraseDups).mp hm)
    have hlen := nodup_length_eq_of_mem_iff' (nodup_eraseDups_nat (x :: ys)) (x :: ys.eraseDups) hnody
      (fun z => by
        constructor
        · intro hz
          have hz' : z = x ∨ z ∈ ys := List.mem_cons.mp ((List.mem_eraseDups).mp hz)
          rcases hz' with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mpr hzys))
        · intro hz
          apply (List.mem_eraseDups).mpr
          rcases List.mem_cons.mp hz with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hzys)))
    rw [hlen, List.length_cons, if_neg hxy]

private theorem sum_range_update_one (workers j : Nat) (hj : j < workers) (g : Nat → Nat) :
    ((List.range workers).map (fun i => g i + (if i == j then 1 else 0))).sum =
      ((List.range workers).map g).sum + 1 := by
  induction workers generalizing j with
  | zero => omega
  | succ w ih =>
      simp only [List.range_succ, List.map_append, List.sum_append,
        List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases hjw : j == w
      · have hjeq : j = w := beq_iff_eq.mp hjw
        subst j
        have hprev : ((List.range w).map (fun i => g i + (if i == w then 1 else 0))).sum =
            ((List.range w).map g).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro i hi
          have hiw : i ≠ w := by
            have hi' := List.mem_range.mp hi
            omega
          have hiw' : (i == w) = false := beq_eq_false_iff_ne.mpr hiw
          simp [hiw']
        rw [hprev]
        simp
        omega
      · have hjne : j ≠ w := by
          intro heq
          apply hjw
          subst j
          simp
        have hjlt : j < w := by omega
        have hlast : (if w == j then 1 else 0) = 0 := by
          have hwj : (w == j) = false := beq_eq_false_iff_ne.mpr (fun e => hjne e.symm)
          simp [hwj]
        rw [ih j hjlt, hlast]
        omega

/-! ## OwnerDistinct -/

private theorem owner_partition_distinct (l : List Nat) (owner : Nat → Nat) (workers : Nat)
    (howner : ∀ x ∈ l, owner x < workers) :
    ((List.range workers).map (fun j => ((l.filter (fun x => owner x == j)).eraseDups).length)).sum =
      l.eraseDups.length := by
  induction l with
  | nil =>
      simp only [List.filter_nil, List.eraseDups_nil, List.length_nil]
      induction workers with
      | zero => simp
      | succ w ihw => simp [List.range_succ, ihw]
  | cons a as ih =>
      have htail : ∀ x ∈ as, owner x < workers := by
        intro x hx
        exact howner x (by simp [hx])
      have ih' := ih htail
      have haow : owner a < workers := howner a (by simp)
      by_cases haas : a ∈ as
      · have hsame : ∀ j,
            ((a :: as).filter (fun x => owner x == j)).eraseDups.length =
              (as.filter (fun x => owner x == j)).eraseDups.length := by
          intro j
          by_cases hja : owner a == j
          · have hp : (a :: as).filter (fun x => owner x == j) =
                a :: (as.filter (fun x => owner x == j)) := by simp [hja]
            rw [hp, eraseDups_cons_length_nat]
            have : a ∈ as.filter (fun x => owner x == j) := by simp [haas, hja]
            simp [this]
          · have hp : (a :: as).filter (fun x => owner x == j) =
                as.filter (fun x => owner x == j) := by simp [hja]
            rw [hp]
        have hleft : ((List.range workers).map (fun j =>
            (((a :: as).filter (fun x => owner x == j)).eraseDups).length)).sum =
            ((List.range workers).map (fun j =>
            ((as.filter (fun x => owner x == j)).eraseDups).length)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro j hj
          exact hsame j
        rw [hleft, ih']
        rw [eraseDups_cons_length_nat]
        simp [haas]
      · have hinc : ∀ j,
            ((a :: as).filter (fun x => owner x == j)).eraseDups.length =
              (as.filter (fun x => owner x == j)).eraseDups.length +
                (if owner a == j then 1 else 0) := by
          intro j
          by_cases hja : owner a == j
          · have hp : (a :: as).filter (fun x => owner x == j) =
                a :: (as.filter (fun x => owner x == j)) := by simp [hja]
            rw [hp, eraseDups_cons_length_nat]
            have hnot : ¬ a ∈ as.filter (fun x => owner x == j) := by
              intro hm
              exact haas ((List.mem_filter.mp hm).1)
            simp [hnot, hja]
          · have hp : (a :: as).filter (fun x => owner x == j) =
                as.filter (fun x => owner x == j) := by simp [hja]
            rw [hp]
            simp [hja]
        let g := fun j => ((as.filter (fun x => owner x == j)).eraseDups).length
        have hleft : ((List.range workers).map (fun j =>
            (((a :: as).filter (fun x => owner x == j)).eraseDups).length)).sum =
            ((List.range workers).map g).sum + 1 := by
          have hpoint : (List.range workers).map (fun j =>
            (((a :: as).filter (fun x => owner x == j)).eraseDups).length) =
            (List.range workers).map (fun j => g j + (if j == owner a then 1 else 0)) := by
            apply List.map_congr_left
            intro j hj
            rw [hinc j]
            have hbeq : (owner a == j) = (j == owner a) := by
              by_cases h : owner a == j
              · have heq : owner a = j := beq_iff_eq.mp h
                subst j
                simp
              · by_cases h' : j == owner a
                · have heq : j = owner a := beq_iff_eq.mp h'
                  subst j
                  simp at h
                · simp [h, h']
            rw [hbeq]
          rw [hpoint]
          exact sum_range_update_one workers (owner a) haow g
        rw [hleft, ih']
        rw [eraseDups_cons_length_nat]
        simp [haas]

theorem OwnerDistinct_sound :
    OwnerDistinct.Goal := by
  intro rows owner workers winner howner
  let ids := (rows.filter (·.1 == winner)).filterMap Prod.snd
  exact owner_partition_distinct ids owner workers (by
    intro x hx
    exact howner x)

/-! ## OrderedFetch -/

private theorem seekHits_spec (hit : Nat → Bool) : ∀ (ids : List Nat) (k : Nat),
    let r := seekHits hit k ids
    r.1 = (ids.filter hit).take k ∧ r.2 ≤ ids.length ∧
      (r.2 > 0 → (ids.take (r.2 - 1)).countP hit < k) ∧
      (r.2 < ids.length → r.1.length = k) := by
  intro ids
  induction ids with
  | nil =>
      intro k
      cases k <;> simp [seekHits]
  | cons x xs ih =>
      intro k
      cases k with
      | zero => simp [seekHits]
      | succ k =>
          by_cases hok : hit x = true
          · have hih := ih k
            simp [seekHits, hok] at hih ⊢
            rcases hih with ⟨hout, hnle, hthird, hfourth⟩
            constructor
            · simp [hout]
            · constructor
              · omega
              · constructor
                · cases hn : (seekHits hit k xs).2 with
                  | zero => simp
                  | succ m =>
                      have hlt : (xs.take m).countP hit < k := by
                        have hp : 0 < (seekHits hit k xs).2 := by simp [hn]
                        simpa [hn] using hthird hp
                      simp [hok, hlt]
                · intro hnlt
                  have hnlt' : (seekHits hit k xs).2 < xs.length := by omega
                  have hl := hfourth hnlt'
                  simp [hl]
          · have hokf : hit x = false := by by_cases h : hit x <;> simp_all
            have hih := ih (k+1)
            simp [seekHits, hokf] at hih ⊢
            rcases hih with ⟨hout, hnle, hthird, hfourth⟩
            constructor
            · simp [hout]
            · constructor
              · omega
              · constructor
                · cases hn : (seekHits hit (k+1) xs).2 with
                  | zero => simp
                  | succ m =>
                      have hlt : (xs.take m).countP hit < k+1 := by
                        have hp : 0 < (seekHits hit (k+1) xs).2 := by simp [hn]
                        simpa [hn] using hthird hp
                      simp [hokf, hlt]
                · intro hnlt
                  have hnlt' : (seekHits hit (k+1) xs).2 < xs.length := by omega
                  have hl := hfourth hnlt'
                  simp [hl]

theorem OrderedFetch_sound :
    OrderedFetch.Goal := by
  intro ids hit k payload
  have h := seekHits_spec hit ids k
  rcases h with ⟨hout, hnle, hthird, hfourth⟩
  constructor
  · rw [hout]
  · exact ⟨hnle, hthird, hfourth⟩

/-! ## StridedPoint -/

private theorem countP_beq_count (l : List Nat) (c : Nat) :
    l.countP (fun x => x == c) = l.count c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.countP_cons, ih, List.count_cons]

theorem StridedPoint_sound :
    StridedPoint.Goal := by
  intro codes base step target hstep
  let diff : Int := target - base
  by_cases hbad : diff < 0 ∨ diff % step ≠ 0
  · have hbB : (decide (diff < 0) || diff % step != 0) = true := by
      rw [Bool.or_eq_true]
      by_cases hlt : diff < 0
      · exact Or.inl (by simp [hlt])
      · have hne : diff % step ≠ 0 := by
          rcases hbad with hneg | hne'
          · exact False.elim (hlt hneg)
          · exact hne'
        exact Or.inr (by simpa using hne)
    dsimp
    rw [if_pos hbB]
    symm
    apply List.countP_eq_zero.mpr
    intro off hoff
    intro htrue
    have heq : base + step * (off : Int) = target := beq_iff_eq.mp htrue
    have hmul : step * (off : Int) = diff := by
      dsimp [diff]
      omega
    rcases hbad with hneg | hmod
    · have hnonneg : 0 ≤ step * (off : Int) :=
        Int.mul_nonneg (Int.le_of_lt hstep) (Int.ofNat_nonneg off)
      omega
    · have hdvd : step ∣ diff := ⟨(off : Int), hmul.symm⟩
      have hz : diff % step = 0 := (Int.dvd_iff_emod_eq_zero).mp hdvd
      exact hmod hz
  · have hdiff_nonneg : 0 ≤ diff := by
      by_cases hlt : diff < 0
      · exact False.elim (hbad (Or.inl hlt))
      · omega
    have hmod0 : diff % step = 0 := by
      by_cases hz : diff % step = 0
      · exact hz
      · exact False.elim (hbad (Or.inr hz))
    have hbB : ¬ (decide (diff < 0) || diff % step != 0) = true := by
      rw [Bool.or_eq_true]
      intro h
      rcases h with h1 | h2
      · have hlt : diff < 0 := by simpa using h1
        exact hbad (Or.inl hlt)
      · have hne : diff % step ≠ 0 := by simpa using h2
        exact hbad (Or.inr hne)
    let q : Int := diff / step
    have hstepne : step ≠ 0 := by omega
    have hqnonneg : 0 ≤ q := by
      dsimp [q]
      exact Int.ediv_nonneg hdiff_nonneg (Int.le_of_lt hstep)
    have hdvd : step ∣ diff := (Int.dvd_iff_emod_eq_zero).mpr hmod0
    have hmulq : step * q = diff := by
      dsimp [q]
      rw [Int.mul_comm]
      exact Int.ediv_mul_cancel hdvd
    have hp : ∀ off : Nat, (base + step * (off : Int) == target) = (off == q.toNat) := by
      intro off
      by_cases hq : off == q.toNat
      · have heqoff : off = q.toNat := beq_iff_eq.mp hq
        subst off
        have hcast : ((q.toNat : Nat) : Int) = q := Int.toNat_of_nonneg hqnonneg
        have hstepq : step * (q.toNat : Int) = diff := by
          simpa [hcast] using hmulq
        have hlhs_eq : base + step * (q.toNat : Int) = target := by
          rw [hstepq]
          dsimp [diff]
          omega
        exact (beq_iff_eq.mpr hlhs_eq).trans hq.symm
      · have hneoff : (off : Int) ≠ q := by
          intro hqeq
          have hcasteq : (off : Int) = (q.toNat : Int) := by
            rw [Int.toNat_of_nonneg hqnonneg]
            exact hqeq
          have hnat : off = q.toNat := by exact_mod_cast hcasteq
          exact hq (by simp [hnat])
        have hlhs_false : (base + step * (off : Int) == target) = false := by
          by_cases ht : (base + step * (off : Int) == target) = true
          · have heq : base + step * (off : Int) = target := beq_iff_eq.mp ht
            have hmul : step * (off : Int) = diff := by
              dsimp [diff]
              omega
            have hqeq : (off : Int) = q := Int.eq_ediv_of_mul_eq_right hstepne hmul
            exact False.elim (hneoff hqeq)
          · exact Bool.eq_false_of_ne_true ht
        rw [hlhs_false, Bool.eq_false_of_ne_true hq]
    change (if (decide (diff < 0) || diff % step != 0) = true then 0
      else codes.count (diff / step).toNat) =
      codes.countP (fun off : Nat => base + step * (off : Int) == target)
    rw [if_neg hbB]
    rw [List.countP_congr (by
      intro off hoff
      rw [hp off])]
    exact (countP_beq_count codes q.toNat).symm


/-! ## HashMap helpers (Misra--Gries / histogram sums) -/

private theorem perm_sum' {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp [Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private def csumL' (l : List (Nat × Nat)) : Nat := (l.map Prod.snd).sum
private def csum' (h : Std.HashMap Nat Nat) : Nat := csumL' h.toList

private def notKey' (k : Nat) (p : Nat × Nat) : Bool := decide (¬(k == p.1) = true)

private theorem csumL_erase' (l : List (Nat × Nat)) (p : Nat × Nat) (hp : p ∈ l) :
    csumL' (l.erase p) + p.2 = csumL' l := by
  induction l with
  | nil => simp at hp
  | cons q qs ih =>
      by_cases hqp : q == p
      · have heq : q = p := beq_iff_eq.mp hqp
        subst q
        simp [csumL']
        omega
      · have hpqs : p ∈ qs := by
          rcases List.mem_cons.mp hp with hpq | hpqs
          · exact False.elim (hqp (by simp [hpq]))
          · exact hpqs
        have hih := ih hpqs
        rw [List.erase_cons_tail hqp]
        dsimp [csumL'] at hih ⊢
        simp only [List.map_cons, List.sum_cons]
        omega

private theorem beq_false_symm {a b : Nat} (h : (a == b) = false) : (b == a) = false := by
  by_cases h' : b == a
  · have he : a = b := (beq_iff_eq.mp h').symm
    have ht : (a == b) = true := by simp [he]
    rw [h] at ht
    contradiction
  · exact Bool.eq_false_of_ne_true h'

private theorem filter_ne_key_eq_erase' (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (k n : Nat) (hp : (k, n) ∈ l) :
    l.filter (notKey' k) = l.erase (k, n) := by
  induction l with
  | nil => simp at hp
  | cons q qs ih =>
      by_cases hq : q == (k, n)
      · have heq : q = (k, n) := beq_iff_eq.mp hq
        subst q
        have hno : ∀ r ∈ qs, (r.1 == k) = false := by
          intro r hr
          have hn := (List.pairwise_cons.mp hnd).1 r hr
          exact beq_false_symm hn
        have hfself : qs.filter (notKey' k) = qs := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxk : (x.1 == k) = false := hno x hx
          have hkx : (k == x.1) = false := beq_false_symm hxk
          simp [notKey', hkx]
        simp [notKey', hfself]
      · have hpqs : (k, n) ∈ qs := by
          rcases List.mem_cons.mp hp with hpq | hpqs
          · exact False.elim (hq (by simp [hpq]))
          · exact hpqs
        have hndtail := (List.pairwise_cons.mp hnd).2
        have ih' := ih hndtail hpqs
        have hdis : (q.1 == k) = false := by
          simpa using (List.pairwise_cons.mp hnd).1 (k, n) hpqs
        have hnot : (k == q.1) = false := by
          by_cases hkq : k == q.1
          · have heq : q.1 = k := (beq_iff_eq.mp hkq).symm
            have ht : (q.1 == k) = true := by simp [heq]
            rw [hdis] at ht
            contradiction
          · exact Bool.eq_false_of_ne_true hkq
        have hqnotkey : notKey' k q = true := by simp [notKey', hnot]
        simp [hqnotkey, ih', List.erase_cons_tail hq]


private theorem entry_of_contains' (h : Std.HashMap Nat Nat) (k : Nat) (hk : h.contains k) :
    ∃ n, (k, n) ∈ h.toList ∧ h.getD k 0 = n := by
  have hsome : h[k]? = some (h.getD k 0) := Std.HashMap.getElem?_eq_some_getD_of_contains hk
  rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList] at hsome
  rcases hsome with ⟨k', hbeq, hmem⟩
  have heq : k' = k := (beq_iff_eq.mp hbeq).symm
  subst k'
  exact ⟨h.getD k 0, hmem, rfl⟩

private theorem csum_insert_new' (h : Std.HashMap Nat Nat) (k v : Nat) (hk : ¬ h.contains k) :
    csum' (h.insert k v) = csum' h + v := by
  have hf : h.toList.filter (notKey' k) = h.toList := by
    apply List.filter_eq_self.mpr
    intro p hp
    by_cases hkp : k == p.1
    · exfalso
      apply hk
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
      rw [Std.HashMap.contains_eq_isSome_getElem?]
      rw [Std.HashMap.getElem?_congr hkp]
      rw [hp]
      simp
    · have hkpn : (k == p.1) = false := Bool.eq_false_of_ne_true hkp
      simp [notKey', hkpn]
  have hpperm : (h.insert k v).toList.Perm ((k, v) :: h.toList.filter (notKey' k)) :=
    Std.HashMap.toList_insert_perm (m := h) (k := k) (v := v)
  rw [hf] at hpperm
  dsimp [csum', csumL']
  rw [perm_sum' (hpperm.map Prod.snd)]
  simp [Nat.add_comm]

private theorem csum_insert_succ' (h : Std.HashMap Nat Nat) (k : Nat) :
    csum' (h.insert k (h.getD k 0 + 1)) = csum' h + 1 := by
  by_cases hk : h.contains k
  · rcases entry_of_contains' h k hk with ⟨n, hnmem, hnget⟩
    have hnd := Std.HashMap.distinct_keys_toList (m := h)
    have hf := filter_ne_key_eq_erase' h.toList hnd k n hnmem
    have hp : (h.insert k (h.getD k 0 + 1)).toList.Perm
        ((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)) :=
      Std.HashMap.toList_insert_perm (m := h) (k := k) (v := h.getD k 0 + 1)
    rw [hf] at hp
    dsimp [csum', csumL']
    rw [perm_sum' (hp.map Prod.snd)]
    have her := csumL_erase' h.toList (k, n) hnmem
    dsimp [csumL'] at her ⊢
    simp only [List.map_cons, List.sum_cons]
    omega
  · have hget : h.getD k 0 = 0 := by
      have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
      exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
    rw [hget]
    simpa using csum_insert_new' h k 1 hk

private def goodSumL' (p : Nat → Bool) (l : List (Nat × Nat)) : Nat :=
  ((l.filter (fun q => p q.1)).map Prod.snd).sum

private def goodSum' (p : Nat → Bool) (h : Std.HashMap Nat Nat) : Nat := goodSumL' p h.toList

private theorem goodSumL_erase' (l : List (Nat × Nat)) (p : Nat → Bool)
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (k n : Nat) (hm : (k, n) ∈ l) (hpk : p k = true) :
    goodSumL' p (l.erase (k, n)) + n = goodSumL' p l := by
  induction l with
  | nil => simp at hm
  | cons q qs ih =>
      by_cases hqp : q == (k, n)
      · have heq : q = (k, n) := beq_iff_eq.mp hqp
        subst q
        have htail : ∀ r ∈ qs, (r.1 == k) = false := by
          intro r hr
          have h := (List.pairwise_cons.mp hnd).1 r hr
          exact beq_false_symm h
        have hself : ((k, n) :: qs).filter (fun q => p q.1) =
            (k, n) :: (qs.filter (fun q => p q.1)) := by simp [hpk]
        dsimp [goodSumL']
        rw [hself]
        simp [goodSumL', hpk]
        omega
      · have hpqs : (k, n) ∈ qs := by
          rcases List.mem_cons.mp hm with hmq | hmq
          · exact False.elim (hqp (by simp [hmq]))
          · exact hmq
        have hih := ih (List.pairwise_cons.mp hnd).2 hpqs
        have he := List.erase_cons_tail (l := qs) hqp
        dsimp [goodSumL'] at hih ⊢
        rw [he]
        simp only [List.filter_cons, List.map_cons, List.sum_cons]
        by_cases hq1 : p q.1 = true
        · simp [hq1]
          omega
        · simp [Bool.eq_false_of_ne_true hq1]
          exact hih

private theorem filter_notKey_eq_self_of_not_contains (h : Std.HashMap Nat Nat) (k : Nat)
    (hk : ¬ h.contains k) : h.toList.filter (notKey' k) = h.toList := by
  apply List.filter_eq_self.mpr
  intro q hq
  rcases q with ⟨q1, q2⟩
  have hget : h[q1]? = some q2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hq
  have hc : h.contains q1 := by
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    simp [hget]
  have hkq : (k == q1) = false := by
    by_cases h' : k == q1
    · have heq : k = q1 := beq_iff_eq.mp h'
      exact False.elim (hk (by rwa [← heq] at hc))
    · exact Bool.eq_false_of_ne_true h'
  simp [notKey', hkq]

private theorem goodSum_insert_succ' (p : Nat → Bool) (h : Std.HashMap Nat Nat) (k : Nat) :
    goodSum' p (h.insert k (h.getD k 0 + 1)) = goodSum' p h + (if p k then 1 else 0) := by
  by_cases hk : h.contains k
  · rcases entry_of_contains' h k hk with ⟨n, hnmem, hnget⟩
    have hnd := Std.HashMap.distinct_keys_toList (m := h)
    have hf := filter_ne_key_eq_erase' h.toList hnd k n hnmem
    have hp : (h.insert k (h.getD k 0 + 1)).toList.Perm
        ((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)) :=
      Std.HashMap.toList_insert_perm (m := h) (k := k) (v := h.getD k 0 + 1)
    by_cases hpk : p k
    · have hpkt : p k = true := hpk
      have hfilt : ((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)).filter (fun q => p q.1) =
          (k, h.getD k 0 + 1) :: (h.toList.erase (k, n)).filter (fun q => p q.1) := by
        rw [hf]
        simp [hpkt]
      dsimp [goodSum', goodSumL']
      rw [perm_sum' ((hp.filter (fun q => p q.1)).map Prod.snd)]
      rw [if_pos hpk]
      change List.sum ((((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)).filter (fun q => p q.1)).map Prod.snd) =
        List.sum ((h.toList.filter (fun q => p q.1)).map Prod.snd) + 1
      rw [hfilt]
      have her := goodSumL_erase' h.toList p hnd k n hnmem hpkt
      dsimp [goodSumL'] at her ⊢
      simp only [List.map_cons, List.sum_cons]
      omega
    · have hpkf : p k = false := Bool.eq_false_of_ne_true hpk
      have hnt : ∀ q, (p q.1 && notKey' k q) = p q.1 := by
        intro q
        by_cases hkq : k == q.1
        · have heq : q.1 = k := (beq_iff_eq.mp hkq).symm
          have hpq : p q.1 = false := by rw [heq, hpkf]
          simp [notKey', hkq, hpq]
        · simp [notKey', hkq]
      have hfilteq : (h.toList.filter (notKey' k)).filter (fun q => p q.1) =
          h.toList.filter (fun q => p q.1) := by
        rw [List.filter_filter]
        exact List.filter_congr (by intro q hq; exact hnt q)
      dsimp [goodSum', goodSumL']
      rw [perm_sum' ((hp.filter (fun q => p q.1)).map Prod.snd)]
      rw [if_neg hpk]
      change List.sum ((((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)).filter (fun q => p q.1)).map Prod.snd) =
        List.sum ((h.toList.filter (fun q => p q.1)).map Prod.snd) + 0
      have hcons : ((k, h.getD k 0 + 1) :: h.toList.filter (notKey' k)).filter (fun q => p q.1) =
          h.toList.filter (fun q => p q.1) := by
        simp [hpkf, hfilteq]
      rw [hcons]
      omega
  · have hget : h.getD k 0 = 0 := by
      have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
      exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
    rw [hget]
    have hf := filter_notKey_eq_self_of_not_contains h k hk
    have hpperm0 := Std.HashMap.toList_insert_perm (m := h) (k := k) (v := 1)
    change (h.insert k 1).toList.Perm ((k, 1) :: h.toList.filter (notKey' k)) at hpperm0
    rw [hf] at hpperm0
    by_cases hpk : p k
    · have hpperm : (h.insert k 1).toList.Perm ((k, 1) :: h.toList) := hpperm0
      dsimp [goodSum', goodSumL']
      rw [perm_sum' ((hpperm.filter (fun q => p q.1)).map Prod.snd)]
      simp [hpk]
      omega
    · have hpkf : p k = false := Bool.eq_false_of_ne_true hpk
      have hpperm : (h.insert k 1).toList.Perm ((k, 1) :: h.toList) := hpperm0
      dsimp [goodSum', goodSumL']
      rw [perm_sum' ((hpperm.filter (fun q => p q.1)).map Prod.snd)]
      simp [hpkf]

private theorem foldl_hist_good (xs : List Nat) (p : Nat → Bool) (h : Std.HashMap Nat Nat) :
    goodSum' p (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) h) =
      goodSum' p h + xs.countP p := by
  induction xs generalizing h with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, goodSum_insert_succ']
      simp [List.countP_cons]
      omega

private theorem histogram_good (xs : List Nat) (p : Nat → Bool) :
    goodSum' p (TinylambOpt.ResearchGoals.SearchBounds.histogram xs) = xs.countP p := by
  unfold TinylambOpt.ResearchGoals.SearchBounds.histogram
  rw [foldl_hist_good]
  simp [goodSum', goodSumL']



private theorem foldl_if_sum_good_aux (p : Nat → Bool) :
    ∀ (l : List (Nat × Nat)) (n : Nat),
      l.foldl (fun n (k, w) => if p k then n + w else n) n =
        n + ((l.filter (fun q => p q.1)).map Prod.snd).sum := by
  intro l
  induction l with
  | nil => intro n; rfl
  | cons q qs ih =>
      intro n
      simp only [List.foldl_cons]
      by_cases hpq : p q.1 = true
      · rw [ih]
        simp [hpq]
        omega
      · have hpqf : p q.1 = false := Bool.eq_false_of_ne_true hpq
        rw [ih]
        simp [hpqf]

private theorem foldl_if_sum_good (p : Nat → Bool) (l : List (Nat × Nat)) :
      l.foldl (fun n (k, w) => if p k then n + w else n) 0 =
        ((l.filter (fun q => p q.1)).map Prod.snd).sum := by
  simpa using foldl_if_sum_good_aux p l 0

private theorem countP_map_good {α β : Type} [BEq β] [LawfulBEq β]
    (xs : List α) (f : α → β) (p : β → Bool) :
    (xs.map f).countP p = xs.countP (fun x => p (f x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.countP_cons, ih]

private theorem countP_or_disjoint_good {α : Type} (l : List α) (p q : α → Bool)
    (hd : ∀ x, ¬ (p x = true ∧ q x = true)) :
    l.countP (fun x => p x || q x) = l.countP p + l.countP q := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons, ih]
      by_cases hpx : p x = true
      · have hqx : q x = false := by
          by_cases h : q x = true
          · exact False.elim (hd x ⟨hpx, h⟩)
          · exact Bool.eq_false_of_ne_true h
        simp [hpx, hqx]
        omega
      · have hpxf : p x = false := Bool.eq_false_of_ne_true hpx
        by_cases hqx : q x = true
        · simp [hpxf, hqx]
          omega
        · have hqxf : q x = false := Bool.eq_false_of_ne_true hqx
          simp [hpxf, hqxf]

/-! ## DatePostings -/

theorem DatePostings_sound :
    DatePostings.Goal := by
  intro rows width lo hi url hwidth
  let hits : List (Nat × Nat) := rows.filter (fun r => r.2 == url)
  let key : Nat × Nat → Nat := fun r => r.1 / width
  let whole : Nat → Bool := fun k => decide (lo ≤ k * width ∧ (k + 1) * width - 1 ≤ hi)
  let inRange : Nat × Nat → Bool := fun r => decide (lo ≤ r.1 ∧ r.1 ≤ hi)
  have hwhole_imp : ∀ r ∈ hits, whole (key r) = true → inRange r = true := by
    intro r hr hw
    have hwprop : lo ≤ key r * width ∧ (key r + 1) * width - 1 ≤ hi := by
      dsimp [whole] at hw
      simpa using hw
    have hle1 : key r * width ≤ r.1 := by
      dsimp [key]
      exact Nat.div_mul_le_self r.1 width
    have hlow : lo ≤ r.1 := Nat.le_trans hwprop.1 hle1
    have hlt : r.1 < (key r + 1) * width := by
      dsimp [key]
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        (Nat.lt_mul_div_succ r.1 hwidth)
    have hle2 : r.1 ≤ hi := by
      have hposprod : 0 < (key r + 1) * width := Nat.mul_pos (by omega) hwidth
      omega
    dsimp [inRange]
    simp [hlow, hle2]
  have hinterior :
      (TinylambOpt.ResearchGoals.SearchBounds.histogram (hits.map key)).toList.foldl
          (fun n (k, w) => if whole k then n + w else n) 0 =
        hits.countP (fun r => whole (key r)) := by
    rw [foldl_if_sum_good]
    change goodSum' whole (TinylambOpt.ResearchGoals.SearchBounds.histogram (hits.map key)) =
      hits.countP (fun r => whole (key r))
    rw [histogram_good]
    exact countP_map_good hits key whole
  have hpoint : ∀ r ∈ hits,
      inRange r = (whole (key r) || (!whole (key r) && inRange r)) := by
    intro r hr
    by_cases hw : whole (key r) = true
    · have hi := hwhole_imp r hr hw
      simp [hw, hi]
    · have hwf : whole (key r) = false := Bool.eq_false_of_ne_true hw
      simp [hwf]
  have hcount1 : hits.countP inRange =
      hits.countP (fun r => whole (key r) || (!whole (key r) && inRange r)) := by
    apply List.countP_congr
    intro r hr
    rw [← hpoint r hr]
  have hdisj : ∀ r, ¬ (whole (key r) = true ∧ (!whole (key r) && inRange r) = true) := by
    intro r h
    have hw := h.1
    have hb := h.2
    simp [hw] at hb
  have hcount2 := countP_or_disjoint_good hits (fun r => whole (key r))
    (fun r => !whole (key r) && inRange r) hdisj
  have hgoal_abstract :
      (TinylambOpt.ResearchGoals.SearchBounds.histogram (hits.map key)).toList.foldl
          (fun n (k, w) => if whole k then n + w else n) 0 +
        hits.countP (fun r => !whole (key r) && inRange r) = hits.countP inRange := by
    rw [hinterior]
    omega
  simpa [hits, key, whole, inRange, Bool.and_assoc,
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.histogram,
    TinylambOpt.ResearchGoals.SearchBounds.histogram] using hgoal_abstract


/-! ## OrderedCodeMin -/

private theorem min?_le_all_g {α : Type} [Min α] [LE α]
    [Std.Refl (fun a b : α => a ≤ b)] [Std.LawfulOrderInf α] [Std.Associative (min : α → α → α)]
    [Trans (fun a b : α => a ≤ b) (fun a b : α => a ≤ b) (fun a b : α => a ≤ b)] :
    ∀ (l : List α) (m : α), l.min? = some m → ∀ a ∈ l, m ≤ a := by
  intro l
  induction l with
  | nil =>
      intro m hm
      simp at hm
  | cons x xs ih =>
      intro m hm a ha
      rw [List.min?_cons] at hm
      cases hopt : xs.min? with
      | none =>
          have hxnil : xs = [] := List.min?_eq_none_iff.mp hopt
          subst xs
          simp at hm
          subst m
          simp at ha
          subst a
          exact Std.le_refl _
      | some y =>
          simp [hopt] at hm
          subst m
          rcases List.mem_cons.mp ha with rfl | ha
          · exact Std.min_le_left
          · have hy := ih y (by simp [hopt]) a ha
            exact Std.le_trans Std.min_le_right hy

private theorem min?_mem_g {α : Type} [Min α] [LE α]
    [Std.Refl (fun a b : α => a ≤ b)] [Std.LawfulOrderInf α] [Std.Associative (min : α → α → α)]
    [Trans (fun a b : α => a ≤ b) (fun a b : α => a ≤ b) (fun a b : α => a ≤ b)]
    [Std.Antisymm (fun a b : α => a ≤ b)] [Std.Total (fun a b : α => a ≤ b)] :
    ∀ (l : List α) (m : α), l.min? = some m → m ∈ l := by
  intro l
  induction l with
  | nil =>
      intro m hm
      simp at hm
  | cons x xs ih =>
      intro m hm
      rw [List.min?_cons] at hm
      cases hopt : xs.min? with
      | none =>
          have hxnil : xs = [] := List.min?_eq_none_iff.mp hopt
          subst xs
          simp at hm
          subst m
          simp
      | some y =>
          simp [hopt] at hm
          subst m
          have hmincases : min x y = x ∨ min x y = y := by
            rcases Std.le_total (a := x) (b := y) with hxy | hyx
            · left
              apply Std.le_antisymm
              · exact Std.min_le_left
              · exact (Std.le_min_iff).mpr ⟨Std.le_refl x, hxy⟩
            · right
              apply Std.le_antisymm
              · exact Std.min_le_right
              · exact (Std.le_min_iff).mpr ⟨hyx, Std.le_refl y⟩
          rcases hmincases with hmin | hmin
          · rw [hmin]
            simp
          · rw [hmin]
            simp [ih y (by simp [hopt])]

private theorem min?_eq_of_le_all_g {α : Type} [Min α] [LE α]
    [Std.Refl (fun a b : α => a ≤ b)] [Std.LawfulOrderInf α] [Std.Associative (min : α → α → α)]
    [Trans (fun a b : α => a ≤ b) (fun a b : α => a ≤ b) (fun a b : α => a ≤ b)]
    [Std.Antisymm (fun a b : α => a ≤ b)] [Std.Total (fun a b : α => a ≤ b)] :
    ∀ (l : List α) (m : α), m ∈ l → (∀ a ∈ l, m ≤ a) → l.min? = some m := by
  intro l
  induction l with
  | nil =>
      intro m hm
      simp at hm
  | cons x xs ih =>
      intro m hm hle
      rw [List.min?_cons]
      cases hopt : xs.min? with
      | none =>
          have hxnil : xs = [] := List.min?_eq_none_iff.mp hopt
          subst xs
          simp at hm
          subst m
          simp
      | some y =>
          have hymem : y ∈ xs := min?_mem_g xs y hopt
          rcases List.mem_cons.mp hm with hmx | htail
          · subst m
            have hxy : x ≤ y := hle y (by simp [hymem])
            have hmin : min x y = x := by
              apply Std.le_antisymm
              · exact Std.min_le_left
              · exact (Std.le_min_iff).mpr ⟨Std.le_refl x, hxy⟩
            simp [hopt, hmin]
          · have hmy : m ≤ y := hle y (by simp [hymem])
            have hym : y ≤ m := min?_le_all_g xs y hopt m htail
            have hyeq : y = m := Std.le_antisymm hym hmy
            subst y
            have hmx : m ≤ x := hle x (by simp)
            have hmin : min x m = m := by
              apply Std.le_antisymm
              · exact Std.min_le_right
              · exact (Std.le_min_iff).mpr ⟨hmx, Std.le_refl m⟩
            simpa [hopt, hmin]

private theorem flatten_nil_of_all_nil : ∀ bs : List (List String),
    (∀ b ∈ bs, b = []) → bs.flatten = [] := by
  intro bs h
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      have hb := h b (by simp)
      subst b
      simp [ih (by intro c hc; exact h c (by simp [hc]))]

private theorem min?_flatten_g (bs : List (List String)) :
    (bs.filterMap (fun b => b.min?)).min? = bs.flatten.min? := by
  by_cases hnone : (bs.filterMap (fun b => b.min?)).min? = none
  · have hlistnil : bs.filterMap (fun b => b.min?) = [] := List.min?_eq_none_iff.mp hnone
    have hempty : ∀ b ∈ bs, b = [] := by
      intro b hb
      have hbnone : b.min? = none := by
        by_cases h : b.min? = none
        · exact h
        · cases hopt : b.min? with
          | none => rfl
          | some mb =>
            have hmem : mb ∈ bs.filterMap (fun b => b.min?) := by
              apply List.mem_filterMap.mpr
              exact ⟨b, hb, hopt⟩
            rw [hlistnil] at hmem
            simp at hmem
      exact List.min?_eq_none_iff.mp hbnone
    have hflatnil : bs.flatten = [] := flatten_nil_of_all_nil bs hempty
    rw [hnone, hflatnil]
    simp
  · cases hopt : (bs.filterMap (fun b => b.min?)).min? with
    | none => exact False.elim (hnone hopt)
    | some m =>
        have hmmem : m ∈ bs.filterMap (fun b => b.min?) := min?_mem_g _ _ hopt
        rcases List.mem_filterMap.mp hmmem with ⟨b, hbmem, hbmin⟩
        have hmbmem : m ∈ b := min?_mem_g b m hbmin
        have hmle : ∀ a ∈ bs.flatten, m ≤ a := by
          intro a ha
          rcases List.mem_flatten.mp ha with ⟨c, hc, hac⟩
          have hcmin : c.min? = some (c.min?.getD "") := by
            cases hcm : c.min? with
            | none =>
                have hcnil : c = [] := List.min?_eq_none_iff.mp hcm
                subst c
                simp at hac
            | some mc => simp [hcm]
          -- use min? of bs.filterMap <= any member
          have hmemc : (c.min?.getD "") ∈ bs.filterMap (fun b => b.min?) := by
            apply List.mem_filterMap.mpr
            exact ⟨c, hc, hcmin⟩
          have hmc : m ≤ c.min?.getD "" := min?_le_all_g _ _ hopt _ hmemc
          have hca : c.min?.getD "" ≤ a := by
            cases hcm : c.min? with
            | none =>
                have hcnil : c = [] := List.min?_eq_none_iff.mp hcm
                subst c
                simp at hac
            | some mc =>
                simpa [hcm] using (min?_le_all_g c mc hcm a hac)
          exact Std.le_trans hmc hca
        have hflatmem : m ∈ bs.flatten := List.mem_flatten.mpr ⟨b, hbmem, hmbmem⟩
        have hf := min?_eq_of_le_all_g bs.flatten m hflatmem hmle
        exact hf.symm

private theorem localMin_eq_min_decode (d : Array String) (cs : List Nat)
    (hsorted : d.toList.Pairwise (fun a b => a ≤ b)) (hvalid : ∀ i ∈ cs, i < d.size) :
    localMin d cs = (cs.filterMap (fun i => d[i]?)).min? := by
  unfold localMin
  cases hmin : cs.min? with
  | none =>
      have hcnil : cs = [] := List.min?_eq_none_iff.mp hmin
      subst cs
      simp
  | some i0 =>
      have hi0mem : i0 ∈ cs := min?_mem_g cs i0 hmin
      have hi0le : ∀ i ∈ cs, i0 ≤ i := min?_le_all_g cs i0 hmin
      have hi0val : i0 < d.size := hvalid i0 hi0mem
      have hget0 : d[i0]? = some d[i0] := by simp [hi0val]
      simp [hmin, hget0]
      symm
      apply min?_eq_of_le_all_g
      · apply List.mem_filterMap.mpr
        exact ⟨i0, hi0mem, hget0⟩
      · intro a ha
        rcases List.mem_filterMap.mp ha with ⟨i, hics, hi⟩
        have hival : i < d.size := hvalid i hics
        have hgeti : d[i]? = some d[i] := by simp [hival]
        rw [hgeti] at hi
        injection hi with hia
        subst a
        by_cases hle : i0 = i
        · subst i
          exact Std.le_refl _
        · have hle' : i0 ≤ i := hi0le i hics
          have hlt : i0 < i := by omega
          have hltlen0 : i0 < d.toList.length := by simpa using hi0val
          have hltlen : i < d.toList.length := by simpa using hival
          have hrel : d.toList[i0] ≤ d.toList[i] :=
            List.Pairwise.rel_getElem_of_lt hltlen0 hltlen hsorted hlt
          have hget0list : d.toList[i0] = d[i0] :=
            Array.getElem_toList (xs := d) (i := i0) (by simpa using hi0val)
          have hgetilist : d.toList[i] = d[i] :=
            Array.getElem_toList (xs := d) (i := i) (by simpa using hival)
          rw [← hget0list, ← hgetilist]
          exact hrel

private theorem filterMap_map_eq {α β γ : Type} (f : α → β) (g : β → Option γ) :
    ∀ l : List α, l.filterMap (fun x => g (f x)) = (l.map f).filterMap g := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases h : g (f x) <;> simp [h, ih]

private theorem pages_localMin_eq (pages : List (Array String × List Nat))
    (hp : ∀ p ∈ pages, p.1.toList.Pairwise (fun a b => a ≤ b) ∧ ∀ i ∈ p.2, i < p.1.size) :
    pages.filterMap (fun p => localMin p.1 p.2) =
      pages.filterMap (fun p => (p.2.filterMap (fun i => p.1[i]?)).min?) := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      have hp1 := hp p (by simp)
      have hpm := localMin_eq_min_decode p.1 p.2 hp1.1 hp1.2
      have hps : ∀ q ∈ ps, q.1.toList.Pairwise (fun a b => a ≤ b) ∧ ∀ i ∈ q.2, i < q.1.size := by
        intro q hq
        exact hp q (by simp [hq])
      simp only [List.filterMap_cons]
      rw [hpm, ih hps]

private theorem flatMap_eq_map_flatten {α β : Type} (f : α → List β) :
    ∀ l : List α, l.flatMap f = (l.map f).flatten := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

theorem OrderedCodeMin_sound :
    OrderedCodeMin.Goal := by
  intro pages hp
  rw [pages_localMin_eq pages hp]
  rw [flatMap_eq_map_flatten]
  rw [filterMap_map_eq]
  change List.min? ((pages.map (fun p => p.2.filterMap (fun i => p.1[i]?))).filterMap (fun b => b.min?)) =
    ((pages.map (fun p => p.2.filterMap (fun i => p.1[i]?))).flatten).min?
  exact min?_flatten_g (pages.map (fun p => p.2.filterMap (fun i => p.1[i]?)))



/-! ## FrontCodedContains -/

private theorem drop_take_append_chars (c rest : List Char) (start : Nat) (h : start ≤ c.length) :
    (c.take start ++ rest).drop start = rest := by
  have htlen : (c.take start).length = start := by
    rw [List.length_take, Nat.min_eq_left h]
  have hdtake : (c.take start).drop start = [] :=
    List.drop_eq_nil_of_le (by rw [htlen]; exact Nat.le_refl _)
  have hd0 := List.drop_append (l₁ := c.take start) (l₂ := rest) (i := start)
  rw [hdtake] at hd0
  have hsub : start - (c.take start).length = 0 := by omega
  rw [hsub] at hd0
  simp only [List.nil_append] at hd0
  exact hd0

private theorem drop_start_append (c s : List Char) (start j : Nat) (h : start ≤ c.length) :
    (c ++ s).drop (start + j) = ((c.drop start) ++ s).drop j := by
  have hc : c = c.take start ++ c.drop start := (List.take_append_drop start c).symm
  rw [hc]
  rw [drop_take_append_chars c (c.drop start) start h]
  rw [List.append_assoc]
  rw [← List.drop_drop, drop_take_append_chars c (c.drop start ++ s) start h]

private theorem occurs_append_right (s t pat : List Char) : occurs s pat → occurs (s ++ t) pat := by
  intro h
  rw [occurs] at h ⊢
  rcases List.any_eq_true.mp h with ⟨i, himem, hpfx⟩
  apply List.any_eq_true.mpr
  refine ⟨i, ?_, ?_⟩
  · apply List.mem_range.mpr
    have hi := List.mem_range.mp himem
    rw [List.length_append]
    omega
  · have hdrop : (s ++ t).drop i = s.drop i ++ t := by
      rw [List.drop_append]
      have hi_le : i ≤ s.length := by
        have hi := List.mem_range.mp himem
        omega
      have hsub : i - s.length = 0 := Nat.sub_eq_zero_of_le hi_le
      simp [hsub]
    rw [hdrop]
    exact List.isPrefixOf_iff_prefix.mpr
      (List.prefix_append_of_prefix (List.isPrefixOf_iff_prefix.mp hpfx))

private theorem occurs_cross_iff (common needle suffix : List Char)
    (hneedle : needle ≠ []) (hnocc : occurs common needle = false) :
    occurs ((common.drop (common.length - (needle.length - 1))) ++ suffix) needle =
      occurs (common ++ suffix) needle := by
  let start := common.length - (needle.length - 1)
  have hstart_le : start ≤ common.length := Nat.sub_le _ _
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [occurs] at h ⊢
    rcases List.any_eq_true.mp h with ⟨j, hjmem, hjpfx⟩
    apply List.any_eq_true.mpr
    refine ⟨start + j, ?_, ?_⟩
    · apply List.mem_range.mpr
      have hjlt := List.mem_range.mp hjmem
      have hlen_shard : ((common.drop start) ++ suffix).length = common.length - start + suffix.length := by
        simp [List.length_drop]
      have hlen_all : (common ++ suffix).length = common.length + suffix.length := by simp
      have hjlt' : j < common.length - start + suffix.length + 1 := by
        simpa [hlen_shard] using hjlt
      have hjle : j ≤ common.length - start + suffix.length := Nat.le_of_lt_succ hjlt'
      have hsum : start + j ≤ common.length + suffix.length := by
        calc
          start + j ≤ start + ((common.length - start) + suffix.length) :=
            Nat.add_le_add_left hjle start
          _ = (start + (common.length - start)) + suffix.length := by rw [Nat.add_assoc]
          _ = common.length + suffix.length := by rw [Nat.add_sub_of_le hstart_le]
      rw [hlen_all]
      exact Nat.lt_succ_of_le hsum
    · rw [drop_start_append common suffix start j hstart_le]
      exact hjpfx
  · intro h
    rw [occurs] at h ⊢
    rcases List.any_eq_true.mp h with ⟨i, himem, hpfx⟩
    by_cases hbig : start ≤ i
    · apply List.any_eq_true.mpr
      refine ⟨i - start, ?_, ?_⟩
      · apply List.mem_range.mpr
        have hi := List.mem_range.mp himem
        have hlen_shard : ((common.drop start) ++ suffix).length = common.length - start + suffix.length := by
          simp [List.length_drop]
        have hlen_all : (common ++ suffix).length = common.length + suffix.length := by simp
        have hi_lt' : i < (common ++ suffix).length + 1 := List.mem_range.mp himem
        have hile : i ≤ (common ++ suffix).length := Nat.le_of_lt_succ hi_lt'
        have hsub_le : i - start ≤ (common ++ suffix).length - start :=
          Nat.sub_le_sub_right hile start
        have hlen_eq : (common ++ suffix).length - start = common.length - start + suffix.length := by
          rw [hlen_all]
          omega
        have hle_shard : i - start ≤ common.length - start + suffix.length := by
          rwa [hlen_eq] at hsub_le
        exact Nat.lt_succ_of_le (by simpa [hlen_shard] using hle_shard)
      · have hdrop := drop_start_append common suffix start (i - start) hstart_le
        rw [← Nat.add_sub_of_le hbig] at hpfx
        rw [hdrop] at hpfx
        exact hpfx
    · have hi_small : i < start := by omega
      have hi_le_c : i ≤ common.length := Nat.le_trans (Nat.le_of_lt hi_small) hstart_le
      have hdropc : (common ++ suffix).drop i = common.drop i ++ suffix := by
        rw [List.drop_append]
        have hsub : i - common.length = 0 := Nat.sub_eq_zero_of_le hi_le_c
        simp [hsub]
      rw [hdropc] at hpfx
      have hpref_all : needle <+: common.drop i ++ suffix := List.isPrefixOf_iff_prefix.mp hpfx
      have hpref_drop : common.drop i <+: common.drop i ++ suffix := List.prefix_append _ _
      have hlen : needle.length ≤ (common.drop i).length := by
        have hLpos : 0 < needle.length := by
          cases needle with
          | nil => simp at hneedle
          | cons c cs => simp
        have hposstart : 0 < start := by omega
        have hcL : needle.length - 1 ≤ common.length := by
          by_cases hle : needle.length - 1 ≤ common.length
          · exact hle
          · have hstart0 : start = 0 := by
              dsimp [start]
              exact Nat.sub_eq_zero_of_le (by omega)
            omega
        simp only [List.length_drop]
        omega
      have hpref_common : needle <+: common.drop i :=
        List.prefix_of_prefix_length_le hpref_all hpref_drop hlen
      have hocc_true : occurs common needle = true := by
        apply List.any_eq_true.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_range.mpr
          have hposneedle : 0 < needle.length := by
            cases needle with
            | nil => simp at hneedle
            | cons c cs => simp
          have hlen' : needle.length ≤ common.length - i := by
            simpa only [List.length_drop] using hlen
          have hi_lt : i < common.length := by omega
          omega
        · exact List.isPrefixOf_iff_prefix.mpr hpref_common
      rw [hnocc] at hocc_true
      contradiction

theorem FrontCodedContains_sound :
    FrontCodedContains.Goal := by
  intro common needle tails hneedle
  let shard := common.drop (common.length - (needle.length - 1))
  dsimp only
  by_cases hocc : occurs common needle
  · have ht : ∀ s ∈ tails, occurs (common ++ s) needle = true := by
      intro s hs
      exact occurs_append_right common s needle hocc
    induction tails with
    | nil => simp
    | cons t ts ih =>
        have ht1 := ht t (by simp)
        have ht2 : ∀ s ∈ ts, occurs (common ++ s) needle = true := by
          intro s hs
          exact ht s (by simp [hs])
        rw [if_pos hocc]
        have ih' : List.replicate ts.length true =
            List.map (fun s => occurs (common ++ s) needle) ts := by
          rw [if_pos hocc] at ih
          exact ih ht2
        simp only [List.map_cons, List.length_cons]
        rw [ht1]
        change List.replicate (ts.length + 1) true = true :: List.map (fun s => occurs (common ++ s) needle) ts
        rw [List.replicate_succ, ih']
  · have hoccf : occurs common needle = false := Bool.eq_false_of_ne_true hocc
    have hcross : ∀ s ∈ tails, occurs (shard ++ s) needle = occurs (common ++ s) needle := by
      intro s hs
      exact occurs_cross_iff common needle s hneedle hoccf
    induction tails with
    | nil => simp
    | cons t ts ih =>
        have ht := hcross t (by simp)
        have hts : ∀ s ∈ ts, occurs (shard ++ s) needle = occurs (common ++ s) needle := by
          intro s hs
          exact hcross s (by simp [hs])
        rw [if_neg hocc]
        simpa [shard] using And.intro ht hts



/-! ## Misra--Gries conservation -/

private theorem empty_csum' : csum' ({} : Std.HashMap Nat Nat) = 0 := by simp [csum', csumL']

private def decStep (acc : Std.HashMap Nat Nat) (p : Nat × Nat) : Std.HashMap Nat Nat :=
  if p.2 = 1 then acc else acc.insert p.1 (p.2 - 1)

private theorem foldl_dec_csum (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (acc : Std.HashMap Nat Nat)
    (hno : ∀ p ∈ l, ¬ acc.contains p.1) :
    csum' (l.foldl decStep acc) = csum' acc + (l.map fun p => p.2 - 1).sum := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      have hnd_head : ∀ q ∈ ps, (p.1 == q.1) = false := (List.pairwise_cons.mp hnd).1
      have hnd_tail : ps.Pairwise (fun a b => (a.1 == b.1) = false) := (List.pairwise_cons.mp hnd).2
      have hnotp := hno p (by simp)
      simp only [List.foldl_cons]
      by_cases hp1 : p.2 = 1
      · have hih := ih hnd_tail acc (by intro q hq; exact hno q (by simp [hq]))
        simp [decStep, hp1, hih]
      · have hins := csum_insert_new' acc p.1 (p.2 - 1) hnotp
        have hno' : ∀ q ∈ ps, ¬ (acc.insert p.1 (p.2 - 1)).contains q.1 := by
          intro q hq hcont
          rw [Std.HashMap.contains_insert] at hcont
          simp only [Bool.or_eq_true] at hcont
          rcases hcont with hb | hm
          · have heq : p.1 = q.1 := beq_iff_eq.mp hb
            have hn := hnd_head q hq
            have : (p.1 == q.1) = true := by simp [heq]
            rw [hn] at this
            contradiction
          · exact hno q (by simp [hq]) (Std.HashMap.contains_iff_mem.mp hm)
        have hih := ih hnd_tail (acc.insert p.1 (p.2 - 1)) hno'
        rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        simp only [List.map_cons, List.sum_cons]
        rw [hih, hins]
        omega

private theorem sum_sub_one (l : List (Nat × Nat)) (hpos : ∀ p ∈ l, 0 < p.2) :
    (l.map fun p => p.2 - 1).sum + l.length = csumL' l := by
  induction l with
  | nil => simp [csumL']
  | cons p ps ih =>
      have htail : ∀ q ∈ ps, 0 < q.2 := by intro q hq; exact hpos q (by simp [hq])
      have hih := ih htail
      have hp : 0 < p.2 := hpos p (by simp)
      simp only [List.map_cons, List.sum_cons, List.length_cons, csumL']
      simp only [csumL'] at hih
      omega

private theorem dec_csum_exact (h : Std.HashMap Nat Nat)
    (hpos : ∀ v, h.contains v → 0 < h.getD v 0) :
    csum' (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}) + h.size = csum' h := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change csum' (h.toList.foldl decStep {}) + h.size = csum' h
  have hnd := Std.HashMap.distinct_keys_toList (m := h)
  have hposl : ∀ p ∈ h.toList, 0 < p.2 := by
    intro p hp
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
    have hg : h.getD p.1 0 = p.2 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hp]
      simp
    rw [← hg]
    apply hpos p.1
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    rw [hp]
    simp
  have hno : ∀ p ∈ h.toList, ¬ ({} : Std.HashMap Nat Nat).contains p.1 := by simp
  have hf := foldl_dec_csum h.toList hnd {} hno
  rw [empty_csum'] at hf
  have hs := sum_sub_one h.toList hposl
  dsimp [csum'] at hf ⊢
  rw [hf]
  have hlen : h.size = h.toList.length := (Std.HashMap.length_toList (m := h)).symm
  rw [hlen]
  omega

private theorem foldl_dec_size (l : List (Nat × Nat)) (acc : Std.HashMap Nat Nat) :
    (l.foldl decStep acc).size ≤ acc.size + l.length := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      by_cases hp : p.2 = 1
      · have hih := ih acc
        simp [decStep, hp]
        omega
      · have hs : (acc.insert p.1 (p.2 - 1)).size ≤ acc.size + 1 := by
          have := Std.HashMap.size_insert_le (m := acc) (k := p.1) (v := p.2 - 1)
          omega
        have hih := ih (acc.insert p.1 (p.2 - 1))
        simp [decStep, hp]
        omega

private theorem dec_size (h : Std.HashMap Nat Nat) :
  (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).size ≤ h.size := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change (h.toList.foldl decStep {}).size ≤ h.size
  have hf := foldl_dec_size h.toList {}
  simpa using hf

private theorem foldl_dec_getD (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (acc : Std.HashMap Nat Nat) (hno : ∀ p ∈ l, ¬ acc.contains p.1) (v : Nat) :
    acc.getD v 0 ≤ (l.foldl decStep acc).getD v 0 + (l.filter (fun p => p.1 == v)).length := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      have hnd_head : ∀ q ∈ ps, (p.1 == q.1) = false := (List.pairwise_cons.mp hnd).1
      have hnd_tail : ps.Pairwise (fun a b => (a.1 == b.1) = false) := (List.pairwise_cons.mp hnd).2
      have hno_tail : ∀ q ∈ ps, ¬ acc.contains q.1 := by intro q hq; exact hno q (by simp [hq])
      simp only [List.foldl_cons]
      by_cases hp1 : p.2 = 1
      · rw [show decStep acc p = acc by simp [decStep, hp1]]
        have hih := ih hnd_tail acc hno_tail
        by_cases hpv : p.1 == v
        · have hf : (p :: ps).filter (fun q => q.1 == v) = p :: ps.filter (fun q => q.1 == v) := by simp [hpv]
          rw [hf]
          simp
          omega
        · have hf : (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) := by simp [hpv]
          rw [hf]
          simpa using hih
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        have hno' : ∀ q ∈ ps, ¬ (acc.insert p.1 (p.2 - 1)).contains q.1 := by
          intro q hq hcont
          rw [Std.HashMap.contains_insert] at hcont
          simp only [Bool.or_eq_true] at hcont
          rcases hcont with hb | hm
          · have heq : p.1 = q.1 := beq_iff_eq.mp hb
            have hn := hnd_head q hq
            have : (p.1 == q.1) = true := by simp [heq]
            rw [hn] at this
            contradiction
          · exact hno q (by simp [hq]) (Std.HashMap.contains_iff_mem.mp hm)
        have hih := ih hnd_tail (acc.insert p.1 (p.2 - 1)) hno'
        by_cases hpv : p.1 == v
        · have heq : p.1 = v := beq_iff_eq.mp hpv
          have hnotv : ¬ acc.contains v := by
            have := hno p (by simp)
            rwa [heq] at this
          have hcf : acc.contains v = false := by by_cases c : acc.contains v <;> simp_all
          have haccd : acc.getD v 0 = 0 := Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
          rw [haccd]
          simp
        · rw [Std.HashMap.getD_insert] at hih
          simp [hpv] at hih
          rw [show (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) by simp [hpv]]
          simpa using hih

private theorem nodup_keys_dec (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false)) (v : Nat) :
    (l.filter (fun p => p.1 == v)).length ≤ 1 := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      rw [List.pairwise_cons] at hnd
      by_cases hpv : p.1 == v
      · have hno : ∀ q ∈ ps, (q.1 == v) = false := by
          intro q hq
          have hnq : (p.1 == q.1) = false := hnd.1 q hq
          have hnq' : p.1 ≠ q.1 := by
            intro he
            have : (p.1 == q.1) = true := by simp [he]
            rw [hnq] at this
            contradiction
          rw [beq_eq_false_iff_ne]
          intro hqv
          have hpe : p.1 = v := beq_iff_eq.mp hpv
          exact hnq' (hpe.trans hqv.symm)
        have hf : ps.filter (fun q => q.1 == v) = [] := by
          rw [List.eq_nil_iff_forall_not_mem]
          intro q hq
          rw [List.mem_filter] at hq
          rcases hq with ⟨hqmem, hqbeq⟩
          have hfq := hno q hqmem
          rw [hfq] at hqbeq
          contradiction
        simp [hpv, hf]
      · have ht := ih hnd.2
        have hf : (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) := by simp [hpv]
        rw [hf]
        exact ht

private theorem foldl_dec_getD_of_not_mem (l : List (Nat × Nat)) (acc : Std.HashMap Nat Nat) (v : Nat)
    (hno : ∀ p ∈ l, (p.1 == v) = false) :
    (l.foldl decStep acc).getD v 0 = acc.getD v 0 := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      have hpv : (p.1 == v) = false := hno p (by simp)
      have hno_tail : ∀ q ∈ ps, (q.1 == v) = false := by intro q hq; exact hno q (by simp [hq])
      by_cases hp1 : p.2 = 1
      · rw [show decStep acc p = acc by simp [decStep, hp1]]
        exact ih acc hno_tail
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        have hih := ih (acc.insert p.1 (p.2 - 1)) hno_tail
        rw [Std.HashMap.getD_insert] at hih
        simp [hpv] at hih
        exact hih

private theorem foldl_dec_allpos (l : List (Nat × Nat)) (hpos : ∀ p ∈ l, 0 < p.2) :
    ∀ (acc : Std.HashMap Nat Nat), (∀ v, acc.contains v → 0 < acc.getD v 0) →
    ∀ v, (l.foldl decStep acc).contains v → 0 < (l.foldl decStep acc).getD v 0 := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      intro acc hacc v hv
      simp only [List.foldl_cons] at hv ⊢
      have htail : ∀ q ∈ ps, 0 < q.2 := by intro q hq; exact hpos q (by simp [hq])
      by_cases hp1 : p.2 = 1
      · simp [decStep, hp1] at hv ⊢
        exact ih htail acc hacc v hv
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]] at hv ⊢
        have hp2 : 0 < p.2 - 1 := by
          have := hpos p (by simp)
          omega
        have hacc' : ∀ v, (acc.insert p.1 (p.2 - 1)).contains v → 0 < (acc.insert p.1 (p.2 - 1)).getD v 0 := by
          intro v hv'
          rw [Std.HashMap.getD_insert]
          by_cases hbeq : p.1 == v
          · simp [hbeq, hp2]
          · have hm : acc.contains v := by
              rw [Std.HashMap.contains_insert] at hv'
              simp [hbeq] at hv'
              exact hv'
            simpa [hbeq] using hacc v hm
        exact ih htail (acc.insert p.1 (p.2 - 1)) hacc' v hv

private theorem dec_allpos (h : Std.HashMap Nat Nat) (hpos : ∀ v, h.contains v → 0 < h.getD v 0) :
    ∀ v, (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).contains v →
      0 < (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).getD v 0 := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change ∀ v, (h.toList.foldl decStep {}).contains v → 0 < (h.toList.foldl decStep {}).getD v 0
  have hposl : ∀ p ∈ h.toList, 0 < p.2 := by
    intro p hp
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
    have hg : h.getD p.1 0 = p.2 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hp]
      simp
    rw [← hg]
    apply hpos p.1
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    rw [hp]
    simp
  exact foldl_dec_allpos h.toList hposl {} (by simp)

private theorem split_entry (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (v n : Nat) (hmem : ∃ p ∈ l, p.1 == v ∧ p.2 = n) :
    ∃ l₁ l₂, l = l₁ ++ (v, n) :: l₂ ∧ ∀ p ∈ l₁ ++ l₂, (p.1 == v) = false := by
  induction l with
  | nil => simp at hmem
  | cons p ps ih =>
      rcases hmem with ⟨q, hqmem, hqv, hqn⟩
      by_cases hpv : p.1 == v
      · have hqeq : q = p := by
          have hqmem' : q = p ∨ q ∈ ps := by simpa using hqmem
          rcases hqmem' with h | hqps
          · exact h
          · have hqe : q.1 = v := beq_iff_eq.mp hqv
            have hpe : p.1 = v := beq_iff_eq.mp hpv
            have : (p.1 == q.1) = true := by simp [hpe, hqe]
            rw [(List.pairwise_cons.mp hnd).1 q hqps] at this
            contradiction
        subst q
        have hpe : p.1 = v := beq_iff_eq.mp hpv
        have hpn : p.2 = n := hqn
        have hno : ∀ r ∈ ps, (r.1 == v) = false := by
          intro r hr
          rw [beq_eq_false_iff_ne]
          intro hrv
          have : (p.1 == r.1) = true := by simp [hpe, hrv]
          rw [(List.pairwise_cons.mp hnd).1 r hr] at this
          contradiction
        refine ⟨[], ps, ?_, ?_⟩
        · cases p with
          | mk p1 p2 => simp_all
        · simpa using hno
      · have hqps : q ∈ ps := by
          have hqmem' : q = p ∨ q ∈ ps := by simpa using hqmem
          rcases hqmem' with h | h
          · subst q
            exfalso
            exact hpv hqv
          · exact h
        have hnd_tail := (List.pairwise_cons.mp hnd).2
        rcases ih hnd_tail ⟨q, hqps, hqv, hqn⟩ with ⟨l₁, l₂, hl, hno⟩
        have hpvf : (p.1 == v) = false := by by_cases h : p.1 == v <;> simp_all
        refine ⟨p :: l₁, l₂, ?_, ?_⟩
        · simp [hl]
        · intro r hr
          simp at hr
          rcases hr with heq | hr
          · subst r
            exact hpvf
          · exact hno r (List.mem_append.mpr hr)

private theorem dec_getD_of_list (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (v n : Nat) (hentry : ∃ p ∈ l, p.1 == v ∧ p.2 = n) :
    n ≤ (l.foldl decStep {}).getD v 0 + 1 := by
  rcases split_entry l hnd v n hentry with ⟨l₁, l₂, hl, hnv⟩
  subst l
  simp only [List.foldl_append, List.foldl_cons]
  have hnv1 : ∀ p ∈ l₁, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
  have hnv2 : ∀ p ∈ l₂, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
  have hfirst := foldl_dec_getD_of_not_mem l₁ {} v hnv1
  simp only [Std.HashMap.getD_empty] at hfirst
  by_cases hn1 : n = 1
  · rw [show decStep (l₁.foldl decStep {}) (v, n) = l₁.foldl decStep {} by simp [decStep, hn1]]
    have hlast := foldl_dec_getD_of_not_mem l₂ (l₁.foldl decStep {}) v hnv2
    rw [hlast, hfirst]
    omega
  · rw [show decStep (l₁.foldl decStep {}) (v, n) = (l₁.foldl decStep {}).insert v (n - 1) by simp [decStep, hn1]]
    have hlast := foldl_dec_getD_of_not_mem l₂ ((l₁.foldl decStep {}).insert v (n - 1)) v hnv2
    rw [hlast, Std.HashMap.getD_insert]
    simp
    omega

private theorem dec_getD (h : Std.HashMap Nat Nat) (v : Nat) :
    h.getD v 0 ≤ (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).getD v 0 + 1 := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change h.getD v 0 ≤ (h.toList.foldl decStep {}).getD v 0 + 1
  have hnd := Std.HashMap.distinct_keys_toList (m := h)
  by_cases hmem : ∃ n, h[v]? = some n
  · rcases hmem with ⟨n, hn⟩
    have hg : h.getD v 0 = n := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hn]
      simp
    rw [hg]
    have hentry : ∃ p ∈ h.toList, p.1 == v ∧ p.2 = n := by
      rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList] at hn
      rcases hn with ⟨k, hkv, hk⟩
      refine ⟨(k, n), hk, ?_, rfl⟩
      have hkv_eq : k = v := (beq_iff_eq.mp hkv).symm
      simp [hkv_eq]
    exact dec_getD_of_list h.toList hnd v n hentry
  · have hg : h.getD v 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?]
      cases h' : h[v]? with
      | none => simp
      | some x => exact False.elim (hmem ⟨x, h'⟩)
    rw [hg]
    exact Nat.zero_le _


private theorem count_append_singleton (pre : List Nat) (k v : Nat) :
    (pre ++ [k]).count v = pre.count v + (if k == v then 1 else 0) := by
  rw [List.count_append]
  simp [List.count]

private def mgStep (slots : Nat) (h : Std.HashMap Nat Nat) (k : Nat) : Std.HashMap Nat Nat :=
  if h.contains k then h.insert k (h.getD k 0 + 1)
  else if h.size < slots then h.insert k 1
  else h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n == 1 then acc else acc.insert j (n - 1)) {}

private theorem candidates_eq_mg (xs : List Nat) (slots : Nat) :
    TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots = xs.foldl (mgStep slots) {} := by
  rfl

private def mgAux (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) : List Nat → Std.HashMap Nat Nat × Nat × Nat
  | [] => (h, d, n)
  | k :: ks =>
      let d' := if h.contains k then d else if h.size < slots then d else d + 1
      mgAux slots (mgStep slots h k) d' (n + 1) ks

private def MGInv (slots : Nat) (pre : List Nat) (st : Std.HashMap Nat Nat × Nat × Nat) : Prop :=
  (∀ v, pre.count v ≤ st.1.getD v 0 + st.2.1) ∧
  (∀ v, st.1.contains v → 0 < st.1.getD v 0) ∧
  csum' st.1 + st.2.1 * (slots + 1) = st.2.2 ∧
  st.1.size ≤ slots ∧
  ∀ v, st.1.getD v 0 ≤ pre.count v

private theorem dec_getD_le' (h : Std.HashMap Nat Nat) (v : Nat) :
    (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n - 1)) {}).getD v 0 ≤
      h.getD v 0 := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change (h.toList.foldl decStep {}).getD v 0 ≤ h.getD v 0
  have hnd := Std.HashMap.distinct_keys_toList (m := h)
  by_cases hmem : ∃ n, h[v]? = some n
  · rcases hmem with ⟨n, hn⟩
    have hg : h.getD v 0 = n := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hn]
      simp
    rw [hg]
    have hentry : ∃ p ∈ h.toList, p.1 == v ∧ p.2 = n := by
      rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList] at hn
      rcases hn with ⟨k, hkv, hk⟩
      refine ⟨(k, n), hk, ?_, rfl⟩
      have hkv_eq : k = v := (beq_iff_eq.mp hkv).symm
      simp [hkv_eq]
    rcases split_entry h.toList hnd v n hentry with ⟨l₁, l₂, hl, hnv⟩
    rw [hl]
    simp only [List.foldl_append, List.foldl_cons]
    have hnv1 : ∀ p ∈ l₁, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
    have hnv2 : ∀ p ∈ l₂, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
    by_cases hn1 : n = 1
    · rw [show decStep (l₁.foldl decStep {}) (v, n) = l₁.foldl decStep {} by simp [decStep, hn1]]
      have hlast := foldl_dec_getD_of_not_mem l₂ (l₁.foldl decStep {}) v hnv2
      rw [hlast]
      have hz1 := foldl_dec_getD_of_not_mem l₁ {} v hnv1
      simp only [Std.HashMap.getD_empty] at hz1
      rw [hz1]
      omega
    · rw [show decStep (l₁.foldl decStep {}) (v, n) = (l₁.foldl decStep {}).insert v (n - 1) by simp [decStep, hn1]]
      have hlast := foldl_dec_getD_of_not_mem l₂ ((l₁.foldl decStep {}).insert v (n - 1)) v hnv2
      rw [hlast, Std.HashMap.getD_insert]
      simp
  · have hg : h.getD v 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?]
      cases h' : h[v]? with
      | none => simp
      | some x => exact False.elim (hmem ⟨x, h'⟩)
    rw [hg]
    have hno : ∀ p ∈ h.toList, (p.1 == v) = false := by
      intro p hp
      by_cases hbeq : p.1 == v
      · have hveq : v = p.1 := (beq_iff_eq.mp hbeq).symm
        have hsome : h[p.1]? = some p.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hp
        rw [← hveq] at hsome
        exact False.elim (hmem ⟨p.2, hsome⟩)
      · exact Bool.eq_false_of_ne_true hbeq
    have hz := foldl_dec_getD_of_not_mem h.toList {} v hno
    simp only [Std.HashMap.getD_empty] at hz
    rw [hz]
    exact Nat.le_refl 0

private theorem mgStep_inv (slots : Nat) (pre : List Nat) (h : Std.HashMap Nat Nat) (d k : Nat)
    (hinv : MGInv slots pre (h, d, pre.length)) :
    MGInv slots (pre ++ [k])
      (mgStep slots h k,
       (if h.contains k then d else if h.size < slots then d else d + 1),
       pre.length + 1) := by
  dsimp [MGInv] at hinv
  rcases hinv with ⟨hcnt, hpos, hcsum, hsize, hlower⟩
  dsimp [MGInv]
  by_cases hk : h.contains k
  · have hget : h.getD k 0 + 1 = (h.insert k (h.getD k 0 + 1)).getD k 0 := by
      rw [Std.HashMap.getD_insert]
      simp
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro v
      rw [count_append_singleton]
      by_cases hv : k == v
      · have hveq : k = v := beq_iff_eq.mp hv
        subst v
        simp [mgStep, hk]
        have := hcnt k
        omega
      · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
        simp [hvf, mgStep, hk]
        rw [Std.HashMap.getD_insert, hvf]
        exact hcnt v
    · intro v hv
      rw [mgStep] at hv
      simp [hk] at hv
      rw [mgStep]
      simp [hk]
      rw [Std.HashMap.getD_insert]
      by_cases hkv : k == v
      · simp [hkv]
      · simp [hkv]
        rcases hv with heq | hm
        · subst v
          exact hpos k hk
        · exact hpos v (Std.HashMap.contains_iff_mem.mpr hm)
    · rw [mgStep]
      simp [hk]
      rw [csum_insert_succ']
      omega
    · rw [mgStep]
      simp [hk]
      have hsz : (h.insert k (h.getD k 0 + 1)).size = h.size := by
        rw [Std.HashMap.size_insert]
        simp [Std.HashMap.mem_iff_contains.mpr hk]
      rw [hsz]
      exact hsize
    · intro v
      rw [count_append_singleton]
      by_cases hv : k == v
      · have hveq : k = v := beq_iff_eq.mp hv
        subst v
        simp [mgStep, hk]
        have := hlower k
        omega
      · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
        simp [hvf, mgStep, hk]
        rw [Std.HashMap.getD_insert, hvf]
        exact hlower v
  · by_cases hfull : h.size < slots
    · have hget0 : h.getD k 0 = 0 := by
        have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
        exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
          have := hcnt k
          rw [hget0] at this
          omega
        · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          rw [Std.HashMap.getD_insert, hvf]
          exact hcnt v
      · intro v hv
        rw [mgStep] at hv
        simp [hk, hfull] at hv
        rw [mgStep]
        simp [hk, hfull]
        rw [Std.HashMap.getD_insert]
        by_cases hkv : k == v
        · simp [hkv]
        · simp [hkv]
          rcases hv with heq | hm
          · subst v
            exfalso
            exact hk (by by_cases c : h.contains k <;> simp_all)
          · exact hpos v (Std.HashMap.contains_iff_mem.mpr hm)
      · rw [mgStep]
        simp [hk, hfull]
        rw [csum_insert_new' h k 1 hk]
        omega
      · rw [mgStep]
        simp [hk, hfull]
        have hs := Std.HashMap.size_insert_le (m := h) (k := k) (v := 1)
        omega
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
        · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          rw [Std.HashMap.getD_insert, hvf]
          exact hlower v
    · have hsizeEq : h.size = slots := by omega
      have hget0 : h.getD k 0 = 0 := by
        have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
        exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
      let h' := h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n == 1 then acc else acc.insert j (n - 1)) {}
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
          have := hcnt k
          rw [hget0] at this
          omega
        · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          have hd := dec_getD h v
          have hc := hcnt v
          omega
      · intro v hv
        rw [mgStep] at hv
        simp [hk, hfull] at hv
        rw [mgStep]
        simp [hk, hfull]
        exact dec_allpos h hpos v hv
      · rw [mgStep]
        simp [hk, hfull]
        have hcs := dec_csum_exact h hpos
        rw [hsizeEq] at hcs
        have hmul : (d + 1) * (slots + 1) = d * (slots + 1) + slots + 1 := by
          rw [Nat.add_mul, Nat.one_mul, Nat.mul_add, Nat.mul_one]
          omega
        rw [hmul]
        omega
      · rw [mgStep]
        simp [hk, hfull]
        exact Nat.le_trans (dec_size h) hsize
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
          have hd := dec_getD_le' h k
          rw [hget0] at hd
          omega
        · have hvf : (k == v) = false := by by_cases hh : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          have hd := dec_getD_le' h v
          have hl := hlower v
          have hcapp : pre.count v ≤ (pre ++ [k]).count v := by
            rw [count_append_singleton]
            omega
          omega

private theorem mgAux_inv (slots : Nat) :
    ∀ (pre : List Nat) (h : Std.HashMap Nat Nat) (d : Nat),
      MGInv slots pre (h, d, pre.length) →
      ∀ rest : List Nat, MGInv slots (pre ++ rest) (mgAux slots h d pre.length rest) := by
  intro pre h d hinv rest
  induction rest generalizing pre h d with
  | nil => simpa [mgAux] using hinv
  | cons k ks ih =>
      let d1 := if h.contains k then d else if h.size < slots then d else d + 1
      have hstep := mgStep_inv slots pre h d k hinv
      have hlen : (pre ++ [k]).length = pre.length + 1 := by simp
      have hstep2 : MGInv slots (pre ++ [k]) (mgStep slots h k, d1, (pre ++ [k]).length) := by
        rw [hlen]
        exact hstep
      have hih := ih (pre ++ [k]) (mgStep slots h k) d1 hstep2
      rw [hlen] at hih
      rw [List.append_assoc] at hih
      dsimp [mgAux]
      exact hih

private theorem mgAux_fst (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) :
    ∀ xs, (mgAux slots h d n xs).1 = xs.foldl (mgStep slots) h := by
  intro xs
  induction xs generalizing h d n with
  | nil => simp [mgAux]
  | cons k ks ih =>
      dsimp [mgAux]
      exact ih (mgStep slots h k) _ (n + 1)

private theorem mgAux_len (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) :
    ∀ xs, (mgAux slots h d n xs).2.2 = n + xs.length := by
  intro xs
  induction xs generalizing h d n with
  | nil => simp [mgAux]
  | cons k ks ih =>
      dsimp [mgAux]
      have hih := ih (mgStep slots h k) (if h.contains k then d else if h.size < slots then d else d + 1) (n + 1)
      omega

/-! ## CancellationDebt -/

theorem CancellationDebt_sound :
    CancellationDebt.Goal := by
  intro xs slots k0
  let st := mgAux slots {} 0 0 xs
  have hinv0 : MGInv slots [] ({}, 0, 0) := by
    dsimp [MGInv]
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro v
      simp
    · intro v hv
      simp at hv
    · simp [empty_csum']
    · simp
    · intro v
      simp
  have hfinal := mgAux_inv slots [] {} 0 hinv0 xs
  dsimp [MGInv] at hfinal
  rcases hfinal with ⟨hcnt, hpos, hcsum, hsize, hlower⟩
  have hfst : st.1 = TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots := by
    dsimp [st]
    rw [mgAux_fst, ← candidates_eq_mg]
  have hlen : st.2.2 = xs.length := by
    dsimp [st]
    simpa using mgAux_len slots {} 0 0 xs
  have hcsumS : csum' st.1 + st.2.1 * (slots + 1) = st.2.2 := by simpa [st] using hcsum
  rw [hlen] at hcsumS
  have hcntS : ∀ v, xs.count v ≤ st.1.getD v 0 + st.2.1 := by
    simpa [st] using hcnt
  have hcntLower : ∀ v, st.1.getD v 0 ≤ xs.count v := by
    simpa [st] using hlower
  have hdiv : (xs.length - csum' st.1) / (slots + 1) = st.2.1 := by
    have hc : csum' st.1 + st.2.1 * (slots + 1) = xs.length := hcsumS
    have hsub : xs.length - csum' st.1 = st.2.1 * (slots + 1) := by omega
    rw [hsub, Nat.mul_comm]
    exact Nat.mul_div_right st.2.1 (by omega : 0 < slots + 1)
  have hdebt : debt xs slots = st.2.1 := by
    unfold debt
    rw [← hfst]
    exact hdiv
  clear hcsum hcnt hlower
  dsimp [debt]
  rw [← hfst]
  change xs.length = csum' st.1 + (slots + 1) * ((xs.length - csum' st.1) / (slots + 1)) ∧
    st.1.getD k0 0 ≤ xs.count k0 ∧
      xs.count k0 ≤ st.1.getD k0 0 + ((xs.length - csum' st.1) / (slots + 1))
  rw [hdiv]
  constructor
  · rw [Nat.mul_comm (slots + 1) st.2.1]
    omega
  · have hc := hcntS k0
    constructor
    · exact hcntLower k0
    · omega


/-! ## CertifiedWinners -/

private def cmpRows (rows : List Nat) (a b : Nat) : Bool :=
  rows.count a > rows.count b || (rows.count a == rows.count b && a ≤ b)

private theorem cmpRows_refl (rows : List Nat) (a : Nat) : cmpRows rows a a = true := by
  unfold cmpRows
  simp

private theorem cmpRows_antisymm (rows : List Nat) {a b : Nat} :
    cmpRows rows a b = true → cmpRows rows b a = true → a = b := by
  intro hab hba
  have habp : rows.count a > rows.count b ∨ (rows.count a = rows.count b ∧ a ≤ b) := by
    simpa [cmpRows, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hab
  have hbap : rows.count b > rows.count a ∨ (rows.count b = rows.count a ∧ b ≤ a) := by
    simpa [cmpRows, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hba
  rcases habp with hgt | ⟨heq, hle⟩
  · rcases hbap with hlt | ⟨heq', hle'⟩ <;> omega
  · rcases hbap with hlt | ⟨heq', hle'⟩
    · omega
    · omega

private theorem cmpRows_total (rows : List Nat) (a b : Nat) :
    (cmpRows rows a b || cmpRows rows b a) = true := by
  by_cases hgt : rows.count a > rows.count b
  · simp [cmpRows, hgt]
  · by_cases hlt : rows.count b > rows.count a
    · simp [cmpRows, hgt, hlt]
    · have heq : rows.count a = rows.count b := by omega
      rcases Nat.le_total a b with hab | hba
      · simp [cmpRows, hgt, hlt, heq, hab]
      · simp [cmpRows, hgt, hlt, heq, hba]

private theorem cmpRows_trans (rows : List Nat) (a b c : Nat) :
    cmpRows rows a b = true → cmpRows rows b c = true → cmpRows rows a c = true := by
  intro hab hbc
  have habp : rows.count a > rows.count b ∨ (rows.count a = rows.count b ∧ a ≤ b) := by
    simpa [cmpRows, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hab
  have hbcp : rows.count b > rows.count c ∨ (rows.count b = rows.count c ∧ b ≤ c) := by
    simpa [cmpRows, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hbc
  have hacp : rows.count a > rows.count c ∨ (rows.count a = rows.count c ∧ a ≤ c) := by
    rcases habp with habgt | ⟨habeq, hable⟩
    · rcases hbcp with hbcgt | ⟨hbceq, hbcle⟩
      · left; omega
      · left; omega
    · rcases hbcp with hbcgt | ⟨hbceq, hbcle⟩
      · left; omega
      · right
        constructor
        · omega
        · exact Nat.le_trans hable hbcle
  simpa [cmpRows, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] using hacp

private theorem cmpRows_good_before_bad (rows : List Nat) (d a b : Nat)
    (ha : d < rows.count a) (hb : ¬ d < rows.count b) : cmpRows rows a b = true := by
  unfold cmpRows
  have hgt : rows.count a > rows.count b := by omega
  simp [hgt]

private theorem ranked_perm (rows keys : List Nat) : (ranked rows keys).Perm keys := by
  change (keys.mergeSort (cmpRows rows)).Perm keys
  exact List.mergeSort_perm keys (cmpRows rows)

private theorem ranked_sorted (rows keys : List Nat) :
    (ranked rows keys).Pairwise (fun a b => cmpRows rows a b) := by
  change (keys.mergeSort (cmpRows rows)).Pairwise (fun a b => cmpRows rows a b = true)
  exact List.pairwise_mergeSort (fun a b c => cmpRows_trans rows a b c) (cmpRows_total rows) keys

private theorem pairwise_append {α : Type} {R : α → α → Prop} :
    ∀ (l₁ l₂ : List α), l₁.Pairwise R → l₂.Pairwise R →
      (∀ a ∈ l₁, ∀ b ∈ l₂, R a b) → (l₁ ++ l₂).Pairwise R := by
  intro l₁
  induction l₁ with
  | nil =>
      intro l₂ h1 h2 hcross
      simpa using h2
  | cons a as ih =>
      intro l₂ h1 h2 hcross
      have htail := ih l₂ (List.pairwise_cons.mp h1).2 h2 (by
        intro x hx y hy
        exact hcross x (by simp [hx]) y hy)
      apply List.Pairwise.cons
      · intro b hb
        rcases List.mem_append.mp hb with hb | hb
        · exact (List.pairwise_cons.mp h1).1 b hb
        · exact hcross a (by simp) b hb
      · exact htail

private theorem sorted_perm_eq {R : Nat → Nat → Bool}
    (hrefl : ∀ a, R a a = true)
    (hantisymm : ∀ {a b}, R a b = true → R b a = true → a = b)
    {l₁ l₂ : List Nat} (hnd1 : l₁.Nodup) (hnd2 : l₂.Nodup)
    (hs1 : l₁.Pairwise (fun a b => R a b = true))
    (hs2 : l₂.Pairwise (fun a b => R a b = true)) (hp : l₁.Perm l₂) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => rfl
      | cons b bs =>
          simp at hp
  | cons a as ih =>
      cases l₂ with
      | nil =>
          simp at hp
      | cons b bs =>
          have ha_mem : a ∈ b :: bs := hp.subset (by simp)
          have hb_mem : b ∈ a :: as := hp.symm.subset (by simp)
          by_cases hab : a = b
          · subst b
            have htail : as.Perm bs := by simpa using (List.cons_perm_iff_perm_erase.mp hp).2
            rw [ih (List.pairwise_cons.mp hnd1).2 (List.pairwise_cons.mp hnd2).2
              (List.pairwise_cons.mp hs1).2 (List.pairwise_cons.mp hs2).2 htail]
          · have ha_tail : a ∈ bs := by
              rcases List.mem_cons.mp ha_mem with h | h
              · exact False.elim (hab h)
              · exact h
            have hb_tail : b ∈ as := by
              rcases List.mem_cons.mp hb_mem with h | h
              · exact False.elim (hab h.symm)
              · exact h
            have hRab : R a b = true := (List.pairwise_cons.mp hs1).1 b hb_tail
            have hRba : R b a = true := (List.pairwise_cons.mp hs2).1 a ha_tail
            exact False.elim (hab (hantisymm hRab hRba))

private theorem perm_middle {α : Type} (a : α) (X Y : List α) :
    (X ++ a :: Y).Perm (a :: (X ++ Y)) := by
  induction X with
  | nil => rfl
  | cons x xs ih =>
      exact (List.Perm.cons x ih).trans (List.Perm.swap a x (xs ++ Y))

private theorem filter_partition_perm {α : Type} (p : α → Bool) :
    ∀ l : List α, (l.filter p ++ l.filter (fun x => !p x)).Perm l := by
  intro l
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · simpa [hp] using (List.Perm.cons a ih)
      · have hpf : p a = false := Bool.eq_false_of_ne_true hp
        simpa [hpf] using ((perm_middle a (as.filter p) (as.filter (fun x => !p x))).trans (List.Perm.cons a ih))

private theorem nodup_count_eq_one (l : List Nat) (hnd : l.Nodup) (a : Nat) (ha : a ∈ l) :
    l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      have hnd' := List.pairwise_cons.mp hnd
      by_cases hxa : x = a
      · subst a
        have hxnot : ¬ x ∈ xs := by
          intro hx
          exact hnd'.1 x hx rfl
        rw [List.count_cons]
        simp [hxnot, List.count_eq_zero.mpr hxnot]
      · have hxb : (x == a) = false := beq_eq_false_iff_ne.mpr hxa
        have ha_tail : a ∈ xs := by
          rcases List.mem_cons.mp ha with h | h
          · exact False.elim (hxa h.symm)
          · exact h
        rw [List.count_cons]
        simp [hxb, ih hnd'.2 ha_tail]

private theorem perm_of_nodup_mem {l₁ l₂ : List Nat} (h1 : l₁.Nodup) (h2 : l₂.Nodup)
    (hm : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  apply List.perm_iff_count.mpr
  intro a
  by_cases ha : a ∈ l₁
  · have hb : a ∈ l₂ := (hm a).mp ha
    rw [nodup_count_eq_one l₁ h1 a ha, nodup_count_eq_one l₂ h2 a hb]
  · have hb : a ∉ l₂ := by
      intro hb
      exact ha ((hm a).mpr hb)
    rw [List.count_eq_zero.mpr ha, List.count_eq_zero.mpr hb]

private theorem mem_candidate_keys {h : Std.HashMap Nat Nat} {g : Nat} :
    g ∈ h.toList.map Prod.fst ↔ h.contains g = true := by
  constructor
  · intro hg
    rcases List.mem_map.mp hg with ⟨p, hp, heq⟩
    have hsome : h[p.1]? = some p.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hp
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    rw [← heq, hsome]
    simp
  · intro hc
    rcases entry_of_contains' h g hc with ⟨n, hn, hnget⟩
    exact List.mem_map.mpr ⟨(g, n), hn, rfl⟩


private theorem length_filter_eq_countP {α : Type} (l : List α) (p : α → Bool) :
    (l.filter p).length = l.countP p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      by_cases h : p a
      · simp [h, ih]
      · have hf : p a = false := Bool.eq_false_of_ne_true h
        simp [hf, ih]

theorem CertifiedWinners_sound :
    CertifiedWinners.Goal := by
  intro xs slots k0
  dsimp
  intro hk
  let st := mgAux slots {} 0 0 xs
  have hinv0 : MGInv slots [] ({}, 0, 0) := by
    dsimp [MGInv]
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro v
      simp
    · intro v hv
      simp at hv
    · simp [empty_csum']
    · simp
    · intro v
      simp
  have hfinal := mgAux_inv slots [] {} 0 hinv0 xs
  dsimp [MGInv] at hfinal
  rcases hfinal with ⟨hcnt, hpos, hcsum, hsize, hlower⟩
  have hfst : st.1 = TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots := by
    dsimp [st]
    rw [mgAux_fst, ← candidates_eq_mg]
  have hlen : st.2.2 = xs.length := by
    dsimp [st]
    simpa using mgAux_len slots {} 0 0 xs
  have hcsumS : csum' st.1 + st.2.1 * (slots + 1) = st.2.2 := by simpa [st] using hcsum
  rw [hlen] at hcsumS
  have hcntS : ∀ v, xs.count v ≤ st.1.getD v 0 + st.2.1 := by
    simpa [st] using hcnt
  have hlowerS : ∀ v, st.1.getD v 0 ≤ xs.count v := by
    simpa [st] using hlower
  have hposS : ∀ v, st.1.contains v = true → 0 < st.1.getD v 0 := by
    simpa [st] using hpos
  have hdiv : (xs.length - csum' st.1) / (slots + 1) = st.2.1 := by
    have hc : csum' st.1 + st.2.1 * (slots + 1) = xs.length := hcsumS
    have hsub : xs.length - csum' st.1 = st.2.1 * (slots + 1) := by omega
    rw [hsub, Nat.mul_comm]
    exact Nat.mul_div_right st.2.1 (by omega : 0 < slots + 1)
  have hdebt : debt xs slots = st.2.1 := by
    unfold debt
    rw [← hfst]
    exact hdiv
  rw [hdebt] at hk
  let d := st.2.1
  let good : Nat → Bool := fun g => decide (d < xs.count g)
  let A := st.1.toList.map Prod.fst
  let B := xs.eraseDups
  have hgood_mem : ∀ g, good g = true → g ∈ A := by
    intro g hg
    by_cases hc : st.1.contains g
    · exact mem_candidate_keys.mpr hc
    · have hget : st.1.getD g 0 = 0 := by
        have hcf : st.1.contains g = false := by by_cases c : st.1.contains g <;> simp_all
        exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
      have hb := hcntS g
      rw [hget] at hb
      dsimp [good] at hg
      simp at hg
      omega
  have hA_sub_B : ∀ g ∈ A, g ∈ B := by
    intro g hg
    have hc : st.1.contains g = true := mem_candidate_keys.mp hg
    have hp := hposS g hc
    have hl := hlowerS g
    have hcountpos : 0 < xs.count g := by omega
    dsimp [B]
    exact List.mem_eraseDups.mpr (List.count_pos_iff.mp hcountpos)
  have hA_nd : A.Nodup := by
    dsimp [A]
    have hnd := Std.HashMap.distinct_keys_toList (m := st.1)
    exact List.Pairwise.map Prod.fst (by
      intro p q hpq hpqfst
      have hpqne : p.fst ≠ q.fst := by
        intro heq
        have ht : (p.fst == q.fst) = true := by simp [heq]
        rw [hpq] at ht
        contradiction
      exact hpqne hpqfst) hnd
  have hB_nd : B.Nodup := by
    dsimp [B]
    exact nodup_eraseDups_nat xs
  let GA := A.filter good
  let GB := B.filter good
  let BA := A.filter (fun g => !good g)
  let BB := B.filter (fun g => !good g)
  have hmemG : ∀ g, g ∈ GA ↔ g ∈ GB := by
    intro g
    dsimp [GA, GB]
    simp only [List.mem_filter]
    constructor
    · rintro ⟨hga, hg⟩
      exact ⟨hA_sub_B g hga, hg⟩
    · rintro ⟨hgb, hg⟩
      exact ⟨hgood_mem g hg, hg⟩
  have hGA_nd : GA.Nodup := by
    dsimp [GA]
    exact (List.filter_sublist (p := good) (l := A)).nodup hA_nd
  have hGB_nd : GB.Nodup := by
    dsimp [GB]
    exact (List.filter_sublist (p := good) (l := B)).nodup hB_nd
  have hGAPermGB : GA.Perm GB := perm_of_nodup_mem hGA_nd hGB_nd hmemG
  have hGAranked : ranked xs GA = ranked xs GB := by
    apply sorted_perm_eq (fun a => cmpRows_refl xs a) (fun {a b} hab hba => cmpRows_antisymm xs hab hba)
    · exact (ranked_perm xs GA).nodup_iff.mpr hGA_nd
    · exact (ranked_perm xs GB).nodup_iff.mpr hGB_nd
    · exact ranked_sorted xs GA
    · exact ranked_sorted xs GB
    · exact (ranked_perm xs GA).trans (hGAPermGB.trans (ranked_perm xs GB).symm)
  have hsplitA : ranked xs A = ranked xs GA ++ ranked xs BA := by
    have hpart : (GA ++ BA).Perm A := by
      dsimp [GA, BA]
      exact filter_partition_perm good A
    have hcatPerm : (ranked xs GA ++ ranked xs BA).Perm A :=
      ((ranked_perm xs GA).append (ranked_perm xs BA)).trans hpart
    have hcatNd : (ranked xs GA ++ ranked xs BA).Nodup := hcatPerm.nodup_iff.mpr hA_nd
    have hcatSorted : (ranked xs GA ++ ranked xs BA).Pairwise (fun a b => cmpRows xs a b = true) := by
      apply pairwise_append (l₁ := ranked xs GA) (l₂ := ranked xs BA)
      · exact ranked_sorted xs GA
      · exact ranked_sorted xs BA
      · intro a ha b hb
        have haga : a ∈ GA := (ranked_perm xs GA).subset ha
        have hbba : b ∈ BA := (ranked_perm xs BA).subset hb
        have hagood : good a = true := (List.mem_filter.mp haga).2
        have hbbad : ¬ good b = true := by
          have hbneg := (List.mem_filter.mp hbba).2
          simpa using hbneg
        exact cmpRows_good_before_bad xs d a b (by simpa [good, d] using hagood) (by simpa [good] using hbbad)
    apply sorted_perm_eq (fun a => cmpRows_refl xs a) (fun {a b} hab hba => cmpRows_antisymm xs hab hba)
    · exact (ranked_perm xs A).nodup_iff.mpr hA_nd
    · exact hcatNd
    · exact ranked_sorted xs A
    · exact hcatSorted
    · exact (ranked_perm xs A).trans hcatPerm.symm
  have hsplitB : ranked xs B = ranked xs GB ++ ranked xs BB := by
    have hpart : (GB ++ BB).Perm B := by
      dsimp [GB, BB]
      exact filter_partition_perm good B
    have hcatPerm : (ranked xs GB ++ ranked xs BB).Perm B :=
      ((ranked_perm xs GB).append (ranked_perm xs BB)).trans hpart
    have hcatNd : (ranked xs GB ++ ranked xs BB).Nodup := hcatPerm.nodup_iff.mpr hB_nd
    have hcatSorted : (ranked xs GB ++ ranked xs BB).Pairwise (fun a b => cmpRows xs a b = true) := by
      apply pairwise_append (l₁ := ranked xs GB) (l₂ := ranked xs BB)
      · exact ranked_sorted xs GB
      · exact ranked_sorted xs BB
      · intro a ha b hb
        have hagb : a ∈ GB := (ranked_perm xs GB).subset ha
        have hbbb : b ∈ BB := (ranked_perm xs BB).subset hb
        have hagood : good a = true := (List.mem_filter.mp hagb).2
        have hbbad : ¬ good b = true := by
          have hbneg := (List.mem_filter.mp hbbb).2
          simpa using hbneg
        exact cmpRows_good_before_bad xs d a b (by simpa [good, d] using hagood) (by simpa [good] using hbbad)
    apply sorted_perm_eq (fun a => cmpRows_refl xs a) (fun {a b} hab hba => cmpRows_antisymm xs hab hba)
    · exact (ranked_perm xs B).nodup_iff.mpr hB_nd
    · exact hcatNd
    · exact ranked_sorted xs B
    · exact hcatSorted
    · exact (ranked_perm xs B).trans hcatPerm.symm
  have hkGA : k0 ≤ (ranked xs GA).length := by
    have hlen1 : (ranked xs GA).length = GA.length := (ranked_perm xs GA).length_eq
    have hlen2 : GA.length = A.countP good := by dsimp [GA]; exact length_filter_eq_countP A good
    rw [hlen1, hlen2]
    dsimp [A, good]
    simpa [← hfst] using hk
  have hkGB : k0 ≤ (ranked xs GB).length := by
    rw [← hGAranked]
    exact hkGA
  rw [← hfst]
  change (ranked xs A).take k0 = (ranked xs B).take k0
  rw [hsplitA, hsplitB, hGAranked]
  rw [List.take_append_of_le_length hkGB, List.take_append_of_le_length hkGB]


end TinylambOpt.ResearchProofs.DuckDBGaps.Hits
