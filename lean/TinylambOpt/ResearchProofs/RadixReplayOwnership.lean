import TinylambOpt.ResearchGoals.RadixReplayOwnership

namespace TinylambOpt.ResearchProofs.RadixReplayOwnership
open ResearchGoals.RadixReplayOwnership
open Research.AdaptiveRadixReplay
open List

/-! ## Consumed-id bookkeeping -/

private theorem consumedIds_append (xs ys : List Event) :
    consumedIds (xs ++ ys) = consumedIds xs ++ consumedIds ys := by
  unfold consumedIds
  rw [List.filter_append, List.flatMap_append]

private theorem consumed_append (ev : Event) (events : List Event) :
    consumedIds (events ++ [ev]) =
      consumedIds events ++ if ev.kind == EKind.spill then [] else ev.ids := by
  obtain ⟨t, bk, ids, kind⟩ := ev
  rcases kind with _|_|_|_
  · unfold consumedIds
    rw [List.filter_append,
      show (filter (fun e => e.kind != EKind.spill) ([⟨t, bk, ids, EKind.part⟩] : List Event)) =
        [⟨t, bk, ids, EKind.part⟩] from rfl]
    rw [show (if (⟨t, bk, ids, EKind.part⟩ : Event).kind == EKind.spill then []
        else (⟨t, bk, ids, EKind.part⟩ : Event).ids) = ids from rfl]
    rw [List.flatMap_append, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  · unfold consumedIds
    rw [List.filter_append,
      show (filter (fun e => e.kind != EKind.spill) ([⟨t, bk, ids, EKind.sealed⟩] : List Event)) =
        [⟨t, bk, ids, EKind.sealed⟩] from rfl]
    rw [show (if (⟨t, bk, ids, EKind.sealed⟩ : Event).kind == EKind.spill then []
        else (⟨t, bk, ids, EKind.sealed⟩ : Event).ids) = ids from rfl]
    rw [List.flatMap_append, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  · unfold consumedIds
    rw [List.filter_append,
      show (filter (fun e => e.kind != EKind.spill) ([⟨t, bk, ids, EKind.replay⟩] : List Event)) =
        [⟨t, bk, ids, EKind.replay⟩] from rfl]
    rw [show (if (⟨t, bk, ids, EKind.replay⟩ : Event).kind == EKind.spill then []
        else (⟨t, bk, ids, EKind.replay⟩ : Event).ids) = ids from rfl]
    rw [List.flatMap_append, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  · unfold consumedIds
    rw [List.filter_append,
      show (filter (fun e => e.kind != EKind.spill) ([⟨t, bk, ids, EKind.spill⟩] : List Event)) =
        [] from rfl]
    rw [show (if (⟨t, bk, ids, EKind.spill⟩ : Event).kind == EKind.spill then []
        else (⟨t, bk, ids, EKind.spill⟩ : Event).ids) = ([] : List Nat) from rfl]
    rw [List.flatMap_append, List.flatMap_nil, List.append_nil]

private theorem consumed_spill (events : List Event) (ev : Event)
    (h : ev.kind = EKind.spill) : consumedIds (events ++ [ev]) = consumedIds events := by
  rw [consumed_append, h]
  rw [if_pos (show (EKind.spill == EKind.spill) = true from rfl)]
  exact List.append_nil _

private theorem consumed_part (events : List Event) (b : Batch) (rows : List Occ) :
    consumedIds (events ++ [record b EKind.part rows]) = consumedIds events ++ occIds rows := by
  rw [consumed_append]
  have h1 : ((record b EKind.part rows).kind == EKind.spill) = false := rfl
  have h2 : (record b EKind.part rows).ids = occIds rows := rfl
  rw [h1, if_neg (by decide), h2]

private theorem occIds_take_append (rows : List Occ) (budget : Nat) :
    occIds (rows.take budget) ++ occIds (rows.drop budget) = occIds rows := by
  unfold occIds
  rw [← List.map_append, List.take_append_drop]

private theorem consumedIds_map_record (live : List Batch) :
    consumedIds (live.map fun b => record b EKind.sealed b.pending) =
      live.flatMap (fun b => occIds b.pending) := by
  unfold consumedIds
  have hall : (live.map (fun b => record b EKind.sealed b.pending)).filter (fun e => e.kind != EKind.spill) = live.map (fun b => record b EKind.sealed b.pending) := by
    refine List.filter_eq_self.mpr (fun e he => ?_)
    simp only [List.mem_map] at he
    obtain ⟨b, _, rfl⟩ := he
    show (EKind.sealed != EKind.spill) = true
    decide
  rw [hall, List.flatMap_map]
  exact rfl

private theorem consumedIds_finish (s : State) :
    consumedIds (finish s) =
      consumedIds s.events ++ s.live.flatMap (fun b => occIds b.pending) := by
  unfold finish
  rw [consumedIds_append, consumedIds_map_record]

private theorem mem_of_mem_drop {α : Type} (l : List α) :
    ∀ (n : Nat) (x : α), x ∈ l.drop n → x ∈ l := by
  induction l with
  | nil => intro n x hx; simp at hx
  | cons a as ih =>
      intro n x hx
      cases n with
      | zero => exact hx
      | succ n => exact List.Mem.tail a (ih n x hx)

private theorem mem_of_mem_filter {α : Type} (p : α → Bool) {l : List α} {x}
    (h : x ∈ l.filter p) : x ∈ l := (List.mem_filter.mp h).1

private theorem nodup_drop_middle {m₁ m₂ : List Nat} {t : Nat}
    (h : Nodup (m₁ ++ t :: m₂)) : Nodup (m₁ ++ m₂) := by
  induction m₁ with
  | nil => exact h.tail
  | cons x xs ih =>
      have h1 : Nodup (x :: (xs ++ t :: m₂)) := h
      have hnm : x ∉ xs ++ t :: m₂ := (List.nodup_cons.mp h1).1
      have h2 : Nodup (xs ++ t :: m₂) := h1.tail
      refine List.Pairwise.cons ?_ (ih h2)
      intro a' ha' he
      subst he
      rcases List.mem_append.mp ha' with h | h
      · exact hnm (List.mem_append.mpr (Or.inl h))
      · exact hnm (List.mem_append.mpr (Or.inr (List.mem_cons_of_mem t h)))

private theorem perm_map {α β : Type} (f : α → β) {l₁ l₂ : List α}
    (h : l₁ ~ l₂) : l₁.map f ~ l₂.map f := by
  induction h with
  | nil => exact List.Perm.refl _
  | @cons x t₁ t₂ h' ih =>
      rw [List.map_cons, List.map_cons]
      exact List.Perm.cons _ ih
  | @swap a b l =>
      rw [List.map_cons, List.map_cons]
      exact List.Perm.swap _ _ _
  | @trans t₁ t₂ t₃ h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem token_ne_of_mem_tail {l₁ l₂ : List Batch} {b : Batch} (t : Nat)
    (hnd : Nodup ((l₁ ++ b :: l₂).map Batch.token)) (hbT : b.token = t)
    (a : Batch) (ha : a ∈ l₂) : a.token ≠ t := by
  rw [List.map_append, List.map_cons] at hnd
  obtain ⟨_, htm, _⟩ := List.nodup_append.mp hnd
  obtain ⟨hnotin, _⟩ := List.nodup_cons.mp htm
  intro hcon
  apply hnotin
  rw [hbT, ← hcon]
  exact List.mem_map.mpr ⟨a, ha, rfl⟩

/-! ## Bucket prefix arithmetic: `testBit` vs `· % 2 ^ ·` -/

private theorem two_pow_succ (d : Nat) : 2 ^ (d + 1) = 2 ^ d * 2 := Nat.pow_succ 2 d

private theorem div_two_pow_aux (k e : Nat) (he : 0 < e) :
    k / e = 2 * (k / (e * 2)) + k % (e * 2) / e := by
  have h := Nat.div_add_mod k (e * 2)
  rw [Nat.mul_assoc] at h
  calc k / e = (k % (e * 2) + e * (2 * (k / (e * 2)))) / e := by
        congr 1
        rw [Nat.add_comm]
        exact h.symm
      _ = k % (e * 2) / e + 2 * (k / (e * 2)) := Nat.add_mul_div_left _ _ he
      _ = 2 * (k / (e * 2)) + k % (e * 2) / e := Nat.add_comm _ _

private theorem div_two_pow (k d : Nat) :
    k / 2 ^ d = 2 * (k / 2 ^ (d + 1)) + k % 2 ^ (d + 1) / 2 ^ d := by
  have hpos : 0 < 2 ^ d := Nat.pow_pos (by omega)
  have h := div_two_pow_aux k (2 ^ d) hpos
  rw [← two_pow_succ d] at h
  exact h

private theorem mod_two_pow_testBit (k d : Nat) :
    k % 2 ^ (d + 1) = k % 2 ^ d + if k.testBit d then 2 ^ d else 0 := by
  have hpos : 0 < 2 ^ d := Nat.pow_pos (by omega)
  have hpos' : 0 < 2 ^ (d + 1) := Nat.pow_pos (by omega)
  have hq2 : k % 2 ^ (d + 1) / 2 ^ d < 2 := by
    have hlt : k % 2 ^ (d + 1) < 2 ^ d * 2 := by
      rw [← two_pow_succ d]
      exact Nat.mod_lt _ hpos'
    exact Nat.div_lt_of_lt_mul hlt
  have hmod : k % 2 ^ d = k % 2 ^ (d + 1) % 2 ^ d :=
    (Nat.mod_mod_of_dvd k (⟨2, two_pow_succ d⟩ : 2 ^ d ∣ 2 ^ (d + 1))).symm
  have hdiv := div_two_pow k d
  have hparity : k / 2 ^ d % 2 = (k % 2 ^ (d + 1) / 2 ^ d) % 2 := by
    rw [hdiv, Nat.add_comm, Nat.add_mul_mod_self_left]
  have hbit : k.testBit d = decide (k / 2 ^ d % 2 = 1) :=
    Nat.testBit_eq_decide_div_mod_eq
  rcases Nat.eq_zero_or_pos (k % 2 ^ (d + 1) / 2 ^ d) with hq | hq
  · -- quotient is 0: bit unset, lower bits are the whole remainder
    have hlt : k % 2 ^ (d + 1) < 2 ^ d := by
      rcases Nat.div_eq_zero_iff.mp hq with h0 | hlt
      · exact absurd h0 (by omega)
      · exact hlt
    have hlow : k % 2 ^ d = k % 2 ^ (d + 1) := by
      rw [hmod, Nat.mod_eq_of_lt hlt]
    have hfalse : k.testBit d = false := by
      rw [hbit, hparity, hq]
      decide
    rw [if_neg (by rw [hfalse]; decide), hlow]
    omega
  · -- quotient is 1: bit set, add 2^d
    have hq1 : k % 2 ^ (d + 1) / 2 ^ d = 1 := by omega
    have htrue : k.testBit d = true := by
      rw [hbit, hparity, hq1]
      decide
    have hdecomp : 2 ^ d * (k % 2 ^ (d + 1) / 2 ^ d) + k % 2 ^ (d + 1) % 2 ^ d
        = k % 2 ^ (d + 1) := Nat.div_add_mod _ _
    rw [if_pos htrue, ← hdecomp, hq1, Nat.mul_one, Nat.add_comm (2 ^ d), ← hmod]

/-! ## Step equations -/

private theorem step_drain (s : State) (t budget : Nat) (b : Batch)
    (hb : s.live.find? (fun b => b.token == t) = some b) :
    step s (.drain t budget) =
      ⟨s.fresh + 1,
        if (b.pending.drop budget).isEmpty then s.live.filter fun j => j.token != t
        else { b with token := s.fresh, pending := b.pending.drop budget } ::
             s.live.filter fun j => j.token != t,
        s.events ++ [record b EKind.part (b.pending.take budget)]⟩ := by
  unfold step
  simp [hb]

private theorem step_drain_nil (s : State) (t budget : Nat) (b : Batch)
    (hb : s.live.find? (fun b => b.token == t) = some b)
    (hdp : b.pending.drop budget = []) :
    step s (.drain t budget) =
      ⟨s.fresh + 1, s.live.filter fun j => j.token != t,
        s.events ++ [record b EKind.part (b.pending.take budget)]⟩ := by
  rw [step_drain s t budget b hb, hdp, List.isEmpty_nil]
  rw [if_pos rfl]

private theorem step_drain_cons (s : State) (t budget : Nat) (b : Batch) (o : Occ) (os : List Occ)
    (hb : s.live.find? (fun b => b.token == t) = some b)
    (hdp : b.pending.drop budget = o :: os) :
    step s (.drain t budget) =
      ⟨s.fresh + 1, { b with token := s.fresh, pending := o :: os } ::
        s.live.filter fun j => j.token != t,
        s.events ++ [record b EKind.part (b.pending.take budget)]⟩ := by
  rw [step_drain s t budget b hb, hdp, List.isEmpty_cons]
  rw [if_neg (by decide)]

private theorem step_split (s : State) (t : Nat) (b : Batch)
    (hb : s.live.find? (fun b => b.token == t) = some b) :
    step s (.split t) =
      ⟨s.fresh + 2,
        ⟨s.fresh, b.depth + 1, b.bucket,
          (b.pending.partition (fun o => o.key.testBit b.depth)).2⟩ ::
        ⟨s.fresh + 1, b.depth + 1, b.bucket + 2 ^ b.depth,
          (b.pending.partition (fun o => o.key.testBit b.depth)).1⟩ ::
        s.live.filter fun j => j.token != t,
        s.events ++ [record b EKind.spill b.pending]⟩ := by
  unfold step
  simp [hb]

private theorem step_drain_none (s : State) (t budget : Nat)
    (hb : s.live.find? (fun b => b.token == t) = none) : step s (.drain t budget) = s := by
  unfold step
  simp [hb]

private theorem step_split_none (s : State) (t : Nat)
    (hb : s.live.find? (fun b => b.token == t) = none) : step s (.split t) = s := by
  unfold step
  simp [hb]

/-! ## Invariant -/

private def Inv (rows : List Occ) (s : State) : Prop :=
  (consumedIds s.events ++ s.live.flatMap (fun b => occIds b.pending)).Perm (occIds rows) ∧
  (s.live.map Batch.token).Nodup ∧
  (∀ b ∈ s.live, b.token < s.fresh) ∧
  (∀ b ∈ s.live, ∀ o ∈ b.pending, o.key % 2 ^ b.depth = b.bucket)

private theorem start_inv (rows : List Occ) : Inv rows (start rows) := by
  unfold start Inv
  refine ⟨?_, ?_, ?_, ?_⟩
  · simpa [consumedIds] using (List.Perm.refl (occIds rows))
  · simpa using (List.nodup_singleton (0 : Nat))
  · intro b hb
    have hb2 : b = ⟨0, 0, 0, rows⟩ := by simpa using hb
    subst hb2
    exact Nat.zero_lt_one
  · intro b hb o ho
    have hb2 : b = ⟨0, 0, 0, rows⟩ := by simpa using hb
    subst hb2
    show o.key % 2 ^ 0 = 0
    rw [Nat.pow_zero, Nat.mod_one]

private theorem inv_drain {rows : List Occ} {s : State} {t budget : Nat} {b : Batch}
    (hinv : Inv rows s) (hb : s.live.find? (fun b => b.token == t) = some b) :
    Inv rows (step s (.drain t budget)) := by
  obtain ⟨hperm, hnd, hlt, hbuck⟩ := hinv
  obtain ⟨hpb, l₁, l₂, hlive, hpre⟩ := List.find?_eq_some_iff_append.mp hb
  have hbt : b.token = t := by simpa using hpb
  have hnd2 : Nodup ((l₁ ++ b :: l₂).map Batch.token) := by rw [← hlive]; exact hnd
  -- the filtered list is exactly the two halves
  have hfilt : s.live.filter (fun j => j.token != t) = l₁ ++ l₂ := by
    rw [hlive, List.filter_append]
    congr 1
    · refine List.filter_eq_self.mpr (fun a ha => ?_)
      simpa using hpre a ha
    · rw [List.filter_cons]
      have hc : (b.token != t) = false := by simp [hbt]
      rw [hc]
      rw [if_neg (by decide)]
      refine List.filter_eq_self.mpr (fun a ha => ?_)
      simpa using token_ne_of_mem_tail t hnd2 hbt a ha
  have hbmb : b ∈ s.live := by rw [hlive]; simp
  have hflat : s.live.flatMap (fun x => occIds x.pending) ~
      occIds b.pending ++ (s.live.filter (fun j => j.token != t)).flatMap (fun x => occIds x.pending) := by
    rw [hlive]
    rw [show (l₁ ++ b :: l₂).filter (fun j => j.token != t) = l₁ ++ l₂ from by
      rw [← hlive]; exact hfilt]
    simp only [List.flatMap_append, List.flatMap_cons]
    exact ((List.Perm.of_eq (List.append_assoc _ _ _)).symm.trans
        (List.perm_append_comm.append_right _)).trans (List.Perm.of_eq (List.append_assoc _ _ _))
  have hnd' : Nodup ((s.live.filter (fun j => j.token != t)).map Batch.token) := by
    rw [hfilt, List.map_append]
    have h2 : Nodup ((l₁ ++ b :: l₂).map Batch.token) := by rw [← hlive]; exact hnd
    rw [List.map_append, List.map_cons, hbt] at h2
    exact nodup_drop_middle h2
  have hlt' : ∀ b' ∈ s.live.filter (fun j => j.token != t), b'.token < s.fresh := by
    intro b' hb'
    exact hlt b' (mem_of_mem_filter _ hb')
  have hbuck' : ∀ b' ∈ s.live.filter (fun j => j.token != t),
      ∀ o ∈ b'.pending, o.key % 2 ^ b'.depth = b'.bucket := by
    intro b' hb'
    exact hbuck b' (mem_of_mem_filter _ hb')
  cases hdp : b.pending.drop budget with
  | nil =>
    have htake : b.pending.take budget = b.pending := by
      have h2 := List.take_append_drop budget b.pending
      rw [hdp, List.append_nil] at h2
      exact h2
    rw [step_drain_nil s t budget b hb hdp]
    refine ⟨?_, hnd', ?_, ?_⟩
    · rw [consumed_part, htake, List.append_assoc]
      exact (List.Perm.append_left _ hflat.symm).trans hperm
    · intro b' hb'
      exact Nat.lt_succ_of_lt (hlt' b' hb')
    · intro b' hb' o ho
      exact hbuck' b' hb' o ho
  | cons o os =>
    rw [step_drain_cons s t budget b o os hb hdp]
    refine ⟨?_, ?_, ?_, ?_⟩
    · rw [consumed_part]
      simp only [List.flatMap_cons]
      rw [← hdp]
      rw [← List.append_assoc (consumedIds s.events) (occIds (b.pending.take budget))
        (occIds (b.pending.drop budget)
          ++ (s.live.filter (fun j => j.token != t)).flatMap (fun x => occIds x.pending)) |>.symm]
      rw [← List.append_assoc (occIds (b.pending.take budget)) (occIds (b.pending.drop budget))
        ((s.live.filter (fun j => j.token != t)).flatMap (fun x => occIds x.pending))]
      rw [occIds_take_append]
      exact (List.Perm.append_left _ hflat.symm).trans hperm
    · rw [List.map_cons]
      refine List.Pairwise.cons ?_ hnd'
      intro n hn hcon
      obtain ⟨a, ha, hEq⟩ := List.mem_map.mp hn
      dsimp only at hcon hEq ⊢
      subst hEq
      have := hlt' a ha
      omega
    · intro b' hb'
      rcases List.mem_cons.mp hb' with rfl | hb'
      · show s.fresh < s.fresh + 1
        omega
      · exact Nat.lt_succ_of_lt (hlt' b' hb')
    · intro b' hb' o' ho'
      rcases List.mem_cons.mp hb' with rfl | hb'
      · dsimp only
        show o'.key % 2 ^ b.depth = b.bucket
        rw [← hdp] at ho'
        exact hbuck b hbmb o' (mem_of_mem_drop b.pending budget o' ho')
      · exact hbuck' b' hb' o' ho'

private theorem inv_split {rows : List Occ} {s : State} {t : Nat} {b : Batch}
    (hinv : Inv rows s) (hb : s.live.find? (fun b => b.token == t) = some b) :
    Inv rows (step s (.split t)) := by
  obtain ⟨hperm, hnd, hlt, hbuck⟩ := hinv
  obtain ⟨hpb, l₁, l₂, hlive, hpre⟩ := List.find?_eq_some_iff_append.mp hb
  have hbt : b.token = t := by simpa using hpb
  have hnd2 : Nodup ((l₁ ++ b :: l₂).map Batch.token) := by rw [← hlive]; exact hnd
  have hfilt : s.live.filter (fun j => j.token != t) = l₁ ++ l₂ := by
    rw [hlive, List.filter_append]
    congr 1
    · refine List.filter_eq_self.mpr (fun a ha => ?_)
      simpa using hpre a ha
    · rw [List.filter_cons]
      have hc : (b.token != t) = false := by simp [hbt]
      rw [hc]
      rw [if_neg (by decide)]
      refine List.filter_eq_self.mpr (fun a ha => ?_)
      simpa using token_ne_of_mem_tail t hnd2 hbt a ha
  have hbmb : b ∈ s.live := by rw [hlive]; simp
  let p : Occ → Bool := fun o => o.key.testBit b.depth
  have hnp : (not ∘ p) = (fun x => !p x) := rfl
  have hp1 : (b.pending.partition p).1 = b.pending.filter p := by
    rw [List.partition_eq_filter_filter]
  have hp2 : (b.pending.partition p).2 = b.pending.filter (fun x => !p x) := by
    rw [List.partition_eq_filter_filter, hnp]
  have hpart : occIds (b.pending.partition p).2 ++ occIds (b.pending.partition p).1 ~
      occIds b.pending := by
    rw [hp1, hp2]
    unfold occIds
    rw [← List.map_append]
    exact perm_map Occ.id
      ((List.perm_append_comm (l₁ := b.pending.filter (fun x => !p x))
          (l₂ := b.pending.filter p)).trans (List.filter_append_perm p b.pending))
  have hflat : s.live.flatMap (fun x => occIds x.pending) ~
      occIds (b.pending.partition p).2 ++ occIds (b.pending.partition p).1 ++
      (s.live.filter (fun j => j.token != t)).flatMap (fun x => occIds x.pending) := by
    rw [hlive]
    rw [show (l₁ ++ b :: l₂).filter (fun j => j.token != t) = l₁ ++ l₂ from by
      rw [← hlive]; exact hfilt]
    show (l₁ ++ b :: l₂).flatMap (fun x => occIds x.pending) ~
      occIds (b.pending.partition p).2 ++ occIds (b.pending.partition p).1 ++
        (l₁ ++ l₂).flatMap (fun x => occIds x.pending)
    simp only [List.flatMap_append, List.flatMap_cons]
    rw [← List.append_assoc]
    refine ((List.perm_append_comm
        (l₁ := l₁.flatMap (fun x => occIds x.pending)) (l₂ := occIds b.pending)
      ).append_right _).trans ?_
    refine ((List.Perm.append hpart.symm (List.Perm.refl _)).append_right _).trans ?_
    exact List.Perm.of_eq (List.append_assoc
      (occIds (b.pending.partition p).2 ++ occIds (b.pending.partition p).1)
      (l₁.flatMap (fun x => occIds x.pending)) (l₂.flatMap (fun x => occIds x.pending)))
  have hnd' : Nodup ((s.live.filter (fun j => j.token != t)).map Batch.token) := by
    rw [hfilt, List.map_append]
    have h2 : Nodup ((l₁ ++ b :: l₂).map Batch.token) := by rw [← hlive]; exact hnd
    rw [List.map_append, List.map_cons, hbt] at h2
    exact nodup_drop_middle h2
  have hlt' : ∀ b' ∈ s.live.filter (fun j => j.token != t), b'.token < s.fresh := by
    intro b' hb'
    exact hlt b' (mem_of_mem_filter _ hb')
  have hbuck' : ∀ b' ∈ s.live.filter (fun j => j.token != t),
      ∀ o ∈ b'.pending, o.key % 2 ^ b'.depth = b'.bucket := by
    intro b' hb'
    exact hbuck b' (mem_of_mem_filter _ hb')
  rw [step_split s t b hb]
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- the spill event consumes nothing
    rw [consumed_spill s.events (record b EKind.spill b.pending) rfl]
    simp only [List.flatMap_cons]
    rw [← List.append_assoc (occIds (b.pending.partition p).2) (occIds (b.pending.partition p).1)
      ((s.live.filter (fun j => j.token != t)).flatMap (fun x => occIds x.pending))]
    exact (List.Perm.append_left _ hflat.symm).trans hperm
  · rw [List.map_cons, List.map_cons]
    refine List.Pairwise.cons ?_ (List.Pairwise.cons ?_ hnd')
    · intro n hn hcon
      dsimp only at hcon hn ⊢
      rcases List.mem_cons.mp hn with heq | hmap
      · subst heq; omega
      · obtain ⟨a, ha, hEq⟩ := List.mem_map.mp hmap
        subst hEq
        have := hlt' a ha
        omega
    · intro n hn hcon
      obtain ⟨a, ha, hEq⟩ := List.mem_map.mp hn
      dsimp only at hcon hEq ⊢
      subst hEq
      have := hlt' a ha
      omega
  · intro b' hb'
    rcases List.mem_cons.mp hb' with rfl | hb'
    · show s.fresh < s.fresh + 2
      omega
    · rcases List.mem_cons.mp hb' with rfl | hb'
      · show s.fresh + 1 < s.fresh + 2
        omega
      · exact Nat.lt_succ_of_lt (Nat.lt_succ_of_lt (hlt' b' hb'))
  · intro b' hb' o ho
    rcases List.mem_cons.mp hb' with rfl | hb'
    · -- left child: bit unset
      have hmem : o ∈ b.pending.filter (fun x => !p x) := by rw [← hp2]; exact ho
      obtain ⟨hmb, hpo⟩ := List.mem_filter.mp hmem
      have hbit : p o = false := by simpa [p] using hpo
      have hbase := hbuck b hbmb o hmb
      dsimp only
      show o.key % 2 ^ (b.depth + 1) = b.bucket
      have hn : ¬ (o.key.testBit b.depth = true) := by
        simpa [p] using hbit
      rw [mod_two_pow_testBit, if_neg hn, Nat.add_zero, hbase]
    · rcases List.mem_cons.mp hb' with rfl | hb'
      · -- right child: bit set
        have hmem : o ∈ b.pending.filter p := by rw [← hp1]; exact ho
        obtain ⟨hmb, hpo⟩ := List.mem_filter.mp hmem
        have hbit : p o = true := by simpa [p] using hpo
        have hbase := hbuck b hbmb o hmb
        dsimp only
        show o.key % 2 ^ (b.depth + 1) = b.bucket + 2 ^ b.depth
        rw [mod_two_pow_testBit, if_pos hbit, hbase]
      · exact hbuck' b' hb' o ho

private theorem step_inv {rows : List Occ} {s : State} (cmd : Cmd)
    (hinv : Inv rows s) : Inv rows (step s cmd) := by
  cases cmd with
  | drain t budget =>
      by_cases hb : (s.live.find? fun b => b.token == t) = none
      · rw [step_drain_none s t budget hb]
        exact hinv
      · have hb' : ∃ b, s.live.find? (fun b => b.token == t) = some b := by
          rcases h : s.live.find? (fun b => b.token == t) with _|b
          · contradiction
          · exact ⟨b, h⟩
        obtain ⟨b, hb⟩ := hb'
        exact inv_drain hinv hb
  | split t =>
      by_cases hb : (s.live.find? fun b => b.token == t) = none
      · rw [step_split_none s t hb]
        exact hinv
      · have hb' : ∃ b, s.live.find? (fun b => b.token == t) = some b := by
          rcases h : s.live.find? (fun b => b.token == t) with _|b
          · contradiction
          · exact ⟨b, h⟩
        obtain ⟨b, hb⟩ := hb'
        exact inv_split hinv hb

private theorem fold_inv (rows : List Occ) :
    ∀ (cmds : List Cmd) (s : State), Inv rows s → Inv rows (cmds.foldl step s) := by
  intro cmds
  induction cmds with
  | nil => exact fun s hs => hs
  | cons c cs ih => exact fun s hs => ih (step s c) (step_inv c hs)

private theorem inv_goal {rows : List Occ} (hnd : (occIds rows).Nodup) (s : State)
    (hinv : Inv rows s) :
    ((consumedIds s.events ++ s.live.flatMap (fun b => occIds b.pending)).Perm (occIds rows)) ∧
    ((s.live.map Batch.token).Nodup) ∧
    (∀ b ∈ s.live, b.token < s.fresh ∧ ∀ o ∈ b.pending, o.key % 2 ^ b.depth = b.bucket) ∧
    (∀ o ∈ rows, consumptionCount (finish s) o.id = 1) ∧
    (∀ k, optAgg rows (finish s) k = refAgg rows k) := by
  obtain ⟨hperm, hnd', hlt, hbuck⟩ := hinv
  refine ⟨hperm, hnd', fun b hb => ⟨hlt b hb, hbuck b hb⟩, ?_, ?_⟩
  · intro o ho
    unfold consumptionCount
    rw [consumedIds_finish]
    have hc := hperm.count o.id
    rw [count_eq_one_of_mem_nodup hnd (List.mem_map_of_mem (f := Occ.id) ho)] at hc
    exact hc
  · intro k
    unfold optAgg
    rw [runEvents_eq_fold, consumedIds_finish]
    rw [perm_fold_comm (stepK rows k) (stepK_comm rows k) hperm (⟨0, 0, 0⟩ : Agg)]
    exact fold_stepK_occIds rows hnd k (⟨0, 0, 0⟩ : Agg)

theorem sound : Goal := by
  intro rows cmds hnd
  have hs : Inv rows (cmds.foldl step (start rows)) :=
    fold_inv rows cmds _ (start_inv rows)
  exact inv_goal hnd _ hs

end TinylambOpt.ResearchProofs.RadixReplayOwnership
