import TinylambOpt.ResearchGoals.Frontiers.DecisionTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.DecisionTables

open TinylambOpt
open TinylambOpt.ResearchGoals.Frontiers.DecisionTables

/-!
Decisions for `ResearchGoals.Frontiers.DecisionTables`.

Nine goals are proved; `NegatedConsumer` is false: FALSE and NULL agree in
WHERE but disagree after NOT (see the explicit counterexample below).
Common list infrastructure is developed once and reused by the proofs.
-/


private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List α), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length ?_ _ rfl)
            · have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
              have hba : b ≠ a := by
                simpa using hb'
              exact fun heq => hba heq.symm
            · rw [List.length_cons] at hxn
              have hlt : (as.filter fun b => !b == a).length ≤ as.length := List.length_filter_le _ _
              omega
  exact key l.length l rfl

private theorem eraseDups_map_length_le {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (l : List α) (f : α → β) : (l.map f).eraseDups.length ≤ l.eraseDups.length := by
  calc
    (l.map f).eraseDups.length ≤ ((l.eraseDups).map f).length := by
      exact List.Nodup.length_le_of_subset (l₁ := (l.map f).eraseDups) (l₂ := (l.eraseDups).map f)
        (nodup_eraseDups (l.map f)) (by
        intro x hx
        rcases List.mem_map.mp ((List.mem_eraseDups).mp hx) with ⟨a, ha, rfl⟩
        exact List.mem_map.mpr ⟨a, (List.mem_eraseDups).mpr ha, rfl⟩)
    _ = l.eraseDups.length := by simp

private theorem map_ext {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ a ∈ l, f a = g a) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons]
      rw [h a (by simp), ih (by intro b hb; exact h b (by simp [hb]))]

private theorem map_eq_on_mem {α β : Type} (l : List α) (f g : α → β) (t : α)
    (h : l.map f = l.map g) (ht : t ∈ l) : f t = g t := by
  induction l with
  | nil => cases ht
  | cons a as ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      rcases h with ⟨ha, has⟩
      rcases List.mem_cons.mp ht with rfl | ht
      · exact ha
      · exact ih has ht

private theorem length_eraseDups_le {α : Type} [BEq α] [LawfulBEq α] :
    ∀ l : List α, l.eraseDups.length ≤ l.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      have hf : (as.filter fun b => ! b == a).length ≤ as.length := List.length_filter_le _ _
      have her := length_eraseDups_le (as.filter fun b => ! b == a)
      omega
termination_by l => l.length
decreasing_by
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : α => ! b == a) as) (Nat.lt_succ_self as.length)

-- 1 SharedResidual
theorem sound_SharedResidual :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.SharedResidual.Goal := by
  intro qs heads tails h t hh ht
  unfold rep
  let p : Row → Bool := fun x => signature qs tails x == signature qs tails h
  have hp_self : p h = true := by simp [p]
  have hfind_ne : heads.find? p ≠ none := by
    intro hnone
    have hn := (List.find?_eq_none.mp hnone) h hh
    exact hn hp_self
  cases hf : heads.find? p with
  | none => exact False.elim (hfind_ne hf)
  | some x =>
      have hpx : p x = true := List.find?_some hf
      have heqSig : signature qs tails x = signature qs tails h := by
        exact (beq_iff_eq).mp hpx
      simp only [Option.map_some]
      congr 1
      exact map_eq_on_mem tails (fun t => answers qs (x++t)) (fun t => answers qs (h++t)) t heqSig ht

-- 2 StateLowerBound
private theorem map_ext_of_eq {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ a ∈ l, f a = g a) : l.map f = l.map g := by
  exact map_ext l f g h

theorem sound_StateLowerBound :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.StateLowerBound.Goal := by
  classical
  intro qs heads tails encode finish hfin
  let sig : Row → List (List Bool) := fun h => signature qs tails h
  let g : Nat → List (List Bool) := fun e =>
    if he : ∃ h ∈ heads, encode h = e then sig (Classical.choose he) else []
  have hg : ∀ h ∈ heads, g (encode h) = sig h := by
    intro h hh
    dsimp [g]
    have hex : ∃ h' ∈ heads, encode h' = encode h := ⟨h, hh, rfl⟩
    rw [dite_eq_left hex]
    let h0 := Classical.choose hex
    have hh0 : h0 ∈ heads := (Classical.choose_spec hex).1
    have henc : encode h0 = encode h := (Classical.choose_spec hex).2
    have hsig : sig h0 = sig h := by
      dsimp [sig, signature]
      apply map_ext
      intro t ht
      have h1 := hfin h0 hh0 t ht
      have h2 := hfin h hh t ht
      rw [← h1, ← h2, henc]
    exact hsig
  have hmap : heads.map sig = heads.map (fun h => g (encode h)) := by
    apply List.map_congr_left
    intro h hh
    exact (hg h hh).symm
  change (heads.map sig).eraseDups.length ≤ (heads.map encode).eraseDups.length
  rw [hmap]
  rw [show heads.map (fun h => g (encode h)) = (heads.map encode).map g by simp [List.map_map]]
  exact eraseDups_map_length_le (heads.map encode) g

-- 3 Coarsening
private def idxAux {α : Type} [BEq α] (a : α) : List α → Option Nat
  | [] => none
  | b :: bs => if b == a then some 0 else (idxAux a bs).map (fun i => i + 1)

private theorem idxAux_none_iff {α : Type} [BEq α] [LawfulBEq α] (a : α) :
    ∀ l, idxAux a l = none ↔ a ∉ l := by
  intro l
  induction l with
  | nil => simp [idxAux]
  | cons b bs ih =>
      by_cases hb : b == a
      · have hba : b = a := (beq_iff_eq).mp hb
        simp [idxAux, hba]
      · have hne : b ≠ a := by intro h; subst h; simp at hb
        constructor
        · intro hnone
          rw [idxAux, ite_eq_right hb] at hnone
          have hbs : idxAux a bs = none := by
            cases hx : idxAux a bs <;> simp [hx] at hnone ⊢
          have hnot := (ih).mp hbs
          intro ha
          have hab : a = b := by
            rcases List.mem_cons.mp ha with h | h
            · exact h
            · exact False.elim (hnot h)
          exact hne hab.symm
        · intro hnot
          rw [idxAux, ite_eq_right hb]
          have hbsnot : a ∉ bs := by intro h; exact hnot (by simp [h])
          rw [(ih).mpr hbsnot]
          simp

private theorem idxAux_getElem? {α : Type} [BEq α] [LawfulBEq α] (a : α) :
    ∀ l, a ∈ l → l[(idxAux a l).getD 0]? = some a := by
  intro l
  induction l with
  | nil => intro h; cases h
  | cons b bs ih =>
      intro hmem
      by_cases hb : b == a
      · have hba : b = a := (beq_iff_eq).mp hb
        rw [idxAux, ite_eq_left hb]
        simp [hba]
      · rw [idxAux, ite_eq_right hb]
        have hbs : a ∈ bs := by
          rcases List.mem_cons.mp hmem with rfl | hbs
          · exact False.elim (hb (by simp))
          · exact hbs
        have ih' := ih hbs
        cases hidx : idxAux a bs with
        | none =>
            exact False.elim (((idxAux_none_iff a bs).mp hidx) hbs)
        | some i =>
            rw [Option.map_some, Option.getD_some]
            rw [List.getElem?_cons_succ]
            simpa [hidx] using ih'

private theorem getElem?_map_of {α β : Type} (f : α → β) (l : List α) (i : Nat) (a : α)
    (h : l[i]? = some a) : (l.map f)[i]? = some (f a) := by
  rw [List.getElem?_map, h]
  rfl

theorem sound_Coarsening :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.Coarsening.Goal := by
  intro qs heads old new hnew
  let g : List (List Bool) → List (List Bool) := fun s =>
    new.map (fun t => s[(idxAux t old).getD 0]?.getD [])
  have hg : ∀ h, g (signature qs old h) = signature qs new h := by
    intro h
    dsimp [g, signature]
    apply map_ext
    intro t ht
    have hgot : old[(idxAux t old).getD 0]? = some t := idxAux_getElem? t old (hnew t ht)
    have hs := getElem?_map_of (fun t => answers qs (h++t)) old ((idxAux t old).getD 0) t hgot
    rw [hs]
    simp
  change (heads.map (signature qs new)).eraseDups.length ≤ (heads.map (signature qs old)).eraseDups.length
  have hmap : heads.map (signature qs new) = (heads.map (signature qs old)).map g := by
    rw [List.map_map]
    apply List.map_congr_left
    intro h hh
    exact (hg h).symm
  rw [hmap]
  exact eraseDups_map_length_le (heads.map (signature qs old)) g

-- 4 JointWidth
private def listProduct {α β : Type} (xs : List α) (ys : List β) : List (α × β) :=
  xs.flatMap (fun x => ys.map (fun y => (x, y)))

private theorem mem_listProduct {α β : Type} (xs : List α) (ys : List β) (a : α) (b : β) :
    (a, b) ∈ listProduct xs ys ↔ a ∈ xs ∧ b ∈ ys := by
  unfold listProduct
  simp

private theorem sum_const_length {α : Type} (xs : List α) (n : Nat) :
    (xs.map (fun _ => n)).sum = xs.length * n := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, List.length_cons, ih, Nat.succ_mul, Nat.add_comm]

private theorem length_listProduct {α β : Type} (xs : List α) (ys : List β) :
    (listProduct xs ys).length = xs.length * ys.length := by
  unfold listProduct
  simp [sum_const_length]

private theorem pair_distinct_le {α β γ : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ] (l : List γ) (f : γ → α) (gg : γ → β) :
    (l.map (fun x => (f x, gg x))).eraseDups.length ≤
      (l.map f).eraseDups.length * (l.map gg).eraseDups.length := by
  calc
    (l.map (fun x => (f x, gg x))).eraseDups.length ≤
        (listProduct (l.map f).eraseDups (l.map gg).eraseDups).length := by
      apply List.Nodup.length_le_of_subset (nodup_eraseDups (l.map (fun x => (f x, gg x))))
      intro p hp
      rcases List.mem_map.mp ((List.mem_eraseDups).mp hp) with ⟨x, hx, rfl⟩
      apply (mem_listProduct _ _ _ _).mpr
      constructor
      · exact (List.mem_eraseDups).mpr (List.mem_map.mpr ⟨x, hx, rfl⟩)
      · exact (List.mem_eraseDups).mpr (List.mem_map.mpr ⟨x, hx, rfl⟩)
    _ = (l.map f).eraseDups.length * (l.map gg).eraseDups.length := length_listProduct _ _

private theorem zipWith_map_map {α β γ δ : Type} (f : α → β) (gg : α → γ) (op : β → γ → δ) :
    ∀ l : List α, List.zipWith op (l.map f) (l.map gg) = l.map (fun x => op (f x) (gg x))
  | [] => rfl
  | a :: as => by simp [zipWith_map_map f gg op as]

theorem sound_JointWidth :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.JointWidth.Goal := by
  intro a b heads tails
  let sa : Row → List (List Bool) := fun h => signature a tails h
  let sb : Row → List (List Bool) := fun h => signature b tails h
  let sab : Row → List (List Bool) := fun h => signature (a++b) tails h
  let op : List Bool → List Bool → List Bool := fun u v => u ++ v
  let g : (List (List Bool) × List (List Bool)) → List (List Bool) := fun p => List.zipWith op p.1 p.2
  have hsab : ∀ h, sab h = g (sa h, sb h) := by
    intro h
    dsimp [sab, sa, sb, g, op, signature, answers]
    simp only [List.map_append]
    exact (zipWith_map_map
      (fun t : Row => List.map (fun q : Query => (q (h++t)).inWhere) a)
      (fun t : Row => List.map (fun q : Query => (q (h++t)).inWhere) b)
      (fun u v : List Bool => u ++ v) tails).symm
  have hmap : heads.map sab = (heads.map (fun h => (sa h, sb h))).map g := by
    rw [List.map_map]
    apply List.map_congr_left
    intro h hh
    dsimp [Function.comp]
    exact hsab h
  change (heads.map (signature (a++b) tails)).eraseDups.length ≤
    (heads.map (signature a tails)).eraseDups.length * (heads.map (signature b tails)).eraseDups.length
  change (heads.map sab).eraseDups.length ≤ (heads.map sa).eraseDups.length * (heads.map sb).eraseDups.length
  rw [hmap]
  exact Nat.le_trans (eraseDups_map_length_le (heads.map (fun h => (sa h, sb h))) g)
    (pair_distinct_le heads sa sb)

-- 9 AddressFirst
theorem sound_AddressFirst :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.AddressFirst.Goal := by
  intro n
  dsimp [classes]
  have h := length_eraseDups_le ((oneHot n).map (signature [mux n] (binaryRows n)))
  simpa [oneHot] using h

-- 7 NegatedConsumer (false)
/-! Counterexample (from `issues/research/frontiers.md`, the 12 known-false goals):
    `rows = [[]]`, `p = fun _ => FALSE`, `q = fun _ => NULL`.
    The premise holds because `FALSE.inWhere = NULL.inWhere = false`.
    `rows.filter (fun r => (p r).not.inWhere) = [[]]` (NOT FALSE = TRUE), while
    `rows.filter (fun r => (q r).not.inWhere) = []` (NOT NULL = NULL). -/
theorem refutation_NegatedConsumer :
    ¬ TinylambOpt.ResearchGoals.Frontiers.DecisionTables.NegatedConsumer.Goal := by
  intro h
  let p : Query := fun _ => SqlBool.tFalse
  let q : Query := fun _ => SqlBool.tNull
  have hprem : ∀ r ∈ ([[]] : List Row), (p r).inWhere = (q r).inWhere := by
    intro r hr
    simp [p, q, SqlBool.inWhere]
  have heq := h p q [[]] hprem
  have hl : ([[]] : List Row).filter (fun r => (p r).not.inWhere) = [[]] := by simp [p, SqlBool.inWhere, SqlBool.not]
  have hr : ([[]] : List Row).filter (fun r => (q r).not.inWhere) = [] := by simp [q, SqlBool.inWhere, SqlBool.not]
  rw [hl, hr] at heq
  simp at heq

-- 8 SignedConsumers
private theorem signed_eq_of_pair (b1 b2 : SqlBool)
    (h : (b1.inWhere, b1.not.inWhere) = (b2.inWhere, b2.not.inWhere)) : b1 = b2 := by
  cases b1 <;> cases b2 <;> simp [SqlBool.inWhere, SqlBool.not] at h ⊢

theorem sound_SignedConsumers :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.SignedConsumers.Goal := by
  intro p q rows hpair consumer
  apply List.filter_congr
  intro r hr
  have hpq : p r = q r := signed_eq_of_pair (p r) (q r) (hpair r hr)
  rw [hpq]


-- 5 WeightedQueries
private theorem countP_congr {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ a ∈ l, p a = q a) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons]
      have ha := h a (by simp)
      have has : ∀ x ∈ as, p x = q x := by intro x hx; exact h x (by simp [hx])
      rw [ih has, ha]

private theorem countP_map {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).countP p = l.countP (fun a => p (f a)) := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [List.countP_cons, ih]

private theorem sum_eraseDups_cons {β γ : Type} [BEq β] [LawfulBEq β] [Add γ] [Zero γ] [Std.Associative (fun x y : γ => x + y)]
    [Std.Commutative (fun x y : γ => x + y)] [Std.LawfulLeftIdentity (fun x y : γ => x + y) 0]
    [Std.LawfulRightIdentity (fun x y : γ => x + y) 0]
    (k : β) (ks : List β) (f : β → γ) :
    ((k :: ks).eraseDups.map f).sum = f k + ((ks.filter (fun x => ! x == k)).eraseDups.map f).sum := by
  rw [List.eraseDups_cons, List.map_cons, List.sum_cons]

private theorem sum_eraseDups_decompose {β γ : Type} [BEq β] [LawfulBEq β]
    [Add γ] [Zero γ] [Std.Associative (fun x y : γ => x + y)]
    [Std.Commutative (fun x y : γ => x + y)] [Std.LawfulLeftIdentity (fun x y : γ => x + y) 0]
    [Std.LawfulRightIdentity (fun x y : γ => x + y) 0]
    (k : β) (f : β → γ) : ∀ ks : List β,
    (ks.eraseDups.map f).sum =
      (if k ∈ ks then f k else 0) + ((ks.filter (fun x => ! x == k)).eraseDups.map f).sum
  | [] => by
      have hz : (0 : γ) + 0 = 0 := Std.LawfulRightIdentity.right_id (0 : γ)
      exact hz.symm
  | a :: as => by
      rw [List.eraseDups_cons, List.map_cons, List.sum_cons]
      by_cases hka : k == a
      · have hbeq : k = a := (beq_iff_eq).mp hka
        subst k
        have hf : (a :: as).filter (fun x => ! x == a) = as.filter (fun x => ! x == a) := by
          simp
        simp [hf]
      · have hne : k ≠ a := by intro h; subst h; simp at hka
        have hka_false : (k == a) = false :=
          Bool.eq_false_of_ne_true (fun h => hne ((beq_iff_eq).mp h))
        have hak : (a == k) = false :=
          Bool.eq_false_of_ne_true (fun h => hne ((beq_iff_eq).mp h).symm)
        have hmem : (k ∈ a :: as) ↔ k ∈ as := by simp [hne]
        have hkfilter : k ∈ as.filter (fun x => ! x == a) ↔ k ∈ as := by
          constructor
          · intro h; exact (List.mem_filter.mp h).1
          · intro h; exact List.mem_filter.mpr ⟨h, by simpa using hka_false⟩
        have ih' := sum_eraseDups_decompose k f (as.filter (fun x => ! x == a))
        have ih'' : ((as.filter (fun x => ! x == a)).eraseDups.map f).sum =
            (if k ∈ as then f k else 0) +
              (((as.filter (fun x => ! x == a)).filter (fun x => ! x == k)).eraseDups.map f).sum := by
          simpa [hkfilter] using ih'
        have hfil : (as.filter (fun x => ! x == k)).filter (fun x => ! x == a) =
            (as.filter (fun x => ! x == a)).filter (fun x => ! x == k) := by
          rw [List.filter_filter, List.filter_filter]
          apply congrArg (fun p : β → Bool => List.filter p as)
          funext x
          cases hxk : x == k <;> cases hxa : x == a <;> simp [Bool.and_comm]
        have hsumR : (((a :: as).filter (fun x => ! x == k)).eraseDups.map f).sum =
            f a + (((as.filter (fun x => ! x == k)).filter (fun x => ! x == a)).eraseDups.map f).sum := by
          have hfil1 : (a :: as).filter (fun x => ! x == k) = a :: as.filter (fun x => ! x == k) := by
            simp [hak]
          rw [hfil1]
          exact sum_eraseDups_cons a (as.filter (fun x => ! x == k)) f
        rw [hsumR, hfil, ih'']
        by_cases hm : k ∈ as
        · have hmcons : k ∈ a :: as := List.mem_cons_of_mem a hm
          simp [hm, hmcons]
          ac_rfl
        · have hmcons : k ∉ a :: as := by
            intro h
            exact hm ((List.mem_cons.mp h).resolve_left hne)
          simp [hm, hmcons]
          ac_rfl
termination_by ks => ks.length
decreasing_by
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun x : β => ! x == a) as) (Nat.lt_succ_self as.length)

private theorem countP_eq_sum_counts {β : Type} [BEq β] [LawfulBEq β] :
    ∀ keys : List β, ∀ P : β → Bool,
    keys.countP P = (keys.eraseDups.map (fun k => if P k then keys.count k else 0)).sum
  | [], P => by simp
  | k :: ks, P => by
      rw [List.countP_cons, List.eraseDups_cons, List.map_cons, List.sum_cons]
      let rest := ks.filter (fun x => ! x == k)
      have ih := countP_eq_sum_counts ks P
      have hother : (rest.eraseDups.map (fun x => if P x then (k :: ks).count x else 0)).sum =
          (rest.eraseDups.map (fun x => if P x then ks.count x else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        have hxrest : x ∈ rest := (List.mem_eraseDups).mp hx
        have hxne : x ≠ k := by
          have hb := (List.mem_filter.mp hxrest).2
          intro h; subst h; simp at hb
        rw [List.count_cons]
        have hkx : (k == x) = false := Bool.eq_false_of_ne_true (fun h => hxne ((beq_iff_eq).mp h).symm)
        simp [hkx]
      have hself : (k :: ks).count k = ks.count k + 1 := List.count_cons_self
      have hdec := sum_eraseDups_decompose k (fun x => if P x then ks.count x else 0) ks
      have hother2 : (((List.filter (fun x => ! x == k) ks).eraseDups.map
            (fun x => if P x then (k :: ks).count x else 0)).sum) =
          (rest.eraseDups.map (fun x => if P x then ks.count x else 0)).sum := by
        change (rest.eraseDups.map (fun x => if P x then (k :: ks).count x else 0)).sum = _
        exact hother
      rw [ih, hdec, hother2]
      rw [hself]
      dsimp [rest]
      by_cases hm : k ∈ ks
      · by_cases hp : P k = true <;> simp [hm, hp] <;> omega
      · have hc : ks.count k = 0 := (List.count_eq_zero).mpr hm
        by_cases hp : P k = true <;> simp [hm, hp, hc] <;> omega

private theorem zip_map_self {α β : Type} (l : List α) (f : α → β) :
    l.zip (l.map f) = l.map (fun a => (a, f a)) := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

theorem sound_WeightedQueries :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.WeightedQueries.Goal := by
  intro qs rows key j h
  let keys := rows.map key
  let P : Row → Bool := fun k => (answers qs k)[j]?.getD false
  have hpoint : ∀ r ∈ rows, (answers qs r)[j]?.getD false = P (key r) := by
    intro r hr
    dsimp [P]
    rw [← h r hr]
  have hcount : rows.countP (fun r => (answers qs r)[j]?.getD false) = keys.countP P := by
    dsimp [keys]
    rw [countP_map]
    apply countP_congr
    intro r hr
    exact hpoint r hr
  have hzip : keys.eraseDups.zip (distinctAnswers qs rows key) =
      keys.eraseDups.map (fun k => (k, answers qs k)) := by
    dsimp [distinctAnswers, keys]
    exact zip_map_self (keys.eraseDups) (fun k => answers qs k)
  dsimp only
  rw [hcount, hzip, List.map_map]
  change keys.countP P = (keys.eraseDups.map (fun k => if P k then keys.count k else 0)).sum
  exact countP_eq_sum_counts keys P

-- 6 GroupDispatch
private def fiber {α β : Type} [BEq β] (key : α → β) (rows : List α) (k : β) :=
  rows.filter (fun r => key r == k)

private def fiberPassSum {α β γ : Type} [BEq β] [Add γ] [Zero γ] [Std.Associative (fun x y : γ => x + y)]
    [Std.Commutative (fun x y : γ => x + y)] [Std.LawfulLeftIdentity (fun x y : γ => x + y) 0]
    [Std.LawfulRightIdentity (fun x y : γ => x + y) 0]
    (key : α → β) (P : α → Bool) (w : α → γ) (rows : List α) (k : β) : γ :=
  ((fiber key rows k).filter P).map w |>.sum

private theorem fiber_nil_of_not_mem {α β : Type} [BEq β] [LawfulBEq β]
    (key : α → β) : ∀ rows : List α, ∀ k, k ∉ rows.map key → fiber key rows k = [] := by
  intro rows k h
  unfold fiber
  apply List.filter_eq_nil_iff.mpr
  intro x hx hxkey
  exact h (List.mem_map.mpr ⟨x, hx, (beq_iff_eq).mp hxkey⟩)

private theorem fiber_sum_eq {α β γ : Type} [BEq β] [LawfulBEq β] [Add γ] [Zero γ] [Std.Associative (fun x y : γ => x + y)]
    [Std.Commutative (fun x y : γ => x + y)] [Std.LawfulLeftIdentity (fun x y : γ => x + y) 0]
    [Std.LawfulRightIdentity (fun x y : γ => x + y) 0]
    (rows : List α) (key : α → β) (P : α → Bool) (w : α → γ) :
    ((rows.filter P).map w).sum =
      ((rows.map key).eraseDups.map (fun k => fiberPassSum key P w rows k)).sum := by
  induction rows with
  | nil => simp [fiberPassSum, fiber]
  | cons a as ih =>
      let ka := key a
      let ks := as.map key
      let rest := ks.filter (fun x => ! x == ka)
      have hfib_self : fiber key (a :: as) ka = a :: fiber key as ka := by
        simp [fiber, ka]
      have hfib_other : ∀ k, k ∈ rest → fiber key (a :: as) k = fiber key as k := by
        intro k hk
        have hkne : k ≠ ka := by
          have hb := (List.mem_filter.mp hk).2
          intro h; subst h; simp at hb
        have hka_neq : (ka == k) = false :=
          Bool.eq_false_of_ne_true (fun h => hkne ((beq_iff_eq).mp h).symm)
        change (a :: as).filter (fun r => key r == k) = as.filter (fun r => key r == k)
        simp [show (key a == k) = false from hka_neq]
      have hself : fiberPassSum key P w (a :: as) ka =
          (if P a then w a else 0) + fiberPassSum key P w as ka := by
        dsimp [fiberPassSum]
        rw [hfib_self]
        by_cases hpa : P a = true
        · simp [hpa]
        · simp [hpa]
          exact (Std.LawfulLeftIdentity.left_id (op := fun x y : γ => x + y)
            (((fiber key as ka).filter P).map w).sum).symm
      have hother : ∀ k, k ∈ rest → fiberPassSum key P w (a :: as) k = fiberPassSum key P w as k := by
        intro k hk
        dsimp [fiberPassSum]
        rw [hfib_other k hk]
      rw [show (a :: as).map key = ka :: ks by rfl]
      rw [List.eraseDups_cons, List.map_cons, List.sum_cons]
      rw [show (((a :: as).filter P).map w).sum =
          (if P a then w a else 0) + (((as.filter P).map w).sum) by
        by_cases hpa : P a = true
        · simp [hpa]
        · simp [hpa]
          exact (Std.LawfulLeftIdentity.left_id (op := fun x y : γ => x + y)
            (((as.filter P).map w).sum)).symm]
      rw [ih, hself]
      have hsum_other : (rest.eraseDups.map (fun k => fiberPassSum key P w (a :: as) k)).sum =
          (rest.eraseDups.map (fun k => fiberPassSum key P w as k)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        exact hother k ((List.mem_eraseDups).mp hk)
      rw [hsum_other]
      let f := fun k => fiberPassSum key P w as k
      have hdec := sum_eraseDups_decompose ka f ks
      have hfzero : ka ∉ ks → f ka = 0 := by
        intro hnot
        dsimp [f, fiberPassSum]
        have hfibnil : fiber key as ka = [] := fiber_nil_of_not_mem key as ka (by simpa [ks] using hnot)
        simp [hfibnil]
      have hdec' : (ks.eraseDups.map f).sum = f ka + (rest.eraseDups.map f).sum := by
        rw [hdec]
        by_cases hmem : ka ∈ ks
        · simp [hmem, rest]
        · simp [hmem, rest, hfzero hmem]
      rw [hdec']
      dsimp [f, rest]
      ac_rfl

private theorem head?_any_eq_of_mem {α : Type} (P : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hconst : ∀ x ∈ l, P x = P a) : l.head?.any P = P a := by
  cases l with
  | nil => cases ha
  | cons b bs =>
      have hb := hconst b (by simp)
      simp [hb]

theorem sound_GroupDispatch :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.GroupDispatch.Goal := by
  intro rows key q hconst
  let P : Row × Int → Bool := fun r => (q r.1).inWhere
  let kf : Row × Int → Nat := fun r => key r.1
  let w : Row × Int → Int := fun r => r.2
  have hmain := fiber_sum_eq rows kf P w
  have hterm : ∀ k ∈ (rows.map kf).eraseDups,
      fiberPassSum kf P w rows k =
        (let g := rows.filter (fun r => kf r == k)
         if g.head?.any P then (g.map w).sum else 0) := by
    intro k hk
    let g := rows.filter (fun r => kf r == k)
    have hgmem : k ∈ rows.map kf := (List.mem_eraseDups).mp hk
    rcases List.mem_map.mp hgmem with ⟨r, hrrows, hrkey⟩
    have hrg : r ∈ g := by
      dsimp [g]
      exact List.mem_filter.mpr ⟨hrrows, by rw [← hrkey]; simp⟩
    have hconst_fiber : ∀ x ∈ g, P x = P r := by
      intro x hx
      have hxrows := (List.mem_filter.mp hx).1
      have hxkey := (List.mem_filter.mp hx).2
      have hxk_eq : kf x = k := (beq_iff_eq).mp hxkey
      have hkeys : kf x = kf r := hxk_eq.trans hrkey.symm
      exact hconst x hxrows r hrrows (by dsimp [kf] at hkeys ⊢; exact hkeys)
    have hhead : g.head?.any P = P r := head?_any_eq_of_mem P g r hrg hconst_fiber
    by_cases hpr : P r = true
    · have hfilter : g.filter P = g := by
        apply List.filter_eq_self.mpr
        intro x hx
        rw [hconst_fiber x hx, hpr]
      dsimp [fiberPassSum, fiber]
      change ((g.filter P).map w).sum = (if g.head?.any P then (g.map w).sum else 0)
      rw [hfilter, hhead, ite_eq_left hpr]
    · have hfilter : g.filter P = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro x hx hh
        have := hconst_fiber x hx
        simp [this, hpr] at hh
      dsimp [fiberPassSum, fiber]
      change ((g.filter P).map w).sum = (if g.head?.any P then (g.map w).sum else 0)
      rw [hfilter, hhead, ite_eq_right hpr]
      simp
  change ((rows.filter P).map w).sum =
    ((rows.map kf).eraseDups.map (fun k =>
      let g := rows.filter (fun r => kf r == k)
      if g.head?.any P then (g.map w).sum else 0)).sum
  rw [hmain]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  exact hterm k hk




def oneHotRow (n i : Nat) : List SqlBool := (List.range n).map (fun j => if i == j then SqlBool.tTrue else SqlBool.tFalse)

theorem oneHotRow_length (n i : Nat) : (oneHotRow n i).length = n := by simp [oneHotRow]

theorem oneHot_get (n i : Nat) (hi : i < n) : (oneHot n)[i]? = some (oneHotRow n i) := by
  simp [oneHot, oneHotRow, List.getElem?_map, List.getElem?_range hi]

-- findIdx helpers copied
theorem findIdx_go_cons {α} (p : α → Bool) (a : α) (as : List α) (off : Nat) :
    List.findIdx.go p (a::as) off = if p a then off else List.findIdx.go p as (off+1) := by rfl
theorem findIdx_go_add {α} (p : α → Bool) : ∀ (l : List α) (off : Nat),
    List.findIdx.go p l off = off + List.findIdx p l := by
  intro l
  induction l with
  | nil => intro off; rfl
  | cons a as ih =>
      intro off
      rw [findIdx_go_cons]
      by_cases hpa : p a = true
      · rw [ite_eq_left hpa]
        have hf : List.findIdx p (a::as) = 0 := by
          change List.findIdx.go p (a::as) 0 = 0
          rw [findIdx_go_cons, ite_eq_left hpa]
        rw [hf]
        omega
      · rw [ite_eq_right hpa]
        rw [ih (off+1)]
        have hf : List.findIdx p (a::as) = List.findIdx p as + 1 := by
          change List.findIdx.go p (a::as) 0 = _
          rw [findIdx_go_cons, ite_eq_right hpa, ih 1]
          omega
        rw [hf]
        omega
theorem findIdx_cons {α} (p : α → Bool) (a : α) (as : List α) :
    List.findIdx p (a :: as) = if p a then 0 else List.findIdx p as + 1 := by
  change List.findIdx.go p (a :: as) 0 = _
  rw [findIdx_go_cons]
  split
  · rfl
  · rw [findIdx_go_add p as 1]
    omega
theorem findIdx_eq_of_getElem? {α} (p : α → Bool) :
    ∀ (l : List α) (i : Nat) (a : α),
    l[i]? = some a → p a = true →
    (∀ j, j < i → ∀ b, l[j]? = some b → p b = false) →
    l.findIdx p = i := by
  intro l
  induction l with
  | nil => intro i a h; cases h
  | cons b bs ih =>
      intro i a hget hpa hprev
      cases i with
      | zero =>
          simp at hget
          subst a
          rw [findIdx_cons, ite_eq_left hpa]
      | succ j =>
          simp at hget
          have hpb : p b = false := hprev 0 (by omega) b (by simp)
          rw [findIdx_cons]
          simp [hpb]
          have hprev' : ∀ k, k < j → ∀ c, bs[k]? = some c → p c = false := by
            intro k hk c hc
            exact hprev (k+1) (by omega) c hc
          have := ih j a hget hpa hprev'
          omega
theorem oneHotRow_get (n i : Nat) (hi : i < n) : (oneHotRow n i)[i]? = some SqlBool.tTrue := by
  simp [oneHotRow, List.getElem?_map, List.getElem?_range hi]
theorem oneHotRow_get_of_lt (n i j : Nat) (hj : j < n) :
    (oneHotRow n i)[j]? = some (if i == j then SqlBool.tTrue else SqlBool.tFalse) := by
  simp [oneHotRow, List.getElem?_map, List.getElem?_range hj]
theorem findIdx_oneHotRow (n i : Nat) (hi : i < n) :
    (oneHotRow n i).findIdx SqlBool.inWhere = i := by
  apply findIdx_eq_of_getElem? SqlBool.inWhere (oneHotRow n i) i SqlBool.tTrue
  · exact oneHotRow_get n i hi
  · rfl
  · intro j hji b hb
    have hj : j < n := by omega
    rw [oneHotRow_get_of_lt n i j hj] at hb
    injection hb with hbeq
    subst b
    have hij : i ≠ j := by omega
    have hbfalse : (i == j) = false := Bool.eq_false_of_ne_true (fun h => hij ((beq_iff_eq).mp h))
    simp [hbfalse, SqlBool.inWhere]
theorem mux_oneHot (n i : Nat) (hi : i < n) (h : Row) :
    mux n (oneHotRow n i ++ h) = h[i]?.getD SqlBool.tNull := by
  unfold mux
  have hlen : (oneHotRow n i).length = n := oneHotRow_length n i
  have htake : (oneHotRow n i ++ h).take n = oneHotRow n i := by
    simp [hlen]
  have hdrop : (oneHotRow n i ++ h).drop n = h := by
    simp [hlen]
  rw [htake, hdrop, findIdx_oneHotRow n i hi]

-- binaryRows facts
theorem binaryRows_length : ∀ n, (binaryRows n).length = 2^n
  | 0 => by simp [binaryRows]
  | n+1 => by
      dsimp [binaryRows]
      rw [List.length_append, List.length_map, List.length_map, binaryRows_length n]
      simp [Nat.pow_succ]
      omega

theorem binaryRows_row_length : ∀ n r, r ∈ binaryRows n → r.length = n
  | 0, r, hr => by
      have : r = [] := by simpa [binaryRows] using hr
      subst r
      rfl
  | n+1, r, hr => by
      dsimp [binaryRows] at hr
      rcases List.mem_append.mp hr with h | h
      · rcases List.mem_map.mp h with ⟨s, hs, rfl⟩
        rw [List.length_cons, binaryRows_row_length n s hs]
      · rcases List.mem_map.mp h with ⟨s, hs, rfl⟩
        rw [List.length_cons, binaryRows_row_length n s hs]

theorem binaryRows_binary : ∀ n r, r ∈ binaryRows n → ∀ b ∈ r, b = SqlBool.tTrue ∨ b = SqlBool.tFalse
  | 0, r, hr => by
      have : r = [] := by simpa [binaryRows] using hr
      subst r
      intro b hb; cases hb
  | n+1, r, hr => by
      dsimp [binaryRows] at hr
      intro b hb
      rcases List.mem_append.mp hr with h | h
      · rcases List.mem_map.mp h with ⟨s, hs, rfl⟩
        cases hb with
        | head => exact Or.inr rfl
        | tail _ hb => exact binaryRows_binary n s hs b hb
      · rcases List.mem_map.mp h with ⟨s, hs, rfl⟩
        cases hb with
        | head => exact Or.inl rfl
        | tail _ hb => exact binaryRows_binary n s hs b hb

theorem revMux_append_oneHot (n i : Nat) (h : Row) (hlen : h.length = n) (hi : i < n) :
    mux n ((h ++ oneHotRow n i).drop n ++ (h ++ oneHotRow n i).take n) = h[i]?.getD SqlBool.tNull := by
  have htake : (h ++ oneHotRow n i).take n = h := by
    simp [hlen]
  have hdrop : (h ++ oneHotRow n i).drop n = oneHotRow n i := by
    simp [hlen]
  rw [htake, hdrop]
  exact mux_oneHot n i hi h

theorem signature_revMux (n : Nat) (h : Row) (hlen : h.length = n) :
    signature [fun r => mux n (r.drop n ++ r.take n)] (oneHot n) h =
      (List.range n).map (fun i => [(h[i]?.getD SqlBool.tNull).inWhere]) := by
  unfold signature answers
  apply List.ext_getElem?
  intro i
  by_cases hi : i < n
  · have hget := oneHot_get n i hi
    have hlenOneHot : (oneHot n).length = n := by simp [oneHot]
    rw [List.getElem?_map, hget, List.getElem?_map, List.getElem?_range hi]
    simp only [Option.map_some, List.map_cons, List.map_nil]
    rw [revMux_append_oneHot n i h hlen hi]
  · have hlenOneHot : (oneHot n).length = n := by simp [oneHot]
    rw [List.getElem?_map]
    rw [List.getElem?_eq_none (by simpa [hlenOneHot] using hi)]
    simp
    rw [List.getElem?_eq_none (by simp; omega : (List.range n).length ≤ i)]
    simp

theorem nodup_map_cons (b : SqlBool) (l : List Row) (hl : l.Nodup) :
    (l.map (fun r => b :: r)).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      simp only [List.map_cons]
      rw [List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨r, hr, heq⟩
        have har : a = r := by cases heq; rfl
        subst r
        exact hl.1 hr
      · exact ih hl.2

theorem map_cons_disjoint (l : List Row) :
    ∀ x ∈ l.map (fun r => SqlBool.tFalse :: r),
      ∀ y ∈ l.map (fun r => SqlBool.tTrue :: r), x ≠ y := by
  intro x hx y hy heq
  rcases List.mem_map.mp hx with ⟨r, hr, rfl⟩
  rcases List.mem_map.mp hy with ⟨s, hs, rfl⟩
  have hh := congrArg List.head? heq
  simp at hh

theorem nodup_binaryRows : ∀ n, (binaryRows n).Nodup
  | 0 => by simp [binaryRows]
  | n+1 => by
      dsimp [binaryRows]
      rw [List.nodup_append]
      exact ⟨nodup_map_cons SqlBool.tFalse _ (nodup_binaryRows n),
             nodup_map_cons SqlBool.tTrue _ (nodup_binaryRows n),
             map_cons_disjoint (binaryRows n)⟩

theorem bin_inWhere_eq (a b : SqlBool) (ha : a = SqlBool.tTrue ∨ a = SqlBool.tFalse)
    (hb : b = SqlBool.tTrue ∨ b = SqlBool.tFalse) (h : a.inWhere = b.inWhere) : a = b := by
  rcases ha with rfl | rfl <;> rcases hb with rfl | rfl <;> simp [SqlBool.inWhere] at h ⊢

theorem signature_injective (n : Nat) (h1 h2 : Row)
    (h1m : h1 ∈ binaryRows n) (h2m : h2 ∈ binaryRows n)
    (heq : signature [fun r => mux n (r.drop n ++ r.take n)] (oneHot n) h1 =
           signature [fun r => mux n (r.drop n ++ r.take n)] (oneHot n) h2) : h1 = h2 := by
  have hlen1 := binaryRows_row_length n h1 h1m
  have hlen2 := binaryRows_row_length n h2 h2m
  rw [signature_revMux n h1 hlen1, signature_revMux n h2 hlen2] at heq
  apply List.ext_getElem?
  intro i
  by_cases hi : i < n
  · have hbit := congrArg (fun l : List (List Bool) => l[i]?) heq
    simp only [List.getElem?_map, List.getElem?_range hi, Option.map_some] at hbit
    injection hbit with hb
    have h1get : h1[i]? = some h1[i] := List.getElem?_eq_getElem (by omega)
    have h2get : h2[i]? = some h2[i] := List.getElem?_eq_getElem (by omega)
    have hbin1 := binaryRows_binary n h1 h1m (h1[i]) (List.getElem_mem (by omega))
    have hbin2 := binaryRows_binary n h2 h2m (h2[i]) (List.getElem_mem (by omega))
    have hval : h1[i] = h2[i] := bin_inWhere_eq _ _ hbin1 hbin2 (by simpa [h1get, h2get] using hb)
    rw [h1get, h2get, hval]
  · rw [List.getElem?_eq_none (by omega : h1.length ≤ i),
        List.getElem?_eq_none (by omega : h2.length ≤ i)]

theorem nodup_map_of_injOn {α β : Type} (l : List α) (f : α → β) (hl : l.Nodup)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      simp only [List.map_cons]
      rw [List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, hfb⟩
        have hab : a = b := hinj a (by simp) b (by simp [hb]) hfb.symm
        subst b
        exact hl.1 hb
      · exact ih hl.2 (by
          intro x hx y hy heq
          exact hinj x (by simp [hx]) y (by simp [hy]) heq)

theorem length_eraseDups_eq_of_nodup {α : Type} [BEq α] [LawfulBEq α] :
    ∀ l : List α, l.Nodup → l.eraseDups.length = l.length := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
      intro hl
      rw [List.nodup_cons] at hl
      rw [List.eraseDups_cons]
      have hf : as.filter (fun b => ! b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by intro h; exact hl.1 (by subst h; exact hb)
        have hbeq : (b == a) = false :=
          Bool.eq_false_of_ne_true (fun h => hba ((beq_iff_eq).mp h))
        simp [hbeq]
      simp [hf, ih hl.2]

theorem sound_PayloadFirst :
    TinylambOpt.ResearchGoals.Frontiers.DecisionTables.PayloadFirst.Goal := by
  intro n
  let revMux : Query := fun r => mux n (r.drop n ++ r.take n)
  let sig : Row → List (List Bool) := fun h => signature [revMux] (oneHot n) h
  dsimp [classes]
  change ((binaryRows n).map sig).eraseDups.length = 2^n
  have hinj : ∀ a ∈ binaryRows n, ∀ b ∈ binaryRows n, sig a = sig b → a = b := by
    intro a ha b hb heq
    dsimp [sig, revMux] at heq ⊢
    exact signature_injective n a b ha hb heq
  have hmapnod : ((binaryRows n).map sig).Nodup :=
    nodup_map_of_injOn (binaryRows n) sig (nodup_binaryRows n) hinj
  rw [length_eraseDups_eq_of_nodup _ hmapnod, List.length_map, binaryRows_length n]


end TinylambOpt.ResearchProofs.Frontiers.DecisionTables
