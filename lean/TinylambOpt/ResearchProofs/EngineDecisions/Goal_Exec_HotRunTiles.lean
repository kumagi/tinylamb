import TinylambOpt.ResearchGoals.EngineDecisions.Executor
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Exec_HotRunTiles

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.HotRunTiles

private theorem isEmpty_eq_true_iff_nil {α : Type} (l : List α) :
    l.isEmpty = true ↔ l = [] := by
  cases l with
  | nil => exact ⟨fun _ => rfl, fun _ => rfl⟩
  | cons a as =>
    constructor
    · intro h
      have h2 : (a :: as).isEmpty = false := rfl
      rw [h2] at h
      cases h
    · intro h
      cases h

-- Membership in one tile, expressed through the pair ordinal `i * r + j`.

private theorem mem_tile (l r width t i j : Nat) :
    (i, j) ∈ tile l r width t ↔
      j < r ∧ t * width ≤ i * r + j ∧
        i * r + j < t * width + min width (l * r - t * width) := by
  rw [tile, List.mem_map]
  constructor
  · rintro ⟨k, hk, heq⟩
    rw [List.mem_range] at hk
    dsimp only at heq
    rw [Prod.mk.injEq] at heq
    obtain ⟨hd, hm⟩ := heq
    subst hd
    subst hm
    have hmod_lt : (t * width + k) % r < r := by
      rcases Nat.eq_zero_or_pos r with hr | hr
      · subst hr
        rw [Nat.mod_zero]
        simpa using hk
      · exact Nat.mod_lt _ hr
    have hdivmod :
        (t * width + k) / r * r + (t * width + k) % r = t * width + k := by
      rw [Nat.mul_comm, Nat.div_add_mod]
    refine ⟨hmod_lt, ?_, ?_⟩
    · rw [hdivmod]
      exact Nat.le_add_right _ _
    · rw [hdivmod]
      omega
  · rintro ⟨hj, hlo, hhi⟩
    refine ⟨i * r + j - t * width, ?_, ?_⟩
    · rw [List.mem_range]
      have h1 := Nat.sub_lt_sub_right hlo hhi
      rwa [Nat.add_sub_cancel_left] at h1
    · dsimp only
      have hsum : t * width + (i * r + j - t * width) = i * r + j := Nat.add_sub_of_le hlo
      rw [hsum]
      have hdiv : (i * r + j) / r = i := by
        rcases Nat.eq_zero_or_pos r with hr | hr
        · rw [hr]
          exact absurd hj (by omega)
        · refine Nat.div_eq_of_lt_le ?_ ?_
          · omega
          · have hmul : (i + 1) * r = i * r + r := by rw [Nat.add_mul, Nat.one_mul]
            omega
      have hmod : (i * r + j) % r = j := by
        rcases Nat.eq_zero_or_pos r with hr | hr
        · rw [hr]
          exact absurd hj (by omega)
        · have hsum2 : i * r + j = j + r * i := by rw [Nat.add_comm, Nat.mul_comm]
          rw [hsum2, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hj]
      rw [hdiv, hmod]

-- The set of pairs covered by `hits` is exactly the passing pairs.

private theorem mem_hits (l r width : Nat) (order : List Nat) (pass : PairId → Bool)
    (hw : 0 < width) (hperm : order.Perm (List.range ((l * r + width - 1) / width)))
    (a b : Nat) :
    ((a, b) ∈ order.flatMap fun t => (tile l r width t).filter pass) ↔
      a < l ∧ b < r ∧ pass (a, b) = true := by
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨t, _, hft⟩
    rw [List.mem_filter] at hft
    obtain ⟨htile, hpass⟩ := hft
    rw [mem_tile l r width t a b] at htile
    obtain ⟨hb, hlo, hhi⟩ := htile
    have hmr : min width (l * r - t * width) ≤ l * r - t * width := Nat.min_le_right _ _
    have holt : a * r + b < l * r := by omega
    have halt : a < l := by
      refine Nat.lt_of_not_le ?_
      intro hle
      have hmul : l * r ≤ a * r := Nat.mul_le_mul_right r hle
      have hge : a * r + b ≥ l * r := by omega
      omega
    exact ⟨halt, hb, hpass⟩
  · rintro ⟨ha, hb, hpass⟩
    have hr0 : 0 < r := by omega
    let o := a * r + b
    have ho : o = a * r + b := rfl
    have holt' : o < l * r := by
      have hmul : (a + 1) * r = a * r + r := by rw [Nat.add_mul, Nat.one_mul]
      have hle : (a + 1) * r ≤ l * r := Nat.mul_le_mul_right r (by omega)
      have ho2 : o < (a + 1) * r := by omega
      exact Nat.lt_of_lt_of_le ho2 hle
    have hcomm : (o / width) * width = width * (o / width) := Nat.mul_comm _ _
    have hlt : o / width < (l * r + width - 1) / width := by
      have hd := Nat.div_add_mod o width
      rw [Nat.lt_div_iff_mul_lt hw]
      have hsub2 : (l * r + width - 1) - (width - 1) = l * r := by omega
      omega
    have hd := Nat.div_add_mod o width
    have hsub : o - (o / width) * width = o % width := by omega
    have hlow : (o / width) * width ≤ o := by omega
    have hhi2 : o < (o / width) * width + min width (l * r - (o / width) * width) := by
      have h1 : o % width < width := Nat.mod_lt o hw
      have h2 : o - (o / width) * width < l * r - (o / width) * width :=
        Nat.sub_lt_sub_right hlow holt'
      omega
    refine ⟨o / width, ?_, List.mem_filter.mpr
      ⟨(mem_tile l r width (o / width) a b).mpr ⟨hb, by omega, by omega⟩, hpass⟩⟩
    · rw [hperm.mem_iff, List.mem_range]
      exact hlt

-- Pairwise distinctness of tiles and of the collected hits.

private theorem nodup_tile (l r width t : Nat) : (tile l r width t).Nodup := by
  refine List.Pairwise.map (f := fun k =>
    (let ordinal := t * width + k; (ordinal / r, ordinal % r))) ?_ List.nodup_range
  intro a b hab hfa
  dsimp only at hfa
  rw [Prod.mk.injEq] at hfa
  obtain ⟨h1, h2⟩ := hfa
  have ea := Nat.div_add_mod (t * width + a) r
  have eb := Nat.div_add_mod (t * width + b) r
  rw [h1, h2] at ea
  have hEq : t * width + a = t * width + b := by
    rw [← ea]
    exact eb
  omega

private theorem tile_disjoint (l r width t s : Nat) (y : PairId)
    (ht : y ∈ tile l r width t) (hs : y ∈ tile l r width s) : t = s := by
  cases y with
  | mk i j =>
    rw [mem_tile l r width t i j] at ht
    rw [mem_tile l r width s i j] at hs
    obtain ⟨_, hlo1, hhi1⟩ := ht
    obtain ⟨_, hlo2, hhi2⟩ := hs
    have b1 : i * r + j < t * width + width :=
      Nat.lt_of_lt_of_le hhi1 (Nat.add_le_add_left (Nat.min_le_left width (l * r - t * width)) _)
    have b2 : i * r + j < s * width + width :=
      Nat.lt_of_lt_of_le hhi2 (Nat.add_le_add_left (Nat.min_le_left width (l * r - s * width)) _)
    rcases Nat.lt_trichotomy t s with hlt | heq | hgt
    · have h1 : (t + 1) * width = t * width + width := by rw [Nat.add_mul, Nat.one_mul]
      have h2 : (t + 1) * width ≤ s * width := Nat.mul_le_mul_right width (by omega)
      omega
    · exact heq
    · have h1 : (s + 1) * width = s * width + width := by rw [Nat.add_mul, Nat.one_mul]
      have h2 : (s + 1) * width ≤ t * width := Nat.mul_le_mul_right width (by omega)
      omega

private theorem nodup_flatMap_of {α β : Type} : ∀ (os : List α) (f : α → List β),
    (∀ a ∈ os, (f a).Nodup) →
    (∀ a ∈ os, ∀ b ∈ os, a ≠ b → ∀ x, x ∈ f a → x ∈ f b → False) →
    os.Nodup → (os.flatMap f).Nodup := by
  intro os f
  induction os with
  | nil => intro _ _ _; exact List.nodup_nil
  | cons t ts ih =>
    intro hf hdis hnd
    rw [List.flatMap_cons, List.nodup_append]
    obtain ⟨hnt, hnts⟩ := List.nodup_cons.mp hnd
    refine ⟨hf t (List.mem_cons_self (a := t) (l := ts)),
      ih (fun a ha => hf a (List.mem_cons_of_mem t ha))
        (fun a ha b hb hne => hdis a (List.mem_cons_of_mem t ha) b (List.mem_cons_of_mem t hb) hne)
        hnts, ?_⟩
    intro x hx1 y hxy
    intro hne2
    subst hne2
    rw [List.mem_flatMap] at hxy
    obtain ⟨s, hst, hxs⟩ := hxy
    exact hdis t (List.mem_cons_self (a := t) (l := ts)) s (List.mem_cons_of_mem t hst)
      (fun heq => hnt (heq ▸ hst)) x hx1 hxs

private theorem count_eq_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α] (la lb : List α)
    (ha : la.Nodup) (hb : lb.Nodup) (m : ∀ a, a ∈ la ↔ a ∈ lb) : List.Perm la lb :=
  List.perm_iff_count.mpr fun a => by
    by_cases h : a ∈ la
    · have h2 : a ∈ lb := (m a).mp h
      rw [List.Nodup.count_of_mem ha h, List.Nodup.count_of_mem hb h2]
    · have h2 : ¬a ∈ lb := fun hh => h ((m a).mpr hh)
      rw [List.count_eq_zero_of_not_mem h, List.count_eq_zero_of_not_mem h2]

private theorem nodup_singleton {α : Type} (x : α) : [x].Nodup :=
  (List.nodup_cons (a := x) (l := [])).mpr ⟨List.not_mem_nil, List.nodup_nil⟩

private theorem nodup_map_of_inj {α β : Type} (f : α → β) (l : List α)
    (hinj : ∀ a b, f a = f b → a = b) (h : l.Nodup) : (List.map f l).Nodup :=
  List.Pairwise.map (f := f) (fun a b hab hfa => hab (hinj a b hfa)) h

-- Nodup of the collected hit list.

private theorem nodup_hits (l r width : Nat) (order : List Nat) (pass : PairId → Bool)
    (hperm : order.Perm (List.range ((l * r + width - 1) / width))) :
    (order.flatMap fun t => (tile l r width t).filter pass).Nodup :=
  nodup_flatMap_of order (fun t => (tile l r width t).filter pass)
    (fun t _ => List.Pairwise.filter pass (nodup_tile l r width t))
    (fun t _ s _ hne x hx hx' =>
      absurd (tile_disjoint l r width t s x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx').1)
        hne)
    (hperm.symm.nodup List.nodup_range)

-- Nodup of `finish`.

private theorem nodup_finish (l r : Nat) (hits : List PairId) (hnd : hits.Nodup) :
    (finish l r hits).Nodup := by
  rw [finish]
  have hA : (hits.map fun (i, j) => (some i, some j)).Nodup := by
    refine nodup_map_of_inj (fun (i, j) => (some i, some j)) hits ?_ hnd
    rintro ⟨i, j⟩ ⟨i', j'⟩ heq
    rw [Prod.mk.injEq] at heq
    have hi := Option.some.inj heq.1
    have hj := Option.some.inj heq.2
    subst hi; subst hj
    rfl
  have hB : (((List.range l).filter fun i => !hits.any (fun p => p.1 == i))
      |>.map fun i => ((some i, none) : Out)).Nodup := by
    refine nodup_map_of_inj (fun i => ((some i, none) : Out)) _ ?_
      (List.Pairwise.filter _ List.nodup_range)
    intro i i' heq
    rw [Prod.mk.injEq] at heq
    exact Option.some.inj heq.1
  have hC : (((List.range r).filter fun j => !hits.any (fun p => p.2 == j))
      |>.map fun j => ((none, some j) : Out)).Nodup := by
    refine nodup_map_of_inj (fun j => ((none, some j) : Out)) _ ?_
      (List.Pairwise.filter _ List.nodup_range)
    intro j j' heq
    rw [Prod.mk.injEq] at heq
    exact Option.some.inj heq.2
  rw [List.nodup_append, List.nodup_append]
  refine ⟨⟨hA, hB, ?_⟩, hC, ?_⟩
  · intro a ha b hb hneq
    rw [List.mem_map] at ha
    rw [List.mem_map] at hb
    obtain ⟨x, _, rfl⟩ := ha
    obtain ⟨xi, xj⟩ := x
    obtain ⟨i, _, rfl⟩ := hb
    dsimp only at hneq
    rw [Prod.mk.injEq] at hneq
    cases hneq.2
  · intro y hy c hc hneq
    rw [List.mem_append] at hy
    rw [List.mem_map] at hc
    obtain ⟨j, _, rfl⟩ := hc
    rcases hy with hy | hy
    · rw [List.mem_map] at hy
      obtain ⟨x, _, rfl⟩ := hy
      obtain ⟨xi, xj⟩ := x
      dsimp only at hneq
      rw [Prod.mk.injEq] at hneq
      cases hneq.1
    · rw [List.mem_map] at hy
      obtain ⟨i, _, rfl⟩ := hy
      rw [Prod.mk.injEq] at hneq
      cases hneq.1

private theorem nat_beq_self (p : Nat) : (p == p) = true := by
  cases h : p == p with
  | true => rfl
  | false => exact absurd (h ▸ beq_self_eq_true p) Bool.noConfusion

-- Converters from "not any" to universal non-membership.

private theorem not_any_fst (hl : List PairId) (a : Nat) :
    (hl.any (fun p => p.1 == a) = false) ↔ ∀ b, (a, b) ∉ hl := by
  constructor
  · intro h b hb
    have h1 : hl.any (fun p => p.1 == a) = true :=
      List.any_eq_true.mpr ⟨(a, b), hb, nat_beq_self a⟩
    rw [h] at h1
    cases h1
  · intro h
    by_cases hb : hl.any (fun p => p.1 == a) = true
    · obtain ⟨x, hx, hbeq⟩ := List.any_eq_true.mp hb
      obtain ⟨i, j⟩ := x
      dsimp only at hbeq
      have hi := beq_iff_eq.mp hbeq
      subst hi
      exact absurd hx (h j)
    · cases h2 : hl.any (fun p => p.1 == a) with
      | true => exact absurd h2 hb
      | false => rfl

private theorem not_any_snd (hl : List PairId) (b : Nat) :
    (hl.any (fun p => p.2 == b) = false) ↔ ∀ a, (a, b) ∉ hl := by
  constructor
  · intro h a ha
    have h1 : hl.any (fun p => p.2 == b) = true :=
      List.any_eq_true.mpr ⟨(a, b), ha, nat_beq_self b⟩
    rw [h] at h1
    cases h1
  · intro h
    by_cases hb : hl.any (fun p => p.2 == b) = true
    · obtain ⟨x, hx, hbeq⟩ := List.any_eq_true.mp hb
      obtain ⟨i, j⟩ := x
      dsimp only at hbeq
      have hj := beq_iff_eq.mp hbeq
      subst hj
      exact absurd hx (h i)
    · cases h2 : hl.any (fun p => p.2 == b) with
      | true => exact absurd h2 hb
      | false => rfl

private theorem not_any_pass (l : Nat) (pass : PairId → Bool) (b : Nat) :
    ((List.range l).any (fun i => pass (i, b)) = false) ↔
      ∀ a, a ∈ List.range l → pass (a, b) = false := by
  constructor
  · intro h a ha
    cases hp : pass (a, b) with
    | true =>
      have h1 : (List.range l).any (fun i => pass (i, b)) = true :=
        List.any_eq_true.mpr ⟨a, ha, hp⟩
      rw [h] at h1
      cases h1
    | false => rfl
  · intro h
    by_cases hb : (List.range l).any (fun i => pass (i, b)) = true
    · obtain ⟨x, hx1, hx2⟩ := List.any_eq_true.mp hb
      rw [h x hx1] at hx2
      cases hx2
    · cases h2 : (List.range l).any (fun i => pass (i, b)) with
      | true => exact absurd h2 hb
      | false => rfl

-- Membership in `finish`, by output shape.

private theorem mem_finish_some_some (l r : Nat) (hits : List PairId) (a b : Nat) :
    ((some a, some b) ∈ finish l r hits) ↔ (a, b) ∈ hits := by
  rw [finish, List.mem_append, List.mem_append, List.mem_map, List.mem_map, List.mem_map]
  constructor
  · intro h
    rcases h with h12 | h3
    · rcases h12 with ⟨p, hp, heq⟩ | ⟨i, hmem, heq⟩
      · obtain ⟨pi, pj⟩ := p
        dsimp only at heq
        rw [Prod.mk.injEq] at heq
        have h1 := Option.some.inj heq.1
        have h2 := Option.some.inj heq.2
        subst h1; subst h2
        exact hp
      · rw [Prod.mk.injEq] at heq
        cases heq.2
    · rcases h3 with ⟨j, hmem, heq⟩
      rw [Prod.mk.injEq] at heq
      cases heq.1
  · intro hh
    left; left
    exact ⟨(a, b), hh, rfl⟩

private theorem mem_finish_some_none (l r : Nat) (hits : List PairId) (a : Nat) :
    ((some a, none) ∈ finish l r hits) ↔
      a ∈ List.range l ∧ hits.any (fun p => p.1 == a) = false := by
  rw [finish, List.mem_append, List.mem_append, List.mem_map, List.mem_map, List.mem_map]
  constructor
  · intro h
    rcases h with h12 | h3
    · rcases h12 with ⟨p, hp, heq⟩ | ⟨i, hmem, heq⟩
      · obtain ⟨pi, pj⟩ := p
        dsimp only at heq
        rw [Prod.mk.injEq] at heq
        cases heq.2
      · obtain ⟨h21, h22⟩ := List.mem_filter.mp hmem
        rw [Prod.mk.injEq] at heq
        have hi := Option.some.inj heq.1
        subst hi
        refine ⟨h21, ?_⟩
        cases hb : hits.any (fun p => p.1 == i) with
        | true => rw [hb] at h22; cases h22
        | false => rfl
    · rcases h3 with ⟨j, hmem, heq⟩
      rw [Prod.mk.injEq] at heq
      cases heq.1
  · intro ⟨hil, hic⟩
    left; right
    refine ⟨a, List.mem_filter.mpr ⟨hil, ?_⟩, rfl⟩
    rw [hic]
    rfl

private theorem mem_finish_none_some (l r : Nat) (hits : List PairId) (b : Nat) :
    ((none, some b) ∈ finish l r hits) ↔
      b ∈ List.range r ∧ hits.any (fun p => p.2 == b) = false := by
  rw [finish, List.mem_append, List.mem_append, List.mem_map, List.mem_map, List.mem_map]
  constructor
  · intro h
    rcases h with h12 | h3
    · rcases h12 with ⟨p, hp, heq⟩ | ⟨i, hmem, heq⟩
      · obtain ⟨pi, pj⟩ := p
        dsimp only at heq
        rw [Prod.mk.injEq] at heq
        cases heq.1
      · rw [Prod.mk.injEq] at heq
        cases heq.1
    · rcases h3 with ⟨j, hmem, heq⟩
      obtain ⟨h21, h22⟩ := List.mem_filter.mp hmem
      rw [Prod.mk.injEq] at heq
      have hj := Option.some.inj heq.2
      subst hj
      refine ⟨h21, ?_⟩
      cases hb : hits.any (fun p => p.2 == j) with
      | true => rw [hb] at h22; cases h22
      | false => rfl
  · intro ⟨hjr, hjc⟩
    right
    refine ⟨b, List.mem_filter.mpr ⟨hjr, ?_⟩, rfl⟩
    rw [hjc]
    rfl

private theorem not_mem_finish_none_none (l r : Nat) (hits : List PairId) :
    ¬((none, none) ∈ finish l r hits) := by
  rw [finish, List.mem_append, List.mem_append, List.mem_map, List.mem_map, List.mem_map]
  intro h
  rcases h with h12 | h3
  · rcases h12 with ⟨p, _, heq⟩ | ⟨i, _, heq⟩
    · obtain ⟨pi, pj⟩ := p
      dsimp only at heq
      rw [Prod.mk.injEq] at heq
      cases heq.1
    · rw [Prod.mk.injEq] at heq
      cases heq.1
  · rcases h3 with ⟨j, _, heq⟩
    rw [Prod.mk.injEq] at heq
    cases heq.2

-- Distinctness of the reference emission and its membership characterization.

private theorem block_mem_fst (r : Nat) (pass : PairId → Bool) (i : Nat) (x : Out)
    (hx : x ∈ (let js := (List.range r).filter (fun k => pass (i, k));
      if js.isEmpty then [(some i, none)] else js.map (fun k => (some i, some k)))) :
    x.1 = some i := by
  dsimp only at hx
  by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
  · rw [if_pos he] at hx
    rw [List.mem_singleton] at hx
    rw [hx]
  · rw [if_neg he] at hx
    rw [List.mem_map] at hx
    obtain ⟨k, _, rfl⟩ := hx
    rfl

private theorem nodup_outer (l r : Nat) (pass : PairId → Bool) :
    (outerRef l r pass).Nodup := by
  have hf : ∀ i ∈ List.range l, ((let js := (List.range r).filter (fun k => pass (i, k));
      if js.isEmpty then [(some i, none)] else js.map (fun k => (some i, some k)))).Nodup := by
    intro i _
    dsimp only
    by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
    · rw [if_pos he]
      exact nodup_singleton (some i, none)
    · rw [if_neg he]
      refine nodup_map_of_inj (fun k => ((some i, some k) : Out)) _ ?_
        (List.Pairwise.filter _ List.nodup_range)
      intro k k' heq
      rw [Prod.mk.injEq] at heq
      exact Option.some.inj heq.2
  have hdis : ∀ i ∈ List.range l, ∀ j ∈ List.range l, i ≠ j →
      ∀ x, x ∈ (let js := (List.range r).filter (fun k => pass (i, k));
          if js.isEmpty then [(some i, none)] else js.map (fun k => (some i, some k))) →
      x ∈ (let js := (List.range r).filter (fun k => pass (j, k));
          if js.isEmpty then [(some j, none)] else js.map (fun k => (some j, some k))) →
      False := by
    intro i _ j _ hne x hx1 hx2
    have e1 : x.1 = some i := block_mem_fst r pass i x hx1
    have e2 : x.1 = some j := block_mem_fst r pass j x hx2
    have hij : some i = some j := by rw [← e1, ← e2]
    exact hne (Option.some.inj hij)
  rw [outerRef, List.nodup_append]
  refine ⟨nodup_flatMap_of (List.range l) (fun i =>
      (let js := (List.range r).filter (fun k => pass (i, k));
        if js.isEmpty then [(some i, none)] else js.map (fun k => (some i, some k))))
      hf hdis List.nodup_range, ?_, ?_⟩
  · refine nodup_map_of_inj (fun j => ((none, some j) : Out)) _ ?_
      (List.Pairwise.filter _ List.nodup_range)
    intro j j' heq
    rw [Prod.mk.injEq] at heq
    exact Option.some.inj heq.2
  · intro x hx y hy hne
    rw [List.mem_flatMap] at hx
    obtain ⟨i, _, hx1⟩ := hx
    dsimp only at hx1
    have e1 : x.1 = some i := block_mem_fst r pass i x hx1
    rw [List.mem_map] at hy
    obtain ⟨j, _, rfl⟩ := hy
    have h1 := congrArg Prod.fst hne
    rw [e1] at h1
    cases h1

private theorem mem_outer_some_some (l r : Nat) (pass : PairId → Bool) (a b : Nat) :
    ((some a, some b) ∈ outerRef l r pass) ↔
      a ∈ List.range l ∧ b ∈ List.range r ∧ pass (a, b) = true := by
  rw [outerRef, List.mem_append]
  constructor
  · intro h
    rcases h with hflat | htail
    · rw [List.mem_flatMap] at hflat
      obtain ⟨i, hi, hb⟩ := hflat
      dsimp only at hb
      by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
      · rw [if_pos he] at hb
        rw [List.mem_singleton] at hb
        rw [Prod.mk.injEq] at hb
        cases hb.2
      · rw [if_neg he] at hb
        rw [List.mem_map] at hb
        obtain ⟨k, hk, heq⟩ := hb
        obtain ⟨hk1, hk2⟩ := List.mem_filter.mp hk
        rw [Prod.mk.injEq] at heq
        have hia := Option.some.inj heq.1
        have hkb := Option.some.inj heq.2
        subst hia
        subst hkb
        exact ⟨hi, hk1, hk2⟩
    · rw [List.mem_map] at htail
      obtain ⟨j, hj, heq⟩ := htail
      rw [Prod.mk.injEq] at heq
      cases heq.1
  · rintro ⟨ha, hb, hp⟩
    left
    rw [List.mem_flatMap]
    refine ⟨a, ha, ?_⟩
    dsimp only
    by_cases he : ((List.range r).filter (fun k => pass (a, k))).isEmpty = true
    · have hmem : b ∈ (List.range r).filter (fun k => pass (a, k)) :=
        List.mem_filter.mpr ⟨hb, hp⟩
      rw [(isEmpty_eq_true_iff_nil _).mp he] at hmem
      exact absurd hmem List.not_mem_nil
    · rw [if_neg he]
      rw [List.mem_map]
      exact ⟨b, List.mem_filter.mpr ⟨hb, hp⟩, rfl⟩

private theorem mem_outer_some_none (l r : Nat) (pass : PairId → Bool) (a : Nat) :
    ((some a, none) ∈ outerRef l r pass) ↔
      a ∈ List.range l ∧ ∀ j, j ∈ List.range r → pass (a, j) = false := by
  rw [outerRef, List.mem_append]
  constructor
  · intro h
    rcases h with hflat | htail
    · rw [List.mem_flatMap] at hflat
      obtain ⟨i, hi, hb⟩ := hflat
      dsimp only at hb
      by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
      · rw [if_pos he] at hb
        rw [List.mem_singleton] at hb
        rw [Prod.mk.injEq] at hb
        have hia := Option.some.inj hb.1
        subst hia
        have hnoneq : ∀ k ∈ List.range r, ¬pass (a, k) = true := by
          have hnil := (isEmpty_eq_true_iff_nil _).mp he
          rw [List.filter_eq_nil_iff] at hnil
          intro k hk hpk
          exact absurd hpk (hnil k hk)
        refine ⟨hi, fun j hj => ?_⟩
        cases hq : pass (a, j) with
        | true => exact absurd hq (hnoneq j hj)
        | false => rfl
      · rw [if_neg he] at hb
        rw [List.mem_map] at hb
        obtain ⟨k, _, heq⟩ := hb
        rw [Prod.mk.injEq] at heq
        cases heq.2
    · rw [List.mem_map] at htail
      obtain ⟨j, hj, heq⟩ := htail
      rw [Prod.mk.injEq] at heq
      cases heq.1
  · rintro ⟨ha, hall⟩
    left
    rw [List.mem_flatMap]
    refine ⟨a, ha, ?_⟩
    dsimp only
    by_cases he : ((List.range r).filter (fun k => pass (a, k))).isEmpty = true
    · rw [if_pos he]
      rw [List.mem_singleton]
    · have hnil : (List.range r).filter (fun k => pass (a, k)) = [] := by
        rw [List.filter_eq_nil_iff]
        intro k hk hpk
        rw [hall k hk] at hpk
        cases hpk
      exact absurd ((isEmpty_eq_true_iff_nil _).mpr hnil) he

private theorem mem_outer_none_some (l r : Nat) (pass : PairId → Bool) (b : Nat) :
    ((none, some b) ∈ outerRef l r pass) ↔
      b ∈ List.range r ∧ (List.range l).any (fun i => pass (i, b)) = false := by
  rw [outerRef, List.mem_append]
  constructor
  · intro h
    rcases h with hflat | htail
    · rw [List.mem_flatMap] at hflat
      obtain ⟨i, hi, hb⟩ := hflat
      dsimp only at hb
      by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
      · rw [if_pos he] at hb
        rw [List.mem_singleton] at hb
        rw [Prod.mk.injEq] at hb
        cases hb.1
      · rw [if_neg he] at hb
        rw [List.mem_map] at hb
        obtain ⟨k, _, heq⟩ := hb
        rw [Prod.mk.injEq] at heq
        cases heq.1
    · rw [List.mem_map] at htail
      obtain ⟨j, hmem, heq⟩ := htail
      rw [Prod.mk.injEq] at heq
      have hjb := Option.some.inj heq.2
      subst hjb
      obtain ⟨hj1, hj2⟩ := List.mem_filter.mp hmem
      refine ⟨hj1, ?_⟩
      cases hb : (List.range l).any (fun i => pass (i, j)) with
      | true => rw [hb] at hj2; cases hj2
      | false => rfl
  · rintro ⟨hjr, hjn⟩
    right
    rw [List.mem_map]
    refine ⟨b, List.mem_filter.mpr ⟨hjr, ?_⟩, rfl⟩
    rw [hjn]
    rfl

private theorem not_mem_outer_none_none (l r : Nat) (pass : PairId → Bool) :
    ¬((none, none) ∈ outerRef l r pass) := by
  rw [outerRef, List.mem_append]
  intro h
  rcases h with hflat | htail
  · rw [List.mem_flatMap] at hflat
    obtain ⟨i, _, hb⟩ := hflat
    dsimp only at hb
    by_cases he : ((List.range r).filter (fun k => pass (i, k))).isEmpty = true
    · rw [if_pos he] at hb
      rw [List.mem_singleton] at hb
      rw [Prod.mk.injEq] at hb
      cases hb.1
    · rw [if_neg he] at hb
      rw [List.mem_map] at hb
      obtain ⟨k, _, heq⟩ := hb
      rw [Prod.mk.injEq] at heq
      cases heq.1
  · rw [List.mem_map] at htail
    obtain ⟨j, _, heq⟩ := htail
    rw [Prod.mk.injEq] at heq
    cases heq.2

theorem sound : Goal := by
  intro l r width order pass hw hperm
  dsimp only
  apply count_eq_of_nodup_mem
  · exact nodup_finish l r _ (nodup_hits l r width order pass hperm)
  · exact nodup_outer l r pass
  · intro o
    cases o with
    | mk x y =>
      cases x with
      | none =>
        cases y with
        | none =>
          refine ⟨fun h => absurd h (not_mem_finish_none_none l r _),
            fun h => absurd h (not_mem_outer_none_none l r pass)⟩
        | some b =>
          rw [mem_finish_none_some, mem_outer_none_some, not_any_snd, not_any_pass]
          constructor
          · rintro ⟨hr, hall⟩
            refine ⟨hr, fun a ha => ?_⟩
            cases hq : pass (a, b) with
            | true =>
              exact absurd ((mem_hits l r width order pass hw hperm a b).mpr
                ⟨List.mem_range.mp ha, List.mem_range.mp hr, hq⟩) (hall a)
            | false => rfl
          · rintro ⟨hr, hall⟩
            refine ⟨hr, fun a ha => ?_⟩
            have h := (mem_hits l r width order pass hw hperm a b).mp ha
            obtain ⟨h1, h2, hp⟩ := h
            rw [hall a (List.mem_range.mpr h1)] at hp
            cases hp
      | some a =>
        cases y with
        | none =>
          rw [mem_finish_some_none, mem_outer_some_none, not_any_fst]
          constructor
          · rintro ⟨hl, hall⟩
            refine ⟨hl, fun j hj => ?_⟩
            cases hq : pass (a, j) with
            | true =>
              exact absurd ((mem_hits l r width order pass hw hperm a j).mpr
                ⟨List.mem_range.mp hl, List.mem_range.mp hj, hq⟩) (hall j)
            | false => rfl
          · rintro ⟨hl, hall⟩
            refine ⟨hl, fun b hb => ?_⟩
            have h := (mem_hits l r width order pass hw hperm a b).mp hb
            obtain ⟨h1, h2, hp⟩ := h
            rw [hall b (List.mem_range.mpr h2)] at hp
            cases hp
        | some b =>
          rw [mem_finish_some_some, mem_outer_some_some,
            mem_hits l r width order pass hw hperm a b]
          exact ⟨fun ⟨h1, h2, hp⟩ => ⟨List.mem_range.mpr h1, List.mem_range.mpr h2, hp⟩,
            fun ⟨h1, h2, hp⟩ => ⟨List.mem_range.mp h1, List.mem_range.mp h2, hp⟩⟩

end TinylambOpt.ResearchProofs.EngineDecisions.Exec_HotRunTiles
