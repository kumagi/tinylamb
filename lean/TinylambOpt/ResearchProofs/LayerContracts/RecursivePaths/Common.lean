import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

instance instLawfulBEqEdge : LawfulBEq Edge where
  eq_of_beq := by
    intro a b h
    cases a with | mk a1 a2 a3 =>
    cases b with | mk b1 b2 b3 =>
    simp [BEq.beq, instBEqEdge.beq] at h
    obtain ⟨h1, h2, h3⟩ := h
    subst h1; subst h2; subst h3; rfl
  rfl := by
    intro a
    cases a with | mk a1 a2 a3 =>
    simp [BEq.beq, instBEqEdge.beq]

def Ex (m : Model) (g : List Edge) (S : Node → Prop) (y : Node) : Prop :=
  ∃ p, S p ∧ ∃ e ∈ g, e.src = p.1 ∧ m.next p.2 e.label = some y.2 ∧ e.dst = y.1

theorem mem_expand (m : Model) (g : List Edge) (w : List Node) (y : Node) :
    y ∈ expand m g w ↔ Ex m g (fun p => p ∈ w) y := by
  obtain ⟨y1, y2⟩ := y
  simp only [expand, Ex, List.mem_flatMap, List.mem_filterMap]
  constructor
  · rintro ⟨⟨v, q⟩, hp, e, he, h⟩
    by_cases hs : (e.src == v) = true
    · simp only [hs, ite_true] at h
      obtain ⟨a, ha, hy⟩ := Option.map_eq_some_iff.mp h
      simp only [Prod.mk.injEq] at hy
      obtain ⟨h1, h2⟩ := hy
      refine ⟨(v, q), hp, e, he, ?_, ?_, ?_⟩
      · simpa using hs
      · simpa [h2] using ha
      · simpa using h1
    · simp [hs] at h
  · rintro ⟨⟨v, q⟩, hp, e, he, h1, h2, h3⟩
    refine ⟨(v, q), hp, e, he, ?_⟩
    have hs : (e.src == v) = true := by simpa using h1
    simp only [hs, ite_true]
    simp only at h2 h3
    simp [h2, h3]

theorem fold_ins (xs seen : List Node) :
    ∃ l', xs.foldl (insertNew id) seen = seen ++ l' ∧ ∀ y, y ∈ l' ↔ (y ∈ xs ∧ y ∉ seen) := by
  induction xs generalizing seen with
  | nil => exact ⟨[], by simp, by simp⟩
  | cons x xs ih =>
    by_cases hx : x ∈ seen
    · obtain ⟨l', h1, h2⟩ := ih seen
      refine ⟨l', ?_, ?_⟩
      · simp [List.foldl, insertNew, hx, h1]
      · intro y
        rw [h2 y]
        constructor
        · rintro ⟨a, b⟩; exact ⟨List.mem_cons_of_mem _ a, b⟩
        · rintro ⟨a, b⟩
          refine ⟨?_, b⟩
          rcases List.mem_cons.mp a with rfl | a
          · exact absurd hx b
          · exact a
    · obtain ⟨l', h1, h2⟩ := ih (seen ++ [x])
      refine ⟨x :: l', ?_, ?_⟩
      · have : insertNew id seen x = seen ++ [x] := by simp [insertNew, hx]
        simp [List.foldl, this, h1]
      · intro y
        simp only [List.mem_cons, h2, List.mem_append, List.mem_singleton]
        constructor
        · rintro (rfl | ⟨a, b⟩)
          · exact ⟨Or.inl rfl, hx⟩
          · exact ⟨Or.inr a, fun h => b (Or.inl h)⟩
        · rintro ⟨a, b⟩
          rcases a with rfl | a
          · exact Or.inl rfl
          · by_cases hyx : y = x
            · exact Or.inl hyx
            · exact Or.inr ⟨a, fun h => by rcases h with h | h | h; exact b h; exact hyx h; simp at h⟩

theorem Ex_congr (m : Model) (g : List Edge) {S S' : Node → Prop} (h : ∀ p, S p ↔ S' p) (y : Node) :
    Ex m g S y ↔ Ex m g S' y := by
  constructor
  · rintro ⟨p, hp, r⟩; exact ⟨p, (h p).mp hp, r⟩
  · rintro ⟨p, hp, r⟩; exact ⟨p, (h p).mpr hp, r⟩

theorem Ex_mono (m : Model) (g : List Edge) {S S' : Node → Prop} (h : ∀ p, S p → S' p) (y : Node) :
    Ex m g S y → Ex m g S' y := by
  rintro ⟨p, hp, r⟩; exact ⟨p, h p hp, r⟩

def Step (m : Model) (g : List Edge) (S : Node → Prop) : Node → Prop := fun y => S y ∨ Ex m g S y

def It (m : Model) (g : List Edge) : Nat → (Node → Prop) → (Node → Prop)
  | 0, S => S
  | n+1, S => It m g n (Step m g S)

theorem It_succ (m : Model) (g : List Edge) (n : Nat) : ∀ S, It m g (n+1) S = Step m g (It m g n S) := by
  induction n with
  | zero => intro S; rfl
  | succ n ih =>
    intro S
    show It m g (n+1) (Step m g S) = Step m g (It m g (n+1) S)
    rw [ih]; rfl

theorem It_fix (m : Model) (g : List Edge) (S : Node → Prop) (h : ∀ y, Ex m g S y → S y) :
    ∀ n, It m g n S = S := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
    have : Step m g S = S := funext fun y => propext ⟨fun hy => hy.elim id (h y), Or.inl⟩
    show It m g n (Step m g S) = S
    rw [this]; exact ih

theorem slow_mem (m : Model) (g : List Edge) (s : Node) :
    ∀ n y, y ∈ slow m g s n ↔ It m g n (fun p => p = s) y := by
  intro n
  induction n with
  | zero => intro y; simp [slow, It]
  | succ n ih =>
    intro y
    rw [It_succ]
    simp only [slow, List.mem_eraseDups, List.mem_append, Step]
    rw [ih y, mem_expand]
    have := Ex_congr m g (S := fun p => p ∈ slow m g s n) (S' := It m g n (fun p => p = s)) ih y
    rw [this]

def Inv (m : Model) (g : List Edge) (seen wave : List Node) : Prop :=
  (∀ y ∈ wave, y ∈ seen) ∧
  (∀ y, Ex m g (fun p => p ∈ seen) y → y ∈ seen ∨ Ex m g (fun p => p ∈ wave) y)

theorem walk_mem (m : Model) (g : List Edge) :
    ∀ n seen wave, Inv m g seen wave → ∀ y, y ∈ walk m g id n seen wave ↔ It m g n (fun p => p ∈ seen) y := by
  intro n
  induction n with
  | zero => intro seen wave _ y; simp [walk, It]
  | succ n ih =>
    intro seen wave hinv y
    obtain ⟨hw, hex⟩ := hinv
    obtain ⟨l', hf, hl'⟩ := fold_ins (expand m g wave) seen
    simp only [walk]
    rw [hf]
    by_cases hn : l' = []
    · subst hn
      have hfix : ∀ y, Ex m g (fun p => p ∈ seen) y → y ∈ seen := by
        intro y hy
        rcases hex y hy with h | h
        · exact h
        · refine Classical.byContradiction fun hns => ?_
          have h3 := (hl' y).mpr ⟨(mem_expand m g wave y).mpr h, hns⟩
          exact absurd h3 (by simp)
      simp only [List.append_nil, beq_self_eq_true, ite_true]
      rw [It_fix m g _ hfix]
    · have hlen : ((seen ++ l').length == seen.length) = false := by
        simp only [List.length_append, beq_eq_false_iff_ne, ne_eq]
        intro h0
        exact hn (List.eq_nil_of_length_eq_zero (by omega))
      rw [hlen]
      simp only [Bool.false_eq_true, ite_false]
      rw [List.drop_left]
      have hinv' : Inv m g (seen ++ l') l' := by
        constructor
        · intro y hy; exact List.mem_append_right _ hy
        · intro y hy
          obtain ⟨p, hp, r⟩ := hy
          rcases List.mem_append.mp hp with hp | hp
          · rcases hex y ⟨p, hp, r⟩ with h | h
            · exact Or.inl (List.mem_append_left _ h)
            · by_cases hys : y ∈ seen
              · exact Or.inl (List.mem_append_left _ hys)
              · exact Or.inl (List.mem_append_right _ ((hl' y).mpr ⟨(mem_expand m g wave y).mpr h, hys⟩))
          · exact Or.inr ⟨p, hp, r⟩
      rw [ih _ _ hinv' y]
      have hset : (fun p => p ∈ seen ++ l') = Step m g (fun p => p ∈ seen) := by
        funext p
        apply propext
        have hE : ∀ q, Ex m g (fun p => p ∈ wave) q → Ex m g (fun p => p ∈ seen) q :=
          fun q hq => Ex_mono m g (S := fun p => p ∈ wave) (S' := fun p => p ∈ seen) hw q hq
        constructor
        · intro hp
          rcases List.mem_append.mp hp with hp | hp
          · exact Or.inl hp
          · exact Or.inr (hE p ((mem_expand m g wave p).mp ((hl' p).mp hp).1))
        · intro hp
          rcases hp with hp | hp
          · exact List.mem_append_left _ hp
          · rcases hex p hp with h | h
            · exact List.mem_append_left _ h
            · by_cases hps : p ∈ seen
              · exact List.mem_append_left _ hps
              · exact List.mem_append_right _ ((hl' p).mpr ⟨(mem_expand m g wave p).mpr h, hps⟩)
      rw [hset]; rfl

theorem Ex_g_congr (m : Model) {g g' : List Edge} (h : ∀ e, e ∈ g ↔ e ∈ g') (S : Node → Prop) (y : Node) :
    Ex m g S y ↔ Ex m g' S y := by
  constructor
  · rintro ⟨p, hp, e, he, r⟩; exact ⟨p, hp, e, (h e).mp he, r⟩
  · rintro ⟨p, hp, e, he, r⟩; exact ⟨p, hp, e, (h e).mpr he, r⟩

theorem It_g_congr (m : Model) {g g' : List Edge} (h : ∀ e, e ∈ g ↔ e ∈ g') (n : Nat) :
    ∀ S, It m g n S = It m g' n S := by
  induction n with
  | zero => intro S; rfl
  | succ n ih =>
    intro S
    have : Step m g S = Step m g' S :=
      funext fun y => propext (or_congr Iff.rfl (Ex_g_congr m h S y))
    show It m g n (Step m g S) = It m g' n (Step m g' S)
    rw [this]; exact ih _

theorem query_mem (m : Model) (g : List Edge) (v n x : Nat) :
    x ∈ query m g v n id ↔ ∃ q, It m g n (fun p => p = (v,0)) (x,q) ∧ m.accept q = true := by
  have hinv : Inv m g [(v,0)] [(v,0)] := ⟨fun y hy => hy, fun y hy => Or.inr hy⟩
  have hw := walk_mem m g n [(v,0)] [(v,0)] hinv
  have hS : (fun p : Node => p ∈ [(v,0)]) = (fun p => p = (v,0)) := by
    funext p; apply propext; simp
  rw [hS] at hw
  simp only [query, List.mem_map, List.mem_filter]
  constructor
  · rintro ⟨⟨a, q⟩, ⟨hp, hq⟩, rfl⟩
    exact ⟨q, (hw _).mp hp, hq⟩
  · rintro ⟨q, hp, hq⟩
    exact ⟨(x, q), ⟨(hw _).mpr hp, hq⟩, rfl⟩

theorem deltaOnly_gen (m : Model) : deltaOnly m := by
  intro g v n x
  rw [query_mem]
  constructor
  · rintro ⟨q, hq, ha⟩; exact ⟨q, (slow_mem m g _ n _).mpr hq, ha⟩
  · rintro ⟨q, hq, ha⟩; exact ⟨q, (slow_mem m g _ n _).mp hq, ha⟩

theorem edgeDedup_gen (m : Model) : edgeDedup m := by
  intro g v n x
  rw [query_mem, query_mem]
  have h : ∀ e, e ∈ g.eraseDups ↔ e ∈ g := fun e => List.mem_eraseDups
  rw [It_g_congr m h]


open Classical in
noncomputable def cnt (S : Node → Prop) : List Node → Nat
  | [] => 0
  | x :: xs => (if S x then 1 else 0) + cnt S xs

theorem cnt_le (S : Node → Prop) (L : List Node) : cnt S L ≤ L.length := by
  induction L with
  | nil => simp [cnt]
  | cons a L ih =>
    unfold cnt
    simp only [List.length_cons]
    split <;> omega

theorem cnt_mono (S S' : Node → Prop) (h : ∀ y, S y → S' y) (L : List Node) : cnt S L ≤ cnt S' L := by
  induction L with
  | nil => simp [cnt]
  | cons a L ih =>
    unfold cnt
    by_cases ha : S a
    · have := h a ha
      simp only [ha, this, ite_true]; omega
    · by_cases ha' : S' a
      · simp only [ha, ha', ite_true, ite_false]; omega
      · simp only [ha, ha', ite_false]; omega

theorem cnt_lt (S S' : Node → Prop) (h : ∀ y, S y → S' y) (x : Node) (L : List Node)
    (hx : x ∈ L) (h1 : ¬ S x) (h2 : S' x) : cnt S L < cnt S' L := by
  induction L with
  | nil => simp at hx
  | cons a L ih =>
    unfold cnt
    rcases List.mem_cons.mp hx with rfl | hx
    · simp only [h1, h2, ite_true, ite_false]
      have := cnt_mono S S' h L
      omega
    · have := ih hx
      by_cases ha : S a
      · have := h a ha
        simp only [ha, this, ite_true]; omega
      · by_cases ha' : S' a
        · simp only [ha, ha', ite_true, ite_false]; omega
        · simp only [ha, ha', ite_false]; omega

theorem It_mono (m : Model) (g : List Edge) (S : Node → Prop) (k : Nat) :
    ∀ y, It m g k S y → It m g (k+1) S y := by
  intro y hy
  rw [It_succ]
  exact Or.inl hy

theorem It_stable (m : Model) (g : List Edge) (S : Node → Prop) (k : Nat)
    (h : It m g (k+1) S = It m g k S) : ∀ j, It m g (k+j) S = It m g k S := by
  intro j
  induction j with
  | zero => rfl
  | succ j ih =>
    show It m g ((k+j)+1) S = _
    rw [It_succ, ih, ← It_succ]; exact h

theorem vertices_mem_src (g : List Edge) (v : Nat) (e : Edge) (he : e ∈ g) : e.src ∈ vertices g v := by
  simp only [vertices, List.mem_eraseDups, List.mem_cons, List.mem_flatMap]
  exact Or.inr ⟨e, he, by simp⟩

theorem vertices_mem_dst (g : List Edge) (v : Nat) (e : Edge) (he : e ∈ g) : e.dst ∈ vertices g v := by
  simp only [vertices, List.mem_eraseDups, List.mem_cons, List.mem_flatMap]
  exact Or.inr ⟨e, he, by simp⟩

theorem vertices_mem_self (g : List Edge) (v : Nat) : v ∈ vertices g v := by
  simp [vertices]

theorem It_fst (m : Model) (g : List Edge) (v : Nat) :
    ∀ k p, It m g k (fun p => p = (v,0)) p → p.1 ∈ vertices g v := by
  intro k
  induction k with
  | zero => intro p hp; subst hp; exact vertices_mem_self g v
  | succ k ih =>
    intro p hp
    rw [It_succ] at hp
    rcases hp with hp | ⟨p', hp', e, he, h1, h2, h3⟩
    · exact ih p hp
    · rw [← h3]; exact vertices_mem_dst g v e he

def univ (m : Model) (g : List Edge) (v : Nat) : List Node :=
  (vertices g v).flatMap (fun a => (List.range m.states).map (fun q => (a, q)))

theorem length_univ (m : Model) (g : List Edge) (v : Nat) :
    (univ m g v).length = (vertices g v).length * m.states := by
  unfold univ
  generalize vertices g v = L
  induction L with
  | nil => simp
  | cons a L ih =>
    simp only [List.flatMap_cons, List.length_append, List.length_map, List.length_range, ih, List.length_cons]
    rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]

theorem mem_univ (m : Model) (g : List Edge) (v : Nat) (p : Node) :
    p ∈ univ m g v ↔ p.1 ∈ vertices g v ∧ p.2 < m.states := by
  obtain ⟨a, q⟩ := p
  simp [univ, List.mem_flatMap, List.mem_map, List.mem_range]

theorem productDepth_of_stateDomain (m : Model) (hs : stateDomain m) : productDepth m := by
  intro g v n x
  simp only []
  rw [query_mem, query_mem]
  have hlen := length_univ m g v
  generalize (vertices g v).length * m.states = B at hlen ⊢
  have hsub : ∀ k p, It m g k (fun p => p = (v,0)) p → p ∈ univ m g v := by
    intro k p hp
    rw [mem_univ]
    refine ⟨It_fst m g v k p hp, ?_⟩
    obtain ⟨a, q⟩ := p
    have : (a, q) ∈ slow m g (v,0) k := (slow_mem m g (v,0) k _).mpr hp
    exact hs g v k a q this
  have hstart : (v,0) ∈ univ m g v := by
    apply hsub 0; rfl
  have hc0 : 1 ≤ cnt (It m g 0 (fun p => p = (v,0))) (univ m g v) := by
    have := cnt_lt (fun _ => False) (It m g 0 (fun p => p = (v,0))) (fun y hy => hy.elim) (v,0) _ hstart (fun h => h) rfl
    omega
  have hchain : ∀ k, (∀ j, j < k → It m g j (fun p => p = (v,0)) ≠ It m g (j+1) (fun p => p = (v,0))) →
      k + 1 ≤ cnt (It m g k (fun p => p = (v,0))) (univ m g v) := by
    intro k
    induction k with
    | zero => intro _; simpa using hc0
    | succ k ih =>
      intro hne
      have h1 := ih (fun j hj => hne j (by omega))
      have hne' := hne k (by omega)
      have hex : ∃ x, It m g (k+1) (fun p => p = (v,0)) x ∧ ¬ It m g k (fun p => p = (v,0)) x := by
        refine Classical.byContradiction fun hno => hne' ?_
        funext y
        apply propext
        constructor
        · intro hy; exact It_mono m g _ k y hy
        · intro hy
          exact Classical.byContradiction fun hy' => hno ⟨y, hy, hy'⟩
      obtain ⟨x0, hx1, hx2⟩ := hex
      have hlt := cnt_lt (It m g k (fun p => p = (v,0))) (It m g (k+1) (fun p => p = (v,0)))
        (It_mono m g _ k) x0 _ (hsub _ _ hx1) hx2 hx1
      omega
  have hexj : ∃ j, j ≤ B ∧ It m g j (fun p => p = (v,0)) = It m g (j+1) (fun p => p = (v,0)) := by
    refine Classical.byContradiction fun hno => ?_
    have := hchain (B+1) (fun j hj hEq => hno ⟨j, by omega, hEq⟩)
    have h2 := cnt_le (It m g (B+1) (fun p => p = (v,0))) (univ m g v)
    omega
  obtain ⟨j, hj, hEq⟩ := hexj
  have hst := It_stable m g _ j hEq.symm
  have e1 : It m g (B+n) (fun p => p = (v,0)) = It m g j (fun p => p = (v,0)) := by
    have : B + n = j + (B - j + n) := by omega
    rw [this]; exact hst _
  have e2 : It m g B (fun p => p = (v,0)) = It m g j (fun p => p = (v,0)) := by
    have : B = j + (B - j) := by omega
    conv => lhs; rw [this]
    exact hst _
  rw [e1, e2]

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic
