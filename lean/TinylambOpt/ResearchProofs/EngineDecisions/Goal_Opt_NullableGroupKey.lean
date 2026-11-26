import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_NullableGroupKey

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.NullableGroupKey

private def dep (xs : List Row) : Prop :=
  ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2

private def qpred (a : Row) (e : Row × Nat) : Bool := ((e.1.1 : Option Nat) == a.1)

/-- Core fact: under the dependency, `compact` preserves and reflects equality
of rows occurring in `xs`. -/
private theorem compact_beq (xs : List Row) (h : dep xs) :
    ∀ a ∈ xs, ∀ b ∈ xs, (compact a == compact b) = (a == b) := by
  intro a ha b hb
  cases a with
  | mk a1 a2 =>
      cases b with
      | mk b1 b2 =>
          cases a1 with
          | none =>
              cases b1 with
              | none => rfl
              | some v => cases a2 <;> cases b2 <;> rfl
          | some v =>
              cases b1 with
              | none => cases a2 <;> cases b2 <;> rfl
              | some w =>
                  by_cases heq : v = w
                  · subst heq
                    have this2 := h (some v, a2) ha (some v, b2) hb
                    simp at this2
                    subst this2
                    simp
                  · have key : (((some v, a2) : Option Nat × Option Nat) == (some w, b2))
                        = (((some v : Option Nat) == some w) && (a2 == b2)) := rfl
                    have key2 : (compact (some v, a2) == compact (some w, b2))
                        = (((some v : Option Nat) == some w) && ((none : Option Nat) == none)) := rfl
                    rw [key, key2]
                    cases hbq : (some v : Option Nat) == (some w : Option Nat) with
                    | true =>
                        have hv : v = w := by
                          have hbeq : ((some v : Option Nat) == some w) = (v == w) := rfl
                          rw [hbeq] at hbq
                          simpa using hbq
                        exact absurd hv heq
                    | false => simp

/-- Fuel-indexed deduplication with a custom key-equality predicate. -/
private def eraseDupsBy.go {α : Type} (p : α → α → Bool) : Nat → List α → List α
  | 0, _ => []
  | _+1, [] => []
  | n+1, a :: as => a :: eraseDupsBy.go p n (as.filter fun b => !(p b a))

private def eraseDupsBy {α : Type} (p : α → α → Bool) (l : List α) : List α :=
  eraseDupsBy.go p l.length l

private theorem eraseDupsBy.go_length {α : Type} (p : α → α → Bool) :
    ∀ (m : Nat) (l : List α) (n : Nat), l.length ≤ m → l.length ≤ n →
      eraseDupsBy.go p m l = eraseDupsBy.go p n l := by
  intro m
  induction m with
  | zero =>
      intro l n hm _
      cases l with
      | nil => cases n <;> rfl
      | cons a as =>
          simp only [List.length_cons] at hm
          omega
  | succ m ih =>
      intro l n hle hn
      cases l with
      | nil => cases n <;> rfl
      | cons a as =>
          cases n with
          | zero =>
              simp only [List.length_cons] at hn
              omega
          | succ n' =>
              show a :: eraseDupsBy.go p m (as.filter fun b => !(p b a))
                = a :: eraseDupsBy.go p n' (as.filter fun b => !(p b a))
              refine congrArg (a :: ·) (ih _ _ ?_ ?_)
              · refine Nat.le_trans (List.length_filter_le _ _) ?_
                simp only [List.length_cons] at hle
                omega
              · refine Nat.le_trans (List.length_filter_le _ _) ?_
                simp only [List.length_cons] at hn
                omega

private theorem eraseDupsBy_cons {α : Type} (p : α → α → Bool) (a : α) (as : List α) :
    eraseDupsBy p (a :: as) = a :: eraseDupsBy p (as.filter fun b => !(p b a)) := by
  have h1 : eraseDupsBy p (a :: as)
      = a :: eraseDupsBy.go p as.length (as.filter fun b => !(p b a)) := rfl
  have h2 : eraseDupsBy.go p as.length (as.filter fun b => !(p b a))
      = eraseDupsBy p (as.filter fun b => !(p b a)) := by
    refine Eq.trans (eraseDupsBy.go_length p _ _ _ ?_ (Nat.le_refl _)) rfl
    exact Nat.le_trans (List.length_filter_le _ _) (Nat.le_refl _)
  rw [h1, h2]

private theorem eraseDupsBy_congr : ∀ {α : Type} (l : List α) (p q : α → α → Bool),
    (∀ a ∈ l, ∀ b ∈ l, p a b = q a b) → eraseDupsBy p l = eraseDupsBy q l := by
  intro α
  have key : ∀ (n : Nat) (l : List α) (p q : α → α → Bool),
      l.length ≤ n → (∀ a ∈ l, ∀ b ∈ l, p a b = q a b) → eraseDupsBy p l = eraseDupsBy q l := by
    intro n
    induction n with
    | zero =>
        intro l p q hl _
        cases l with
        | nil => rfl
        | cons a as =>
            simp only [List.length_cons] at hl
            omega
    | succ m ih =>
        intro l p q hle h
        cases l with
        | nil => rfl
        | cons a as =>
            have hfilt : as.filter (fun b => !(p b a)) = as.filter (fun b => !(q b a)) := by
              refine List.filter_congr ?_
              intro x hx
              exact congrArg (fun z => !z) (h x (List.mem_cons_of_mem a hx) a (by simp))
            have has : as.length ≤ m := by
              have h1 : (a :: as).length = as.length + 1 := rfl
              simp only [h1] at hle
              omega
            have hF : (as.filter (fun b => !(q b a))).length ≤ m :=
              Nat.le_trans (List.length_filter_le _ _) has
            rw [eraseDupsBy_cons, eraseDupsBy_cons, hfilt]
            refine congrArg (a :: ·) ?_
            refine ih _ p q hF ?_
            intro x hx y hy
            exact h x (List.mem_cons_of_mem a (List.mem_filter.mp hx).1)
              y (List.mem_cons_of_mem a (List.mem_filter.mp hy).1)
  intro l p q h
  exact key l.length l p q (Nat.le_refl _) h

private theorem eraseDupsBy_eq_eraseDups : ∀ {α : Type} [BEq α] (l : List α),
    eraseDupsBy (fun x y => x == y) l = l.eraseDups := by
  intro α inst l
  have key : ∀ (n : Nat) (l : List α), l.length ≤ n →
      eraseDupsBy (fun x y => x == y) l = l.eraseDups := by
    intro n
    induction n with
    | zero =>
        intro l hl
        cases l with
        | nil => rfl
        | cons a as =>
            simp only [List.length_cons] at hl
            omega
    | succ m ih =>
        intro l hl
        cases l with
        | nil => rfl
        | cons a as =>
            have hfilt : (as.filter fun b => !(b == a)).length ≤ m := by
              refine Nat.le_trans (List.length_filter_le _ _) ?_
              have h1 : (a :: as).length = as.length + 1 := rfl
              simp only [h1] at hl
              omega
            rw [eraseDupsBy_cons, List.eraseDups_cons]
            refine congrArg (a :: ·) ?_
            exact ih _ hfilt
  exact key l.length l (Nat.le_refl _)

private theorem eraseDups_map_compact : ∀ (l : List Row),
    (l.map compact).eraseDups = (eraseDupsBy (fun x y => compact x == compact y) l).map compact := by
  have key : ∀ (n : Nat) (l : List Row), l.length ≤ n →
      (l.map compact).eraseDups = (eraseDupsBy (fun x y => compact x == compact y) l).map compact := by
    intro n
    induction n with
    | zero =>
        intro l hl
        cases l with
        | nil => rfl
        | cons a as =>
            simp only [List.length_cons] at hl
            omega
    | succ m ih =>
        intro l hl
        cases l with
        | nil => rfl
        | cons a as =>
            have has : as.length ≤ m := by
              have h1 : (a :: as).length = as.length + 1 := rfl
              simp only [h1] at hl
              omega
            have hfilt : (as.filter fun b => !(compact b == compact a)).length ≤ m :=
              Nat.le_trans (List.length_filter_le _ _) has
            have hfilter : (as.map compact).filter (fun b => !(b == compact a))
                = (as.filter (fun b => !(compact b == compact a))).map compact := by
              rw [List.filter_map]
              refine congrArg (List.map compact) (List.filter_congr ?_)
              intro b _
              rfl
            rw [List.map_cons, List.eraseDups_cons, hfilter]
            rw [ih _ hfilt]
            rw [eraseDupsBy_cons, List.map_cons]
  intro l
  exact key l.length l (Nat.le_refl _)

/-- The distinct-key lists coincide up to `compact`. -/
private theorem heads_eq (xs : List Row) (h : dep xs) :
    (xs.map compact).eraseDups = (xs.map id).eraseDups.map compact := by
  have hcb : ∀ a ∈ xs, ∀ b ∈ xs, (fun x y => compact x == compact y) a b = (fun x y => x == y) a b :=
    compact_beq xs h
  have h1 : eraseDupsBy (fun x y => compact x == compact y) xs
      = eraseDupsBy (fun x y => x == y) xs := eraseDupsBy_congr xs _ _ hcb
  rw [eraseDups_map_compact, h1, eraseDupsBy_eq_eraseDups]
  rw [List.map_id]

/-- `groups` factored into the distinct-key list and the entry computation. -/
private def GRep (xs : List Row) (k : Row → Row) (K : Row) : Option (Row × Nat) :=
  match xs.find? (fun r => k r == K) with
  | none => none
  | some rep => some (rep, xs.countP (fun r => k r == K))

private theorem grep_none (xs : List Row) (k : Row → Row) (K : Row)
    (hf : xs.find? (fun r => k r == K) = none) : GRep xs k K = none := by
  unfold GRep
  rw [hf]

private theorem grep_val (xs : List Row) (k : Row → Row) (K : Row) (rep : Row)
    (hf : xs.find? (fun r => k r == K) = some rep) :
    GRep xs k K = some (rep, xs.countP (fun r => k r == K)) := by
  unfold GRep
  rw [hf]

private theorem groups_decomp (xs : List Row) (k : Row → Row) :
    groups xs k = ((xs.map k).eraseDups).filterMap (GRep xs k) := by
  unfold groups
  refine congrArg (fun f => List.filterMap f ((xs.map k).eraseDups)) ?_
  funext K
  cases hf : xs.find? (fun r => k r == K) with
  | none => rw [grep_none xs k K hf]; rfl
  | some rep => rw [grep_val xs k K rep hf]; rfl

private theorem find?_congr' {α : Type} : ∀ (l : List α) (p q : α → Bool),
    (∀ a ∈ l, p a = q a) → l.find? p = l.find? q := by
  intro l
  induction l with
  | nil => intro p q _; rfl
  | cons a as ih =>
      intro p q h
      have ha := h a (by simp)
      cases hpa : p a with
      | true =>
          have hqa : q a = true := by
            rw [← ha]
            exact hpa
          rw [List.find?_cons_of_pos hpa, List.find?_cons_of_pos hqa]
      | false =>
          have hqa : q a = false := by
            rw [← ha]
            exact hpa
          rw [List.find?_cons_of_neg (by simpa using hpa),
            List.find?_cons_of_neg (by simpa using hqa)]
          refine ih p q ?_
          intro x hx
          exact h x (List.mem_cons_of_mem a hx)

private theorem countP_congr' {α : Type} : ∀ (l : List α) (p q : α → Bool),
    (∀ a ∈ l, p a = q a) → l.countP p = l.countP q := by
  intro l
  induction l with
  | nil => intro p q _; rfl
  | cons a as ih =>
      intro p q h
      have ha := h a (by simp)
      rw [List.countP_cons, List.countP_cons]
      rw [ih p q (by intro x hx; exact h x (by simp [hx])), ha]

/-- Entries computed from `id` keys coincide with those from their `compact` image. -/
private theorem grep_agree (xs : List Row) (h : dep xs) :
    ∀ K ∈ (xs.map id).eraseDups, GRep xs compact (compact K) = GRep xs id K := by
  intro K hK
  have hK' : K ∈ xs := by rwa [List.mem_eraseDups, List.map_id] at hK
  have hcb := compact_beq xs h
  have hpred : ∀ r ∈ xs, (fun x => compact x == compact K) r = (fun x => x == K) r := by
    intro r hr
    exact hcb r hr K hK'
  unfold GRep
  simp only [id]
  have hc : xs.countP (fun r => compact r == compact K)
      = xs.countP (fun r => r == K) := countP_congr' xs _ _ hpred
  have hf : xs.find? (fun r => compact r == compact K)
      = xs.find? (fun r => r == K) := find?_congr' xs _ _ hpred
  rw [hf, hc]

private theorem fm_map {α β γ : Type} (f : α → β) : ∀ (l : List α) (g : β → Option γ),
    (l.map f).filterMap g = l.filterMap (fun x => g (f x)) := by
  intro l
  induction l with
  | nil => intro g; rfl
  | cons a as ih =>
      intro g
      rw [List.filterMap_cons, List.map_cons, List.filterMap_cons, ih g]

private theorem filterMap_congr' {α β : Type} : ∀ (l : List α) (f g : α → Option β),
    (∀ a ∈ l, f a = g a) → l.filterMap f = l.filterMap g := by
  intro l
  induction l with
  | nil => intro f g _; rfl
  | cons a as ih =>
      intro f g h
      rw [List.filterMap_cons, List.filterMap_cons]
      rw [ih f g (by intro x hx; exact h x (List.mem_cons_of_mem a hx)), h a (by simp)]

private theorem filter_filterMap {α β : Type} : ∀ (l : List α) (E : α → Option β)
    (q : β → Bool) (P : α → Bool),
    (∀ K ∈ l, (E K).all q = P K) → (l.filterMap E).filter q = (l.filter P).filterMap E := by
  intro l
  induction l with
  | nil => intro E q P _; rfl
  | cons a as ih =>
      intro E q P h
      have ha := h a (by simp)
      cases hE : E a with
      | none =>
          have hpa : P a = true := by simpa [hE] using ha
          simp [hE, hpa, ih E q P (by intro x hx; exact h x (by simp [hx]))]
      | some v =>
          have hq : q v = P a := by simpa [hE] using ha
          by_cases hp : P a
          · simp [hE, hp, hq, ih E q P (by intro x hx; exact h x (by simp [hx]))]
          · simp [hE, hp, hq, ih E q P (by intro x hx; exact h x (by simp [hx]))]

private theorem find?_some_imp {α : Type} : ∀ (l : List α) (p : α → Bool) (a : α),
    l.find? p = some a → p a = true := by
  intro l
  induction l with
  | nil => intro p a h; simp at h
  | cons x xs ih =>
      intro p a h
      cases hpx : p x with
      | true =>
          rw [List.find?_cons_of_pos hpx] at h
          cases h
          exact hpx
      | false =>
          rw [List.find?_cons_of_neg (by simpa using hpx)] at h
          exact ih p a h

private theorem find?_self {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (l : List α) (a : α), a ∈ l → l.find? (fun r => r == a) = some a := by
  intro l
  induction l with
  | nil => intro a h; cases h
  | cons x xs ih =>
      intro a ha
      rw [List.mem_cons] at ha
      rcases ha with heq | ha
      · subst heq
        rw [List.find?_cons_of_pos (by simp)]
      · cases hxa : (x == a) with
        | true =>
            have hxa' : x = a := by simpa using hxa
            rw [hxa']
            rw [List.find?_cons_of_pos (by simp)]
        | false =>
            rw [List.find?_cons_of_neg (by simpa using hxa)]
            exact ih a ha

private theorem exists_append_of_mem' {α : Type} (w : α) :
    ∀ {l : List α}, w ∈ l → ∃ s t, l = s ++ w :: t := by
  intro l
  induction l with
  | nil => intro h; contradiction
  | cons a as ih =>
      intro h
      rw [List.mem_cons] at h
      rcases h with heq | h
      · subst heq
        exact ⟨[], as, rfl⟩
      · obtain ⟨s, t, hs⟩ := ih h
        exact ⟨a :: s, t, by rw [List.cons_append, hs]⟩

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      simp [List.mem_eraseDups, nodup_eraseDups (as.filter fun b => ! b == a)]
  termination_by l.length
  decreasing_by
    simp_wf
    have hle := List.length_filter_le (fun b : α => ! b == a) as
    omega

private theorem filter_eq_single {α : Type} (w : α) (p : α → Bool) :
    ∀ (l : List α), w ∈ l → l.Nodup → (∀ x ∈ l, p x = true → x = w) →
      l.filter p = if p w then [w] else [] := by
  intro l
  induction l with
  | nil =>
      intro h _ _
      exact absurd h (by simp)
  | cons a as ih =>
      intro hm hnd hall
      cases hpa : p a with
      | true =>
          have heq : a = w := hall a (by simp) hpa
          subst heq
          have hs : as.filter p = [] := by
            refine List.filter_eq_nil_iff.mpr ?_
            intro x hx hx'
            exact (List.nodup_cons.mp hnd).1 (((hall x (by simp [hx]) hx').symm) ▸ hx)
          rw [List.filter_cons_of_pos hpa, hs]
          simp [hpa]
      | false =>
          rw [List.filter_cons_of_neg (by simpa using hpa)]
          rw [List.mem_cons] at hm
          rcases hm with heq | ht
          · subst heq
            rw [hpa]
            have hif : (if (false : Bool) = true then [w] else []) = ([] : List α) := by simp
            rw [hif]
            refine List.filter_eq_nil_iff.mpr ?_
            intro x hx hx'
            obtain ⟨hna, _⟩ := List.nodup_cons.mp hnd
            have hxe : x = w := hall x (List.mem_cons_of_mem w hx) hx'
            exact hna (hxe ▸ hx)
          · refine ih ht (List.nodup_cons.mp hnd).2 ?_
            intro x hx hx'
            exact hall x (List.mem_cons_of_mem a hx) hx'

private theorem compact_fst (r : Row) : (compact r).1 = r.1 := by
  cases r with
  | mk k d => cases k <;> simp [compact]

theorem sound : Goal := by
  intro xs
  constructor
  · -- Perm → dependency
    intro hperm
    apply Classical.byContradiction
    intro hnd
    rcases Classical.not_forall.1 hnd with ⟨a, ha⟩
    rcases Classical.not_imp.1 ha with ⟨ha, ha'⟩
    rcases Classical.not_forall.1 ha' with ⟨b, hb⟩
    rcases Classical.not_imp.1 hb with ⟨hb, hb'⟩
    rcases Classical.not_imp.1 hb' with ⟨hs, h12⟩
    rcases Classical.not_imp.1 h12 with ⟨heq, hne⟩
    have hab : a ≠ b := by
      intro hcon
      cases hcon
      exact hne rfl
    have ha1 : a.1.isSome = true := hs
    have hqa : qpred a (a, xs.countP (fun r => r == a)) = true := by
      simp [qpred]
    have hqb : qpred a (b, xs.countP (fun r => r == b)) = true := by
      simp only [qpred]
      rw [← heq]
      simp
    have hmem_a : (a, xs.countP (fun r => r == a)) ∈ groups xs id := by
      rw [groups_decomp, List.map_id]
      refine List.mem_filterMap.mpr ⟨a, ?_, ?_⟩
      · rw [List.mem_eraseDups]
        exact ha
      · exact grep_val xs id a a (find?_self xs a ha)
    have hmem_b : (b, xs.countP (fun r => r == b)) ∈ groups xs id := by
      rw [groups_decomp, List.map_id]
      refine List.mem_filterMap.mpr ⟨b, ?_, ?_⟩
      · rw [List.mem_eraseDups]
        exact hb
      · exact grep_val xs id b b (find?_self xs b hb)
    have hid : 2 ≤ ((groups xs id).filter (qpred a)).length := by
      obtain ⟨s, t, hst⟩ := exists_append_of_mem' _ hmem_a
      have hbs : (b, xs.countP (fun r => r == b)) ∈ (s ++ t).filter (qpred a) := by
        rw [List.mem_filter]
        refine ⟨?_, hqb⟩
        rw [List.mem_append]
        rw [hst] at hmem_b
        rcases List.mem_append.mp hmem_b with hsb | htb
        · exact Or.inl hsb
        · rcases List.mem_cons.mp htb with hcon | hat
          · exact absurd ((congrArg Prod.fst hcon).symm) hab
          · exact Or.inr hat
      rw [List.filter_append] at hbs
      have h1 := List.length_pos_of_mem hbs
      rw [List.length_append] at h1
      rw [hst, List.filter_append, List.filter_cons_of_pos hqa,
        List.length_append, List.length_cons]
      omega
    -- compact side: exactly one entry can carry the first component `a.1`
    have hca : compact a = (a.1, none) := by
      cases a with
      | mk k d =>
          cases k with
          | none => simp at ha1
          | some v => simp [compact]
    have hkey : (a.1, none) ∈ (xs.map compact).eraseDups := by
      rw [List.mem_eraseDups]
      exact List.mem_map.mpr ⟨a, ha, hca⟩
    have huniq : ∀ x ∈ (xs.map compact).eraseDups, (x.1 == a.1) = true → x = (a.1, none) := by
      intro x hx hxb
      have hx1 : x.1 = a.1 := by
        cases h1 : x.1 with
        | none =>
            cases h2 : a.1 with
            | none => rfl
            | some w =>
                rw [h1, h2] at hxb
                simp at hxb
        | some v =>
            cases h2 : a.1 with
            | none =>
                rw [h1, h2] at hxb
                simp at hxb
            | some w =>
                rw [h1, h2] at hxb
                have hvw : v = w := by simpa using hxb
                subst hvw
                rfl
      rcases List.mem_map.mp (List.mem_eraseDups.mp hx) with ⟨s, hs', rfl⟩
      have hsome : s.1 = a.1 := by
        rw [← compact_fst s]
        exact hx1
      have hcs : compact s = (s.1, none) := by
        cases s with
        | mk k d =>
            cases k with
            | none =>
                rw [← hsome] at ha1
                simp at ha1
            | some v => simp [compact]
      rw [hcs, hsome]
    have hall' : ∀ K ∈ (xs.map compact).eraseDups,
        (GRep xs compact K).all (qpred a) = true → K = (a.1, none) := by
      intro K hK hq
      refine huniq K hK ?_
      unfold GRep at hq
      cases hf : xs.find? (fun r => compact r == K) with
      | none =>
          have hsome : (xs.find? (fun r => compact r == K)).isSome = true := by
            rw [List.find?_isSome]
            rcases List.mem_map.mp (List.mem_eraseDups.mp hK) with ⟨s, hs, rfl⟩
            exact ⟨s, hs, by simp⟩
          rw [hf] at hsome
          simp at hsome
      | some rep =>
          have hrep : (fun r => compact r == K) rep = true :=
            find?_some_imp (l := xs) (p := (fun r => compact r == K)) (a := rep) hf
          have hce : compact rep = K := by simpa using hrep
          simp [hf, qpred] at hq ⊢
          have hrfst : rep.1 = K.1 := by
            rw [← compact_fst rep, ← hce]
          rw [← hrfst]
          exact hq
    cases hf : xs.find? (fun r => compact r == (a.1, none)) with
    | none =>
        have hsome : (xs.find? (fun r => compact r == (a.1, none))).isSome = true := by
          rw [List.find?_isSome]
          exact ⟨a, ha, by rw [hca]; simp⟩
        rw [hf] at hsome
        simp at hsome
    | some rep =>
        have hG : GRep xs compact (a.1, none)
            = some (rep, xs.countP (fun r => compact r == (a.1, none))) := by
          unfold GRep
          rw [hf]
        have hrep : (fun r => compact r == (a.1, none)) rep = true :=
          find?_some_imp (l := xs) (p := (fun r => compact r == (a.1, none))) (a := rep) hf
        have hce : compact rep = (a.1, none) := by simpa using hrep
        have hrfst : rep.1 = a.1 := by
          rw [← compact_fst rep, hce]
        have hq2 : qpred a (rep, xs.countP (fun r => compact r == (a.1, none))) = true := by
          simp [qpred, hrfst]
        have hP1 : (fun K => (GRep xs compact K).all (qpred a)) (a.1, none) = true := by
          show (GRep xs compact (a.1, none)).all (qpred a) = true
          rw [hG]
          simp [hq2]
        have hnd' : ((xs.map compact).eraseDups).Nodup := nodup_eraseDups _
        have hcount1 : ((groups xs compact).filter (qpred a)).length = 1 := by
          rw [groups_decomp,
            filter_filterMap (l := (xs.map compact).eraseDups) (E := GRep xs compact)
              (q := qpred a) (P := fun K => (GRep xs compact K).all (qpred a))
              (fun K _ => rfl)]
          rw [filter_eq_single (a.1, none) (fun K => (GRep xs compact K).all (qpred a))
            (xs.map compact).eraseDups hkey hnd' hall']
          simp [hG, hq2]
        have hpermF : ((groups xs compact).filter (qpred a)).Perm
            ((groups xs id).filter (qpred a)) := List.Perm.filter (p := qpred a) hperm
        have hlen := List.Perm.length_eq hpermF
        rw [hcount1] at hlen
        omega
  · -- dependency → Perm
    intro hdep
    refine List.Perm.of_eq ?_
    have hhead : (xs.map compact).eraseDups = (xs.map id).eraseDups.map compact :=
      heads_eq xs hdep
    rw [groups_decomp, groups_decomp, hhead, fm_map]
    refine filterMap_congr' _ _ _ ?_
    intro K hK
    exact grep_agree xs hdep K hK

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_NullableGroupKey
