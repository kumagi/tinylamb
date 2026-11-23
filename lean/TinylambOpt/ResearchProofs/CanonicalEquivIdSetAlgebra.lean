import TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra

namespace TinylambOpt.ResearchProofs.CanonicalEquivIdSetAlgebra

open TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra

private theorem not_mem_erase_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    {a : α} {l : List α} (h : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp [List.erase]
  | cons b t ih =>
      have hb : b ∉ t := (List.nodup_cons.mp h).1
      have ht : t.Nodup := (List.nodup_cons.mp h).2
      by_cases heq : (b == a) = true
      · have hba : b = a := beq_iff_eq.mp heq
        subst b
        simpa [List.erase_cons_head] using hb
      · rw [List.erase_cons_tail heq]
        simp only [List.mem_cons]
        rintro (hab | ha)
        · exact heq (beq_iff_eq.mpr hab.symm)
        · exact ih ht ha

private theorem perm_of_nodup_of_mem {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        rw [List.eq_nil_iff_forall_not_mem]
        intro x hx
        simpa using (hmem x).mpr hx
      subst l₂
      exact List.Perm.refl []
  | cons a t ih =>
      have hat : a ∉ t := (List.nodup_cons.mp h₁).1
      have ht : t.Nodup := (List.nodup_cons.mp h₁).2
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      have he2 : (l₂.erase a).Nodup := h₂.erase a
      have hme : ∀ x, x ∈ t ↔ x ∈ l₂.erase a := by
        intro x
        constructor
        · intro hx
          have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx])
          have hxa : x ≠ a := by
            intro heq
            subst x
            exact hat hx
          exact (List.mem_erase_of_ne hxa).mpr hx2
        · intro hx
          have hxa : x ≠ a := by
            intro heq
            subst x
            exact (not_mem_erase_self_of_nodup h₂) hx
          have hx2 : x ∈ l₂ := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using (hmem x).mpr hx2
      have hp : t.Perm (l₂.erase a) := ih ht he2 hme
      exact (List.Perm.cons a hp).trans (List.perm_cons_erase ha2).symm

private theorem length_of_nodup_of_mem {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.length = l₂.length :=
  (perm_of_nodup_of_mem h₁ h₂ hmem).length_eq

private theorem nodup_filter {α : Type} {p : α → Bool} {l : List α}
    (h : l.Nodup) : (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have ha : a ∉ t := (List.nodup_cons.mp h).1
      have ht : t.Nodup := (List.nodup_cons.mp h).2
      have iht : (t.filter p).Nodup := ih ht
      by_cases hp : p a = true
      · simp [hp, List.nodup_cons, ha, iht]
      · simpa [hp] using iht

private theorem perm_append_singleton {α : Type} (l : List α) (a : α) :
    (l ++ [a]).Perm (a :: l) := by
  induction l with
  | nil => simp
  | cons b t ih =>
      exact (List.Perm.cons b ih).trans (List.Perm.swap a b t)

private theorem foldl_insertUniq_nodup {α : Type} [BEq α] [LawfulBEq α]
    (xs acc : List α) (hacc : acc.Nodup) :
    (xs.foldl insertUniq acc).Nodup := by
  induction xs generalizing acc with
  | nil => simpa using hacc
  | cons x xs ih =>
      apply ih
      unfold insertUniq
      by_cases hc : acc.contains x
      · rw [ite_eq_left hc]
        exact hacc
      · rw [ite_eq_right hc]
        have hxacc : x ∉ acc := by
          intro h
          exact hc (List.contains_iff_mem.mpr h)
        have hnodx : (x :: acc).Nodup := List.nodup_cons.mpr ⟨hxacc, hacc⟩
        exact (perm_append_singleton acc x).symm.nodup hnodx

private theorem foldl_insertUniq_mem {α : Type} [BEq α] [LawfulBEq α]
    (xs acc : List α) (y : α) :
    y ∈ xs.foldl insertUniq acc ↔ y ∈ acc ∨ y ∈ xs := by
  induction xs generalizing acc with
  | nil => simp
  | cons x xs ih =>
      change y ∈ xs.foldl insertUniq (insertUniq acc x) ↔ y ∈ acc ∨ y ∈ x :: xs
      rw [ih]
      have hstep : y ∈ insertUniq acc x ↔ y ∈ acc ∨ y = x := by
        unfold insertUniq
        by_cases hc : acc.contains x
        · rw [ite_eq_left hc]
          constructor
          · intro hy
            exact Or.inl hy
          · intro hy
            rcases hy with hy | rfl
            · exact hy
            · exact List.contains_iff_mem.mp hc
        · rw [ite_eq_right hc]
          simp
      rw [hstep]
      simp [or_assoc]

private theorem dedupList_nodup {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) : (dedupList xs).Nodup := by
  unfold dedupList
  exact foldl_insertUniq_nodup xs [] (by simp)

private theorem mem_dedupList {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (y : α) : y ∈ dedupList xs ↔ y ∈ xs := by
  unfold dedupList
  simpa using (foldl_insertUniq_mem xs [] y)

private theorem mem_except_filter {β : Type} [BEq β] [LawfulBEq β]
    (x : β) (s t : List β) :
    ((!s.contains x && !t.contains x) = true ↔ x ∉ s ∧ x ∉ t) := by
  simp [List.contains_eq_mem]

private theorem canonIntersectCount {β : Type} [BEq β] [LawfulBEq β]
    (s1 s2 s3 : List β) :
    ((dedupList s1).filter (fun x => s2.contains x && s3.contains x)).length =
      (let c3 := dedupList s3
       let c2 := dedupList (s2.filter (fun x => c3.contains x))
       (dedupList (s1.filter (fun x => c2.contains x))).length) := by
  dsimp only
  let c3 := dedupList s3
  let c2 := dedupList (s2.filter (fun x => c3.contains x))
  let Lref := (dedupList s1).filter (fun x => s2.contains x && s3.contains x)
  let Lopt := dedupList (s1.filter (fun x => c2.contains x))
  apply length_of_nodup_of_mem
  · exact nodup_filter (dedupList_nodup s1)
  · exact dedupList_nodup _
  · intro x
    have hc2 : ∀ z, z ∈ c2 ↔ z ∈ s2 ∧ z ∈ c3 := by
      intro z
      simp [c2, mem_dedupList, List.mem_filter]
    have hc3 : ∀ z, z ∈ c3 ↔ z ∈ s3 := by
      intro z
      exact mem_dedupList s3 z
    have hLref : x ∈ Lref ↔ x ∈ s1 ∧ x ∈ s2 ∧ x ∈ s3 := by
      simp [Lref, mem_dedupList, List.mem_filter, Bool.and_eq_true]
    have hLopt : x ∈ Lopt ↔ x ∈ s1 ∧ x ∈ s2 ∧ x ∈ s3 := by
      simp [Lopt, mem_dedupList, List.mem_filter, hc2, hc3]
    exact hLref.trans hLopt.symm

private theorem canonExceptCount {β : Type} [BEq β] [LawfulBEq β]
    (s1 s2 s3 : List β) :
    ((dedupList s1).filter (fun x => !s2.contains x && !s3.contains x)).length =
      (let c2 := dedupList s2
       let c3 := dedupList s3
       let c1 := dedupList s1
       (c1.filter (fun p => !c2.contains p && !c3.contains p)).length) := by
  dsimp only
  let c2 := dedupList s2
  let c3 := dedupList s3
  let c1 := dedupList s1
  let Lref := (dedupList s1).filter (fun x => !s2.contains x && !s3.contains x)
  let Lopt := c1.filter (fun p => !c2.contains p && !c3.contains p)
  apply length_of_nodup_of_mem
  · exact nodup_filter (dedupList_nodup s1)
  · exact nodup_filter (dedupList_nodup s1)
  · intro x
    have hLref : x ∈ Lref ↔ x ∈ s1 ∧ x ∉ s2 ∧ x ∉ s3 := by
      simp [Lref, mem_dedupList, List.mem_filter]
    have hLopt : x ∈ Lopt ↔ x ∈ s1 ∧ x ∉ s2 ∧ x ∉ s3 := by
      simp [Lopt, c1, c2, c3, mem_dedupList, List.mem_filter]
    exact hLref.trans hLopt.symm

private theorem nodup_map_of_injective {α β : Type} (f : α → β)
    (hf : Function.Injective f) {l : List α} (h : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have ha : a ∉ t := (List.nodup_cons.mp h).1
      have ht : t.Nodup := (List.nodup_cons.mp h).2
      apply List.nodup_cons.mpr
      constructor
      · simp only [List.mem_map]
        rintro ⟨z, hz, hza⟩
        have hza' : z = a := hf hza
        subst z
        exact ha hz
      · exact ih ht

private theorem length_eq_of_mem_image {α β : Type} [BEq α] [LawfulBEq α]
    [BEq β] [LawfulBEq β] (f : α → β) (hf : Function.Injective f)
    {l₁ : List α} {l₂ : List β} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hmem : ∀ y, y ∈ l₂ ↔ ∃ x, x ∈ l₁ ∧ f x = y) :
    l₁.length = l₂.length := by
  have hm : (l₁.map f).Nodup := nodup_map_of_injective f hf h₁
  have hmem' : ∀ y, y ∈ l₁.map f ↔ y ∈ l₂ := by
    intro y
    rw [List.mem_map]
    exact (hmem y).symm
  calc
    l₁.length = (l₁.map f).length := by simp
    _ = l₂.length := (perm_of_nodup_of_mem hm h₂ hmem').length_eq

private def projectPairs {γ : Type} (d : List DimRow) (g : Option String → γ)
    (f : List FactRow) : List (γ × Nat) :=
  f.flatMap fun (sk, dt) =>
    d.filterMap fun (dsk, attr) =>
      if sk == dsk then some (g attr, dt) else none

private theorem join_eq_project (d : List DimRow) (f : List FactRow) :
    joinProjectAttrDate f d = projectPairs d id f := by
  rfl

private theorem canon_eq_project (d : List DimRow) (canon : Option String → Nat)
    (f : List FactRow) :
    toCanonPairs d canon f = projectPairs d canon f := by
  rfl

private theorem mem_projectPairs {γ : Type} (d : List DimRow)
    (g : Option String → γ) (f : List FactRow) (p : γ × Nat) :
    p ∈ projectPairs d g f ↔ ∃ (sk dt : Nat) (attr : Option String),
      (sk, dt) ∈ f ∧ (sk, attr) ∈ d ∧ g attr = p.1 ∧ dt = p.2 := by
  simp only [projectPairs, List.mem_flatMap, List.mem_filterMap]
  constructor
  · rintro ⟨⟨sk, dt⟩, hf, ⟨⟨dsk, attr⟩, hd, hif⟩⟩
    by_cases heq : sk == dsk
    · have hdsk : dsk = sk := (beq_iff_eq.mp heq).symm
      have hp : (g attr, dt) = p := by
        simp [heq] at hif
        exact hif
      refine ⟨sk, dt, attr, hf, ?_, ?_, ?_⟩
      · simpa [hdsk] using hd
      · exact congrArg Prod.fst hp
      · exact congrArg Prod.snd hp
    · simp [heq] at hif
  · rintro ⟨sk, dt, attr, hf, hd, hg, hdt⟩
    refine ⟨(sk, dt), hf, (sk, attr), hd, ?_⟩
    have hp : (g attr, dt) = p := by
      apply Prod.ext
      · exact hg
      · exact hdt
    simpa using hp

private theorem mem_canon_of_mem_ref (d : List DimRow) (canon : Option String → Nat)
    (f : List FactRow) (hcanon : ∀ a b, canon a = canon b ↔ a = b)
    (x : Option String × Nat) (y : Nat × Nat) (hy : (canon x.1, x.2) = y) :
    y ∈ toCanonPairs d canon f ↔ x ∈ joinProjectAttrDate f d := by
  rw [canon_eq_project, join_eq_project, mem_projectPairs, mem_projectPairs]
  constructor
  · rintro ⟨sk, dt, attr, hf, hd, hcy, hdt⟩
    have hattr : attr = x.1 :=
      (hcanon attr x.1).mp (hcy.trans (congrArg Prod.fst hy).symm)
    refine ⟨sk, dt, attr, hf, hd, ?_, ?_⟩
    · change attr = x.1
      exact hattr
    · exact hdt.trans (congrArg Prod.snd hy).symm
  · rintro ⟨sk, dt, attr, hf, hd, hattr, hdt⟩
    have hattr' : attr = x.1 := by
      change attr = x.1 at hattr
      exact hattr
    refine ⟨sk, dt, attr, hf, hd, ?_, ?_⟩
    · rw [hattr']
      exact congrArg Prod.fst hy
    · exact hdt.trans (congrArg Prod.snd hy)

theorem sound : Goal := by
  constructor
  · intro d canon f1 f2 f3 hcanon
    let φ : Option String × Nat → Nat × Nat := fun p => (canon p.1, p.2)
    have hφ : Function.Injective φ := by
      intro a b hab
      apply Prod.ext
      · have hfst : (φ a).1 = (φ b).1 := congrArg Prod.fst hab
        exact (hcanon a.1 b.1).mp (by simpa [φ] using hfst)
      · have hsnd : (φ a).2 = (φ b).2 := congrArg Prod.snd hab
        simpa [φ] using hsnd
    let s1 := joinProjectAttrDate f1 d
    let s2 := joinProjectAttrDate f2 d
    let s3 := joinProjectAttrDate f3 d
    let t1 := toCanonPairs d canon f1
    let t2 := toCanonPairs d canon f2
    let t3 := toCanonPairs d canon f3
    have exists_canon : ∀ (f : List FactRow) (y : Nat × Nat),
        y ∈ toCanonPairs d canon f ↔ ∃ (sk dt : Nat) (attr : Option String),
          (sk, dt) ∈ f ∧ (sk, attr) ∈ d ∧ canon attr = y.1 ∧ dt = y.2 := by
      intro f y
      rw [canon_eq_project, mem_projectPairs]
    have witness_ref {f : List FactRow} {sk dt : Nat} {attr : Option String}
        (hf : (sk, dt) ∈ f) (hd : (sk, attr) ∈ d) :
        (attr, dt) ∈ joinProjectAttrDate f d := by
      rw [join_eq_project, mem_projectPairs]
      exact ⟨sk, dt, attr, hf, hd, rfl, rfl⟩
    have inter_ref_eq_canon :
        (setIntersect s1 s2 s3).length = (setIntersect t1 t2 t3).length := by
      unfold setIntersect
      let Lref := (dedupList s1).filter (fun x => s2.contains x && s3.contains x)
      let Lcan := (dedupList t1).filter (fun y => t2.contains y && t3.contains y)
      have hnr : Lref.Nodup := nodup_filter (dedupList_nodup s1)
      have hnc : Lcan.Nodup := nodup_filter (dedupList_nodup t1)
      apply length_eq_of_mem_image φ hφ hnr hnc
      intro y
      constructor
      · intro hy
        have hy' : y ∈ dedupList t1 ∧ t2.contains y = true ∧ t3.contains y = true := by
          simpa [Lcan, List.mem_filter, Bool.and_eq_true] using hy
        have hy1 : y ∈ t1 := (mem_dedupList t1 y).mp hy'.1
        have hy2 : y ∈ t2 := List.contains_iff_mem.mp hy'.2.1
        have hy3 : y ∈ t3 := List.contains_iff_mem.mp hy'.2.2
        rcases (exists_canon f1 y).mp hy1 with ⟨sk1, dt1, attr1, hf1, hd1, hcy1, hdt1⟩
        let x : Option String × Nat := (attr1, dt1)
        have hφx : φ x = y := by
          apply Prod.ext
          · exact hcy1
          · exact hdt1
        have hx1 : x ∈ s1 := by
          dsimp [x, s1]
          exact witness_ref hf1 hd1
        rcases (exists_canon f2 y).mp hy2 with ⟨sk2, dt2, attr2, hf2, hd2, hcy2, hdt2⟩
        have hx2raw : (attr2, dt2) ∈ s2 := by
          dsimp [s2]
          exact witness_ref hf2 hd2
        have hxeq : x = (attr2, dt2) := by
          apply hφ
          apply Prod.ext
          · exact hcy1.trans hcy2.symm
          · exact hdt1.trans hdt2.symm
        have hx2 : x ∈ s2 := by simpa [hxeq] using hx2raw
        rcases (exists_canon f3 y).mp hy3 with ⟨sk3, dt3, attr3, hf3, hd3, hcy3, hdt3⟩
        have hx3raw : (attr3, dt3) ∈ s3 := by
          dsimp [s3]
          exact witness_ref hf3 hd3
        have hxeq3 : x = (attr3, dt3) := by
          apply hφ
          apply Prod.ext
          · exact hcy1.trans hcy3.symm
          · exact hdt1.trans hdt3.symm
        have hx3 : x ∈ s3 := by simpa [hxeq3] using hx3raw
        have hxL : x ∈ Lref := by
          simp [Lref, mem_dedupList, List.mem_filter, hx1, hx2, hx3]
        exact ⟨x, hxL, hφx⟩
      · rintro ⟨x, hxL, hφx⟩
        have hx' : x ∈ s1 ∧ s2.contains x = true ∧ s3.contains x = true := by
          simpa [Lref, mem_dedupList, List.mem_filter, Bool.and_eq_true] using hxL
        have hx1 : x ∈ s1 := hx'.1
        have hx2 : x ∈ s2 := List.contains_iff_mem.mp hx'.2.1
        have hx3 : x ∈ s3 := List.contains_iff_mem.mp hx'.2.2
        have hc1 : y ∈ t1 := (mem_canon_of_mem_ref d canon f1 hcanon x y hφx).mpr hx1
        have hc2 : y ∈ t2 := (mem_canon_of_mem_ref d canon f2 hcanon x y hφx).mpr hx2
        have hc3 : y ∈ t3 := (mem_canon_of_mem_ref d canon f3 hcanon x y hφx).mpr hx3
        simp [Lcan, List.mem_filter, mem_dedupList, hc1, hc2, hc3]
    have except_ref_eq_canon :
        (setExcept s1 s2 s3).length = (setExcept t1 t2 t3).length := by
      unfold setExcept
      let Lref := (dedupList s1).filter (fun x => !s2.contains x && !s3.contains x)
      let Lcan := (dedupList t1).filter (fun y => !t2.contains y && !t3.contains y)
      have hnr : Lref.Nodup := nodup_filter (dedupList_nodup s1)
      have hnc : Lcan.Nodup := nodup_filter (dedupList_nodup t1)
      apply length_eq_of_mem_image φ hφ hnr hnc
      intro y
      constructor
      · intro hy
        have hy' : y ∈ dedupList t1 ∧ (!t2.contains y && !t3.contains y) = true := by
          simpa [Lcan, List.mem_filter] using hy
        have hy1 : y ∈ t1 := (mem_dedupList t1 y).mp hy'.1
        have hnot := (mem_except_filter y t2 t3).mp hy'.2
        have hn2 : ¬ t2.contains y = true := by
          intro ht
          exact hnot.1 (List.contains_iff_mem.mp ht)
        have hn3 : ¬ t3.contains y = true := by
          intro ht
          exact hnot.2 (List.contains_iff_mem.mp ht)
        rcases (exists_canon f1 y).mp hy1 with ⟨sk1, dt1, attr1, hf1, hd1, hcy1, hdt1⟩
        let x : Option String × Nat := (attr1, dt1)
        have hφx : φ x = y := by
          apply Prod.ext
          · exact hcy1
          · exact hdt1
        have hx1 : x ∈ s1 := by
          dsimp [x, s1]
          exact witness_ref hf1 hd1
        have hxnot2 : x ∉ s2 := by
          intro hx2
          have : y ∈ t2 := (mem_canon_of_mem_ref d canon f2 hcanon x y hφx).mpr hx2
          exact hn2 (List.contains_iff_mem.mpr this)
        have hxnot3 : x ∉ s3 := by
          intro hx3
          have : y ∈ t3 := (mem_canon_of_mem_ref d canon f3 hcanon x y hφx).mpr hx3
          exact hn3 (List.contains_iff_mem.mpr this)
        have hxL : x ∈ Lref := by
          simp [Lref, mem_dedupList, List.mem_filter, hx1, hxnot2, hxnot3]
        exact ⟨x, hxL, hφx⟩
      · rintro ⟨x, hxL, hφx⟩
        have hx' : x ∈ s1 ∧ (!s2.contains x && !s3.contains x) = true := by
          simpa [Lref, mem_dedupList, List.mem_filter] using hxL
        have hx1 : x ∈ s1 := hx'.1
        have hxnot := (mem_except_filter x s2 s3).mp hx'.2
        have hxnot2 : x ∉ s2 := hxnot.1
        have hxnot3 : x ∉ s3 := hxnot.2
        have hc1 : y ∈ t1 := (mem_canon_of_mem_ref d canon f1 hcanon x y hφx).mpr hx1
        have hn2 : ¬ t2.contains y = true := by
          intro ht
          have hyt : y ∈ t2 := List.contains_iff_mem.mp ht
          rcases (exists_canon f2 y).mp hyt with ⟨sk2, dt2, attr2, hf2, hd2, hcy2, hdt2⟩
          let z : Option String × Nat := (attr2, dt2)
          have hφz : φ z = y := by
            apply Prod.ext
            · exact hcy2
            · exact hdt2
          have hzx : z = x := hφ (hφz.trans hφx.symm)
          have hx2 : x ∈ s2 := by
            have hz2 : z ∈ s2 := by
              dsimp [z, s2]
              exact witness_ref hf2 hd2
            simpa [hzx] using hz2
          exact hxnot2 hx2
        have hn3 : ¬ t3.contains y = true := by
          intro ht
          have hyt : y ∈ t3 := List.contains_iff_mem.mp ht
          rcases (exists_canon f3 y).mp hyt with ⟨sk3, dt3, attr3, hf3, hd3, hcy3, hdt3⟩
          let z : Option String × Nat := (attr3, dt3)
          have hφz : φ z = y := by
            apply Prod.ext
            · exact hcy3
            · exact hdt3
          have hzx : z = x := hφ (hφz.trans hφx.symm)
          have hx3 : x ∈ s3 := by
            have hz3 : z ∈ s3 := by
              dsimp [z, s3]
              exact witness_ref hf3 hd3
            simpa [hzx] using hz3
          exact hxnot3 hx3
        have hyn2 : y ∉ t2 := by
          intro hyt
          exact hn2 (List.contains_iff_mem.mpr hyt)
        have hyn3 : y ∉ t3 := by
          intro hyt
          exact hn3 (List.contains_iff_mem.mpr hyt)
        have hnot : (!t2.contains y && !t3.contains y) = true :=
          (mem_except_filter y t2 t3).mpr ⟨hyn2, hyn3⟩
        simpa [Lcan, List.mem_filter, mem_dedupList, hc1] using (And.intro hyn2 hyn3)
    constructor
    · unfold optCanonIntersectCount refIntersectCount
      dsimp only
      change (let c3 := dedupList t3
              let c2 := dedupList (t2.filter (fun p => c3.contains p))
              (dedupList (t1.filter (fun p => c2.contains p))).length) =
        (setIntersect (joinProjectAttrDate f1 d) (joinProjectAttrDate f2 d)
          (joinProjectAttrDate f3 d)).length
      simpa [s1, s2, s3] using
        ((canonIntersectCount t1 t2 t3).symm.trans inter_ref_eq_canon.symm)
    · unfold optCanonExceptCount refExceptCount
      dsimp only
      change (let c2 := dedupList t2
              let c3 := dedupList t3
              let c1 := dedupList t1
              (c1.filter (fun p => !c2.contains p && !c3.contains p)).length) =
        (setExcept (joinProjectAttrDate f1 d) (joinProjectAttrDate f2 d)
          (joinProjectAttrDate f3 d)).length
      simpa [s1, s2, s3] using
        ((canonExceptCount t1 t2 t3).symm.trans except_ref_eq_canon.symm)
  · refine ⟨[(1, some "Smith"), (2, some "Smith"), (3, none)],
      [(1, 100), (3, 200), (1, 100)], [(2, 100)], [(2, 100)], ?_⟩
    decide

end TinylambOpt.ResearchProofs.CanonicalEquivIdSetAlgebra
