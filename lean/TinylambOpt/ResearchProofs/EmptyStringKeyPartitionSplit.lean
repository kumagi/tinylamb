import TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit

/-!
Proved

- `Goal` — Empty-string partition split + cheap predicate reorder + exact int AVG
  (ClickBench Q17/Q22/Q31/Q32): (1) partitioning `GROUP BY (uid, phrase)` into
  `phrase = ""`, `phrase = NULL`, and `phrase ≠ ""` avoids string hashing on the hot
  empty partition and agrees with unified Top-K, (2) checking `phrase ≠ ""` before
  expensive `URL LIKE` preserves total boolean filtering while bounding substring
  checks to `phrase ≠ ""` rows, and (3) integer `(sum, cnt)` accumulation preserves
  `AVG(CAST(int AS FLOAT))`.

The reference model is defined in
`TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit`.
-/

namespace TinylambOpt.ResearchProofs.EmptyStringKeyPartitionSplit

open List
open TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit

abbrev Entry := PhraseKey × Nat

private def entryLe (a b : Entry) : Prop := leEntry a b = true

private structure Rank where
  cnt : Int
  tag : Nat
  uid : Int
  str : String

private def keyRank (k : PhraseKey) : Rank :=
  match k with
  | .emptyKey u => ⟨0, 0, u, ""⟩
  | .nonEmptyKey u s => ⟨0, 1, u, s⟩
  | .nullKey u => ⟨0, 2, u, ""⟩

private def entryRank (e : Entry) : Rank :=
  let r := keyRank e.1
  ⟨-(e.2 : Int), r.tag, r.uid, r.str⟩

private def rankLe (r s : Rank) : Prop :=
  r.cnt < s.cnt ∨ (r.cnt = s.cnt ∧
    (r.tag < s.tag ∨ (r.tag = s.tag ∧
      (r.uid < s.uid ∨ (r.uid = s.uid ∧ r.str ≤ s.str)))))

private theorem rankLe_total (r s : Rank) : rankLe r s ∨ rankLe s r := by
  rcases r with ⟨c, t, u, v⟩
  rcases s with ⟨c', t', u', v'⟩
  simp only [rankLe]
  by_cases hc : c = c'
  · subst c'
    by_cases ht : t = t'
    · subst t'
      by_cases hu : u = u'
      · subst u'
        simp
        exact (Std.le_total (a := v) (b := v'))
      · simp [hu]
        omega
    · simp [ht]
      omega
  · simp [hc]
    omega

private theorem rankLe_trans (r s q : Rank) (h1 : rankLe r s) (h2 : rankLe s q) : rankLe r q := by
  rcases r with ⟨c, t, u, v⟩
  rcases s with ⟨c', t', u', v'⟩
  rcases q with ⟨c'', t'', u'', v''⟩
  simp only [rankLe] at h1 h2 ⊢
  by_cases hc1 : c = c' <;> by_cases hc2 : c' = c'' <;> by_cases hc3 : c = c'' <;>
    by_cases ht1 : t = t' <;> by_cases ht2 : t' = t'' <;> by_cases ht3 : t = t'' <;>
    by_cases hu1 : u = u' <;> by_cases hu2 : u' = u'' <;> by_cases hu3 : u = u'' <;> simp_all
  · exact Std.le_trans h1 h2
  all_goals omega

private theorem rankLe_antisymm (r s : Rank) (h1 : rankLe r s) (h2 : rankLe s r) : r = s := by
  rcases r with ⟨c, t, u, v⟩
  rcases s with ⟨c', t', u', v'⟩
  simp only [rankLe] at h1 h2 ⊢
  by_cases hc : c = c' <;> by_cases ht : t = t' <;> by_cases hu : u = u' <;> simp_all
  · have : v = v' := Std.le_antisymm h1 h2
    subst v'
    rfl
  all_goals omega

private theorem leEntry_iff_rankLe (a b : Entry) :
    leEntry a b = true ↔ rankLe (entryRank a) (entryRank b) := by
  rcases a with ⟨ka, ca⟩
  rcases b with ⟨kb, cb⟩
  by_cases hcnt : ca = cb
  · subst cb
    cases ka with
    | nonEmptyKey ua sa =>
        cases kb with
        | nonEmptyKey ub sb =>
            simp [leEntry, entryRank, keyRank, rankLe, keyTag]
            by_cases hu : ua = ub <;> simp [hu] <;> omega
        | _ => simp [leEntry, entryRank, keyRank, rankLe, keyTag] <;> omega
    | _ => cases kb <;> simp [leEntry, entryRank, keyRank, rankLe, keyTag] <;> omega
  · have hne1 : (ca != cb) = true := by simp [hcnt]
    have hcnt' : ¬ cb = ca := fun h => hcnt h.symm
    have hne2 : (cb != ca) = true := by simp [hcnt']
    cases ka <;> cases kb <;> simp [leEntry, entryRank, keyRank, rankLe, hne1] <;> omega

private theorem entryLe_antisymm (a b : Entry) (h1 : entryLe a b) (h2 : entryLe b a) : a = b := by
  have r1 := (leEntry_iff_rankLe a b).mp h1
  have r2 := (leEntry_iff_rankLe b a).mp h2
  have hr := rankLe_antisymm (entryRank a) (entryRank b) r1 r2
  rcases a with ⟨ka, ca⟩
  rcases b with ⟨kb, cb⟩
  cases ka <;> cases kb <;> simp [entryRank, keyRank] at hr ⊢
  all_goals (try omega)
  all_goals (simp_all; omega)

private theorem entryLe_total (a b : Entry) : entryLe a b ∨ entryLe b a := by
  by_cases h : leEntry a b
  · exact Or.inl h
  · right
    have ht := rankLe_total (entryRank a) (entryRank b)
    rcases ht with ht | ht
    · exact False.elim (h ((leEntry_iff_rankLe a b).mpr ht))
    · exact (leEntry_iff_rankLe b a).mpr ht

private theorem entryLe_trans (a b c : Entry) (h1 : entryLe a b) (h2 : entryLe b c) : entryLe a c := by
  exact (leEntry_iff_rankLe a c).mpr
    (rankLe_trans _ _ _ ((leEntry_iff_rankLe a b).mp h1) ((leEntry_iff_rankLe b c).mp h2))

private theorem bool_not_true_eq_false {b : Bool} (h : ¬ b = true) : b = false := by
  cases b <;> simp_all

private theorem insertSortedE_cons_pos (x y : Entry) (ys : List Entry) (h : leEntry x y = true) :
    insertSortedE x (y :: ys) = x :: y :: ys := by
  simp [insertSortedE, h]

private theorem insertSortedE_cons_neg (x y : Entry) (ys : List Entry) (h : leEntry x y = false) :
    insertSortedE x (y :: ys) = y :: insertSortedE x ys := by
  simp [insertSortedE, h]

private theorem mem_insertSortedE {x : Entry} {l : List Entry} :
    x ∈ insertSortedE x l := by
  induction l with
  | nil => simp [insertSortedE]
  | cons y ys ih =>
      by_cases h : leEntry x y
      · simp [insertSortedE, h]
      · simp [insertSortedE, h, ih]

private theorem insertSortedE_perm (x : Entry) : ∀ l : List Entry, insertSortedE x l ~ x :: l := by
  intro l
  induction l with
  | nil => exact List.Perm.refl _
  | cons y ys ih =>
      by_cases h : leEntry x y
      · rw [insertSortedE_cons_pos _ _ _ h]
      · rw [insertSortedE_cons_neg _ _ _ (bool_not_true_eq_false h)]
        exact (List.Perm.cons y ih).trans (List.Perm.swap x y ys)

private theorem insertSortedE_pairwise (x : Entry) :
    ∀ l : List Entry, l.Pairwise entryLe → (insertSortedE x l).Pairwise entryLe := by
  intro l
  induction l with
  | nil => intro _; simp [insertSortedE]
  | cons y ys ih =>
      intro hp
      obtain ⟨hy, hys⟩ := List.pairwise_cons.mp hp
      by_cases h : leEntry x y
      · rw [insertSortedE_cons_pos _ _ _ h]
        refine List.pairwise_cons.mpr ⟨?_, List.pairwise_cons.mpr ⟨hy, hys⟩⟩
        intro z hz
        rcases List.mem_cons.mp hz with rfl | hzys
        · exact h
        · exact entryLe_trans x y z h (hy z hzys)
      · rw [insertSortedE_cons_neg _ _ _ (bool_not_true_eq_false h)]
        refine List.pairwise_cons.mpr ⟨?_, ih hys⟩
        intro z hz
        have hzmem : z ∈ x :: ys := (insertSortedE_perm x ys).mem_iff.mp hz
        rcases List.mem_cons.mp hzmem with hxz | hzys
        · subst z
          rcases entryLe_total x y with hxy | hyx
          · exact False.elim (h hxy)
          · exact hyx
        · exact hy z hzys

private theorem sortEntries_perm : ∀ l : List Entry, sortEntries l ~ l := by
  intro l
  induction l with
  | nil => exact List.Perm.refl _
  | cons x xs ih =>
      change insertSortedE x (sortEntries xs) ~ x :: xs
      exact (insertSortedE_perm x (sortEntries xs)).trans (List.Perm.cons x ih)

private theorem sortEntries_pairwise : ∀ l : List Entry, (sortEntries l).Pairwise entryLe := by
  intro l
  induction l with
  | nil => simp [sortEntries]
  | cons x xs ih => exact insertSortedE_pairwise x (sortEntries xs) ih

private theorem sortEntries_cong {l₁ l₂ : List Entry} (h : l₁ ~ l₂) : sortEntries l₁ = sortEntries l₂ := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ hab hba => entryLe_antisymm a b hab hba)
  · exact sortEntries_pairwise _
  · exact sortEntries_pairwise _
  · exact (sortEntries_perm l₁).trans (h.trans (sortEntries_perm l₂).symm)

private theorem beq_false_of_ne {α : Type} [BEq α] [LawfulBEq α] {a b : α} (h : a ≠ b) :
    (a == b) = false := by
  cases h' : a == b with
  | false => rfl
  | true => exact False.elim (h (beq_iff_eq.mp h'))

private def distinctKeys (m : List Entry) : Prop :=
  m.Pairwise (fun a b => a.1 ≠ b.1)

private theorem distinctKeys_perm {m₁ m₂ : List Entry} (hp : m₁ ~ m₂) :
    distinctKeys m₁ ↔ distinctKeys m₂ :=
  List.Perm.pairwise_iff (fun {a b : Entry} h heq => h heq.symm) hp

private theorem keys_addCount (m : List Entry) (k : PhraseKey) {b : Entry}
    (hb : b ∈ addCount m k) : b.1 = k ∨ b.1 ∈ m.map Prod.fst := by
  induction m with
  | nil =>
      simp [addCount] at hb
      left
      simpa using congrArg Prod.fst hb
  | cons p rest ih =>
      rcases p with ⟨k', c⟩
      by_cases h : (k' == k)
      · simp [addCount, h] at hb
        rcases hb with rfl | hb
        · left
          have := beq_iff_eq.mp h
          simpa [this]
        · right
          exact List.mem_map.mpr ⟨b, List.mem_cons_of_mem (k', c) hb, rfl⟩
      · simp [addCount, h] at hb
        rcases hb with rfl | hb
        · right
          simp
        · rcases ih hb with hEq | hmem
          · exact Or.inl hEq
          · exact Or.inr (by simp [hmem])

private theorem nodup_addCount (m : List Entry) (k : PhraseKey) (hnd : distinctKeys m) :
    distinctKeys (addCount m k) := by
  induction m with
  | nil => simp [addCount, distinctKeys]
  | cons p rest ih =>
      rcases p with ⟨k', c⟩
      have hnd_rest : distinctKeys rest := List.Pairwise.of_cons hnd
      have hkp_rest : ∀ q ∈ rest, k' ≠ q.1 := (List.pairwise_cons.mp hnd).1
      by_cases h : (k' == k)
      · have heq : k' = k := beq_iff_eq.mp h
        subst k'
        unfold distinctKeys at hnd_rest ⊢
        simp only [addCount, h, ↓reduceIte]
        refine List.pairwise_cons.mpr ⟨?_, hnd_rest⟩
        intro q hq
        exact hkp_rest q hq
      · unfold distinctKeys at hnd_rest ⊢
        have hkne : k' ≠ k := by
          intro heq
          subst k'
          simp at h
        have hneNew : ∀ q ∈ addCount rest k, (k', c).1 ≠ q.1 := by
          intro q hq heq
          rcases keys_addCount rest k hq with hqk | hqmem
          · exact hkne (heq.trans hqk)
          · rcases List.mem_map.mp hqmem with ⟨r, hr, hrk⟩
            exact hkp_rest r hr (heq.trans hrk.symm)
        simp only [addCount, h, ↓reduceIte]
        exact List.pairwise_cons.mpr ⟨hneNew, ih hnd_rest⟩

private theorem nodup_foldl_addCount (ks : List PhraseKey) (m : List Entry) (hnd : distinctKeys m) :
    distinctKeys (ks.foldl addCount m) := by
  induction ks generalizing m with
  | nil => exact hnd
  | cons k ks ih => exact ih (addCount m k) (nodup_addCount m k hnd)

private theorem addCount_perm_compat {m₁ m₂ : List Entry} (hp : m₁ ~ m₂) (k : PhraseKey) :
    distinctKeys m₁ → addCount m₁ k ~ addCount m₂ k := by
  refine List.Perm.rec (motive := fun l₁ l₂ hp => distinctKeys l₁ → addCount l₁ k ~ addCount l₂ k)
    ?_ ?_ ?_ ?_ hp
  · intro _
    rfl
  · intro p l₁ l₂ hp ih hnd
    rcases p with ⟨kp, cp⟩
    have hnd_tail : distinctKeys l₁ := List.Pairwise.of_cons hnd
    by_cases hk : (kp == k)
    · simp [addCount, hk]
      exact hp
    · simp [addCount, hk]
      exact ih hnd_tail
  · intro a b l hnd
    rcases a with ⟨ka, ca⟩
    rcases b with ⟨kb, cb⟩
    have hkane : kb ≠ ka := (List.pairwise_cons.mp hnd).1 (ka, ca) (by simp)
    by_cases hka : (ka == k)
    · have heq : ka = k := beq_iff_eq.mp hka
      have hkbne : kb ≠ k := by intro h; exact hkane (h.trans heq.symm)
      subst ka
      have hkb : (kb == k) = false := beq_false_of_ne hkbne
      simp [addCount, hka, hkb]
      exact List.Perm.swap (k, ca + 1) (kb, cb) l
    · by_cases hkb : (kb == k)
      · have heq : kb = k := beq_iff_eq.mp hkb
        subst kb
        have hkane' : ka ≠ k := fun h => hkane h.symm
        have hka' : (ka == k) = false := beq_false_of_ne hkane'
        simp [addCount, hka', hkb]
        exact List.Perm.swap (ka, ca) (k, cb + 1) l
      · simp [addCount, hka, hkb]
        exact List.Perm.swap (ka, ca) (kb, cb) (addCount l k)
  · intro l₁ l₂ l₃ h₁ h₂ ih₁ ih₂ hnd
    exact (ih₁ hnd).trans (ih₂ ((distinctKeys_perm h₁).mp hnd))

private theorem foldl_addCount_perm_of_acc :
    ∀ (ks : List PhraseKey) {m₁ m₂ : List Entry}, m₁ ~ m₂ → distinctKeys m₁ →
      (ks.foldl addCount m₁ ~ ks.foldl addCount m₂) := by
  intro ks
  induction ks with
  | nil => intro m₁ m₂ hp hnd; simpa
  | cons k ks ih =>
      intro m₁ m₂ hp hnd
      simp only [List.foldl_cons]
      exact ih (addCount_perm_compat hp k hnd) (nodup_addCount m₁ k hnd)

private theorem addCount_comm_perm (m : List Entry) (a b : PhraseKey) (hab : a ≠ b)
    (hnd : distinctKeys m) :
    addCount (addCount m a) b ~ addCount (addCount m b) a := by
  induction m with
  | nil =>
      have hba : b ≠ a := fun h => hab h.symm
      have habf : (a == b) = false := beq_false_of_ne hab
      have hbaf : (b == a) = false := beq_false_of_ne hba
      simp [addCount, habf, hbaf]
      exact List.Perm.swap _ _ _
  | cons p rest ih =>
      rcases p with ⟨kp, cp⟩
      have hnd_rest : distinctKeys rest := List.Pairwise.of_cons hnd
      by_cases hpa : (kp == a)
      · have heq : kp = a := beq_iff_eq.mp hpa
        subst kp
        have hpb : (a == b) = false := beq_false_of_ne hab
        have hba : b ≠ a := fun h => hab h.symm
        have hpb' : (b == a) = false := beq_false_of_ne hba
        simp [addCount, hpa, hpb, hpb']
      · by_cases hpb : (kp == b)
        · have heq : kp = b := beq_iff_eq.mp hpb
          subst kp
          have hpa' : (a == b) = false := beq_false_of_ne hab
          have hba : b ≠ a := fun h => hab h.symm
          have hpa'' : (b == a) = false := beq_false_of_ne hba
          simp [addCount, hpa', hpb, hpa'']
        · simp [addCount, hpa, hpb]
          exact ih hnd_rest

private theorem foldl_addCount_of_keyperm :
    ∀ {l₁ l₂ : List PhraseKey}, l₁ ~ l₂ →
    ∀ (m : List Entry), distinctKeys m → (l₁.foldl addCount m ~ l₂.foldl addCount m) := by
  intro l₁ l₂ hp
  refine List.Perm.rec (motive := fun a b hp => ∀ (m : List Entry), distinctKeys m →
      (a.foldl addCount m ~ b.foldl addCount m)) ?_ ?_ ?_ ?_ hp
  · intro m hnd
    rfl
  · intro x a b hp ih m hnd
    simp only [List.foldl_cons]
    exact ih (addCount m x) (nodup_addCount m x hnd)
  · intro a b l m hnd
    simp only [List.foldl_cons]
    by_cases hab : a = b
    · subst b
      rfl
    · exact foldl_addCount_perm_of_acc l (addCount_comm_perm m a b hab hnd).symm
        (nodup_addCount (addCount m b) a (nodup_addCount m b hnd))
  · intro l₁ l₂ l₃ h₁ h₂ ih₁ ih₂ m hnd
    exact (ih₁ m hnd).trans (ih₂ m hnd)

private def rowKeys (rows : List HitRow) : List PhraseKey :=
  rows.map (fun r => classifyKey r.uid r.phrase)

private def emptyRows (rows : List HitRow) : List HitRow :=
  rows.filter (fun r => r.phrase == some "")

private def nullRows (rows : List HitRow) : List HitRow :=
  rows.filter (fun r => r.phrase.isNone)

private def strRows (rows : List HitRow) : List HitRow :=
  rows.filter (fun r => isNonEmptyPhrase r.phrase)

private def optKeys (rows : List HitRow) : List PhraseKey :=
  (emptyRows rows).map (fun r => PhraseKey.emptyKey r.uid) ++
  (nullRows rows).map (fun r => PhraseKey.nullKey r.uid) ++
  (strRows rows).map (fun r => classifyKey r.uid r.phrase)

private theorem optKeys_none (uid : Int) (url : String) (val : Option Int) (rs : List HitRow) :
    optKeys ({uid := uid, phrase := none, url := url, val := val} :: rs) =
      (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
      (PhraseKey.nullKey uid ::
        ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
         (strRows rs).map (fun r => classifyKey r.uid r.phrase))) := by
  simp [optKeys, emptyRows, nullRows, strRows, isNonEmptyPhrase]

private theorem optKeys_empty (uid : Int) (url : String) (val : Option Int) (rs : List HitRow) :
    optKeys ({uid := uid, phrase := some "", url := url, val := val} :: rs) =
      PhraseKey.emptyKey uid :: optKeys rs := by
  simp [optKeys, emptyRows, nullRows, strRows, isNonEmptyPhrase]

private theorem optKeys_str (uid : Int) (s : String) (url : String) (val : Option Int) (rs : List HitRow)
    (hs : s ≠ "") :
    optKeys ({uid := uid, phrase := some s, url := url, val := val} :: rs) =
      (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
      ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
       (PhraseKey.nonEmptyKey uid s ::
         (strRows rs).map (fun r => classifyKey r.uid r.phrase))) := by
  simp [optKeys, emptyRows, nullRows, strRows, isNonEmptyPhrase, classifyKey, hs]

private theorem rowKeys_optKeys_perm : ∀ rows : List HitRow, rowKeys rows ~ optKeys rows := by
  intro rows
  induction rows with
  | nil => simp [rowKeys, optKeys, emptyRows, nullRows, strRows]
  | cons r rs ih =>
      rcases r with ⟨uid, phrase, url, val⟩
      cases phrase with
      | none =>
          rw [optKeys_none]
          change PhraseKey.nullKey uid :: rowKeys rs ~
            (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
            (PhraseKey.nullKey uid ::
              ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
               (strRows rs).map (fun r => classifyKey r.uid r.phrase)))
          calc
            PhraseKey.nullKey uid :: rowKeys rs ~ PhraseKey.nullKey uid :: optKeys rs := List.Perm.cons _ ih
            _ ~ (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
                (PhraseKey.nullKey uid ::
                  ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
                   (strRows rs).map (fun r => classifyKey r.uid r.phrase))) := by
                    simpa [optKeys] using
                      (List.perm_middle (a := PhraseKey.nullKey uid)
                        (l₁ := (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid))
                        (l₂ := (nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
                               (strRows rs).map (fun r => classifyKey r.uid r.phrase))).symm
      | some s =>
          by_cases hs : s = ""
          · subst s
            rw [optKeys_empty]
            exact List.Perm.cons _ ih
          · rw [optKeys_str _ _ _ _ _ hs]
            change classifyKey uid (some s) :: rowKeys rs ~
              (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
              ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
               (PhraseKey.nonEmptyKey uid s ::
                 (strRows rs).map (fun r => classifyKey r.uid r.phrase)))
            rw [show classifyKey uid (some s) = PhraseKey.nonEmptyKey uid s by simp [classifyKey, hs]]
            calc
              PhraseKey.nonEmptyKey uid s :: rowKeys rs ~
                  PhraseKey.nonEmptyKey uid s :: optKeys rs := List.Perm.cons _ ih
              _ ~ (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
                  (PhraseKey.nonEmptyKey uid s ::
                    ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
                     (strRows rs).map (fun r => classifyKey r.uid r.phrase))) := by
                    simpa [optKeys] using
                      (List.perm_middle (a := PhraseKey.nonEmptyKey uid s)
                        (l₁ := (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid))
                        (l₂ := (nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
                               (strRows rs).map (fun r => classifyKey r.uid r.phrase))).symm
              _ ~ (emptyRows rs).map (fun r => PhraseKey.emptyKey r.uid) ++
                  ((nullRows rs).map (fun r => PhraseKey.nullKey r.uid) ++
                   (PhraseKey.nonEmptyKey uid s ::
                     (strRows rs).map (fun r => classifyKey r.uid r.phrase))) := by
                    exact List.Perm.append_left _
                      (List.perm_middle (a := PhraseKey.nonEmptyKey uid s)
                        (l₁ := (nullRows rs).map (fun r => PhraseKey.nullKey r.uid))
                        (l₂ := (strRows rs).map (fun r => classifyKey r.uid r.phrase))).symm

private def toEmpty (p : Int × Nat) : Entry := (PhraseKey.emptyKey p.1, p.2)
private def toNull (p : Int × Nat) : Entry := (PhraseKey.nullKey p.1, p.2)

private theorem mapEmpty_addIntCount (m : List (Int × Nat)) (u : Int) :
    (addIntCount m u).map toEmpty = addCount (m.map toEmpty) (PhraseKey.emptyKey u) := by
  induction m with
  | nil => simp [addIntCount, addCount, toEmpty]
  | cons p rest ih =>
      rcases p with ⟨u', c⟩
      by_cases h : (u' == u)
      · have heq : u' = u := beq_iff_eq.mp h
        subst u'
        simp [addIntCount, addCount, toEmpty, h, ih]
      · have hne : u' ≠ u := by intro he; subst u'; simp at h
        have hneq : PhraseKey.emptyKey u' ≠ PhraseKey.emptyKey u := by
          intro hh
          cases hh
          exact hne rfl
        have hbe : (PhraseKey.emptyKey u' == PhraseKey.emptyKey u) = false := beq_false_of_ne hneq
        simp [addIntCount, addCount, toEmpty, h, hbe, ih]

private theorem mapNull_addIntCount (m : List (Int × Nat)) (u : Int) :
    (addIntCount m u).map toNull = addCount (m.map toNull) (PhraseKey.nullKey u) := by
  induction m with
  | nil => simp [addIntCount, addCount, toNull]
  | cons p rest ih =>
      rcases p with ⟨u', c⟩
      by_cases h : (u' == u)
      · have heq : u' = u := beq_iff_eq.mp h
        subst u'
        simp [addIntCount, addCount, toNull, h, ih]
      · have hne : u' ≠ u := by intro he; subst u'; simp at h
        have hneq : PhraseKey.nullKey u' ≠ PhraseKey.nullKey u := by
          intro hh
          cases hh
          exact hne rfl
        have hbe : (PhraseKey.nullKey u' == PhraseKey.nullKey u) = false := beq_false_of_ne hneq
        simp [addIntCount, addCount, toNull, h, hbe, ih]

private theorem emptyIntFold_aux (l : List HitRow) (m : List (Int × Nat)) :
    (l.foldl (fun acc r => addIntCount acc r.uid) m).map toEmpty =
      (l.map (fun r => PhraseKey.emptyKey r.uid)).foldl addCount (m.map toEmpty) := by
  induction l generalizing m with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons, List.map_cons]
      simpa [mapEmpty_addIntCount] using ih (addIntCount m r.uid)

private theorem nullIntFold_aux (l : List HitRow) (m : List (Int × Nat)) :
    (l.foldl (fun acc r => addIntCount acc r.uid) m).map toNull =
      (l.map (fun r => PhraseKey.nullKey r.uid)).foldl addCount (m.map toNull) := by
  induction l generalizing m with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons, List.map_cons]
      simpa [mapNull_addIntCount] using ih (addIntCount m r.uid)

private theorem emptyIntFold (rows : List HitRow) :
    ((emptyRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toEmpty =
      ((emptyRows rows).map (fun r => PhraseKey.emptyKey r.uid)).foldl addCount [] := by
  exact emptyIntFold_aux (emptyRows rows) []

private theorem nullIntFold (rows : List HitRow) :
    ((nullRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toNull =
      ((nullRows rows).map (fun r => PhraseKey.nullKey r.uid)).foldl addCount [] := by
  exact nullIntFold_aux (nullRows rows) []

private theorem addCount_append_tag (A B : List Entry) (k : PhraseKey)
    (hA : ∀ p ∈ A, keyTag p.1 ≠ keyTag k) :
    addCount (A ++ B) k = A ++ addCount B k := by
  induction A with
  | nil => rfl
  | cons p As ih =>
      have hpne : p.1 ≠ k := by
        intro heq
        exact hA p (by simp) (by rw [heq])
      have hpbe : (p.1 == k) = false := beq_false_of_ne hpne
      have hAs : ∀ q ∈ As, keyTag q.1 ≠ keyTag k := fun q hq => hA q (by simp [hq])
      simpa [addCount, hpbe] using ih hAs

private theorem foldl_addCount_append_tag (A B : List Entry) (ks : List PhraseKey)
    (hA : ∀ p ∈ A, ∀ k ∈ ks, keyTag p.1 ≠ keyTag k) :
    ks.foldl addCount (A ++ B) = A ++ ks.foldl addCount B := by
  induction ks generalizing A B with
  | nil => simp
  | cons k ks ih =>
      have hAk : ∀ p ∈ A, keyTag p.1 ≠ keyTag k := fun p hp => hA p hp k (by simp)
      have hAks : ∀ p ∈ A, ∀ k' ∈ ks, keyTag p.1 ≠ keyTag k' :=
        fun p hp k' hk' => hA p hp k' (by simp [hk'])
      rw [List.foldl_cons, addCount_append_tag A B k hAk]
      exact ih A (addCount B k) hAks

private theorem allTag_addCount (m : List Entry) (k : PhraseKey) (t : Nat)
    (hm : ∀ p ∈ m, keyTag p.1 = t) (hk : keyTag k = t) :
    ∀ p ∈ addCount m k, keyTag p.1 = t := by
  intro p hp
  rcases keys_addCount m k hp with hp_eq | hp_mem
  · rw [hp_eq, hk]
  · rcases List.mem_map.mp hp_mem with ⟨k', hk'm, hk'⟩
    rw [← hk']
    exact hm k' hk'm

private theorem allTag_fold_aux (ks : List PhraseKey) (m : List Entry) (t : Nat)
    (hm : ∀ p ∈ m, keyTag p.1 = t) (hks : ∀ k ∈ ks, keyTag k = t) :
    ∀ p ∈ ks.foldl addCount m, keyTag p.1 = t := by
  induction ks generalizing m with
  | nil => exact hm
  | cons k ks ih =>
      apply ih
      · exact allTag_addCount m k t hm (hks k (by simp))
      · intro k' hk'
        exact hks k' (by simp [hk'])

private theorem allTag_emptyFold (rows : List HitRow) :
    ∀ p ∈ ((emptyRows rows).map (fun r => PhraseKey.emptyKey r.uid)).foldl addCount [],
      keyTag p.1 = 0 := by
  apply allTag_fold_aux
  · intro p hp; exact False.elim (List.not_mem_nil hp)
  · intro k hk
    rcases List.mem_map.mp hk with ⟨r, hr, rfl⟩
    rfl

private theorem allTag_nullFold (rows : List HitRow) :
    ∀ p ∈ ((nullRows rows).map (fun r => PhraseKey.nullKey r.uid)).foldl addCount [],
      keyTag p.1 = 2 := by
  apply allTag_fold_aux
  · intro p hp; exact False.elim (List.not_mem_nil hp)
  · intro k hk
    rcases List.mem_map.mp hk with ⟨r, hr, rfl⟩
    rfl

private theorem fold_classify_aux (l : List HitRow) (m : List Entry) :
    l.foldl (fun acc r => addCount acc (classifyKey r.uid r.phrase)) m =
      (l.map (fun r => classifyKey r.uid r.phrase)).foldl addCount m := by
  induction l generalizing m with
  | nil => rfl
  | cons r rs ih => simp [List.foldl_cons, ih]

private theorem fold_classify (l : List HitRow) :
    l.foldl (fun acc r => addCount acc (classifyKey r.uid r.phrase)) [] =
      (l.map (fun r => classifyKey r.uid r.phrase)).foldl addCount [] := by
  exact fold_classify_aux l []

private theorem optKeys_fold_eq_all (rows : List HitRow) :
    (optKeys rows).foldl addCount [] =
      ((emptyRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toEmpty ++
      ((nullRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toNull ++
      ((strRows rows).map (fun r => classifyKey r.uid r.phrase)).foldl addCount [] := by
  rw [emptyIntFold, nullIntFold]
  simp only [optKeys, List.foldl_append]
  let mE := ((emptyRows rows).map (fun r => PhraseKey.emptyKey r.uid)).foldl addCount []
  let mN := ((nullRows rows).map (fun r => PhraseKey.nullKey r.uid)).foldl addCount []
  have hEN : ((nullRows rows).map (fun r => PhraseKey.nullKey r.uid)).foldl addCount mE =
      mE ++ mN := by
    simpa [mN] using
      (foldl_addCount_append_tag mE [] ((nullRows rows).map (fun r => PhraseKey.nullKey r.uid)) (by
        intro p hp k hk
        have htag := allTag_emptyFold rows p hp
        rcases List.mem_map.mp hk with ⟨r, hr, rfl⟩
        simp [keyTag] at htag ⊢
        omega))
  rw [hEN]
  have hS : ((strRows rows).map (fun r => classifyKey r.uid r.phrase)).foldl addCount (mE ++ mN) =
      (mE ++ mN) ++ ((strRows rows).map (fun r => classifyKey r.uid r.phrase)).foldl addCount [] := by
    simpa using
      (foldl_addCount_append_tag (mE ++ mN) []
        ((strRows rows).map (fun r => classifyKey r.uid r.phrase)) (by
          intro p hp k hk
          have hkTag : keyTag k = 1 := by
            rcases List.mem_map.mp hk with ⟨r, hr, hrk⟩
            simp [strRows, isNonEmptyPhrase] at hr
            rcases r with ⟨ruid, rph, rurl, rval⟩
            cases rph with
            | none => simp at hr
            | some rs =>
                by_cases hrs : rs = ""
                · simp [hrs] at hr
                · simp [classifyKey, hrs] at hrk
                  rw [← hrk]
                  simp [keyTag]
          rcases List.mem_append.mp hp with hpE | hpN
          · have htag := allTag_emptyFold rows p hpE
            rw [htag, hkTag]
            omega
          · have htag := allTag_nullFold rows p hpN
            rw [htag, hkTag]
            omega))
  rw [hS]

private theorem topK_sound (k : Nat) (rows : List HitRow) :
    optSplitTopK k rows = refGroupTopK k rows := by
  have hperm : rowKeys rows ~ optKeys rows := rowKeys_optKeys_perm rows
  have hmap : (rowKeys rows).foldl addCount [] ~ (optKeys rows).foldl addCount [] :=
    foldl_addCount_of_keyperm hperm [] (by simp [distinctKeys])
  have hsort : sortEntries ((rowKeys rows).foldl addCount []) =
      sortEntries ((optKeys rows).foldl addCount []) := sortEntries_cong hmap
  unfold refGroupTopK optSplitTopK
  dsimp only
  have hRef : rows.foldl (fun acc r => addCount acc (classifyKey r.uid r.phrase)) [] =
      (rowKeys rows).foldl addCount [] := by
    simpa [rowKeys] using fold_classify rows
  rw [hRef]
  rw [fold_classify]
  change (sortEntries (((emptyRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toEmpty ++
      ((nullRows rows).foldl (fun acc r => addIntCount acc r.uid) []).map toNull ++
      ((strRows rows).map (fun r => classifyKey r.uid r.phrase)).foldl addCount [])).take k =
    (sortEntries ((rowKeys rows).foldl addCount [])).take k
  rw [← optKeys_fold_eq_all]
  exact congrArg (fun l => l.take k) hsort.symm

private theorem predicate_sound (containsUrl : String → Bool) (r : HitRow) :
    evalPredReordered containsUrl r = evalPredNaive containsUrl r := by
  unfold evalPredReordered evalPredNaive
  cases containsUrl r.url <;> cases isNonEmptyPhrase r.phrase <;> rfl

private def avgStep (s : Int × Nat) : Option Int → Int × Nat
  | none => s
  | some v => (s.1 + v, s.2 + 1)

private def avgAdd (a b : Int × Nat) : Int × Nat := (a.1 + b.1, a.2 + b.2)

private theorem avgStep_eq_add (s : Int × Nat) (ov : Option Int) :
    avgStep s ov = avgAdd s (avgStep (0, 0) ov) := by
  cases ov with
  | none => simp [avgStep, avgAdd]
  | some v => simp [avgStep, avgAdd]

private theorem avgAdd_assoc (a b c : Int × Nat) :
    avgAdd (avgAdd a b) c = avgAdd a (avgAdd b c) := by
  ext <;> simp [avgAdd] <;> omega

private theorem fold_avgStep (l : List (Option Int)) (s : Int × Nat) :
    l.foldl avgStep s = avgAdd s (l.foldl avgStep (0, 0)) := by
  induction l generalizing s with
  | nil => simp [avgAdd]
  | cons ov xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      calc
        xs.foldl avgStep (avgStep s ov) =
            avgAdd (avgStep s ov) (xs.foldl avgStep (0, 0)) := ih (avgStep s ov)
        _ = avgAdd (avgAdd s (avgStep (0, 0) ov)) (xs.foldl avgStep (0, 0)) := by
              rw [avgStep_eq_add]
        _ = avgAdd s (avgAdd (avgStep (0, 0) ov) (xs.foldl avgStep (0, 0))) := avgAdd_assoc _ _ _
        _ = avgAdd s (xs.foldl avgStep (avgStep (0, 0) ov)) := by
              rw [ih (avgStep (0, 0) ov)]

private theorem avg_concat_sound (xs ys : List (Option Int)) :
    avgStateOfOpts (xs ++ ys) =
      (let (s1, c1) := avgStateOfOpts xs
       let (s2, c2) := avgStateOfOpts ys
       (s1 + s2, c1 + c2)) := by
  unfold avgStateOfOpts
  rw [List.foldl_append]
  change ys.foldl avgStep (xs.foldl avgStep (0, 0)) =
    avgAdd (xs.foldl avgStep (0, 0)) (ys.foldl avgStep (0, 0))
  exact fold_avgStep ys (xs.foldl avgStep (0, 0))

theorem sound : TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit.Goal := by
  refine ⟨topK_sound, predicate_sound, ?_⟩
  intro xs ys
  exact avg_concat_sound xs ys


end TinylambOpt.ResearchProofs.EmptyStringKeyPartitionSplit

-- C++ references to this model or its proof.
-- executor/detail/typed_stream_aggregate_partitioned.cpp
