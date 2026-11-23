import TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket

namespace TinylambOpt.ResearchProofs.OffsetHeavyReverseTopNAndQuotientBucket

open TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket

/-! ## Generic list helpers -/

private theorem perm_append_singleton {α : Type} (as : List α) (a : α) :
    (as ++ [a]).Perm (a :: as) := by
  induction as with
  | nil => simp
  | cons b bs ih =>
      exact (List.Perm.cons b ih).trans (List.Perm.swap a b bs)

private theorem perm_reverse {α : Type} (l : List α) : l.reverse.Perm l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.reverse_cons]
      exact (List.Perm.append_right [a] ih).trans (perm_append_singleton as a)

private theorem perm_reverse_of_perm {α : Type} {l₁ l₂ : List α} (h : l₁.Perm l₂) :
    l₁.reverse.Perm l₂.reverse := by
  exact (perm_reverse l₁).trans (h.trans (perm_reverse l₂).symm)

private theorem reverse_take_reverse {α : Type} (l : List α) (k : Nat)
    (hk : k ≤ l.length) : (l.reverse.take k).reverse = l.drop (l.length - k) := by
  have hsplit := List.take_append_drop (l.length - k) l
  have hrev : l.reverse = (l.drop (l.length - k)).reverse ++ (l.take (l.length - k)).reverse := by
    calc
      l.reverse = (l.take (l.length - k) ++ l.drop (l.length - k)).reverse := by rw [hsplit]
      _ = (l.drop (l.length - k)).reverse ++ (l.take (l.length - k)).reverse := by
        rw [List.reverse_append]
  have hlen : (l.drop (l.length - k)).reverse.length = k := by
    rw [List.length_reverse, List.length_drop]
    omega
  have htake : (((l.drop (l.length - k)).reverse ++ (l.take (l.length - k)).reverse).take k) =
      (l.drop (l.length - k)).reverse := by
    have htake0 : (((l.drop (l.length - k)).reverse ++ (l.take (l.length - k)).reverse).take
        ((l.drop (l.length - k)).reverse.length)) = (l.drop (l.length - k)).reverse :=
      List.take_left
    rw [hlen] at htake0
    exact htake0
  calc
    (l.reverse.take k).reverse =
        (((l.drop (l.length - k)).reverse ++ (l.take (l.length - k)).reverse).take k).reverse := by
      rw [hrev]
    _ = ((l.drop (l.length - k)).reverse).reverse := by rw [htake]
    _ = l.drop (l.length - k) := by rw [List.reverse_reverse]

private def SortedBy {α : Type} (le : α → α → Bool) (l : List α) : Prop :=
  l.Pairwise fun a b => le a b = true

private theorem insertSortedBy_perm {α : Type} (le : α → α → Bool) (x : α) (l : List α) :
    (insertSortedBy le x l).Perm (x :: l) := by
  induction l with
  | nil => simp [insertSortedBy]
  | cons y ys ih =>
      cases h : le x y with
      | true => simp [insertSortedBy, h]
      | false =>
          simp [insertSortedBy, h]
          exact (List.Perm.cons y ih).trans (List.Perm.swap x y ys)

private theorem forall_mem_insertSortedBy {α : Type} (le : α → α → Bool) {y x : α} {l : List α}
    (hyl : ∀ z ∈ l, le y z = true) (hyx : le y x = true) :
    ∀ z ∈ insertSortedBy le x l, le y z = true := by
  induction l with
  | nil =>
      intro z hz
      simp [insertSortedBy] at hz
      subst z
      exact hyx
  | cons a as ih =>
      have hya : le y a = true := hyl a List.mem_cons_self
      have hyas : ∀ z ∈ as, le y z = true := fun z hz => hyl z (List.mem_cons_of_mem a hz)
      simp only [insertSortedBy]
      by_cases hxa : le x a = true
      · rw [ite_eq_left hxa]
        intro z hz
        rcases List.mem_cons.mp hz with rfl | hz
        · exact hyx
        · rcases List.mem_cons.mp hz with rfl | hz
          · exact hya
          · exact hyas z hz
      · rw [ite_eq_right hxa]
        intro z hz
        rcases List.mem_cons.mp hz with rfl | hz
        · exact hya
        · exact ih hyas z hz

private theorem insertSortedBy_pairwise {α : Type} (le : α → α → Bool)
    (htrans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (htotal : ∀ a b : α, le a b = true ∨ le b a = true)
    {x : α} {l : List α} (hs : SortedBy le l) : SortedBy le (insertSortedBy le x l) := by
  unfold SortedBy at hs ⊢
  induction l with
  | nil => simp [insertSortedBy]
  | cons y ys ih =>
      have hp := List.pairwise_cons.mp hs
      have hhead : ∀ z ∈ ys, le y z = true := hp.1
      cases hxy : le x y with
      | true =>
          simp [insertSortedBy, hxy]
          refine ⟨?_, hp.1, hp.2⟩
          intro z hz
          exact htrans x y z hxy (hhead z hz)
      | false =>
          simp [insertSortedBy, hxy]
          have hyx : le y x = true := by
            rcases htotal x y with h | h
            · rw [hxy] at h
              contradiction
            · exact h
          exact ⟨forall_mem_insertSortedBy le hhead hyx, ih hp.2⟩

private theorem sortBy_perm {α : Type} (le : α → α → Bool) (l : List α) :
    (sortBy le l).Perm l := by
  induction l with
  | nil => simp [sortBy]
  | cons x xs ih =>
      simp only [sortBy]
      exact (insertSortedBy_perm le x (sortBy le xs)).trans (List.Perm.cons x ih)

private theorem sortBy_pairwise {α : Type} (le : α → α → Bool)
    (htrans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (htotal : ∀ a b : α, le a b = true ∨ le b a = true) (l : List α) :
    SortedBy le (sortBy le l) := by
  induction l with
  | nil => simp [SortedBy, sortBy]
  | cons x xs ih =>
      simp only [sortBy]
      exact insertSortedBy_pairwise le htrans htotal ih

private theorem sorted_perm_eq {α : Type} [DecidableEq α] (le : α → α → Bool)
    (hanti : ∀ a b : α, le a b = true → le b a = true → a = b)
    {l₁ l₂ : List α} (nd₁ : l₁.Nodup) (nd₂ : l₂.Nodup)
    (hp : l₁.Perm l₂) (hs₁ : SortedBy le l₁) (hs₂ : SortedBy le l₂) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => rfl
      | cons b bs =>
          have hlen := hp.length_eq
          simp at hlen
  | cons a as ih =>
      have ha_mem : a ∈ l₂ := hp.subset List.mem_cons_self
      cases l₂ with
      | nil => exact absurd ha_mem List.not_mem_nil
      | cons b bs =>
          have hb_mem : b ∈ a :: as := hp.symm.subset List.mem_cons_self
          have hba : b = a := by
            rcases List.mem_cons.mp ha_mem with heq | htail
            · exact heq.symm
            · rcases List.mem_cons.mp hb_mem with rfl | hbs
              · rfl
              · have hab : le a b = true := (List.pairwise_cons.mp hs₁).1 b hbs
                have hba' : le b a = true := (List.pairwise_cons.mp hs₂).1 a htail
                have heq := hanti a b hab hba'
                have : b ∈ bs := by simpa [heq] using htail
                exact absurd this (List.nodup_cons.mp nd₂).1
          subst b
          have hp' : as.Perm bs := by
            simpa [List.erase_cons_head] using (List.Perm.erase a hp)
          have htail : as = bs := ih (List.nodup_cons.mp nd₁).2 (List.nodup_cons.mp nd₂).2
            hp' (List.pairwise_cons.mp hs₁).2 (List.pairwise_cons.mp hs₂).2
          rw [htail]

private theorem reverse_sorted {α : Type} (le : α → α → Bool) {l : List α}
    (hs : SortedBy le l) : SortedBy (fun a b => le b a) l.reverse := by
  rw [SortedBy, List.pairwise_reverse]
  exact hs

private theorem increasing_pairs_eq_of_mem_iff {l₁ l₂ : List (Int × Nat)}
    (h₁ : l₁.Pairwise (fun a b => a.1 < b.1))
    (h₂ : l₂.Pairwise (fun a b => a.1 < b.1))
    (hmem : ∀ q, q ∈ l₁ ↔ q ∈ l₂) : l₁ = l₂ := by
  revert l₂
  induction l₁ with
  | nil =>
      intro l₂ h₂ hmem
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((hmem b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ h₂ hmem
      obtain ⟨haft, hrinc⟩ := List.pairwise_cons.mp h₁
      cases l₂ with
      | nil => exact absurd ((hmem a).mp List.mem_cons_self) List.not_mem_nil
      | cons b bs =>
          obtain ⟨hbft, hbinc⟩ := List.pairwise_cons.mp h₂
          have hba : b = a := by
            by_cases hlt : b.1 < a.1
            · have hbmem : b ∈ a :: r := (hmem b).mpr List.mem_cons_self
              rcases List.mem_cons.mp hbmem with hba' | hbr
              · have : b.1 = a.1 := by rw [hba']
                omega
              · have hab := haft b hbr
                omega
            · by_cases hgt : a.1 < b.1
              · have hamem : a ∈ b :: bs := (hmem a).mp List.mem_cons_self
                rcases List.mem_cons.mp hamem with hab' | hbs
                · exact hab'.symm
                · have hbb := hbft a hbs
                  omega
              · have hamem : a ∈ b :: bs := (hmem a).mp List.mem_cons_self
                rcases List.mem_cons.mp hamem with hab' | hbs
                · exact hab'.symm
                · have hbb := hbft a hbs
                  omega
          subst b
          have hmem' : ∀ q, q ∈ r ↔ q ∈ bs := by
            intro q
            have hq : (q = a ∨ q ∈ r) ↔ (q = a ∨ q ∈ bs) := by
              simpa [List.mem_cons] using hmem q
            constructor
            · intro hqr
              have hne : q ≠ a := by
                intro hqa
                subst q
                have hlt := haft a hqr
                omega
              rcases (hq.mp (Or.inr hqr)) with hqa | hqs
              · exact absurd hqa hne
              · exact hqs
            · intro hqs
              have hne : q ≠ a := by
                intro hqa
                subst q
                have hlt := hbft a hqs
                omega
              rcases (hq.mpr (Or.inr hqs)) with hqa | hqr
              · exact absurd hqa hne
              · exact hqr
          have htail : r = bs := ih hrinc hbinc hmem'
          rw [htail]

/-! ## Properties of `descLe` / `ascLe` -/

private theorem descLe_iff (a b : Item) :
    descLe a b = true ↔ b.1 < a.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2) := by
  by_cases h : a.1 = b.1
  · have hbe : (a.1 == b.1) = true := by simpa using h
    simp [descLe, h]
  · have hbe : (a.1 == b.1) = false := by simpa using h
    simp [descLe, h]
    omega

private theorem descLe_refl (a : Item) : descLe a a = true := by
  simp [descLe]

private theorem descLe_trans (a b c : Item) (hab : descLe a b = true) (hbc : descLe b c = true) :
    descLe a c = true := by
  rw [descLe_iff] at hab hbc ⊢
  rcases hab with hab | ⟨habS, habT⟩
  · rcases hbc with hbc | ⟨hbcS, hbcT⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases hbc with hbc | ⟨hbcS, hbcT⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, by omega⟩

private theorem descLe_total (a b : Item) : descLe a b = true ∨ descLe b a = true := by
  rw [descLe_iff, descLe_iff]
  by_cases h : a.1 = b.1
  · by_cases ht : a.2 ≤ b.2
    · exact Or.inl (Or.inr ⟨h, ht⟩)
    · exact Or.inr (Or.inr ⟨h.symm, by omega⟩)
  · by_cases hlt : b.1 < a.1
    · exact Or.inl (Or.inl hlt)
    · by_cases hgt : a.1 < b.1
      · exact Or.inr (Or.inl hgt)
      · omega

private theorem descLe_anti (a b : Item) (hab : descLe a b = true) (hba : descLe b a = true) :
    a = b := by
  rw [descLe_iff] at hab hba
  rcases hab with hab | ⟨habS, habT⟩ <;> rcases hba with hba | ⟨hbaS, hbaT⟩
  · omega
  · omega
  · omega
  · apply Prod.ext <;> omega

private theorem ascLe_trans (a b c : Item) (hab : ascLe a b = true) (hbc : ascLe b c = true) :
    ascLe a c = true := by
  exact descLe_trans c b a hbc hab

private theorem ascLe_total (a b : Item) : ascLe a b = true ∨ ascLe b a = true := by
  exact descLe_total b a

private theorem ascLe_anti (a b : Item) (hab : ascLe a b = true) (hba : ascLe b a = true) :
    a = b := by
  exact (descLe_anti b a hab hba).symm

/-! ## Part 1: offset-heavy reverse top-N -/

private theorem part1 : ∀ (off lim : Nat) (xs : List Item),
    xs.Nodup → optReverseAscSlice off lim xs = refDescSlice off lim xs := by
  intro off lim xs hnd
  let D := sortBy descLe xs
  let A := sortBy ascLe xs
  have hpermD : D.Perm xs := sortBy_perm descLe xs
  have hpermA : A.Perm xs := sortBy_perm ascLe xs
  have hsd : SortedBy descLe D := sortBy_pairwise descLe descLe_trans descLe_total xs
  have hsa : SortedBy ascLe A := sortBy_pairwise ascLe ascLe_trans ascLe_total xs
  have hndD : D.Nodup := hpermD.symm.nodup hnd
  have hndA : A.Nodup := hpermA.symm.nodup hnd
  have hndDrev : D.reverse.Nodup := (perm_reverse D).symm.nodup hndD
  have hrevSorted : SortedBy ascLe D.reverse := by
    change SortedBy (fun a b => descLe b a) D.reverse
    exact reverse_sorted descLe hsd
  have hpermRev : D.reverse.Perm A := (perm_reverse D).trans (hpermD.trans hpermA.symm)
  have hrev : D.reverse = A := sorted_perm_eq ascLe ascLe_anti
    hndDrev hndA hpermRev hrevSorted hsa
  change (if xs.length ≤ off then [] else ((A.take (xs.length - off)).reverse).take lim) =
    (D.drop off).take lim
  by_cases hle : xs.length ≤ off
  · rw [ite_eq_left hle]
    have hlenD : D.length = xs.length := hpermD.length_eq
    have hdrop : D.drop off = [] := List.drop_eq_nil_of_le (by omega)
    rw [hdrop]
    simp
  · rw [ite_eq_right hle]
    have hoff : off ≤ xs.length := by omega
    have hlenD : D.length = xs.length := hpermD.length_eq
    have hkeep : xs.length - off ≤ D.length := by omega
    have hrevtake : (A.take (xs.length - off)).reverse = D.drop off := by
      rw [← hrev]
      rw [reverse_take_reverse D (xs.length - off) hkeep]
      congr 1
      omega
    rw [hrevtake]

/-! ## Part 2: monotonicity of `Int.ediv · 60` -/

private theorem part2 : ∀ (t1 t2 : Int), t1 ≤ t2 → Int.ediv t1 60 ≤ Int.ediv t2 60 := by
  intro t1 t2 h
  exact Int.ediv_le_ediv (by decide) h

/-! ## Part 3: dense quotient bucket sweep -/

private def bk (t : Int) : Int := Int.ediv t 60

private def cnt (b : Int) (ts : List Int) : Nat := (ts.filter fun t => bk t == b).length

private def keyLe (a b : Int × Nat) : Bool := decide (a.1 ≤ b.1)

private def lookupCnt (m : List (Int × Nat)) (b : Int) : Nat :=
  (m.find? (fun p => p.1 == b)).map Prod.snd |>.getD 0

private def BucketInv (m : List (Int × Nat)) : Prop :=
  m.Pairwise (fun p q => p.1 ≠ q.1) ∧ ∀ p ∈ m, 0 < p.2

private theorem addBucketCnt_keys_mem {m : List (Int × Nat)} {b k : Int} :
    k ∈ (addBucketCnt m b).map Prod.fst ↔ k ∈ m.map Prod.fst ∨ k = b := by
  induction m with
  | nil => simp [addBucketCnt]
  | cons p ps ih =>
      by_cases hpb : p.1 = b
      · simp [addBucketCnt, hpb]
        intro hk
        exact Or.inl hk
      · simp [addBucketCnt, hpb, ih, or_left_comm, or_comm]

private theorem addBucketCnt_pairwise {m : List (Int × Nat)} {b : Int}
    (h : m.Pairwise (fun p q => p.1 ≠ q.1)) :
    (addBucketCnt m b).Pairwise (fun p q => p.1 ≠ q.1) := by
  induction m with
  | nil => simp [addBucketCnt]
  | cons p ps ih =>
      have hp := List.pairwise_cons.mp h
      by_cases hpb : p.1 = b
      · rw [show addBucketCnt (p :: ps) b = (p.1, p.snd + 1) :: ps by simp [addBucketCnt, hpb]]
        rw [List.pairwise_cons]
        exact ⟨hp.1, hp.2⟩
      · rw [show addBucketCnt (p :: ps) b = p :: addBucketCnt ps b by simp [addBucketCnt, hpb]]
        rw [List.pairwise_cons]
        refine ⟨?_, ih hp.2⟩
        intro q hq
        have hqk : q.1 ∈ (addBucketCnt ps b).map Prod.fst := List.mem_map.mpr ⟨q, hq, rfl⟩
        have hkeys := addBucketCnt_keys_mem.mp hqk
        rcases hkeys with hqps | hqb
        · intro heq
          rcases List.mem_map.mp hqps with ⟨q', hq'ps, hq'eq⟩
          exact hp.1 q' hq'ps (heq.trans hq'eq.symm)
        · intro heq
          exact hpb (heq.trans hqb)

private theorem addBucketCnt_pos {m : List (Int × Nat)} {b : Int}
    (h : ∀ p ∈ m, 0 < p.2) : ∀ p ∈ addBucketCnt m b, 0 < p.2 := by
  induction m with
  | nil => simp [addBucketCnt]
  | cons p ps ih =>
      by_cases hpb : p.1 = b
      · rw [show addBucketCnt (p :: ps) b = (p.1, p.snd + 1) :: ps by simp [addBucketCnt, hpb]]
        intro q hq
        rcases List.mem_cons.mp hq with hqeq | hq
        · rw [hqeq]
          omega
        · exact h q (List.mem_cons_of_mem p hq)
      · rw [show addBucketCnt (p :: ps) b = p :: addBucketCnt ps b by simp [addBucketCnt, hpb]]
        intro q hq
        rcases List.mem_cons.mp hq with hqeq | hq
        · rw [hqeq]
          exact h p List.mem_cons_self
        · exact ih (fun z hz => h z (List.mem_cons_of_mem p hz)) q hq

private theorem addBucketCnt_inv {m : List (Int × Nat)} {b : Int} (h : BucketInv m) :
    BucketInv (addBucketCnt m b) :=
  ⟨addBucketCnt_pairwise h.1, addBucketCnt_pos h.2⟩

private theorem lookupCnt_add (m : List (Int × Nat)) (b b' : Int) :
    lookupCnt (addBucketCnt m b) b' = lookupCnt m b' + (if b == b' then 1 else 0) := by
  induction m with
  | nil =>
      simp [addBucketCnt, lookupCnt]
      by_cases h : b = b' <;> simp [h]
  | cons p ps ih =>
      by_cases hpb : p.1 = b
      · by_cases hb' : b' = p.1
        · subst b'
          subst b
          simp [addBucketCnt, lookupCnt]
        · have hpne : p.1 ≠ b' := by intro h; exact hb' h.symm
          have hbne : b ≠ b' := by omega
          rw [show addBucketCnt (p :: ps) b = (p.1, p.snd + 1) :: ps by simp [addBucketCnt, hpb]]
          simp [lookupCnt, hpne, hbne]
      · by_cases hb' : b' = p.1
        · subst b'
          have hpne : b ≠ p.1 := by omega
          rw [show addBucketCnt (p :: ps) b = p :: addBucketCnt ps b by simp [addBucketCnt, hpb]]
          simp [lookupCnt, hpne]
        · have hpne : p.1 ≠ b' := by intro h; exact hb' h.symm
          rw [show addBucketCnt (p :: ps) b = p :: addBucketCnt ps b by simp [addBucketCnt, hpb]]
          unfold lookupCnt
          rw [show List.find? (fun q : Int × Nat => q.1 == b') (p :: addBucketCnt ps b) =
              List.find? (fun q : Int × Nat => q.1 == b') (addBucketCnt ps b) by simp [hpne]]
          rw [show List.find? (fun q : Int × Nat => q.1 == b') (p :: ps) =
              List.find? (fun q : Int × Nat => q.1 == b') ps by simp [hpne]]
          exact ih

private theorem cnt_cons (b : Int) (t : Int) (ts : List Int) :
    cnt b (t :: ts) = cnt b ts + (if bk t == b then 1 else 0) := by
  by_cases h : bk t == b
  · simp [cnt, h, Nat.add_comm]
  · simp [cnt, h]

private theorem foldl_bucket_inv (acc : List (Int × Nat)) (ts : List Int) (hacc : BucketInv acc) :
    BucketInv (ts.foldl (fun a t => addBucketCnt a (bk t)) acc) ∧
      ∀ b : Int, lookupCnt (ts.foldl (fun a t => addBucketCnt a (bk t)) acc) b =
        lookupCnt acc b + cnt b ts := by
  induction ts generalizing acc with
  | nil =>
      refine ⟨hacc, ?_⟩
      intro b
      simp [cnt, lookupCnt]
  | cons t ts ih =>
      have hacc' : BucketInv (addBucketCnt acc (bk t)) := addBucketCnt_inv hacc
      have hres := ih (addBucketCnt acc (bk t)) hacc'
      refine ⟨hres.1, ?_⟩
      intro b
      change lookupCnt (ts.foldl (fun a t => addBucketCnt a (bk t)) (addBucketCnt acc (bk t))) b =
        lookupCnt acc b + cnt b (t :: ts)
      rw [hres.2 b, lookupCnt_add]
      rw [cnt_cons]
      omega

private theorem lookupCnt_eq_of_mem {m : List (Int × Nat)} (hinv : BucketInv m) {p : Int × Nat}
    (hp : p ∈ m) : lookupCnt m p.1 = p.2 := by
  induction m with
  | nil => exact absurd hp List.not_mem_nil
  | cons q qs ih =>
      rcases List.mem_cons.mp hp with rfl | hpqs
      · simp [lookupCnt]
      · have hqne : q.1 ≠ p.1 := (List.pairwise_cons.mp hinv.1).1 p hpqs
        have hne : (q.1 == p.1) = false := by simpa using hqne
        simp [lookupCnt, hne]
        exact ih ⟨(List.pairwise_cons.mp hinv.1).2,
          fun z hz => hinv.2 z (List.mem_cons_of_mem q hz)⟩ hpqs

private theorem exists_of_lookupCnt_pos {m : List (Int × Nat)} (hinv : BucketInv m) {b : Int}
    (h : 0 < lookupCnt m b) : ∃ p ∈ m, p.1 = b ∧ p.2 = lookupCnt m b := by
  induction m with
  | nil => simp [lookupCnt] at h
  | cons q qs ih =>
      by_cases hqb : q.1 = b
      · refine ⟨q, List.mem_cons_self, hqb, ?_⟩
        simp [lookupCnt, hqb]
      · have hne : (q.1 == b) = false := by simpa using hqb
        simp [lookupCnt, hne] at h
        obtain ⟨p, hp, hpb, hpv⟩ := ih ⟨(List.pairwise_cons.mp hinv.1).2,
          fun z hz => hinv.2 z (List.mem_cons_of_mem q hz)⟩ h
        refine ⟨p, List.mem_cons_of_mem q hp, hpb, ?_⟩
        simpa [lookupCnt, hne] using hpv

private theorem mem_fold_bucket (ts : List Int) (p : Int × Nat) :
    p ∈ ts.foldl (fun a t => addBucketCnt a (bk t)) [] ↔
      0 < cnt p.1 ts ∧ p.2 = cnt p.1 ts := by
  let m := ts.foldl (fun a t => addBucketCnt a (bk t)) []
  have hres := foldl_bucket_inv [] ts (by simp [BucketInv])
  have hinv : BucketInv m := hres.1
  have hlookup : ∀ b : Int, lookupCnt m b = cnt b ts := by
    intro b
    rw [hres.2 b]
    simp [lookupCnt]
  constructor
  · intro hp
    have hp2 : lookupCnt m p.1 = p.2 := lookupCnt_eq_of_mem hinv hp
    have hpos : 0 < p.2 := hinv.2 p hp
    constructor
    · rw [← hlookup, hp2]
      exact hpos
    · rw [← hlookup, hp2]
  · rintro ⟨hpos, heq⟩
    have hl : 0 < lookupCnt m p.1 := by rw [hlookup]; exact hpos
    obtain ⟨q, hqm, hq1, hq2⟩ := exists_of_lookupCnt_pos hinv hl
    have hqcnt : q = p := by
      apply Prod.ext
      · exact hq1
      · rw [hq2, hlookup]
        exact heq.symm
    simpa [hqcnt] using hqm

/-! ### The sweep side -/

private theorem mem_sweep_aux (lo hi : Int) (ts : List Int) (p : Int × Nat) :
    p ∈ optDenseMinuteSweep lo hi ts ↔
      ∃ idx : Nat, idx < (if Int.ediv hi 60 < Int.ediv lo 60 then 0
          else (Int.ediv hi 60 - Int.ediv lo 60).toNat + 1) ∧
        Int.ediv lo 60 + Int.ofNat idx = p.1 ∧ 0 < cnt p.1 ts ∧ p.2 = cnt p.1 ts := by
  let span := if Int.ediv hi 60 < Int.ediv lo 60 then 0
    else (Int.ediv hi 60 - Int.ediv lo 60).toNat + 1
  let f : Nat → Option (Int × Nat) := fun idx =>
    let b := Int.ediv lo 60 + Int.ofNat idx
    let c := cnt b ts
    if 0 < c then some (b, c) else none
  change p ∈ (List.range span).filterMap f ↔
    ∃ idx : Nat, idx < span ∧ Int.ediv lo 60 + Int.ofNat idx = p.1 ∧
      0 < cnt p.1 ts ∧ p.2 = cnt p.1 ts
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨idx, hidx, hsome⟩
    simp only [List.mem_range] at hidx
    dsimp [f] at hsome
    by_cases hc : 0 < cnt (Int.ediv lo 60 + Int.ofNat idx) ts
    · have hsp := (by simpa [hc] using hsome :
        0 < cnt (Int.ediv lo 60 + Int.ofNat idx) ts ∧
          (Int.ediv lo 60 + Int.ofNat idx, cnt (Int.ediv lo 60 + Int.ofNat idx) ts) = p)
      have he := hsp.2
      have hkey' : Int.ediv lo 60 + Int.ofNat idx = p.1 := congrArg Prod.fst he
      have hcnt' : cnt (Int.ediv lo 60 + Int.ofNat idx) ts = p.2 := congrArg Prod.snd he
      refine ⟨idx, hidx, hkey', ?_, ?_⟩
      · rw [← hkey']
        exact hsp.1
      · rw [← hkey']
        exact hcnt'.symm
    · have hsp := (by simpa [hc] using hsome :
        0 < cnt (Int.ediv lo 60 + Int.ofNat idx) ts ∧
          (Int.ediv lo 60 + Int.ofNat idx, cnt (Int.ediv lo 60 + Int.ofNat idx) ts) = p)
      exact absurd hsp.1 hc
  · rintro ⟨idx, hidx, hkey, hpos, hval⟩
    refine ⟨idx, List.mem_range.mpr hidx, ?_⟩
    dsimp [f]
    have hc : 0 < cnt (Int.ediv lo 60 + Int.ofNat idx) ts := by
      rw [hkey]
      exact hpos
    simp
    refine ⟨hc, ?_⟩
    apply Prod.ext
    · exact hkey
    · change cnt (Int.ediv lo 60 + Int.ofNat idx) ts = p.2
      rw [hkey]
      exact hval.symm

private theorem sweep_key_pairwise (lo hi : Int) (ts : List Int) :
    (optDenseMinuteSweep lo hi ts).Pairwise (fun a b => a.1 < b.1) := by
  unfold optDenseMinuteSweep
  let span := if Int.ediv hi 60 < Int.ediv lo 60 then 0
    else (Int.ediv hi 60 - Int.ediv lo 60).toNat + 1
  let f : Nat → Option (Int × Nat) := fun idx =>
    let b := Int.ediv lo 60 + Int.ofNat idx
    let c := cnt b ts
    if 0 < c then some (b, c) else none
  change (List.range span).filterMap f |>.Pairwise (fun a b => a.1 < b.1)
  induction span with
  | zero => simp
  | succ span ih =>
      rw [List.range_succ, List.filterMap_append, List.pairwise_append]
      refine ⟨ih, ?_, ?_⟩
      · cases hfv : f span with
        | none => simp [hfv]
        | some v => simp [hfv]
      · intro p hp q hq
        rcases List.mem_filterMap.mp hp with ⟨i, hi, hfi⟩
        rcases List.mem_filterMap.mp hq with ⟨j, hj, hfj⟩
        have hi' : i < span := by simpa [List.mem_range] using hi
        have hj' : j = span := by simpa using hj
        subst j
        dsimp [f] at hfi hfj
        have key_of {n : Nat} {r : Int × Nat}
            (h : (if 0 < cnt (Int.ediv lo 60 + Int.ofNat n) ts then
                some (Int.ediv lo 60 + Int.ofNat n, cnt (Int.ediv lo 60 + Int.ofNat n) ts)
              else none) = some r) :
            r.1 = Int.ediv lo 60 + Int.ofNat n := by
          by_cases hc : 0 < cnt (Int.ediv lo 60 + Int.ofNat n) ts
          · have hh := (by simpa [hc] using h :
              0 < cnt (Int.ediv lo 60 + Int.ofNat n) ts ∧
                (Int.ediv lo 60 + Int.ofNat n, cnt (Int.ediv lo 60 + Int.ofNat n) ts) = r)
            exact (congrArg Prod.fst hh.2).symm
          · have hh := (by simpa [hc] using h :
              0 < cnt (Int.ediv lo 60 + Int.ofNat n) ts ∧
                (Int.ediv lo 60 + Int.ofNat n, cnt (Int.ediv lo 60 + Int.ofNat n) ts) = r)
            exact absurd hh.1 hc
        have hpk := key_of hfi
        have hqk := key_of hfj
        rw [hpk, hqk]
        have hlt : (Int.ofNat i : Int) < Int.ofNat span := Int.ofNat_lt.mpr hi'
        omega

/-! ### The reference side -/

private theorem keyLe_trans (a b c : Int × Nat) (hab : keyLe a b = true) (hbc : keyLe b c = true) :
    keyLe a c = true := by
  simp [keyLe] at hab hbc ⊢
  omega

private theorem keyLe_total (a b : Int × Nat) : keyLe a b = true ∨ keyLe b a = true := by
  simp [keyLe]
  omega

private theorem pairwise_key_of_sorted {l : List (Int × Nat)}
    (hs : l.Pairwise (fun a b => keyLe a b = true))
    (hnd : l.Pairwise (fun a b => a.1 ≠ b.1)) :
    l.Pairwise (fun a b => a.1 < b.1) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.pairwise_cons] at hs hnd ⊢
      refine ⟨?_, ih hs.2 hnd.2⟩
      intro y hy
      have hle : x.1 ≤ y.1 := by
        have := hs.1 y hy
        simpa [keyLe] using this
      have hne : x.1 ≠ y.1 := hnd.1 y hy
      omega

private theorem pairwise_key_ne_of_mem {l : List (Int × Nat)}
    (h : l.Pairwise (fun a b => a.1 ≠ b.1)) :
    ∀ x y, x ∈ l → y ∈ l → x ≠ y → x.1 ≠ y.1 := by
  induction l with
  | nil =>
      intro x y hx
      exact absurd hx List.not_mem_nil
  | cons z zs ih =>
      intro x y hx hy hxy
      have hp := List.pairwise_cons.mp h
      rcases List.mem_cons.mp hx with rfl | hxs
      · rcases List.mem_cons.mp hy with rfl | hys
        · exact absurd rfl hxy
        · exact hp.1 y hys
      · rcases List.mem_cons.mp hy with rfl | hys
        · intro hxz
          exact hp.1 x hxs hxz.symm
        · exact ih hp.2 x y hxs hys hxy

private theorem key_pairwise_nodup {l : List (Int × Nat)}
    (h : l.Pairwise (fun a b => a.1 ≠ b.1)) : l.Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons]
      refine ⟨?_, ih (List.pairwise_cons.mp h).2⟩
      intro hxmem
      exact (List.pairwise_cons.mp h).1 x hxmem rfl

private theorem pairwise_key_ne_erase {l : List (Int × Nat)}
    (h : l.Pairwise (fun a b => a.1 ≠ b.1)) (x : Int × Nat) :
    (l.erase x).Pairwise (fun a b => a.1 ≠ b.1) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hp := List.pairwise_cons.mp h
      by_cases hxa : a == x
      · simpa [List.erase_cons, hxa] using hp.2
      · rw [show (a :: as).erase x = a :: as.erase x by simp [hxa]]
        rw [List.pairwise_cons]
        refine ⟨?_, ih hp.2⟩
        intro q hq
        exact hp.1 q (List.mem_of_mem_erase hq)

private theorem perm_pairwise_key {l₁ l₂ : List (Int × Nat)} (hperm : l₁.Perm l₂)
    (hnd : l₂.Pairwise (fun a b => a.1 ≠ b.1)) :
    l₁.Pairwise (fun a b => a.1 ≠ b.1) := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => simp
      | cons z zs => simp at hperm
  | cons x xs ih =>
      cases l₂ with
      | nil => simp at hperm
      | cons z zs =>
          have hndup1 : (x :: xs).Nodup := hperm.symm.nodup (key_pairwise_nodup hnd)
          rw [List.pairwise_cons]
          refine ⟨?_, ?_⟩
          · intro y hy
            have hx2 : x ∈ z :: zs := hperm.subset List.mem_cons_self
            have hy2 : y ∈ z :: zs := hperm.subset (List.mem_cons_of_mem x hy)
            have hxy : x ≠ y := by
              intro heq
              exact (List.nodup_cons.mp hndup1).1 (by simpa [heq] using hy)
            exact pairwise_key_ne_of_mem hnd x y hx2 hy2 hxy
          · have hp' : xs.Perm ((z :: zs).erase x) := by
              simpa [List.erase_cons_head] using (List.Perm.erase x hperm)
            exact ih hp' (pairwise_key_ne_erase hnd x)

private theorem mem_ref (ts : List Int) (p : Int × Nat) :
    p ∈ refQuotientGroupSort ts ↔ 0 < cnt p.1 ts ∧ p.2 = cnt p.1 ts := by
  unfold refQuotientGroupSort
  let m := ts.foldl (fun acc t => addBucketCnt acc (bk t)) []
  change p ∈ sortBy keyLe m ↔ _
  rw [(sortBy_perm keyLe m).mem_iff]
  exact mem_fold_bucket ts p

private theorem ref_key_pairwise (ts : List Int) :
    (refQuotientGroupSort ts).Pairwise (fun a b => a.1 < b.1) := by
  unfold refQuotientGroupSort
  let m := ts.foldl (fun acc t => addBucketCnt acc (bk t)) []
  have hres := foldl_bucket_inv [] ts (by simp [BucketInv])
  have hnd : m.Pairwise (fun a b => a.1 ≠ b.1) := hres.1.1
  have hsorted : SortedBy keyLe (sortBy keyLe m) := sortBy_pairwise keyLe keyLe_trans keyLe_total m
  apply pairwise_key_of_sorted hsorted
  apply perm_pairwise_key (sortBy_perm keyLe m)
  exact hnd

private theorem part3 : ∀ (lo hi : Int) (ts : List Int),
    (∀ t ∈ ts, lo ≤ t ∧ t ≤ hi) →
      optDenseMinuteSweep lo hi ts = refQuotientGroupSort ts := by
  intro lo hi ts hbounds
  apply increasing_pairs_eq_of_mem_iff (sweep_key_pairwise lo hi ts) (ref_key_pairwise ts)
  intro p
  constructor
  · intro hp
    have hs := (mem_sweep_aux lo hi ts p).mp hp
    rcases hs with ⟨idx, hidx, hkey, hpos, hval⟩
    exact (mem_ref ts p).mpr ⟨hpos, hval⟩
  · intro hp
    apply (mem_sweep_aux lo hi ts p).mpr
    obtain ⟨hpos, hval⟩ := (mem_ref ts p).mp hp
    have hex : ∃ t ∈ ts, bk t = p.1 := by
      have hcntpos : 0 < (ts.filter fun t => bk t == p.1).length := by simpa [cnt] using hpos
      cases hf : (ts.filter fun t => bk t == p.1) with
      | nil => simp [hf] at hcntpos
      | cons t rest =>
          have htmem : t ∈ ts.filter (fun t => bk t == p.1) := by rw [hf]; simp
          have htp := List.mem_filter.mp htmem
          exact ⟨t, htp.1, by simpa using htp.2⟩
    obtain ⟨t, ht, hbk⟩ := hex
    have hblo : Int.ediv lo 60 ≤ p.1 := by
      rw [← hbk]
      exact part2 lo t (hbounds t ht).1
    have hbhi : p.1 ≤ Int.ediv hi 60 := by
      rw [← hbk]
      exact part2 t hi (hbounds t ht).2
    let span := if Int.ediv hi 60 < Int.ediv lo 60 then 0
      else (Int.ediv hi 60 - Int.ediv lo 60).toNat + 1
    have hspanpos : 0 < span := by
      dsimp [span]
      by_cases hb : Int.ediv hi 60 < Int.ediv lo 60
      · omega
      · simp [hb]
    let idx := (p.1 - Int.ediv lo 60).toNat
    have hidxlt : idx < span := by
      dsimp [idx, span]
      by_cases hb : Int.ediv hi 60 < Int.ediv lo 60
      · omega
      · simp [hb]
        have hnonneg : 0 ≤ p.1 - Int.ediv lo 60 := by omega
        have hle : p.1 - Int.ediv lo 60 ≤ Int.ediv hi 60 - Int.ediv lo 60 := by omega
        have hto : (p.1 - Int.ediv lo 60).toNat ≤
            (Int.ediv hi 60 - Int.ediv lo 60).toNat := by
          exact Int.toNat_le_toNat hle
        omega
    have hkey : Int.ediv lo 60 + Int.ofNat idx = p.1 := by
      dsimp [idx]
      have hnonneg : 0 ≤ p.1 - Int.ediv lo 60 := by omega
      rw [Int.toNat_of_nonneg hnonneg]
      omega
    exact ⟨idx, hidxlt, hkey, hpos, hval⟩

theorem sound : TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket.Goal := by
  exact ⟨part1, part2, part3⟩

end TinylambOpt.ResearchProofs.OffsetHeavyReverseTopNAndQuotientBucket
