import TinylambOpt.ResearchGoals.Frontiers.RankedJoins

namespace TinylambOpt.ResearchProofs.Frontiers.RankedJoinsProduct

open TinylambOpt.ResearchGoals.Frontiers.RankedJoins

private theorem sortedInt (xs : List Int) : xs.mergeSort.Pairwise (fun a b : Int => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
    (by intro a b c hab hbc; by_cases h1 : a ≤ b <;> by_cases h2 : b ≤ c <;> simp_all; omega)
    (by intro a b; by_cases h : a ≤ b <;> simp [h] <;> omega) xs
  exact h.imp (by intro a b hab; simpa using hab)

private theorem countLE_take : ∀ (s : List Int), s.Pairwise (fun a b => a ≤ b) →
    ∀ (v : Int) (k : Nat),
    (s.take k).countP (fun y => y ≤ v) = min k (s.countP (fun y => y ≤ v)) := by
  intro s
  induction s with
  | nil => intro hs v k; simp
  | cons a s ih =>
      intro hs v k
      have hs' := List.Pairwise.of_cons hs
      cases k with
      | zero => simp
      | succ k =>
          by_cases ha : a ≤ v
          · have hrel := ih hs' v k
            simp [ha, hrel]
          · have hnone : ∀ y ∈ s, ¬ (decide (y ≤ v)) = true := by
              intro y hy hbt
              have hay := (List.pairwise_cons.mp hs).1 y hy
              have hyv : y ≤ v := by simpa using hbt
              omega
            have hcnt0 : s.countP (fun y => y ≤ v) = 0 := List.countP_eq_zero.mpr hnone
            have hcntTake0 : (s.take k).countP (fun y => y ≤ v) = 0 :=
              List.countP_eq_zero.mpr (by
                intro y hy hbt
                exact hnone y ((List.take_sublist k s).subset hy) hbt)
            rw [List.take_succ_cons]
            simp [ha, hcnt0, hcntTake0]

private theorem countP_lt_eq_pred (s : List Int) (c : Int) :
    s.countP (fun y => y < c) = s.countP (fun y => y ≤ c - 1) := by
  apply List.countP_congr
  intro y hy
  simp
  omega

private theorem countP_le_eq_sum_do (s : List Int) (c : Int) :
    s.countP (fun y => y ≤ c) = s.countP (fun y => y < c) + s.countP (fun y => y == c) := by
  induction s with
  | nil => simp
  | cons a s ih =>
      by_cases hac : a ≤ c
      · by_cases hal : a < c
        · have haeq : (a == c) = false := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
        · have haeq : (a == c) = true := by simp; omega
          simp [hac, hal, haeq, ih]
          omega
      · have hal : ¬ a < c := by omega
        have haeq : (a == c) = false := by simp; omega
        simp [hac, hal, haeq, ih]

private theorem count_eq_of_countLE {A B : List Int}
    (hcnt : ∀ v, A.countP (fun y => y ≤ v) = B.countP (fun y => y ≤ v)) :
    A.Perm B := by
  apply List.perm_iff_count.mpr
  intro c
  have hleA := countP_le_eq_sum_do A c
  have hleB := countP_le_eq_sum_do B c
  rw [countP_lt_eq_pred] at hleA
  rw [countP_lt_eq_pred] at hleB
  have hleA' := hleA
  rw [hcnt c, hcnt (c-1)] at hleA'
  have heqA : A.countP (fun y => y == c) = B.countP (fun y => y == c) := by omega
  simpa [List.count_eq_countP] using heqA

private theorem cnt_qualified (ok : Int → Int → Bool) (X Y : List Int) (v : Int) :
    (qualified X Y ok).countP (fun z => z ≤ v) =
      (X.map fun x => Y.countP (fun y => ok x y && (x + y ≤ v))).sum := by
  simp only [qualified, List.countP_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  change (List.map (fun x_1 => x + x_1) (List.filter (ok x) Y)).countP (fun z => z ≤ v) = _
  rw [List.countP_map]
  rw [List.countP_filter]
  apply List.countP_congr
  intro y hy
  simp [Function.comp_apply, Bool.and_comm]

private theorem countP_not_add (p : Int → Bool) (s : List Int) :
    s.countP p + s.countP (fun y => !p y) = s.length := by
  induction s with
  | nil => simp
  | cons a s ih =>
      by_cases ha : p a <;> simp [ha] at * <;> omega

private theorem sum_eq_zero_of_mem_eq_zero : ∀ (l : List Nat), (∀ x ∈ l, x = 0) → l.sum = 0 := by
  intro l
  induction l with
  | nil => intro h; simp
  | cons a l ih =>
      intro h
      simp only [List.sum_cons]
      have ha : a = 0 := h a (by simp)
      have ht : l.sum = 0 := ih (by intro x hx; exact h x (by simp [hx]))
      omega

private theorem map_sum_le_map_sum {α} (l : List α) (f g : α → Nat) (h : ∀ x ∈ l, f x ≤ g x) :
    (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (by intro x hx; exact h x (by simp [hx]))
      omega

private theorem le_sum_of_mem {α} (l : List α) (f : α → Nat) {x : α} (hx : x ∈ l) :
    f x ≤ (l.map f).sum := by
  induction l with
  | nil => simp at hx
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rcases List.mem_cons.mp hx with hxa | hx
      · subst x
        omega
      · have hh := ih hx
        omega

private theorem mem_drop_index {l : List Int} {n : Nat} {y : Int} :
    y ∈ l.drop n → ∃ j, n + j < l.length ∧ l[n+j]? = some y := by
  induction n generalizing l with
  | zero =>
      intro hy
      rcases List.mem_iff_getElem?.mp hy with ⟨j, hj⟩
      rcases List.getElem?_eq_some_iff.mp hj with ⟨hjlt, _⟩
      exact ⟨j, by simpa using hjlt, by simpa using hj⟩
  | succ n ih =>
      intro hy
      cases l with
      | nil => simp at hy
      | cons a l =>
          change y ∈ List.drop n l at hy
          rcases ih hy with ⟨j, hjlt, hjval⟩
          refine ⟨j, ?_, ?_⟩
          · simpa [Nat.succ_add] using hjlt
          · simpa [Nat.succ_add] using hjval

private theorem mem_take_index {l : List Int} {n : Nat} {y : Int}
    (hy : y ∈ l.take n) : ∃ j, j < n ∧ l[j]? = some y := by
  rcases List.mem_iff_getElem?.mp hy with ⟨j, hj⟩
  rw [List.getElem?_take] at hj
  by_cases hjn : j < n
  · simp [hjn] at hj
    exact ⟨j, hjn, hj⟩
  · simp [hjn] at hj

private theorem length_take_of_mem_drop {s : List Int} {K : Nat} {y : Int}
    (hy : y ∈ s.drop K) : (s.take K).length = K := by
  rcases mem_drop_index hy with ⟨j, hj, _⟩
  exact List.length_take_of_le (by omega)

private theorem prefix_le_drop {s : List Int} (hs : s.Pairwise (fun a b => a ≤ b))
    {K : Nat} {y : Int} (hy : y ∈ s.drop K) : ∀ z ∈ s.take K, z ≤ y := by
  intro z hz
  rcases mem_drop_index hy with ⟨j, hjlt, hjval⟩
  rcases mem_take_index hz with ⟨l, hlt, hlval⟩
  have hjl : l < K + j := by omega
  have hlmem : l < s.length := (List.getElem?_eq_some_iff.mp hlval).1
  have hjmem : K + j < s.length := by simpa using hjlt
  have hrel := hs.rel_getElem_of_lt hlmem hjmem hjl
  have hzval : s[l] = z := (List.getElem?_eq_some_iff.mp hlval).2
  have hyval : s[K+j] = y := (List.getElem?_eq_some_iff.mp hjval).2
  simpa [hzval, hyval] using hrel

private theorem exists_mem_of_length_pos {l : List Int} {K : Nat}
    (hlen : l.length = K) (hK : 0 < K) : ∃ z, z ∈ l := by
  cases l with
  | nil => simp at hlen; omega
  | cons z t => exact ⟨z, by simp⟩

private theorem col_count_le_sum (ok : Int → Int → Bool) (A' B' : List Int) (v y : Int)
    (hy : y ∈ B') :
    A'.countP (fun z => ok z y && (z + y ≤ v)) ≤
      (A'.map fun z => B'.countP (fun w => ok z w && (z + w ≤ v))).sum := by
  induction A' with
  | nil => simp
  | cons z zs ih =>
      rw [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases hz : (ok z y && (z + y ≤ v)) = true
      · have hpos : 0 < B'.countP (fun w => ok z w && (z + w ≤ v)) :=
          (List.countP_pos_iff).mpr ⟨y, hy, hz⟩
        simp [hz]
        omega
      · simp [hz]
        omega

/-! Key idea: compare the two sorted prefix lists via `∀ v, countP (· ≤ v)`.
With `K = k + h` and sorted prefixes `A'`, `B'`, if a qualifying pair `(x,y)`
lies outside `A' × B'` and `x+y ≤ v`, then the per-axis hole bounds give at
least `K - h = k` qualifying pairs inside `A' × B'` with sum ≤ v. Hence, as
long as the truncated product has fewer than `k` elements ≤ v, no full pair is
missed; otherwise both truncated counts are ≥ k. -/
theorem HolesPerAxis_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.HolesPerAxis.Goal := by
  intro a b ok k h hrow hcol
  by_cases hk0 : k = 0
  · subst k
    simp [best]
  let K := k + h
  let A := a.mergeSort
  let B := b.mergeSort
  let A' := A.take K
  let B' := B.take K
  let S := (qualified a b ok).mergeSort
  let T := (qualified A' B' ok).mergeSort
  have hKpos : 0 < K := by dsimp [K]; omega
  have hSsort : S.Pairwise (fun x y : Int => x ≤ y) := by simpa [S] using sortedInt (qualified a b ok)
  have hTsort : T.Pairwise (fun x y : Int => x ≤ y) := by simpa [T] using sortedInt (qualified A' B' ok)
  have hAperm : A.Perm a := by simpa [A] using List.mergeSort_perm a (fun x y : Int => decide (x ≤ y))
  have hBperm : B.Perm b := by simpa [B] using List.mergeSort_perm b (fun x y : Int => decide (x ≤ y))
  have hAsort : A.Pairwise (fun x y : Int => x ≤ y) := by simpa [A] using sortedInt a
  have hBsort : B.Pairwise (fun x y : Int => x ≤ y) := by simpa [B] using sortedInt b
  have hAapp : A' ++ A.drop K = A := by simp [A']
  have hBapp : B' ++ B.drop K = B := by simp [B']
  have hrow' : ∀ x ∈ A', B'.countP (fun y => !ok x y) ≤ h := by
    intro x hx
    have hxA : x ∈ A := (List.take_subset K A) hx
    have hxa : x ∈ a := hAperm.mem_iff.mp hxA
    have happ0 : (B' ++ B.drop K).countP (fun y => !ok x y) = B'.countP (fun y => !ok x y) + (B.drop K).countP (fun y => !ok x y) := List.countP_append
    have happ : B.countP (fun y => !ok x y) = B'.countP (fun y => !ok x y) + (B.drop K).countP (fun y => !ok x y) := by
      simpa [hBapp] using happ0
    have hle : B'.countP (fun y => !ok x y) ≤ B.countP (fun y => !ok x y) := by omega
    rw [hBperm.countP_eq (fun y => !ok x y)] at hle
    exact Nat.le_trans hle (hrow x hxa)
  have hcol' : ∀ y ∈ B', A'.countP (fun x => !ok x y) ≤ h := by
    intro y hy
    have hyB : y ∈ B := (List.take_subset K B) hy
    have hyb : y ∈ b := hBperm.mem_iff.mp hyB
    have happ0 : (A' ++ A.drop K).countP (fun x => !ok x y) = A'.countP (fun x => !ok x y) + (A.drop K).countP (fun x => !ok x y) := List.countP_append
    have happ : A.countP (fun x => !ok x y) = A'.countP (fun x => !ok x y) + (A.drop K).countP (fun x => !ok x y) := by
      simpa [hAapp] using happ0
    have hle : A'.countP (fun x => !ok x y) ≤ A.countP (fun x => !ok x y) := by omega
    rw [hAperm.countP_eq (fun x => !ok x y)] at hle
    exact Nat.le_trans hle (hcol y hyb)
  have hcntTake : ∀ v, (S.take k).countP (fun z => z ≤ v) = (T.take k).countP (fun z => z ≤ v) := by
    intro v
    rw [countLE_take S hSsort v k, countLE_take T hTsort v k]
    let cnt : List Int → List Int → Nat := fun X Y =>
      (X.map fun x => Y.countP (fun y => ok x y && (x + y ≤ v))).sum
    have hScnt : S.countP (fun z => z ≤ v) = cnt A B := by
      change (qualified a b ok).mergeSort.countP (fun z => z ≤ v) = cnt A B
      rw [(List.mergeSort_perm (qualified a b ok) (fun x y : Int => decide (x ≤ y))).countP_eq (fun z => z ≤ v)]
      rw [cnt_qualified ok a b v]
      dsimp [cnt]
      calc
        (a.map fun x => b.countP (fun y => ok x y && (x + y ≤ v))).sum =
            (a.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro x hx
              exact (hBperm.countP_eq (fun y => ok x y && (x + y ≤ v))).symm
        _ = (A.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
              exact ((hAperm.map (fun x => B.countP (fun y => ok x y && (x + y ≤ v)))).sum_nat).symm
    have hTcnt : T.countP (fun z => z ≤ v) = cnt A' B' := by
      change (qualified A' B' ok).mergeSort.countP (fun z => z ≤ v) = cnt A' B'
      rw [(List.mergeSort_perm (qualified A' B' ok) (fun x y : Int => decide (x ≤ y))).countP_eq (fun z => z ≤ v)]
      rw [cnt_qualified ok A' B' v]
    rw [hScnt, hTcnt]
    have hBlen_of_drop : ∀ {y : Int}, y ∈ B.drop K → B'.length = K := by
      intro y hy
      simpa [B'] using length_take_of_mem_drop (s := B) (K := K) hy
    have hAlen_of_drop : ∀ {x : Int}, x ∈ A.drop K → A'.length = K := by
      intro x hx
      simpa [A'] using length_take_of_mem_drop (s := A) (K := K) hx
    have hokRow : ∀ x ∈ A', B'.length = K → k ≤ B'.countP (fun y => ok x y) := by
      intro x hx hlen
      have hsum := countP_not_add (fun y => ok x y) B'
      have hhole := hrow' x hx
      rw [hlen] at hsum
      dsimp [K] at hsum hhole ⊢
      omega
    have hokCol : ∀ y ∈ B', A'.length = K → k ≤ A'.countP (fun x => ok x y) := by
      intro y hy hlen
      have hsum := countP_not_add (fun x => ok x y) A'
      have hhole := hcol' y hy
      rw [hlen] at hsum
      dsimp [K] at hsum hhole ⊢
      omega
    have houtsideY : ∀ x y, x ∈ A' → y ∈ B.drop K → x + y ≤ v →
        k ≤ B'.countP (fun z => ok x z && (x + z ≤ v)) := by
      intro x y hx hydrop hxy
      have hBlen : B'.length = K := hBlen_of_drop hydrop
      have hple : ∀ z ∈ B', z ≤ y := by simpa [B'] using prefix_le_drop hBsort (K := K) hydrop
      have hokcnt := hokRow x hx hBlen
      apply Nat.le_trans hokcnt
      apply List.countP_mono_left
      intro z hz hoz
      have hzy := hple z hz
      have hzle : x + z ≤ v := by omega
      simp [hoz, hzle]
    have houtsideX : ∀ x y, x ∈ A.drop K → y ∈ B' → x + y ≤ v →
        k ≤ A'.countP (fun z => ok z y && (z + y ≤ v)) := by
      intro x y hxdrop hy hxy
      have hAlen : A'.length = K := hAlen_of_drop hxdrop
      have hple : ∀ z ∈ A', z ≤ x := by simpa [A'] using prefix_le_drop hAsort (K := K) hxdrop
      have hokcnt := hokCol y hy hAlen
      apply Nat.le_trans hokcnt
      apply List.countP_mono_left
      intro z hz hoz
      have hzx := hple z hz
      have hzle : z + y ≤ v := by omega
      simp [hoz, hzle]
    by_cases hlt : cnt A' B' < k
    · have hdropA : ((A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum = 0 := by
        apply sum_eq_zero_of_mem_eq_zero
        intro n hn
        rcases List.mem_map.mp hn with ⟨x, hxdrop, hxf⟩
        subst n
        rw [List.countP_eq_zero]
        intro y hy hpy
        have hp := hpy
        rw [Bool.and_eq_true] at hp
        have hxy : x + y ≤ v := by simpa using hp.2
        by_cases hydrop : y ∈ B.drop K
        · have hzex := exists_mem_of_length_pos (hAlen_of_drop hxdrop) hKpos
          rcases hzex with ⟨z, hz⟩
          have hzx := prefix_le_drop hAsort (K := K) hxdrop z hz
          have hzy : z + y ≤ v := by omega
          have hkc := houtsideY z y hz hydrop hzy
          have hmem := le_sum_of_mem A' (fun z => B'.countP (fun w => ok z w && (z + w ≤ v))) hz
          dsimp [cnt] at hlt
          omega
        · have hyTake : y ∈ B.take K := by
            have : y ∈ B' ++ B.drop K := by
              have hyB : y ∈ B := hy
              simpa [hBapp] using hyB
            rcases List.mem_append.mp this with hy' | hy'
            · simpa [B'] using hy'
            · exact False.elim (hydrop hy')
          have hkc := houtsideX x y hxdrop hyTake hxy
          have hcolbound := col_count_le_sum ok A' B' v y hyTake
          dsimp [cnt] at hlt
          omega
      have hterm : ∀ x ∈ A', B.countP (fun y => ok x y && (x + y ≤ v)) =
          B'.countP (fun y => ok x y && (x + y ≤ v)) := by
        intro x hx
        have hdropB : (B.drop K).countP (fun y => ok x y && (x + y ≤ v)) = 0 := by
          rw [List.countP_eq_zero]
          intro y hy hpy
          have hp := hpy
          rw [Bool.and_eq_true] at hp
          have hxy : x + y ≤ v := by simpa using hp.2
          have hkc := houtsideY x y hx hy hxy
          have hmem := le_sum_of_mem A' (fun z => B'.countP (fun w => ok z w && (z + w ≤ v))) hx
          dsimp [cnt] at hlt
          omega
        have happ0 : (B' ++ B.drop K).countP (fun y => ok x y && (x + y ≤ v)) =
            B'.countP (fun y => ok x y && (x + y ≤ v)) + (B.drop K).countP (fun y => ok x y && (x + y ≤ v)) := List.countP_append
        have happ : B.countP (fun y => ok x y && (x + y ≤ v)) =
            B'.countP (fun y => ok x y && (x + y ≤ v)) + (B.drop K).countP (fun y => ok x y && (x + y ≤ v)) := by
          simpa [hBapp] using happ0
        rw [happ, hdropB, Nat.add_zero]
      have hfull_eq : cnt A B = cnt A' B' := by
        dsimp [cnt]
        have hdecomp : (A.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum =
            (A'.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum +
            ((A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
          calc
            (A.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum =
                ((A' ++ A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by rw [hAapp]
            _ = (A'.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum +
                ((A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
                  rw [List.map_append, List.sum_append]
        rw [hdecomp, hdropA, Nat.add_zero]
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        exact hterm x hx
      rw [hfull_eq]
    · have hkTrunc : k ≤ cnt A' B' := by omega
      dsimp [cnt] at hkTrunc
      have hinner : ∀ x ∈ A', B'.countP (fun y => ok x y && (x + y ≤ v)) ≤
          B.countP (fun y => ok x y && (x + y ≤ v)) := by
        intro x hx
        have happ0 : (B' ++ B.drop K).countP (fun y => ok x y && (x + y ≤ v)) =
            B'.countP (fun y => ok x y && (x + y ≤ v)) + (B.drop K).countP (fun y => ok x y && (x + y ≤ v)) := List.countP_append
        have happ : B.countP (fun y => ok x y && (x + y ≤ v)) =
            B'.countP (fun y => ok x y && (x + y ≤ v)) + (B.drop K).countP (fun y => ok x y && (x + y ≤ v)) := by
          simpa [hBapp] using happ0
        omega
      have hsum : (A'.map fun x => B'.countP (fun y => ok x y && (x + y ≤ v))).sum ≤
          (A'.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum :=
        map_sum_le_map_sum A' _ _ hinner
      have hdecomp : (A.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum =
          (A'.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum +
          ((A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
        calc
          (A.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum =
              ((A' ++ A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by rw [hAapp]
          _ = (A'.map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum +
              ((A.drop K).map fun x => B.countP (fun y => ok x y && (x + y ≤ v))).sum := by
                rw [List.map_append, List.sum_append]
      have hkFull : k ≤ cnt A B := by
        dsimp [cnt]
        rw [hdecomp]
        omega
      rw [Nat.min_eq_left hkTrunc, Nat.min_eq_left hkFull]
  have hperm := count_eq_of_countLE hcntTake
  have htake : S.take k = T.take k := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    · intro a b ha hb hab hba; omega
    · exact List.Pairwise.take hSsort
    · exact List.Pairwise.take hTsort
    · exact hperm
  simpa [best, S, T, A', B', A, B, K] using htake

private theorem filter_true_id : ∀ (l : List Int), l.filter (fun _ => true) = l := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

/-! `BoundedProduct` is `HolesPerAxis` with the everywhere-true predicate and
`h = 0`: there are no holes, and `qualified _ _ (fun _ _ => true) = sums`. -/
theorem BoundedProduct_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.BoundedProduct.Goal := by
  intro a b k
  have h := HolesPerAxis_sound a b (fun _ _ => true) k 0 (by simp) (by simp)
  simpa [qualified, sums, filter_true_id] using h

end TinylambOpt.ResearchProofs.Frontiers.RankedJoinsProduct
