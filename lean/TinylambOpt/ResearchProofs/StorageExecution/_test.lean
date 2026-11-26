import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
namespace T
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem pairwise_getD_le {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {i j : Nat}
    (hi : i < ys.length) (hj : j < ys.length) (hij : i ≤ j) : ys.getD i 0 ≤ ys.getD j 0 := by
  simp only [List.getD, List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hj, Option.getD_some]
  by_cases hij' : i < j
  · exact List.Pairwise.rel_getElem_of_lt hi hj hs hij'
  · have : i = j := by omega
    subst this; exact Nat.le_refl _

private theorem searchBound_spec (xs : List Nat) (hs : xs.Pairwise (· ≤ ·)) (v fuel lo hi : Nat)
    (hlo : ∀ i, i < lo → xs.getD i 0 < v)
    (hhi : ∀ i, hi ≤ i → i < xs.length → v ≤ xs.getD i 0)
    (hlohi : lo ≤ hi) (hhiN : hi ≤ xs.length)
    (hf : hi - lo < 2 ^ fuel) :
    let j := searchBound xs.toArray v fuel lo hi
    (∀ i, i < j → xs.getD i 0 < v) ∧
    (∀ i, j ≤ i → i < xs.length → v ≤ xs.getD i 0) ∧
    lo ≤ j ∧ j ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have he : lo = hi := by omega
      subst he
      exact ⟨hlo, hhi, Nat.le_refl _, Nat.le_refl _⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have he : lo = hi := by omega
        subst he
        have hj : searchBound xs.toArray v (fuel + 1) lo lo = lo := by
          simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.searchBound]
        rw [hj]
        exact ⟨hlo, hhi, Nat.le_refl _, Nat.le_refl _⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hmid_lo : lo ≤ mid := by omega
        have hmid_hi : mid < hi := by omega
        have hmidN : mid < xs.length := by omega
        have hmidv : xs.toArray[mid]?.getD 0 = xs.getD mid 0 := by
          simp [List.getD]
        by_cases hmv : xs.toArray[mid]?.getD 0 < v
        · have hmv' : xs.getD mid 0 < v := by simpa [hmidv] using hmv
          have hlo' : ∀ i, i < mid + 1 → xs.getD i 0 < v := by
            intro i hi_lt
            by_cases h : i ≤ mid
            · have hle := pairwise_getD_le hs (by omega : i < xs.length) hmidN h
              omega
            · omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by omega
          have hres := ih (mid + 1) hi hlo' hhi (by omega) hhiN hf'
          obtain ⟨r1, r2, r3, r4⟩ := hres
          have hge' : ¬ (lo ≥ hi) := by omega
          have hcond : xs[((lo + hi) / 2)]?.getD 0 < v := by
            simpa [List.getD] using hmv
          have hsb : searchBound xs.toArray v (fuel + 1) lo hi = searchBound xs.toArray v fuel (mid + 1) hi := by
            simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.searchBound, hge', hcond]
            rfl
          rw [hsb]
          exact ⟨r1, r2, by omega, r4⟩
        · have hmv' : v ≤ xs.getD mid 0 := by
            have : ¬ xs.getD mid 0 < v := by simpa [hmidv] using hmv
            omega
          have hhi' : ∀ i, mid ≤ i → i < xs.length → v ≤ xs.getD i 0 := by
            intro i hmi hil
            have hle := pairwise_getD_le hs hmidN hil (by omega : mid ≤ i)
            omega
          have hf' : mid - lo < 2 ^ fuel := by omega
          have hres := ih lo mid hlo hhi' hmid_lo (by omega) hf'
          obtain ⟨r1, r2, r3, r4⟩ := hres
          have hge' : ¬ (lo ≥ hi) := by omega
          have hcond : ¬ (xs[((lo + hi) / 2)]?.getD 0 < v) := by
            have : ¬ xs[mid]?.getD 0 < v := by simpa [List.getD] using (by omega : ¬ xs.getD mid 0 < v)
            simpa using this
          have hsb : searchBound xs.toArray v (fuel + 1) lo hi = searchBound xs.toArray v fuel lo mid := by
            simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.searchBound, hge', hcond]
            rfl
          rw [hsb]
          exact ⟨r1, r2, r3, by omega⟩

end T


private theorem take_countP_lt (ys : List Nat) (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ∀ i, i < ys.countP (fun y => y < v) → ys.getD i 0 < v := by
  induction ys with
  | nil => intro i hi; simp at hi
  | cons a as ih =>
      by_cases ha : a < v
      · simp [ha]
        intro i hi
        by_cases hi0 : i = 0
        · subst hi0; simp; exact ha
        · have hi' : i - 1 < as.countP (fun y => y < v) := by
            omega
          have has : as.Pairwise (· ≤ ·) := hs.tail
          have hval := ih has (i-1) hi'
          have hi_eq : i = (i - 1) + 1 := by omega
          rw [hi_eq]
          simp [List.getElem?_cons_succ]
          exact hval
      · simp [ha]
        intro i hi
        have hc : as.countP (fun y => y < v) = 0 := by
          -- since a≥v and sorted, all as≥a≥v, so none <v
          have has : as.Pairwise (· ≤ ·) := hs.tail
          have hall : ∀ y ∈ as, v ≤ y := by
            intro y hy
            have ha_le_y : a ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          exact List.countP_eq_zero.mpr (fun y hy => by
            have : v ≤ y := hall y hy
            simp [Nat.not_lt.mpr this])
        omega

private theorem drop_countP_ge (ys : List Nat) (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ∀ i, ys.countP (fun y => y < v) ≤ i → i < ys.length → v ≤ ys.getD i 0 := by
  induction ys with
  | nil => intro i hi hil; simp at hil
  | cons a as ih =>
      by_cases ha : a < v
      · simp [ha]
        intro i hi hil
        by_cases hi0 : i = 0
        · subst hi0
          omega
        · have hi' : as.countP (fun y => y < v) ≤ i - 1 := by omega
          have hil' : i - 1 < as.length := by omega
          have has : as.Pairwise (· ≤ ·) := hs.tail
          have hval := ih has (i-1) hi' hil'
          have hi_eq : i = (i - 1) + 1 := by omega
          rw [hi_eq]
          simp [List.getElem?_cons_succ]
          exact hval
      · have has : as.Pairwise (· ≤ ·) := hs.tail
        have hall : ∀ y ∈ as, v ≤ y := by
          intro y hy
          have ha_le_y : a ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hc : as.countP (fun y => y < v) = 0 := by
          exact List.countP_eq_zero.mpr (fun y hy => by
            have : v ≤ y := hall y hy
            simp [Nat.not_lt.mpr this])
        have hc0 : (a::as).countP (fun y => y < v) = 0 := by simp [List.countP_cons, ha, hc]
        intro i hi hil
        rw [hc0] at hi
        by_cases hi0 : i = 0
        · subst hi0; simp; omega
        · have hal : ∀ y ∈ as, a ≤ y := fun y hy => List.rel_of_pairwise_cons hs hy
          have hval : v ≤ (a::as).getD i 0 := by
            have hil' : i < as.length + 1 := by simpa [List.length_cons] using hil
            have hi1 : i - 1 < as.length := by omega
            have hi_eq : i = (i-1)+1 := by omega
            rw [hi_eq]
            simp [List.getElem?_cons_succ]
            have heq : as[i-1]?.getD 0 = as[i-1] := by simp [List.getElem?_eq_getElem hi1]
            rw [heq]
            have hay : a ≤ as[i-1] := List.rel_of_pairwise_cons hs (List.getElem_mem hi1)
            omega
          exact hval

private theorem lower_eq_countP (ys : List Nat) (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.lower ys v = ys.countP (fun y => y < v) := by
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.lower
  have hlo : ∀ i, i < 0 → ys.getD i 0 < v := by intro i hi; omega
  have hhi : ∀ i, ys.length ≤ i → i < ys.length → v ≤ ys.getD i 0 := by intro i hi hil; omega
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    by_cases h : ys.length = 0
    · simp [h]
    · have hw : width ys.length = ys.length.log2 + 1 := by simp [width, h]
      rw [hw]
      have : ys.length < 2 ^ (ys.length.log2 + 1) := by
        have := Nat.lt_pow_succ_log2 (x:=2) (y:=ys.length) (by decide : 1 < 2) (by omega : 0 < ys.length)
        simpa [Nat.log2] using this
      omega
  have spec := searchBound_spec ys hs v (width ys.length + 1) 0 ys.length hlo hhi (by omega) (by omega) hf
  obtain ⟨h1, h2, h3, h4⟩ := spec
  -- countP satisfies same bounds
  have hc_lt : ∀ i, i < ys.countP (fun y => y < v) → ys.getD i 0 < v := take_countP_lt ys hs v
  have hc_ge : ∀ i, ys.countP (fun y => y < v) ≤ i → i < ys.length → v ≤ ys.getD i 0 := drop_countP_ge ys hs v
  -- uniqueness
  have heq : searchBound ys.toArray v (width ys.length + 1) 0 ys.length = ys.countP (fun y => y < v) := by
    let j := searchBound ys.toArray v (width ys.length + 1) 0 ys.length
    let c := ys.countP (fun y => y < v)
    by_cases hlt : j < c
    · have h1j : ys.getD j 0 < v := hc_lt j hlt
      have h2j : v ≤ ys.getD j 0 := h2 j (by omega) (by
        have : j < ys.length := by omega
        exact this)
      omega
    · by_cases hgt : j > c
      · have h1c : ys.getD c 0 < v := h1 c (by omega)
      have h2c : v ≤ ys.getD c 0 := hc_ge c (by omega) (by
        have : c < ys.length := by omega
        exact this)
      omega
      · omega
  exact heq
