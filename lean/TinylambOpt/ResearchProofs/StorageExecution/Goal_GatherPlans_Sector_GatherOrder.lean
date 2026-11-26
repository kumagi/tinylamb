import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_GatherOrder

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Sector

private theorem covers_nil_iff (ys : List Nat) : ([] : List Extent) ∈ covers ys ↔ ys = [] := by
  induction ys with
  | nil => simp [covers]
  | cons y ys ih =>
      rw [covers, List.mem_flatMap]
      constructor
      · rintro ⟨q, hq, hmem⟩
        cases q <;> simp at hmem
      · intro h
        simp at h

private theorem covers_nonempty (ys : List Nat) : ∃ ps, ps ∈ covers ys := by
  induction ys with
  | nil => exact ⟨[], by simp [covers]⟩
  | cons y ys ih =>
      obtain ⟨q, hq⟩ := ih
      cases q with
      | nil =>
          exact ⟨[(y, y)], by
            rw [covers]
            exact List.mem_flatMap.mpr ⟨[], hq, by simp⟩⟩
      | cons e rest =>
          exact ⟨(y, y) :: e :: rest, by
            rw [covers]
            exact List.mem_flatMap.mpr ⟨e :: rest, hq, by simp⟩⟩

/-- The head extent always starts at the first input point. -/
private theorem covers_head : ∀ (ys : List Nat) (ps : List Extent),
    ps ∈ covers ys → ps.head?.map Prod.fst = ys.head? := by
  intro ys
  induction ys with
  | nil =>
      intro ps hps
      simp only [covers, List.mem_singleton] at hps
      subst ps
      rfl
  | cons y ys ih =>
      intro ps hps
      change ps ∈ (covers ys).flatMap (fun q => match q with
        | [] => [[(y, y)]]
        | (_, b) :: rest => [(y, y) :: q, (y, b) :: rest]) at hps
      rw [List.mem_flatMap] at hps
      obtain ⟨q, hq, hmem⟩ := hps
      cases q with
      | nil =>
          have hys : ys = [] := (covers_nil_iff ys).mp hq
          subst ys
          simp only [List.mem_singleton] at hmem
          subst ps
          rfl
      | cons e rest =>
          rcases e with ⟨a, b⟩
          simp at hmem
          rcases hmem with rfl | rfl <;> rfl

private theorem covers_endpoints : ∀ (ys : List Nat) (ps : List Extent),
    ps ∈ covers ys → ∀ e ∈ ps, e.1 ∈ ys ∧ e.2 ∈ ys := by
  intro ys
  induction ys with
  | nil =>
      intro ps hps e he
      simp only [covers, List.mem_singleton] at hps
      subst ps
      simp at he
  | cons y ys ih =>
      intro ps hps e he
      change ps ∈ (covers ys).flatMap (fun q => match q with
        | [] => [[(y, y)]]
        | (_, b) :: rest => [(y, y) :: q, (y, b) :: rest]) at hps
      rw [List.mem_flatMap] at hps
      obtain ⟨q, hq, hmem⟩ := hps
      cases q with
      | nil =>
          simp only [List.mem_singleton] at hmem
          subst ps
          simp at he
          subst e
          exact ⟨by simp, by simp⟩
      | cons f rest =>
          simp at hmem
          rcases hmem with rfl | rfl
          · rcases List.mem_cons.mp he with heq | he
            · subst e
              exact ⟨by simp, by simp⟩
            · obtain ⟨h1, h2⟩ := ih (f :: rest) hq e he
              exact ⟨List.mem_cons_of_mem y h1, List.mem_cons_of_mem y h2⟩
          · have hbmem : f.2 ∈ ys := (ih (f :: rest) hq f (by simp)).2
            rcases List.mem_cons.mp he with heq | he
            · subst e
              exact ⟨by simp, List.mem_cons_of_mem y hbmem⟩
            · obtain ⟨h1, h2⟩ := ih (f :: rest) hq e (List.mem_cons_of_mem f he)
              exact ⟨List.mem_cons_of_mem y h1, List.mem_cons_of_mem y h2⟩

/-- Every plan enumerated by `covers` covers all (sorted) input points. -/
private theorem covers_cover : ∀ (ys : List Nat) (ps : List Extent),
    ys.Pairwise (fun a b => a ≤ b) → ps ∈ covers ys →
    ∀ z ∈ ys, ∃ a b : Nat, (a, b) ∈ ps ∧ a ≤ z ∧ z ≤ b := by
  intro ys
  induction ys with
  | nil =>
      intro ps hpair hps z hz
      simp at hz
  | cons y ys ih =>
      intro ps hpair hps z hz
      have hpair_y := (List.pairwise_cons.mp hpair).1
      have hpair_tail := (List.pairwise_cons.mp hpair).2
      change ps ∈ (covers ys).flatMap (fun q => match q with
        | [] => [[(y, y)]]
        | (_, b) :: rest => [(y, y) :: q, (y, b) :: rest]) at hps
      rw [List.mem_flatMap] at hps
      obtain ⟨q, hq, hmem⟩ := hps
      cases q with
      | nil =>
          have hys : ys = [] := (covers_nil_iff ys).mp hq
          subst ys
          simp only [List.mem_singleton] at hmem
          subst ps
          simp at hz
          subst z
          exact ⟨y, y, by simp, by omega, by omega⟩
      | cons e rest =>
          rcases e with ⟨a, b⟩
          simp at hmem
          rcases hmem with rfl | rfl
          · simp only [List.mem_cons] at hz
            rcases hz with rfl | hz
            · exact ⟨z, z, by simp, by omega, by omega⟩
            · obtain ⟨u, v, he, hu, hv⟩ := ih ((a, b) :: rest) hpair_tail hq z hz
              exact ⟨u, v, List.mem_cons_of_mem _ he, hu, hv⟩
          · have hheadq : ((a, b) :: rest).head?.map Prod.fst = ys.head? :=
              covers_head ys ((a, b) :: rest) hq
            simp only [List.head?_cons, Option.map_some] at hheadq
            have hbmem : b ∈ ys := (covers_endpoints ys ((a, b) :: rest) hq (a, b) (by simp)).2
            have hy_le_b : y ≤ b := hpair_y b hbmem
            simp only [List.mem_cons] at hz
            rcases hz with rfl | hz
            · exact ⟨z, b, by simp, by omega, by omega⟩
            · have hyshead : ys.head? = some a := hheadq.symm
              obtain ⟨tl, htl⟩ := List.head?_eq_some_iff.mp hyshead
              subst ys
              have hy_le_a : y ≤ a := hpair_y a (by simp)
              obtain ⟨u, v, he, hu, hv⟩ := ih ((a, b) :: rest) hpair_tail hq z hz
              simp only [List.mem_cons] at he
              rcases he with he | he
              · cases he
                exact ⟨y, b, by simp, by omega, hv⟩
              · exact ⟨u, v, List.mem_cons_of_mem _ he, hu, hv⟩

private theorem points_sorted (xs : List Nat) :
    (points xs).Pairwise (fun a b => a ≤ b) := by
  unfold points
  have hs := List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (trans := fun a b c hab hbc => by
      simpa only [decide_eq_true_eq] using Nat.le_trans
        (of_decide_eq_true hab) (of_decide_eq_true hbc))
    (total := fun a b => by
      rcases Nat.le_total a b with h | h
      · simp [h]
      · simp [h])
    xs.eraseDups
  simpa only [decide_eq_true_eq] using hs

private theorem choose_mem (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  obtain ⟨q, hq⟩ := covers_nonempty (points xs)
  have hq' : q ∈ (covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b) :=
    List.mem_mergeSort.mpr hq
  unfold choose
  cases hhead : ((covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b)).head? with
  | none =>
      have hnil : (covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b) = [] :=
        List.head?_eq_none_iff.mp hhead
      rw [hnil] at hq'
      simp at hq'
  | some ps =>
      simp only [Option.getD_some]
      obtain ⟨tl, htl⟩ := List.head?_eq_some_iff.mp hhead
      have hmem : ps ∈ (covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b) := by
        rw [htl]
        simp
      exact List.mem_mergeSort.mp hmem

private theorem choose_covers (f : Price) (xs : List Nat) :
    ∀ x ∈ xs, ∃ a b : Nat, (a, b) ∈ choose f xs ∧ a ≤ x ∧ x ≤ b := by
  intro x hx
  have hxpts : x ∈ points xs := by simpa [points] using hx
  exact covers_cover (points xs) (choose f xs) (points_sorted xs) (choose_mem f xs) x hxpts

private def flat (ps : List Extent) : List Nat :=
  ps.flatMap (fun (a, b) => (List.range (b + 1 - a)).map (fun i => a + i))

private theorem mem_flat_of_extent {ps : List Extent} {a b x : Nat}
    (he : (a, b) ∈ ps) (hlo : a ≤ x) (hhi : x ≤ b) : x ∈ flat ps := by
  rw [flat, List.mem_flatMap]
  refine ⟨(a, b), he, ?_⟩
  rw [List.mem_map]
  refine ⟨x - a, ?_, ?_⟩
  · rw [List.mem_range]
    omega
  · omega

private theorem mem_tagged_of_flat {ps : List Extent} {src : Nat → Nat} {x : Nat}
    (hx : x ∈ flat ps) : (x, src x) ∈ tagged ps src := by
  rw [flat, List.mem_flatMap] at hx
  obtain ⟨⟨a, b⟩, he, hmem⟩ := hx
  rw [tagged, List.mem_flatMap]
  refine ⟨(a, b), he, ?_⟩
  rw [List.mem_map] at hmem ⊢
  obtain ⟨i, hi, hix⟩ := hmem
  refine ⟨i, hi, ?_⟩
  subst x
  rfl

private theorem tagged_value {ps : List Extent} {src : Nat → Nat} {p : Nat × Nat}
    (hp : p ∈ tagged ps src) : p.2 = src p.1 := by
  rw [tagged, List.mem_flatMap] at hp
  obtain ⟨⟨a, b⟩, he, hmem⟩ := hp
  rw [List.mem_map] at hmem
  obtain ⟨i, hi, rfl⟩ := hmem
  rfl

private theorem lookup_eq_of_mem {l : List (Nat × Nat)} {k v : Nat}
    (hall : ∀ p ∈ l, p.1 = k → p.2 = v) (hmem : (k, v) ∈ l) :
    List.lookup k l = some v := by
  induction l with
  | nil => simp at hmem
  | cons p ps ih =>
      rcases p with ⟨a, w⟩
      rw [List.lookup_cons]
      by_cases hka : k == a
      · have hkea : k = a := by simpa using hka
        simp [hka]
        exact hall (a, w) (by simp) hkea.symm
      · have hkne : k ≠ a := by
          intro h
          subst a
          simp at hka
        simp [hka]
        apply ih
        · intro q hq hqk
          exact hall q (by simp [hq]) hqk
        · simpa [hkne] using hmem

theorem sound_GatherOrder : Sector.GatherOrder.Goal := by
  intro shift startup xs src
  apply List.map_congr_left
  intro i hi
  obtain ⟨a, b, he, hlo, hhi⟩ := choose_covers (price shift startup) xs i hi
  have hflat : i ∈ flat (choose (price shift startup) xs) :=
    mem_flat_of_extent he hlo hhi
  have htagged : (i, src i) ∈ tagged (choose (price shift startup) xs) src :=
    mem_tagged_of_flat hflat
  apply lookup_eq_of_mem
  · intro p hp hpi
    rw [← hpi]
    exact tagged_value hp
  · exact htagged

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_GatherOrder
