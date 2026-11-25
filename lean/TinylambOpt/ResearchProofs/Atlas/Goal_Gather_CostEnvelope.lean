import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Gather_CostEnvelope

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

private def runCostAfter (S P prev : Nat) : List Nat → Nat
  | [] => 0
  | q :: qs => P + min S (P * (q - prev - 1)) + runCostAfter S P q qs

private theorem runCostAfter_eq_sum (S P : Nat) : ∀ (xs : List Nat) (prev : Nat),
    runCostAfter S P prev xs =
      ((List.map (fun p : Nat × Nat => P + min S (P * (p.2 - p.1 - 1))) ((prev :: xs).zip xs)).sum) := by
  intro xs
  induction xs with
  | nil => intro prev; simp [runCostAfter]
  | cons y ys ih =>
      intro prev
      rw [runCostAfter, ih y]
      simp [List.zip_cons_cons]

private theorem runCost_cons (S P x : Nat) (xs : List Nat) :
    runCost (x :: xs) S P = S + P + runCostAfter S P x xs := by
  cases xs with
  | nil => simp [runCost, runCostAfter]
  | cons y ys =>
      rw [runCostAfter_eq_sum]
      simp [runCost]

private theorem rca_le_len (S P : Nat) : ∀ (xs : List Nat) (prev : Nat),
    runCostAfter S P prev xs ≤ xs.length * (S + P) := by
  intro xs
  induction xs with
  | nil => intro prev; simp [runCostAfter]
  | cons y ys ih =>
      intro prev
      rw [runCostAfter]
      change P + min S (P * (y - prev - 1)) + runCostAfter S P y ys ≤ (ys.length + 1) * (S + P)
      have hmin : min S (P * (y - prev - 1)) ≤ S := Nat.min_le_left _ _
      have hih := ih y
      rw [Nat.add_mul]
      simp
      omega

private theorem runCost_le_len (S P : Nat) : ∀ xs : List Nat,
    runCost xs S P ≤ xs.length * (S + P) := by
  intro xs
  cases xs with
  | nil => simp [runCost]
  | cons x xs =>
      rw [runCost_cons]
      have h := rca_le_len S P xs x
      change S + P + runCostAfter S P x xs ≤ (xs.length + 1) * (S + P)
      rw [Nat.add_mul]
      simp
      omega

private theorem rca_span (S P b : Nat) : ∀ (xs : List Nat) (prev : Nat),
    xs.Pairwise (fun a c : Nat => a < c) →
    (∀ p ∈ xs, p < b) →
    (∀ p ∈ xs, prev < p) →
    prev < b →
    P + runCostAfter S P prev xs ≤ P * (b - prev) := by
  intro xs
  induction xs with
  | nil =>
      intro prev hp hmem hge hpb
      simp [runCostAfter]
      simpa using (Nat.mul_le_mul_left P (by omega : 1 ≤ b - prev))
  | cons x xs ih =>
      intro prev hp hmem hge hpb
      have hpair := List.pairwise_cons.mp hp
      have hxxs : ∀ p ∈ xs, x < p := by intro p hpin; exact hpair.1 p hpin
      have htail : xs.Pairwise (fun a c : Nat => a < c) := hpair.2
      have hmem' : ∀ p ∈ xs, p < b := by intro p hpin; exact hmem p (by simp [hpin])
      have hpx : prev < x := hge x (by simp)
      have hxb : x < b := hmem x (by simp)
      have hih := ih x htail hmem' hxxs hxb
      have hmin : min S (P * (x - prev - 1)) ≤ P * (x - prev - 1) := Nat.min_le_right _ _
      have hadd : x - prev - 1 + (b - x) = b - prev - 1 := by omega
      have hAB : P * (x - prev - 1) + P * (b - x) = P * (b - prev - 1) := by rw [← Nat.mul_add, hadd]
      have hlast : P * (b - prev - 1) + P ≤ P * (b - prev) := by
        have heq2 : P * (b - prev - 1) + P = P * ((b - prev - 1) + 1) := by
          rw [Nat.mul_add, Nat.mul_one]
        have heq3 : (b - prev - 1) + 1 = b - prev := by omega
        rw [heq2, heq3]
        exact Nat.le_refl _
      rw [runCostAfter]
      calc
        P + (P + min S (P * (x - prev - 1)) + runCostAfter S P x xs)
            = (P + min S (P * (x - prev - 1))) + (P + runCostAfter S P x xs) := by omega
        _ ≤ (P + P * (x - prev - 1)) + (P * (b - x)) :=
            Nat.add_le_add (Nat.add_le_add_left hmin P) hih
        _ = (P * (x - prev - 1) + P * (b - x)) + P := by omega
        _ = P * (b - prev - 1) + P := by rw [hAB]
        _ ≤ P * (b - prev) := hlast

private theorem nodup_eraseDups_nat_aux : ∀ n, ∀ xs : List Nat, xs.length ≤ n → xs.eraseDups.Nodup := by
  intro n
  induction n with
  | zero =>
      intro xs h
      cases xs with
      | nil => simp
      | cons x xs => simp at h
  | succ n ih =>
      intro xs h
      cases xs with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons]
          rw [List.nodup_cons]
          constructor
          · intro hxmem
            have hxmem' : x ∈ xs.filter (fun b => ! b == x) := List.mem_eraseDups.mp hxmem
            simp at hxmem'
          · exact ih _ (by
              have hfl : (xs.filter (fun b => ! b == x)).length ≤ xs.length :=
                List.length_filter_le _ _
              have hxl : xs.length ≤ n := by simpa using h
              omega)

private theorem nodup_eraseDups_nat (xs : List Nat) : xs.eraseDups.Nodup :=
  nodup_eraseDups_nat_aux xs.length xs (Nat.le_refl _)

private theorem pairwise_lt_of_pairwise_le_nodup : ∀ xs : List Nat,
    xs.Pairwise (fun a b : Nat => a ≤ b) → xs.Nodup → xs.Pairwise (fun a b : Nat => a < b) := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      intro hle hnd
      rw [List.pairwise_cons] at hle ⊢
      rw [List.nodup_cons] at hnd
      constructor
      · intro y hy
        have hxy : x ≤ y := hle.1 y hy
        have hne : x ≠ y := by
          intro heq
          apply hnd.1
          simpa [heq] using hy
        omega
      · exact ih hle.2 hnd.2

private theorem mem_le_of_pairwise_le_append_singleton : ∀ (ys : List Nat) (z : Nat),
    (ys ++ [z]).Pairwise (fun a b : Nat => a ≤ b) → ∀ p ∈ ys, p ≤ z := by
  intro ys
  induction ys with
  | nil => simp
  | cons y ys ih =>
      intro z hpair p hp
      have hc := List.pairwise_cons.mp hpair
      have hyz : y ≤ z := hc.1 z (by simp)
      simp only [List.mem_cons] at hp
      rcases hp with hpy | hpys
      · subst p
        exact hyz
      · have hpz : p ≤ z := ih z hc.2 p hpys
        omega

theorem sound_Gather_CostEnvelope : CostEnvelope.Goal := by
  intro ids cap setup price hcap
  let ps := (pages cap ids).mergeSort
  change runCost ps setup price ≤ min (ps.length * (setup + price))
    (setup + price * (ps.getLast?.getD 0 + 1 - ps.head?.getD 0))
  rw [Nat.le_min]
  constructor
  · exact runCost_le_len setup price ps
  · cases hps : ps with
    | nil => simp [runCost]
    | cons x xs =>
        have hle_pair : (x :: xs).Pairwise (fun a b : Nat => a ≤ b) := by
          rw [← hps]
          have hp := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
            (by
              intro a b c hab hbc
              have hab' : a ≤ b := by simpa using hab
              have hbc' : b ≤ c := by simpa using hbc
              simpa using (Nat.le_trans hab' hbc'))
            (by
              intro a b
              by_cases h : a ≤ b
              · simp [h]
              · have hab : b ≤ a := Nat.le_of_not_ge h
                simp [h, hab]) (pages cap ids)
          exact hp.imp (by intro a b h; simpa using h)
        have hnd_pages : (pages cap ids).Nodup := by
          unfold pages
          exact nodup_eraseDups_nat _
        have hnd : (x :: xs).Nodup := by
          rw [← hps]
          exact (List.mergeSort_perm (pages cap ids) (fun a b : Nat => a ≤ b)).symm.nodup hnd_pages
        have hstrict : (x :: xs).Pairwise (fun a b : Nat => a < b) :=
          pairwise_lt_of_pairwise_le_nodup _ hle_pair hnd
        let last := (x :: xs).getLast?.getD 0
        have hlast_some : (x :: xs).getLast? = some last := by
          dsimp [last]
          cases xs with
          | nil => simp
          | cons y ys => simp [List.getLast?]
        obtain ⟨ys, hys⟩ := (List.getLast?_eq_some_iff.mp hlast_some)
        have htail_pair : xs.Pairwise (fun a b : Nat => a < b) := (List.pairwise_cons.mp hstrict).2
        have hx_lt_all : ∀ p ∈ xs, x < p := by
          intro p hp
          exact (List.pairwise_cons.mp hstrict).1 p hp
        have hall_lt_b : ∀ p ∈ xs, p < last + 1 := by
          intro p hp
          have hp_in : p ∈ x :: xs := by simp [hp]
          rw [hys] at hp_in
          simp at hp_in
          rcases hp_in with hp_ys | hp_eq
          · have hle := mem_le_of_pairwise_le_append_singleton ys last (by simpa [hys] using hle_pair) p hp_ys
            omega
          · subst p
            omega
        have hx_lt_b : x < last + 1 := by
          have hp_in : x ∈ x :: xs := by simp
          rw [hys] at hp_in
          simp at hp_in
          rcases hp_in with hx_ys | hx_eq
          · have hle := mem_le_of_pairwise_le_append_singleton ys last (by simpa [hys] using hle_pair) x hx_ys
            omega
          · omega
        have hspan := rca_span setup price (last + 1) xs x
          htail_pair hall_lt_b hx_lt_all hx_lt_b
        rw [runCost_cons]
        dsimp [last] at hspan ⊢
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Gather_CostEnvelope
