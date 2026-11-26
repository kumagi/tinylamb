import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Gather.OptimalRead` — `∀ ids cap setup price ext, 0 < cap → (∀ e ∈ ext, e.1 < e.2) →
  (∀ p ∈ pages cap ids, covers ext p = true) → runCost (pages cap ids |>.mergeSort)
  setup price ≤ readCost setup price ext`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Gather_OptimalRead

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

private def runCostAfter (S P prev : Nat) : List Nat → Nat
  | [] => 0
  | q :: qs => P + min S (P * (q - prev - 1)) + runCostAfter S P q qs

private theorem runCostAfter_eq_sum (S P : Nat) : ∀ (xs : List Nat) (prev : Nat),
    runCostAfter S P prev xs = ((List.map (fun p : Nat × Nat => P + min S (P * (p.2 - p.1 - 1))) ((prev :: xs).zip xs)).sum) := by
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

private def lastD (d : Nat) : List Nat → Nat
  | [] => d
  | [x] => x
  | _ :: y :: ys => lastD d (y :: ys)

private theorem rca_append (S P : Nat) : ∀ (u : List Nat) (prev : Nat) (v : List Nat),
  runCostAfter S P prev (u ++ v) =
    if u = [] then runCostAfter S P prev v
    else runCostAfter S P prev u + runCostAfter S P (lastD 0 u) v := by
  intro u
  induction u with
  | nil => intro prev v; simp [runCostAfter]
  | cons x u ih =>
      cases u with
      | nil => intro prev v; simp [runCostAfter, lastD]
      | cons y ys =>
          intro prev v
          rw [List.cons_append, runCostAfter, ih x v]
          simp [runCostAfter, lastD, Nat.add_assoc]

private theorem lastD_lt (d b : Nat) : ∀ {xs : List Nat}, (∀ p ∈ xs, p < b) → xs ≠ [] → lastD d xs < b := by
  intro xs h hne
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil => simpa [lastD] using h x (by simp)
      | cons y ys =>
          exact ih (by intro p hp; exact h p (by simp [hp])) (by simp)

private theorem lastD_ge_head (d : Nat) : ∀ (x : Nat) (xs : List Nat),
    (x :: xs).Pairwise (fun a b : Nat => a < b) → x ≤ lastD d (x :: xs) := by
  intro x xs
  induction xs generalizing x with
  | nil => simp [lastD]
  | cons y ys ih =>
      intro hp
      have hpair := List.pairwise_cons.mp hp
      have hxy : x < y := hpair.1 y (by simp)
      have hy : y ≤ lastD d (y :: ys) := ih y hpair.2
      change x ≤ lastD d (y :: ys)
      omega

private theorem rca_mono_prev {S P prev₁ prev₂ : Nat} (hprev : prev₁ ≤ prev₂) :
    ∀ {xs : List Nat}, (∀ p ∈ xs, prev₂ ≤ p) → runCostAfter S P prev₂ xs ≤ runCostAfter S P prev₁ xs := by
  intro xs hx
  induction xs with
  | nil => simp [runCostAfter]
  | cons q qs ih =>
      have hq2 : prev₂ ≤ q := hx q (by simp)
      have hsub : q - prev₂ - 1 ≤ q - prev₁ - 1 := by omega
      have hmul : P * (q - prev₂ - 1) ≤ P * (q - prev₁ - 1) := Nat.mul_le_mul_left P hsub
      have hmin : min S (P * (q - prev₂ - 1)) ≤ min S (P * (q - prev₁ - 1)) := by
        rw [Nat.le_min]
        constructor
        · exact Nat.min_le_left _ _
        · exact Nat.le_trans (Nat.min_le_right _ _) hmul
      rw [runCostAfter, runCostAfter]
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
        omega
      rw [runCostAfter]
      calc
        P + (P + min S (P * (x - prev - 1)) + runCostAfter S P x xs)
            = (P + min S (P * (x - prev - 1))) + (P + runCostAfter S P x xs) := by omega
        _ ≤ (P + P * (x - prev - 1)) + (P * (b - x)) :=
            Nat.add_le_add (Nat.add_le_add_left hmin P) hih
        _ = (P * (x - prev - 1) + P * (b - x)) + P := by omega
        _ = P * (b - prev - 1) + P := by rw [hAB]
        _ ≤ P * (b - prev) := hlast

private def splitBelow (b : Nat) : List Nat → List Nat × List Nat
  | [] => ([], [])
  | x :: xs =>
      if x < b then
        let (i, o) := splitBelow b xs
        (x :: i, o)
      else ([], x :: xs)

private theorem splitBelow_append (b : Nat) : ∀ xs : List Nat,
    let (i, o) := splitBelow b xs
    xs = i ++ o := by
  intro xs
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hxb : x < b
      · rcases hsplit : splitBelow b xs with ⟨i, o⟩
        rw [hsplit] at ih
        have hsp : splitBelow b (x :: xs) = (x :: i, o) := by
          unfold splitBelow; rw [if_pos hxb, hsplit]
        rw [hsp]
        simp [ih]
      · have hsp : splitBelow b (x :: xs) = ([], x :: xs) := by
          unfold splitBelow; rw [if_neg hxb]
        rw [hsp]
        simp

private theorem splitBelow_fst_lt (b : Nat) : ∀ xs : List Nat,
    ∀ p ∈ (splitBelow b xs).1, p < b := by
  intro xs
  induction xs with
  | nil => simp [splitBelow]
  | cons x xs ih =>
      by_cases hxb : x < b
      · rcases hsplit : splitBelow b xs with ⟨i, o⟩
        rw [hsplit] at ih
        have hsp : splitBelow b (x :: xs) = (x :: i, o) := by
          unfold splitBelow; rw [if_pos hxb, hsplit]
        rw [hsp]
        intro p hp
        simp only [List.mem_cons] at hp
        rcases hp with rfl | hp
        · exact hxb
        · exact ih p hp
      · have hsp : splitBelow b (x :: xs) = ([], x :: xs) := by
          unfold splitBelow; rw [if_neg hxb]
        rw [hsp]
        intro p hp
        simp at hp

private theorem splitBelow_snd_ge (b : Nat) : ∀ xs : List Nat,
    xs.Pairwise (fun a c : Nat => a < c) →
    ∀ p ∈ (splitBelow b xs).2, b ≤ p := by
  intro xs
  induction xs with
  | nil => simp [splitBelow]
  | cons x xs ih =>
      intro hp
      have hpair := List.pairwise_cons.mp hp
      by_cases hxb : x < b
      · rcases hsplit : splitBelow b xs with ⟨i, o⟩
        have hsp : splitBelow b (x :: xs) = (x :: i, o) := by
          unfold splitBelow; rw [if_pos hxb, hsplit]
        rw [hsp]
        have ih' := ih hpair.2
        rw [hsplit] at ih'
        exact ih'
      · have hsp : splitBelow b (x :: xs) = ([], x :: xs) := by
          unfold splitBelow; rw [if_neg hxb]
        rw [hsp]
        intro q hqin
        simp only [List.mem_cons] at hqin
        rcases hqin with rfl | hqin
        · omega
        · have hxq : x < q := hpair.1 q hqin
          omega

private theorem filter_out_eq_split (a b : Nat) : ∀ xs : List Nat,
    xs.Pairwise (fun a c : Nat => a < c) →
    (∀ p ∈ xs, a ≤ p) →
    xs.filter (fun p => p < a || b ≤ p) = (splitBelow b xs).2 := by
  intro xs
  induction xs with
  | nil => simp [splitBelow]
  | cons x0 xs ih =>
      intro hp hge
      have hpair := List.pairwise_cons.mp hp
      have hxa : a ≤ x0 := hge x0 (by simp)
      have htail : ∀ p ∈ xs, a ≤ p := by intro p hpin; exact hge p (by simp [hpin])
      by_cases hxb : x0 < b
      · rcases hsplit : splitBelow b xs with ⟨i, o⟩
        have hsp : splitBelow b (x0 :: xs) = (x0 :: i, o) := by
          unfold splitBelow; rw [if_pos hxb, hsplit]
        rw [hsp]
        have hdrop : (x0 < a || b ≤ x0) = false := by simp [hxa, hxb]
        simp only [List.filter_cons, hdrop, ↓reduceIte]
        rw [ih hpair.2 htail]
        rw [hsplit]
        rfl
      · have hsp : splitBelow b (x0 :: xs) = ([], x0 :: xs) := by
          unfold splitBelow; rw [if_neg hxb]
        rw [hsp]
        apply List.filter_eq_self.mpr
        intro q hqin
        simp only [List.mem_cons] at hqin
        by_cases hqeq : q = x0
        · subst q
          have hbx : b ≤ x0 := by omega
          simp [hbx]
        · have hqin' : q ∈ xs := by simpa [hqeq] using hqin
          have hxq : x0 < q := hpair.1 q hqin'
          have hbx : b ≤ x0 := by omega
          have hbq : b ≤ q := by omega
          simp [hbq]

private theorem prefixSublist (l₁ l₂ : List Nat) : l₁.Sublist (l₁ ++ l₂) := by
  induction l₁ with
  | nil => simp
  | cons a l ih => exact List.Sublist.cons_cons a ih

private theorem seqBound (S P a b : Nat) (hab : a < b) : ∀ (ps : List Nat) (prev : Nat),
    ps.Pairwise (fun x y : Nat => x < y) →
    prev < a →
    runCostAfter S P prev ps ≤ P * (b - a) + S +
      runCostAfter S P prev (ps.filter (fun p => p < a || b ≤ p)) := by
  intro ps
  induction ps with
  | nil =>
      intro prev hp hpa
      simp [runCostAfter]

  | cons x xs ih =>
      intro prev hp hpa
      have hpair := List.pairwise_cons.mp hp
      have hxxs : ∀ p ∈ xs, x < p := by intro p hpin; exact hpair.1 p hpin
      have htail : xs.Pairwise (fun u v : Nat => u < v) := hpair.2
      by_cases hxa : x < a
      · have hfilter : (x :: xs).filter (fun p => p < a || b ≤ p) =
            x :: (xs.filter (fun p => p < a || b ≤ p)) := by
          simp [hxa]
        have hih := ih x htail hxa
        calc
          runCostAfter S P prev (x :: xs) = P + min S (P * (x - prev - 1)) + runCostAfter S P x xs := by rfl
          _ ≤ P + min S (P * (x - prev - 1)) + (P * (b - a) + S + runCostAfter S P x (xs.filter (fun p => p < a || b ≤ p))) :=
            Nat.add_le_add_left hih _
          _ = P * (b - a) + S + (P + min S (P * (x - prev - 1)) + runCostAfter S P x (xs.filter (fun p => p < a || b ≤ p))) := by omega
          _ = P * (b - a) + S + runCostAfter S P prev ((x :: xs).filter (fun p => p < a || b ≤ p)) := by
            rw [hfilter]
            rfl
      · have hax : a ≤ x := by omega
        by_cases hxb : x < b
        · have hgeall : ∀ p ∈ x :: xs, a ≤ p := by
            intro p hpin
            simp only [List.mem_cons] at hpin
            rcases hpin with rfl | hpin
            · exact hax
            · have hxp : x < p := hxxs p hpin
              omega
          have hfilter := filter_out_eq_split a b (x :: xs) hp hgeall
          rcases hsplit : splitBelow b xs with ⟨i, o⟩
          have hspins : splitBelow b (x :: xs) = (x :: i, o) := by
            unfold splitBelow; rw [if_pos hxb, hsplit]
          have happend : x :: xs = (x :: i) ++ o := by
            have ha := splitBelow_append b (x :: xs)
            rw [hspins] at ha
            exact ha
          rw [hspins] at hfilter
          have hfilter' : (x :: xs).filter (fun p => p < a || b ≤ p) = o := by simpa using hfilter
          rw [hfilter']
          have hmem_i_xs : ∀ {p : Nat}, p ∈ i → p ∈ xs := by
            intro p hpin
            have ha := splitBelow_append b xs
            rw [hsplit] at ha
            rw [ha]
            exact List.mem_append.mpr (Or.inl hpin)
          have hpair_ins : (x :: i).Pairwise (fun u v : Nat => u < v) := by
            rw [List.pairwise_cons]
            constructor
            · intro p hpin
              exact hxxs p (hmem_i_xs hpin)
            · exact List.Pairwise.sublist (by
                rw [show xs = i ++ o by
                  have ha := splitBelow_append b xs
                  rw [hsplit] at ha
                  exact ha]
                exact prefixSublist i o) htail
          have hi_pair : i.Pairwise (fun u v : Nat => u < v) := (List.pairwise_cons.mp hpair_ins).2
          have hi_lt : ∀ p ∈ i, p < b := by
            intro p hpin
            exact splitBelow_fst_lt b xs p (by rw [hsplit]; exact hpin)
          have hi_gt : ∀ p ∈ i, x < p := by intro p hpin; exact hxxs p (hmem_i_xs hpin)
          have hpin_lt : ∀ p ∈ x :: i, p < b := by
            intro p hpin
            simp only [List.mem_cons] at hpin
            rcases hpin with rfl | hpin
            · exact hxb
            · exact hi_lt p hpin
          have hlast_lt : lastD 0 (x :: i) < b := lastD_lt 0 b hpin_lt (by simp)
          have hlast_prev : prev ≤ lastD 0 (x :: i) := by
            have hle : x ≤ lastD 0 (x :: i) := lastD_ge_head 0 x i hpair_ins
            omega
          have hout_ge : ∀ p ∈ o, lastD 0 (x :: i) ≤ p := by
            intro p hpin
            have hbp : b ≤ p := by
              have hg := splitBelow_snd_ge b xs htail
              rw [hsplit] at hg
              exact hg p hpin
            omega
          have hmono := rca_mono_prev (S := S) (P := P) hlast_prev hout_ge
          have hspan := rca_span S P b i x hi_pair hi_lt hi_gt hxb
          have hfirst : P + min S (P * (x - prev - 1)) ≤ P + S := by
            exact Nat.add_le_add_left (Nat.min_le_left S (P * (x - prev - 1))) P
          have hins : runCostAfter S P prev (x :: i) ≤ S + P * (b - a) := by
            rw [runCostAfter]
            have hsub : P * (b - x) ≤ P * (b - a) := Nat.mul_le_mul_left P (by omega)
            omega
          rw [happend]
          change runCostAfter S P prev (x :: (i ++ o)) ≤ P * (b - a) + S + runCostAfter S P prev o
          have hconcat := rca_append S P (x :: i) prev o
          simp at hconcat
          rw [hconcat]
          omega
        · have hbx : b ≤ x := by omega
          have hall : ∀ p ∈ x :: xs, b ≤ p := by
            intro p hpin
            simp only [List.mem_cons] at hpin
            rcases hpin with rfl | hpin
            · exact hbx
            · have hxp : x < p := hxxs p hpin
              omega
          have hfilter : (x :: xs).filter (fun p => p < a || b ≤ p) = x :: xs := by
            apply List.filter_eq_self.mpr
            intro p hpin
            have hbp := hall p hpin
            simp [hbp]
          rw [hfilter]
          omega

private theorem splitIneq (S P a b : Nat) (hab : a < b) : ∀ ps : List Nat,
    ps.Pairwise (fun x y : Nat => x < y) →
    runCost ps S P ≤ S + P * (b - a) + runCost (ps.filter (fun p => p < a || b ≤ p)) S P := by
  intro ps hp
  cases ps with
  | nil => simp [runCost]
  | cons x xs =>
      have hpair := List.pairwise_cons.mp hp
      have hxxs : ∀ p ∈ xs, x < p := by intro p hpin; exact hpair.1 p hpin
      have htail : xs.Pairwise (fun u v : Nat => u < v) := hpair.2
      rw [runCost_cons]
      by_cases hxa : x < a
      · have hfilter : (x :: xs).filter (fun p => p < a || b ≤ p) =
            x :: (xs.filter (fun p => p < a || b ≤ p)) := by
          simp [hxa]
        rw [hfilter]
        rw [runCost_cons]
        have hdrop : (x :: xs).filter (fun p => p < a || b ≤ p) =
            x :: (xs.filter (fun p => p < a || b ≤ p)) := hfilter
        have hs := seqBound S P a b hab xs x htail hxa
        omega
      · have hax : a ≤ x := by omega
        by_cases hxb : x < b
        · have hgeall : ∀ p ∈ x :: xs, a ≤ p := by
            intro p hpin
            simp only [List.mem_cons] at hpin
            rcases hpin with rfl | hpin
            · exact hax
            · have hxp : x < p := hxxs p hpin
              omega
          have hfilter := filter_out_eq_split a b (x :: xs) hp hgeall
          cases xs with
          | nil =>
              have hf : (x :: ([] : List Nat)).filter (fun p => p < a || b ≤ p) = [] := by
                simp [hax, hxb]
              rw [hf]
              simp [runCostAfter]
              simp [runCost]
              have hge : 1 ≤ b - a := by omega
              have hmul := Nat.mul_le_mul_left P hge
              omega
          | cons y ys =>
              rcases hsplit : splitBelow b (y :: ys) with ⟨i, o⟩
              have hspins : splitBelow b (x :: y :: ys) = (x :: i, o) := by
                unfold splitBelow; rw [if_pos hxb, hsplit]
              have happend : x :: y :: ys = (x :: i) ++ o := by
                have ha := splitBelow_append b (x :: y :: ys)
                rw [hspins] at ha
                exact ha
              rw [hspins] at hfilter
              have hfilter' : (x :: y :: ys).filter (fun p => p < a || b ≤ p) = o := by simpa using hfilter
              rw [hfilter']
              have hmem_i_ys : ∀ {p : Nat}, p ∈ i → p ∈ y :: ys := by
                intro p hpin
                have ha := splitBelow_append b (y :: ys)
                rw [hsplit] at ha
                rw [ha]
                exact List.mem_append.mpr (Or.inl hpin)
              have hpair_ins : (x :: i).Pairwise (fun u v : Nat => u < v) := by
                rw [List.pairwise_cons]
                constructor
                · intro p hpin
                  exact hxxs p (by simp [hmem_i_ys hpin])
                · exact List.Pairwise.sublist (by
                    rw [show y :: ys = i ++ o by
                      have ha := splitBelow_append b (y :: ys)
                      rw [hsplit] at ha
                      exact ha]
                    exact prefixSublist i o) htail
              have hi_pair : i.Pairwise (fun u v : Nat => u < v) := (List.pairwise_cons.mp hpair_ins).2
              have hi_lt : ∀ p ∈ i, p < b := by
                intro p hpin
                exact splitBelow_fst_lt b (y :: ys) p (by rw [hsplit]; exact hpin)
              have hi_gt : ∀ p ∈ i, x < p := by
                intro p hpin
                exact hxxs p (by simp [hmem_i_ys hpin])
              have hspan := rca_span S P b i x hi_pair hi_lt hi_gt hxb
              rw [show y :: ys = i ++ o by simpa using congrArg List.tail happend]
              change S + P + runCostAfter S P x (i ++ o) ≤ S + P * (b - a) + runCost o S P
              cases i with
              | nil =>
                  simp [runCostAfter] at hspan
                  cases o with
                  | nil =>
                      simp [runCostAfter, runCost]
                      have hba : b - x ≤ b - a := by omega
                      have hmul := Nat.mul_le_mul_left P hba
                      omega
                  | cons q qs =>
                      rw [runCost_cons]
                      change S + P + runCostAfter S P x (q :: qs) ≤
                        S + P * (b - a) + (S + P + runCostAfter S P q qs)
                      change S + P + (P + min S (P * (q - x - 1)) + runCostAfter S P q qs) ≤
                        S + P * (b - a) + (S + P + runCostAfter S P q qs)
                      have hminle : min S (P * (q - x - 1)) ≤ S := Nat.min_le_left _ _
                      have hba : b - x ≤ b - a := by omega
                      have hmul := Nat.mul_le_mul_left P hba
                      omega
              | cons z zs =>
                  change S + P + runCostAfter S P x (z :: (zs ++ o)) ≤ S + P * (b - a) + runCost o S P
                  have hconcat := rca_append S P (z :: zs) x o
                  simp at hconcat
                  rw [hconcat]
                  have hpin_lt : ∀ p ∈ z :: zs, p < b := by
                    intro p hpin
                    exact hi_lt p (by simp [hpin])
                  have hlast_lt : lastD 0 (z :: zs) < b :=
                    lastD_lt 0 b hpin_lt (by simp)
                  have hout_ge : ∀ p ∈ o, lastD 0 (z :: zs) ≤ p := by
                    intro p hpin
                    have hbp : b ≤ p := by
                      have hg := splitBelow_snd_ge b (y :: ys) htail
                      rw [hsplit] at hg
                      exact hg p hpin
                    omega
                  have hxle : x ≤ lastD 0 (z :: zs) :=
                    lastD_ge_head 0 x (z :: zs) hpair_ins
                  have hmono := rca_mono_prev (S := S) (P := P) hxle hout_ge
                  change S + P + (runCostAfter S P x (z :: zs) + runCostAfter S P (lastD 0 (z :: zs)) o) ≤
                    S + P * (b - a) + runCost o S P
                  cases o with
                  | nil =>
                      change S + P + (runCostAfter S P x (z :: zs) + 0) ≤
                        S + P * (b - a) + 0
                      have hba : b - x ≤ b - a := by omega
                      have hmul := Nat.mul_le_mul_left P hba
                      omega
                  | cons q qs =>
                      rw [runCost_cons]
                      change S + P + (runCostAfter S P x (z :: zs) +
                        (P + min S (P * (q - lastD 0 (z :: zs) - 1)) + runCostAfter S P q qs)) ≤
                        S + P * (b - a) + (S + P + runCostAfter S P q qs)
                      have hminle : min S (P * (q - lastD 0 (z :: zs) - 1)) ≤ S := Nat.min_le_left _ _
                      have hba : b - x ≤ b - a := by omega
                      have hmul := Nat.mul_le_mul_left P hba
                      omega
        · have hbx : b ≤ x := by omega
          have hall : ∀ p ∈ x :: xs, b ≤ p := by
            intro p hpin
            simp only [List.mem_cons] at hpin
            rcases hpin with rfl | hpin
            · exact hbx
            · have hxp : x < p := hxxs p hpin
              omega
          have hfilter : (x :: xs).filter (fun p => p < a || b ≤ p) = x :: xs := by
            apply List.filter_eq_self.mpr
            intro p hpin
            have hbp := hall p hpin
            simp [hbp]
          rw [hfilter]
          rw [runCost_cons]
          omega

private theorem nodupEraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodupEraseDups (as.filter (fun b => ! b == a))
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem pagesSorted (cap : Nat) (ids : List Nat) :
    ((pages cap ids).mergeSort).Pairwise (fun a b : Nat => a < b) := by
  let ps := (pages cap ids).mergeSort
  have hle' : ps.Pairwise (fun a b : Nat => (decide (a ≤ b) : Bool) = true) := by
    dsimp [ps]
    apply List.pairwise_mergeSort
    · intro x y z hxy hyz
      simp at hxy hyz ⊢
      omega
    · intro x y
      simp
      omega
  have hle : ps.Pairwise (fun a b : Nat => a ≤ b) := by
    simpa using hle'
  have hnodup0 : (pages cap ids).Nodup := by
    dsimp [pages]
    exact nodupEraseDups _
  have hperm : ps.Perm (pages cap ids) := by
    dsimp [ps]
    exact List.mergeSort_perm _ _
  have hnodup : ps.Nodup := hperm.symm.nodup hnodup0
  exact (hle.and hnodup).imp (by intro a b h; omega)

theorem sound_Gather_OptimalRead : OptimalRead.Goal := by
  intro ids cap setup price ext hcap hpos hcover
  let ps := (pages cap ids).mergeSort
  have hsorted : ps.Pairwise (fun a b : Nat => a < b) := by
    dsimp [ps]
    exact pagesSorted cap ids
  have hperm : ps.Perm (pages cap ids) := by
    dsimp [ps]
    exact List.mergeSort_perm _ _
  have aux : ∀ (ext : List (Nat × Nat)) (qs : List Nat),
      qs.Pairwise (fun a b : Nat => a < b) →
      (∀ e ∈ ext, e.1 < e.2) →
      (∀ p ∈ qs, covers ext p = true) →
      runCost qs setup price ≤ readCost setup price ext := by
    intro ext
    induction ext with
    | nil =>
        intro qs hpair hpos' hcover'
        have hnil : qs = [] := by
          cases qs with
          | nil => rfl
          | cons p tl =>
              have hc := hcover' p (by simp)
              simp [covers] at hc
        rw [hnil]
        simp [runCost, readCost]
    | cons e ext ih =>
        intro qs hpair hpos' hcover'
        rcases e with ⟨a, b⟩
        have hab : a < b := hpos' (a, b) (by simp)
        have hpos'' : ∀ e ∈ ext, e.1 < e.2 := by
          intro e he
          exact hpos' e (by simp [he])
        have hcover'' : ∀ p ∈ qs.filter (fun p => p < a || b ≤ p), covers ext p = true := by
          intro p hp
          have hpin : p ∈ qs := (List.mem_filter.mp hp).1
          have hcond := (List.mem_filter.mp hp).2
          have hc := hcover' p hpin
          simp [covers] at hc
          rcases hc with hc | hc
          · simp at hcond
            omega
          · simpa [covers] using hc
        have hout_pair : (qs.filter (fun p => p < a || b ≤ p)).Pairwise (fun a b : Nat => a < b) :=
          hpair.filter _
        have hih := ih (qs.filter (fun p => p < a || b ≤ p)) hout_pair hpos'' hcover''
        have hs := splitIneq setup price a b hab qs hpair
        simp [readCost] at hih ⊢
        omega
  exact aux ext ps hsorted hpos (by
    intro p hp
    exact hcover p (hperm.mem_iff.mp hp))
end TinylambOpt.ResearchProofs.Atlas.Goal_Gather_OptimalRead
