import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Proof that a sorted line sequence has exactly one transition per distinct line. -/
namespace Nov26.Proof_SelectionNetworks_CoalescedGatherFootprint
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint

private def countFrom (prev : Option Nat) (l : List Nat) : Nat :=
  (l.zip (prev :: l.map some)).countP (fun p => some p.1 != p.2)

private theorem countFrom_cons (prev : Option Nat) (a : Nat) (rest : List Nat) :
    countFrom prev (a :: rest) = (if some a != prev then 1 else 0) + countFrom (some a) rest := by
  simp [countFrom, List.countP_cons]
  omega

private theorem countFrom_eq_none_of_lt (prev : Nat) : ∀ l : List Nat,
    (∀ b ∈ l, prev < b) → countFrom (some prev) l = countFrom none l := by
  intro l
  induction l with
  | nil => intro h; simp [countFrom]
  | cons b bs ih =>
      intro hb
      have hprev_b : prev < b := hb b (by simp)
      rw [countFrom_cons, countFrom_cons]
      have hne : some b != some prev := by simp [show b ≠ prev by omega]
      have hnone : some b != none := by simp
      simp [hne, hnone]

private theorem filter_ne_eq_dropWhile (a : Nat) (rest : List Nat)
    (hpair : rest.Pairwise (fun x y => x ≤ y)) (hle : ∀ b ∈ rest, a ≤ b) :
    rest.filter (fun b => !b == a) = rest.dropWhile (fun b => b == a) := by
  induction rest with
  | nil => simp
  | cons b bs ih =>
      rcases List.pairwise_cons.mp hpair with ⟨hle_bs, hpair_bs⟩
      have hle_b : a ≤ b := hle b (by simp)
      by_cases hba : b == a
      · have hbaeq : b = a := by simpa using hba
        subst b
        simp [hba, ih hpair_bs (by intro c hc; have := hle_bs c hc; omega)]
      · have hbne : b ≠ a := by intro heq; apply hba; simp [heq]
        have hlt : a < b := by omega
        have hbs_ne : ∀ c ∈ bs, (!c == a) = true := by
          intro c hc
          have hbc : b ≤ c := hle_bs c hc
          have hca : c ≠ a := by omega
          simp [hca]
        simp [hba, List.filter_eq_self.mpr hbs_ne]

private theorem gt_of_mem_dropWhile_eq (a : Nat) : ∀ rest : List Nat,
    rest.Pairwise (fun x y => x ≤ y) → (∀ b ∈ rest, a ≤ b) →
    ∀ z ∈ rest.dropWhile (fun b => b == a), a < z := by
  intro rest
  induction rest with
  | nil => intro h1 h2 z hz; simp at hz
  | cons x xs ih =>
      intro hpair hle z hz
      rcases List.pairwise_cons.mp hpair with ⟨hle_xs, hpair_xs⟩
      have hle_all_xs : ∀ d ∈ xs, a ≤ d := fun d hd => hle d (by simp [hd])
      by_cases hxa : x == a
      · have hz' : z ∈ xs.dropWhile (fun y => y == a) := by
          simpa [List.dropWhile_cons, hxa] using hz
        exact ih hpair_xs hle_all_xs z hz'
      · have hz' : z ∈ x :: xs := by
          simpa [List.dropWhile_cons, hxa] using hz
        rcases List.mem_cons.mp hz' with hzx | hzxs
        · have hx_eq : x = z := hzx.symm
          subst z
          have hle_x : a ≤ x := hle x (by simp)
          have hxne : x ≠ a := by intro heq; apply hxa; simp [heq]
          omega
        · have hle_z : a ≤ z := hle z (by simp [hzxs])
          have hxz : x ≤ z := hle_xs z hzxs
          have hle_x : a ≤ x := hle x (by simp)
          have hxne : x ≠ a := by intro heq; apply hxa; simp [heq]
          have hlt_x : a < x := by omega
          omega

private theorem countFrom_eq_dropWhile_eq (a : Nat) : ∀ rest : List Nat,
    (∀ b ∈ rest, a ≤ b) → countFrom (some a) rest = countFrom (some a) (rest.dropWhile (fun b => b == a)) := by
  intro rest
  induction rest with
  | nil => intro h; simp [countFrom]
  | cons x xs ih =>
      intro hle
      by_cases hxa : x == a
      · have hd : (x :: xs).dropWhile (fun b => b == a) = xs.dropWhile (fun b => b == a) := by
          simp [List.dropWhile_cons, hxa]
        have hxeq : x = a := by simpa using hxa
        subst x
        rw [hd, countFrom_cons]
        have hfirst : (some a != some a) = false := by simp
        rw [hfirst]
        simp
        exact ih (fun d hd' => hle d (by simp [hd']))
      · have hd : (x :: xs).dropWhile (fun b => b == a) = x :: xs := by
          simp [List.dropWhile_cons, hxa]
        rw [hd]

private theorem length_dropWhile_lt_of_mem (a : Nat) : ∀ rest : List Nat,
    rest.Pairwise (fun x y => x ≤ y) → (∀ b ∈ rest, a ≤ b) → a ∈ rest →
    (rest.dropWhile (fun b => b == a)).length < rest.length := by
  intro rest
  induction rest with
  | nil => intro h1 h2 hmem; simp at hmem
  | cons x xs ih =>
      intro hpair hle hmem
      rcases List.pairwise_cons.mp hpair with ⟨hle_xs, hpair_xs⟩
      rcases List.mem_cons.mp hmem with hax | haxs
      · have hd : (x :: xs).dropWhile (fun b => b == a) = xs.dropWhile (fun b => b == a) := by
          have hxa : x == a := by simpa [hax]
          simp [List.dropWhile_cons, hxa]
        rw [hd]
        simp only [List.length_cons]
        have hle_drop := (List.dropWhile_sublist (p := fun y => y == a) (l := xs)).length_le
        omega
      · by_cases hxa : x == a
        · have hd : (x :: xs).dropWhile (fun b => b == a) = xs.dropWhile (fun b => b == a) := by
            simp [List.dropWhile_cons, hxa]
          rw [hd]
          simp only [List.length_cons]
          have hih := ih hpair_xs (fun d hd' => hle d (by simp [hd'])) haxs
          omega
        · exfalso
          have hxa_le : x ≤ a := hle_xs a haxs
          have hax_le : a ≤ x := hle x (by simp)
          have hxeq : x = a := by omega
          apply hxa
          simp [hxeq]

private theorem countFrom_none_eq_eraseDups (l : List Nat)
    (hpair : l.Pairwise (fun a b => a ≤ b)) : countFrom none l = l.eraseDups.length := by
  induction hlen : l.length using Nat.strongRecOn generalizing l hpair with
  | ind n ih =>
      cases l with
      | nil => simp [countFrom]
      | cons a rest =>
          simp only [List.length_cons] at hlen
          rcases List.pairwise_cons.mp hpair with ⟨hle, hpair_rest⟩
          have hlen_rest : rest.length < n := by omega
          have ih_rest := ih rest.length hlen_rest rest hpair_rest rfl
          rw [countFrom_cons, List.eraseDups_cons]
          have hhead : some a != none := by simp
          simp [hhead]
          by_cases ha : a ∈ rest
          · have hle_all : ∀ b ∈ rest, a ≤ b := hle
            have hdrop_pair : (rest.dropWhile (fun x => x == a)).Pairwise (fun x y => x ≤ y) :=
              List.Pairwise.sublist (List.dropWhile_sublist (p := fun x => x == a) (l := rest)) hpair_rest
            have hdrop_gt : ∀ b ∈ rest.dropWhile (fun x => x == a), a < b := by
              simpa using (gt_of_mem_dropWhile_eq a rest hpair_rest hle_all)
            have hdrop_len_lt : (rest.dropWhile (fun x => x == a)).length < n := by
              have hlt := length_dropWhile_lt_of_mem a rest hpair_rest hle_all ha
              omega
            have ih_drop := ih (rest.dropWhile (fun x => x == a)).length hdrop_len_lt
              (rest.dropWhile (fun x => x == a)) hdrop_pair rfl
            rw [countFrom_eq_dropWhile_eq a rest hle_all]
            rw [countFrom_eq_none_of_lt a _ hdrop_gt]
            rw [ih_drop]
            rw [filter_ne_eq_dropWhile a rest hpair_rest hle_all]
            omega
          · have hlt_all : ∀ b ∈ rest, a < b := by
              intro b hb
              have hle_b := hle b hb
              have hbne : b ≠ a := by intro heq; subst b; exact ha hb
              omega
            have hfilter : rest.filter (fun b => !b == a) = rest := by
              apply List.filter_eq_self.mpr
              intro b hb
              have hbne : b ≠ a := by intro heq; subst b; exact ha hb
              simp [hbne]
            rw [countFrom_eq_none_of_lt a rest hlt_all]
            rw [ih_rest]
            rw [hfilter]
            omega

example : ∀ (lines : List Nat), lines.Pairwise (fun a b => a ≤ b) →
    countFrom none lines = lines.eraseDups.length := by
  intro lines h
  exact countFrom_none_eq_eraseDups lines h

theorem sound : Goal := by
  intro sel n hn
  let lines := (tile sel n).map (fun x => x / n)
  have hpair0 : (tile sel n).Pairwise (fun a b => decide (a / n ≤ b / n) = true) := by
    have ht : ∀ (a b c : Nat), decide (a / n ≤ b / n) = true → decide (b / n ≤ c / n) = true →
        decide (a / n ≤ c / n) = true := by
      intro a b c hab hbc
      simp only [decide_eq_true_eq] at hab hbc ⊢
      omega
    have htot : ∀ (a b : Nat), (decide (a / n ≤ b / n) || decide (b / n ≤ a / n)) = true := by
      intro a b
      by_cases h : a / n ≤ b / n
      · simp [h]
      · have : b / n ≤ a / n := by omega
        simp [h, this]
    simpa [tile] using (List.pairwise_mergeSort ht htot sel)
  have hpair1 : (tile sel n).Pairwise (fun a b => a / n ≤ b / n) := by
    exact hpair0.imp (by intro a b h; simpa using h)
  have hlines : lines.Pairwise (fun a b => a ≤ b) := by
    dsimp [lines]
    exact hpair1.map (fun x => x / n) (by intro a b h; exact h)
  show countFrom none lines = lines.eraseDups.length
  exact countFrom_none_eq_eraseDups lines hlines

end Nov26.Proof_SelectionNetworks_CoalescedGatherFootprint