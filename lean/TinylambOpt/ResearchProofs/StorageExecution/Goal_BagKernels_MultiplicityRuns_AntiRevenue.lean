import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

private theorem sum_eraseDups_filter_aux (x : Nat) (h : Nat → Nat) :
    ∀ n, ∀ l : List Nat, l.length = n → (x ∈ l ∨ h x = 0) →
      (l.eraseDups.map h).sum = ((l.filter (fun b => !b == x)).eraseDups.map h).sum + h x := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hlen hc
      cases l with
      | nil =>
          simp at hc ⊢
          exact hc.symm
      | cons y ys =>
          by_cases hyx : y == x
          · have hyx' : y = x := by simpa using hyx
            subst y
            simp [List.eraseDups_cons, Nat.add_comm]
          · have hlen_l' : (ys.filter (fun b => !b == y)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !b == y) ys
              simp at hlen
              omega
            have hcond : x ∈ ys.filter (fun b => !b == y) ∨ h x = 0 := by
              rcases hc with hcin | hx0
              · left
                have hxne : x ≠ y := by
                  intro heq
                  subst heq
                  simp at hyx
                simp only [List.mem_cons] at hcin
                rcases hcin with hxy | hxys
                · subst hxy
                  contradiction
                · simp [hxne, hxys]
              · right
                exact hx0
            have ih' := ih (ys.filter (fun b => !b == y)).length hlen_l'
              (ys.filter (fun b => !b == y)) rfl hcond
            calc
              ((y :: ys).eraseDups.map h).sum =
                  h y + ((ys.filter (fun b => !b == y)).eraseDups.map h).sum := by
                simp [List.eraseDups_cons]
              _ = h y + ((((ys.filter (fun b => !b == y)).filter (fun b => !b == x)).eraseDups.map h).sum + h x) := by
                rw [ih']
              _ = (((y :: ys).filter (fun b => !b == x)).eraseDups.map h).sum + h x := by
                simp [hyx, List.eraseDups_cons, List.filter_filter, Bool.and_comm, Nat.add_assoc]

private theorem sum_eraseDups_filter (x : Nat) (h : Nat → Nat) (l : List Nat)
    (hc : x ∈ l ∨ h x = 0) :
    (l.eraseDups.map h).sum = ((l.filter (fun b => !b == x)).eraseDups.map h).sum + h x :=
  sum_eraseDups_filter_aux x h l.length l rfl hc

private theorem sum_count_cons_ne (x : Nat) (f : Nat → Nat) (xs l : List Nat)
    (hl : ∀ k ∈ l, (k == x) = false) :
    (l.map (fun k => (x :: xs).count k * f k)).sum =
    (l.map (fun k => xs.count k * f k)).sum := by
  induction l with
  | nil => simp
  | cons k ks ih =>
      have hkne : k ≠ x := by simpa using hl k (by simp)
      have hks : ∀ a ∈ ks, (a == x) = false := by
        intro a ha
        exact hl a (by simp [ha])
      have ih' := ih hks
      simp only [List.map_cons, List.sum_cons]
      rw [ih']
      simp [hkne.symm]

private theorem sum_eq_sum_count (f : Nat → Nat) : ∀ l : List Nat,
    (l.map f).sum = (l.eraseDups.map (fun k => l.count k * f k)).sum := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hcond : x ∈ xs ∨ (fun k => xs.count k * f k) x = 0 := by
        by_cases hx : x ∈ xs
        · exact Or.inl hx
        · right
          simp [List.count_eq_zero_of_not_mem hx]
      have hB := sum_eraseDups_filter x (fun k => xs.count k * f k) xs hcond
      have hne : ((xs.filter (fun b => !b == x)).eraseDups.map
          (fun k => (x :: xs).count k * f k)).sum =
          ((xs.filter (fun b => !b == x)).eraseDups.map
          (fun k => xs.count k * f k)).sum := by
        apply sum_count_cons_ne
        intro k hk
        have hk' : k ∈ xs.filter (fun b => !b == x) := by simpa using hk
        have hkne : k ≠ x := by
          simp at hk'
          exact hk'.2
        simpa using hkne
      calc
        ((x :: xs).map f).sum
            = f x + (xs.map f).sum := by simp
        _ = f x + (xs.eraseDups.map (fun k => xs.count k * f k)).sum := by rw [ih]
        _ = f x + (((xs.filter (fun b => !b == x)).eraseDups.map
              (fun k => xs.count k * f k)).sum + xs.count x * f x) := by rw [hB]
        _ = (xs.count x + 1) * f x + ((xs.filter (fun b => !b == x)).eraseDups.map
              (fun k => xs.count k * f k)).sum := by
            simp [Nat.add_mul, Nat.add_assoc, Nat.add_comm]
        _ = ((x :: xs).eraseDups.map (fun k => (x :: xs).count k * f k)).sum := by
            simp only [List.eraseDups_cons, List.map_cons, List.sum_cons]
            rw [hne]
            simp

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (bag.expand rs).count k = bag.mass rs k := by
  unfold bag
  simp only
  induction rs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x, n⟩
      by_cases hxk : x == k
      · have hxk' : x = k := by simpa using hxk
        subst x
        simp [ih]
      · simp [hxk, ih, List.count_replicate]

private theorem mem_expand_mem_keys (rs : List (Nat × Nat)) {k : Nat}
    (hk : k ∈ bag.expand rs) : k ∈ bag.keys rs := by
  unfold bag at hk ⊢
  simp only [List.mem_flatMap, List.mem_map] at hk ⊢
  rcases hk with ⟨r, hrs, hkmem⟩
  rcases r with ⟨x, n⟩
  simp at hkmem
  exact ⟨(x, n), hrs, hkmem.2.symm⟩

private theorem mass_eq_zero_of_not_mem_keys (rs : List (Nat × Nat)) (k : Nat)
    (hk : k ∉ bag.keys rs) : bag.mass rs k = 0 := by
  rw [← count_expand_eq_mass rs k]
  exact List.count_eq_zero_of_not_mem (fun hmem => hk (mem_expand_mem_keys rs hmem))

private theorem sum_filter_eq_sum_map_ite (P : Nat → Bool) (l : List Nat) :
    (l.filter P).sum = (l.map (fun k => if P k then k else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : P x = true
      · simp [hx, ih]
      · have hx' : P x = false := by cases hpx : P x <;> simp_all
        simp [hx', ih]

private theorem sum_replicate (n x : Nat) : (List.replicate n x).sum = n * x := by
  induction n with
  | zero => simp
  | succ n ih => simp [Nat.succ_mul]

private theorem mass_cons_eq (x n : Nat) (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass ((x, n) :: rs) k = bag.mass rs k + (if x == k then n else 0) := by
  by_cases hxk : x == k
  · have hxk' : x = k := by simpa using hxk
    subst x
    simp [bag, Nat.add_comm]
  · simp [bag, hxk]

private theorem sum_mass_cons_ne (g : Nat → Nat) (x n : Nat) (rs : List (Nat × Nat))
    (l : List Nat) (hl : ∀ k ∈ l, (k == x) = false) :
    (l.map (fun k => bag.mass ((x, n) :: rs) k * g k)).sum =
    (l.map (fun k => bag.mass rs k * g k)).sum := by
  induction l with
  | nil => simp
  | cons k ks ih =>
      have hkne : k ≠ x := by simpa using hl k (by simp)
      have hks : ∀ a ∈ ks, (a == x) = false := by
        intro a ha
        exact hl a (by simp [ha])
      have ih' := ih hks
      simp only [List.map_cons, List.sum_cons]
      rw [ih']
      simp [mass_cons_eq, hkne.symm]

private theorem sum_expand_eq_weighted (g : Nat → Nat) : ∀ rs : List (Nat × Nat),
    ((bag.expand rs).map g).sum = weighted bag rs g := by
  intro rs
  induction rs with
  | nil => simp [bag, weighted]
  | cons r rs ih =>
      rcases r with ⟨x, n⟩
      have hcond : x ∈ bag.keys rs ∨ (fun k => bag.mass rs k * g k) x = 0 := by
        by_cases hx : x ∈ bag.keys rs
        · exact Or.inl hx
        · right
          simp [mass_eq_zero_of_not_mem_keys rs x hx]
      have hB := sum_eraseDups_filter x (fun k => bag.mass rs k * g k) (bag.keys rs) hcond
      have hkeys : bag.keys ((x, n) :: rs) = x :: bag.keys rs := rfl
      have hmhead : bag.mass ((x, n) :: rs) x = bag.mass rs x + n := by
        simpa using mass_cons_eq x n rs x
      have hne : (((bag.keys rs).filter (fun b => !b == x)).eraseDups.map
          (fun k => bag.mass ((x, n) :: rs) k * g k)).sum =
          (((bag.keys rs).filter (fun b => !b == x)).eraseDups.map
          (fun k => bag.mass rs k * g k)).sum := by
        apply sum_mass_cons_ne
        intro k hk
        have hk' : k ∈ (bag.keys rs).filter (fun b => !b == x) := by simpa using hk
        have hkne : k ≠ x := by
          simp at hk'
          exact hk'.2
        simpa using hkne
      have hexp : bag.expand ((x, n) :: rs) = List.replicate n x ++ bag.expand rs := rfl
      calc
        ((bag.expand ((x, n) :: rs)).map g).sum
            = (List.replicate n (g x)).sum + ((bag.expand rs).map g).sum := by
              rw [hexp]
              simp [List.map_append, List.sum_append, List.map_replicate]
        _ = n * g x + weighted bag rs g := by rw [sum_replicate, ih]
        _ = n * g x + ((bag.keys rs).eraseDups.map
              (fun k => bag.mass rs k * g k)).sum := by rfl
        _ = n * g x + (((bag.keys rs).filter (fun b => !b == x)).eraseDups.map
              (fun k => bag.mass rs k * g k)).sum + bag.mass rs x * g x := by
              rw [hB, Nat.add_assoc]
        _ = (bag.mass rs x + n) * g x + (((bag.keys rs).filter (fun b => !b == x)).eraseDups.map
              (fun k => bag.mass rs k * g k)).sum := by
              simp [Nat.add_mul]
              ac_rfl
        _ = ((bag.keys ((x, n) :: rs)).eraseDups.map
              (fun k => bag.mass ((x, n) :: rs) k * g k)).sum := by
              rw [hkeys, List.eraseDups_cons]
              simp only [List.map_cons, List.sum_cons]
              rw [hmhead, hne]
        _ = weighted bag ((x, n) :: rs) g := by rfl

example : weighted bag [(3, 2), (7, 3)] (fun x => if bag.mass [(7, 1)] x == 0 then x else 0) =
    ((bag.expand [(3, 2), (7, 3)]).filter (fun x => !(bag.expand [(7, 1)]).contains x)).sum := by
  decide

example : weighted bag [(3, 2), (3, 4), (7, 0)] (fun x =>
    if bag.mass [(3, 0), (5, 2)] x == 0 then x else 0) =
    ((bag.expand [(3, 2), (3, 4), (7, 0)]).filter
      (fun x => !(bag.expand [(3, 0), (5, 2)]).contains x)).sum := by
  decide

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  let P : Nat → Bool := fun x => !(bag.expand c).contains x
  let g : Nat → Nat := fun k => if P k then k else 0
  have hmassc : ∀ k, bag.mass c k = (bag.expand c).count k := by
    intro k
    exact (count_expand_eq_mass c k).symm
  have hcond : ∀ k, (bag.mass c k == 0) = (!(bag.expand c).contains k) := by
    intro k
    by_cases hmem : k ∈ bag.expand c
    · have hcpos : 0 < bag.mass c k := by
        rw [hmassc]
        exact List.count_pos_iff.mpr hmem
      have hz : (bag.mass c k == 0) = false := by simp [Nat.ne_of_gt hcpos]
      have hcont : (bag.expand c).contains k = true := by simpa using hmem
      simpa [hz, hcont] using hmem
    · have hczero : bag.mass c k = 0 := by
        rw [hmassc]
        exact List.count_eq_zero_of_not_mem hmem
      have hz : (bag.mass c k == 0) = true := by simp [hczero]
      have hcont : (bag.expand c).contains k = false := by simpa using hmem
      simpa [hz, hcont] using hmem
  have hg : (fun x => if bag.mass c x == 0 then x else 0) = g := by
    funext k
    dsimp [g, P]
    rw [hcond k]
  calc
    weighted bag a (fun x => if bag.mass c x == 0 then x else 0)
        = weighted bag a g := by rw [hg]
    _ = ((bag.expand a).map g).sum := (sum_expand_eq_weighted g a).symm
    _ = ((bag.expand a).filter P).sum := by
      simpa [g] using (sum_filter_eq_sum_map_ite P (bag.expand a)).symm
    _ = ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).sum := by rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_AntiRevenue
