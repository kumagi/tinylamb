import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `AdditiveProduct.ProductRevenue` — `∀ a b, weighted bag (a,b) id = a.sum*b.length +
  b.sum*a.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_ProductRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

/-! ## Generic list lemmas -/

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Nat → Nat) (keys : List Nat) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Nat, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            have hkaeq : k = a := beq_iff_eq.mp h
            have haeq : (a == k) = true := beq_iff_eq.mpr hkaeq.symm
            simp [haeq] at hane)
          simp [List.count_cons, hka, hrestc]
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem count_eraseDups_le_one {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.eraseDups.count a ≤ 1 := by
  let motive : Nat → Prop := fun n => ∀ (l : List α), l.length = n → ∀ a, l.eraseDups.count a ≤ 1
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen a
    cases l with
    | nil => simp
    | cons b bs =>
        rw [List.eraseDups_cons, List.count_cons]
        let rest := bs.filter (fun c => !(c == b))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun c : α => !(c == b)) bs)
            (Nat.lt_succ_self _)
        have ihrest : ∀ c, rest.eraseDups.count c ≤ 1 := by
          intro c
          exact ih rest.length hrestlen rest rfl c
        by_cases hba : b == a
        · have hzero : rest.eraseDups.count a = 0 := by
            rw [List.count_eq_zero]
            intro hm
            rw [List.mem_eraseDups] at hm
            have hf := (List.mem_filter.mp hm).2
            have hab : (a == b) = true := beq_iff_eq.mpr (beq_iff_eq.mp hba).symm
            simp [hab] at hf
          have hz' : (List.filter (fun c : α => !(c == b)) bs).eraseDups.count a = 0 := by
            simpa [rest] using hzero
          simp [hba, hz']
        · have hbfalse : (b == a) = false := Bool.eq_false_of_ne_true hba
          simp [hbfalse]
          exact ihrest a
  exact H l rfl a

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup :=
  List.nodup_iff_count.mpr (fun a => count_eraseDups_le_one l a)

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  have hle : l.count a ≤ 1 := List.nodup_iff_count.mp hn a
  have hpos : 0 < l.count a := List.count_pos_iff.mpr ha
  omega

private theorem sum_eq_of_nodup_mem (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  have hperm : l₁.Perm l₂ := List.perm_iff_count.mpr (by
    intro d
    by_cases hd : d ∈ l₁
    · have hd2 : d ∈ l₂ := (hmem d).mp hd
      rw [count_eq_one_of_mem_nodup hnd₁ hd, count_eq_one_of_mem_nodup hnd₂ hd2]
    · have hd2 : d ∉ l₂ := by intro h; exact hd ((hmem d).mpr h)
      rw [List.count_eq_zero.mpr hd, List.count_eq_zero.mpr hd2])
  exact List.Perm.sum_nat (hperm.map f)

/-! ## AdditiveProduct bag facts -/

private theorem count_flatMap {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (g : α → List α) (a : α) :
    (xs.flatMap g).count a = (xs.map fun x => (g x).count a).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.count_append, ih]

private theorem count_add_left (x : Nat) : ∀ (b : List Nat) (k : Nat),
    (b.map fun y => x + y).count k = if x ≤ k then b.count (k - x) else 0 := by
  intro b
  induction b with
  | nil => intro k; simp
  | cons y ys ih =>
      intro k
      by_cases hle : x ≤ k
      · by_cases hy : y = k - x
        · have heq : x + y = k := by omega
          have hbeq1 : (x + y == k) = true := beq_iff_eq.mpr heq
          have hbeq2 : (y == k - x) = true := beq_iff_eq.mpr hy
          simp [List.count_cons, hbeq1, hbeq2, ih, hle]
        · have hybeq : (y == k - x) = false := by simpa [beq_iff_eq] using hy
          have hxyne : x + y ≠ k := by
            intro he
            apply hy
            omega
          have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using hxyne
          simp [List.count_cons, hbeq1, hybeq, ih, hle]
      · have heqfalse : x + y ≠ k := by omega
        have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using heqfalse
        simp [List.count_cons, hbeq1, ih, hle]

private theorem mass_eq_sum (a b : List Nat) (k : Nat) :
    bag.mass (a, b) k = (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum := by
  change (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0).sum =
    (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum
  rw [show (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0) =
       (a.eraseDups.map fun x => a.count x * (if x ≤ k then b.count (k - x) else 0)) by
       apply List.map_congr_left
       intro x hx
       by_cases hle : x ≤ k <;> simp [hle]]
  rw [dedup_weight_sum]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (bag.expand (a, b)).count k = bag.mass (a, b) k := by
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl]
  rw [count_flatMap]
  rw [mass_eq_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [count_add_left]

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ bag.expand (a, b) → k ∈ bag.keys (a, b) := by
  intro h
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hrest⟩
  rw [List.mem_map] at hrest
  rcases hrest with ⟨y, hy, hxy⟩
  rw [show bag.keys (a, b) = a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y)) by rfl]
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hx
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hy
    · exact hxy

private theorem mem_expand_of_mem_keys (a b : List Nat) (k : Nat) :
    k ∈ bag.keys (a, b) → k ∈ bag.expand (a, b) := by
  intro h
  rw [show bag.keys (a, b) = a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hrest⟩
  rw [List.mem_map] at hrest
  rcases hrest with ⟨y, hy, hxy⟩
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl]
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · exact List.mem_eraseDups.mp hx
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · exact List.mem_eraseDups.mp hy
    · exact hxy

/-! ## Sum of the expanded Cartesian product -/

private theorem sum_shift (x : Nat) (b : List Nat) :
    (b.map fun y => x + y).sum = b.length * x + b.sum := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      simp only [Nat.succ_mul]
      omega

private theorem expand_sum (a b : List Nat) :
    (bag.expand (a, b)).sum = a.sum * b.length + b.sum * a.length := by
  induction a with
  | nil => simp [bag]
  | cons x xs ih =>
      change (((b.map fun y => x + y) ++ bag.expand (xs, b)).sum) =
        (x :: xs).sum * b.length + b.sum * (x :: xs).length
      rw [List.sum_append, ih, sum_shift]
      simp only [List.sum_cons, List.length_cons]
      rw [Nat.add_mul]
      rw [Nat.mul_add]
      rw [Nat.mul_one]
      rw [Nat.mul_comm b.length x]
      omega

/-! ## Goal -/

theorem sound_ProductRevenue : ProductRevenue.Goal := by
  intro a b
  let f : Nat → Nat := id
  let m : Nat → Nat := fun k => bag.mass (a, b) k
  change ((bag.keys (a, b)).eraseDups.map fun k => m k * f k).sum =
    a.sum * b.length + b.sum * a.length

  have hswap : ((bag.keys (a, b)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (a, b)).eraseDups.map fun k => m k * f k).sum := by
    apply sum_eq_of_nodup_mem
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d
      rw [List.mem_eraseDups, List.mem_eraseDups]
      constructor
      · exact mem_expand_of_mem_keys a b d
      · exact mem_keys_of_mem_expand a b d

  have hmass : ((bag.expand (a, b)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (a, b)).map f).sum := by
    rw [← dedup_weight_sum f (bag.expand (a, b))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [m]
    rw [count_expand_eq_mass]

  calc
    ((bag.keys (a, b)).eraseDups.map fun k => m k * f k).sum
        = ((bag.expand (a, b)).eraseDups.map fun k => m k * f k).sum := hswap
    _ = ((bag.expand (a, b)).map f).sum := hmass
    _ = a.sum * b.length + b.sum * a.length := by
        simpa [f] using expand_sum a b

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_ProductRevenue
