import TinylambOpt.ResearchGoals.ExecutionStrategies.SearchBounds

namespace TinylambOpt.ResearchProofs.Wave7SearchBounds

open TinylambOpt.ResearchGoals.SearchBounds

private def Desc (xs : List Nat) : Prop := xs.Pairwise (fun a b => b ≤ a)

private theorem sum_map_zero (xs : List Nat) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

private theorem perm_sum {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [List.sum_cons, ih]
  | swap a b l => simp [List.sum_cons, Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem nodup_eraseDups : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Nat), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length hlen _ rfl)
            have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
            rw [Nat.not_beq_eq_true_eq] at hb'
            exact fun heq => hb' heq.symm
  exact key l.length l rfl

private theorem count_succ (k x : Nat) (xs : List Nat) :
    (x :: xs).count k = (if x == k then xs.count k + 1 else xs.count k) := by
  by_cases h : x == k
  · have h' : x = k := beq_iff_eq.mp h
    subst k
    simp
  · have hne : x ≠ k := by intro heq; apply h; subst k; simp
    simp [h, List.count_cons_of_ne hne]

private theorem update_sum (ks : List Nat) (hnd : ks.Nodup) (x : Nat) (hx : x ∈ ks)
    (c f : Nat → Nat) :
    (ks.map fun k => (if x = k then c k + 1 else c k) * f k).sum =
      (ks.map fun k => c k * f k).sum + f x := by
  induction ks with
  | nil => simp at hx
  | cons k ks ih =>
      have hnd' := List.pairwise_cons.mp hnd
      simp only [List.map_cons, List.sum_cons]
      rcases List.mem_cons.mp hx with hkx | hxks
      · subst k
        have hnot : ∀ y ∈ ks, x ≠ y := by
          intro y hy heq
          exact hnd'.1 y hy heq
        have hmap : (ks.map fun y => (if x = y then c y + 1 else c y) * f y) =
            (ks.map fun y => c y * f y) := by
          apply List.map_congr_left
          intro y hy
          simp [hnot y hy]
        rw [hmap]
        simp [Nat.add_mul]
        omega
      · have hkx : k ≠ x := by
          intro heq
          exact hnd'.1 x hxks heq
        have hxk : x ≠ k := by intro heq; exact hkx heq.symm
        have hndtail := hnd'.2
        have hrec := ih hndtail hxks
        simp [hxk]
        rw [hrec]
        omega

private theorem sum_count_mul_of_nodup (xs : List Nat) (f : Nat → Nat) :
    ∀ (ks : List Nat), ks.Nodup → (∀ x ∈ xs, x ∈ ks) →
      (xs.map f).sum = (ks.map fun k => xs.count k * f k).sum := by
  induction xs with
  | nil =>
      intro ks hnd hsub
      induction ks with
      | nil => simp
      | cons k ks ih =>
          simp only [List.map_nil, List.sum_nil, List.map_cons, List.sum_cons,
            List.count_nil, Nat.zero_mul, Nat.zero_add]
          exact (sum_map_zero ks).symm
  | cons x xs ih =>
      intro ks hnd hsub
      have hxks : x ∈ ks := hsub x (by simp)
      have htail : ∀ z ∈ xs, z ∈ ks := by intro z hz; exact hsub z (by simp [hz])
      have hih := ih ks hnd htail
      have hupd := update_sum ks hnd x hxks (fun k => xs.count k) f
      simp only [List.map_cons, List.sum_cons]
      rw [hih]
      simpa [count_succ, beq_iff_eq, Nat.add_comm] using hupd.symm

/-! ### Cauchy--Schwarz for lists of natural pairs (identity with sum of squares). -/

private def csX (ps : List (Nat × Nat)) : Int := (ps.map fun p => ((p.1^2 : Nat) : Int)).sum
private def csY (ps : List (Nat × Nat)) : Int := (ps.map fun p => ((p.2^2 : Nat) : Int)).sum
private def csU (ps : List (Nat × Nat)) : Int := (ps.map fun p => ((p.1*p.2 : Nat) : Int)).sum

private def csCross : List (Nat × Nat) → Int
  | [] => 0
  | (x,y)::ps => csCross ps + (ps.map fun p => ((x:Int)*(p.2:Int) - (p.1:Int)*(y:Int))^2).sum

private theorem sum_cast_map {α : Type} (xs : List α) (f : α → Nat) :
    (xs.map fun a => ((f a : Nat) : Int)).sum = ((xs.map f).sum : Int) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem csX_eq (ps) : csX ps = ((ps.map fun p => (p.1^2 : Nat)).sum : Int) :=
  sum_cast_map ps (fun p => p.1^2)
private theorem csY_eq (ps) : csY ps = ((ps.map fun p => (p.2^2 : Nat)).sum : Int) :=
  sum_cast_map ps (fun p => p.2^2)
private theorem csU_eq (ps) : csU ps = ((ps.map fun p => (p.1*p.2 : Nat)).sum : Int) :=
  sum_cast_map ps (fun p => p.1*p.2)

private theorem csX_cons (x y : Nat) (ps : List (Nat × Nat)) :
    csX ((x,y)::ps) = (x:Int)^2 + csX ps := by simp [csX]
private theorem csY_cons (x y : Nat) (ps : List (Nat × Nat)) :
    csY ((x,y)::ps) = (y:Int)^2 + csY ps := by simp [csY]
private theorem csU_cons (x y : Nat) (ps : List (Nat × Nat)) :
    csU ((x,y)::ps) = (x:Int)*(y:Int) + csU ps := by simp [csU]

private theorem sum_nonneg {α : Type} (xs : List α) (f : α → Int) (hf : ∀ x, 0 ≤ f x) :
    0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Int.add_nonneg (hf x) ih

private theorem cs_cross_nonneg (ps : List (Nat × Nat)) : 0 ≤ csCross ps := by
  induction ps with
  | nil => simp [csCross]
  | cons p ps ih =>
      rcases p with ⟨x,y⟩
      simp only [csCross]
      exact Int.add_nonneg ih
        (sum_nonneg ps (fun q => ((x:Int)*q.2 - q.1*(y:Int))^2) (fun q => Int.sq_nonneg _))

private theorem cs_cross_new (x y : Nat) (ps : List (Nat × Nat)) :
    (ps.map fun p => ((x:Int)*(p.2:Int) - (p.1:Int)*(y:Int))^2).sum =
      (x:Int)^2 * csY ps + (y:Int)^2 * csX ps - 2*(x:Int)*(y:Int)*csU ps := by
  induction ps with
  | nil => simp [csX, csY, csU]
  | cons p ps ih =>
      rcases p with ⟨x',y'⟩
      simp only [List.map_cons, List.sum_cons]
      rw [csY_cons, csX_cons, csU_cons]
      grind

private theorem cs_identity (ps : List (Nat × Nat)) :
    csU ps ^ 2 + csCross ps = csX ps * csY ps := by
  induction ps with
  | nil => simp [csU, csX, csY, csCross]
  | cons p ps ih =>
      rcases p with ⟨x,y⟩
      simp only [csCross]
      rw [csU_cons, csY_cons, csX_cons, cs_cross_new]
      grind

private theorem cs_nat (ps : List (Nat × Nat)) :
    ((ps.map fun p => p.1 * p.2).sum)^2 ≤
      (ps.map fun p => (p.1^2 : Nat)).sum * (ps.map fun p => (p.2^2 : Nat)).sum := by
  have hid := cs_identity ps
  have hc := cs_cross_nonneg ps
  have hineq : (csU ps)^2 ≤ csX ps * csY ps := by omega
  have hineq' : (((ps.map fun p => (p.1*p.2 : Nat)).sum : Int)^2 ≤
      ((ps.map fun p => (p.1^2 : Nat)).sum : Int) * ((ps.map fun p => (p.2^2 : Nat)).sum : Int)) := by
    simpa [csU_eq, csX_eq, csY_eq] using hineq
  exact_mod_cast hineq'

private theorem cs_fun {α : Type} (ks : List α) (f g : α → Nat) :
    ((ks.map fun k => f k * g k).sum)^2 ≤
      (ks.map fun k => (f k)^2).sum * (ks.map fun k => (g k)^2).sum := by
  have h := cs_nat (ks.map fun k => (f k, g k))
  simp only [List.map_map] at h
  have h1 : (ks.map ((fun p : Nat × Nat => p.1 * p.2) ∘ fun k => (f k, g k))) =
      (ks.map fun k => f k * g k) := by
    apply List.map_congr_left
    intro k hk
    rfl
  have h2 : (ks.map ((fun p : Nat × Nat => p.1 ^ 2) ∘ fun k => (f k, g k))) =
      (ks.map fun k => (f k)^2) := by
    apply List.map_congr_left
    intro k hk
    rfl
  have h3 : (ks.map ((fun p : Nat × Nat => p.2 ^ 2) ∘ fun k => (f k, g k))) =
      (ks.map fun k => (g k)^2) := by
    apply List.map_congr_left
    intro k hk
    rfl
  rw [h1, h2, h3] at h
  exact h

/-! ### 1. CollisionBound -/

theorem CollisionBound_sound : TinylambOpt.ResearchGoals.SearchBounds.CollisionBound.Goal := by
  intro a b
  let ks := (a ++ b).eraseDups
  have hnd : ks.Nodup := by simpa [ks] using nodup_eraseDups (a ++ b)
  have hsuba : ∀ x ∈ a, x ∈ ks := by
    intro x hx
    simp [ks, List.mem_eraseDups, hx]
  have hsubb : ∀ x ∈ b, x ∈ ks := by
    intro x hx
    simp [ks, List.mem_eraseDups, hx]
  have ha := sum_count_mul_of_nodup a (fun k => b.count k) ks hnd hsuba
  have hb := sum_count_mul_of_nodup a (fun k => a.count k) ks hnd hsuba
  have hb' := sum_count_mul_of_nodup b (fun k => b.count k) ks hnd hsubb
  have hnd_a : (a.eraseDups).Nodup := nodup_eraseDups a
  have hnd_b : (b.eraseDups).Nodup := nodup_eraseDups b
  have hma0 := sum_count_mul_of_nodup a (fun k => a.count k) a.eraseDups hnd_a
    (by intro x hx; simp [List.mem_eraseDups, hx])
  have hmb0 := sum_count_mul_of_nodup b (fun k => b.count k) b.eraseDups hnd_b
    (by intro x hx; simp [List.mem_eraseDups, hx])
  have hma : secondMoment a = (a.map fun k => a.count k).sum := by
    dsimp [secondMoment, counts]
    rw [List.map_map]
    have hmap : (a.eraseDups.map ((fun x : Nat => x^2) ∘ fun y => a.count y)) =
        (a.eraseDups.map fun k => a.count k * a.count k) := by
      apply List.map_congr_left
      intro k hk
      change (a.count k)^2 = a.count k * a.count k
      rw [Nat.pow_two]
    rw [hmap, ← hma0]
  have hmb : secondMoment b = (b.map fun k => b.count k).sum := by
    dsimp [secondMoment, counts]
    rw [List.map_map]
    have hmap : (b.eraseDups.map ((fun x : Nat => x^2) ∘ fun y => b.count y)) =
        (b.eraseDups.map fun k => b.count k * b.count k) := by
      apply List.map_congr_left
      intro k hk
      change (b.count k)^2 = b.count k * b.count k
      rw [Nat.pow_two]
    rw [hmap, ← hmb0]
  have hcs := cs_fun ks (fun k => a.count k) (fun k => b.count k)
  simp only [Nat.pow_two] at hcs
  change (fanout a b)^2 ≤ secondMoment a * secondMoment b
  rw [hma, hmb]
  change ((a.map fun k => b.count k).sum)^2 ≤ (a.map fun k => a.count k).sum * (b.map fun k => b.count k).sum
  rw [ha, hb, hb']
  simpa [Nat.pow_two] using hcs

/-! ### 4. CappedJoin -/

private theorem foldl_hist (ys : List Nat) : ∀ (xs : List Nat) (h : Std.HashMap Nat Nat),
    (∀ k, h.getD k 0 = xs.count k) →
    ∀ k, (ys.foldl (fun h x => h.insert x (h.getD x 0 + 1)) h).getD k 0 = (xs ++ ys).count k := by
  induction ys with
  | nil =>
      intro xs h hh k
      simpa using hh k
  | cons y ys ih =>
      intro xs h hh k
      have hins : ∀ j, (h.insert y (h.getD y 0 + 1)).getD j 0 = (xs ++ [y]).count j := by
        intro j
        rw [Std.HashMap.getD_insert]
        by_cases hj : y = j
        · subst j
          simp [hh, List.count_append]
        · have hbeq : (y == j) = false := beq_eq_false_iff_ne.mpr hj
          simp [hbeq, hh, List.count]
      rw [List.foldl_cons]
      have hrec := ih (xs ++ [y]) (h.insert y (h.getD y 0 + 1)) hins k
      simpa [List.count_append, List.count, Nat.add_assoc] using hrec

private theorem histogram_getD (b : List Nat) (x : Nat) : (histogram b).getD x 0 = b.count x := by
  have h := foldl_hist b [] {} (by intro k; simp) x
  simpa [histogram] using h

private theorem remaining_eq_sub (freq : Std.HashMap Nat Nat) (k : Nat) :
    ∀ xs, remaining freq k xs = k - (xs.map fun x => freq.getD x 0).sum := by
  intro xs
  induction xs generalizing k with
  | nil =>
      cases k <;> simp [remaining]
  | cons x xs ih =>
      cases k with
      | zero => simp [remaining]
      | succ k =>
          change remaining freq (Nat.succ k) (x :: xs) =
            Nat.succ k - (freq.getD x 0 + (xs.map fun y => freq.getD y 0).sum)
          rw [remaining]
          rw [ih (Nat.succ k - freq.getD x 0)]
          omega

theorem CappedJoin_sound : TinylambOpt.ResearchGoals.SearchBounds.CappedJoin.Goal := by
  intro a b k
  let freq := histogram b
  have hrem := remaining_eq_sub freq k a
  have hfan : (a.map fun x => freq.getD x 0).sum = fanout a b := by
    simp [fanout, freq, histogram_getD]
  rw [hrem, hfan]
  by_cases hk : fanout a b ≤ k
  · rw [Nat.min_eq_right hk]
    omega
  · rw [Nat.min_eq_left (by omega)]
    omega

/-! ### 5. GroupSumFence -/

private theorem sum_le_ub (xs : List Int) (upper : Int) (hx : ∀ x ∈ xs, x ≤ upper) :
    xs.sum ≤ max 0 upper * (xs.length : Int) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx' : x ≤ max 0 upper :=
        Int.le_trans (hx x (by simp)) (Int.le_max_right 0 upper)
      have htail : ∀ z ∈ xs, z ≤ upper := by intro z hz; exact hx z (by simp [hz])
      have hih := ih htail
      simp only [List.sum_cons, List.length_cons]
      have hcast : ((xs.length + 1 : Nat) : Int) = (xs.length : Int) + 1 := by omega
      rw [hcast, Int.mul_add, Int.mul_one]
      omega

private theorem block_le (xs : List Int) (upper : Int) (n : Nat)
    (hlen : xs.length ≤ n) (hx : ∀ x ∈ xs, x ≤ upper) :
    xs.sum ≤ max 0 upper * (n : Int) := by
  have h := sum_le_ub xs upper hx
  have hnn : (xs.length : Int) ≤ (n : Int) := by exact_mod_cast hlen
  have h0 : 0 ≤ max 0 upper := Int.le_max_left 0 upper
  exact Int.le_trans h (Int.mul_le_mul_of_nonneg_left hnn h0)

theorem GroupSumFence_sound : TinylambOpt.ResearchGoals.SearchBounds.GroupSumFence.Goal := by
  intro seen cutoff blocks hblocks hop
  have hle : (blocks.map fun b => b.1.sum).sum ≤
      (blocks.map fun b : List Int × Int × Nat => max 0 b.2.1 * (b.2.2 : Int)).sum := by
    induction blocks with
    | nil => simp
    | cons b bs ih =>
        simp only [List.map_cons, List.sum_cons]
        have hb := hblocks b (by simp)
        have htail : ∀ z ∈ bs, z.1.length ≤ z.2.2 ∧ ∀ x ∈ z.1, x ≤ z.2.1 := by
          intro z hz; exact hblocks z (by simp [hz])
        have hnonneg : 0 ≤ max 0 b.2.1 * (b.2.2 : Int) :=
          Int.mul_nonneg (Int.le_max_left 0 b.2.1) (Int.natCast_nonneg b.2.2)
        have hopTail : optimistic seen (bs.map Prod.snd) < cutoff := by
          dsimp [optimistic] at hop ⊢
          simp only [List.map_cons, List.sum_cons] at hop ⊢
          omega
        have hih := ih htail hopTail
        exact Int.add_le_add (block_le b.1 b.2.1 b.2.2 hb.1 hb.2) hih
  dsimp [optimistic] at hop ⊢
  have hle' : (blocks.map fun b => b.1.sum).sum ≤
      ((blocks.map Prod.snd).map fun (upper, n) => max 0 upper * (n:Int)).sum := by
    rw [List.map_map]
    have hmap : (blocks.map ((fun x : Int × Nat => max 0 x.1 * (x.2 : Int)) ∘ Prod.snd)) =
        (blocks.map fun b => max 0 b.2.1 * (b.2.2 : Int)) := by
      apply List.map_congr_left
      intro b hb
      rfl
    rw [hmap]
    exact hle
  have hop' : seen + ((blocks.map Prod.snd).map fun x : Int × Nat => max 0 x.1 * (x.2 : Int)).sum < cutoff := by
    simpa using hop
  exact Int.lt_of_le_of_lt (Int.add_le_add_left hle' seen) hop'

/-! ### 7. UnseenTopK -/

theorem UnseenTopK_sound : TinylambOpt.ResearchGoals.SearchBounds.UnseenTopK.Goal := by
  intro shards cutoff hbound hsum k hk
  by_cases hmem : k ∈ shards.flatMap (fun s => s.2.1)
  · exact hmem
  have hle : (shards.map fun s => s.1.count k).sum ≤ (shards.map fun s => s.2.2).sum := by
    clear hk
    induction shards with
    | nil => simp
    | cons s ss ih =>
        have hks : k ∉ s.2.1 := by
          intro hkIn
          apply hmem
          simp [hkIn]
        have hcss : k ∉ ss.flatMap (fun t => t.2.1) := by
          intro hkIn
          apply hmem
          simp [hkIn]
        have hb : ∀ t ∈ ss, ∀ j, j ∉ t.2.1 → t.1.count j ≤ t.2.2 := by
          intro t ht; exact hbound t (by simp [ht])
        have hsumTail : (ss.map fun t => t.2.2).sum < cutoff := by
          simp only [List.map_cons, List.sum_cons] at hsum
          omega
        have hih := ih hb hsumTail hcss
        have hc := hbound s (by simp) k hks
        simp only [List.map_cons, List.sum_cons]
        omega
  exfalso
  omega

/-! ### 8. PredicateExchange -/

theorem PredicateExchange_sound : TinylambOpt.ResearchGoals.SearchBounds.PredicateExchange.Goal := by
  intro rows ca cb
  dsimp
  let n := rows.length
  let a := rows.countP Prod.fst
  let b := rows.countP Prod.snd
  have hA : a ≤ n := by simpa [a, n] using List.countP_le_length (p := Prod.fst) (l := rows)
  have hB : b ≤ n := by simpa [b, n] using List.countP_le_length (p := Prod.snd) (l := rows)
  change (ca*n + cb*a ≤ cb*n + ca*b) ↔ ca*(n-b) ≤ cb*(n-a)
  rw [Nat.mul_sub_left_distrib, Nat.mul_sub_left_distrib]
  have hca : ca*b ≤ ca*n := Nat.mul_le_mul_left ca hB
  have hcb : cb*a ≤ cb*n := Nat.mul_le_mul_left cb hA
  omega

/-! ### Shared helpers -/

private theorem sum_le_sum_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α]
    (l L : List α) (f : α → Nat) (hl : l.Nodup) (hsub : ∀ x ∈ l, x ∈ L) :
    (l.map f).sum ≤ (L.map f).sum := by
  induction l generalizing L with
  | nil => simp
  | cons a as ih =>
      have haL : a ∈ L := hsub a (by simp)
      have hper := List.perm_cons_erase (l := L) haL
      have has : as.Nodup := (List.pairwise_cons.mp hl).2
      have hnota : ∀ x ∈ as, x ≠ a := by
        intro x hx he
        exact (List.pairwise_cons.mp hl).1 x hx he.symm
      have hsub' : ∀ x ∈ as, x ∈ L.erase a := by
        intro x hx
        rw [List.mem_erase_of_ne (hnota x hx)]
        exact hsub x (by simp [hx])
      have hih := ih (L.erase a) has hsub'
      have hsum : (L.map f).sum = ((a :: L.erase a).map f).sum := perm_sum (hper.map f)
      simp only [List.map_cons, List.sum_cons] at hsum
      rw [hsum]
      simp only [List.map_cons, List.sum_cons]
      omega


private def pairs (xs ys : List Nat) : List (Nat × Nat) := xs.flatMap fun x => ys.map fun y => (x, y)


private theorem map_const_mul (ys : List Nat) (c : Nat) (g : Nat → Nat) :
    (ys.map fun y => c * g y).sum = c * (ys.map g).sum := by
  induction ys with
  | nil => simp
  | cons y ys ih => simp [Nat.mul_add, ih]


private theorem product_sum (xs ys : List Nat) (f g : Nat → Nat) :
    ((pairs xs ys).map fun p => f p.1 * g p.2).sum = (xs.map f).sum * (ys.map g).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih =>
      simp only [pairs, List.flatMap_cons, List.map_append, List.sum_append]
      rw [show List.flatMap (fun x => List.map (fun y => (x, y)) ys) xs = pairs xs ys by rfl]
      rw [ih]
      have hfg : ((ys.map fun y => (x, y)).map fun p => f p.1 * g p.2).sum =
          f x * (ys.map g).sum := by
        simp only [List.map_map]
        have hmap : ((fun p : Nat × Nat => f p.1 * g p.2) ∘ fun y => (x, y)) = fun y => f x * g y := by
          funext y
          rfl
        rw [hmap]
        exact map_const_mul ys (f x) g
      rw [hfg]
      simp only [List.map_cons, List.sum_cons]
      rw [Nat.add_mul]


private theorem sum_ones_length {α : Type} (xs : List α) : (xs.map fun _ => 1).sum = xs.length := by
  induction xs with
  | nil => simp
  | cons a as ih => simp [ih]; omega


private theorem getD_take_eq {l : List Nat} {n s : Nat} (hs : s < n) :
    (l.take n).getD s 0 = l.getD s 0 := by
  rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_take]
  simp [hs]


private theorem map_range_getD (l : List Nat) :
    (List.range l.length).map (fun s => l.getD s 0) = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      change List.map (fun s => (a :: as).getD s 0) (List.range (as.length + 1)) = a :: as
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map]
      rw [show ((fun s => (a :: as).getD s 0) ∘ Nat.succ) = fun s => as.getD s 0 by
        funext s
        simp]
      rw [ih]
      simp


private theorem sum_getD_take (l : List Nat) :
    ((List.range l.length).map fun s => l.getD s 0).sum = l.sum := by
  rw [map_range_getD]


private theorem sum_range_le (f g : Nat → Nat) (n : Nat) (h : ∀ s, s < n → f s ≤ g s) :
    ((List.range n).map f).sum ≤ ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hih := ih (by intro s hs; exact h s (by omega))
      have hn := h n (by omega)
      simp [List.range_succ]
      omega


private theorem pairwise_le_nodup_strict {l : List Nat} (hle : l.Pairwise (fun a b => a ≤ b))
    (hnd : l.Nodup) : l.Pairwise (fun a b => a < b) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hle' := List.pairwise_cons.mp hle
      have hnd' := List.pairwise_cons.mp hnd
      apply List.Pairwise.cons
      · intro b hb
        have h : a ≤ b := hle'.1 b hb
        have hne : a ≠ b := hnd'.1 b hb
        omega
      · exact ih hle'.2 hnd'.2


private theorem sum_le_sum_nodup_zero (l L : List Nat) (f : Nat → Nat) (hl : l.Nodup)
    (hzero : ∀ x ∈ l, x ∉ L → f x = 0) : (l.map f).sum ≤ (L.map f).sum := by
  induction l generalizing L with
  | nil => simp
  | cons a as ih =>
      have has : as.Nodup := (List.pairwise_cons.mp hl).2
      by_cases haL : a ∈ L
      · have hper := List.perm_cons_erase (l := L) haL
        have hsum : (L.map f).sum = f a + ((L.erase a).map f).sum := by
          have hp := perm_sum (hper.map f)
          simpa using hp
        have hnota : a ∉ as := by
          intro h
          exact (List.pairwise_cons.mp hl).1 a h rfl
        have hzero' : ∀ x ∈ as, x ∉ L.erase a → f x = 0 := by
          intro x hx hxL
          apply hzero x (by simp [hx])
          intro hxLa
          apply hxL
          rw [List.mem_erase_of_ne]
          · exact hxLa
          · intro hxa
            exact hnota (by simpa [hxa] using hx)
        have hih := ih (L.erase a) has hzero'
        rw [hsum]
        simp only [List.map_cons, List.sum_cons]
        omega
      · have hfa : f a = 0 := hzero a (by simp) haL
        have hzero' : ∀ x ∈ as, x ∉ L → f x = 0 := by
          intro x hx hxL
          exact hzero x (by simp [hx]) hxL
        have hih := ih L has hzero'
        simp [hfa]
        omega


private theorem nodup_length_le_of_lt (is : List Nat) (hnd : is.Nodup) {m : Nat}
    (hlt : ∀ i ∈ is, i < m) : is.length ≤ m := by
  have hzero : ∀ x ∈ is, x ∉ List.range m → (fun _ : Nat => 1) x = 0 := by
    intro x hx hnot
    exfalso
    apply hnot
    simp [hlt x hx]
  have hle := sum_le_sum_nodup_zero is (List.range m) (fun _ => 1) hnd hzero
  have hl := sum_ones_length is
  have hr : ((List.range m).map fun _ => 1).sum = m := by simpa [sum_ones_length]
  rw [hl, hr] at hle
  exact hle


private theorem idx_ge_head_plus : ∀ (is : List Nat), is.Pairwise (fun a b => a < b) →
    ∀ s, s < is.length → is.headD 0 + s ≤ is.getD s 0 := by
  intro is
  induction is with
  | nil => intro h s hs; simp at hs
  | cons a as ih =>
      intro h s hs
      have htail := (List.pairwise_cons.mp h).2
      have hheadlt := (List.pairwise_cons.mp h).1
      cases s with
      | zero => simp
      | succ s =>
          have hslt : s < as.length := by simpa using hs
          have hih := ih htail s hslt
          have hheadAs : a + 1 ≤ as.headD 0 := by
            cases as with
            | nil => simp at hslt
            | cons b bs =>
                have hb : a < b := hheadlt b (by simp)
                simp only [List.headD_cons]
                omega
          simp only [List.getD_cons_succ, List.headD_cons]
          omega


private theorem idx_ge_pos (is : List Nat) (hinc : is.Pairwise (fun a b => a < b))
    (s : Nat) (hs : s < is.length) : s ≤ is.getD s 0 := by
  have h := idx_ge_head_plus is hinc s hs
  have h0 : 0 ≤ is.headD 0 := Nat.zero_le _
  omega


private theorem getD_mem {l : List Nat} {i : Nat} (h : i < l.length) : l.getD i 0 ∈ l := by
  have hm : l[i]'h ∈ l := List.getElem_mem h
  rw [List.getElem_eq_getD (l := l) (i := i) (h := h) (fallback := 0)] at hm
  exact hm


private theorem desc_get_le {l : List Nat} (hd : Desc l) {i j : Nat}
    (hij : i ≤ j) (hj : j < l.length) : l.getD j 0 ≤ l.getD i 0 := by
  by_cases h : i = j
  · subst j
    exact Nat.le_refl _
  · have hijlt : i < j := by omega
    have hi : i < l.length := by omega
    have hrel := List.Pairwise.rel_getElem_of_lt hi hj hd hijlt
    rw [← List.getElem_eq_getD (l := l) (i := j) (h := hj) (fallback := 0)]
    rw [← List.getElem_eq_getD (l := l) (i := i) (h := hi) (fallback := 0)]
    exact hrel


private theorem topk_sum (α : List Nat) (hα : Desc α) (is : List Nat)
    (hnd : is.Nodup) (hlt : ∀ i ∈ is, i < α.length) :
    (is.map fun i => α.getD i 0).sum ≤ (α.take is.length).sum := by
  let isAsc := is.mergeSort (fun a b : Nat => a ≤ b)
  have hp : isAsc.Perm is := List.mergeSort_perm is (fun a b : Nat => a ≤ b)
  have hsum : (isAsc.map fun i => α.getD i 0).sum = (is.map fun i => α.getD i 0).sum :=
    perm_sum (hp.map (fun i => α.getD i 0))
  rw [← hsum]
  have hndAsc : isAsc.Nodup := hp.nodup_iff.mpr hnd
  have hasc : isAsc.Pairwise (fun a b : Nat => a ≤ b) := by
    have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
      (trans := by intro a b c h1 h2; simp only [decide_eq_true_eq] at h1 h2 ⊢; exact Nat.le_trans h1 h2)
      (total := by intro a b; simp only [decide_eq_true_eq, Bool.or_eq_true]; omega) is
    simpa only [decide_eq_true_eq] using h
  have hinc : isAsc.Pairwise (fun a b => a < b) := pairwise_le_nodup_strict hasc hndAsc
  have hlenle : is.length ≤ α.length := nodup_length_le_of_lt is hnd hlt
  have hlenAsc : isAsc.length = is.length := hp.length_eq
  -- compare termwise with α.take
  have hterm : ∀ s, s < isAsc.length → α.getD (isAsc.getD s 0) 0 ≤ (α.take isAsc.length).getD s 0 := by
    intro s hs
    have hpos := idx_ge_pos isAsc hinc s hs
    have hltj : isAsc.getD s 0 < α.length := by
      apply hlt
      have hs' : s < isAsc.length := by omega
      exact hp.subset (getD_mem hs')
    have hle : α.getD (isAsc.getD s 0) 0 ≤ α.getD s 0 := desc_get_le hα hpos hltj
    have hsα : s < α.length := by omega
    have ht := getD_take_eq (s := s) (n := isAsc.length) (l := α) (by omega : s < isAsc.length)
    rw [ht]
    exact hle
  -- sum over isAsc equals sum over positions, then map to take
  have hmap : (isAsc.map fun i => α.getD i 0).sum =
      ((List.range isAsc.length).map fun s => α.getD (isAsc.getD s 0) 0).sum := by
    induction isAsc with
    | nil => simp
    | cons a as ih =>
        change (α.getD a 0 + (as.map fun i => α.getD i 0).sum) =
          ((List.range (as.length + 1)).map fun s => α.getD ((a :: as).getD s 0) 0).sum
        rw [List.range_succ_eq_map]
        simp only [List.map_cons, List.map_map, List.sum_cons]
        rw [show ((fun s => α.getD ((a :: as).getD s 0) 0) ∘ Nat.succ) =
            fun s => α.getD (as.getD s 0) 0 by
          funext s
          simp [List.getD_cons_succ]]
        rw [ih]
        simp
  rw [hmap]
  rw [← hlenAsc]
  have htakeLen : (α.take isAsc.length).length = isAsc.length := by
    rw [List.length_take]
    exact Nat.min_eq_left (by omega)
  rw [← sum_getD_take (α.take isAsc.length)]
  rw [htakeLen]
  apply sum_range_le
  intro s hs
  apply hterm
  simpa using hs


/-! ### TriangleBound -/

private def fTri (r s t : Edges) (x y : Nat) : Nat :=
  s.countP (fun p => y == p.1 && t.contains (p.2, x))


private theorem triangles_len (r s t : Edges) :
    (triangles r s t).length = (r.map fun p => fTri r s t p.1 p.2).sum := by
  simp [triangles, fTri, List.length_flatMap, List.countP_eq_length_filter]


private theorem countP_mono {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = true → q x = true) : l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.countP_cons]
      by_cases hp : p a = true
      · have hq : q a = true := h a (by simp) hp
        have hi := ih (by intro x hx; exact h x (by simp [hx]))
        simp [hp, hq]
        omega
      · have hi := ih (by intro x hx; exact h x (by simp [hx]))
        simp [hp]
        omega


private def ds (s : Edges) (y : Nat) : Nat := (s.map Prod.fst).count y


private def ct (t : Edges) (x : Nat) : Nat := (t.map Prod.snd).count x


private theorem ds_countP (s : Edges) (y : Nat) : ds s y = s.countP (fun p => p.1 == y) := by
  dsimp [ds]
  rw [List.count_eq_countP, List.countP_map]
  rfl


private theorem ct_countP (t : Edges) (x : Nat) : ct t x = t.countP (fun p => p.2 == x) := by
  dsimp [ct]
  rw [List.count_eq_countP, List.countP_map]
  rfl


private theorem fTri_le_ds (r s t : Edges) (x y : Nat) : fTri r s t x y ≤ ds s y := by
  rw [ds_countP]
  dsimp [fTri]
  apply countP_mono
  intro p hp hpp
  have hy : (y == p.1) = true := by
    by_cases h : (y == p.1) <;> simp_all
  have hy' : p.1 = y := (beq_iff_eq.mp hy).symm
  simp [hy']


private theorem nodup_map_snd_of_fst_eq (l : List (Nat × Nat)) (y : Nat)
    (hl : l.Nodup) (hfirst : ∀ p ∈ l, p.1 = y) : (l.map Prod.snd).Nodup := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      have hp1 : p.1 = y := hfirst p (by simp)
      have hps : ∀ q ∈ ps, q.1 = y := by intro q hq; exact hfirst q (by simp [hq])
      have hndps : ps.Nodup := (List.pairwise_cons.mp hl).2
      have hih := ih hndps hps
      apply List.Pairwise.cons
      · intro s hs
        rw [List.mem_map] at hs
        rcases hs with ⟨q, hq, hqs⟩
        intro heq
        have hq2 : q.2 = p.2 := by omega
        have hq1 : q.1 = y := hps q hq
        have hqp : q = p := by
          cases p with
          | mk p1 p2 =>
            cases q with
            | mk q1 q2 => simp_all
        exact (List.pairwise_cons.mp hl).1 q hq hqp.symm
      · exact hih


private theorem fTri_le_ct (r s t : Edges) (hs : s.Nodup) (x y : Nat) : fTri r s t x y ≤ ct t x := by
  let Q := s.filter (fun p => y == p.1 && t.contains (p.2, x))
  have hQnd : Q.Nodup := by
    exact (List.filter_sublist (p := fun p => y == p.1 && t.contains (p.2, x)) (l := s)).nodup hs
  have hfirst : ∀ p ∈ Q, p.1 = y := by
    intro p hp
    have h := (List.mem_filter.mp hp).2
    have hy : (y == p.1) = true := by by_cases h' : (y == p.1) <;> simp_all
    have hy' : p.1 = y := (beq_iff_eq.mp hy).symm
    exact hy' 
  have hmnd : (Q.map Prod.snd).Nodup := nodup_map_snd_of_fst_eq Q y hQnd hfirst
  have hsub : ∀ z ∈ Q.map Prod.snd, z ∈ (t.filter fun p => p.2 == x).map Prod.fst := by
    intro z hz
    rcases List.mem_map.mp hz with ⟨p, hpQ, rfl⟩
    have hc : t.contains (p.2, x) = true := by
      have h := (List.mem_filter.mp hpQ).2
      by_cases h' : t.contains (p.2, x) <;> simp_all
    have hm : (p.2, x) ∈ t := by simpa using hc
    have hpfilter : (p.2, x) ∈ t.filter (fun p => p.2 == x) := by
      rw [List.mem_filter]
      exact ⟨hm, by simp⟩
    exact List.mem_map.mpr ⟨(p.2, x), hpfilter, rfl⟩
  have hle := sum_le_sum_of_nodup_mem (Q.map Prod.snd)
    ((t.filter fun p => p.2 == x).map Prod.fst) (fun _ => 1) hmnd hsub
  have sum_ones {α : Type} (l : List α) : (l.map fun _ => 1).sum = l.length := by
    induction l with
    | nil => simp
    | cons a as ih => simp [ih]; omega
  have hle' : Q.length ≤ (t.filter fun p => p.2 == x).length := by
    have hlen1 : ((Q.map Prod.snd).map fun _ => 1).sum = Q.length := by
      induction Q with
      | nil => simp
      | cons p ps ih => simp at ih ⊢; omega
    have hlen2 : (((t.filter fun p => p.2 == x).map Prod.fst).map fun _ => 1).sum =
        (t.filter fun p => p.2 == x).length := by
      induction (t.filter fun p => p.2 == x) with
      | nil => simp
      | cons p ps ih => simp at ih ⊢; omega
    rw [hlen1, hlen2] at hle
    exact hle
  change s.countP (fun p => y == p.1 && t.contains (p.2, x)) ≤ ct t x
  rw [ct_countP, List.countP_eq_length_filter, List.countP_eq_length_filter]
  change Q.length ≤ (t.filter fun p => p.2 == x).length
  exact hle'


private theorem sum_map_le {α : Type} (l : List α) (f g : α → Nat) (h : ∀ x ∈ l, f x ≤ g x) :
    (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have hi := ih (by intro x hx; exact h x (by simp [hx]))
      exact Nat.add_le_add (h a (by simp)) hi


private theorem tri_f_sq_le (r s t : Edges) (hr : r.Nodup) (hs : s.Nodup) :
    (r.map fun p => fTri r s t p.1 p.2 ^ 2).sum ≤
      (r.map fun p => ds s p.2 * ct t p.1).sum := by
  apply sum_map_le
  intro p hp
  have h1 := fTri_le_ds r s t p.1 p.2
  have h2 := fTri_le_ct r s t hs p.1 p.2
  have hsq := Nat.mul_le_mul h1 h2
  simpa [Nat.pow_two] using hsq


private theorem sum_counts_eq_length (xs ks : List Nat) (hnd : ks.Nodup)
    (hsub : ∀ x ∈ xs, x ∈ ks) : (ks.map fun k => xs.count k).sum = xs.length := by
  have h := sum_count_mul_of_nodup xs (fun _ => 1) ks hnd hsub
  rw [sum_ones_length] at h
  simpa using h.symm


private theorem sum_ds_ct_le (r s t : Edges) (hr : r.Nodup) (ht : t.Nodup) :
    (r.map fun p => ds s p.2 * ct t p.1).sum ≤ s.length * t.length := by
  let Rs := (r.map Prod.fst).eraseDups
  let Ys := (r.map Prod.snd).eraseDups
  let L := pairs Rs Ys
  have hndY : Ys.Nodup := by dsimp [Ys]; exact nodup_eraseDups (r.map Prod.snd)
  have hsub : ∀ p ∈ r, p ∈ L := by
    intro p hp
    have hx : p.1 ∈ Rs := by
      simp [Rs]
      exact ⟨p.2, hp⟩
    have hy : p.2 ∈ Ys := by
      simp [Ys]
      exact ⟨p.1, hp⟩
    dsimp [L, pairs]
    rw [List.mem_flatMap]
    refine ⟨p.1, hx, ?_⟩
    rw [List.mem_map]
    exact ⟨p.2, hy, rfl⟩
  have hle1 := sum_le_sum_of_nodup_mem r L (fun p => ds s p.2 * ct t p.1) hr hsub
  have hprod : (L.map fun p => ds s p.2 * ct t p.1).sum =
      (Ys.map fun y => ds s y).sum * (Rs.map fun x => ct t x).sum := by
    dsimp [L]
    have h := product_sum Rs Ys (fun x => ct t x) (fun y => ds s y)
    simpa [L, pairs, Nat.mul_comm] using h
  have hsumDs : (Ys.map fun y => ds s y).sum ≤ s.length := by
    let Sy := (s.map Prod.fst).eraseDups
    have hndSy : Sy.Nodup := nodup_eraseDups (s.map Prod.fst)
    have hzero : ∀ y ∈ Ys, y ∉ Sy → ds s y = 0 := by
      intro y hy hnot
      dsimp [ds]
      rw [List.count_eq_zero]
      intro hmem
      apply hnot
      simp [Sy, List.mem_eraseDups, hmem]
    have hle := sum_le_sum_nodup_zero Ys Sy (fun y => ds s y) hndY hzero
    have heq := sum_counts_eq_length (s.map Prod.fst) Sy hndSy
      (by intro x hx; simp [Sy, List.mem_eraseDups, hx])
    dsimp [ds] at hle
    have heq' : (Sy.map fun k => (s.map Prod.fst).count k).sum = s.length := by simpa using heq
    change (Ys.map fun y => (s.map Prod.fst).count y).sum ≤ s.length
    omega
  have hsumCt : (Rs.map fun x => ct t x).sum ≤ t.length := by
    let Tx := (t.map Prod.snd).eraseDups
    have hndTx : Tx.Nodup := nodup_eraseDups (t.map Prod.snd)
    have hndR : Rs.Nodup := by dsimp [Rs]; exact nodup_eraseDups (r.map Prod.fst)
    have hzero : ∀ x ∈ Rs, x ∉ Tx → ct t x = 0 := by
      intro x hx hnot
      dsimp [ct]
      rw [List.count_eq_zero]
      intro hmem
      apply hnot
      simp [Tx, List.mem_eraseDups, hmem]
    have hle := sum_le_sum_nodup_zero Rs Tx (fun x => ct t x) hndR hzero
    have heq := sum_counts_eq_length (t.map Prod.snd) Tx hndTx
      (by intro x hx; simp [Tx, List.mem_eraseDups, hx])
    dsimp [ct] at hle
    have heq' : (Tx.map fun k => (t.map Prod.snd).count k).sum = t.length := by simpa using heq
    change (Rs.map fun x => (t.map Prod.snd).count x).sum ≤ t.length
    omega
  rw [hprod] at hle1
  have hnonneg1 : 0 ≤ (Ys.map fun y => ds s y).sum := Nat.zero_le _
  exact Nat.le_trans hle1 (Nat.mul_le_mul hsumDs hsumCt)


theorem TriangleBound_sound :
    TinylambOpt.ResearchGoals.SearchBounds.TriangleBound.Goal := by
  intro r s t hr hs ht
  rw [triangles_len]
  have hcs := cs_fun r (fun p => fTri r s t p.1 p.2) (fun _ => 1)
  have hcs' : ((r.map fun p => fTri r s t p.1 p.2).sum)^2 ≤
      (r.map fun p => fTri r s t p.1 p.2 ^ 2).sum * r.length := by
    have hcs2 := hcs
    simp only [Nat.pow_one, Nat.mul_one] at hcs2
    have hone := sum_ones_length r
    rw [hone] at hcs2
    simpa using hcs2
  have hsq := tri_f_sq_le r s t hr hs
  have hsum := sum_ds_ct_le r s t hr ht
  have hmul1 : (r.map fun p => fTri r s t p.1 p.2 ^ 2).sum * r.length ≤
      (r.map fun p => ds s p.2 * ct t p.1).sum * r.length := Nat.mul_le_mul_right r.length hsq
  have hmul2 : (r.map fun p => ds s p.2 * ct t p.1).sum * r.length ≤
      (s.length * t.length) * r.length := Nat.mul_le_mul_right r.length hsum
  have hassoc : (s.length * t.length) * r.length = r.length * s.length * t.length := by
    rw [Nat.mul_comm (s.length * t.length) r.length, Nat.mul_assoc]
  omega


/-! ### FrequentCandidates -/

private def decStep (acc : Std.HashMap Nat Nat) (p : Nat × Nat) : Std.HashMap Nat Nat :=
  if p.2 = 1 then acc else acc.insert p.1 (p.2 - 1)


private theorem foldl_dec_size (l : List (Nat × Nat)) (acc : Std.HashMap Nat Nat) :
    (l.foldl decStep acc).size ≤ acc.size + l.length := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      by_cases hp : p.2 = 1
      · have hih := ih acc
        simp [decStep, hp]
        omega
      · have hs : (acc.insert p.1 (p.2 - 1)).size ≤ acc.size + 1 := by
          have := Std.HashMap.size_insert_le (m := acc) (k := p.1) (v := p.2 - 1)
          omega
        have hih := ih (acc.insert p.1 (p.2 - 1))
        simp [decStep, hp]
        omega


private theorem dec_size (h : Std.HashMap Nat Nat) :
  (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).size ≤ h.size := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change (h.toList.foldl decStep {}).size ≤ h.size
  have hf := foldl_dec_size h.toList {}
  simpa using hf


private def csumL (l : List (Nat × Nat)) : Nat := (l.map Prod.snd).sum


private def csum (h : Std.HashMap Nat Nat) : Nat := csumL h.toList


private theorem empty_csum : csum ({} : Std.HashMap Nat Nat) = 0 := by simp [csum, csumL]


private def notKey (k : Nat) (p : Nat × Nat) : Bool := decide ((decide (¬ (k == p.1))) = true)


private theorem my_beq_false_of_ne {a b : Nat} (h : a ≠ b) : (a == b) = false := beq_eq_false_iff_ne.mpr h


private theorem csum_insert_new (h : Std.HashMap Nat Nat) (k v : Nat) (hk : ¬ h.contains k) :
    csum (h.insert k v) = csum h + v := by
  have hf : h.toList.filter (notKey k) = h.toList := by
    apply List.filter_eq_self.mpr
    intro p hp
    by_cases hkp : k == p.1
    · exfalso
      apply hk
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
      rw [Std.HashMap.contains_eq_isSome_getElem?]
      rw [Std.HashMap.getElem?_congr hkp]
      rw [hp]
      simp
    · simp [notKey, hkp]
  have hpperm : (h.insert k v).toList.Perm ((k, v) :: h.toList.filter (notKey k)) :=
    Std.HashMap.toList_insert_perm (m := h) (k := k) (v := v)
  rw [hf] at hpperm
  dsimp [csum, csumL]
  rw [perm_sum (hpperm.map Prod.snd)]
  simp [Nat.add_comm]


private theorem contains_foldl_dec (l : List (Nat × Nat)) (acc : Std.HashMap Nat Nat) (v : Nat)
    (hno : ∀ p ∈ l, (p.1 == v) = false) (hacc : ¬ acc.contains v) :
    ¬ (l.foldl decStep acc).contains v := by
  induction l generalizing acc with
  | nil => simpa using hacc
  | cons p ps ih =>
      simp only [List.foldl_cons]
      apply ih
      · intro q hq
        exact hno q (by simp [hq])
      · by_cases hp1 : p.2 = 1
        · simpa [decStep, hp1] using hacc
        · simp only [decStep, hp1]
          have hpv : p.1 ≠ v := by
            intro he
            have ht : (p.1 == v) = true := by simp [he]
            rw [hno p (by simp)] at ht
            contradiction
          simpa [hpv] using hacc


private theorem foldl_dec_csum (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (acc : Std.HashMap Nat Nat)
    (hno : ∀ p ∈ l, ¬ acc.contains p.1) :
    csum (l.foldl decStep acc) = csum acc + (l.map fun p => p.2 - 1).sum := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      have hnd_head : ∀ q ∈ ps, (p.1 == q.1) = false := (List.pairwise_cons.mp hnd).1
      have hnd_tail : ps.Pairwise (fun a b => (a.1 == b.1) = false) := (List.pairwise_cons.mp hnd).2
      have hnotp := hno p (by simp)
      simp only [List.foldl_cons]
      by_cases hp1 : p.2 = 1
      · have hih := ih hnd_tail acc (by intro q hq; exact hno q (by simp [hq]))
        simp [decStep, hp1, hih]
      · have hins := csum_insert_new acc p.1 (p.2 - 1) hnotp
        have hno' : ∀ q ∈ ps, ¬ (acc.insert p.1 (p.2 - 1)).contains q.1 := by
          intro q hq hcont
          rw [Std.HashMap.contains_insert] at hcont
          simp only [Bool.or_eq_true] at hcont
          rcases hcont with hb | hm
          · have heq : p.1 = q.1 := beq_iff_eq.mp hb
            have hn := hnd_head q hq
            have : (p.1 == q.1) = true := by simp [heq]
            rw [hn] at this
            contradiction
          · exact hno q (by simp [hq]) (Std.HashMap.contains_iff_mem.mp hm)
        have hih := ih hnd_tail (acc.insert p.1 (p.2 - 1)) hno'
        rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        simp only [List.map_cons, List.sum_cons]
        rw [hih, hins]
        omega


private theorem sum_sub_one (l : List (Nat × Nat)) (hpos : ∀ p ∈ l, 0 < p.2) :
    (l.map fun p => p.2 - 1).sum + l.length = csumL l := by
  induction l with
  | nil => simp [csumL]
  | cons p ps ih =>
      have htail : ∀ q ∈ ps, 0 < q.2 := by intro q hq; exact hpos q (by simp [hq])
      have hih := ih htail
      have hp : 0 < p.2 := hpos p (by simp)
      simp only [List.map_cons, List.sum_cons, List.length_cons, csumL]
      simp only [csumL] at hih
      omega


private theorem dec_csum (h : Std.HashMap Nat Nat) (hpos : ∀ v, h.contains v → 0 < h.getD v 0) :
    csum (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}) + h.size ≤ csum h := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change csum (h.toList.foldl decStep {}) + h.size ≤ csum h
  have hnd := Std.HashMap.distinct_keys_toList (m := h)
  have hposl : ∀ p ∈ h.toList, 0 < p.2 := by
    intro p hp
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
    have hg : h.getD p.1 0 = p.2 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hp]
      simp
    rw [← hg]
    apply hpos p.1
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    rw [hp]
    simp
  have hno : ∀ p ∈ h.toList, ¬ ({} : Std.HashMap Nat Nat).contains p.1 := by simp
  have hf := foldl_dec_csum h.toList hnd {} hno
  rw [empty_csum] at hf
  have hs := sum_sub_one h.toList hposl
  dsimp [csum] at hf ⊢
  rw [hf]
  have hlen : h.size = h.toList.length := (Std.HashMap.length_toList (m := h)).symm
  rw [hlen]
  simp only [Nat.zero_add]
  rw [hs]
  exact Nat.le_refl _


private theorem foldl_dec_getD (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (acc : Std.HashMap Nat Nat) (hno : ∀ p ∈ l, ¬ acc.contains p.1) (v : Nat) :
    acc.getD v 0 ≤ (l.foldl decStep acc).getD v 0 + (l.filter (fun p => p.1 == v)).length := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      have hnd_head : ∀ q ∈ ps, (p.1 == q.1) = false := (List.pairwise_cons.mp hnd).1
      have hnd_tail : ps.Pairwise (fun a b => (a.1 == b.1) = false) := (List.pairwise_cons.mp hnd).2
      have hno_tail : ∀ q ∈ ps, ¬ acc.contains q.1 := by intro q hq; exact hno q (by simp [hq])
      simp only [List.foldl_cons]
      by_cases hp1 : p.2 = 1
      · rw [show decStep acc p = acc by simp [decStep, hp1]]
        have hih := ih hnd_tail acc hno_tail
        by_cases hpv : p.1 == v
        · have hf : (p :: ps).filter (fun q => q.1 == v) = p :: ps.filter (fun q => q.1 == v) := by simp [hpv]
          rw [hf]
          simp
          omega
        · have hf : (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) := by simp [hpv]
          rw [hf]
          simpa using hih
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        have hno' : ∀ q ∈ ps, ¬ (acc.insert p.1 (p.2 - 1)).contains q.1 := by
          intro q hq hcont
          rw [Std.HashMap.contains_insert] at hcont
          simp only [Bool.or_eq_true] at hcont
          rcases hcont with hb | hm
          · have heq : p.1 = q.1 := beq_iff_eq.mp hb
            have hn := hnd_head q hq
            have : (p.1 == q.1) = true := by simp [heq]
            rw [hn] at this
            contradiction
          · exact hno q (by simp [hq]) (Std.HashMap.contains_iff_mem.mp hm)
        have hih := ih hnd_tail (acc.insert p.1 (p.2 - 1)) hno' 
        by_cases hpv : p.1 == v
        · have heq : p.1 = v := beq_iff_eq.mp hpv
          have hnotv : ¬ acc.contains v := by
            have := hno p (by simp)
            rwa [heq] at this
          have hcf : acc.contains v = false := by by_cases c : acc.contains v <;> simp_all
          have haccd : acc.getD v 0 = 0 := Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
          rw [haccd]
          simp
        · rw [Std.HashMap.getD_insert] at hih
          simp [hpv] at hih
          rw [show (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) by simp [hpv]]
          simpa using hih


private theorem nodup_keys_dec (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (v : Nat) : (l.filter (fun p => p.1 == v)).length ≤ 1 := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      rw [List.pairwise_cons] at hnd
      by_cases hpv : p.1 == v
      · have hno : ∀ q ∈ ps, (q.1 == v) = false := by
          intro q hq
          have hnq : (p.1 == q.1) = false := hnd.1 q hq
          have hnq' : p.1 ≠ q.1 := by
            intro he
            have : (p.1 == q.1) = true := by simp [he]
            rw [hnq] at this
            contradiction
          rw [beq_eq_false_iff_ne]
          intro hqv
          have hpe : p.1 = v := beq_iff_eq.mp (by simpa using hpv)
          exact hnq' (hpe.trans hqv.symm)
        have hf : ps.filter (fun q => q.1 == v) = [] := by
          rw [List.eq_nil_iff_forall_not_mem]
          intro q hq
          rw [List.mem_filter] at hq
          rcases hq with ⟨hqmem, hqbeq⟩
          have hfq := hno q hqmem
          rw [hfq] at hqbeq
          contradiction
        simp [hpv, hf]
      · have := ih hnd.2
        have hf : (p :: ps).filter (fun q => q.1 == v) = ps.filter (fun q => q.1 == v) := by simp [hpv]
        rw [hf]
        exact this


private theorem foldl_dec_getD_of_not_mem (l : List (Nat × Nat)) (acc : Std.HashMap Nat Nat) (v : Nat)
    (hno : ∀ p ∈ l, (p.1 == v) = false) :
    (l.foldl decStep acc).getD v 0 = acc.getD v 0 := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      have hpv : (p.1 == v) = false := hno p (by simp)
      have hno_tail : ∀ q ∈ ps, (q.1 == v) = false := by intro q hq; exact hno q (by simp [hq])
      by_cases hp1 : p.2 = 1
      · rw [show decStep acc p = acc by simp [decStep, hp1]]
        exact ih acc hno_tail
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        have hih := ih (acc.insert p.1 (p.2 - 1)) hno_tail
        rw [hih, Std.HashMap.getD_insert]
        simp [hpv]


private theorem split_entry (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (v n : Nat) (hmem : ∃ p ∈ l, p.1 == v ∧ p.2 = n) :
    ∃ l₁ l₂, l = l₁ ++ (v, n) :: l₂ ∧ ∀ p ∈ l₁ ++ l₂, (p.1 == v) = false := by
  induction l with
  | nil => simp at hmem
  | cons p ps ih =>
      rcases hmem with ⟨q, hqmem, hqv, hqn⟩
      by_cases hpv : p.1 == v
      · have hqeq : q = p := by
          have hqmem' : q = p ∨ q ∈ ps := by simpa using hqmem
          rcases hqmem' with h | hqps
          · exact h
          · have hqe : q.1 = v := beq_iff_eq.mp hqv
            have hpe : p.1 = v := beq_iff_eq.mp hpv
            have : (p.1 == q.1) = true := by simp [hpe, hqe]
            rw [(List.pairwise_cons.mp hnd).1 q hqps] at this
            contradiction
        subst q
        have hpe : p.1 = v := beq_iff_eq.mp hpv
        have hpn : p.2 = n := hqn
        have hno : ∀ r ∈ ps, (r.1 == v) = false := by
          intro r hr
          rw [beq_eq_false_iff_ne]
          intro hrv
          have : (p.1 == r.1) = true := by simp [hpe, hrv]
          rw [(List.pairwise_cons.mp hnd).1 r hr] at this
          contradiction
        refine ⟨[], ps, ?_, ?_⟩
        · cases p with
          | mk p1 p2 => simp_all
        · simpa using hno
      · have hqps : q ∈ ps := by
          have hqmem' : q = p ∨ q ∈ ps := by simpa using hqmem
          rcases hqmem' with h | h
          · subst q
            exfalso
            exact hpv hqv
          · exact h
        have hnd_tail := (List.pairwise_cons.mp hnd).2
        rcases ih hnd_tail ⟨q, hqps, hqv, hqn⟩ with ⟨l₁, l₂, hl, hno⟩
        have hpvf : (p.1 == v) = false := by by_cases h : p.1 == v <;> simp_all
        refine ⟨p :: l₁, l₂, ?_, ?_⟩
        · simp [hl]
        · intro r hr
          simp at hr
          rcases hr with heq | hr
          · subst r
            exact hpvf
          · exact hno r (List.mem_append.mpr hr)


private theorem dec_getD_of_list (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (v n : Nat) (hentry : ∃ p ∈ l, p.1 == v ∧ p.2 = n) :
    n ≤ (l.foldl decStep {}).getD v 0 + 1 := by
  rcases split_entry l hnd v n hentry with ⟨l₁, l₂, hl, hnv⟩
  subst l
  simp only [List.foldl_append, List.foldl_cons]
  have hnv1 : ∀ p ∈ l₁, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
  have hnv2 : ∀ p ∈ l₂, (p.1 == v) = false := by intro p hp; exact hnv p (by simp [hp])
  have hfirst := foldl_dec_getD_of_not_mem l₁ {} v hnv1
  simp only [Std.HashMap.getD_empty] at hfirst
  by_cases hn1 : n = 1
  · rw [show decStep (l₁.foldl decStep {}) (v, n) = l₁.foldl decStep {} by simp [decStep, hn1]]
    have hlast := foldl_dec_getD_of_not_mem l₂ (l₁.foldl decStep {}) v hnv2
    rw [hlast, hfirst]
    omega
  · rw [show decStep (l₁.foldl decStep {}) (v, n) = (l₁.foldl decStep {}).insert v (n - 1) by simp [decStep, hn1]]
    have hlast := foldl_dec_getD_of_not_mem l₂ ((l₁.foldl decStep {}).insert v (n - 1)) v hnv2
    rw [hlast, Std.HashMap.getD_insert]
    simp
    omega


private theorem dec_getD (h : Std.HashMap Nat Nat) (v : Nat) :
    h.getD v 0 ≤ (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).getD v 0 + 1 := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change h.getD v 0 ≤ (h.toList.foldl decStep {}).getD v 0 + 1
  have hnd := Std.HashMap.distinct_keys_toList (m := h)
  by_cases hmem : ∃ n, h[v]? = some n
  · rcases hmem with ⟨n, hn⟩
    have hg : h.getD v 0 = n := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hn]
      simp
    rw [hg]
    have hentry : ∃ p ∈ h.toList, p.1 == v ∧ p.2 = n := by
      rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList] at hn
      rcases hn with ⟨k, hkv, hk⟩
      refine ⟨(k, n), hk, ?_, rfl⟩
      have hkv_eq : k = v := (beq_iff_eq.mp hkv).symm
      simp [hkv_eq]
    exact dec_getD_of_list h.toList hnd v n hentry
  · have hg : h.getD v 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?]
      cases h' : h[v]? with
      | none => simp
      | some x => exact False.elim (hmem ⟨x, h'⟩)
    rw [hg]
    exact Nat.zero_le _


private theorem foldl_dec_allpos (l : List (Nat × Nat)) (hpos : ∀ p ∈ l, 0 < p.2)
    (acc : Std.HashMap Nat Nat) (hacc : ∀ v, acc.contains v → 0 < acc.getD v 0) :
    ∀ v, (l.foldl decStep acc).contains v → 0 < (l.foldl decStep acc).getD v 0 := by
  induction l generalizing acc with
  | nil => simpa using hacc
  | cons p ps ih =>
      simp only [List.foldl_cons]
      have hpos_tail : ∀ q ∈ ps, 0 < q.2 := by intro q hq; exact hpos q (by simp [hq])
      by_cases hp1 : p.2 = 1
      · rw [show decStep acc p = acc by simp [decStep, hp1]]
        exact ih hpos_tail acc hacc
      · rw [show decStep acc p = acc.insert p.1 (p.2 - 1) by simp [decStep, hp1]]
        have hp2 : 2 ≤ p.2 := by
          have := hpos p (by simp)
          omega
        have hacc' : ∀ v, (acc.insert p.1 (p.2 - 1)).contains v → 0 < (acc.insert p.1 (p.2 - 1)).getD v 0 := by
          intro v hv
          rw [Std.HashMap.getD_insert]
          by_cases hbeq : p.1 == v
          · simp [hbeq]
            omega
          · have hm : acc.contains v := by
              rw [Std.HashMap.contains_insert] at hv
              simp [hbeq] at hv
              exact hv
            simpa [hbeq] using hacc v hm
        exact ih hpos_tail (acc.insert p.1 (p.2 - 1)) hacc'


private theorem dec_allpos (h : Std.HashMap Nat Nat) (hpos : ∀ v, h.contains v → 0 < h.getD v 0) :
    ∀ v, (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).contains v →
      0 < (h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n-1)) {}).getD v 0 := by
  rw [Std.HashMap.fold_eq_foldl_toList]
  change ∀ v, (h.toList.foldl decStep {}).contains v → 0 < (h.toList.foldl decStep {}).getD v 0
  have hposl : ∀ p ∈ h.toList, 0 < p.2 := by
    intro p hp
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
    have hg : h.getD p.1 0 = p.2 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hp]
      simp
    rw [← hg]
    apply hpos p.1
    rw [Std.HashMap.contains_eq_isSome_getElem?]
    rw [hp]
    simp
  exact foldl_dec_allpos h.toList hposl {} (by simp)


private theorem csumL_erase (l : List (Nat × Nat)) (p : Nat × Nat) (hp : p ∈ l) :
    csumL (l.erase p) + p.2 = csumL l := by
  induction l with
  | nil => simp at hp
  | cons q qs ih =>
      by_cases hqp : q == p
      · have heq : q = p := beq_iff_eq.mp hqp
        subst q
        simp [csumL]
        omega
      · have hpqs : p ∈ qs := by
          rcases List.mem_cons.mp hp with hpq | hpqs
          · have heq : p = q := hpq
            apply False.elim
            apply hqp
            simp [heq]
          · exact hpqs
        have hih := ih hpqs
        rw [List.erase_cons_tail hqp]
        dsimp [csumL] at hih ⊢
        simp only [List.map_cons, List.sum_cons]
        omega


private theorem filter_ne_key_eq_erase (l : List (Nat × Nat))
    (hnd : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (k n : Nat) (hp : (k, n) ∈ l) :
    l.filter (notKey k) = l.erase (k, n) := by
  induction l with
  | nil => simp at hp
  | cons q qs ih =>
      by_cases hq : q == (k, n)
      · have heq : q = (k, n) := beq_iff_eq.mp hq
        subst q
        have hno : ∀ r ∈ qs, (r.1 == k) = false := by
          intro r hr
          rw [beq_eq_false_iff_ne]
          intro hrk
          have hn := (List.pairwise_cons.mp hnd).1 r hr
          have : ((k, n).fst == r.fst) = true := by simp [hrk]
          rw [hn] at this
          contradiction
        have hfself : qs.filter (notKey k) = qs := by
          apply List.filter_eq_self.mpr
          intro x hx
          by_cases hxk : k == x.1
          · have heq' : x.1 = k := (beq_iff_eq.mp hxk).symm
            have : (x.1 == k) = true := by simp [heq']
            rw [hno x hx] at this
            contradiction
          · have hxk' : (x.1 == k) = false := by by_cases h' : x.1 == k <;> simp_all
            have hkxf : (k == x.1) = false := by
              by_cases h' : k == x.1
              · have hke : k = x.1 := beq_iff_eq.mp h'
                have ht : (x.1 == k) = true := by rw [hke]; simp
                rw [hxk'] at ht
                contradiction
              · have hne : k ≠ x.1 := by
                  intro he
                  have ht : (k == x.1) = true := by rw [he]; simp
                  exact hxk ht
                exact beq_eq_false_iff_ne.mpr hne
            simp [notKey, hkxf]
        have herase : qs.erase (k, n) = qs := by
          rw [List.erase_eq_self_iff]
          intro hmem
          have := hno (k, n) hmem
          simp at this
        simp [notKey, hfself]
      · have hpqs : (k, n) ∈ qs := by
          rcases List.mem_cons.mp hp with hpq | hpqs
          · exfalso
            apply hq
            simpa using hpq.symm
          · exact hpqs
        have hnd_tail := (List.pairwise_cons.mp hnd).2
        have hih := ih hnd_tail hpqs
        have hqk : (k == q.1) = false := by
          by_cases hb : k == q.1
          · have heq : k = q.1 := beq_iff_eq.mp hb
            have hnq := (List.pairwise_cons.mp hnd).1 (k, n) hpqs
            have ht : (q.fst == k) = true := by
              have hkeq : q.fst = k := heq.symm
              rw [hkeq]
              simp
            rw [hnq] at ht
            contradiction
          · by_cases h' : k == q.1 <;> simp_all
        rw [List.erase_cons_tail (by
          intro ht
          exact hq ht)]
        rw [show List.filter (notKey k) (q :: qs) = q :: List.filter (notKey k) qs by
          simp [notKey, hqk]]
        rw [hih]


private theorem entry_of_contains (h : Std.HashMap Nat Nat) (k : Nat) (hk : h.contains k) :
    ∃ n, (k, n) ∈ h.toList ∧ h.getD k 0 = n := by
  have hsome : h[k]? = some (h.getD k 0) := Std.HashMap.getElem?_eq_some_getD_of_contains hk
  rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList] at hsome
  rcases hsome with ⟨k', hbeq, hmem⟩
  have heq : k' = k := (beq_iff_eq.mp hbeq).symm
  subst k'
  exact ⟨h.getD k 0, hmem, rfl⟩


private theorem csum_insert_succ (h : Std.HashMap Nat Nat) (k : Nat) :
    csum (h.insert k (h.getD k 0 + 1)) = csum h + 1 := by
  by_cases hk : h.contains k
  · rcases entry_of_contains h k hk with ⟨n, hnmem, hnget⟩
    have hnd := Std.HashMap.distinct_keys_toList (m := h)
    have hf := filter_ne_key_eq_erase h.toList hnd k n hnmem
    have hp : (h.insert k (h.getD k 0 + 1)).toList.Perm
        ((k, h.getD k 0 + 1) :: h.toList.filter (notKey k)) :=
      Std.HashMap.toList_insert_perm (m := h) (k := k) (v := h.getD k 0 + 1)
    rw [hf] at hp
    dsimp [csum, csumL]
    rw [perm_sum (hp.map Prod.snd)]
    have her := csumL_erase h.toList (k, n) hnmem
    dsimp [csumL] at her ⊢
    simp only [List.map_cons, List.sum_cons]
    omega
  · have hget : h.getD k 0 = 0 := by
      have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
      exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
    rw [hget]
    simpa using csum_insert_new h k 1 hk


private def mgStep (slots : Nat) (h : Std.HashMap Nat Nat) (k : Nat) : Std.HashMap Nat Nat :=
  if h.contains k then h.insert k (h.getD k 0 + 1)
  else if h.size < slots then h.insert k 1
  else h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n == 1 then acc else acc.insert j (n-1)) {}


private theorem candidates_eq_mg (xs : List Nat) (slots : Nat) :
    TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots = xs.foldl (mgStep slots) {} := by
  rfl


private def mgAux (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) : List Nat → Std.HashMap Nat Nat × Nat × Nat
  | [] => (h, d, n)
  | k :: ks =>
      let d' := if h.contains k then d else if h.size < slots then d else d + 1
      mgAux slots (mgStep slots h k) d' (n + 1) ks


private def MGInv (slots : Nat) (pre : List Nat) (st : Std.HashMap Nat Nat × Nat × Nat) : Prop :=
  (∀ v, pre.count v ≤ st.1.getD v 0 + st.2.1) ∧
  (∀ v, st.1.contains v → 0 < st.1.getD v 0) ∧
  csum st.1 + st.2.1 * (slots + 1) ≤ st.2.2 ∧
  st.1.size ≤ slots


private theorem count_append_singleton (pre : List Nat) (k v : Nat) :
    (pre ++ [k]).count v = pre.count v + (if k == v then 1 else 0) := by
  rw [List.count_append]
  simp [List.count]


private theorem mgStep_inv (slots : Nat) (pre : List Nat) (h : Std.HashMap Nat Nat) (d k : Nat)
    (hinv : MGInv slots pre (h, d, pre.length)) :
    MGInv slots (pre ++ [k])
      (mgStep slots h k,
       (if h.contains k then d else if h.size < slots then d else d + 1),
       pre.length + 1) := by
  dsimp [MGInv] at hinv
  rcases hinv with ⟨hcnt, hpos, hcsum, hsize⟩
  dsimp [MGInv]
  by_cases hk : h.contains k
  · have hget : h.getD k 0 + 1 = (h.insert k (h.getD k 0 + 1)).getD k 0 := by
      rw [Std.HashMap.getD_insert]
      simp
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro v
      rw [count_append_singleton]
      by_cases hv : k == v
      · have hveq : k = v := beq_iff_eq.mp hv
        subst v
        simp [mgStep, hk]
        have := hcnt k
        omega
      · have hvf : (k == v) = false := by by_cases h : k == v <;> simp_all
        simp [hvf, mgStep, hk]
        rw [Std.HashMap.getD_insert, hvf]
        exact hcnt v
    · intro v hv
      rw [mgStep] at hv
      simp [hk] at hv
      rw [mgStep]
      simp [hk]
      rw [Std.HashMap.getD_insert]
      by_cases hkv : k == v
      · simp [hkv]
      · simp [hkv]
        rcases hv with heq | hm
        · subst v
          exact hpos k hk
        · exact hpos v (Std.HashMap.contains_iff_mem.mpr hm)
    · rw [mgStep]
      simp [hk]
      rw [csum_insert_succ]
      omega
    · rw [mgStep]
      simp [hk]
      have hsz : (h.insert k (h.getD k 0 + 1)).size = h.size := by
        rw [Std.HashMap.size_insert]
        simp [Std.HashMap.mem_iff_contains.mpr hk]
      rw [hsz]
      exact hsize
  · by_cases hfull : h.size < slots
    · have hget0 : h.getD k 0 = 0 := by
        have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
        exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
      refine ⟨?_, ?_, ?_, ?_⟩
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
          have := hcnt k
          rw [hget0] at this
          omega
        · have hvf : (k == v) = false := by by_cases h : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          rw [Std.HashMap.getD_insert, hvf]
          exact hcnt v
      · intro v hv
        rw [mgStep] at hv
        simp [hk, hfull] at hv
        rw [mgStep]
        simp [hk, hfull]
        rw [Std.HashMap.getD_insert]
        by_cases hkv : k == v
        · simp [hkv]
        · simp [hkv]
          rcases hv with heq | hm
          · subst v
            exfalso
            exact hk (by by_cases c : h.contains k <;> simp_all)
          · exact hpos v (Std.HashMap.contains_iff_mem.mpr hm)
      · rw [mgStep]
        simp [hk, hfull]
        rw [csum_insert_new h k 1 hk]
        omega
      · rw [mgStep]
        simp [hk, hfull]
        have hs := Std.HashMap.size_insert_le (m := h) (k := k) (v := 1)
        omega
    · have hsizeEq : h.size = slots := by omega
      have hget0 : h.getD k 0 = 0 := by
        have hcf : h.contains k = false := by by_cases c : h.contains k <;> simp_all
        exact Std.HashMap.getD_eq_fallback_of_contains_eq_false hcf
      let h' := h.fold (fun (acc : Std.HashMap Nat Nat) j n => if n = 1 then acc else acc.insert j (n - 1)) {}
      refine ⟨?_, ?_, ?_, ?_⟩
      · intro v
        rw [count_append_singleton]
        by_cases hv : k == v
        · have hveq : k = v := beq_iff_eq.mp hv
          subst v
          simp [mgStep, hk, hfull]
          have := hcnt k
          rw [hget0] at this
          omega
        · have hvf : (k == v) = false := by by_cases h : k == v <;> simp_all
          simp [hvf, mgStep, hk, hfull]
          have hd := dec_getD h v
          have hc := hcnt v
          omega
      · intro v hv
        rw [mgStep] at hv
        simp [hk, hfull] at hv
        rw [mgStep]
        simp [hk, hfull]
        exact dec_allpos h hpos v hv
      · rw [mgStep]
        simp [hk, hfull]
        have hcs := dec_csum h hpos
        rw [hsizeEq] at hcs
        have hmul : (d + 1) * (slots + 1) = d * (slots + 1) + slots + 1 := by
          rw [Nat.add_mul, Nat.one_mul, Nat.mul_add, Nat.mul_one]
          omega
        rw [hmul]
        omega
      · rw [mgStep]
        simp [hk, hfull]
        exact Nat.le_trans (dec_size h) hsize


private theorem mgAux_inv (slots : Nat) :
    ∀ (pre : List Nat) (h : Std.HashMap Nat Nat) (d : Nat),
      MGInv slots pre (h, d, pre.length) →
      ∀ rest : List Nat, MGInv slots (pre ++ rest) (mgAux slots h d pre.length rest) := by
  intro pre h d hinv rest
  induction rest generalizing pre h d with
  | nil => simpa [mgAux] using hinv
  | cons k ks ih =>
      let d1 := if h.contains k then d else if h.size < slots then d else d + 1
      have hstep := mgStep_inv slots pre h d k hinv
      have hlen : (pre ++ [k]).length = pre.length + 1 := by simp
      have hstep2 : MGInv slots (pre ++ [k]) (mgStep slots h k, d1, (pre ++ [k]).length) := by
        rw [hlen]
        exact hstep
      have hih := ih (pre ++ [k]) (mgStep slots h k) d1 hstep2
      rw [hlen] at hih
      rw [List.append_assoc] at hih
      dsimp [mgAux]
      exact hih


private theorem mgAux_fst (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) :
    ∀ xs, (mgAux slots h d n xs).1 = xs.foldl (mgStep slots) h := by
  intro xs
  induction xs generalizing h d n with
  | nil => simp [mgAux]
  | cons k ks ih =>
      dsimp [mgAux]
      exact ih (mgStep slots h k) _ (n + 1)


private theorem mgAux_len (slots : Nat) (h : Std.HashMap Nat Nat) (d n : Nat) :
    ∀ xs, (mgAux slots h d n xs).2.2 = n + xs.length := by
  intro xs
  induction xs generalizing h d n with
  | nil => simp [mgAux]
  | cons k ks ih =>
      dsimp [mgAux]
      have hih := ih (mgStep slots h k) (if h.contains k then d else if h.size < slots then d else d + 1) (n + 1)
      omega


theorem FrequentCandidates_sound :
    TinylambOpt.ResearchGoals.SearchBounds.FrequentCandidates.Goal := by
  intro xs slots k
  have hinv0 : MGInv slots [] ({}, 0, 0) := by
    dsimp [MGInv]
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro v
      simp
    · intro v hv
      simp at hv
    · rw [empty_csum]
      simp
    · simp
  have hfinal := mgAux_inv slots [] {} 0 hinv0 xs
  dsimp [MGInv] at hfinal
  rcases hfinal with ⟨hcnt, hpos, hcsum, hsize⟩
  constructor
  · change (TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots).size ≤ slots
    rw [candidates_eq_mg]
    rw [← mgAux_fst slots {} 0 0 xs]
    simpa using hsize
  · intro hlt
    by_cases hcand : (TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots).contains k = true
    · exact hcand
    · exfalso
      have hnot' : (mgAux slots {} 0 0 xs).1.contains k = false := by
        rw [mgAux_fst slots {} 0 0 xs, ← candidates_eq_mg]
        by_cases c : (TinylambOpt.ResearchGoals.SearchBounds.candidates xs slots).contains k <;> simp_all
      have hget : (mgAux slots {} 0 0 xs).1.getD k 0 = 0 :=
        Std.HashMap.getD_eq_fallback_of_contains_eq_false hnot'
      have hc := hcnt k
      rw [hget] at hc
      simp at hc
      have hlen : (mgAux slots {} 0 0 xs).2.2 = xs.length := by
        simpa using mgAux_len slots {} 0 0 xs
      have hsum : (mgAux slots {} 0 0 xs).2.1 * (slots + 1) ≤ xs.length := by
        rw [← hlen]
        omega
      have hmul : xs.count k * (slots + 1) ≤ (mgAux slots {} 0 0 xs).2.1 * (slots + 1) :=
        Nat.mul_le_mul_right (slots + 1) hc
      omega


/-! ### SortedDegreeBound -/

private theorem desc_head_max {l : List Nat} (hd : Desc l) {x : Nat} (hx : x ∈ l) :
    x ≤ l.headD 0 := by
  cases l with
  | nil => simp at hx
  | cons a as =>
      simp only [List.headD_cons]
      rcases List.mem_cons.mp hx with rfl | hx
      · exact Nat.le_refl _
      · exact (List.pairwise_cons.mp hd).1 x hx


private theorem desc_perm_eq : ∀ (l₁ l₂ : List Nat), Desc l₁ → Desc l₂ → l₁.Perm l₂ → l₁ = l₂
  | [], l₂, _, _, p => by
      have hlen := p.length_eq
      cases l₂ with
      | nil => rfl
      | cons b bs => simp at hlen
  | a :: as, l₂, hd1, hd2, p => by
      cases l₂ with
      | nil =>
          have hlen := p.length_eq
          simp at hlen
      | cons b bs =>
          have ha_mem : a ∈ b :: bs := p.subset (by simp)
          have hb_mem : b ∈ a :: as := p.symm.subset (by simp)
          have hab : a ≤ b := desc_head_max hd2 ha_mem
          have hba : b ≤ a := desc_head_max hd1 hb_mem
          have heq : a = b := by omega
          subst b
          have htail : as.Perm bs := by simpa using (List.cons_perm_iff_perm_erase.mp p).2
          have hd1t : Desc as := (List.pairwise_cons.mp hd1).2
          have hd2t : Desc bs := (List.pairwise_cons.mp hd2).2
          simp [desc_perm_eq as bs hd1t hd2t htail]


private def lePair (p q : Nat × Nat) : Bool := if p.1 = q.1 then p.2 ≤ q.2 else q.1 ≤ p.1


private theorem lePair_trans : ∀ a b c : Nat × Nat, lePair a b = true → lePair b c = true → lePair a c = true := by
  intro a b c hab hbc
  unfold lePair at hab hbc ⊢
  by_cases hab1 : a.1 = b.1
  · have ha2b2 : a.2 ≤ b.2 := by simpa [hab1] using hab
    by_cases hbc1 : b.1 = c.1
    · have hb2c2 : b.2 ≤ c.2 := by simpa [hbc1] using hbc
      have hac1 : a.1 = c.1 := by omega
      simp [hac1]
      exact Nat.le_trans ha2b2 hb2c2
    · have hc1b1 : c.1 ≤ b.1 := by simpa [hbc1] using hbc
      have hac1 : a.1 ≠ c.1 := by omega
      simp [hac1]
      omega
  · have hb1a1 : b.1 ≤ a.1 := by simpa [hab1] using hab
    by_cases hbc1 : b.1 = c.1
    · have hb2c2 : b.2 ≤ c.2 := by simpa [hbc1] using hbc
      have hac1 : a.1 ≠ c.1 := by omega
      simp [hac1]
      omega
    · have hc1b1 : c.1 ≤ b.1 := by simpa [hbc1] using hbc
      have hac1 : a.1 ≠ c.1 := by omega
      simp [hac1]
      omega


private theorem lePair_total (a b : Nat × Nat) : lePair a b || lePair b a = true := by
  unfold lePair
  by_cases hab : a.1 = b.1
  · simp [hab]
    omega
  · have hba : b.1 ≠ a.1 := by omega
    simp [hab, hba]
    omega


private def sortedPairs (xs : List Nat) : List (Nat × Nat) :=
  (xs.eraseDups.map fun k => (xs.count k, k)).mergeSort lePair


private theorem sortedPairs_perm (xs : List Nat) :
    (sortedPairs xs).Perm (xs.eraseDups.map fun k => (xs.count k, k)) := by
  simpa [sortedPairs] using List.mergeSort_perm (xs.eraseDups.map fun k => (xs.count k, k)) lePair


private theorem sortedPairs_nodup (xs : List Nat) : (sortedPairs xs).Nodup := by
  apply (sortedPairs_perm xs).nodup_iff.mpr
  exact (nodup_eraseDups xs).map (fun k => (xs.count k, k))
    (by intro a b hne heq; exact hne (congrArg Prod.snd heq))


private theorem sortedPairs_desc (xs : List Nat) : Desc ((sortedPairs xs).map Prod.fst) := by
  dsimp [Desc]
  have hpair := List.pairwise_mergeSort (le := lePair) lePair_trans
    lePair_total (xs.eraseDups.map fun k => (xs.count k, k))
  rw [List.pairwise_map]
  apply hpair.imp
  intro p q hpq
  unfold lePair at hpq
  by_cases h : p.1 = q.1
  · simp [h]
  · simp [h] at hpq
    exact hpq


private theorem counts_desc (xs : List Nat) : Desc ((counts xs).mergeSort.reverse) := by
  have hasc : ((counts xs).mergeSort (fun a b : Nat => a ≤ b)).Pairwise (fun a b : Nat => a ≤ b) := by
    have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
      (trans := by intro a b c h1 h2; simp only [decide_eq_true_eq] at h1 h2 ⊢; exact Nat.le_trans h1 h2)
      (total := by intro a b; simp only [decide_eq_true_eq, Bool.or_eq_true]; omega) (counts xs)
    simpa only [decide_eq_true_eq] using h
  dsimp [Desc]
  rw [List.pairwise_reverse]
  simpa using hasc


private theorem perm_reverse {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.reverse.Perm l₂.reverse := by
  induction p with
  | nil => simp
  | cons a p ih =>
      simpa [List.reverse_cons] using ih.append (List.Perm.refl [a])
  | swap a b l => simpa [List.reverse_cons] using (List.Perm.refl l.reverse).append (List.Perm.swap b a [])
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂


private theorem sortedPairs_fst_eq_counts (xs : List Nat) :
    (sortedPairs xs).map Prod.fst = (counts xs).mergeSort.reverse := by
  have hperm1 : ((sortedPairs xs).map Prod.fst).Perm (counts xs) := by
    have hp := (sortedPairs_perm xs).map Prod.fst
    have hmap : (xs.eraseDups.map (Prod.fst ∘ fun k => (xs.count k, k))) =
        xs.eraseDups.map (fun k => xs.count k) := by
      apply List.map_congr_left
      intro k hk
      rfl
    rw [List.map_map] at hp
    rw [hmap] at hp
    exact hp
  have hperm2 : ((counts xs).mergeSort.reverse).Perm (counts xs) := by
    exact (perm_reverse (List.mergeSort_perm (counts xs) (fun a b : Nat => a ≤ b))).trans
      (List.reverse_perm (counts xs))
  have hperm : ((sortedPairs xs).map Prod.fst).Perm ((counts xs).mergeSort.reverse) :=
    hperm1.trans hperm2.symm
  exact desc_perm_eq ((sortedPairs xs).map Prod.fst) ((counts xs).mergeSort.reverse)
    (sortedPairs_desc xs) (counts_desc xs) hperm


private def prefI (u : List Int) (s : Nat) : Int := (u.take (s+1)).sum


private theorem telescoping_int (v : List Int) (n : Nat) (hn : n ≤ v.length) :
    ((List.range n).map fun s => v.getD s 0 - v.getD (s+1) 0).sum + v.getD n 0 = v.getD 0 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hnle : n ≤ v.length := by omega
      have hih := ih hnle
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      omega


private theorem sum_add_mul_range (p d : Nat → Int) (x : Int) : ∀ m,
    ((List.range m).map fun s => (x + p s) * d s).sum =
      x * ((List.range m).map d).sum + ((List.range m).map fun s => p s * d s).sum := by
  intro m
  induction m with
  | zero => simp
  | succ m ih =>
      simp only [List.range_succ, List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      grind


private theorem abel_int (u v : List Int) (hlen : u.length = v.length) :
    ((u.zip v).map fun p => p.1 * p.2).sum =
      ((List.range u.length).map fun s =>
        prefI u s * (v.getD s 0 - v.getD (s+1) 0)).sum := by
  induction u generalizing v with
  | nil =>
      cases v with
      | nil => simp [prefI]
      | cons y ys => simp at hlen
  | cons x xs ih =>
      cases v with
      | nil => simp at hlen
      | cons y ys =>
          have htail : xs.length = ys.length := by simpa using hlen
          have hih := ih ys htail
          have htel := telescoping_int ys ys.length (Nat.le_refl _)
          have hysn : ys.getD ys.length 0 = 0 := by simp
          have hsumdiff : ((List.range ys.length).map fun s => ys.getD s 0 - ys.getD (s+1) 0).sum = ys.getD 0 0 := by
            omega
          change (((x :: xs).zip (y :: ys)).map (fun p => p.1 * p.2)).sum =
            ((List.range (xs.length + 1)).map fun s =>
              prefI (x :: xs) s * ((y :: ys).getD s 0 - (y :: ys).getD (s+1) 0)).sum
          rw [List.range_succ_eq_map]
          simp only [List.map_cons, List.map_map, List.sum_cons]
          have hfirst : prefI (x :: xs) 0 * ((y :: ys).getD 0 0 - (y :: ys).getD 1 0) =
              x * (y - ys.getD 0 0) := by simp [prefI]
          rw [hfirst]
          have htailmap :
              ((List.map ((fun s => prefI (x :: xs) s *
                  ((y :: ys).getD s 0 - (y :: ys).getD (s+1) 0)) ∘ Nat.succ) (List.range xs.length)).sum) =
              ((List.range xs.length).map fun s =>
                (x + prefI xs s) * (ys.getD s 0 - ys.getD (s+1) 0)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro s hs
            simp [prefI]
          rw [htailmap]
          rw [sum_add_mul_range]
          have hdiff : ((List.range xs.length).map fun s => ys.getD s 0 - ys.getD (s+1) 0).sum = ys.getD 0 0 := by
            rw [htail]
            exact hsumdiff
          rw [hdiff]
          simp only [List.zip_cons_cons, List.map_cons, List.sum_cons] at hih ⊢
          rw [hih]
          grind


private def dot (u v : List Nat) : Nat := ((u.zip v).map fun p => p.1 * p.2).sum


private theorem dot_of_pairs (M : List (Nat × Nat)) (f g : Nat → Nat) :
    (M.map fun p => f p.1 * g p.2).sum =
      dot (M.map fun p => f p.1) (M.map fun p => g p.2) := by
  induction M with
  | nil => simp [dot]
  | cons p ps ih => simp [dot, ih]


private theorem dot_comm (u v : List Nat) : dot u v = dot v u := by
  induction u generalizing v with
  | nil => cases v <;> simp [dot]
  | cons a as ih =>
      cases v with
      | nil => simp [dot]
      | cons b bs =>
          change a * b + dot as bs = b * a + dot bs as
          rw [ih bs, Nat.mul_comm a b]


private theorem dot_cast (u v : List Nat) :
    (((u.map fun x : Nat => (x : Int)).zip (v.map fun x : Nat => (x : Int))).map fun p => p.1 * p.2).sum =
      (dot u v : Int) := by
  revert v
  induction u with
  | nil => intro v; cases v <;> simp [dot]
  | cons a as ih =>
      intro v
      cases v with
      | nil => simp [dot]
      | cons b bs =>
          simp only [List.map_cons, List.zip_cons_cons, List.map_cons, List.sum_cons]
          rw [ih bs]
          simp [dot]


private theorem sum_range_le_int (f g : Nat → Int) (n : Nat) (h : ∀ s, s < n → f s ≤ g s) :
    ((List.range n).map f).sum ≤ ((List.range n).map g).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hih := ih (by intro s hs; exact h s (by omega))
      have hn := h n (by omega)
      simp only [List.range_succ, List.map_append, List.sum_append, List.map_cons, List.map_nil,
        List.sum_cons, List.sum_nil]
      omega


private theorem dot_take_le (u v : List Nat) (n : Nat) (hnu : n ≤ u.length) (hnv : n ≤ v.length) :
    dot (u.take n) (v.take n) ≤ dot u v := by
  induction n generalizing u v with
  | zero => simp [dot]
  | succ n ih =>
      cases u with
      | nil => simp at hnu
      | cons a as =>
          cases v with
          | nil => simp at hnv
          | cons b bs =>
              simp only [List.length_cons] at hnu hnv
              change a * b + dot (as.take n) (bs.take n) ≤ a * b + dot as bs
              have hih := ih as bs (by omega) (by omega)
              omega


private theorem dot_le_right (w u v : List Nat) (hw : w.length = u.length) (huv : u.length = v.length)
    (h : ∀ s, s < u.length → u.getD s 0 ≤ v.getD s 0) : dot w u ≤ dot w v := by
  induction w generalizing u v with
  | nil => simp [dot]
  | cons a ws ih =>
      cases u with
      | nil => simp at hw
      | cons b bs =>
          cases v with
          | nil => simp at huv
          | cons c cs =>
              simp only [List.length_cons] at hw huv
              change a * b + dot ws bs ≤ a * c + dot ws cs
              have h0 : b ≤ c := by
                have := h 0 (by simp)
                simpa using this
              have htail : ∀ s, s < bs.length → bs.getD s 0 ≤ cs.getD s 0 := by
                intro s hs
                have := h (s+1) (by simp; omega)
                simpa [List.getD_cons_succ] using this
              have hi := ih bs cs (by omega) (by omega) htail
              have hmul : a * b ≤ a * c := Nat.mul_le_mul_left a h0
              omega


private theorem le_of_lt_nat {a b : Nat} (h : a < b) : a ≤ b := Nat.le_of_lt h


private theorem getD_pairwise_of_inc (α : List Nat) (hα : Desc α) (is : List Nat)
    (hinc : is.Pairwise (fun a b => a < b)) (hlt : ∀ i ∈ is, i < α.length) :
    Desc (is.map fun i => α.getD i 0) := by
  induction is with
  | nil => simp [Desc]
  | cons i rest ih =>
      have hhead : ∀ j ∈ rest, i < j := (List.pairwise_cons.mp hinc).1
      have htinc := (List.pairwise_cons.mp hinc).2
      have htlt : ∀ j ∈ rest, j < α.length := by intro j hj; exact hlt j (by simp [hj])
      have htail := ih htinc htlt
      apply List.Pairwise.cons
      · intro v hv
        rw [List.mem_map] at hv
        rcases hv with ⟨j, hj, rfl⟩
        have hij : i ≤ j := le_of_lt_nat (hhead j hj)
        exact desc_get_le hα hij (hlt j (by simp [hj]))
      · simpa [Desc] using htail


private theorem getD_map_nat_of_lt (l : List Nat) (f : Nat → Nat) (k : Nat) (h : k < l.length) :
    (l.map f).getD k 0 = f (l.getD k 0) := by
  rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_map]
  cases hopt : l[k]? with
  | none =>
      have hlenle : l.length ≤ k := List.getElem?_eq_none_iff.mp hopt
      omega
  | some v => simp [hopt, List.getD_eq_getElem?_getD]


private theorem getD_map_cast (l : List Nat) (k : Nat) :
    (l.map fun x : Nat => (x : Int)).getD k 0 = (l.getD k 0 : Int) := by
  induction l generalizing k with
  | nil => simp
  | cons a as ih =>
      cases k with
      | zero => simp
      | succ k =>
          rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_map]
          cases hopt : as[k]? <;> simp [hopt, ih k]


private theorem rearrange_core (α β : List Nat) (hα : Desc α) (hβ : Desc β)
    (M : List (Nat × Nat))
    (hinc : (M.map Prod.fst).Pairwise (fun a b => a < b))
    (hlt1 : ∀ p ∈ M, p.1 < α.length)
    (hsecond : (M.map Prod.snd).Nodup)
    (hlt2 : ∀ p ∈ M, p.2 < β.length) :
    (M.map fun p => α.getD p.1 0 * β.getD p.2 0).sum ≤ dot α β := by
  let is := M.map Prod.fst
  let js := M.map Prod.snd
  let uNat := is.map fun i => α.getD i 0
  let vNat := js.map fun j => β.getD j 0
  let uI := uNat.map fun x : Nat => (x : Int)
  let vI := vNat.map fun x : Nat => (x : Int)
  let βA := (β.take M.length).map fun x : Nat => (x : Int)
  let p := M.length
  have hislen : is.length = p := by simp [is, p]
  have hjslen : js.length = p := by simp [js, p]
  have hisnd : is.Nodup := hinc.imp (fun {a b} hlt => Nat.ne_of_lt hlt)
  have hltis : ∀ i ∈ is, i < α.length := by
    intro i hi
    rcases List.mem_map.mp hi with ⟨q, hq, rfl⟩
    exact hlt1 q hq
  have hltjs : ∀ j ∈ js, j < β.length := by
    intro j hj
    rcases List.mem_map.mp hj with ⟨q, hq, rfl⟩
    exact hlt2 q hq
  have hpα : p ≤ α.length := by
    rw [← hislen]
    exact nodup_length_le_of_lt is hisnd hltis
  have hpβ : p ≤ β.length := by
    rw [← hjslen]
    exact nodup_length_le_of_lt js hsecond hltjs
  have huld : Desc uNat := getD_pairwise_of_inc α hα is hinc hltis
  have hdotL : (M.map fun p => α.getD p.1 0 * β.getD p.2 0).sum = dot uNat vNat := by
    have h0 := dot_of_pairs M (fun i => α.getD i 0) (fun j => β.getD j 0)
    change (M.map fun p => α.getD p.1 0 * β.getD p.2 0).sum =
      dot ((M.map Prod.fst).map fun i => α.getD i 0) ((M.map Prod.snd).map fun j => β.getD j 0)
    rw [List.map_map, List.map_map]
    exact h0
  have hlenUI : uI.length = vI.length := by simp [uI, vI, uNat, vNat, hislen, hjslen]
  have hdu_nonneg : ∀ s, s < p → (0 : Int) ≤ uI.getD s 0 - uI.getD (s+1) 0 := by
    intro s hs
    have hslt : s < uNat.length := by simpa [uNat, is, hislen] using hs
    by_cases hslast : s + 1 < uNat.length
    · have hrel := (List.pairwise_iff_getElem.mp huld) s (s+1) hslt hslast (by omega)
      have hleN : uNat.getD (s+1) 0 ≤ uNat.getD s 0 := by
        rw [← List.getElem_eq_getD (l := uNat) (i := s+1) (h := hslast) (fallback := 0)]
        rw [← List.getElem_eq_getD (l := uNat) (i := s) (h := hslt) (fallback := 0)]
        exact hrel
      have hleI : uI.getD (s+1) 0 ≤ uI.getD s 0 := by
        rw [show uI = uNat.map fun x : Nat => (x : Int) by rfl]
        rw [getD_map_cast, getD_map_cast]
        exact_mod_cast hleN
      omega
    · have hslena : uNat.length = p := by simp [uNat, is, p]
      have hseq : s + 1 = uNat.length := by dsimp [p] at hslena ⊢; omega
      have hlastval : uI.getD (s+1) 0 = 0 := by simp [uI, hseq]
      rw [hlastval]
      rw [show uI = uNat.map fun x : Nat => (x : Int) by rfl, getD_map_cast]
      have h0int : (0 : Int) ≤ (uNat.getD s 0 : Int) := by exact_mod_cast Nat.zero_le (uNat.getD s 0)
      omega
  have hpref_le : ∀ s, s < p → prefI vI s ≤ prefI βA s := by
    intro s hs
    have hsltjs : s + 1 ≤ js.length := by omega
    have hndtake : (js.take (s+1)).Nodup := hsecond.sublist (List.take_sublist _ _)
    have hbe := topk_sum β hβ (js.take (s+1)) hndtake (by
      intro j hj
      exact hltjs j (List.mem_of_mem_take hj))
    have hprefV : prefI vI s = ((vNat.take (s+1)).sum : Int) := by
      dsimp [prefI, vI]
      rw [← List.map_take]
      simpa using (sum_cast_map (vNat.take (s+1)) (fun x => x))
    have hprefA : prefI βA s = ((β.take (s+1)).sum : Int) := by
      dsimp [prefI, βA]
      rw [← List.map_take, List.take_take]
      have hmin : min (s + 1) M.length = s + 1 := by dsimp [p] at hs ⊢; omega
      rw [hmin]
      simpa using (sum_cast_map (β.take (s+1)) (fun x => x))
    rw [hprefV, hprefA]
    have hbt : (js.take (s+1)).length = s + 1 := by
      exact List.length_take_of_le hsltjs
    have hbe' : (vNat.take (s+1)).sum ≤ (β.take (s+1)).sum := by
      have hvtake : vNat.take (s+1) = (js.take (s+1)).map (fun j => β.getD j 0) := by
        dsimp [vNat]
        rw [List.map_take]
      rw [hvtake]
      simpa [hbt] using hbe
    exact_mod_cast hbe'
  have hsumle : ((List.range p).map fun s => prefI vI s * (uI.getD s 0 - uI.getD (s+1) 0)).sum ≤
      ((List.range p).map fun s => prefI βA s * (uI.getD s 0 - uI.getD (s+1) 0)).sum := by
    apply sum_range_le_int
    intro s hs
    exact Int.mul_le_mul_of_nonneg_right (hpref_le s hs) (hdu_nonneg s hs)
  have habel1 := abel_int vI uI hlenUI.symm
  have hlenA : βA.length = uI.length := by
    have hmin : min M.length β.length = M.length := Nat.min_eq_left (by simpa [p] using hpβ)
    simp [βA, uI, uNat, is, p, hmin]
  have habel2 := abel_int βA uI hlenA
  have hlenvI : vI.length = p := by simp [vI, vNat, js, p]
  have hlenuI : uI.length = p := by simp [uI, uNat, is, p]
  have hdotIuv_le : ((uI.zip vI).map fun q => q.1 * q.2).sum ≤
      ((βA.zip uI).map fun q => q.1 * q.2).sum := by
    have hcommI : ((uI.zip vI).map fun q => q.1 * q.2).sum =
        ((vI.zip uI).map fun q => q.1 * q.2).sum := by
      rw [dot_cast uNat vNat, dot_cast vNat uNat, dot_comm uNat vNat]
    rw [hcommI, habel1, habel2, hlenvI, hlenA, hlenuI]
    exact hsumle
  have hdotBAu : dot (β.take p) uNat ≤ dot (β.take p) (α.take p) := by
    apply dot_le_right
    · rw [List.length_take, Nat.min_eq_left (by simpa [p] using hpβ)]
      simp [uNat, is, p]
    · simp [uNat, is, p, hpα]
    · intro s hs
      have hsis : s < is.length := by simpa [uNat] using hs
      have hsp : s < p := by simpa [hislen] using hsis
      have hispos : s ≤ is.getD s 0 := idx_ge_pos is hinc s hsis
      have hltidx : is.getD s 0 < α.length := hltis (is.getD s 0) (getD_mem hsis)
      have hleN : α.getD (is.getD s 0) 0 ≤ α.getD s 0 := desc_get_le hα hispos hltidx
      have hgetu : uNat.getD s 0 = α.getD (is.getD s 0) 0 := by
        dsimp [uNat]
        exact getD_map_nat_of_lt is (fun i => α.getD i 0) s hsis
      have hgeta : (α.take p).getD s 0 = α.getD s 0 := getD_take_eq (by omega)
      rw [hgetu, hgeta]
      exact hleN
  have hdotprefix : dot (β.take p) (α.take p) ≤ dot β α := dot_take_le β α p hpβ hpα
  have hcastBA : ((βA.zip uI).map fun q => q.1 * q.2).sum = (dot (β.take p) uNat : Int) := by
    simpa [βA] using dot_cast (β.take p) uNat
  have hcastUV : ((uI.zip vI).map fun q => q.1 * q.2).sum = (dot uNat vNat : Int) := dot_cast uNat vNat
  rw [hcastUV, hcastBA] at hdotIuv_le
  have hle1 : dot uNat vNat ≤ dot (β.take p) uNat := by exact_mod_cast hdotIuv_le
  have hdotprefix' : dot (β.take p) (α.take p) ≤ dot α β := by
    simpa [dot_comm] using hdotprefix
  have hleNat : dot uNat vNat ≤ dot α β := Nat.le_trans hle1 (Nat.le_trans hdotBAu hdotprefix')
  rw [hdotL]
  exact hleNat


private theorem mem_sortedPairs (xs : List Nat) (k : Nat) (hk : k ∈ xs.eraseDups) :
    (xs.count k, k) ∈ sortedPairs xs := by
  rw [(sortedPairs_perm xs).mem_iff]
  exact List.mem_map.mpr ⟨k, hk, rfl⟩


private theorem rank_get (xs : List Nat) (k : Nat) (hk : k ∈ xs.eraseDups) :
    (sortedPairs xs).getD ((sortedPairs xs).idxOf (xs.count k, k)) (0, 0) = (xs.count k, k) := by
  have hmem := mem_sortedPairs xs k hk
  have hlt := List.idxOf_lt_length_of_mem hmem
  have hg := List.getElem_idxOf hlt
  rw [List.getElem_eq_getD (fallback := (0, 0))] at hg
  exact hg


private theorem rank_inj (xs : List Nat) (k1 k2 : Nat)
    (hk1 : k1 ∈ xs.eraseDups) (hk2 : k2 ∈ xs.eraseDups)
    (h : (sortedPairs xs).idxOf (xs.count k1, k1) = (sortedPairs xs).idxOf (xs.count k2, k2)) :
    k1 = k2 := by
  have g1 := rank_get xs k1 hk1
  have g2 := rank_get xs k2 hk2
  rw [h] at g1
  have heq : (xs.count k1, k1) = (xs.count k2, k2) := g1.symm.trans g2
  exact congrArg Prod.snd heq


private theorem fanout_eq_eraseDups (a b : List Nat) :
    fanout a b = (a.eraseDups.map fun k => a.count k * b.count k).sum := by
  dsimp [fanout]
  exact sum_count_mul_of_nodup a (fun k => b.count k) a.eraseDups (nodup_eraseDups a)
    (by intro x hx; simp [List.mem_eraseDups, hx])


private theorem eraseDups_filter_sum (a b : List Nat) :
    (a.eraseDups.map fun k => a.count k * b.count k).sum =
      ((a.eraseDups.filter fun k => k ∈ b).map fun k => a.count k * b.count k).sum := by
  induction a.eraseDups with
  | nil => simp
  | cons k ks ih =>
      by_cases hk : k ∈ b
      · simp [hk, ih]
      · simp [hk, ih, List.count_eq_zero.mpr hk]


private theorem nodup_map_of_inj (l : List Nat) (hnd : l.Nodup) (f : Nat → Nat)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hnd' := List.pairwise_cons.mp hnd
      have hih := ih hnd'.2 (by
        intro x hx y hy heq
        exact hinj x (by simp [hx]) y (by simp [hy]) heq)
      apply List.Pairwise.cons
      · intro y hy
        rw [List.mem_map] at hy
        rcases hy with ⟨b, hb, rfl⟩
        intro heq
        have hab : a = b := hinj a (by simp) b (by simp [hb]) heq
        exact hnd'.1 b hb hab
      · exact hih


private theorem getD_map_fst (l : List (Nat × Nat)) (i : Nat) :
    (l.map Prod.fst).getD i 0 = (l.getD i (0, 0)).1 := by
  induction l generalizing i with
  | nil => simp
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ i =>
          rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_map]
          cases hopt : ps[i]? <;> simp [hopt]


private theorem mergeSort_by_fst (l : List (Nat × Nat)) :
    l.mergeSort (fun p q : Nat × Nat => p.1 ≤ q.1) |>.Perm l :=
  List.mergeSort_perm l (fun p q : Nat × Nat => p.1 ≤ q.1)


private theorem ascending_fst (l : List (Nat × Nat)) (hnd : (l.map Prod.fst).Nodup) :
    ((l.mergeSort (fun p q : Nat × Nat => p.1 ≤ q.1)).map Prod.fst).Pairwise (fun a b => a < b) := by
  have hp := mergeSort_by_fst l
  have hpair0 := List.pairwise_mergeSort (le := fun p q : Nat × Nat => p.1 ≤ q.1)
    (trans := by
      intro p q r hpq hqr
      simp only [decide_eq_true_eq] at hpq hqr ⊢
      exact Nat.le_trans hpq hqr)
    (total := by
      intro p q
      simp only [decide_eq_true_eq, Bool.or_eq_true]
      omega) l
  have hpair : (l.mergeSort (fun p q : Nat × Nat => p.1 ≤ q.1)).Pairwise
      (fun p q : Nat × Nat => p.1 ≤ q.1) := by
    simpa only [decide_eq_true_eq] using hpair0
  have hasc : ((l.mergeSort (fun p q : Nat × Nat => p.1 ≤ q.1)).map Prod.fst).Pairwise
      (fun a b : Nat => a ≤ b) := hpair.map Prod.fst (by intro p q hpq; exact hpq)
  exact pairwise_le_nodup_strict hasc ((hp.map Prod.fst).nodup_iff.mpr hnd)


theorem SortedDegreeBound_sound :
    TinylambOpt.ResearchGoals.SearchBounds.SortedDegreeBound.Goal := by
  intro a b
  let common := a.eraseDups.filter fun k => k ∈ b
  let R := common.map fun k => ((sortedPairs a).idxOf (a.count k, k), (sortedPairs b).idxOf (b.count k, k))
  let M := R.mergeSort (fun p q : Nat × Nat => p.1 ≤ q.1)
  let α := (counts a).mergeSort.reverse
  let β := (counts b).mergeSort.reverse
  have hα : Desc α := by dsimp [α]; exact counts_desc a
  have hβ : Desc β := by dsimp [β]; exact counts_desc b
  have hcommon_nd : common.Nodup := by
    dsimp [common]
    exact (List.filter_sublist (p := fun k => k ∈ b) (l := a.eraseDups)).nodup (nodup_eraseDups a)
  have hRnd1 : (R.map Prod.fst).Nodup := by
    dsimp [R]
    rw [List.map_map]
    apply nodup_map_of_inj common hcommon_nd
    intro k1 hk1 k2 hk2 heq
    apply rank_inj a k1 k2
    · exact (List.mem_filter.mp (by simpa only [common] using hk1)).1
    · exact (List.mem_filter.mp (by simpa only [common] using hk2)).1
    · exact heq
  have hRnd2 : (R.map Prod.snd).Nodup := by
    dsimp [R]
    rw [List.map_map]
    apply nodup_map_of_inj common hcommon_nd
    intro k1 hk1 k2 hk2 heq
    apply rank_inj b k1 k2
    · have hm := List.mem_filter.mp (by simpa only [common] using hk1)
      exact (List.mem_eraseDups (a := k1) (l := b)).mpr (by simpa using hm.2)
    · have hm := List.mem_filter.mp (by simpa only [common] using hk2)
      exact (List.mem_eraseDups (a := k2) (l := b)).mpr (by simpa using hm.2)
    · exact heq
  have hMperm : M.Perm R := by simpa [M] using mergeSort_by_fst R
  have hMnd1 : (M.map Prod.fst).Nodup := (hMperm.map Prod.fst).nodup_iff.mpr hRnd1
  have hMnd2 : (M.map Prod.snd).Nodup := (hMperm.map Prod.snd).nodup_iff.mpr hRnd2
  have hMinc : (M.map Prod.fst).Pairwise (fun a b => a < b) := by
    dsimp [M]
    exact ascending_fst R hRnd1
  have hlt1 : ∀ p ∈ M, p.1 < α.length := by
    intro p hp
    have hpR : p ∈ R := hMperm.subset hp
    rcases List.mem_map.mp hpR with ⟨k, hk, rfl⟩
    have hm := List.mem_filter.mp (by simpa only [common] using hk)
    have hlen : (sortedPairs a).length = α.length := by
      have h := congrArg List.length (sortedPairs_fst_eq_counts a)
      simpa [α] using h
    rw [← hlen]
    exact List.idxOf_lt_length_of_mem (mem_sortedPairs a k hm.1)
  have hlt2 : ∀ p ∈ M, p.2 < β.length := by
    intro p hp
    have hpR : p ∈ R := hMperm.subset hp
    rcases List.mem_map.mp hpR with ⟨k, hk, rfl⟩
    have hlen : (sortedPairs b).length = β.length := by
      have h := congrArg List.length (sortedPairs_fst_eq_counts b)
      simpa [β] using h
    rw [← hlen]
    exact List.idxOf_lt_length_of_mem (mem_sortedPairs b k
      ((List.mem_eraseDups (a := k) (l := b)).mpr
        (by have hm := List.mem_filter.mp (by simpa only [common] using hk); simpa using hm.2)))
  have hsumM : (M.map fun p => α.getD p.1 0 * β.getD p.2 0).sum = fanout a b := by
    have hpermSum : (M.map fun p => α.getD p.1 0 * β.getD p.2 0).sum =
        (R.map fun p => α.getD p.1 0 * β.getD p.2 0).sum :=
      perm_sum (hMperm.map (fun p => α.getD p.1 0 * β.getD p.2 0))
    rw [hpermSum]
    have hpoint : ∀ k ∈ common,
        α.getD ((sortedPairs a).idxOf (a.count k, k)) 0 *
          β.getD ((sortedPairs b).idxOf (b.count k, k)) 0 = a.count k * b.count k := by
      intro k hk
      have hkc := List.mem_filter.mp (by simpa only [common] using hk)
      have hka : k ∈ a.eraseDups := hkc.1
      have hkb : k ∈ b.eraseDups := by
        have hpos : k ∈ b := by simpa using hkc.2
        exact (List.mem_eraseDups (a := k) (l := b)).mpr hpos
      have ga := rank_get a k hka
      have gb := rank_get b k hkb
      have ga' : α.getD ((sortedPairs a).idxOf (a.count k, k)) 0 = a.count k := by
        dsimp [α]
        rw [← sortedPairs_fst_eq_counts]
        rw [getD_map_fst]
        exact congrArg Prod.fst ga
      have gb' : β.getD ((sortedPairs b).idxOf (b.count k, k)) 0 = b.count k := by
        dsimp [β]
        rw [← sortedPairs_fst_eq_counts]
        rw [getD_map_fst]
        exact congrArg Prod.fst gb
      rw [ga', gb']
    have hmapEq : (R.map fun p => α.getD p.1 0 * β.getD p.2 0) =
        (common.map fun k => a.count k * b.count k) := by
      dsimp [R]
      rw [List.map_map]
      apply List.map_congr_left
      intro k hk
      exact hpoint k (by simpa only [common] using hk)
    rw [hmapEq]
    rw [← eraseDups_filter_sum a b, fanout_eq_eraseDups]
  rw [← hsumM]
  exact rearrange_core α β hα hβ M hMinc hlt1 hMnd2 hlt2


end TinylambOpt.ResearchProofs.Wave7SearchBounds
