import TinylambOpt.ResearchGoals.ExecutionStrategies.SearchBounds

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.SearchBounds

open TinylambOpt.ResearchGoals.SearchBounds

/-!
# Proofs for the eight execution-strategy search bounds

Goal file: `TinylambOpt.ResearchGoals.ExecutionStrategies.SearchBounds`.

* `CollisionBound` — Cauchy–Schwarz over the shared key support
  `(a ++ b).eraseDups`.  `fanout` and `secondMoment` are rewritten as
  frequency-weighted sums over that support (`sum_count_mul_of_nodup`),
  then a sum-of-squares identity (`cs_identity`) gives the inequality.
* `SortedDegreeBound` — rearrangement inequality for the sorted frequency
  spectra.
* `TriangleBound` — double counting: group triangles by their `t`-edge,
  Cauchy–Schwarz, then bound the squared path counts by edge products.
* `FrequentCandidates` — Misra–Gries invariant on the `HashMap` fold.
* `CappedJoin` — `remaining` is shown equal to `k - Σ freq.getD` by
  induction, and `histogram b` counts equal `b.count`.
* `GroupSumFence` — each block's sum is bounded by `max 0 upper * n`;
  summing preserves the strict cutoff.
* `UnseenTopK` — if `k` were absent from every shard's top list, each
  shard's count bound would sum below `cutoff`, contradicting the
  hypothesis.
* `PredicateExchange` — pure `Nat` arithmetic after expanding
  `countP`/`length` bounds.
-/

/-! ### List helpers -/

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

private theorem sum_const_zero (l : List Nat) : (l.map fun _ => (0:Nat)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem count_succ (k x : Nat) (xs : List Nat) :
    List.count k (x :: xs) = (if x == k then xs.count k + 1 else xs.count k) := by
  rw [List.count_cons]
  by_cases h : x = k
  · subst h
    simp
  · have hb : (x == k) = false := beq_eq_false_iff_ne.mpr h
    simp [hb]

/-- Adding one occurrence of `x` to `xs` adds `f x` to the support sum,
provided every element of `xs` lies in the nodup support `ks` containing `x`. -/
private theorem key_sum (ks : List Nat) (hnd : ks.Nodup) (x : Nat) (hx : x ∈ ks)
    (c f : Nat → Nat) :
    (ks.map fun k => (if x == k then c k + 1 else c k) * f k).sum =
      (ks.map fun k => c k * f k).sum + f x := by
  induction ks with
  | nil => simp at hx
  | cons k ks ih =>
      have hcons := List.nodup_cons.mp hnd
      have hndtail : ks.Nodup := hcons.2
      have hknot : k ∉ ks := hcons.1
      rcases List.mem_cons.mp hx with hkx | hxks
      · subst x
        have hmap : (ks.map fun j => (if k == j then c j + 1 else c j) * f j) =
            ks.map fun j => c j * f j := by
          apply List.map_congr_left
          intro j hj
          have hb : (k == j) = false := by
            rw [beq_eq_false_iff_ne]
            intro h
            subst j
            exact hknot hj
          simp [hb]
        have hkk : (k == k) = true := beq_iff_eq.mpr rfl
        simp only [List.map_cons, List.sum_cons]
        rw [hmap, hkk]
        simp
        rw [Nat.add_mul, Nat.one_mul]
        exact Nat.add_right_comm _ _ _
      · have hxk : x ≠ k := by
          intro h
          subst x
          exact hknot hxks
        have hb : (x == k) = false := beq_eq_false_iff_ne.mpr hxk
        have hih := ih hndtail hxks
        simp only [List.map_cons, List.sum_cons]
        rw [hih]
        simp [hb]
        omega

/-- Grouping a list sum by a nodup key support:
`Σ_{x∈xs} f x = Σ_{k∈ks} (xs.count k) * f k`. -/
private theorem sum_count_mul_of_nodup (xs : List Nat) (f : Nat → Nat) :
    ∀ (ks : List Nat), ks.Nodup → (∀ x ∈ xs, x ∈ ks) →
      (xs.map f).sum = (ks.map fun k => xs.count k * f k).sum := by
  induction xs with
  | nil =>
      intro ks hnd hsub
      have hmap : (ks.map fun k => ([] : List Nat).count k * f k) = ks.map fun _ => (0:Nat) := by
        apply List.map_congr_left
        intro k hk
        simp
      rw [hmap, sum_const_zero]
      rfl
  | cons x xs ih =>
      intro ks hnd hsub
      have hxks : x ∈ ks := hsub x (by simp)
      have htail : ∀ z ∈ xs, z ∈ ks := by intro z hz; exact hsub z (by simp [hz])
      have hih := ih ks hnd htail
      have hkey := key_sum ks hnd x hxks (fun k => xs.count k) f
      have hmap : (ks.map fun k => (x :: xs).count k * f k) =
          ks.map fun k => (if x == k then xs.count k + 1 else xs.count k) * f k := by
        apply List.map_congr_left
        intro k hk
        rw [count_succ]
      rw [hmap, hkey]
      simp only [List.map_cons, List.sum_cons]
      rw [hih]
      omega

/-! ### Cauchy–Schwarz for lists of natural pairs (sum-of-squares identity) -/

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

theorem sound_CollisionBound : CollisionBound.Goal := by
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
  have ha2 := sum_count_mul_of_nodup a (fun k => a.count k) ks hnd hsuba
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
  rw [ha, ha2, hb']
  simpa [Nat.pow_two] using hcs

/-! ### 5. CappedJoin -/

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
          simp only [List.map_cons, List.sum_cons, remaining]
          rw [ih (Nat.succ k - freq.getD x 0)]
          omega

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
          simp [hbeq, hh, List.count_append, List.count_singleton]
      rw [List.foldl_cons]
      have hrec := ih (xs ++ [y]) (h.insert y (h.getD y 0 + 1)) hins k
      simpa [List.count_append] using hrec

private theorem histogram_getD (b : List Nat) (x : Nat) : (histogram b).getD x 0 = b.count x := by
  have h := foldl_hist b [] {} (by intro k; simp) x
  simpa [histogram] using h

theorem sound_CappedJoin : CappedJoin.Goal := by
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

/-! ### 6. GroupSumFence -/

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

theorem sound_GroupSumFence : GroupSumFence.Goal := by
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

theorem sound_UnseenTopK : UnseenTopK.Goal := by
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

theorem sound_PredicateExchange : PredicateExchange.Goal := by
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


/-! ### 2. TriangleBound -/

private theorem nodup_filter {α : Type} {l : List α} (p : α → Bool) (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have h' := List.nodup_cons.mp h
      by_cases hp : p x = true
      · have e : ((x::xs).filter p) = x :: (xs.filter p) := by simp [hp]
        rw [e]
        have hxin : x ∉ xs := h'.1
        have hnotin : x ∉ xs.filter p := by
          intro hmem
          exact hxin (List.mem_filter.mp hmem).1
        exact List.nodup_cons.mpr ⟨hnotin, ih h'.2⟩
      · have hpx : p x = false := by cases h2 : p x <;> simp_all
        have e : ((x::xs).filter p) = xs.filter p := by simp [hpx]
        rw [e]
        exact ih h'.2

private theorem nodup_map_injOn {α β : Type} {l : List α} (f : α → β) (h : l.Nodup)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have h' := List.nodup_cons.mp h
      simp only [List.map_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rw [List.mem_map] at hmem
        obtain ⟨a, ha, hfa⟩ := hmem
        have hax : a = x := hinj a (by simp [ha]) x (by simp) hfa
        subst hax
        exact h'.1 (by simpa using ha)
      · exact ih h'.2 (fun a ha b hb => hinj a (by simp [ha]) b (by simp [hb]))

private theorem filter_and_length_le {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter fun a => p a && q a).length ≤ (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · by_cases hq : q x = true
        · have e1 : ((x::xs).filter fun a => p a && q a) = x :: (xs.filter fun a => p a && q a) := by
            simp [hp, hq]
          have e2 : ((x::xs).filter p) = x :: (xs.filter p) := by simp [hp]
          rw [e1, e2, List.length_cons, List.length_cons]
          have := ih
          omega
        · have hqx : q x = false := by cases h : q x <;> simp_all
          have e1 : ((x::xs).filter fun a => p a && q a) = xs.filter fun a => p a && q a := by
            simp [hp, hqx]
          have e2 : ((x::xs).filter p) = x :: (xs.filter p) := by simp [hp]
          rw [e1, e2, List.length_cons]
          have := ih
          omega
      · have hpx : p x = false := by cases h : p x <;> simp_all
        have e1 : ((x::xs).filter fun a => p a && q a) = xs.filter fun a => p a && q a := by
          simp [hpx]
        have e2 : ((x::xs).filter p) = xs.filter p := by simp [hpx]
        rw [e1, e2]
        exact ih

private theorem sum_mono {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have h1 := h x (by simp)
      have h2 := ih (fun y hy => h y (by simp [hy]))
      omega

private theorem sum_const {α : Type} (l : List α) (c : Nat) :
    (l.map fun _ => c).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, List.length_cons, Nat.add_mul, Nat.one_mul]
      omega

private theorem nodup_flatMap_disjoint {α β : Type} {l : List α} {f : α → List β}
    (hl : l.Nodup) (hf : ∀ a ∈ l, (f a).Nodup)
    (hcross : ∀ a ∈ l, ∀ b ∈ l, a ≠ b → ∀ x ∈ f a, ∀ y ∈ f b, x ≠ y) :
    (l.flatMap f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have h' := List.nodup_cons.mp hl
      have hxs : xs.Nodup := h'.2
      have hxnot : x ∉ xs := h'.1
      rw [List.flatMap_cons, List.nodup_append]
      refine ⟨hf x (by simp), ?_, ?_⟩
      · exact ih hxs (fun a ha => hf a (by simp [ha]))
          (fun a ha b hb hne u hu v hv =>
            hcross a (by simp [ha]) b (by simp [hb]) hne u hu v hv)
      · intro a ha b hb h
        rw [List.mem_flatMap] at hb
        obtain ⟨c, hc, hbc⟩ := hb
        exact (hcross x (by simp) c (by simp [hc]) (fun hh => hxnot (hh.symm ▸ hc)) a ha b hbc) h

private def pairList {α β : Type} (xs : List α) (ys : List β) : List (α × β) :=
  xs.flatMap fun a => ys.map fun b => (a, b)

private theorem mem_pairList {α β : Type} {xs : List α} {ys : List β} {p : α × β} :
    p ∈ pairList xs ys ↔ p.1 ∈ xs ∧ p.2 ∈ ys := by
  unfold pairList
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨a, ha, hmem⟩
    rw [List.mem_map] at hmem
    obtain ⟨b, hb, hab⟩ := hmem
    have h1 : a = p.1 := congrArg Prod.fst hab
    have h2 : b = p.2 := congrArg Prod.snd hab
    subst h1
    subst h2
    exact ⟨ha, hb⟩
  · rintro ⟨ha, hb⟩
    refine ⟨p.1, ha, ?_⟩
    rw [List.mem_map]
    exact ⟨p.2, hb, rfl⟩

private theorem length_pairList {α β : Type} (xs : List α) (ys : List β) :
    (pairList xs ys).length = xs.length * ys.length := by
  unfold pairList
  rw [List.length_flatMap]
  have hmap : (xs.map fun a => (ys.map fun b => (a, b)).length) =
      xs.map fun _ => ys.length := by
    apply List.map_congr_left
    intro a ha
    rw [List.length_map]
  rw [hmap]
  exact sum_const xs ys.length

/-- `compE s t e es` holds when the `s`-edge `es = (y', z)` starts where the
`r`-edge `e = (x, y)` ends and `(z, x)` closes a triangle in `t`. -/
private def compE (s t : Edges) (e : Nat × Nat) : (Nat × Nat) → Bool :=
  fun es => e.2 == es.1 && t.contains (es.2, e.1)

private def outE (s : Edges) (e : Nat × Nat) : Nat :=
  (s.filter fun es => e.2 == es.1).length

private def cE (s t : Edges) (e : Nat × Nat) : Nat :=
  (s.filter (compE s t e)).length

private theorem cE_le_outE (s t : Edges) (e : Nat × Nat) :
    cE s t e ≤ outE s e := by
  unfold cE outE compE
  exact filter_and_length_le s (fun es => e.2 == es.1) (fun es => t.contains (es.2, e.1))

private theorem triangles_length (r s t : Edges) :
    (triangles r s t).length = (r.map fun e => cE s t e).sum := by
  unfold triangles cE compE
  rw [List.length_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro e he
  obtain ⟨x, y⟩ := e
  rw [List.length_map]
  rfl

/-- All `(e, es_z, es_w)` with `e ∈ r`, `es_z` completing `e` through `s`/`t`,
and `es_w` any `s`-edge leaving `e`'s head. -/
private def qlistT (r s t : Edges) :
    List ((Nat × Nat) × (Nat × Nat) × (Nat × Nat)) :=
  r.flatMap fun e =>
    (s.filter (compE s t e)).flatMap fun esz =>
      (s.filter fun es => e.2 == es.1).map fun esw => (e, esz, esw)

private theorem qlist_length (r s t : Edges) :
    (qlistT r s t).length = (r.map fun e => cE s t e * outE s e).sum := by
  have hFe : ∀ e : Nat × Nat,
      ((s.filter (compE s t e)).flatMap fun esz =>
        (s.filter fun es => e.2 == es.1).map fun esw => (e, esz, esw)).length =
        cE s t e * outE s e := by
    intro e
    unfold cE outE
    rw [List.length_flatMap]
    have hmap : ((s.filter (compE s t e)).map fun esz =>
        ((s.filter fun es => e.2 == es.1).map fun esw => (e, esz, esw)).length) =
        (s.filter (compE s t e)).map fun _ =>
          (s.filter fun es => e.2 == es.1).length := by
      apply List.map_congr_left
      intro esz hesz
      rw [List.length_map]
    rw [hmap, sum_const]
  unfold qlistT
  rw [List.length_flatMap]
  simp only [hFe]

private theorem qlist_nodup (r s t : Edges) (hr : r.Nodup) (hs : s.Nodup) :
    (qlistT r s t).Nodup := by
  unfold qlistT
  apply nodup_flatMap_disjoint hr
  · intro e he
    apply nodup_flatMap_disjoint (nodup_filter _ hs)
    · intro esz hesz
      apply nodup_map_injOn _ (nodup_filter _ hs)
      intro esw1 _ esw2 _ h
      have h' : esw1 = esw2 := congrArg (fun w => w.2.2) h
      exact h'
    · intro esz1 he1 esz2 he2 hne u hu v hv h
      rw [List.mem_map] at hu hv
      obtain ⟨esw1, -, rfl⟩ := hu
      obtain ⟨esw2, -, rfl⟩ := hv
      exact hne (congrArg (fun w => w.2.1) h)
  · intro e1 he1 e2 he2 hne u hu v hv h
    rw [List.mem_flatMap] at hu hv
    obtain ⟨esz1, -, hu2⟩ := hu
    obtain ⟨esz2, -, hv2⟩ := hv
    rw [List.mem_map] at hu2 hv2
    obtain ⟨esw1, -, rfl⟩ := hu2
    obtain ⟨esw2, -, rfl⟩ := hv2
    exact hne (congrArg Prod.fst h)

/-- The injection `(e, es_z, es_w) ↦ (es_w, (es_z.2, e.1))` into `s × t`. -/
private def phiT (v : (Nat × Nat) × (Nat × Nat) × (Nat × Nat)) : Nat × Nat :=
  (v.2.2, (v.2.1.2, v.1.1))

private theorem phiT_injOn (r s t : Edges) :
    ∀ v ∈ qlistT r s t, ∀ w ∈ qlistT r s t, phiT v = phiT w → v = w := by
  intro v hv w hw h
  rw [qlistT, List.mem_flatMap] at hv hw
  obtain ⟨e1, he1, hvf⟩ := hv
  obtain ⟨e2, he2, hwf⟩ := hw
  rw [List.mem_flatMap] at hvf hwf
  obtain ⟨esz1, hesz1, hvm⟩ := hvf
  obtain ⟨esz2, hesz2, hwm⟩ := hwf
  rw [List.mem_map] at hvm hwm
  obtain ⟨esw1, hesw1, rfl⟩ := hvm
  obtain ⟨esw2, hesw2, rfl⟩ := hwm
  -- membership facts
  have hsz1 := List.mem_filter.mp hesz1
  have hsw1 := List.mem_filter.mp hesw1
  have hsz2 := List.mem_filter.mp hesz2
  have hsw2 := List.mem_filter.mp hesw2
  simp only [compE, Bool.and_eq_true] at hsz1 hsz2
  -- hsz1 : esz1 ∈ s ∧ (e1.2 == esz1.1) = true ∧ t.contains (esz1.2, e1.1) = true
  have he21 : esz1.1 = e1.2 := (beq_iff_eq.mp hsz1.2.1).symm
  have he22 : esz2.1 = e2.2 := (beq_iff_eq.mp hsz2.2.1).symm
  have ho21 : esw1.1 = e1.2 := (beq_iff_eq.mp hsw1.2).symm
  have ho22 : esw2.1 = e2.2 := (beq_iff_eq.mp hsw2.2).symm
  -- h : phiT (e1,esz1,esw1) = phiT (e2,esz2,esw2)
  have h' : (esw1, (esz1.2, e1.1)) = (esw2, (esz2.2, e2.1)) := h
  simp only [Prod.mk.injEq] at h'
  -- h' : esw1 = esw2 ∧ (esz1.2 = esz2.2 ∧ e1.1 = e2.1)
  have hesw : esw1 = esw2 := h'.1
  have hesz2eq : esz1.2 = esz2.2 := h'.2.1
  have he1eq : e1.1 = e2.1 := h'.2.2
  have he12 : e1.2 = e2.2 := by
    rw [← ho21, ← ho22, hesw]
  have hee : e1 = e2 := Prod.ext he1eq he12
  have hesz1eq : esz1.1 = esz2.1 := by
    rw [he21, he12, ← he22]
  have hesz : esz1 = esz2 := Prod.ext hesz1eq hesz2eq
  have hvw : (e1, esz1, esw1) = (e2, esz2, esw2) := by
    apply Prod.ext
    · exact hee
    · apply Prod.ext
      · exact hesz
      · exact hesw
  exact hvw

private theorem phiT_subset (r s t : Edges) :
    (qlistT r s t).map phiT ⊆ pairList s t := by
  intro a ha
  rw [List.mem_map] at ha
  obtain ⟨v, hv, rfl⟩ := ha
  rw [qlistT, List.mem_flatMap] at hv
  obtain ⟨e, he, hvf⟩ := hv
  rw [List.mem_flatMap] at hvf
  obtain ⟨esz, hesz, hvm⟩ := hvf
  rw [List.mem_map] at hvm
  obtain ⟨esw, hesw, rfl⟩ := hvm
  rw [mem_pairList]
  refine ⟨?_, ?_⟩
  · -- (phiT (e,esz,esw)).1 = esw ∈ s
    have := (List.mem_filter.mp hesw).1
    exact this
  · -- (phiT (e,esz,esw)).2 = (esz.2, e.1) ∈ t
    have hmem := (List.mem_filter.mp hesz).2
    simp only [compE, Bool.and_eq_true] at hmem
    exact List.contains_iff_mem.mp hmem.2

theorem sound_TriangleBound : TriangleBound.Goal := by
  intro r s t hr hs ht
  rw [triangles_length]
  have hcs := cs_fun r (fun _ => (1:Nat)) (fun e => cE s t e)
  dsimp only at hcs
  have h1 : (r.map fun k => (1:Nat) * cE s t k) = r.map fun k => cE s t k := by
    apply List.map_congr_left
    intro k hk
    exact Nat.one_mul _
  have h2 : (r.map fun k => ((1:Nat))^2) = r.map fun _ => (1:Nat) := by
    apply List.map_congr_left
    intro k hk
    rfl
  rw [h1, h2, sum_const, Nat.mul_one] at hcs
  have hsq : (r.map fun e => (cE s t e)^2).sum ≤
      (r.map fun e => cE s t e * outE s e).sum := by
    apply sum_mono
    intro e he
    rw [Nat.pow_two]
    exact Nat.mul_le_mul le_rfl (cE_le_outE s t e)
  have hQ : (r.map fun e => cE s t e * outE s e).sum = (qlistT r s t).length :=
    (qlist_length r s t).symm
  have hQle : (qlistT r s t).length ≤ s.length * t.length := by
    have hmapnd : ((qlistT r s t).map phiT).Nodup :=
      nodup_map_injOn _ (qlist_nodup r s t hr hs) (phiT_injOn r s t)
    have hsub : ((qlistT r s t).map phiT) ⊆ pairList s t := phiT_subset r s t
    have hle := List.Nodup.length_le_of_subset hmapnd hsub
    rw [List.length_map, length_pairList] at hle
    exact hle
  calc ((r.map fun e => cE s t e).sum)^2
      ≤ r.length * (r.map fun e => (cE s t e)^2).sum := hcs
    _ ≤ r.length * (r.map fun e => cE s t e * outE s e).sum := Nat.mul_le_mul_left _ hsq
    _ = r.length * (qlistT r s t).length := by rw [hQ]
    _ ≤ r.length * (s.length * t.length) := Nat.mul_le_mul_left _ hQle
    _ = r.length * s.length * t.length := by rw [Nat.mul_assoc]

end TinylambOpt.ResearchProofs.ExecutionStrategies.SearchBounds
