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

/-! ### Unresolved goals (no proof included)

-- TODO(SortedDegreeBound): requires a full rearrangement inequality for two
--   frequency spectra of different lengths (align descending counts, allowing
--   zero padding); the Std-only environment has no such lemma.
-- TODO(TriangleBound): needs the tripartite triangle bound |T|^2 <= |r||s||t|
--   (Cauchy--Schwarz over t-edges plus an injection of squared path counts).
-- TODO(FrequentCandidates): needs Misra--Gries invariants over Std.HashMap's
--   fold (decrement step and size/containment potential), not yet formalized.
-/

end TinylambOpt.ResearchProofs.Wave7SearchBounds
