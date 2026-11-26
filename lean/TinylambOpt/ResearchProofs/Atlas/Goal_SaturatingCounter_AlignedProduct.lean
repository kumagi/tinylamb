import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `SaturatingCounter.AlignedProduct` — `∀ c (b : Lane), 0 < b.stride → ((rows layout
  c).map (fun r => ((List.range b.n).map (fun i => if r.1 == b.start+i*b.stride then
  r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum = ((layout.lanes c).map (aligned ·
  b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_AlignedProduct

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b; simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by rw [bezout.eq_1]; simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]; simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd' : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd']
        grind

private theorem bezout_one_coeff (d : Nat) :
    (1 : Int) = (bezout 1 d).1 + (d : Int) * (bezout 1 d).2 := by
  have h := bezout_spec 1 d
  simpa using h

private theorem unit_mod_sub_dvd (t : Int) (d : Nat) :
    (d : Int) ∣ ((t * (bezout 1 d).1) % (d : Int) - t) := by
  have h := bezout_one_coeff d
  refine ⟨-t * (bezout 1 d).2 - (t * (bezout 1 d).1) / (d : Int), ?_⟩
  have hdecomp := Int.emod_add_ediv_mul (t * (bezout 1 d).1) (d : Int)
  grind

private theorem intersection_stride_one (lo cnt s d m : Nat) (z w bb bs : Int)
    (hcnt : cnt ≠ 0) (hm : m ≠ 0) (hd : 0 < d) :
    intersection (positions ⟨lo, 1, cnt, z, w⟩) (positions ⟨s, d, m, bb, bs⟩) =
      let high := min ((lo : Int) + ((cnt - 1 : Nat) : Int)) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
      let low := max (lo : Int) (s : Int)
      let root := (lo : Int) + (((s : Int) - (lo : Int)) * (bezout 1 d).1) % (d : Int)
      let first := low + (root - low) % (d : Int)
      ⟨first, d, if first ≤ high then ((high - first) / (d : Int)).toNat + 1 else 0⟩ := by
  simp [intersection, positions, lastVal, hcnt, hm, Nat.ne_of_gt hd, Nat.gcd_one_left]

private theorem first_specs (lo cnt s d m : Nat) (hd : 0 < d) :
    let _high := min ((lo : Int) + ((cnt - 1 : Nat) : Int)) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
    let low := max (lo : Int) (s : Int)
    let root := (lo : Int) + (((s : Int) - (lo : Int)) * (bezout 1 d).1) % (d : Int)
    let first := low + (root - low) % (d : Int)
    (first - (s : Int)) % (d : Int) = 0 ∧ (s : Int) ≤ first ∧ (lo : Int) ≤ first ∧
    ((s : Int) ≤ (lo : Int) → first < (lo : Int) + (d : Int)) ∧
    ((lo : Int) ≤ (s : Int) → first = (s : Int)) := by
  dsimp only
  let low := max (lo : Int) (s : Int)
  let t : Int := (s : Int) - (lo : Int)
  let root := (lo : Int) + (t * (bezout 1 d).1) % (d : Int)
  let first := low + (root - low) % (d : Int)
  have hdI : (0 : Int) < (d : Int) := by exact_mod_cast hd
  have hdn : (d : Int) ≠ 0 := by omega
  have hroot_dvd : (d : Int) ∣ root - (s : Int) := by
    have ht := unit_mod_sub_dvd t d
    have heq : root - (s : Int) = (t * (bezout 1 d).1) % (d : Int) - t := by
      dsimp [root, t]; omega
    rw [heq]; exact ht
  have hroot_sub : (root - (s : Int)) % (d : Int) = 0 := Int.emod_eq_zero_of_dvd hroot_dvd
  have hmod_dvd : (d : Int) ∣ ((root - low) % (d : Int)) - (root - low) := by
    refine ⟨-((root - low) / (d : Int)), ?_⟩
    have h := Int.emod_add_ediv_mul (root - low) (d : Int)
    rw [Int.mul_neg, Int.mul_comm (d : Int) ((root - low) / (d : Int))]
    omega
  have hfirst_dvd : (d : Int) ∣ first - (s : Int) := by
    have hEq : first - (s : Int) = (root - (s : Int)) + (((root - low) % (d : Int)) - (root - low)) := by
      dsimp [first]; omega
    rw [hEq]
    exact Int.dvd_add hroot_dvd hmod_dvd
  have hfirst_mod : (first - (s : Int)) % (d : Int) = 0 := Int.emod_eq_zero_of_dvd hfirst_dvd
  have hfirst_ge_s : (s : Int) ≤ first := by
    have hnon : 0 ≤ (root - low) % (d : Int) := Int.emod_nonneg _ hdn
    have hlow : (s : Int) ≤ low := by dsimp [low]; omega
    dsimp [first]; omega
  have hfirst_ge_lo : (lo : Int) ≤ first := by
    have hnon : 0 ≤ (root - low) % (d : Int) := Int.emod_nonneg _ hdn
    have hlow : (lo : Int) ≤ low := by dsimp [low]; omega
    dsimp [first]; omega
  have hfirst_lt : (s : Int) ≤ (lo : Int) → first < (lo : Int) + (d : Int) := by
    intro hs
    have hnonneg : 0 ≤ (root - low) % (d : Int) := Int.emod_nonneg _ hdn
    have hlt : (root - low) % (d : Int) < (d : Int) := Int.emod_lt_of_pos _ hdI
    have hlow : low = (lo : Int) := by dsimp [low]; omega
    dsimp [first]; omega
  have hfirst_eq_s : (lo : Int) ≤ (s : Int) → first = (s : Int) := by
    intro hs
    have hlow : low = (s : Int) := by dsimp [low]; omega
    have hrem0 : (root - low) % (d : Int) = 0 := by rw [hlow]; exact hroot_sub
    dsimp [first]; rw [hrem0]; omega
  exact ⟨hfirst_mod, hfirst_ge_s, hfirst_ge_lo, hfirst_lt, hfirst_eq_s⟩

/-! list helpers -/
private theorem sum_range_succ (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem sum_map_zero_int {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_map_sum_comm {α β : Type} (la : List α) (lb : List β) (f : α → β → Int) :
    (la.map (fun a => (lb.map (fun b => f a b)).sum)).sum =
      (lb.map (fun b => (la.map (fun a => f a b)).sum)).sum := by
  induction la with
  | nil => simp [sum_map_zero_int]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      exact (sum_map_add lb (fun b => f a b)
        (fun b => (List.map (fun a => f a b) as).sum)).symm

private theorem sum_map_ite_filter {α : Type} (l : List α) (p : α → Prop) [DecidablePred p] (f : α → Int) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem filter_range_between (m j0 len : Nat) (h : j0 + len ≤ m) :
    (List.range m).filter (fun j => j0 ≤ j ∧ j < j0 + len) = List.range' j0 len := by
  induction m generalizing j0 len with
  | zero =>
      have hj0 : j0 = 0 := by omega
      have hlen : len = 0 := by omega
      subst j0; subst len; simp
  | succ m ih =>
      rw [show m + 1 = Nat.succ m by omega, List.range_succ, List.filter_append]
      by_cases hle : j0 + len ≤ m
      · rw [ih j0 len hle]
        have hlast : ¬ (j0 ≤ m ∧ m < j0 + len) := by omega
        simp [hlast]
      · by_cases hlen0 : len = 0
        · subst len
          have hnil : (List.range m).filter (fun j => j0 ≤ j ∧ j < j0 + 0) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro j hj
            by_cases hp : j0 ≤ j ∧ j < j0 + 0
            · have hjm : j < m := List.mem_range.mp hj
              omega
            · simp
          rw [hnil]
          simp
        · have hpred : j0 + (len - 1) = m := by omega
          have hle' : j0 + (len - 1) ≤ m := by omega
          have hfilter : (List.range m).filter (fun j => j0 ≤ j ∧ j < j0 + len) =
              (List.range m).filter (fun j => j0 ≤ j ∧ j < j0 + (len - 1)) := by
            apply List.filter_congr
            intro j hj
            have hjm : j < m := List.mem_range.mp hj
            by_cases h1 : j0 ≤ j ∧ j < j0 + len <;>
              by_cases h2 : j0 ≤ j ∧ j < j0 + (len - 1) <;>
              simp [h1, h2] <;> omega
          rw [hfilter, ih j0 (len - 1) hle']
          have hlast : j0 ≤ m ∧ m < j0 + len := by omega
          simp [hlast]
          have happ := List.range'_append (s := j0) (m := len - 1) (n := 1) (step := 1)
          simp [hpred] at happ
          simpa [show (len - 1) + 1 = len by omega] using happ

private theorem sum_range_between (m j0 len : Nat) (f : Nat → Int) (h : j0 + len ≤ m) :
    ((List.range m).map (fun j => if j0 ≤ j ∧ j < j0 + len then f j else 0)).sum =
      ((List.range len).map (fun k => f (j0 + k))).sum := by
  rw [sum_map_ite_filter]
  rw [filter_range_between m j0 len h]
  rw [List.range'_eq_map_range]
  rw [List.map_map]
  simp [Function.comp_def]

private theorem sum_map_eq_zero {α : Type} (l : List α) (f : α → Int)
    (h : ∀ a ∈ l, f a = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]
      simp

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Nat) * k := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

private theorem sum_prod (cnt : Nat) (x dx y dy : Int) :
    ((List.range cnt).map (fun (k : Nat) => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (cnt : Int) * x * y + tri cnt * (x * dy + y * dx) + squares cnt * dx * dy := by
  induction cnt with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih =>
      rw [sum_range_succ, ih]
      rw [show ((k + 1 : Nat) : Int) = (k : Int) + 1 by omega]
      rw [tri_succ, squares_succ]
      grind

private theorem aligned_stride_one (lo cnt s d m : Nat) (z w bb bs : Int) (hd : 0 < d) :
    aligned ⟨lo, 1, cnt, z, w⟩ ⟨s, d, m, bb, bs⟩ =
      ((List.range m).map (fun j : Nat =>
        let p := (s : Int) + (j : Int) * (d : Int)
        if (lo : Int) ≤ p ∧ p < (lo : Int) + (cnt : Int) then
          (z + (p - (lo : Int)) * w) * (bb + bs * (j : Int)) else 0)).sum := by
  by_cases hcnt : cnt = 0
  · subst cnt
    have hright : aligned ⟨lo, 1, 0, z, w⟩ ⟨s, d, m, bb, bs⟩ = 0 := by
      simp [aligned, intersection, positions, tri, squares,
        TinylambOpt.ResearchGoals.PairAggregates.choose2]
    rw [hright]
    symm
    apply sum_map_eq_zero
    intro j hjmem
    by_cases hc : (lo : Int) ≤ (s : Int) + (j : Int) * (d : Int) ∧
        (s : Int) + (j : Int) * (d : Int) < (lo : Int)
    · omega
    · simp [hc]
  by_cases hm : m = 0
  · subst m
    simp [aligned, intersection, positions, tri, squares,
      TinylambOpt.ResearchGoals.PairAggregates.choose2]
  let high := min ((lo : Int) + ((cnt - 1 : Nat) : Int)) ((s : Int) + ((m - 1 : Nat) : Int) * (d : Int))
  let low := max (lo : Int) (s : Int)
  let root := (lo : Int) + (((s : Int) - (lo : Int)) * (bezout 1 d).1) % (d : Int)
  let first := low + (root - low) % (d : Int)
  let q : Int := (high - first) / (d : Int)
  have hs := first_specs lo cnt s d m hd
  change ((first - (s : Int)) % (d : Int) = 0 ∧ (s : Int) ≤ first ∧ (lo : Int) ≤ first ∧
    ((s : Int) ≤ (lo : Int) → first < (lo : Int) + (d : Int)) ∧
    ((lo : Int) ≤ (s : Int) → first = (s : Int))) at hs
  obtain ⟨hfirst_mod, hfirst_ge_s, hfirst_ge_lo, hfirst_lt, hfirst_eq_s⟩ := hs
  have hdI : (0 : Int) < (d : Int) := by exact_mod_cast hd
  have hfirst_dvd : (d : Int) ∣ first - (s : Int) := Int.dvd_of_emod_eq_zero hfirst_mod
  let j0 : Int := (first - (s : Int)) / (d : Int)
  have hj0_nonneg : 0 ≤ j0 := by
    dsimp [j0]
    exact Int.ediv_nonneg (by omega) (by omega)
  have hj0_mul : j0 * (d : Int) = first - (s : Int) := by
    dsimp [j0]
    exact Int.ediv_mul_cancel hfirst_dvd
  have hpmod (j : Nat) : (d : Int) ∣ ((s : Int) + (j : Int) * (d : Int)) - (s : Int) := by
    refine ⟨(j : Int), ?_⟩
    grind
  have hge_first (j : Nat) (hjlo : (lo : Int) ≤ (s : Int) + (j : Int) * (d : Int)) :
      first ≤ (s : Int) + (j : Int) * (d : Int) := by
    by_cases hls : (lo : Int) ≤ (s : Int)
    · rw [hfirst_eq_s hls]
      have hmul : 0 ≤ (j : Int) * (d : Int) := Int.mul_nonneg (by omega) (by omega)
      omega
    · have hsl : (s : Int) ≤ (lo : Int) := by omega
      have hltlo := hfirst_lt hsl
      have hdvddiff : (d : Int) ∣ first - ((s : Int) + (j : Int) * (d : Int)) := by
        have hd2 := Int.dvd_sub hfirst_dvd (hpmod j)
        have heq : first - (s : Int) - ((s : Int) + (j : Int) * (d : Int) - (s : Int)) =
            first - ((s : Int) + (j : Int) * (d : Int)) := by grind
        rw [← heq]
        exact hd2
      by_cases hle : first ≤ (s : Int) + (j : Int) * (d : Int)
      · exact hle
      · exfalso
        have hpos : 0 < first - ((s : Int) + (j : Int) * (d : Int)) := by omega
        have hdle : (d : Int) ≤ first - ((s : Int) + (j : Int) * (d : Int)) :=
          Int.le_of_dvd hpos hdvddiff
        omega
  have hdecomp : (high - first) % (d : Int) + q * (d : Int) = high - first := by
    dsimp [q]
    exact Int.emod_add_ediv_mul (high - first) (d : Int)
  have hrem_nonneg : 0 ≤ (high - first) % (d : Int) := Int.emod_nonneg _ (by omega)
  have hrem_lt : (high - first) % (d : Int) < (d : Int) := Int.emod_lt_of_pos _ hdI
  have hp_gen (j : Nat) : (s : Int) + (j : Int) * (d : Int) = first + ((j : Int) - j0) * (d : Int) := by
    grind
  have hj0c : (j0.toNat : Int) = j0 := Int.toNat_of_nonneg hj0_nonneg
  by_cases hfh : first ≤ high
  · have hq_nonneg : 0 ≤ q := by
      dsimp [q]
      exact Int.ediv_nonneg (by omega) (by omega)
    have hq_le : j0 + q ≤ ((m - 1 : Nat) : Int) := by
      have hqle' : q ≤ ((m - 1 : Nat) : Int) - j0 := by
        dsimp [q]
        rw [Int.ediv_le_iff_le_mul hdI]
        have hminB : high ≤ (s : Int) + ((m - 1 : Nat) : Int) * (d : Int) := by dsimp [high]; omega
        have hfirst_eq : first = (s : Int) + j0 * (d : Int) := by grind
        grind
      omega
    have hlen_le_m : j0.toNat + q.toNat + 1 ≤ m := by
      have hqc : (q.toNat : Int) = q := Int.toNat_of_nonneg hq_nonneg
      omega
    let len := q.toNat + 1
    have hlen_le_m' : j0.toNat + len ≤ m := by
      dsimp [len]
      omega
    have hcond_iff (j : Nat) (hj : j < m) :
        ((lo : Int) ≤ (s : Int) + (j : Int) * (d : Int) ∧
         (s : Int) + (j : Int) * (d : Int) < (lo : Int) + (cnt : Int)) ↔
        (j0.toNat ≤ j ∧ j < j0.toNat + len) := by
      constructor
      · intro hqual
        have hfirst_le := hge_first j hqual.1
        have hplehigh : (s : Int) + (j : Int) * (d : Int) ≤ high := by
          have hA : (s : Int) + (j : Int) * (d : Int) ≤ (lo : Int) + ((cnt - 1 : Nat) : Int) := by
            omega
          have hjle : (j : Int) ≤ ((m - 1 : Nat) : Int) := by omega
          have hmuld : (j : Int) * (d : Int) ≤ ((m - 1 : Nat) : Int) * (d : Int) :=
            Int.mul_le_mul_of_nonneg_right hjle (by omega)
          have hB : (s : Int) + (j : Int) * (d : Int) ≤ (s : Int) + ((m - 1 : Nat) : Int) * (d : Int) := by
            omega
          dsimp [high]
          omega
        have hj0leJ : j0.toNat ≤ j := by
          have hprod : 0 ≤ ((j : Int) - j0) * (d : Int) := by
            rw [hp_gen j] at hfirst_le
            omega
          have hmuld : j0 * (d : Int) ≤ (j : Int) * (d : Int) := by grind
          have hji : j0 ≤ (j : Int) := Int.le_of_mul_le_mul_right hmuld hdI
          omega
        have hjlt : j < j0.toNat + len := by
          have hprod_le : ((j : Int) - j0) * (d : Int) ≤ high - first := by
            rw [hp_gen j] at hplehigh
            omega
          have hprod_lt : ((j : Int) - j0) * (d : Int) < (q + 1) * (d : Int) := by grind
          have hdiff_lt : (j : Int) - j0 < q + 1 :=
            Int.lt_of_mul_lt_mul_right hprod_lt (by omega)
          have hqc : (q.toNat : Int) = q := Int.toNat_of_nonneg hq_nonneg
          dsimp [len]
          omega
        exact ⟨hj0leJ, hjlt⟩
      · intro hjl
        have hprod_nonneg : 0 ≤ ((j : Int) - j0) * (d : Int) := by
          have : j0 ≤ (j : Int) := by omega
          exact Int.mul_nonneg (by omega) (by omega)
        have hlo : (lo : Int) ≤ (s : Int) + (j : Int) * (d : Int) := by
          rw [hp_gen j]
          omega
        have hprod_le_q : ((j : Int) - j0) * (d : Int) ≤ q * (d : Int) := by
          have : (j : Int) - j0 ≤ q := by
            have hqc : (q.toNat : Int) = q := Int.toNat_of_nonneg hq_nonneg
            dsimp [len] at hjl
            omega
          exact Int.mul_le_mul_of_nonneg_right this (by omega)
        have hplehigh : (s : Int) + (j : Int) * (d : Int) ≤ high := by
          rw [hp_gen j]
          grind
        have hminA : high ≤ (lo : Int) + ((cnt - 1 : Nat) : Int) := by dsimp [high]; omega
        have hltA : (s : Int) + (j : Int) * (d : Int) < (lo : Int) + (cnt : Int) := by
          dsimp [high] at hplehigh hminA
          omega
        exact ⟨hlo, hltA⟩
    have hint : intersection (positions ⟨lo, 1, cnt, z, w⟩) (positions ⟨s, d, m, bb, bs⟩) =
        ⟨first, d, len⟩ := by
      rw [intersection_stride_one lo cnt s d m z w bb bs hcnt hm hd]
      dsimp [high, low, root, first, q, len]
      rw [ite_eq_left hfh]
    have hright : aligned ⟨lo, 1, cnt, z, w⟩ ⟨s, d, m, bb, bs⟩ =
        ((List.range len).map (fun k : Nat =>
          (z + (first + (k : Int) * (d : Int) - (lo : Int)) * w) *
            (bb + bs * (((j0.toNat + k : Nat) : Int))))).sum := by
      rw [aligned, hint]
      dsimp only
      simp [firstAt, slopeAt, Nat.div_self hd, j0]
      rw [← sum_prod]
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      have hj0c' : (j0.toNat : Int) = j0 := hj0c
      grind
    have hleft : ((List.range m).map (fun j : Nat =>
        let p := (s : Int) + (j : Int) * (d : Int)
        if (lo : Int) ≤ p ∧ p < (lo : Int) + (cnt : Int) then
          (z + (p - (lo : Int)) * w) * (bb + bs * (j : Int)) else 0)).sum =
        ((List.range len).map (fun k : Nat =>
          (z + (first + (k : Int) * (d : Int) - (lo : Int)) * w) *
            (bb + bs * (((j0.toNat + k : Nat) : Int))))).sum := by
      rw [show (List.range m).map (fun j : Nat =>
        let p := (s : Int) + (j : Int) * (d : Int)
        if (lo : Int) ≤ p ∧ p < (lo : Int) + (cnt : Int) then
          (z + (p - (lo : Int)) * w) * (bb + bs * (j : Int)) else 0) =
        (List.range m).map (fun j : Nat =>
        if j0.toNat ≤ j ∧ j < j0.toNat + len then
          (z + (((s : Int) + (j : Int) * (d : Int)) - (lo : Int)) * w) * (bb + bs * (j : Int)) else 0) by
        apply List.map_congr_left
        intro j hjmem
        have hj := List.mem_range.mp hjmem
        by_cases hc : (lo : Int) ≤ (s : Int) + (j : Int) * (d : Int) ∧
            (s : Int) + (j : Int) * (d : Int) < (lo : Int) + (cnt : Int)
        · have hc' := (hcond_iff j hj).mp hc
          simp [hc, hc']
        · have hc' : ¬ (j0.toNat ≤ j ∧ j < j0.toNat + len) := by
            intro hh
            exact hc ((hcond_iff j hj).mpr hh)
          have hc'' : ¬ (j0 ≤ (j : Int) ∧ j < j0.toNat + len) := by
            intro hh
            apply hc'
            constructor <;> omega
          simp [hc, hc'']]
      rw [sum_range_between m j0.toNat len _ hlen_le_m']
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      have hp := hp_gen (j0.toNat + k)
      have hj0c' : (j0.toNat : Int) = j0 := hj0c
      grind
    rw [hright, hleft]
  · have hint : intersection (positions ⟨lo, 1, cnt, z, w⟩) (positions ⟨s, d, m, bb, bs⟩) =
        ⟨first, d, 0⟩ := by
      rw [intersection_stride_one lo cnt s d m z w bb bs hcnt hm hd]
      dsimp [high, low, root, first]
      rw [ite_eq_right hfh]
    have hright : aligned ⟨lo, 1, cnt, z, w⟩ ⟨s, d, m, bb, bs⟩ = 0 := by
      rw [aligned, hint]
      simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
    rw [hright]
    symm
    apply sum_map_eq_zero
    intro j hjmem
    have hj := List.mem_range.mp hjmem
    have hnot : ¬ ((lo : Int) ≤ (s : Int) + (j : Int) * (d : Int) ∧
        (s : Int) + (j : Int) * (d : Int) < (lo : Int) + (cnt : Int)) := by
      intro hc
      have hfirst_le := hge_first j hc.1
      have hplehigh : (s : Int) + (j : Int) * (d : Int) ≤ high := by
        have hjle : (j : Int) ≤ ((m - 1 : Nat) : Int) := by omega
        have hmuld : (j : Int) * (d : Int) ≤ ((m - 1 : Nat) : Int) * (d : Int) :=
          Int.mul_le_mul_of_nonneg_right hjle (by omega)
        have hA : (s : Int) + (j : Int) * (d : Int) ≤ (lo : Int) + ((cnt - 1 : Nat) : Int) := by omega
        have hB : (s : Int) + (j : Int) * (d : Int) ≤ (s : Int) + ((m - 1 : Nat) : Int) * (d : Int) := by omega
        dsimp [high]
        omega
      omega
    simp [hnot]

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_range_single (n target : Nat) (v : Int) :
    ((List.range n).map (fun i => if i = target then v else 0)).sum =
      if target < n then v else 0 := by
  induction n generalizing target with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_singleton]
      rw [ih]
      by_cases ht : target = n
      · subst target; simp
      · have hne : n ≠ target := by omega
        by_cases hlt : target < n
        · have htn : target < n + 1 := by omega
          simp [hne, hlt, htn]
        · have htn : ¬ target < n + 1 := by omega
          simp [hne, show ¬ target < n by omega, htn]

private theorem rows_saturating (c : Config) :
    rows SaturatingCounter.layout c = (List.range c.n).map (fun i : Nat =>
      (i, if i < c.split then c.base + c.step * (i : Int) else c.base + c.step * (c.split : Int))) := by
  unfold rows
  rw [show (List.range c.n).filterMap (fun (i : Nat) =>
        (SaturatingCounter.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun (i : Nat) =>
        some (i, if i < c.split then c.base + c.step * (i : Int) else c.base + c.step * (c.split : Int))) from by
    apply filterMap_congr'
    intro i hi
    by_cases hil : i < c.split <;> simp [SaturatingCounter.layout, hil]]
  exact filterMap_some_eq_map _ _

theorem sound_SaturatingCounter_AlignedProduct : SaturatingCounter.AlignedProduct.Goal := by
  rintro ⟨n, split, parameter, base, step, bias, dict⟩ ⟨s, d, m, bb, bs⟩ hd
  simp only at hd
  rw [rows_saturating]
  rw [List.map_map, Function.comp_def]
  simp only [beq_iff_eq]
  rw [sum_map_sum_comm]
  have hswap : ((List.range m).map (fun j : Nat => ((List.range n).map (fun i : Nat =>
      if i = s + j * d then
        (if i < split then base + step * (i : Int) else base + step * (split : Int)) *
          (bb + bs * (j : Int)) else 0)).sum)).sum =
      ((List.range m).map (fun j : Nat => if s + j * d < n then
        (if s + j * d < split then base + step * ((s + j * d : Nat) : Int)
          else base + step * (split : Int)) * (bb + bs * (j : Int)) else 0)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hjmem
    rw [show (fun i : Nat => if i = s + j * d then
        (if i < split then base + step * (i : Int) else base + step * (split : Int)) *
          (bb + bs * (j : Int)) else 0) =
      (fun i : Nat => if i = s + j * d then
        (if s + j * d < split then base + step * ((s + j * d : Nat) : Int)
          else base + step * (split : Int)) * (bb + bs * (j : Int)) else 0) by
      funext i
      by_cases hi : i = s + j * d
      · subst i; simp
      · simp [hi]]
    rw [sum_range_single]
  rw [hswap]
  simp [SaturatingCounter.layout, span, lane]
  rw [aligned_stride_one 0 (min n split) s d m base step bb bs hd]
  rw [aligned_stride_one split (n - split) s d m (base + step * (split : Int)) 0 bb bs hd]
  rw [← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro j hjmem
  let t : Nat := s + j * d
  let p : Int := (s : Int) + (j : Int) * (d : Int)
  have hp : p = (t : Int) := by simp [p, t]
  have hp' : p = (s : Int) + (j : Int) * (d : Int) := rfl
  have hp_nonneg : (0 : Int) ≤ p := by
    rw [hp]
    omega
  by_cases htlt : t < n
  · have htlt' : s + j * d < n := by simpa [t] using htlt
    by_cases htsplit : t < split
    · have htsplit' : s + j * d < split := by simpa [t] using htsplit
      have hp_min : p < ((min n split : Nat) : Int) := by
        rw [hp]
        exact_mod_cast (show t < min n split by omega)
      have hp_split : ¬ ((split : Int) ≤ p) := by
        rw [hp]
        omega
      rw [← hp']
      simp [htlt', htsplit', hp_nonneg, hp_min, hp_split]
      grind
    · have htsplit' : ¬ s + j * d < split := by simpa [t] using htsplit
      have hp_min : ¬ (p < ((min n split : Nat) : Int)) := by
        rw [hp]
        omega
      have hp_split_ge : (split : Int) ≤ p := by
        rw [hp]
        omega
      have hp_split_lt : p < (split : Int) + ((n - split : Nat) : Int) := by
        rw [hp]
        exact_mod_cast (show t < split + (n - split) by omega)
      rw [← hp']
      simp [htlt', htsplit', hp_nonneg, hp_min, hp_split_ge, hp_split_lt]
  · have htlt' : ¬ s + j * d < n := by simpa [t] using htlt
    have hp_min : ¬ (p < ((min n split : Nat) : Int)) := by
      rw [hp]
      omega
    have hp_split : ¬ ((split : Int) ≤ p ∧
        p < (split : Int) + ((n - split : Nat) : Int)) := by
      rw [hp]
      omega
    rw [← hp']
    simp [htlt', hp_nonneg, hp_min, hp_split]

end TinylambOpt.ResearchProofs.Atlas.Goal_SaturatingCounter_AlignedProduct
