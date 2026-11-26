import TinylambOpt.ResearchGoals.RestartRaces.JobJ3

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `RankOneTile` — Is equality of every cross product both necessary and sufficient to
  represent a tile by two *integer*, nonnegative vectors for every selection and
  year-weighted sum? Allows proportional neighborhoods beyond exact twins.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.JobJ3_RankOneTile
open TinylambOpt.ResearchGoals.RestartRaces.JobJ3.RankOneTile

theorem bezout (a b : Nat) : ∃ x y : Int, x * (a : Int) + y * (b : Int) = (Nat.gcd a b : Int) := by
  revert a
  refine Nat.strongRecOn b (fun b ih a => ?_)
  cases b with
      | zero => exact ⟨1, 0, by simp⟩
      | succ b' =>
          have hmodlt : a % (b'+1) < b'+1 := Nat.mod_lt _ (Nat.succ_pos b')
          rcases ih (a % (b'+1)) hmodlt (b'+1) with ⟨x, y, hxy⟩
          have hg : Nat.gcd a (b'+1) = Nat.gcd (b'+1) (a % (b'+1)) := by
            rw [Nat.gcd_comm a (b'+1), Nat.gcd_rec (b'+1) a, Nat.gcd_comm]
          refine ⟨y, x - y * (a / (b'+1) : Nat), ?_⟩
          rw [hg]
          have hdivNat : a = (a / (b'+1)) * (b'+1) + a % (b'+1) := by
            rw [Nat.mul_comm]
            exact (Nat.div_add_mod a (b'+1)).symm
          have hdivInt : (a : Int) = ((a / (b'+1)) * (b'+1) + a % (b'+1) : Nat) := by
            exact_mod_cast hdivNat
          have hmod : ((a % (b'+1) : Nat) : Int) = (a : Int) - (a/(b'+1) : Nat) * (b'+1 : Nat) := by
            grind
          rw [hmod] at hxy
          grind

def gcdList : List Nat → Nat
  | [] => 0
  | a :: as => Nat.gcd a (gcdList as)

theorem zipWith_sum_mul {α : Type} (zs : List Int) (vs : List α) (f : α → Int) (c : Int) :
    (List.zipWith (fun z v => (c * z) * f v) zs vs).sum =
      c * (List.zipWith (fun z v => z * f v) zs vs).sum := by
  induction zs generalizing vs with
  | nil => simp
  | cons z zs ih =>
      cases vs with
      | nil => simp
      | cons v vs =>
          simp [ih]
          grind

theorem bezout_list : ∀ l : List Nat, ∃ zs : List Int, zs.length = l.length ∧
    (List.zipWith (fun z v => z * v) zs (l.map (fun v : Nat => (v : Int)))).sum = (gcdList l : Int)
  | [] => ⟨[], rfl, by simp [gcdList]⟩
  | a :: as => by
      rcases bezout_list as with ⟨zs, hlen, hz⟩
      rcases bezout a (gcdList as) with ⟨x, y, hxy⟩
      refine ⟨x :: zs.map (fun z => y * z), by simp [hlen], ?_⟩
      simp [gcdList]
      have hz' : (List.zipWith (fun z (v : Nat) => z * (v : Int)) zs as).sum = (gcdList as : Int) := by
        rw [← List.zipWith_map_right]
        exact hz
      have htail := zipWith_sum_mul zs as (fun v : Nat => (v : Int)) y
      rw [htail, hz']
      grind

theorem gcdList_dvd (l : List Nat) (k : Nat) (hk : k ∈ l) : gcdList l ∣ k := by
  induction l with
  | nil => simp at hk
  | cons a as ih =>
      simp [gcdList] at hk ⊢
      rcases hk with hka | hkas
      · subst k
        exact Nat.gcd_dvd_left a (gcdList as)
      · exact Nat.dvd_trans (Nat.gcd_dvd_right a (gcdList as)) (ih hkas)

theorem gcdList_ne_zero {l : List Nat} {k : Nat} (hk : k ∈ l) (hk0 : k ≠ 0) : gcdList l ≠ 0 := by
  intro hg
  have hdvd := gcdList_dvd l k hk
  rw [hg] at hdvd
  exact hk0 (Nat.eq_zero_of_zero_dvd hdvd)

theorem range'_shift : ∀ n s : Nat, List.range' (s+1) n 1 = (List.range' s n 1).map (fun k => k+1)
  | 0, s => by simp
  | n+1, s => by simp [List.range'_succ, range'_shift n (s+1), range'_shift n s]
theorem range_succ_cons (n : Nat) : List.range (n+1) = 0 :: (List.range n).map (fun k => k+1) := by
  rw [List.range_eq_range', List.range_eq_range', List.range'_succ]
  congr
  exact range'_shift n 0

theorem zipWith_sum_range {zs : List Int} {l : List Nat} (hlen : zs.length = l.length)
    (f : Int → Nat → Int) :
    (List.zipWith f zs l).sum =
      ((List.range l.length).map (fun k => f (zs[k]?.getD 0) (l[k]?.getD 0))).sum := by
  induction zs generalizing l with
  | nil =>
      cases l with
      | nil => simp
      | cons a as => simp at hlen
  | cons z zs ih =>
      cases l with
      | nil => simp at hlen
      | cons a as =>
          simp at hlen ⊢
          rw [ih hlen]
          rw [range_succ_cons]
          simp
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          simp


theorem sum_map_mul_right (l : List Int) (c : Int) : (l.map (fun x => x * c)).sum = l.sum * c := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]; grind

theorem sum_map_mul_left (l : List Int) (c : Int) : (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]; grind

theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ : α => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

theorem dot_eq_of_row (m : Nat) (w : Nat → Nat → Nat) (a b : Nat → Nat) (y : Nat → Int) (i : Nat)
    (h : ∀ j ∈ List.range m, w i j = a i * b j) :
    dot m (w i) y = (a i : Int) * dot m b y := by
  unfold dot
  have hmap : (List.range m).map (fun j => (w i j : Int) * y j) =
      (List.range m).map (fun j => (a i : Int) * ((b j : Int) * y j)) := by
    apply List.map_congr_left
    intro j hj
    rw [h j hj]
    grind
  rw [hmap]
  rw [show (List.range m).map (fun j => (a i : Int) * ((b j : Int) * y j)) =
      ((List.range m).map (fun j => (b j : Int) * y j)).map (fun x => (a i : Int) * x) by
        simp [List.map_map]]
  rw [sum_map_mul_left]

theorem full_eq_of_entry (n m : Nat) (w : Nat → Nat → Nat) (a b : Nat → Nat)
    (x y : Nat → Int) (h : ∀ i ∈ List.range n, ∀ j ∈ List.range m, w i j = a i * b j) :
    full n m w x y = dot n a x * dot m b y := by
  induction n with
  | zero => simp [full, dot]
  | succ n ih =>
      have htail := ih (fun i hi j hj => by
        have hin : i < n := by simpa using hi
        have hmem : i ∈ List.range (n+1) := by simpa using (show i < n+1 by omega)
        exact h i hmem j hj)
      have htail' : ((List.range n).map (fun i => x i * dot m (w i) y)).sum =
          dot n a x * dot m b y := by
        simpa [full] using htail
      have hlast := dot_eq_of_row m w a b y n (fun j hj => h n (by simp) j hj)
      unfold full
      simp only [List.range_succ, List.map_append, List.sum_append, List.map_cons,
        List.map_nil, List.sum_cons, List.sum_nil]
      rw [htail', hlast]
      unfold dot
      simp only [List.range_succ, List.map_append, List.sum_append, List.map_cons,
        List.map_nil, List.sum_cons, List.sum_nil]
      grind


theorem sum_range_indicator (n i : Nat) (hi : i < n) (f : Nat → Int) :
    ((List.range n).map (fun k => (if k == i then 1 else 0) * f k)).sum = f i := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      by_cases hin : i < n
      · rw [ih hin]
        have hne : (n == i) = false := by
          have : n ≠ i := by omega
          simpa [this]
        have hne' : n ≠ i := by omega
        simp [hne, hne']
      · have hieq : i = n := by omega
        subst i
        have hzero : ((List.range n).map (fun k => (if k == n then 1 else 0) * f k)).sum = 0 := by
          have hmap : (List.range n).map (fun k => (if k == n then 1 else 0) * f k) =
              (List.range n).map (fun _ : Nat => (0 : Int)) := by
            apply List.map_congr_left
            intro k hk
            have hkn : k < n := by simpa using hk
            have hkne : k ≠ n := by omega
            simp [hkne]
          rw [hmap]
          exact sum_map_zero _
        rw [hzero]
        simp

theorem full_indicator (n m i k : Nat) (hi : i < n) (hk : k < m) (w : Nat → Nat → Nat) :
    full n m w (fun p => if p == i then 1 else 0) (fun q => if q == k then 1 else 0) = (w i k : Int) := by
  unfold full dot
  have hdot : ((List.range m).map (fun q => (w i q : Int) * (if q == k then 1 else 0))).sum = (w i k : Int) := by
    have hs := sum_range_indicator m k hk (fun q => (w i q : Int))
    rw [show (List.range m).map (fun q => (w i q : Int) * (if q == k then 1 else 0)) =
        (List.range m).map (fun q => (if q == k then 1 else 0) * (w i q : Int)) by
      apply List.map_congr_left
      intro q hq
      grind]
    exact hs
  have hmap : (List.range n).map (fun p =>
      (if p == i then 1 else 0) *
        ((List.range m).map (fun q => (w p q : Int) * (if q == k then 1 else 0))).sum) =
      (List.range n).map (fun p => (if p == i then 1 else 0) * (w i k : Int)) := by
    apply List.map_congr_left
    intro p hp
    by_cases hpi : p == i
    · have hpi' : p = i := by simpa using hpi
      subst p
      rw [hdot]
    · have hpz : (if p == i then 1 else 0 : Int) = 0 := by simp [hpi]
      rw [hpz]
      simp
  rw [hmap]
  exact sum_range_indicator n i hi (fun _ => (w i k : Int))

theorem dot_indicator (n i : Nat) (hi : i < n) (a : Nat → Nat) :
    dot n a (fun p => if p == i then 1 else 0) = (a i : Int) := by
  unfold dot
  have hs := sum_range_indicator n i hi (fun p => (a p : Int))
  rw [show (List.range n).map (fun p => (a p : Int) * (if p == i then 1 else 0)) =
      (List.range n).map (fun p => (if p == i then 1 else 0) * (a p : Int)) by
    apply List.map_congr_left
    intro p hp
    grind]
  exact hs

theorem sound : Goal := by
  classical
  intro n m w
  constructor
  · intro hcross
    by_cases hEx : ∃ i ∈ List.range n, ∃ j ∈ List.range m, w i j ≠ 0
    · rcases hEx with ⟨i0, hi0, j0, hj0, hwne⟩
      let row := (List.range m).map (fun j => w i0 j)
      let g := gcdList row
      let b : Nat → Nat := fun j => w i0 j / g
      have hwpos : 0 < w i0 j0 := Nat.pos_of_ne_zero hwne
      have hg_ne : g ≠ 0 := by
        apply gcdList_ne_zero (k := w i0 j0)
        · exact List.mem_map.mpr ⟨j0, hj0, rfl⟩
        · exact hwne
      have hgpos : 0 < g := Nat.pos_of_ne_zero hg_ne
      have hgdvd : ∀ j ∈ List.range m, g ∣ w i0 j := by
        intro j hj
        simpa [g, row] using gcdList_dvd row (w i0 j) (List.mem_map.mpr ⟨j, hj, rfl⟩)
      have hb_eq : ∀ j ∈ List.range m, w i0 j = g * b j := by
        intro j hj
        have hd := hgdvd j hj
        dsimp [b]
        rw [Nat.mul_comm]
        exact (Nat.div_mul_cancel hd).symm
      have hbj0pos : 0 < b j0 := by
        have hgle : g ≤ w i0 j0 := Nat.le_of_dvd hwpos (hgdvd j0 hj0)
        dsimp [b]
        exact Nat.div_pos hgle hgpos
      rcases bezout_list row with ⟨zs, hlen, hz⟩
      let z : Nat → Int := fun j => zs[j]?.getD 0
      have hconv := zipWith_sum_range (l := row) (f := fun zz (v : Nat) => zz * (v : Int)) (by simpa [row] using hlen)
      have hrowlen : row.length = m := by simp [row]
      have hrowget : ∀ k ∈ List.range m, row[k]?.getD 0 = w i0 k := by
        intro k hk
        have hklt : k < m := by simpa using hk
        rw [show row = (List.range m).map (fun j => w i0 j) by rfl]
        rw [List.getElem?_map]
        simp [hklt]
      have hconv' : (List.zipWith (fun zz (v : Nat) => zz * (v : Int)) zs row).sum =
          ((List.range m).map (fun j => z j * (w i0 j : Int))).sum := by
        rw [hconv]
        apply congrArg List.sum
        rw [hrowlen]
        apply List.map_congr_left
        intro k hk
        dsimp [z]
        rw [hrowget k hk]
      have hsum_g : ((List.range m).map (fun j => z j * (w i0 j : Int))).sum = (g : Int) := by
        rw [← hconv']
        have hz' := hz
        rw [List.zipWith_map_right] at hz'
        simpa [g] using hz'
      have hsum_b : ((List.range m).map (fun j => z j * (b j : Int))).sum = 1 := by
        have hmap : (List.range m).map (fun j => z j * (w i0 j : Int)) =
            (List.range m).map (fun j => (g : Int) * (z j * (b j : Int))) := by
          apply List.map_congr_left
          intro j hj
          have hji := hb_eq j hj
          have hjint : (w i0 j : Int) = (g : Int) * (b j : Int) := by exact_mod_cast hji
          rw [hjint]
          grind
        rw [hmap] at hsum_g
        rw [show (List.range m).map (fun j => (g : Int) * (z j * (b j : Int))) =
            ((List.range m).map (fun j => z j * (b j : Int))).map (fun x => (g : Int) * x) by
          simp [List.map_map]] at hsum_g
        rw [sum_map_mul_left] at hsum_g
        have hgint : (g : Int) ≠ 0 := by exact_mod_cast hg_ne
        have htmp : (g : Int) * ((List.range m).map (fun j => z j * (b j : Int))).sum = (g : Int) * 1 := by
          simpa using hsum_g
        exact (Int.mul_eq_mul_left_iff hgint).mp htmp
      have hcrossEq : ∀ i ∈ List.range n, ∀ j ∈ List.range m,
          w i j * b j0 = w i j0 * b j := by
        intro i hi j hj
        have hcr := hcross i hi i0 hi0 j hj j0 hj0
        have hb0i := hb_eq j0 hj0
        have hbji := hb_eq j hj
        have hnat : w i j * (g * b j0) = w i j0 * (g * b j) := by
          rw [← hb0i, ← hbji]
          exact hcr
        have hcancel : (w i j * b j0) * g = (w i j0 * b j) * g := by
          grind
        exact Nat.eq_of_mul_eq_mul_right hgpos hcancel
      let S : Nat → Int := fun i => ((List.range m).map (fun j => z j * (w i j : Int))).sum
      have hSmul : ∀ i ∈ List.range n, S i * (b j0 : Int) = (w i j0 : Int) := by
        intro i hi
        have hterm : ∀ j ∈ List.range m,
            (z j * (w i j : Int)) * (b j0 : Int) = (w i j0 : Int) * (z j * (b j : Int)) := by
          intro j hj
          have hnat := hcrossEq i hi j hj
          have hint : (w i j : Int) * (b j0 : Int) = (w i j0 : Int) * (b j : Int) := by
            exact_mod_cast hnat
          grind
        have hsum : ((List.range m).map (fun j => (z j * (w i j : Int)) * (b j0 : Int))).sum =
            ((List.range m).map (fun j => (w i j0 : Int) * (z j * (b j : Int)))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro j hj
          exact hterm j hj
        rw [show (List.range m).map (fun j => (z j * (w i j : Int)) * (b j0 : Int)) =
              ((List.range m).map (fun j => z j * (w i j : Int))).map (fun x => x * (b j0 : Int)) by
            simp [List.map_map]] at hsum
        rw [show (List.range m).map (fun j => (w i j0 : Int) * (z j * (b j : Int))) =
              ((List.range m).map (fun j => z j * (b j : Int))).map (fun x => (w i j0 : Int) * x) by
            simp [List.map_map]] at hsum
        rw [sum_map_mul_right, sum_map_mul_left, hsum_b] at hsum
        simpa [S] using hsum
      let a : Nat → Nat := fun i => (S i).toNat
      have hSnonneg : ∀ i ∈ List.range n, 0 ≤ S i := by
        intro i hi
        by_cases hs : 0 ≤ S i
        · exact hs
        · have hsneg : S i < 0 := Int.lt_of_not_ge hs
          have hbpos : (0 : Int) < (b j0 : Int) := by exact_mod_cast hbj0pos
          have hprod : S i * (b j0 : Int) < 0 := Int.mul_neg_of_neg_of_pos hsneg hbpos
          rw [hSmul i hi] at hprod
          have hnonneg : (0 : Int) ≤ (w i j0 : Int) := by exact_mod_cast (Nat.zero_le _)
          omega
      have ha_eq : ∀ i ∈ List.range n, b j0 * a i = w i j0 := by
        intro i hi
        have hint : ((b j0 * a i : Nat) : Int) = (w i j0 : Int) := by
          dsimp [a]
          rw [Int.toNat_of_nonneg (hSnonneg i hi)]
          have hs := hSmul i hi
          grind
        exact_mod_cast hint
      have hentry : ∀ i ∈ List.range n, ∀ j ∈ List.range m, w i j = a i * b j := by
        intro i hi j hj
        have h1 : w i j * b j0 = (a i * b j) * b j0 := by
          rw [hcrossEq i hi j hj, ← ha_eq i hi]
          grind
        exact Nat.eq_of_mul_eq_mul_right hbj0pos h1
      refine ⟨a, b, ?_⟩
      intro x y
      exact full_eq_of_entry n m w a b x y hentry
    · refine ⟨fun _ => 0, fun _ => 0, ?_⟩
      intro x y
      have hz : ∀ i ∈ List.range n, ∀ j ∈ List.range m, w i j = 0 := by
        intro i hi j hj
        by_cases hz' : w i j = 0
        · exact hz'
        · exact False.elim (hEx ⟨i, hi, j, hj, hz'⟩)
      have hrowzero : ∀ i ∈ List.range n, dot m (w i) y = 0 := by
        intro i hi
        unfold dot
        have hmap : (List.range m).map (fun j => (w i j : Int) * y j) =
            (List.range m).map (fun _ => 0) := by
          apply List.map_congr_left
          intro j hj
          rw [hz i hi j hj]
          simp
        rw [hmap]
        exact sum_map_zero _
      unfold full
      have hmap : (List.range n).map (fun i => x i * dot m (w i) y) =
          (List.range n).map (fun _ => 0) := by
        apply List.map_congr_left
        intro i hi
        rw [hrowzero i hi]
        simp
      rw [hmap]
      rw [sum_map_zero]
      have hdot0n : dot n (fun _ : Nat => 0) x = 0 := by
        unfold dot
        have hm : (List.range n).map (fun i => ((0 : Nat) : Int) * x i) =
            (List.range n).map (fun _ : Nat => (0 : Int)) := by simp
        rw [hm]
        exact sum_map_zero _
      have hdot0m : dot m (fun _ : Nat => 0) y = 0 := by
        unfold dot
        have hm : (List.range m).map (fun j => ((0 : Nat) : Int) * y j) =
            (List.range m).map (fun _ : Nat => (0 : Int)) := by simp
        rw [hm]
        exact sum_map_zero _
      rw [hdot0n, hdot0m]
      grind
  · rintro ⟨a, b, h⟩
    intro i hi j hj k hk l hl
    have hi' : i < n := by simpa using hi
    have hj' : j < n := by simpa using hj
    have hk' : k < m := by simpa using hk
    have hl' : l < m := by simpa using hl
    have coef (ii : Nat) (hii : ii < n) (kk : Nat) (hkk : kk < m) :
        w ii kk = a ii * b kk := by
      have hh := h (fun p => if p == ii then 1 else 0) (fun q => if q == kk then 1 else 0)
      rw [full_indicator n m ii kk hii hkk w] at hh
      rw [dot_indicator n ii hii a] at hh
      rw [dot_indicator m kk hkk b] at hh
      exact_mod_cast hh
    have hwik := coef i hi' k hk'
    have hwil := coef i hi' l hl'
    have hwjk := coef j hj' k hk'
    have hwjl := coef j hj' l hl'
    rw [hwik, hwil, hwjk, hwjl]
    grind



end TinylambOpt.ResearchProofs.RestartRaces.JobJ3_RankOneTile
