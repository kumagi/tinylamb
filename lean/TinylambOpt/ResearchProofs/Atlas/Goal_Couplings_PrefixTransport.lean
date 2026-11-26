import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.

Proved

- `Couplings.PrefixTransport` — `∀ rs, let bins := ((rs.map Prod.fst)++(rs.map
  Prod.snd)).mergeSort.eraseDups cost (monotone rs) = ((bins.zip (bins.drop 1)).map
  (fun (a,b) => (b-a).toNat * ((rs.countP (·.1 ≤ a):Int)-rs.countP (·.2 ≤
  a)).natAbs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings

/-! ### Sorting utilities -/

private def intLe (a b : Int) : Bool := a ≤ b

private theorem intLe_trans (a b c : Int) : intLe a b → intLe b c → intLe a c := by
  simp [intLe]
  omega

private theorem intLe_total (a b : Int) : intLe a b || intLe b a := by
  unfold intLe
  by_cases h : a ≤ b
  · simp [h]
  · have h' : b ≤ a := by omega
    simp [h, h']

private theorem sorted_mergeSort (l : List Int) : l.mergeSort.Pairwise (fun a b : Int => a ≤ b) := by
  have hp := List.pairwise_mergeSort intLe_trans intLe_total l
  exact hp.imp (by intro a b h; simpa [intLe] using h)

private theorem mergeSort_eq_of_perm {l₁ l₂ : List Int} (h : List.Perm l₁ l₂) :
    l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    (fun a b _ _ hab hba => by omega)
  · exact sorted_mergeSort l₁
  · exact sorted_mergeSort l₂
  · exact (List.mergeSort_perm l₁ intLe).trans (h.trans (List.mergeSort_perm l₂ intLe).symm)

private theorem eraseDups_pairwise_le : ∀ l : List Int,
    l.Pairwise (fun a b : Int => a ≤ b) → l.eraseDups.Pairwise (fun a b : Int => a ≤ b)
  | [], h => by simp
  | a :: t, h => by
      rw [List.eraseDups_cons]
      rw [List.pairwise_cons] at h ⊢
      refine ⟨?_, ?_⟩
      · intro b hb
        exact h.1 b ((List.mem_filter.mp (List.mem_eraseDups.mp hb)).1)
      · exact eraseDups_pairwise_le (t.filter (fun b => !b == a)) (h.2.filter _)
termination_by l => l.length
decreasing_by simp_wf; exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

private theorem eraseDups_nodup : ∀ l : List Int, l.eraseDups.Nodup
  | [] => by simp
  | a :: t => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      refine ⟨?_, eraseDups_nodup (t.filter (fun b => !b == a))⟩
      intro ha
      have hf := List.mem_filter.mp (List.mem_eraseDups.mp ha)
      have hne : (!a == a) = true := hf.2
      simp at hne
termination_by l => l.length
decreasing_by simp_wf; exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

private theorem pairwise_lt_of_le_of_nodup {l : List Int}
    (hle : l.Pairwise (fun a b : Int => a ≤ b)) (hnd : l.Nodup) :
    l.Pairwise (fun a b : Int => a < b) := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.pairwise_cons] at hle ⊢
      rw [List.nodup_cons] at hnd
      refine ⟨?_, ih hle.2 hnd.2⟩
      intro b hb
      have hab_le := hle.1 b hb
      have hne : a ≠ b := by
        intro hEq
        subst b
        exact hnd.1 hb
      omega

private theorem bins_pairwise_lt (xs ys : List Int) :
    ((xs ++ ys).mergeSort.eraseDups).Pairwise (fun a b : Int => a < b) := by
  apply pairwise_lt_of_le_of_nodup
  · exact eraseDups_pairwise_le _ (sorted_mergeSort _)
  · exact eraseDups_nodup _

/-! ### Cost as cumulative absolute differences -/

private def pc : List Int → List Int → Nat
  | [], _ => 0
  | _ :: _, [] => 0
  | x :: xs, y :: ys => (x - y).natAbs + pc xs ys

private theorem pc_eq_zip (xs ys : List Int) :
    pc xs ys = ((xs.zip ys).map (fun p => (p.1 - p.2).natAbs)).sum := by
  induction xs generalizing ys with
  | nil => simp [pc]
  | cons x xs ih =>
      cases ys with
      | nil => simp [pc]
      | cons y ys => simp [pc, ih]

private theorem zip_map_rows (rs : Rows) : (rs.map Prod.fst).zip (rs.map Prod.snd) = rs := by
  induction rs with
  | nil => simp
  | cons r rs ih => cases r <;> simp [ih]

private theorem cost_eq_pc (rs : Rows) :
    cost rs = pc (rs.map Prod.fst) (rs.map Prod.snd) := by
  rw [pc_eq_zip, zip_map_rows]
  rfl

private theorem cost_mono_eq_pc (rs : Rows) :
    cost (monotone rs) = pc (rs.map Prod.fst).mergeSort (rs.map Prod.snd).mergeSort := by
  rw [pc_eq_zip]
  simp [monotone, cost]

/-! ### Membership in a zip -/

private theorem mem_zip_fst : ∀ {a b : Int} {as bs : List Int},
    (a, b) ∈ as.zip bs → a ∈ as
  | a, b, [], bs, h => by simp at h
  | a, b, c :: cs, [], h => by simp at h
  | a, b, c :: cs, d :: ds, h => by
      simp only [List.zip_cons_cons, List.mem_cons] at h
      simp only [List.mem_cons]
      rcases h with hEq | h
      · left
        exact congrArg Prod.fst hEq
      · right
        exact mem_zip_fst h

private theorem mem_zip_snd : ∀ {a b : Int} {as bs : List Int},
    (a, b) ∈ as.zip bs → b ∈ bs
  | a, b, [], bs, h => by simp at h
  | a, b, c :: cs, [], h => by simp at h
  | a, b, c :: cs, d :: ds, h => by
      simp only [List.zip_cons_cons, List.mem_cons] at h
      simp only [List.mem_cons]
      rcases h with hEq | h
      · left
        exact congrArg Prod.snd hEq
      · right
        exact mem_zip_snd h

private theorem zip_drop_mem_fst : ∀ (m : List Int) (g : Int × Int),
    g ∈ m.zip (m.drop 1) → g.1 ∈ m
  | [], g, h => by simp at h
  | a :: [], g, h => by simp at h
  | a :: b :: t, g, h => by
      simp at h
      simp only [List.mem_cons]
      rcases h with hEq | h
      · left
        exact congrArg Prod.fst hEq
      · right
        simpa only [List.mem_cons] using zip_drop_mem_fst (b :: t) g h

/-! ### One row decomposes into the gaps it straddles -/

private def between (x y a : Int) : Prop := (x ≤ a ∧ a < y) ∨ (y ≤ a ∧ a < x)
private instance (x y a : Int) : Decidable (between x y a) := by
  unfold between
  infer_instance
private def btwnB (x y a : Int) : Bool := decide (between x y a)
private def gapW (g : Int × Int) : Nat := (g.2 - g.1).toNat

private theorem btwnB_change_first {a b u v : Int} (hab : a < b) (hbv : b ≤ v) :
    btwnB a u v = btwnB b u v := by
  by_cases hvu : v < u
  · have h1 : btwnB a u v = true := by
      apply decide_eq_true
      unfold between
      exact Or.inl ⟨by omega, hvu⟩
    have h2 : btwnB b u v = true := by
      apply decide_eq_true
      unfold between
      exact Or.inl ⟨hbv, hvu⟩
    rw [h1, h2]
  · have h1 : ¬ between a u v := by
      unfold between
      rintro (h | h) <;> omega
    have h2 : ¬ between b u v := by
      unfold between
      rintro (h | h) <;> omega
    simp [btwnB, h1, h2]

private theorem btwnB_change_lower {a b u v : Int} (hab : a < b) (hbv : b ≤ v) :
    btwnB u a v = btwnB u b v := by
  by_cases hvu : v < u
  · have h1 : btwnB u a v = true := by
      apply decide_eq_true
      unfold between
      exact Or.inr ⟨by omega, hvu⟩
    have h2 : btwnB u b v = true := by
      apply decide_eq_true
      unfold between
      exact Or.inr ⟨hbv, hvu⟩
    rw [h1, h2]
  · have h1 : ¬ between u a v := by
      unfold between
      rintro (h | h) <;> omega
    have h2 : ¬ between u b v := by
      unfold between
      rintro (h | h) <;> omega
    simp [btwnB, h1, h2]

private theorem row_gap : ∀ (bins : List Int),
    bins.Pairwise (fun a b : Int => a < b) → ∀ (x y : Int),
    x ∈ bins → y ∈ bins →
    (x - y).natAbs =
      (((bins.zip (bins.drop 1)).filter (fun g => btwnB x y g.1)).map gapW).sum := by
  intro bins
  induction bins with
  | nil =>
      intro h x y hx
      simp at hx
  | cons a t ih =>
      intro h x y hx hy
      cases t with
      | nil =>
          simp at hx hy
          subst x
          subst y
          simp [btwnB, between]
      | cons b t =>
          have hp := List.pairwise_cons.mp h
          have hab : a < b := hp.1 b (by simp)
          have htail : (b :: t).Pairwise (fun u v : Int => u < v) := hp.2
          have hhead : ∀ z ∈ b :: t, a < z := hp.1
          have hbmin : ∀ z ∈ t, b < z := (List.pairwise_cons.mp htail).1
          have hzip : (a :: b :: t).zip ((a :: b :: t).drop 1) = (a, b) :: (b :: t).zip t := by
            simp
          rcases List.mem_cons.mp hx with hxa | hx
          · subst x
            rcases List.mem_cons.mp hy with hya | hy
            · subst y
              have hfilt : ((a :: b :: t).zip ((a :: b :: t).drop 1)).filter
                    (fun g => btwnB a a g.1) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro g hg
                simp [btwnB, between]
              rw [hfilt]
              simp
            · have hay : a < y := hhead y hy
              have hby : b ≤ y := by
                rcases List.mem_cons.mp hy with hyb | hyt
                · subst y
                  omega
                · have := hbmin y hyt
                  omega
              have hpred : ∀ g ∈ (b :: t).zip t,
                  btwnB a y g.1 = btwnB b y g.1 := by
                intro g hg
                have hgmem : g.1 ∈ b :: t := zip_drop_mem_fst (b :: t) g (by simpa using hg)
                have hbc : b ≤ g.1 := by
                  rcases List.mem_cons.mp hgmem with hgb | hgt
                  · omega
                  · have := hbmin g.1 hgt
                    omega
                exact (btwnB_change_first (a := a) (b := b) (u := y) (v := g.1) hab hbc)
              rw [hzip]
              simp only [List.filter_cons]
              have hfirst : btwnB a y a = true := by simp [btwnB, between, hay]
              rw [ite_eq_left hfirst]
              have hfilt : ((b :: t).zip t).filter (fun g => btwnB a y g.1) =
                  ((b :: t).zip t).filter (fun g => btwnB b y g.1) := by
                apply List.filter_congr
                intro g hg
                exact hpred g hg
              rw [hfilt]
              have ih' := ih htail b y (by simp) hy
              simp only [List.map_cons, List.sum_cons]
              have ih'' :
                  (b - y).natAbs =
                    List.sum (List.map gapW (((b :: t).zip t).filter (fun g : Int × Int => btwnB b y g.1))) := by
                simpa using ih'
              rw [← ih'']
              simp [gapW]
              omega
          · rcases List.mem_cons.mp hy with hya | hy
            · subst y
              have hax : a < x := hhead x hx
              have hbx : b ≤ x := by
                rcases List.mem_cons.mp hx with hxb | hxt
                · subst x
                  omega
                · have := hbmin x hxt
                  omega
              have hpred : ∀ g ∈ (b :: t).zip t,
                  btwnB x a g.1 = btwnB x b g.1 := by
                intro g hg
                have hgmem : g.1 ∈ b :: t := zip_drop_mem_fst (b :: t) g (by simpa using hg)
                have hbc : b ≤ g.1 := by
                  rcases List.mem_cons.mp hgmem with hgb | hgt
                  · omega
                  · have := hbmin g.1 hgt
                    omega
                exact (btwnB_change_lower (a := a) (b := b) (u := x) (v := g.1) hab hbc)
              rw [hzip]
              simp only [List.filter_cons]
              have hfirst : btwnB x a a = true := by simp [btwnB, between, hax]
              rw [ite_eq_left hfirst]
              have hfilt : ((b :: t).zip t).filter (fun g => btwnB x a g.1) =
                  ((b :: t).zip t).filter (fun g => btwnB x b g.1) := by
                apply List.filter_congr
                intro g hg
                exact hpred g hg
              rw [hfilt]
              have ih' := ih htail x b hx (by simp)
              simp only [List.map_cons, List.sum_cons]
              have ih'' :
                  (x - b).natAbs =
                    List.sum (List.map gapW (((b :: t).zip t).filter (fun g : Int × Int => btwnB x b g.1))) := by
                simpa using ih'
              rw [← ih'']
              simp [gapW]
              omega
            · have hfirst : ¬ btwnB x y a := by
                have hxa : a < x := hhead x hx
                have hya : a < y := hhead y hy
                simp [btwnB, between]
                constructor <;> intro hh <;> omega
              rw [hzip]
              simp only [List.filter_cons, ite_eq_right hfirst]
              exact ih htail x y hx hy

/-! ### Sorted columns: a prefix count difference counts straddling rows -/

private theorem count_between_eq (xs ys : List Int)
    (hxs : xs.Pairwise (fun a b : Int => a ≤ b))
    (hys : ys.Pairwise (fun a b : Int => a ≤ b))
    (hlen : xs.length = ys.length) (a : Int) :
    (xs.zip ys).countP (fun p => btwnB p.1 p.2 a) =
      (((xs.countP (fun z : Int => decide (z ≤ a)) : Int) -
        (ys.countP (fun z : Int => decide (z ≤ a)) : Int)).natAbs) := by
  revert ys hxs hys hlen
  induction xs with
  | nil =>
      intro ys hxs hys hlen
      have hynil : ys = [] := by simpa using hlen.symm
      subst ys
      simp
  | cons x xs ih =>
      intro ys hxs hys hlen
      cases ys with
      | nil => simp at hlen
      | cons y ys =>
          have hlen' : xs.length = ys.length := by simp at hlen; omega
          have hxs' : xs.Pairwise (fun a b : Int => a ≤ b) := (List.pairwise_cons.mp hxs).2
          have hys' : ys.Pairwise (fun a b : Int => a ≤ b) := (List.pairwise_cons.mp hys).2
          have ih' := ih ys hxs' hys' hlen'
          by_cases hxa : x ≤ a
          · by_cases hya : y ≤ a
            · have hb0 : btwnB x y a = false := by simp [btwnB, between, hxa, hya]
              simp only [List.zip_cons_cons, List.countP_cons]
              rw [ih']
              simp [hxa, hya, hb0]
              omega
            · have hymin : ∀ z ∈ ys, y ≤ z := (List.pairwise_cons.mp hys).1
              have hcy0 : ys.countP (fun z : Int => decide (z ≤ a)) = 0 := by
                apply List.countP_eq_zero.mpr
                intro z hz
                have hyz := hymin z hz
                have hzgt : ¬ z ≤ a := by omega
                simp [hzgt]
              have hay : a < y := by omega
              have hb1 : btwnB x y a = true := by simp [btwnB, between, hxa, hya, hay]
              simp only [List.zip_cons_cons, List.countP_cons]
              rw [ih']
              simp [hxa, hya, hcy0, hb1]
              omega
          · by_cases hya : y ≤ a
            · have hxmin : ∀ z ∈ xs, x ≤ z := (List.pairwise_cons.mp hxs).1
              have hcx0 : xs.countP (fun z : Int => decide (z ≤ a)) = 0 := by
                apply List.countP_eq_zero.mpr
                intro z hz
                have hxz := hxmin z hz
                have hzgt : ¬ z ≤ a := by omega
                simp [hzgt]
              have hax : a < x := by omega
              have hb1 : btwnB x y a = true := by simp [btwnB, between, hxa, hya, hax]
              simp only [List.zip_cons_cons, List.countP_cons]
              rw [ih']
              simp [hxa, hya, hcx0, hb1]
              omega
            · have hxmin : ∀ z ∈ xs, x ≤ z := (List.pairwise_cons.mp hxs).1
              have hymin : ∀ z ∈ ys, y ≤ z := (List.pairwise_cons.mp hys).1
              have hcx0 : xs.countP (fun z : Int => decide (z ≤ a)) = 0 := by
                apply List.countP_eq_zero.mpr
                intro z hz
                have hxz := hxmin z hz
                have hzgt : ¬ z ≤ a := by omega
                simp [hzgt]
              have hcy0 : ys.countP (fun z : Int => decide (z ≤ a)) = 0 := by
                apply List.countP_eq_zero.mpr
                intro z hz
                have hyz := hymin z hz
                have hzgt : ¬ z ≤ a := by omega
                simp [hzgt]
              have hb0 : btwnB x y a = false := by
                simp [btwnB, between, hxa, hya] <;> omega
              simp only [List.zip_cons_cons, List.countP_cons]
              rw [ih']
              simp [hxa, hya, hcx0, hcy0, hb0]

/-! ### Swapping the two sums -/

private theorem sum_map_add {α : Type} (f g : α → Nat) : ∀ l : List α,
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum
  | [] => by simp
  | a :: l => by simp [sum_map_add f g l]; omega

private theorem sum_map_mul_ind (w : Nat) (p : α → Bool) : ∀ l : List α,
    (l.map (fun x => w * (if p x = true then 1 else 0))).sum = w * l.countP p
  | [] => by simp
  | a :: l => by
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      by_cases h : p a = true
      · simp [h, Nat.mul_succ, sum_map_mul_ind w p l, Nat.add_comm]
      · simp [h, sum_map_mul_ind w p l]

private theorem sum_map_zero {α : Type} : ∀ l : List α, (l.map (fun _ => 0)).sum = 0
  | [] => by simp
  | a :: l => by simp [sum_map_zero l]

private theorem swap_gap_rows (G : List (Int × Int)) (P : List (Int × Int)) :
    (G.map (fun g => gapW g * P.countP (fun p => btwnB p.1 p.2 g.1))).sum =
      (P.map (fun p => ((G.filter (fun g => btwnB p.1 p.2 g.1)).map gapW).sum)).sum := by
  induction G with
  | nil => simp [sum_map_zero]
  | cons g G ih =>
      rw [List.map_cons, List.sum_cons, ih]
      have hsplit : ∀ p : Int × Int,
          (((g :: G).filter (fun h => btwnB p.1 p.2 h.1)).map gapW).sum =
            gapW g * (if btwnB p.1 p.2 g.1 = true then 1 else 0) +
              ((G.filter (fun h => btwnB p.1 p.2 h.1)).map gapW).sum := by
        intro p
        by_cases h : btwnB p.1 p.2 g.1 = true
        · simp [h]
        · simp [h]
      simp only [hsplit]
      rw [sum_map_add, sum_map_mul_ind]

/-! ### The CDF-difference formula for two sorted columns -/

private theorem sorted_pc_eq_gap (xs ys : List Int)
    (hxs : xs.Pairwise (fun a b : Int => a ≤ b))
    (hys : ys.Pairwise (fun a b : Int => a ≤ b))
    (hlen : xs.length = ys.length) :
    let bins := (xs ++ ys).mergeSort.eraseDups
    pc xs ys =
      ((bins.zip (bins.drop 1)).map (fun g =>
        gapW g * (((xs.countP (fun z : Int => decide (z ≤ g.1)) : Int) -
          (ys.countP (fun z : Int => decide (z ≤ g.1)) : Int)).natAbs))).sum := by
  dsimp
  let bins := (xs ++ ys).mergeSort.eraseDups
  have hbinslt : bins.Pairwise (fun a b : Int => a < b) := by
    dsimp [bins]
    exact bins_pairwise_lt xs ys
  have hrow : ∀ p ∈ xs.zip ys,
      (p.1 - p.2).natAbs =
        (((bins.zip (bins.drop 1)).filter (fun g => btwnB p.1 p.2 g.1)).map gapW).sum := by
    intro p hp
    have hp1 : p.1 ∈ xs := mem_zip_fst hp
    have hp2 : p.2 ∈ ys := mem_zip_snd hp
    apply row_gap bins hbinslt
    · dsimp [bins]
      exact List.mem_eraseDups.mpr (List.mem_mergeSort.mpr (List.mem_append.mpr (Or.inl hp1)))
    · dsimp [bins]
      exact List.mem_eraseDups.mpr (List.mem_mergeSort.mpr (List.mem_append.mpr (Or.inr hp2)))
  rw [pc_eq_zip]
  have hmap := List.map_congr_left (l := xs.zip ys) hrow
  rw [hmap]
  rw [← swap_gap_rows (bins.zip (bins.drop 1)) (xs.zip ys)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro g hg
  rw [← count_between_eq xs ys hxs hys hlen g.1]

/-! ### Conclusion -/

theorem sound_Couplings_PrefixTransport : Couplings.PrefixTransport.Goal := by
  intro rs
  let X := rs.map Prod.fst
  let Y := rs.map Prod.snd
  let bins := (X ++ Y).mergeSort.eraseDups
  have hcost : cost (monotone rs) = pc X.mergeSort Y.mergeSort := by
    dsimp [X, Y]
    exact cost_mono_eq_pc rs
  have hp : List.Perm (X ++ Y) (X.mergeSort ++ Y.mergeSort) :=
    ((List.mergeSort_perm X intLe).append (List.mergeSort_perm Y intLe)).symm
  have hm : (X ++ Y).mergeSort = (X.mergeSort ++ Y.mergeSort).mergeSort :=
    mergeSort_eq_of_perm hp
  have hbins : bins = (X.mergeSort ++ Y.mergeSort).mergeSort.eraseDups := by
    dsimp [bins]
    rw [hm]
  have hgap := sorted_pc_eq_gap X.mergeSort Y.mergeSort
    (sorted_mergeSort X) (sorted_mergeSort Y) (by simp [X, Y])
  dsimp at hgap
  change cost (monotone rs) =
    ((bins.zip (bins.drop 1)).map (fun g =>
      (g.2 - g.1).toNat *
        (((rs.countP (fun p : Int × Int => decide (p.1 ≤ g.1)) : Int) -
          (rs.countP (fun p : Int × Int => decide (p.2 ≤ g.1)) : Int)).natAbs))).sum
  have hcntX : ∀ a : Int,
      (X.mergeSort).countP (fun z : Int => decide (z ≤ a)) =
        rs.countP (fun p : Int × Int => decide (p.1 ≤ a)) := by
    intro a
    calc
      (X.mergeSort).countP (fun z : Int => decide (z ≤ a)) =
          X.countP (fun z : Int => decide (z ≤ a)) :=
        List.Perm.countP_eq (fun z : Int => decide (z ≤ a)) (List.mergeSort_perm X intLe)
      _ = rs.countP (fun p : Int × Int => decide (p.1 ≤ a)) := by
        simp [X, Function.comp_def]
  have hcntY : ∀ a : Int,
      (Y.mergeSort).countP (fun z : Int => decide (z ≤ a)) =
        rs.countP (fun p : Int × Int => decide (p.2 ≤ a)) := by
    intro a
    calc
      (Y.mergeSort).countP (fun z : Int => decide (z ≤ a)) =
          Y.countP (fun z : Int => decide (z ≤ a)) :=
        List.Perm.countP_eq (fun z : Int => decide (z ≤ a)) (List.mergeSort_perm Y intLe)
      _ = rs.countP (fun p : Int × Int => decide (p.2 ≤ a)) := by
        simp [Y, Function.comp_def]
  rw [hcost, hgap, hbins]
  simp [gapW, hcntX, hcntY]

end TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_PrefixTransport
