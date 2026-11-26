import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Common

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Named results include `Rt_append`, `It_iff_Rt`, `Rt_walk`.
-/

set_option maxHeartbeats 0
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

def Tr (m : Model) (g : List Edge) (p y : Node) : Prop :=
  ∃ e ∈ g, e.src = p.1 ∧ m.next p.2 e.label = some y.2 ∧ e.dst = y.1

def Rt (m : Model) (g : List Edge) : Nat → Node → Node → Prop
  | 0, p0, p => p = p0
  | t+1, p0, p => ∃ pm, Rt m g t p0 pm ∧ Tr m g pm p

theorem Rt_append (m : Model) (g : List Edge) (a : Nat) (p0 p1 : Node) (h1 : Rt m g a p0 p1) :
    ∀ b p2, Rt m g b p1 p2 → Rt m g (a+b) p0 p2 := by
  intro b
  induction b with
  | zero => intro p2 h; subst h; exact h1
  | succ b ih =>
    intro p2 ⟨pm, h, htr⟩
    exact ⟨pm, ih pm h, htr⟩

theorem It_iff_Rt (m : Model) (g : List Edge) (s : Node) :
    ∀ n p, It m g n (fun p => p = s) p ↔ ∃ t, t ≤ n ∧ Rt m g t s p := by
  intro n
  induction n with
  | zero =>
    intro p
    constructor
    · intro h; exact ⟨0, Nat.le_refl _, h⟩
    · rintro ⟨t, ht, h⟩
      have : t = 0 := by omega
      subst this; exact h
  | succ n ih =>
    intro p
    rw [It_succ]
    constructor
    · rintro (h | ⟨p', hp', htr⟩)
      · obtain ⟨t, ht, hr⟩ := (ih p).mp h
        exact ⟨t, by omega, hr⟩
      · obtain ⟨t, ht, hr⟩ := (ih p').mp hp'
        exact ⟨t+1, by omega, ⟨p', hr, htr⟩⟩
    · rintro ⟨t, ht, hr⟩
      by_cases htn : t ≤ n
      · exact Or.inl ((ih p).mpr ⟨t, htn, hr⟩)
      · have : t = n + 1 := by omega
        subst this
        obtain ⟨pm, hr', htr⟩ := hr
        exact Or.inr ⟨pm, (ih pm).mpr ⟨n, Nat.le_refl _, hr'⟩, htr⟩

theorem Rt_walk (m : Model) (g : List Edge) (n : Nat) : ∀ p0 p, Rt m g n p0 p →
    ∃ f : Nat → Node, f 0 = p0 ∧ (∀ i, i < n → Tr m g (f i) (f (i+1))) ∧ f n = p := by
  induction n with
  | zero => intro p0 p h; exact ⟨fun _ => p0, rfl, fun i hi => absurd hi (by omega), h.symm⟩
  | succ n ih =>
    intro p0 p ⟨pm, hr, htr⟩
    obtain ⟨f, h0, hs, hn⟩ := ih p0 pm hr
    refine ⟨fun i => if i ≤ n then f i else p, ?_, ?_, ?_⟩
    · show (if 0 ≤ n then f 0 else p) = p0
      rw [ite_eq_left (Nat.zero_le _), h0]
    · intro i hi
      show Tr m g (if i ≤ n then f i else p) (if i + 1 ≤ n then f (i+1) else p)
      by_cases hin : i < n
      · rw [ite_eq_left (by omega), ite_eq_left (by omega)]
        exact hs i hin
      · have : i = n := by omega
        subst this
        rw [ite_eq_left (Nat.le_refl _), ite_eq_right (by omega), hn]
        exact htr
    · show (if n + 1 ≤ n then f (n+1) else p) = p
      rw [ite_eq_right (by omega)]

theorem walk_Rt (m : Model) (g : List Edge) (f : Nat → Node) :
    ∀ n, (∀ i, i < n → Tr m g (f i) (f (i+1))) → Rt m g n (f 0) (f n) := by
  intro n
  induction n with
  | zero => intro _; rfl
  | succ n ih =>
    intro h
    exact ⟨f n, ih (fun i hi => h i (by omega)), h n (by omega)⟩

theorem reach_shorten (m : Model) (g : List Edge) (v : Nat) (rk : Nat → Nat)
    (M1 : ∀ q x r, m.next q x = some r → rk q ≤ rk r)
    (M2 : ∀ q q' x r', rk q ≤ rk q' → m.next q' x = some r' → ∃ r, m.next q x = some r ∧ rk r ≤ rk r') :
    ∀ n p, Rt m g n (v,0) p → ∃ t, t < (vertices g v).length ∧ ∃ p', Rt m g t (v,0) p' ∧ p'.1 = p.1 := by
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
    intro p hp
    by_cases hn : n < (vertices g v).length
    · exact ⟨n, hn, p, hp, rfl⟩
    · obtain ⟨f, h0, hs, hfn⟩ := Rt_walk m g n _ _ hp
      -- vertices stay inside
      have hV : ∀ i, i ≤ n → (f i).1 ∈ vertices g v := by
        intro i
        induction i with
        | zero => intro _; rw [h0]; exact vertices_mem_self g v
        | succ i ihi =>
          intro hi
          obtain ⟨e, he, _, _, h4⟩ := hs i (by omega)
          rw [← h4]; exact vertices_mem_dst g v e he
      -- rank monotone along the walk
      have hR : ∀ i j, i ≤ j → j ≤ n → rk (f i).2 ≤ rk (f j).2 := by
        intro i j hij
        induction j with
        | zero => intro _; have : i = 0 := by omega
                  subst this; exact Nat.le_refl _
        | succ j ihj =>
          intro hj
          by_cases hij' : i ≤ j
          · obtain ⟨e, he, _, h2, _⟩ := hs j (by omega)
            exact Nat.le_trans (ihj hij' (by omega)) (M1 _ _ _ h2)
          · have : i = j + 1 := by omega
            subst this; exact Nat.le_refl _
      -- pigeonhole
      have hdup : ∃ i j, i < j ∧ j ≤ n ∧ (f i).1 = (f j).1 := by
        refine Classical.byContradiction fun hno => ?_
        have p1 := List.pairwise_lt_range (n := n+1)
        have p2 : List.Pairwise (fun a b => (f a).1 ≠ (f b).1) (List.range (n+1)) :=
          List.Pairwise.imp_of_mem (fun {a b} ha hb hlt h => hno ⟨a, b, hlt, by
            have := List.mem_range.mp hb; omega, h⟩) p1
        have p3 : (List.map (fun i => (f i).1) (List.range (n+1))).Nodup := List.pairwise_map.mpr p2
        have p4 := List.Nodup.length_le_of_subset p3 (l₂ := vertices g v) (by
          intro y hy
          obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hy
          exact hV i (by have := List.mem_range.mp hi; omega))
        simp at p4
        omega
      obtain ⟨i, j, hij, hjn, hv⟩ := hdup
      -- suffix replay
      have hsuf : Rt m g (n - j) (f j) (f n) := by
        have := walk_Rt m g (fun k => f (j + k)) (n - j) (fun k hk => by
          have := hs (j + k) (by omega)
          simpa [Nat.add_assoc] using this)
        have e : j + (n - j) = n := by omega
        simpa [e] using this
      have hpre : Rt m g i (v,0) (f i) := by
        have := walk_Rt m g f i (fun k hk => hs k (by omega))
        rw [h0] at this; exact this
      have hrep : ∀ t pe, Rt m g t (f j) pe → ∃ q', Rt m g t (f i) q' ∧ q'.1 = pe.1 ∧ rk q'.2 ≤ rk pe.2 := by
        intro t
        induction t with
        | zero =>
          intro pe h
          subst h
          exact ⟨f i, rfl, hv, hR i j (by omega) hjn⟩
        | succ t iht =>
          intro pe ⟨pm, hr, e, he, h1, h2, h3⟩
          obtain ⟨q', hq', hq1, hq2⟩ := iht pm hr
          obtain ⟨r, hr1, hr2⟩ := M2 q'.2 pm.2 e.label pe.2 hq2 h2
          exact ⟨(e.dst, r), ⟨q', hq', e, he, by rw [h1, hq1], hr1, rfl⟩, h3.symm ▸ rfl, hr2⟩
      obtain ⟨q', hq', hq1, _⟩ := hrep _ _ hsuf
      have hcomp := Rt_append m g i _ _ hpre _ _ hq'
      have hlt : i + (n - j) < n := by omega
      obtain ⟨t, ht, p', hp', hp'1⟩ := ih (i + (n - j)) hlt q' hcomp
      exact ⟨t, ht, p', hp', by rw [hp'1, hq1, hfn]⟩


theorem plainDepth_of_monotone (m : Model) (hacc : ∀ q, m.accept q = true) (rk : Nat → Nat)
    (M1 : ∀ q x r, m.next q x = some r → rk q ≤ rk r)
    (M2 : ∀ q q' x r', rk q ≤ rk q' → m.next q' x = some r' → ∃ r, m.next q x = some r ∧ rk r ≤ rk r') :
    plainDepth m := by
  intro g v n x hx
  rw [query_mem] at hx ⊢
  obtain ⟨q, hq, _⟩ := hx
  obtain ⟨t, ht, hr⟩ := (It_iff_Rt m g _ n _).mp hq
  obtain ⟨t', ht', p', hp', hp1⟩ := reach_shorten m g v rk M1 M2 t _ hr
  have hpp : p' = (x, p'.2) := Prod.ext hp1 rfl
  refine ⟨p'.2, (It_iff_Rt m g _ _ _).mpr ⟨t', by omega, ?_⟩, hacc _⟩
  rw [← hpp]; exact hp'

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic
