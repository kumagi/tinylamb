import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `EditFilter.ClassLength` — `∀ k s t (keep : Nat → Bool), Edits k s t → ((s.countP
  keep:Int)-t.countP keep).natAbs ≤ k`
- `EditFilter.GramLowerBound` — `∀ s t k q, 0 < q → Edits k s t → max (grams q
  s).length (grams q t).length ≤ shared (grams q s) (grams q t)+k*q`
- `EditFilter.HistogramFence` — `∀ k s t, Edits k s t → ((s++t).eraseDups.map (fun a =>
  ((s.count a:Int)-t.count a).natAbs)).sum ≤ 2*k`
-/

namespace TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem sum_erase_of_mem {α : Type} [BEq α] [LawfulBEq α] {f : α → Nat} {l : List α} {x : α} (h : x ∈ l) :
    (l.map f).sum = f x + ((l.erase x).map f).sum := by
  rcases List.exists_erase_eq h with ⟨l₁, l₂, hnot, rfl, herase⟩
  rw [herase]
  simp [List.map_append, List.sum_append]
  omega

private theorem eraseDups_filter_not {α : Type} [BEq α] [LawfulBEq α]
    (x : α) (l : List α) :
    (l.eraseDups).erase x = (l.filter fun b => !(b == x)).eraseDups := by
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons y ys =>
          have hn : n = ys.length + 1 := by simpa using hlen.symm
          have hflen : (ys.filter fun b => !(b == y)).length < n := by
            have := List.length_filter_le (fun b => !(b == y)) ys
            omega
          by_cases hyx : y = x
          · subst y
            rw [List.eraseDups_cons]
            simp
          · have hyx' : (y == x) = false := beq_eq_false_iff_ne.mpr hyx
            have hyx'' : ¬ (y == x) = true := by simp [hyx']
            rw [List.eraseDups_cons]
            rw [List.erase_cons_tail hyx'']
            congr 1
            rw [ih (ys.filter fun b => !(b == y)).length hflen
              (ys.filter fun b => !(b == y)) rfl]
            have hposx : ((fun b : α => !(b == x)) y) = true := by simp [hyx']
            rw [List.filter_cons_of_pos (p := fun b : α => !(b == x)) hposx, List.eraseDups_cons]
            congr 1
            rw [List.filter_filter, List.filter_filter]
            apply congrArg List.eraseDups
            apply congrArg (fun f : α → Bool => List.filter f ys)
            funext b
            cases hb1 : b == y <;> cases hb2 : b == x <;> simp [*]

private theorem sum_eraseDups_le {α : Type} [BEq α] [LawfulBEq α]
    {f g : α → Nat} (l m : List α)
    (hfg : ∀ x, f x ≤ g x) (hsub : ∀ x ∈ l, x ∈ m) :
    (l.eraseDups.map f).sum ≤ (m.eraseDups.map g).sum := by
  induction hlen : l.length using Nat.strongRecOn generalizing l m with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          have hn : n = xs.length + 1 := by simpa using hlen.symm
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          have hxm : x ∈ m := hsub x (by simp)
          have hsub' : ∀ y ∈ (xs.filter fun b => !(b == x)), y ∈ m.filter (fun b => !(b == x)) := by
            intro y hy
            have hyxs : y ∈ xs := (List.mem_filter.mp hy).1
            have hyne : y ≠ x := by
              have := (List.mem_filter.mp hy).2
              exact fun he => by subst y; simp at this
            exact (List.mem_filter).mpr ⟨hsub y (by simp [hyxs]), by simpa [beq_eq_false_iff_ne] using hyne⟩
          have hflen : (xs.filter fun b => !(b == x)).length < n := by
            have := List.length_filter_le (fun b => !(b == x)) xs
            omega
          have ih' := ih (xs.filter fun b => !(b == x)).length hflen
            (xs.filter fun b => !(b == x)) (m.filter (fun b => !(b == x))) hsub' rfl
          rw [← eraseDups_filter_not x m] at ih'
          have hsum := sum_erase_of_mem (f := g) (l := m.eraseDups)
            (x := x) ((List.mem_eraseDups).mpr hxm)
          have hxle := hfg x
          omega

private theorem count_filter_of_ne {α : Type} [BEq α] [LawfulBEq α]
    {x g : α} (hgx : g ≠ x) (l : List α) :
    (l.filter fun b => !(b == x)).count g = l.count g := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y = x
      · subst y
        have hxg : (x == g) = false := beq_eq_false_iff_ne.mpr (fun h => hgx h.symm)
        simp only [List.filter_cons, List.count_cons]
        simp [ih, hxg]
      · have hyx' : (y == x) = false := beq_eq_false_iff_ne.mpr hyx
        have hpos : ((fun b : α => !(b == x)) y) = true := by simp [hyx']
        rw [List.filter_cons_of_pos (p := fun b : α => !(b == x)) hpos]
        rw [List.count_cons, ih, List.count_cons]

private theorem count_add_filter_not {α : Type} [BEq α] [LawfulBEq α]
    (x : α) (l : List α) : l.count x + (l.filter fun b => !(b == x)).length = l.length := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y = x
      · subst y
        simp
        omega
      · have hyx' : (y == x) = false := beq_eq_false_iff_ne.mpr hyx
        have hpos : ((fun b : α => !(b == x)) y) = true := by simp [hyx']
        rw [List.filter_cons_of_pos (p := fun b : α => !(b == x)) hpos]
        rw [List.count_cons]
        simp [hyx']
        omega

private theorem sum_eraseDups_count (l : List Word) :
    (l.eraseDups.map (fun g => l.count g)).sum = l.length := by
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp at hlen; subst n; simp
      | cons x xs =>
          have hn : n = xs.length + 1 := by simpa using hlen.symm
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          rw [show (x::xs).count x = xs.count x + 1 by simp]
          have hcnt : ∀ g ∈ (xs.filter fun b => !(b == x)).eraseDups,
              (x::xs).count g = (xs.filter fun b => !(b == x)).count g := by
            intro g hg
            have hg' := (List.mem_eraseDups.mp hg)
            have hgxf : (g == x) = false := by simpa using (List.mem_filter.mp hg').2
            have hneq : g ≠ x := beq_eq_false_iff_ne.mp hgxf
            rw [count_filter_of_ne hneq]
            rw [List.count_cons]
            have hxg : (x == g) = false := beq_eq_false_iff_ne.mpr (fun h => hneq h.symm)
            simp [hxg]
          have hmap : ((xs.filter fun b => !(b == x)).eraseDups.map (fun g => (x::xs).count g)).sum =
              ((xs.filter fun b => !(b == x)).eraseDups.map (fun g => (xs.filter fun b => !(b == x)).count g)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left (l := (xs.filter fun b => !(b == x)).eraseDups)
              (f := fun g => (x::xs).count g) (g := fun g => (xs.filter fun b => !(b == x)).count g)
            intro g hg
            exact hcnt g hg
          rw [hmap]
          have hflen : (xs.filter fun b => !(b == x)).length < n := by
            have := List.length_filter_le (fun b => !(b == x)) xs
            omega
          rw [ih (xs.filter fun b => !(b == x)).length hflen (xs.filter fun b => !(b == x)) rfl]
          have hlen' := count_add_filter_not x xs
          omega

private theorem shared_sublist_mono {c a b : List Word} (h : c.Sublist a) : shared c b ≤ shared a b := by
  unfold shared
  apply sum_eraseDups_le
  · intro x
    have hx := List.Sublist.count_le x h
    omega
  · intro x hx
    exact List.Sublist.subset h hx

private theorem shared_self (a : List Word) : shared a a = a.length := by
  unfold shared
  simpa using (sum_eraseDups_count a)


private theorem edits_trans {i j : Nat} {a b c : Word} (hab : Edits j a b) (hbc : Edits i b c) : Edits (j+i) a c := by
  induction hbc with
  | refl s => simpa using hab
  | drop l r x h ih => simpa [Nat.add_assoc] using (Edits.drop l r x (ih hab))
  | add l r x h ih => simpa [Nat.add_assoc] using (Edits.add l r x (ih hab))
  | subst l r x y h ih => simpa [Nat.add_assoc] using (Edits.subst l r x y (ih hab))

private theorem edits_add_one (l r : Word) (x : Nat) : Edits 1 (l++r) (l++x::r) := by
  simpa using (Edits.add l r x (Edits.refl (l++r)))
private theorem edits_drop_one (l r : Word) (x : Nat) : Edits 1 (l++x::r) (l++r) := by
  simpa using (Edits.drop l r x (Edits.refl (l++x::r)))
private theorem edits_subst_one (l r : Word) (x y : Nat) : Edits 1 (l++x::r) (l++y::r) := by
  simpa using (Edits.subst l r x y (Edits.refl (l++x::r)))
private theorem edits_symm {k : Nat} {a b : Word} (h : Edits k a b) : Edits k b a := by
  induction h with
  | refl s => exact Edits.refl s
  | drop l r x h ih => simpa [Nat.add_comm] using edits_trans (edits_add_one l r x) ih
  | add l r x h ih => simpa [Nat.add_comm] using edits_trans (edits_drop_one l r x) ih
  | subst l r x y h ih => simpa [Nat.add_comm] using edits_trans (edits_subst_one l r y x) ih

private theorem sum_map_eq_zero_of_all_zero {α : Type} {f : α → Nat} {l : List α}
    (h : ∀ a ∈ l, f a = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ a ∈ xs, f a = 0 := by intro a ha; exact h a (by simp [ha])
      rw [hx, ih ht]

private theorem sum_indicator_eraseDups {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    (l : List α) (y : α) :
    (l.eraseDups.map (fun a => if a = y then 1 else 0)).sum = if y ∈ l then 1 else 0 := by
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          have hn : n = xs.length + 1 := by simpa using hlen.symm
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          by_cases hxy : x = y
          · subst x
            simp
            have hflen : (xs.filter fun b => !(b == y)).length < n := by
              have := List.length_filter_le (fun b => !(b == y)) xs
              omega
            rw [show ((xs.filter fun b => !(b == y)).eraseDups.map
                (fun a => if a = y then 1 else 0)).sum = 0 by
              apply sum_map_eq_zero_of_all_zero
              intro a ha
              have ha' := (List.mem_eraseDups.mp ha)
              have hyf : (a == y) = false := by simpa using (List.mem_filter.mp ha').2
              have hay : a ≠ y := beq_eq_false_iff_ne.mp hyf
              simp [hay]]
          · have hxy' : (x == y) = false := beq_eq_false_iff_ne.mpr hxy
            simp [hxy]
            have hflen : (xs.filter fun b => !(b == x)).length < n := by
              have := List.length_filter_le (fun b => !(b == x)) xs
              omega
            rw [ih (xs.filter fun b => !(b == x)).length hflen
              (xs.filter fun b => !(b == x)) rfl]
            have hyx' : (y == x) = false := beq_eq_false_iff_ne.mpr (fun h => hxy (h).symm)
            have hmem : y ∈ (xs.filter fun b : α => !(b == x)) ↔ y ∈ xs := by
              constructor
              · intro hy; exact (List.mem_filter.mp hy).1
              · intro hy; exact (List.mem_filter).mpr ⟨hy, by simp [hyx']⟩
            have hyx : y ≠ x := fun h => hxy h.symm
            by_cases hys : y ∈ xs
            · simp [hmem.mpr hys, hys]
            · simp [hys, hyx]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun a => f a + g a)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

private theorem sum_eraseDups_count_le {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    (l m : List α) :
    (l.eraseDups.map (fun a => m.count a)).sum ≤ m.length := by
  induction m with
  | nil => simp; apply sum_map_eq_zero_of_all_zero; intro a ha; rfl
  | cons y ys ih =>
      have hbump : (l.eraseDups.map (fun a => (y::ys).count a)).sum =
          (l.eraseDups.map (fun a => ys.count a)).sum +
            (l.eraseDups.map (fun a => if a = y then 1 else 0)).sum := by
        rw [show (l.eraseDups.map (fun a => (y::ys).count a)).sum =
            (l.eraseDups.map (fun a => ys.count a + if a = y then 1 else 0)).sum by
          apply congrArg List.sum
          apply List.map_congr_left (l := l.eraseDups)
            (f := fun a => (y::ys).count a)
            (g := fun a => ys.count a + if a = y then 1 else 0)
          intro a ha
          rw [List.count_cons]
          by_cases hay : a = y
          · subst y
            simp
          · have hya : y ≠ a := fun h => hay h.symm
            simp [hay, hya]]
        rw [sum_map_add]
      rw [hbump, sum_indicator_eraseDups]
      by_cases hy : y ∈ l
      · simp [hy]
        omega
      · simp [hy]
        omega

private inductive Touched : ∀ {k : Nat} {a t : Word}, Edits k a t → Word → Prop where
  | refl (s : Word) : Touched (Edits.refl s) []
  | drop {k : Nat} {a : Word} (l r : Word) (x : Nat) {h : Edits k a (l++x::r)}
      (w : Word) (hw : Touched h w) : Touched (Edits.drop l r x h) (x::w)
  | add {k : Nat} {a : Word} (l r : Word) (x : Nat) {h : Edits k a (l++r)}
      (w : Word) (hw : Touched h w) : Touched (Edits.add l r x h) (x::w)
  | subst {k : Nat} {a : Word} (l r : Word) (x y : Nat) {h : Edits k a (l++x::r)}
      (w : Word) (hw : Touched h w) : Touched (Edits.subst l r x y h) (x::y::w)

private theorem touched_exists {k : Nat} {a t : Word} (h : Edits k a t) :
    ∃ w, Touched h w := by
  induction h with
  | refl s => exact ⟨[], Touched.refl s⟩
  | drop l r x h ih => rcases ih with ⟨w, hw⟩; exact ⟨x::w, Touched.drop l r x w hw⟩
  | add l r x h ih => rcases ih with ⟨w, hw⟩; exact ⟨x::w, Touched.add l r x w hw⟩
  | subst l r x y h ih => rcases ih with ⟨w, hw⟩; exact ⟨x::y::w, Touched.subst l r x y w hw⟩

private theorem touched_length {k : Nat} {a t : Word} {h : Edits k a t} {w : Word}
    (hw : Touched h w) : w.length ≤ 2*k := by
  induction hw with
  | refl s => simp
  | drop l r x w hw ih => simp only [List.length_cons]; omega
  | add l r x w hw ih => simp only [List.length_cons]; omega
  | subst l r x y w hw ih => simp only [List.length_cons]; omega

private theorem count_cons_eq (a b : Nat) (l : List Nat) :
    (b::l).count a = l.count a + if a = b then 1 else 0 := by
  rw [List.count_cons]
  by_cases h : a = b
  · subst b
    simp
  · have hb : (b == a) = false := beq_eq_false_iff_ne.mpr (fun hb => h hb.symm)
    simp [h, hb]

private theorem count_diff_le_touched {k : Nat} {a t : Word} {h : Edits k a t} {w : Word}
    (hw : Touched h w) (c : Nat) :
    ((a.count c : Int) - t.count c).natAbs ≤ w.count c := by
  induction hw with
  | refl s => simp
  | drop l r x w hw ih =>
      simp only [List.count_append] at ih ⊢
      rw [show (x::w).count c = w.count c + if c = x then 1 else 0 by exact count_cons_eq c x w]
      rw [show (x::r).count c = r.count c + if c = x then 1 else 0 by exact count_cons_eq c x r] at ih
      by_cases hcx : c = x
      · subst x
        simp at ih ⊢
        omega
      · simp [hcx] at ih ⊢
        omega
  | add l r x w hw ih =>
      simp only [List.count_append] at ih ⊢
      rw [show (x::w).count c = w.count c + if c = x then 1 else 0 by exact count_cons_eq c x w]
      rw [show (x::r).count c = r.count c + if c = x then 1 else 0 by exact count_cons_eq c x r]
      by_cases hcx : c = x
      · subst x
        simp at ih ⊢
        omega
      · simp [hcx] at ih ⊢
        omega
  | subst l r x y w hw ih =>
      simp only [List.count_append] at ih ⊢
      rw [show (x::y::w).count c = (y::w).count c + if c = x then 1 else 0 by exact count_cons_eq c x (y::w)]
      rw [show (y::w).count c = w.count c + if c = y then 1 else 0 by exact count_cons_eq c y w]
      rw [show (x::r).count c = r.count c + if c = x then 1 else 0 by exact count_cons_eq c x r] at ih
      rw [show (y::r).count c = r.count c + if c = y then 1 else 0 by exact count_cons_eq c y r]
      by_cases hcx : c = x <;> by_cases hcy : c = y <;>
        simp [hcx, hcy] at ih ⊢ <;> omega


private theorem range_drop_shift (n k : Nat) :
    (List.range n).drop k = (List.range (n-k)).map (fun j => k+j) := by
  rw [← show List.range' 0 n = List.range n by simp [List.range'_eq_map_range]]
  rw [List.drop_range']
  simp [List.range'_eq_map_range]

private theorem grams_drop (q k : Nat) (hq : 0 < q) (s : Word) :
    (grams q s).drop k = grams q (s.drop k) := by
  unfold grams
  rw [← List.map_drop]
  rw [range_drop_shift]
  rw [List.map_map]
  have hn : s.length + 1 - q - k = (s.drop k).length + 1 - q := by
    rw [List.length_drop]
    omega
  rw [hn]
  simp [Function.comp_def, List.drop_drop]

private theorem grams_length (q : Nat) (s : Word) : (grams q s).length = s.length+1-q := by
  simp [grams]

private theorem gramAt_eq_of_le {q : Nat} (hq : 0 < q) (l r : Word) (x : Nat)
    (i : Nat) (hi : i + q ≤ l.length) :
    ((l++x::r).drop i).take q = ((l++r).drop i).take q := by
  have hi_le : i ≤ l.length := by omega
  rw [List.drop_append_of_le_length hi_le, List.drop_append_of_le_length hi_le]
  have htake : q ≤ (l.drop i).length := by rw [List.length_drop]; omega
  rw [List.take_append_of_le_length htake, List.take_append_of_le_length htake]

private theorem grams_take_eq {q : Nat} (hq : 0 < q) (l r : Word) (x : Nat) (k : Nat)
    (hk : k ≤ l.length + 1 - q) :
    (grams q (l++x::r)).take k = (grams q (l++r)).take k := by
  induction k with
  | zero => simp
  | succ k ih =>
      have hk' : k ≤ l.length + 1 - q := by omega
      rw [List.take_add_one, List.take_add_one]
      rw [ih hk']
      congr 1
      unfold grams
      rw [List.getElem?_map, List.getElem?_map]
      have hklen1 : k < (List.range ((l++x::r).length+1-q)).length := by
        simp only [List.length_range, List.length_append, List.length_cons]
        omega
      have hklen2 : k < (List.range ((l++r).length+1-q)).length := by
        simp only [List.length_range, List.length_append]
        omega
      rw [List.getElem?_eq_getElem hklen1, List.getElem?_eq_getElem hklen2]
      simp only [Option.map_some]
      simp
      apply gramAt_eq_of_le hq l r x k
      omega

private theorem take_append_drop_sublist (l : List Word) {i j : Nat} (hij : i ≤ j) :
    (l.take i ++ l.drop j).Sublist l := by
  induction i generalizing j l with
  | zero => simpa using (List.drop_sublist j l)
  | succ i ih =>
      cases j with
      | zero => omega
      | succ j =>
          cases l with
          | nil => simp
          | cons a l' =>
              rw [List.take_succ_cons, List.drop_succ_cons]
              simp [List.Sublist.cons_cons a (ih (j := j) (l := l') (by omega))]


private theorem count_cons_eq_word (a b : Word) (l : List Word) :
    (b::l).count a = l.count a + if a = b then 1 else 0 := by
  rw [List.count_cons]
  by_cases h : a = b
  · subst b; simp
  · have hb : (b == a) = false := beq_eq_false_iff_ne.mpr (fun hb => h hb.symm)
    simp [h, hb]

private theorem shared_cons_formula (s l : List Word) (x : Word) :
    shared s (x::l) = shared s l + (if x ∈ s ∧ l.count x < s.count x then 1 else 0) := by
  unfold shared
  by_cases hxs : x ∈ s
  · have hxsd : x ∈ s.eraseDups := (List.mem_eraseDups).mpr hxs
    have hne : ∀ g ∈ (s.eraseDups).erase x, g ≠ x := by
      intro g hg
      have hgf : g ∈ (s.filter fun b : Word => !(b == x)) := by
        apply (List.mem_eraseDups).mp
        rw [← eraseDups_filter_not x s]
        exact hg
      have hbeq : (g == x) = false := by simpa using (List.mem_filter.mp hgf).2
      exact beq_eq_false_iff_ne.mp hbeq
    rw [sum_erase_of_mem (f := fun g => min (s.count g) ((x::l).count g)) hxsd]
    rw [sum_erase_of_mem (f := fun g => min (s.count g) (l.count g)) hxsd]
    rw [show (((s.eraseDups).erase x).map (fun g => min (s.count g) ((x::l).count g))).sum =
        (((s.eraseDups).erase x).map (fun g => min (s.count g) (l.count g))).sum by
      apply congrArg List.sum
      apply List.map_congr_left
      intro g hg
      have hgne := hne g hg
      rw [count_cons_eq_word]
      simp [hgne]]
    rw [count_cons_eq_word]
    simp [hxs]
    by_cases hlt : l.count x < s.count x <;> simp [hlt] <;> omega
  · have hcnt : s.count x = 0 := (List.count_eq_zero).mpr hxs
    have hterm : min (s.count x) ((x::l).count x) = min (s.count x) (l.count x) := by
      rw [hcnt]
      simp
    rw [show (s.eraseDups.map (fun g => min (s.count g) ((x::l).count g))) =
        (s.eraseDups.map (fun g => min (s.count g) (l.count g))) by
      apply List.map_congr_left
      intro g hg
      have hgne : g ≠ x := by
        intro he
        subst g
        exact hxs ((List.mem_eraseDups).mp hg)
      rw [count_cons_eq_word]
      simp [hgne]]
    simp [hxs, hcnt]


private theorem sum_map_le_gen {α : Type} (l : List α) {f g : α → Nat}
    (h : ∀ a ∈ l, f a ≤ g a) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have ht : ∀ b ∈ as, f b ≤ g b := by intro b hb; exact h b (by simp [hb])
      have := ih ht
      omega

private theorem sum_map_add_gen {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun a => f a + g a)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

private theorem shared_right_mono {s c a : List Word} (h : c.Sublist a) : shared s c ≤ shared s a := by
  unfold shared
  apply sum_map_le_gen
  intro g hg
  have hc := List.Sublist.count_le g h
  omega

private theorem shared_le_of_counts {a u v mid : List Word}
    (hcnt : ∀ g, v.count g ≤ u.count g + mid.count g) :
    shared a v ≤ shared a u + mid.length := by
  unfold shared
  have hle : (a.eraseDups.map (fun g => min (a.count g) (v.count g))).sum ≤
      (a.eraseDups.map (fun g => min (a.count g) (u.count g) + mid.count g)).sum := by
    apply sum_map_le_gen
    intro g hg
    have hc := hcnt g
    omega
  have heq := sum_map_add_gen a.eraseDups (fun g => min (a.count g) (u.count g)) (fun g => mid.count g)
  have hcntle := sum_eraseDups_count_le a mid
  omega

private theorem drop_cons_at (l r : Word) (x : Nat) : (l++x::r).drop (l.length+1) = r := by
  induction l with
  | nil => simp
  | cons a l' ih => simp

private theorem drop_append_at (l r : Word) : (l++r).drop l.length = r := by
  rw [List.drop_append_of_le_length (Nat.le_refl l.length)]
  simp

private theorem grams_step_bounds {q : Nat} (hq : 0 < q) (l r : Word) (x : Nat) :
    let A := grams q (l++r)
    let B := grams q (l++x::r)
    let k := l.length + 1 - q
    let C := A.take k ++ A.drop l.length
    let midA := (A.drop k).take (l.length - k)
    let midB := (B.drop k).take (l.length + 1 - k)
    (∀ g, A.count g ≤ C.count g + midA.count g) ∧
    (∀ g, B.count g ≤ C.count g + midB.count g) ∧
    midA.length ≤ q - 1 ∧ midB.length ≤ q ∧ C.Sublist A ∧ C.Sublist B := by
  dsimp only
  let A := grams q (l++r)
  let B := grams q (l++x::r)
  let k := l.length + 1 - q
  let C := A.take k ++ A.drop l.length
  let midA := (A.drop k).take (l.length - k)
  let midB := (B.drop k).take (l.length + 1 - k)
  change (∀ g, A.count g ≤ C.count g + midA.count g) ∧
    (∀ g, B.count g ≤ C.count g + midB.count g) ∧
    midA.length ≤ q - 1 ∧ midB.length ≤ q ∧ C.Sublist A ∧ C.Sublist B
  have hkB : k ≤ l.length := by dsimp [k]; omega
  have hkA : k ≤ l.length + 1 := by dsimp [k]; omega
  have hAtake : A.take k = B.take k := by
    dsimp [A, B, k]
    symm
    exact grams_take_eq hq l r x k (by omega)
  have hAdrop : A.drop l.length = B.drop (l.length+1) := by
    dsimp [A, B]
    rw [grams_drop q l.length hq, grams_drop q (l.length+1) hq]
    rw [drop_append_at, drop_cons_at]
  have hCeqB : C = B.take k ++ B.drop (l.length+1) := by
    dsimp [C]
    rw [hAtake, hAdrop]
  have hAdecomp : A = A.take k ++ midA ++ A.drop l.length := by
    calc
      A = A.take k ++ A.drop k := (List.take_append_drop k A).symm
      _ = A.take k ++ (midA ++ A.drop l.length) := by
        congr 1
        dsimp [midA]
        rw [show A.drop l.length = (A.drop k).drop (l.length-k) by
          rw [List.drop_drop]
          congr 1
          omega]
        exact (List.take_append_drop (l.length-k) (A.drop k)).symm
      _ = A.take k ++ midA ++ A.drop l.length := by simp [List.append_assoc]
  have hBdecomp : B = B.take k ++ midB ++ B.drop (l.length+1) := by
    calc
      B = B.take k ++ B.drop k := (List.take_append_drop k B).symm
      _ = B.take k ++ (midB ++ B.drop (l.length+1)) := by
        congr 1
        dsimp [midB]
        rw [show B.drop (l.length+1) = (B.drop k).drop (l.length+1-k) by
          rw [List.drop_drop]
          congr 1
          omega]
        exact (List.take_append_drop (l.length+1-k) (B.drop k)).symm
      _ = B.take k ++ midB ++ B.drop (l.length+1) := by simp [List.append_assoc]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro g
    rw [hAdecomp]
    dsimp [C]
    simp only [List.count_append]
    omega
  · intro g
    rw [hBdecomp, hCeqB]
    simp only [List.count_append]
    omega
  · dsimp [midA]
    rw [List.length_take]
    dsimp [k]
    omega
  · dsimp [midB]
    rw [List.length_take]
    dsimp [k]
    omega
  · dsimp [C]
    exact take_append_drop_sublist A hkB
  · rw [hCeqB]
    exact take_append_drop_sublist B hkA

theorem sound_GramLowerBound : GramLowerBound.Goal := by
  intro s t k q hq h
  induction h with
  | refl s =>
      rw [shared_self]
      simp [grams_length]
  | drop l r x h ih =>
      rename_i k a
      obtain ⟨hAc, hBc, hmidA, hmidB, hCsubA, hCsubB⟩ := grams_step_bounds hq l r x
      have h1 := shared_le_of_counts (a := grams q a) hBc
      have h2 := shared_right_mono (s := grams q a) hCsubA
      have hsh : shared (grams q a) (grams q (l++x::r)) ≤ shared (grams q a) (grams q (l++r)) + q := by
        omega
      have hlenA := grams_length q (l++x::r)
      have hlenB := grams_length q (l++r)
      have hlenrel : (l++x::r).length = (l++r).length + 1 := by simp; omega
      have hlenle : (grams q (l++r)).length ≤ (grams q (l++x::r)).length := by
        rw [hlenA, hlenB, hlenrel]
        omega
      have hmaxle : max (grams q a).length (grams q (l++r)).length ≤
          max (grams q a).length (grams q (l++x::r)).length := by omega
      have hmul : (k+1)*q = k*q + q := by simp [Nat.add_mul]
      calc
        max (grams q a).length (grams q (l++r)).length
            ≤ max (grams q a).length (grams q (l++x::r)).length := hmaxle
        _ ≤ shared (grams q a) (grams q (l++x::r)) + k*q := ih
        _ ≤ shared (grams q a) (grams q (l++r)) + q + k*q := by omega
        _ = shared (grams q a) (grams q (l++r)) + (k+1)*q := by omega
  | add l r x h ih =>
      rename_i k a
      obtain ⟨hAc, hBc, hmidA, hmidB, hCsubA, hCsubB⟩ := grams_step_bounds hq l r x
      have h1 := shared_le_of_counts (a := grams q a) hAc
      have h2 := shared_right_mono (s := grams q a) hCsubB
      have hsh : shared (grams q a) (grams q (l++r)) + 1 ≤ shared (grams q a) (grams q (l++x::r)) + q := by
        omega
      have hlenA := grams_length q (l++r)
      have hlenB := grams_length q (l++x::r)
      have hlenrel : (l++x::r).length = (l++r).length + 1 := by simp; omega
      have hlenle : (grams q (l++x::r)).length ≤ (grams q (l++r)).length + 1 := by
        rw [hlenA, hlenB, hlenrel]
        omega
      have hmaxle : max (grams q a).length (grams q (l++x::r)).length ≤
          max (grams q a).length (grams q (l++r)).length + 1 := by omega
      have hmul : (k+1)*q = k*q + q := by simp [Nat.add_mul]
      calc
        max (grams q a).length (grams q (l++x::r)).length
            ≤ max (grams q a).length (grams q (l++r)).length + 1 := hmaxle
        _ ≤ (shared (grams q a) (grams q (l++r)) + k*q) + 1 := by omega
        _ ≤ (shared (grams q a) (grams q (l++x::r)) + q) + k*q := by omega
        _ = shared (grams q a) (grams q (l++x::r)) + (k+1)*q := by rw [hmul]; omega
  | subst l r x y h ih =>
      rename_i k a
      obtain ⟨hAcx, hBcx, hmidAx, hmidBx, hCsubAx, hCsubBx⟩ := grams_step_bounds hq l r x
      obtain ⟨hAcy, hBcy, hmidAy, hmidBy, hCsubAy, hCsubBy⟩ := grams_step_bounds hq l r y
      have h1 := shared_le_of_counts (a := grams q a) hBcx
      have h2 := shared_right_mono (s := grams q a) hCsubBy
      have hsh : shared (grams q a) (grams q (l++x::r)) ≤ shared (grams q a) (grams q (l++y::r)) + q := by
        omega
      have hlenA := grams_length q (l++x::r)
      have hlenB := grams_length q (l++y::r)
      have hlenrel : (l++x::r).length = (l++y::r).length := by simp
      have hlenEq : (grams q (l++x::r)).length = (grams q (l++y::r)).length := by rw [hlenA, hlenB, hlenrel]
      have hmul : (k+1)*q = k*q + q := by simp [Nat.add_mul]
      calc
        max (grams q a).length (grams q (l++y::r)).length
            = max (grams q a).length (grams q (l++x::r)).length := by rw [hlenEq]
        _ ≤ shared (grams q a) (grams q (l++x::r)) + k*q := ih
        _ ≤ shared (grams q a) (grams q (l++y::r)) + q + k*q := by omega
        _ = shared (grams q a) (grams q (l++y::r)) + (k+1)*q := by rw [hmul]; omega

theorem sound_ClassLength : ClassLength.Goal := by
  intro k s t keep h
  induction h with
  | refl s => simp
  | drop l r x h ih =>
      simp only [List.countP_append, List.countP_cons] at ih ⊢
      by_cases hk : keep x = true <;> simp [hk] at ih ⊢ <;> omega
  | add l r x h ih =>
      simp only [List.countP_append, List.countP_cons] at ih ⊢
      by_cases hk : keep x = true <;> simp [hk] at ih ⊢ <;> omega
  | subst l r x y h ih =>
      simp only [List.countP_append, List.countP_cons] at ih ⊢
      by_cases hx : keep x = true <;> by_cases hy : keep y = true <;> simp [hx,hy] at ih ⊢ <;> omega


theorem sound_HistogramFence : HistogramFence.Goal := by
  intro k s t h
  rcases touched_exists h with ⟨w, hw⟩
  have hle1 : ((s++t).eraseDups.map (fun a => ((s.count a:Int)-t.count a).natAbs)).sum ≤
      ((s++t).eraseDups.map (fun a => w.count a)).sum := by
    apply sum_eraseDups_le
    · intro a
      exact count_diff_le_touched hw a
    · intro a ha
      exact ha
  have hle2 := sum_eraseDups_count_le (s++t) w
  have hlen := touched_length hw
  omega


end TinylambOpt.ResearchProofs.Atlas.TextExecution.EditFilter
