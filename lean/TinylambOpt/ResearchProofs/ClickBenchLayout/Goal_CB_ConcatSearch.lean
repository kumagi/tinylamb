import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ConcatSearch

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open ConcatSearch

/-! ## strict / weak ascending predicates -/

private def SAsc (l : List Nat) : Prop :=
  match l with
  | [] => True
  | [_] => True
  | a :: b :: r => a < b ∧ SAsc (b :: r)

private def Asc (l : List Nat) : Prop :=
  match l with
  | [] => True
  | [_] => True
  | a :: b :: r => a ≤ b ∧ Asc (b :: r)

private theorem sasc_tail : ∀ (l : List Nat) (a : Nat), SAsc (a :: l) → SAsc l := by
  intro l a h
  cases l with
  | nil => exact True.intro
  | cons b t => exact h.2

private theorem asc_tail : ∀ (l : List Nat) (a : Nat), Asc (a :: l) → Asc l := by
  intro l a h
  cases l with
  | nil => exact True.intro
  | cons b t => exact h.2

private theorem sasc_cons_all (a : Nat) :
    ∀ (l : List Nat), (∀ x ∈ l, a < x) → SAsc l → SAsc (a :: l) := by
  intro l hall hs
  cases l with
  | nil => exact True.intro
  | cons b t => exact ⟨hall b (List.mem_cons.mpr (Or.inl rfl)), hs⟩

private theorem asc_cons_all_le (a : Nat) :
    ∀ (l : List Nat), (∀ x ∈ l, a ≤ x) → Asc l → Asc (a :: l) := by
  intro l hall hs
  cases l with
  | nil => exact True.intro
  | cons b t => exact ⟨hall b (List.mem_cons.mpr (Or.inl rfl)), hs⟩

private theorem sasc_head : ∀ (a : Nat) (l : List Nat), SAsc (a :: l) → ∀ x ∈ l, a < x := by
  intro a l
  induction l generalizing a with
  | nil => intro _ x hx; exact absurd hx List.not_mem_nil
  | cons b t ih =>
    intro h x hx
    have hb : a < b := h.1
    rw [List.mem_cons] at hx
    rcases hx with rfl | hut
    · exact hb
    · have hbx : b < x := ih b h.2 x hut
      omega

private theorem asc_head : ∀ (a : Nat) (l : List Nat), Asc (a :: l) → ∀ x ∈ l, a ≤ x := by
  intro a l
  induction l generalizing a with
  | nil => intro _ x hx; exact absurd hx List.not_mem_nil
  | cons b t ih =>
    intro h x hx
    have hb : a ≤ b := h.1
    rw [List.mem_cons] at hx
    rcases hx with rfl | hut
    · exact hb
    · have hbx : b ≤ x := ih b h.2 x hut
      omega

private theorem sasc_filter :
    ∀ (l : List Nat), SAsc l → ∀ (p : Nat → Bool), SAsc (l.filter p) := by
  intro l
  induction l with
  | nil => intro _ _; exact True.intro
  | cons a t ih =>
    intro hs p
    cases h : p a with
    | true =>
      rw [List.filter_cons_of_pos h]
      refine sasc_cons_all a (List.filter p t) ?_ (ih (sasc_tail t a hs) p)
      intro x hx
      rw [List.mem_filter] at hx
      exact sasc_head a t hs x hx.1
    | false =>
      rw [List.filter_cons_of_neg (by simpa using h)]
      exact ih (sasc_tail t a hs) p

private theorem asc_filter :
    ∀ (l : List Nat), Asc l → ∀ (p : Nat → Bool), Asc (l.filter p) := by
  intro l
  induction l with
  | nil => intro _ _; exact True.intro
  | cons a t ih =>
    intro hs p
    cases h : p a with
    | true =>
      rw [List.filter_cons_of_pos h]
      refine asc_cons_all_le a (List.filter p t) ?_ (ih (asc_tail t a hs) p)
      intro x hx
      rw [List.mem_filter] at hx
      exact asc_head a t hs x hx.1
    | false =>
      rw [List.filter_cons_of_neg (by simpa using h)]
      exact ih (asc_tail t a hs) p

private theorem sasc_append_last :
    ∀ (l : List Nat) (a : Nat), SAsc l → (∀ x ∈ l, x < a) → SAsc (l ++ [a]) := by
  intro l
  induction l with
  | nil => intro a _ _; exact True.intro
  | cons b t ih =>
    intro a hs hall
    show SAsc (b :: (t ++ [a]))
    cases t with
    | nil => exact ⟨hall b (List.mem_cons.mpr (Or.inl rfl)), True.intro⟩
    | cons c u =>
      refine ⟨hs.1, ih a hs.2 ?_⟩
      intro x hx
      exact hall x (List.mem_cons.mpr (Or.inr hx))

private theorem sasc_range : ∀ (n : Nat), SAsc (List.range n) := by
  intro n
  induction n with
  | zero => exact True.intro
  | succ k ih =>
    rw [List.range_succ]
    refine sasc_append_last (List.range k) k ih ?_
    intro x hx
    rw [List.mem_range] at hx
    exact hx

/-! ## equality criterion -/

private theorem eq_of_mem_sasc : ∀ (l₁ : List Nat), SAsc l₁ → ∀ (l₂ : List Nat), SAsc l₂ →
    (∀ x, x ∈ l₁ ↔ x ∈ l₂) → l₁ = l₂ := by
  intro l₁
  induction l₁ with
  | nil =>
    intro _ l₂ _ hmem
    cases l₂ with
    | nil => rfl
    | cons b t =>
      exact absurd ((hmem b).mpr (List.mem_cons.mpr (Or.inl rfl))) List.not_mem_nil
  | cons a r ih =>
    intro hs l₂ ht hmem
    cases l₂ with
    | nil =>
      exact absurd ((hmem a).mp (List.mem_cons.mpr (Or.inl rfl))) List.not_mem_nil
    | cons b t =>
      have ha : a ∈ b :: t := (hmem a).mp (List.mem_cons.mpr (Or.inl rfl))
      have hb : b ∈ a :: r := (hmem b).mpr (List.mem_cons.mpr (Or.inl rfl))
      rcases List.mem_cons.mp ha with hab | har
      · have hrest : ∀ x, x ∈ r ↔ x ∈ t := by
          intro x
          constructor
          · intro hx
            have h2 : x ∈ a :: r := List.mem_cons.mpr (Or.inr hx)
            rcases List.mem_cons.mp ((hmem x).mp h2) with hxb | hxt
            · exact absurd (sasc_head a r hs a (hab.symm ▸ (hxb ▸ hx))) (Nat.lt_irrefl _)
            · exact hxt
          · intro hx
            have h2 : x ∈ b :: t := List.mem_cons.mpr (Or.inr hx)
            rcases List.mem_cons.mp ((hmem x).mpr h2) with hxa | hxr
            · exact absurd (hab ▸ (sasc_head b t ht a (hxa ▸ hx))) (Nat.lt_irrefl _)
            · exact hxr
        subst hab
        rw [ih (sasc_tail r _ hs) t (sasc_tail t _ ht) hrest]
      · have hba : b < a := sasc_head b t ht a har
        rcases List.mem_cons.mp hb with hbq | hbr
        · exact absurd (hbq ▸ hba) (Nat.lt_irrefl _)
        · have h2 : a < b := sasc_head a r hs b hbr
          omega

/-! ## Bool and facts -/

private theorem and_true {p q : Bool} (h : (p && q) = true) : p = true ∧ q = true := by
  cases p <;> cases q <;> simp_all

private theorem and_intro {p q : Bool} (hp : p = true) (hq : q = true) : (p && q) = true := by
  cases p <;> cases q <;> simp_all

/-! ## prefix-of predicates -/

private theorem ip_nil (h : Word) : ([] : Word).isPrefixOf h = true := rfl

private theorem ip_cons (u v : UInt8) (us vs : Word) :
    (u :: us).isPrefixOf (v :: vs) = ((u == v) && us.isPrefixOf vs) := rfl

private theorem ip_cnil (u : UInt8) (us : Word) :
    (u :: us : Word).isPrefixOf ([] : Word) = false := rfl

private theorem ip_len : ∀ (n h : Word), n.isPrefixOf h = true → n.length ≤ h.length := by
  intro n
  induction n with
  | nil => intro h _; exact Nat.zero_le _
  | cons u ns ih =>
    intro h hpre
    cases h with
    | nil => rw [ip_cnil] at hpre; exact absurd hpre (by decide)
    | cons v hs =>
      rw [ip_cons] at hpre
      obtain ⟨_, hrest⟩ := and_true hpre
      have hlen := ih hs hrest
      show ns.length + 1 ≤ hs.length + 1
      exact Nat.succ_le_succ hlen

private theorem P1 : ∀ (n a b : Word), n.isPrefixOf a = true → n.isPrefixOf (a ++ b) = true := by
  intro n
  induction n with
  | nil => intro a b _; exact ip_nil _
  | cons u ns ih =>
    intro a b h
    cases a with
    | nil => exact absurd h (by rw [ip_cnil]; decide)
    | cons v as =>
      rw [ip_cons] at h
      obtain ⟨h1, h2⟩ := and_true h
      rw [show ((v :: as : Word) ++ b) = v :: (as ++ b) from rfl, ip_cons]
      exact and_intro h1 (ih as b h2)

private theorem P2 :
    ∀ (n a b : Word), n.isPrefixOf (a ++ b) = true → n.length ≤ a.length →
      n.isPrefixOf a = true := by
  intro n
  induction n with
  | nil => intro a b _ _; exact ip_nil _
  | cons u ns ih =>
    intro a b h hlen
    cases a with
    | nil =>
      have h1 : (u :: ns).length = ns.length + 1 := rfl
      have h0 : ([] : Word).length = 0 := rfl
      rw [h0, h1] at hlen
      omega
    | cons v as =>
      rw [show (u :: ns : Word).isPrefixOf ((v :: as) ++ b) =
          ((u == v) && ns.isPrefixOf (as ++ b))
        from by rw [show ((v :: as) ++ b) = v :: (as ++ b) from rfl]; rw [ip_cons]] at h
      obtain ⟨h1, h2⟩ := and_true h
      rw [ip_cons]
      exact and_intro h1 (ih as b h2 (by
        have h1' : (u :: ns : Word).length = ns.length + 1 := rfl
        have h2' : (v :: as : Word).length = as.length + 1 := rfl
        rw [h1', h2'] at hlen
        omega))

private theorem starts_mem (n h : Word) (i : Nat) :
    i ∈ starts n h ↔ (i ≤ h.length ∧ n.isPrefixOf (h.drop i) = true) := by
  rw [starts, List.mem_filter, List.mem_range]
  constructor
  · intro ⟨h1, h2⟩; exact ⟨by omega, h2⟩
  · intro ⟨h1, h2⟩; exact ⟨by omega, h2⟩

private theorem starts_sasc (n h : Word) : SAsc (starts n h) := by
  show SAsc ((List.range (h.length + 1)).filter (fun i => n.isPrefixOf (h.drop i)))
  exact sasc_filter _ (sasc_range _) _

private theorem nonempty_mem :
    ∀ (l : List α), l.isEmpty = false → ∃ x, x ∈ l := by
  intro l hl
  cases l with
  | nil =>
    exact absurd hl (by simp [List.isEmpty])
  | cons x _ => exact ⟨x, List.mem_cons.mpr (Or.inl rfl)⟩

private theorem starts_nil_nonempty (w : Word) : !(starts [] w).isEmpty = true := by
  have hm : (0 : Nat) ∈ starts [] w :=
    (starts_mem [] w 0).mpr ⟨by omega, ip_nil _⟩
  cases h : starts [] w with
  | nil => exact absurd (h ▸ hm) (by simp_all)
  | cons x xs => exact rfl

/-! ## foldl/scanl facts (mirroring Goal_CB_LengthRollup) -/

private theorem foldl_add : ∀ (ys : List Nat) (s : Nat), ys.foldl (·+·) s = s + ys.sum := by
  intro ys
  induction ys with
  | nil => intro s; rfl
  | cons y ys ih =>
    intro s
    simp only [List.foldl_cons, List.sum_cons]
    rw [ih (s + y)]
    omega

private theorem scanl_get (ys : List Nat) (s i : Nat) (hi : i ≤ ys.length) :
    (ys.scanl (·+·) s)[i]? = some (s + (ys.take i).sum) := by
  rw [List.getElem?_scanl, ite_eq_left hi, foldl_add]

/-! ## ends / cumulative sums -/

private def lensOf (ws : List Word) : List Nat := ws.map List.length

private def endsOf (ws : List Word) : List Nat := ((lensOf ws).scanl (·+·) 0).drop 1

private def cumY (ys : List Nat) (i : Nat) : Nat := (ys.take i).sum

private theorem cumY_cons (y : Nat) (ys : List Nat) (j : Nat) :
    cumY (y :: ys) (j + 1) = y + cumY ys j := by
  rw [cumY, cumY, show ((y :: ys).take (j + 1)) = y :: ys.take j from rfl, List.sum_cons]

private theorem cumY_succ :
    ∀ (ys : List Nat) (i : Nat), i < ys.length → cumY ys (i + 1) = cumY ys i + ys[i]! := by
  intro ys
  induction ys with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons y t ih =>
    intro i hi
    cases i with
    | zero =>
      have he1 : cumY (y :: t) 1 = y + cumY t 0 := cumY_cons y t 0
      have he2 : cumY (y :: t) 0 = 0 := rfl
      have he3 : (y :: t)[0]! = y := rfl
      have he4 : cumY t 0 = 0 := rfl
      rw [he1, he2, he3, he4]
      omega
    | succ j =>
      have hlt : j < t.length := by simpa using hi
      have hrec : cumY t (j + 1) = cumY t j + t[j]! := ih j hlt
      have he1 : cumY (y :: t) (j + 1 + 1) = y + cumY t (j + 1) := cumY_cons y t (j + 1)
      have he2 : cumY (y :: t) (j + 1) = y + cumY t j := cumY_cons y t j
      have he3 : (y :: t)[j + 1]! = t[j]! := rfl
      rw [he1, he2, he3, hrec]
      omega

private theorem cumY_mono :
    ∀ (ys : List Nat) (j i : Nat), j ≤ i → cumY ys j ≤ cumY ys i := by
  intro ys
  induction ys with
  | nil => intro j i _; cases j <;> cases i <;> exact Nat.le_refl _
  | cons y t ih =>
    intro j i hij
    show cumY (y :: t) j ≤ cumY (y :: t) i
    cases j with
    | zero =>
      cases i with
      | zero => exact Nat.le_refl _
      | succ k =>
        rw [show cumY (y :: t) 0 = 0 from rfl, cumY_cons]
        exact Nat.zero_le _
    | succ k =>
      cases i with
      | zero => exact absurd hij (by omega)
      | succ l =>
        rw [cumY_cons, cumY_cons]
        exact Nat.add_le_add_left (ih k l (by omega)) _

private theorem cumY_last (ys : List Nat) : cumY ys ys.length = ys.sum := by
  rw [cumY, List.take_length]

private theorem lensLen (ws : List Word) : (lensOf ws).length = ws.length :=
  List.length_map _

private theorem lensGet :
    ∀ (ws : List Word) (i : Nat), i < ws.length → (lensOf ws)[i]! = (ws[i]!).length := by
  intro ws
  induction ws with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons w t ih =>
    intro i hi
    cases i with
    | zero => exact rfl
    | succ j =>
      have hlt : j < t.length := by simpa using hi
      show (lensOf (w :: t))[j + 1]! = ((w :: t)[j + 1]!).length
      have h1 : (lensOf (w :: t))[j + 1]! = (lensOf t)[j]! := rfl
      have h2 : ((w :: t)[j + 1]!).length = (t[j]!).length := rfl
      rw [h1, h2]
      exact ih j hlt

private theorem lensSum (ws : List Word) : (lensOf ws).sum = (ws.flatten).length := by
  induction ws with
  | nil => rfl
  | cons w t ih =>
    show (List.map List.length (w :: t)).sum = ((w :: t).flatten).length
    rw [List.map_cons, List.sum_cons, List.flatten_cons, List.length_append]
    exact congrArg (List.length w + ·) ih

private theorem endsOf_len (ws : List Word) : (endsOf ws).length = ws.length := by
  rw [endsOf, List.length_drop, List.length_scanl, lensLen]
  omega

private theorem endsOf_get :
    ∀ (ws : List Word) (i : Nat), i < ws.length →
      (endsOf ws)[i]? = some (cumY (lensOf ws) (i + 1)) := by
  intro ws i hi
  rw [endsOf, List.getElem?_drop]
  rw [show (1 + i) = i + 1 from Nat.add_comm 1 i,
      scanl_get (lensOf ws) 0 (i + 1) (by rw [lensLen]; omega)]
  rw [Nat.zero_add]
  rfl

private theorem get!? {α : Type} [Inhabited α] :
    ∀ (l : List α) (i : Nat) (a : α), l[i]? = some a → l[i]! = a := by
  intro l
  induction l with
  | nil => intro i a h; exact absurd h (by cases i <;> simp_all)
  | cons b t ih =>
    intro i a h
    cases i with
    | zero => cases h; exact rfl
    | succ j =>
      have h1 : (b :: t)[j + 1]? = t[j]? := rfl
      have h2 : (b :: t)[j + 1]! = t[j]! := rfl
      have h3 : t[j]! = a := ih j a (h1 ▸ h)
      rw [h2, h3]

/-! ## flatten drop facts -/

private theorem drop_append_le' :
    ∀ (a b : List α) (n : Nat), n ≤ a.length → (a ++ b).drop n = a.drop n ++ b := by
  intro a
  induction a with
  | nil =>
    intro b n hn
    have h0 : ([] : List α).length = 0 := rfl
    rw [h0] at hn
    have hn0 : n = 0 := by omega
    subst hn0
    exact rfl
  | cons c t ih =>
    intro b n hn
    cases n with
    | zero => exact rfl
    | succ m =>
      have h1 : ((c :: t) ++ b).drop (m + 1) = (t ++ b).drop m := rfl
      have h2 : List.drop (m + 1) (c :: t) ++ b = List.drop m t ++ b := rfl
      have hl : (c :: t).length = t.length + 1 := rfl
      rw [hl] at hn
      rw [h1, ih b m (by omega), h2]

private theorem flatten_take_len :
    ∀ (ws : List Word) (i : Nat), i ≤ ws.length →
      ((ws.take i).flatten).length = cumY (lensOf ws) i := by
  intro ws
  induction ws with
  | nil =>
    intro i hi
    have h0 : ([] : List Word).length = 0 := rfl
    rw [h0] at hi
    have hii : i = 0 := by omega
    subst hii
    exact rfl
  | cons w t ih =>
    intro i hi
    cases i with
    | zero => exact rfl
    | succ j =>
      have hle : j ≤ t.length := by simpa using hi
      show ((w :: t).take (j + 1)).flatten.length = cumY (lensOf (w :: t)) (j + 1)
      have hx : (w :: t).take (j + 1) = w :: t.take j := rfl
      have h2 : lensOf (w :: t) = w.length :: lensOf t := rfl
      rw [hx, List.flatten_cons, List.length_append, ih j hle, h2, cumY_cons]

private theorem drop_cons_get :
    ∀ (ws : List Word) (i : Nat), i < ws.length → ws.drop i = ws[i]! :: ws.drop (i + 1) := by
  intro ws
  induction ws with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons w t ih =>
    intro i hi
    cases i with
    | zero => exact rfl
    | succ j =>
      have hlt : j < t.length := by simpa using hi
      show (w :: t).drop (j + 1) = (w :: t)[j + 1]! :: (w :: t).drop (j + 1 + 1)
      have h1 : (w :: t).drop (j + 1) = t.drop j := rfl
      have h2 : (w :: t)[j + 1]! = t[j]! := rfl
      have h3 : (w :: t).drop (j + 1 + 1) = t.drop (j + 1) := rfl
      rw [h1, h2, h3]
      exact ih j hlt

private theorem flatten_drop_cum :
    ∀ (ws : List Word) (i : Nat), i ≤ ws.length →
      (ws.flatten).drop (cumY (lensOf ws) i) = (ws.drop i).flatten := by
  intro ws i hi
  have hsp : ws = ws.take i ++ ws.drop i := (List.take_append_drop i ws).symm
  have hlen : ((ws.take i).flatten).length = cumY (lensOf ws) i := flatten_take_len ws i hi
  have hc : cumY (lensOf (List.take i ws ++ List.drop i ws)) i = cumY (lensOf ws) i := by
    rw [← hsp]
  rw [hsp, List.flatten_append, hc, ← hlen]
  rw [show ((List.take i ws).flatten).length = ((List.take i ws).flatten).length + 0
      from (Nat.add_zero _).symm]
  rw [List.drop_length_add_append, List.drop_zero, ← hsp]

private theorem flatten_drop_at :
    ∀ (ws : List Word) (i pos : Nat), i < ws.length → pos ≤ (ws[i]!).length →
      (ws.flatten).drop (cumY (lensOf ws) i + pos) =
        (ws[i]!).drop pos ++ (ws.drop (i + 1)).flatten := by
  intro ws i pos hi hpos
  have h1 : (ws.flatten).drop (cumY (lensOf ws) i + pos)
      = ((ws.flatten).drop (cumY (lensOf ws) i)).drop pos := (List.drop_drop ..).symm
  rw [h1, flatten_drop_cum ws i (by omega)]
  rw [show ws.drop i = ws[i]! :: ws.drop (i + 1) from drop_cons_get ws i hi, List.flatten_cons]
  exact drop_append_le' (ws[i]!) ((ws.drop (i + 1)).flatten) pos hpos

private theorem starts_flat_of (ws : List Word) (n : Word) (i pos : Nat) (w : Word)
    (hi : i < ws.length) (hw : ws[i]! = w) (hps : pos ∈ starts n w) :
    cumY (lensOf ws) i + pos ∈ starts n (ws.flatten) := by
  subst hw
  rw [starts_mem] at hps ⊢
  obtain ⟨h1, h2⟩ := hps
  refine ⟨?_, ?_⟩
  · have hle : cumY (lensOf ws) (i + 1) ≤ (ws.flatten).length := by
      have h2' : cumY (lensOf ws) (i + 1) ≤ cumY (lensOf ws) ((lensOf ws).length) :=
        cumY_mono (lensOf ws) (i + 1) ((lensOf ws).length)
          (by rw [lensLen]; exact hi)
      rw [cumY_last, lensSum] at h2'
      exact h2'
    have h3 : (ws[i]!).length = cumY (lensOf ws) (i + 1) - cumY (lensOf ws) i := by
      have hcs : cumY (lensOf ws) (i + 1) = cumY (lensOf ws) i + (lensOf ws)[i]! :=
        cumY_succ (lensOf ws) i (by rw [lensLen]; exact hi)
      have h4 : (lensOf ws)[i]! = (ws[i]!).length := lensGet ws i hi
      rw [hcs, h4]
      omega
    have h5 : cumY (lensOf ws) (i + 1) = cumY (lensOf ws) i + (ws[i]!).length := by
      rw [cumY_succ (lensOf ws) i (by rw [lensLen]; exact hi), lensGet ws i hi]
    have h6 : cumY (lensOf ws) i + pos ≤ cumY (lensOf ws) (i + 1) := by
      rw [h5]
      exact Nat.add_le_add_left h1 _
    exact Nat.le_trans h6 hle
  · rw [flatten_drop_at ws i pos hi h1]
    exact P1 n (ws[i]!.drop pos) (ws.drop (i + 1)).flatten h2

/-! ## findIdx specifications -/

private theorem findIdx_intro (g : Nat) :
    ∀ (l : List Nat) (i : Nat), i < l.length → decide (g < l[i]!) = true →
      (∀ j < i, decide (g < l[j]!) = false) → l.findIdx (g < ·) = i := by
  intro l
  induction l with
  | nil => intro i hi _ _; exact absurd hi (by simp)
  | cons a t ih =>
    intro i hi hval hmin
    rw [List.findIdx_cons]
    split <;> rename_i hc
    · cases i with
      | zero => rfl
      | succ k =>
        exfalso
        have h0 := hmin 0 (by omega)
        rw [show (a :: t)[0]! = a from rfl] at h0
        exact absurd hc (by simpa using h0)
    · cases i with
      | zero =>
        rw [show (a :: t)[0]! = a from rfl] at hval
        exact absurd hval hc
      | succ k =>
        have hk : List.findIdx (g < ·) t = k := by
          refine ih k (by simpa using hi) ?_ ?_
          · rw [← show (a :: t)[k + 1]! = t[k]! from rfl]
            exact hval
          · intro j hj
            rw [← show (a :: t)[j + 1]! = t[j]! from rfl]
            exact hmin (j + 1) (by omega)
        rw [hk]

private theorem findIdx_elim (g : Nat) :
    ∀ (l : List Nat) (i : Nat), l.findIdx (g < ·) = i → i < l.length →
      decide (g < l[i]!) = true ∧ ∀ j < i, decide (g < l[j]!) = false := by
  intro l
  induction l with
  | nil => intro i hi hlen; exact absurd hlen (by simp)
  | cons a t ih =>
    intro i hi hlen
    rw [List.findIdx_cons] at hi
    split at hi <;> rename_i hc
    · subst hi
      constructor
      · rw [show (a :: t)[0]! = a from rfl]
        exact hc
      · intro j hj
        omega
    · cases i with
      | zero => exact absurd hi (by simp)
      | succ k =>
        have hkk : List.findIdx (g < ·) t = k := Nat.succ.inj hi
        have hlt : k < t.length := by simpa using hlen
        obtain ⟨hva, hm⟩ := ih k hkk hlt
        constructor
        · rw [show (a :: t)[k + 1]! = t[k]! from rfl]
          exact hva
        · intro j hj
          cases j with
          | zero =>
            rw [show (a :: t)[0]! = a from rfl]
            exact (by simpa using hc)
          | succ m =>
            rw [show (a :: t)[m + 1]! = t[m]! from rfl]
            exact hm m (by omega)

private theorem fmono (l : List Nat) (g₁ g₂ i₁ i₂ : Nat) (hgle : g₁ ≤ g₂)
    (h₁ : l.findIdx (g₁ < ·) = i₁) (hlt₁ : i₁ < l.length)
    (h₂ : l.findIdx (g₂ < ·) = i₂) (hlt₂ : i₂ < l.length) : i₁ ≤ i₂ := by
  by_cases h : i₁ ≤ i₂
  · exact h
  · have h2v : decide (g₂ < l[i₂]!) = true := (findIdx_elim g₂ l i₂ h₂ hlt₂).1
    have h1m : decide (g₁ < l[i₂]!) = false := (findIdx_elim g₁ l i₁ h₁ hlt₁).2 i₂ (by omega)
    have hv₂ : g₂ < l[i₂]! := of_decide_eq_true h2v
    have h1n : ¬ g₁ < l[i₂]! := of_decide_eq_false h1m
    have hb : g₁ < l[i₂]! := by omega
    exact absurd hb h1n

/-! ## the attribution function -/

private def attrib (nlen : Nat) (L : List Nat) (g : Nat) : Option Nat :=
  match L[L.findIdx (g < ·)]? with
  | none => none
  | some e => if g + nlen ≤ e then some (L.findIdx (g < ·)) else none

private theorem attrib_spec (nlen : Nat) (L : List Nat) (g x : Nat) :
    attrib nlen L g = some x ↔
      ∃ e, L[L.findIdx (g < ·)]? = some e ∧ g + nlen ≤ e ∧ x = L.findIdx (g < ·) := by
  have hdef : attrib nlen L g =
      (match L[L.findIdx (g < ·)]? with
        | none => none
        | some e => if g + nlen ≤ e then some (L.findIdx (g < ·)) else none) := rfl
  cases h : L[L.findIdx (g < ·)]? with
  | none =>
    constructor
    · intro hx
      rw [hdef, h] at hx
      simp at hx
    · intro ⟨e, h1, _, _⟩
      exact absurd h1 (by simp)
  | some e =>
    have h2 : attrib nlen L g =
        (if g + nlen ≤ e then some (L.findIdx (g < ·)) else none) := by
      rw [hdef, h]
    constructor
    · intro hx
      rw [h2] at hx
      by_cases hle : g + nlen ≤ e
      · rw [if_pos hle] at hx
        exact ⟨e, rfl, hle, (Option.some.inj hx).symm⟩
      · rw [if_neg hle] at hx
        exact absurd hx (by simp)
    · intro ⟨e', h1, hle, hx⟩
      have he : e = e' := Option.some.inj h1
      subst he
      rw [h2, hx]
      exact if_pos hle

private theorem attrib_mono (nlen : Nat) (L : List Nat) (g₁ g₂ i₁ i₂ : Nat) (hgle : g₁ ≤ g₂)
    (h₁ : attrib nlen L g₁ = some i₁) (h₂ : attrib nlen L g₂ = some i₂) : i₁ ≤ i₂ := by
  rw [attrib_spec] at h₁ h₂
  obtain ⟨e₁, hge₁, _, hi₁⟩ := h₁
  obtain ⟨e₂, hge₂, _, hi₂⟩ := h₂
  obtain ⟨hlt₁, _⟩ := List.getElem?_eq_some_iff.mp hge₁
  obtain ⟨hlt₂, _⟩ := List.getElem?_eq_some_iff.mp hge₂
  have hb₁ : i₁ < L.length := hi₁.symm ▸ hlt₁
  have hb₂ : i₂ < L.length := hi₂.symm ▸ hlt₂
  exact fmono L g₁ g₂ i₁ i₂ hgle hi₁.symm hb₁ hi₂.symm hb₂

private theorem asc_attrib (nlen : Nat) (L : List Nat) :
    ∀ (l : List Nat), SAsc l → Asc (l.filterMap (attrib nlen L)) := by
  intro l
  induction l with
  | nil => intro _; exact True.intro
  | cons a t ih =>
    intro hs
    rw [List.filterMap_cons]
    cases hfa : attrib nlen L a with
    | none => exact ih (sasc_tail t a hs)
    | some i =>
      refine asc_cons_all_le i _ ?_ (ih (sasc_tail t a hs))
      intro y hy
      obtain ⟨g, hg, hgy⟩ := List.mem_filterMap.mp hy
      refine attrib_mono nlen L a g i y ?_ hfa hgy
      have hlt := sasc_head a t hs g hg
      omega

private theorem natbeq : ∀ a : Nat, (a == a) = true := by
  intro a
  induction a with
  | zero => rfl
  | succ m ih => exact ih

private theorem len_filter_le (p : Nat → Bool) (l : List Nat) :
    (l.filter p).length ≤ l.length := by
  induction l with
  | nil => exact Nat.zero_le _
  | cons a t ih =>
    cases h : p a with
    | false =>
      rw [List.filter_cons_of_neg (by simpa using h)]
      have hl : (a :: t).length = t.length + 1 := rfl
      omega
    | true =>
      rw [List.filter_cons_of_pos h]
      have hl1 : (a :: List.filter p t).length = (List.filter p t).length + 1 := rfl
      have hl2 : (a :: t).length = t.length + 1 := rfl
      omega

private theorem asc_eraseDups_aux : ∀ (n : Nat) (l : List Nat), l.length ≤ n →
    Asc l → SAsc (l.eraseDups) := by
  intro n
  induction n with
  | zero =>
    intro l hle _
    cases l with
    | nil => exact True.intro
    | cons a t =>
      exact absurd hle (by rw [List.length_cons]; omega)
  | succ k ih =>
    intro l hle ha
    cases l with
    | nil => exact True.intro
    | cons a t =>
      cases t with
      | nil => exact True.intro
      | cons b r =>
        rw [List.eraseDups_cons]
        refine sasc_cons_all a _ ?_ (ih (List.filter (fun y => !y == a) (b :: r)) ?_ ?_)
        · intro x hx
          rw [List.mem_eraseDups] at hx
          rw [List.mem_filter] at hx
          have hle' : a ≤ x := asc_head a (b :: r) ha x hx.1
          have hne : x ≠ a := by
            intro he
            rw [he] at hx
            have h2 : (a == a) = true := natbeq a
            have h3 : !(a == a) = true := hx.2
            rw [h2] at h3
            exact absurd h3 (by decide)
          omega
        · have hlen : (List.filter (fun y => !y == a) (b :: r)).length ≤ (b :: r).length :=
            len_filter_le (fun y => !y == a) (b :: r)
          have h2 : (a :: (b :: r)).length ≤ k + 1 := hle
          rw [List.length_cons, Nat.succ_le_succ_iff] at h2
          exact Nat.le_trans hlen h2
        · exact asc_filter (b :: r) (asc_tail (b :: r) a ha) (fun y => !y == a)

private theorem asc_eraseDups (l : List Nat) (ha : Asc l) : SAsc (l.eraseDups) :=
  asc_eraseDups_aux l.length l (Nat.le_refl _) ha


/-! ## zipIdx helpers -/

private theorem zipIdx_cons_k {α : Type} (x : α) (xs : List α) (k : Nat) :
    (x :: xs).zipIdx k = (x, k) :: xs.zipIdx (k + 1) := rfl

private theorem zipIdx_mem_get {α : Type} [Inhabited α] :
    ∀ (xs : List α) (i k : Nat), i < xs.length → (xs[i]!, k + i) ∈ xs.zipIdx k := by
  intro xs
  induction xs with
  | nil => intro i k hi; exact absurd hi (by simp)
  | cons a t ih =>
    intro i k hi
    cases i with
    | zero =>
      rw [zipIdx_cons_k, Nat.add_zero]
      exact List.mem_cons.mpr (Or.inl rfl)
    | succ j =>
      have hlt : j < t.length := by simpa using hi
      rw [zipIdx_cons_k]
      rw [show k + (j + 1) = (k + 1) + j from by omega]
      refine List.Mem.tail _ (ih j (k + 1) hlt)

/-! ## RHS list is strictly ascending -/

private theorem sasc_zipIdx_fm (n : Word) :
    ∀ (xs : List (Option Word)) (k : Nat),
      SAsc ((xs.zipIdx k).filterMap
        (fun (x, i) => if x.any (fun w => !(starts n w).isEmpty) then some i else none)) := by
  intro xs
  induction xs with
  | nil => intro k; exact True.intro
  | cons o t ih =>
    intro k
    rw [zipIdx_cons_k]
    cases hc : o.any (fun w => !(starts n w).isEmpty) with
    | true =>
      rw [List.filterMap_cons]
      simp only [hc]
      refine sasc_cons_all k _ ?_ (ih (k + 1))
      intro y hy
      obtain ⟨q, hq, hbody⟩ := List.mem_filterMap.mp hy
      obtain ⟨z, j⟩ := q
      obtain ⟨h1, _, _⟩ := List.mem_zipIdx hq
      cases hz : z.any (fun w => !(starts n w).isEmpty) with
      | false =>
        simp [hz] at hbody
      | true =>
        simp [hz] at hbody
        subst hbody
        omega
    | false =>
      rw [List.filterMap_cons]
      simp only [hc]
      exact ih (k + 1)

/-! ## filterMap congruence -/

private theorem filterMap_congr {α β : Type} {f g : α → Option β} (l : List α)
    (h : ∀ a ∈ l, f a = g a) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    rw [List.filterMap_cons, List.filterMap_cons, h a (List.mem_cons.mpr (Or.inl rfl))]
    rw [ih (fun x hx => h x (List.mem_cons.mpr (Or.inr hx)))]

private theorem wsGetD : ∀ (xs : List (Option Word)) (i : Nat), i < xs.length →
    (xs.map (·.getD []))[i]! = (xs[i]!).getD [] := by
  intro xs
  induction xs with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons o t ih =>
    intro i hi
    cases i with
    | zero => exact rfl
    | succ j =>
      have hlt : j < t.length := by simpa using hi
      have h1 : ((o :: t).map (·.getD []))[j + 1]! = (t.map (·.getD []))[j]! := rfl
      have h2 : ((o :: t)[j + 1]!).getD [] = (t[j]!).getD [] := rfl
      rw [h1, h2, ih j hlt]

private theorem obind_cases (x : Option Nat) (f : Nat → Option Nat) :
    Option.bind x f = (match x with
      | none => none
      | some a => f a) := by
  cases x with
  | none => rfl
  | some a => rfl

private theorem do_eq (L : List Nat) (nlen pos : Nat) :
    ((do
      let i := L.findIdx (pos < ·)
      let e ← L[i]?
      if pos + nlen ≤ e then some i else none) : Option Nat) = attrib nlen L pos := by
  have h1 : ((do
      let i := L.findIdx (pos < ·)
      let e ← L[i]?
      if pos + nlen ≤ e then some i else none) : Option Nat) =
      Option.bind L[L.findIdx (pos < ·)]? (fun e =>
        if pos + nlen ≤ e then some (L.findIdx (pos < ·)) else none) := rfl
  have h2 : attrib nlen L pos = Option.bind L[L.findIdx (pos < ·)]? (fun e =>
      if pos + nlen ≤ e then some (L.findIdx (pos < ·)) else none) := by
    unfold attrib
    exact (obind_cases _ _).symm
  rw [h1, h2]

private theorem fspec (n : Word) : ∀ (v : Option Word) (j : Nat),
    (match (v, j) with
      | (x, i) => if x.any (fun w => !(starts n w).isEmpty) then some i else none) =
      if v.any (fun w => !(starts n w).isEmpty) = true then some j else none := by
  intro v j; exact rfl

/-! ## main theorem -/

theorem sound : Goal := by
  intro xs needle
  by_cases hempty : needle.isEmpty
  · have hn2 : needle = [] := by
      cases needle with
      | nil => rfl
      | cons u us => exact absurd hempty (by simp)
    subst hn2
    show xs.zipIdx.filterMap (fun (x, i) => x.map (fun _ => i)) =
      xs.zipIdx.filterMap (fun (x, i) =>
        if x.any (fun w => !(starts [] w).isEmpty) then some i else none)
    refine filterMap_congr xs.zipIdx fun p _ => ?_
    obtain ⟨o, i⟩ := p
    cases o with
    | none => exact rfl
    | some w =>
      have hc : (some w).any (fun w' => !(starts [] w').isEmpty) = true := by
        simpa using (starts_nil_nonempty w)
      exact (if_pos hc).symm
  · obtain ⟨u, us, hnu⟩ : ∃ u us, needle = u :: us := by
      cases needle with
      | nil => exact absurd hempty (by simp)
      | cons u us => exact ⟨u, us, rfl⟩
    have hn : 0 < needle.length := by rw [hnu]; exact Nat.succ_pos _
    let ws := xs.map (·.getD [])
    have hws : ws = xs.map (·.getD []) := rfl
    have hlhs : rowIDs xs needle =
        ((starts needle ws.flatten).filterMap (attrib needle.length (endsOf ws))).eraseDups := by
      have hdef : rowIDs xs needle =
          (if needle.isEmpty then xs.zipIdx.filterMap (fun (x, i) => x.map (fun _ => i))
            else ((starts needle ws.flatten).filterMap (fun pos =>
              (do
                let i := (endsOf ws).findIdx (pos < ·)
                let e ← (endsOf ws)[i]?
                if pos + needle.length ≤ e then some i else none))).eraseDups) := rfl
      rw [hdef, if_neg hempty]
      refine congrArg ((·.eraseDups) : List Nat → List Nat)
        (congrArg (List.filterMap · (starts needle ws.flatten)) ?_)
      funext pos
      exact do_eq (endsOf ws) needle.length pos
    rw [hlhs]
    refine eq_of_mem_sasc _ (asc_eraseDups _
        (asc_attrib needle.length (endsOf ws) _ (starts_sasc needle ws.flatten))) _
      (sasc_zipIdx_fm needle xs 0) ?_
    intro z
    rw [List.mem_eraseDups, List.mem_filterMap, List.mem_filterMap]
    constructor
    · rintro ⟨g, hg, ha⟩
      rw [attrib_spec] at ha
      obtain ⟨e, hge, hfit, hx⟩ := ha
      let i := (endsOf ws).findIdx (g < ·)
      have hidef : i = (endsOf ws).findIdx (g < ·) := rfl
      subst hx
      rw [← hidef] at hge
      obtain ⟨hltL, _⟩ := List.getElem?_eq_some_iff.mp hge
      have hiws : i < ws.length := by rw [← endsOf_len ws]; exact hltL
      have hxlen : ws.length = xs.length := by rw [hws, List.length_map]
      have hixs : i < xs.length := hxlen ▸ hiws
      have he' : cumY (lensOf ws) (i + 1) = e :=
        Option.some.inj ((endsOf_get ws i hiws).symm.trans hge)
      rw [← he'] at hfit
      have hmin0 := (findIdx_elim g (endsOf ws) i hidef.symm hltL).2
      have ge : cumY (lensOf ws) i ≤ g := by
        by_cases hz : i = 0
        · rw [hz]
          show cumY (lensOf ws) 0 ≤ g
          rw [show cumY (lensOf ws) 0 = 0 from rfl]
          exact Nat.zero_le _
        · obtain ⟨k, hk⟩ : ∃ k, i = k + 1 := ⟨i - 1, by omega⟩
          rw [hk] at hltL hmin0 ⊢
          have h2 : k < (endsOf ws).length := by omega
          have hlk : k < ws.length := by rw [← endsOf_len]; exact h2
          have hgk : (endsOf ws)[k]! = cumY (lensOf ws) (k + 1) :=
            get!? _ _ _ (endsOf_get ws k hlk)
          have hk0 := hmin0 k (by omega)
          rw [hgk] at hk0
          have hnn : ¬ g < cumY (lensOf ws) (k + 1) := of_decide_eq_false hk0
          omega
      have hgc : cumY (lensOf ws) i + (g - cumY (lensOf ws) i) = g := by omega
      have hlenw : (ws[i]!).length =
          cumY (lensOf ws) (i + 1) - cumY (lensOf ws) i := by
        rw [cumY_succ (lensOf ws) i (by rw [lensLen]; exact hiws), lensGet ws i hiws]
        omega
      have hpos : g - cumY (lensOf ws) i ≤ (ws[i]!).length := by omega
      have hpre' : needle.isPrefixOf ((ws[i]!).drop (g - cumY (lensOf ws) i)) = true := by
        obtain ⟨_, hpre⟩ := (starts_mem needle ws.flatten g).mp hg
        rw [← hgc] at hpre
        rw [flatten_drop_at ws i (g - cumY (lensOf ws) i) hiws hpos] at hpre
        refine P2 needle ((ws[i]!).drop (g - cumY (lensOf ws) i))
          ((ws.drop (i + 1)).flatten) hpre ?_
        have hd : ((ws[i]!).drop (g - cumY (lensOf ws) i)).length
            = (ws[i]!).length - (g - cumY (lensOf ws) i) := List.length_drop
        omega
      have hpis : g - cumY (lensOf ws) i ∈ starts needle (ws[i]!) :=
        (starts_mem needle (ws[i]!) _).mpr ⟨hpos, hpre'⟩
      have hv : (xs[i]!).getD [] = ws[i]! := (wsGetD xs i hixs).symm
      have hany : (xs[i]!).any (fun w => !(starts needle w).isEmpty) = true := by
        cases hxi : xs[i]! with
        | none =>
          have hnil : ws[i]! = ([] : Word) := by rw [← hv, hxi]; exact rfl
          have h2 : needle.isPrefixOf ((ws[i]!).drop (g - cumY (lensOf ws) i)) = true :=
            ((starts_mem needle (ws[i]!) _).mp hpis).2
          rw [hnil, List.drop_nil, hnu] at h2
          simp at h2
        | some w =>
          have hw : w = ws[i]! := by rw [← hv, hxi]; exact rfl
          have hne : (starts needle w).isEmpty = false := by
            have hpis' : g - cumY (lensOf ws) i ∈ starts needle w := by rw [hw]; exact hpis
            cases hst : starts needle w with
            | nil =>
              rw [hst] at hpis'
              exact absurd hpis' List.not_mem_nil
            | cons x xs' => exact rfl
          simpa using hne
      have heq : (xs[i]!, 0 + i) = (xs[i]!, i) :=
        congrArg (fun k => (xs[i]!, k)) (Nat.zero_add i)
      refine ⟨(xs[i]!, i), heq ▸ zipIdx_mem_get xs i 0 hixs, ?_⟩
      exact if_pos hany
    · intro ⟨p, hp, hbody⟩
      obtain ⟨v, j⟩ := p
      obtain ⟨_, hk1, hv⟩ := List.mem_zipIdx hp
      simp at hv
      have hjxs : j < xs.length := by simpa using hk1
      have hbody' : (if v.any (fun w => !(starts needle w).isEmpty) = true then some j else none) =
          some z := hbody
      have hcond : v.any (fun w => !(starts needle w).isEmpty) = true := by
        by_cases hc : v.any (fun w => !(starts needle w).isEmpty) = true
        · exact hc
        · rw [if_neg hc] at hbody'
          exact absurd hbody' (by simp)
      rw [if_pos hcond] at hbody'
      have hbj : j = z := Option.some.inj hbody'
      subst hbj
      cases v with
      | none =>
        simp [Option.any] at hcond
      | some w =>
        have hjws : j < ws.length := by rw [hws, List.length_map]; exact hjxs
        have hsome : xs[j]? = some (some w) :=
          List.getElem?_eq_some_iff.mpr ⟨hjxs, hv.symm⟩
        have hvb : xs[j]! = some w := get!? xs j (some w) hsome
        have hw : ws[j]! = w := by rw [hws, wsGetD xs j hjxs, hvb]; exact rfl
        obtain ⟨pos, hps⟩ : ∃ pos, pos ∈ starts needle w := by
          have hne : (starts needle w).isEmpty = false := by
            simpa [Option.any] using hcond
          exact nonempty_mem _ hne
        have hps' : pos ∈ starts needle (ws[j]!) := by rw [hw]; exact hps
        obtain ⟨hple, hppre⟩ := (starts_mem needle w pos).mp hps
        have hnle : needle.length ≤ (w.drop pos).length := ip_len needle _ hppre
        have hd : (w.drop pos).length = w.length - pos := List.length_drop
        have hc := cumY_succ (lensOf ws) j (by rw [lensLen]; exact hjws)
        have hl := lensGet ws j hjws
        have gmember : cumY (lensOf ws) j + pos ∈ starts needle ws.flatten :=
          starts_flat_of ws needle j pos w hjws hw hps
        refine ⟨cumY (lensOf ws) j + pos, gmember, ?_⟩
        have hwl : (ws[j]!).length = w.length := congrArg List.length hw
        have hjL : j < (endsOf ws).length := by rw [endsOf_len]; exact hjws
        have hfj : (endsOf ws).findIdx (cumY (lensOf ws) j + pos < ·) = j := by
          refine findIdx_intro (cumY (lensOf ws) j + pos) (endsOf ws) j hjL ?_ ?_
          · have hjv : (endsOf ws)[j]! = cumY (lensOf ws) (j + 1) :=
              get!? _ _ _ (endsOf_get ws j hjws)
            rw [hjv]
            refine decide_eq_true ?_
            omega
          · intro k hk
            have h1 : k < (endsOf ws).length := Nat.lt_trans hk hjL
            rw [endsOf_len] at h1
            have hkv : (endsOf ws)[k]! = cumY (lensOf ws) (k + 1) :=
              get!? _ _ _ (endsOf_get ws k h1)
            rw [hkv]
            refine decide_eq_false ?_
            have hmono : cumY (lensOf ws) (k + 1) ≤ cumY (lensOf ws) j :=
              cumY_mono (lensOf ws) (k + 1) j (by omega)
            omega
        have hge : (endsOf ws)[(endsOf ws).findIdx (cumY (lensOf ws) j + pos < ·)]? =
            some (cumY (lensOf ws) (j + 1)) := by
          rw [hfj]
          exact endsOf_get ws j hjws
        rw [attrib_spec]
        refine ⟨cumY (lensOf ws) (j + 1), hge, ?_, hfj.symm⟩
        omega

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ConcatSearch
