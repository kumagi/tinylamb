import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchProofs.CloudBI.Helpers

open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-!
# CloudBI 証明モデル用の共通補題

`Totals = ⟨cnt, nn, sum⟩` の可換モノイド構造、`summarize` の成分刻画・分割・
並べ替え不変性、述語による分割の perm 補題、`eraseDups`・整列リスト・
`Std.HashMap` ヒストグラム畳み込みの変換を集める。CloudBI の 4 モジュール
（`Quotes`・`NestedBI`・`DeleteFiles`・`PublicBI`）の証明から import される。
-/

/-! ### Totals 代数 -/

theorem totals_ext {a b : Totals} (h1 : a.cnt = b.cnt) (h2 : a.nn = b.nn)
    (h3 : a.sum = b.sum) : a = b := by
  cases a with | mk ca na sa =>
  cases b with | mk cb nb sb =>
    subst h1; subst h2; subst h3
    rfl

/-- NestedBI の `scale` と同じ作用（モジュール横断で使えるようにここで定義）。 -/
def scaleT (a : Totals) (n : Nat) : Totals :=
  ⟨a.cnt * n, a.nn * n, a.sum * (n : Int)⟩

theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  cases a with | mk ca na sa =>
  cases b with | mk cb nb sb =>
    refine totals_ext ?_ ?_ ?_ <;> simp [plus, Nat.add_comm, Int.add_comm]

theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a with | mk ca na sa =>
  cases b with | mk cb nb sb =>
  cases c with | mk cc nc sc =>
    refine totals_ext ?_ ?_ ?_ <;> simp only [plus] <;> omega

theorem plus_zero (a : Totals) : plus a zero = a := by
  cases a with | mk ca na sa =>
    refine totals_ext ?_ ?_ ?_ <;> simp [plus, zero]

theorem plus_zero' (a : Totals) : plus zero a = a := by
  rw [plus_comm, plus_zero]

theorem plus_left_comm (a b c : Totals) : plus a (plus b c) = plus b (plus a c) := by
  rw [← plus_assoc, ← plus_assoc, plus_comm a b]

theorem plus_right_swap (a b c : Totals) : plus (plus a b) c = plus (plus a c) b := by
  rw [plus_assoc, plus_assoc a c b, plus_comm b c]

theorem scale_one (a : Totals) : scaleT a 1 = a := by
  cases a with | mk ca na sa =>
    refine totals_ext ?_ ?_ ?_ <;> simp [scaleT]

theorem scale_zero (a : Totals) : scaleT a 0 = zero := by
  cases a with | mk ca na sa =>
    refine totals_ext ?_ ?_ ?_ <;> simp [scaleT, zero]

theorem scale_add (a : Totals) (m n : Nat) :
    scaleT a (m + n) = plus (scaleT a m) (scaleT a n) := by
  cases a with | mk ca na sa =>
    refine totals_ext ?_ ?_ ?_
    · simp only [scaleT, plus, Nat.mul_add]
    · simp only [scaleT, plus, Nat.mul_add]
    · simp only [scaleT, plus]
      rw [← Int.ofNat_add_ofNat, Int.mul_add]

theorem scale_plus (a b : Totals) (n : Nat) :
    scaleT (plus a b) n = plus (scaleT a n) (scaleT b n) := by
  cases a with | mk ca na sa =>
  cases b with | mk cb nb sb =>
    refine totals_ext ?_ ?_ ?_ <;>
      simp only [scaleT, plus, Nat.add_mul, Int.add_mul]

theorem weighted_add (m n : Nat) (v : Option Int) :
    weighted (m + n) v = plus (weighted m v) (weighted n v) := by
  refine totals_ext ?_ ?_ ?_
  · cases v <;> simp only [weighted, plus]
  · cases v <;> simp [weighted, plus]
  · cases v with
    | none => simp [weighted, plus]
    | some val =>
        simp only [weighted, plus]
        rw [← Int.ofNat_add_ofNat, Int.add_mul]

/-! ### List 基本補題 -/

theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ z ∈ xs, p z = false := by intro z hz; exact h z (by simp [hz])
      simp [hx, ih ht]

/-- 要素を含む列は、その要素の前後に分解できる。 -/
theorem exists_decomp_of_mem {α : Type} (l : List α) (a : α) (h : a ∈ l) :
    ∃ u v, l = u ++ a :: v := by
  induction l with
  | nil => exact absurd h (by simp)
  | cons x l ih =>
      rcases List.mem_cons.mp h with rfl | h'
      · exact ⟨[], l, rfl⟩
      · obtain ⟨u, v, huv⟩ := ih h'
        exact ⟨x :: u, v, congrArg (fun t => x :: t) huv⟩

theorem perm_move_front {α : Type} (u v : List α) (x : α) :
    List.Perm (u ++ x :: v) (x :: u ++ v) := by
  induction u with
  | nil => exact List.Perm.refl _
  | cons y u ih =>
      exact (List.Perm.cons y ih).trans (List.Perm.swap x y (u ++ v))

theorem nodup_mid_not_mem {α : Type} {u : List α} {x : α} {v : List α}
    (h : (u ++ x :: v).Nodup) : x ∉ u ++ v := by
  induction u with
  | nil =>
      intro hm
      exact (List.nodup_cons.mp h).1 hm
  | cons y u' ih =>
      intro hm
      cases List.mem_cons.mp hm with
      | inl hxy =>
          subst hxy
          exact (List.nodup_cons.mp h).1 (by simp)
      | inr hm' =>
          exact ih (List.nodup_cons.mp h).2 hm'

/-- 重複なし列が同じ所属を持つなら並べ替え。 -/
theorem perm_of_nodup_mem {α : Type} {l₁ : List α} (n₁ : l₁.Nodup) :
    ∀ (l₂ : List α), l₂.Nodup → (∀ a, a ∈ l₁ ↔ a ∈ l₂) → List.Perm l₁ l₂ := by
  induction l₁ with
  | nil =>
      intro l₂ _ h
      cases l₂ with
      | nil => exact List.Perm.refl _
      | cons y ys =>
          have hy : y ∈ ([] : List α) := (h y).mpr (by simp)
          exact absurd hy (by simp)
  | cons x t ih =>
      intro l₂ n₂ h
      have hx : x ∈ l₂ := (h x).mp (by simp)
      obtain ⟨u, v, huv⟩ := exists_decomp_of_mem l₂ x hx
      have hn' : (u ++ x :: v).Nodup := by rw [← huv]; exact n₂
      have hp : List.Perm (u ++ x :: v) (x :: (u ++ v)) := perm_move_front u v x
      have h2n : (x :: (u ++ v)).Nodup := hp.nodup_iff.mp hn'
      have nxu : x ∉ u ++ v := nodup_mid_not_mem hn'
      have hxt : x ∉ t := (List.nodup_cons.mp n₁).1
      have hmem : ∀ a, a ∈ t ↔ a ∈ u ++ v := by
        intro a
        constructor
        · intro ha
          have h2 : a ∈ l₂ := (h a).mp (List.mem_cons_of_mem x ha)
          rw [huv] at h2
          rw [List.mem_append, List.mem_cons] at h2
          rcases h2 with hu | hax | hv
          · exact List.mem_append.mpr (Or.inl hu)
          · subst hax
            exact absurd ha hxt
          · exact List.mem_append.mpr (Or.inr hv)
        · intro h2
          have h3 : a ∈ l₂ := by
            rw [huv, List.mem_append]
            exact Or.imp (fun hu => hu) (fun hv => List.mem_cons_of_mem x hv)
              (List.mem_append.mp h2)
          have h5 : a ∈ x :: t := (h a).mpr h3
          cases List.mem_cons.mp h5 with
          | inl hax =>
              subst hax
              exact absurd h2 nxu
          | inr ht => exact ht
      have hnt : t.Nodup := (List.nodup_cons.mp n₁).2
      have hperm := ih hnt (u ++ v) (List.nodup_cons.mp h2n).2 hmem
      have hback : List.Perm (x :: (u ++ v)) l₂ := by
        rw [huv]
        exact hp.symm
      exact (List.Perm.cons x hperm).trans hback

/-! ### 畳み込みと並べ替え -/

/-- Bag sum of `Totals` as a right fold. -/
def TSum (xs : List Totals) : Totals := xs.foldr (fun a b => plus a b) zero

theorem foldl_plus (xs : List Totals) (z : Totals) :
    xs.foldl (fun a b => plus a b) z = plus z (TSum xs) := by
  induction xs generalizing z with
  | nil => simp [TSum, plus_zero]
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      have hx : TSum (x :: xs) = plus x (TSum xs) := rfl
      rw [hx, plus_assoc]

theorem TSum_append (xs ys : List Totals) :
    TSum (xs ++ ys) = plus (TSum xs) (TSum ys) := by
  induction xs generalizing ys with
  | nil => exact (plus_zero' (TSum ys)).symm
  | cons x xs ih =>
      calc TSum ((x :: xs) ++ ys)
          = plus x (TSum (xs ++ ys)) := rfl
        _ = plus x (plus (TSum xs) (TSum ys)) := by rw [ih]
        _ = plus (plus x (TSum xs)) (TSum ys) := (plus_assoc x _ _).symm
        _ = plus (TSum (x :: xs)) (TSum ys) := rfl

theorem foldl_plus_perm {xs ys : List Totals} (h : List.Perm xs ys) (z : Totals) :
    xs.foldl (fun a b => plus a b) z = ys.foldl (fun a b => plus a b) z :=
  List.Perm.foldl_eq' h (by
    intro x _ y _ acc
    rw [plus_assoc, plus_comm x y, ← plus_assoc]) z

theorem TSum_perm {xs ys : List Totals} (h : List.Perm xs ys) : TSum xs = TSum ys := by
  rw [← plus_zero' (TSum xs), ← plus_zero' (TSum ys),
    ← foldl_plus xs zero, ← foldl_plus ys zero]
  exact foldl_plus_perm h zero

theorem TSum_nil : TSum ([] : List Totals) = zero := rfl

theorem foldl_add_perm {α : Type} {xs ys : List α} (h : List.Perm xs ys) (f : α → Nat)
    (z : Nat) :
    xs.foldl (fun a x => a + f x) z = ys.foldl (fun a x => a + f x) z :=
  List.Perm.foldl_eq' h (by
    intro x _ y _ z
    omega) z

theorem map_sum_perm {α : Type} (g : α → Nat) {l₁ l₂ : List α} (p : List.Perm l₁ l₂) :
    (l₁.map g).sum = (l₂.map g).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp only [List.map_cons, List.sum_cons, Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

theorem map_sum_perm_int {α : Type} (g : α → Int) {l₁ l₂ : List α} (p : List.Perm l₁ l₂) :
    (l₁.map g).sum = (l₂.map g).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp only [List.map_cons, List.sum_cons, Int.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

theorem foldl_add_map_eq_sum {α : Type} (l : List α) (g : α → Nat) :
    l.foldl (fun a x => a + g x) 0 = (l.map g).sum := by
  suffices h : ∀ (z : Nat) (l : List α), l.foldl (fun a x => a + g x) z
      = z + (l.map g).sum by
    simpa using h 0 l
  intro z l
  induction l generalizing z with
  | nil => simp
  | cons x l ih =>
      rw [List.foldl_cons, List.map_cons, List.sum_cons, ih (z + g x)]
      omega

/-! ### summarize の成分刻画 -/

theorem foldl_plus_from_zero (z : Totals) (ws : List (Option Int)) :
    ws.foldl (fun a v => plus a (weighted 1 v)) z
      = plus z (ws.foldl (fun a v => plus a (weighted 1 v)) zero) := by
  induction ws generalizing z with
  | nil => simp [plus_zero]
  | cons v ws ih =>
      show List.foldl (fun a w => plus a (weighted 1 w)) (plus z (weighted 1 v)) ws
          = plus z
            (List.foldl (fun a w => plus a (weighted 1 w)) (plus zero (weighted 1 v)) ws)
      rw [ih (plus z (weighted 1 v)), ih (plus zero (weighted 1 v)), plus_zero']
      exact plus_assoc z (weighted 1 v) _

theorem foldl_weighted (ws : List (Option Int)) (z : Totals) :
    ws.foldl (fun a v => plus a (weighted 1 v)) z = plus z (summarize ws) :=
  foldl_plus_from_zero z ws

theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (summarize vs) (weighted 1 v) := by
  show (v :: vs).foldl (fun a v' => plus a (weighted 1 v')) zero = _
  rw [List.foldl_cons, foldl_weighted vs (plus zero (weighted 1 v)), plus_zero', plus_comm]

theorem plus_cnt (a b : Totals) : (plus a b).cnt = a.cnt + b.cnt := by
  cases a; cases b; rfl

theorem plus_nn (a b : Totals) : (plus a b).nn = a.nn + b.nn := by
  cases a; cases b; rfl

theorem plus_sum (a b : Totals) : (plus a b).sum = a.sum + b.sum := by
  cases a; cases b; rfl

theorem summarize_cnt (vs : List (Option Int)) : (summarize vs).cnt = vs.length := by
  induction vs with
  | nil => rfl
  | cons v vs ih =>
      rw [summarize_cons, plus_cnt, ih]
      cases v <;> simp [weighted]

theorem summarize_nn (vs : List (Option Int)) :
    (summarize vs).nn = (vs.filterMap id).length := by
  induction vs with
  | nil => rfl
  | cons v vs ih =>
      rw [summarize_cons, plus_nn, ih]
      cases v <;> simp [weighted]

theorem summarize_sum (vs : List (Option Int)) :
    (summarize vs).sum = (vs.map (fun v => v.getD 0)).sum := by
  induction vs with
  | nil => rfl
  | cons v vs ih =>
      rw [summarize_cons, plus_sum, ih]
      cases v <;> simp [weighted, List.map_cons, List.sum_cons, Int.add_comm]

theorem summarize_eq (vs : List (Option Int)) :
    summarize vs =
      ⟨vs.length, (vs.filterMap id).length, (vs.map (fun v => v.getD 0)).sum⟩ :=
  totals_ext (summarize_cnt vs) (summarize_nn vs) (summarize_sum vs)

theorem summarize_nil : summarize ([] : List (Option Int)) = zero := rfl

theorem summarize_append (vs ws : List (Option Int)) :
    summarize (vs ++ ws) = plus (summarize vs) (summarize ws) := by
  show (vs ++ ws).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_append, foldl_weighted vs zero,
    foldl_weighted ws (plus zero (summarize vs))]
  rw [plus_zero']

theorem summarize_perm {vs ws : List (Option Int)} (h : List.Perm vs ws) :
    summarize vs = summarize ws := by
  refine totals_ext ?_ ?_ ?_
  · rw [summarize_cnt, summarize_cnt, h.length_eq]
  · rw [summarize_nn, summarize_nn]
    have : List.Perm (vs.filterMap id) (ws.filterMap id) := h.filterMap id
    exact this.length_eq
  · rw [summarize_sum, summarize_sum]
    exact map_sum_perm_int (α := Option Int) (g := fun v : Option Int => v.getD 0) h

/-! ### predicate による分割 -/

/-- 排他的な 2 条件へ filter して連結するのは、論理和で filter する列の並べ替え。 -/
theorem filter_disjoint_perm {α : Type} (p q : α → Bool) (l : List α)
    (h : ∀ x ∈ l, (p x && q x) = false) :
    List.Perm (l.filter p ++ l.filter q) (l.filter (fun x => p x || q x)) := by
  induction l with
  | nil => exact List.Perm.nil
  | cons x l ih =>
      have hl : ∀ y ∈ l, (p y && q y) = false := by
        intro y hy; exact h y (List.mem_cons_of_mem x hy)
      by_cases hpx : p x = true
      · have hqx : q x = false := by simpa [hpx] using h x (by simp)
        rw [List.filter_cons_of_pos hpx,
          List.filter_cons_of_neg (p := q) (by simpa using hqx),
          List.filter_cons_of_pos (p := fun y => p y || q y) (by simp [hpx])]
        exact List.Perm.cons x (ih hl)
      · have hpx' : p x = false := Bool.eq_false_of_ne_true hpx
        by_cases hqx : q x = true
        · rw [List.filter_cons_of_neg (p := p) (by simpa using hpx'),
            List.filter_cons_of_pos (p := q) hqx,
            List.filter_cons_of_pos (p := fun y => p y || q y) (by simp [hpx', hqx])]
          have key : List.Perm
              (l.filter p ++ x :: l.filter q) (x :: (l.filter p ++ l.filter q)) :=
            (List.Perm.of_eq (List.append_assoc (l.filter p) [x] (l.filter q)).symm).trans
              ((List.perm_append_singleton x (l.filter p)).append (List.Perm.refl (l.filter q)))
          exact key.trans (List.Perm.cons x (ih hl))
        · have hqx' : q x = false := Bool.eq_false_of_ne_true hqx
          rw [List.filter_cons_of_neg (p := p) (by simpa using hpx'),
            List.filter_cons_of_neg (p := q) (by simpa using hqx'),
            List.filter_cons_of_neg (p := fun y => p y || q y) (by simp [hpx', hqx'])]
          exact ih hl

/-- 真偽分割：`p` と `!p` の filter を連結すると原列の並べ替え。 -/
theorem filter_not_partition {α : Type} (p : α → Bool) (l : List α) :
    List.Perm (l.filter p ++ l.filter (fun x => !p x)) l := by
  induction l with
  | nil => exact List.Perm.nil
  | cons x l ih =>
      cases h : p x with
      | true =>
          rw [List.filter_cons_of_pos h,
            List.filter_cons_of_neg (p := fun y => !p y) (by simp [h])]
          exact List.Perm.cons x ih
      | false =>
          rw [List.filter_cons_of_neg (p := p) (by simpa using h),
            List.filter_cons_of_pos (p := fun y => !p y) (by simp [h])]
          have key : List.Perm
              (l.filter p ++ x :: l.filter (fun y => !p y))
              (x :: l.filter p ++ l.filter (fun y => !p y)) :=
            (List.Perm.of_eq (List.append_assoc (l.filter p) [x] (l.filter (fun y => !p y))).symm).trans
              ((List.perm_append_singleton x (l.filter p)).append
                (List.Perm.refl (l.filter (fun y => !p y))))
          exact key.trans (List.Perm.cons x ih)

/-! ### eraseDups -/

theorem nodup_eraseDups [BEq α] [LawfulBEq α] : (l : List α) → l.eraseDups.Nodup
  | [] => List.Pairwise.nil
  | x :: l => by
      rw [List.eraseDups_cons]
      refine List.Pairwise.cons (fun b hb => ?_) (nodup_eraseDups _)
      intro hxb
      have h2 := List.mem_filter.mp (List.mem_eraseDups.mp hb)
      rw [hxb] at h2
      simp at h2
  termination_by l => l.length
  decreasing_by
    all_goals
      refine Nat.lt_of_le_of_lt (List.length_filter_le ..) ?_
      exact Nat.lt_succ_self _

/-- 条件 p を満たす要素の集合は `eraseDups` で不変。 -/
theorem all_eq_all_eraseDups [BEq α] [LawfulBEq α] (p : α → Bool) (l : List α) :
    l.all p = l.eraseDups.all p := by
  cases h1 : l.all p with
  | true =>
      have hall : ∀ x ∈ l, p x = true := List.all_eq_true.mp h1
      have hb : l.eraseDups.all p = true :=
        List.all_eq_true.mpr (fun x hx => hall x (List.mem_eraseDups.mp hx))
      exact hb.symm
  | false =>
      by_cases hl : ∃ x ∈ l, p x = false
      · obtain ⟨x, hx, hpx⟩ := hl
        have hx' : x ∈ l.eraseDups := List.mem_eraseDups.mpr hx
        cases h2 : l.eraseDups.all p with
        | true =>
            have htrue : p x = true := List.all_eq_true.mp h2 x hx'
            exact absurd (hpx ▸ htrue) (by simp)
        | false => rfl
      · have h1' : l.all p = true :=
          List.all_eq_true.mpr fun x hx => by
            by_cases hpx : p x = true
            · exact hpx
            · exact absurd ⟨x, hx, Bool.eq_false_of_ne_true hpx⟩ hl
        have h2' : l.eraseDups.all p = true :=
          List.all_eq_true.mpr fun x hx => by
            by_cases hpx : p x = true
            · exact hpx
            · exact absurd ⟨x, List.mem_eraseDups.mp hx, Bool.eq_false_of_ne_true hpx⟩ hl
        rw [h1] at h1'
        exact absurd h1' (by simp)

/-- 単一値列の `eraseDups`。 -/
theorem map_const_eraseDups {α : Type} [BEq α] [LawfulBEq α] (a : α) :
    ∀ (l : List α), l ≠ [] → (l.map (fun _ => a)).eraseDups = [a] := by
  intro l
  cases l with
  | nil => intro h; exact absurd rfl h
  | cons w l =>
      rw [List.map_cons, List.eraseDups_cons]
      have hfilter : List.filter (fun b => !(b == a)) (l.map fun _ => a) = [] := by
        apply filter_eq_nil_of_all_false
        intro y hy
        obtain ⟨u, _, hu⟩ := List.mem_map.mp hy
        subst hu
        have : (a == a) = true := beq_self_eq_true a
        simp [this]
      rw [hfilter]
      simp

/-- 条件 p を満たす要素のリストで `eraseDups` するとは、最初の出現だけを
    残すこと（`foldl_bucket_partition` のキー列用）。 -/
theorem foldl_bucket_partition {β : Type} (f : β → Nat) (g : β → Option Int) :
    ∀ (bs : List Nat), bs.Nodup →
      ∀ (z : Totals) (rows : List β),
        (bs.foldl (fun a b => plus a (summarize ((rows.filter (fun r => f r == b)).map g))) z)
          = plus z (summarize ((rows.filter (fun r => bs.contains (f r))).map g))
  | [], _, z, rows => by
      have hfil : rows.filter (fun r => List.contains ([] : List Nat) (f r)) = [] := by
        apply filter_eq_nil_of_all_false
        intro r _
        rfl
      rw [hfil, List.map_nil, summarize_nil, plus_zero]
      rfl
  | b :: bs, hnd, z, rows => by
      have hnd' : bs.Nodup := (List.nodup_cons.mp hnd).2
      have hb : b ∉ bs := (List.nodup_cons.mp hnd).1
      let p : β → Bool := fun r => f r == b
      let q : β → Bool := fun r => bs.contains (f r)
      have hdis : ∀ r ∈ rows, (p r && q r) = false := by
        intro r hr
        by_cases h : (f r == b) = true
        · by_cases hc : bs.contains (f r) = true
          · exfalso
            have hfb : f r = b := (beq_iff_eq (α := Nat)).mp h
            have hb' : b ∈ bs := hfb ▸ List.contains_iff_mem.mp hc
            exact absurd hb' hb
          · simp only [p, q, Bool.and_eq_false_imp]
            exact fun _ => Bool.eq_false_of_ne_true hc
        · simp [p, q, h]
      have hpart := filter_disjoint_perm p q rows hdis
      have hcombine : plus (summarize ((rows.filter p).map g))
          (summarize ((rows.filter q).map g))
          = summarize ((rows.filter (fun r => p r || q r)).map g) := by
        rw [← summarize_append, ← List.map_append]
        exact summarize_perm (hpart.map g)
      rw [List.foldl_cons, foldl_bucket_partition f g bs hnd']
      rw [plus_assoc]
      congr 1
      show plus (summarize ((rows.filter p).map g))
          (summarize ((rows.filter q).map g))
        = summarize ((rows.filter (fun r => (b :: bs).contains (f r))).map g)
      rw [hcombine]
      congr 1
      congr 1
      apply List.filter_congr
      intro r _
      simp only [p, q, List.contains_cons]

/-! ### 整列リスト（Bool 関係の takeWhile と filter） -/

/-- 関係 `le` について `p` の不成立が後方閉包（`a ≤ b` かつ `a` が不成立なら
    `b` も不成立）のとき、整列列の `takeWhile p` は `filter p` と一致する。 -/
theorem takeWhile_eq_filter_of_sorted {α : Type} (le : α → α → Bool) (p : α → Bool)
    (l : List α)
    (hle : ∀ {a b}, le a b = true → ¬ (p a = true) → ¬ (p b = true))
    (hp : l.Pairwise (fun a b => le a b = true)) : l.takeWhile p = l.filter p := by
  induction l with
  | nil => rfl
  | cons x l ih =>
      have hpc := List.pairwise_cons.mp hp
      by_cases hx : p x = true
      · rw [List.takeWhile_cons, ite_eq_left hx, List.filter_cons_of_pos hx]
        rw [ih hpc.2]
      · rw [List.takeWhile_cons, ite_eq_right hx]
        symm
        apply filter_eq_nil_of_all_false
        intro y hy
        rcases List.mem_cons.mp hy with rfl | hy'
        · exact Bool.eq_false_of_ne_true hx
        · exact Bool.eq_false_of_ne_true (hle (hpc.1 y hy') hx)

/-! ### 整列リストの一意性 -/

/-- `compare a b != .gt` は `isLE` と同じ。 -/
theorem ne_gt_iff_isLE {o : Ordering} : (o != Ordering.gt) = (o.isLE = true) := by
  cases o <;> rfl

theorem cmp_le_trans {α : Type} [Ord α] [Std.TransOrd α] {a b c : α}
    (h₁ : (compare a b != Ordering.gt) = true) (h₂ : (compare b c != Ordering.gt) = true) :
    (compare a c != Ordering.gt) = true := by
  rw [ne_gt_iff_isLE] at *
  exact Std.TransOrd.isLE_trans h₁ h₂

theorem cmp_le_total {α : Type} [LE α] [Ord α] [Std.LawfulOrderOrd α]
    [Std.IsLinearOrder α]
    (a b : α) : ((compare a b != Ordering.gt) || (compare b a != Ordering.gt)) = true := by
  by_cases h₁ : (compare a b != Ordering.gt) = true
  · simp [h₁]
  by_cases h₂ : (compare b a != Ordering.gt) = true
  · simp [h₂]
  have ha : ¬ a ≤ b := by
    intro hab
    have : (compare a b).isLE = true := (Std.LawfulOrderOrd.isLE_compare a b).mpr hab
    rw [ne_gt_iff_isLE] at h₁
    exact h₁ this
  have hb : ¬ b ≤ a := by
    intro hba
    have : (compare b a).isLE = true := (Std.LawfulOrderOrd.isLE_compare b a).mpr hba
    rw [ne_gt_iff_isLE] at h₂
    exact h₂ this
  cases Std.IsLinearOrder.le_total a b with
  | inl h => exact absurd h ha
  | inr h => exact absurd h hb

theorem cmp_antisymm {α : Type} [LE α] [Ord α] [Std.LawfulOrderOrd α] [Std.LawfulEqOrd α]
    {a b : α} (h₁ : (compare a b != Ordering.gt) = true)
    (h₂ : (compare b a != Ordering.gt) = true) : a = b := by
  have key {o : Ordering} (h : (o != Ordering.gt) = true) : o.isLE = true := by
    cases o <;> simp_all
  exact (Std.le_antisymm_iff (α := α)).mp
    ⟨(Std.LawfulOrderOrd.isLE_compare a b).mp (key h₁),
      (Std.LawfulOrderOrd.isLE_compare b a).mp (key h₂)⟩

/-- Bool 関係による整列＋重複なし列は所属する要素だけで決まる。 -/
theorem sorted_eq_of {α : Type} (le : α → α → Bool)
    (hant : ∀ a b, le a b = true → le b a = true → a = b)
    {l₁ l₂ : List α} (h₁ : l₁.Pairwise (fun a b => le a b = true))
    (h₂ : l₂.Pairwise (fun a b => le a b = true)) (hperm : List.Perm l₁ l₂) : l₁ = l₂ :=
  List.Perm.eq_of_pairwise (fun a b _ _ => hant a b) h₁ h₂ hperm

theorem mergeSort_eraseDups_eq_of {α : Type} [BEq α] [LawfulBEq α] (le : α → α → Bool)
    (htrans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (htotal : ∀ a b, (le a b || le b a) = true)
    (hant : ∀ a b, le a b = true → le b a = true → a = b)
    (xs ys : List α) (hmem : ∀ a, a ∈ xs ↔ a ∈ ys) :
    xs.eraseDups.mergeSort le = ys.eraseDups.mergeSort le := by
  refine sorted_eq_of le hant (List.pairwise_mergeSort htrans htotal _)
    (List.pairwise_mergeSort htrans htotal _) ?_
  refine perm_of_nodup_mem ?_ (ys.eraseDups.mergeSort le) ?_ ?_
  · exact (List.mergeSort_perm _ _).nodup_iff.mpr (nodup_eraseDups _)
  · exact (List.mergeSort_perm _ _).nodup_iff.mpr (nodup_eraseDups _)
  · intro a
    rw [List.mem_mergeSort, List.mem_eraseDups, List.mem_mergeSort, List.mem_eraseDups]
    exact hmem a

/-- 法な `Ord` 付き型（`compare` ベース）の整列一意性。 -/
theorem mergeSort_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α]
    [LE α] [Ord α] [Std.LawfulOrderOrd α] [Std.LawfulEqOrd α] [Std.TransOrd α]
    [Std.IsLinearOrder α]
    (xs ys : List α) (hmem : ∀ a, a ∈ xs ↔ a ∈ ys) :
    xs.eraseDups.mergeSort (fun a b => compare a b != Ordering.gt)
      = ys.eraseDups.mergeSort (fun a b => compare a b != Ordering.gt) :=
  mergeSort_eraseDups_eq_of (fun a b => compare a b != Ordering.gt)
    (fun _ _ _ => cmp_le_trans) cmp_le_total (fun _ _ => cmp_antisymm) xs ys hmem

/-- 重複なし列の `eraseDups` は恒等。 -/
theorem eraseDups_of_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α)
    (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons b ys ih =>
      obtain ⟨hbn, hnd⟩ := List.nodup_cons.mp h
      rw [List.eraseDups_cons]
      rw [List.filter_eq_self.mpr (fun c hc => by
        simpa using fun hca : c = b => hbn (hca ▸ hc))]
      rw [ih hnd]

/-- `mergeSort` の出力は `le` について整列（推移・全体の前向き Bool 関係）。 -/
theorem mergeSort_pairwise {α : Type} (le : α → α → Bool)
    (htrans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (htotal : ∀ a b, (le a b || le b a) = true) (l : List α) :
    (l.mergeSort le).Pairwise (fun a b => le a b = true) :=
  List.pairwise_mergeSort htrans htotal l

/-- `decide (· ≤ ·)` 関係の推移性（推移性から持ち上げ）。 -/
theorem decide_rel_trans {α : Type} {r : α → α → Prop} [DecidableRel r]
    (htr : ∀ a b c, r a b → r b c → r a c) (a b c : α)
    (h₁ : (decide (r a b)) = true) (h₂ : (decide (r b c)) = true) :
    (decide (r a c)) = true :=
  decide_eq_true (htr a b c (of_decide_eq_true h₁) (of_decide_eq_true h₂))

/-- `decide (· ≤ ·)` 関係の全体性（全体性から持ち上げ）。 -/
theorem decide_rel_total {α : Type} {r : α → α → Prop} [DecidableRel r]
    (htotal : ∀ a b, r a b ∨ r b a) (a b : α) :
    ((decide (r a b)) || (decide (r b a))) = true := by
  cases htotal a b with
  | inl h => rw [decide_eq_true h, Bool.true_or]
  | inr h =>
      by_cases h' : decide (r a b) = true
      · rw [h', Bool.true_or]
      · rw [Bool.eq_false_of_ne_true h', Bool.false_or, decide_eq_true h]

/-- 素の `mergeSort`（`decide (· ≤ ·)` 既定関係）の整列一意性。 -/
theorem mergeSort_unique_decide {α : Type} [BEq α] [LawfulBEq α] [LE α]
    [Std.IsLinearOrder α] [DecidableRel ((· ≤ ·) : α → α → Prop)]
    (htrans : ∀ a b c : α, a ≤ b → b ≤ c → a ≤ c)
    (xs ys : List α) (hmem : ∀ a, a ∈ xs ↔ a ∈ ys) :
    xs.eraseDups.mergeSort (fun a b => decide (a ≤ b))
      = ys.eraseDups.mergeSort (fun a b => decide (a ≤ b)) := by
  refine mergeSort_eraseDups_eq_of (fun a b => decide (a ≤ b)) ?_ ?_ ?_ xs ys hmem
  · intro a b c h₁ h₂
    exact decide_eq_true (htrans a b c (of_decide_eq_true h₁) (of_decide_eq_true h₂))
  · intro a b
    by_cases hd : decide (a ≤ b) = true
    · rw [hd, Bool.true_or]
    · rw [Bool.eq_false_of_ne_true hd, Bool.false_or]
      cases Std.IsLinearOrder.le_total (α := α) a b with
      | inl h => exact absurd (decide_eq_true h) hd
      | inr h => exact decide_eq_true h
  · intro a b h₁ h₂
    exact (Std.le_antisymm_iff (α := α)).mp
      ⟨of_decide_eq_true h₁, of_decide_eq_true h₂⟩

/-- 並べ替えの証拠だけから `mergeSort` の `eraseDups` 除去を畳み込む（順序公理不要）。 -/
theorem mergeSort_unique {α : Type} [BEq α] [LawfulBEq α] {le : α → α → Bool}
    {ys : List α} (h1 : List.Perm (ys.eraseDups.mergeSort le) ys) :
    ys.mergeSort le = ys.eraseDups.mergeSort le :=
  congrArg (fun L : List α => L.mergeSort le)
    ((eraseDups_of_nodup ys
        ((List.Perm.nodup_iff h1).mp
          ((List.Perm.nodup_iff (List.mergeSort_perm ys.eraseDups le)).mpr
            (nodup_eraseDups ys)))).symm)

/-! ### Option String の整列一意性（LawfulOrderOrd インスタンスが無いので手当て） -/

theorem optStr_cmp_total : ∀ (a b : Option String),
    ((compare a b != Ordering.gt) || (compare b a != Ordering.gt)) = true := by
  intro a b
  cases a with
  | none => cases b with
    | none => rfl
    | some y => rfl
  | some x =>
      cases b with
      | none => rfl
      | some y => exact cmp_le_total (α := String) x y

theorem optStr_cmp_antisymm : ∀ (a b : Option String),
    (compare a b != Ordering.gt) = true → (compare b a != Ordering.gt) = true → a = b := by
  intro a b
  cases a with
  | none =>
      cases b with
      | none => intro _ _; rfl
      | some y =>
          intro _ h₂
          exact absurd h₂ (by
            have key : (compare (some y) (none : Option String) != Ordering.gt) = false := rfl
            rw [key]
            simp)
  | some x =>
      cases b with
      | none =>
          intro h₁ _
          exact absurd h₁ (by
            have key : (compare (some x) (none : Option String) != Ordering.gt) = false := rfl
            rw [key]
            simp)
      | some y => intro h₁ h₂; exact congrArg some (cmp_antisymm (α := String) h₁ h₂)

theorem optStr_mergeSort_eraseDups_eq (xs ys : List (Option String))
    (hmem : ∀ a, a ∈ xs ↔ a ∈ ys) :
    xs.eraseDups.mergeSort (fun a b => compare a b != Ordering.gt)
      = ys.eraseDups.mergeSort (fun a b => compare a b != Ordering.gt) :=
  mergeSort_eraseDups_eq_of (fun a b => compare a b != Ordering.gt)
    (fun _ _ _ => cmp_le_trans (α := Option String)) optStr_cmp_total
    (fun a b => optStr_cmp_antisymm a b) xs ys hmem

/-! ### Int の max?/min? と並べ替え・スケーリング -/

theorem int_max?_perm {xs ys : List Int} (p : List.Perm xs ys) : xs.max? = ys.max? := by
  cases h : xs.max? with
  | none =>
      have hxs : xs = [] := List.max?_eq_none_iff.mp h
      subst hxs
      have hys : ys = [] := List.Perm.eq_nil p.symm
      subst hys
      exact h
  | some a =>
      obtain ⟨ham, hge⟩ := (List.max?_eq_some_iff (α := Int)).mp h
      exact ((List.max?_eq_some_iff (α := Int)).mpr
        ⟨p.mem_iff.mp ham, fun b hb => hge b (p.mem_iff.mpr hb)⟩).symm

theorem int_min?_perm {xs ys : List Int} (p : List.Perm xs ys) : xs.min? = ys.min? := by
  cases h : xs.min? with
  | none =>
      have hxs : xs = [] := List.min?_eq_none_iff.mp h
      subst hxs
      have hys : ys = [] := List.Perm.eq_nil p.symm
      subst hys
      exact h
  | some a =>
      obtain ⟨ham, hge⟩ := (List.min?_eq_some_iff (α := Int)).mp h
      exact ((List.min?_eq_some_iff (α := Int)).mpr
        ⟨p.mem_iff.mp ham, fun b hb => hge b (p.mem_iff.mpr hb)⟩).symm

/-- 正係数の掛け算は `max?`/`min?` と交換する。 -/
theorem int_max?_map_mul (m : Int) (hm : 0 < m) (xs : List Int) :
    (xs.map (fun x => x * m)).max? = xs.max?.map (fun x => x * m) := by
  cases h : xs.max? with
  | none =>
      have hxs : xs = [] := List.max?_eq_none_iff.mp h
      subst hxs
      simp
  | some a =>
      obtain ⟨ham, hge⟩ := (List.max?_eq_some_iff (α := Int)).mp h
      show (xs.map (fun x => x * m)).max? = some (a * m)
      refine List.max?_eq_some_iff.mpr ⟨List.mem_map.mpr ⟨a, ham, rfl⟩, ?_⟩
      intro y hy
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hy
      have hmul := (Int.mul_le_mul_left (a := m) hm).mpr (hge x hx)
      rwa [Int.mul_comm m x, Int.mul_comm m a] at hmul

theorem int_min?_map_mul (m : Int) (hm : 0 < m) (xs : List Int) :
    (xs.map (fun x => x * m)).min? = xs.min?.map (fun x => x * m) := by
  cases h : xs.min? with
  | none =>
      have hxs : xs = [] := List.min?_eq_none_iff.mp h
      subst hxs
      simp
  | some a =>
      obtain ⟨ham, hge⟩ := (List.min?_eq_some_iff (α := Int)).mp h
      show (xs.map (fun x => x * m)).min? = some (a * m)
      refine List.min?_eq_some_iff.mpr ⟨List.mem_map.mpr ⟨a, ham, rfl⟩, ?_⟩
      intro y hy
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hy
      have hmul := (Int.mul_le_mul_left (a := m) hm).mpr (hge x hx)
      rwa [Int.mul_comm m x, Int.mul_comm m a] at hmul

/-! ### ヒストグラム畳み込み（Nat 値） -/

theorem filter_eq_singleton_of_pairwise {α β : Type} [BEq α] [LawfulBEq α] (k : α) (v : β) :
    ∀ (xs : List (α × β)), (k, v) ∈ xs →
      xs.Pairwise (fun p q => (p.1 == q.1) = false) →
      xs.filter (fun p => (k == p.1)) = [(k, v)]
  | [], hmem, _ => absurd hmem (by simp)
  | (a, b) :: xs, hmem, hp => by
      have hpc := List.pairwise_cons.mp hp
      by_cases hk : (k == a) = true
      · have hka : k = a := (beq_iff_eq (α := α)).mp hk
        subst hka
        have hnil : xs.filter (fun p => (k == p.1)) = [] := by
          apply filter_eq_nil_of_all_false
          intro q hq
          exact hpc.1 q hq
        rw [List.filter_cons_of_pos (p := fun t : α × β => (k == t.1)) (by simpa using hk), hnil]
        cases List.mem_cons.mp hmem with
        | inl heq =>
            cases heq
            rfl
        | inr hmem' =>
            exfalso
            have hf : (k, v) ∈ xs.filter (fun p => (k == p.1)) :=
              List.mem_filter.mpr ⟨hmem', beq_self_eq_true (α := α) k⟩
            rw [hnil] at hf
            exact absurd hf (by simp)
      · have hk' : (k == a) = false := Bool.eq_false_of_ne_true hk
        rcases List.mem_cons.mp hmem with heq | hmem'
        · have hka : k = a := congrArg Prod.fst heq
          rw [hka] at hk
          exact absurd (beq_self_eq_true a) hk
        · rw [List.filter_cons_of_neg (p := fun t : α × β => (k == t.1)) (by simpa using hk)]
          exact filter_eq_singleton_of_pairwise k v xs hmem' hpc.2

theorem sum_filter_partition {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    (L.map g).sum =
      ((L.filter p).map g).sum + ((L.filter (fun x => !p x)).map g).sum := by
  rw [← map_sum_perm g (filter_not_partition p L), List.map_append, List.sum_append]

theorem toList_matching_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Nat) :
    ((m.toList.filter (fun p => (k == p.1))).map fun p => p.2 * f p.1).sum =
      m.getD k 0 * f k := by
  by_cases hmem : k ∈ m
  · have hget : m[k]? = some (m.getD k 0) := Std.HashMap.getElem?_eq_some_getD hmem
    obtain ⟨k', hbeq, hm'⟩ :=
      (Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList (m := m)).mp hget
    have hk'eq : k = k' := by simpa using hbeq
    subst k'
    have hfilt : m.toList.filter (fun p => (k == p.1)) = [(k, m.getD k 0)] :=
      filter_eq_singleton_of_pairwise k (m.getD k 0) m.toList hm'
        (Std.HashMap.distinct_keys_toList (m := m))
    rw [hfilt]
    simp
  · have hfilt : m.toList.filter (fun p => (k == p.1)) = [] := by
      apply filter_eq_nil_of_all_false
      intro p hp
      by_cases hbeq : (k == p.1) = true
      · have hgetp : m[p.1]? = some p.2 :=
          (Std.HashMap.mem_toList_iff_getElem?_eq_some (m := m)).mp hp
        have hkeq : k = p.1 := (beq_iff_eq (α := α)).mp hbeq
        have hg2 : m[k]? = some p.2 := hkeq ▸ hgetp
        rw [Std.HashMap.getElem?_eq_none hmem] at hg2
        exact absurd hg2 (by simp)
      · exact Bool.eq_false_of_ne_true hbeq
    rw [hfilt, List.map_nil, List.sum_nil]
    have hzero : m.getD k 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, Std.HashMap.getElem?_eq_none hmem]
      rfl
    rw [hzero, Nat.zero_mul]

/-- `k` と異なるキーだけを残す filter は、`toList_insert_perm` のものと一致する。 -/
theorem filter_notkey_eq {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) :
    m.toList.filter (fun x => !(k == x.1)) =
      m.toList.filter (fun x : α × Nat => decide (¬(k == x.1))) := by
  apply List.filter_congr
  intro x _
  cases h : (k == x.1) <;> simp [h]

theorem hist_insert_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Nat) :
    ((m.insert k (m.getD k 0 + 1)).toList.map fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + f k := by
  let v := m.getD k 0 + 1
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have hsum := map_sum_perm (fun p : α × Nat => p.2 * f p.1) hp
  have hmatch := toList_matching_sum m k f
  have hflt := filter_notkey_eq (m := m) (k := k)
  have hpart := sum_filter_partition m.toList (fun p : α × Nat => (k == p.1))
    (fun p => p.2 * f p.1)
  rw [hsum, ← hflt]
  simp only [List.map_cons, List.sum_cons]
  rw [hpart, hmatch, show v = m.getD k 0 + 1 from rfl, Nat.add_mul, Nat.one_mul]
  omega

theorem histogram_foldl_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (m : Std.HashMap α Nat) (f : α → Nat) :
    ((xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).toList.map
        fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + (xs.map f).sum := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, hist_insert_sum]
      simp only [List.map_cons, List.sum_cons]
      omega

theorem histogram_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Nat) :
    ((histogram xs).toList.map fun p => p.2 * f p.1).sum = (xs.map f).sum := by
  unfold histogram
  have h := histogram_foldl_sum xs ({} : Std.HashMap α Nat) f
  simpa using h

/-- `histogram` の toList 畳み込み（積付き, foldl 形）。 -/
theorem histogram_toList_foldl_add {α : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] (xs : List α) (f : α → Nat) :
    (histogram xs).toList.foldl (fun a (k, n) => a + f k * n) 0 = (xs.map f).sum := by
  have key : ∀ (L : List (α × Nat)) (z : Nat),
      L.foldl (fun a (k, n) => a + f k * n) z = z + (L.map fun p => p.2 * f p.1).sum := by
    intro L
    induction L with
    | nil => intro z; simp
    | cons p L ih =>
        cases p with
        | mk k n =>
            intro z
            simp only [List.foldl_cons, List.map_cons, List.sum_cons, ih (z + f k * n)]
            rw [show f k * n = n * f k from Nat.mul_comm _ _]
            omega
  unfold histogram
  rw [key _ 0, Nat.zero_add]
  exact histogram_sum xs f

theorem histogram_getD {α : Type} [BEq α] [LawfulBEq α] [Hashable α] [LawfulHashable α]
    (xs : List α) (a : α) : (histogram xs).getD a 0 = xs.count a := by
  unfold histogram
  suffices h : ∀ (m : Std.HashMap α Nat),
      (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD a 0 = m.getD a 0 + xs.count a by
    rw [h]
    simp
  induction xs with
  | nil => intro m; simp
  | cons x xs ih =>
      intro m
      rw [List.foldl_cons, ih, List.count_cons, Std.HashMap.getD_insert]
      cases h : (x == a)
      · simp [h]
      · have : a = x := ((beq_iff_eq (α := α)).mp h).symm
        subst this
        simp
        omega

theorem histogram_toList_foldl {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Nat) :
    (histogram xs).toList.foldl (fun a (k, n) => a + f k * n) 0
      = xs.foldl (fun a k => a + f k) 0 := by
  rw [histogram_toList_foldl_add, foldl_add_map_eq_sum]

/-! ### histogram 畳み込み（Totals 対象） -/

theorem foldl_entries (f : α → Totals) (L : List (α × Nat)) (z : Totals) :
    L.foldl (fun a (k, n) => plus a (scaleT (f k) n)) z
      = plus z (TSum (L.map fun p => scaleT (f p.1) p.2)) := by
  have h : L.foldl (fun a p => plus a (scaleT (f p.1) p.2)) z
      = plus z (TSum (L.map fun p => scaleT (f p.1) p.2)) := by
    rw [← List.foldl_map]
    exact foldl_plus _ _
  exact h

theorem toList_matching_scale {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Totals) :
    TSum ((m.toList.filter (fun p => (k == p.1))).map fun p => scaleT (f p.1) p.2)
      = scaleT (f k) (m.getD k 0) := by
  by_cases hmem : k ∈ m
  · have hget : m[k]? = some (m.getD k 0) := Std.HashMap.getElem?_eq_some_getD hmem
    obtain ⟨k', hbeq, hm'⟩ :=
      (Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList (m := m)).mp hget
    have hk'eq : k = k' := by simpa using hbeq
    subst k'
    have hfilt : m.toList.filter (fun p => (k == p.1)) = [(k, m.getD k 0)] :=
      filter_eq_singleton_of_pairwise k (m.getD k 0) m.toList hm'
        (Std.HashMap.distinct_keys_toList (m := m))
    rw [hfilt]
    simp [TSum, plus_zero]
  · have hget : m[k]? = none := Std.HashMap.getElem?_eq_none hmem
    have hno : ∀ p ∈ m.toList, (k == p.1) = false := by
      intro p hp
      by_cases hbeq : (k == p.1) = true
      · have hgetp : m[p.1]? = some p.2 :=
          (Std.HashMap.mem_toList_iff_getElem?_eq_some (m := m)).mp hp
        have hkeq : k = p.1 := (beq_iff_eq (α := α)).mp hbeq
        have hg2 : m[k]? = some p.2 := hkeq ▸ hgetp
        rw [hget] at hg2
        exact absurd hg2 (by simp)
      · exact Bool.eq_false_of_ne_true hbeq
    have hfilt : m.toList.filter (fun p => (k == p.1)) = [] := by
      apply filter_eq_nil_of_all_false
      intro p hp
      exact hno p hp
    rw [hfilt]
    simp [TSum]
    have hzero : m.getD k 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hget]
      rfl
    rw [hzero]
    exact (scale_zero _).symm

theorem TSum_filter_partition {α : Type} (L : List α) (p : α → Bool) (g : α → Totals) :
    TSum (L.map g) =
      plus (TSum ((L.filter p).map g)) (TSum ((L.filter (fun x => !p x)).map g)) := by
  rw [← TSum_perm ((filter_not_partition p L).map g), List.map_append, TSum_append]

theorem hist_insert_scale {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Totals) :
    TSum ((m.insert k (m.getD k 0 + 1)).toList.map fun p => scaleT (f p.1) p.2)
      = plus (TSum (m.toList.map fun p => scaleT (f p.1) p.2)) (f k) := by
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := m.getD k 0 + 1)
  have hsum := TSum_perm (hp.map (fun p : α × Nat => scaleT (f p.1) p.2))
  have hmatch := toList_matching_scale m k f
  have hflt := filter_notkey_eq (m := m) (k := k)
  have hpart := TSum_filter_partition m.toList (fun p : α × Nat => (k == p.1))
    (fun p => scaleT (f p.1) p.2)
  rw [hsum, ← hflt]
  show plus (scaleT (f k) (m.getD k 0 + 1))
      (TSum ((m.toList.filter (fun x => !(k == x.1))).map fun p => scaleT (f p.1) p.2))
    = plus (TSum (m.toList.map fun p => scaleT (f p.1) p.2)) (f k)
  rw [scale_add, scale_one, hpart, hmatch]
  exact plus_right_swap _ _ _

theorem histogram_scale {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Totals) :
    (histogram xs).toList.foldl (fun a (k, n) => plus a (scaleT (f k) n)) zero
      = xs.foldl (fun a k => plus a (f k)) zero := by
  unfold histogram
  suffices h : ∀ (m : Std.HashMap α Nat),
      (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).toList.foldl
          (fun a (k, n) => plus a (scaleT (f k) n)) zero
        = xs.foldl (fun a k => plus a (f k))
            (m.toList.foldl (fun a (k, n) => plus a (scaleT (f k) n)) zero) by
    have h0 := h {}
    simp only [Std.HashMap.toList_empty, List.foldl_nil] at h0
    exact h0
  induction xs with
  | nil => intro m; exact rfl
  | cons x xs ih =>
      intro m
      rw [List.foldl_cons, ih, List.foldl_cons]
      congr 1
      rw [foldl_entries f (m.insert x (m.getD x 0 + 1)).toList zero,
        foldl_entries f m.toList zero, hist_insert_scale m x f, plus_zero', plus_zero']

end TinylambOpt.ResearchProofs.CloudBI.Helpers
