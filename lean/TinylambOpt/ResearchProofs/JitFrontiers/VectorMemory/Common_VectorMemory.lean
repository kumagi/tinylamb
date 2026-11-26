import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

/-! ---------- tiny generic list/option helpers ---------- -/

theorem option_bind_map_isSome {α β : Type} (o : Option α) (f : α → β) :
    (o.bind (fun x => some (f x))).isSome = o.isSome := by
  cases o <;> simp

theorem option_bind_cons {α β : Type} (o : Option (List α)) (b : α) (g : List α → Option β) :
    (o.bind (fun l => some (b :: l))).bind g = o.bind (fun l => g (b :: l)) := by
  cases o <;> simp

theorem mapM_option_congr {α β : Type} (l : List α) (f g : α → Option β)
    (h : ∀ a ∈ l, f a = g a) : l.mapM f = l.mapM g := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.mapM_cons, List.mapM_cons]
      have hga := h a (by simp)
      cases hg : g a with
      | none =>
          have hf : f a = none := by rw [hga, hg]
          simp [hf]
      | some b =>
          have hf : f a = some b := by rw [hga, hg]
          simp [hf, ih (fun x hx => h x (by simp [hx]))]

theorem mapM_option_isSome {α β : Type} (l : List α) (f : α → Option β) :
    (l.mapM f).isSome = true ↔ ∀ a ∈ l, (f a).isSome = true := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.mapM_cons]
      cases hfa : f a with
      | none => simp [hfa]
      | some b => simp [option_bind_map_isSome, ih, hfa]

theorem mapM_foldlM_eq {α β γ : Type} (l : List α) (f : α → Option β)
    (g : γ → β → Option γ) (init : γ) :
    (l.mapM f).bind (fun bs => bs.foldlM g init) =
      l.foldlM (fun m a => (f a).bind (fun b => g m b)) init := by
  induction l generalizing init with
  | nil => simp
  | cons a as ih =>
      rw [List.mapM_cons]
      cases hfa : f a with
      | none => simp [hfa]
      | some b =>
          simp [option_bind_cons, hfa]
          rw [Option.bind_comm]
          apply congrArg
          funext s'
          exact ih s'

theorem load_congr_addrs (m n : List Nat) (addrs : List Nat)
    (h : ∀ a ∈ addrs, m[a]? = n[a]?) :
    addrs.mapM (fun a => m[a]?) = addrs.mapM (fun a => n[a]?) := by
  exact mapM_option_congr addrs (fun a => m[a]?) (fun a => n[a]?) h

theorem load_congr (k : Kernel) (j : Job) (m n : List Nat) (i : Nat)
    (h : ∀ a ∈ k.loads j i, m[a]? = n[a]?) :
    load k j m i = load k j n i := by
  simp only [load]
  rw [load_congr_addrs m n (k.loads j i) h]

theorem load_set_preserve (k : Kernel) (j : Job) (mem : List Nat) (i addr x : Nat)
    (hnot : addr ∉ k.loads j i) (_haddr : addr < mem.length) :
    load k j (mem.set addr x) i = load k j mem i := by
  apply load_congr k j (mem.set addr x) mem i
  intro a ha
  have hne : addr ≠ a := by
    intro heq
    apply hnot
    simpa [heq] using ha
  simp [hne]

theorem load_isSome_eq (k : Kernel) (j : Job) (mem : List Nat) (i : Nat) :
    (load k j mem i).isSome = ((k.loads j i).mapM (fun a => mem[a]?)).isSome := by
  simp [load]

theorem load_isSome_iff (k : Kernel) (j : Job) (mem : List Nat) (i : Nat) :
    (load k j mem i).isSome = true ↔ ∀ a ∈ k.loads j i, a < mem.length := by
  rw [load_isSome_eq, mapM_option_isSome]
  constructor <;> intro h <;> intro a ha <;> simpa using h a ha

/-! ---------- GatherCache ---------- -/

theorem gatherCache_generic (k : Kernel) : gatherCache k := by
  intro j ids m n h
  apply List.map_congr_left
  intro i hi
  apply load_congr k j m n i
  intro a ha
  apply h a
  simp only [addresses, List.mem_eraseDups]
  exact List.mem_flatMap.mpr ⟨i, hi, ha⟩

/-! ---------- vector as a single foldlM ("applyAll") ---------- -/

theorem vector_eq_apply (k : Kernel) (j : Job) (mem : List Nat) (ids : List Nat) :
    vector k j mem ids =
      ids.foldlM (fun m i => (load k j mem i).bind (fun x => put m (k.store j i) x)) mem := by
  unfold vector
  change (ids.mapM (fun i => (load k j mem i).map (fun x => (k.store j i, x)))).bind
      (fun changes => changes.foldlM (fun m p => put m p.1 p.2) mem) = _
  rw [mapM_foldlM_eq]
  congr 1
  funext m i
  cases h : load k j mem i with
  | none => simp [h]
  | some x => simp [h]

theorem applyAll_from_isSome_iff (k : Kernel) (j : Job) (mem init : List Nat) (ids : List Nat)
    (hlen : init.length = mem.length) :
    (ids.foldlM (fun m i => (load k j mem i).bind (fun x => put m (k.store j i) x)) init).isSome = true ↔
      ∀ i ∈ ids, (∀ a ∈ k.loads j i, a < mem.length) ∧ k.store j i < mem.length := by
  induction ids generalizing init with
  | nil => simp
  | cons i is ih =>
      simp only [List.foldlM_cons]
      cases hl : load k j mem i with
      | none =>
          have hbad : ¬ (∀ a ∈ k.loads j i, a < mem.length) := by
            intro hgood
            have hsome : (load k j mem i).isSome = true := (load_isSome_iff k j mem i).mpr hgood
            simp [hl] at hsome
          simp [hl, hbad]
      | some x =>
          have hgood : ∀ a ∈ k.loads j i, a < mem.length :=
            (load_isSome_iff k j mem i).mp (by simp [hl])
          by_cases hs : k.store j i < mem.length
          · have hsi : k.store j i < init.length := by omega
            have hput : put init (k.store j i) x = some (init.set (k.store j i) x) := by
              simp [put, hsi]
            have hlen' : (init.set (k.store j i) x).length = mem.length := by
              simp [hlen]
            simp [hput]
            constructor
            · intro h
              exact ⟨⟨hgood, hs⟩, (ih (init.set (k.store j i) x) hlen').mp h⟩
            · intro h
              exact (ih (init.set (k.store j i) x) hlen').mpr h.2
          · have hsi : ¬ k.store j i < init.length := by omega
            have hput : put init (k.store j i) x = none := by
              simp [put, hsi]
            simp [hput, hs]

theorem applyAll_isSome_iff (k : Kernel) (j : Job) (mem : List Nat) (ids : List Nat) :
    (ids.foldlM (fun m i => (load k j mem i).bind (fun x => put m (k.store j i) x)) mem).isSome = true ↔
      ∀ i ∈ ids, (∀ a ∈ k.loads j i, a < mem.length) ∧ k.store j i < mem.length := by
  exact applyAll_from_isSome_iff k j mem mem ids rfl

theorem tailExact_generic (k : Kernel) : tailExact k := by
  intro j mem ids
  rw [vector_eq_apply, applyAll_isSome_iff]
  simp [valid, and_left_comm, and_assoc, and_comm]

/-! ---------- DependenceGuard ---------- -/

theorem scalar_append (k : Kernel) (j : Job) (mem : List Nat) (as bs : List Nat) :
    (scalar k j mem as).bind (fun m => scalar k j m bs) = scalar k j mem (as ++ bs) := by
  simp [scalar]

theorem foldlM_congr_step {s α : Type} (l : List α) (f g : s → α → Option s)
    (h : ∀ a ∈ l, ∀ m, f m a = g m a) (init : s) :
    l.foldlM f init = l.foldlM g init := by
  induction l generalizing init with
  | nil => simp
  | cons a as ih =>
      simp only [List.foldlM_cons]
      have hfirst := h a (by simp) init
      rw [hfirst]
      generalize ho : g init a = o
      cases o with
      | none => simp [ho]
      | some m' =>
          simp [ho]
          exact ih (fun b hb => h b (by simp [hb])) m'

theorem scalar_eq_apply_of_rawFree (k : Kernel) (j : Job) (mem : List Nat) (ids : List Nat)
    (h : rawFree k j ids) :
    scalar k j mem ids =
      ids.foldlM (fun m i => (load k j mem i).bind (fun x => put m (k.store j i) x)) mem := by
  induction ids generalizing mem with
  | nil => simp [scalar]
  | cons i is ih =>
      simp only [rawFree] at h
      rcases h with ⟨hnot, htail⟩
      have ih' := ih mem htail
      simp only [scalar, List.foldlM_cons]
      cases hl : load k j mem i with
      | none => simp [hl]
      | some x =>
          by_cases hs : k.store j i < mem.length
          · have hput : put mem (k.store j i) x = some (mem.set (k.store j i) x) := by
              simp [put, hs]
            simp [hput]
            have ih' := ih (mem.set (k.store j i) x) htail
            simp [scalar] at ih'
            have hloadeq : ∀ t ∈ is,
                load k j (mem.set (k.store j i) x) t = load k j mem t := by
              intro t ht
              apply load_set_preserve
              · intro hmem
                exact hnot t ht (by simpa [hmem])
              · exact hs
            rw [ih']
            apply foldlM_congr_step
            intro t ht m
            rw [hloadeq t ht]
          · have hput : put mem (k.store j i) x = none := by
              simp [put, hs]
            simp [hl, hput]

theorem dependenceGuard_generic (k : Kernel) : dependenceGuard k := by
  intro j mem ids h
  rw [vector_eq_apply]
  exact (scalar_eq_apply_of_rawFree k j mem ids h).symm

/-! ---------- ChunkLocal ---------- -/

theorem take_append_drop_take {α : Type} (l : List α) (a b : Nat) :
    l.take a ++ (l.drop a).take b = l.take (a + b) := by
  induction a generalizing l with
  | zero => simp
  | succ a ih =>
      cases l with
      | nil => simp
      | cons x xs => simp [Nat.succ_add, ih]

theorem q_covers (n w : Nat) (hw : 0 < w) : n ≤ ((n + w - 1) / w) * w := by
  let k := n / w
  let r := n % w
  have hdecomp : k * w + r = n := by simpa [k, r, Nat.mul_comm] using Nat.div_add_mod n w
  have hr : r < w := Nat.mod_lt n hw
  by_cases hr0 : r = 0
  · have hn : n = k * w := by omega
    have hsub : k * w + w - 1 = k * w + (w - 1) := by omega
    have hdiv : (n + w - 1) / w = k := by
      rw [hn, hsub]
      have hw1 : (w - 1) < w := by omega
      have hcalc : (w * k + (w - 1)) / w = k + (w - 1) / w := Nat.mul_add_div hw k (w - 1)
      rw [Nat.mul_comm w k] at hcalc
      rw [hcalc, Nat.div_eq_of_lt hw1]
      omega
    rw [hdiv, hn]
    exact Nat.le_refl _
  · let kw := k * w
    have hdecomp' : kw + r = n := by simpa [kw] using hdecomp
    have hmul : (k + 1) * w = kw + w := by simp [kw, Nat.succ_mul]
    have hk1 : k + 1 ≤ (n + w - 1) / w := by
      rw [Nat.le_div_iff_mul_le hw, hmul]
      omega
    have hnle : n ≤ (k + 1) * w := by
      rw [hmul]
      omega
    exact Nat.le_trans hnle (Nat.mul_le_mul_right w hk1)

theorem flatten_chunks (ids : List Nat) (w r : Nat) :
    ((List.range r).map (fun t => (ids.drop (t * w)).take w)).flatten = ids.take (r * w) := by
  induction r with
  | zero => simp
  | succ r ih =>
      rw [List.range_succ, List.map_append, List.flatten_append, ih]
      simp
      rw [take_append_drop_take, Nat.succ_mul]

theorem batches_flatten (w : Nat) (hw : 0 < w) (ids : List Nat) :
    (batches w ids).flatten = ids := by
  unfold batches
  rw [flatten_chunks]
  exact List.take_of_length_le (q_covers ids.length w hw)

theorem fold_batches_scalar (k : Kernel) (j : Job) (mem : List Nat) (bs : List (List Nat))
    (h : ∀ b ∈ bs, rawFree k j b) :
    bs.foldlM (vector k j) mem = scalar k j mem bs.flatten := by
  induction bs generalizing mem with
  | nil => simp [scalar]
  | cons b bs ih =>
      simp only [List.foldlM_cons]
      rw [dependenceGuard_generic k j mem b (h b (by simp))]
      cases hsc : scalar k j mem b with
      | none =>
          rw [show (b :: bs).flatten = b ++ bs.flatten by rfl]
          rw [← scalar_append k j mem b bs.flatten]
          simp [hsc]
      | some m' =>
          simp [hsc]
          rw [ih m' (fun c hc => h c (by simp [hc]))]
          rw [← scalar_append k j mem b bs.flatten]
          simp [hsc]

theorem chunkLocal_generic (k : Kernel) : chunkLocal k := by
  intro j mem ids w hw hraw
  have hfold := fold_batches_scalar k j mem (batches w ids)
      (by intro b hb; exact hraw b hb)
  rw [hfold, batches_flatten w hw ids]

theorem rawFree_append (k : Kernel) (j : Job) (as bs : List Nat) :
    rawFree k j (as ++ bs) ↔ rawFree k j as ∧ rawFree k j bs ∧
      ∀ a ∈ as, ∀ b ∈ bs, k.store j a ∉ k.loads j b := by
  induction as with
  | nil => simp [rawFree]
  | cons a as ih =>
      simp [rawFree, ih, and_assoc, and_left_comm, and_comm]
      intro hrbs hras
      constructor
      · intro h
        rcases h with ⟨hcross, hall⟩
        exact ⟨fun b hb => hall b (Or.inr hb), fun t ht => hall t (Or.inl ht), hcross⟩
      · intro h
        rcases h with ⟨hbs, has, hcross⟩
        exact ⟨hcross, fun t ht => match ht with | Or.inl hta => has t hta | Or.inr htb => hbs t htb⟩

theorem rawFree_range_of_pair (k : Kernel) (j : Job) (n : Nat)
    (h : ∀ i < n, ∀ t < n, i < t → k.store j i ∉ k.loads j t) :
    rawFree k j (List.range n) := by
  induction n with
  | zero => simp [rawFree]
  | succ n ih =>
      rw [List.range_succ, rawFree_append]
      refine ⟨ih (fun i hi t ht hit => h i (Nat.lt_trans hi (Nat.lt_succ_self n))
          t (Nat.lt_trans ht (Nat.lt_succ_self n)) hit), ?_, ?_⟩
      · simp [rawFree]
      · intro a ha b hb
        have ha' : a < n := by simpa using ha
        have hb' : b = n := by simpa using hb
        subst b
        exact h a (Nat.lt_trans ha' (Nat.lt_succ_self n)) n (Nat.lt_succ_self n) ha'

theorem rawFree_reverse_of_pair (k : Kernel) (j : Job) (n : Nat)
    (h : ∀ i < n, ∀ t < n, t < i → k.store j i ∉ k.loads j t) :
    rawFree k j (List.range n).reverse := by
  induction n with
  | zero => simp [rawFree]
  | succ n ih =>
      rw [List.range_succ, List.reverse_append]
      simp only [List.reverse_cons, List.reverse_nil]
      refine ⟨?_, ih (fun i hi t ht hti => h i (Nat.lt_trans hi (Nat.lt_succ_self n))
          t (Nat.lt_trans ht (Nat.lt_succ_self n)) hti)⟩
      intro t ht
      have ht' : t < n := by simpa using ht
      exact h n (Nat.lt_succ_self n) t (Nat.lt_trans ht' (Nat.lt_succ_self n)) ht'

theorem fwd_safe (j : Job) (n : Nat) (hj : j.dst ≤ j.src) :
    rawFree ForwardCopy.kernel j (List.range n) := by
  apply rawFree_range_of_pair
  intro i hi t ht hit
  simp [ForwardCopy.kernel]
  omega

theorem fwd_disj (j : Job) (n : Nat) (hj : j.src + n ≤ j.dst) :
    rawFree ForwardCopy.kernel j (List.range n) := by
  apply rawFree_range_of_pair
  intro i hi t ht hit
  simp [ForwardCopy.kernel]
  omega

theorem rev_reflect (j : Job) (i : Nat) (hi : i < j.window) :
    ReverseInput.kernel.loads j (j.window - 1 - i) = [j.src + i] := by
  simp [ReverseInput.kernel]
  congr 1
  omega

theorem rev_inplace (j : Job) (hs : j.src = j.dst) (hw : j.window = 2) :
    ¬ rawFree ReverseInput.kernel j [0, 1] := by
  rcases j with ⟨src, dst, stride, window, ids, mask⟩
  simp at hs hw
  subst window
  subst dst
  simp [ReverseInput.kernel, rawFree]

theorem stride_compress (j : Job) (n : Nat) (hd : j.dst ≤ j.src) (hs : 0 < j.stride) :
    rawFree StrideRead.kernel j (List.range n) := by
  apply rawFree_range_of_pair
  intro i hi t ht hit
  simp [StrideRead.kernel]
  have htle : t ≤ t * j.stride := by
    have := Nat.mul_le_mul_left t hs
    simpa using this
  omega

theorem filter_eq_nil_of_all_eq (a : Nat) (l : List Nat) (h : ∀ x ∈ l, x = a) :
    l.filter (fun b => ! (b == a)) = [] := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : x = a := h x (by simp)
      subst x
      simp [ih (fun y hy => h y (by simp [hy]))]

theorem eraseDups_const (a : Nat) (l : List Nat) (h : ∀ x ∈ l, x = a) :
    (a :: l).eraseDups = [a] := by
  simp [List.eraseDups_cons, filter_eq_nil_of_all_eq a l h]

theorem stride_broadcast (j : Job) (ids : List Nat) (hs : j.stride = 0) (hne : ids ≠ []) :
    addresses StrideRead.kernel j ids = [j.src] := by
  cases ids with
  | nil => contradiction
  | cons a as =>
      simp [addresses, StrideRead.kernel, hs]
      apply eraseDups_const
      intro x hx
      simp at hx
      simp [hx]

theorem strideStore_repeat (j : Job) (i : Nat) (hs : j.stride = 0) :
    StrideStore.kernel.store j i = j.dst := by
  simp [StrideStore.kernel, hs]

theorem neighbor_inplace (j : Job) (n : Nat) (hs : j.src = j.dst) :
    rawFree NeighborSum.kernel j (List.range n) := by
  rcases j with ⟨src, dst, stride, window, ids, mask⟩
  simp at hs
  subst dst
  apply rawFree_range_of_pair
  intro i hi t ht hit
  simp [NeighborSum.kernel]
  omega

theorem delta_reverse (j : Job) (n : Nat) (hs : j.src = j.dst) :
    rawFree BackwardDelta.kernel j (List.range n).reverse := by
  rcases j with ⟨src, dst, stride, window, ids, mask⟩
  simp at hs
  subst dst
  apply rawFree_reverse_of_pair
  intro i hi t ht hti
  simp [BackwardDelta.kernel]
  omega

theorem delta_first (j : Job) (mem : List Nat) (hs : j.src < mem.length) :
    load BackwardDelta.kernel j mem 0 = some 0 := by
  simp [BackwardDelta.kernel, load, hs]

theorem gather_same (j : Job) (mem : List Nat) (i t : Nat) (h : j.ids[i]? = j.ids[t]?) :
    load IndexedGather.kernel j mem i = load IndexedGather.kernel j mem t := by
  simp [IndexedGather.kernel, load, h]

theorem scatter_stable (j : Job) (i t : Nat) (h : j.ids[i]? = j.ids[t]?) :
    IndexedScatter.kernel.store j i = IndexedScatter.kernel.store j t := by
  simp [IndexedScatter.kernel, h]

theorem ring_periodic (j : Job) (mem : List Nat) (i : Nat) :
    load RingWindow.kernel j mem (i + j.window + 1) = load RingWindow.kernel j mem i := by
  have h1 : (i + j.window + 1) % (j.window + 1) = i % (j.window + 1) := by
    rw [Nat.add_assoc, Nat.add_mod_right]
  have h2 : (i + j.window + 1 + 1) % (j.window + 1) = (i + 1) % (j.window + 1) := by
    rw [show i + j.window + 1 + 1 = (i + 1) + (j.window + 1) by omega]
    rw [Nat.add_mod_right]
  simp [RingWindow.kernel, load, h1, h2]

theorem set_comm_of_ne (m : List Nat) (a b x y : Nat) (hne : a ≠ b) :
    (m.set a x).set b y = (m.set b y).set a x := by
  ext i
  simp only [List.getElem?_set]
  by_cases hia : a = i
  · by_cases hib : b = i
    · have : a = b := by rw [hia, hib]
      exact (hne this).elim
    · simp [hia, hib]
  · by_cases hib : b = i
    · simp [hia, hib]
    · simp [hia, hib]

theorem put_comm_of_ne (m : List Nat) (a b x y : Nat) (hne : a ≠ b) :
    (put m a x).bind (fun m' => put m' b y) =
      (put m b y).bind (fun m' => put m' a x) := by
  by_cases ha : a < m.length
  · by_cases hb : b < m.length
    · simp [put, ha, hb, set_comm_of_ne m a b x y hne]
    · rw [show put m b y = none by simp [put, hb]]
      simp [put, ha, hb]
  · by_cases hb : b < m.length
    · rw [show put m a x = none by simp [put, ha]]
      simp [put, ha, hb]
    · rw [show put m a x = none by simp [put, ha]]
      rw [show put m b y = none by simp [put, hb]]
      simp

theorem nodup_map_injective (f : Nat → Nat) (l : List Nat) (h : (l.map f).Nodup) :
    ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y := by
  induction l with
  | nil => simp
  | cons z zs ih =>
      simp only [List.map_cons, List.nodup_cons] at h
      rcases h with ⟨hnot, hnd⟩
      simp only [List.mem_cons]
      intro x hx y hy heq
      rcases hx with hxz | hx
      · rcases hy with hyz | hy
        · rw [hxz, hyz]
        · subst x
          have hmem : f y ∈ zs.map f := by exact List.mem_map.mpr ⟨y, hy, rfl⟩
          have hne : f z ≠ f y := by intro hfy; apply hnot; simpa [hfy] using hmem
          exact False.elim (hne heq)
      · rcases hy with hyz | hy
        · subst y
          have hmem : f x ∈ zs.map f := by exact List.mem_map.mpr ⟨x, hx, rfl⟩
          have hne : f z ≠ f x := by intro hfx; apply hnot; simpa [hfx] using hmem
          exact False.elim (hne heq.symm)
        · exact ih hnd x hx y hy heq

theorem perm_foldlM_eq {α s : Type} (len : s → Nat) (f : s → α → Option s)
    {l₁ l₂ : List α} (p : l₁.Perm l₂)
    (hlenf : ∀ m a r, f m a = some r → len r = len m)
    (hnone : ∀ a ∈ l₁, ∀ m m' : s, len m = len m' → (f m a).isSome = (f m' a).isSome)
    (comm : ∀ x ∈ l₁, ∀ y ∈ l₁, ∀ m : s,
      (f m x).bind (fun m' => f m' y) = (f m y).bind (fun m' => f m' x)) :
    ∀ init : s, l₁.foldlM f init = l₂.foldlM f init := by
  induction p using List.Perm.recOnSwap' with
  | nil => intro init; rfl
  | cons x p ih =>
      intro init
      simp only [List.foldlM_cons]
      cases hfx : f init x with
      | none => simp [hfx]
      | some init' =>
          exact ih
            (fun a ha m m' hlen => hnone a (by simp [ha]) m m' hlen)
            (fun a ha b hb m => comm a (by simp [ha]) b (by simp [hb]) m) init'
  | swap' x y p ih =>
      intro init
      simp only [List.foldlM_cons]
      cases hfx : f init x with
      | none =>
          cases hfy : f init y with
          | none => simp [hfy, hfx]
          | some sy =>
              have hsx : f sy x = none := by
                have hs := hnone x (by simp) init sy (hlenf init y sy hfy).symm
                simp [hfx] at hs
                cases h : f sy x with
                | none => rfl
                | some _ => simp [h] at hs
              simp [hfy, hsx, hfx]
      | some sx =>
          cases hfy : f init y with
          | none =>
              have hsy : f sx y = none := by
                have hs := hnone y (by simp) init sx (hlenf init x sx hfx).symm
                simp [hfy] at hs
                cases h : f sx y with
                | none => rfl
                | some _ => simp [h] at hs
              simp [hfy, hsy]
          | some sy =>
              simp [hfy, hfx]
              have hc := comm y (by simp) x (by simp) init
              simp [hfy, hfx] at hc
              rw [hc]
              cases hstep : f sx y with
              | none => simp [hstep]
              | some s' =>
                  simp [hstep]
                  exact ih
                    (fun a ha m m' hlen => hnone a (by simp [ha]) m m' hlen)
                    (fun a ha b hb m => comm a (by simp [ha]) b (by simp [hb]) m)
                    s'
  | trans p₁ p₂ ih₁ ih₂ =>
      intro init
      rw [ih₁ hnone comm init]
      exact ih₂
        (fun a ha m m' hlen => hnone a (p₁.symm.subset ha) m m' hlen)
        (fun a ha b hb m => comm a (p₁.symm.subset ha) b (p₁.symm.subset hb) m) init

theorem unorderedScatter_generic (k : Kernel) : unorderedScatter k := by
  intro j m a b hp hnod
  rw [vector_eq_apply, vector_eq_apply]
  let f : List Nat → Nat → Option (List Nat) :=
    fun init i => (load k j m i).bind (fun x => put init (k.store j i) x)
  change a.foldlM f m = b.foldlM f m
  apply perm_foldlM_eq List.length f hp
  · intro init i r hr
    cases hl : load k j m i with
    | none => simp [f, hl] at hr
    | some x =>
        simp [f, hl, put] at hr
        rcases hr with ⟨hlt, hset⟩
        subst r
        simp
  · intro i hi init init' hlen
    cases hl : load k j m i with
    | none => simp [f, hl]
    | some x =>
        have hv : k.store j i < init.length ↔ k.store j i < init'.length := by omega
        by_cases hlt : k.store j i < init.length
        · have hlt' : k.store j i < init'.length := hv.mp hlt
          simp [f, hl, put, hlt, hlt']
        · have hlt' : ¬ k.store j i < init'.length := fun hh => hlt (hv.mpr hh)
          simp [f, hl, put, hlt, hlt']
  · intro x hx y hy init
    have hxy : x = y ∨ k.store j x ≠ k.store j y := by
      by_cases h : x = y
      · exact Or.inl h
      · right
        intro heq
        exact h (nodup_map_injective (k.store j) a hnod x hx y hy heq)
    cases hxy with
    | inl heq => subst y; rfl
    | inr hne =>
        cases hlx : load k j m x with
        | none => simp [f, hlx]
        | some vx =>
            cases hly : load k j m y with
            | none => simp [f, hly]
            | some vy =>
                simp [f, hlx, hly, put_comm_of_ne init (k.store j x) (k.store j y) vx vy hne]

def applyAllFrom (k : Kernel) (j : Job) (mem init : List Nat) (ids : List Nat) : Option (List Nat) :=
  ids.foldlM (fun m i => (load k j mem i).bind (fun x => put m (k.store j i) x)) init

theorem applyAll_eq_foldl_set (k : Kernel) (j : Job) (mem init : List Nat) (ids : List Nat) (out : List Nat)
    (h : applyAllFrom k j mem init ids = some out) :
    out = ids.foldl (fun m i => m.set (k.store j i) ((load k j mem i).getD 0)) init := by
  induction ids generalizing init with
  | nil => simp [applyAllFrom] at h; simpa using h.symm
  | cons i is ih =>
      simp only [applyAllFrom, List.foldlM_cons] at h
      cases hl : load k j mem i with
      | none => simp [hl] at h
      | some x =>
          by_cases hv : k.store j i < init.length
          · have hput : put init (k.store j i) x = some (init.set (k.store j i) x) := by
              simp [put, hv]
            have ht : applyAllFrom k j mem (init.set (k.store j i) x) is = some out := by
              simpa [applyAllFrom, hl, hput] using h
            have ih' := ih (init.set (k.store j i) x) ht
            simp [hl]
            exact ih'
          · have hput : put init (k.store j i) x = none := by
              simp [put, hv]
            simp [hl, hput] at h




theorem foldl_set_getElem_set_ne_mem (mem : List Nat) (pairs : List (Nat × Nat))
    (a b x : Nat) (hb : b ≠ a) :
    (pairs.foldl (fun m p => m.set p.1 p.2) (mem.set b x))[a]? =
      (pairs.foldl (fun m p => m.set p.1 p.2) mem)[a]? := by
  induction pairs generalizing mem a b x with
  | nil => simp [hb]
  | cons p ps ih =>
      simp only [List.foldl_cons]
      by_cases hpb : p.1 = b
      · subst b
        simp [List.set_set]
      · have hnebp : b ≠ p.1 := fun h => hpb h.symm
        rw [set_comm_of_ne mem b p.1 x p.2 hnebp]
        exact ih (mem.set p.1 p.2) a b x hb

theorem foldl_set_getElem (mem : List Nat) (pairs : List (Nat × Nat)) (a : Nat)
    (hvalid : ∀ p ∈ pairs, p.1 < mem.length) :
    (pairs.foldl (fun m p => m.set p.1 p.2) mem)[a]? =
      match (pairs.filter (fun p => p.1 == a)).getLast? with
      | some p => some p.2
      | none => mem[a]? := by
  induction pairs generalizing mem with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      have hpv : p.1 < mem.length := hvalid p (by simp)
      have htail : ∀ q ∈ ps, q.1 < mem.length := fun q hq => hvalid q (by simp [hq])
      by_cases heq : p.1 == a
      · have hpa : p.1 = a := by simpa using heq
        have htail' : ∀ q ∈ ps, q.1 < (mem.set p.1 p.2).length := by
          simpa [List.length_set] using htail
        have ih' := ih (mem.set p.1 p.2) htail'
        have hfilter : (p :: ps).filter (fun q => q.1 == a) = p :: (ps.filter (fun q => q.1 == a)) := by
          simp [heq]
        rw [hfilter]
        rw [ih']
        rw [List.getLast?_cons]
        cases hgl : (ps.filter (fun q => q.1 == a)).getLast? with
        | none =>
            simp [List.getElem?_set, hpa]
            omega
        | some q => simp
      · have hpne : p.1 ≠ a := by
          intro h; apply heq; simpa [h]
        have hfilter : (p :: ps).filter (fun q => q.1 == a) = ps.filter (fun q => q.1 == a) := by
          simp [heq]
        rw [hfilter]
        rw [foldl_set_getElem_set_ne_mem mem ps a p.1 p.2 hpne]
        exact ih mem htail


theorem filter_map_pairs (f g : Nat → Nat) (l : List Nat) (a : Nat) :
    (l.map (fun t => (f t, g t))).filter (fun p => p.1 == a) =
      (l.filter (fun t => f t == a)).map (fun t => (f t, g t)) := by
  induction l with
  | nil => simp
  | cons t ts ih =>
      by_cases h : f t == a <;> simp [h, ih]

theorem deadStore_generic (k : Kernel) : deadStore k := by
  intro j mem ids out i hvec hi
  have happ : applyAllFrom k j mem mem ids = some out := by
    simpa [applyAllFrom, vector_eq_apply] using hvec
  have hval : ∀ t ∈ ids, (∀ a ∈ k.loads j t, a < mem.length) ∧ k.store j t < mem.length := by
    have hs : (applyAllFrom k j mem mem ids).isSome = true := by simp [happ]
    simpa [applyAllFrom] using (applyAll_isSome_iff k j mem ids).mp hs
  have hload : ∀ t ∈ ids, (load k j mem t).isSome = true := by
    intro t ht
    exact (load_isSome_iff k j mem t).mpr (hval t ht).1
  have hfoldset : out = ids.foldl (fun m t => m.set (k.store j t) ((load k j mem t).getD 0)) mem :=
    applyAll_eq_foldl_set k j mem mem ids out happ
  let pairs := ids.map (fun t => (k.store j t, (load k j mem t).getD 0))
  have hfoldpairs : out = pairs.foldl (fun m p => m.set p.1 p.2) mem := by
    rw [hfoldset]
    simp [pairs, List.foldl_map]
  let a := k.store j i
  have hvalidpairs : ∀ p ∈ pairs, p.1 < mem.length := by
    intro p hp
    simp [pairs] at hp
    rcases hp with ⟨t, ht, rfl⟩
    exact (hval t ht).2
  have hget := foldl_set_getElem mem pairs a hvalidpairs
  have hfilter : pairs.filter (fun p => p.1 == a) =
      (ids.filter (fun t => k.store j t == a)).map
        (fun t => (k.store j t, (load k j mem t).getD 0)) := by
    simpa [pairs] using filter_map_pairs (k.store j) (fun t => (load k j mem t).getD 0) ids a
  rw [hfilter, List.getLast?_map] at hget
  let filtered := ids.filter (fun t => k.store j t == a)
  have hne : filtered ≠ [] := by
    intro hnil
    have : i ∈ filtered := by simp [filtered, hi, a]
    simp [hnil] at this
  let last := filtered.getLast?.getD i
  have hlastmem : last ∈ ids := by
    cases hgl : filtered.getLast? with
    | none =>
        have hiF : i ∈ filtered := by simp [filtered, a, hi]
        have hiids : i ∈ ids := (List.mem_filter.mp hiF).1
        simpa [last, hgl] using hiids
    | some t =>
        have htlast : t ∈ filtered.getLast? := by simp [hgl]
        have htmem : t ∈ filtered := List.mem_of_mem_getLast? htlast
        have htids : t ∈ ids := (List.mem_filter.mp (by simpa only [filtered] using htmem)).1
        simpa [last, hgl] using htids
  have hloadlast : (load k j mem last).isSome = true := hload last hlastmem
  have hgetlast : some ((load k j mem last).getD 0) = load k j mem last := by
    cases hl : load k j mem last with
    | none => simp [hl] at hloadlast
    | some v => simp [hl]
  have hlast_eq : filtered.getLast? = some last := by
    cases hgl : filtered.getLast? with
    | none =>
        have hiF : i ∈ filtered := by simp [filtered, a, hi]
        have hnil : filtered = [] := List.getLast?_eq_none_iff.mp hgl
        simp [hnil] at hiF
    | some t => simp [last, hgl]
  change out[a]? = load k j mem last
  rw [hfoldpairs]
  rw [hget]
  simp [filtered, hlast_eq, hgetlast]

theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ as.filter (fun b => ! (b == a)) := List.mem_eraseDups.mp h
        simp at hf
      · exact nodup_eraseDups (as.filter (fun b => ! (b == a)))
termination_by l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => ! (b == a)) as
  omega

theorem nodup_map_of_injective (f : Nat → Nat) (l : List Nat)
    (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) (hn : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.map_cons, List.nodup_cons]
      have hn' := List.nodup_cons.mp hn
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨b, hb, heq⟩
        have hab : a = b := hinj a (by simp) b (by simp [hb]) heq.symm
        subst b
        exact hn'.1 hb
      · exact ih (fun x hx y hy h => hinj x (by simp [hx]) y (by simp [hy]) h) hn'.2

theorem stride_distinct (j : Job) (n : Nat) (hs : 0 < j.stride) :
    ((List.range n).map (StrideStore.kernel.store j)).Nodup := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_singleton]
      rw [List.nodup_append]
      refine ⟨ih, by simp, ?_⟩
      intro x hx y hy
      simp at hy
      subst y
      rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
      have hi' : i < n := by simpa using hi
      have hmul : i * j.stride < n * j.stride := Nat.mul_lt_mul_of_pos_right hi' hs
      simp [StrideStore.kernel]
      omega

theorem scatter_allDistinct (j : Job) (n : Nat) (hnle : n ≤ j.ids.length)
    (hnd : (j.ids.take n).Nodup) :
    ((List.range n).map (IndexedScatter.kernel.store j)).Nodup := by
  apply nodup_map_of_injective (IndexedScatter.kernel.store j) (List.range n) ?_ (List.nodup_range)
  intro a ha b hb heq
  have ha' : a < n := by simpa using ha
  have hb' : b < n := by simpa using hb
  have hlen : (j.ids.take n).length = n := by simp [hnle]
  have hgeta : (j.ids.take n).getD a 0 = j.ids[a]?.getD 0 := by
    simp [List.getElem?_take, ha']
  have hgetb : (j.ids.take n).getD b 0 = j.ids[b]?.getD 0 := by
    simp [List.getElem?_take, hb']
  have heqcode : j.ids[a]?.getD 0 = j.ids[b]?.getD 0 := by
    simp [IndexedScatter.kernel] at heq
    omega
  have hgeteq : (j.ids.take n).getD a 0 = (j.ids.take n).getD b 0 := by
    rw [hgeta, hgetb, heqcode]
  exact (hnd.getD_inj (by simpa [hlen] using ha') (by simpa [hlen] using hb')).mp hgeteq


theorem addresses_length_eq (k : Kernel) (j : Job) (ids ref : List Nat) (href : ref.Nodup)
    (hmem : ∀ a, a ∈ addresses k j ids ↔ a ∈ ref) :
    (addresses k j ids).length = ref.length := by
  have hp := (List.perm_ext_iff_of_nodup (nodup_eraseDups ((ids.flatMap (k.loads j)))) href).mpr hmem
  exact hp.length_eq

theorem offset_nodup (s : Nat) (l : List Nat) (h : l.Nodup) :
    (l.map (fun k => s + k)).Nodup := by
  apply nodup_map_of_injective (fun k => s + k) l ?_ h
  intro a ha b hb heq
  omega

theorem sharedHalo (j : Job) (n : Nat) (hn : 0 < n) :
    (addresses NeighborSum.kernel j (List.range n)).length = n + 1 := by
  let ref := (List.range (n+1)).map (fun k => j.src + k)
  have href : ref.Nodup := by
    exact offset_nodup j.src (List.range (n+1)) List.nodup_range
  have hmem : ∀ a, a ∈ addresses NeighborSum.kernel j (List.range n) ↔ a ∈ ref := by
    intro a
    constructor
    · intro ha
      simp [addresses, List.mem_eraseDups, NeighborSum.kernel, List.mem_flatMap] at ha
      rcases ha with ⟨i, hi, hia⟩
      rcases hia with hia | hia
      · subst hia
        exact List.mem_map.mpr ⟨i, by simpa using (Nat.lt_trans hi (Nat.lt_succ_self n)), rfl⟩
      · subst hia
        exact List.mem_map.mpr ⟨i+1, by simpa using Nat.succ_lt_succ hi, by omega⟩
    · intro ha
      simp [ref] at ha
      rcases ha with ⟨k, hk, rfl⟩
      have hk' : k < n+1 := by simpa using hk
      by_cases hk0 : k = 0
      · subst k
        apply List.mem_eraseDups.mpr
        apply List.mem_flatMap.mpr
        refine ⟨0, by simp [hn], ?_⟩
        simp [NeighborSum.kernel]
      · have hpos : 0 < k := Nat.pos_of_ne_zero hk0
        apply List.mem_eraseDups.mpr
        apply List.mem_flatMap.mpr
        have hk_le : k ≤ n := Nat.le_of_lt_succ hk'
        have hkdec : k - 1 < k := Nat.sub_lt hpos (by decide)
        refine ⟨k-1, by simpa using Nat.lt_of_lt_of_le hkdec hk_le, ?_⟩
        simp [NeighborSum.kernel]
        omega
  rw [addresses_length_eq NeighborSum.kernel j (List.range n) ref href hmem]
  simp [ref]

theorem ringFootprint (j : Job) (ids : List Nat) :
    (addresses RingWindow.kernel j ids).length ≤ j.window + 1 := by
  let ref := (List.range (j.window+1)).map (fun k => j.src + k)
  have href : ref.Nodup := offset_nodup j.src (List.range (j.window+1)) List.nodup_range
  have hsub : addresses RingWindow.kernel j ids ⊆ ref := by
    intro a ha
    simp [addresses, List.mem_eraseDups, RingWindow.kernel, List.mem_flatMap] at ha
    rcases ha with ⟨i, hi, hia⟩
    rcases hia with hia | hia
    · subst hia
      exact List.mem_map.mpr ⟨i % (j.window+1), by simpa using Nat.mod_lt i (Nat.succ_pos j.window), rfl⟩
    · subst hia
      exact List.mem_map.mpr ⟨(i+1) % (j.window+1), by simpa using Nat.mod_lt (i+1) (Nat.succ_pos j.window), rfl⟩
  have hlen := List.Nodup.length_le_of_subset
    (nodup_eraseDups (ids.flatMap (RingWindow.kernel.loads j))) hsub
  simpa [ref, addresses] using hlen

theorem uniqueReads (j : Job) (n : Nat) (hnle : n ≤ j.ids.length) :
    (addresses IndexedGather.kernel j (List.range n)).length = (j.ids.take n).eraseDups.length := by
  let ref := (j.ids.take n).eraseDups.map (fun c => j.src + c)
  have href : ref.Nodup := offset_nodup j.src (j.ids.take n).eraseDups (nodup_eraseDups _)
  have hmem : ∀ a, a ∈ addresses IndexedGather.kernel j (List.range n) ↔ a ∈ ref := by
    intro a
    constructor
    · intro ha
      simp [addresses, List.mem_eraseDups, IndexedGather.kernel, List.mem_flatMap] at ha
      rcases ha with ⟨i, hi, rfl⟩
      have hi' : i < n := by simpa using hi
      have hiids : i < j.ids.length := Nat.lt_of_lt_of_le hi' hnle
      have hctake : j.ids[i]?.getD 0 ∈ j.ids.take n := by
        simp [List.getElem?_take, hiids]
        apply List.mem_iff_getElem.mpr
        refine ⟨i, by simpa [hnle] using hi', ?_⟩
        rw [List.getElem_take]
      exact List.mem_map.mpr ⟨j.ids[i]?.getD 0,
        List.mem_eraseDups.mpr hctake, rfl⟩
    · intro ha
      simp [ref] at ha
      rcases ha with ⟨c, hc, rfl⟩
      have hctake : c ∈ j.ids.take n := hc
      rcases List.mem_iff_getElem?.mp hctake with ⟨i, higet⟩
      simp [List.getElem?_take] at higet
      rcases higet with ⟨hi', higet⟩
      apply List.mem_eraseDups.mpr
      apply List.mem_flatMap.mpr
      refine ⟨i, by simpa using hi', ?_⟩
      simp [IndexedGather.kernel, higet]
  have hlen := addresses_length_eq IndexedGather.kernel j (List.range n) ref href hmem
  simpa [ref] using hlen

theorem wordCohort (j : Job) :
    (addresses PackedBits.kernel j (List.range 64)).length = 1 := by
  let ref : List Nat := [j.src]
  have href : ref.Nodup := by simp [ref]
  have hmem : ∀ a, a ∈ addresses PackedBits.kernel j (List.range 64) ↔ a ∈ ref := by
    intro a
    constructor
    · intro ha
      simp [addresses, List.mem_eraseDups, PackedBits.kernel, List.mem_flatMap] at ha
      rcases ha with ⟨i, hi, rfl⟩
      have hi' : i < 64 := by simpa using hi
      have hdiv : i / 64 = 0 := Nat.div_eq_of_lt hi'
      simp [hdiv, ref]
    · intro ha
      simp [ref] at ha
      subst a
      apply List.mem_eraseDups.mpr
      apply List.mem_flatMap.mpr
      refine ⟨0, by simp, ?_⟩
      simp [PackedBits.kernel]
  have hlen := addresses_length_eq PackedBits.kernel j (List.range 64) ref href hmem
  simpa [ref] using hlen

theorem wordBoundary (j : Job) (n : Nat) :
    (addresses PackedBits.kernel j (List.range n)).length = (n + 63) / 64 := by
  let ref := (List.range ((n+63)/64)).map (fun k => j.src + k)
  have href : ref.Nodup := offset_nodup j.src (List.range ((n+63)/64)) List.nodup_range
  have hmem : ∀ a, a ∈ addresses PackedBits.kernel j (List.range n) ↔ a ∈ ref := by
    intro a
    constructor
    · intro ha
      simp [addresses, List.mem_eraseDups, PackedBits.kernel, List.mem_flatMap] at ha
      rcases ha with ⟨i, hi, rfl⟩
      have hi' : i < n := by simpa using hi
      have hle : i + 64 ≤ n + 63 := by omega
      have hdivle : (i + 64) / 64 ≤ (n + 63) / 64 := Nat.div_le_div_right hle
      rw [Nat.add_div_right i (by decide : 0 < 64)] at hdivle
      exact List.mem_map.mpr ⟨i/64, by simpa using Nat.lt_of_succ_le hdivle, rfl⟩
    · intro ha
      simp [ref] at ha
      rcases ha with ⟨k, hk, rfl⟩
      have hk' : k < (n+63)/64 := by simpa using hk
      have hnpos : 0 < n := by
        by_cases hk0 : k = 0
        · subst k
          simp at hk'
          omega
        · exact Nat.pos_of_ne_zero (fun hn0 => by subst n; simp at hk')
      by_cases hk0 : k = 0
      · subst k
        apply List.mem_eraseDups.mpr
        apply List.mem_flatMap.mpr
        refine ⟨0, by simpa using hnpos, ?_⟩
        simp [PackedBits.kernel]
      · let i := k * 64
        have hsucc : k + 1 ≤ (n+63)/64 := Nat.succ_le_of_lt hk'
        have hmul : (k+1)*64 ≤ n+63 := (Nat.le_div_iff_mul_le (by decide : 0 < 64)).mp hsucc
        have hexpand : (k+1)*64 = k*64 + 64 := by omega
        rw [hexpand] at hmul
        have hi_lt : i < n := by
          dsimp [i]
          omega
        have hdiv : i / 64 = k := by
          dsimp [i]
          rw [Nat.mul_comm k 64, Nat.mul_div_right k (by decide : 0 < 64)]
        apply List.mem_eraseDups.mpr
        apply List.mem_flatMap.mpr
        refine ⟨i, by simpa using hi_lt, ?_⟩
        simp [PackedBits.kernel, hdiv]
  have hlen := addresses_length_eq PackedBits.kernel j (List.range n) ref href hmem
  simpa [ref] using hlen

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common
