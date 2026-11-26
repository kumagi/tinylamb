import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `DistinctCount.TwoEvictions` — Repaired deletions in either order agree, including
  repeated outgoing values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_TwoEvictions_Common

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem count_pos_of_mem {l : List Nat} {a : Nat} (ha : a ∈ l) : 0 < l.count a := by
  have hne : l.count a ≠ 0 := by
    intro h
    exact ((List.count_eq_zero).mp h) ha
  omega

theorem count_filter_ne {a b : Nat} (hba : b ≠ a) (l : List Nat) :
    (l.filter (fun z => !(z == a))).count b = l.count b := by
  induction l with
  | nil => simp [List.count]
  | cons c cs ih =>
      by_cases hca : c == a
      · have hcb : c ≠ b := by
          intro h
          have hca' : c = a := by simpa using hca
          exact hba (h.symm.trans hca')
        simp [List.count_cons, hca, hcb, ih]
      · simp [List.count_cons, hca, ih]

theorem length_eq_count_add_filter_length (a : Nat) (l : List Nat) :
    l.length = l.count a + (l.filter (fun z => !(z == a))).length := by
  induction l with
  | nil => simp [List.count]
  | cons b bs ih =>
      by_cases hba : b == a
      · simp [List.count_cons, hba]
        omega
      · simp [List.count_cons, hba]
        omega

theorem sum_map_eq_of_mem {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ a, a ∈ l → f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : f a = g a := h a (by simp)
      have has : ∀ b, b ∈ as → f b = g b := by
        intro b hb
        exact h b (by simp [hb])
      simp [ha, ih has]

theorem length_eq_sum_count_eraseDups (l : List Nat) :
    l.length = (l.eraseDups.map (fun a => l.count a)).sum := by
  let motive : Nat → Prop := fun n =>
    ∀ l : List Nat, l.length = n → l.length = (l.eraseDups.map (fun a => l.count a)).sum
  have hmain := Nat.strongRecOn l.length (motive := motive) (fun n ih => by
    intro l hlen
    cases l with
    | nil => simp [List.eraseDups]
    | cons a as =>
        let f := as.filter (fun b => !(b == a))
        have hflt : f.length < n := by
          dsimp [f]
          have hle := List.length_filter_le (fun b => !(b == a)) as
          have hlen' : as.length + 1 = n := by simpa using hlen
          omega
        have ihf := ih f.length hflt f rfl
        have hsum : (f.eraseDups.map (fun b => f.count b)).sum =
            (f.eraseDups.map (fun b => (a :: as).count b)).sum := by
          apply sum_map_eq_of_mem
          intro b hb
          have hbf' : b ∈ f := (List.mem_eraseDups).mp hb
          have hbf'' : b ∈ as.filter (fun z => !(z == a)) := by
            simpa [f] using hbf'
          have hbp : (!(b == a)) = true := (List.mem_filter.mp hbf'').2
          have hbne : b ≠ a := by
            intro h
            subst b
            simp at hbp
          have h1 : f.count b = as.count b := by
            dsimp [f]
            exact count_filter_ne hbne as
          have hba : ¬ b == a := by
            cases h : b == a <;> simp_all
          have hab : a ≠ b := by intro h; exact hbne h.symm
          have h2 : (a :: as).count b = as.count b := by simp [List.count_cons, hba, hab]
          exact h1.trans h2.symm
        have hlen_as : as.length = as.count a + f.length := by
          dsimp [f]
          exact length_eq_count_add_filter_length a as
        have hcount_a : (a :: as).count a = as.count a + 1 := by simp [List.count_cons]
        rw [List.eraseDups_cons]
        dsimp [f] at ihf hsum hlen_as ⊢
        simp only [List.length_cons, List.map_cons, List.sum_cons]
        rw [← hsum, ← ihf]
        rw [hlen_as, hcount_a]
        omega)
  exact hmain l rfl

theorem nodup_eraseDups_filter (p : Nat → Bool) (l : List Nat) :
    (l.filter p).eraseDups.Nodup := by
  induction l generalizing p with
  | nil => simp [List.eraseDups]
  | cons a as ih =>
      by_cases hpa : p a = true
      · rw [List.filter_cons_of_pos hpa, List.eraseDups_cons]
        simp only [List.nodup_cons]
        constructor
        · intro hmem
          have hf : a ∈ (as.filter p).filter (fun b => !(b == a)) :=
            (List.mem_eraseDups).mp hmem
          simp at hf
        · simpa [List.filter_filter, Bool.and_comm] using
            ih (fun b => (!(b == a)) && p b)
      · rw [List.filter_cons_of_neg hpa]
        exact ih p

theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have h := nodup_eraseDups_filter (fun _ => true) l
  have heq : l.filter (fun _ => true) = l := List.filter_eq_self.mpr (by simp)
  simpa [heq] using h

theorem perm_eraseDups_length {a b : List Nat} (p : a.Perm b) :
    a.eraseDups.length = b.eraseDups.length := by
  have hm : ∀ z, z ∈ a.eraseDups ↔ z ∈ b.eraseDups := by
    intro z
    rw [List.mem_eraseDups, List.mem_eraseDups]
    exact p.mem_iff
  have hndb := nodup_eraseDups b
  have hper : a.eraseDups.Perm b.eraseDups :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups a) hndb).2 hm
  exact hper.length_eq

theorem sum_map_ge_length {α : Type} (f : α → Nat) (l : List α)
    (hall : ∀ a, a ∈ l → 1 ≤ f a) : l.length ≤ (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : 1 ≤ f a := hall a (by simp)
      have has : ∀ b, b ∈ as → 1 ≤ f b := by
        intro b hb
        exact hall b (by simp [hb])
      simp only [List.length_cons, List.map_cons, List.sum_cons]
      have := ih has
      omega

theorem sum_map_ge_length_succ_of_exists {α : Type} (f : α → Nat) (l : List α)
    (hall : ∀ a, a ∈ l → 1 ≤ f a) (hex : ∃ a, a ∈ l ∧ 1 < f a) :
    l.length + 1 ≤ (l.map f).sum := by
  induction l with
  | nil => simp at hex
  | cons a as ih =>
      have ha : 1 ≤ f a := hall a (by simp)
      have has : ∀ b, b ∈ as → 1 ≤ f b := by
        intro b hb
        exact hall b (by simp [hb])
      simp only [List.length_cons, List.map_cons, List.sum_cons]
      rcases hex with ⟨b, hb, hbgt⟩
      simp only [List.mem_cons] at hb
      rcases hb with rfl | hb
      · have hs := sum_map_ge_length f as has
        omega
      · have ih' := ih has ⟨b, hb, hbgt⟩
        omega

theorem eraseDups_length_le_length (l : List Nat) : l.eraseDups.length ≤ l.length := by
  have hs := length_eq_sum_count_eraseDups l
  have hle : l.eraseDups.length ≤ (l.eraseDups.map (fun a => l.count a)).sum := by
    apply sum_map_ge_length
    intro a ha
    have ham : a ∈ l := (List.mem_eraseDups).mp ha
    exact Nat.succ_le_of_lt (count_pos_of_mem ham)
  rw [← hs] at hle
  exact hle

theorem nodup_of_eraseDups_length_eq_length {l : List Nat}
    (h : l.eraseDups.length = l.length) : l.Nodup := by
  apply List.nodup_iff_count.mpr
  intro a
  by_cases hle : l.count a ≤ 1
  · exact hle
  · have hgt : 1 < l.count a := by omega
    have ha : a ∈ l := by
      by_cases ha : a ∈ l
      · exact ha
      · have hz : l.count a = 0 := (List.count_eq_zero).mpr ha
        omega
    have ham : a ∈ l.eraseDups := (List.mem_eraseDups).mpr ha
    have hall : ∀ z, z ∈ l.eraseDups → 1 ≤ l.count z := by
      intro z hz
      have hzl : z ∈ l := (List.mem_eraseDups).mp hz
      exact Nat.succ_le_of_lt (count_pos_of_mem hzl)
    have hs := sum_map_ge_length_succ_of_exists (fun z => l.count z) l.eraseDups hall
      ⟨a, ham, hgt⟩
    rw [← length_eq_sum_count_eraseDups l] at hs
    omega

theorem eraseDups_length_eq_length_of_nodup {l : List Nat} (hnd : l.Nodup) :
    l.eraseDups.length = l.length := by
  have hs := length_eq_sum_count_eraseDups l
  have hcnt : ∀ a, l.count a = if a ∈ l then 1 else 0 :=
    (List.nodup_iff_count_eq_ite).mp hnd
  have hpoint : ∀ a, a ∈ l.eraseDups → l.count a = 1 := by
    intro a ha
    have hal : a ∈ l := (List.mem_eraseDups).mp ha
    simp [hcnt a, hal]
  have hsum : (l.eraseDups.map (fun a => l.count a)).sum = l.eraseDups.length := by
    have hsum' := sum_map_eq_of_mem l.eraseDups (fun a => l.count a) (fun _ => 1) hpoint
    have hconst : (l.eraseDups.map (fun _ => 1)).sum = l.eraseDups.length := by
      induction l.eraseDups with
      | nil => simp
      | cons a as ih =>
          simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
          omega
    exact hsum'.trans hconst
  omega

theorem count_eq_length_of_eraseDups_length_one {l : List Nat} {x : Nat}
    (h : l.eraseDups.length = 1) (hx : x ∈ l) : l.count x = l.length := by
  cases he : l.eraseDups with
  | nil => simp [he] at h
  | cons z zs =>
      simp [he] at h
      have hzs : zs = [] := by simpa using h
      subst zs
      have hx' : x ∈ l.eraseDups := (List.mem_eraseDups).mpr hx
      have hxz : x = z := by simpa [he] using hx'
      subst x
      have hs := length_eq_sum_count_eraseDups l
      simpa [he] using hs.symm

theorem forall_eq_of_eraseDups_length_one {l : List Nat} {x : Nat}
    (h : l.eraseDups.length = 1) (hx : x ∈ l) : ∀ y, y ∈ l → y = x := by
  cases he : l.eraseDups with
  | nil => simp [he] at h
  | cons z zs =>
      simp [he] at h
      have hzs : zs = [] := by simpa using h
      subst zs
      have hxz : x = z := by
        have hx' : x ∈ l.eraseDups := (List.mem_eraseDups).mpr hx
        simpa [he] using hx'
      subst x
      intro y hy
      have hy' : y ∈ l.eraseDups := (List.mem_eraseDups).mpr hy
      simpa [he] using hy'

theorem eraseDups_length_eq_one_of_all_eq {l : List Nat} {x : Nat}
    (hne : l ≠ []) (hall : ∀ y, y ∈ l → y = x) : l.eraseDups.length = 1 := by
  have hmem : x ∈ l := by
    cases l with
    | nil => contradiction
    | cons y ys =>
        have hy : y = x := hall y (by simp)
        simpa [hy]
  have hm : ∀ z, z ∈ l.eraseDups ↔ z ∈ [x] := by
    intro z
    rw [List.mem_eraseDups]
    simp only [List.mem_singleton]
    constructor
    · exact hall z
    · intro hz
      subst z
      exact hmem
  have hper : l.eraseDups.Perm [x] :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups l) (by simp)).2 hm
  simpa using hper.length_eq

theorem eraseDups_length_erase_of_count_one {l : List Nat} {x : Nat}
    (hmem : x ∈ l) (hc : l.count x = 1) :
    (l.erase x).eraseDups.length = l.eraseDups.length - 1 := by
  have hmemed : x ∈ l.eraseDups := (List.mem_eraseDups).mpr hmem
  have hm : ∀ z, z ∈ (l.erase x).eraseDups ↔ z ∈ l.eraseDups.erase x := by
    intro z
    constructor
    · intro hz
      have hzlx : z ∈ l.erase x := (List.mem_eraseDups).mp hz
      have hzl : z ∈ l := List.mem_of_mem_erase hzlx
      have hzne : z ≠ x := by
        intro heq
        subst z
        have hc0 : (l.erase x).count x = 0 := by simp [List.count_erase_self, hc]
        exact ((List.count_eq_zero).mp hc0) hzlx
      exact (List.mem_erase_of_ne hzne).2 ((List.mem_eraseDups).mpr hzl)
    · intro hz
      have hzled : z ∈ l.eraseDups := List.mem_of_mem_erase hz
      have hzl : z ∈ l := (List.mem_eraseDups).mp hzled
      have hzne : z ≠ x := by
        intro heq
        subst z
        have hcle : (l.eraseDups).count x ≤ 1 :=
          (List.nodup_iff_count.mp (nodup_eraseDups l)) x
        have hc0 : (l.eraseDups.erase x).count x = 0 := by
          rw [List.count_erase_self]
          omega
        exact ((List.count_eq_zero).mp hc0) hz
      exact (List.mem_eraseDups).mpr ((List.mem_erase_of_ne hzne).2 hzl)
  have hper : (l.erase x).eraseDups.Perm (l.eraseDups.erase x) :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups (l.erase x))
      ((nodup_eraseDups l).erase x)).2 hm
  rw [hper.length_eq, List.length_erase_of_mem hmemed]

theorem eraseDups_length_erase_of_count_gt_one {l : List Nat} {x : Nat}
    (hgt : 1 < l.count x) :
    (l.erase x).eraseDups.length = l.eraseDups.length := by
  have hmem : x ∈ l := by
    by_cases hmem : x ∈ l
    · exact hmem
    · have hz : l.count x = 0 := (List.count_eq_zero).mpr hmem
      omega
  have hmemx : x ∈ l.erase x := by
    have hc0 : (l.erase x).count x ≠ 0 := by
      rw [List.count_erase_self]
      omega
    by_cases hmemx : x ∈ l.erase x
    · exact hmemx
    · have hz : (l.erase x).count x = 0 := (List.count_eq_zero).mpr hmemx
      exact False.elim (hc0 hz)
  have hm : ∀ z, z ∈ (l.erase x).eraseDups ↔ z ∈ l.eraseDups := by
    intro z
    rw [List.mem_eraseDups, List.mem_eraseDups]
    constructor
    · intro hz
      exact List.mem_of_mem_erase hz
    · intro hz
      by_cases hzx : z = x
      · subst z
        exact hmemx
      · exact (List.mem_erase_of_ne hzx).2 hz
  have hper : (l.erase x).eraseDups.Perm l.eraseDups :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups (l.erase x))
      (nodup_eraseDups l)).2 hm
  exact hper.length_eq


theorem eraseDups_length_eq_of_forall_mem {l1 l2 : List Nat}
    (h : ∀ z, z ∈ l1 ↔ z ∈ l2) : l1.eraseDups.length = l2.eraseDups.length := by
  have hp : l1.eraseDups.Perm l2.eraseDups :=
    (List.perm_ext_iff_of_nodup (nodup_eraseDups l1) (nodup_eraseDups l2)).2 (by
      intro z
      rw [List.mem_eraseDups, List.mem_eraseDups]
      exact h z)
  exact hp.length_eq

theorem eraseDups_length_cons_of_mem {a : Nat} {l : List Nat} (ha : a ∈ l) :
    (a :: l).eraseDups.length = l.eraseDups.length := by
  apply eraseDups_length_eq_of_forall_mem
  intro z
  rw [List.mem_cons]
  constructor
  · intro hz
    rcases hz with hza | hzl
    · subst z
      exact ha
    · exact hzl
  · intro hzl
    exact Or.inr hzl

theorem eraseDups_length_cons_of_not_mem {a : Nat} {l : List Nat} (ha : a ∉ l) :
    (a :: l).eraseDups.length = l.eraseDups.length + 1 := by
  rw [List.eraseDups_cons]
  have hf : l.filter (fun z => !(z == a)) = l := List.filter_eq_self.mpr (by
    intro z hz
    have hza : z ≠ a := by
      intro h
      subst z
      exact ha hz
    simp [hza])
  rw [hf]
  simp

theorem exists_ne_of_eraseDups_length_gt_one {l : List Nat} {x : Nat}
    (hx : x ∈ l) (hv : 1 < l.eraseDups.length) : ∃ y, y ∈ l ∧ y ≠ x := by
  by_cases h : ∃ y, y ∈ l ∧ y ≠ x
  · exact h
  · have hall : ∀ y, y ∈ l → y = x := by
      intro y hy
      by_cases hyx : y = x
      · exact hyx
      · exact False.elim (h ⟨y, hy, hyx⟩)
    have hne : l ≠ [] := by
      intro hnil
      subst l
      simp at hx
    have hlen := eraseDups_length_eq_one_of_all_eq hne hall
    omega

theorem exists_count_gt_one_of_eraseDups_length_lt_length {l : List Nat}
    (h : l.eraseDups.length < l.length) : ∃ d, d ∈ l ∧ 1 < l.count d := by
  by_cases h' : ∃ d, d ∈ l ∧ 1 < l.count d
  · exact h'
  · have hall_le : ∀ d, l.count d ≤ 1 := by
      intro d
      by_cases hd : d ∈ l
      · have hnot : ¬ 1 < l.count d := by
          intro hgt
          exact h' ⟨d, hd, hgt⟩
        omega
      · have hz : l.count d = 0 := (List.count_eq_zero).mpr hd
        omega
    have hnd : l.Nodup := List.nodup_iff_count.mpr hall_le
    have heq := eraseDups_length_eq_length_of_nodup hnd
    omega

theorem eraseDups_length_filter_eq {l : List Nat} {x : Nat} (hx : x ∈ l) :
    (l.filter (fun z => !(z == x))).eraseDups.length = l.eraseDups.length - 1 := by
  have hmemed : x ∈ l.eraseDups := (List.mem_eraseDups).mpr hx
  have hm : ∀ z, z ∈ l.filter (fun w => !(w == x)) ↔ z ∈ l.eraseDups.erase x := by
    intro z
    constructor
    · intro hz
      have hzmem : z ∈ l := (List.mem_filter.mp hz).1
      have hzne : z ≠ x := by
        intro heq
        subst z
        have hp : (!(x == x)) = true := (List.mem_filter.mp hz).2
        simp at hp
      exact (List.mem_erase_of_ne hzne).2 ((List.mem_eraseDups).mpr hzmem)
    · intro hz
      have hzled : z ∈ l.eraseDups := List.mem_of_mem_erase hz
      have hzl : z ∈ l := (List.mem_eraseDups).mp hzled
      have hzne : z ≠ x := by
        intro heq
        subst z
        have hcle : (l.eraseDups).count x ≤ 1 :=
          (List.nodup_iff_count.mp (nodup_eraseDups l)) x
        have hc0 : (l.eraseDups.erase x).count x = 0 := by
          rw [List.count_erase_self]
          omega
        exact ((List.count_eq_zero).mp hc0) hz
      exact List.mem_filter.mpr ⟨hzl, by simp [hzne]⟩
  have hlen1 : (l.filter (fun z => !(z == x))).eraseDups.length =
      (l.eraseDups.erase x).eraseDups.length := eraseDups_length_eq_of_forall_mem hm
  have hlen2 : (l.eraseDups.erase x).eraseDups.length =
      (l.eraseDups.erase x).length :=
    eraseDups_length_eq_length_of_nodup ((nodup_eraseDups l).erase x)
  rw [hlen1, hlen2, List.length_erase_of_mem hmemed]

theorem eraseDups_length_append_replicate_of_mem {t : List Nat} {y : Nat}
    (hy : y ∈ t) (k : Nat) :
    (t ++ List.replicate k y).eraseDups.length = t.eraseDups.length := by
  apply eraseDups_length_eq_of_forall_mem
  intro z
  rw [List.mem_append, List.mem_replicate]
  constructor
  · intro hz
    rcases hz with hzt | hzrep
    · exact hzt
    · rcases hzrep with ⟨_, hzy⟩
      subst z
      exact hy
  · intro hzt
    exact Or.inl hzt


theorem distinctCount_repaired_eq_value {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    repaired DistinctCount.model xs x = value DistinctCount.model (xs.erase x) := by
  by_cases hlen : xs.length == 1
  · have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hlen
      | cons y ys =>
          simp at hlen
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [repaired, dropOne, value, DistinctCount.model]
  · have hlenb : (xs.length == 1) = false := by
      cases h : xs.length == 1
      · rfl
      · exact False.elim (hlen h)
    have hlen_gt : 1 < xs.length := by
      have hne1 : xs.length ≠ 1 := by
        intro h
        apply hlen
        simp [h]
      have hp : 0 < xs.length := List.length_pos_of_mem hx
      omega
    have hne : xs.erase x ≠ [] := by
      intro hnil
      have hl := congrArg List.length hnil
      rw [List.length_erase_of_mem hx] at hl
      simp at hl
      omega
    have his := isEmpty_false_of_ne_nil hne
    by_cases hv1 : xs.eraseDups.length == 1
    · have hv1nat : xs.eraseDups.length = 1 := by simpa using hv1
      have hall : ∀ y, y ∈ xs → y = x := forall_eq_of_eraseDups_length_one hv1nat hx
      have herase : (xs.erase x).eraseDups.length = 1 := by
        apply eraseDups_length_eq_one_of_all_eq hne
        intro y hy
        exact hall y (List.mem_of_mem_erase hy)
      have hv : value DistinctCount.model (xs.erase x) = some 1 := by
        simp [value, DistinctCount.model, his, herase]
      have hdrop : dropOne DistinctCount.model xs x = some (some 1) := by
        simp [dropOne, DistinctCount.model, hlenb, hv1nat]
      simp [repaired, hdrop, hv]
    · by_cases hvn : xs.eraseDups.length == xs.length
      · have hvnnat : xs.eraseDups.length = xs.length := by simpa using hvn
        have hnd : xs.Nodup := nodup_of_eraseDups_length_eq_length hvnnat
        have herase : (xs.erase x).eraseDups.length = xs.length - 1 := by
          rw [eraseDups_length_eq_length_of_nodup (hnd.erase x),
            List.length_erase_of_mem hx]
        have hv : value DistinctCount.model (xs.erase x) = some (xs.length - 1) := by
          simp [value, DistinctCount.model, his, herase]
        have hn1 : xs.length ≠ 1 := by omega
        have hdrop : dropOne DistinctCount.model xs x = some (some (xs.length - 1)) := by
          simp [dropOne, DistinctCount.model, hlenb, hvnnat, hn1]
        simp [repaired, hdrop, hv]
      · have hv1false' : xs.eraseDups.length ≠ 1 := by
          intro h
          apply hv1
          simp [h]
        have hvnfalse' : xs.eraseDups.length ≠ xs.length := by
          intro h
          apply hvn
          simp [h]
        have hdrop : dropOne DistinctCount.model xs x = none := by
          simp [dropOne, DistinctCount.model, hlenb, hv1false', hvnfalse']
        simp [repaired, hdrop]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_TwoEvictions_Common


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_TwoEvictions_Common

theorem sound : DistinctCount.TwoEvictions.Goal := by
  intro xs x y hx hy
  by_cases hxy : x = y
  · subst y
    rfl
  have hyin : y ∈ xs := List.mem_of_mem_erase hy
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  rw [distinctCount_repaired_eq_value hy, distinctCount_repaired_eq_value hxiny]
  let l := (xs.erase x).erase y
  let r := (xs.erase y).erase x
  have hp : l.Perm r := by
    dsimp [l, r]
    exact erase_erase_perm xs x y hx hy
  have hlen : l.length = r.length := hp.length_eq
  have hres : l.eraseDups.length = r.eraseDups.length := perm_eraseDups_length hp
  change value DistinctCount.model l = value DistinctCount.model r
  by_cases hl : l = []
  · have hrl : r.length = 0 := by simpa [hl] using hlen.symm
    have hr : r = [] := by simpa using hrl
    simp [value, hl, hr]
  · have hr : r ≠ [] := by
      intro hr
      have hll : l.length = 0 := by simpa [hr] using hlen
      have : l = [] := by simpa using hll
      exact hl this
    have hle := isEmpty_false_of_ne_nil hl
    have hre := isEmpty_false_of_ne_nil hr
    simp [value, hle, hre, DistinctCount.model, hres]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_TwoEvictions
