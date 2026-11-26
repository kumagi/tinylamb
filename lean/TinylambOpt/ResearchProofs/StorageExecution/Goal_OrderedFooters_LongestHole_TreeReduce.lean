import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.BridgeRun` — `∀ (a b : List Nat), let s := (a.reverse.takeWhile (· ==
  0)).length let p := (b.takeWhile (· == 0)).length longestZero (a++b)=max (max
  (longestZero a) (longestZero b)) (s+p)`
- `LongestHole.TreeReduce` — Merge an arbitrary spill tree without visiting the raw
  rows again.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

/-- 小さな入力での真偽確認（探索的チェック）。 -/
private def samples : List (List Nat) :=
  [[], [0], [1], [0, 0], [0, 1], [1, 0], [0, 0, 1], [1, 0, 0], [0, 1, 0], [0, 0, 0]]

private def pairs : List (Tree × Tree) :=
  (samples.map Tree.leaf).flatMap (fun a => (samples.map Tree.leaf).map (fun b => (a, b)))

private def trees : List Tree :=
  (samples.map Tree.leaf) ++
  pairs.map (fun p => Tree.node p.1 p.2) ++
  pairs.map (fun p => Tree.node (Tree.node p.1 p.2) (Tree.node p.2 p.1))

#eval (trees.all (fun t => rootState agg t == agg.reference (plain t)), trees.length)

/-! `takeWhile` と `max?` に関する補助補題。 -/

private theorem takeWhile_append_true {p : Nat → Bool} (a b : List Nat)
    (h : ∀ x ∈ a, p x = true) : (a ++ b).takeWhile p = a ++ b.takeWhile p := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    simp only [List.cons_append, List.takeWhile_cons]
    have hx : p x = true := h x (by simp)
    simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_all_true {p : Nat → Bool} (a : List Nat)
    (h : ∀ x ∈ a, p x = true) : a.takeWhile p = a := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    have hx : p x = true := h x (by simp)
    simp [List.takeWhile_cons, hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_false {p : Nat → Bool} (a b : List Nat)
    (h : ∃ x ∈ a, p x = false) : (a ++ b).takeWhile p = a.takeWhile p := by
  induction a with
  | nil => rcases h with ⟨x, hx, _⟩; simp at hx
  | cons x xs ih =>
    simp only [List.cons_append, List.takeWhile_cons]
    by_cases hx : p x = true
    · simp [hx]
      have h' : ∃ y ∈ xs, p y = false := by
        rcases h with ⟨y, hy, hfy⟩
        simp at hy
        rcases hy with rfl | hy
        · simp [hx] at hfy
        · exact ⟨y, hy, hfy⟩
      exact ih h'
    · have hx' : p x = false := by cases hp : p x <;> simp_all
      simp [hx']

private theorem mem_takeWhile_prop {p : Nat → Bool} {l : List Nat} {x : Nat}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
    by_cases ha : p a = true
    · have hmem : x = a ∨ x ∈ as.takeWhile p := by simpa [List.takeWhile_cons, ha] using h
      rcases hmem with rfl | hmem
      · exact ha
      · exact ih hmem
    · have ha' : p a = false := by cases hp : p a <;> simp_all
      simpa [List.takeWhile_cons, ha'] using h

private theorem takeWhile_eq_take_length {p : Nat → Bool} (l : List Nat) :
    l.takeWhile p = l.take (l.takeWhile p).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
    by_cases ha : p a = true
    · simpa [List.takeWhile_cons, ha, List.length_cons] using congrArg (List.cons a) ih
    · have ha' : p a = false := by cases hp : p a <;> simp_all
      simp [List.takeWhile_cons, ha']

private theorem takeWhile_length_eq_length_iff {p : Nat → Bool} {l : List Nat} :
    (l.takeWhile p).length = l.length ↔ ∀ x ∈ l, p x = true := by
  constructor
  · intro h x hx
    have heq : l.takeWhile p = l := (List.takeWhile_sublist (p := p) (l := l)).eq_of_length h
    exact mem_takeWhile_prop (p := p) (heq.symm ▸ hx)
  · intro h
    rw [takeWhile_all_true l h]

private theorem getD_max_le {l : List Nat} {b : Nat} (h : ∀ x ∈ l, x ≤ b) : l.max?.getD 0 ≤ b := by
  cases hm : l.max? with
  | none => simp [Option.getD]
  | some m =>
    simp only [Option.getD]
    exact h m (List.max?_mem (xs := l) hm)

private theorem le_getD_max {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hx
  | some m =>
    simp only [Option.getD]
    exact ((List.max?_le_iff (xs := l) hm).mp (Nat.le_refl m)) x hx

/-- 先頭 zero 列の連結則。 -/
private theorem prefix_append (a b : List Nat) :
    (a ++ b).takeWhile (fun x : Nat => x == 0) =
      if (∀ x ∈ a, (x == 0) = true) then a ++ b.takeWhile (fun x : Nat => x == 0)
      else a.takeWhile (fun x : Nat => x == 0) := by
  classical
  by_cases ha : ∀ x ∈ a, (x == 0) = true
  · have ht := takeWhile_append_true (p := fun x : Nat => x == 0) a b ha
    rw [if_pos ha, ht]
  · obtain ⟨x, hx, hfx⟩ : ∃ x ∈ a, (x == 0) = false := by
      apply Classical.byContradiction
      intro hc
      apply ha
      intro y hy
      apply Classical.byContradiction
      intro hne
      exact hc ⟨y, hy, by cases (y == 0) <;> simp_all⟩
    have ht := takeWhile_append_false (p := fun x : Nat => x == 0) a b ⟨x, hx, hfx⟩
    rw [if_neg ha, ht]

/-- 末尾 zero 列の連結則。 -/
private theorem suffix_append (a b : List Nat) :
    (a ++ b).reverse.takeWhile (fun x : Nat => x == 0) =
      if (∀ x ∈ b, (x == 0) = true) then b.reverse ++ a.reverse.takeWhile (fun x : Nat => x == 0)
      else b.reverse.takeWhile (fun x : Nat => x == 0) := by
  classical
  rw [List.reverse_append]
  by_cases hb : ∀ x ∈ b, (x == 0) = true
  · have hbr : ∀ x ∈ b.reverse, (x == 0) = true := by
      intro x hx
      exact hb x (List.mem_reverse.mp hx)
    have h := prefix_append b.reverse a.reverse
    simpa [hb, hbr] using h
  · have hbr : ¬ (∀ x ∈ b.reverse, (x == 0) = true) := by
      intro h
      apply hb
      intro x hx
      exact h x (List.mem_reverse.mpr hx)
    have h := prefix_append b.reverse a.reverse
    simpa [hb, hbr] using h

/-- 境界をまたぐ零列の和。 -/
private theorem bridgeRun : BridgeRun.Goal := by
  intro a b
  dsimp only
  unfold longestZero peak
  apply Nat.le_antisymm
  · apply getD_max_le
    intro x hx
    simp only [List.mem_map, List.mem_range] at hx
    rcases hx with ⟨i, hi, rfl⟩
    by_cases hia : i ≤ a.length
    · by_cases hsplit : a.length ≤ i + (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length
      · have h1 : (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length ≤
            (a.length - i) + (b.takeWhile (fun x : Nat => x == 0)).length := by
          have hdrop : (a ++ b).drop i = a.drop i ++ b := List.drop_append_of_le_length hia
          rw [hdrop]
          by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
          · rw [takeWhile_append_true (p := fun x : Nat => x == 0) _ _ hall]
            simp [List.length_append, List.length_drop]
          · obtain ⟨y, hy, hfy⟩ : ∃ y ∈ a.drop i, (y == 0) = false := by
              apply Classical.byContradiction
              intro hc
              apply hall
              intro y hy
              apply Classical.byContradiction
              intro hne
              exact hc ⟨y, hy, by cases (y == 0) <;> simp_all⟩
            rw [takeWhile_append_false (p := fun x : Nat => x == 0) _ _ ⟨y, hy, hfy⟩]
            have hle := List.Sublist.length_le
              (List.takeWhile_sublist (p := fun x : Nat => x == 0) (l := a.drop i))
            simp [List.length_drop] at hle
            omega
        have h2 : a.length - i ≤ (a.reverse.takeWhile (fun x : Nat => x == 0)).length := by
          have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
            intro x hx
            by_cases hx0 : (x == 0) = true
            · exact hx0
            · have hfx : (x == 0) = false := by
                cases hval : (x == 0) <;> simp [hval] at hx0 ⊢
              have hdrop : (a ++ b).drop i = a.drop i ++ b := List.drop_append_of_le_length hia
              have hsplit' := hsplit
              rw [hdrop, takeWhile_append_false (p := fun x : Nat => x == 0) _ _ ⟨x, hx, hfx⟩] at hsplit'
              have hle := List.Sublist.length_le
                (List.takeWhile_sublist (p := fun x : Nat => x == 0) (l := a.drop i))
              have hne : ((a.drop i).takeWhile (fun x : Nat => x == 0)).length ≠ (a.drop i).length := by
                intro heq
                have heqlist : (a.drop i).takeWhile (fun x : Nat => x == 0) = a.drop i :=
                  (List.takeWhile_sublist (p := fun x : Nat => x == 0) (l := a.drop i)).eq_of_length heq
                have hxmem : x ∈ (a.drop i).takeWhile (fun x : Nat => x == 0) := heqlist.symm ▸ hx
                have hpx := mem_takeWhile_prop (p := fun x : Nat => x == 0) hxmem
                simp [hfx] at hpx
              have hlt : ((a.drop i).takeWhile (fun x : Nat => x == 0)).length < (a.drop i).length :=
                Nat.lt_of_le_of_ne hle hne
              simp [List.length_drop] at hlt hsplit'
              omega
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by
            rw [List.reverse_drop]
          have hallrev : ∀ x ∈ a.reverse.take (a.length - i), (x == 0) = true := by
            intro x hx
            have hx' : x ∈ (a.drop i).reverse := hrev.symm ▸ hx
            exact hall x (List.mem_reverse.mp hx')
          have htw_take : (a.reverse.take (a.length - i)).takeWhile (fun x : Nat => x == 0) =
              a.reverse.take (a.length - i) :=
            takeWhile_all_true _ hallrev
          have htake_tw : (a.reverse.takeWhile (fun x : Nat => x == 0)).take (a.length - i) =
              a.reverse.take (a.length - i) := by
            rw [List.take_takeWhile, htw_take]
          have hlen_le : ((a.reverse.takeWhile (fun x : Nat => x == 0)).take (a.length - i)).length ≤
              (a.reverse.takeWhile (fun x : Nat => x == 0)).length :=
            List.length_take_le' (a.length - i) _
          have hlen_take : (a.reverse.take (a.length - i)).length = a.length - i := by
            rw [List.length_take_of_le]
            simp [List.length_reverse]
          rw [htake_tw] at hlen_le
          rw [hlen_take] at hlen_le
          exact hlen_le
        omega
      · rw [Nat.not_le] at hsplit
        have hdrop : (a ++ b).drop i = a.drop i ++ b := List.drop_append_of_le_length hia
        rw [hdrop]
        have hall_or : (((a.drop i) ++ b).takeWhile (fun x : Nat => x == 0)).length =
            ((a.drop i).takeWhile (fun x : Nat => x == 0)).length := by
          by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
          · exfalso
            have hsplit' := hsplit
            rw [hdrop] at hsplit'
            rw [takeWhile_append_true (p := fun x : Nat => x == 0) _ _ hall] at hsplit'
            simp [List.length_append, List.length_drop] at hsplit'
            omega
          · obtain ⟨y, hy, hfy⟩ : ∃ y ∈ a.drop i, (y == 0) = false := by
              apply Classical.byContradiction
              intro hc
              apply hall
              intro y hy
              apply Classical.byContradiction
              intro hne
              exact hc ⟨y, hy, by cases (y == 0) <;> simp_all⟩
            rw [takeWhile_append_false (p := fun x : Nat => x == 0) _ _ ⟨y, hy, hfy⟩]
        rw [hall_or]
        have hmem : ((a.drop i).takeWhile (fun x : Nat => x == 0)).length ∈
            (List.range (a.length + 1)).map
              (fun j => ((a.drop j).takeWhile (fun x : Nat => x == 0)).length) :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩
        exact Nat.le_trans (le_getD_max hmem)
          (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        rw [List.drop_append]
        have hnil : a.drop i = [] := List.drop_eq_nil_of_le (Nat.le_of_lt (Nat.not_le.mp hia))
        simp [hnil]
      rw [hdrop]
      have hmem : ((b.drop (i - a.length)).takeWhile (fun x : Nat => x == 0)).length ∈
          (List.range (b.length + 1)).map
            (fun j => ((b.drop j).takeWhile (fun x : Nat => x == 0)).length) :=
        List.mem_map.mpr ⟨i - a.length, by simp [List.mem_range, List.length_append] at hi ⊢; omega, rfl⟩
      exact Nat.le_trans (le_getD_max hmem)
        (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨i, hi, rfl⟩
        have hdrop : (a ++ b).drop i = a.drop i ++ b :=
          List.drop_append_of_le_length (by omega : i ≤ a.length)
        have hle : ((a.drop i).takeWhile (fun x : Nat => x == 0)).length ≤
            (((a.drop i) ++ b).takeWhile (fun x : Nat => x == 0)).length := by
          by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
          · rw [takeWhile_append_true (p := fun x : Nat => x == 0) _ _ hall]
            rw [takeWhile_all_true (p := fun x : Nat => x == 0) _ hall]
            simp [List.length_append]
          · obtain ⟨y, hy, hfy⟩ : ∃ y ∈ a.drop i, (y == 0) = false := by
              apply Classical.byContradiction
              intro hc
              apply hall
              intro y hy
              apply Classical.byContradiction
              intro hne
              exact hc ⟨y, hy, by cases (y == 0) <;> simp_all⟩
            rw [takeWhile_append_false (p := fun x : Nat => x == 0) _ _ ⟨y, hy, hfy⟩]
            exact Nat.le_refl _
        have hle' : ((a.drop i).takeWhile (fun x : Nat => x == 0)).length ≤
            (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length := by
          rw [hdrop]
          exact hle
        have hmem : (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length ∈
            (List.range ((a ++ b).length + 1)).map
              (fun j => (((a ++ b).drop j).takeWhile (fun x : Nat => x == 0)).length) :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
        exact Nat.le_trans hle' (le_getD_max hmem)
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨j, hj, rfl⟩
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by simp [List.drop_append]
        have hmem : ((b.drop j).takeWhile (fun x : Nat => x == 0)).length ∈
            (List.range ((a ++ b).length + 1)).map
              (fun k => (((a ++ b).drop k).takeWhile (fun x : Nat => x == 0)).length) :=
          List.mem_map.mpr ⟨a.length + j, by simp [List.mem_range, List.length_append]; omega, by rw [hdrop]⟩
        exact le_getD_max hmem
    · let s := (a.reverse.takeWhile (fun x : Nat => x == 0)).length
      let p := (b.takeWhile (fun x : Nat => x == 0)).length
      have hs : s ≤ a.length := by
        have hle := List.Sublist.length_le
          (List.takeWhile_sublist (p := fun x : Nat => x == 0) (l := a.reverse))
        simpa [List.length_reverse] using hle
      let i := a.length - s
      have hi_le : i ≤ a.length := by omega
      have hdrop : (a ++ b).drop i = a.drop i ++ b := List.drop_append_of_le_length hi_le
      have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
        intro x hx
        have hxrev : x ∈ a.reverse.take s := by
          have hxrev' : x ∈ (a.drop i).reverse := List.mem_reverse.mpr hx
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by
            rw [List.reverse_drop]
          rw [hrev] at hxrev'
          rw [List.mem_take_iff_getElem] at hxrev' ⊢
          rcases hxrev' with ⟨j, hj, rfl⟩
          exact ⟨j, by simp [List.length_drop] at hj; omega, rfl⟩
        have hsub : a.reverse.take s = a.reverse.takeWhile (fun x : Nat => x == 0) :=
          (takeWhile_eq_take_length a.reverse).symm
        rw [hsub] at hxrev
        exact mem_takeWhile_prop (p := fun x : Nat => x == 0) hxrev
      have htake : ((a ++ b).drop i).takeWhile (fun x : Nat => x == 0) =
          a.drop i ++ b.takeWhile (fun x : Nat => x == 0) := by
        rw [hdrop]
        exact takeWhile_append_true (p := fun x : Nat => x == 0) _ _ hall
      have hlen : (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length = s + p := by
        rw [htake]
        simp [List.length_append, List.length_drop]
        omega
      have hmem : (((a ++ b).drop i).takeWhile (fun x : Nat => x == 0)).length ∈
          (List.range ((a ++ b).length + 1)).map
            (fun j => (((a ++ b).drop j).takeWhile (fun x : Nat => x == 0)).length) :=
        List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
      have hle := le_getD_max hmem
      rw [hlen] at hle
      exact hle

/-- `agg` の join は到達可能な要約について連結と一致する。 -/
private theorem stitch_agg : stitch agg := by
  intro a b
  dsimp [stitch, agg]
  rw [List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases ha : ∀ x ∈ a, (x == 0) = true
    · have hp : (a.takeWhile (fun x : Nat => x == 0)).length = a.length :=
        (takeWhile_length_eq_length_iff (p := fun x : Nat => x == 0)).2 ha
      rw [prefix_append a b, if_pos ha]
      simp [hp]
    · have hp : (a.takeWhile (fun x : Nat => x == 0)).length ≠ a.length := by
        intro h
        exact ha ((takeWhile_length_eq_length_iff (p := fun x : Nat => x == 0)).1 h)
      rw [prefix_append a b, if_neg ha]
      simp [hp]
  · by_cases hb : ∀ x ∈ b, (x == 0) = true
    · have hp : (b.reverse.takeWhile (fun x : Nat => x == 0)).length = b.length := by
        have hbr : ∀ x ∈ b.reverse, (x == 0) = true := by
          intro x hx
          exact hb x (List.mem_reverse.mp hx)
        have hrevlen : b.reverse.length = b.length := by simp
        simpa [hrevlen] using
          (takeWhile_length_eq_length_iff (p := fun x : Nat => x == 0)).2 hbr
      rw [suffix_append a b, if_pos hb]
      simp [hp]
    · have hp : (b.reverse.takeWhile (fun x : Nat => x == 0)).length ≠ b.length := by
        intro h
        have hbr : ∀ x ∈ b.reverse, (x == 0) = true :=
          (takeWhile_length_eq_length_iff (p := fun x : Nat => x == 0)).1 (by simpa [List.length_reverse] using h)
        apply hb
        intro x hx
        exact hbr x (List.mem_reverse.mpr hx)
      rw [suffix_append a b, if_neg hb]
      simp [hp]
  · simpa using (bridgeRun a b).symm

/-- 木を辿る要約計算は、葉を連結した生データの要約と一致する。 -/
theorem sound_TreeReduce : LongestHole.TreeReduce.Goal := by
  intro t
  induction t with
  | leaf xs => simp [rootState, plain]
  | node a b iha ihb =>
    simp only [rootState, plain, iha, ihb]
    exact stitch_agg _ _

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_TreeReduce
