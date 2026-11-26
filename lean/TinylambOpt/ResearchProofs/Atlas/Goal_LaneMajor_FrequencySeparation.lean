import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `LaneMajor.FrequencySeparation` — `∀ c, c.step ≠ 0 → c.step.natAbs*(c.n/width c) <
  c.bias.natAbs → (distinctValues layout c).length = c.n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneMajor_FrequencySeparation

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LaneMajor

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.eraseDups_cons]
      have hf : (as.filter (fun b => !b == a)) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by intro he; exact hl.1 (he ▸ hb)
        simp [hba]
      rw [hf, ih hl.2]

private theorem nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem natAbs_sub_lt_of_lt (a b w : Nat) (ha : a < w) (hb : b < w) :
    ((a : Int) - (b : Int)).natAbs < w := by
  by_cases h : a ≤ b
  · have hnon : 0 ≤ (b : Int) - (a : Int) := Int.sub_nonneg.mpr (by
      exact_mod_cast (show a ≤ b by omega))
    have hneg : (a : Int) - (b : Int) = -((b : Int) - (a : Int)) := by omega
    rw [hneg, Int.natAbs_neg,
      Int.natAbs_sub_of_nonneg_of_le (by omega : 0 ≤ (a : Int)) (by exact_mod_cast h),
      Int.natAbs_natCast, Int.natAbs_natCast]
    omega
  · have hnon : 0 ≤ (a : Int) - (b : Int) := Int.sub_nonneg.mpr (by
      exact_mod_cast (show b ≤ a by omega))
    rw [Int.natAbs_sub_of_nonneg_of_le (by omega : 0 ≤ (b : Int)) (by exact_mod_cast (show b ≤ a by omega)),
      Int.natAbs_natCast, Int.natAbs_natCast]
    omega

private theorem natAbs_sub_le_of_le (a b B : Nat) (ha : a ≤ B) (hb : b ≤ B) :
    ((a : Int) - (b : Int)).natAbs ≤ B := by
  have hlt := natAbs_sub_lt_of_lt a b (B+1) (by omega) (by omega)
  omega

theorem sound_LaneMajor_FrequencySeparation : FrequencySeparation.Goal := by
  intro c hs hsep
  let w := width c
  have hw : 0 < w := by dsimp [w, width]; omega
  let val : Nat → Int := fun i =>
    c.base + c.step*(i/w:Nat) + c.bias*(i%w:Nat)
  have hinj : ∀ i < c.n, ∀ j < c.n, val i = val j → i = j := by
    intro i hi j hj he
    let qi := i / w
    let ri := i % w
    let qj := j / w
    let rj := j % w
    have hri : ri < w := by dsimp [ri]; exact Nat.mod_lt i hw
    have hrj : rj < w := by dsimp [rj]; exact Nat.mod_lt j hw
    have hqi : qi ≤ c.n / w := by
      dsimp [qi]
      exact Nat.div_le_div_right (by omega : i ≤ c.n)
    have hqj : qj ≤ c.n / w := by
      dsimp [qj]
      exact Nat.div_le_div_right (by omega : j ≤ c.n)
    have hrel : c.bias * ((ri : Int) - (rj : Int)) =
        c.step * ((qj : Int) - (qi : Int)) := by
      change c.base + c.step*(qi:Int) + c.bias*(ri:Int) =
        c.base + c.step*(qj:Int) + c.bias*(rj:Int) at he
      simp only [Int.add_assoc] at he
      have h1 := Int.add_left_cancel he
      rw [Int.mul_sub, Int.mul_sub]
      omega
    have hrreq : ri = rj := by
      by_cases hne : ri = rj
      · exact hne
      · have habs := congrArg Int.natAbs hrel
        simp only [Int.natAbs_mul] at habs
        have hrdiff : ((ri : Int) - (rj : Int)).natAbs < w :=
          natAbs_sub_lt_of_lt ri rj w hri hrj
        have hqdiff : ((qj : Int) - (qi : Int)).natAbs ≤ c.n / w :=
          natAbs_sub_le_of_le qj qi (c.n / w) hqj hqi
        have hpos : 0 < ((ri : Int) - (rj : Int)).natAbs := by
          rw [Int.natAbs_pos]
          intro hz
          have hc : (ri : Int) = (rj : Int) := by omega
          exact hne (by exact_mod_cast hc)
        have hleft : c.bias.natAbs ≤ c.bias.natAbs * ((ri : Int) - (rj : Int)).natAbs := by
          simpa using Nat.mul_le_mul_left c.bias.natAbs (Nat.succ_le_of_lt hpos)
        have hright : c.step.natAbs * ((qj : Int) - (qi : Int)).natAbs ≤
            c.step.natAbs * (c.n / w) := Nat.mul_le_mul_left _ hqdiff
        have hchain : c.bias.natAbs ≤ c.step.natAbs * (c.n / w) := by
          calc
            c.bias.natAbs ≤ c.bias.natAbs * ((ri : Int) - (rj : Int)).natAbs := hleft
            _ = c.step.natAbs * ((qj : Int) - (qi : Int)).natAbs := habs
            _ ≤ c.step.natAbs * (c.n / w) := hright
        exact (Nat.not_lt_of_ge hchain hsep).elim
    have hqreq : qi = qj := by
      have hzero : c.step * ((qi : Int) - (qj : Int)) = 0 := by
        rw [hrreq] at hrel
        simp only [Int.sub_self, Int.mul_zero] at hrel
        rw [Int.mul_sub] at hrel ⊢
        omega
      have hdiff : (qi : Int) = (qj : Int) := by
        apply (Int.mul_eq_mul_left_iff hs).mp
        rw [Int.mul_sub] at hzero
        omega
      exact_mod_cast hdiff
    have hdi := Nat.div_add_mod i w
    have hdj := Nat.div_add_mod j w
    have hdii : i = qi*w+ri := by
      simpa [qi, ri, Nat.mul_comm] using hdi.symm
    have hdjj : j = qj*w+rj := by
      simpa [qj, rj, Nat.mul_comm] using hdj.symm
    rw [hqreq, hrreq] at hdii
    omega
  have hrows : (rows layout c).map Prod.snd = (List.range c.n).map val := by
    unfold rows
    simp only [layout, Option.map_some, filterMap_some_eq_map, List.map_map,
      Function.comp_apply, val]
    rfl
  unfold distinctValues
  rw [hrows]
  have hnd : ((List.range c.n).map val).Nodup := by
    apply nodup_map_of_injective _ _ _ List.nodup_range
    intro i hi j hj he
    exact hinj i (List.mem_range.mp hi) j (List.mem_range.mp hj) he
  rw [eraseDups_eq_self_of_nodup _ hnd, List.length_map, List.length_range]

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneMajor_FrequencySeparation
