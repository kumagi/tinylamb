import TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

set_option maxHeartbeats 0
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ClockWindow

open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage
open TinylambOpt.ResearchGoals.Compression.Wire (byteCount sbytes ubytes width zig)
open ClockWindow

theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  unfold width
  by_cases ha : a = 0
  · subst ha; simp
  · have hb : b ≠ 0 := by omega
    have h1 : 2 ^ a.log2 ≤ a := Nat.log2_self_le ha
    have h2 : a.log2 ≤ b.log2 := (Nat.le_log2 hb).mpr (Nat.le_trans h1 h)
    simp [ha, hb]; omega

theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  have := width_mono h
  omega

theorem sbytes_toward_zero_pos (low : Int) (h : 0 < low) : sbytes (low - 1) ≤ sbytes low := by
  unfold sbytes
  apply ubytes_mono
  obtain ⟨k, rfl⟩ : ∃ k : Nat, low = (k + 1 : Nat) := ⟨low.toNat - 1, by omega⟩
  have e : ((k+1 : Nat) : Int) - 1 = (k : Nat) := by omega
  rw [e]
  simp [zig]
  omega

theorem sbytes_toward_zero_neg (low : Int) (h : low < 0) : sbytes (low + 1) ≤ sbytes low := by
  unfold sbytes
  apply ubytes_mono
  obtain ⟨k, rfl⟩ : ∃ k : Nat, low = Int.negSucc k := ⟨(-low - 1).toNat, by omega⟩
  cases k with
  | zero => simp [zig]
  | succ k =>
    have e : Int.negSucc (k+1) + 1 = Int.negSucc k := by omega
    rw [e]
    simp [zig]
    omega

theorem step_down (ds : List Int) (w e : Nat) (low : Int) (hpos : 0 < low)
    (hna : ∀ d ∈ ds, d ≠ low + ((2^w : Nat) - 1 : Int)) :
    cost ds w e (low - 1) ≤ cost ds w e low := by
  unfold cost
  dsimp only
  have h1 := sbytes_toward_zero_pos low hpos
  have h2 : ds.countP (fun d => decide (d < low - 1 ∨ low - 1 + ((2^w : Nat) - 1 : Int) < d)) ≤
      ds.countP (fun d => decide (d < low ∨ low + ((2^w : Nat) - 1 : Int) < d)) := by
    apply List.countP_mono_left
    intro d hd hp
    have hne := hna d hd
    simp only [decide_eq_true_eq] at hp ⊢
    omega
  have := Nat.mul_le_mul_right e h2
  omega

theorem step_up (ds : List Int) (w e : Nat) (low : Int) (hneg : low < 0)
    (hna : ∀ d ∈ ds, d ≠ low) :
    cost ds w e (low + 1) ≤ cost ds w e low := by
  unfold cost
  dsimp only
  have h1 := sbytes_toward_zero_neg low hneg
  have h2 : ds.countP (fun d => decide (d < low + 1 ∨ low + 1 + ((2^w : Nat) - 1 : Int) < d)) ≤
      ds.countP (fun d => decide (d < low ∨ low + ((2^w : Nat) - 1 : Int) < d)) := by
    apply List.countP_mono_left
    intro d hd hp
    have hne := hna d hd
    simp only [decide_eq_true_eq] at hp ⊢
    omega
  have := Nat.mul_le_mul_right e h2
  omega

theorem reach (ds : List Int) (w e : Nat) :
    ∀ n : Nat, ∀ low : Int, low.natAbs = n → ∃ a ∈ anchors ds w, cost ds w e a ≤ cost ds w e low := by
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
    intro low hlow
    by_cases hanc : low ∈ anchors ds w
    · exact ⟨low, hanc, Nat.le_refl _⟩
    · have h0 : low ≠ 0 := fun h => hanc (by simp [anchors, h])
      have hd1 : ∀ d ∈ ds, d ≠ low := fun d hd h => hanc (by
        simp only [anchors, List.mem_cons, List.mem_flatMap, List.mem_cons, List.not_mem_nil]
        exact Or.inr ⟨d, hd, Or.inl h.symm⟩)
      have hd2 : ∀ d ∈ ds, d ≠ low + ((2^w : Nat) - 1 : Int) := fun d hd h => hanc (by
        simp only [anchors, List.mem_cons, List.mem_flatMap, List.mem_cons, List.not_mem_nil]
        refine Or.inr ⟨d, hd, Or.inr (Or.inl ?_)⟩
        omega)
      by_cases hp : 0 < low
      · obtain ⟨a, ha, hc⟩ := ih (low - 1).natAbs (by omega) (low - 1) rfl
        exact ⟨a, ha, Nat.le_trans hc (step_down ds w e low hp hd2)⟩
      · have hn : low < 0 := by omega
        obtain ⟨a, ha, hc⟩ := ih (low + 1).natAbs (by omega) (low + 1) rfl
        exact ⟨a, ha, Nat.le_trans hc (step_up ds w e low hn hd1)⟩

theorem argmin (c : Int → Nat) : ∀ L : List Int, L ≠ [] → ∃ a ∈ L, ∀ b ∈ L, c a ≤ c b := by
  intro L
  induction L with
  | nil => intro h; exact absurd rfl h
  | cons x L ih =>
    intro _
    by_cases hL : L = []
    · subst hL; exact ⟨x, by simp, by simp⟩
    · obtain ⟨a, ha, hmin⟩ := ih hL
      by_cases hc : c x ≤ c a
      · refine ⟨x, by simp, ?_⟩
        intro b hb
        rcases List.mem_cons.mp hb with rfl | hb
        · exact Nat.le_refl _
        · exact Nat.le_trans hc (hmin b hb)
      · refine ⟨a, List.mem_cons_of_mem _ ha, ?_⟩
        intro b hb
        rcases List.mem_cons.mp hb with rfl | hb
        · omega
        · exact hmin b hb

theorem sound : ClockWindow.Goal := by
  intro ds w e
  obtain ⟨a, ha, hmin⟩ := argmin (cost ds w e) (anchors ds w) (by simp [anchors])
  refine ⟨a, ha, ?_⟩
  intro other
  obtain ⟨a', ha', hc⟩ := reach ds w e other.natAbs other rfl
  exact Nat.le_trans (hmin a' ha') hc

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ClockWindow
