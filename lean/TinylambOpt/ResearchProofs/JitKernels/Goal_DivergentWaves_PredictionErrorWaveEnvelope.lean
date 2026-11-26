import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! If predicted lane lengths overestimate actual work by at most e componentwise, each vector wave loses at most e ticks; the whole schedule therefore stays within e per wave of the predicted cost. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_PredictionErrorWaveEnvelope

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem max?_some_of_mem_nat : ∀ (l : List Nat) (a : Nat), a ∈ l → ∃ m, l.max? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.max?_cons]

theorem le_peak : ∀ (l : List Nat) (a : Nat), a ∈ l → a ≤ peak l := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp [peak]
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat (y :: ys) y (by simp)
            simp [peak, List.max?_cons, hm]
            exact Nat.le_max_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat (y :: ys) y (by simp)
            have hle := ih a ha
            simp [peak, List.max?_cons, hm] at hle ⊢
            omega

theorem peak_le_of_forall : ∀ (l : List Nat) (c : Nat), (∀ x ∈ l, x ≤ c) → peak l ≤ c := by
  intro l c h
  by_cases hnil : l = []
  · subst l; simp [peak]
  · have hmem : l.head hnil ∈ l := List.head_mem hnil
    obtain ⟨m, hm⟩ := max?_some_of_mem_nat l (l.head hnil) hmem
    have hmle := h m (List.max?_mem hm)
    simp [peak, hm]
    omega

theorem zip_bound_index : ∀ (a b : List Nat) (e : Nat), a.length = b.length →
    (∀ p ∈ a.zip b, p.1 ≤ p.2 + e) →
    ∀ i (hi : i < a.length) (hib : i < b.length), a[i] ≤ b[i] + e := by
  intro a
  induction a with
  | nil =>
      intro b e hlen hpt i hi hib
      simp at hi
  | cons a0 as ih =>
      intro b e hlen hpt i hi hib
      cases b with
      | nil => simp at hlen
      | cons b0 bs =>
          simp only [List.length_cons, Nat.succ.injEq] at hlen
          cases i with
          | zero =>
              have hp : (a0, b0) ∈ (a0 :: as).zip (b0 :: bs) := by simp
              simpa using hpt (a0, b0) hp
          | succ i =>
              have hi' : i < as.length := by simpa using hi
              have hib : i < bs.length := by omega
              have hpt' : ∀ p ∈ as.zip bs, p.1 ≤ p.2 + e := by
                intro p hp
                exact hpt p (by simp [hp])
              have hrec := ih bs e hlen hpt' i hi'
              simpa using hrec hib

theorem sum_map_le : ∀ (l : List Nat) (f g : Nat → Nat),
    (∀ x ∈ l, f x ≤ g x) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro f g h
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have htail := ih f g (by intro y hy; exact h y (by simp [hy]))
      omega

theorem wave_le : ∀ (a b : List Nat) (w e j : Nat), 0 < w → a.length = b.length →
    (∀ p ∈ a.zip b, p.1 ≤ p.2 + e) →
    peak ((a.drop (j * w)).take w) ≤ peak ((b.drop (j * w)).take w) + e := by
  intro a b w e j hw hlen hpt
  apply peak_le_of_forall
  intro u hu
  rw [List.mem_iff_getElem] at hu
  obtain ⟨i, hi, hiu⟩ := hu
  have hsome : ((a.drop (j * w)).take w)[i]? = some u := by
    rw [List.getElem?_eq_some_iff]
    exact ⟨hi, hiu⟩
  rw [List.getElem?_take] at hsome
  have hiw : i < w := by
    by_cases hiw : i < w
    · exact hiw
    · simp [hiw] at hsome
  simp [hiw] at hsome
  obtain ⟨hidx, hiu'⟩ := List.getElem?_eq_some_iff.mp hsome
  have hidxb : j * w + i < b.length := by omega
  have ha_le := zip_bound_index a b e hlen hpt (j * w + i) hidx hidxb
  have hv_some : ((b.drop (j * w)).take w)[i]? = some (b[j * w + i]) := by
    rw [List.getElem?_take, List.getElem?_drop]
    simp [hiw, hidxb]
  obtain ⟨hvbound, hv_eq⟩ := List.getElem?_eq_some_iff.mp hv_some
  have hv_mem : b[j * w + i] ∈ (b.drop (j * w)).take w := by
    rw [List.mem_iff_getElem]
    exact ⟨i, hvbound, hv_eq⟩
  have hpeakb := le_peak ((b.drop (j * w)).take w) (b[j * w + i]) hv_mem
  omega

theorem sum_map_add_const : ∀ (l : List Nat) (f : Nat → Nat) (e : Nat),
    (l.map (fun x => f x + e)).sum = (l.map f).sum + l.length * e := by
  intro l
  induction l with
  | nil => intro f e; simp
  | cons x xs ih =>
      intro f e
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      rw [Nat.succ_mul]
      omega

theorem prediction : PredictionErrorWaveEnvelope.Goal := by
  intro a b w e hw hlen hpt
  unfold ticks waves
  have hK : (a.length + w - 1) / w = (b.length + w - 1) / w := by rw [hlen]
  rw [hK]
  let K := (b.length + w - 1) / w
  simp only [List.length_map, List.length_range]
  change ((List.range K).map (fun j => peak ((a.drop (j * w)).take w))).sum ≤
    ((List.range K).map (fun j => peak ((b.drop (j * w)).take w))).sum + K * e
  have hle : ((List.range K).map (fun j => peak ((a.drop (j * w)).take w))).sum ≤
      ((List.range K).map (fun j => peak ((b.drop (j * w)).take w) + e)).sum := by
    apply sum_map_le
    intro j hj
    exact wave_le a b w e j hw hlen hpt
  have hsumadd := sum_map_add_const (List.range K) (fun j => peak ((b.drop (j * w)).take w)) e
  simp only [List.length_range] at hsumadd
  rw [← hsumadd]
  exact hle

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_PredictionErrorWaveEnvelope
