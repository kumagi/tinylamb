import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Exchanges.PacketLowerBound` — `∀ cap xs wire, 0 < cap → (∀ d, volume wire d = volume
  xs d) → (∀ p ∈ wire, p.2 ≤ cap) → traffic cap xs ≤ wire.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_PacketLowerBound

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodup_eraseDups (List.filter (fun b => ! b == a) as)
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem packets_le_iff (cap v c : Nat) (hcap : 0 < cap) :
    packets cap v ≤ c ↔ v ≤ cap * c := by
  unfold packets
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  omega

private theorem sum_map_le_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem sum_le_cap_mul_length (cap : Nat) : ∀ ys : List Bucket,
    (∀ p ∈ ys, p.2 ≤ cap) → (ys.map Prod.snd).sum ≤ cap * ys.length := by
  intro ys h
  induction ys with
  | nil => simp
  | cons p ps ih =>
      have hp : p.2 ≤ cap := h p (by simp)
      have hps : ∀ q ∈ ps, q.2 ≤ cap := by intro q hq; exact h q (by simp [hq])
      have ih' := ih hps
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      calc
        p.snd + (ps.map Prod.snd).sum ≤ cap + cap * ps.length := Nat.add_le_add hp ih'
        _ = cap * ps.length + cap := by omega
        _ = cap * (ps.length + 1) := by rw [Nat.mul_succ]

private theorem packets_le_filter_length (cap d : Nat) (hcap : 0 < cap) :
    ∀ wire : List Bucket, (∀ p ∈ wire, p.2 ≤ cap) →
      packets cap (volume wire d) ≤ (wire.filter (fun p => p.1 == d)).length := by
  intro wire hw
  apply (packets_le_iff cap (volume wire d) _ hcap).2
  have hs := sum_le_cap_mul_length cap (wire.filter (fun p => p.1 == d)) (by
    intro p hp
    exact hw p (List.mem_filter.mp hp).1)
  simpa [volume, List.countP_eq_length_filter] using hs

private theorem sum_map_add' {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem sum_if_eq_countP (ds : List Nat) (d : Nat) :
    (ds.map (fun x => if d == x then 1 else 0)).sum = ds.countP (fun x => d == x) := by
  induction ds with
  | nil => simp
  | cons x xs ih =>
      by_cases h : d = x
      · subst x
        simp at ih ⊢
        omega
      · simp [h] at ih ⊢
        exact ih

private theorem countP_le_one (ds : List Nat) (hnd : ds.Nodup) (d : Nat) :
    ds.countP (fun x => d == x) ≤ 1 := by
  rw [List.countP_eq_length_filter]
  induction ds with
  | nil => simp
  | cons x xs ih =>
      have hnot : x ∉ xs := (List.nodup_cons.mp hnd).1
      cases hx : d == x with
      | true =>
          have hnone : (xs.filter (fun y => d == y)) = [] := by
            rw [List.filter_eq_nil_iff]
            intro y hy hytrue
            have hdx : d = x := by simpa using hx
            have hdy : d = y := by simpa using hytrue
            have hyx : y = x := by omega
            exact hnot (by simpa [hyx] using hy)
          simp [hx, hnone]
      | false =>
          have ih' := ih (List.nodup_cons.mp hnd).2
          simp [hx, ih']

private theorem sum_countP_le_nodup (ds : List Nat) (hnd : ds.Nodup) :
    ∀ wire : List Bucket, (ds.map (fun d => wire.countP (fun p => p.1 == d))).sum ≤ wire.length := by
  intro wire
  induction wire with
  | nil =>
      simp only [List.countP_nil, List.length_nil]
      exact Nat.le_of_eq (sum_map_zero ds)
  | cons p rest ih =>
      simp only [List.countP_cons]
      rw [sum_map_add']
      rw [sum_if_eq_countP]
      have hm := countP_le_one ds hnd p.1
      change _ ≤ rest.length + 1
      omega

private theorem sum_filter_len_le_nodup (ds : List Nat) (hnd : ds.Nodup) :
    ∀ wire : List Bucket, (ds.map (fun d => (wire.filter (fun p => p.1 == d)).length)).sum ≤ wire.length := by
  intro wire
  simpa [List.countP_eq_length_filter] using sum_countP_le_nodup ds hnd wire

theorem sound_Exchanges_PacketLowerBound : PacketLowerBound.Goal := by
  intro cap xs wire hcap hvol hw
  unfold traffic
  let ds := (xs.map Prod.fst).eraseDups
  change (ds.map (fun d => packets cap (volume xs d))).sum ≤ wire.length
  calc
    (ds.map (fun d => packets cap (volume xs d))).sum ≤
        (ds.map (fun d => (wire.filter (fun p => p.1 == d)).length)).sum := by
      apply sum_map_le_nat
      intro d hd
      have hf := packets_le_filter_length cap d hcap wire hw
      rw [hvol d] at hf
      exact hf
    _ ≤ wire.length := sum_filter_len_le_nodup ds (nodup_eraseDups (xs.map Prod.fst)) wire

/-! Exhaustive small-input check: destinations 0..1, bytes 0..2, lists up to
length 2, capacity 1..3. All admissible wires satisfy the bound (evaluates true). -/
#eval
  let b1 : List (List Bucket) :=
    (List.range 2).flatMap (fun d => (List.range 3).map (fun w => [(d, w)]))
  let bks : List (List Bucket) := [[]] ++ b1 ++ b1.flatMap (fun a => b1.map (fun b => a ++ b))
  bks.all (fun xs =>
    bks.all (fun wire =>
      (List.range 3).all (fun ci =>
        let cap := ci + 1
        if (List.range 3).all (fun d => volume wire d == volume xs d) &&
            wire.all (fun p => p.2 <= cap)
        then traffic cap xs <= wire.length else true)))

end TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_PacketLowerBound
