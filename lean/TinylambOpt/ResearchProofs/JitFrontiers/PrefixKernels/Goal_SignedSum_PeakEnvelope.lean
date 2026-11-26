import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `SignedSum.PeakEnvelope` — `∀ w s rs, let ps := (rs.map (·.getD 0)).scanl (·+·) 0
  fits w s → (cert update w s rs = true ↔ -limit w ≤ s + ps.min?.getD 0 ∧ s +
  ps.max?.getD 0 < limit w)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_PeakEnvelope

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

def minMaxAux : List Int → Int × Int
  | [] => (0, 0)
  | [x] => (x, x)
  | x :: y :: xs => let p := minMaxAux (y :: xs); (min x p.1, max x p.2)

theorem min?_eq_aux : ∀ l : List Int, l.min?.getD 0 = (minMaxAux l).1 := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases xs with
      | nil => simp [minMaxAux]
      | cons y ys =>
          cases hm : (y :: ys).min? with
          | none => simp at hm
          | some m =>
              have hmval : m = (minMaxAux (y :: ys)).1 := by
                rw [← ih]
                rw [hm]
                simp
              simp [List.min?_cons, hm, hmval, minMaxAux]

theorem max?_eq_aux : ∀ l : List Int, l.max?.getD 0 = (minMaxAux l).2 := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases xs with
      | nil => simp [minMaxAux]
      | cons y ys =>
          cases hm : (y :: ys).max? with
          | none => simp at hm
          | some m =>
              have hmval : m = (minMaxAux (y :: ys)).2 := by
                rw [← ih]
                rw [hm]
                simp
              simp [List.max?_cons, hm, hmval, minMaxAux]

theorem all_fits_aux (w : Nat) (s : Int) (hs : fits w s = true) :
    ∀ l : List Int,
      l.all (fun p => fits w (s + p)) = true ↔
        -limit w ≤ s + (minMaxAux l).1 ∧ s + (minMaxAux l).2 < limit w := by
  intro l
  induction l with
  | nil =>
      simp [minMaxAux]
      have hs' := (fits_true w s).mp hs
      omega
  | cons x xs ih =>
      cases xs with
      | nil =>
          simp [minMaxAux, fits_true]
      | cons y ys =>
          change ((fits w (s + x) && (y :: ys).all (fun p => fits w (s + p))) = true) ↔
            -limit w ≤ s + min x (minMaxAux (y :: ys)).1 ∧
              s + max x (minMaxAux (y :: ys)).2 < limit w
          rw [Bool.and_eq_true]
          rw [ih]
          rw [fits_true]
          constructor
          · rintro ⟨hx, hmin, hmax⟩
            constructor
            · have hmn := Int.min_le_left x (minMaxAux (y :: ys)).1
              omega
            · have hmx := Int.le_max_left x (minMaxAux (y :: ys)).2
              omega
          · rintro ⟨hlo, hhi⟩
            have hmn := Int.min_le_left x (minMaxAux (y :: ys)).1
            have hmx := Int.le_max_left x (minMaxAux (y :: ys)).2
            constructor <;> constructor <;> omega

theorem states_signedSum (s : Int) :
    ∀ rs : List Row, states SignedSum.update s rs =
      (rs.map (fun r => r.getD 0)).scanl (fun x y : Int => x + y) s := by
  intro rs
  induction rs generalizing s with
  | nil => simp [states]
  | cons r rs ih =>
      rw [states_cons, List.map_cons, List.scanl_cons, ih]
      simp [SignedSum.update, apply]

theorem scanl_add_shift (l : List Int) (s : Int) :
    l.scanl (fun x y : Int => x + y) s = (l.scanl (fun x y : Int => x + y) 0).map (fun x => s + x) := by
  induction l generalizing s with
  | nil => simp
  | cons a t ih =>
      rw [List.scanl_cons, List.scanl_cons, List.map_cons]
      rw [ih (s + a)]
      have hzero : (0 : Int) + a = a := by omega
      rw [hzero, ih a]
      rw [List.map_map]
      simp only [Int.add_zero, Function.comp_apply]
      change s :: List.map (fun x : Int => s + a + x) (List.scanl (fun x y : Int => x + y) 0 t) =
        s :: List.map (fun x : Int => s + (a + x)) (List.scanl (fun x y : Int => x + y) 0 t)
      congr 1
      apply List.map_congr_left
      intro x hx
      omega

theorem cert_signedSum_ps (w : Nat) (s : Int) (rs : List Row) (hs : fits w s = true) :
    cert SignedSum.update w s rs =
      ((rs.map (fun r => r.getD 0)).scanl (fun x y : Int => x + y) 0).all
        (fun p => fits w (s + p)) := by
  rw [cert_states]
  rw [states_signedSum]
  rw [scanl_add_shift]
  rw [← List.map_drop]
  rw [List.all_map]
  generalize hps : (List.scanl (fun x y : Int => x + y) 0 (List.map (fun r => r.getD 0) rs)) = ps
  cases ps with
  | nil => simp [List.all_nil, hs]
  | cons x xs =>
      have hx : x = 0 := by
        have hh := congrArg List.head? hps
        simp at hh
        omega
      rw [hx]
      rw [List.drop_cons (by decide : 0 < 1)]
      simp only [Nat.sub_self, List.drop_zero]
      rw [List.all_cons]
      rw [show fits w (s + 0) = true by simpa using hs]
      simp
      apply List.all_congr rfl
      intro y
      rfl

theorem sound : SignedSum.PeakEnvelope.Goal := by
  intro w s rs
  dsimp only
  intro hs
  rw [cert_signedSum_ps w s rs hs]
  rw [min?_eq_aux, max?_eq_aux]
  exact all_fits_aux w s hs _

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_PeakEnvelope
