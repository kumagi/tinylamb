import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private def mn (xs : List Nat) : Nat := xs.min?.getD 0
private def mx (xs : List Nat) : Nat := peak xs
private def dd (xs : List Nat) : Nat := peak (xs.zipIdx.map (fun (x,i) => x - mn (xs.drop i)))

/-- The reference uses `min?.getD x`; on the nonempty suffix this is `mn`. -/
private def ddRef (xs : List Nat) : Nat :=
  peak (xs.zipIdx.map (fun (x,i) => x - (xs.drop i).min?.getD x))

private theorem mn_drop_getD (xs : List Nat) (i x : Nat) (hi : i < xs.length) :
    (xs.drop i).min?.getD x = mn (xs.drop i) := by
  have hne : xs.drop i ≠ [] := by
    intro hdrop
    have hle : xs.length ≤ i := List.drop_eq_nil_iff.mp hdrop
    omega
  cases hm : (xs.drop i).min? with
  | none =>
      rw [List.min?_eq_none_iff] at hm
      contradiction
  | some m => simp [mn, hm]

private theorem ddRef_eq_dd (xs : List Nat) : ddRef xs = dd xs := by
  unfold ddRef dd peak
  have hmap :
      xs.zipIdx.map (fun (x,i) => x - (xs.drop i).min?.getD x) =
      xs.zipIdx.map (fun (x,i) => x - mn (xs.drop i)) := by
    apply List.ext_getElem?
    intro i
    simp only [List.getElem?_map, List.getElem?_zipIdx]
    by_cases hi : i < xs.length
    · have hs : xs[i]? = some (xs[i]'hi) := List.getElem?_eq_getElem hi
      have hmin : (xs.drop i).min?.getD (xs[i]'hi) = mn (xs.drop i) :=
        mn_drop_getD xs i (xs[i]'hi) hi
      simp [hs, hmin]
    · have hn : xs[i]? = none := List.getElem?_eq_none (by simpa using hi)
      simp [hn]
  rw [hmap]

private theorem ref_eq (xs : List Nat) :
    agg.reference xs = xs.head?.map (fun _ => (mn xs, mx xs, dd xs)) := by
  change xs.head?.map (fun _ => (mn xs, mx xs, ddRef xs)) =
         xs.head?.map (fun _ => (mn xs, mx xs, dd xs))
  simp [ddRef_eq_dd]

/-- The zip-index of a tail is shifted by one, and `drop` moves accordingly. -/
private theorem dd_tail_shift (x : Nat) (ys : List Nat) :
    (ys.zipIdx 1).map (fun p => p.1 - mn ((x::ys).drop p.2)) =
      ys.zipIdx.map (fun p => p.1 - mn (ys.drop p.2)) := by
  apply List.ext_getElem?
  intro i
  simp only [List.getElem?_map, List.getElem?_zipIdx]
  by_cases hi : i < ys.length
  · have hsome : ys[i]? = some (ys[i]'hi) := List.getElem?_eq_getElem hi
    simp [hsome, Nat.add_comm]
  · have hnone : ys[i]? = none := List.getElem?_eq_none (by simpa using hi)
    simp [hnone]

private theorem dd_cons (x : Nat) (ys : List Nat) :
    dd (x::ys) = max (x - mn (x::ys)) (dd ys) := by
  unfold dd peak
  simp only [List.zipIdx_cons, List.map_cons]
  rw [dd_tail_shift]
  unfold mn
  generalize hL : (ys.zipIdx.map (fun p => p.fst - (List.drop p.snd ys).min?.getD 0)) = L
  cases hM : L.max? <;> simp [hM]

private theorem mn_cons_of_ne (x : Nat) (ys : List Nat) (h : ys ≠ []) :
    mn (x::ys) = min x (mn ys) := by
  cases ys with
  | nil => contradiction
  | cons y ys => simp [mn]

private theorem mx_cons (x : Nat) (ys : List Nat) :
    mx (x::ys) = max x (mx ys) := by
  cases ys <;> simp [mx, peak]

private theorem mn_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    mn (a++b) = min (mn a) (mn b) := by
  induction a with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil => simpa [mn] using (mn_cons_of_ne x b hb)
      | cons y ys =>
          have hxs : y::ys ≠ [] := by simp
          have hne : (y::ys)++b ≠ [] := by simp
          rw [List.cons_append, mn_cons_of_ne x _ hne, ih hxs, mn_cons_of_ne x _ hxs]
          simp [Nat.min_assoc]

private theorem mx_append (a b : List Nat) :
    mx (a++b) = max (mx a) (mx b) := by
  induction a with
  | nil => simp [mx, peak]
  | cons x xs ih =>
      rw [List.cons_append, mx_cons, ih, mx_cons]
      simp [Nat.max_assoc]

private theorem mn_cons_append (x : Nat) (xs b : List Nat) (hb : b ≠ []) :
    mn (x::(xs++b)) = min (mn (x::xs)) (mn b) := by
  simpa [List.cons_append] using mn_append (x::xs) b (by simp) hb

private theorem mx_cons_append (x : Nat) (xs b : List Nat) :
    mx (x::(xs++b)) = max (mx (x::xs)) (mx b) := by
  simpa [List.cons_append] using mx_append (x::xs) b

private theorem dd_append (a b : List Nat) (hb : b ≠ []) :
    dd (a++b) = max (max (dd a) (dd b)) (mx a - mn b) := by
  induction a with
  | nil => simp [dd, peak, mx, mn]
  | cons x xs ih =>
      rw [List.cons_append, dd_cons, ih, mn_cons_append x xs b hb, dd_cons, mx_cons]
      have hmn : mn (x::xs) ≤ x := by
        have hneq : x::xs ≠ [] := by simp
        have hmn_eq : mn (x::xs) = (x::xs).min hneq := by
          simp [mn, List.min?_eq_some_min hneq]
        rw [hmn_eq]
        exact List.min_le_of_mem (by simp)
      omega

private theorem dd_cons_append (x : Nat) (xs b : List Nat) (hb : b ≠ []) :
    dd (x::(xs++b)) = max (max (dd (x::xs)) (dd b)) (mx (x::xs) - mn b) := by
  simpa [List.cons_append] using dd_append (x::xs) b hb

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil =>
      simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil =>
          simp [agg, optionalJoin]
      | cons y ys =>
          have hhead : ((x::xs)++(y::ys)).head? = some x := by simp
          rw [ref_eq (x::xs), ref_eq (y::ys), ref_eq ((x::xs)++(y::ys)), hhead]
          simp only [agg, optionalJoin, List.head?_cons, Option.map_some]
          rw [mn_append _ _ (by simp) (by simp), mx_append, dd_append _ _ (by simp)]

/-- Generic consequences of the stitch law (as in the LongestHole proof). -/

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState f (f.reference xs) (k / 2)
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : f.join a a =
          f.reference ((List.replicate (k / 2) xs).flatten ++
                       (List.replicate (k / 2) xs).flatten) := by
        rw [ha']
        exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++
          (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond, ite_true]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) =
          f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega,
            ← List.replicate_append_replicate, List.flatten_append,
            List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

private theorem runInput_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    runInput f := by
  intro rs
  have gen : ∀ (l : List (Nat × Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc)
        (l.map (fun (v, n) => repeatState f (f.reference [v]) n)) =
      f.reference (acc ++ l.flatMap (fun (v, n) => List.replicate n v)) := by
    intro l
    induction l with
    | nil => intro acc; simp
    | cons r rs ih =>
      obtain ⟨v, n⟩ := r
      intro acc
      simp only [List.map_cons, List.foldl, List.flatMap_cons]
      rw [repeatPower_of_stitch f h]
      rw [h acc (List.replicate n [v]).flatten]
      have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
        induction n with
        | zero => simp
        | succ n ih2 => simp [List.replicate_succ, ih2]
      rw [hflat, ih (acc ++ List.replicate n v)]
      simp [List.append_assoc]
  simpa using gen rs []

theorem sound_RunInput : Drawdown.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_RunInput
