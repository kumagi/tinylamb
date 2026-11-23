import Std
import TinylambOpt.ResearchGoals.DemandScalar

namespace TinylambOpt.ResearchProofs.DemandScalar
open TinylambOpt.ResearchGoals.DemandScalar

abbrev Cell := Option Int
abbrev Inner := List (Cell × Cell)
abbrev Outer := List (Bool × Cell)

def matchingRows (inner : Inner) (k : Int) : List (Cell × Cell) :=
  inner.filter (fun r => r.1 == some k)

def matchingValues (inner : Inner) (k : Int) : List Cell :=
  (matchingRows inner k).map Prod.snd

def desiredHit (vals : List Cell) : Option Hit :=
  match vals with
  | [] => none
  | [v] => some (.one v)
  | _ => some .many

def stepIndex (m : Std.HashMap Int Hit) (r : Cell × Cell) : Std.HashMap Int Hit :=
  match r.1 with
  | none => m
  | some k => m.insert k (if m.contains k then .many else .one r.2)

def foldIndexFrom (m : Std.HashMap Int Hit) (inner : Inner) : Std.HashMap Int Hit :=
  inner.foldl stepIndex m

theorem index_eq_foldIndexFrom (inner : Inner) :
    index inner = foldIndexFrom {} inner := by
  rfl

theorem stepIndex_get (m : Std.HashMap Int Hit) (acc : Inner) (r : Cell × Cell)
    (h : ∀ k, m[k]? = desiredHit (matchingValues acc k)) :
    ∀ kq, (stepIndex m r)[kq]? = desiredHit (matchingValues (acc ++ [r]) kq) := by
  intro kq
  rcases r with ⟨key, v⟩
  cases key with
  | none =>
      simp [stepIndex, matchingValues, matchingRows, h]
  | some k0 =>
      by_cases hk : k0 = kq
      · subst kq
        have hm : m[k0]? = desiredHit (matchingValues acc k0) := h k0
        have hc : m.contains k0 = (desiredHit (matchingValues acc k0)).isSome := by
          rw [Std.HashMap.contains_eq_isSome_getElem?, hm]
        have happ : matchingValues (acc ++ [(some k0, v)]) k0 = matchingValues acc k0 ++ [v] := by
          simp [matchingValues, matchingRows]
        rw [happ]
        simp [stepIndex, hc]
        cases hvals : matchingValues acc k0 with
        | nil => rfl
        | cons v' rest =>
            cases rest with
            | nil => simp [desiredHit]
            | cons _ _ => simp [desiredHit]
      · have hkbeq : (k0 == kq) = false := by simp [hk]
        simp [stepIndex, Std.HashMap.getElem?_insert, hkbeq, h kq]
        have hno : matchingValues (acc ++ [(some k0, v)]) kq = matchingValues acc kq := by
          simp [matchingValues, matchingRows, hk]
        rw [hno]

theorem foldIndexFrom_get : ∀ (acc : Inner) (m : Std.HashMap Int Hit)
    (_h : ∀ k, m[k]? = desiredHit (matchingValues acc k)) (rest : Inner),
    ∀ k, (foldIndexFrom m rest)[k]? = desiredHit (matchingValues (acc ++ rest) k) := by
  intro acc m h rest
  induction rest generalizing acc m with
  | nil =>
      intro k
      simpa [foldIndexFrom] using h k
  | cons r rs ih =>
      intro k
      have h' : ∀ k', (stepIndex m r)[k']? = desiredHit (matchingValues (acc ++ [r]) k') :=
        stepIndex_get m acc r h
      have res := ih (acc ++ [r]) (stepIndex m r) h' k
      simpa [foldIndexFrom, List.append_assoc] using res

theorem index_get (inner : Inner) (k : Int) :
    (index inner)[k]? = desiredHit (matchingValues inner k) := by
  have h0 : ∀ k, ({} : Std.HashMap Int Hit)[k]? = desiredHit (matchingValues ([] : Inner) k) := by
    intro k; simp [desiredHit, matchingValues, matchingRows]
  have res := foldIndexFrom_get [] {} h0 inner k
  simpa [index_eq_foldIndexFrom] using res

def refHits (inner : Inner) (active : Bool) (key : Cell) : List (Cell × Cell) :=
  inner.filter (fun r => active && key.isSome && r.1 == key)

def refValue (inner : Inner) (active : Bool) (key : Cell) : Cell :=
  (refHits inner active key).head?.bind Prod.snd

theorem refHits_inactive (inner : Inner) (key : Cell) : refHits inner false key = [] := by
  simp [refHits]

theorem refHits_keynone (inner : Inner) (active : Bool) : refHits inner active none = [] := by
  simp [refHits]

theorem refHits_active_some (inner : Inner) (k : Int) :
    refHits inner true (some k) = matchingRows inner k := by
  simp [refHits, matchingRows]

theorem refValue_eq_head (inner : Inner) (active : Bool) (key : Cell) :
    refValue inner active key = (refHits inner active key).head?.bind Prod.snd := by
  simp [refValue, refHits]

theorem hit_eq_desired (inner : Inner) (active : Bool) (key : Cell) :
    (if active then key.bind (fun k => (index inner)[k]?) else none) =
      match refHits inner active key with
      | [] => none
      | [(_, v)] => some (.one v)
      | _ => some .many := by
  cases active with
  | false => simp [refHits_inactive]
  | true =>
      cases key with
      | none => simp [refHits_keynone]
      | some k =>
          simp only [ite_true, Option.bind_some]
          rw [index_get, refHits_active_some]
          cases hrows : matchingRows inner k with
          | nil => simp [desiredHit, matchingValues, hrows]
          | cons r rest =>
              cases rest with
              | nil =>
                  rcases r with ⟨kr, v⟩
                  simp [desiredHit, matchingValues, hrows]
              | cons r2 rest2 =>
                  simp [desiredHit]
                  cases hvals : matchingValues inner k with
                  | nil => simp [matchingValues, hrows] at hvals
                  | cons v vs =>
                      cases vs with
                      | nil => simp [matchingValues, hrows] at hvals
                      | cons _ _ => rfl

def prepend (v : Cell) : Answer → Answer
  | .done vs => .done (v :: vs)
  | .failed vs j => .failed (v :: vs) j

def prefixAnswer (acc : List Cell) : Answer → Answer
  | .done vs => .done (acc ++ vs)
  | .failed vs j => .failed (acc ++ vs) j

theorem prefixAnswer_shift (acc : List Cell) (v : Cell) (a : Answer) :
    prefixAnswer acc (prepend v a) = prefixAnswer (acc ++ [v]) a := by
  cases a <;> simp [prefixAnswer, prepend, List.append_assoc]

theorem reference_step (inner : Inner) (i : Nat) (active : Bool) (key : Cell) (tail : Outer) :
    reference inner i ((active, key) :: tail) =
      if 1 < (refHits inner active key).length then .failed [] i
      else prepend (refValue inner active key) (reference inner (i+1) tail) := by
  by_cases h : 1 < (inner.filter (fun r => active && key.isSome && r.1 == key)).length
  · simp [reference, refHits, h, Id.run]
    rfl
  · simp [reference, refHits, refValue, h, Id.run]
    rfl

def probeArr (m : Std.HashMap Int Hit) (arr : Array Cell) : Outer → Answer
  | [] => .done arr.toList
  | (active, key) :: tail =>
      let hit := if active then key.bind (fun k => m[k]?) else none
      match hit with
      | some .many => .failed arr.toList arr.size
      | some (.one v) => probeArr m (arr.push v) tail
      | none => probeArr m (arr.push none) tail

def probeLoop (m : Std.HashMap Int Hit) (outer : Outer) (arr : Array Cell) : Answer :=
  Id.run do
    let mut emitted := arr
    for (active, key) in outer do
      let hit := if active then key.bind (fun k => m[k]?) else none
      match hit with
      | some .many => return .failed emitted.toList emitted.size
      | some (.one v) => emitted := emitted.push v
      | none => emitted := emitted.push none
    return .done emitted.toList

theorem probe_eq_probeArr (m : Std.HashMap Int Hit) : ∀ (outer : Outer) (arr : Array Cell),
    probeLoop m outer arr = probeArr m arr outer := by
  intro outer
  induction outer with
  | nil => intro arr; simp [probeLoop, probeArr]
  | cons x tail ih =>
      intro arr
      cases x with
      | mk active key =>
        by_cases ha : active = true
        · subst active
          rcases key with _ | k
          · simp [probeLoop, probeArr, List.forIn_cons] at *
            change probeLoop m tail (arr.push none) = probeArr m (arr.push none) tail
            exact ih _
          · rcases hmk : m[k]? with _ | v
            · simp [probeLoop, probeArr, List.forIn_cons, hmk] at *
              change probeLoop m tail (arr.push none) = probeArr m (arr.push none) tail
              exact ih _
            · rcases v with v | v
              · simp [probeLoop, probeArr, List.forIn_cons, hmk] at *
                change probeLoop m tail (arr.push v) = probeArr m (arr.push v) tail
                exact ih _
              · simp [probeLoop, probeArr, List.forIn_cons, hmk]
        · simp [probeLoop, probeArr, List.forIn_cons, ha] at *
          change probeLoop m tail (arr.push none) = probeArr m (arr.push none) tail
          exact ih _

def probeAccIdx (m : Std.HashMap Int Hit) (i : Nat) : Outer → Answer
  | [] => .done []
  | (active, key) :: tail =>
      let hit := if active then key.bind (fun k => m[k]?) else none
      match hit with
      | some .many => .failed [] i
      | some (.one v) => prepend v (probeAccIdx m (i+1) tail)
      | none => prepend none (probeAccIdx m (i+1) tail)

theorem prefixAnswer_nil (a : Answer) : prefixAnswer [] a = a := by
  cases a <;> simp [prefixAnswer]

theorem probeArr_eq_prefix_probeAccIdx (m : Std.HashMap Int Hit) :
    ∀ (outer : Outer) (arr : Array Cell) (i : Nat),
      i = arr.size → probeArr m arr outer = prefixAnswer arr.toList (probeAccIdx m i outer) := by
  intro outer
  induction outer with
  | nil =>
      intro arr i hi
      simp [probeArr, probeAccIdx, prefixAnswer]
  | cons x tail ih =>
      intro arr i hi
      cases x with
      | mk active key =>
          by_cases ha : active = true
          · subst active
            rcases key with _ | k
            · simp [probeArr, probeAccIdx]
              rw [prefixAnswer_shift]
              simpa using (ih (arr.push none) (i+1) (by simp [hi]))
            · rcases hmk : m[k]? with _ | v
              · simp [probeArr, probeAccIdx, hmk]
                rw [prefixAnswer_shift]
                simpa using (ih (arr.push none) (i+1) (by simp [hi]))
              · rcases v with v | v
                · simp [probeArr, probeAccIdx, hmk]
                  rw [prefixAnswer_shift]
                  simpa using (ih (arr.push v) (i+1) (by simp [hi]))
                · simp [probeArr, probeAccIdx, prefixAnswer, hi, hmk]
          · simp [probeArr, probeAccIdx, ha]
            rw [prefixAnswer_shift]
            simpa using (ih (arr.push none) (i+1) (by simp [hi]))

theorem probe_eq_probeAccIdx (m : Std.HashMap Int Hit) (outer : Outer) :
    probe m outer = probeAccIdx m 0 outer := by
  change probeLoop m outer #[] = probeAccIdx m 0 outer
  rw [probe_eq_probeArr]
  have h := probeArr_eq_prefix_probeAccIdx m outer #[] 0 (by simp)
  simpa [prefixAnswer_nil] using h

theorem probeAccIdx_eq_reference (inner : Inner) (i : Nat) (outer : Outer) :
    probeAccIdx (index inner) i outer = reference inner i outer := by
  induction outer generalizing i with
  | nil => simp [probeAccIdx, reference]
  | cons x tail ih =>
      cases x with
      | mk active key =>
          simp only [probeAccIdx]
          rw [reference_step, hit_eq_desired]
          cases hrows : refHits inner active key with
          | nil =>
              have hval : refValue inner active key = none := by
                rw [refValue_eq_head, hrows]
                rfl
              simp [hval]
              rw [ih (i+1)]
          | cons r rest =>
              cases rest with
              | nil =>
                  rcases r with ⟨rk, v⟩
                  have hval : refValue inner active key = v := by
                    rw [refValue_eq_head, hrows]
                    rfl
                  simp [hval]
                  rw [ih (i+1)]
              | cons r2 rest2 =>
                  simp

theorem sound : TinylambOpt.ResearchGoals.DemandScalar.Goal := by
  intro inner outer
  exact (probe_eq_probeAccIdx (index inner) outer).trans (probeAccIdx_eq_reference inner 0 outer)

end TinylambOpt.ResearchProofs.DemandScalar
