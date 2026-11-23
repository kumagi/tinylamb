import TinylambOpt.ResearchGoals.OrderedTagFrontier

/-
Two evaluators run the same NFA with the same epsilon bound.  The reference
`fullRun` keeps every history; `frontier` keeps, after each epsilon round and
each byte, only the earliest history per state (`firstStates`).  Because edges
never inspect tags, the earliest history per state is exactly the history the
reference would first accept, so `winner` agrees and the frontier is free of
duplicate state ids.
-/
namespace TinylambOpt.ResearchProofs.OrderedTagFrontier

open TinylambOpt.Research.RegexTaggedDeterminize
open TinylambOpt.ResearchGoals.OrderedTagFrontier

/-! ### The `firstStates` foldl and its invariants -/

def stepF (out : List Config) (c : Config) : List Config :=
  if out.any (fun d => d.1 == c.1) then out else out ++ [c]

theorem firstStates_eq (cs : List Config) :
    firstStates cs = List.foldl stepF [] cs := rfl

theorem any_fst_eq (out : List Config) (c : Config) :
    (out.any fun d => d.1 == c.1) = decide (c.1 ∈ out.map Prod.fst) := by
  induction out with
  | nil => simp
  | cons d ds ih =>
      rw [List.any_cons, ih, List.map_cons]
      by_cases hd : d.1 = c.1
      · simp [hd]
      · have hd2 : ¬ c.1 = d.1 := fun he => hd he.symm
        by_cases hm : c.1 ∈ ds.map Prod.fst <;> simp [hd, hd2, hm]

theorem stepF_nil (x : Config) : stepF [] x = [x] := by
  unfold stepF
  simp

theorem stepF_seen {out : List Config} {x : Config} (h : x.1 ∈ out.map Prod.fst) :
    stepF out x = out := by
  unfold stepF
  have hc : out.any (fun d => d.1 == x.1) = true := by rw [any_fst_eq]; simp [h]
  simp [hc]

theorem stepF_fresh {out : List Config} {x : Config} (h : x.1 ∉ out.map Prod.fst) :
    stepF out x = out ++ [x] := by
  unfold stepF
  have hc : out.any (fun d => d.1 == x.1) = false := by rw [any_fst_eq]; simp [h]
  simp [hc]

theorem foldl_stepF_acc (cs out : List Config) :
    List.foldl stepF out cs =
      out ++ (firstStates cs).filter (fun c => ! (c.1 ∈ out.map Prod.fst)) := by
  induction cs generalizing out with
  | nil => simp [firstStates]
  | cons x xs ih =>
      have hx : firstStates (x :: xs) =
          [x] ++ (firstStates xs).filter (fun c => ! (c.1 ∈ [x].map Prod.fst)) := by
        show List.foldl stepF [] (x :: xs) = _
        rw [List.foldl_cons, stepF_nil]
        exact ih (out := [x])
      rw [List.foldl_cons, ih (out := stepF out x), hx,
        List.filter_append, List.filter_filter]
      by_cases h : x.1 ∈ out.map Prod.fst
      · rw [stepF_seen h]
        have h1 : List.filter (fun c => ! (c.1 ∈ out.map Prod.fst)) [x] = [] := by
          refine List.filter_eq_nil_iff.mpr (fun c hc => ?_)
          rw [List.mem_singleton] at hc
          rw [hc]
          simp [h]
        have h2 : List.filter (fun a => (fun c => ! (c.1 ∈ out.map Prod.fst)) a &&
            (fun c => ! (c.1 ∈ [x].map Prod.fst)) a) (firstStates xs) =
            List.filter (fun c => ! (c.1 ∈ out.map Prod.fst)) (firstStates xs) := by
          refine List.filter_congr (fun c _ => ?_)
          by_cases hc : c.1 = x.1
          · cases c with
            | mk s ts =>
                subst hc
                simp [h]
          · simp [hc]
        rw [h1, List.nil_append, h2]
      · rw [stepF_fresh h]
        have h1 : List.filter (fun c => ! (c.1 ∈ out.map Prod.fst)) [x] = [x] := by
          refine List.filter_eq_self.mpr (fun c hc => ?_)
          rw [List.mem_singleton] at hc
          rw [hc]
          simp [h]
        have h2 : List.filter (fun a => (fun c => ! (c.1 ∈ out.map Prod.fst)) a &&
            (fun c => ! (c.1 ∈ [x].map Prod.fst)) a) (firstStates xs) =
            List.filter (fun c => ! (c.1 ∈ (out ++ [x]).map Prod.fst)) (firstStates xs) := by
          refine List.filter_congr (fun c _ => ?_)
          by_cases hc : c.1 = x.1 <;> by_cases hoc : c.1 ∈ out.map Prod.fst <;>
            simp [hc, hoc, List.mem_append, h]
        rw [h1, h2, List.append_assoc]

theorem firstStates_append (cs ds : List Config) :
    firstStates (cs ++ ds) =
      firstStates cs ++ (firstStates ds).filter
        (fun c => ! (c.1 ∈ (firstStates cs).map Prod.fst)) := by
  have e1 : firstStates (cs ++ ds) = List.foldl stepF (firstStates cs) ds := by
    rw [firstStates_eq, List.foldl_append, ← firstStates_eq]
  rw [e1, foldl_stepF_acc]

theorem foldl_stepF_nodup : ∀ (cs out : List Config),
    (out.map Prod.fst).Nodup → ((List.foldl stepF out cs).map Prod.fst).Nodup := by
  intro cs
  induction cs with
  | nil => intro out h; exact h
  | cons x xs ih =>
      intro out h
      rw [List.foldl_cons]
      apply ih (stepF out x)
      by_cases hx : x.1 ∈ out.map Prod.fst
      · rw [stepF_seen hx]
        exact h
      · rw [stepF_fresh hx, List.map_append, List.map_singleton]
        refine List.nodup_append.mpr ⟨h, by simp, ?_⟩
        intro a ha b hb
        rw [List.mem_singleton] at hb
        subst hb
        intro e
        exact hx (by rw [← e]; exact ha)

theorem firstStates_nodup (cs : List Config) :
    ((firstStates cs).map Prod.fst).Nodup := by
  rw [firstStates_eq]
  exact foldl_stepF_nodup cs [] (by simp)

theorem foldl_stepF_keep (cs out : List Config)
    (h : ((cs.map Prod.fst) ++ (out.map Prod.fst)).Nodup) :
    List.foldl stepF out cs = out ++ cs := by
  induction cs generalizing out with
  | nil => simp
  | cons x xs ih =>
      have hnd := List.nodup_cons.mp h
      have h1 : x.1 ∉ out.map Prod.fst := fun hx => hnd.1 (by simp [hx])
      rw [List.foldl_cons, stepF_fresh h1,
        ih (out := out ++ [x]) (by
          show ((xs.map Prod.fst) ++ ((out ++ [x]).map Prod.fst)).Nodup
          rw [List.map_append, List.map_singleton]
          have h2 := List.nodup_append.mp hnd.2
          refine List.nodup_append.mpr ⟨h2.1, ?_, ?_⟩
          · refine List.nodup_append.mpr ⟨h2.2.1, by simp, ?_⟩
            intro a ha b hb
            rw [List.mem_singleton] at hb
            subst hb
            intro e
            exact h1 (by rw [← e]; exact ha)
          · intro a ha b hb
            rcases List.mem_append.mp hb with hb1 | hb2
            · exact h2.2.2 a ha b hb1
            · rw [List.mem_singleton] at hb2
              subst hb2
              intro e
              exact hnd.1 (List.mem_append.mpr (Or.inl (by rw [← e]; exact ha))))]
      rw [List.append_assoc]
      rfl

theorem firstStates_idem (cs : List Config) :
    firstStates (firstStates cs) = firstStates cs := by
  rw [firstStates_eq,
    foldl_stepF_keep (cs := firstStates cs) (out := []) (by simpa using firstStates_nodup cs)]
  simp

/-! ### Membership characterisation -/

theorem foldl_stepF_mem : ∀ (cs out : List Config) (c : Config),
    c ∈ List.foldl stepF out cs → c ∈ out ∨ c ∈ cs := by
  intro cs
  induction cs with
  | nil => intro out c h; exact Or.inl h
  | cons x xs ih =>
      intro out c h
      rw [List.foldl_cons] at h
      rcases ih (stepF out x) c h with ho | hxs
      · by_cases hx : x.1 ∈ out.map Prod.fst
        · rw [stepF_seen hx] at ho
          exact Or.inl ho
        · rw [stepF_fresh hx, List.mem_append, List.mem_singleton] at ho
          rcases ho with ho | heq
          · exact Or.inl ho
          · rw [heq]
            exact Or.inr (List.mem_cons.mpr (Or.inl rfl))
      · exact Or.inr (List.mem_cons.mpr (Or.inr hxs))

theorem firstStates_sub {cs : List Config} {c : Config} (h : c ∈ firstStates cs) :
    c ∈ cs := by
  rw [firstStates_eq] at h
  rcases foldl_stepF_mem cs [] c h with h0 | hcs
  · exact absurd h0 (by simp)
  · exact hcs

theorem foldl_stepF_cov : ∀ (cs out : List Config) (c : Config),
    (c ∈ out ∨ c ∈ cs) → ∃ k ∈ List.foldl stepF out cs, k.1 = c.1 := by
  intro cs
  induction cs with
  | nil =>
      intro out c hc
      exact ⟨c, hc.elim id (fun h => absurd h (by simp)), rfl⟩
  | cons x xs ih =>
      intro out c hc
      rw [List.foldl_cons]
      rcases hc with ho | hcx
      · apply ih (stepF out x) c
        left
        by_cases hx : x.1 ∈ out.map Prod.fst
        · rw [stepF_seen hx]
          exact ho
        · rw [stepF_fresh hx, List.mem_append]
          exact Or.inl ho
      · rcases List.mem_cons.mp hcx with heq | hxs
        · rw [heq]
          by_cases hx : x.1 ∈ out.map Prod.fst
          · obtain ⟨k, hko, hke⟩ := List.mem_map.mp hx
            obtain ⟨w, hw, hwe⟩ := ih (stepF out x) k (Or.inl (by rw [stepF_seen hx]; exact hko))
            exact ⟨w, hw, hwe.trans hke⟩
          · have hin : x ∈ stepF out x := by
              rw [stepF_fresh hx]
              exact List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl))
            obtain ⟨w, hw, hwe⟩ := ih (stepF out x) x (Or.inl hin)
            exact ⟨w, hw, hwe⟩
        · exact ih (stepF out x) c (Or.inr hxs)

theorem firstStates_cov {cs : List Config} {c : Config} (h : c ∈ cs) :
    ∃ k ∈ firstStates cs, k.1 = c.1 := by
  rw [firstStates_eq]
  exact foldl_stepF_cov cs [] c (Or.inr h)

/-! ### `find?` passes through `firstStates` for state-only predicates -/

theorem find?_none_mem {p : Config → Bool} {l : List Config} {a : Config}
    (h : a ∈ l) (hn : List.find? p l = none) : p a = false := by
  induction l with
  | nil => exact absurd h (by simp)
  | cons y ys ih =>
      rcases List.mem_cons.mp h with heq | hy
      · rw [heq]
        cases hpy : p y with
        | true =>
            rw [List.find?_cons_of_pos hpy] at hn
            exact absurd hn (by simp)
        | false => rfl
      · have hn' : List.find? p ys = none := by
          cases hpy0 : p y with
          | true =>
              rw [List.find?_cons_of_pos hpy0] at hn
              exact absurd hn (by simp)
          | false =>
              rw [List.find?_cons_of_neg (by simp [hpy0])] at hn
              exact hn
        exact ih hy hn'

theorem foldl_stepF_find? (p : Config → Bool)
    (hp : ∀ c d : Config, c.1 = d.1 → p c = p d) :
    ∀ (cs out : List Config),
      List.find? p (List.foldl stepF out cs) = List.find? p (out ++ cs) := by
  intro cs
  induction cs with
  | nil => intro out; show List.find? p out = List.find? p (out ++ []); rw [List.append_nil]
  | cons x xs ih =>
      intro out
      rw [List.foldl_cons, ih (stepF out x)]
      by_cases h : x.1 ∈ out.map Prod.fst
      · rw [stepF_seen h, List.find?_append, List.find?_append]
        by_cases hpx : p x = true
        · obtain ⟨k, hko, hke⟩ := List.mem_map.mp h
          have hpk : p k = true := by
            rw [hp k x hke]
            exact hpx
          have hsome : List.find? p out ≠ none := by
            intro hnone
            have hpk' := find?_none_mem hko hnone
            exact absurd (hpk.symm.trans hpk') (by decide)
          have hfo : ∀ o : Option Config, (List.find? p out).or o = List.find? p out := by
            intro o
            cases hw : List.find? p out with
            | none => exact absurd hw hsome
            | some w => simp [Option.or]
          rw [List.find?_cons_of_pos hpx, hfo, hfo]
        · rw [List.find?_cons_of_neg hpx]
      · rw [stepF_fresh h, List.append_assoc]
        rfl

theorem firstStates_find? (p : Config → Bool)
    (hp : ∀ c d : Config, c.1 = d.1 → p c = p d) (cs : List Config) :
    List.find? p (firstStates cs) = List.find? p cs := by
  rw [firstStates_eq, foldl_stepF_find? p hp]
  simp

/-! ### FlatMaps that only look at the state -/

theorem firstStates_flatMap (f : Config → List Config)
    (hf : ∀ c d : Config, c.1 = d.1 → (f c).map Prod.fst = (f d).map Prod.fst)
    (cs : List Config) :
    firstStates (cs.flatMap f) = firstStates ((firstStates cs).flatMap f) := by
  have aux : ∀ (cs out acc : List Config), acc = firstStates (out.flatMap f) →
      List.foldl stepF acc (cs.flatMap f) =
        firstStates ((List.foldl stepF out cs).flatMap f) := by
    intro cs
    induction cs with
    | nil => intro out acc h; simpa using h
    | cons x xs ih =>
        intro out acc h
        rw [List.flatMap_cons, List.foldl_append, List.foldl_cons]
        apply ih (stepF out x) (List.foldl stepF acc (f x))
        by_cases hx : x.1 ∈ out.map Prod.fst
        · rw [stepF_seen hx, ← h, foldl_stepF_acc (out := acc) (cs := f x)]
          have hempty : ((firstStates (f x)).filter fun c => ! (c.1 ∈ acc.map Prod.fst)) = [] := by
            refine List.filter_eq_nil_iff.mpr (fun c hc => ?_)
            have hcm := firstStates_sub hc
            obtain ⟨k, hko, hke⟩ := List.mem_map.mp hx
            have h1 : c.1 ∈ (f k).map Prod.fst := by
              have hcx : c.1 ∈ (f x).map Prod.fst := List.mem_map.mpr ⟨c, hcm, rfl⟩
              rw [hf k x hke]
              exact hcx
            obtain ⟨y, hy, hye⟩ := List.mem_map.mp h1
            have hy' : y ∈ out.flatMap f := List.mem_flatMap.mpr ⟨k, hko, hy⟩
            obtain ⟨z, hz, hze⟩ := firstStates_cov hy'
            rw [← h] at hz
            have hmem : c.1 ∈ acc.map Prod.fst := List.mem_map.mpr ⟨z, hz, hze.trans hye⟩
            simp [hmem]
          rw [hempty, List.append_nil]
        · rw [stepF_fresh hx, List.flatMap_append, firstStates_append, ← h,
            foldl_stepF_acc (out := acc) (cs := f x)]
          simp only [List.flatMap, List.map_cons, List.map_nil, List.flatten_cons,
            List.flatten_nil, List.append_nil]
  rw [firstStates_eq]
  exact aux cs [] [] rfl

theorem stepEps_fst (nfa : NFA) (pos : Pos) :
    ∀ c d : Config, c.1 = d.1 →
      (stepEps nfa pos c).map Prod.fst = (stepEps nfa pos d).map Prod.fst := by
  intro c d
  cases c with
  | mk q tc =>
      cases d with
      | mk q' td =>
          intro h
          have hq : q = q' := h
          subst hq
          unfold stepEps
          induction nfa.edges q generalizing tc td with
          | nil => rfl
          | cons e es ih =>
              cases e with
              | eps q' => simpa using ih tc td
              | setTag k q' => simpa using ih tc td
              | byte b q' => simpa using ih tc td

theorem stepByte_fst (nfa : NFA) (b : Byte) :
    ∀ c d : Config, c.1 = d.1 →
      (stepByte nfa c b).map Prod.fst = (stepByte nfa d b).map Prod.fst := by
  intro c d
  cases c with
  | mk q tc =>
      cases d with
      | mk q' td =>
          intro h
          have hq : q = q' := h
          subst hq
          unfold stepByte
          induction nfa.edges q generalizing tc td with
          | nil => rfl
          | cons e es ih =>
              cases e with
              | eps q' => simpa using ih tc td
              | setTag k q' => simpa using ih tc td
              | byte b' q' =>
                  by_cases hb : b = b' <;> simpa [hb] using ih tc td

/-! ### `close` versus `fullClosure` -/

theorem firstStates_H (nfa : NFA) (pos : Nat) (cs : List Config) :
    firstStates (cs ++ cs.flatMap (stepEps nfa pos)) =
      firstStates (firstStates cs ++ (firstStates cs).flatMap (stepEps nfa pos)) := by
  have e1 : firstStates (cs ++ cs.flatMap (stepEps nfa pos)) =
      firstStates cs ++ (firstStates (cs.flatMap (stepEps nfa pos))).filter
        (fun c => ! (c.1 ∈ (firstStates cs).map Prod.fst)) := firstStates_append _ _
  have e2 : firstStates (firstStates cs ++ (firstStates cs).flatMap (stepEps nfa pos)) =
      firstStates (firstStates cs) ++
        (firstStates ((firstStates cs).flatMap (stepEps nfa pos))).filter
          (fun c => ! (c.1 ∈ (firstStates (firstStates cs)).map Prod.fst)) :=
    firstStates_append _ _
  rw [e1, e2, firstStates_idem,
    firstStates_flatMap (stepEps nfa pos) (stepEps_fst nfa pos) cs]

theorem firstStates_closure (nfa : NFA) (pos : Nat) :
    ∀ (limit : Nat) (cs ds : List Config), firstStates cs = firstStates ds →
      firstStates (fullClosure nfa pos limit cs) =
        firstStates (fullClosure nfa pos limit ds) := by
  intro limit
  induction limit with
  | zero => intro cs ds h; simp only [fullClosure]; exact h
  | succ k ih =>
      intro cs ds h
      have e1 : fullClosure nfa pos (k + 1) cs =
          fullClosure nfa pos k (cs ++ cs.flatMap (stepEps nfa pos)) := by
        simp only [fullClosure]
      have e2 : fullClosure nfa pos (k + 1) ds =
          fullClosure nfa pos k (ds ++ ds.flatMap (stepEps nfa pos)) := by
        simp only [fullClosure]
      rw [e1, e2]
      apply ih
      rw [firstStates_H nfa pos cs, firstStates_H nfa pos ds, h]

theorem close_fullClosure (nfa : NFA) (pos : Nat) :
    ∀ (limit : Nat) (cs : List Config),
      close nfa pos limit cs = firstStates (fullClosure nfa pos limit cs) := by
  intro limit
  induction limit with
  | zero => intro cs; simp only [fullClosure, close]
  | succ k ih =>
      intro cs
      have e1 : close nfa pos (k + 1) cs =
          close nfa pos k (firstStates cs ++ (firstStates cs).flatMap (stepEps nfa pos)) := rfl
      have e2 : fullClosure nfa pos (k + 1) cs =
          fullClosure nfa pos k (cs ++ cs.flatMap (stepEps nfa pos)) := by
        simp only [fullClosure]
      rw [e1, ih, e2]
      apply firstStates_closure nfa pos k
      exact (firstStates_H nfa pos cs).symm

/-! ### `walk` versus `fullRunFrom` -/

theorem walk_fullRunFrom (nfa : NFA) (limit : Nat) :
    ∀ (bs : List Byte) (pos : Nat) (cs : List Config),
      walk nfa limit pos (firstStates cs) bs =
        firstStates (fullRunFrom nfa limit bs pos cs) := by
  intro bs
  induction bs with
  | nil => intro pos cs; rfl
  | cons b bs ih =>
      intro pos cs
      have key : close nfa (pos + 1) limit
          ((firstStates cs).flatMap fun c => stepByte nfa c b) =
          firstStates (fullClosure nfa (pos + 1) limit
            (cs.flatMap fun c => stepByte nfa c b)) := by
        rw [close_fullClosure]
        apply firstStates_closure nfa (pos + 1) limit
        exact (firstStates_flatMap (fun c => stepByte nfa c b) (stepByte_fst nfa b) cs).symm
      show walk nfa limit (pos + 1)
        (close nfa (pos + 1) limit ((firstStates cs).flatMap fun c => stepByte nfa c b)) bs = _
      rw [key]
      exact ih (pos + 1) (fullClosure nfa (pos + 1) limit
        (cs.flatMap fun c => stepByte nfa c b))

/-! ### The frontier is the reference run, deduplicated by state -/

theorem frontier_eq_fullRun (nfa : NFA) (limit : Nat) (input : List Byte) :
    frontier nfa limit input = firstStates (fullRun nfa limit input) := by
  show walk nfa limit 0 (close nfa 0 limit [initConfig nfa]) input = _
  rw [close_fullClosure]
  exact walk_fullRunFrom nfa limit input 0 (fullClosure nfa 0 limit [initConfig nfa])

theorem sound : TinylambOpt.ResearchGoals.OrderedTagFrontier.Goal := by
  intro nfa limit input
  rw [frontier_eq_fullRun]
  constructor
  · unfold winner
    rw [firstStates_find? (fun c => nfa.accept c.1)
      (fun c d h => by rw [h])]
  · exact firstStates_nodup (fullRun nfa limit input)

end TinylambOpt.ResearchProofs.OrderedTagFrontier
