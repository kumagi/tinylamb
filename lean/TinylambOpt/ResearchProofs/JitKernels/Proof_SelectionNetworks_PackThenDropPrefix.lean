import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Proof that dropping selected rows can be performed by dropping the corresponding source prefix. -/
namespace Nov26.Proof_SelectionNetworks_PackThenDropPrefix
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PackThenDropPrefix
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PackThenDropPrefix
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PackThenDropPrefix
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PackThenDropPrefix

private theorem positions_zipIdx (m : List Bool) (k : Nat) :
    ((m.zipIdx k).filter Prod.fst).map Prod.snd = (positions m).map (fun i => i + k) := by
  induction m generalizing k with
  | nil => simp [positions]
  | cons b bs ih =>
      unfold positions
      simp only [List.zipIdx_cons, List.filter_cons, List.map_cons]
      cases b <;> simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem positions_false (m : List Bool) :
    positions (false :: m) = (positions m).map (fun i => i + 1) := by
  change (((false :: m).zipIdx 0).filter Prod.fst).map Prod.snd =
    (positions m).map (fun i => i + 1)
  rw [List.zipIdx_cons]
  simp only [List.filter_cons, List.map_cons]
  exact positions_zipIdx m 1

private theorem positions_true (m : List Bool) :
    positions (true :: m) = 0 :: (positions m).map (fun i => i + 1) := by
  change (((true :: m).zipIdx 0).filter Prod.fst).map Prod.snd =
    0 :: (positions m).map (fun i => i + 1)
  rw [List.zipIdx_cons]
  simp only [List.filter_cons, List.map_cons]
  exact congrArg (fun l => 0 :: l) (positions_zipIdx m 1)

private theorem gather_cons_succ (x : Nat) (xs idxs : List Nat) :
    gather (x :: xs) (idxs.map (fun i => i + 1)) = gather xs idxs := by
  have hfun : (fun i => (x :: xs)[i + 1]?) = (fun i => xs[i]?) := by
    funext i
    simp
  unfold gather
  rw [List.filterMap_map]
  apply List.ext_getElem?
  intro j
  change (idxs.filterMap (fun i => (x :: xs)[i + 1]?))[j]? =
    (idxs.filterMap (fun i => xs[i]?))[j]?
  rw [hfun]

private theorem compress_false (x : Nat) (xs : List Nat) (m : List Bool) :
    compress (x :: xs) (false :: m) = compress xs m := by
  rw [compress, positions_false, gather_cons_succ]
  rfl

private theorem compress_true (x : Nat) (xs : List Nat) (m : List Bool) :
    compress (x :: xs) (true :: m) = x :: compress xs m := by
  rw [compress, positions_true]
  change (0 :: (positions m).map (fun i => i + 1)).filterMap (fun i => (x :: xs)[i]?) =
    x :: compress xs m
  simp only [List.filterMap_cons, List.getElem?_cons_zero]
  rw [List.filterMap_map]
  have hfun : (fun i => (x :: xs)[i + 1]?) = (fun i => xs[i]?) := by
    funext i
    simp
  change x :: (positions m).filterMap (fun i => (x :: xs)[i + 1]?) =
    x :: compress xs m
  rw [hfun]
  rfl

private theorem positions_get_le : ∀ (m : List Bool) (k i : Nat),
    (positions m)[k]? = some i → k ≤ i := by
  intro m
  induction m with
  | nil => intro k i h; simp [positions] at h
  | cons b bs ih =>
      intro k i h
      cases b
      · rw [positions_false] at h
        rw [List.getElem?_map] at h
        cases hp : (positions bs)[k]? with
        | none => simp [hp] at h
        | some j =>
            simp [hp] at h
            have hle := ih k j hp
            omega
      · rw [positions_true] at h
        cases k with
        | zero =>
            simp at h
            omega
        | succ k =>
            rw [List.getElem?_cons_succ, List.getElem?_map] at h
            cases hp : (positions bs)[k]? with
            | none => simp [hp] at h
            | some j =>
                simp [hp] at h
                have hle := ih k j hp
                omega

theorem sound : Goal := by
  intro xs m k i hlen hpos
  induction i generalizing xs m k with
  | zero =>
      have hk : k = 0 := by
        have hle := positions_get_le m k 0 hpos
        omega
      subst k
      simp
  | succ i ih =>
      cases m with
      | nil => simp [positions] at hpos
      | cons b mt =>
          cases xs with
          | nil => simp at hlen
          | cons x xt =>
              simp at hlen
              cases b
              · rw [positions_false] at hpos
                rw [List.getElem?_map] at hpos
                have hpos_mt : (positions mt)[k]? = some i := by
                  cases hp : (positions mt)[k]? with
                  | none => simp [hp] at hpos
                  | some j =>
                      simp [hp] at hpos
                      subst j
                      rfl
                have hrec := ih xt mt k hlen hpos_mt
                rw [compress_false]
                simpa using hrec
              · rw [positions_true] at hpos
                cases k with
                | zero => simp at hpos
                | succ k =>
                    rw [List.getElem?_cons_succ, List.getElem?_map] at hpos
                    have hpos_mt : (positions mt)[k]? = some i := by
                      cases hp : (positions mt)[k]? with
                      | none => simp [hp] at hpos
                      | some j =>
                          simp [hp] at hpos
                          subst j
                          rfl
                    have hrec := ih xt mt k hlen hpos_mt
                    rw [compress_true]
                    simpa using hrec

end Nov26.Proof_SelectionNetworks_PackThenDropPrefix