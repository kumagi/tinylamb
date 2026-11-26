import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.AmendLeaf` — Update one cached block using the left and right
  partial states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def M : Nat := 65521
private def B : Nat := 257

/-! Reachable states always carry components in `[0, M)`; the monoid laws for
`agg.join` are needed only on this set (e.g. `p * 1 % M = p` requires `p < M`). -/
private def Good (s : Nat × Nat) : Prop := s.1 < M ∧ s.2 < M

private theorem good_ref (xs : List Nat) : Good (agg.reference xs) := by
  constructor
  · dsimp [agg]
    exact Nat.mod_lt _ (by decide : 0 < M)
  · dsimp [agg]
    suffices ∀ (l : List Nat) (acc : Nat), acc < M →
        List.foldl (fun h x => (B * h + x) % M) acc l < M from this xs 0 (by decide)
    intro l
    induction l with
    | nil => intro acc h; exact h
    | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ (by decide : 0 < M))

private theorem good_join {a b : Nat × Nat} (ha : Good a) (hb : Good b) : Good (agg.join a b) := by
  rcases a with ⟨p, h⟩
  rcases b with ⟨q, j⟩
  dsimp [Good] at ha hb
  dsimp [agg, Good]
  exact ⟨Nat.mod_lt _ (by decide : 0 < M), Nat.mod_lt _ (by decide : 0 < M)⟩

private theorem e_eq : agg.reference [] = (1, 0) := by
  rfl

private theorem join_left_id {x : Nat × Nat} (hx : Good x) : agg.join (agg.reference []) x = x := by
  rw [e_eq]
  rcases x with ⟨p, h⟩
  change p < 65521 ∧ h < 65521 at hx
  simp [agg, Nat.mod_eq_of_lt hx.1, Nat.mod_eq_of_lt hx.2]

private theorem join_right_id {x : Nat × Nat} (hx : Good x) : agg.join x (agg.reference []) = x := by
  rw [e_eq]
  rcases x with ⟨p, h⟩
  change p < 65521 ∧ h < 65521 at hx
  simp [agg, Nat.mod_eq_of_lt hx.1, Nat.mod_eq_of_lt hx.2]

private theorem mul_assoc_mod {p q r : Nat} (hp : p < M) (_hq : q < M) (hr : r < M) :
    (p * q % M * r) % M = (p * (q * r % M)) % M := by
  calc
    (p * q % M * r) % M = ((p * q) % M * (r % M)) % M := by rw [Nat.mod_eq_of_lt hr]
    _ = ((p * q) * r) % M := by rw [← Nat.mul_mod]
    _ = (p * (q * r)) % M := by rw [Nat.mul_assoc]
    _ = ((p % M) * ((q * r) % M)) % M := by rw [Nat.mul_mod]
    _ = (p * ((q * r) % M)) % M := by rw [Nat.mod_eq_of_lt hp]

private theorem join_assoc {a b c : Nat × Nat} (ha : Good a) (hb : Good b) (hc : Good c) :
    agg.join (agg.join a b) c = agg.join a (agg.join b c) := by
  rcases a with ⟨p, h⟩
  rcases b with ⟨q, j⟩
  rcases c with ⟨r, l⟩
  dsimp [Good] at ha hb hc
  dsimp [agg]
  apply Prod.ext
  · exact mul_assoc_mod ha.1 hb.1 hc.1
  · have hmod : ((h * q + j) % M * r + l) % M =
        (h * (q * r % M) + (j * r + l) % M) % M := by
      calc
        ((h * q + j) % M * r + l) % M
            = (((h * q + j) % M * r) % M + l % M) % M := by rw [Nat.add_mod]
        _ = (((h * q + j) % M * (r % M)) % M + l % M) % M := by rw [Nat.mod_eq_of_lt hc.1]
        _ = (((h * q + j) * r) % M + l % M) % M := by
              rw [← Nat.mul_mod (h * q + j) r M]
        _ = (((h * q + j) * r + l)) % M := by rw [← Nat.add_mod]
        _ = (h * (q * r) + (j * r + l)) % M := by
              congr 1
              rw [Nat.add_mul, Nat.mul_assoc, Nat.add_assoc]
        _ = (h * (q * r) % M + (j * r + l) % M) % M := by rw [Nat.add_mod]
        _ = (h * (q * r % M) % M + (j * r + l) % M) % M := by
              rw [Nat.mul_mod h (q * r) M, Nat.mod_eq_of_lt ha.2]
        _ = (h * (q * r % M) + (j * r + l) % M) % M := by
              simpa [Nat.mod_mod] using
                (Nat.add_mod (h * (q * r % M)) ((j * r + l) % M) M).symm
    exact hmod

private theorem good_foldl (a : Nat × Nat) (ha : Good a) :
    ∀ L : List (Nat × Nat), (∀ x ∈ L, Good x) → Good (L.foldl agg.join a) := by
  intro L
  induction L generalizing a with
  | nil => intro h; simpa using ha
  | cons x xs ih =>
      intro h
      simp only [List.foldl_cons]
      exact ih (agg.join a x) (good_join ha (h x (by simp)))
        (by intro y hy; exact h y (by simp [hy]))

private theorem fold_with_acc (e : Nat × Nat) (he : Good e)
    (hleft : ∀ x, Good x → agg.join e x = x)
    (hright : ∀ x, Good x → agg.join x e = x)
    (hassoc : ∀ x y z, Good x → Good y → Good z → agg.join (agg.join x y) z = agg.join x (agg.join y z)) :
    ∀ (a : Nat × Nat), Good a → ∀ L : List (Nat × Nat), (∀ x ∈ L, Good x) →
      L.foldl agg.join a = agg.join a (L.foldl agg.join e) := by
  intro a ha L
  induction L generalizing a with
  | nil =>
      intro h
      simpa using (hright a ha).symm
  | cons x xs ih =>
      intro h
      have hx : Good x := h x (by simp)
      have hxs : ∀ y ∈ xs, Good y := by intro y hy; exact h y (by simp [hy])
      have hax : Good (agg.join a x) := good_join ha hx
      have hfe : Good (xs.foldl agg.join e) := good_foldl e he xs hxs
      simp only [List.foldl_cons]
      rw [ih (agg.join a x) hax hxs]
      rw [ih (agg.join e x) (good_join he hx) hxs]
      rw [hleft x hx]
      rw [hassoc a x (xs.foldl agg.join e) ha hx hfe]

private theorem amend_general (e : Nat × Nat) (he : Good e)
    (hleft : ∀ x, Good x → agg.join e x = x)
    (hright : ∀ x, Good x → agg.join x e = x)
    (hassoc : ∀ x y z, Good x → Good y → Good z → agg.join (agg.join x y) z = agg.join x (agg.join y z))
    (ref : List Nat → Nat × Nat) (href : ∀ xs, Good (ref xs)) :
    ∀ (a : Nat × Nat) (ps : List (List Nat)) i xs, Good a → i < ps.length →
      ((ps.set i xs).map ref).foldl agg.join a =
        agg.join (((ps.take i).map ref).foldl agg.join a)
          (agg.join (ref xs) (((ps.drop (i + 1)).map ref).foldl agg.join e)) := by
  intro a ps
  induction ps generalizing a with
  | nil =>
      intro i xs ha hi
      simp at hi
  | cons p ps ih =>
      intro i xs ha hi
      cases i with
      | zero =>
          simp only [List.set_cons_zero, List.take_zero, List.map_cons, List.foldl_cons,
                     List.map_nil, List.foldl_nil, List.drop]
          have hxs : Good (ref xs) := href xs
          have hps : ∀ y ∈ (ps.map ref), Good y := by
            intro y hy
            rcases List.mem_map.mp hy with ⟨z, hz, rfl⟩
            exact href z
          have hax : Good (agg.join a (ref xs)) := good_join ha hxs
          rw [fold_with_acc e he hleft hright hassoc (agg.join a (ref xs)) hax (ps.map ref) hps]
          rw [hassoc a (ref xs) ((ps.map ref).foldl agg.join e) ha hxs
                (good_foldl e he (ps.map ref) hps)]
      | succ k =>
          have hk : k < ps.length := by simpa using hi
          have hp : Good (ref p) := href p
          have hap : Good (agg.join a (ref p)) := good_join ha hp
          simpa [List.set, List.take, List.drop] using ih (agg.join a (ref p)) k xs hap hk

theorem sound_AmendLeaf : AmendLeaf.Goal := by
  intro ps i xs hi
  simpa [combine] using
    (amend_general (agg.reference []) (good_ref [])
      (fun x hx => join_left_id hx) (fun x hx => join_right_id hx)
      (fun x y z hx hy hz => join_assoc hx hy hz)
      agg.reference good_ref
      (agg.reference []) ps i xs (good_ref []) hi)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_AmendLeaf
