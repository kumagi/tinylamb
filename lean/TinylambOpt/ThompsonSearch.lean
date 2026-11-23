import TinylambOpt.RegexBitsetNfa

namespace TinylambOpt.RegexBitsetNfa.Thompson

private theorem snocInd {m : Word → Prop} (empty : m []) (step : ∀ w b, m w → m (w ++ [b]))
    (w : Word) : m w := by
  have aux (xs : Word) : m xs.reverse := by
    induction xs with
    | nil => exact empty
    | cons b bs ih => simpa using step _ b ih
  simpa using aux w.reverse

private def after (n : NFA) (w : Word) : SSet := setSearchRun n (initSet n) w

private theorem run_append (n : NFA) (x y : Word) (s : SSet) :
    setSearchRun n s (x ++ y) = setSearchRun n (setSearchRun n s x) y := by
  induction x generalizing s with
  | nil => rfl
  | cons b xs ih => exact ih _

private theorem after_snoc (n : NFA) (w : Word) (b : Byte) :
    after n (w ++ [b]) = searchStep n (after n w) b :=
  run_append ..

private def injected (n : NFA) (w : Word) := epsClose n (fun i => after n w i || startSet n i)

private theorem after_closed (n : NFA) (w : Word) :
    ∀ i j, after n w i = true → j ∈ n.eps i → j < n.states → after n w j = true := by
  apply snocInd (m := fun w => ∀ i j, after n w i = true → j ∈ n.eps i →
      j < n.states → after n w j = true) _ _ w
  · exact fun i j => epsClose_closed n _ i j
  · intro w b _ i j
    rw [after_snoc]
    exact epsClose_closed n _ i j

private theorem injected_cases (n : NFA) (w : Word) (i : Nat) (h : injected n w i = true) :
    after n w i = true ∨ initSet n i = true := by
  apply Bool.or_eq_true_iff.mp
  apply epsClose_least n _ (fun q => after n w q || initSet n q) _ _ i h
  · intro q hq h
    rcases Bool.or_eq_true_iff.mp h with hs | hs
    · simp [hs]
    · simp [show initSet n q = true from epsClose_contains n _ q hq hs]
  · intro q _ h j hj range
    rcases Bool.or_eq_true_iff.mp h with hs | hs
    · simp [after_closed n w q j hs hj range]
    · simp [show initSet n j = true from epsClose_closed n _ q j hs hj range]

namespace Program

private theorem edge_target (p : Program l) (i j : Nat) (c : Option Byte) :
    j ∈ p.targets i c ↔ j < p.nodes.length ∧ p.graph.edge (p.node i) c (p.node j) = true := by
  simp [targets]

private theorem machine_accept (p : Program l) (j : Nat) :
    p.machine.accept j = true ↔ p.node j = p.graph.exit := by
  letI := p.eqQ
  simp [machine]

/-- Executing one labeled graph edge advances the consumed prefix, and an
    epsilon edge stays at the same boundary. Each port has a valid index. -/
private theorem physical_closed (p : Program l) :
    Closed p.graph (fun q w => injected p.machine w (p.index q) = true) := by
  intro i c j w he hi
  have target : p.index j ∈ p.targets (p.index i) c :=
    (edge_target ..).mpr ⟨p.index_lt j,by simpa using he⟩
  cases c with
  | none =>
    simpa [injected] using epsClose_closed p.machine _ _ _ hi target (p.index_lt j)
  | some b =>
    apply epsClose_contains p.machine _ _ (p.index_lt j)
    apply Bool.or_eq_true_iff.mpr ∘ Or.inl
    rw [show w ++ (some b).toList = w ++ [b] from rfl, after_snoc]
    apply epsClose_contains p.machine _ _ (p.index_lt j)
    simp only [transStep,show p.index j < p.machine.states from p.index_lt j, ↓reduceIte,
      List.any_eq_true, Bool.and_eq_true]
    exact ⟨p.index i,List.mem_range.mpr (p.index_lt i),hi,decide_eq_true target⟩

private theorem entry_injected (p : Program l) (w : Word) :
    injected p.machine w (p.index p.graph.entry) = true := by
  apply epsClose_contains p.machine _ _ (p.index_lt _)
  simp [startSet,machine,p.index_lt]

private theorem closure_preserves (p : Program l) (s : SSet) (labels : p.Q → Lang)
    (closed : Closed p.graph labels) (w : Word)
    (base : ∀ i, i < p.nodes.length → s i = true → labels (p.node i) w)
    (i : Nat) (hi : epsClose p.machine s i = true) : labels (p.node i) w := by
  classical
  have h := epsClose_least p.machine s (fun q => decide (labels (p.node q) w))
    (fun q hq h => by simpa using base q hq h) (by
      intro q _ h j edge _
      have hg := ((edge_target p q j none).mp edge).2
      simpa using closed _ none _ w hg (of_decide_eq_true h)) i hi
  exact of_decide_eq_true h

private theorem run_invariant (p : Program l) (labels : p.Q → Lang) (closed : Closed p.graph labels)
    (entry : ∀ w, labels p.graph.entry w) (w : Word) :
    ∀ i, after p.machine w i = true → labels (p.node i) w := by
  apply snocInd (m := fun w => ∀ i, after p.machine w i = true → labels (p.node i) w) _ _ w
  · intro i hi
    apply closure_preserves p _ labels closed [] _ i hi
    intro q _ h
    have he : q = p.index p.graph.entry := of_decide_eq_true (Bool.and_eq_true_iff.mp h).1
    simpa [he] using entry []
  · intro w b ih i hi
    rw [after_snoc] at hi
    apply closure_preserves p _ labels closed (w++[b]) _ i hi
    intro j hj h
    simp only [transStep,show j < p.machine.states from hj, ↓reduceIte,
      List.any_eq_true, Bool.and_eq_true] at h
    obtain ⟨q,_,hq,edge⟩ := h
    have hq' : labels (p.node q) w := by
      apply closure_preserves p _ labels closed w _ q hq
      intro k _ hk
      rcases Bool.or_eq_true_iff.mp hk with ha | hs
      · exact ih k ha
      · have he : k = p.index p.graph.entry := of_decide_eq_true (Bool.and_eq_true_iff.mp hs).1
        simpa [he] using entry w
    exact closed _ (some b) _ _ ((edge_target p q j (some b)).mp (of_decide_eq_true edge)).2 hq'

theorem machine_search (p : Program l) (w : Word) (width : Nat) (fit : p.machine.states ≤ width) :
    containsMatch p.machine width w = true ↔ ∃ pre mid post, w = pre ++ mid ++ post ∧ l mid := by
  rw [containsMatch_iff_reference _ _ fit]
  constructor
  · rintro ⟨prefixBytes,post,rfl,h⟩
    obtain ⟨i,_,hi,accept⟩ := (by simpa [after, List.any_eq_true, Bool.and_eq_true] using h :
      ∃ i ∈ List.range p.machine.states, after p.machine prefixBytes i = true ∧
        p.machine.accept i = true)
    obtain ⟨labels,closed,entry,exitLabel⟩ := p.correct.invariant (fun _ => True)
    have label := run_invariant p labels closed (fun w => entry w trivial) _ i hi
    rw [(machine_accept ..).mp accept] at label
    obtain ⟨pre,mid,rfl,_,hm⟩ := exitLabel _ label
    exact ⟨pre,mid,post,rfl,hm⟩
  · rintro ⟨pre,mid,post,rfl,hm⟩
    have exitOn := p.correct.forward _ (physical_closed p) pre mid (entry_injected p pre) hm
    have accept : p.machine.accept (p.index p.graph.exit) = true :=
      (machine_accept ..).mpr (p.node_index _)
    rcases injected_cases _ _ _ exitOn with fromRun | fromEmpty
    · exact ⟨pre++mid,post,rfl,List.any_eq_true.mpr
        ⟨_,List.mem_range.mpr (p.index_lt _),Bool.and_eq_true_iff.mpr ⟨fromRun,accept⟩⟩⟩
    · exact ⟨[],_,rfl,List.any_eq_true.mpr
        ⟨_,List.mem_range.mpr (p.index_lt _),Bool.and_eq_true_iff.mpr ⟨fromEmpty,accept⟩⟩⟩

end Program

/-- End-to-end statement for every source AST and input, including nullable
    nested stars and matches that end strictly inside a row. -/
theorem compiled_search_iff (r : Regex) (w : Word) (width : Nat) (fit : (compile r).states ≤ width) :
    containsMatch (compile r) width w = true ↔
      ∃ pre mid post, w = pre ++ mid ++ post ∧ language r mid :=
  (assemble r).machine_search w width fit

end TinylambOpt.RegexBitsetNfa.Thompson
