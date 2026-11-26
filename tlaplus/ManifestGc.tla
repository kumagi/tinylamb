----------------------------- MODULE ManifestGc -----------------------------
EXTENDS Naturals

CONSTANTS RootsFirst, CheckParent
VARIABLES head, published, sealed, gcPhase, rootView, sealView, keyExists
vars == <<head, published, sealed, gcPhase, rootView, sealView, keyExists>>

\* Both candidates use M0 as parent and cover the same end L as the seal.
\* Only M1 references K. K and both candidates have already been uploaded.
Candidates == {"M1", "M2"}

Init ==
    /\ head = "M0"
    /\ published = {}
    /\ sealed = FALSE
    /\ gcPhase = 0
    /\ rootView = "M0"
    /\ sealView = FALSE
    /\ keyExists = TRUE

Publish(m) ==
    /\ ~sealed
    /\ m \notin published
    /\ ~CheckParent \/ head = "M0"
    /\ head' = m
    /\ published' = published \cup {m}
    /\ UNCHANGED <<sealed, gcPhase, rootView, sealView, keyExists>>

\* T1 has already closed publication by the time T3's seal is visible.
Seal ==
    /\ ~sealed
    /\ sealed' = TRUE
    /\ UNCHANGED <<head, published, gcPhase, rootView, sealView, keyExists>>

ReadRoots ==
    /\ gcPhase = (IF RootsFirst THEN 0 ELSE 1)
    /\ gcPhase' = gcPhase + 1
    /\ rootView' = head
    /\ UNCHANGED <<head, published, sealed, sealView, keyExists>>

ReadSeal ==
    /\ gcPhase = (IF RootsFirst THEN 1 ELSE 0)
    /\ gcPhase' = gcPhase + 1
    /\ sealView' = sealed
    /\ UNCHANGED <<head, published, sealed, rootView, keyExists>>

Sweep ==
    /\ gcPhase = 2
    /\ gcPhase' = 3
    /\ keyExists' = (keyExists /\ (~sealView \/ rootView = "M1"))
    /\ UNCHANGED <<head, published, sealed, rootView, sealView>>

Next == (\E m \in Candidates : Publish(m)) \/ Seal \/ ReadRoots \/ ReadSeal \/ Sweep
Spec == Init /\ [][Next]_vars

HeadResolves == head = "M1" => keyExists
\* Both candidates have parent M0, so any other published sibling is lost.
PublishedChain == published \subseteq {head}
=============================================================================
