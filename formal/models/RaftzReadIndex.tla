--------------------------- MODULE RaftzReadIndex ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, MaxTerm, MaxIndex, MaxReads,
          RequireCurrentTerm, RequireQuorum, RequireApplied

VARIABLES epoch, term, leaders, committed, knownCommit, currentTermCommitted,
          applied, issued, reads

vars == <<epoch, term, leaders, committed, knownCommit, currentTermCommitted,
          applied, issued, reads>>

Quorums == {q \in SUBSET Nodes : 2 * Cardinality(q) > Cardinality(Nodes)}
ReadIds == 1..MaxReads
Phases == {"unused", "waiting", "collecting", "ready", "done"}

Init ==
    /\ epoch = 0
    /\ term = [n \in Nodes |-> 0]
    /\ leaders = {}
    /\ committed = 0
    /\ knownCommit = [n \in Nodes |-> 0]
    /\ currentTermCommitted = [n \in Nodes |-> FALSE]
    /\ applied = [n \in Nodes |-> 0]
    /\ issued = 0
    /\ reads = [r \in ReadIds |->
        [owner |-> 0, requestTerm |-> 0, floor |-> 0, index |-> 0,
         phase |-> "unused", acks |-> {}, responses |-> {}, returned |-> 0]]

Active(r) ==
    /\ reads[r].owner \in leaders
    /\ reads[r].requestTerm = term[reads[r].owner]

Elect(n, q) ==
    /\ epoch < MaxTerm
    /\ n \in q
    /\ epoch' = epoch + 1
    /\ term' = [v \in Nodes |-> IF v \in q THEN epoch + 1 ELSE term[v]]
    /\ leaders' = (leaders \ q) \cup {n}
    /\ currentTermCommitted' =
        [v \in Nodes |-> IF v \in q THEN FALSE ELSE currentTermCommitted[v]]
    /\ UNCHANGED <<committed, knownCommit, applied, issued, reads>>

Commit(n, q) ==
    /\ n \in leaders
    /\ n \in q
    /\ \A v \in q : term[v] <= term[n]
    /\ committed < MaxIndex
    /\ committed' = committed + 1
    /\ knownCommit' = [knownCommit EXCEPT ![n] = committed + 1]
    /\ term' = [v \in Nodes |-> IF v \in q THEN term[n] ELSE term[v]]
    /\ leaders' = (leaders \ q) \cup {n}
    /\ currentTermCommitted' = [v \in Nodes |->
        IF v = n THEN TRUE ELSE IF v \in q THEN FALSE ELSE currentTermCommitted[v]]
    /\ UNCHANGED <<epoch, applied, issued, reads>>

Invoke(n) ==
    /\ n \in leaders
    /\ issued < MaxReads
    /\ issued' = issued + 1
    /\ reads' = [reads EXCEPT ![issued + 1] =
        [owner |-> n, requestTerm |-> term[n], floor |-> committed, index |-> 0,
         phase |-> "waiting", acks |-> {}, responses |-> {}, returned |-> 0]]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, applied>>

Register(r) ==
    /\ reads[r].phase = "waiting"
    /\ Active(r)
    /\ ~RequireCurrentTerm \/ currentTermCommitted[reads[r].owner]
    /\ \A s \in 1..(r - 1) :
        ~(reads[s].phase = "waiting" /\ reads[s].owner = reads[r].owner
          /\ reads[s].requestTerm = reads[r].requestTerm)
    /\ reads' = [reads EXCEPT
        ![r].phase = "collecting",
        ![r].index = knownCommit[reads[r].owner],
        ![r].acks = {reads[r].owner}]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, applied, issued>>

HeartbeatReply(r, v) ==
    /\ reads[r].phase = "collecting"
    /\ Active(r)
    /\ v # reads[r].owner
    /\ term[v] <= reads[r].requestTerm
    /\ v \notin reads[r].responses
    /\ reads' = [reads EXCEPT ![r].responses = @ \cup {v}]
    /\ term' = [term EXCEPT ![v] = reads[r].requestTerm]
    /\ leaders' = leaders \ {v}
    /\ currentTermCommitted' = [currentTermCommitted EXCEPT ![v] = FALSE]
    /\ UNCHANGED <<epoch, committed, knownCommit, applied, issued>>

DeliverReply(r, v) ==
    /\ reads[r].phase = "collecting"
    /\ Active(r)
    /\ v \in reads[r].responses \ reads[r].acks
    /\ reads' = [reads EXCEPT ![r].acks = @ \cup {v}]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, applied, issued>>

Release(r) ==
    /\ reads[r].phase = "collecting"
    /\ Active(r)
    /\ ~RequireQuorum \/ reads[r].acks \in Quorums
    /\ reads' = [s \in ReadIds |->
        IF s <= r /\ reads[s].phase = "collecting"
           /\ reads[s].owner = reads[r].owner
           /\ reads[s].requestTerm = reads[r].requestTerm
        THEN [reads[s] EXCEPT !.phase = "ready"] ELSE reads[s]]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, applied, issued>>

Apply(n) ==
    /\ applied[n] < knownCommit[n]
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, issued, reads>>

Complete(r) ==
    /\ reads[r].phase = "ready"
    /\ ~RequireApplied \/ applied[reads[r].owner] >= reads[r].index
    /\ reads' = [reads EXCEPT ![r].phase = "done",
                             ![r].returned = applied[reads[r].owner]]
    /\ UNCHANGED <<epoch, term, leaders, committed, knownCommit,
                   currentTermCommitted, applied, issued>>

Next ==
    \/ \E n \in Nodes, q \in Quorums : Elect(n, q) \/ Commit(n, q)
    \/ \E n \in Nodes : Invoke(n) \/ Apply(n)
    \/ \E r \in ReadIds : Register(r) \/ Release(r) \/ Complete(r)
    \/ \E r \in ReadIds, v \in Nodes : HeartbeatReply(r, v) \/ DeliverReply(r, v)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ epoch \in 0..MaxTerm
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ leaders \subseteq Nodes
    /\ committed \in 0..MaxIndex
    /\ knownCommit \in [Nodes -> 0..MaxIndex]
    /\ currentTermCommitted \in [Nodes -> BOOLEAN]
    /\ applied \in [Nodes -> 0..MaxIndex]
    /\ issued \in 0..MaxReads
    /\ reads \in [ReadIds ->
        [owner : Nodes \cup {0}, requestTerm : 0..MaxTerm,
         floor : 0..MaxIndex, index : 0..MaxIndex, phase : Phases,
         acks : SUBSET Nodes, responses : SUBSET Nodes, returned : 0..MaxIndex]]

CommitBounds == \A n \in Nodes : applied[n] <= knownCommit[n] /\ knownCommit[n] <= committed

ReadIndexSafety == \A r \in ReadIds :
    reads[r].phase \in {"ready", "done"} => reads[r].index >= reads[r].floor

AppliedBarrier == \A r \in ReadIds :
    reads[r].phase = "done" => reads[r].returned >= reads[r].index

NoStaleRead == \A r \in ReadIds :
    reads[r].phase = "done" => reads[r].returned >= reads[r].floor

NoCompletedRead == \A r \in ReadIds : reads[r].phase # "done"

NoBatchedRelease == ~\E r, s \in ReadIds :
    /\ r < s
    /\ reads[r].phase = "ready" /\ reads[s].phase = "ready"
    /\ reads[r].owner = reads[s].owner
    /\ reads[r].requestTerm = reads[s].requestTerm
    /\ reads[r].acks \notin Quorums

=============================================================================
