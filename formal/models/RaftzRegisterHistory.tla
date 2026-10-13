------------------------ MODULE RaftzRegisterHistory ------------------------
EXTENDS Integers, Sequences, FiniteSets, Json, TLC

Ops == TLCEval(ndJsonDeserialize("history.ndjson"))
Ids == DOMAIN Ops
Completed == {i \in Ids : Ops[i].returned >= 0}
Candidates == Completed \cup {i \in Ids : Ops[i].kind = "write"}
Predecessors(i) == {j \in Completed : Ops[j].returned < Ops[i].invoked}

VARIABLES done, value
vars == <<done, value>>

Init == /\ done = {} /\ value = ""

Linearize(i) ==
    /\ i \in Candidates \ done
    /\ Predecessors(i) \subseteq done
    /\ Ops[i].kind = "write" \/ Ops[i].result = value
    /\ value' = IF Ops[i].kind = "write" THEN Ops[i].value ELSE value
    /\ done' = done \cup {i}
    /\ Completed \subseteq done' => PrintT(<<"RAFTZ_HISTORY_COMPLETE", Cardinality(Completed)>>)

Next == \E i \in Candidates : Linearize(i)
Spec == Init /\ [][Next]_vars

HistoryTypeInv ==
    /\ done \subseteq Candidates
    /\ value \in {""} \cup {Ops[i].value : i \in {j \in Ids : Ops[j].kind = "write"}}

NoLinearization == ~(Completed \subseteq done)
=============================================================================
