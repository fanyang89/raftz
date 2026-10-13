-------------------------- MODULE RaftzReadTrace --------------------------
EXTENDS Integers, Sequences, FiniteSets, Bags, Json, TLC

Steps == TLCEval(ndJsonDeserialize("steps.ndjson"))
Nodes == 1..3
VARIABLES pc, reads, outbox, network, fences
vars == <<pc, reads, outbox, network, fences>>

SeqSet(xs) == {xs[i] : i \in DOMAIN xs}
BagOf(xs) == [m \in SeqSet(xs) |-> Cardinality({i \in DOMAIN xs : xs[i] = m})]
Add(m, bag) == bag (+) SetToBag({m})
Remove(m, bag) == bag (-) SetToBag({m})
FilterBag(bag, predicate(_)) == [m \in {x \in DOMAIN bag : predicate(x)} |-> bag[m]]

EmptyReads == [waiting |-> <<>>, pending |-> <<>>, ready |-> <<>>,
               tracked |-> <<>>, applied |-> 0, completed |-> <<>>, value |-> "", writes |-> <<>>]

Find(xs, ctx) ==
    LET positions == {k \in DOMAIN xs : xs[k].context = ctx}
    IN IF positions = {} THEN 0 ELSE CHOOSE k \in positions : TRUE

CanServe(e, n) ==
    IF n = 0 THEN FALSE
    ELSE IF e.after[n].role # "leader" \/ e.after[n].commit = 0 THEN FALSE
    ELSE e.after[n].log[e.after[n].commit].term = e.after[n].term

Reset(s, e) == [n \in Nodes |->
    IF e.before[n].term # e.after[n].term \/ e.before[n].role # e.after[n].role
    THEN [s[n] EXCEPT !.waiting = <<>>, !.pending = <<>>] ELSE s[n]]

Invoke(s, e) ==
    IF e.kind = "propose" THEN [s EXCEPT
        ![e.node].writes = Append(@, [value |-> e.command, completed |-> 0])]
    ELSE IF e.kind # "read" THEN s
    ELSE [s EXCEPT
        ![e.node].waiting = Append(@, e.context),
        ![e.node].tracked = Append(@, [context |-> e.context, index |-> -1])]

Ack(s, n, ctx, voter) ==
    LET k == Find(s[n].pending, ctx)
    IN IF k = 0 THEN s ELSE
       LET pending == [s[n].pending EXCEPT ![k].acks = @ \cup {voter}]
       IN IF Cardinality(pending[k].acks) < 2
          THEN [s EXCEPT ![n].pending = pending]
          ELSE [s EXCEPT
              ![n].ready = @ \o [i \in 1..k |->
                  [context |-> pending[i].context, index |-> pending[i].index]],
              ![n].pending = SubSeq(pending, k+1, Len(pending))]

ReceiveAck(s, e) ==
    IF e.kind # "receive" THEN s
    ELSE IF ~e.has_packet THEN s
    ELSE IF e.packet.kind # "heartbeat_response" THEN s
    ELSE IF e.after[e.node].role # "leader" \/ e.packet.term # e.after[e.node].term THEN s
    ELSE Ack(s, e.node, e.packet.context, e.packet.source)

BeforeReplay(e) == ReceiveAck(Invoke(Reset(reads, e), e), e)
NewContexts(e) == IF CanServe(e, e.node) THEN BeforeReplay(e)[e.node].waiting ELSE <<>>

Replay(s, e) ==
    IF ~CanServe(e, e.node) THEN s
    ELSE [s EXCEPT
        ![e.node].pending = @ \o [k \in DOMAIN s[e.node].waiting |->
            [context |-> s[e.node].waiting[k], index |-> e.after[e.node].commit, acks |-> {e.node}]],
        ![e.node].waiting = <<>>]

Mark(s, n) == [s EXCEPT
    ![n].tracked = [k \in DOMAIN s[n].tracked |->
        LET t == s[n].tracked[k]
            r == Find(s[n].ready, t.context)
        IN IF r = 0 THEN t ELSE
           [t EXCEPT !.index = IF @ > s[n].ready[r].index THEN @ ELSE s[n].ready[r].index]],
    ![n].ready = <<>>]

Eligible(t, applied) == t.index >= 0 /\ t.index <= applied
Complete(s, n) ==
    LET completed == SelectSeq(s[n].tracked, LAMBDA t : Eligible(t, s[n].applied))
    IN [s EXCEPT
        ![n].completed = @ \o [k \in DOMAIN completed |->
            [context |-> completed[k].context, index |-> s[n].applied, value |-> s[n].value]],
        ![n].tracked = SelectSeq(@, LAMBDA t : ~Eligible(t, s[n].applied))]

Apply(s, e) ==
    LET n == e.node
        indices == {k \in (s[n].applied+1)..e.target : e.after[n].log[k].data # ""}
        commands == {e.after[n].log[k].data : k \in indices}
        value == IF indices = {} THEN s[n].value
                 ELSE e.after[n].log[CHOOSE k \in indices : \A j \in indices : k >= j].data
    IN [s EXCEPT
        ![n].applied = e.target,
        ![n].value = value,
        ![n].writes = [k \in DOMAIN s[n].writes |->
            IF s[n].writes[k].value \in commands
            THEN [s[n].writes[k] EXCEPT !.completed = 1] ELSE s[n].writes[k]]]

ReadStep(e) ==
    LET s == Replay(BeforeReplay(e), e)
    IN CASE e.kind = "read_ready" -> Mark(s, e.node)
         [] e.kind = "apply" -> Apply(s, e)
         [] e.kind = "complete_reads" -> Complete(s, e.node)
         [] OTHER -> s

HeartbeatPackets(n, term, contexts) == SetToBag(
    {[kind |-> "heartbeat", source |-> n, dest |-> v, term |-> term, context |-> ctx]
     : v \in Nodes \ {n}, ctx \in SeqSet(contexts)})

NewPackets(e) ==
    LET sent == IF e.node = 0 THEN EmptyBag
                ELSE HeartbeatPackets(e.node, e.after[e.node].term, NewContexts(e))
        beat == IF e.kind # "beat" THEN EmptyBag
                ELSE IF Len(reads[e.node].pending) = 0 THEN EmptyBag
                ELSE HeartbeatPackets(e.node, e.after[e.node].term,
                    <<reads[e.node].pending[Len(reads[e.node].pending)].context>>)
        reply == IF e.kind # "receive" THEN EmptyBag
                 ELSE IF ~e.has_packet THEN EmptyBag
                 ELSE IF e.packet.kind # "heartbeat" THEN EmptyBag
                 ELSE IF e.packet.term < e.before[e.node].term THEN EmptyBag
                 ELSE SetToBag({[kind |-> "heartbeat_response", source |-> e.node,
                     dest |-> e.packet.source, term |-> e.after[e.node].term, context |-> e.packet.context]})
    IN sent (+) beat (+) reply

Released(e) == IF e.kind = "release"
              THEN FilterBag(outbox, LAMBDA m : m.source = e.node) ELSE EmptyBag

NetworkStep(e) ==
    LET sent == network (+) Released(e)
    IN IF ~e.has_packet THEN sent
       ELSE CASE e.kind \in {"receive", "drop"} -> Remove(e.packet, sent)
              [] e.kind = "duplicate" -> Add(e.packet, sent)
              [] OTHER -> sent

EnabledEvent(e) ==
    /\ IF ~e.has_packet THEN TRUE ELSE e.packet \in DOMAIN network
    /\ IF e.kind = "read"
       THEN e.after[e.node].role = "leader" /\ e.context \notin DOMAIN fences ELSE TRUE
    /\ IF e.kind = "propose"
       THEN \A n \in Nodes : \A w \in SeqSet(reads[n].writes) : w.value # e.command ELSE TRUE
    /\ IF e.kind = "apply"
       THEN reads[e.node].applied < e.target
            /\ e.target <= e.after[e.node].commit
            /\ e.target <= e.after[e.node].durable_commit
       ELSE TRUE

Matches(s, expected) == \A n \in Nodes :
    /\ s[n].waiting = expected[n].waiting
    /\ s[n].pending = [k \in DOMAIN expected[n].pending |->
        [expected[n].pending[k] EXCEPT !.acks = SeqSet(@)]]
    /\ s[n].ready = expected[n].ready
    /\ s[n].tracked = expected[n].tracked
    /\ s[n].applied = expected[n].applied
    /\ s[n].value = expected[n].value
    /\ s[n].writes = expected[n].writes
    /\ BagOf(s[n].completed) = BagOf(expected[n].completed)

TraceInit ==
    /\ pc = 1
    /\ reads = [n \in Nodes |-> EmptyReads]
    /\ outbox = EmptyBag
    /\ network = EmptyBag
    /\ fences = [ctx \in {} |-> 0]

TraceNext ==
    \/ /\ pc <= Len(Steps)
       /\ LET e == Steps[pc]
          IN /\ EnabledEvent(e)
             /\ reads' = ReadStep(e)
             /\ outbox' = (outbox (-) Released(e)) (+) NewPackets(e)
             /\ network' = NetworkStep(e)
             /\ fences' = IF e.kind = "read"
                          THEN LET commits == {e.before[n].commit : n \in Nodes}
                                   floor == CHOOSE c \in commits : \A d \in commits : c >= d
                               IN [ctx \in DOMAIN fences \cup {e.context} |->
                                   IF ctx = e.context THEN floor ELSE fences[ctx]]
                          ELSE fences
             /\ Matches(reads', e.reads)
             /\ outbox' = BagOf(e.pending)
             /\ network' = BagOf(e.network)
       /\ pc' = pc + 1
       /\ pc' = Len(Steps)+1 => PrintT(<<"RAFTZ_TRACE_COMPLETE", Len(Steps)>>)
    \/ /\ pc = Len(Steps)+1
       /\ UNCHANGED vars

ReadBarrierInv == \A n \in Nodes :
    \A r \in SeqSet(reads[n].ready) \cup SeqSet(reads[n].completed)
             \cup SeqSet(SelectSeq(reads[n].tracked, LAMBDA t : t.index >= 0)) :
        r.context \in DOMAIN fences /\ r.index >= fences[r.context]

TraceSpec == TraceInit /\ [][TraceNext]_vars
=============================================================================
