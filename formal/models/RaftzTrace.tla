---------------------------- MODULE RaftzTrace ----------------------------
\* Copyright 2024 The etcd Authors
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.
\*
\*
\* This is the formal specification for the Raft consensus algorithm.
\*
\* Copyright 2014 Diego Ongaro, 2015 Brandon Amos and Huanchen Zhang,
\* 2016 Daniel Ricketts, 2021 George Pîrlea and Darius Foo.
\*
\* This work is licensed under the Creative Commons Attribution-4.0
\* International License https://creativecommons.org/licenses/by/4.0/

\* Modified for raftz: concrete trace checkpoints and local atomic-storage adapter.
EXTENDS etcdraft, Json, TLC

\* Local observation adapter; upstream etcdraft is unchanged. See trace.md.
Steps == TLCEval(ndJsonDeserialize("steps.ndjson"))
VARIABLE pc, micro, durableLog
traceVars == <<vars, pc, micro, durableLog>>
Instruction == Steps[pc]

BagOf(xs) == FoldSeq(LAMBDA m, b: WithMessage(m, b), EmptyBag, xs)

Matches(s) ==
    /\ \A i \in Server:
        /\ currentTerm[i] = s.states[i].term
        /\ votedFor[i] = s.states[i].vote
        /\ state[i] = s.states[i].role
        /\ commitIndex[i] = s.states[i].commit
        /\ log[i] = s.states[i].log
        /\ durableState[i].currentTerm = s.states[i].durable_term
        /\ durableState[i].votedFor = s.states[i].durable_vote
        /\ durableState[i].commitIndex = s.states[i].durable_commit
        /\ durableState[i].log = Len(s.states[i].durable_log)
        /\ durableLog[i] = s.states[i].durable_log
        /\ config[i] = [jointConfig |-> <<Server, {}>>, learners |-> {}]
    /\ messages = BagOf(s.network)
    /\ pendingMessages = BagOf(s.pending)

SelfVote(i) ==
    /\ state[i] = Candidate
    /\ votedFor[i] = i
    /\ votesResponded' = [votesResponded EXCEPT ![i] = @ \cup {i}]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = @ \cup {i}]
    /\ UNCHANGED <<messageVars, serverVars, leaderVars, logVars, configVars, durableState>>

SelfAck(i) ==
    /\ state[i] = Leader
    /\ durableState[i].currentTerm = currentTerm[i]
    /\ durableLog[i] = log[i]
    /\ matchIndex' = [matchIndex EXCEPT ![i][i] = Len(durableLog[i])]
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, pendingConfChangeIndex, logVars, configVars, durableState>>

Persist(i) ==
    /\ PersistState(i)
    /\ UNCHANGED <<messageVars, serverVars, candidateVars, leaderVars, logVars, configVars>>

Release(i) ==
    /\ durableState[i].currentTerm = currentTerm[i]
    /\ durableState[i].votedFor = votedFor[i]
    /\ durableState[i].commitIndex = commitIndex[i]
    /\ durableLog[i] = log[i]
    /\ SendPendingMessages(i)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, configVars, durableState>>

SendRequest(i, m) ==
    /\ CASE m.mtype = RequestVoteRequest -> RequestVote(i, m.mdest)
         [] m.mtype = AppendEntriesRequest ->
             IF m.msubtype = "heartbeat" THEN Heartbeat(i, m.mdest)
             ELSE AppendEntries(i, m.mdest, <<m.mprevLogIndex+1, m.mprevLogIndex+Len(m.mentries)+1>>)
    /\ pendingMessages' = WithMessage(m, pendingMessages)

TraceInit == /\ Init
             /\ pc = 1
             /\ micro = 0
             /\ durableLog = [i \in Server |-> <<>>]

Action(s) ==
    CASE s.op = "check" -> /\ Matches(s.expected) /\ UNCHANGED vars
      [] s.op = "timeout" -> Timeout(s.node)
      [] s.op = "selfVote" -> SelfVote(s.node)
      [] s.op = "leader" -> BecomeLeader(s.node)
      [] s.op = "client" -> ClientRequest(s.node, s.value)
      [] s.op = "commit" -> AdvanceCommitIndex(s.node)
      [] s.op = "send" -> SendRequest(s.node, s.msg)
      [] s.op = "receive" -> /\ s.msg \in DOMAIN messages /\ Receive(s.msg) /\ vars' # vars
      [] s.op = "persist" -> Persist(s.node)
      [] s.op = "release" -> Release(s.node)
      [] s.op = "selfAck" -> SelfAck(s.node)
      [] s.op = "restart" -> Restart(s.node)
      [] s.op = "drop" -> /\ s.msg \in DOMAIN messages /\ DropMessage(s.msg)
      [] s.op = "duplicate" -> DuplicateMessage(s.msg)

TraceNext ==
    \/ /\ pc <= Len(Steps)
       /\ micro < Instruction.bound
       /\ Action(Instruction)
       /\ IF Instruction.op = "persist"
             THEN durableLog' = [durableLog EXCEPT ![Instruction.node] = log[Instruction.node]]
             ELSE UNCHANGED durableLog
       /\ IF Instruction.op = "receive" /\ messages' = messages
             THEN /\ pc' = pc /\ micro' = micro + 1
             ELSE /\ pc' = pc + 1 /\ micro' = 0
       /\ pc' = Len(Steps)+1 => PrintT(<<"RAFTZ_TRACE_COMPLETE", Len(Steps)>>)
    \/ /\ pc = Len(Steps)+1
       /\ UNCHANGED traceVars

TraceSpec == TraceInit /\ [][TraceNext]_traceVars
=============================================================================
