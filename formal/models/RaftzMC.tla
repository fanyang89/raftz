------------------------------- MODULE RaftzMC -------------------------------
EXTENDS MCetcdraft

CONSTANT MessageLimit
ASSUME MessageLimit \in Nat

MessageBound ==
    BagCardinality(messages) + BagCardinality(pendingMessages) <= MessageLimit

=============================================================================
