package cs.bilkent.joker.operator.impl;

import cs.bilkent.joker.operator.Tuple;

public interface OutputCollector
{

    void add ( Tuple tuple );

    void add ( int portIndex, Tuple tuple );

    void recordInvocationLatency ( String operatorId, long latency );

    TuplesImpl getOutputTuples ();

    void clear ();

}
