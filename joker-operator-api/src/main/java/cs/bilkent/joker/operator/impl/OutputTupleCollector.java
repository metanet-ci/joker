package cs.bilkent.joker.operator.impl;

import cs.bilkent.joker.operator.Tuple;

public interface OutputTupleCollector
{

    void add ( Tuple tuple );

    void add ( int portIndex, Tuple tuple );

    TuplesImpl getOutputTuples ();

    void clear ();

}
