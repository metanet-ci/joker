package cs.bilkent.zanza.engine.tuplequeue;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;

public interface TupleQueue
{

    void offerTuple ( Tuple tuple );

    List<Tuple> pollTuples ( int count );

    List<Tuple> pollTuplesAtLeast ( int count );

    int size ();

    void clear ();

}
