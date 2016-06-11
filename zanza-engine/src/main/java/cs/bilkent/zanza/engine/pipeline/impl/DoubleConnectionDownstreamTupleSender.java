package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class DoubleConnectionDownstreamTupleSender implements DownstreamTupleSender
{

    private final int sourcePortIndex1;

    private final int destinationPortIndex1;

    private final TupleQueueContext tupleQueueContext1;

    private final int sourcePortIndex2;

    private final int destinationPortIndex2;

    private final TupleQueueContext tupleQueueContext2;

    public DoubleConnectionDownstreamTupleSender ( final int sourcePortIndex1,
                                                   final int destinationPortIndex1,
                                                   final TupleQueueContext tupleQueueContext1,
                                                   final int sourcePortIndex2,
                                                   final int destinationPortIndex2,
                                                   final TupleQueueContext tupleQueueContext2 )
    {
        this.sourcePortIndex1 = sourcePortIndex1;
        this.destinationPortIndex1 = destinationPortIndex1;
        this.tupleQueueContext1 = tupleQueueContext1;
        this.sourcePortIndex2 = sourcePortIndex2;
        this.destinationPortIndex2 = destinationPortIndex2;
        this.tupleQueueContext2 = tupleQueueContext2;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        tupleQueueContext1.offer( destinationPortIndex1, tuples.getTuples( sourcePortIndex1 ) );
        tupleQueueContext2.offer( destinationPortIndex2, tuples.getTuples( sourcePortIndex2 ) );
        return null;
    }

}
