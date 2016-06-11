package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class QuadrupleConnectionDownstreamTupleSender implements DownstreamTupleSender
{

    private final int sourcePortIndex1;

    private final int destinationPortIndex1;

    private final TupleQueueContext tupleQueueContext1;

    private final int sourcePortIndex2;

    private final int destinationPortIndex2;

    private final TupleQueueContext tupleQueueContext2;

    private final int sourcePortIndex3;

    private final int destinationPortIndex3;

    private final TupleQueueContext tupleQueueContext3;

    private final int sourcePortIndex4;

    private final int destinationPortIndex4;

    private final TupleQueueContext tupleQueueContext4;

    public QuadrupleConnectionDownstreamTupleSender ( final int sourcePortIndex1,
                                                      final int destinationPortIndex1,
                                                      final TupleQueueContext tupleQueueContext1,
                                                      final int sourcePortIndex2,
                                                      final int destinationPortIndex2,
                                                      final TupleQueueContext tupleQueueContext2,
                                                      final int sourcePortIndex3,
                                                      final int destinationPortIndex3,
                                                      final TupleQueueContext tupleQueueContext3,
                                                      final int sourcePortIndex4,
                                                      final int destinationPortIndex4,
                                                      final TupleQueueContext tupleQueueContext4 )
    {
        this.sourcePortIndex1 = sourcePortIndex1;
        this.destinationPortIndex1 = destinationPortIndex1;
        this.tupleQueueContext1 = tupleQueueContext1;
        this.sourcePortIndex2 = sourcePortIndex2;
        this.destinationPortIndex2 = destinationPortIndex2;
        this.tupleQueueContext2 = tupleQueueContext2;
        this.sourcePortIndex3 = sourcePortIndex3;
        this.destinationPortIndex3 = destinationPortIndex3;
        this.tupleQueueContext3 = tupleQueueContext3;
        this.sourcePortIndex4 = sourcePortIndex4;
        this.destinationPortIndex4 = destinationPortIndex4;
        this.tupleQueueContext4 = tupleQueueContext4;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        tupleQueueContext1.offer( destinationPortIndex1, tuples.getTuples( sourcePortIndex1 ) );
        tupleQueueContext2.offer( destinationPortIndex2, tuples.getTuples( sourcePortIndex2 ) );
        tupleQueueContext3.offer( destinationPortIndex3, tuples.getTuples( sourcePortIndex3 ) );
        tupleQueueContext4.offer( destinationPortIndex4, tuples.getTuples( sourcePortIndex4 ) );
        return null;
    }

}
