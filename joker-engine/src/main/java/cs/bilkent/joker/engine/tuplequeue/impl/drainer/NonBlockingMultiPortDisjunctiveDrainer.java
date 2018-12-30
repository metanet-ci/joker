package cs.bilkent.joker.engine.tuplequeue.impl.drainer;

import java.util.stream.IntStream;

import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;


public class NonBlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    public static MultiPortDrainer newGreedyDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        final MultiPortDrainer drainer = new NonBlockingMultiPortDisjunctiveDrainer( operatorId, inputPortCount, maxBatchSize );
        final int[] inputPorts = IntStream.range( 0, inputPortCount ).toArray();
        final ScheduleWhenTuplesAvailable strategy = scheduleWhenTuplesAvailableOnAny( AT_LEAST, inputPortCount, 1, inputPorts );
        drainer.setParameters( AT_LEAST, inputPorts, strategy.getTupleCounts() );

        return drainer;
    }


    public NonBlockingMultiPortDisjunctiveDrainer ( final String operatorId, final int inputPortCount, final int maxBatchSize )
    {
        super( operatorId, inputPortCount, maxBatchSize );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        boolean satisfied = false;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCountsToCheck[ i ];
            final int tupleCountIndex = i + 1;
            final int tupleCount = tupleCountsToCheck[ tupleCountIndex ];

            if ( tupleCount != NO_TUPLES_AVAILABLE )
            {
                if ( tupleQueues[ portIndex ].size() >= tupleCount )
                {
                    tupleCountsBuffer[ tupleCountIndex ] = tupleCountsToDrain[ tupleCountIndex ];
                    satisfied = true;
                }
                else
                {
                    tupleCountsBuffer[ tupleCountIndex ] = NO_TUPLES_AVAILABLE;
                }
            }
        }

        return satisfied ? tupleCountsBuffer : null;
    }

}
