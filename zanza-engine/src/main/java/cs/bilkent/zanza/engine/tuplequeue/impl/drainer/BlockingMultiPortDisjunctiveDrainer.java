package cs.bilkent.zanza.engine.tuplequeue.impl.drainer;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount;


public class BlockingMultiPortDisjunctiveDrainer extends MultiPortDrainer
{

    private final long timeoutInMillisPerQueue;

    private final int[] tupleCountsBuffer;

    public BlockingMultiPortDisjunctiveDrainer ( final int inputPortCount, final int maxBatchSize, final long timeoutInMillis )
    {
        super( inputPortCount, maxBatchSize );
        this.timeoutInMillisPerQueue = inputPortCount > 0 ? (long) Math.ceil( ( (double) timeoutInMillis ) / inputPortCount ) : 0;
        this.tupleCountsBuffer = new int[ inputPortCount * 2 ];
    }

    public void setParameters ( final TupleAvailabilityByCount tupleAvailabilityByCount, final int[] inputPorts, final int[] tupleCounts )
    {
        super.setParameters( tupleAvailabilityByCount, inputPorts, tupleCounts );
        System.arraycopy( this.tupleCounts, 0, tupleCountsBuffer, 0, tupleCountsBuffer.length );
    }

    @Override
    protected int[] checkQueueSizes ( final TupleQueue[] tupleQueues )
    {
        boolean notFound = true;
        for ( int i = 0; i < limit; i += 2 )
        {
            final int portIndex = tupleCounts[ i ];
            final int tupleCountIndex = i + 1;
            final int tupleCount = tupleCounts[ tupleCountIndex ];
            final TupleQueue tupleQueue = tupleQueues[ portIndex ];
            if ( notFound )
            {
                if ( tupleQueue.awaitMinimumSize( tupleCount, timeoutInMillisPerQueue ) )
                {
                    tupleCountsBuffer[ tupleCountIndex ] = tupleCount;
                    notFound = false;
                }
                else
                {
                    tupleCountsBuffer[ tupleCountIndex ] = NO_TUPLES_AVAILABLE;
                }
            }
            else if ( tupleQueue.size() >= tupleCount )
            {
                tupleCountsBuffer[ tupleCountIndex ] = tupleCount;
            }
            else
            {
                tupleCountsBuffer[ tupleCountIndex ] = NO_TUPLES_AVAILABLE;
            }
        }

        return tupleCountsBuffer;
    }

}
