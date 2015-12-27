package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueuesConsumer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;

public class PartitionedTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );


    private final String operatorId;

    private final int inputPortCount;

    private final List<String> partitionFieldNames;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final ConcurrentMap<Object, TupleQueue[]> tupleQueuesByPartitionKeys = new ConcurrentHashMap<>();

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount, final List<String> partitionFieldNames,
                                          final Supplier<TupleQueue> tupleQueueSupplier )
    {
        this.operatorId = operatorId;
        this.inputPortCount = inputPortCount;
        this.partitionFieldNames = partitionFieldNames;
        this.tupleQueuesConstructor = ( partitionKey ) -> {
            final TupleQueue[] tupleQueues = new TupleQueue[ inputPortCount ];
            for ( int i = 0; i < inputPortCount; i++ )
            {
                tupleQueues[ i ] = tupleQueueSupplier.get();
            }
            return tupleQueues;
        };
    }

    @Override
    public String getOperatorId ()
    {
        return operatorId;
    }

    @Override
    public void add ( final PortsToTuples portsToTuples )
    {
        for ( int portIndex : portsToTuples.getPorts() )
        {
            checkArgument( portIndex >= this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port index: " + portIndex );
            for ( Tuple tuple : portsToTuples.getTuples( portIndex ) )
            {
                final TupleQueue[] tupleQueues = tupleQueuesByPartitionKeys.computeIfAbsent                         ( tuple.getValues( partitionFieldNames ),
                                                                                             tupleQueuesConstructor );
                tupleQueues[ portIndex ].offerTuple( tuple );
            }
        }

    }

    @Override
    public void drain ( TupleQueuesConsumer tupleQueuesConsumer )
    {
        for ( TupleQueue[] tupleQueues : tupleQueuesByPartitionKeys.values() )
        {
            tupleQueuesConsumer.accept( tupleQueues );
            if ( tupleQueuesConsumer.getPortsToTuples() != null )
            {
                return;
            }
        }
    }

    @Override
    public void clear ()
    {
        LOGGER.info( "Clearing partitioned tuple queues of operator: {}", operatorId );

        for ( TupleQueue[] tupleQueues : tupleQueuesByPartitionKeys.values() )
        {
            for ( TupleQueue tupleQueue : tupleQueues )
            {
                tupleQueue.clear();
            }
        }

        tupleQueuesByPartitionKeys.clear();
    }

}
