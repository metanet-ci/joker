package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.Tuple;


public class PartitionedTupleQueueContext extends AbstractTupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );


    private final Function<Tuple, Object> partitionKeyExtractor;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    // TODO IS CONCURRENT MAP NEEDED? MAY NOT BE ON SOME CASES
    private final ConcurrentMap<Object, TupleQueue[]> tupleQueuesByPartitionKeys = new ConcurrentHashMap<>();

    public PartitionedTupleQueueContext ( final String operatorId,
                                          final int inputPortCount,
                                          final Function<Tuple, Object> partitionKeyExtractor,
                                          final Supplier<TupleQueue> tupleQueueSupplier )
    {
        super( operatorId, inputPortCount );
        this.partitionKeyExtractor = partitionKeyExtractor;
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

    protected TupleQueue[] getTupleQueues ( final PortsToTuples input )
    {
        if ( input == null )
        {
            return null;
        }

        final Object partitionKey = getPartitionKey( input );
        if ( partitionKey == null )
        {
            return null;
        }

        return tupleQueuesByPartitionKeys.computeIfAbsent( partitionKey, tupleQueuesConstructor );
    }

    @Override
    public void drain ( TupleQueueDrainer drainer )
    {
        // TODO RANDOMIZATION IS NEEDED HERE !!!
        for ( Map.Entry<Object, TupleQueue[]> entry : tupleQueuesByPartitionKeys.entrySet() )
        {
            drainer.drain( entry.getKey(), entry.getValue() );
            if ( drainer.getResult() != null )
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

    private Object getPartitionKey ( final PortsToTuples portsToTuples )
    {
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();

        if ( portToTuplesList.isEmpty() )
        {
            return null;
        }

        final List<Tuple> tuples = portToTuplesList.get( 0 ).getTuples();
        if ( tuples.isEmpty() )
        {
            return null;
        }

        return partitionKeyExtractor.apply( tuples.get( 0 ) );
    }

}
