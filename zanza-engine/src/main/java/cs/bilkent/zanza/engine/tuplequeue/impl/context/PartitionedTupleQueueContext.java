package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.ArrayList;
import java.util.Collections;
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
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;


public class PartitionedTupleQueueContext implements TupleQueueContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionedTupleQueueContext.class );


    private final String operatorId;

    private final int inputPortCount;

    private final List<String> partitionFieldNames;

    private final Function<Object, TupleQueue[]> tupleQueuesConstructor;

    private final ConcurrentMap<Object, TupleQueue[]> tupleQueuesByPartitionKeys = new ConcurrentHashMap<>();

    public PartitionedTupleQueueContext ( final String operatorId, final int inputPortCount, final List<String> partitionFieldNames,
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
        final List<Object> partitionFieldValues = getPartitionFieldValues( portsToTuples );
        if ( partitionFieldValues == null )
        {
            return;
        }

        final TupleQueue[] tupleQueues = tupleQueuesByPartitionKeys.computeIfAbsent( partitionFieldValues, tupleQueuesConstructor );

        for ( PortToTuples portToTuples : portsToTuples.getPortToTuplesList() )
        {
            final int portIndex = portToTuples.getPortIndex();
            checkArgument( portIndex < this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            tupleQueues[ portIndex ].offerTuples( portToTuples.getTuples() );
        }
    }

    @Override
    public List<PortToTupleCount> tryAdd ( final PortsToTuples portsToTuples, final long timeoutInMillis )
    {
        final List<Object> partitionFieldValues = getPartitionFieldValues( portsToTuples );
        if ( partitionFieldValues == null )
        {
            return Collections.emptyList();
        }

        final List<PortToTupleCount> counts = new ArrayList<>();
        final TupleQueue[] tupleQueues = tupleQueuesByPartitionKeys.computeIfAbsent( partitionFieldValues, tupleQueuesConstructor );

        for ( PortToTuples portToTuples : portsToTuples.getPortToTuplesList() )
        {
            final int portIndex = portToTuples.getPortIndex();
            checkArgument( portIndex >= this.inputPortCount,
                           "Tuples have invalid input port index for operator: " + operatorId + " input port count: " + inputPortCount
                           + " input port " + "index: " + portIndex );

            final int count = tupleQueues[ portIndex ].tryOfferTuples( portToTuples.getTuples(), timeoutInMillis );
            counts.add( new PortToTupleCount( portIndex, count ) );
        }

        return counts;
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

    private List<Object> getPartitionFieldValues ( final PortsToTuples portsToTuples )
    {
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();

        if ( portToTuplesList.isEmpty() )
        {
            return null;
        }

        final List<Tuple> tuples = portToTuplesList.get( 0 ).getTuples();
        return tuples.isEmpty() ? null : tuples.get( 0 ).getValues( partitionFieldNames );
    }

}
