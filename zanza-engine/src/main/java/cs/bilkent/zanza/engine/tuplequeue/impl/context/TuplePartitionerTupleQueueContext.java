package cs.bilkent.zanza.engine.tuplequeue.impl.context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainer;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.PortToTupleCount;

public class TuplePartitionerTupleQueueContext implements TupleQueueContext
{

    private final PartitionedTupleQueueContext internal;

    private final Function<Tuple, Object> partitionKeyExtractor;

    public TuplePartitionerTupleQueueContext ( final PartitionedTupleQueueContext internal,
                                               final Function<Tuple, Object> partitionKeyExtractor )
    {
        this.internal = internal;
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    @Override
    public String getOperatorId ()
    {
        return null;
    }

    @Override
    public void add ( final PortsToTuples input )
    {
        final Map<Object, PortsToTuples> tuplesByKey = new HashMap<>();

        for ( PortToTuples port : input.getPortToTuplesList() )
        {
            for ( Tuple tuple : port.getTuples() )
            {
                final Object key = partitionKeyExtractor.apply( tuple );
                final PortsToTuples tuples = tuplesByKey.computeIfAbsent( key, k -> new PortsToTuples() );
                tuples.add( port.getPortIndex(), tuple );
            }
        }

        tuplesByKey.values().forEach( internal::add );
    }

    @Override
    public List<PortToTupleCount> tryAdd ( final PortsToTuples input, final long timeoutInMillis )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void drain ( final TupleQueueDrainer drainer )
    {
        internal.drain( drainer );
    }

    @Override
    public void clear ()
    {
        internal.clear();
    }

}
