package cs.bilkent.zanza.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * Used for incoming and outgoing tuples
 */
public class PortsToTuples
{

    private static final BinaryOperator<PortsToTuples> PORTS_TO_TUPLES_COMBINER = ( p1, p2 ) -> {
        for ( int port : p2.getPorts() )
        {
            for ( Tuple tuple : p2.getTuples( port ) )
            {
                p1.add( port, tuple );
            }
        }

        return p1;
    };


    public static final Collector<Tuple, PortsToTuples, PortsToTuples> COLLECT_TO_DEFAULT_PORT = Collector.of( PortsToTuples::new,
                                                                                                               PortsToTuples::add,
                                                                                                               PORTS_TO_TUPLES_COMBINER,
                                                                                                               IDENTITY_FINISH );

    public static Collector<Tuple, PortsToTuples, PortsToTuples> collectToPort ( final int targetPort )
    {
        final Supplier<PortsToTuples> supplier = PortsToTuples::new;
        final BiConsumer<PortsToTuples, Tuple> biConsumer = ( p, t ) -> {
            p.add( targetPort, t );
        };
        return Collector.of( supplier, biConsumer, PORTS_TO_TUPLES_COMBINER, IDENTITY_FINISH );
    }

    public static Collector<Tuple, PortsToTuples, PortsToTuples> collectTo ( final PortsToTuples target, final int targetPort )
    {
        final Supplier<PortsToTuples> supplier = PortsToTuples::new;
        final BiConsumer<PortsToTuples, Tuple> biConsumer = ( p, t ) -> {
            p.add( targetPort, t );
        };
        final Function<PortsToTuples, PortsToTuples> finisher = ( p ) -> PORTS_TO_TUPLES_COMBINER.apply( target, p );

        return Collector.of( supplier, biConsumer, PORTS_TO_TUPLES_COMBINER, finisher );
    }

    private final Int2ObjectHashMap<List<Tuple>> tuplesByPort = new Int2ObjectHashMap<>();

    public PortsToTuples ()
    {
    }

    public PortsToTuples ( final Tuple tuple, final Tuple... tuples )
    {
        checkNotNull( tuple, "tuple can't be null" );
        add( tuple );
        for ( Tuple t : tuples )
        {
            add( t );
        }
    }

    public PortsToTuples ( final List<Tuple> tuples )
    {
        checkNotNull( tuples, "tuples can't be null" );
        addAll( tuples );
    }

    public void add ( final Tuple tuple )
    {
        checkNotNull( tuple, "tuple can't be null" );
        add( Port.DEFAULT_PORT_INDEX, tuple );
    }

    public void addAll ( final List<Tuple> tuples )
    {
        checkNotNull( tuples, "tuples can't be null" );
        addAll( Port.DEFAULT_PORT_INDEX, tuples );
    }

    public void add ( final int portIndex, final Tuple tuple )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuple, "tuple can't be null" );
        final List<Tuple> tuples = getOrCreateTuples( portIndex );
        tuples.add( tuple );
    }

    public void addAll ( final int portIndex, final List<Tuple> tuplesToAdd )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuplesToAdd, "tuples can't be null" );
        final List<Tuple> tuples = getOrCreateTuples( portIndex );
        tuples.addAll( tuplesToAdd );
    }

    private List<Tuple> getOrCreateTuples ( final int portIndex )
    {
        return tuplesByPort.computeIfAbsent( portIndex, ignoredPortIndex -> new ArrayList<>() );
    }

    public List<Tuple> getTuples ( final int portIndex )
    {
        final List<Tuple> tuples = tuplesByPort.get( portIndex );
        return tuples != null ? tuples : Collections.emptyList();
    }

    public Tuple getTuple ( final int portIndex, final int tupleIndex )
    {
        final List<Tuple> tuples = tuplesByPort.get( portIndex );
        if ( tuples != null && tuples.size() > tupleIndex )
        {
            return tuples.get( tupleIndex );
        }

        throw new IllegalArgumentException( "no tuple exists for port index " + portIndex + " and tuple index " + tupleIndex );
    }

    public List<Tuple> getTuplesByDefaultPort ()
    {
        return getTuples( Port.DEFAULT_PORT_INDEX );
    }

    public int[] getPorts ()
    {
        final Int2ObjectHashMap<List<Tuple>>.KeySet keys = tuplesByPort.keySet();
        final int[] ports = new int[ keys.size() ];

        final Int2ObjectHashMap<List<Tuple>>.KeyIterator it = keys.iterator();

        for ( int i = 0; i < keys.size(); i++ )
        {
            ports[ i ] = it.nextInt();
        }

        Arrays.sort( ports );

        return ports;
    }

    public int getPortCount ()
    {
        return tuplesByPort.keySet().size();
    }

    @Override
    public String toString ()
    {
        return "PortsToTuples{" +
               "tuplesByPort=" + tuplesByPort +
               '}';
    }
}
