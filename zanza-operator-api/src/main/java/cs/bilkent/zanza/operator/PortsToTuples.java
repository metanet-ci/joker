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
import cs.bilkent.zanza.flow.Port;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * Contains {@link Tuple} instances mapped to some ports specified by indices.
 * Used for providing input tuples and output tuples of the {@link Operator#process(InvocationContext)} method.
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


    private final Int2ObjectHashMap<List<Tuple>> tuplesByPort;

    public PortsToTuples ()
    {
        tuplesByPort = new Int2ObjectHashMap<>();
    }

    PortsToTuples ( Int2ObjectHashMap<List<Tuple>> tuplesByPort )
    {
        this.tuplesByPort = tuplesByPort;
    }

    /**
     * Initializes a new object and adds the provided tuples into the default port.
     *
     * @param tuple
     *         tuple to add to the default port
     * @param tuples
     *         tuples to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    public PortsToTuples ( final Tuple tuple, final Tuple... tuples )
    {
        this();
        checkNotNull( tuple, "tuple can't be null" );
        add( tuple );
        for ( Tuple t : tuples )
        {
            add( t );
        }
    }

    /**
     * Initializes a new object and adds the provided tuples into the default port.
     *
     * @param tuples
     *         tuples to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    public PortsToTuples ( final List<Tuple> tuples )
    {
        this();
        checkNotNull( tuples, "tuples can't be null" );
        addAll( tuples );
    }

    /**
     * Adds the tuple to the default port.
     *
     * @param tuple
     *         tuple to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    public void add ( final Tuple tuple )
    {
        checkNotNull( tuple, "tuple can't be null" );
        add( Port.DEFAULT_PORT_INDEX, tuple );
    }

    /**
     * Adds the tuples to the default port.
     *
     * @param tuples
     *         tuples to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    public void addAll ( final List<Tuple> tuples )
    {
        checkNotNull( tuples, "tuples can't be null" );
        addAll( Port.DEFAULT_PORT_INDEX, tuples );
    }

    /**
     * Adds the tuple to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuple
     *         tuple to add to the specified port
     */
    public void add ( final int portIndex, final Tuple tuple )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuple, "tuple can't be null" );
        final List<Tuple> tuples = getOrCreateTuples( portIndex );
        tuples.add( tuple );
    }

    /**
     * Adds the tuples to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuplesToAdd
     *         tuples to add to the specified port
     */
    public void addAll ( final int portIndex, final List<Tuple> tuplesToAdd )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuplesToAdd, "tuples can't be null" );
        final List<Tuple> tuples = getOrCreateTuples( portIndex );
        tuples.addAll( tuplesToAdd );
    }

    /**
     * Returns the tuples added to the given port index.
     *
     * @param portIndex
     *         the port index that tuples are added to
     *
     * @return the tuples added to the given port index
     */
    public List<Tuple> getTuples ( final int portIndex )
    {
        final List<Tuple> tuples = tuplesByPort.get( portIndex );
        return tuples != null ? tuples : Collections.emptyList();
    }

    /**
     * Returns the tuple added to the given port index with the given tuple index.
     *
     * @param portIndex
     *         the port index that tuple is added to
     * @param tupleIndex
     *         the order which the tuple is added to the given port index
     *
     * @return the tuple added to the given port index with the given order
     */
    public Tuple getTuple ( final int portIndex, final int tupleIndex )
    {
        final List<Tuple> tuples = tuplesByPort.get( portIndex );
        if ( tuples != null && tuples.size() > tupleIndex )
        {
            return tuples.get( tupleIndex );
        }

        throw new IllegalArgumentException( "no tuple exists for port index " + portIndex + " and tuple index " + tupleIndex );
    }

    /**
     * Returns all the tuples added to the default port.
     *
     * @return all the tuples added to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    public List<Tuple> getTuplesByDefaultPort ()
    {
        return getTuples( Port.DEFAULT_PORT_INDEX );
    }

    /**
     * Returns all the port indices to which some tuples are added.
     *
     * @return all the port indices to which some tuples are added
     */
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

    /**
     * Returns the number of ports that contain tuples.
     *
     * @return the number of ports that contain tuples.
     */
    public int getPortCount ()
    {
        return tuplesByPort.keySet().size();
    }

    /**
     * Returns the number of tuples that are added to the given port.
     *
     * @param portIndex
     *         port index to check number of tuples that are added.
     *
     * @return the number of tuples that are added to the given port.
     */
    public int getTupleCount ( final int portIndex )
    {
        final List<Tuple> tuples = getTuples( portIndex );
        return tuples != null ? tuples.size() : 0;
    }

    private List<Tuple> getOrCreateTuples ( final int portIndex )
    {
        return tuplesByPort.computeIfAbsent( portIndex, ignoredPortIndex -> new ArrayList<>() );
    }

    @Override
    public String toString ()
    {
        return "PortsToTuples{" +
               "tuplesByPort=" + tuplesByPort +
               '}';
    }
}
