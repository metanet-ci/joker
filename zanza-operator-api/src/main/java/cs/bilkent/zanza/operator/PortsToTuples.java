package cs.bilkent.zanza.operator;


import java.util.ArrayList;
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
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;


/**
 * Contains {@link Tuple} instances mapped to some ports specified by indices.
 * Used for providing input tuples and output tuples of the {@link Operator#invoke(InvocationContext)} method.
 */
public class PortsToTuples
{

    /**
     * Holds list of tuples added to a port index
     */
    public static class PortToTuples
    {

        private final int portIndex;

        private final List<Tuple> tuples;

        PortToTuples ( final int portIndex, final List<Tuple> tuples )
        {
            this.portIndex = portIndex;
            this.tuples = tuples;
        }

        public int getPortIndex ()
        {
            return portIndex;
        }

        public List<Tuple> getTuples ()
        {
            return tuples;
        }

        @Override
        public boolean equals ( final Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            final PortToTuples that = (PortToTuples) o;

            if ( portIndex != that.portIndex )
            {
                return false;
            }
            return tuples.equals( that.tuples );

        }

        @Override
        public int hashCode ()
        {
            int result = portIndex;
            result = 31 * result + tuples.hashCode();
            return result;
        }

        @Override
        public String toString ()
        {
            return "PortToTuples{" +
                   "portIndex=" + portIndex +
                   ", tuples=" + tuples +
                   '}';
        }
    }


    private static final BinaryOperator<PortsToTuples> PORTS_TO_TUPLES_COMBINER = ( p1, p2 ) -> {
        for ( PortToTuples portToTuples : p2.getPortToTuplesList() )
        {
            for ( Tuple tuple : portToTuples.getTuples() )
            {
                p1.add( portToTuples.portIndex, tuple );
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


    private final ArrayList<PortToTuples> tuplesByPort;

    public PortsToTuples ()
    {
        tuplesByPort = new ArrayList<>( 1 );
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
        int i = 0;
        for (; i < tuplesByPort.size(); i++ )
        {
            final PortToTuples portToTuples = tuplesByPort.get( i );
            switch ( Integer.compare( portToTuples.portIndex, portIndex ) )
            {
                case -1:
                    continue;
                case 0:
                    portToTuples.tuples.add( tuple );
                    return;
                case 1:
                    break;
            }
        }

        final List<Tuple> tuples = new ArrayList<>( 2 );
        tuples.add( tuple );
        tuplesByPort.add( i, new PortToTuples( portIndex, tuples ) );
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
        addAll( portIndex, tuplesToAdd, true );
    }

    void addAll ( final int portIndex, final List<Tuple> tuplesToAdd, boolean copy )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuplesToAdd, "tuples can't be null" );
        int i = 0;
        for (; i < tuplesByPort.size(); i++ )
        {
            final PortToTuples portToTuples = tuplesByPort.get( i );
            switch ( Integer.compare( portToTuples.portIndex, portIndex ) )
            {
                case -1:
                    continue;
                case 0:
                    portToTuples.tuples.addAll( tuplesToAdd );
                    return;
                case 1:
                    break;
            }
        }

        final List<Tuple> tuples = copy ? new ArrayList<>( tuplesToAdd ) : tuplesToAdd;
        tuplesByPort.add( i, new PortToTuples( portIndex, tuples ) );
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
        final PortToTuples portToTuples = getPortToTuples( portIndex );
        return portToTuples != null ? portToTuples.getTuples() : Collections.emptyList();
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
        final PortToTuples portToTuples = getPortToTuples( portIndex );
        if ( portToTuples != null && portToTuples.tuples.size() > tupleIndex )
        {
            return portToTuples.getTuples().get( tupleIndex );
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
     * Returns list of {@link PortToTuples} instances of which each one contains tuples added to a port index
     *
     * @return list of {@link PortToTuples} instances of which each one contains tuples added to a port index
     */
    public List<PortToTuples> getPortToTuplesList ()
    {
        return unmodifiableList( tuplesByPort );
    }

    /**
     * Returns the number of ports that contain tuples.
     *
     * @return the number of ports that contain tuples.
     */
    public int getPortCount ()
    {
        return tuplesByPort.size();
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

    private PortToTuples getPortToTuples ( final int portIndex )
    {
        for ( PortToTuples p : tuplesByPort )
        {
            if ( p.portIndex == portIndex )
            {
                return p;
            }
        }

        return null;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final PortsToTuples that = (PortsToTuples) o;

        return tuplesByPort.equals( that.tuplesByPort );

    }

    @Override
    public int hashCode ()
    {
        return tuplesByPort.hashCode();
    }

    @Override
    public String toString ()
    {
        return "PortsToTuples{" +
               "tuplesByPort=" + tuplesByPort +
               '}';
    }
}
