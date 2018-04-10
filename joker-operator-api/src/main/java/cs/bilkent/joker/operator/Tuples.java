package cs.bilkent.joker.operator;

import java.util.List;

import cs.bilkent.joker.flow.Port;

/**
 * Contains {@link Tuple} objects collected for multiple ports of an operator.
 */
public interface Tuples
{

    /**
     * Adds the tuple to the default port.
     *
     * @param tuple
     *         tuple to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    Tuples add ( Tuple tuple );

    default Tuples add ( Tuple tuple1, Tuple tuple2 )
    {
        return add( tuple1 ).add( tuple2 );
    }

    default Tuples add ( Tuple tuple1, Tuple tuple2, Tuple tuple3 )
    {
        return add( tuple1 ).add( tuple2 ).add( tuple3 );
    }

    default Tuples add ( Tuple tuple1, Tuple tuple2, Tuple tuple3, Tuple tuple4 )
    {
        return add( tuple1 ).add( tuple2 ).add( tuple3 ).add( tuple4 );
    }

    default Tuples add ( Tuple tuple1, Tuple tuple2, Tuple tuple3, Tuple tuple4, Tuple tuple5 )
    {
        return add( tuple1 ).add( tuple2 ).add( tuple3 ).add( tuple4 ).add( tuple5 );
    }


    /**
     * Adds the tuples to the default port.
     *
     * @param tuples
     *         tuples to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    Tuples add ( List<Tuple> tuples );

    /**
     * Adds the tuple to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuple
     *         tuple to add to the specified port
     */
    Tuples add ( int portIndex, Tuple tuple );

    default Tuples add ( int portIndex, Tuple tuple1, Tuple tuple2 )
    {
        return add( portIndex, tuple1 ).add( portIndex, tuple2 );
    }

    default Tuples add ( int portIndex, Tuple tuple1, Tuple tuple2, Tuple tuple3 )
    {
        return add( portIndex, tuple1 ).add( portIndex, tuple2 ).add( portIndex, tuple3 );
    }

    default Tuples add ( int portIndex, Tuple tuple1, Tuple tuple2, Tuple tuple3, Tuple tuple4 )
    {
        return add( portIndex, tuple1 ).add( portIndex, tuple2 ).add( portIndex, tuple3 ).add( portIndex, tuple4 );
    }

    default Tuples add ( int portIndex, Tuple tuple1, Tuple tuple2, Tuple tuple3, Tuple tuple4, Tuple tuple5 )
    {
        return add( portIndex, tuple1 ).add( portIndex, tuple2 ).add( portIndex, tuple3 ).add( portIndex, tuple4 ).add( portIndex, tuple5 );
    }

    /**
     * Adds the tuples to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuples
     *         tuples to add to the specified port
     */
    void add ( int portIndex, List<Tuple> tuples );

    /**
     * Returns the tuples added to the given port index.
     *
     * @param portIndex
     *         the port index that tuples are added to
     *
     * @return the tuples added to the given port index
     */
    List<Tuple> getTuples ( int portIndex );

    /**
     * Returns the tuple added to the given port index with the given tuple index, or null if no tuple exists with the given indices.
     *
     * @param portIndex
     *         the port index that tuple is added to
     * @param tupleIndex
     *         the order which the tuple is added to the given port index
     *
     * @return the tuple added to the given port index with the given tuple index if exists, null otherwise
     */
    Tuple getTupleOrNull ( int portIndex, int tupleIndex );

    /**
     * Returns the tuple added to the given port index with the given tuple index.
     *
     * @param portIndex
     *         the port index that tuple is added to
     * @param tupleIndex
     *         the order which the tuple is added to the given port index
     *
     * @return the tuple added to the given port index with the given tuple index
     *
     * @throws IllegalArgumentException
     *         if there is no tuple exists with the given indices
     */
    default Tuple getTupleOrFail ( final int portIndex, final int tupleIndex )
    {
        final Tuple tuple = getTupleOrNull( portIndex, tupleIndex );
        if ( tuple != null )
        {
            return tuple;
        }

        throw new IllegalArgumentException( "no tuple exists for port index " + portIndex + " and tuple index " + tupleIndex );
    }

    /**
     * Returns all the tuples added to the default port.
     *
     * @return all the tuples added to the default port.
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    default List<Tuple> getTuplesByDefaultPort ()
    {
        return getTuples( Port.DEFAULT_PORT_INDEX );
    }

    /**
     * Returns the number of ports that contain tuples.
     *
     * @return the number of ports that contain tuples.
     */
    int getPortCount ();

    /**
     * Returns the number of tuples that are added to the given port.
     *
     * @param portIndex
     *         port index to check number of tuples that are added.
     *
     * @return the number of tuples that are added to the given port.
     */
    int getTupleCount ( int portIndex );

    /**
     * Returns the number of ports that include a tuple
     *
     * @return the number of ports that include a tuple
     */
    default int getNonEmptyPortCount ()
    {
        int count = 0;
        for ( int i = 0, j = getPortCount(); i < j; i++ )
        {
            if ( getTupleCount( i ) > 0 )
            {
                count++;
            }
        }

        return count;
    }

    default boolean isEmpty ()
    {
        for ( int i = 0, j = getPortCount(); i < j; i++ )
        {
            if ( getTupleCount( i ) > 0 )
            {
                return false;
            }
        }

        return true;
    }

    default boolean isNonEmpty ()
    {
        return !isEmpty();
    }

    void clear ();

}
