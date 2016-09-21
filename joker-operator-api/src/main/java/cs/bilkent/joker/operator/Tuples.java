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
    void add ( Tuple tuple );

    /**
     * Adds the tuples to the default port.
     *
     * @param tuples
     *         tuples to add to the default port
     *
     * @see Port#DEFAULT_PORT_INDEX
     */
    void addAll ( List<Tuple> tuples );

    /**
     * Adds the tuple to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuple
     *         tuple to add to the specified port
     */
    void add ( int portIndex, Tuple tuple );

    /**
     * Adds the tuples to the port specified by the port index.
     *
     * @param portIndex
     *         the index of the port to which the tuple is added
     * @param tuplesToAdd
     *         tuples to add to the specified port
     */
    void addAll ( int portIndex, List<Tuple> tuplesToAdd );

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
