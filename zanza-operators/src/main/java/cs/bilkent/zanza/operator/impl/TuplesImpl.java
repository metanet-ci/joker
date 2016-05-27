package cs.bilkent.zanza.operator.impl;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import cs.bilkent.zanza.flow.Port;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import static java.util.Collections.unmodifiableList;


/**
 * Contains {@link Tuple} instances mapped to some ports specified by indices.
 * Used for providing input tuples and output tuples of the {@link Operator#invoke(InvocationContext)} method.
 */
public final class TuplesImpl implements Tuples
{

    private static final int INITIAL_CAPACITY = 2;

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
    public static TuplesImpl newInstance ( final int portCount, final Tuple tuple, final Tuple... tuples )
    {
        TuplesImpl tuplesImpl = new TuplesImpl( portCount );

        checkNotNull( tuple, "tuple can't be null" );
        tuplesImpl.add( tuple );
        for ( Tuple t : tuples )
        {
            tuplesImpl.add( t );
        }

        return tuplesImpl;
    }


    private final List<Tuple>[] tuplesByPort;

    public TuplesImpl ( final int portCount )
    {
        tuplesByPort = new List[ portCount ];
        for ( int i = 0; i < portCount; i++ )
        {
            tuplesByPort[ i ] = new ArrayList<>( INITIAL_CAPACITY );
        }
    }

    @Override
    public void add ( final Tuple tuple )
    {
        checkNotNull( tuple, "tuple can't be null" );
        add( DEFAULT_PORT_INDEX, tuple );
    }

    @Override
    public void addAll ( final List<Tuple> tuples )
    {
        checkNotNull( tuples, "tuples can't be null" );
        addAll( DEFAULT_PORT_INDEX, tuples );
    }

    @Override
    public void add ( final int portIndex, final Tuple tuple )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuple, "tuple can't be null" );

        tuplesByPort[ portIndex ].add( tuple );
    }

    @Override
    public void addAll ( final int portIndex, final List<Tuple> tuplesToAdd )
    {
        checkArgument( portIndex >= 0, "port must be non-negative" );
        checkNotNull( tuplesToAdd, "tuples can't be null" );

        tuplesByPort[ portIndex ].addAll( tuplesToAdd );
    }

    @Override
    public List<Tuple> getTuples ( final int portIndex )
    {
        return unmodifiableList( tuplesByPort[ portIndex ] );
    }

    public List<Tuple> getTuplesModifiable ( final int portIndex )
    {
        return tuplesByPort[ portIndex ];
    }

    @Override
    public Tuple getTupleOrNull ( final int portIndex, final int tupleIndex )
    {
        final List<Tuple> tuples = tuplesByPort[ portIndex ];
        if ( tuples != null && tuples.size() > tupleIndex )
        {
            return tuples.get( tupleIndex );
        }

        return null;
    }

    @Override
    public int getPortCount ()
    {
        return tuplesByPort.length;
    }

    @Override
    public int getTupleCount ( final int portIndex )
    {
        return tuplesByPort[ portIndex ].size();
    }

    @Override
    public void clear ()
    {
        for ( List<Tuple> tuples : tuplesByPort )
        {
            tuples.clear();
        }
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

        final TuplesImpl that = (TuplesImpl) o;

        return Arrays.equals( tuplesByPort, that.tuplesByPort );

    }

    @Override
    public int hashCode ()
    {
        return Arrays.hashCode( tuplesByPort );
    }

    @Override
    public String toString ()
    {
        return Arrays.toString( tuplesByPort );
    }

}
