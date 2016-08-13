package cs.bilkent.joker.operator.impl;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import static java.util.Collections.unmodifiableList;


/**
 * Contains {@link Tuple} instances mapped to some ports specified by indices.
 * Used for providing input tuples and output tuples of the {@link Operator#invoke(InvocationContext)} method.
 */
public final class TuplesImpl implements Tuples
{

    private static final String INITIAL_CAPACITY_SYS_PARAM = "cs.bilkent.joker.TuplesImpl.INITIAL_CAPACITY";

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    static
    {
        int sysArg = -1;
        try
        {
            String val = System.getProperty( INITIAL_CAPACITY_SYS_PARAM );
            if ( val != null )
            {
                val = val.trim();
                if ( val.length() > 0 )
                {
                    sysArg = Integer.parseInt( val );
                    System.out.println(
                            "Static initialization: " + TuplesImpl.class.getSimpleName() + " initial capacity is set to " + sysArg );
                }
            }
        }
        catch ( Exception e )
        {
            System.err.println( "Static initialization: " + TuplesImpl.class.getSimpleName() + " initial capacity failed " );
            e.printStackTrace();
        }

        INITIAL_CAPACITY = sysArg != -1 ? sysArg : DEFAULT_INITIAL_CAPACITY;
    }

    private static final int INITIAL_CAPACITY;


    private final List<Tuple>[] ports;

    public TuplesImpl ( final int portCount )
    {
        ports = new List[ portCount ];
        for ( int i = 0; i < portCount; i++ )
        {
            ports[ i ] = new ArrayList<>( INITIAL_CAPACITY );
        }
    }

    @Override
    public void add ( final Tuple tuple )
    {
        checkArgument( tuple != null );
        add( DEFAULT_PORT_INDEX, tuple );
    }

    @Override
    public void addAll ( final List<Tuple> tuples )
    {
        checkArgument( tuples != null );
        addAll( DEFAULT_PORT_INDEX, tuples );
    }

    @Override
    public void add ( final int portIndex, final Tuple tuple )
    {
        checkArgument( tuple != null );
        ports[ portIndex ].add( tuple );
    }

    @Override
    public void addAll ( final int portIndex, final List<Tuple> tuples )
    {
        checkArgument( tuples != null );

        ports[ portIndex ].addAll( tuples );
    }

    @Override
    public List<Tuple> getTuples ( final int portIndex )
    {
        return unmodifiableList( ports[ portIndex ] );
    }

    public List<Tuple> getTuplesModifiable ( final int portIndex )
    {
        return ports[ portIndex ];
    }

    @Override
    public Tuple getTupleOrNull ( final int portIndex, final int tupleIndex )
    {
        final List<Tuple> tuples = ports[ portIndex ];
        if ( tuples != null && tuples.size() > tupleIndex )
        {
            return tuples.get( tupleIndex );
        }

        return null;
    }

    @Override
    public int getPortCount ()
    {
        return ports.length;
    }

    @Override
    public int getTupleCount ( final int portIndex )
    {
        return ports[ portIndex ].size();
    }

    @Override
    public void clear ()
    {
        for ( List<Tuple> tuples : ports )
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

        return Arrays.equals( ports, that.ports );

    }

    @Override
    public int hashCode ()
    {
        return Arrays.hashCode( ports );
    }

    @Override
    public String toString ()
    {
        return Arrays.toString( ports );
    }

}
