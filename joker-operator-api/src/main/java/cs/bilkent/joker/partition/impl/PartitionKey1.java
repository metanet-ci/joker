package cs.bilkent.joker.partition.impl;

import java.util.AbstractList;
import java.util.List;

import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;

public class PartitionKey1 extends AbstractList<Object> implements PartitionKey
{

    private final Object val0;

    private final int hashCode;

    public PartitionKey1 ( final Object val0 )
    {
        this.val0 = val0;
        this.hashCode = computeHashCode( val0 );
    }

    @Override
    public int partitionHashCode ()
    {
        return hashCode;
    }

    @Override
    public Object get ( final int index )
    {
        if ( index == 0 )
        {
            return val0;
        }

        throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + 1 );
    }

    @Override
    public int size ()
    {
        return 1;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null )
        {
            return false;
        }
        if ( getClass() != o.getClass() )
        {
            return o instanceof List && super.equals( o );
        }

        final PartitionKey1 n2 = (PartitionKey1) o;
        return val0.equals( n2.val0 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    public static int computeHashCode ( final Object val0 )
    {
        return hashHead( val0 );
    }

}
