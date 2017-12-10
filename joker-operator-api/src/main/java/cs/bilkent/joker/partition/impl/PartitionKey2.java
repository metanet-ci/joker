package cs.bilkent.joker.partition.impl;

import java.util.AbstractList;
import java.util.List;

import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashTail;

public class PartitionKey2 extends AbstractList<Object> implements PartitionKey
{

    private final Object val0;

    private final Object val1;

    private final int hashCode;

    public PartitionKey2 ( final Object val0, final Object val1 )
    {
        this.val0 = val0;
        this.val1 = val1;
        this.hashCode = computeHashCode( val0, val1 );
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

        if ( index == 1 )
        {
            return val1;
        }

        throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + 2 );
    }

    @Override
    public int size ()
    {
        return 2;
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

        final PartitionKey2 n2 = (PartitionKey2) o;
        return val0.equals( n2.val0 ) && val1.equals( n2.val1 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    public static int computeHashCode ( final Object val0, final Object val1 )
    {
        return hashTail( hashHead( val0 ), val1 );
    }

}
