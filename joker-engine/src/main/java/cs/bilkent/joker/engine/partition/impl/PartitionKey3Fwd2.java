package cs.bilkent.joker.engine.partition.impl;

import java.util.AbstractList;
import java.util.List;

import cs.bilkent.joker.partition.impl.PartitionKey;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashTail;

public class PartitionKey3Fwd2 extends AbstractList<Object> implements PartitionKey
{

    private final Object val0;

    private final Object val1;

    private final Object val2;

    private final int partitionHashCode;

    private final int hashCode;

    PartitionKey3Fwd2 ( final Object val0, final Object val1, final Object val2 )
    {
        this.val0 = val0;
        this.val1 = val1;
        this.val2 = val2;
        this.partitionHashCode = computePartitionHashCode( val0, val1 );
        this.hashCode = hashTail( partitionHashCode, val2 );
    }

    @Override
    public int partitionHashCode ()
    {
        return partitionHashCode;
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

        if ( index == 2 )
        {
            return val2;
        }

        throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + 3 );
    }

    @Override
    public int size ()
    {
        return 3;
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

        final PartitionKey3Fwd2 n2 = (PartitionKey3Fwd2) o;
        return val0.equals( n2.val0 ) && val1.equals( n2.val1 ) && val2.equals( n2.val2 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    static int computePartitionHashCode ( final Object val0, final Object val1 )
    {
        return hashTail( hashHead( val0 ), val1 );
    }

}
