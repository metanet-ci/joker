package cs.bilkent.joker.engine.partition.impl;

import java.util.AbstractList;
import java.util.List;

import cs.bilkent.joker.engine.partition.PartitionKey;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.engine.partition.impl.PartitionKeyUtil.hashTail;

public class PartitionKey2Fwd1 extends AbstractList<Object> implements PartitionKey
{

    private final Object val0;

    private final Object val1;

    private final int partitionHashCode;

    private final int hashCode;

    public PartitionKey2Fwd1 ( final Object val0, final Object val1 )
    {
        this.val0 = val0;
        this.val1 = val1;
        this.partitionHashCode = computePartitionHashCode( val0 );
        this.hashCode = hashTail( partitionHashCode, val1 );
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

        final PartitionKey2Fwd1 n2 = (PartitionKey2Fwd1) o;
        return val0.equals( n2.val0 ) && val1.equals( n2.val1 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    public static int computePartitionHashCode ( final Object val0 )
    {
        return hashHead( val0 );
    }

}
