package cs.bilkent.joker.engine.partition.impl;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashTail;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.rangeCheck;

public class PartitionKeyNFwd3 extends AbstractList<Object> implements PartitionKey
{

    private final Object[] values;

    private final int hashCode;

    private final int partitionHashCode;

    PartitionKeyNFwd3 ( final Tuple tuple, final List<String> partitionFieldNames )
    {
        final int j = partitionFieldNames.size();
        this.values = new Object[ j ];

        final Object headVal0 = tuple.get( partitionFieldNames.get( 0 ) );
        final Object headVal1 = tuple.get( partitionFieldNames.get( 1 ) );
        final Object headVal2 = tuple.get( partitionFieldNames.get( 2 ) );
        int hashCode = computePartitionHash( headVal0, headVal1, headVal2 );
        this.partitionHashCode = hashCode;
        this.values[ 0 ] = headVal0;
        this.values[ 1 ] = headVal1;
        this.values[ 2 ] = headVal2;

        for ( int i = 3; i < j; i++ )
        {
            final Object val = tuple.get( partitionFieldNames.get( i ) );
            this.values[ i ] = val;
            hashCode = hashTail( hashCode, val );
        }

        this.hashCode = hashCode;
    }

    @Override
    public int partitionHashCode ()
    {
        return partitionHashCode;
    }

    @Override
    public Object get ( final int index )
    {
        rangeCheck( index, values.length );
        return values[ index ];
    }

    @Override
    public int size ()
    {
        return values.length;
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

        final PartitionKeyNFwd3 objects = (PartitionKeyNFwd3) o;
        return hashCode == objects.hashCode && Arrays.equals( values, objects.values );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    static int computePartitionHash ( final Object val0, final Object val1, final Object val2 )
    {
        return hashTail( hashTail( hashHead( val0 ), val1 ), val2 );
    }

}
