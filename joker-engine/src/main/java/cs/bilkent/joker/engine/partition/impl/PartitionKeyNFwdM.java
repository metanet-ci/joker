package cs.bilkent.joker.engine.partition.impl;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.partition.impl.PartitionKey;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashTail;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.rangeCheck;

public class PartitionKeyNFwdM extends AbstractList<Object> implements PartitionKey
{

    private final Object[] values;

    private final int hashCode;

    private final int partitionHashCode;

    PartitionKeyNFwdM ( final Tuple tuple, final List<String> partitionFieldNames, final int forwardKeyLimit )
    {
        final int j = partitionFieldNames.size();
        this.values = new Object[ j ];

        final Object headVal = tuple.getObject( partitionFieldNames.get( 0 ) );
        int hashCode = hashHead( headVal );
        this.values[ 0 ] = headVal;

        for ( int i = 1; i < forwardKeyLimit; i++ )
        {
            final Object val = tuple.getObject( partitionFieldNames.get( i ) );
            this.values[ i ] = val;
            hashCode = hashTail( hashCode, val );
        }

        this.partitionHashCode = hashCode;

        for ( int i = forwardKeyLimit; i < j; i++ )
        {
            final Object val = tuple.getObject( partitionFieldNames.get( i ) );
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

        final PartitionKeyNFwdM objects = (PartitionKeyNFwdM) o;
        return hashCode == objects.hashCode && Arrays.equals( values, objects.values );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    static int computePartitionHash ( final Tuple tuple, final List<String> partitionFieldNames, final int forwardKeyLimit )
    {
        int hashCode = hashHead( tuple.getObject( partitionFieldNames.get( 0 ) ) );

        for ( int i = 1; i < forwardKeyLimit; i++ )
        {
            hashCode = hashTail( hashCode, tuple.getObject( partitionFieldNames.get( i ) ) );
        }

        return hashCode;
    }

}
