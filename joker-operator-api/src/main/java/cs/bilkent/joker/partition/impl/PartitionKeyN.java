package cs.bilkent.joker.partition.impl;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashHead;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.hashTail;
import static cs.bilkent.joker.partition.impl.PartitionKeyUtil.rangeCheck;

public class PartitionKeyN extends AbstractList<Object> implements PartitionKey
{

    private final Object[] values;

    private final int hashCode;

    public PartitionKeyN ( final Tuple tuple, final List<String> partitionFieldNames )
    {
        final int j = partitionFieldNames.size();
        this.values = new Object[ j ];

        final Object headVal = tuple.get( partitionFieldNames.get( 0 ) );
        int hashCode = hashHead( headVal );
        this.values[ 0 ] = headVal;
        for ( int i = 1; i < j; i++ )
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
        return hashCode;
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

        final PartitionKeyN objects = (PartitionKeyN) o;
        return hashCode == objects.hashCode && Arrays.equals( values, objects.values );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKeyN{" + "values=" + Arrays.toString( values ) + '}';
    }

    public static int computeHashCode ( final Tuple tuple, final List<String> partitionFieldNames )
    {
        int hashCode = hashHead( tuple.getObject( partitionFieldNames.get( 0 ) ) );
        for ( int i = 1, j = partitionFieldNames.size(); i < j; i++ )
        {
            hashCode = hashTail( hashCode, tuple.getObject( partitionFieldNames.get( i ) ) );
        }

        return hashCode;
    }

}
