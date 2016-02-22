package cs.bilkent.zanza.engine.partition;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;

public class PartitionKey3
{

    private final Object val1;

    private final Object val2;

    private final Object val3;

    private final int hashCode;

    public PartitionKey3 ( final Tuple tuple, final List<String> partitionFieldNames )
    {
        this( tuple.getObject( partitionFieldNames.get( 0 ) ),
              tuple.getObject( partitionFieldNames.get( 1 ) ),
              tuple.getObject( partitionFieldNames.get( 2 ) ) );
    }

    public PartitionKey3 ( final Object val1, final Object val2, final Object val3 )
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.hashCode = computeHashCode();
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

        final PartitionKey3 that = (PartitionKey3) o;

        if ( !val1.equals( that.val1 ) )
        {
            return false;
        }
        if ( !val2.equals( that.val2 ) )
        {
            return false;
        }

        return val3.equals( that.val3 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKey3{" +
               "val1=" + val1 +
               ", val2=" + val2 +
               ", val3=" + val3 +
               '}';
    }

    private int computeHashCode ()
    {
        int result = val1.hashCode();
        result = 31 * result + val2.hashCode();
        return 31 * result + val3.hashCode();
    }

}
