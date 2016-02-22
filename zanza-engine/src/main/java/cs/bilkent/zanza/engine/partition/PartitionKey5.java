package cs.bilkent.zanza.engine.partition;

import java.util.List;

import cs.bilkent.zanza.operator.Tuple;

public class PartitionKey5
{

    private final Object val1;

    private final Object val2;

    private final Object val3;

    private final Object val4;

    private final Object val5;

    private final int hashCode;

    public PartitionKey5 ( final Tuple tuple, final List<String> partitionFieldNames )
    {
        this( tuple.getObject( partitionFieldNames.get( 0 ) ),
              tuple.getObject( partitionFieldNames.get( 1 ) ),
              tuple.getObject( partitionFieldNames.get( 2 ) ),
              tuple.getObject( partitionFieldNames.get( 3 ) ),
              tuple.getObject( partitionFieldNames.get( 4 ) ) );
    }

    public PartitionKey5 ( final Object val1, final Object val2, final Object val3, final Object val4, final Object val5 )
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.val4 = val4;
        this.val5 = val5;
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

        final PartitionKey5 that = (PartitionKey5) o;

        if ( !val1.equals( that.val1 ) )
        {
            return false;
        }
        if ( !val2.equals( that.val2 ) )
        {
            return false;
        }
        if ( !val3.equals( that.val3 ) )
        {
            return false;
        }
        if ( !val4.equals( that.val4 ) )
        {
            return false;
        }

        return val5.equals( that.val5 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKey5{" +
               "val1=" + val1 +
               ", val2=" + val2 +
               ", val3=" + val3 +
               ", val4=" + val4 +
               ", val5=" + val5 +
               '}';
    }

    private int computeHashCode ()
    {
        int result = val1.hashCode();
        result = 31 * result + val2.hashCode();
        result = 31 * result + val3.hashCode();
        result = 31 * result + val4.hashCode();
        return 31 * result + val5.hashCode();
    }

}
