package cs.bilkent.zanza.engine.partition.impl;

public class PartitionKey4
{

    private final Object val1;

    private final Object val2;

    private final Object val3;

    private final Object val4;

    private final int hashCode;

    public PartitionKey4 ( final Object val1, final Object val2, final Object val3, final Object val4 )
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.val4 = val4;
        this.hashCode = computeHashCode( val1, val2, val3, val4 );
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

        final PartitionKey4 that = (PartitionKey4) o;

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

        return val4.equals( that.val4 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKey4{" +
               "val1=" + val1 +
               ", val2=" + val2 +
               ", val3=" + val3 +
               ", val4=" + val4 +
               '}';
    }

    public static int computeHashCode ( final Object val1, final Object val2, final Object val3, final Object val4 )
    {
        int result = val1.hashCode();
        result = 31 * result + val2.hashCode();
        result = 31 * result + val3.hashCode();
        return 31 * result + val4.hashCode();
    }

}
