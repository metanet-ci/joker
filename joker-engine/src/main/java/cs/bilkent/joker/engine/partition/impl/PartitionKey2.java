package cs.bilkent.joker.engine.partition.impl;

public class PartitionKey2
{

    private final Object val1;

    private final Object val2;

    private final int hashCode;

    public PartitionKey2 ( final Object val1, final Object val2 )
    {
        this.val1 = val1;
        this.val2 = val2;
        this.hashCode = computeHashCode( val1, val2 );
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

        final PartitionKey2 that = (PartitionKey2) o;

        if ( !val1.equals( that.val1 ) )
        {
            return false;
        }

        return val2.equals( that.val2 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKey2{" + "val1=" + val1 + ", val2=" + val2 + '}';
    }

    public static int computeHashCode ( final Object val1, final Object val2 )
    {
        int result = val1.hashCode();
        return 31 * result + val2.hashCode();
    }

}
