package cs.bilkent.zanza.engine.partition.impl;

public class PartitionKey9
{

    private final Object val1;

    private final Object val2;

    private final Object val3;

    private final Object val4;

    private final Object val5;

    private final Object val6;

    private final Object val7;

    private final Object val8;

    private final Object val9;

    private final int hashCode;

    public PartitionKey9 ( final Object val1,
                           final Object val2,
                           final Object val3,
                           final Object val4,
                           final Object val5,
                           final Object val6,
                           final Object val7,
                           final Object val8,
                           final Object val9 )
    {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.val4 = val4;
        this.val5 = val5;
        this.val6 = val6;
        this.val7 = val7;
        this.val8 = val8;
        this.val9 = val9;
        this.hashCode = computeHashCode( val1, val2, val3, val4, val5, val6, val7, val8, val9 );
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

        final PartitionKey9 that = (PartitionKey9) o;

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
        if ( !val5.equals( that.val5 ) )
        {
            return false;
        }
        if ( !val6.equals( that.val6 ) )
        {
            return false;
        }
        if ( !val7.equals( that.val7 ) )
        {
            return false;
        }
        if ( !val8.equals( that.val8 ) )
        {
            return false;
        }

        return val9.equals( that.val9 );
    }

    @Override
    public int hashCode ()
    {
        return hashCode;
    }

    @Override
    public String toString ()
    {
        return "PartitionKey9{" +
               "val1=" + val1 +
               ", val2=" + val2 +
               ", val3=" + val3 +
               ", val4=" + val4 +
               ", val5=" + val5 +
               ", val6=" + val6 +
               ", val7=" + val7 +
               ", val8=" + val8 +
               ", val9=" + val9 +
               '}';
    }

    public static int computeHashCode ( final Object val1,
                                        final Object val2,
                                        final Object val3,
                                        final Object val4,
                                        final Object val5,
                                        final Object val6,
                                        final Object val7,
                                        final Object val8,
                                        final Object val9 )
    {
        int result = val1.hashCode();
        result = 31 * result + val2.hashCode();
        result = 31 * result + val3.hashCode();
        result = 31 * result + val4.hashCode();
        result = 31 * result + val5.hashCode();
        result = 31 * result + val6.hashCode();
        result = 31 * result + val7.hashCode();
        result = 31 * result + val8.hashCode();
        return 31 * result + val9.hashCode();
    }

}
