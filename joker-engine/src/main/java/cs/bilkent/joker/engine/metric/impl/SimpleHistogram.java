package cs.bilkent.joker.engine.metric.impl;

import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;

public class SimpleHistogram
{

    private static final double REPORT_RATIO = 0.05d;


    private final int[] counts;

    private int nonEmptyCount;

    private long valueSum;

    public SimpleHistogram ( final int maxValue )
    {
        checkArgument( maxValue > 0 );
        this.counts = new int[ maxValue + 1 ];
    }

    public void record ( final int value )
    {
        checkArgument( value >= 0 );
        counts[ value ]++;
        if ( value > 0 )
        {
            nonEmptyCount++;
            valueSum += value;
        }
    }

    public void reset ()
    {
        for ( int i = 0; i < counts.length; i++ )
        {
            counts[ i ] = 0;
        }
        nonEmptyCount = 0;
        valueSum = 0;
    }

    @Override
    public String toString ()
    {
        if ( nonEmptyCount == 0 )
        {
            return counts[ 0 ] > 0 ? "[ EMPTY=" + counts[ 0 ] + " ]" : "NO DATA!";
        }

        final StringBuilder sb = new StringBuilder();
        sb.append( "[ AVG= " ).append( ( (double) valueSum ) / nonEmptyCount );
        if ( counts[ 0 ] > 0 )
        {
            sb.append( " EMPTY COUNT=" )
              .append( counts[ 0 ] )
              .append( " EMPTY / NON-EMPTY RATIO=" )
              .append( ( (double) counts[ 0 ] ) / nonEmptyCount );
        }

        int sum = 0;
        int lastIdx = 1;

        for ( int i = 1; i < counts.length; i++ )
        {
            final double cumulativeRatio = ( (double) sum ) / nonEmptyCount;
            if ( cumulativeRatio >= REPORT_RATIO )
            {
                append( sb, lastIdx, cumulativeRatio );

                sum = 0;
                lastIdx = i;
            }

            final int count = counts[ i ];
            if ( count > 0 )
            {
                final double ratio = ( (double) count ) / nonEmptyCount;

                if ( ratio >= REPORT_RATIO )
                {
                    if ( sum > 0 )
                    {
                        append( sb, lastIdx, cumulativeRatio );
                    }

                    append( sb, i, ratio );

                    sum = 0;
                    lastIdx = ( i + 1 );

                }
                else
                {
                    sum += count;
                }
            }
        }

        if ( sum > 0 )
        {
            append( sb, lastIdx, ( (double) sum ) / nonEmptyCount );
        }

        sb.append( " ]" );

        return sb.toString();
    }

    private void append ( final StringBuilder sb, final int size, final double ratio )
    {
        sb.append( " <size=" )
          .append( size )
          .append( " (" )
          .append( String.format( "%.3f", ( (double) size ) / ( counts.length - 1 ) ) )
          .append( " full), ratio=" )
          .append( String.format( "%.3f", ratio ) )
          .append( ">" );
    }

}
