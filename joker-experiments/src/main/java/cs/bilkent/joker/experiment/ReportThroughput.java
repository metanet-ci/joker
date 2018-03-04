package cs.bilkent.joker.experiment;


import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.readAllLines;

public class ReportThroughput
{

    static final String S = System.getProperty( "file.separator" );

    public static void main ( String[] args ) throws IOException
    {
        checkArgument( args.length == 3, "expected arguments: dir name, max operator cost, regionId" );

        final String dir = args[ 0 ];
        final int maxOperatorCost = Integer.parseInt( args[ 1 ] );
        final int regionId = Integer.parseInt( args[ 2 ] );

        writeToFile( dir + S + "throughputs_r" + regionId + ".txt", writer -> {
            int cost = 1;
            while ( cost <= maxOperatorCost )
            {
                final long initialThroughput = readInitialThroughput( dir, cost, regionId );
                final long finalThroughput = readFinalThroughput( dir, cost, regionId );
                final int finalReplicaCount = readFinalReplicaCount( dir, cost, regionId );
                final String finalFlowSummary = readFinalFlowSummary( dir, cost );
                writer.println( cost + "|" + initialThroughput + "|" + finalThroughput + "|" + finalReplicaCount + "|" + finalFlowSummary );
                cost *= 2;
            }
        } );
    }


    private static long readInitialThroughput ( final String dir, final int operatorCost, final int regionId )
    {
        return readThroughput( dir, operatorCost, 0, regionId );
    }

    private static long readFinalThroughput ( final String dir, final int operatorCost, final int regionId )
    {
        final int finalFlowVersion = readFinalFlowVersion( dir, operatorCost );
        return readThroughput( dir, operatorCost, finalFlowVersion, regionId );
    }

    private static int readFinalReplicaCount ( final String dir, final int operatorCost, final int regionId )
    {
        final int finalFlowVersion = readFinalFlowVersion( dir, operatorCost );
        final String path = dir + S + operatorCost + S + "flow" + finalFlowVersion + "_r" + regionId + "_replicaCount.txt";
        return Integer.valueOf( readSingleLine( path ) );
    }

    private static int readFinalFlowVersion ( final String dir, final int operatorCost )
    {
        return Integer.valueOf( readSingleLine( dir + S + operatorCost + S + "last.txt" ) );
    }

    private static String readFinalFlowSummary ( final String dir, final int operatorCost )
    {
        final int finalFlowVersion = readFinalFlowVersion( dir, operatorCost );
        final String path = dir + S + operatorCost + S + "flow" + finalFlowVersion + "_summary.txt";
        return readSingleLine( path );
    }

    private static long readThroughput ( final String dir, final int operatorCost, final int flowVersion, final int regionId )
    {
        final String path = dir + S + operatorCost + S + "flow" + flowVersion + "_p" + regionId + "_0_throughput_0.txt";
        return Long.valueOf( readSingleLine( path ) );
    }

    static String readSingleLine ( final String path )
    {
        final List<String> lines;
        try
        {
            lines = readAllLines( Paths.get( path ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        checkState( lines.size() == 1 );
        return lines.get( 0 );
    }

    static void writeToFile ( final String fileName, final Consumer<PrintWriter> consumer )
    {
        final PrintWriter writer;

        try
        {
            writer = new PrintWriter( fileName );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        consumer.accept( writer );

        writer.close();
    }

}
