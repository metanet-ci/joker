package cs.bilkent.joker.experiment;

import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.experiment.ReportThroughput.S;
import static cs.bilkent.joker.experiment.ReportThroughput.readSingleLine;
import static cs.bilkent.joker.experiment.ReportThroughput.writeToFile;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

public class ReportCosts
{

    public static void main ( String[] args )
    {
        checkArgument( args.length == 4, "expected arguments: dir name, max operator cost, region id, pipeline start index" );

        final String dir = args[ 0 ];
        final int maxOperatorCost = Integer.parseInt( args[ 1 ] );
        final int regionId = parseInt( args[ 2 ] );
        final int pipelineStartIndex = parseInt( args[ 3 ] );

        int cost = 1;
        while ( cost <= maxOperatorCost )
        {
            final int c = cost;
            final String costDir = dir + S + cost;

            writeToFile( costDir + S + "costs" + regionId + "_" + pipelineStartIndex + ".txt", writer ->
            {
                final int lastFlowVersion = parseInt( readSingleLine( costDir + S + "last.txt" ) );

                int flowVersion = 0;
                while ( flowVersion <= lastFlowVersion )
                {
                    try
                    {
                        final String prefix = costDir + S + "flow" + flowVersion + "_p" + regionId + "_" + pipelineStartIndex;
                        final String flowSummary = readSingleLine( costDir + S + "flow" + flowVersion + "_summary.txt" );
                        final String path =
                                costDir + S + "flow" + flowVersion + "_p" + regionId + "_" + pipelineStartIndex + "_throughput_0.txt";
                        final long throughput = Long.parseLong( readSingleLine( path ) );
                        final double pipelineCost = parseDouble( readSingleLine( prefix + "_costPipeline.txt" ) );
                        final int operatorCount = parseInt( readSingleLine( prefix + "_operatorCount.txt" ) );
                        final Object[] operatorCosts = new Object[ operatorCount ];
                        for ( int i = 0; i < operatorCount; i++ )
                        {
                            operatorCosts[ i ] = parseDouble( readSingleLine( prefix + "_costOperator_" + i + ".txt" ) );
                        }

                        final String log = flowVersion + "|" + flowSummary + "|" + throughput + "|" + pipelineCost + "|" + Joiner.on( "|" )
                                                                                                                                 .join( operatorCosts );
                        writer.println( log );
                    }
                    catch ( Exception e )
                    {

                    }

                    flowVersion++;
                }
            } );

            cost *= 2;
        }
    }

}
