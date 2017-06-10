package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.ExperimentalAdaptationTracker.FlowMetricsReporter;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistory;
import cs.bilkent.joker.engine.metric.PipelineMetricsHistorySummarizer;

public class FlowMetricsFileReporter implements FlowMetricsReporter
{

    private final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    private final File dir;

    public FlowMetricsFileReporter ( final JokerConfig config, final File dir )
    {
        this.pipelineMetricsHistorySummarizer = config.getAdaptationConfig().getPipelineMetricsHistorySummarizer();
        this.dir = dir;
    }

    public void init ()
    {
        createFile( "lock" );
    }

    @Override
    public void report ( final FlowExecutionPlan flowExecutionPlan, final FlowMetrics flowMetrics )
    {
        writeToFile( "last.txt", writer -> writer.println( flowExecutionPlan.getVersion() ) );

        writeToFile( "flow" + flowExecutionPlan.getVersion() + "_summary.txt", writer ->
        {
            writer.println( flowExecutionPlan.toPlanSummaryString() );
        } );

        writeToFile( "flow" + flowExecutionPlan.getVersion() + "_metricPeriod.txt", writer ->
        {
            writer.println( flowMetrics.getPeriod() );
        } );

        writeToFile( "flow" + flowExecutionPlan.getVersion() + "_regionCount.txt", writer ->
        {
            writer.println( flowExecutionPlan.getRegionCount() );
        } );

        for ( final RegionExecutionPlan regionExecPlan : flowExecutionPlan.getRegionExecutionPlans() )
        {
            final String regionFileNamePrefix = "flow" + flowExecutionPlan.getVersion() + "_r" + regionExecPlan.getRegionId();
            writeToFile( regionFileNamePrefix + "_replicaCount.txt", writer -> writer.println( regionExecPlan.getReplicaCount() ) );
            writeToFile( regionFileNamePrefix + "_pipelineCount.txt", writer -> writer.println( regionExecPlan.getPipelineCount() ) );

            for ( PipelineId pipelineId : regionExecPlan.getPipelineIds() )
            {
                final String pipelineFileNamePrefix = "flow" + flowExecutionPlan.getVersion() + "_p" + pipelineId.getRegionId() + "_"
                                                      + pipelineId.getPipelineStartIndex();

                writeToFile( pipelineFileNamePrefix + "_operatorCount.txt",
                             writer -> writer.println( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId
                                                                                                                    .getPipelineStartIndex() ) ) );

                final PipelineMetricsHistory pipelineMetricsHistory = flowMetrics.getPipelineMetricsHistory( pipelineId );
                final PipelineMetrics pipelineMetrics = pipelineMetricsHistorySummarizer.summarize( pipelineMetricsHistory );

                writeToFile( pipelineFileNamePrefix + "_cpu.txt", writer -> writer.println( pipelineMetrics.getAvgCpuUtilizationRatio() ) );

                final long[] totalInboundThroughputs = pipelineMetrics.getTotalInboundThroughputs();
                for ( int i = 0; i < pipelineMetrics.getInputPortCount(); i++ )
                {
                    final int index = i;
                    writeToFile( pipelineFileNamePrefix + "_throughput_" + i + ".txt", writer ->
                    {
                        writer.println( totalInboundThroughputs[ index ] );
                    } );
                }

                writeToFile( pipelineFileNamePrefix + "_costPipeline.txt",
                             writer -> writer.println( pipelineMetrics.getAvgPipelineCost() ) );

                final double[] avgOperatorCosts = pipelineMetrics.getAvgOperatorCosts();
                for ( int i = 0; i < pipelineMetrics.getOperatorCount(); i++ )
                {
                    final int index = i;
                    writeToFile( pipelineFileNamePrefix + "_costOperator_" + i + ".txt", writer ->
                    {
                        writer.println( avgOperatorCosts[ index ] );
                    } );
                }
            }
        }

        //        visualize( flowExecutionPlan, dir.getPath() );
    }

    private void writeToFile ( final String fileName, final Consumer<PrintWriter> consumer )
    {
        final PrintWriter writer;

        try
        {
            writer = new PrintWriter( createFile( fileName ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        consumer.accept( writer );

        writer.close();
    }

    private File createFile ( final String fileName )
    {
        final File reportFile = new File( dir, fileName );
        if ( fileName.equals( "last.txt" ) )
        {
            return reportFile;
        }

        checkState( !reportFile.exists() );

        try
        {
            final boolean created = reportFile.createNewFile();
            checkState( created );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return reportFile;
    }

}
