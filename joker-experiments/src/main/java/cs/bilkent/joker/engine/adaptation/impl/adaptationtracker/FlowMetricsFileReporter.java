package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.ExperimentalAdaptationTracker.FlowMetricsReporter;
import static cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.Visualizer.visualize;
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

    private static final String REPORT_FILE_NAME = "report.txt";


    private final PipelineMetricsHistorySummarizer pipelineMetricsHistorySummarizer;

    private final File dir;

    public FlowMetricsFileReporter ( final JokerConfig config, final File dir )
    {
        this.pipelineMetricsHistorySummarizer = config.getAdaptationConfig().getPipelineMetricsHistorySummarizer();
        this.dir = dir;
    }

    public void init ()
    {
        final File reportFile = new File( dir, REPORT_FILE_NAME );
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
    }

    @Override
    public void report ( final FlowExecutionPlan flowExecutionPlan, final FlowMetrics flowMetrics )
    {
        final File reportFile = new File( dir, REPORT_FILE_NAME );
        checkState( reportFile.exists() );

        final PrintWriter writer;

        try
        {
            writer = new PrintWriter( new FileWriter( reportFile, true ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        writer.println( "FLOW_EXECUTION_PLAN_VERSION" );
        writer.println( flowExecutionPlan.getVersion() );
        writer.println( "FLOW_EXECUTION_PLAN_SUMMARY" );
        writer.println( flowExecutionPlan.toPlanSummaryString() );
        writer.println( "METRIC_PERIOD" );
        writer.println( flowMetrics.getPeriod() );
        writer.println( "REGION_COUNT" );
        writer.println( flowExecutionPlan.getRegionCount() );
        for ( RegionExecutionPlan regionExecPlan : flowExecutionPlan.getRegionExecutionPlans() )
        {
            writer.println( "REGION_ID" );
            writer.println( regionExecPlan.getRegionId() );
            writer.println( "REPLICA_COUNT" );
            writer.println( regionExecPlan.getReplicaCount() );
            writer.println( "PIPELINE_COUNT" );
            writer.println( regionExecPlan.getPipelineCount() );
            for ( PipelineId pipelineId : regionExecPlan.getPipelineIds() )
            {
                writer.println( "PIPELINE_START_INDEX" );
                writer.println( pipelineId.getPipelineStartIndex() );
                writer.println( "OPERATOR_COUNT" );
                writer.println( regionExecPlan.getOperatorCountByPipelineStartIndex( pipelineId.getPipelineStartIndex() ) );

                final PipelineMetricsHistory pipelineMetricsHistory = flowMetrics.getPipelineMetricsHistory( pipelineId );
                final PipelineMetrics pipelineMetrics = pipelineMetricsHistorySummarizer.summarize( pipelineMetricsHistory );

                writer.println( "CPU_UTILIZATION_RATIO" );
                writer.println( pipelineMetrics.getAvgCpuUtilizationRatio() );
                writer.println( "INPUT_PORT_COUNT" );
                writer.println( pipelineMetrics.getInputPortCount() );
                writer.println( "INBOUND_THROUGHPUTS" );
                writer.println( Arrays.toString( pipelineMetrics.getTotalInboundThroughputs() ) );
                writer.println( "OPERATOR_COSTS" );
                writer.println( Arrays.toString( pipelineMetrics.getAvgOperatorCosts() ) );
                writer.println( "PIPELINE_COST" );
                writer.println( pipelineMetrics.getAvgPipelineCost() );
            }
        }

        writer.close();

        visualize( flowExecutionPlan, dir.getPath() );
    }

}
