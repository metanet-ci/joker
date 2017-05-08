package cs.bilkent.joker.engine.adaptation.impl;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.AdaptationConfig;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.metric.FlowMetrics;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class DefaultAdaptationTracker implements AdaptationTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DefaultAdaptationTracker.class );


    private final AdaptationConfig adaptationConfig;

    @Inject
    public DefaultAdaptationTracker ( final JokerConfig jokerConfig )
    {
        this.adaptationConfig = jokerConfig.getAdaptationConfig();
    }

    @Override
    public void init ( final ShutdownHook hook, final FlowExecutionPlan flowExecutionPlan )
    {
        if ( !isVisualizationEnabled() )
        {
            return;
        }

        visualize( flowExecutionPlan );
    }

    @Override
    public void onPeriod ( final FlowExecutionPlan flowExecutionPlan, final FlowMetrics flowMetrics )
    {
    }

    @Override
    public void onFlowExecutionPlanChange ( final FlowExecutionPlan newFlowExecutionPlan )
    {
        if ( !isVisualizationEnabled() )
        {
            return;
        }

        visualize( newFlowExecutionPlan );
    }

    private boolean isVisualizationEnabled ()
    {
        return adaptationConfig.isVisualizationEnabled();
    }

    private void visualize ( final FlowExecutionPlan flowExecutionPlan )
    {
        if ( !isVisualizationEnabled() )
        {
            return;
        }

        try
        {
            final ProcessBuilder pb = new ProcessBuilder( "python",
                                                          "viz.py",
                                                          "-p",
                                                          flowExecutionPlan.toPlanSummaryString(),
                                                          "-o",
                                                          "flow" + flowExecutionPlan.getVersion() + ".pdf" );
            pb.redirectOutput( Redirect.INHERIT );
            pb.redirectError( Redirect.INHERIT );

            final Process p = pb.start();
            p.waitFor( 10, SECONDS );

            if ( p.exitValue() != 0 )
            {
                LOGGER.warn( "Cannot visualize {} exit value: {}", flowExecutionPlan.toPlanSummaryString(), p.exitValue() );
            }
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Cannot visualize " + flowExecutionPlan.toPlanSummaryString(), e );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Interrupted visualization of " + flowExecutionPlan.toPlanSummaryString(), e );
            Thread.currentThread().interrupt();
        }
    }

}
