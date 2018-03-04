package cs.bilkent.joker.engine.adaptation.impl.adaptationtracker;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.engine.flow.FlowExecPlan;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;

final class Visualizer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Visualizer.class );

    private Visualizer ()
    {

    }

    static void visualize ( final FlowExecPlan execPlan, final String dir )
    {
        final String summary = execPlan.toSummaryString();
        try
        {
            final String vizPath = System.getProperty( "vizPath", "viz.py" );
            checkState( new File( vizPath ).exists() );

            final String flowPath = dir + System.getProperty( "file.separator" ) + "flow" + execPlan.getVersion() + ".pdf";
            checkState( !new File( flowPath ).exists() );

            final ProcessBuilder pb = new ProcessBuilder( "python", vizPath, "-p", summary, "-o", flowPath );
            pb.redirectOutput( Redirect.INHERIT );
            pb.redirectError( Redirect.INHERIT );

            final Process p = pb.start();
            p.waitFor( 10, SECONDS );

            if ( p.exitValue() != 0 )
            {
                LOGGER.warn( "Cannot visualize {} exit value: {}", summary, p.exitValue() );
            }
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Cannot visualize " + summary, e );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Interrupted visualization of " + summary, e );
            Thread.currentThread().interrupt();
        }
    }

}
