package cs.bilkent.joker.experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.typesafe.config.ConfigFactory.systemProperties;
import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.ExperimentalAdaptationTracker;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.FlowMetricsFileReporter;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.engine.region.impl.DefaultRegionExecutionPlanFactory;
import cs.bilkent.joker.flow.FlowDef;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ExperimentRunner
{

    private static final int KEY_RANGE = 100000;

    private static final int VALUE_RANGE = 10;

    private static final int TUPLES_PER_KEY = 8;

    private static final int KEYS_PER_INVOCATION = 256;

    public static void main ( String[] args ) throws InterruptedException, ExecutionException, TimeoutException, ClassNotFoundException,
                                                             IllegalAccessException, InstantiationException
    {
        final Map<String, Object> defaults = new HashMap<>();
        defaults.put( "reportDir", fromAnyRef( System.getProperty( "user.dir" ) ) );
        defaults.put( "keyRange", KEY_RANGE );
        defaults.put( "valueRange", VALUE_RANGE );
        defaults.put( "tuplesPerKey", TUPLES_PER_KEY );
        defaults.put( "keysPerInvocation", KEYS_PER_INVOCATION );
        final Config config = systemProperties().withFallback( ConfigValueFactory.fromMap( defaults ) );

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getAdaptationConfigBuilder().enableAdaptation();
        //        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getFlowDefOptimizerConfigBuilder().enableMergeRegions();

        final JokerConfig jokerConfig = configBuilder.build( config );

        final String flowDefFactoryClassName = config.getString( "flowFactory" );
        final FlowDefFactory flowDefFactory = (FlowDefFactory) Class.forName( flowDefFactoryClassName ).newInstance();
        final FlowDef flow = flowDefFactory.createFlow( jokerConfig );

        final String reportDir = config.getString( "reportDir" );
        final FlowMetricsFileReporter reporter = new FlowMetricsFileReporter( jokerConfig, new File( reportDir ) );
        reporter.init();

        final ExperimentalAdaptationTracker adaptationTracker = new ExperimentalAdaptationTracker( jokerConfig, reporter );
        final Joker joker = new JokerBuilder().setJokerConfig( jokerConfig )
                                              .setRegionExecutionPlanFactory( new DefaultRegionExecutionPlanFactory( jokerConfig ) )
                                              .setAdaptationTracker( adaptationTracker )
                                              .build();

        joker.run( flow );

        //        final Thread commander = createCommanderThread( joker, adaptationTracker );
        //        commander.start();

        while ( !adaptationTracker.isShutdownTriggered() )
        {
            sleepUninterruptibly( 1, SECONDS );
        }

        joker.shutdown().get( 60, SECONDS );
        System.exit( 0 );
    }

    private static Thread createCommanderThread ( final Joker joker, final ExperimentalAdaptationTracker adaptationTracker )
    {
        return new Thread( () ->
                           {
                               String line;
                               final BufferedReader reader = new BufferedReader( new InputStreamReader( System.in, Charsets.UTF_8 ) );
                               try
                               {
                                   while ( !adaptationTracker.isShutdownTriggered() && ( line = reader.readLine() ) != null )
                                   {
                                       final String command = line.trim();
                                       if ( command.isEmpty() )
                                       {
                                           continue;
                                       }
                                       else if ( "disable".equals( command ) )
                                       {
                                           try
                                           {
                                               try
                                               {
                                                   joker.disableAdaptation().get( 30, SECONDS );
                                               }
                                               catch ( InterruptedException e )
                                               {
                                                   Thread.currentThread().interrupt();
                                                   e.printStackTrace();
                                               }
                                               catch ( ExecutionException | TimeoutException e )
                                               {
                                                   e.printStackTrace();
                                               }
                                           }
                                           catch ( JokerException e )
                                           {
                                               e.printStackTrace();
                                           }
                                           return;
                                       }
                                       else
                                       {
                                           System.out.println( "PLEASE TYPE \"disable\" TO DISABLE ADAPTATION" );
                                       }
                                   }
                               }
                               catch ( IOException e )
                               {
                                   e.printStackTrace();
                               }
                           } );
    }

}
