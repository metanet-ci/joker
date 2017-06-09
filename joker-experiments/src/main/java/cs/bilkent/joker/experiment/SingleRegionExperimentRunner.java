package cs.bilkent.joker.experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import static cs.bilkent.joker.experiment.BaseMultiplierOperator.MULTIPLICATION_COUNT;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEYS_PER_INVOCATION_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEY_RANGE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.TUPLES_PER_KEY_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.VALUE_RANGE_CONFIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class SingleRegionExperimentRunner
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
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        final JokerConfig jokerConfig = configBuilder.build( config );

        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );
        final List<Integer> operatorCosts = Arrays.stream( config.getString( "operatorCosts" ).split( "_" ) )
                                                  .map( Integer::parseInt )
                                                  .collect( toList() );

        final FlowDef flow = createFlow( keyRange, valueRange, tuplesPerKey, keysPerInvocation, operatorCosts );

        final String reportDir = config.getString( "reportDir" );
        final FlowMetricsFileReporter reporter = new FlowMetricsFileReporter( jokerConfig, new File( reportDir ) );
        reporter.init();

        final ExperimentalAdaptationTracker adaptationTracker = new ExperimentalAdaptationTracker( jokerConfig, reporter );
        final Joker joker = new JokerBuilder().setJokerConfig( jokerConfig )
                                              .setRegionExecutionPlanFactory( new DefaultRegionExecutionPlanFactory( jokerConfig ) )
                                              .setAdaptationTracker( adaptationTracker )
                                              .build();

        joker.run( flow );

        final Thread commander = createCommanderThread( joker, adaptationTracker );
        commander.start();

        while ( !adaptationTracker.isShutdownTriggered() )
        {
            sleepUninterruptibly( 1, SECONDS );
        }

        joker.shutdown().get( 60, SECONDS );
        System.exit( 0 );
    }

    private static FlowDef createFlow ( final int keyRange,
                                        final int valueRange,
                                        final int tuplesPerKey,
                                        final int keysPerInvocation,
                                        final List<Integer> operatorCosts )
    {
        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( KEY_RANGE_CONFIG_PARAMETER, keyRange );
        beaconConfig.set( VALUE_RANGE_CONFIG_PARAMETER, valueRange );
        beaconConfig.set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey );
        beaconConfig.set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptionerConfig = new OperatorConfig();
        ptionerConfig.set( MULTIPLICATION_COUNT, operatorCosts.get( 0 ) );

        OperatorDef ptioner = OperatorDefBuilder.newInstance( "m0", PartitionedStatefulMultiplierOperator.class )
                                                .setConfig( ptionerConfig ).setPartitionFieldNames( singletonList( "key1" ) )
                                                .build();

        flowDefBuilder.add( ptioner );

        flowDefBuilder.connect( beacon.getId(), ptioner.getId() );

        for ( int i = 1; i < operatorCosts.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( MULTIPLICATION_COUNT, operatorCosts.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
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
