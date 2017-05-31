package cs.bilkent.joker.experiment;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
import cs.bilkent.joker.engine.region.impl.DefaultRegionExecutionPlanFactory;
import static cs.bilkent.joker.experiment.BaseMultiplierOperator.MULTIPLICATION_COUNT;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class ExperimentRunner
{

    private static final int KEY_RANGE = 100000;

    private static final int TUPLES_PER_KEY = 8;

    public static void main ( String[] args ) throws InterruptedException, ExecutionException, TimeoutException, ClassNotFoundException,
                                                             IllegalAccessException, InstantiationException
    {
        final Map<String, Object> defaults = new HashMap<>();
        defaults.put( "reportDir", fromAnyRef( System.getProperty( "user.dir" ) ) );
        defaults.put( "keyRange", KEY_RANGE );
        defaults.put( "tuplesPerKey", TUPLES_PER_KEY );
        final Config config = systemProperties().withFallback( ConfigValueFactory.fromMap( defaults ) );

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getAdaptationConfigBuilder().enableAdaptation();
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        final JokerConfig jokerConfig = configBuilder.build( config );

        final int keyRange = config.getInt( "keyRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final List<Integer> operatorCosts = Arrays.stream( config.getString( "operatorCosts" ).split( "_" ) )
                                                  .map( Integer::parseInt )
                                                  .collect( toList() );

        final FlowDef flow = createFlow( keyRange, tuplesPerKey, operatorCosts );

        final String reportDir = config.getString( "reportDir" );
        final FlowMetricsFileReporter reporter = new FlowMetricsFileReporter( jokerConfig, new File( reportDir ) );
        reporter.init();

        final ExperimentalAdaptationTracker adaptationTracker = new ExperimentalAdaptationTracker( jokerConfig, reporter );
        final Joker joker = new JokerBuilder().setJokerConfig( jokerConfig )
                                              .setRegionExecutionPlanFactory( new DefaultRegionExecutionPlanFactory( jokerConfig ) )
                                              .setAdaptationTracker( adaptationTracker )
                                              .build();

        joker.run( flow );

        while ( !adaptationTracker.isShutdownTriggered() )
        {
            sleepUninterruptibly( 1, SECONDS );
        }

        joker.shutdown().get( 60, SECONDS );
    }

    private static FlowDef createFlow ( final int keyRange, final int tuplesPerKey, final List<Integer> operatorCosts )
    {
        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( TUPLE_COUNT_CONFIG_PARAMETER, 4096 );
        beaconConfig.set( TUPLE_POPULATOR_CONFIG_PARAMETER, new BeaconFn( new Random(), keyRange, tuplesPerKey ) );

        OperatorRuntimeSchemaBuilder beaconSchemaBuilder = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchemaBuilder.addOutputField( 0, "key", Integer.class )
                           .addOutputField( 0, "val1", Integer.class )
                           .addOutputField( 0, "val2", Integer.class );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", BeaconOperator.class )
                                               .setConfig( beaconConfig )
                                               .setExtendingSchema( beaconSchemaBuilder )
                                               .build();

        flowDefBuilder.add( beacon );

        OperatorRuntimeSchemaBuilder ptionerSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        OperatorRuntimeSchema ptionerSchema = ptionerSchemaBuilder.addInputField( 0, "key", Integer.class )
                                                                  .addInputField( 0, "val1", Integer.class )
                                                                  .addInputField( 0, "val2", Integer.class )
                                                                  .addOutputField( 0, "key", Integer.class )
                                                                  .addOutputField( 0, "val1", Integer.class )
                                                                  .addOutputField( 0, "val2", Integer.class )
                                                                  .build();

        OperatorConfig ptionerConfig = new OperatorConfig();
        ptionerConfig.set( MULTIPLICATION_COUNT, operatorCosts.get( 0 ) );

        OperatorDef ptioner = OperatorDefBuilder.newInstance( "m0", PartitionedStatefulMultiplierOperator.class )
                                                .setExtendingSchema( ptionerSchema )
                                                .setConfig( ptionerConfig )
                                                .setPartitionFieldNames( singletonList( "key" ) )
                                                .build();

        flowDefBuilder.add( ptioner );

        flowDefBuilder.connect( beacon.getId(), ptioner.getId() );

        for ( int i = 1; i < operatorCosts.size(); i++ )
        {
            OperatorRuntimeSchemaBuilder multiplierSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
            OperatorRuntimeSchema multiplierSchema = multiplierSchemaBuilder.addInputField( 0, "key", Integer.class )
                                                                            .addInputField( 0, "val1", Integer.class )
                                                                            .addInputField( 0, "val2", Integer.class )
                                                                            .addOutputField( 0, "key", Integer.class )
                                                                            .addOutputField( 0, "val1", Integer.class )
                                                                            .addOutputField( 0, "val2", Integer.class )
                                                                            .build();

            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( StatelessMultiplierOperator.MULTIPLICATION_COUNT, operatorCosts.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m" + i, StatelessMultiplierOperator.class )
                                                       .setExtendingSchema( multiplierSchema )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
    }

}
