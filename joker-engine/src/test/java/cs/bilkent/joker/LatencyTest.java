package cs.bilkent.joker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.operators.NopSinkOperator;
import cs.bilkent.joker.operators.PartitionedMapperOperator;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

//@Ignore
public class LatencyTest extends AbstractJokerTest
{

    private static final int KEY_RANGE = 10000;

    private static final int MULTIPLIER_VALUE = 100;


    static class ValueGenerator implements Consumer<Tuple>
    {

        private final int[] vals;
        private int curr;

        ValueGenerator ( final int keyRange )
        {
            final List<Integer> v = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                for ( int key = 0; key < keyRange; key++ )
                {
                    v.add( key );
                }
            }
            for ( int i = 0; i < 10; i++ )
            {
                shuffle( v );
            }
            vals = new int[ v.size() ];
            for ( int i = 0; i < v.size(); i++ )
            {
                vals[ i ] = v.get( i );
            }
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            //            LockSupport.parkNanos( 1 );

            final int key = vals[ curr++ ];
            final int value = key + 1;

            tuple.set( "key", key ).set( "value", value );
            if ( curr == vals.length )
            {
                curr = 0;
            }
        }

    }


    @Test
    public void test ()
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beaconConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beaconConfig ).build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 16; i++ )
                                                                              {
                                                                                  val = val * MULTIPLIER_VALUE - val;
                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", MapperOperator.class )
                                                         .setConfig( multiplierConfig )
                                                         .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon ).add( multiplier ).connect( "beacon", "multiplier" ).build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 2 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 3 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 511 );
        configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        joker.run( flow );

        sleepUninterruptibly( 300, SECONDS );
    }

    @Test
    public void test2 ()
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beaconConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beaconConfig ).build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 1024; i++ )
                                                                              {
                                                                                  val = val * MULTIPLIER_VALUE - val;
                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", MapperOperator.class )
                                                         .setConfig( multiplierConfig )
                                                         .build();

        final OperatorDef forEach = OperatorDefBuilder.newInstance( "nopSink", NopSinkOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( multiplier )
                                                 .add( forEach )
                                                 .connect( "beacon", "multiplier" )
                                                 .connect( "multiplier", "nopSink" )
                                                 .build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 64 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 3 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 255 );
        configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        joker.run( flow );

        sleepUninterruptibly( 300, SECONDS );
    }

    @Test
    public void test3 () throws InterruptedException, ExecutionException, TimeoutException
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beaconConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorRuntimeSchema beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                                                                                           .addOutputField( 0, "value", Integer.class )
                                                                                           .build();

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorRuntimeSchema multiplierSchema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                               .addInputField( 0, "value", Integer.class )
                                                                                               .addOutputField( 0, "key", Integer.class )
                                                                                               .addOutputField( 0, "mult", Integer.class )
                                                                                               .build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 1; i++ )
                                                                              {
                                                                                  val = val * MULTIPLIER_VALUE - val;
                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", PartitionedMapperOperator.class )
                                                         .setExtendingSchema( multiplierSchema )
                                                         .setConfig( multiplierConfig )
                                                         .setPartitionFieldNames( singletonList( "key" ) )
                                                         .build();

        final OperatorDef nopSink = OperatorDefBuilder.newInstance( "nopSink", NopSinkOperator.class ).build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( multiplier )
                                                 .add( nopSink )
                                                 .connect( "beacon", "multiplier" )
                                                 .connect( "multiplier", "nopSink" )
                                                 .build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 4096 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 3 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 15 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingPeriod( 20 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingCount( 5 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 15 );
        configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        final FlowExecPlan execPlan = joker.run( flow );

        sleepUninterruptibly( 10, SECONDS );

        joker.rebalanceRegion( execPlan.getVersion(), 1, 2 ).get( 30, SECONDS );

        sleepUninterruptibly( 120, SECONDS );

        joker.shutdown().get();
    }

    @Test
    public void test4 () throws InterruptedException, ExecutionException, TimeoutException
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beaconConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorRuntimeSchema beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                                                                                           .addOutputField( 0, "value", Integer.class )
                                                                                           .build();

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorRuntimeSchema multiplierSchema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                               .addInputField( 0, "value", Integer.class )
                                                                                               .addOutputField( 0, "key", Integer.class )
                                                                                               .addOutputField( 0, "mult", Integer.class )
                                                                                               .build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 1; i++ )
                                                                              {
                                                                                  val = val * MULTIPLIER_VALUE - val;
                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", PartitionedMapperOperator.class )
                                                         .setExtendingSchema( multiplierSchema )
                                                         .setConfig( multiplierConfig )
                                                         .setPartitionFieldNames( singletonList( "key" ) )
                                                         .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon ).add( multiplier ).connect( "beacon", "multiplier" ).build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 4096 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 3 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 15 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingPeriod( 20 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingCount( 5 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 15 );
        configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );
        configBuilder.getPartitionServiceConfigBuilder().setPartitionCount( 96 );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        final FlowExecPlan execPlan = joker.run( flow );

        sleepUninterruptibly( 10, SECONDS );

        joker.rebalanceRegion( execPlan.getVersion(), 1, 2 ).get( 30, SECONDS );

        sleepUninterruptibly( 120, SECONDS );

        joker.shutdown().get();
    }

}
