package cs.bilkent.joker;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.RegionExecPlan;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.PartitionedMapperOperator;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ModelTest extends AbstractJokerTest
{

    private static final int KEY_RANGE = 10000;

    private static final int MULTIPLIER_VALUE = 271;


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
            final int key = vals[ curr++ ];
            final int value = key + 1;

            tuple.set( "key", key ).set( "value", value );
            if ( curr == vals.length )
            {
                curr = 0;
            }
        }
    }


    private JokerConfig config;

    private FlowDef flow;

    @Before
    public void init ()
    {
        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 4096 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineReplicaRunnerConfigBuilder().enforceThreadAffinity( true );

        // disable latency tracking...
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 16383 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyStageTickMask( 16383 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingPeriod( 100_000_000 );
        configBuilder.getPipelineManagerConfigBuilder().setInterArrivalTimeTrackingCount( 1 );

        config = configBuilder.build();

        final int emittedTupleCountPerSourceInvocation = 1;
        final int multiplicationCount1 = 1;
        final int multiplicationCount2 = 1;

        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig sourceConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER,
                                                                      emittedTupleCountPerSourceInvocation );

        final OperatorRuntimeSchema sourceSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                                                                                           .addOutputField( 0, "value", Integer.class )
                                                                                           .build();

        final OperatorDef source = OperatorDefBuilder.newInstance( "src", BeaconOperator.class )
                                                     .setConfig( sourceConfig )
                                                     .setExtendingSchema( sourceSchema )
                                                     .build();

        final OperatorRuntimeSchema multiplier1Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                                .addInputField( 0, "value", Integer.class )
                                                                                                .addOutputField( 0, "key", Integer.class )
                                                                                                .addOutputField( 0, "mult1", Integer.class )
                                                                                                .build();

        final BiConsumer<Tuple, Tuple> multiplier1Func = ( input, output ) -> {
            int val = input.getInteger( "value" );
            for ( int i = 0; i < multiplicationCount1; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult1", val );
        };

        final OperatorConfig multiplier1Config = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                           multiplier1Func );

        final OperatorDef multiplier1 = OperatorDefBuilder.newInstance( "mult1", PartitionedMapperOperator.class )
                                                          .setExtendingSchema( multiplier1Schema )
                                                          .setConfig( multiplier1Config )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();

        final OperatorRuntimeSchema multiplier2Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                                .addInputField( 0, "mult1", Integer.class )
                                                                                                .addOutputField( 0, "key", Integer.class )
                                                                                                .addOutputField( 0, "mult2", Integer.class )
                                                                                                .build();

        final BiConsumer<Tuple, Tuple> multiplier2Func = ( input, output ) -> {
            int val = input.getInteger( "mult1" );
            for ( int i = 0; i < multiplicationCount2; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            output.set( "key", input.get( "key" ) ).set( "mult2", val );
        };

        final OperatorConfig multiplier2Config = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                           multiplier2Func );

        final OperatorDef multiplier2 = OperatorDefBuilder.newInstance( "mult2", PartitionedMapperOperator.class )
                                                          .setExtendingSchema( multiplier2Schema )
                                                          .setConfig( multiplier2Config )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();

        flow = new FlowDefBuilder().add( source )
                                   .add( multiplier1 )
                                   .add( multiplier2 )
                                   .connect( source.getId(), multiplier1.getId() )
                                   .connect( multiplier1.getId(), multiplier2.getId() )
                                   .build();
    }

    @Test
    public void test_model1 ()
    {
        final Joker joker = new JokerBuilder().setJokerConfig( config ).build();

        final FlowExecPlan execPlan = joker.run( flow );

        sleepUninterruptibly( 10, SECONDS );

        final RegionExecPlan ptionedStatefulRegionExecPlan = getPartitionedStatefulRegion( execPlan );

        // the ptioned stateful region has a single pipeline, which contains 2 operators.
        // splits the pipeline from the 2nd operator (operatorIndex=1), which is the last parameter
        joker.splitPipeline( execPlan.getVersion(), ptionedStatefulRegionExecPlan.getPipelineId( 0 ), singletonList( 1 ) ).join();

        sleepUninterruptibly( 2, MINUTES );
    }

    @Test
    public void test_model2 ()
    {
        final Joker joker = new JokerBuilder().setJokerConfig( config ).build();

        final FlowExecPlan execPlan = joker.run( flow );

        sleepUninterruptibly( 10, SECONDS );

        // uncomment the following lines to change replica count
        //        int newReplicaCount = 2;
        //        joker.rebalanceRegion( execPlan.getVersion(), getPartitionedStatefulRegion( execPlan ).getRegionId(), newReplicaCount);

        sleepUninterruptibly( 120, SECONDS );
    }

    @NotNull
    private RegionExecPlan getPartitionedStatefulRegion ( final FlowExecPlan execPlan )
    {
        return execPlan.getRegionExecPlans()
                       .stream()
                       .filter( r -> r.getRegionType() == PARTITIONED_STATEFUL )
                       .findFirst()
                       .orElseThrow( IllegalStateException::new );
    }

}
