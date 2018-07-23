package cs.bilkent.joker;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.ForEachOperator;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
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
        final OperatorConfig beacon1Config = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                 .set( TUPLE_COUNT_CONFIG_PARAMETER, 1024 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beacon1Config ).build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 1; i++ )
                                                                              {
                                                                                  val = val * MULTIPLIER_VALUE - val;
                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", MapperOperator.class )
                                                         .setConfig( multiplierConfig )
                                                         .build();

        final OperatorConfig forEachConfig = new OperatorConfig();
        forEachConfig.set( ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> {
        } );

        final FlowDef flow = new FlowDefBuilder().add( beacon ).add( multiplier )
                                                 .connect( "beacon", "multiplier" )
                                                 .build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 64 );
        configBuilder.getTupleQueueManagerConfigBuilder().setMultiThreadedQueueDrainLimit( 1 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        joker.run( flow );

        sleepUninterruptibly( 300, SECONDS );
    }

    @Test
    public void test2 ()
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beacon1Config = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                 .set( TUPLE_COUNT_CONFIG_PARAMETER, 1 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beacon1Config ).build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < 64; i++ )
                                                                              {

                                                                                  val = val * MULTIPLIER_VALUE - val;

                                                                              }
                                                                              val = val * MULTIPLIER_VALUE - val;
                                                                              output.set( "key", input.get( "key" ) ).set( "mult", val );
                                                                          } );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", MapperOperator.class )
                                                         .setConfig( multiplierConfig )
                                                         .build();

        final OperatorConfig forEachConfig = new OperatorConfig().set( ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER,
                                                                       (Consumer<Tuple>) tuple -> {
                                                                       } );

        final OperatorDef forEach = OperatorDefBuilder.newInstance( "foreach", ForEachOperator.class ).setConfig( forEachConfig ).build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( multiplier )
                                                 .add( forEach )
                                                 .connect( "beacon", "multiplier" )
                                                 .connect( "multiplier", "foreach" )
                                                 .build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 16 );
        configBuilder.getTupleQueueManagerConfigBuilder().setMultiThreadedQueueDrainLimit( 1 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        joker.run( flow );

        sleepUninterruptibly( 300, SECONDS );
    }

    @Test
    public void test3 ()
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beacon1Config = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                 .set( TUPLE_COUNT_CONFIG_PARAMETER, 32 );

        final OperatorRuntimeSchema beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                                                                                           .build();

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beacon1Config )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorDef ptioned = OperatorDefBuilder.newInstance( "p", DummyOperator.class )
                                                      .setPartitionFieldNames( singletonList( "key" ) )
                                                      .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon ).add( ptioned ).connect( "beacon", "p" ).build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( 16 );
        configBuilder.getTupleQueueManagerConfigBuilder().setMultiThreadedQueueDrainLimit( 1 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 1 );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        final FlowExecPlan execPlan = joker.run( flow );

        sleepUninterruptibly( 60, SECONDS );

        joker.rebalanceRegion( execPlan.getVersion(), 1, 2 );

        sleepUninterruptibly( 60, SECONDS );

    }

    @OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = @SchemaField( name = "key", type = Integer.class ) ) )
    public static class DummyOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            ctx.getInputTuplesByDefaultPort().forEach( ctx::output );
        }
    }

}
