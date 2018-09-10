package cs.bilkent.joker.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.typesafe.config.Config;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.typesafe.config.ConfigFactory.systemProperties;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LatencyTestMain
{

    private static final int KEY_RANGE = 10000;

    private static final int MULTIPLIER_VALUE = 100;

    public static final String PRODUCED_TUPLE_COUNT_PER_SOURCE_INVOCATION = "producedTupleCountPerSourceInvocation";

    public static final String MAPPER_OPERATOR_BATCH_SIZE = "mapperOperatorBatchSize";

    public static final String MULTIPLICATION_COUNT = "multiplicationCount";


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


    public static void main ( String[] args ) throws ExecutionException, InterruptedException, TimeoutException
    {
        final Config config = systemProperties();
        final int producedTupleCountPerSourceInvocation = config.getInt( PRODUCED_TUPLE_COUNT_PER_SOURCE_INVOCATION );
        final int mapperOperatorBatchSize = config.getInt( MAPPER_OPERATOR_BATCH_SIZE );
        final int multiplicationCount = config.getInt( MULTIPLICATION_COUNT );

        System.out.println( ">>>>> PRODUCED_TUPLE_COUNT_PER_SOURCE_INVOCATION: " + producedTupleCountPerSourceInvocation );
        System.out.println( ">>>>> mapperOperatorBatchSize: " + mapperOperatorBatchSize );
        System.out.println( ">>>>> MULTIPLICATION_COUNT: " + multiplicationCount );

        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE );
        final OperatorConfig beacon1Config = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                 .set( TUPLE_COUNT_CONFIG_PARAMETER,
                                                                       producedTupleCountPerSourceInvocation );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class ).setConfig( beacon1Config ).build();

        final OperatorConfig multiplierConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER,
                                                                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> {
                                                                              int val = input.getInteger( "value" );
                                                                              for ( int i = 0; i < multiplicationCount; i++ )
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
        configBuilder.getTupleQueueDrainerConfigBuilder().setMaxBatchSize( mapperOperatorBatchSize );
        configBuilder.getTupleQueueManagerConfigBuilder().setMultiThreadedQueueDrainLimit( 1 );
        configBuilder.getMetricManagerConfigBuilder().setTickMask( 3 );
        configBuilder.getMetricManagerConfigBuilder().setPipelineMetricsScanningPeriodInMillis( 1000 );
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        configBuilder.getPipelineManagerConfigBuilder().setLatencyTickMask( 0 );
        configBuilder.getPipelineManagerConfigBuilder().setLatencyComponentTickMask( 2047 );

        final Joker joker = new JokerBuilder().setJokerConfig( configBuilder.build() ).build();

        joker.run( flow );

        sleepUninterruptibly( 120, SECONDS );

        joker.shutdown().get( 60, SECONDS );
    }

}
