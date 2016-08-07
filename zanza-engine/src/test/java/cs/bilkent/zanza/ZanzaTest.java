package cs.bilkent.zanza;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.zanza.Zanza.ZanzaBuilder;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.region.FlowDeploymentDef.RegionGroup;
import cs.bilkent.zanza.engine.region.RegionConfig;
import cs.bilkent.zanza.engine.region.RegionDef;
import cs.bilkent.zanza.engine.region.impl.AbstractRegionConfigFactory;
import cs.bilkent.zanza.flow.FlowDef;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST_BUT_SAME_ON_ALL_PORTS;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAll;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.zanza.operators.BeaconOperator;
import static cs.bilkent.zanza.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.BeaconOperator.TUPLE_GENERATOR_CONFIG_PARAMETER;
import cs.bilkent.zanza.operators.ForEachOperator;
import static cs.bilkent.zanza.operators.ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER;
import cs.bilkent.zanza.operators.MapperOperator;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.zanza.testutils.ZanzaAbstractTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class ZanzaTest extends ZanzaAbstractTest
{

    private static final int PARTITIONED_STATEFUL_REGION_REPLICA_COUNT = 4;

    private static final int KEY_RANGE = 1000;

    private static final int VALUE_RANGE = 5;

    private static final int MULTIPLIER_VALUE = 100;

    @Test
    public void testEndToEndSystem () throws InterruptedException, ExecutionException, TimeoutException
    {

        final ValueGenerator valueGenerator1 = new ValueGenerator( KEY_RANGE, VALUE_RANGE );
        final ValueGenerator valueGenerator2 = new ValueGenerator( KEY_RANGE, VALUE_RANGE );
        final ValueCollector valueCollector = new ValueCollector( KEY_RANGE );

        final OperatorConfig beacon1Config = new OperatorConfig();
        beacon1Config.set( TUPLE_GENERATOR_CONFIG_PARAMETER, valueGenerator1 );
        beacon1Config.set( TUPLE_COUNT_CONFIG_PARAMETER, 2 );

        final OperatorRuntimeSchemaBuilder beacon1Schema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beacon1Schema.getOutputPortSchemaBuilder( 0 ).addField( "key", Integer.class ).addField( "value", Integer.class );

        final OperatorDef beacon1 = OperatorDefBuilder.newInstance( "beacon1", BeaconOperator.class )
                                                      .setConfig( beacon1Config )
                                                      .setExtendingSchema( beacon1Schema )
                                                      .build();

        final OperatorConfig beacon2Config = new OperatorConfig();
        beacon2Config.set( TUPLE_GENERATOR_CONFIG_PARAMETER, valueGenerator2 );
        beacon2Config.set( TUPLE_COUNT_CONFIG_PARAMETER, 3 );

        final OperatorRuntimeSchemaBuilder beacon2Schema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beacon2Schema.getOutputPortSchemaBuilder( 0 ).addField( "key", Integer.class ).addField( "value", Integer.class );

        final OperatorDef beacon2 = OperatorDefBuilder.newInstance( "beacon2", BeaconOperator.class )
                                                      .setConfig( beacon2Config )
                                                      .setExtendingSchema( beacon2Schema )
                                                      .build();

        final OperatorRuntimeSchemaBuilder joinSchema = new OperatorRuntimeSchemaBuilder( 2, 1 );
        joinSchema.addInputField( 0, "key", Integer.class )
                  .addInputField( 0, "value", Integer.class )
                  .addInputField( 1, "key", Integer.class )
                  .addInputField( 1, "value", Integer.class )
                  .addOutputField( 0, "key", Integer.class )
                  .addOutputField( 0, "value", Integer.class );

        final OperatorDef join = OperatorDefBuilder.newInstance( "joiner", JoinOperator.class )
                                                   .setExtendingSchema( joinSchema )
                                                   .setPartitionFieldNames( singletonList( "key" ) )
                                                   .build();

        final OperatorRuntimeSchemaBuilder summerSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        summerSchema.addInputField( 0, "key", Integer.class )
                    .addInputField( 0, "value", Integer.class )
                    .addOutputField( 0, "key", Integer.class )
                    .addOutputField( 0, "sum", Integer.class );

        final OperatorDef summer = OperatorDefBuilder.newInstance( "summer", SummerOperator.class )
                                                     .setExtendingSchema( summerSchema )
                                                     .setPartitionFieldNames( singletonList( "key" ) )
                                                     .build();

        final OperatorConfig multiplierConfig = new OperatorConfig();
        multiplierConfig.set( MAPPER_CONFIG_PARAMETER, (Function<Tuple, Tuple>) input ->
        {
            final Tuple output = new Tuple();
            output.set( "key", input.get( "key" ) );
            output.set( "mult", MULTIPLIER_VALUE * input.getInteger( "sum" ) );
            return output;
        } );

        final OperatorRuntimeSchemaBuilder multiplierSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        multiplierSchema.addInputField( 0, "key", Integer.class )
                        .addInputField( 0, "sum", Integer.class )
                        .addOutputField( 0, "key", Integer.class )
                        .addOutputField( 0, "mult", Integer.class );

        final OperatorDef multiplier = OperatorDefBuilder.newInstance( "multiplier", MapperOperator.class )
                                                         .setConfig( multiplierConfig )
                                                         .setExtendingSchema( multiplierSchema )
                                                         .build();

        final OperatorConfig collectorConfig = new OperatorConfig();
        collectorConfig.set( CONSUMER_FUNCTION_CONFIG_PARAMETER, valueCollector );

        final OperatorRuntimeSchemaBuilder foreachSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        foreachSchema.addInputField( 0, "key", Integer.class ).addInputField( 0, "mult", Integer.class );

        final OperatorDef collector = OperatorDefBuilder.newInstance( "collector", ForEachOperator.class )
                                                        .setConfig( collectorConfig )
                                                        .setExtendingSchema( foreachSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon1 )
                                                 .add( beacon2 )
                                                 .add( join )
                                                 .add( summer )
                                                 .add( multiplier )
                                                 .add( collector )
                                                 .connect( "beacon1", "joiner", 0 )
                                                 .connect( "beacon2", "joiner", 1 )
                                                 .connect( "joiner", "summer" )
                                                 .connect( "summer", "multiplier" )
                                                 .connect( "multiplier", "collector" )
                                                 .build();

        final ZanzaConfig zanzaConfig = new ZanzaConfig();
        final StaticRegionConfigFactory regionConfigFactory = new StaticRegionConfigFactory( zanzaConfig,
                                                                                             PARTITIONED_STATEFUL_REGION_REPLICA_COUNT );
        final Zanza zanza = new ZanzaBuilder().setRegionConfigFactory( regionConfigFactory ).setZanzaConfig( zanzaConfig ).build();

        zanza.start( flow );

        sleepUninterruptibly( 30, SECONDS );

        zanza.shutdown().get( 2, MINUTES );

        System.out.println( "Value generator 1 is invoked " + valueGenerator1.invocationCount.get() + " times." );
        System.out.println( "Value generator 2 is invoked " + valueGenerator2.invocationCount.get() + " times." );
        System.out.println( "Collector is invoked " + valueCollector.invocationCount.get() + " times." );

        for ( int i = 0; i < valueCollector.values.length(); i++ )
        {
            final int expected = ( valueGenerator1.generatedValues[ i ].intValue() + valueGenerator2.generatedValues[ i ].intValue() )
                                 * MULTIPLIER_VALUE;
            final int actual = valueCollector.values.get( i );
            assertEquals( expected, actual );
        }
    }

    static class StaticRegionConfigFactory extends AbstractRegionConfigFactory
    {

        private final int replicaCount;

        public StaticRegionConfigFactory ( final ZanzaConfig zanzaConfig, final int replicaCount )
        {
            super( zanzaConfig );
            this.replicaCount = replicaCount;
        }

        @Override
        protected List<RegionConfig> createRegionConfigs ( final RegionGroup regionGroup )
        {
            final List<RegionDef> regions = regionGroup.getRegions();
            final int replicaCount = regions.get( 0 ).getRegionType() == PARTITIONED_STATEFUL ? this.replicaCount : 1;
            final List<List<Integer>> pipelineStartIndicesList = new ArrayList<>();
            for ( RegionDef region : regions )
            {
                final int operatorCount = region.getOperatorCount();
                final List<Integer> pipelineStartIndices = operatorCount == 1 ? singletonList( 0 ) : asList( 0, operatorCount / 2 );
                pipelineStartIndicesList.add( pipelineStartIndices );
            }

            final List<RegionConfig> regionConfigs = new ArrayList<>( regions.size() );
            for ( int i = 0; i < regions.size(); i++ )
            {
                regionConfigs.add( new RegionConfig( regions.get( 0 ), pipelineStartIndicesList.get( i ), replicaCount ) );
            }

            return regionConfigs;
        }
    }


    static class ValueGenerator implements Function<Random, Tuple>
    {

        private final int keyRange;

        private final int valueRange;

        private final AtomicInteger[] generatedValues;

        private final AtomicInteger invocationCount = new AtomicInteger();

        public ValueGenerator ( final int keyRange, final int valueRange )
        {
            this.keyRange = keyRange;
            this.valueRange = valueRange;
            this.generatedValues = new AtomicInteger[ keyRange ];
            for ( int i = 0; i < keyRange; i++ )
            {
                this.generatedValues[ i ] = new AtomicInteger( 0 );
            }
        }

        @Override
        public Tuple apply ( final Random random )
        {
            sleepUninterruptibly( 1, MICROSECONDS );
            invocationCount.incrementAndGet();

            final int key = random.nextInt( keyRange );
            final int value = random.nextInt( valueRange ) + 1;

            final AtomicInteger valueHolder = generatedValues[ key ];
            int existing;
            do
            {
                existing = valueHolder.get();
            } while ( !valueHolder.compareAndSet( existing, existing + value ) );

            final Tuple tuple = new Tuple();
            tuple.set( "key", key );
            tuple.set( "value", value );
            return tuple;
        }

    }


    static class ValueCollector implements Consumer<Tuple>
    {

        private final AtomicReferenceArray<Integer> values;

        private final AtomicInteger invocationCount = new AtomicInteger();

        public ValueCollector ( final int keyRange )
        {
            this.values = new AtomicReferenceArray<>( keyRange );
            for ( int i = 0; i < keyRange; i++ )
            {
                this.values.set( i, 0 );
            }
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            values.set( tuple.getInteger( "key" ), tuple.getInteger( "mult" ) );
            invocationCount.incrementAndGet();
        }

    }


    @OperatorSpec( inputPortCount = 2, outputPortCount = 1, type = PARTITIONED_STATEFUL )
    public static class JoinOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnAll( AT_LEAST_BUT_SAME_ON_ALL_PORTS, 2, 1, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            final Tuples input = invocationContext.getInput();
            final Tuples output = invocationContext.getOutput();
            input.getTuples( 0 ).forEach( output::add );
            input.getTuples( 1 ).forEach( output::add );
        }

    }


    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = PARTITIONED_STATEFUL )
    public static class SummerOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            final KVStore kvStore = invocationContext.getKVStore();
            final Tuples input = invocationContext.getInput();
            final Tuples output = invocationContext.getOutput();

            for ( Tuple tuple : input.getTuples( 0 ) )
            {
                final Object key = tuple.get( "key" );
                final int currSum = kvStore.getIntegerValueOrDefault( key, 0 );
                final int newSum = currSum + tuple.getInteger( "value" );

                kvStore.set( key, newSum );

                final Tuple result = new Tuple();
                result.set( "key", key );
                result.set( "sum", newSum );
                output.add( result );
            }
        }

    }

}
