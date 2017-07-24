package cs.bilkent.joker.engine.partition.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.JokerTest.StaticRegionExecutionPlanFactory2;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.ForEachOperator;
import static cs.bilkent.joker.operators.ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith( value = Parameterized.class )
public class FwdKeyIntegrationTest extends AbstractJokerTest
{

    private static final int KEY_RANGE = 1000;

    private static final int VALUE_RANGE = 5;

    @Parameters( name = "fwdComponentCount={0}, keyComponentCount={1}" )
    public static Collection<Object[]> data ()
    {
        return asList( new Object[][] { { 1, 2 }, { 1, 3 }, { 2, 3 }, { 1, 4 }, { 2, 4 }, { 3, 4 }, { 4, 5 } } );
    }


    private final int forwardedComponentCount;

    private final int keyComponentCount;

    public FwdKeyIntegrationTest ( final int forwardedComponentCount, final int keyComponentCount )
    {
        this.forwardedComponentCount = forwardedComponentCount;
        this.keyComponentCount = keyComponentCount;
    }

    @Test
    public void testPartitionedStatefulRegionWithForwardingKey () throws InterruptedException, ExecutionException, TimeoutException
    {
        final ValueGenerator valueGenerator = new ValueGenerator( KEY_RANGE, keyComponentCount, VALUE_RANGE );
        final ValueCollector valueCollector = new ValueCollector( keyComponentCount );

        final List<String> partitionFieldNames = new ArrayList<>();
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        final OperatorRuntimeSchemaBuilder passer1Schema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder passer2Schema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        final OperatorRuntimeSchemaBuilder foreachSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        for ( int i = 0; i < keyComponentCount; i++ )
        {
            partitionFieldNames.add( "key" + i );
            beaconSchema.addOutputField( 0, "key" + i, Integer.class );
            passer1Schema.addInputField( 0, "key" + i, Integer.class );
            passer2Schema.addInputField( 0, "key" + i, Integer.class );
            passer1Schema.addOutputField( 0, "key" + i, Integer.class );
            passer2Schema.addOutputField( 0, "key" + i, Integer.class );
            foreachSchema.addInputField( 0, "key" + i, Integer.class );
        }

        beaconSchema.addOutputField( 0, "value", Integer.class );
        passer1Schema.addInputField( 0, "value", Integer.class );
        passer1Schema.addOutputField( 0, "value", Integer.class );
        passer2Schema.addInputField( 0, "value", Integer.class );
        passer2Schema.addOutputField( 0, "value", Integer.class );
        foreachSchema.addInputField( 0, "value", Integer.class );

        final OperatorConfig beacon1Config = new OperatorConfig();
        beacon1Config.set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator );
        beacon1Config.set( TUPLE_COUNT_CONFIG_PARAMETER, 20 );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beacon1Config )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final List<String> forwardedPartitionFieldNames = partitionFieldNames.subList( 0, forwardedComponentCount );
        final OperatorDef passer1 = OperatorDefBuilder.newInstance( "passer1", ValuePasserOperator.class )
                                                      .setExtendingSchema( passer1Schema )
                                                      .setPartitionFieldNames( forwardedPartitionFieldNames )
                                                      .build();

        final OperatorDef passer2 = OperatorDefBuilder.newInstance( "passer2", ValuePasserOperator.class )
                                                      .setExtendingSchema( passer2Schema )
                                                      .setPartitionFieldNames( partitionFieldNames )
                                                      .build();

        final OperatorConfig collectorConfig = new OperatorConfig();
        collectorConfig.set( CONSUMER_FUNCTION_CONFIG_PARAMETER, valueCollector );

        final OperatorDef collector = OperatorDefBuilder.newInstance( "collector", ForEachOperator.class )
                                                        .setConfig( collectorConfig )
                                                        .setExtendingSchema( foreachSchema )
                                                        .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( passer1 )
                                                 .add( passer2 )
                                                 .add( collector )
                                                 .connect( "beacon", "passer1" )
                                                 .connect( "passer1", "passer2" )
                                                 .connect( "passer2", "collector" )
                                                 .build();

        final JokerConfig jokerConfig = new JokerConfig();
        final StaticRegionExecutionPlanFactory2 regionExecPlanFactory = new StaticRegionExecutionPlanFactory2( jokerConfig, 4 );
        final Joker joker = new JokerBuilder().setRegionExecutionPlanFactory( regionExecPlanFactory ).setJokerConfig( jokerConfig ).build();

        joker.run( flow );

        sleepUninterruptibly( 10, SECONDS );

        joker.shutdown().get( 60, SECONDS );

        System.out.println( "Value generator 1 is invoked " + valueGenerator.invocationCount.get() + " times." );
        System.out.println( "Value generator 2 is invoked " + valueCollector.invocationCount.get() + " times." );

        for ( Map.Entry<List<Integer>, AtomicInteger> e : valueGenerator.generatedValues.entrySet() )
        {
            final List<Integer> key = e.getKey();
            final AtomicInteger expected = e.getValue();
            final AtomicInteger value = valueCollector.collectedValues.get( key );
            assertNotNull( "key: " + key + " expected: " + expected + " no value", value );
            assertEquals( "key: " + key + " expected: " + expected + " value: " + value, expected.get(), value.get() );
        }
    }

    @OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = PARTITIONED_STATEFUL )
    public static class ValuePasserOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
        }

        @Override
        public void invoke ( final InvocationContext context )
        {
            final Tuples input = context.getInput();
            final Tuples output = context.getOutput();
            input.getTuplesByDefaultPort().forEach( output::add );
        }

    }


    static class ValueGenerator implements Consumer<Tuple>
    {

        static final Random RANDOM = new Random();

        private final int keyRange;

        private final int keyComponentCount;

        private final int valueRange;

        private final Map<List<Integer>, AtomicInteger> generatedValues = new ConcurrentHashMap<>();

        private final AtomicInteger invocationCount = new AtomicInteger();

        ValueGenerator ( final int keyRange, final int keyComponentCount, final int valueRange )
        {
            this.keyRange = keyRange;
            this.keyComponentCount = keyComponentCount;
            this.valueRange = valueRange;
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            sleepUninterruptibly( 1, MICROSECONDS );
            invocationCount.incrementAndGet();

            final List<Integer> key = new ArrayList<>();
            for ( int i = 0; i < keyComponentCount; i++ )
            {
                final int c = RANDOM.nextInt( keyRange );
                key.add( c );
                tuple.set( "key" + i, c );
            }

            final int value = RANDOM.nextInt( valueRange ) + 1;
            tuple.set( "value", value );

            final AtomicInteger valueHolder = generatedValues.computeIfAbsent( key, k -> new AtomicInteger() );
            final int existing = valueHolder.get();
            valueHolder.set( existing + value );
        }

    }


    static class ValueCollector implements Consumer<Tuple>
    {

        private final int keyComponentCount;

        private final Map<List<Integer>, AtomicInteger> collectedValues = new ConcurrentHashMap<>();

        private final AtomicInteger invocationCount = new AtomicInteger();

        ValueCollector ( final int keyComponentCount )
        {
            this.keyComponentCount = keyComponentCount;
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            invocationCount.incrementAndGet();

            final List<Integer> key = new ArrayList<>();
            for ( int i = 0; i < keyComponentCount; i++ )
            {
                key.add( tuple.getInteger( "key" + i ) );
            }

            final int value = tuple.getInteger( "value" );

            final AtomicInteger valueHolder = collectedValues.computeIfAbsent( key, k -> new AtomicInteger() );
            final int existing = valueHolder.get();
            valueHolder.set( existing + value );
        }

    }

}
