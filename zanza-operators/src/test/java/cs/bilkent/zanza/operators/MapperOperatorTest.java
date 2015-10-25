package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.operator.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class MapperOperatorTest
{

    private final MapperOperator operator = new MapperOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMapper ()
    {
        operator.init( initContext );
    }

    @Test
    public void shouldInitializeWithMapper ()
    {
        final Function<Tuple, Tuple> mapper = tuple -> tuple;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithInvalidMapper ()
    {
        final Function<String, String> mapper = str -> str;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithTupleCount ()
    {
        final Function<String, String> mapper = str -> str;
        final int tupleCount = 5;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        initContext.getConfig().set( MapperOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, tupleCount );
    }

    @Test
    public void shouldProcessFunctionReturnInitializedTupleCountInSchedulingStrategy ()
    {
        final Function<String, String> mapper = str -> str;
        final int tupleCount = 5;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        initContext.getConfig().set( MapperOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        operator.init( initContext );
        final InvocationResult result = operator.process( new SimpleInvocationContext( new PortsToTuples(), InvocationReason.SUCCESS ) );
        assertScheduleWhenTuplesAvailableStrategy( result.getSchedulingStrategy(), tupleCount );
    }

    @Test
    public void shouldMapSingleTupleForSuccessfulInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        shouldMultiplyCountValuesBy2( new SimpleInvocationContext( portsToTuples, InvocationReason.SUCCESS ) );
    }

    @Test
    public void shouldMapSingleTupleForErroneousInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        shouldMultiplyCountValuesBy2( new SimpleInvocationContext( portsToTuples, InvocationReason.SHUTDOWN ) );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForSuccessfulInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( InvocationReason.SUCCESS );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForErroneousInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( InvocationReason.INPUT_PORT_CLOSED );
    }

    private void shouldNotMapWitHInvalidMapperFor ( final InvocationReason invocationReason )
    {
        final Function<String, String> mapper = str -> str;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        operator.init( initContext );
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        operator.process( new SimpleInvocationContext( portsToTuples, invocationReason ) );
    }

    @Test
    public void shouldMapMultipleTuplesForSuccessfulInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples();
        portsToTuples.add( new Tuple( "count", 5 ) );
        portsToTuples.add( new Tuple( "count", 10 ) );
        shouldMultiplyCountValuesBy2( new SimpleInvocationContext( portsToTuples, InvocationReason.SUCCESS ) );
    }

    @Test
    public void shouldMapMultipleTuplesForErroneousInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples();
        portsToTuples.add( new Tuple( "count", 5 ) );
        portsToTuples.add( new Tuple( "count", 10 ) );
        shouldMultiplyCountValuesBy2( new SimpleInvocationContext( portsToTuples, InvocationReason.SUCCESS ) );
    }

    private void shouldMultiplyCountValuesBy2 ( final InvocationContext invocationContext )
    {
        initializeOperatorWithMultipleBy2Mapper();

        final InvocationResult result = operator.process( invocationContext );
        final List<Tuple> inputTuples = invocationContext.getInputTuples().getTuplesByDefaultPort();
        final List<Tuple> outputTuples = result.getOutputTuples().getTuplesByDefaultPort();
        assertThat( outputTuples, hasSize( inputTuples.size() ) );
        for ( int i = 0; i < outputTuples.size(); i++ )
        {
            final Tuple inputTuple = inputTuples.get( i );
            final Tuple outputTuple = outputTuples.get( i );
            assertThat( outputTuple.getInteger( "count" ), equalTo( 2 * inputTuple.getInteger( "count" ) ) );
        }
    }

    @Test
    public void shouldReturnAnyTuplesAvailableSchedulingStrategyForSuccessfulInvocation ()
    {
        initializeOperatorWithMultipleBy2Mapper();

        final InvocationResult result = operator.process( new SimpleInvocationContext( new PortsToTuples(), InvocationReason.SUCCESS ) );

        assertScheduleWhenTuplesAvailableStrategy( result.getSchedulingStrategy(), ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldReturnAnyTuplesAvailableSchedulingStrategyForErroneousInvocation ()
    {
        initializeOperatorWithMultipleBy2Mapper();

        final InvocationResult result = operator.process( new SimpleInvocationContext( new PortsToTuples(), InvocationReason.SHUTDOWN ) );

        assertThat( result.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
    }

    private void initializeOperatorWithMultipleBy2Mapper ()
    {
        final Function<Tuple, Tuple> mapper = input -> {
            Tuple output = new Tuple();
            output.set( "count", input.getInteger( "count" ) * 2 );
            return output;
        };

        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        operator.init( initContext );
    }

    public static void assertScheduleWhenTuplesAvailableStrategy ( final SchedulingStrategy strategy, int tupleCount )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        final Map<Integer, Integer> tupleCountByPortIndex = scheduleWhenTuplesAvailable.getTupleCountByPortIndex();
        assertThat( tupleCountByPortIndex.size(), equalTo( 1 ) );
        assertThat( tupleCountByPortIndex.get( DEFAULT_PORT_INDEX ), equalTo( tupleCount ) );
    }

}
