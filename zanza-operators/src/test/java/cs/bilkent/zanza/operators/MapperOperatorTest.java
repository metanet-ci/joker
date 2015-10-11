package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationReason;
import static cs.bilkent.zanza.operator.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.ShutdownRequested;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class MapperOperatorTest
{

    private final MapperOperator operator = new MapperOperator();

    private final SimpleOperatorContext operatorContext = new SimpleOperatorContext();

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMapper ()
    {
        operator.init( operatorContext );
    }

    @Test
    public void shouldInitializeWithMapper ()
    {
        final Function<Tuple, Tuple> mapper = tuple -> tuple;
        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithInvalidMapper ()
    {
        final Function<String, String> mapper = str -> str;
        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithTupleCount ()
    {
        final Function<String, String> mapper = str -> str;
        final int tupleCount = 5;
        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        operatorContext.getConfig().set( MapperOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, tupleCount );
    }

    @Test
    public void shouldProcessFunctionReturnInitializedTupleCountInSchedulingStrategy ()
    {
        final Function<String, String> mapper = str -> str;
        final int tupleCount = 5;
        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        operatorContext.getConfig().set( MapperOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        operator.init( operatorContext );
        final ProcessingResult result = operator.process( new PortsToTuples(), SuccessfulInvocation.INSTANCE );
        assertScheduleWhenTuplesAvailableStrategy( result.getSchedulingStrategy(), tupleCount );
    }

    @Test
    public void shouldMapSingleTupleForSuccessfulInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        shouldMultiplyCountValuesBy2( portsToTuples, SuccessfulInvocation.INSTANCE );
    }

    @Test
    public void shouldMapSingleTupleForErroneousInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        shouldMultiplyCountValuesBy2( portsToTuples, ShutdownRequested.INSTANCE );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForSuccessfulInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( SuccessfulInvocation.INSTANCE );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForErroneousInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( ShutdownRequested.INSTANCE );
    }

    private void shouldNotMapWitHInvalidMapperFor ( final InvocationReason reason )
    {
        final Function<String, String> mapper = str -> str;
        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        operator.init( operatorContext );
        final PortsToTuples portsToTuples = new PortsToTuples( new Tuple( "count", 5 ) );
        operator.process( portsToTuples, reason );
    }

    @Test
    public void shouldMapMultipleTuplesForSuccessfulInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples();
        portsToTuples.add( new Tuple( "count", 5 ) );
        portsToTuples.add( new Tuple( "count", 10 ) );
        shouldMultiplyCountValuesBy2( portsToTuples, SuccessfulInvocation.INSTANCE );
    }

    @Test
    public void shouldMapMultipleTuplesForErroneousInvocation ()
    {
        final PortsToTuples portsToTuples = new PortsToTuples();
        portsToTuples.add( new Tuple( "count", 5 ) );
        portsToTuples.add( new Tuple( "count", 10 ) );
        shouldMultiplyCountValuesBy2( portsToTuples, ShutdownRequested.INSTANCE );
    }

    private void shouldMultiplyCountValuesBy2 ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        initializeOperatorWithMultipleBy2Mapper();

        final ProcessingResult result = operator.process( portsToTuples, reason );
        final List<Tuple> inputTuples = portsToTuples.getTuplesByDefaultPort();
        final List<Tuple> outputTuples = result.getPortsToTuples().getTuplesByDefaultPort();
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

        final ProcessingResult result = operator.process( new PortsToTuples(), SuccessfulInvocation.INSTANCE );

        assertScheduleWhenTuplesAvailableStrategy( result.getSchedulingStrategy(), ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldReturnAnyTuplesAvailableSchedulingStrategyForErroneousInvocation ()
    {
        initializeOperatorWithMultipleBy2Mapper();

        final ProcessingResult result = operator.process( new PortsToTuples(), ShutdownRequested.INSTANCE );

        assertThat( result.getSchedulingStrategy(), equalTo( ScheduleNever.INSTANCE ) );
    }

    private void initializeOperatorWithMultipleBy2Mapper ()
    {
        final Function<Tuple, Tuple> mapper = input -> {
            Tuple output = new Tuple();
            output.set( "count", input.getInteger( "count" ) * 2 );
            return output;
        };

        operatorContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        operator.init( operatorContext );
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
