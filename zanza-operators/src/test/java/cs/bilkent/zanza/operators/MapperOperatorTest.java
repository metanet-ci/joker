package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.zanza.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static cs.bilkent.zanza.operators.MapperOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class MapperOperatorTest
{

    private final MapperOperator operator = new MapperOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final TuplesImpl input = new TuplesImpl( 1 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, input, output );

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

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldInitializeWithInvalidMapper ()
    {
        final Function<String, String> mapper = str -> str;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldInitializeWithTupleCount ()
    {
        final Function<String, String> mapper = str -> str;
        final int tupleCount = 5;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, tupleCount );
    }

    @Test
    public void shouldMapSingleTupleForSuccessfulInvocation ()
    {
        input.add( new Tuple( 1, "count", 5 ) );

        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test
    public void shouldMapSingleTupleForErroneousInvocation ()
    {
        input.add( new Tuple( 1, "count", 5 ) );
        invocationContext.setReason( SHUTDOWN );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForSuccessfulInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( SUCCESS );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForErroneousInvocation ()
    {
        shouldNotMapWitHInvalidMapperFor( INPUT_PORT_CLOSED );
    }

    private void shouldNotMapWitHInvalidMapperFor ( final InvocationReason invocationReason )
    {
        final Function<String, String> mapper = str -> str;
        initContext.getConfig().set( MAPPER_CONFIG_PARAMETER, mapper );

        operator.init( initContext );
        input.add( new Tuple( 1, "count", 5 ) );
        invocationContext.setReason( invocationReason );
        operator.invoke( invocationContext );
    }

    @Test
    public void shouldMapMultipleTuplesForSuccessfulInvocation ()
    {
        input.add( new Tuple( 1, "count", 5 ) );
        input.add( new Tuple( 2, "count", 10 ) );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test
    public void shouldMapMultipleTuplesForErroneousInvocation ()
    {
        input.add( new Tuple( 1, "count", 5 ) );
        input.add( new Tuple( 2, "count", 10 ) );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    private void shouldMultiplyCountValuesBy2 ( final InvocationContextImpl invocationContext )
    {
        initializeOperatorWithMultipleBy2Mapper();

        operator.invoke( invocationContext );
        final List<Tuple> inputTuples = invocationContext.getInput().getTuplesByDefaultPort();
        final List<Tuple> outputTuples = invocationContext.getOutput().getTuplesByDefaultPort();
        assertThat( outputTuples, hasSize( inputTuples.size() ) );
        for ( int i = 0; i < outputTuples.size(); i++ )
        {
            final Tuple inputTuple = inputTuples.get( i );
            final Tuple outputTuple = outputTuples.get( i );
            assertThat( outputTuple.getInteger( "count" ), equalTo( 2 * inputTuple.getInteger( "count" ) ) );
            assertThat( outputTuple.getSequenceNumber(), equalTo( inputTuple.getSequenceNumber() ) );
        }
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

    static void assertScheduleWhenTuplesAvailableStrategy ( final SchedulingStrategy strategy, int tupleCount )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( scheduleWhenTuplesAvailable.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( tupleCount ) );
    }

}
