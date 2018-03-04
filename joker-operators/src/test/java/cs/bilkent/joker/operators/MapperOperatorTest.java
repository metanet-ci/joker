package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.InvocationContext.InvocationReason;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationContext;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class MapperOperatorTest extends AbstractJokerTest
{

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final DefaultInvocationContext invocationContext = new DefaultInvocationContext( 1, key -> null, output );

    private final TuplesImpl input = invocationContext.createInputTuples( null );

    private final OperatorConfig config = new OperatorConfig();

    private MapperOperator operator;

    private InitializationContextImpl initContext;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationContext.setInvocationReason( SUCCESS );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class ).setConfig( config ).build();
        operator = (MapperOperator) operatorDef.createOperator();
        initContext = new InitializationContextImpl( operatorDef, new boolean[] { true } );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMapper ()
    {
        operator.init( initContext );
    }

    @Test
    public void shouldInitializeWithMapper ()
    {
        final BiConsumer<Tuple, Tuple> mapper = ( input, output ) -> input.consumeEntries( output::set );
        config.set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldInitializeWithInvalidMapper ()
    {
        final BiConsumer<String, String> mapper = ( s, s2 ) -> {

        };
        config.set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldMapSingleTupleForSuccessfulInvocation ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "count", 5 );
        input.add( tuple );

        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test
    public void shouldMapSingleTupleForErroneousInvocation ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( "count", 5 );
        input.add( tuple );

        invocationContext.setInvocationReason( SHUTDOWN );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForSuccessfulInvocation ()
    {
        shouldNotMapWithInvalidMapperFor( SUCCESS );
    }

    @Test( expected = ClassCastException.class )
    public void shouldNotMapWithInvalidMapperForErroneousInvocation ()
    {
        shouldNotMapWithInvalidMapperFor( INPUT_PORT_CLOSED );
    }

    private void shouldNotMapWithInvalidMapperFor ( final InvocationReason invocationReason )
    {
        final BiConsumer<String, String> mapper = ( s1, s2 ) -> {

        };
        config.set( MAPPER_CONFIG_PARAMETER, mapper );

        operator.init( initContext );
        final Tuple tuple = new Tuple();
        tuple.set( "count", 5 );
        input.add( tuple );

        invocationContext.setInvocationReason( invocationReason );
        operator.invoke( invocationContext );
    }

    @Test
    public void shouldMapMultipleTuplesForSuccessfulInvocation ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( "count", 5 );
        input.add( tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "count", 10 );
        input.add( tuple2 );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    @Test
    public void shouldMapMultipleTuplesForErroneousInvocation ()
    {
        final Tuple tuple1 = new Tuple();
        tuple1.set( "count", 5 );
        input.add( tuple1 );
        final Tuple tuple2 = new Tuple();
        tuple2.set( "count", 10 );
        input.add( tuple2 );
        shouldMultiplyCountValuesBy2( invocationContext );
    }

    private void shouldMultiplyCountValuesBy2 ( final DefaultInvocationContext invocationContext )
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
        }
    }

    private void initializeOperatorWithMultipleBy2Mapper ()
    {
        final BiConsumer<Tuple, Tuple> mapper = ( input1, output1 ) -> output1.set( "count", input1.getInteger( "count" ) * 2 );

        config.set( MAPPER_CONFIG_PARAMETER, mapper );
        operator.init( initContext );
    }

    static void assertScheduleWhenTuplesAvailableStrategy ( final SchedulingStrategy strategy, int tupleCount )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( scheduleWhenTuplesAvailable.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( tupleCount ) );
    }

}
