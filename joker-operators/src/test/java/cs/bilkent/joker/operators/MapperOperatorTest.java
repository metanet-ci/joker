package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.joker.operator.InvocationCtx.InvocationReason;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.INPUT_PORT_CLOSED;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
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

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( 1, key -> null, output );

    private final TuplesImpl input = invocationCtx.createInputTuples( null );

    private final OperatorConfig config = new OperatorConfig();

    private MapperOperator operator;

    private InitCtxImpl initCtx;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationCtx.setInvocationReason( SUCCESS );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class ).setConfig( config ).build();
        operator = (MapperOperator) operatorDef.createOperator();
        initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoMapper ()
    {
        operator.init( initCtx );
    }

    @Test
    public void shouldInitializeWithMapper ()
    {
        final BiConsumer<Tuple, Tuple> mapper = ( input, output ) -> input.sinkTo( output::set );
        config.set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initCtx );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldInitializeWithInvalidMapper ()
    {
        final BiConsumer<String, String> mapper = ( s, s2 ) -> {

        };
        config.set( MAPPER_CONFIG_PARAMETER, mapper );

        final SchedulingStrategy strategy = operator.init( initCtx );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldMapSingleTupleForSuccessfulInvocation ()
    {
        final Tuple tuple = Tuple.of( "count", 5 );
        input.add( tuple );

        shouldMultiplyCountValuesBy2( invocationCtx );
    }

    @Test
    public void shouldMapSingleTupleForErroneousInvocation ()
    {
        final Tuple tuple = Tuple.of( "count", 5 );
        input.add( tuple );

        invocationCtx.setInvocationReason( SHUTDOWN );
        shouldMultiplyCountValuesBy2( invocationCtx );
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

        operator.init( initCtx );
        final Tuple tuple = Tuple.of( "count", 5 );
        input.add( tuple );

        invocationCtx.setInvocationReason( invocationReason );
        operator.invoke( invocationCtx );
    }

    @Test
    public void shouldMapMultipleTuplesForSuccessfulInvocation ()
    {
        final Tuple tuple1 = Tuple.of( "count", 5 );
        final Tuple tuple2 = Tuple.of( "count", 10 );
        input.add( tuple1, tuple2 );
        shouldMultiplyCountValuesBy2( invocationCtx );
    }

    @Test
    public void shouldMapMultipleTuplesForErroneousInvocation ()
    {
        final Tuple tuple1 = Tuple.of( "count", 5 );
        final Tuple tuple2 = Tuple.of( "count", 10 );
        input.add( tuple1, tuple2 );
        shouldMultiplyCountValuesBy2( invocationCtx );
    }

    private void shouldMultiplyCountValuesBy2 ( final DefaultInvocationCtx invocationCtx )
    {
        initializeOperatorWithMultipleBy2Mapper();

        operator.invoke( invocationCtx );
        final List<Tuple> inputTuples = invocationCtx.getInput().getTuplesByDefaultPort();
        final List<Tuple> outputTuples = invocationCtx.getOutput().getTuplesByDefaultPort();
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
        operator.init( initCtx );
    }

    static void assertScheduleWhenTuplesAvailableStrategy ( final SchedulingStrategy strategy, int tupleCount )
    {
        assertTrue( strategy instanceof ScheduleWhenTuplesAvailable );
        final ScheduleWhenTuplesAvailable scheduleWhenTuplesAvailable = (ScheduleWhenTuplesAvailable) strategy;
        assertThat( scheduleWhenTuplesAvailable.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( tupleCount ) );
    }

}
