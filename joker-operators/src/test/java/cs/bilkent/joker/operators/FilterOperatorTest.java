package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SHUTDOWN;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.joker.operators.FilterOperator.PREDICATE_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.MapperOperatorTest.assertScheduleWhenTuplesAvailableStrategy;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class FilterOperatorTest extends AbstractJokerTest
{

    private final Predicate<Tuple> positiveCountsPredicate = tuple -> tuple.getInteger( "count" ) > 0;

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( 1, key -> null, output );

    private final TuplesImpl input = invocationCtx.createInputTuples( null );

    private final OperatorConfig config = new OperatorConfig();

    private FilterOperator operator;

    private InitCtxImpl initCtx;

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationCtx.setInvocationReason( SUCCESS );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "filter", FilterOperator.class ).setConfig( config ).build();
        operator = (FilterOperator) operatorDef.createOperator();
        initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoPredicate ()
    {
        operator.init( initCtx );
    }

    @Test
    public void shouldInitializeWithPredicate ()
    {
        config.set( PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( initCtx );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldInitializeWithInvalidPredicate ()
    {
        config.set( PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( initCtx );

        assertScheduleWhenTuplesAvailableStrategy( strategy, 1 );
    }

    @Test
    public void shouldFilterMultipleTuplesForSuccessfulInvocation ()
    {
        final Tuple tuple1 = Tuple.of( "count", -1 );
        final Tuple tuple2 = Tuple.of( "count", 1 );
        input.add( tuple1, tuple2 );
        shouldFilterTuplesWithPositiveCount( invocationCtx );
    }

    @Test
    public void shouldFilterMultipleTuplesForErroneousInvocation ()
    {
        final Tuple tuple1 = Tuple.of( "count", -1 );
        final Tuple tuple2 = Tuple.of( "count", 1 );
        input.add( tuple1, tuple2 );

        invocationCtx.setInvocationReason( SHUTDOWN );
        shouldFilterTuplesWithPositiveCount( invocationCtx );
    }

    private void shouldFilterTuplesWithPositiveCount ( final DefaultInvocationCtx invocationCtx )
    {
        config.set( PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );
        operator.init( initCtx );

        operator.invoke( invocationCtx );
        final List<Tuple> outputTuples = invocationCtx.getOutput().getTuplesByDefaultPort();

        final long expectedCount = outputTuples.stream().filter( positiveCountsPredicate ).count();

        assertThat( outputTuples, hasSize( (int) expectedCount ) );
        final List<Tuple> inputTuples = invocationCtx.getInput().getTuplesByDefaultPort();
        for ( Tuple outputTuple : outputTuples )
        {
            assertTrue( positiveCountsPredicate.test( outputTuple ) );
            assertTrue( inputTuples.contains( outputTuple ) );
        }
    }
}
