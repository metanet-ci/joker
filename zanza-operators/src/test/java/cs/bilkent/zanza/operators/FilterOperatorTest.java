package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.function.Predicate;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import static cs.bilkent.zanza.operators.MapperOperatorTest.assertScheduleWhenTuplesAvailableStrategy;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.utils.SimpleInitializationContext;
import cs.bilkent.zanza.utils.SimpleInvocationContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class FilterOperatorTest
{

    private final FilterOperator operator = new FilterOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    private final Predicate<Tuple> positiveCountsPredicate = tuple -> tuple.getInteger( "count" ) > 0;

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoPredicate ()
    {
        operator.init( initContext );
    }

    @Test
    public void shouldInitializeWithPredicate ()
    {
        initContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithInvalidPredicate ()
    {
        initContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithTupleCount ()
    {
        final int tupleCount = 5;
        initContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );
        initContext.getConfig().set( FilterOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        final SchedulingStrategy strategy = operator.init( initContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, tupleCount );
    }

    @Test
    public void shouldFilterMultipleTuplesForSuccessfulInvocation ()
    {
        final PortsToTuples input = new PortsToTuples();
        input.add( new Tuple( "count", -1 ) );
        input.add( new Tuple( "count", 1 ) );
        shouldFilterTuplesWithPositiveCount( new SimpleInvocationContext( input, InvocationReason.SUCCESS ) );
    }

    @Test
    public void shouldFilterMultipleTuplesForErroneousInvocation ()
    {
        final PortsToTuples input = new PortsToTuples();
        input.add( new Tuple( "count", -1 ) );
        input.add( new Tuple( "count", 1 ) );
        shouldFilterTuplesWithPositiveCount( new SimpleInvocationContext( input, InvocationReason.SHUTDOWN ) );
    }

    private void shouldFilterTuplesWithPositiveCount ( final InvocationContext invocationContext )
    {
        initContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );
        operator.init( initContext );

        final InvocationResult result = operator.process( invocationContext );
        final List<Tuple> outputTuples = result.getOutputTuples().getTuplesByDefaultPort();

        final long expectedCount = outputTuples.stream().filter( positiveCountsPredicate ).count();

        assertThat( outputTuples, hasSize( (int) expectedCount ) );
        final List<Tuple> inputTuples = invocationContext.getInputTuples().getTuplesByDefaultPort();
        for ( Tuple outputTuple : outputTuples )
        {
            assertTrue( positiveCountsPredicate.test( outputTuple ) );
            assertTrue( inputTuples.contains( outputTuple ) );
        }
    }
}
