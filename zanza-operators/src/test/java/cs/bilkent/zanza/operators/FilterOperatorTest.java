package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.function.Predicate;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationReason;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.ShutdownRequested;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import static cs.bilkent.zanza.operators.MapperOperatorTest.assertScheduleWhenTuplesAvailableStrategy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class FilterOperatorTest
{

    private final FilterOperator operator = new FilterOperator();

    private final SimpleOperatorContext operatorContext = new SimpleOperatorContext();

    private final Predicate<Tuple> positiveCountsPredicate = tuple -> tuple.getInteger( "count" ) > 0;

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailWithNoPredicate ()
    {
        operator.init( operatorContext );
    }

    @Test
    public void shouldInitializeWithPredicate ()
    {
        operatorContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithInvalidPredicate ()
    {
        operatorContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, ANY_NUMBER_OF_TUPLES );
    }

    @Test
    public void shouldInitializeWithTupleCount ()
    {
        final int tupleCount = 5;
        operatorContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );
        operatorContext.getConfig().set( FilterOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );

        final SchedulingStrategy strategy = operator.init( operatorContext );

        assertScheduleWhenTuplesAvailableStrategy( strategy, tupleCount );
    }

    @Test
    public void shouldFilterMultipleTuplesForSuccessfulInvocation ()
    {
        final PortsToTuples input = new PortsToTuples();
        input.add( new Tuple( "count", -1 ) );
        input.add( new Tuple( "count", 1 ) );
        shouldFilterTuplesWithPositiveCount( input, SuccessfulInvocation.INSTANCE );
    }

    @Test
    public void shouldFilterMultipleTuplesForErroneousInvocation ()
    {
        final PortsToTuples input = new PortsToTuples();
        input.add( new Tuple( "count", -1 ) );
        input.add( new Tuple( "count", 1 ) );
        shouldFilterTuplesWithPositiveCount( input, ShutdownRequested.INSTANCE );
    }

    private void shouldFilterTuplesWithPositiveCount ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        operatorContext.getConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, positiveCountsPredicate );
        operator.init( operatorContext );

        final ProcessingResult result = operator.process( portsToTuples, reason );
        final List<Tuple> outputTuples = result.getPortsToTuples().getTuplesByDefaultPort();

        final long expectedCount = outputTuples.stream().filter( positiveCountsPredicate ).count();

        assertThat( outputTuples, hasSize( (int) expectedCount ) );
        final List<Tuple> inputTuples = portsToTuples.getTuplesByDefaultPort();
        for ( Tuple outputTuple : outputTuples )
        {
            assertTrue( positiveCountsPredicate.test( outputTuple ) );
            assertTrue( inputTuples.contains( outputTuple ) );
        }
    }
}
