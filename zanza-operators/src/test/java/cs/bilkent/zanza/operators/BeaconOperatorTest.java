package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.invocationreason.SuccessfulInvocation;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class BeaconOperatorTest
{

    private final BeaconOperator operator = new BeaconOperator();

    private final SimpleOperatorContext operatorContext = new SimpleOperatorContext();

    @Test
    public void shouldGenerateTuplesWithRandomCountField ()
    {
        final int tupleCount = 10;
        final int maxInt = 100;
        final Function<Random, Tuple> generator = ( random ) -> new Tuple( "count", random.nextInt( maxInt ) );
        operatorContext.getConfig().set( BeaconOperator.TUPLE_GENERATOR_CONFIG_PARAMETER, generator );
        operatorContext.getConfig().set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        operator.init( operatorContext );

        final ProcessingResult result = operator.process( null, SuccessfulInvocation.INSTANCE );
        assertThat( result.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );

        final PortsToTuples output = result.getPortsToTuples();
        assertThat( output.getPortCount(), equalTo( 1 ) );

        final List<Tuple> tuples = output.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( tupleCount ) );
        for ( Tuple tuple : tuples )
        {
            assertThat( maxInt, greaterThan( tuple.getInteger( "count" ) ) );
        }
    }

}
