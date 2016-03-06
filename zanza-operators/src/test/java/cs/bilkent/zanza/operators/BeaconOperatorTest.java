package cs.bilkent.zanza.operators;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.scheduling.ScheduleWhenAvailable;
import cs.bilkent.zanza.utils.SimpleInitializationContext;
import cs.bilkent.zanza.utils.SimpleInvocationContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;


public class BeaconOperatorTest
{

    private final BeaconOperator operator = new BeaconOperator();

    private final SimpleInitializationContext initContext = new SimpleInitializationContext();

    @Test
    public void shouldGenerateTuplesWithRandomCountField ()
    {
        final int tupleCount = 10;
        final int maxInt = 100;
        final Function<Random, Tuple> generator = ( random ) -> new Tuple( "count", random.nextInt( maxInt ) );
        initContext.getConfig().set( BeaconOperator.TUPLE_GENERATOR_CONFIG_PARAMETER, generator );
        initContext.getConfig().set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        operator.init( initContext );

        final InvocationResult result = operator.invoke( new SimpleInvocationContext( InvocationReason.SUCCESS, null ) );
        assertThat( result.getSchedulingStrategy(), equalTo( ScheduleWhenAvailable.INSTANCE ) );

        final PortsToTuples output = result.getOutputTuples();
        assertThat( output.getPortCount(), equalTo( 1 ) );

        final List<Tuple> tuples = output.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( tupleCount ) );
        for ( Tuple tuple : tuples )
        {
            assertThat( maxInt, greaterThan( tuple.getInteger( "count" ) ) );
        }
    }

}
