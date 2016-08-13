package cs.bilkent.joker.operators;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_GENERATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;


public class BeaconOperatorTest extends AbstractJokerTest
{

    private final BeaconOperator operator = new BeaconOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    @Test
    public void shouldGenerateTuplesWithRandomCountField ()
    {
        final int tupleCount = 10;
        final int maxInt = 100;
        final Function<Random, Tuple> generator = ( random ) -> new Tuple( "count", random.nextInt( maxInt ) );
        initContext.getConfig().set( TUPLE_GENERATOR_CONFIG_PARAMETER, generator );
        initContext.getConfig().set( TUPLE_COUNT_CONFIG_PARAMETER, tupleCount );
        operator.init( initContext );

        final TuplesImpl output = new TuplesImpl( 1 );
        final InvocationContextImpl invocationContext = new InvocationContextImpl( SUCCESS, null, output );

        operator.invoke( invocationContext );

        assertThat( output.getNonEmptyPortCount(), equalTo( 1 ) );

        final List<Tuple> tuples = output.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( tupleCount ) );
        for ( Tuple tuple : tuples )
        {
            assertThat( maxInt, greaterThan( tuple.getInteger( "count" ) ) );
        }
    }

}
