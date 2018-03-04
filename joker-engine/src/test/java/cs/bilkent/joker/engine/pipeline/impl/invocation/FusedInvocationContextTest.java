package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.OutputTupleCollector;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class FusedInvocationContextTest extends AbstractJokerTest
{

    @Mock
    private Function<PartitionKey, KVStore> kvStoreSupplier;

    @Mock
    private OutputTupleCollector outputCollector;

    private FusedInvocationContext invocationContext;

    @Before
    public void init ()
    {
        invocationContext = new FusedInvocationContext( 1, kvStoreSupplier, outputCollector );
    }

    @Test
    public void when_noOutputIsAdded_then_noInputIsPresent ()
    {
        assertThat( invocationContext.getInputCount(), equalTo( 0 ) );
    }

    @Test
    public void when_outputIsAdded_then_singleInputIsPresent ()
    {
        invocationContext.add( new Tuple() );

        assertThat( invocationContext.getInputCount(), equalTo( 1 ) );
    }

    @Test
    public void when_outputIsAddedMultipleTimes_then_singleInputIsPresent ()
    {
        invocationContext.add( new Tuple() );
        invocationContext.add( new Tuple() );

        assertThat( invocationContext.getInputCount(), equalTo( 1 ) );
    }

    @Test
    public void when_invocationContextIsReset_then_outputSupplierIsCleared ()
    {
        invocationContext.reset();

        verify( outputCollector ).clear();
    }

    @Test
    public void when_invocationContextIsReset_then_noInputIsPresent ()
    {
        invocationContext.add( new Tuple() );
        invocationContext.reset();

        assertThat( invocationContext.getInputCount(), equalTo( 0 ) );
    }

}
