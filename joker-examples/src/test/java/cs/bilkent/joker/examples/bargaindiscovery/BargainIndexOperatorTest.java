package cs.bilkent.joker.examples.bargaindiscovery;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator.ASKED_SIZE_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator.ASKED_TICKER_SYMBOL_PRICE_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator.BARGAIN_INDEX_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.operator.InvocationContext.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.impl.InitializationContextImpl;
import cs.bilkent.joker.operator.impl.InvocationContextImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BargainIndexOperatorTest extends AbstractJokerTest
{

    private static final String TUPLE_PARTITION_KEY = "key1";

    private BargainIndexOperator operator;

    private final TuplesImpl input = new TuplesImpl( 2 );

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final KVStore kvStore = new InMemoryKVStore();

    private final InvocationContextImpl invocationContext = new InvocationContextImpl();

    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationContext.setInvocationParameters( SUCCESS, input, output, kvStore );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", BargainIndexOperator.class )
                                                          .setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) )
                                                          .build();
        operator = (BargainIndexOperator) operatorDef.createOperator();
        final InitializationContextImpl initContext = new InitializationContextImpl( operatorDef, new boolean[] { true, true } );
        operator.init( initContext );
    }

    @Test
    public void shouldReturnNoOutputWithQuoteButNoVWAP ()
    {
        input.add( 1, newQuoteTuple( 0, 5d, 100 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( 0 ), equalTo( 0 ) );
    }

    @Test
    public void shouldReturnNoOutputFromQuoteWithAskedPriceHigherThanCVWAP ()
    {
        setCVWAPInKvStore( 50 );
        input.add( 1, newQuoteTuple( 0, 0.6, 1 ) );

        operator.invoke( invocationContext );

        assertThat( output.getTupleCount( 0 ), equalTo( 0 ) );
    }

    @Test
    public void shouldReturnOutputFromQuoteWithAskedPriceLowerThanCVWAP ()
    {
        setCVWAPInKvStore( 50 );
        input.add( 1, newQuoteTuple( 0, 0.4, 1 ) );

        operator.invoke( invocationContext );

        assertThat( output.getNonEmptyPortCount(), equalTo( 1 ) );

        final Tuple outputTuple = output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 );
        assertThat( outputTuple.get( BARGAIN_INDEX_FIELD ), equalTo( Math.exp( 10 ) ) );
    }

    @Test
    public void shouldReturnOutputsFromQuotesWithUpToDateVWAPs ()
    {
        setCVWAPInKvStore( 50 );
        input.add( 1, newQuoteTuple( 0, 0.4, 1 ) );
        input.add( 0, newCVWAPTuple( 1, 60 ) );
        input.add( 1, newQuoteTuple( 1, 0.3, 2 ) );

        operator.invoke( invocationContext );

        assertThat( output.getNonEmptyPortCount(), equalTo( 1 ) );

        final Tuple outputTuple1 = output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 );
        assertThat( outputTuple1.get( BARGAIN_INDEX_FIELD ), equalTo( Math.exp( 10 ) ) );
        final Tuple outputTuple2 = output.getTupleOrFail( DEFAULT_PORT_INDEX, 1 );
        assertThat( outputTuple2.get( BARGAIN_INDEX_FIELD ), equalTo( Math.exp( 30 ) * 2 ) );

        assertThat( kvStore.get( TUPLE_PARTITION_KEY ), equalTo( 60d ) );
    }

    private Tuple newCVWAPTuple ( final long timestamp, final double cvwap )
    {
        final Tuple tuple = new Tuple();
        tuple.set( TICKER_SYMBOL_FIELD, TUPLE_PARTITION_KEY );
        tuple.set( CVWAP_FIELD, cvwap );
        tuple.set( TIMESTAMP_FIELD, timestamp );

        return tuple;
    }

    private Tuple newQuoteTuple ( final long timestamp, final double askedPrice, final int askedSize )
    {
        final Tuple tuple = new Tuple();
        tuple.set( TICKER_SYMBOL_FIELD, TUPLE_PARTITION_KEY );
        tuple.set( TIMESTAMP_FIELD, timestamp );
        tuple.set( ASKED_TICKER_SYMBOL_PRICE_FIELD, askedPrice );
        tuple.set( ASKED_SIZE_FIELD, askedSize );

        return tuple;
    }

    private void setCVWAPInKvStore ( final double cvwap )
    {
        kvStore.set( TUPLE_PARTITION_KEY, cvwap );
    }

}
