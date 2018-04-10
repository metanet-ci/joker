package cs.bilkent.joker.examples.bargaindiscovery;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SLIDE_FACTOR_CONfIG_PARAMETER;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TUPLE_INPUT_VWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TUPLE_VOLUME_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.VOLUMES_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.VWAPS_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_KEY;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import static cs.bilkent.joker.flow.Port.DEFAULT_PORT_INDEX;
import static cs.bilkent.joker.operator.InvocationCtx.InvocationReason.SUCCESS;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.InMemoryKVStore;
import cs.bilkent.joker.operator.impl.InitCtxImpl;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.partition.impl.PartitionKey;
import cs.bilkent.joker.partition.impl.PartitionKey1;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class VWAPAggregatorOperatorTest extends AbstractJokerTest
{

    private static final String TUPLE_PARTITION_KEY = "key1";

    private static final PartitionKey PARTITION_KEY = new PartitionKey1( TUPLE_PARTITION_KEY );

    private final KVStore kvStore = new InMemoryKVStore();

    private final TuplesImpl output = new TuplesImpl( 1 );

    private final DefaultInvocationCtx invocationCtx = new DefaultInvocationCtx( 1, key -> kvStore, output );

    private final TuplesImpl input = invocationCtx.createInputTuples( PARTITION_KEY );

    private final OperatorConfig config = new OperatorConfig();

    private Operator operator;

    private InitCtxImpl initCtx;


    @Before
    public void init () throws InstantiationException, IllegalAccessException
    {
        invocationCtx.setInvocationReason( SUCCESS );

        final OperatorDef operatorDef = OperatorDefBuilder.newInstance( "op", VWAPAggregatorOperator.class )
                                                          .setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) )
                                                          .setConfig( config )
                                                          .build();

        operator = operatorDef.createOperator();
        initCtx = new InitCtxImpl( operatorDef, new boolean[] { true } );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithNoWindowSize ()
    {
        operator.init( initCtx );
    }

    @Test
    public void shouldNotProduceOutputBeforeFirstWindowCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( 0 ), equalTo( 0 ) );
        assertWindow( 2, new double[] { 5, 10, 0 }, new double[] { 20, 25, 0 }, 15, 45 );
    }

    @Test
    public void shouldProduceOutputForFirstWindow ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTuple( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 45, 105 );

        assertWindow( 3, new double[] { 5, 10, 30 }, new double[] { 20, 25, 60 }, 45, 105 );
    }

    @Test
    public void shouldNotProduceOutputBeforeSlideCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );
        addInputTuple( 40, 50, 4 );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTuple( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 45, 105 );

        assertWindow( 4, new double[] { 40, 10, 30 }, new double[] { 50, 25, 60 }, 80, 135 );
    }

    @Test
    public void shouldProduceOutputWhenSlideCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );
        addInputTuple( 40, 50, 4 );
        addInputTuple( 50, 40, 5 );

        operator.invoke( invocationCtx );

        assertThat( output.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTuple( output.getTupleOrFail( DEFAULT_PORT_INDEX, 0 ), 45, 105 );
        assertTuple( output.getTupleOrFail( DEFAULT_PORT_INDEX, 1 ), 120, 150 );

        assertWindow( 5, new double[] { 40, 50, 30 }, new double[] { 50, 40, 60 }, 120, 150 );
    }

    private void configure ()
    {
        final int windowSize = 3;
        final int slideFactor = 2;

        config.set( WINDOW_SIZE_CONfIG_PARAMETER, windowSize ).set( SLIDE_FACTOR_CONfIG_PARAMETER, slideFactor );

        operator.init( initCtx );
    }

    private void addInputTuple ( final double vwap, final double volume, final long timestamp )
    {
        final Tuple tuple = Tuple.of( TUPLE_INPUT_VWAP_FIELD,
                                      vwap,
                                      TUPLE_VOLUME_FIELD,
                                      volume,
                                      TICKER_SYMBOL_FIELD,
                                      TUPLE_PARTITION_KEY,
                                      TIMESTAMP_FIELD,
                                      timestamp );

        input.add( tuple );
    }

    private void assertTuple ( final Tuple tuple, final double vwap, final double volume )
    {
        assertThat( tuple.get( TICKER_SYMBOL_FIELD ), equalTo( TUPLE_PARTITION_KEY ) );
        assertThat( tuple.getDouble( SINGLE_VWAP_FIELD ), equalTo( vwap ) );
        assertThat( tuple.getDouble( SINGLE_VOLUME_FIELD ), equalTo( volume ) );
    }

    private void assertWindow ( final int tupleCount,
                                final double[] vwaps,
                                final double[] volumes,
                                final double vwapSum,
                                final double volumeSum )
    {
        final Tuple window = kvStore.get( WINDOW_KEY );
        assertNotNull( window );

        assertThat( window.get( TUPLE_COUNT_FIELD ), equalTo( tupleCount ) );

        assertThat( window.get( VWAPS_FIELD ), equalTo( vwaps ) );
        assertThat( window.get( VOLUMES_FIELD ), equalTo( volumes ) );
        assertThat( window.get( SINGLE_VWAP_FIELD ), equalTo( vwapSum ) );
        assertThat( window.get( SINGLE_VOLUME_FIELD ), equalTo( volumeSum ) );
    }
}
