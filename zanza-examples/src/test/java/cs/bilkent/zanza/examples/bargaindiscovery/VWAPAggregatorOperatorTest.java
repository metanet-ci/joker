package cs.bilkent.zanza.examples.bargaindiscovery;

import org.junit.Test;

import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TUPLE_COUNT_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.VOLUMES_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.VWAPS_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_KEY;
import static cs.bilkent.zanza.flow.Port.DEFAULT_PORT_INDEX;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationContext.InvocationReason;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.InitializationContextImpl;
import cs.bilkent.zanza.operator.impl.InvocationContextImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.kvstore.impl.InMemoryKVStore;
import cs.bilkent.zanza.operator.kvstore.impl.KeyDecoratedKVStore;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class VWAPAggregatorOperatorTest
{

    private static final String TUPLE_PARTITION_KEY = "key1";

    private final VWAPAggregatorOperator operator = new VWAPAggregatorOperator();

    private final InitializationContextImpl initContext = new InitializationContextImpl();

    private final PortsToTuples input = new PortsToTuples();

    private final KVStore kvStore = new KeyDecoratedKVStore( TUPLE_PARTITION_KEY, new InMemoryKVStore() );

    private final InvocationContext invocationContext = new InvocationContextImpl( InvocationReason.SUCCESS, input, kvStore );

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailToInitWithNoWindowSize ()
    {
        operator.init( initContext );
    }

    @Test
    public void shouldNotProduceOutputBeforeFirstWindowCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );

        final InvocationResult result = operator.invoke( invocationContext );

        assertThat( result.getOutputTuples().getPortCount(), equalTo( 0 ) );
        assertWindow( 2, new double[] { 5, 10, 0 }, new double[] { 20, 25, 0 }, 15, 45 );
    }

    @Test
    public void shouldProduceOutputForFirstWindow ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );

        final InvocationResult result = operator.invoke( invocationContext );

        final PortsToTuples outputTuples = result.getOutputTuples();
        assertThat( outputTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTuple( outputTuples.getTuple( DEFAULT_PORT_INDEX, 0 ), 45, 105 );

        assertWindow( 3, new double[] { 5, 10, 30 }, new double[] { 20, 25, 60 }, 45, 105 );
    }

    @Test
    public void shouldNotProduceOutputBeforeSlideFactorCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );
        addInputTuple( 40, 50, 4 );

        final InvocationResult result = operator.invoke( invocationContext );

        final PortsToTuples outputTuples = result.getOutputTuples();
        assertThat( outputTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 1 ) );
        assertTuple( outputTuples.getTuple( DEFAULT_PORT_INDEX, 0 ), 45, 105 );

        assertWindow( 4, new double[] { 40, 10, 30 }, new double[] { 50, 25, 60 }, 80, 135 );
    }

    @Test
    public void shouldProduceOutputWhenSlideFactorCompletes ()
    {
        configure();

        addInputTuple( 5, 20, 1 );
        addInputTuple( 10, 25, 2 );
        addInputTuple( 30, 60, 3 );
        addInputTuple( 40, 50, 4 );
        addInputTuple( 50, 40, 5 );

        final InvocationResult result = operator.invoke( invocationContext );

        final PortsToTuples outputTuples = result.getOutputTuples();
        assertThat( outputTuples.getTupleCount( DEFAULT_PORT_INDEX ), equalTo( 2 ) );
        assertTuple( outputTuples.getTuple( DEFAULT_PORT_INDEX, 0 ), 45, 105 );
        assertTuple( outputTuples.getTuple( DEFAULT_PORT_INDEX, 1 ), 120, 150 );

        assertWindow( 5, new double[] { 40, 50, 30 }, new double[] { 50, 40, 60 }, 120, 150 );
    }

    private void configure ()
    {
        final int windowSize = 3;
        final int slideFactor = 2;

        initContext.getConfig().set( VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER, windowSize );
        initContext.getConfig().set( VWAPAggregatorOperator.SLIDE_FACTOR_CONfIG_PARAMETER, slideFactor );
        initContext.setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) );

        operator.init( initContext );
    }

    private void addInputTuple ( final double vwap, final double volume, final long timestamp )
    {
        final Tuple tuple = new Tuple();
        tuple.set( VWAPAggregatorOperator.TUPLE_INPUT_VWAP_FIELD, vwap );
        tuple.set( VWAPAggregatorOperator.TUPLE_VOLUME_FIELD, volume );
        tuple.set( TICKER_SYMBOL_FIELD, TUPLE_PARTITION_KEY );
        tuple.set( VWAPAggregatorOperator.TIMESTAMP_FIELD, timestamp );

        input.add( tuple );
    }

    private void assertTuple ( final Tuple tuple, final double vwap, final double volume )
    {
        assertThat( tuple.get( TICKER_SYMBOL_FIELD ), equalTo( TUPLE_PARTITION_KEY ) );
        assertThat( tuple.getDouble( VWAPAggregatorOperator.SINGLE_VWAP_FIELD ), equalTo( vwap ) );
        assertThat( tuple.getDouble( VWAPAggregatorOperator.SINGLE_VOLUME_FIELD ), equalTo( volume ) );
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
