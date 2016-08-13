package cs.bilkent.joker.examples.bargaindiscovery;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TUPLE_INPUT_VWAP_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TUPLE_VOLUME_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TIMESTAMP_FIELD, type = Long.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                                                                                                                                                                                     @SchemaField( name = VWAPAggregatorOperator.SINGLE_VOLUME_FIELD, type = Double.class ),
                                                                                                                                                                                                                                                                     @SchemaField( name = VWAPAggregatorOperator.SINGLE_VWAP_FIELD, type = Double.class ),
                                                                                                                                                                                                                                                                     @SchemaField( name = VWAPAggregatorOperator.TIMESTAMP_FIELD, type = Long.class ) } ) } )
public class VWAPAggregatorOperator implements Operator
{

    public static final String WINDOW_SIZE_CONfIG_PARAMETER = "windowSize";

    public static final String SLIDE_FACTOR_CONfIG_PARAMETER = "slideFactor";

    public static final String TICKER_SYMBOL_FIELD = "tickersymbol";

    public static final String SINGLE_VWAP_FIELD = "svwap";

    public static final String SINGLE_VOLUME_FIELD = "svolume";

    public static final String TUPLE_INPUT_VWAP_FIELD = "myvwap";

    public static final String TUPLE_VOLUME_FIELD = "volume";

    public static final String TIMESTAMP_FIELD = "timestamp";

    static final String TUPLE_COUNT_FIELD = "tupleCount";

    static final String WINDOW_KEY = "window";

    static final String VWAPS_FIELD = "vwaps";

    static final String VOLUMES_FIELD = "volumes";


    private int windowSize;

    private int slideFactor;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.windowSize = context.getConfig().getOrFail( WINDOW_SIZE_CONfIG_PARAMETER );
        this.slideFactor = context.getConfig().getOrDefault( SLIDE_FACTOR_CONfIG_PARAMETER, 1 );

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();
        final KVStore kvStore = invocationContext.getKVStore();

        final Tuple currentWindow = kvStore.getOrDefault( WINDOW_KEY, this::createWindowTuple );
        final double[] vwapValues = currentWindow.get( VWAPS_FIELD );
        final double[] volumeValues = currentWindow.get( VOLUMES_FIELD );
        double vwapSum = currentWindow.get( SINGLE_VWAP_FIELD );
        double volumeSum = currentWindow.get( SINGLE_VOLUME_FIELD );
        int tupleCount = currentWindow.get( TUPLE_COUNT_FIELD );

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            final double vwap = tuple.getDoubleValueOrDefault( TUPLE_INPUT_VWAP_FIELD, 0d );
            final double volume = tuple.getDoubleValueOrDefault( TUPLE_VOLUME_FIELD, 0d );

            final int i = tupleCount++ % windowSize;

            vwapSum -= vwapValues[ i ];
            volumeSum -= volumeValues[ i ];

            vwapValues[ i ] = vwap;
            volumeValues[ i ] = volume;

            vwapSum += vwap;
            volumeSum += volume;

            if ( endOfWindow( tupleCount ) || endOfSlide( tupleCount ) )
            {
                final Tuple outputTuple = createOutputTuple( tuple.getString( TICKER_SYMBOL_FIELD ),
                                                             tuple.getLong( TIMESTAMP_FIELD ),
                                                             vwapSum,
                                                             volumeSum );

                output.add( outputTuple );
            }
        }

        currentWindow.set( SINGLE_VWAP_FIELD, vwapSum );
        currentWindow.set( SINGLE_VOLUME_FIELD, volumeSum );
        currentWindow.set( TUPLE_COUNT_FIELD, tupleCount );
        kvStore.set( WINDOW_KEY, currentWindow );
    }

    private Tuple createWindowTuple ()
    {
        final Tuple tuple = new Tuple();
        tuple.set( VWAPS_FIELD, new double[ windowSize ] );
        tuple.set( VOLUMES_FIELD, new double[ windowSize ] );
        tuple.set( SINGLE_VWAP_FIELD, 0d );
        tuple.set( SINGLE_VOLUME_FIELD, 0d );
        tuple.set( TUPLE_COUNT_FIELD, 0 );
        return tuple;
    }

    private boolean endOfWindow ( final int tupleCount )
    {
        return tupleCount == windowSize;
    }

    private boolean endOfSlide ( final int tupleCount )
    {
        return tupleCount > windowSize && ( tupleCount - this.windowSize ) % this.slideFactor == 0;
    }

    private Tuple createOutputTuple ( final String tickerSymbol, final long timestamp, final double vwapSum, final double volumeSum )
    {
        final Tuple tuple = new Tuple();
        tuple.set( TICKER_SYMBOL_FIELD, tickerSymbol );
        tuple.set( TIMESTAMP_FIELD, timestamp );

        tuple.set( SINGLE_VWAP_FIELD, vwapSum );
        tuple.set( SINGLE_VOLUME_FIELD, volumeSum );

        return tuple;
    }

}
