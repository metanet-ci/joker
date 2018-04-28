package cs.bilkent.joker.examples.bargaindiscovery;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
@OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TUPLE_INPUT_VWAP_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TUPLE_VOLUME_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TIMESTAMP_FIELD, type = Long.class ) } ) }, outputs = {
        @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD,
                type = String.class ),
                                                                        @SchemaField( name = VWAPAggregatorOperator.SINGLE_VOLUME_FIELD,
                                                                                type = Double.class ),
                                                                        @SchemaField( name = VWAPAggregatorOperator.SINGLE_VWAP_FIELD,
                                                                                type = Double.class ),
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

    public static final String TUPLE_COUNT_FIELD = "tupleCount";

    public static final String WINDOW_KEY = "window";

    public static final String VWAPS_FIELD = "vwaps";

    public static final String VOLUMES_FIELD = "volumes";


    private int windowSize;

    private int slideFactor;

    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        this.windowSize = ctx.getConfig().getOrFail( WINDOW_SIZE_CONfIG_PARAMETER );
        this.slideFactor = ctx.getConfig().getOrDefault( SLIDE_FACTOR_CONfIG_PARAMETER, 1 );
        this.outputSchema = ctx.getOutputPortSchema( 0 );

        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final KVStore kvStore = ctx.getKVStore();

        final Tuple currentWindow = kvStore.getOrDefault( WINDOW_KEY, this::createWindowTuple );
        final double[] vwapValues = currentWindow.get( VWAPS_FIELD );
        final double[] volumeValues = currentWindow.get( VOLUMES_FIELD );
        double vwapSum = currentWindow.get( SINGLE_VWAP_FIELD );
        double volumeSum = currentWindow.get( SINGLE_VOLUME_FIELD );
        int tupleCount = currentWindow.get( TUPLE_COUNT_FIELD );

        for ( Tuple input : ctx.getInputTuplesByDefaultPort() )
        {
            final double vwap = input.getDoubleValueOrDefault( TUPLE_INPUT_VWAP_FIELD, 0d );
            final double volume = input.getDoubleValueOrDefault( TUPLE_VOLUME_FIELD, 0d );

            final int i = tupleCount++ % windowSize;

            vwapSum -= vwapValues[ i ];
            volumeSum -= volumeValues[ i ];

            vwapValues[ i ] = vwap;
            volumeValues[ i ] = volume;

            vwapSum += vwap;
            volumeSum += volume;

            if ( endOfWindow( tupleCount ) || endOfSlide( tupleCount ) )
            {
                final Tuple result = createOutputTuple( input.getString( TICKER_SYMBOL_FIELD ),
                                                        input.getLong( TIMESTAMP_FIELD ),
                                                        vwapSum,
                                                        volumeSum );
                result.attachTo( input );
                ctx.output( result );
            }
        }

        currentWindow.set( SINGLE_VWAP_FIELD, vwapSum ).set( SINGLE_VOLUME_FIELD, volumeSum ).set( TUPLE_COUNT_FIELD, tupleCount );
        kvStore.set( WINDOW_KEY, currentWindow );
    }

    private Tuple createWindowTuple ()
    {
        return Tuple.of( VWAPS_FIELD,
                         new double[ windowSize ],
                         VOLUMES_FIELD,
                         new double[ windowSize ],
                         SINGLE_VWAP_FIELD,
                         0d,
                         SINGLE_VOLUME_FIELD,
                         0d,
                         TUPLE_COUNT_FIELD,
                         0 );
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
        return Tuple.of( outputSchema,
                         TICKER_SYMBOL_FIELD,
                         tickerSymbol,
                         TIMESTAMP_FIELD,
                         timestamp,
                         SINGLE_VWAP_FIELD,
                         vwapSum,
                         SINGLE_VOLUME_FIELD,
                         volumeSum );
    }

}
