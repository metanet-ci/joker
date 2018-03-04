package cs.bilkent.joker.examples.bargaindiscovery;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.joker.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import cs.bilkent.joker.operator.spec.OperatorType;
import static java.util.Comparator.comparing;

@OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
@OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = CVWAP_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = TIMESTAMP_FIELD,
                                                                                                         type = Long.class ) } ),
                            @PortSchema( portIndex = 1, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = BargainIndexOperator.ASKED_TICKER_SYMBOL_PRICE_FIELD, type = Double.class ),

                                                                                                 @SchemaField( name = BargainIndexOperator.ASKED_SIZE_FIELD, type = Integer.class ) } ) }, outputs = {
        @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = TICKER_SYMBOL_FIELD, type = String.class ),
                                                                        @SchemaField( name = BargainIndexOperator.BARGAIN_INDEX_FIELD,
                                                                                type = Double.class ) } ) } )
public class BargainIndexOperator implements Operator
{

    private static final Logger LOGGER = LoggerFactory.getLogger( BargainIndexOperator.class );

    public static final String ASKED_TICKER_SYMBOL_PRICE_FIELD = "askptickersymbolprice";

    public static final String ASKED_SIZE_FIELD = "asksize";

    public static final String BARGAIN_INDEX_FIELD = "bargainindex";


    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitializationContext ctx )
    {
        outputSchema = ctx.getOutputPortSchema( 0 );
        return scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );
    }

    @Override
    public void invoke ( final InvocationContext ctx )
    {
        final KVStore kvStore = ctx.getKVStore();
        final Iterator<Tuple> it = new MergedTupleListsIterator( ctx.getInputTuples( 0 ),
                                                                 ctx.getInputTuples( 1 ),
                                                                 comparing( t -> t.getLong( TIMESTAMP_FIELD ) ) );
        while ( it.hasNext() )
        {
            final Tuple tuple = it.next();
            final String tickerSymbol = tuple.get( TICKER_SYMBOL_FIELD );
            if ( tuple.contains( CVWAP_FIELD ) )
            {
                kvStore.set( tickerSymbol, tuple.get( CVWAP_FIELD ) );
            }
            else
            {
                final Double cvwap = kvStore.get( tickerSymbol );
                if ( cvwap != null )
                {
                    final Tuple bargainIndex = createBargainIndexTuple( cvwap, tuple );
                    if ( bargainIndex != null )
                    {
                        ctx.output( bargainIndex );
                    }
                }
                //                else
                //                {
                //                    LOGGER.warn( "Join missed for quote: " + tuple );
                //                }
            }
        }
    }

    private Tuple createBargainIndexTuple ( final double cvwap, final Tuple quote )
    {
        final double askedTickerSymbolPrice = quote.getDouble( ASKED_TICKER_SYMBOL_PRICE_FIELD ) * 100;
        if ( cvwap > askedTickerSymbolPrice )
        {
            final int askedSize = quote.getInteger( ASKED_SIZE_FIELD );
            final double bargainIndex = Math.exp( cvwap - askedTickerSymbolPrice ) * askedSize;

            final Tuple outputTuple = new Tuple( outputSchema );
            outputTuple.set( BARGAIN_INDEX_FIELD, bargainIndex );

            return outputTuple;
        }

        return null;
    }

}
