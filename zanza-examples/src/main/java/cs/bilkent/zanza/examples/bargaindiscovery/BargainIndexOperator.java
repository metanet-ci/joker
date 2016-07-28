package cs.bilkent.zanza.examples.bargaindiscovery;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cs.bilkent.zanza.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;

@OperatorSpec( type = OperatorType.PARTITIONED_STATEFUL, inputPortCount = 2, outputPortCount = 1 )
@OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = CVWAPFunction
                                                                                                                              .CVWAP_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TIMESTAMP_FIELD, type = Long.class ) } ),
                            @PortSchema( portIndex = 1, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                 @SchemaField( name = BargainIndexOperator.ASKED_TICKER_SYMBOL_PRICE_FIELD, type = Double.class ),
                                                                                                 @SchemaField( name = BargainIndexOperator.ASKED_SIZE_FIELD, type = Integer.class ),
                                                                                                 @SchemaField( name = VWAPAggregatorOperator.TIMESTAMP_FIELD, type = Long.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = VWAPAggregatorOperator.TICKER_SYMBOL_FIELD, type = String.class ),
                                                                                                                                                                                                                                                                     @SchemaField( name = BargainIndexOperator.BARGAIN_INDEX_FIELD, type = Double.class ) } ) } )
public class BargainIndexOperator implements Operator
{

    private static final Logger LOGGER = LoggerFactory.getLogger( BargainIndexOperator.class );

    static final String ASKED_TICKER_SYMBOL_PRICE_FIELD = "askptickersymbol";

    static final String ASKED_SIZE_FIELD = "asksize";

    static final String BARGAIN_INDEX_FIELD = "bargainindex";

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        return scheduleWhenTuplesAvailableOnAny( 1, 0, 1 );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();
        final KVStore kvStore = invocationContext.getKVStore();
        final Iterator<Tuple> it = new MergedTupleListsIterator( input.getTuples( 0 ),
                                                                 input.getTuples( 1 ),
                                                                 ( left, right ) -> left.getLong( TIMESTAMP_FIELD )
                                                                                        .compareTo( right.getLong( TIMESTAMP_FIELD ) ) );
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
                        output.add( bargainIndex );
                    }
                }
                else
                {
                    LOGGER.warn( "Join missed for quote: " + tuple );
                }
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

            final Tuple outputTuple = new Tuple();
            outputTuple.set( BARGAIN_INDEX_FIELD, bargainIndex );

            return outputTuple;
        }

        return null;
    }

}
