package cs.bilkent.joker.experiment.bargain;

import static cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator.ASKED_SIZE_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator.ASKED_TICKER_SYMBOL_PRICE_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.utils.Pair;

@OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = TICKER_SYMBOL_FIELD,
        type = String.class ),
                                                                                             @SchemaField( name = ASKED_TICKER_SYMBOL_PRICE_FIELD, type = Double.class ),
                                                                                             @SchemaField( name = ASKED_SIZE_FIELD, type = Integer.class ) } ) } )
public class QuoteBeaconOperator extends TickerPriceBaseOperator
{

    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        this.outputSchema = ctx.getOutputPortSchema( 0 );
        return super.init( ctx );
    }

    @Override
    Tuple nextTuple ()
    {
        final Pair<String, Double> tickerPrice = nextTickerPrice();
        return Tuple.of( outputSchema,
                         TICKER_SYMBOL_FIELD,
                         tickerPrice._1,
                         ASKED_TICKER_SYMBOL_PRICE_FIELD,
                         tickerPrice._2,
                         ASKED_SIZE_FIELD,
                         1,
                         TIMESTAMP_FIELD,
                         nextTimestamp() );
    }

}
