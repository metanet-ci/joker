package cs.bilkent.zanza.examples.bargaindiscovery;

import java.util.function.Function;

import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import cs.bilkent.zanza.operator.Tuple;

public class CVWAPFunction implements Function<Tuple, Tuple>
{

    public static final String CVWAP_FIELD = "cvwap";

    @Override
    public Tuple apply ( final Tuple tuple )
    {
        final Tuple vwapTuple = new Tuple( TICKER_SYMBOL_FIELD, tuple.getString( TICKER_SYMBOL_FIELD ) );
        vwapTuple.set( TIMESTAMP_FIELD, tuple.get( TIMESTAMP_FIELD ) );

        final double cvwap = ( tuple.getDouble( SINGLE_VWAP_FIELD ) / tuple.getDouble( SINGLE_VOLUME_FIELD ) ) * 100d;
        vwapTuple.set( CVWAP_FIELD, cvwap );

        return vwapTuple;
    }
}
