package cs.bilkent.joker.examples.bargaindiscovery;

import java.util.function.BiConsumer;

import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import cs.bilkent.joker.operator.Tuple;

public class CVWAPFunction implements BiConsumer<Tuple, Tuple>
{

    public static final String CVWAP_FIELD = "cvwap";

    @Override
    public void accept ( final Tuple input, final Tuple output )
    {
        output.set( TICKER_SYMBOL_FIELD, input.getString( TICKER_SYMBOL_FIELD ) );
        output.set( TIMESTAMP_FIELD, input.get( TIMESTAMP_FIELD ) );

        final double cvwap = ( input.getDouble( SINGLE_VWAP_FIELD ) / input.getDouble( SINGLE_VOLUME_FIELD ) ) * 100d;
        output.set( CVWAP_FIELD, cvwap );
    }
}
