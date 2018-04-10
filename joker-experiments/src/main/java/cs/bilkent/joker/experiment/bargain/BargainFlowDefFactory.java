package cs.bilkent.joker.experiment.bargain;

import java.util.function.Consumer;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.examples.bargaindiscovery.BargainIndexOperator;
import cs.bilkent.joker.examples.bargaindiscovery.CVWAPFunction;
import static cs.bilkent.joker.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import cs.bilkent.joker.experiment.FlowDefFactory;
import static cs.bilkent.joker.experiment.bargain.TickerPriceBaseOperator.MAX_PRICE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.bargain.TickerPriceBaseOperator.MIN_PRICE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.bargain.TickerPriceBaseOperator.TICKERS_PER_TIME_UNIT_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.bargain.TickerPriceBaseOperator.TUPLES_PER_INVOCATION_CONFIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.ForEachOperator;
import static cs.bilkent.joker.operators.ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static java.util.Collections.singletonList;

public class BargainFlowDefFactory implements FlowDefFactory
{
    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int tickerCount = config.getInt( "tickerCount" );
        final int minPrice = config.getInt( "minPrice" );
        final int maxPrice = config.getInt( "maxPrice" );
        final int tradesPerTimeUnit = config.getInt( "tradesPerTimeUnit" );
        final int quotesPerTimeUnit = config.getInt( "quotesPerTimeUnit" );
        final int tuplesPerInvocation = config.getInt( "tuplesPerInvocation" );

        TickerGenerator.init( tickerCount );

        final OperatorConfig tradeBeaconConfig = new OperatorConfig().set( MIN_PRICE_CONFIG_PARAMETER, minPrice )
                                                                     .set( MAX_PRICE_CONFIG_PARAMETER, maxPrice )
                                                                     .set( TICKERS_PER_TIME_UNIT_CONFIG_PARAMETER, tradesPerTimeUnit )
                                                                     .set( TUPLES_PER_INVOCATION_CONFIG_PARAMETER, tuplesPerInvocation );

        final OperatorDef tradeBeaconOperator = OperatorDefBuilder.newInstance( "tb", TradeBeaconOperator.class )
                                                                  .setConfig( tradeBeaconConfig )
                                                                  .build();

        final OperatorConfig quoteBeaconConfig = new OperatorConfig().set( MIN_PRICE_CONFIG_PARAMETER, minPrice )
                                                                     .set( MAX_PRICE_CONFIG_PARAMETER, maxPrice )
                                                                     .set( TICKERS_PER_TIME_UNIT_CONFIG_PARAMETER, quotesPerTimeUnit )
                                                                     .set( TUPLES_PER_INVOCATION_CONFIG_PARAMETER, tuplesPerInvocation );

        final OperatorDef quoteBeaconOperator = OperatorDefBuilder.newInstance( "qb", QuoteBeaconOperator.class )
                                                                  .setConfig( quoteBeaconConfig )
                                                                  .build();

        final OperatorConfig vwapAggregatorConfig = new OperatorConfig().set( WINDOW_SIZE_CONfIG_PARAMETER, 5 );

        final OperatorDefBuilder vwapAggregatorOperator = OperatorDefBuilder.newInstance( "vw", VWAPAggregatorOperator.class )
                                                                            .setConfig( vwapAggregatorConfig )
                                                                            .setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) );

        final OperatorConfig cvwapConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER, new CVWAPFunction() );

        final OperatorRuntimeSchemaBuilder cvwapSchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        cvwapSchemaBuilder.addInputField( 0, TICKER_SYMBOL_FIELD, String.class )
                          .addInputField( 0, SINGLE_VOLUME_FIELD, Double.class )
                          .addInputField( 0, SINGLE_VWAP_FIELD, Double.class )
                          .addInputField( 0, TIMESTAMP_FIELD, Long.class )
                          .addOutputField( 0, TICKER_SYMBOL_FIELD, String.class )
                          .addOutputField( 0, CVWAP_FIELD, Double.class )
                          .addOutputField( 0, TIMESTAMP_FIELD, Long.class );

        final OperatorDefBuilder cvwapOperator = OperatorDefBuilder.newInstance( "cv", MapperOperator.class )
                                                                   .setConfig( cvwapConfig )
                                                                   .setExtendingSchema( cvwapSchemaBuilder.build() );

        final OperatorDef bargainIndexOperator = OperatorDefBuilder.newInstance( "bi", BargainIndexOperator.class )
                                                                   .setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) )
                                                                   .build();

        final OperatorConfig sinkOperatorConfig = new OperatorConfig().set( CONSUMER_FUNCTION_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> {
        } );

        final OperatorDef sinkOperator = OperatorDefBuilder.newInstance( "si", ForEachOperator.class )
                                                           .setConfig( sinkOperatorConfig )
                                                           .build();

        return new FlowDefBuilder().add( tradeBeaconOperator )
                                   .add( quoteBeaconOperator )
                                   .add( vwapAggregatorOperator )
                                   .add( cvwapOperator )
                                   .add( bargainIndexOperator )
                                   .add( sinkOperator )
                                   .connect( "tb", "vw" )
                                   .connect( "vw", "cv" )
                                   .connect( "cv", "bi", 0 )
                                   .connect( "qb", "bi", 1 )
                                   .connect( "bi", "si" )
                                   .build();
    }

}
