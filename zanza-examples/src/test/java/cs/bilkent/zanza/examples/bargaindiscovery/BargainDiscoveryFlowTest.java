package cs.bilkent.zanza.examples.bargaindiscovery;

import org.junit.Test;

import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import cs.bilkent.zanza.flow.FlowBuilder;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PartitionKeyExtractors;
import cs.bilkent.zanza.operators.MapperOperator;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static org.junit.Assert.assertNotNull;

public class BargainDiscoveryFlowTest
{

    @Test
    public void shouldCreateBargainDiscoveryFlow ()
    {
        final FlowBuilder flowBuilder = new FlowBuilder();

        final OperatorConfig vwapAggregatorConfig = new OperatorConfig();
        vwapAggregatorConfig.set( WINDOW_SIZE_CONfIG_PARAMETER, 5 );
        vwapAggregatorConfig.setPartitionKeyExtractor( PartitionKeyExtractors.fieldAsPartitionKey( TICKER_SYMBOL_FIELD ) );

        flowBuilder.add( "vwapAggregator", VWAPAggregatorOperator.class, vwapAggregatorConfig );

        final OperatorConfig cvwapConfig = new OperatorConfig();
        cvwapConfig.set( MAPPER_CONFIG_PARAMETER, new CVWAPFunction() );

        flowBuilder.add( "cvwap", MapperOperator.class, cvwapConfig );

        flowBuilder.add( "bargainIndex", BargainIndexOperator.class, null );

        flowBuilder.connect( "vwapAggregator", "cvwap" );
        flowBuilder.connect( "cvwap", "bargainIndex" );

        assertNotNull( flowBuilder.build() );
    }

}
