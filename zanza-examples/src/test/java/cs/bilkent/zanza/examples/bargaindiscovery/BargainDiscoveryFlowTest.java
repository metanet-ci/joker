package cs.bilkent.zanza.examples.bargaindiscovery;

import org.junit.Test;

import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import cs.bilkent.zanza.flow.FlowBuilder;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
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

        flowBuilder.add( OperatorDefinitionBuilder.newInstance("vwapAggregator", VWAPAggregatorOperator.class).setConfig( vwapAggregatorConfig ).setPartitionKeyExtractor( PartitionKeyExtractors.fieldAsPartitionKey( TICKER_SYMBOL_FIELD ) ) );

        final OperatorConfig cvwapConfig = new OperatorConfig();
        cvwapConfig.set( MAPPER_CONFIG_PARAMETER, new CVWAPFunction() );

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "cvwap", MapperOperator.class ).setConfig( cvwapConfig ) );

        flowBuilder.add(OperatorDefinitionBuilder.newInstance( "bargainIndex", BargainIndexOperator.class ) );

        flowBuilder.connect( "vwapAggregator", "cvwap" );
        flowBuilder.connect( "cvwap", "bargainIndex" );

        assertNotNull( flowBuilder.build() );
    }

}
