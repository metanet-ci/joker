package cs.bilkent.joker.examples.bargaindiscovery;

import org.junit.Test;

import static cs.bilkent.joker.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import static cs.bilkent.joker.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.flow.OperatorDefBuilder;
import cs.bilkent.joker.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;

public class BargainDiscoveryFlowTest extends AbstractJokerTest
{

    @Test
    public void shouldCreateBargainDiscoveryFlow ()
    {
        final FlowDefBuilder flowBuilder = new FlowDefBuilder();

        final OperatorConfig vwapAggregatorConfig = new OperatorConfig();
        vwapAggregatorConfig.set( WINDOW_SIZE_CONfIG_PARAMETER, 5 );

        flowBuilder.add( OperatorDefBuilder.newInstance( "vwapAggregator", VWAPAggregatorOperator.class )
                                           .setConfig( vwapAggregatorConfig )
                                           .setPartitionFieldNames( singletonList( TICKER_SYMBOL_FIELD ) ) );

        final OperatorConfig cvwapConfig = new OperatorConfig();
        cvwapConfig.set( MAPPER_CONFIG_PARAMETER, new CVWAPFunction() );

        final OperatorRuntimeSchemaBuilder schemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        schemaBuilder.addInputField( 0, TICKER_SYMBOL_FIELD, String.class )
                     .addInputField( 0, SINGLE_VOLUME_FIELD, Double.class )
                     .addInputField( 0, SINGLE_VWAP_FIELD, Double.class )
                     .addInputField( 0, TIMESTAMP_FIELD, Long.class )
                     .addOutputField( 0, TICKER_SYMBOL_FIELD, String.class )
                     .addOutputField( 0, CVWAP_FIELD, Double.class )
                     .addOutputField( 0, TIMESTAMP_FIELD, Long.class );

        flowBuilder.add( OperatorDefBuilder.newInstance( "cvwap", MapperOperator.class )
                                           .setConfig( cvwapConfig )
                                           .setExtendingSchema( schemaBuilder.build() ) );

        final OperatorDefBuilder bargainIndexOperatorBuilder = OperatorDefBuilder.newInstance( "bargainIndex", BargainIndexOperator.class )
                                                                                 .setPartitionFieldNames( singletonList(
                                                                                         TICKER_SYMBOL_FIELD ) );
        flowBuilder.add( bargainIndexOperatorBuilder );

        flowBuilder.connect( "vwapAggregator", "cvwap" );
        flowBuilder.connect( "cvwap", "bargainIndex" );

        assertNotNull( flowBuilder.build() );
    }

}
