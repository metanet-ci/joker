package cs.bilkent.zanza.examples.bargaindiscovery;

import org.junit.Test;

import static cs.bilkent.zanza.examples.bargaindiscovery.CVWAPFunction.CVWAP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VOLUME_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.SINGLE_VWAP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TICKER_SYMBOL_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.TIMESTAMP_FIELD;
import static cs.bilkent.zanza.examples.bargaindiscovery.VWAPAggregatorOperator.WINDOW_SIZE_CONfIG_PARAMETER;
import cs.bilkent.zanza.flow.FlowDefBuilder;
import cs.bilkent.zanza.flow.OperatorDefBuilder;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operators.MapperOperator;
import static cs.bilkent.zanza.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.zanza.testutils.ZanzaTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;

public class BargainDiscoveryFlowTest extends ZanzaTest
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
        schemaBuilder.getInputPortSchemaBuilder( 0 )
                     .addField( TICKER_SYMBOL_FIELD, String.class )
                     .addField( SINGLE_VOLUME_FIELD, Double.class )
                     .addField( SINGLE_VWAP_FIELD, Double.class )
                     .addField( TIMESTAMP_FIELD, Long.class );
        schemaBuilder.getOutputPortSchemaBuilder( 0 )
                     .addField( TICKER_SYMBOL_FIELD, String.class )
                     .addField( CVWAP_FIELD, Double.class )
                     .addField( TIMESTAMP_FIELD, Long.class );

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
