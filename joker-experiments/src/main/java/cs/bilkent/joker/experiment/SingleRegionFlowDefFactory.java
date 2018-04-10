package cs.bilkent.joker.experiment;

import java.util.Arrays;
import java.util.List;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.experiment.BaseMultiplierOperator.MULTIPLICATION_COUNT;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEYS_PER_INVOCATION_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEY_RANGE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.TUPLES_PER_KEY_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.VALUE_RANGE_CONFIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class SingleRegionFlowDefFactory implements FlowDefFactory
{

    public SingleRegionFlowDefFactory ()
    {
    }

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );
        final List<Integer> operatorCosts = Arrays.stream( config.getString( "operatorCosts" ).split( "_" ) )
                                                  .map( Integer::parseInt )
                                                  .collect( toList() );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig().set( KEY_RANGE_CONFIG_PARAMETER, keyRange )
                                                          .set( VALUE_RANGE_CONFIG_PARAMETER, valueRange )
                                                          .set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey )
                                                          .set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptionerConfig = new OperatorConfig().set( MULTIPLICATION_COUNT, operatorCosts.get( 0 ) );

        OperatorDef ptioner = OperatorDefBuilder.newInstance( "m0", PartitionedStatefulMultiplierOperator.class )
                                                .setConfig( ptionerConfig )
                                                .setPartitionFieldNames( singletonList( "key1" ) )
                                                .build();

        flowDefBuilder.add( ptioner );

        flowDefBuilder.connect( beacon.getId(), ptioner.getId() );

        for ( int i = 1; i < operatorCosts.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig().set( MULTIPLICATION_COUNT, operatorCosts.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
    }

}
