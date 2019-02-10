package cs.bilkent.joker.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.ForEachOperator;
import static cs.bilkent.joker.operators.ForEachOperator.CONSUMER_FUNCTION_CONFIG_PARAMETER;
import static java.util.Collections.shuffle;

public class SingleRegionFlowDefFactory3 implements FlowDefFactory
{

    private static final int MULTIPLIER_VALUE = 271;


    static class ValueGenerator implements Consumer<Tuple>
    {
        private final int[] vals;
        private int curr;

        ValueGenerator ( final int keyRange )
        {
            final List<Integer> v = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                for ( int key = 0; key < keyRange; key++ )
                {
                    v.add( key );
                }
            }
            for ( int i = 0; i < 10; i++ )
            {
                shuffle( v );
            }
            vals = new int[ v.size() ];
            for ( int i = 0; i < v.size(); i++ )
            {
                vals[ i ] = v.get( i );
            }
        }

        @Override
        public void accept ( final Tuple tuple )
        {
            final int key = vals[ curr++ ];
            final int value = key + 1;

            tuple.set( "key", key ).set( "value", value );
            if ( curr == vals.length )
            {
                curr = 0;
            }
        }
    }


    public SingleRegionFlowDefFactory3 ()
    {
    }

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int keyRange = config.getInt( "keyRange" );
        final int tuplesPerInvocation = config.getInt( "tuplesPerInvocation" );

        final ValueGenerator valueGenerator = new ValueGenerator( keyRange );
        final OperatorConfig sourceConfig = new OperatorConfig().set( TUPLE_POPULATOR_CONFIG_PARAMETER, valueGenerator )
                                                                .set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, tuplesPerInvocation );

        final OperatorRuntimeSchema sourceSchema = new OperatorRuntimeSchemaBuilder( 0, 1 ).addOutputField( 0, "key", Integer.class )
                                                                                           .addOutputField( 0, "value", Integer.class )
                                                                                           .build();

        final OperatorConfig sinkConfig = new OperatorConfig().set( CONSUMER_FUNCTION_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> {
        } );

        final OperatorDef source = OperatorDefBuilder.newInstance( "src", BeaconOperator.class )
                                                     .setConfig( sourceConfig )
                                                     .setExtendingSchema( sourceSchema )
                                                     .build();

        final OperatorDef sink = OperatorDefBuilder.newInstance( "sink", ForEachOperator.class ).setConfig( sinkConfig ).build();

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        flowDefBuilder.add( source ).add( sink ).connect( source.getId(), sink.getId() );

        return flowDefBuilder.build();
    }

}
