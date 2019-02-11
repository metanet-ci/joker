package cs.bilkent.joker.experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
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
import cs.bilkent.joker.operators.MapperOperator;
import cs.bilkent.joker.operators.PartitionedMapperOperator;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class SingleRegionFlowDefFactory4 implements FlowDefFactory
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

            tuple.set( "key", key ).set( "value", value ).setPartitionKey( key );
            if ( curr == vals.length )
            {
                curr = 0;
            }
        }
    }


    public SingleRegionFlowDefFactory4 ()
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

        final OperatorDef source = OperatorDefBuilder.newInstance( "src", BeaconOperator.class )
                                                     .setConfig( sourceConfig )
                                                     .setExtendingSchema( sourceSchema )
                                                     .build();

        final List<Integer> operatorCosts = Arrays.stream( config.getString( "operatorCosts" ).split( "_" ) )
                                                  .map( Integer::parseInt )
                                                  .collect( toList() );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        flowDefBuilder.add( source );

        final OperatorRuntimeSchema multiplier0Schema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                                .addInputField( 0, "value", Integer.class )
                                                                                                .addOutputField( 0, "key", Integer.class )
                                                                                                .addOutputField( 0, "value", Integer.class )
                                                                                                .build();

        final OperatorConfig multiplier0Config = new OperatorConfig().set( PartitionedMapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                           new Multiplication( operatorCosts.get( 0 ) ) );

        final OperatorDef multiplier0 = OperatorDefBuilder.newInstance( "m0", PartitionedMapperOperator.class )
                                                          .setExtendingSchema( multiplier0Schema )
                                                          .setConfig( multiplier0Config )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();

        flowDefBuilder.add( multiplier0 );

        flowDefBuilder.connect( source.getId(), multiplier0.getId() );

        for ( int i = 1; i < operatorCosts.size(); i++ )
        {
            final OperatorRuntimeSchema multiplierSchema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                                   .addInputField( 0,
                                                                                                                   "value",
                                                                                                                   Integer.class )
                                                                                                   .addOutputField( 0,
                                                                                                                    "key",
                                                                                                                    Integer.class )
                                                                                                   .addOutputField( 0,
                                                                                                                    "value",
                                                                                                                    Integer.class )
                                                                                                   .build();

            final OperatorConfig multiplierConfig = new OperatorConfig().set( MapperOperator.MAPPER_CONFIG_PARAMETER,
                                                                              new Multiplication( operatorCosts.get( i ) ) );

            final OperatorDef multiplier = OperatorDefBuilder.newInstance( "m" + i, MapperOperator.class )
                                                             .setExtendingSchema( multiplierSchema )
                                                             .setConfig( multiplierConfig )
                                                             .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
    }

    static class Multiplication implements BiConsumer<Tuple, Tuple>
    {

        private final int multiplicationCount;

        Multiplication ( final int multiplicationCount )
        {
            this.multiplicationCount = multiplicationCount;
        }

        @Override
        public void accept ( final Tuple input, final Tuple output )
        {
            int val = input.getInteger( "value" );
            for ( int i = 0; i < multiplicationCount; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            final Object key = input.get( "key" );
            output.set( "key", key ).set( "value", val ).setPartitionKey( key );
        }
    }

}
