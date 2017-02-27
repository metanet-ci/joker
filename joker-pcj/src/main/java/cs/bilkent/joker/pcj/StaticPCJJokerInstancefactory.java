package cs.bilkent.joker.pcj;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.migration.MigrationService;
import cs.bilkent.joker.engine.region.impl.AbstractRegionExecutionPlanFactory;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StaticPCJJokerInstancefactory implements PCJJokerInstanceFactory
{

    public StaticPCJJokerInstancefactory ()
    {
    }

    @Override
    public Joker createJokerInstance ( final Object jokerId, final MigrationService migrationService )
    {
        final JokerConfig jokerConfig = new JokerConfig();
        final Joker joker = new JokerBuilder( jokerConfig ).setRegionExecutionPlanFactory( new StaticRegionExecutionPlanFactory(
                jokerConfig,
                                                                                                                                 2 ) )
                                                           .setJokerId( jokerId )
                                                           .build();
        final Random random = new Random();
        final OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( TUPLE_COUNT_CONFIG_PARAMETER, 10 );
        beaconConfig.set( TUPLE_POPULATOR_CONFIG_PARAMETER, (Consumer<Tuple>) tuple ->
        {
            sleepUninterruptibly( 250 + random.nextInt( 100 ), TimeUnit.MILLISECONDS );
            tuple.set( "field1", random.nextInt( 10 ) );
        } );
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchema.addOutputField( 0, "field1", Integer.class );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorConfig mapperConfig = new OperatorConfig();
        mapperConfig.set( MAPPER_CONFIG_PARAMETER,
                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> output.set( "field1", input.get( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder mapperSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        mapperSchema.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef mapper = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class )
                                                     .setConfig( mapperConfig )
                                                     .setExtendingSchema( mapperSchema )
                                                     .build();

        final FlowDef flowDef = new FlowDefBuilder().add( beacon ).add( mapper ).connect( "beacon", "mapper" ).build();
        joker.run( flowDef );
        return joker;
    }

    static class StaticRegionExecutionPlanFactory extends AbstractRegionExecutionPlanFactory
    {

        private final int replicaCount;

        public StaticRegionExecutionPlanFactory ( final JokerConfig jokerConfig, final int replicaCount )
        {
            super( jokerConfig );
            this.replicaCount = replicaCount;
        }

        @Override
        protected RegionExecutionPlan createRegionExecutionPlan ( final RegionDef regionDef )
        {
            final int replicaCount = regionDef.getRegionType() == PARTITIONED_STATEFUL ? this.replicaCount : 1;
            final int operatorCount = regionDef.getOperatorCount();
            final List<Integer> pipelineStartIndices = operatorCount == 1 ? singletonList( 0 ) : asList( 0, operatorCount / 2 );

            return new RegionExecutionPlan( regionDef, pipelineStartIndices, replicaCount );
        }
    }

}
