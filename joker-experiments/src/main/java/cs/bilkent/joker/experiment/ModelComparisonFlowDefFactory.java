package cs.bilkent.joker.experiment;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.experiment.SingleRegionFlowDefFactory2.ValueGenerator;
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
import cs.bilkent.joker.operators.FilterOperator;
import cs.bilkent.joker.operators.PartitionedFilterOperator;
import cs.bilkent.joker.operators.StatefulFilterOperator;
import static java.util.Collections.singletonList;

public class ModelComparisonFlowDefFactory implements FlowDefFactory
{

    private static final int MULTIPLIER_VALUE = 271;

    public static void main ( String[] args ) throws IllegalAccessException, InterruptedException, ExecutionException,
                                                             InstantiationException, TimeoutException, ClassNotFoundException
    {
        System.setProperty( "flowFactory", "cs.bilkent.joker.experiment.ModelComparisonFlowDefFactory" );
        System.setProperty( "vizPath", "joker-experiments/scripts/viz.py" );
        System.setProperty( "joker.engine.metricManager.pipelineMetricsScanningPeriodInMillis", "1000" );
        System.setProperty( "joker.engine.metricManager.warmupIterations", "10" );
        System.setProperty( "joker.engine.metricManager.historySize", "20" );
        System.setProperty( "reportDir", "joker-experiments/scripts/model" );

        System.setProperty( "keyRange", "1084" );
        System.setProperty( "tuplesPerInvocation", "1" );

        System.setProperty( "multiplicationCount1", "20" );
        System.setProperty( "multiplicationCount2", "10" );
        System.setProperty( "multiplicationCount3", "20" );
        System.setProperty( "multiplicationCount4", "480" );
        System.setProperty( "multiplicationCount5", "720" );
        System.setProperty( "multiplicationCount6", "1160" );
        System.setProperty( "multiplicationCount7", "200" );

        System.setProperty( "selectivity1", "100" );
        System.setProperty( "selectivity2", "100" );
        System.setProperty( "selectivity3", "90" );
        System.setProperty( "selectivity4", "60" );
        System.setProperty( "selectivity5", "70" );
        System.setProperty( "selectivity6", "100" );
        System.setProperty( "selectivity7", "100" );

        ExperimentRunner.main( args );

        //        SelectivityConfigurablePredicate pred = new SelectivityConfigurablePredicate( "op", 5, 5, 1 );
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
        //        System.out.println(pred.test( Tuple.of( "key" , 1, "value", 1 ) ));
    }


    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();
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

        final int multiplicationCount1 = config.getInt( "multiplicationCount1" );
        final int multiplicationCount2 = config.getInt( "multiplicationCount2" );
        final int multiplicationCount3 = config.getInt( "multiplicationCount3" );
        final int multiplicationCount4 = config.getInt( "multiplicationCount4" );
        final int multiplicationCount5 = config.getInt( "multiplicationCount5" );
        final int multiplicationCount6 = config.getInt( "multiplicationCount6" );
        final int multiplicationCount7 = config.getInt( "multiplicationCount7" );

        final int selectivity1 = config.getInt( "selectivity1" );
        final int selectivity2 = config.getInt( "selectivity2" );
        final int selectivity3 = config.getInt( "selectivity3" );
        final int selectivity4 = config.getInt( "selectivity4" );
        final int selectivity5 = config.getInt( "selectivity5" );
        final int selectivity6 = config.getInt( "selectivity6" );
        final int selectivity7 = config.getInt( "selectivity7" );

        System.out.println( "PARAMETERS: " );
        System.out.println( "keyRange: " + keyRange );
        System.out.println( "tuplesPerInvocation: " + tuplesPerInvocation );
        System.out.println( "multiplicationCount1: " + multiplicationCount1 );
        System.out.println( "multiplicationCount2: " + multiplicationCount2 );
        System.out.println( "multiplicationCount3: " + multiplicationCount3 );
        System.out.println( "multiplicationCount4: " + multiplicationCount4 );
        System.out.println( "multiplicationCount5: " + multiplicationCount5 );
        System.out.println( "multiplicationCount6: " + multiplicationCount6 );
        System.out.println( "multiplicationCount7: " + multiplicationCount7 );
        System.out.println( "selectivity1: " + selectivity1 );
        System.out.println( "selectivity2: " + selectivity2 );
        System.out.println( "selectivity3: " + selectivity3 );
        System.out.println( "selectivity4: " + selectivity4 );
        System.out.println( "selectivity5: " + selectivity5 );
        System.out.println( "selectivity6: " + selectivity6 );
        System.out.println( "selectivity7: " + selectivity7 );

        final String multiplier1Id = "m1";
        final String multiplier2Id = "m2";
        final String multiplier3Id = "m3";
        final String multiplier4Id = "m4";
        final String multiplier5Id = "m5";
        final String multiplier6Id = "m6";
        final String multiplier7Id = "m7";

        final int period = 100;

        final Supplier<Predicate<Tuple>> predicateSupplier1 = () -> new SelectivityConfigurablePredicate( multiplier1Id, selectivity1,
                                                                                                          period,
                                                                                                          multiplicationCount1 );
        final Supplier<Predicate<Tuple>> predicateSupplier2 = () -> new SelectivityConfigurablePredicate( multiplier2Id, selectivity2,
                                                                                                          period,
                                                                                                          multiplicationCount2 );
        final Supplier<Predicate<Tuple>> predicateSupplier3 = () -> new SelectivityConfigurablePredicate( multiplier3Id, selectivity3,
                                                                                                          period,
                                                                                                          multiplicationCount3 );
        final Supplier<Predicate<Tuple>> predicateSupplier4 = () -> new SelectivityConfigurablePredicate( multiplier4Id, selectivity4,
                                                                                                          period,
                                                                                                          multiplicationCount4 );
        final Supplier<Predicate<Tuple>> predicateSupplier5 = () -> new SelectivityConfigurablePredicate( multiplier5Id, selectivity5,
                                                                                                          period,
                                                                                                          multiplicationCount5 );
        final Supplier<Predicate<Tuple>> predicateSupplier6 = () -> new SelectivityConfigurablePredicate( multiplier6Id, selectivity6,
                                                                                                          period,
                                                                                                          multiplicationCount6 );
        final Supplier<Predicate<Tuple>> predicateSupplier7 = () -> new SelectivityConfigurablePredicate( multiplier7Id, selectivity7,
                                                                                                          period,
                                                                                                          multiplicationCount7 );

        final OperatorRuntimeSchema multiplierSchema = new OperatorRuntimeSchemaBuilder( 1, 1 ).addInputField( 0, "key", Integer.class )
                                                                                               .addInputField( 0, "value", Integer.class )
                                                                                               .addOutputField( 0, "key", Integer.class )
                                                                                               .addOutputField( 0, "value", Integer.class )
                                                                                               .build();

        final OperatorConfig multiplierConfig1 = new OperatorConfig().set( PartitionedFilterOperator.PREDICATE_CONFIG_PARAMETER,
                                                                           predicateSupplier1 );

        final OperatorDef multiplier1 = OperatorDefBuilder.newInstance( multiplier1Id, PartitionedFilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig1 )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();

        final OperatorConfig multiplierConfig2 = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicateSupplier2 );

        final OperatorDef multiplier2 = OperatorDefBuilder.newInstance( multiplier2Id, FilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig2 )
                                                          .build();

        final OperatorConfig multiplierConfig3 = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicateSupplier3 );

        final OperatorDef multiplier3 = OperatorDefBuilder.newInstance( multiplier3Id, FilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig3 )
                                                          .build();

        final OperatorConfig multiplierConfig4 = new OperatorConfig().set( StatefulFilterOperator.PREDICATE_CONFIG_PARAMETER,
                                                                           predicateSupplier4 );

        final OperatorDef multiplier4 = OperatorDefBuilder.newInstance( multiplier4Id, StatefulFilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig4 )
                                                          .build();

        final OperatorConfig multiplierConfig5 = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicateSupplier5 );

        final OperatorDef multiplier5 = OperatorDefBuilder.newInstance( multiplier5Id, FilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig5 )
                                                          .build();

        final OperatorConfig multiplierConfig6 = new OperatorConfig().set( PartitionedFilterOperator.PREDICATE_CONFIG_PARAMETER,
                                                                           predicateSupplier6 );

        final OperatorDef multiplier6 = OperatorDefBuilder.newInstance( multiplier6Id, PartitionedFilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig6 )
                                                          .setPartitionFieldNames( singletonList( "key" ) )
                                                          .build();

        final OperatorConfig multiplierConfig7 = new OperatorConfig().set( FilterOperator.PREDICATE_CONFIG_PARAMETER, predicateSupplier7 );

        final OperatorDef multiplier7 = OperatorDefBuilder.newInstance( multiplier7Id, FilterOperator.class )
                                                          .setExtendingSchema( multiplierSchema )
                                                          .setConfig( multiplierConfig7 )
                                                          .build();

        flowDefBuilder.add( source );
        flowDefBuilder.add( multiplier1 );
        flowDefBuilder.add( multiplier2 );
        flowDefBuilder.add( multiplier3 );
        flowDefBuilder.add( multiplier4 );
        flowDefBuilder.add( multiplier5 );
        flowDefBuilder.add( multiplier6 );
        flowDefBuilder.add( multiplier7 );

        flowDefBuilder.connect( source.getId(), multiplier1.getId() );
        flowDefBuilder.connect( multiplier1.getId(), multiplier2.getId() );
        flowDefBuilder.connect( multiplier2.getId(), multiplier3.getId() );
        flowDefBuilder.connect( multiplier3.getId(), multiplier4.getId() );
        flowDefBuilder.connect( multiplier4.getId(), multiplier5.getId() );
        flowDefBuilder.connect( multiplier5.getId(), multiplier6.getId() );
        flowDefBuilder.connect( multiplier6.getId(), multiplier7.getId() );

        return flowDefBuilder.build();
    }


    static class SelectivityConfigurablePredicate implements Predicate<Tuple>
    {

        private final String operatorId;

        private final int selectivity;

        private final int period;

        private final int multiplicationCount;

        private int sum;

        private int count;

        private SelectivityConfigurablePredicate ( final String operatorId,
                                                   final int selectivity,
                                                   final int period,
                                                   final int multiplicationCount )
        {
            this.operatorId = operatorId;
            this.selectivity = selectivity;
            this.period = period;
            this.multiplicationCount = multiplicationCount;
        }

        @Override
        public boolean test ( final Tuple input )
        {
            int val = input.getInteger( "value" ) + sum;
            for ( int i = 0; i < multiplicationCount; i++ )
            {
                val = val * MULTIPLIER_VALUE;
            }
            val = val * MULTIPLIER_VALUE;
            sum += val;

            if ( ++count <= selectivity )
            {
                if ( count == period )
                {
                    count = 0;
                    sum = 0;
                }

                return true;
            }

            if ( count == period )
            {
                count = 0;
                sum = 0;
            }

            return false;
        }

    }

}
