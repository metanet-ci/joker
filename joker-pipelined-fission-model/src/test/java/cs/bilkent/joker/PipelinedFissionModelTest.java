package cs.bilkent.joker;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import cs.bilkent.joker.pipelinedFissionModel.Operator;
import cs.bilkent.joker.pipelinedFissionModel.PipelinedFissionAlgorithm;
import cs.bilkent.joker.pipelinedFissionModel.Program;
import cs.bilkent.joker.pipelinedFissionModel.StateKind;
import cs.bilkent.joker.pipelinedFissionModel.scalabilityFunction.LinearScalabilityFunction;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PipelinedFissionModelTest
{
    @Test
    public void testModel ()
    {
        final LinearScalabilityFunction linearSF = new LinearScalabilityFunction();

        // index, cost, selectivity, kind
        final Operator o1 = new Operator( 1, 10, 1, StateKind.Stateless, linearSF );
        final Operator o2 = new Operator( 2, 5, 1, StateKind.Stateless, linearSF );
        final Operator o3 = new Operator( 3, 10, 0.9, StateKind.Stateless, linearSF );
        final Operator o4 = new Operator( 4, 240, 0.6, StateKind.Stateful, linearSF );
        final Operator o5 = new Operator( 5, 360, 0.7, StateKind.Stateless, linearSF );
        final Operator o6 = new Operator( 6, 580, 1, StateKind.PartitionedStateful, linearSF );
        final Operator o7 = new Operator( 7, 100, 1, StateKind.Stateless, linearSF );

        final List<Operator> operators = Arrays.asList( o1, o2, o3, o4, o5, o6, o7 );

        final int numCores = 4;
        final double fusionCostThreshold = 50; // if a region has cost smaller than this, it won't be parallelized
        final double threadSwitchingOverhead = 1; // see the paper for description
        final double replicationCostFactor = 1; // see the paper for description

        // find the optimized program configuration
        final Program program = PipelinedFissionAlgorithm.pipelinedFission( operators,
                                                                            numCores,
                                                                            fusionCostThreshold,
                                                                            threadSwitchingOverhead,
                                                                            replicationCostFactor );

        assertThat( program.toString(), is( "{[(1,2,3),(4)]x1,[(5),(6),(7)]x2}" ) );
    }

    @Test
    public void testConfigurations ()
    {
        final LinearScalabilityFunction linearSF = new LinearScalabilityFunction();

        final int numCores = 12;
        final double multiplicationCost = 1;
        // if a region has cost smaller than the fusion cost threshold, it won't be parallelized
        final double fusionCostThreshold = 20 * multiplicationCost;
        final double threadSwitchingOverhead = 10; // TBP: in terms of the multiplication cost
        final double replicationCostFactor = 7.5; // TBP: in terms of the multiplication cost

        // index, cost, selectivity, kind
        final Operator o1 = new Operator( 1, 20, 1, StateKind.Stateless, linearSF );
        final Operator o2 = new Operator( 2, 10, 1, StateKind.Stateless, linearSF );
        final Operator o3 = new Operator( 3, 20, 0.9, StateKind.Stateless, linearSF );
        final Operator o4 = new Operator( 4, 480, 0.6, StateKind.Stateful, linearSF );
        final Operator o5 = new Operator( 5, 720, 0.7, StateKind.Stateless, linearSF );
        final Operator o6 = new Operator( 6, 1160, 1, StateKind.PartitionedStateful, linearSF );
        final Operator o7 = new Operator( 7, 200, 1, StateKind.Stateless, linearSF );

        final List<Operator> operators = Arrays.asList( o1, o2, o3, o4, o5, o6, o7 );
        for (final Operator operator : operators)
        {
            System.out.println(String.format("Operator %d", operator.getIndex()));
            System.out.println(String.format("\tcost: %f multiplications", operator.getCost()));
            System.out.println(String.format("\tselectivity: %f", operator.getSelectivity()));
            System.out.println(String.format("\tstateKind: %s", operator.getKind()));
        }

        for ( double selectivityFactor : new double[]{0.1, 0.2, 0.4, 0.8, 1.0})
        {
            final List<Operator> adjustedOperators = operators.stream()
                                                                  .map( operator -> new Operator( operator.getIndex(),
                                                                                                  operator.getCost() * multiplicationCost,
                                                                                                  operator.getSelectivity() * selectivityFactor,
                                                                                                  operator.getKind(),
                                                                                                  operator.getScalabilityFunction() ) )
                                                                  .collect( Collectors.toList() );
            final Program program = PipelinedFissionAlgorithm.pipelinedFission( adjustedOperators,
                                                                                numCores,
                                                                                fusionCostThreshold,
                                                                                threadSwitchingOverhead,
                                                                                replicationCostFactor );
            System.out.println(String.format("For selectivity factor %f, the optimal configuration is: %s", selectivityFactor, program));
            /*
            Operator 1
                cost: 20.000000 multiplications
                selectivity: 1.000000
                stateKind: Stateless
            Operator 2
                cost: 10.000000 multiplications
                selectivity: 1.000000
                stateKind: Stateless
            Operator 3
                cost: 20.000000 multiplications
                selectivity: 0.900000
                stateKind: Stateless
            Operator 4
                cost: 480.000000 multiplications
                selectivity: 0.600000
                stateKind: Stateful
            Operator 5
                cost: 720.000000 multiplications
                selectivity: 0.700000
                stateKind: Stateless
            Operator 6
                cost: 1160.000000 multiplications
                selectivity: 1.000000
                stateKind: PartitionedStateful
            Operator 7
                cost: 200.000000 multiplications
                selectivity: 1.000000
                stateKind: Stateless
            For selectivity factor 0.100000, the optimal configuration is: {[(1),(2,3)]x1,[(4)]x1,[(5,6,7)]x1}
            For selectivity factor 0.200000, the optimal configuration is: {[(1,2,3)]x2,[(4)]x1,[(5,6,7)]x1}
            For selectivity factor 0.400000, the optimal configuration is: {[(1,2,3)]x1,[(4)]x1,[(5,6,7)]x1}
            For selectivity factor 0.800000, the optimal configuration is: {[(1,2,3)]x1,[(4)]x1,[(5,6,7)]x2}
            For selectivity factor 1.000000, the optimal configuration is: {[(1,2,3)]x1,[(4)]x1,[(5,6,7)]x3}
            */
        }
    }
}
