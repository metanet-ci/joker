package cs.bilkent.joker;

import java.util.Arrays;
import java.util.List;

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
        final Operator o6 = new Operator( 6, 580, 1, StateKind.Stateless, linearSF );
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
}
