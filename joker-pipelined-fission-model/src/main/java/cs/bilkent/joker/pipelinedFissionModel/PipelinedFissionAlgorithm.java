package cs.bilkent.joker.pipelinedFissionModel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import cs.bilkent.joker.pipelinedFissionModel.scalabilityFunction.LinearScalabilityFunction;

public class PipelinedFissionAlgorithm
{
    //pipelined fission algorithm to find optimal configuration
    public static Program pipelinedFission ( List<Operator> O,
                                             int numCores,
                                             double fusionCostThreshold,
                                             double threadSwitchingOverhead,
                                             double replicationCostFactor )
    {
        double maxThroughput = Double.MIN_VALUE;
        Program bestProgram = new Program();

        for ( int i = 0; i <= 10; i++ )
        {
            ArrayList<Integer> nonParallelizableRegionIndices = new ArrayList<>();
            // phase 1: configure set of all regions
            ArrayList<Region> R = configureRegions( O, fusionCostThreshold, nonParallelizableRegionIndices );

            double coreForPipeline = i * 0.1 * numCores;

            // phase 2: configure set of all pipelines
            R = configurePipelines( R, coreForPipeline, threadSwitchingOverhead, replicationCostFactor );
            // phase 3: configure the number of replicas
            R = configureReplicas( R, nonParallelizableRegionIndices, numCores, threadSwitchingOverhead, replicationCostFactor );

            //add regions to program
            Program P = new Program();
            for ( Region aR : R )
            {
                P.addRegion( aR );
            }

            if ( P.calculateThroughput( numCores, threadSwitchingOverhead, replicationCostFactor ) > maxThroughput )
            {
                maxThroughput = P.calculateThroughput( numCores, threadSwitchingOverhead, replicationCostFactor );
                bestProgram = P;
            }
        }

        return bestProgram;
    }

    static class PipelineUtilizationPair
    {
        Pipeline pipeline;
        double utilization;
        int regionIndex;
        int pipelineIndex;
    }


    static class RegionUtilizationPair
    {
        Region region;
        double utilization;
        int regionIndex;
    }

    // method to configure regions
    public static ArrayList<Region> configureRegions ( List<Operator> O,
                                                        double fusionCostThreshold,
                                                        List<Integer> nonParallelizableRegionIndices )
    {
        //regions to return
        ArrayList<Region> R = new ArrayList<>();

        //current list of operators
        ArrayList<Operator> C = new ArrayList<>();

        //variables
        StateKind kind = null;
        ArrayList<String> keys = null;

        //list of indexes of marked operators
        HashSet<Integer> marked = new HashSet<>();

        // first scan to configure regions
        int operatorCount = O.size();
        for ( int i = 0; i < operatorCount; i++ )
        {
            Operator currentOperator = O.get( i );
            if ( currentOperator.getKind() != StateKind.Stateful && ( C.isEmpty() || ( kind == StateKind.Stateless ) ) )
            {
                kind = currentOperator.getKind();

                if ( currentOperator.getKind() == StateKind.PartitionedStateful )
                {
                    keys = currentOperator.getKeys();
                }
            }
            else if ( currentOperator.getKind() == StateKind.PartitionedStateful && keys != null
                      && keys.containsAll( currentOperator.getKeys() ) )
            {
                keys = currentOperator.getKeys();
            }
            else if ( currentOperator.getKind() != StateKind.Stateless )
            {
                // add current region to regions if its cost is greater than threshold
                if ( computeCost( C ) > fusionCostThreshold )
                {
                    Region currentRegion = new Region();
                    Pipeline pipeline = new Pipeline();

                    // add operators to pipeline and region
                    for ( Operator aC : C )
                    {
                        pipeline.addOperator( aC );
                        marked.add( aC.getIndex() );
                    }

                    currentRegion.addPipeline( pipeline );
                    R.add( currentRegion );
                }

                C.clear();

                if ( currentOperator.getKind() != StateKind.Stateful )
                {
                    i = i - 1;
                }
                continue;
            }

            C.add( currentOperator );
        }

        // consider the last region
        if ( computeCost( C ) > fusionCostThreshold )
        {
            Region currentRegion = new Region();
            Pipeline pipeline = new Pipeline();

            for ( Operator aC : C )
            {
                pipeline.addOperator( aC );
                marked.add( aC.getIndex() );
            }

            currentRegion.addPipeline( pipeline );

            R.add( currentRegion );
        }

        C.clear();

        // add unmarked operators to region
        for ( int i = 0; i < operatorCount; i++ )
        {
            if ( marked.contains( i + 1 ) && ( C.size() != 0 ) )
            {
                Region currentRegion = new Region();
                Pipeline pipeline = new Pipeline();

                for ( Operator aC : C )
                {
                    pipeline.addOperator( aC );
                    marked.add( aC.getIndex() );
                }

                currentRegion.addPipeline( pipeline );

                if ( currentRegion.getFirstPipeline().getFirstOperator().getIndex() == 1 )
                {
                    R.add( 0, currentRegion );
                    nonParallelizableRegionIndices.add( 0 );
                }
                else
                {
                    // insert region to right location
                    for ( int index = 0; index < R.size(); index++ )
                    {
                        if ( R.get( index ).getLastPipeline().getLastOperator().getIndex() + 1 == currentRegion.getFirstPipeline()
                                                                                                               .getFirstOperator()
                                                                                                               .getIndex() )
                        {
                            R.add( index + 1, currentRegion );
                            nonParallelizableRegionIndices.add( index + 1 );
                            break;
                        }
                    }
                }

                C.clear();

            }
            else if ( !marked.contains( i + 1 ) )
            {
                C.add( O.get( i ) );
            }
        }

        // consider last region
        if ( C.size() > 0 )
        {
            Region currentRegion = new Region();
            Pipeline pipeline = new Pipeline();

            for ( Operator aC : C )
            {
                pipeline.addOperator( aC );
                marked.add( aC.getIndex() );
            }

            currentRegion.addPipeline( pipeline );
            R.add( currentRegion );
            nonParallelizableRegionIndices.add( R.size() - 1 );
        }

        return R;
    }

    // method to configure pipelines
    private static ArrayList<Region> configurePipelines ( ArrayList<Region> regions,
                                                          double maxUtilization,
                                                          double threadSwitchingOverhead,
                                                          double replicationCostFactor )
    {
        PipelineUtilizationPair pair = findBottleneckPipeline( regions, threadSwitchingOverhead, replicationCostFactor );
        Pipeline bottleneckPipeline = pair.pipeline;
        double totalUtilization = pair.utilization;

        while ( totalUtilization < maxUtilization )
        {
            Region bestSplit = findBestSplit( bottleneckPipeline, threadSwitchingOverhead );
            Region bottleneckRegion = new Region();
            bottleneckRegion.addPipeline( bottleneckPipeline );

            if ( bestSplit.getNumPipelines() == 1 || ( bestSplit.calculateThroughput( threadSwitchingOverhead )
                                                       <= bottleneckRegion.calculateThroughput( threadSwitchingOverhead ) ) )
            {
                break;
            }

            // insert improved pipeline to its location
            int regionIndex = pair.regionIndex;
            Region region = new Region();
            Iterator<Pipeline> pipelines = regions.get( regionIndex ).getPipelines().iterator();

            int count = 0;
            while ( pipelines.hasNext() && count < pair.pipelineIndex )
            {
                region.addPipeline( pipelines.next() );
                count++;
            }

            for ( final Pipeline pipeline : bestSplit.getPipelines() )
            {
                region.addPipeline( pipeline );
            }

            // skip the bottleneck
            pipelines.next();

            while ( pipelines.hasNext() )
            {
                region.addPipeline( pipelines.next() );
            }

            // insert improved pipeline to its location
            regions.set( regionIndex, region );

            // find new bottleneck pipeline
            pair = findBottleneckPipeline( regions, threadSwitchingOverhead, replicationCostFactor );
            bottleneckPipeline = pair.pipeline;
            totalUtilization = pair.utilization;
        }

        return regions;
    }

    // method to configure replicas
    private static ArrayList<Region> configureReplicas ( ArrayList<Region> regions,
                                                         List<Integer> nonParallelizableRegionIndices,
                                                         int numCores,
                                                         double threadSwitchingOverhead,
                                                         double replicationCostFactor )
    {
        Program program = new Program();
        for ( Region region : regions )
        {
            program.addRegion( region );
        }

        RegionUtilizationPair pair = findBottleneckRegion( regions, threadSwitchingOverhead, replicationCostFactor );
        double totalUtilization = pair.utilization;
        Region bottleneckRegion = pair.region;

        // check whether there is enough core
        while ( totalUtilization < numCores )
        {
            if ( nonParallelizableRegionIndices.contains( pair.regionIndex ) )
            {
                break;
            }

            double throughput = program.calculateThroughput( numCores, threadSwitchingOverhead, replicationCostFactor );
            int numReplicas = bottleneckRegion.getNumReplicas();
            bottleneckRegion.setNumReplicas( numReplicas + 1 );
            double enhancedThroughput = program.calculateThroughput( numCores, threadSwitchingOverhead, replicationCostFactor );

            if ( throughput >= enhancedThroughput )
            {
                bottleneckRegion.setNumReplicas( numReplicas );
                break;
            }

            // find new bottleneck region
            pair = findBottleneckRegion( regions, threadSwitchingOverhead, replicationCostFactor );
            bottleneckRegion = pair.region;
            totalUtilization = pair.utilization;
        }

        return regions;
    }

    // method to find bottleneck region
    private static RegionUtilizationPair findBottleneckRegion ( ArrayList<Region> regions,
                                                                double threadSwitchingOverhead,
                                                                double replicationCostFactor )
    {
        LinearScalabilityFunction ls = new LinearScalabilityFunction();
        Operator dummyOperator = new Operator( -1, 0, 1, StateKind.Stateless, ls );
        Pipeline dummyPipeline = new Pipeline();
        Region emptyRegion = new Region();
        emptyRegion.addPipeline( dummyPipeline );
        dummyPipeline.addOperator( dummyOperator );

        double throughput = Double.MAX_VALUE;
        Region bottleneckRegion = new Region();
        int bottleneckRegionIndex = 0;

        int regionCount = regions.size();
        for ( int i = 0; i < regionCount; i++ )
        {
            Program program = new Program();

            for ( int j = 0; j <= i; j++ )
            {
                program.addRegion( regions.get( j ) );
            }

            // We need to add an empty region, otherwise we are being unfair
            // due to the thread switching overhead being added when we add a new region
            program.addRegion( emptyRegion );

            double newThroughput = program.calculateUnboundedThroughput( threadSwitchingOverhead, replicationCostFactor );
            if ( newThroughput < throughput )
            {
                throughput = newThroughput;
                bottleneckRegion = regions.get( i );
                bottleneckRegionIndex = i;
            }
        }

        RegionUtilizationPair pair = new RegionUtilizationPair();
        pair.region = bottleneckRegion;
        pair.regionIndex = bottleneckRegionIndex;
        pair.utilization = calculateUtilization( regions, threadSwitchingOverhead, replicationCostFactor );
        return pair;
    }

    private static double computeCost ( List<Operator> O )
    {
        double cost = 0;
        double selectivity = 1.0;
        for ( Operator aO : O )
        {
            cost += aO.getCost() * selectivity;
            selectivity *= aO.getSelectivity();
        }
        return cost;
    }

    private static PipelineUtilizationPair findBottleneckPipeline ( ArrayList<Region> regions,
                                                                    double threadSwitchingOverhead,
                                                                    double replicationCostFactor )
    {
        PipelineUtilizationPair pair = new PipelineUtilizationPair();
        Pipeline bottleneckPipeline = new Pipeline();
        // hash map to keep pipelines' region
        LinkedHashMap<Pipeline, Integer> pipelines = new LinkedHashMap<>();
        ArrayList<Double> inputRates = new ArrayList<>();

        // find all pipelines in regions
        int regionCount = regions.size();
        for ( int i = 0; i < regionCount; i++ )
        {
            for ( final Pipeline pipeline : regions.get( i ).getPipelines() )
            {
                pipelines.put( pipeline, i );
            }
        }

        // find input rates
        Iterator<Pipeline> pipelineKeys = pipelines.keySet().iterator();

        double selectivity = 1;
        int pipelineCount = pipelines.size();
        for ( int i = 0; i < pipelineCount; i++ )
        {
            inputRates.add( selectivity );
            selectivity *= pipelineKeys.next().getSelectivity();
        }

        // find bottleneck pipeline
        double cost = Double.MIN_VALUE;
        pipelineKeys = pipelines.keySet().iterator();
        for ( int i = 0; i < pipelineCount; i++ )
        {
            Pipeline current = pipelineKeys.next();
            if ( inputRates.get( i ) * current.getCost() > cost )
            {
                bottleneckPipeline = current;
                cost = inputRates.get( i ) * current.getCost();
            }
        }

        // find pipeline index in region
        int pipelineIndex = 0;
        pair.regionIndex = pipelines.get( bottleneckPipeline );

        for ( final Pipeline p : regions.get( pair.regionIndex ).getPipelines() )
        {
            if ( p.equals( bottleneckPipeline ) )
            {
                break;
            }
            else
            {
                pipelineIndex++;
            }
        }

        pair.pipeline = bottleneckPipeline;
        pair.utilization = calculateUtilization( regions, threadSwitchingOverhead, replicationCostFactor );
        pair.pipelineIndex = pipelineIndex;

        return pair;
    }

    private static Region findBestSplit ( Pipeline pipeline, double threadSwitchingOverhead )
    {
        List<Operator> operators = pipeline.getOperators();
        Region bestRegion = new Region();

        if ( operators.size() == 1 )
        {
            bestRegion.addPipeline( pipeline );
            return bestRegion;
        }

        double maxThroughput = Double.MIN_VALUE;

        int operatorCount = operators.size();
        for ( int i = 1; i < operatorCount; i++ )
        {
            Pipeline firstHalf = new Pipeline();
            Pipeline secondHalf = new Pipeline();

            for ( int j = 0; j < i; j++ )
            {
                firstHalf.addOperator( operators.get( j ) );
            }

            for ( int j = i; j < operatorCount; j++ )
            {
                secondHalf.addOperator( operators.get( j ) );
            }

            Region currentRegion = new Region();
            currentRegion.addPipeline( firstHalf );
            currentRegion.addPipeline( secondHalf );

            double throughput = currentRegion.calculateThroughput( threadSwitchingOverhead );

            if ( throughput > maxThroughput )
            {
                maxThroughput = throughput;
                bestRegion = currentRegion;
            }
        }

        return bestRegion;
    }

    private static double calculateUtilization ( ArrayList<Region> regions, double threadSwitchingOverhead, double replicationCostFactor )
    {
        Program p = new Program();
        for ( Region region : regions )
        {
            p.addRegion( region );
        }
        return p.calculateUnboundedThroughput( threadSwitchingOverhead, replicationCostFactor );
    }
}
