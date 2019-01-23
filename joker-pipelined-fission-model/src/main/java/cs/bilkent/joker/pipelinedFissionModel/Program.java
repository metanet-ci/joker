package cs.bilkent.joker.pipelinedFissionModel;

import java.util.ArrayList;
import java.util.Iterator;

public class Program implements Cloneable
{
    private ArrayList<Region> regions;

    @Override
    public Object clone ()
    {
        Program program = new Program();
        for ( Region region : regions )
        {
            program.addRegion( (Region) region.clone() );
        }
        return program;
    }

    public Program ()
    {
        regions = new ArrayList<>();
    }

    public double calculateUnboundedThroughput ( double threadSwitchingOverhead, double replicationCostFactor )
    {
        int size = regions.size();
        double throughput = Double.MAX_VALUE;
        for ( int index = 0; index < size; index++ )
        {
            Region region = regions.get( index );
            double selectivity = region.calculateSelectivity();
            double costForSwitchingBetweenThreads = 0;
            if ( index > 0 )
            {
                costForSwitchingBetweenThreads += threadSwitchingOverhead;
            }
            if ( index < size - 1 )
            {
                costForSwitchingBetweenThreads += selectivity * threadSwitchingOverhead;
            }
            double regionThroughput = region.calculateParallelThroughput( threadSwitchingOverhead, replicationCostFactor );
            regionThroughput = 1 / ( costForSwitchingBetweenThreads + ( 1 / regionThroughput ) );
            if ( throughput > regionThroughput )
            {
                throughput = regionThroughput * selectivity;
            }
            else
            {
                throughput = throughput * selectivity;
            }
        }
        throughput = throughput / calculateSelectivity();
        return throughput;
    }

    public double calculateThroughput ( int numCores, double threadSwitchingOverhead, double replicationCostFactor )
    {
        double unboundedThroughput = calculateUnboundedThroughput( threadSwitchingOverhead, replicationCostFactor );
        double unboundedUtil = calculateUnboundedUtilization( threadSwitchingOverhead, replicationCostFactor );
        if ( unboundedUtil >= numCores )
        {
            return ( numCores / unboundedUtil ) * unboundedThroughput;
        }
        else
        {
            return unboundedThroughput;
        }
    }

    public double calculateUnboundedUtilization ( double threadSwitchingOverhead, double replicationCostFactor )
    {
        double utilization = 0;
        double throughput = calculateUnboundedThroughput( threadSwitchingOverhead, replicationCostFactor );
        int rIndex = 0;
        for ( Region region : regions )
        {
            final int replicas = region.getNumReplicas();
            double rOverhead = 0.0;
            if ( rIndex > 0 )
            {
                rOverhead += threadSwitchingOverhead;
            }
            if ( rIndex < getNumRegions() - 1 )
            {
                rOverhead += threadSwitchingOverhead * region.calculateSelectivity();
            }
            utilization += throughput * rOverhead;
            double replicationCost = replicationCostFactor * Region.log2( replicas );
            utilization += throughput * replicationCost;
            throughput = throughput / replicas;
            Iterator<Pipeline> pit = region.getPipelines().iterator();
            for ( int pIndex = 0; pit.hasNext(); ++pIndex )
            {
                Pipeline pipeline = pit.next();
                double pOverhead = 0.0;
                if ( pIndex > 0 )
                {
                    pOverhead += threadSwitchingOverhead;
                }
                if ( pIndex < region.getNumPipelines() - 1 )
                {
                    pOverhead += threadSwitchingOverhead * pipeline.getSelectivity();
                }
                utilization += replicas * throughput * ( pipeline.getCost() + pOverhead );
                throughput *= pipeline.getSelectivity();
            }
            throughput = throughput * replicas;
            rIndex++;
        }
        return utilization;
    }

    public double calculateUtilization ( int numCores, double threadSwitchingOverhead, double replicationCostFactor )
    {
        double unboundedUtilization = calculateUnboundedUtilization( threadSwitchingOverhead, replicationCostFactor );
        if ( unboundedUtilization <= numCores )
        {
            return unboundedUtilization;
        }
        else
        {
            return numCores;
        }
    }

    public double calculateSelectivity ()
    {
        double selectivity = 1;
        for ( Region region : regions )
        {
            selectivity = selectivity * region.calculateSelectivity();
        }
        return selectivity;
    }

    public int getNumRegions ()
    {
        return regions.size();
    }

    public void addRegion ( Region region )
    {
        regions.add( region );
    }

    public ArrayList<Region> getRegions ()
    {
        return regions;
    }

    public Region getFirstRegion ()
    {
        if ( regions.isEmpty() )
        {
            return null;
        }
        return regions.get( 0 );
    }

    public Region getLastRegion ()
    {
        if ( regions.isEmpty() )
        {
            return null;
        }
        return regions.get( regions.size() - 1 );
    }

    // @Override
    public String toString ()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append( "{" );
        boolean first = true;
        for ( Region region : regions )
        {
            if ( !first )
            {
                buffer.append( "," );
            }
            else
            {
                first = false;
            }
            buffer.append( region );
        }
        buffer.append( "}" );
        return buffer.toString();
    }

    public boolean verify ( ArrayList<String> errors )
    {
        boolean noErrors = true;
        if ( regions.size() == 0 )
        {
            noErrors = false;
            errors.add( "Program is empty" );
        }
        for ( Region region : regions )
        {
            noErrors &= region.verify( errors );
        }
        Operator lastOper = null;
        for ( Region region : regions )
        {
            if ( lastOper != null )
            {
                Pipeline firstPipeline = region.getFirstPipeline();
                if ( firstPipeline != null )
                {
                    Operator firstOper = firstPipeline.getFirstOperator();
                    if ( firstOper != null && firstOper.getIndex() != lastOper.getIndex() + 1 )
                    {
                        noErrors = false;
                        errors.add( "Region +" + region + " has a gap after its predecessor" );
                    }
                }
            }
            Pipeline lastPipeline = region.getLastPipeline();
            if ( lastPipeline != null )
            {
                lastOper = lastPipeline.getLastOperator();
            }
        }
        return noErrors;
    }
}


