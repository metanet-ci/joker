package cs.bilkent.joker.test.pipelinedFissionModel;

import java.util.ArrayList;
import java.util.List;

public class Region implements Cloneable
{
    private ArrayList<Pipeline> pipelines;
    private int numReplicas;
    private StateKind kind = StateKind.Stateless;
    private ArrayList<String> keys;

    public Object clone ()
    {
        Region region = new Region();
        for ( Pipeline pipeline : pipelines )
        {
            region.addPipeline( pipeline );
        }
        region.setNumReplicas( numReplicas );
        return region;
    }

    public Region ()
    {
        numReplicas = 1;
        pipelines = new ArrayList<>();
    }

    public double calculateSelectivity ()
    {
        int size = pipelines.size();
        double selectivity = 1;
        for ( Pipeline pipeline : pipelines )
        {
            selectivity = selectivity * pipeline.getSelectivity();
        }
        return selectivity;
    }

    public double calculateThroughput ( double threadSwitchingOverhead )
    {
        int size = pipelines.size();
        double throughput = Double.MAX_VALUE;
        for ( int index = 0; index < size; index++ )
        {
            Pipeline pipeline = pipelines.get( index );
            double selectivity = pipeline.getSelectivity();
            double costForSwitchingBetweenThreads = 0;
            if ( index > 0 )
            {
                costForSwitchingBetweenThreads += threadSwitchingOverhead;
            }
            if ( index < size - 1 )
            {
                costForSwitchingBetweenThreads += ( selectivity * threadSwitchingOverhead );
            }
            double cost = costForSwitchingBetweenThreads + pipeline.getCost();
            double maximumInputThroughput = 1 / cost;
            if ( throughput > maximumInputThroughput )
            {
                throughput = maximumInputThroughput * selectivity;
            }
            else
            {
                throughput = throughput * selectivity;
            }
        }
        throughput = throughput / calculateSelectivity();
        return throughput;
    }

    public double calculateParallelThroughput ( double threadSwitchingOverhead, double replicationCostFactor )
    {
        double replicationCost = replicationCostFactor * log2( getNumReplicas() );
        double parallelThroughput =
                1 / ( replicationCost + ( 1 / ( calculateThroughput( threadSwitchingOverhead ) * getDerivedScalability() ) ) );
        return parallelThroughput;
    }

    public double getDerivedScalability ()
    {
        return pipelines.get( 0 ).getFirstOperator().getScalability().getScale( numReplicas );
    }

    public int getNumPipelines ()
    {
        return pipelines.size();
    }

    public int getNumReplicas ()
    {
        return numReplicas;
    }

    public StateKind getKind ()
    {
        return kind;
    }

    public void setNumReplicas ( int numReplicas )
    {
        this.numReplicas = numReplicas;
    }

    public boolean hasKeys ()
    {
        return keys != null;
    }

    public ArrayList<String> getKeys ()
    {
        return keys;
    }

    public void addPipeline ( Pipeline pipeline )
    {
        pipelines.add( pipeline );
        StateKind oKind = pipeline.getKind();
        ArrayList<String> oKeys = pipeline.getKeys();
        StateKindMerger.StateKindAndKeys result = StateKindMerger.mergeLeft( kind, keys, oKind, oKeys );
        kind = result.kind;
        keys = result.keys;
    }

    public List<Pipeline> getPipelines ()
    {
        return pipelines;
    }

    public Pipeline getFirstPipeline ()
    {
        if ( pipelines.isEmpty() )
        {
            return null;
        }
        return pipelines.get( 0 );
    }

    public Pipeline getLastPipeline ()
    {
        if ( pipelines.isEmpty() )
        {
            return null;
        }
        return pipelines.get( pipelines.size() - 1 );
    }

    public static double log2 ( double n )
    {
        return ( Math.log( n ) / Math.log( 2 ) );
    }

    public boolean verify ( ArrayList<String> errors )
    {
        boolean noErrors = true;
        if ( pipelines.size() == 0 )
        {
            noErrors = false;
            errors.add( "Region is empty" );
        }
        for ( Pipeline pipeline : pipelines )
        {
            noErrors &= pipeline.verify( errors );
        }
        Operator lastOper = null;
        for ( Pipeline pipeline : pipelines )
        {
            if ( lastOper != null )
            {
                Operator firstOper = pipeline.getFirstOperator();
                if ( firstOper != null && firstOper.getIndex() != lastOper.getIndex() + 1 )
                {
                    noErrors = false;
                    errors.add( "Pipeline +" + pipeline + " has a gap after its predecessor" );
                }
            }
            lastOper = pipeline.getLastOperator();
        }

        return noErrors;
    }

    // @Override
    public String toString ()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append( "[" );
        boolean first = true;
        for ( Pipeline pipeline : pipelines )
        {
            if ( !first )
            {
                buffer.append( "," );
            }
            else
            {
                first = false;
            }
            buffer.append( pipeline );
        }
        buffer.append( "]x" );
        buffer.append( numReplicas );
        return buffer.toString();
    }
}