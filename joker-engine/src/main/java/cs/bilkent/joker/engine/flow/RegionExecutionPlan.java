package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.checkPipelineStartIndicesToMerge;
import static cs.bilkent.joker.engine.region.impl.RegionExecutionPlanUtil.checkPipelineStartIndicesToSplit;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Represents an execution plan for a {@link RegionDef} object. Each region contains a chain of operators.
 * Operators of a region are split into pipelines. Due to its type, regions can have multiple replicas.
 * For instance, if a region has 2 replicas, each pipeline of the region will have 2 replicas as well.
 * Only {@link OperatorType#PARTITIONED_STATEFUL} regions can have multiple replicas for now.
 * <p>
 * Each pipeline is represented with a {@link PipelineId} object, which contains two field: id of the region and in-region index of the
 * pipeline's first operator.
 */
public class RegionExecutionPlan
{

    private final RegionDef regionDef;

    private final int replicaCount;

    private final List<Integer> pipelineStartIndices;

    public RegionExecutionPlan ( final RegionDef regionDef, final List<Integer> pipelineStartIndices, final int replicaCount )
    {
        checkArgument( ( regionDef.getRegionType() == STATEFUL || regionDef.getRegionType() == STATELESS )
                       ? replicaCount == 1
                       : replicaCount > 0,
                       "Invalid replica count: %s for %s region with id: %s",
                       replicaCount,
                       regionDef.getRegionType(),
                       regionDef.getRegionId() );
        validatePipelineStartIndices( regionDef.getOperators(), pipelineStartIndices );
        this.regionDef = regionDef;
        this.replicaCount = replicaCount;
        this.pipelineStartIndices = unmodifiableList( new ArrayList<>( pipelineStartIndices ) );
    }

    private void validatePipelineStartIndices ( final List<OperatorDef> operators, final List<Integer> pipelineStartIndices )
    {
        if ( pipelineStartIndices.get( 0 ) != 0 )
        {
            pipelineStartIndices.add( 0, 0 );
        }
        checkArgument( pipelineStartIndices.size() <= operators.size(), "invalid pipeline start indices: %s", pipelineStartIndices );
        int i = -1;
        for ( int startIndex : pipelineStartIndices )
        {
            checkArgument( startIndex > i, "invalid pipeline start indices: %s", pipelineStartIndices );
            i = startIndex;
        }
        checkArgument( i < operators.size() );
    }

    /**
     * Returns id of the region
     *
     * @return id of the region
     */
    public int getRegionId ()
    {
        return regionDef.getRegionId();
    }

    /**
     * Returns definition of the region
     *
     * @return definition of the region
     */
    public RegionDef getRegionDef ()
    {
        return regionDef;
    }

    public OperatorType getRegionType ()
    {
        return regionDef.getRegionType();
    }

    /**
     * Returns replica count of the region execution plan
     *
     * @return replica count of the region execution plan
     */
    public int getReplicaCount ()
    {
        return replicaCount;
    }

    /**
     * Returns number of pipelines in the region execution plan
     *
     * @return number of pipelines in the region execution plan
     */
    public int getPipelineCount ()
    {
        return pipelineStartIndices.size();
    }

    /**
     * Returns ids of the pipelines present in the region execution plan
     *
     * @return ids of the pipelines present in the region execution plan
     */
    public List<PipelineId> getPipelineIds ()
    {
        return pipelineStartIndices.stream().map( i -> new PipelineId( regionDef.getRegionId(), i ) ).collect( toList() );
    }

    /**
     * Returns in-region indices of the operators which are first operators of pipelines
     *
     * @return in-region indices of the operators which are first operators of pipelines
     */
    public List<Integer> getPipelineStartIndices ()
    {
        return pipelineStartIndices;
    }

    /**
     * Returns in-region index of the first operator of the pipeline
     *
     * @param pipelineIndex
     *         to get in-region index of its first operator
     *
     * @return in-region index of the first operator of the pipeline
     */
    public int getPipelineStartIndex ( final int pipelineIndex )
    {
        return pipelineStartIndices.get( pipelineIndex );
    }

    public PipelineId getPipelineId ( final int pipelineIndex )
    {
        return new PipelineId( getRegionId(), getPipelineStartIndex( pipelineIndex ) );
    }

    /**
     * Returns index of pipeline of the operator with the given in-region index
     *
     * @param pipelineStartIndex
     *         in-region index of the operator which is first operator of the pipeline
     *
     * @returnindex of pipeline of the operator with the given in-region index
     */
    public int getPipelineIndex ( final int pipelineStartIndex )
    {
        final int index = pipelineStartIndices.indexOf( pipelineStartIndex );
        checkArgument( index != -1, "invalid pipeline start index: %s", pipelineStartIndex );

        return index;
    }

    /**
     * Returns number of operators in the pipeline given with the in-region operator index
     *
     * @param pipelineStartIndex
     *         in-region index of the operator which is first operator of the pipeline
     *
     * @return number of operators in the pipeline given with the in-region operator index
     */
    public int getOperatorCountByPipelineStartIndex ( final int pipelineStartIndex )
    {
        return getOperatorDefsByPipelineStartIndex( pipelineStartIndex ).length;
    }

    /**
     * Returns number of operators in the pipeline given with the pipeline index
     *
     * @param pipelineIndex
     *         index of the pipeline in the region
     *
     * @return number of operators in the pipeline given with the pipeline index
     */
    public int getOperatorCountByPipelineIndex ( final int pipelineIndex )
    {
        return getOperatorDefsByPipelineIndex( pipelineIndex ).length;
    }

    /**
     * Returns definitions of the operators in the pipeline given with the in-region operator index
     *
     * @param pipelineStartIndex
     *         in-region index of the operator which is first operator of the pipeline
     *
     * @return definitions of the operators in the pipeline given with the in-region operator index
     */
    public OperatorDef[] getOperatorDefsByPipelineStartIndex ( final int pipelineStartIndex )
    {
        final int index = getPipelineIndex( pipelineStartIndex );
        return getOperatorDefsByPipelineIndex( index );
    }

    /**
     * Returns definitions of the operators in the pipeline given with the in-region operator index
     *
     * @param pipelineIndex
     *         index of the pipeline in the region
     *
     * @return definitions of the operators in the pipeline given with the in-region operator index
     */
    public OperatorDef[] getOperatorDefsByPipelineIndex ( final int pipelineIndex )
    {
        final List<OperatorDef> operators = regionDef.getOperators();
        final int startIndex = pipelineStartIndices.get( pipelineIndex );
        final int endIndex = ( pipelineIndex + 1 < pipelineStartIndices.size() )
                             ? pipelineStartIndices.get( pipelineIndex + 1 )
                             : operators.size();
        final List<OperatorDef> operatorDefs = operators.subList( startIndex, endIndex );
        final OperatorDef[] operatorDefsArr = new OperatorDef[ operatorDefs.size() ];
        operatorDefs.toArray( operatorDefsArr );
        return operatorDefsArr;
    }

    public RegionExecutionPlan withNewReplicaCount ( int newReplicaCount )
    {
        checkArgument( newReplicaCount > 0 );
        if ( newReplicaCount == replicaCount )
        {
            return this;
        }

        checkState( regionDef.getRegionType() == PARTITIONED_STATEFUL );
        return new RegionExecutionPlan( regionDef, pipelineStartIndices, newReplicaCount );
    }

    public RegionExecutionPlan withMergedPipelines ( final List<Integer> pipelineStartIndicesToMerge )
    {
        checkArgument( checkPipelineStartIndicesToMerge( this, pipelineStartIndicesToMerge ),
                       "invalid pipeline start indices to merge: %s current pipeline start indices: %s regionId=%s",
                       pipelineStartIndicesToMerge,
                       getPipelineStartIndices(),
                       getRegionId() );

        return new RegionExecutionPlan( regionDef, getPipelineStartIndicesAfterMerge( pipelineStartIndicesToMerge ), replicaCount );
    }

    public RegionExecutionPlan withSplitPipeline ( final List<Integer> pipelineStartIndicesToSplit )
    {
        checkArgument( checkPipelineStartIndicesToSplit( this, pipelineStartIndicesToSplit ),
                       "invalid pipeline start indices to split: %s current pipeline start indices: %s regionId=%s",
                       pipelineStartIndicesToSplit,
                       getPipelineStartIndices(),
                       getRegionId() );

        return new RegionExecutionPlan( regionDef, getPipelineStartIndicesAfterSplit( pipelineStartIndicesToSplit ), replicaCount );
    }

    private List<Integer> getPipelineStartIndicesAfterMerge ( final List<Integer> pipelineStartIndicesToMerge )
    {
        final List<Integer> newPipelineStartIndices = new ArrayList<>();
        final List<Integer> excluded = pipelineStartIndicesToMerge.subList( 1, pipelineStartIndicesToMerge.size() );
        for ( int startIndex : pipelineStartIndices )
        {
            if ( !excluded.contains( startIndex ) )
            {
                newPipelineStartIndices.add( startIndex );
            }
        }

        return newPipelineStartIndices;
    }

    private List<Integer> getPipelineStartIndicesAfterSplit ( final List<Integer> startIndicesToSplit )
    {
        final int firstPipelineIndex = getPipelineIndex( startIndicesToSplit.get( 0 ) );
        final List<Integer> newPipelineStartIndices = new ArrayList<>( getPipelineStartIndices() );
        for ( int i = 1; i < startIndicesToSplit.size(); i++ )
        {
            newPipelineStartIndices.add( firstPipelineIndex + i, startIndicesToSplit.get( i ) );
        }

        return newPipelineStartIndices;
    }

    public String toPlanSummaryString ()
    {
        return range( 0, getPipelineCount() ).mapToObj( this::getOperatorDefsByPipelineIndex )
                                             .map( operatorDefs -> Arrays.stream( operatorDefs )
                                                                         .map( OperatorDef::getId )
                                                                         .collect( joining( ",", "(", ")" ) ) )
                                             .collect( joining( ",", "[", "]*" + replicaCount ) );
    }

    @Override
    public String toString ()
    {
        return "RegionExecutionPlan{" + "regionDef=" + regionDef + ", replicaCount=" + replicaCount + ", pipelineStartIndices="
               + pipelineStartIndices + '}';
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final RegionExecutionPlan that = (RegionExecutionPlan) o;

        return replicaCount == that.replicaCount && regionDef.equals( that.regionDef )
               && pipelineStartIndices.equals( that.pipelineStartIndices );
    }

    @Override
    public int hashCode ()
    {
        int result = regionDef.hashCode();
        result = 31 * result + replicaCount;
        result = 31 * result + pipelineStartIndices.hashCode();
        return result;
    }

}
