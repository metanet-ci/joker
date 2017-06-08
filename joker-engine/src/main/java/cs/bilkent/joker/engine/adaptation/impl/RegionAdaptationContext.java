package cs.bilkent.joker.engine.adaptation.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import cs.bilkent.joker.engine.adaptation.AdaptationAction;
import cs.bilkent.joker.engine.adaptation.BottleneckResolver;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.flow.RegionExecutionPlan;
import cs.bilkent.joker.engine.metric.PipelineMetrics;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkArgument;
import static cs.bilkent.joker.impl.com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.emptyList;
import static java.util.Collections.reverse;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

public class RegionAdaptationContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionExecutionPlan.class );


    private final RegionDef regionDef;

    private final Multimap<PipelineId, AdaptationAction> blacklists = HashMultimap.create();

    private final Set<PipelineId> nonResolvablePipelineIds = new TreeSet<>();

    private final Map<PipelineId, PipelineMetrics> pipelineMetricsByPipelineId = new TreeMap<>();

    private RegionExecutionPlan currentExecutionPlan;

    private List<Pair<AdaptationAction, List<PipelineId>>> adaptationActions = emptyList();

    RegionAdaptationContext ( final RegionExecutionPlan currentExecutionPlan )
    {
        this.regionDef = currentExecutionPlan.getRegionDef();
        this.currentExecutionPlan = currentExecutionPlan;
    }

    public int getRegionId ()
    {
        return regionDef.getRegionId();
    }

    PipelineMetrics getPipelineMetrics ( final PipelineId pipelineId )
    {
        return pipelineMetricsByPipelineId.get( pipelineId );
    }

    Collection<AdaptationAction> getBlacklist ( final PipelineId pipelineId )
    {
        return unmodifiableCollection( blacklists.get( pipelineId ) );
    }

    Set<PipelineId> getNonResolvablePipelineIds ()
    {
        return unmodifiableSet( nonResolvablePipelineIds );
    }

    RegionExecutionPlan getCurrentExecutionPlan ()
    {
        return currentExecutionPlan;
    }

    RegionExecutionPlan getBaseExecutionPlan ()
    {
        return adaptationActions.isEmpty() ? null : adaptationActions.get( 0 )._1.getCurrentRegionExecutionPlan();
    }

    List<AdaptationAction> getAdaptationActions ()
    {
        return adaptationActions.stream().map( Pair::firstElement ).collect( toList() );
    }

    List<PipelineId> getAdaptingPipelineIds ()
    {
        return adaptationActions.stream().flatMap( p -> p._2.stream() ).collect( toList() );
    }

    void updateRegionMetrics ( final List<PipelineMetrics> regionMetrics,
                               final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate )
    {
        checkArgument( regionMetrics != null );
        checkArgument( loadChangePredicate != null );
        checkState( adaptationActions.isEmpty(),
                    "Region metrics cannot be updated while region %s has ongoing adaptations: %s",
                    getRegionId(),
                    adaptationActions );

        for ( PipelineMetrics pipelineMetrics : regionMetrics )
        {
            updatePipelineMetrics( pipelineMetrics, loadChangePredicate );
        }
    }

    private void updatePipelineMetrics ( final PipelineMetrics newPipelineMetrics,
                                         final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate )
    {
        checkArgument( newPipelineMetrics != null );
        final PipelineId pipelineId = newPipelineMetrics.getPipelineId();
        checkPipelineId( pipelineId );
        checkArgument( loadChangePredicate != null );

        final PipelineMetrics prev = pipelineMetricsByPipelineId.put( pipelineId, newPipelineMetrics );
        if ( loadChangePredicate.test( prev, newPipelineMetrics ) )
        {
            LOGGER.info( "Load change detected! Region: {} PipelineId: {} Prev: {}, New: {}",
                         getRegionId(),
                         pipelineId,
                         prev,
                         newPipelineMetrics );
            blacklists.removeAll( pipelineId );
            nonResolvablePipelineIds.remove( pipelineId );
        }
    }

    List<AdaptationAction> resolveIfBottleneck ( final Predicate<PipelineMetrics> bottleneckPredicate,
                                                 final List<BottleneckResolver> bottleneckResolvers )
    {
        checkArgument( bottleneckPredicate != null );
        checkArgument( bottleneckResolvers != null );
        checkState( adaptationActions.isEmpty(),
                    "Region %s cannot try to resolve bottleneck before evaluation of adaptation actions: %s",
                    getRegionId(),
                    adaptationActions );

        final List<PipelineId> bottleneckPipelineIds = getBottleneckPipelineIds( bottleneckPredicate );
        if ( bottleneckPipelineIds.isEmpty() )
        {
            return emptyList();
        }

        final List<Pair<AdaptationAction, List<PipelineId>>> resolutions = resolveBottlenecks( bottleneckPipelineIds, bottleneckResolvers );
        if ( resolutions.isEmpty() )
        {
            nonResolvablePipelineIds.addAll( bottleneckPipelineIds );
            adaptationActions = emptyList();

            return emptyList();
        }

        adaptationActions = resolutions;
        currentExecutionPlan = resolutions.get( resolutions.size() - 1 )._1.getNewRegionExecutionPlan();

        return resolutions.stream().map( Pair::firstElement ).collect( toList() );
    }

    private boolean isRegionNonResolvable ()
    {
        if ( regionDef.isSource() )
        {
            return true;
        }

        if ( regionDef.getOperatorCount() == 1 && regionDef.getRegionType() != PARTITIONED_STATEFUL )
        {
            return true;
        }

        if ( nonResolvablePipelineIds.size() == pipelineMetricsByPipelineId.size() )
        {
            LOGGER.warn( "Region: {} has all {} pipelines non-resolvable!", getRegionId(), pipelineMetricsByPipelineId.size() );

            return true;
        }

        // TODO not sure about this part
        if ( !nonResolvablePipelineIds.isEmpty() )
        {
            LOGGER.warn( "Region {} has non resolvable bottleneck pipeline ids: {}", getRegionId(), nonResolvablePipelineIds );

            return true;
        }

        return false;
    }

    private List<PipelineId> getBottleneckPipelineIds ( final Predicate<PipelineMetrics> bottleneckPredicate )
    {
        if ( isRegionNonResolvable() )
        {
            return emptyList();
        }

        if ( adaptationActions.size() > 0 )
        {
            return getAdaptingPipelineIds();
        }

        final List<PipelineMetrics> bottleneckPipelineMetrics = pipelineMetricsByPipelineId.values()
                                                                                           .stream()
                                                                                           .filter( bottleneckPredicate )
                                                                                           .collect( toList() );

        for ( PipelineMetrics pipelineMetrics : bottleneckPipelineMetrics )
        {
            LOGGER.info( "Region {} has a bottleneck pipeline {} with metrics: {}",
                         getRegionId(),
                         pipelineMetrics.getPipelineId(),
                         pipelineMetrics );
        }

        return bottleneckPipelineMetrics.stream().map( PipelineMetrics::getPipelineId ).collect( toList() );
    }

    private List<Pair<AdaptationAction, List<PipelineId>>> resolveBottlenecks ( final List<PipelineId> bottleneckPipelineIds,
                                                                                final List<BottleneckResolver> bottleneckResolvers )
    {
        if ( bottleneckResolvers != null && bottleneckResolvers.size() > 0 )
        {
            final List<PipelineMetrics> bottleneckPipelinesMetrics = getBottleneckPipelineMetrics( bottleneckPipelineIds );

            for ( BottleneckResolver bottleneckResolver : bottleneckResolvers )
            {
                final List<Pair<AdaptationAction, List<PipelineId>>> candidates = resolveBottlenecks( bottleneckPipelineIds,
                                                                                                      bottleneckPipelinesMetrics,
                                                                                                      bottleneckResolver );

                if ( candidates.isEmpty() )
                {
                    continue;
                }

                LOGGER.info( "Region {} will try adaptations: {}", getRegionId(), candidates );

                return candidates;
            }
        }
        else
        {
            LOGGER.warn( "Cannot resolve bottleneck pipelines {} of Region {} because there is no bottleneck resolver!",
                         bottleneckPipelineIds,
                         getRegionId() );
        }

        return emptyList();
    }

    private List<PipelineMetrics> getBottleneckPipelineMetrics ( final List<PipelineId> bottleneckPipelineIds )
    {
        final List<PipelineMetrics> bottleneckPipelinesMetrics = new ArrayList<>();
        for ( PipelineId bottleneckPipelineId : bottleneckPipelineIds )
        {
            final PipelineMetrics pipelineMetrics = pipelineMetricsByPipelineId.get( bottleneckPipelineId );
            checkState( pipelineMetrics != null,
                        "No metrics in Region %s for bottleneck pipeline %s",
                        getRegionId(),
                        bottleneckPipelineId );
            bottleneckPipelinesMetrics.add( pipelineMetrics );
        }
        return bottleneckPipelinesMetrics;
    }

    private List<Pair<AdaptationAction, List<PipelineId>>> resolveBottlenecks ( final List<PipelineId> bottleneckPipelineIds,
                                                                                final List<PipelineMetrics> bottleneckPipelinesMetrics,
                                                                                final BottleneckResolver bottleneckResolver )
    {
        final List<Pair<AdaptationAction, List<PipelineId>>> candidates = bottleneckResolver.resolve( currentExecutionPlan,
                                                                                                      bottleneckPipelinesMetrics );

        if ( candidates.isEmpty() )
        {
            LOGGER.warn( "No candidate adaptation for Region: {} bottleneck pipelines: {}", getRegionId(), bottleneckPipelineIds );

            return emptyList();
        }

        for ( Pair<AdaptationAction, List<PipelineId>> p : candidates )
        {
            final AdaptationAction adaptationAction = p._1;
            final List<PipelineId> pipelineIds = p._2;

            for ( PipelineId bottleneckPipelineId : pipelineIds )
            {
                if ( blacklists.containsEntry( bottleneckPipelineId, adaptationAction ) )
                {
                    LOGGER.warn( "Candidate adaptation: {} is already blacklisted for Region: {} bottleneck pipelines: {}",
                                 adaptationAction,
                                 getRegionId(),
                                 pipelineIds );

                    return emptyList();
                }
            }
        }

        return candidates;
    }

    boolean isAdaptationSuccessful ( final List<PipelineMetrics> regionMetrics,
                                     final BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate )
    {
        checkArgument( regionMetrics != null );
        checkArgument( adaptationEvaluationPredicate != null );
        regionMetrics.stream().map( PipelineMetrics::getPipelineId ).forEach( this::checkPipelineId );
        checkState( !adaptationActions.isEmpty(),
                    "Cannot evaluate metrics: %s for Region %s has no adaptation action",
                    regionMetrics,
                    getRegionId() );

        final PipelineMetrics regionNewInboundMetrics = regionMetrics.get( 0 );
        final PipelineMetrics regionBottleneckInboundMetrics = pipelineMetricsByPipelineId.get( regionNewInboundMetrics.getPipelineId() );
        checkState( regionBottleneckInboundMetrics != null );

        final boolean success = adaptationEvaluationPredicate.test( regionBottleneckInboundMetrics, regionNewInboundMetrics );
        if ( success )
        {
            LOGGER.info( "Adaptations are beneficial for Region {} with new metrics: {} bottleneck metrics: {} and adaptation actions: {}",
                         getRegionId(),
                         regionNewInboundMetrics,
                         regionBottleneckInboundMetrics,
                         adaptationActions );
        }
        else
        {
            LOGGER.info(
                    "Adaptations are not beneficial for Region {} with new metrics: {} bottleneck metrics: {} and adaptation actions: {}",
                    getRegionId(),
                    regionNewInboundMetrics,
                    regionBottleneckInboundMetrics,
                    adaptationActions );
        }

        return success;
    }

    void finalizeAdaptation ( final List<PipelineMetrics> regionMetrics )
    {
        checkArgument( regionMetrics != null );
        regionMetrics.stream().map( PipelineMetrics::getPipelineId ).forEach( this::checkPipelineId );
        checkState( !adaptationActions.isEmpty(),
                    "Cannot finalize adaptation with metrics: %s for Region %s has no adaptation action",
                    regionMetrics,
                    getRegionId() );

        final List<PipelineId> adaptingPipelineIds = getAdaptingPipelineIds();
        for ( PipelineMetrics pipelineMetrics : regionMetrics )
        {
            if ( adaptingPipelineIds.contains( pipelineMetrics.getPipelineId() ) )
            {
                pipelineMetricsByPipelineId.put( pipelineMetrics.getPipelineId(), pipelineMetrics );
            }
        }

        blacklists.removeAll( adaptingPipelineIds );
        adaptationActions = emptyList();

        final Set<PipelineId> pipelineIdsToDelete = new HashSet<>( pipelineMetricsByPipelineId.keySet() );
        for ( PipelineMetrics pipelineMetrics : regionMetrics )
        {
            pipelineIdsToDelete.remove( pipelineMetrics.getPipelineId() );
        }

        for ( PipelineId pipelineId : pipelineIdsToDelete )
        {
            pipelineMetricsByPipelineId.remove( pipelineId );
            blacklists.removeAll( pipelineId );
            nonResolvablePipelineIds.remove( pipelineId );
        }
    }

    List<AdaptationAction> rollbackAdaptation ( final boolean blacklist )
    {
        checkState( !adaptationActions.isEmpty(), "Cannot rollback adaptation for Region %s has no adaptation action", getRegionId() );

        if ( blacklist )
        {
            addToBlacklist();
        }

        currentExecutionPlan = getBaseExecutionPlan();
        checkState( currentExecutionPlan != null );

        final List<AdaptationAction> actions = adaptationActions.stream().map( Pair::firstElement ).collect( toList() );
        checkState( !actions.isEmpty() );
        adaptationActions = emptyList();

        reverse( actions );
        final List<AdaptationAction> rollbackActions = actions.stream().map( AdaptationAction::rollback ).collect( toList() );
        rollbackActions.forEach( rollbackAction -> checkState( rollbackAction != null ) );

        return rollbackActions;
    }

    private void addToBlacklist ()
    {
        for ( Pair<AdaptationAction, List<PipelineId>> p : adaptationActions )
        {
            final AdaptationAction adaptationAction = p._1;
            for ( PipelineId adaptingPipelineId : p._2 )
            {
                addToBlacklist( adaptingPipelineId, adaptationAction );
            }
        }
    }

    private void addToBlacklist ( final PipelineId pipelineId, final AdaptationAction adaptationAction )
    {
        checkArgument( !blacklists.containsEntry( pipelineId, adaptationAction ) );
        blacklists.put( pipelineId, adaptationAction );
    }

    private void checkPipelineId ( final PipelineId pipelineId )
    {
        checkArgument( pipelineId != null );
        checkArgument( currentExecutionPlan.getPipelineIds().contains( pipelineId ) );
    }

}
