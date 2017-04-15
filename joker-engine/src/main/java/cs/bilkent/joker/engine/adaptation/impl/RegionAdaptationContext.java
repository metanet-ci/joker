package cs.bilkent.joker.engine.adaptation.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

public class RegionAdaptationContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionExecutionPlan.class );


    private final RegionDef regionDef;

    private final Multimap<PipelineId, RegionExecutionPlan> blacklists = HashMultimap.create();

    private final Set<PipelineId> nonResolvablePipelineIds = new HashSet<>();

    private final Map<PipelineId, PipelineMetrics> pipelineMetricsByPipelineId = new TreeMap<>();

    private RegionExecutionPlan currentExecutionPlan, baseExecutionPlan;

    private PipelineId adaptingPipelineId;

    private AdaptationAction adaptationAction;

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

    Collection<RegionExecutionPlan> getBlacklist ( final PipelineId pipelineId )
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
        return baseExecutionPlan;
    }

    PipelineId getAdaptingPipelineId ()
    {
        return adaptingPipelineId;
    }

    AdaptationAction getAdaptationAction ()
    {
        return adaptationAction;
    }

    void updateRegionMetrics ( final RegionExecutionPlan regionExecutionPlan,
                               final List<PipelineMetrics> regionMetrics,
                               final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate )
    {
        checkArgument( regionExecutionPlan != null );
        checkArgument( regionMetrics != null );
        checkArgument( loadChangePredicate != null );
        checkState( adaptingPipelineId == null,
                    "Region metrics cannot be updated while region %s is adapting %s",
                    getRegionId(),
                    adaptingPipelineId );

        if ( !regionExecutionPlan.equals( currentExecutionPlan ) )
        {
            LOGGER.info( "Execution plan of Region: {} is changed. Prev: {} New: {}",
                         getRegionId(),
                         currentExecutionPlan,
                         regionExecutionPlan );
            currentExecutionPlan = regionExecutionPlan;
        }

        final Set<PipelineId> pipelineIdsToDelete = new HashSet<>( pipelineMetricsByPipelineId.keySet() );

        for ( PipelineMetrics pipelineMetrics : regionMetrics )
        {
            updatePipelineMetrics( pipelineMetrics, loadChangePredicate );
            pipelineIdsToDelete.remove( pipelineMetrics.getPipelineId() );
        }

        for ( PipelineId pipelineId : pipelineIdsToDelete )
        {
            pipelineMetricsByPipelineId.remove( pipelineId );
            blacklists.removeAll( pipelineId );
            nonResolvablePipelineIds.remove( pipelineId );
        }
    }

    AdaptationAction resolveIfBottleneck ( final Predicate<PipelineMetrics> bottleneckPredicate,
                                           final List<BottleneckResolver> bottleneckResolvers )
    {
        checkArgument( bottleneckPredicate != null );
        checkState( adaptationAction == null,
                    "Region %s cannot try to resolve bottleneck before evaluation of adaptation action: %s",
                    getRegionId(),
                    adaptationAction );

        if ( regionDef.isSource() )
        {
            return null;
        }

        final PipelineId bottleneckPipelineId = getBottleneckPipeline( bottleneckPredicate );
        if ( bottleneckPipelineId == null )
        {
            return null;
        }

        LOGGER.info( "Region {} has a bottleneck pipeline {}", getRegionId(), bottleneckPipelineId );

        final Pair<AdaptationAction, RegionExecutionPlan> bottleneckResolution = resolveBottleneck( bottleneckPipelineId,
                                                                                                    bottleneckResolvers );
        if ( bottleneckResolution == null )
        {
            nonResolvablePipelineIds.add( bottleneckPipelineId );
            adaptingPipelineId = null;
            LOGGER.info( "Region {} has no possible adaptations for bottleneck pipeline: {}", getRegionId(), bottleneckPipelineId );

            return null;
        }

        baseExecutionPlan = currentExecutionPlan;
        adaptingPipelineId = bottleneckPipelineId;
        currentExecutionPlan = bottleneckResolution._2;
        adaptationAction = bottleneckResolution._1;

        LOGGER.info( "Region {} will try to resolve bottleneck pipeline {} with {}",
                     getRegionId(),
                     bottleneckPipelineId,
                     adaptationAction );

        return adaptationAction;
    }

    AdaptationAction evaluateAdaptation ( final PipelineMetrics newPipelineMetrics,
                                          final BiPredicate<PipelineMetrics, PipelineMetrics> adaptationEvaluationPredicate )
    {
        checkArgument( newPipelineMetrics != null );
        checkArgument( adaptationEvaluationPredicate != null );
        checkPipelineId( newPipelineMetrics.getPipelineId() );
        checkState( newPipelineMetrics.getPipelineId().equals( adaptingPipelineId ),
                    "Cannot evaluate metrics: %s for Region %s because it is not adapting pipeline: %s",
                    newPipelineMetrics,
                    getRegionId(),
                    adaptingPipelineId );
        checkState( adaptationAction != null,
                    "Cannot evaluate metrics: %s for Region %s because adapting pipeline %s has no adaptation action",
                    newPipelineMetrics,
                    getRegionId(),
                    adaptingPipelineId );

        final PipelineMetrics bottleneckPipelineMetrics = pipelineMetricsByPipelineId.get( adaptingPipelineId );
        checkState( bottleneckPipelineMetrics != null,
                    "bottleneck pipeline metrics not found for adapting pipeline: %s",
                    adaptingPipelineId );

        if ( adaptationEvaluationPredicate.test( bottleneckPipelineMetrics, newPipelineMetrics ) )
        {
            finalizeAdaptation( newPipelineMetrics );
            LOGGER.info( "Adaptation is beneficial for bottleneck pipeline {} of Region {} with new metrics: {} bottleneck metrics: {} ",
                         newPipelineMetrics.getPipelineId(),
                         getRegionId(),
                         newPipelineMetrics,
                         bottleneckPipelineMetrics );

            return null;
        }
        else
        {
            LOGGER.info( "Adaptation is not beneficial for bottleneck pipeline {} of Region {} with new metrics: {} bottleneck metrics: {}",
                         newPipelineMetrics.getPipelineId(),
                         getRegionId(),
                         newPipelineMetrics,
                         bottleneckPipelineMetrics );

            final AdaptationAction rollback = rollbackAdaptation();
            checkState( rollback != null );

            return rollback;
        }
    }

    private void updatePipelineMetrics ( final PipelineMetrics newPipelineMetrics,
                                         final BiPredicate<PipelineMetrics, PipelineMetrics> loadChangePredicate )
    {
        checkArgument( adaptingPipelineId == null );
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

    private PipelineId getBottleneckPipeline ( final Predicate<PipelineMetrics> bottleneckPredicate )
    {
        if ( adaptingPipelineId != null )
        {
            return adaptingPipelineId;
        }

        for ( PipelineMetrics pipelineMetrics : pipelineMetricsByPipelineId.values() )
        {
            if ( bottleneckPredicate.test( pipelineMetrics ) )
            {
                final PipelineId pipelineId = pipelineMetrics.getPipelineId();
                if ( nonResolvablePipelineIds.contains( pipelineId ) )
                {
                    continue;
                }

                return pipelineId;
            }
        }

        return null;
    }

    private Pair<AdaptationAction, RegionExecutionPlan> resolveBottleneck ( final PipelineId bottleneckPipelineId,
                                                                            final List<BottleneckResolver> bottleneckResolvers )
    {
        final PipelineMetrics pipelineMetrics = pipelineMetricsByPipelineId.get( bottleneckPipelineId );
        checkState( pipelineMetrics != null, "No metrics in Region %s for bottleneck pipeline %s", getRegionId(), bottleneckPipelineId );

        if ( bottleneckResolvers != null && bottleneckResolvers.size() > 0 )
        {
            for ( BottleneckResolver bottleneckResolver : bottleneckResolvers )
            {
                final Pair<AdaptationAction, RegionExecutionPlan> bottleneckResolution = resolveBottleneck( bottleneckPipelineId,
                                                                                                            pipelineMetrics,
                                                                                                            bottleneckResolver );
                if ( bottleneckResolution != null )
                {
                    return bottleneckResolution;
                }
            }
        }
        else
        {
            LOGGER.warn( "Cannot resolve bottleneck pipeline {} of Region {} because there is no bottleneck resolver!",
                         bottleneckPipelineId,
                         getRegionId() );
        }

        return null;
    }

    private Pair<AdaptationAction, RegionExecutionPlan> resolveBottleneck ( final PipelineId bottleneckPipelineId,
                                                                            final PipelineMetrics pipelineMetrics,
                                                                            final BottleneckResolver bottleneckResolver )
    {
        final AdaptationAction action = bottleneckResolver.resolve( currentExecutionPlan, pipelineMetrics );
        if ( action == null )
        {
            return null;
        }

        final RegionExecutionPlan newExecutionPlan = action.getNewRegionExecutionPlan();
        if ( blacklists.containsEntry( bottleneckPipelineId, newExecutionPlan ) )
        {
            LOGGER.info( "Region {} cannot resolve bottleneck pipeline {} with {} because it is blacklisted.",
                         getRegionId(),
                         bottleneckPipelineId,
                         action );

            return null;
        }

        return Pair.of( action, newExecutionPlan );
    }

    private void finalizeAdaptation ( final PipelineMetrics newPipelineMetrics )
    {
        pipelineMetricsByPipelineId.put( adaptingPipelineId, newPipelineMetrics );
        baseExecutionPlan = null;
        adaptingPipelineId = null;
        adaptationAction = null;
    }

    private AdaptationAction rollbackAdaptation ()
    {
        addToBlacklist( adaptingPipelineId, currentExecutionPlan );
        currentExecutionPlan = baseExecutionPlan;
        baseExecutionPlan = null;
        final AdaptationAction rollbackAction = adaptationAction.rollback();
        adaptationAction = null;
        return rollbackAction;
    }

    private void addToBlacklist ( final PipelineId pipelineId, final RegionExecutionPlan regionExecutionPlan )
    {
        checkArgument( !blacklists.containsEntry( pipelineId, regionExecutionPlan ) );
        blacklists.put( pipelineId, regionExecutionPlan );
    }

    private void checkPipelineId ( final PipelineId pipelineId )
    {
        checkArgument( pipelineId != null );
        checkArgument( currentExecutionPlan.getPipelineIds().contains( pipelineId ) );
    }

}
