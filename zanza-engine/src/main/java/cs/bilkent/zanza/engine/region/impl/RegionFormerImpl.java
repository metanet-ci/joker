package cs.bilkent.zanza.engine.region.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.region.RegionFormer;
import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.region.RegionDefinition;

public class RegionFormerImpl implements RegionFormer
{

    private static final Logger LOGGER = LoggerFactory.getLogger( RegionFormerImpl.class );


    @Override
    public List<RegionDefinition> createRegions ( final FlowDefinition flowDefinition )
    {
        return null;
    }

    Collection<List<OperatorDefinition>> getOperatorSequences ( final FlowDefinition flow )
    {

        final Collection<List<OperatorDefinition>> sequences = new ArrayList<>();

        final Set<OperatorDefinition> processedSequenceStartOperators = new HashSet<>();
        final Set<OperatorDefinition> sequenceStartOperators = new HashSet<>( flow.getOperatorsWithNoInputPorts() );
        List<OperatorDefinition> currentOperatorSequence = new ArrayList<>();

        OperatorDefinition operator;
        while ( ( operator = removeRandomOperator( sequenceStartOperators ) ) != null )
        {
            processedSequenceStartOperators.add( operator );
            LOGGER.debug( "Starting new sequence with oid={}", operator.id );

            while ( true )
            {
                currentOperatorSequence.add( operator );
                LOGGER.debug( "Adding oid={} to current sequence", operator.id );

                final Collection<OperatorDefinition> downstreamOperators = flow.getDownstreamOperators( operator.id );
                final OperatorDefinition downstreamOperator = getDownstreamOperatorIfSingle( flow, downstreamOperators );
                if ( downstreamOperator != null )
                {
                    operator = downstreamOperator;
                }
                else
                {
                    downstreamOperators.removeAll( processedSequenceStartOperators );
                    sequenceStartOperators.addAll( downstreamOperators );
                    sequences.add( currentOperatorSequence );

                    LOGGER.debug( "Sequence Completed! Sequence={}. New sequence starts={}",
                                  getOperatorIds( currentOperatorSequence ),
                                  getOperatorIds( downstreamOperators ) );
                    currentOperatorSequence = new ArrayList<>();
                    break;
                }
            }
        }

        return sequences;
    }

    private OperatorDefinition removeRandomOperator ( final Set<OperatorDefinition> operators )
    {
        OperatorDefinition operator = null;
        final Iterator<OperatorDefinition> it = operators.iterator();
        if ( it.hasNext() )
        {
            operator = it.next();
            it.remove();
        }

        return operator;
    }

    private OperatorDefinition getDownstreamOperatorIfSingle ( final FlowDefinition flow,
                                                               final Collection<OperatorDefinition> downstreamOperators )
    {
        if ( downstreamOperators.size() == 1 )
        {
            final OperatorDefinition next = downstreamOperators.iterator().next();
            final Collection<Port> upstreamPorts = flow.getUpstreamPorts( next.id );
            return upstreamPorts.size() == 1 ? next : null;
        }
        else
        {
            return null;
        }
    }

    private Stream<String> getOperatorIds ( final Collection<OperatorDefinition> operators )
    {
        return operators.stream().map( op -> op.id );
    }

}
