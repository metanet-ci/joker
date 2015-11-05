package cs.bilkent.zanza.flow;


import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


public class FlowDefinition
{
    public final Map<String, OperatorDefinition> operators;

    public final Set<ConnectionDefinition> connections;

    public FlowDefinition ( final Map<String, OperatorDefinition> operators, final Set<ConnectionDefinition> connections )
    {
        validateFlowDefinition( operators, connections );
        this.operators = Collections.unmodifiableMap( operators );
        this.connections = Collections.unmodifiableSet( connections );
    }

    // TODO improve flow validation
    private void validateFlowDefinition ( final Map<String, OperatorDefinition> operators, final Set<ConnectionDefinition> connections )
    {
        checkNotNull( operators );
        checkNotNull( connections );
        final long connectionCount = connections.stream().flatMap( con -> Stream.of( con.source.operatorId, con.target.operatorId ) )
                                                .distinct()
                                                .count();
        checkState( operators.size() == connectionCount, "Invalid flow definition!" );
    }

    public boolean containsOperator ( final String operatorId )
    {
        return operators.containsKey( operatorId );
    }

    public OperatorDefinition getOperator ( final String operatorId )
    {
        return operators.get( operatorId );
    }

    public boolean isConnected ( final String operator1, final String operator2 )
    {
        return connections.stream()
                          .anyMatch( con -> con.source.operatorId.equals( operator1 ) && con.target.operatorId.equals( operator2 ) );
    }
}
