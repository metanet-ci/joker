package cs.bilkent.zanza.operator.flow;


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
        final long connectionCount = connections.stream()
                                                .flatMap( con -> Stream.of( con.source.operatorName, con.target.operatorName ) )
                                                .distinct()
                                                .count();
        checkState( operators.size() == connectionCount, "Invalid flow definition!" );
    }

    public boolean containsOperator ( final String operatorId )
    {
        return operators.containsKey( operatorId );
    }

    public boolean isConnected ( final String operator1, final String operator2 )
    {
        return connections.stream()
                          .anyMatch( con -> con.source.operatorName.equals( operator1 ) && con.target.operatorName.equals( operator2 ) );
    }
}
