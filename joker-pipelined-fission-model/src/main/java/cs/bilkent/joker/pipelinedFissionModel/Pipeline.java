package cs.bilkent.joker.pipelinedFissionModel;

import java.util.ArrayList;

public class Pipeline implements Cloneable
{
    private ArrayList<Operator> operators;
    private ArrayList<String> keys;
    private StateKind kind;

    @Override
    public Object clone ()
    {
        Pipeline pipeline = new Pipeline();
        for ( Operator oper : operators )
        {
            pipeline.addOperator( oper );
        }
        return pipeline;
    }

    public Pipeline ()
    {
        operators = new ArrayList<>();
        kind = StateKind.Stateless;
    }

    public int getNumOperators ()
    {
        return operators.size();
    }

    public double getCost ()
    {
        double cost = 0;
        double selectivity = 1.0;
        for ( Operator operator : operators )
        {
            cost += operator.getCost() * selectivity;
            selectivity = selectivity * operator.getSelectivity();
        }
        return cost;
    }

    public double getMaximumThroughput ()
    {
        double cost = getCost();
        double maximumInputThroughput = 1 / cost;
        return maximumInputThroughput;
    }

    public double getSelectivity ()
    {
        double selectivity = 1.0;
        for ( Operator operator : operators )
        {
            selectivity = selectivity * operator.getSelectivity();
        }
        return selectivity;
    }

    void addOperator ( Operator oper )
    {
        operators.add( oper );
        StateKind oKind = oper.getKind();
        ArrayList<String> oKeys = oper.getKeys();
        StateKindMerger.StateKindAndKeys result = StateKindMerger.mergeLeft( kind, keys, oKind, oKeys );
        kind = result.kind;
        keys = result.keys;
    }

    public StateKind getKind ()
    {
        return kind;
    }

    public boolean hasKeys ()
    {
        return keys != null;
    }

    public ArrayList<String> getKeys ()
    {
        return keys;
    }

    public ArrayList<Operator> getOperators ()
    {
        return operators;
    }

    public Operator getFirstOperator ()
    {
        if ( operators.isEmpty() )
        {
            return null;
        }
        return operators.get( 0 );
    }

    public Operator getLastOperator ()
    {
        if ( operators.isEmpty() )
        {
            return null;
        }
        return operators.get( operators.size() - 1 );
    }

    public boolean verify ( ArrayList<String> errors )
    {
        boolean noErrors = true;
        if ( operators.size() == 0 )
        {
            noErrors = false;
            errors.add( "Pipeline is empty" );
        }
        Operator lastOper = null;
        for ( Operator oper : operators )
        {
            if ( lastOper != null )
            {
                if ( oper.getIndex() != lastOper.getIndex() + 1 )
                {
                    noErrors = false;
                    errors.add( "Opertor +" + oper + " has a gap after its predecessor" );
                }
            }
            lastOper = oper;
        }
        return noErrors;
    }

    public String toString ()
    {
        final StringBuilder buffer = new StringBuilder();
        buffer.append( "(" );
        boolean first = true;
        for ( Operator oper : operators )
        {
            if ( !first )
            {
                buffer.append( "," );
            }
            else
            {
                first = false;
            }
            buffer.append( oper.getIndex() );
        }
        buffer.append( ")" );
        return buffer.toString();
    }
}
