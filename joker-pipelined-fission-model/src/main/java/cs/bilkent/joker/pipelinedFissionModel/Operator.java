package cs.bilkent.joker.pipelinedFissionModel;

import java.util.ArrayList;

import cs.bilkent.joker.pipelinedFissionModel.scalabilityFunction.ScalabilityFunction;

public class Operator
{
    private int index;
    private double cost;
    private double selectivity;
    private StateKind kind;
    private ScalabilityFunction scalabilityFunction;
    private ArrayList<String> keys = null;

    public Operator ( int index, double cost, double selectivity, StateKind kind, ScalabilityFunction scalabilityFunction )
    {
        this.index = index;
        this.cost = cost;
        this.selectivity = selectivity;
        this.kind = kind;
        this.scalabilityFunction = scalabilityFunction;
        if ( kind == StateKind.PartitionedStateful )
        {
            keys = new ArrayList<>();
        }
    }

    public boolean hasKeys ()
    {
        return keys != null;
    }

    void setKeys ( ArrayList<String> keys )
    {
        this.keys.clear();
        this.keys.addAll( keys );
    }

    public ArrayList<String> getKeys ()
    {
        return keys;
    }

    public int getIndex ()
    {
        return index;
    }

    public ScalabilityFunction getScalability ()
    {
        return scalabilityFunction;
    }

    public void setScalabilityFunction ( ScalabilityFunction scalabilityFunction )
    {
        this.scalabilityFunction = scalabilityFunction;
    }

    public double getSelectivity ()
    {
        return selectivity;
    }

    public double getCost ()
    {
        return cost;
    }

    public StateKind getKind ()
    {
        return kind;
    }

    public void setIndex ( int index )
    {
        this.index = index;
    }

}
