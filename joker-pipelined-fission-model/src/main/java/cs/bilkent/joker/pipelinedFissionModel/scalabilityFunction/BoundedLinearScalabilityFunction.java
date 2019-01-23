package cs.bilkent.joker.pipelinedFissionModel.scalabilityFunction;

public class BoundedLinearScalabilityFunction extends ScalabilityFunction
{
    private int bound;
    private double epsilon;

    public BoundedLinearScalabilityFunction ( int bound )
    {
        this.bound = bound;
        epsilon = 0.1;
    }

    public BoundedLinearScalabilityFunction ( int bound, double epsilon )
    {
        this.bound = bound;
        this.epsilon = epsilon;
    }

    public double getScale ( int scale )
    {
        if ( scale <= bound )
        {
            return scale;
        }
        return bound / Math.pow( 1 + epsilon, scale - bound );
    }
}
