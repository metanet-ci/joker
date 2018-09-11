package cs.bilkent.joker.engine.metric.impl;

public class AverageCalculator
{

    private int count;
    private long sum;

    public void record ( final long value )
    {
        count++;
        sum += value;
    }

    public double getAverage ()
    {
        return count > 0 ? ( (double) sum ) / count : 0;
    }

    public void reset ()
    {
        count = 0;
        sum = 0;
    }

}
