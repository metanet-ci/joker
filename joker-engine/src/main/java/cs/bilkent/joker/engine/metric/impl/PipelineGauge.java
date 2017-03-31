package cs.bilkent.joker.engine.metric.impl;

import java.util.function.Supplier;

import com.codahale.metrics.Gauge;

import cs.bilkent.joker.engine.flow.PipelineId;

class PipelineGauge<T> implements Gauge<T>
{

    private final PipelineId id;

    private final Supplier<T> gauge;

    PipelineGauge ( final PipelineId id, final Supplier<T> gauge )
    {
        this.id = id;
        this.gauge = gauge;
    }

    public PipelineId getId ()
    {
        return id;
    }

    @Override
    public T getValue ()
    {
        return gauge.get();
    }

}
