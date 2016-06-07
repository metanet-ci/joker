package cs.bilkent.zanza.engine.pipeline;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;

/**
 * @see OperatorInstance
 */
enum OperatorInstanceStatus
{

    /**
     * Indicates that the operator has not been initialized yet.
     */
    INITIAL,

    /**
     * Indicates that initialization of an operator has failed. It may be because {@link Operator#init(InitializationContext)} method
     * throws an exception or it returns an invalid scheduling strategy.
     */
    INITIALIZATION_FAILED,

    /**
     * Indicates that the operator is being invoked with its scheduling strategy successfully.
     */
    RUNNING,

    /**
     * Indicates that normal running schedule of an operator has ended and it is completing its invocations. It may be because
     * operator's all input ports are closed or an operator invocation returns {@link ScheduleNever} as new scheduling strategy.
     */
    COMPLETING,

    /**
     * Indicates that an operator has completed its invocations and there will not be any new invocation.
     */
    COMPLETED,

    /**
     * Indicates that an operator has been shut down.
     */
    SHUT_DOWN

}
