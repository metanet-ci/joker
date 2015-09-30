package cs.bilkent.zanza.operator;

public class Port {

    public static final int DEFAULT_PORT_INDEX = 0;

    public final String sourceOperatorName;

    public final int sourcePortIndex;

    public final String targetOperatorName;

    public final int targetPortIndex;

    public Port(String sourceOperatorName, String targetOperatorName) {
        this(sourceOperatorName, DEFAULT_PORT_INDEX, targetOperatorName, DEFAULT_PORT_INDEX);
    }

    public Port(String sourceOperatorName, int sourcePortIndex, String targetOperatorName, int targetPortIndex) {
        this.sourceOperatorName = sourceOperatorName;
        this.sourcePortIndex = sourcePortIndex;
        this.targetOperatorName = targetOperatorName;
        this.targetPortIndex = targetPortIndex;
    }

    public boolean isSourceDefaultPort() {
        return sourcePortIndex == DEFAULT_PORT_INDEX;
    }

    public boolean isTargetDefaultPort() {
        return targetPortIndex == DEFAULT_PORT_INDEX;
    }

}
