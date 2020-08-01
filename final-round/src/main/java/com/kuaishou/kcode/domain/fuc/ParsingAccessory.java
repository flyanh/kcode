package com.kuaishou.kcode.domain.fuc;

/**
 * @author flyan
 * date 2020-07-15
 * @function 解析配件，包含了要解析的数据块提供给解析工厂进行解析
 */
public class ParsingAccessory {

    private byte[] handleBuffer = new byte[(256 << 10) + 128];
    private int handleLength;
    private int currTimeEigenvalue;
    private boolean stopSignal;     /* 停止信号，用于通知解析工厂停止 */

    public byte[] getHandleBuffer() {
        return handleBuffer;
    }

    public int getHandleLength() {
        return handleLength;
    }

    public int getCurrTimeEigenvalue() {
        return currTimeEigenvalue;
    }

    public boolean isStopSignal() {
        return stopSignal;
    }

    public ParsingAccessory setHandleBuffer(byte[] handleBuffer) {
        this.handleBuffer = handleBuffer;
        return this;
    }

    public ParsingAccessory setHandleLength(int handleLength) {
        this.handleLength = handleLength;
        return this;
    }

    public ParsingAccessory setCurrTimeEigenvalue(int currTimeEigenvalue) {
        this.currTimeEigenvalue = currTimeEigenvalue;
        return this;
    }

    public ParsingAccessory setStopSignal(boolean stopSignal) {
        this.stopSignal = stopSignal;
        return this;
    }
}
