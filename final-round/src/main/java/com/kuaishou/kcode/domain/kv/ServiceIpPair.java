package com.kuaishou.kcode.domain.kv;

/**
 * @author flyan
 * date 2020-07-07
 * @function 主被调 IP 对
 */
public class ServiceIpPair {

    private String caller;          /* 调用者 */
    private String responder;       /* 响应者 */
    private int callerIp;           /* 调用者 IP 编码 */
    private int responderIP;        /* 响应者 IP 编码 */
    private int hash;               /* 哈希，唯一标识一个主被调 IP 对 */

    public ServiceIpPair(String caller, String responder, final int callerIP, final int responderIP) {
        this.caller = caller;
        this.responder = responder;
        this.callerIp = callerIP;
        this.responderIP = responderIP;
        this.hash = ((callerIP & 0xFFFF) << 16) | (responderIP & 0xFFFF);  /* 饿汉模式，立马计算哈希 */
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceIpPair that = (ServiceIpPair) o;
        return callerIp == that.callerIp &&
                responderIP == that.responderIP;
    }

    public int getIpPair() {
        return hash;
    }

    /* 简简单单的哈希 */
    @Override
    public int hashCode() {
        return hash;
    }

    public String getCaller() {
        return caller;
    }

    public String getResponder() {
        return responder;
    }

    public int getCallerIp() {
        return callerIp;
    }

    public int getResponderIP() {
        return responderIP;
    }
}
