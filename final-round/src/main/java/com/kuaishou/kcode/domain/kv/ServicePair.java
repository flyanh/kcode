package com.kuaishou.kcode.domain.kv;

import java.util.Objects;

/**
 * @author flyan
 * date 2020-07-14
 * @function 服务调用对
 */
public class ServicePair {

    private String caller;
    private String responder;
    private int hash;

    public ServicePair(String caller, String responder) {
        this.caller = caller;
        this.responder = responder;
        hash = Objects.hash(caller, responder);
    }

    @Override
    public String toString() {
        return "ServicePair{" +
                "caller='" + caller + '\'' +
                ", responder='" + responder + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServicePair that = (ServicePair) o;
        return Objects.equals(caller, that.caller) &&
                Objects.equals(responder, that.responder);
    }

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

}
