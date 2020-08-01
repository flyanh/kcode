package com.kuaishou.kcode.check.demo;

import java.util.Objects;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
public class Q2Input {
    private String caller;
    private String responder;
    private String time;
    private String type;

    public Q2Input(String line) {
        String[] split = line.substring(2).split(",");
        this.caller = split[0];
        this.responder = split[1];
        this.time = split[2];
        this.type = split[3];
    }

    public String getCaller() {
        return caller;
    }

    public void setCaller(String caller) {
        this.caller = caller;
    }

    public String getResponder() {
        return responder;
    }

    public void setResponder(String responder) {
        this.responder = responder;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Q2Input{");
        sb.append("caller='").append(caller).append('\'');
        sb.append(", responder='").append(responder).append('\'');
        sb.append(", time='").append(time).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q2Input q2Input = (Q2Input) o;
        return Objects.equals(caller, q2Input.caller) &&
                Objects.equals(responder, q2Input.responder) &&
                Objects.equals(time, q2Input.time) &&
                Objects.equals(type, q2Input.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(caller, responder, time, type);
    }
}