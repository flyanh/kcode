package com.kuaishou.kcode.check.demo;

import static java.lang.Double.parseDouble;

import java.util.Objects;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
// 380ms/99.99%
public class Q2ResultValue {
    private int DIFF;
    private String type;
    private int value;

    public Q2ResultValue(String result) {
        if (result.contains("ms")) {
            DIFF = 0;
            type = "P99";
            this.value = Integer.valueOf(result.replace("ms", ""));
        } else if (result.contains("%")) {
            type = "SR";
            DIFF = 5;
            this.value = (int) (parseDouble(result.replace("%", "")) * 100);
        } else {
            System.out.println("q2 result value error:" + result);
            throw new RuntimeException("q2 result error");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q2ResultValue that = (Q2ResultValue) o;
        return DIFF == that.DIFF &&
                (that.value >= value - DIFF && that.value <= value + DIFF) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DIFF, type);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Q2ResultValue{");
        sb.append("DIFF=").append(DIFF).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}