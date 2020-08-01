package com.kuaishou.kcode.check.demo;

import static java.lang.Double.parseDouble;

import java.util.Objects;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
// 2020-06-18 12:33,serviceB,172.17.60.3,serviceC,172.17.60.4,380ms
public class Q1Result {
    private int DIFF;
    private String prefix;
    private String type;
    private int value;


    public Q1Result(String line) {
        int i = line.lastIndexOf(",");
        this.prefix = line.substring(0, i);
        String valueStr = line.substring(i + 1, line.length());
        if (valueStr.contains("ms")) {
            DIFF = 0;
            type = "P99";
            this.value = Integer.valueOf(valueStr.replace("ms", ""));
        } else if (valueStr.contains("%")) {
            type = "SR";
            DIFF = 5;
            this.value = (int) (parseDouble(valueStr.replace("%", "")) * 100);
        } else {
            System.out.println("alert value error:" + valueStr);
            throw new RuntimeException("alert result error");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q1Result that = (Q1Result) o;
        return DIFF == that.DIFF &&
                (that.value >= value - DIFF && that.value <= value + DIFF) &&
                Objects.equals(prefix, that.prefix) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DIFF, prefix, type);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlertValue{");
        sb.append("DIFF=").append(DIFF);
        sb.append(", prefix='").append(prefix).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}