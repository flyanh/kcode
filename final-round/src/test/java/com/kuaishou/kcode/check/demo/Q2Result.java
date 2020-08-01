package com.kuaishou.kcode.check.demo;

import java.util.LinkedList;
import java.util.Objects;

/**
 * @author KCODE
 * Created on 2020-07-04
 */
public class Q2Result {
    private String longestPath;

    private LinkedList<Q2ResultValue> values;

    public Q2Result(String line) {
        String[] split = line.split("\\|");
        this.longestPath = split[0];
        values = new LinkedList<>();
        String[] valueStrs = split[1].split(",");
        for (String value : valueStrs) {
            Q2ResultValue q2ResultValue = new Q2ResultValue(value);
            values.addLast(q2ResultValue);
        }
    }

    public String getLongestPath() {
        return longestPath;
    }

    public void setLongestPath(String longestPath) {
        this.longestPath = longestPath;
    }

    public LinkedList<Q2ResultValue> getValues() {
        return values;
    }

    public void setValues(LinkedList<Q2ResultValue> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q2Result q2Result = (Q2Result) o;
        return Objects.equals(longestPath, q2Result.longestPath) &&
                Objects.equals(values, q2Result.values); // LinkList equals
    }

    @Override
    public int hashCode() {
        return Objects.hash(longestPath, values);
    }
}