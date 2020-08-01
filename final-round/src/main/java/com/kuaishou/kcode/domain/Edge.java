package com.kuaishou.kcode.domain;

/**
 * @author flyan
 * date 2020-07-07
 * @function 表示有向无权图的一条边
 */
public class Edge {

    private int u;
    private int v;      /* (u, v) 是一个有向顶点对 */
    private int next;   /* 下一条边在哪？即在数组中的位置 */

    public Edge(int u, int v, int next) {
        this.u = u;
        this.v = v;
        this.next = next;
    }

    public int getV() {
        return v;
    }

    public int getNext() {
        return next;
    }

}
