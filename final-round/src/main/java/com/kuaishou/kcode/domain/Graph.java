package com.kuaishou.kcode.domain;

/**
 * @author flyan
 * date 2020-07-08
 * @function 有向无权图，使用前向星建立，效率还可，听说邻接表之类的建图方式更好。
 */
public class Graph {

    private Edge[] edges;   /* 边集 */
    private int[] head;     /* 指向 i 结点的第一条边 */
    private int[] outDegree;/* 每个节点的出度 */
    private int edgeCount;  /* 边数 */

    public Graph() {
        this(1000);
    }

    public Graph(int maxEdge) {
        edges = new Edge[maxEdge];
        outDegree = new int[maxEdge];
        head = new int[maxEdge];
        for(int i = 0; i < maxEdge; ++i) {
            head[i] = -1;
        }
    }

    /**
     * 添加一条单向边
     *
     * @param u 节点
     * @param v 终点
     */
    public void addEdge(int u, int v) {
        edges[edgeCount] = new Edge(u, v, head[u]);
        head[u] = edgeCount++;
        outDegree[u]++; /* 记录该节点的出度 */
    }

    /**
     * 获取一条边
     *
     * @param edgeIndex 该边在边集数组中的索引
     * @return 边
     */
    public Edge getEdge(int edgeIndex) {
        return edges[edgeIndex];
    }

    /**
     * 获取出度表，里面有所有节点的出度信息
     *
     * @return 出度表
     */
    public int[] getOutDegreeTable() {
        return outDegree;
    }

    /**
     * 获取某个节点的出度
     *
     * @param node 节点
     * @return 出度
     */
    public int getOutDegree(int node) {
        return outDegree[node];
    }

    /**
     * 获取某个节点在边集数组中的起始位置
     *
     * @param node 节点
     * @return 起始位置
     */
    public int getHead(int node) {
        return head[node];
    }

}
