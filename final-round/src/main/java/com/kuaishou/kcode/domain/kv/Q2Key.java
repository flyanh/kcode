package com.kuaishou.kcode.domain.kv;

import java.util.Objects;

/**
 * @author flyan
 * date 2020-07-16
 * @function 阶段 2 查询 Key
 */
public class Q2Key {

    private String caller;
    private String responder;
    private String time;
    private String type;
    private int hash;

    public Q2Key(String caller, String responder, String time, String type) {
        this.caller = caller;
        this.responder = responder;
        this.time = time;
        this.type = type;
        /* 饿汉模式，立马计算哈希值 */
        hash = 29791 * caller.hashCode() + 961 * responder.hashCode() + 31 * time.hashCode() + type.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Q2Key q2Key = (Q2Key) o;
        return Objects.equals(caller, q2Key.caller) &&
                Objects.equals(responder, q2Key.responder) &&
                Objects.equals(time, q2Key.time) &&
                Objects.equals(type, q2Key.type);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * 通过一个魔数获取该 Key 的魔法索引
     *
     * @param magic 魔数
     * @return 魔法索引
     */
    public int dynicMagicIndex(int magic){
        return hash & magic;
    }

}
