package com.alibaba.otter.clave.progress.select;

import java.io.Serializable;
import java.util.List;

/**
 * 数据对象
 * 
 * @author jianghang 2013-3-28 下午09:21:32
 * @version 1.0.0
 */
public class Message<T> implements Serializable {

    private static final long serialVersionUID = 4999493579483771204L;
    private Long              id;
    private List<T>           datas;

    public Message(Long id, List<T> datas){
        this.id = id;
        this.datas = datas;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<T> getDatas() {
        return datas;
    }

    public void setDatas(List<T> datas) {
        this.datas = datas;
    }

}
