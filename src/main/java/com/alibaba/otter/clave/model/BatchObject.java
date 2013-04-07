package com.alibaba.otter.clave.model;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.clave.utils.ClaveToStringStyle;

/**
 * 数据batch对象
 * 
 * @author jianghang 2013-3-28 下午11:19:13
 * @version 1.0.0
 */
public abstract class BatchObject<T> implements Serializable {

    private static final long serialVersionUID = 3211077130963551303L;

    public abstract void merge(T data);

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ClaveToStringStyle.DEFAULT_STYLE);
    }

}
