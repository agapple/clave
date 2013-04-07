package com.alibaba.otter.clave.model;

import java.util.Comparator;

/**
 * 按照EventColumn的index进行排序
 * 
 * @author jianghang 2013-3-28 下午09:18:28
 * @version 1.0.0
 */
public class EventColumnIndexComparable implements Comparator<EventColumn> {

    public int compare(EventColumn o1, EventColumn o2) {
        if (o1.getIndex() < o2.getIndex()) {
            return -1;
        } else if (o1.getIndex() == o2.getIndex()) {
            return 0;
        } else {
            return 1;
        }
    }

}
