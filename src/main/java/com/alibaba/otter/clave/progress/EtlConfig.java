package com.alibaba.otter.clave.progress;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * 相关etl规则定义
 * 
 * @author jianghang 2013-4-7 下午03:15:14
 * @version 1.0.0
 */
public class EtlConfig {

    public static class TablePair {

        private String                        source;
        private String                        target;

        private List<EtlPair<String, String>> columnPairs    = Lists.newArrayList();
        private ColumnPairMode                columnPairMode = ColumnPairMode.INCLUDE;

        private int                           weight         = 1;                     // 定义数据处理权重

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public List<EtlPair<String, String>> getColumnPairs() {
            return columnPairs;
        }

        public void setColumnPairs(List<EtlPair<String, String>> columnPairs) {
            this.columnPairs = columnPairs;
        }

        public ColumnPairMode getColumnPairMode() {
            return columnPairMode;
        }

        public void setColumnPairMode(ColumnPairMode columnPairMode) {
            this.columnPairMode = columnPairMode;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

    }

    public enum ColumnPairMode {
        INCLUDE, EXCLUDE;
    }

    public static class EtlPair<S, T> {

        private S source;
        private T target;

        public S getSource() {
            return source;
        }

        public void setSource(S source) {
            this.source = source;
        }

        public T getTarget() {
            return target;
        }

        public void setTarget(T target) {
            this.target = target;
        }

    }
}
