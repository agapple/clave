package com.alibaba.otter.clave.progress.select.canal;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.clave.exceptions.ClaveException;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.select.Message;

/**
 * 基于canal client模式实现数据获取方式
 * 
 * @author jianghang 2013-3-28 下午10:01:19
 * @version 1.0.0
 */
public class CanalClientSelector extends AbstractCanalSelector {

    private String                  destination;
    private String                  zkServers;
    private List<InetSocketAddress> address;
    private String                  username  = "";
    private String                  password  = "";
    private volatile boolean        running   = false; // 是否处于运行中

    private CanalConnector          connector;
    private String                  filter;
    private int                     batchSize = 5000;

    public CanalClientSelector(){

    }

    public CanalClientSelector(String destination, String zkServers){
        this.destination = destination;
        this.zkServers = zkServers;
    }

    public CanalClientSelector(String destination, List<InetSocketAddress> address){
        this.destination = destination;
        this.address = address;
    }

    public void start() {
        if (StringUtils.isNotEmpty(zkServers)) {
            connector = CanalConnectors.newClusterConnector(zkServers, destination, username, password);
        } else if (!CollectionUtils.isEmpty(address)) {
            connector = CanalConnectors.newClusterConnector(address, destination, username, password);
        } else {
            throw new ClaveException("no server zkservers or canal server address!");
        }

        connector.connect();
        connector.subscribe(filter);
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }

        connector.disconnect();
        running = false;
    }

    public boolean isStart() {
        return running;
    }

    public Message<EventData> selector() throws InterruptedException {
        com.alibaba.otter.canal.protocol.Message message = null;
        while (running) {
            message = connector.getWithoutAck(batchSize);
            if (message == null || message.getId() == -1L) { // 代表没数据
                continue;
            } else {
                break;
            }
        }

        if (!running) {
            throw new InterruptedException();
        }

        return selector(message);
    }

    public void ack(Long batchId) {
        connector.ack(batchId);
    }

    public void rollback(Long batchId) {
        connector.rollback(batchId);
    }

    public void rollback() {
        connector.rollback();
    }

    // ================= setter / getter =====================

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public void setAddress(List<InetSocketAddress> address) {
        this.address = address;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
