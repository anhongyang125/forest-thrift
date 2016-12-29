package com.zhizus.forest.thrift.client.cluster.loadbalance;

import com.google.common.collect.Lists;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.cluster.LoadBalance;
import com.zhizus.forest.thrift.client.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;


/**
 * Created by Dempe on 2016/12/26.
 */
public abstract class AbstractLoadBalance implements LoadBalance<ServerInfo> {

    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractLoadBalance.class);

    public Registry registry;

    private List<ServerInfo> serverInfoList = Lists.newArrayList();

    private IsolationStrategy<ServerInfo> isolationStrategy;

    public AbstractLoadBalance(Registry registry, IsolationStrategy<ServerInfo> isolationStrategy) {
        this.registry = registry;
        this.isolationStrategy = isolationStrategy;
    }

    public List<ServerInfo> getAvailableServerList() {
        List<ServerInfo> availableList = Lists.newArrayList();
        Set<ServerInfo> failed = isolationStrategy.getFailed();
        for (ServerInfo serverInfo : serverInfoList) {
            if (!failed.contains(serverInfo)) {
                availableList.add(serverInfo);
            }
        }
        if (availableList.isEmpty()) {
            for (ServerInfo serverInfo : serverInfoList) {
                availableList.add(serverInfo);
            }
            LOGGER.warn("available server list is empty, use failed back list instead");
        }
        return serverInfoList;
    }

    @Override
    public void setList(List<ServerInfo> list) {
        serverInfoList =list;
    }
}
