package com.zhizus.forest.thrift.client.cluster.loadbalance;

import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.registry.Registry;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Dempe on 2016/12/22.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    private AtomicInteger idx = new AtomicInteger(0);

    public RoundRobinLoadBalance(Registry registry, IsolationStrategy<ServerInfo> isolationStrategy) {
        super(registry, isolationStrategy);
    }

    @Override
    public ServerInfo select(String key) {
        List<ServerInfo> availableServerList = getAvailableServerList();
        return availableServerList.get(idx.incrementAndGet() % availableServerList.size());
    }
}
