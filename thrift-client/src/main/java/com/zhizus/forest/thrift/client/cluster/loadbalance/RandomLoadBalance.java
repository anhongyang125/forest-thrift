package com.zhizus.forest.thrift.client.cluster.loadbalance;


import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.registry.Registry;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Dempe on 2016/12/26.
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public RandomLoadBalance(Registry registry, IsolationStrategy<ServerInfo> isolationStrategy) {
        super(registry, isolationStrategy);
    }

    @Override
    public ServerInfo select(String key) {
        List<ServerInfo> availableServers = getAvailableServerList();
        int idx = (int) (ThreadLocalRandom.current().nextDouble() * availableServers.size());
        return availableServers.get((idx) % availableServers.size());
    }


}
