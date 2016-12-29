package com.zhizus.forest.thrift.client.cluster.loadbalance;

import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.registry.Registry;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 低并发优先
 * Created by Dempe on 2016/12/22.
 */
public class ActiveWeightLoadBalance extends AbstractLoadBalance {

    public ActiveWeightLoadBalance(Registry registry, IsolationStrategy<ServerInfo> isolationStrategy) {
        super(registry, isolationStrategy);
    }

    @Override
    public ServerInfo select(String key) {
        List<ServerInfo> availableServerList = getAvailableServerList();
        if (availableServerList.size() < 1) {
            return null;
        }
        Collections.sort(availableServerList, new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                return o1.activeCountGet() - o2.activeCountGet();
            }
        });
        return availableServerList.get(0);
    }
}
