package com.zhizus.forest.thrift.client.cluster.loadbalance;

import com.google.common.collect.Lists;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Dempe on 2016/12/22.
 */
public class LocalFirstLoadBalance extends RandomLoadBalance {

    private final static Logger LOGGER = LoggerFactory.getLogger(LocalFirstLoadBalance.class);

    public LocalFirstLoadBalance(Registry registry, IsolationStrategy<ServerInfo> isolationStrategy) {
        super(registry, isolationStrategy);
    }

    public ServerInfo select(String key) {
        String hostAddress = HashLoadBalance.NetUtils.getLocalAddress().getHostAddress();
        long localIP = ipToLong(hostAddress);
        List<ServerInfo> serverInfos = filterLocalServerInfo(localIP);
        if (serverInfos.isEmpty()) {
            return super.select(key);
        }
        int index = ThreadLocalRandom.current().nextInt(serverInfos.size());
        return serverInfos.get(index % serverInfos.size());
    }

    private List<ServerInfo> filterLocalServerInfo(long localIP) {
        List<ServerInfo> localServers = Lists.newArrayList();
        if (localIP == 0) {
            return localServers;
        }
        List<ServerInfo> availableServerList = getAvailableServerList();
        for (ServerInfo serverInfo : availableServerList) {
            long ip = ipToLong(serverInfo.getHost());
            if (ip == localIP) {
                localServers.add(serverInfo);
            }
        }
        return localServers;
    }


    public static long ipToLong(final String addr) {
        final String[] addressBytes = addr.split("\\.");
        int length = addressBytes.length;
        if (length < 3) {
            return 0;
        }
        long ip = 0;
        try {
            for (int i = 0; i < 4; i++) {
                ip <<= 8;
                ip |= Integer.parseInt(addressBytes[i]);
            }
        } catch (Exception e) {
            LOGGER.warn("Warn ipToInt addr is wrong: addr=" + addr);
        }

        return ip;
    }
}
