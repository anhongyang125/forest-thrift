package com.zhizus.forest.thrift.client.registry.zk;

import com.google.common.collect.Lists;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.registry.Registry;
import com.zhizus.forest.thrift.client.registry.RegistryListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.*;

/**
 * Created by Dempe on 2016/12/8.
 */
public class ZkRegistry implements TreeCacheListener, Registry<ServerInfo> {

    private final static InstanceSerializer serializer = new JsonInstanceSerializer(ServerInfo.class);

    private ServiceDiscovery<ServerInfo> serviceDiscovery;

    private Set<RegistryListener> listeners = Collections.synchronizedSet(new HashSet<RegistryListener>());

    private String name;

    private final static String BASE_PATH = "forest_thrift_client";

    public ZkRegistry(String connStr, String name) throws Exception {
        this(connStr, name, BASE_PATH);
    }

    public ZkRegistry(String connStr, String name, String basePath) throws Exception {
        this.name = name;
        CuratorFramework client = CuratorFrameworkFactory.newClient(connStr, new ExponentialBackoffRetry(1000, 3));
        client.start();
        serviceDiscovery = ServiceDiscoveryBuilder.builder(ServerInfo.class)
                .client(client)
                .basePath(basePath)
                .serializer(serializer)
                .build();
        serviceDiscovery.start();
    }

    public List<ServerInfo> list() throws Exception {
        List<ServerInfo> serverInfoList = Lists.newArrayList();
        Collection<ServiceInstance<ServerInfo>> serviceInstances = serviceDiscovery.queryForInstances(name);
        for (ServiceInstance<ServerInfo> serviceInstance : serviceInstances) {
            serverInfoList.add(serviceInstance.getPayload());
        }
        return serverInfoList;
    }

    public void addListener(RegistryListener listener) {
        listeners.add(listener);
    }


    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        ChildData data = event.getData();
        ServiceInstance<ServerInfo> serviceInstance = serializer.deserialize(data.getData());
        switch (event.getType()) {
            case NODE_ADDED: {
                for (RegistryListener listener : listeners) {
                    listener.onFresh();
                }
                break;
            }
            case NODE_UPDATED: {
                for (RegistryListener listener : listeners) {
                    listener.onFresh();
                }
                break;
            }
            case NODE_REMOVED: {
                for (RegistryListener listener : listeners) {
                    listener.onRemove(serviceInstance.getPayload());
                }
                break;
            }
            default:
        }
    }
}
