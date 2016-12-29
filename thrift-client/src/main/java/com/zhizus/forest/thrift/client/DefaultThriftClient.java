/*
 * Copyright (c) 2014 yy.com. 
 *
 * All Rights Reserved.
 *
 * This program is the confidential and proprietary information of 
 * YY.INC. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with yy.com.
 */
package com.zhizus.forest.thrift.client;

import com.zhizus.forest.thrift.client.cluster.HAStrategy;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.cluster.LoadBalance;
import com.zhizus.forest.thrift.client.cluster.ha.FailedFastStrategy;
import com.zhizus.forest.thrift.client.cluster.ha.FailedOverStrategy;
import com.zhizus.forest.thrift.client.cluster.loadbalance.*;
import com.zhizus.forest.thrift.client.cluster.privoder.PooledClusterProvider;
import com.zhizus.forest.thrift.client.registry.Registry;
import com.zhizus.forest.thrift.client.registry.RegistryListener;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultThriftClient implements RegistryListener<ServerInfo> {

    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultThriftClient.class);

    public enum LoadBalanceType {RANDOM, ROBBIN, HASH, ACTIVE_WEIGHT, LOCAL_FIRST}

    public enum HAStrategyType {FAILED_OVER, FAILED_FAST}

    private LoadBalance<ServerInfo> loadBalance;

    private HAStrategy<ServerInfo> haStrategy;

    private Registry<ServerInfo> registry;

    private PooledClusterProvider pooledClusterProvider;

    public DefaultThriftClient(LoadBalanceType loadBalanceType, HAStrategyType haStrategyType,
                               Registry<ServerInfo> registry, GenericKeyedObjectPoolConfig poolConfig, PingValidate pingValidate) throws Exception {
        this(loadBalanceType, haStrategyType, registry, poolConfig, pingValidate, new IsolationStrategy<ServerInfo>());
    }

    public DefaultThriftClient(LoadBalanceType loadBalanceType, HAStrategyType haStrategyType,
                               GenericKeyedObjectPoolConfig poolConfig, Registry<ServerInfo> registry) throws Exception {
        this(loadBalanceType, haStrategyType, registry, poolConfig, null, new IsolationStrategy<ServerInfo>());
    }

    public DefaultThriftClient(LoadBalanceType loadBalanceType, HAStrategyType haStrategyType,
                               Registry<ServerInfo> registry, GenericKeyedObjectPoolConfig poolConfig, PingValidate pingValidate,
                               IsolationStrategy<ServerInfo> infoIsolationStrategy) throws Exception {
        this.registry = registry;
        registry.addListener(this);
        switch (loadBalanceType) {
            case RANDOM:
                this.loadBalance = new RandomLoadBalance(registry, infoIsolationStrategy);
                break;
            case ROBBIN:
                this.loadBalance = new RoundRobinLoadBalance(registry, infoIsolationStrategy);
                break;
            case HASH:
                this.loadBalance = new HashLoadBalance(registry, infoIsolationStrategy);
                break;
            case ACTIVE_WEIGHT:
                this.loadBalance = new ActiveWeightLoadBalance(registry, infoIsolationStrategy);
                break;
            case LOCAL_FIRST:
                this.loadBalance = new LocalFirstLoadBalance(registry, infoIsolationStrategy);
                break;
            default:
                this.loadBalance = new RandomLoadBalance(registry, infoIsolationStrategy);
        }
        List<ServerInfo> list = registry.list();
        loadBalance.setList(list);
        this.pooledClusterProvider = new PooledClusterProvider(poolConfig, infoIsolationStrategy, pingValidate);
        switch (haStrategyType) {
            case FAILED_FAST:
                this.haStrategy = new FailedFastStrategy<>(pooledClusterProvider);
                break;
            case FAILED_OVER:
                this.haStrategy = new FailedOverStrategy<>(pooledClusterProvider);
                break;
            default:
                this.pooledClusterProvider = new PooledClusterProvider(poolConfig, infoIsolationStrategy, pingValidate);
                this.haStrategy = new FailedFastStrategy<>(pooledClusterProvider);
        }
    }

    public DefaultThriftClient(Registry<ServerInfo> registry, GenericKeyedObjectPoolConfig poolConfig, PingValidate pingValidate) throws Exception {
        this(LoadBalanceType.RANDOM, HAStrategyType.FAILED_FAST, registry, poolConfig, pingValidate, new IsolationStrategy<ServerInfo>());
    }

    public DefaultThriftClient(Registry<ServerInfo> registry, GenericKeyedObjectPoolConfig poolConfig) throws Exception {
        this(LoadBalanceType.RANDOM, HAStrategyType.FAILED_FAST, registry, poolConfig, null, new IsolationStrategy<ServerInfo>());
    }

    public <X extends TServiceClient> X iface(Class<X> ifaceClass) throws Exception {
        return haStrategy.iface(loadBalance, null, ifaceClass);
    }

    public <X extends TServiceClient> X ifaceByHash(Class<X> ifaceClass, String hashKey) throws Exception {
        return haStrategy.iface(loadBalance, hashKey, ifaceClass);
    }


    @Override
    public void onFresh() {
        try {
            List<ServerInfo> list = registry.list();
            if (list != null && !list.isEmpty()) {
                loadBalance.setList(list);
            }
        } catch (Exception e) {
            LOGGER.error("registry list err,", e);
        }
    }

    @Override
    public void onRemove(ServerInfo serverInfo) {
        // 当key从配置中心移除时，主动清除连接池里面配置
        pooledClusterProvider.clear(serverInfo);
    }

}
