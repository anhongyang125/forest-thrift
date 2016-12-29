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
import com.zhizus.forest.thrift.client.cluster.loadbalance.RandomLoadBalance;
import com.zhizus.forest.thrift.client.cluster.privoder.PooledClusterProvider;
import com.zhizus.forest.thrift.client.registry.Registry;
import com.zhizus.forest.thrift.client.registry.RegistryListener;
import com.zhizus.forest.thrift.client.registry.conf.ConfRegistry;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultThriftClient implements RegistryListener<ServerInfo> {

    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultThriftClient.class);

    public enum LoadBalanceType {RANDOM, ROBBIN, HASH}

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
            default:
                this.loadBalance = new RandomLoadBalance(registry, infoIsolationStrategy);
        }
        List<ServerInfo> list = registry.list();
        loadBalance.setList(list);
        switch (haStrategyType) {
            case FAILED_FAST:
                this.pooledClusterProvider = new PooledClusterProvider(poolConfig, infoIsolationStrategy, pingValidate);
                this.haStrategy = new FailedFastStrategy<>(pooledClusterProvider);
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
            if(list!=null&&!list.isEmpty()){
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

    public static void main(String[] args) throws Exception {

        // 连接池配置，
        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

        // 校验接口，连接池的validateObject方法会调用到这里的ping方法
        // 这里主要用于对thrift描述文件层面的心跳校验支持
        PingValidate pingValidate = new PingValidate() {
            @Override
            public boolean ping(ServerInfo key, TTransport transport) {
                return true;
            }
        };
        // 客户端熔断策略，默认1min中10次异常则自动熔断，恢复时间也为1min中
        IsolationStrategy<ServerInfo> infoIsolationStrategy = new IsolationStrategy<>();

        DefaultThriftClient thriftClient = new DefaultThriftClient(LoadBalanceType.RANDOM, HAStrategyType.FAILED_FAST,
                new ConfRegistry("localhost:9999"), poolConfig, pingValidate, infoIsolationStrategy);
        //每次使用client请调用iface接口，这里通过代理模式包装了异常统计和回池操作，一个 iface生成的代理对象调用多次会出现问题
        //  thriftClient.iface(YourThrift.class);
    }


}
