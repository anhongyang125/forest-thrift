package com.zhizus.forest.thrift.client.cluster;

import org.apache.thrift.TServiceClient;

/**
 * Created by Dempe on 2016/12/26.
 */
public interface HAStrategy<T> {

    <X extends TServiceClient> X iface(LoadBalance<T> loadBalance, String key, Class<X> ifaceClass) throws Exception;
}
