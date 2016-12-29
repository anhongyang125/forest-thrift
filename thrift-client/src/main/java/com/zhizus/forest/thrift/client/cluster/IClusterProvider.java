package com.zhizus.forest.thrift.client.cluster;

import com.zhizus.forest.thrift.client.ServerInfo;
import org.apache.thrift.TServiceClient;

/**
 * Created by Dempe on 2016/12/26.
 */
public interface IClusterProvider<T> {

    <X extends TServiceClient> X iface(T serverInfo, Class<X> ifaceClass) throws Exception;

    void clear(ServerInfo key);
}
