package com.zhizus.forest.thrift.client;

import org.apache.thrift.transport.TTransport;

/**
 * Created by Dempe on 2016/12/26.
 */
public interface PingValidate {
    boolean ping(ServerInfo key, TTransport transport);
}
