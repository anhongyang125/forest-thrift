package com.zhizus.forest.thrift.client.cluster;

import com.zhizus.forest.thrift.client.ServerInfo;

import java.util.List;

/**
 * Created by Dempe on 2016/12/26.
 */
public interface LoadBalance<T> {

    T select(String key);

    List<ServerInfo> getAvailableServerList();

    void setList(List<ServerInfo> list);
}
