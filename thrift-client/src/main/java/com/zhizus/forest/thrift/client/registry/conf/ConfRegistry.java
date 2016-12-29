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
package com.zhizus.forest.thrift.client.registry.conf;

import com.google.common.collect.Lists;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.registry.Registry;
import com.zhizus.forest.thrift.client.registry.RegistryListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConfRegistry implements Registry {

    private Set<RegistryListener> listeners = Collections.synchronizedSet(new HashSet<RegistryListener>());

    private List<ServerInfo> serverInfoList = Lists.newArrayList();

    public ConfRegistry(String confStr) {
        serverInfoList = ServerInfo.ofs(confStr);
    }

    @Override
    public List list() {
        return serverInfoList;
    }

    @Override
    public void addListener(RegistryListener listener) {
        listeners.add(listener);
    }
}
