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
package com.zhizus.forest.thrift.client.registry;

import java.util.List;

public interface Registry<T> {

    List<T> list() throws Exception;

    void addListener(RegistryListener listener);

}