package com.zhizus.forest.thrift.client.registry;

/**
 * Created by Dempe on 2016/12/26.
 */
public interface RegistryListener<T> {


    void onFresh();

    /**
     * 返回移除掉的服务器
     */
    void onRemove(T T);

}
