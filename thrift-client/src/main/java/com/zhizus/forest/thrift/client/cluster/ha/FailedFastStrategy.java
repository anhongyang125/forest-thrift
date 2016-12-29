package com.zhizus.forest.thrift.client.cluster.ha;

import com.zhizus.forest.thrift.client.cluster.HAStrategy;
import com.zhizus.forest.thrift.client.cluster.IClusterProvider;
import com.zhizus.forest.thrift.client.cluster.LoadBalance;
import org.apache.thrift.TServiceClient;

/**
 * Created by Dempe on 2016/12/26.
 */
public class FailedFastStrategy<T> implements HAStrategy<T> {

    private IClusterProvider provider;

    public FailedFastStrategy(IClusterProvider provider) {
        this.provider = provider;
    }

    @Override
    public <X extends TServiceClient> X iface(LoadBalance<T> loadBalance, String key, Class<X> ifaceClass) throws Exception {

        T select = loadBalance.select(key);
        if (select == null) {
            throw new IllegalArgumentException("ServerInfo is null !");
        }
        return (X) provider.iface(select, ifaceClass);
    }
}
