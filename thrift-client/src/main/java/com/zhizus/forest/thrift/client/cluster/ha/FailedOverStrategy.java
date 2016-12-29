package com.zhizus.forest.thrift.client.cluster.ha;

import com.zhizus.forest.thrift.client.cluster.HAStrategy;
import com.zhizus.forest.thrift.client.cluster.IClusterProvider;
import com.zhizus.forest.thrift.client.cluster.LoadBalance;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dempe on 2016/12/26.
 */
public class FailedOverStrategy<T> implements HAStrategy<T> {

    private final static Logger LOGGER = LoggerFactory.getLogger(FailedOverStrategy.class);

    private IClusterProvider provider;

    private static final long MAX_RETRY_TIMEOUT = 5000L;//最大重试连接超时时间

    private static final int MAX_RETRY_COUNT = 5;

    public FailedOverStrategy(IClusterProvider provider) {
        this.provider = provider;
    }

    @Override
    public <X extends TServiceClient> X iface(LoadBalance<T> loadBalance, String key, Class<X> ifaceClass) throws Exception {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < loadBalance.getAvailableServerList().size()
                && i < MAX_RETRY_COUNT && System.currentTimeMillis() - startTime < MAX_RETRY_TIMEOUT; i++) {
            T select = null;
            try {
                select = loadBalance.select(key);
                if (select == null) {
                    throw new IllegalArgumentException("ServerInfo is null !");
                }
                return (X) provider.iface(select, ifaceClass);
            } catch (Exception e) {
                LOGGER.warn("iface failed. tryNum:{},select:{}", i, select, e);
                throw e;
            }
        }
        return null;
    }
}
