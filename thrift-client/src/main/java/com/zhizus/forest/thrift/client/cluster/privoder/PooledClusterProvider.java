package com.zhizus.forest.thrift.client.cluster.privoder;

import com.zhizus.forest.thrift.client.PingValidate;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.cluster.IClusterProvider;
import com.zhizus.forest.thrift.client.cluster.IsolationStrategy;
import com.zhizus.forest.thrift.client.utils.ThriftClientUtils;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Created by Dempe on 2016/12/26.
 */
public class PooledClusterProvider implements IClusterProvider<ServerInfo> {

    private GenericKeyedObjectPool<ServerInfo, TTransport> pool;

    private IsolationStrategy<ServerInfo> isolationStrategy;

    public PooledClusterProvider(GenericKeyedObjectPoolConfig poolConfig, IsolationStrategy isolationStrategy, PingValidate pingValidate) {
        this.isolationStrategy = isolationStrategy;
        pool = new GenericKeyedObjectPool<ServerInfo, TTransport>(new KeyedPooledThriftConnFactory(isolationStrategy, pingValidate), poolConfig);
    }

    public PooledClusterProvider(PingValidate pingValidate) {
        this(new GenericKeyedObjectPoolConfig(), new IsolationStrategy(), pingValidate);
    }

    @Override
    public <X extends TServiceClient> X iface(final ServerInfo serverInfo, final Class<X> ifaceClass) throws Exception {
        final TTransport transport = pool.borrowObject(serverInfo);
        // 代理
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(ifaceClass);
        factory.setFilter(new MethodFilter() {
            @Override
            public boolean isHandled(Method m) {
                return ThriftClientUtils.getInterfaceMethodNames(ifaceClass).contains(m.getName());
            }
        });
        try {
            X x = (X) factory.create(new Class[]{TProtocol.class}, new Object[]{new TBinaryProtocol(transport)});
            ((Proxy) x).setHandler(new MethodHandler() {
                @Override
                public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                    boolean success = false;
                    try {
                        Object result = proceed.invoke(self, args);
                        success = true;
                        //统计调用次数
                        serverInfo.incrementAndGet();
                        return result;
                    } finally {
                        if (success) {
                            pool.returnObject(serverInfo, transport);
                        } else {
                            isolationStrategy.fail(serverInfo);
                            pool.invalidateObject(serverInfo, transport);
                        }
                    }
                }
            });
            return x;
        } catch (NoSuchMethodException | IllegalArgumentException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("fail to create proxy.", e);
        }
    }

    public void clear(ServerInfo key) {
        pool.clear(key);
    }

    /**
     * Created by Dempe on 2016/12/23.
     */
    public static class KeyedPooledThriftConnFactory implements KeyedPooledObjectFactory<ServerInfo, TTransport> {

        private static final Logger logger = LoggerFactory.getLogger(KeyedPooledThriftConnFactory.class);

        private int timeout;
        private IsolationStrategy<ServerInfo> isolationStrategy;

        private PingValidate pingValidate;

        public KeyedPooledThriftConnFactory(IsolationStrategy<ServerInfo> isolationStrategy, PingValidate pingValidate) {
            this.isolationStrategy = isolationStrategy;
            this.pingValidate = pingValidate;
            this.timeout = (int) TimeUnit.SECONDS.toMillis(5L);
        }

        @Override
        public PooledObject<TTransport> makeObject(ServerInfo info) throws Exception {
            TSocket tsocket = new TSocket(info.getHost(), info.getPort());
            tsocket.setTimeout(timeout);
            TFramedTransport transport = new TFramedTransport(tsocket);
            transport.open();
            logger.trace("make new thrift connection:{}", info);
            return new DefaultPooledObject<TTransport>(transport);
        }


        @Override
        public void destroyObject(ServerInfo key, PooledObject<TTransport> p) throws Exception {

        }

        @Override
        public boolean validateObject(ServerInfo key, PooledObject<TTransport> p) {
            boolean validate = p.getObject().isOpen() && (pingValidate == null || pingValidate.ping(key, p.getObject()));
            if (!validate) {
                isolationStrategy.fail(key);
            }
            return validate;
        }


        @Override
        public void activateObject(ServerInfo key, PooledObject<TTransport> p) throws Exception {

        }

        @Override
        public void passivateObject(ServerInfo key, PooledObject<TTransport> p) throws Exception {

        }
    }
}
