package com.zhizus.forest.thrift.demo.client;

import com.zhizus.forest.thrift.client.DefaultThriftClient;
import com.zhizus.forest.thrift.client.PingValidate;
import com.zhizus.forest.thrift.client.ServerInfo;
import com.zhizus.forest.thrift.client.registry.conf.ConfRegistry;
import com.zhizus.forest.thrift.demo.gen.Sample;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dempe on 2016/12/29.
 */
public class SampleThriftClientDemo {
    private final static Logger LOGGER = LoggerFactory.getLogger(SampleThriftClientDemo.class);

    public static void main(String[] args) throws Exception {

        DefaultThriftClient client = new DefaultThriftClient(DefaultThriftClient.LoadBalanceType.RANDOM,
                DefaultThriftClient.HAStrategyType.FAILED_FAST, new ConfRegistry("localhost:7777"), new GenericKeyedObjectPoolConfig(), new PingValidate() {
            @Override
            public boolean ping(ServerInfo key, TTransport transport) {
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                Sample.Client sampleClient = new Sample.Client(protocol);
                try {
                    return sampleClient.ping();
                } catch (TException e) {
                    LOGGER.error("ping err : " + e.getMessage(), e);
                    return false;
                }
            }
        });
        for (int i = 0; i < 1000; i++) {
            Sample.Client sampleClient = client.iface(Sample.Client.class);
            String hello = sampleClient.hello("forest thrift");
            LOGGER.info("hello value:{}", hello);
        }

    }
}
