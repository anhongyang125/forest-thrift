package com.zhizus.forest.thrift.demo.server;

import com.zhizus.forest.thrift.demo.gen.Sample;
import com.zhizus.forest.thrift.server.AbstractThriftServer;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

/**
 * Created by Dempe on 2016/12/29.
 */
public class SampleServer extends AbstractThriftServer {
    @Override
    public String getServerName() {
        return "SampleServer";
    }

    @Override
    public int getPort() {
        return 7777;
    }

    @Override
    public int getFramedSize() {
        return 16777216;
    }

    @Override
    public TProcessor getProcessor() {
        return new Sample.Processor(new Sample.Iface() {

            @Override
            public String hello(String para) throws TException {
                return "hello " + para;
            }

            @Override
            public boolean ping() throws TException {
                return true;
            }
        });
    }

    public static void main(String[] args) {
        new SampleServer().start();
    }
}
