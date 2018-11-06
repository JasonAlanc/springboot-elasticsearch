package com.alanc;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author alanc
 * @ClassName MyConfig
 * @Description TODO 配置类
 * @date: 2018/11/3 11:03
 */

@Configuration
public class MyConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {


        TransportAddress node = new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300);

        TransportAddress node1 = new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301);

        TransportAddress node2 = new TransportAddress(InetAddress.getByName("127.0.0.1"), 9302);

        Settings settings = Settings.builder().put("cluster.name", "alanc").build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node2);


        return client;

    }

}
