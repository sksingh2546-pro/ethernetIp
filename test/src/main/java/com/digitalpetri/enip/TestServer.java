package com.digitalpetri.enip;

public class TestServer
{
    public static void main(String[] Args) throws Exception
    {
        EtherNetIpServerConfig config = EtherNetIpServerConfig.builder("192.168.1.4").build();
        EtherNetIpServer server = new EtherNetIpServer(config);

        System.out.println("Listening for EtherNet/IP clients on port " + config.getPort());
        server.run();
    }
}
