package com.digitalpetri.enip;


import java.time.Duration;


public class TestClient
{
    public static void main(String[] args) throws Exception {
        EtherNetIpClientConfig config = EtherNetIpClientConfig.builder("192.168.1.4")
                .setSerialNumber(0x04)
                .setVendorId(0x00)
                .setTimeout(Duration.ofSeconds(10))
                .build();
        EtherNetIpClient client = new EtherNetIpClient(config);

        client.connect().get();

        client.listIdentity().whenComplete((li, ex) -> {
//
            System.out.println(li);
        });

        client.disconnect().get();

// Call this before application / JVM shutdown
        EtherNetIpShared.releaseSharedResources();

    }
}
