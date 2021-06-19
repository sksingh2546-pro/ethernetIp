package com.digitalpetri.enip;

import com.digitalpetri.enip.cip.CipClient;
import com.digitalpetri.enip.cip.CipConnectionPool;
import com.digitalpetri.enip.cip.epath.DataSegment;
import com.digitalpetri.enip.cip.epath.EPath;
import com.digitalpetri.enip.cip.epath.PortSegment;

import com.digitalpetri.enip.logix.services.ReadTagService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;


public class TestClient
{
    public static void main(String[] args) throws Exception {
        EtherNetIpClientConfig config = EtherNetIpClientConfig.builder("10.0.11.5")
                .setSerialNumber(0x00)
                .setVendorId(0x00)
                .setTimeout(Duration.ofSeconds(2))
                .build();

        EtherNetIpClient client = new EtherNetIpClient(config);

        client.connect().get();

        client.listIdentity().whenComplete((li, ex) -> {
            if (li != null) {
                li.getIdentity().ifPresent(id -> {
                    System.out.println("productName=" + id.getProductName());
                    System.out.println("revisionMajor=" + id.getRevisionMajor());
                    System.out.println("revisionMinor=" +id.getDeviceType());
                });
            } else {
                ex.printStackTrace();
            }
        });

        client.disconnect().get();

        EPath.PaddedEPath connectionPath = new EPath.PaddedEPath(
                new PortSegment(1, new byte[]{(byte) 0}));

        CipClient client1 = new CipClient(config, connectionPath);

        client1.connect().get();

        CipConnectionPool pool = new CipConnectionPool(2, client1, connectionPath, 500);

// the tag we'll use as an example
        EPath.PaddedEPath requestPath = new EPath.PaddedEPath(
                new DataSegment.AnsiDataSegment("DINT"));
        ReadTagService readTagService=new ReadTagService(requestPath);
        System.out.println(pool.acquire().isDone());
        pool.acquire().whenComplete((connection, ex) -> {
            //System.out.println(connection.getO2tConnectionId());
            if (connection != null) {
                CompletableFuture<ByteBuf> f = client1.invokeConnected(connection.getO2tConnectionId(), readTagService);
                System.out.println(f.isDone());
                try {


                    f.whenComplete((data, ex2) -> {
                        if (data != null) {
                            System.out.println("Tag data: " + ByteBufUtil.hexDump(data));
                        } else {
                            ex2.printStackTrace();
                        }
                        pool.release(connection);
                    });
                }catch (Exception e){
                    System.out.println(e);
                }
            } else {
                ex.printStackTrace();
            }
        });



// Call this before application / JVM shutdown
        EtherNetIpShared.releaseSharedResources();
    }
}
