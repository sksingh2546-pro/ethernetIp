package com.digitalpetri.enip;

import com.digitalpetri.enip.codec.EnipCodec;
import com.digitalpetri.enip.commands.Command;
import com.digitalpetri.enip.commands.CommandCode;
import com.digitalpetri.enip.commands.SendRRData;
import com.digitalpetri.enip.commands.SendUnitData;
import com.digitalpetri.enip.cpf.ConnectedDataItemResponse;
import com.digitalpetri.enip.cpf.CpfPacket;
import com.digitalpetri.enip.cpf.UnconnectedDataItemResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class EtherNetIpServer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ExecutorService executor;

    private final Map<Long, PendingRequest<? extends Command>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong senderContext = new AtomicLong(0L);

    private volatile long sessionHandle;

    private final EtherNetIpServerConfig config;

    public EtherNetIpServer(EtherNetIpServerConfig config) {
        this.config = config;

        executor = config.getExecutor();
    }

    public void run() throws InterruptedException
    {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EtherNetIpServer instance = this;

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new EnipCodec());
                            ch.pipeline().addLast(new EtherNetIpServerHandler(instance));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(config.getPort()).sync();

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public EtherNetIpServerConfig getConfig() {
        return config;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    private void onChannelRead(ChannelHandlerContext channelHandlerContext, EnipPacket packet) {
        CommandCode commandCode = packet.getCommandCode();
        EnipStatus status = packet.getStatus();

        System.out.println("server received " + packet.getCommandCode());

        if (commandCode == CommandCode.SendUnitData) {
            if (status == EnipStatus.EIP_SUCCESS) {
                onUnitDataReceived((SendUnitData) packet.getCommand());
            } else {
                config.getLoggingContext().forEach(MDC::put);
                try {
                    logger.warn("Received SendUnitData command with status: {}", status);
                } finally {
                    config.getLoggingContext().keySet().forEach(MDC::remove);
                }
            }
        } else {
            if (commandCode == CommandCode.RegisterSession) {
                if (status == EnipStatus.EIP_SUCCESS) {
                    sessionHandle = packet.getSessionHandle();
                    System.out.println("Register request: session handle: " + sessionHandle);
                    channelHandlerContext.writeAndFlush(packet);
                } else {
                    sessionHandle = 0L;
                }
            } else if (packet.getCommand() instanceof SendRRData) {
                CpfPacket cpfPacket = ((SendRRData) packet.getCommand()).getPacket();

                Arrays.stream(cpfPacket.getItems()).forEach(item -> {
                    if (item instanceof ConnectedDataItemResponse) {
                        System.out.println("Received connected data item response");
                    } else if (item instanceof UnconnectedDataItemResponse) {
                        UnconnectedDataItemResponse response = (UnconnectedDataItemResponse) item;

                        ByteBuf data = response.getData();
                        System.out.println("Received unconnected data item response: " + ByteBufUtil.hexDump(data));
                    }
                });
            }

            PendingRequest<?> pending = pendingRequests.remove(packet.getSenderContext());

            if (pending != null) {
                pending.timeout.cancel();

                if (status == EnipStatus.EIP_SUCCESS) {
                    pending.promise.complete(packet.getCommand());
                } else {
                    pending.promise.completeExceptionally(new Exception("EtherNet/IP status: " + status));
                }
            } else {
                config.getLoggingContext().forEach(MDC::put);
                try {
                    logger.debug("Received response for unknown context: {}", packet.getSenderContext());
                } finally {
                    config.getLoggingContext().keySet().forEach(MDC::remove);
                }

                if (packet.getCommand() instanceof SendRRData) {
                    CpfPacket cpfPacket = ((SendRRData) packet.getCommand()).getPacket();

                    Arrays.stream(cpfPacket.getItems()).forEach(item -> {
                        if (item instanceof ConnectedDataItemResponse) {
                            ReferenceCountUtil.safeRelease(((ConnectedDataItemResponse) item).getData());
                        } else if (item instanceof UnconnectedDataItemResponse) {
                            ReferenceCountUtil.safeRelease(((UnconnectedDataItemResponse) item).getData());
                        }
                    });
                }
            }
        }
    }

    private void onChannelInactive(ChannelHandlerContext ctx) {
        config.getLoggingContext().forEach(MDC::put);
        try {
            logger.debug("onChannelInactive() {} <-> {}",
                ctx.channel().localAddress(), ctx.channel().remoteAddress());
        } finally {
            config.getLoggingContext().keySet().forEach(MDC::remove);
        }
    }

    private void onExceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws InterruptedException {
        config.getLoggingContext().forEach(MDC::put);
        try {
            logger.debug("onExceptionCaught() {} <-> {}",
                ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);
        } finally {
            config.getLoggingContext().keySet().forEach(MDC::remove);
        }
         Thread.sleep(1000);
        ctx.channel().close();
    }

    /**
     * Subclasses can override this to handle incoming
     * {@link com.digitalpetri.enip.commands.SendUnitData} commands.
     *
     * @param command the {@link com.digitalpetri.enip.commands.SendUnitData} command received.
     */
    protected void onUnitDataReceived(SendUnitData command) {}

    private static final class EtherNetIpServerHandler extends SimpleChannelInboundHandler<EnipPacket> {

        private final ExecutorService executor;

        private final EtherNetIpServer server;

        private EtherNetIpServerHandler(EtherNetIpServer server) {
            this.server = server;

            executor = server.getExecutor();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, EnipPacket packet) {
            executor.execute(() -> server.onChannelRead(channelHandlerContext, packet));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            server.onChannelInactive(ctx);

            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            server.onExceptionCaught(ctx, cause);

            super.exceptionCaught(ctx, cause);
        }

    }

    private static final class PendingRequest<T> {

        private final CompletableFuture<Command> promise = new CompletableFuture<>();

        private final Timeout timeout;

        @SuppressWarnings("unchecked")
        private PendingRequest(CompletableFuture<T> future, Timeout timeout) {
            this.timeout = timeout;

            promise.whenComplete((r, ex) -> {
                if (r != null) {
                    try {
                        future.complete((T) r);
                    } catch (ClassCastException e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    future.completeExceptionally(ex);
                }
            });
        }

    }

}
