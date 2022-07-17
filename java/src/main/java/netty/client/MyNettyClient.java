package netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import netty.NettyUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MyNettyClient {

    public Channel channel = null;
    private MockTransportResponseHandler handler = new MockTransportResponseHandler();

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        MyNettyClient client = new MyNettyClient();
        client.init();
    }

    public void init() throws InterruptedException, ExecutionException, TimeoutException {
        NioEventLoopGroup eventExecutors = new NioEventLoopGroup();

        try {

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new MyNettyClientHandler(handler));
                            socketChannel.pipeline().addLast(handler);
                        }
                    });

            InetSocketAddress address = new InetSocketAddress("localhost", 16668);
            ChannelFuture channelFuture = bootstrap.connect(address).sync();
            channel = channelFuture.channel();


            ChannelFuture pushFuture = pushMergedData(" this is mocked shuffle data", new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    System.out.println("#######################client push data successfully finally!####################");
                }

                @Override
                public void onFailure(Throwable e) {
                    System.err.println("push data(mocked) failed!");
                }
            });

            pushFuture.get(10, TimeUnit.SECONDS);
            channelFuture.channel().closeFuture().sync();

        } catch (Exception e) {
            throw e;
        } finally {
            eventExecutors.shutdownGracefully();
        }
    }

    public ChannelFuture pushMergedData(String pushMergedData, RpcResponseCallback callback) {
       System.out.printf("Pushing merged data to {}", NettyUtils.getRemoteAddress(channel));

        long requestId = 1;
        handler.addRpcRequest(requestId, callback);

//        pushMergedData.requestId = requestId;

        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        return channel.writeAndFlush(Unpooled.copiedBuffer(pushMergedData, CharsetUtil.UTF_8))
                .addListener(listener);
    }

    public class StdChannelListener
            implements GenericFutureListener<Future<? super Void>> {
        final long startTime;
        final Object requestId;

        public StdChannelListener(Object requestId) {
            this.startTime = System.currentTimeMillis();
            this.requestId = requestId;
        }

        public StdChannelListener() {
            this.startTime = System.currentTimeMillis();
            this.requestId = null;
        }

        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
                long timeTaken = System.currentTimeMillis() - startTime;
                System.out.printf("Sending request {} to {} took {} ms", requestId,
                        NettyUtils.getRemoteAddress(channel), timeTaken);
            } else {
                String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                        NettyUtils.getRemoteAddress(channel), future.cause());
                System.err.println(errorMsg);
                channel.close();
                try {
                    handleFailure(errorMsg, future.cause());
                } catch (Exception e) {
                    System.err.println("Uncaught exception in RPC response callback handler!" + e);
                }
            }
        }

        protected void handleFailure(String errorMsg, Throwable cause) throws Exception {}
    }

    private class RpcChannelListener extends StdChannelListener {
        final long rpcRequestId;
        final RpcResponseCallback callback;

        RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
            super("RPC " + rpcRequestId);
            this.rpcRequestId = rpcRequestId;
            this.callback = callback;
        }

        @Override
        protected void handleFailure(String errorMsg, Throwable cause) {
            callback.onFailure(new IOException(errorMsg, cause));
        }
    }
}
