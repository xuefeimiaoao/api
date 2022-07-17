package netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.SystemUtils;

import java.net.InetSocketAddress;

public class MyNettyServer {

    public static void main(String[] args) throws InterruptedException {
        initServer();

    }

    public static void initServer() throws InterruptedException {
        // If true, we will prefer allocating off-heap byte buffers within Netty.
        Boolean preferDirectBufs = true;

        ChannelFuture channelFuture;
        String hostToBind = "127.0.0.1";
        int portToBind = 16668;

        IOMode ioMode = IOMode.valueOf("NIO");
//        EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, 1,
//                "module-test" + "-boss");
//        EventLoopGroup workerGroup =  NettyUtils.createEventLoop(ioMode, 0,
//                "module-test" + "-server");
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {


/*            PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
                    preferDirectBufs, true *//* allowCache *//*, 0);*/

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
//                    .option(ChannelOption.ALLOCATOR, allocator)
//                    .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
//                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)  // 设置保持活动连接状态
//                    .childOption(ChannelOption.TCP_NODELAY, true)
//                    .childOption(ChannelOption.ALLOCATOR, allocator)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new TestAutoReadHandler());
                            socketChannel.pipeline().addLast(new NettyServerHandler());
//                            socketChannel.pipeline().addLast(new NettyServerHandler2());
                        }
                    });


            InetSocketAddress address = new InetSocketAddress(portToBind);
            channelFuture = bootstrap.bind(address).sync();
            System.out.println("server is ready");
            // 等待服务端口的关闭
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            throw e;
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }
}
