package netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[DEBUG]NettyServerHandler channelActive!");
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        super.channelRead(ctx, msg);
        System.out.println("[DEBUG]NettyServerHandler channelRead!");
        System.out.println("server ctx = " + ctx);
        ByteBuf buf = (ByteBuf) msg;
        String msgFromCli = buf.toString(CharsetUtil.UTF_8);
        System.out.println("客户端发送消息是:" + msgFromCli);
        if (msgFromCli.contains("mocked shuffle data")) {
            System.out.println("收到客户端的push data请求:" + msgFromCli);
            ctx.channel().writeAndFlush(Unpooled.copiedBuffer("push data successfully!", CharsetUtil.UTF_8));
        }
        System.out.println("客户端地址:" + ctx.channel().remoteAddress());
    }

    // 数据读取完毕
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        super.channelReadComplete(ctx);
        // 对发送数据进行编码
        System.out.println("[DEBUG]NettyServerHandler channelReadComplete!");
        ctx.writeAndFlush(Unpooled.copiedBuffer("hello 客户端, 服务端已经readComplete！", CharsetUtil.UTF_8));
    }

    // 处理异常

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        super.exceptionCaught(ctx, cause);
        System.out.println("[DEBUG]NettyServerHandler exceptionCaught!");
        ctx.channel().close();
    }
}


class NettyServerHandler2 extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[DEBUG]NettyServerHandler2 channelActive!");
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        super.channelRead(ctx, msg);
        System.out.println("[DEBUG]NettyServerHandler2 channelRead!");
        System.out.println("server ctx = " + ctx);
        ByteBuf buf = (ByteBuf) msg;
        String msgFromCli = buf.toString(CharsetUtil.UTF_8);
        System.out.println("客户端发送消息是:" + msgFromCli);
        if (msgFromCli.contains("mocked shuffle data")) {
            System.out.println("收到客户端的push data请求:" + msgFromCli);
            ctx.channel().writeAndFlush(Unpooled.copiedBuffer("push data successfully!", CharsetUtil.UTF_8));
        }
        System.out.println("客户端地址:" + ctx.channel().remoteAddress());
    }

    // 数据读取完毕
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        super.channelReadComplete(ctx);
        // 对发送数据进行编码
        System.out.println("[DEBUG]NettyServerHandler2 channelReadComplete!");
        ctx.writeAndFlush(Unpooled.copiedBuffer("hello 客户端, 服务端已经readComplete！", CharsetUtil.UTF_8));
    }

    // 处理异常

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        super.exceptionCaught(ctx, cause);
        System.out.println("[DEBUG]NettyServerHandler2 exceptionCaught!");
        ctx.channel().close();
    }
}
