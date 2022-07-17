package netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

public class MyNettyClientHandler extends ChannelInboundHandlerAdapter {

    private MockTransportResponseHandler handler = null;

    public MyNettyClientHandler(MockTransportResponseHandler h) {
        handler = h;
    }
    // 当channel就绪就会触发
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        super.channelActive(ctx);
        System.out.println("[DEBUG]MyNettyClientHandler channelActive!");
        System.out.println("client " + ctx);
        ctx.writeAndFlush(Unpooled.copiedBuffer("hello server", CharsetUtil.UTF_8));
    }

    // 当通道有读取时间时，触发
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        super.channelRead(ctx, msg);
        System.out.println("[DEBUG]MyNettyClientHandler channelRead!");
        ByteBuf buf = (ByteBuf) msg;
        String str = buf.toString(CharsetUtil.UTF_8);
        System.out.println("收到服务器的信息：" + str);
        System.out.println("服务器的地址:" + ctx.channel().remoteAddress());

        handler.handleCallback(str);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        super.exceptionCaught(ctx, cause);
        System.out.println("[DEBUG]MyNettyClientHandler exceptionCaught!");
        ctx.channel().close();
    }
}
