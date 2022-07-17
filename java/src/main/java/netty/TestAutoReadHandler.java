package netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.internal.ConcurrentSet;

public class TestAutoReadHandler extends ChannelInboundHandlerAdapter {
    ConcurrentSet<Channel> channels = new ConcurrentSet<>();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[DEBUG]TestAutoReadHandler channel active!");
        pauseAllChannels();
        super.channelActive(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[DEBUG]TestAutoReadHandler handler added!");
        channels.add(ctx.channel());
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[DEBUG]TestAutoReadHandler handler removed!");
        if (!ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(true);
        }
        channels.remove(ctx.channel());
        super.handlerRemoved(ctx);
    }


    private void pauseAllChannels() {
        channels.stream().forEach(c -> {
            if (c.config().isAutoRead()) {
                System.out.println("Worker memory level is critical, channel : " + c + " stop receive data.");
                c.config().setAutoRead(false);
            }
        });
    }
}
