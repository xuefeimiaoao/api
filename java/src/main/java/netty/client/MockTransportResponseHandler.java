package netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MockTransportResponseHandler extends ChannelInboundHandlerAdapter {
    private Map<Long, RpcResponseCallback> outstandingRpcs = new ConcurrentHashMap<>();

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
    private AtomicLong timeOfLastRequestNs = new AtomicLong();

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId, callback);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf data = (ByteBuf) msg;
        String str = data.toString(CharsetUtil.UTF_8);
        handleCallback(str);
    }

    public void handleCallback(String msg) {
        if (msg.contains("push data successfully")) {
            RpcResponseCallback callback = outstandingRpcs.get(1L);
            callback.onSuccess(null);
        }
    }

    /** Updates the time of the last request to the current system time. */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }
}
