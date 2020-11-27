package deview2020.requestscoping;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

/**
 * NB: This server is greatly simplified to show the concept of asynchronous server, so you shouldn't use this
 *     in production. You might want to use Netty or Armeria if you are using HTTP protocol.
 *
 * The server which works asynchronously. The protocol is:
 * <pre>{@code
 * -----------------------------------------
 * | length(4bytes) | body(variable)       |
 * -----------------------------------------
 * }</pre>
 * the length has a 32bit integer value which indicates the size of the whole packet.
 */
public final class AsyncServer {

    public static void main(String[] args) throws IOException {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(8080));

        final Selector selector = Selector.open();
        new EventLoop(selector).start();
        for (;;) {
            final SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private static class EventLoop extends Thread implements Executor {

        private final Selector selector;
        private final HelloService service;
        private final Queue<Runnable> jobs = new ConcurrentLinkedQueue<>();

        EventLoop(Selector selector) {
            this.selector = selector;
            service = new HelloService();
        }

        @Override
        public void run() {
            for (;;) {
                try {
                    final int keys = selector.select(500);
                    if (keys > 0) {
                        final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                        for (SelectionKey selectionKey : selectionKeys) {
                            if (selectionKey.isReadable()) {
                                final SocketChannel channel = (SocketChannel) selectionKey.channel();
                                final ByteBuffer request = readRequest(channel);
                                if (request == null) {
                                    channel.close();
                                    continue;
                                }
                                final CompletableFuture<ByteBuffer> future = service.serve(request, this);
                                future.thenAcceptAsync(response -> writeResponse(channel, response), this);
                            }
                        }
                    }
                    runJobs();
                } catch (IOException e) {
                    System.err.println("Unexpected exception while selecting. " + e);
                }
            }
        }

        private void runJobs() {
            for (;;) {
                final Runnable work = jobs.poll();
                if (work == null) {
                    return;
                }
                work.run();
            }
        }

        @Nullable
        private static ByteBuffer readRequest(SocketChannel channel) throws IOException {
            final ByteBuffer sizeBuffer = readBytes(channel, 4);
            if (sizeBuffer == null) {
                return null;
            }
            return readBytes(channel, sizeBuffer.getInt());
        }

        @Nullable
        private static ByteBuffer readBytes(SocketChannel channel, int size) throws IOException {
            final ByteBuffer bodyBuffer = ByteBuffer.allocate(size);
            int totalRead = 0;
            while (totalRead < size) {
                final int read = channel.read(bodyBuffer);
                if (read < 0) {
                    // The sender violated the protocol so we just return null for simplicity.
                    return null;
                }
                totalRead += read;
            }
            bodyBuffer.flip();
            return bodyBuffer;
        }

        private static void writeResponse(SocketChannel channel, ByteBuffer response) {
            final int total = 4 + response.remaining();
            final ByteBuffer responseBuffer = ByteBuffer.allocate(total);
            responseBuffer.putInt(response.remaining());
            responseBuffer.put(response);
            responseBuffer.flip();
            try {
                int written = channel.write(responseBuffer);
                while (written < total) {
                    written += channel.write(responseBuffer);
                }
            } catch (IOException e) {
                System.err.println("Unexpected exception while writing. " + e);
            }
        }

        @Override
        public void execute(Runnable command) {
            jobs.add(command);
        }
    }

    private static final class HelloService {

        private final ExecutorService blockingExecutor = Executors.newFixedThreadPool(10);

        CompletableFuture<ByteBuffer> serve(ByteBuffer request, EventLoop eventLoop) {
            final String requestBody = StandardCharsets.UTF_8.decode(request).toString();
            final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            eventLoop.execute(() -> {
                // Let's suppose that response is complete later by another event.
                // (e.g. an HTTP call to another backend)
                final ByteBuffer response = StandardCharsets.UTF_8.encode("Hello " + requestBody);
                future.complete(response);
            });

            /* If it's a blocking call, we should use other executor to do the job.
            blockingExecutor.submit(() -> {
                System.err.println("request body: " + requestBody);
                try {
                    // Simulate a blocking call (e.g. send a query to RDB)
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    // ignored
                }
                final ByteBuffer response = StandardCharsets.UTF_8.encode("Hello " + requestBody);
                future.complete(response);
            });
            */
            return future;
        }
    }
}
