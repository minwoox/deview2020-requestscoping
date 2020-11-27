package deview2020.requestscoping;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class BlockingClient {

    public static void main(String[] args) throws IOException {
        final Socket socket = new Socket("127.0.0.1", 8080);
        System.err.println("Connected");
        try {
            final OutputStream outputStream = socket.getOutputStream();
            final String name = "Armeria";
            final byte[] request = name.getBytes(StandardCharsets.UTF_8);

            final byte[] size = ByteBuffer.allocate(4).putInt(request.length).array();
            outputStream.write(size);
            outputStream.write(request);
            outputStream.flush();

            final InputStream inputStream = socket.getInputStream();
            final ByteBuffer responseSizeBuffer = ByteBuffer.allocate(4);
            inputStream.read(responseSizeBuffer.array());
            final int responseSize = responseSizeBuffer.getInt();
            final ByteBuffer response = ByteBuffer.allocate(responseSize);
            inputStream.read(response.array());
            System.err.println("Received response: " + StandardCharsets.UTF_8.decode(response));
            socket.close();
        } catch (IOException e) {
            System.err.println("Unexpected exception while sending and receiving. " + e);
        }
    }
}
