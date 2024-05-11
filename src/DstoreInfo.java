import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class DstoreInfo implements Comparable<DstoreInfo> {
    int port;
    Socket socket;
    PrintWriter out;
    BufferedReader in;
    AtomicInteger fileCount;

    @Override
    public int compareTo(DstoreInfo dstoreInfo) {
        return Integer.compare(this.getFileCount(), dstoreInfo.getFileCount());
    }

    public DstoreInfo (int port, Socket socket, BufferedReader in) throws IOException {
        this.port = port;
        this.socket = socket;

        out = new PrintWriter(socket.getOutputStream(), true);
        this.in = in;

        fileCount = new AtomicInteger(0);
    }

    public void writeLine(String message) {
        out.println(message);
    }

    public int getFileCount() {
        return fileCount.get();
    }

    public void setFileCount(int fileCount) {
        this.fileCount.set(fileCount);
    }
    public void incrementFileCount() {
        this.fileCount.incrementAndGet();
    }

    public void decrementFileCount() {
        this.fileCount.decrementAndGet();
    }

    public int getPort() {
        return port;
    }

    public Socket getSocket() {
        return socket;
    }

    public String waitForMessage(int timeout) {
        try {
            socket.setSoTimeout(timeout);
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }
}