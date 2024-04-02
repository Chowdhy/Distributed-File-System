import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class DstoreInfo implements Comparable<DstoreInfo> {
    int port;
    Socket socket;
    PrintWriter out;
    BufferedReader in;
    int fileCount;

    @Override
    public int compareTo(DstoreInfo dstoreInfo) {
        return Integer.compare(this.getFileCount(), dstoreInfo.getFileCount());
    }

    public DstoreInfo (int port, Socket socket, PrintWriter out, BufferedReader in) {
        this.port = port;
        this.socket = socket;
        this.out = out;
        this.in = in;
        fileCount = 0;
    }

    public void writeLine(String message) {
        out.println(message);
    }

    public BufferedReader getReader() {
        return in;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public int getPort() {
        return port;
    }

    public Socket getSocket() {
        return socket;
    }
}