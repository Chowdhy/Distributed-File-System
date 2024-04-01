import java.io.BufferedReader;
import java.io.PrintWriter;

public class DstoreInfo {
    int port;
    DstoreListener listener;
    PrintWriter out;
    BufferedReader in;
    int fileCount;

    interface DstoreListener {
        void messageReceived(String message);
    }

    public DstoreInfo (int port, PrintWriter out, BufferedReader in) {
        this.port = port;
        this.out = out;
        this.in = in;
        fileCount = 0;
    }

    public void setOnMessageReceived(DstoreListener listener) {
        this.listener = listener;
    }

    public void removeListener() {
        this.listener = null;
    }

    public void writeLine(String message) {
        out.println(message);
    }

    public void fire(String message) {
        System.out.println("REACHED INFO CLASS");
        listener.messageReceived(message);
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
}