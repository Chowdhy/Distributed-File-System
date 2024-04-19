import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class DstoreInfo implements Comparable<DstoreInfo> {
    int port;
    Socket socket;
    PrintWriter out;
    BufferedReader in;
    int fileCount;
    List<DstoreMessageListener> listeners;

    @Override
    public int compareTo(DstoreInfo dstoreInfo) {
        return Integer.compare(this.getFileCount(), dstoreInfo.getFileCount());
    }

    public DstoreInfo (int port, Socket socket) throws IOException {
        this.port = port;
        this.socket = socket;

        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        listeners = new ArrayList<>();

        fileCount = 0;

        new Thread(() -> {
            try {
                listenToMessages();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void listenToMessages() throws IOException {
        try {
            String message;
            while ((message = in.readLine()) != null) {
                for (var listener : List.copyOf(listeners)) {
                    listener.messageReceived(message);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (socket != null) socket.close();
        }
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

    public void addListener(DstoreMessageListener listener) {
        listeners.add(listener);
    }

    public void removeListener(DstoreMessageListener listener) {
        listeners.remove(listener);
    }
    public boolean hasListener(DstoreMessageListener listener) {
        return listeners.contains(listener);
    }

    public String waitForMessage(String commandWord, int timeout) throws TimeoutException {
        AtomicReference<String> receivedMessage = new AtomicReference<>();

        DstoreMessageListener listener = (message -> {
            if (message.split(" ")[0].equals(commandWord)) {
                receivedMessage.set(message);
            }
        });

        Future<String> stringFuture = Executors.newSingleThreadScheduledExecutor().submit(() -> {
            addListener(listener);
            while (receivedMessage.get() == null) {

            }
            removeListener(listener);
            return receivedMessage.get();
        });

        try {
            return stringFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new TimeoutException(e.toString());
        } finally {
            if (hasListener(listener)) removeListener(listener);
        }
    }

    public boolean isAlive() {
        return !socket.isClosed();
    }

    interface DstoreMessageListener {
        void messageReceived(String message);
    }
}