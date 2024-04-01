import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private ServerSocket serverSocket;
    private final Map<String, FileInfo> fileIndex = new HashMap<>();
    private final int replicationFactor;
    private final int timeout;
    private final List<Integer> previousPorts = new ArrayList<>();
    private final int rebalancePeriod;
    private final Map<Integer, DstoreInfo> dstorePorts = new HashMap<>();
    
    private void log(String message) {
        System.out.println("[CONTROLLER] " + message);
    }
    
    private void error(String message) {
        System.err.println("[CONTROLLER] Error: " + message);
    }

    public Controller(int cport, int replicationFactor, int timeout, int rebalancePeriod) {
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        try {
            serverSocket = new ServerSocket(cport);
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(new ServiceThread(socket)).start();
                } catch (Exception e) {
                    error(e.toString());
                }
            }
        } catch (Exception e) {
            error(e.toString());
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    error(e.toString());
                }
            }
        }
    }

    private void sendClientMessage(PrintWriter out, String message) {
        out.println(message);
        log("Client TCP message sent: " + message);
    }

    private void sendDstoreMessage(int port, String message) {
        dstorePorts.get(port).writeLine(message);
        log("Dstore (" + port + ") TCP message sent: " + message);
    }

    private void join(int port) {
        try {
            InetAddress address = InetAddress.getLocalHost();
            Socket socket = new Socket(address, port);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            DstoreInfo info = new DstoreInfo(port, out, in);
            dstorePorts.put(port, info);

            String line;
            while((line = in.readLine()) != null) {
                info.fire(line);
                log("Dstore (" + port + ") TCP message received: " + line);
            }
            //socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Integer> selectRDstores() {
        DstoreInfo[] dstores = dstorePorts.values().toArray(new DstoreInfo[0]);
        Arrays.sort(dstores, Comparator.comparingInt(DstoreInfo::getFileCount));

        List<Integer> selected = new ArrayList<>();

        for (int i = 0; i < replicationFactor; i++) {
           selected.add(dstores[i].getPort());
        }

        return selected;
    }

    private boolean fileExists(String fileName) {
        return fileIndex.containsKey(fileName);
    }

    private boolean storeInProgress(String fileName) {
        if (!fileExists(fileName)) return false;
        return fileIndex.get(fileName).getStatus().equals(FileInfo.Status.STORE_IN_PROGRESS);
    }

    private boolean removeInProgress(String fileName) {
        if (!fileExists(fileName)) return false;
        return fileIndex.get(fileName).getStatus().equals(FileInfo.Status.REMOVE_IN_PROGRESS);
    }

    private void store(PrintWriter out, String fileName, int fileSize) {
        if (fileExists(fileName) || storeInProgress(fileName) || removeInProgress(fileName)) {
            sendClientMessage(out, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }

        List<Integer> usedPorts = selectRDstores();
        StringBuilder usedPortString = new StringBuilder();

        AtomicInteger inc = new AtomicInteger();

        fileIndex.put(fileName, new FileInfo(fileSize, usedPorts));

        for (var port : usedPorts) {
            usedPortString.append(" ").append(port);
            DstoreInfo info = dstorePorts.get(port);
            info.setOnMessageReceived(message -> {
                if (message.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
                    info.setFileCount(info.getFileCount() + 1);
                    inc.getAndIncrement();
                }

                if (inc.get() == replicationFactor) {
                    fileIndex.get(fileName).setStatus(FileInfo.Status.STORE_COMPLETE);
                    sendClientMessage(out, Protocol.STORE_COMPLETE_TOKEN);
                }
            });
        }

        sendClientMessage(out, Protocol.STORE_TO_TOKEN + usedPortString);

        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                for (int port : usedPorts) {
                    dstorePorts.get(port).removeListener();
                }

                if (inc.get() != replicationFactor) {
                    fileIndex.remove(fileName);
                }
            }
        };

        timer.schedule(task, timeout);
    }

    private void load(PrintWriter out, String fileName) {
        if (!fileExists(fileName) || storeInProgress(fileName) || removeInProgress(fileName)) {
            sendClientMessage(out, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        FileInfo info = fileIndex.get(fileName);
        int fileSize = info.getSize();

        for (int port : info.getDstorePorts()) {
            if (!previousPorts.contains(port)) {
                previousPorts.add(port);
                sendClientMessage(out, Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize);
                return;
            }
        }

        sendClientMessage(out, Protocol.ERROR_LOAD_TOKEN);
    }

    private void remove(PrintWriter out, String fileName) {
        if (!fileExists(fileName) || storeInProgress(fileName) || removeInProgress(fileName)) {
            sendClientMessage(out, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        FileInfo info = fileIndex.get(fileName);
        info.setStatus(FileInfo.Status.REMOVE_IN_PROGRESS);
        List<Integer> ports = info.getDstorePorts();
        AtomicInteger inc = new AtomicInteger();

        for (int port : ports) {
            sendDstoreMessage(port, Protocol.REMOVE_TOKEN + " " + fileName);

            dstorePorts.get(port).setOnMessageReceived(message -> {
                if (message.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName) || message.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName)) {
                    inc.getAndIncrement();

                    if (inc.get() == ports.size()) {
                        fileIndex.remove(fileName);
                        sendClientMessage(out, Protocol.REMOVE_COMPLETE_TOKEN);
                    }
                }
            });
        }

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                for (int port : ports) {
                    dstorePorts.get(port).removeListener();
                }
            }
        };

        timer.schedule(task, timeout);
    }

    private void list(PrintWriter out) {
        StringBuilder message = new StringBuilder("LIST");
        for (Map.Entry<String, FileInfo> entry : fileIndex.entrySet()) {
            if (entry.getValue().getStatus().equals(FileInfo.Status.STORE_COMPLETE)) {
                message.append(" ").append(entry.getKey());
            }
        }
        sendClientMessage(out, message.toString());
    }

    private void checkAliveStores() {
        for (int port : dstorePorts.keySet()) {
            try {
                sendDstoreMessage(port, "CHECK");
            } catch (Exception e) {
                dstorePorts.remove(port);
                for (var info : fileIndex.values()) {
                    if (info.getDstorePorts().contains(port)) {
                        info.getDstorePorts().remove(port);
                    }
                }
            }
        }
    }

    private Map<String, Integer> getUnbalancedFiles() {
        Map<String, Integer> unbalanced = new HashMap<>();
        for (Map.Entry<String, FileInfo> entry : fileIndex.entrySet()) {
            int count = entry.getValue().getDstorePorts().size();
            if (count < replicationFactor) {
                unbalanced.put(entry.getKey(), replicationFactor - count);
            }
        }
        return unbalanced;
    }

    private void rebalance() {
        Map<String, Integer> filesStored = new HashMap<>();

        int N = dstorePorts.size();
        int R = replicationFactor;
        int F = fileIndex.size();

        double threshold = (double) (R * F) / N;
        int minimum = (int) Math.floor(threshold);
        int maximum = (int) Math.ceil(threshold);

        Map<Integer, String[]> currents = new HashMap<>();
        Map<Integer, List<String>> overs = new HashMap<>();
        Map<Integer, Integer> unders = new HashMap<>();

        int oversum = 0;
        AtomicInteger undersum = new AtomicInteger();

        dstorePorts.forEach((port, info) -> {
            DstoreInfo.DstoreListener listener = message -> {
                String[] args = message.split(" ");
                if (args[0].equals(Protocol.LIST_TOKEN)) {
                    String[] fileNames = Arrays.copyOfRange(args, 1, args.length);
                    currents.put(port, fileNames);
                    for (var fileName : fileNames) {
                        if (!filesStored.containsKey(fileName)) {
                            filesStored.put(fileName, 1);
                        } else {
                            filesStored.put(fileName, filesStored.get(fileName) + 1);
                        }
                    }
                }
            };
            info.setOnMessageReceived(listener);
        });

        Map<String, Integer> unbalancedFiles = new HashMap<>();

        filesStored.forEach((fileName, count) -> {
            if (count < replicationFactor) {
                unbalancedFiles.put(fileName, count);
            }
        });

        currents.forEach((port, fileNames) -> {
            if (fileNames.length < minimum) {
                unders.put(port, minimum - fileNames.length);
                undersum.set(undersum.get() + (minimum - fileNames.length));
            } else if (fileNames.length > maximum) {
                overs.put(port, List.of(fileNames));
            }
        });

        for (int port : overs.keySet()) {
            List<String> fileNames = overs.get(port);
            while (fileNames.size() > maximum) {

            }
        }

        /*
        if (oversum > undersum) {
            int difference = oversum - undersum;

            for (Map.Entry<Integer, Integer> entry : currents.entrySet()) {
                int port = entry.getKey();
                int current = entry.getValue();
                if (minimum <= current && current < maximum) {
                    unders.put(port, maximum - current);
                    difference = difference - (maximum - current);
                }
                if (difference == 0) break;
            }
        } else if (undersum > oversum) {
            int difference = undersum - oversum;

            for (Map.Entry<Integer, Integer> entry : currents.entrySet()) {
                int port = entry.getKey();
                int current = entry.getValue();
                if (minimum < current && current <= maximum) {
                    overs.put(port, current - minimum);
                    difference = difference - (current - minimum);
                }
                if (difference == 0) break;
            }
        }*/
    }

    public static void main(String[] args) {
        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
    }

    private class ServiceThread implements Runnable {
        Socket client;
        ServiceThread(Socket c) {
            client=c;
        }
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                String line;
                while((line = in.readLine()) != null) {
                    log("TCP message received: " + line);

                    String[] cmd = line.split(" ");

                    if (!cmd[0].equals(Protocol.RELOAD_TOKEN)) previousPorts.clear();

                    switch (cmd[0]) {
                        case Protocol.JOIN_TOKEN:
                            join(Integer.parseInt(cmd[1]));
                            break;

                        case Protocol.STORE_TOKEN:
                            store(out, cmd[1], Integer.parseInt(cmd[2]));
                            break;

                        case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN:
                            load(out, cmd[1]);
                            break;

                        case Protocol.REMOVE_TOKEN:
                            remove(out, cmd[1]);
                            break;

                        case Protocol.LIST_TOKEN:
                            list(out);
                            break;
                    }
                }
                client.close();
            } catch(Exception e) {
                error(e.toString());
            }
        }
    }
}