import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private ServerSocket serverSocket;
    private final Map<String, FileInfo> fileIndex;
    private final int replicationFactor;
    private final int timeout;
    private final List<Integer> previousPorts;
    private final int rebalancePeriod;
    private final Map<Integer, DstoreInfo> dstorePorts;
    
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

        fileIndex = new HashMap<>();
        previousPorts = new ArrayList<>();
        dstorePorts = new HashMap<>();

        try {
            serverSocket = new ServerSocket(cport);
            log("Controller server started on port " + cport);

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(this::rebalance, rebalancePeriod, rebalancePeriod, TimeUnit.MILLISECONDS);

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

    private String listenToDstoreMessage(int port) throws SocketTimeoutException {
        try {
            dstorePorts.get(port).getSocket().setSoTimeout(timeout);

            String message = dstorePorts.get(port).getReader().readLine();
            if (message == null) throw new SocketTimeoutException();

            log("Dstore (" + port + ") TCP message received: " + message);
            return message;
        } catch (SocketTimeoutException e) {
            throw new SocketTimeoutException();
        } catch (IOException e) {
            throw new SocketTimeoutException();
        }
    }

    private void join(int port) {
        try {
            InetAddress address = InetAddress.getLocalHost();
            Socket socket = new Socket(address, port);
            log("Connection established on port " + port);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            DstoreInfo info = new DstoreInfo(port, socket, out, in);
            dstorePorts.put(port, info);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        rebalance();
    }

    private List<Integer> selectRDstores() {
        DstoreInfo[] dstores = dstorePorts.values().toArray(new DstoreInfo[0]);
        Arrays.sort(dstores);

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

            new Thread(() -> {
                try {
                    String message = listenToDstoreMessage(port);

                    if (message.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
                        info.setFileCount(info.getFileCount() + 1);
                        inc.getAndIncrement();
                    }

                    if (inc.get() == replicationFactor) {
                        fileIndex.get(fileName).setStatus(FileInfo.Status.STORE_COMPLETE);
                        sendClientMessage(out, Protocol.STORE_COMPLETE_TOKEN);
                    }
                } catch (SocketTimeoutException e) {
                    log("Request timed out");
                }
            }).start();
        }

        sendClientMessage(out, Protocol.STORE_TO_TOKEN + usedPortString);

        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

            try {
                String message = listenToDstoreMessage(port);

                if (message.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName) || message.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName)) {
                    inc.getAndIncrement();

                    if (inc.get() == ports.size()) {
                        fileIndex.remove(fileName);
                        sendClientMessage(out, Protocol.REMOVE_COMPLETE_TOKEN);
                    }
                }
            } catch (SocketTimeoutException e) {

            }
        }
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
        List<Integer> storesToRemove = new ArrayList<>();
        for (int port : dstorePorts.keySet()) {
            try {
                sendDstoreMessage(port, "CHECK");
                listenToDstoreMessage(port);
            } catch (SocketTimeoutException e) {
                log("GONE");
                storesToRemove.add(port);
                for (var info : fileIndex.values()) {
                    if (info.getDstorePorts().contains(port)) {
                        info.getDstorePorts().remove(port);
                    }
                }
            }
        }

        for (int port : storesToRemove) {
            dstorePorts.remove(port);
        }
    }

    private Integer getUnbalancedPort(int minimum, int maximum, Map<Integer, List<String>> ports) {
        for (var entry : ports.entrySet()) {
            int port = entry.getKey();
            int fileCount = entry.getValue().size();

            System.out.println("count: " + fileCount);
            System.out.println("min: " + minimum);
            System.out.println("max: " + maximum);

            if (fileCount < minimum || fileCount > maximum) return port;
        }

        return null;
    }

    boolean rebalancing = false;

    private void rebalance() {
        log("Rebalance requested");

        if (rebalancing) {
            log("Rebalance already in progress");
            return;
        } else {
            log("Rebalance started");
            rebalancing = true;
        }

        while (true) {
            boolean processed = true;

            for (String fileName : fileIndex.keySet()) {
                if (storeInProgress(fileName) || removeInProgress(fileName)) {
                    processed = false;
                    break;
                }
            }

            if (processed) break;
        }

        checkAliveStores();

        Map<String, List<Integer>> filesStored = new HashMap<>();

        List<Integer> ports = new ArrayList<>();

        int N = dstorePorts.size();
        int R = replicationFactor;
        int F = fileIndex.size();

        double threshold = (double) (R * F) / N;
        int minimum = (int) Math.floor(threshold);
        int maximum = (int) Math.ceil(threshold);

        Map<Integer, List<String>> currents = new HashMap<>();

        Map<Integer, Map<String, List<Integer>>> sends = new HashMap<>();
        Map<Integer, List<String>> removals = new HashMap<>();

        dstorePorts.forEach((port, info) -> {
            ports.add(port);
            sendDstoreMessage(port, Protocol.LIST_TOKEN);
            try {
                String message = listenToDstoreMessage(port);

                String[] args = message.split(" ");
                if (args[0].equals(Protocol.LIST_TOKEN)) {
                    sends.put(port, new HashMap<>());
                    removals.put(port, new ArrayList<>());

                    List<String> fileNames = List.of(Arrays.copyOfRange(args, 1, args.length));
                    currents.put(port, fileNames);
                    for (var fileName : fileNames) {
                        if (!filesStored.containsKey(fileName)) filesStored.put(fileName, new ArrayList<>());
                        filesStored.get(fileName).add(port);
                    }
                }
            } catch (SocketTimeoutException e) {

            }
        });

        Map<String, Integer> unbalancedFiles = new HashMap<>();

        filesStored.forEach((fileName, storedPorts) -> {
            int count = storedPorts.size();

            if (count < replicationFactor) {
                unbalancedFiles.put(fileName, replicationFactor - count);
            }
        });

        AtomicBoolean changed = new AtomicBoolean(false);

        unbalancedFiles.forEach((fileName, numberNeeded) -> {
            int takingFrom = filesStored.get(fileName).getFirst();

            sends.get(takingFrom).put(fileName, new ArrayList<>());

            for (int i = 0; i < numberNeeded; i++) {
                Integer newPort = findLowestNotContaining(fileName, currents);

                if (newPort != null) {
                    changed.set(true);

                    sends.get(takingFrom).get(fileName).add(newPort);
                    currents.get(newPort).add(fileName);
                    filesStored.get(fileName).add(newPort);
                }
            }
        });

        Integer unbalancedPort;
        System.out.println(getUnbalancedPort(minimum, maximum, currents));
        while ((unbalancedPort = getUnbalancedPort(minimum, maximum, currents)) != null) {
            changed.set(true);
            ports.sort(Comparator.comparingInt(port -> currents.get(port).size()));

            if (currents.get(unbalancedPort).size() > maximum) {
                String chosenFile = null;
                int minimumPort = unbalancedPort;
                int minimumCount = Integer.MAX_VALUE;
                for (String fileName : currents.get(unbalancedPort)) {
                    Integer port = findLowestNotContaining(fileName, currents);

                    if (port == null) continue;

                    int fileCount = currents.get(port).size();
                    if (fileCount < minimumCount) {
                        minimumPort = port;
                        minimumCount = fileCount;
                        chosenFile = fileName;
                    }
                }

                if (minimumPort != unbalancedPort) {
                    if (!sends.get(unbalancedPort).containsKey(chosenFile)) sends.get(unbalancedPort).put(chosenFile, new ArrayList<>());

                    sends.get(unbalancedPort).get(chosenFile).add(minimumPort);

                    removals.get(unbalancedPort).add(chosenFile);

                    filesStored.get(chosenFile).remove(unbalancedPort);
                    filesStored.get(chosenFile).add(minimumPort);
                }
            } else {
                List<String> fileList = currents.get(unbalancedPort);
                Integer takingFrom = findHighestNotContainingSome(fileList, currents);

                if (takingFrom != null) {
                    for (String fileName : currents.get(takingFrom)) {
                        if (!fileList.contains(fileName)) {
                            if (!sends.get(unbalancedPort).containsKey(fileName)) sends.get(unbalancedPort).put(fileName, new ArrayList<>());

                            sends.get(takingFrom).get(fileName).add(unbalancedPort);

                            removals.get(takingFrom).add(fileName);

                            filesStored.get(fileName).remove(takingFrom);
                            filesStored.get(fileName).add(unbalancedPort);

                            break;
                        }
                    }
                }
            }
        }

        if (changed.get()) {
            for (int port : ports) {
                StringBuilder request = new StringBuilder("REBALANCE_STORE ");

                Map<String, List<Integer>> sendingTo = sends.get(port);
                request.append(sendingTo.size());
                for (var entry : sendingTo.entrySet()) {
                    String fileName = entry.getKey();
                    List<Integer> sendingToPorts = entry.getValue();

                    request.append(" ").append(fileName).append(" ").append(sendingToPorts.size());

                    for (int sendingToPort : sendingToPorts) {
                        dstorePorts.get(sendingToPort).setFileCount(dstorePorts.get(sendingToPort).getFileCount() + 1);
                        request.append(" ").append(sendingToPort);
                    }
                }

                List<String> storeRemovals = removals.get(port);
                request.append(" ").append(storeRemovals.size());
                for (var fileName : storeRemovals) {
                    request.append(" ").append(fileName);
                }

                sendDstoreMessage(port, request.toString());

                try {
                    String message = listenToDstoreMessage(port);

                    if (message.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                        dstorePorts.get(port).setFileCount(currents.get(port).size());
                    }
                } catch (SocketTimeoutException e) {

                }
            }

            for (var entry : filesStored.entrySet()) {
                String fileName = entry.getKey();
                List<Integer> storedPorts = entry.getValue();

                fileIndex.get(fileName).setDstorePorts(storedPorts);
            }
        } else {
            log("No rebalance needed");
        }

        log("Rebalance completed");
        rebalancing = false;
    }

    private Integer findLowestNotContaining(String fileName, Map<Integer, List<String>> ports) {
        List<Integer> portsNotContaining = new ArrayList<>();

        ports.forEach((port, fileList) -> {
            if (!fileList.contains(fileName)) {
                portsNotContaining.add(port);
            }
        });

        portsNotContaining.sort(Comparator.comparingInt(port -> ports.get(port).size()));

        return portsNotContaining.getFirst();
    }

    private Integer findHighestNotContainingSome(List<String> fileNames, Map<Integer, List<String>> ports) {
        List<Integer> portsNotContaining = new ArrayList<>();

        ports.forEach((port, fileList) -> {
            for (var fileName : fileList) {
                if (!fileList.contains(fileName)) {
                    portsNotContaining.add(port);
                }
                break;
            }
        });

        portsNotContaining.sort(Comparator.comparingInt(port -> ports.get(port).size()));

        return portsNotContaining.getLast();
    }

    public static void main(String[] args) {
        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
    }

    private class ServiceThread implements Runnable {
        Socket socket;
        ServiceThread(Socket s) {
            socket = s;
        }
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
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
                socket.close();
            } catch(Exception e) {
                error(e.toString());
            }
        }
    }
}