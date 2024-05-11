import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    private ServerSocket serverSocket;
    private final Map<String, FileInfo> fileIndex;
    private final int replicationFactor;
    private final int timeout;
    private final List<Integer> previousPorts;
    private final Map<Integer, DstoreInfo> dstorePorts;
    private boolean isRebalancing = false;
    private final List<Socket> sockets;
    private final AtomicInteger operationsInProgress;

    private void log(Object o) {
        if (o == null) {
            log("null");
            return;
        }

        System.out.println("[CONTROLLER] " + o);
    }
    
    private void error(String message) {
        System.err.println("[CONTROLLER] Error: " + message);
    }

    private void error(Exception e) {
        e.printStackTrace(System.err);
    }

    private void incrementOperations() {
        operationsInProgress.incrementAndGet();
    }

    private void decrementOperations() {
        operationsInProgress.decrementAndGet();

        if (operationsInProgress.get() < 0) {
            operationsInProgress.set(0);
        }
    }

    public Controller(int cport, int replicationFactor, int timeout, int rebalancePeriod) {
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;

        fileIndex = new ConcurrentHashMap<>();
        previousPorts = new ArrayList<>();
        dstorePorts = new ConcurrentHashMap<>();
        sockets = new ArrayList<>();

        operationsInProgress = new AtomicInteger(0);

        try {
            serverSocket = new ServerSocket(cport);
            log("Controller server started on port " + cport);

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(this::rebalance, rebalancePeriod, rebalancePeriod, TimeUnit.SECONDS);

            listenForConnections();
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

    public boolean notEnoughDstores(PrintWriter out) {
        if (dstorePorts.size() < replicationFactor) {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return true;
        } else {
            return false;
        }
    }

    private void listenForConnections() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                log("Connection requested accepted");
                sockets.add(socket);
                new Thread(new ServiceThread(socket)).start();
            } catch (Exception e) {
                error(e.toString());
            }
        }
    }

    private void restartSockets() {
        for (Socket socket : sockets) {
            new Thread(new ServiceThread(socket)).start();
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

    private String waitForDstoreMessage(int port) {
        DstoreInfo info = dstorePorts.get(port);
        String message = info.waitForMessage(timeout);
        if (message == null) {
            log("No message received from port " + port);
        } else {
            log("Message received from port " + port + ": " + message);
        }
        return message;
    }

    private void join(int port, Socket socket, BufferedReader in) {
        try {
            log("Connection established on port " + port);

            DstoreInfo info = new DstoreInfo(port, socket, in);
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
        } else {
            fileIndex.put(fileName, new FileInfo(fileSize, new ArrayList<>()));

            incrementOperations();

            List<Integer> usedPorts = selectRDstores();
            StringBuilder usedPortString = new StringBuilder();

            fileIndex.get(fileName).setDstorePorts(usedPorts);

            CountDownLatch latch = new CountDownLatch(replicationFactor);

            for (var port : usedPorts) {
                usedPortString.append(" ").append(port);
                DstoreInfo info = dstorePorts.get(port);

                new Thread(() -> {
                    String expectedMessage = Protocol.STORE_ACK_TOKEN + " " + fileName;
                    String message = waitForDstoreMessage(port);

                    if (message != null && message.equals(expectedMessage)) {
                        latch.countDown();
                        info.incrementFileCount();
                        log("Received store ack from port " + port);
                    } else if (message == null) {
                        error("Store ack timeout on port " + port);
                    } else {
                        error("Expected '" + expectedMessage + "' from port " + port + ", received '" + message + "'");
                    }
                }).start();
            }

            sendClientMessage(out, Protocol.STORE_TO_TOKEN + usedPortString);

            try {
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    fileIndex.get(fileName).setStatus(FileInfo.Status.STORE_COMPLETE);
                    log("ALL STORE ACKS RECEIVED");
                    sendClientMessage(out, Protocol.STORE_COMPLETE_TOKEN);
                } else {
                    fileIndex.remove(fileName);
                    error("NOT RECEIVED ALL STORE ACKS");
                }
                decrementOperations();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
        } else {
            incrementOperations();

            FileInfo info = fileIndex.get(fileName);
            info.setStatus(FileInfo.Status.REMOVE_IN_PROGRESS);
            List<Integer> ports = info.getDstorePorts();

            CountDownLatch latch = new CountDownLatch(ports.size());

            for (int port : ports) {
                new Thread(() -> {
                    String expectedMessage = Protocol.REMOVE_ACK_TOKEN + " " + fileName;
                    String message = waitForDstoreMessage(port);

                    if (message != null && (message.equals(expectedMessage) || message.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName))) {
                        latch.countDown();
                        if (message.equals(expectedMessage)) {
                            DstoreInfo dInfo = dstorePorts.get(port);
                            dInfo.decrementFileCount();
                        }
                    } else if (message == null) {
                        error("Remove ack timeout on port " + port);
                    } else {
                        error("Expected '" + expectedMessage + "' from port " + port + ", received '" + message + "'");
                    }
                }).start();
                sendDstoreMessage(port, Protocol.REMOVE_TOKEN + " " + fileName);
            }

            try {
                if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    fileIndex.remove(fileName);
                    sendClientMessage(out, Protocol.REMOVE_COMPLETE_TOKEN);
                } else {
                    error("NOT RECEIVED ALL REMOVE ACKS");
                }
                decrementOperations();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void list(PrintWriter out) {
        StringBuilder message = new StringBuilder("LIST");
        for (Map.Entry<String, FileInfo> entry : Set.copyOf(fileIndex.entrySet())) {
            if (entry.getValue().getStatus().equals(FileInfo.Status.STORE_COMPLETE)) {
                message.append(" ").append(entry.getKey());
            }
        }
        sendClientMessage(out, message.toString());
    }

    private void checkAliveStores() {
        List<Integer> storesToRemove = new ArrayList<>();
        for (int port : dstorePorts.keySet()) {
            if (dstorePorts.get(port).getSocket().isClosed()) {
                log("PORT " + port + " IS CLOSED");
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

            log("Port " + port + " file count: " + fileCount + "; acceptable range " + minimum + "-" + maximum);

            if (fileCount < minimum || fileCount > maximum) return port;
        }

        return null;
    }

    private void rebalance() {
        log("Rebalance requested");

        if (isRebalancing) {
            log("Rebalance already in progress");
            return;
        } else {
            log("Rebalance started");
            isRebalancing = true;
        }

        try {
            while (operationsInProgress.get() > 0);

            checkAliveStores();

            if (dstorePorts.size() < replicationFactor) {
                log("Not enough Dstores to rebalance");
            } else {
                Set<String> filesInMemory = new HashSet<>(Set.copyOf(fileIndex.keySet()));
                Map<String, List<Integer>> filesStored = new ConcurrentHashMap<>();
                List<Integer> ports = new ArrayList<>();

                AtomicBoolean changed = new AtomicBoolean(false);

                int dstoreNumber = dstorePorts.keySet().size();
                int fileNumber = fileIndex.size();

                double threshold = (double) (replicationFactor * fileNumber) / dstoreNumber;
                int minimum = (int) Math.floor(threshold);
                int maximum = (int) Math.ceil(threshold);

                Map<Integer, List<String>> currents = new ConcurrentHashMap<>();

                Map<Integer, Map<String, List<Integer>>> sends = new ConcurrentHashMap<>();
                Map<Integer, List<String>> removals = new ConcurrentHashMap<>();

                for (int port : dstorePorts.keySet()) {
                    ports.add(port);

                    new Thread(() -> {
                        String message = waitForDstoreMessage(port);

                        if (message != null) {
                            String[] args = message.split(" ");
                            if (args[0].equals(Protocol.LIST_TOKEN)) {
                                sends.put(port, new ConcurrentHashMap<>());
                                removals.put(port, new ArrayList<>());

                                List<String> fileNames = new ArrayList<>(List.of(Arrays.copyOfRange(args, 1, args.length)));
                                currents.put(port, fileNames);
                                for (var fileName : fileNames) {
                                    if (!filesInMemory.contains(fileName) || removeInProgress(fileName)) {
                                        removals.get(port).add(fileName);
                                        changed.set(true);
                                        break;
                                    }

                                    if (!filesStored.containsKey(fileName))
                                        filesStored.put(fileName, new ArrayList<>());
                                    filesStored.get(fileName).add(port);
                                }
                            } else {
                                error("Expected 'LIST' from port " + port + ", received '" + message + "'");
                            }
                        } else {
                            ports.remove(port);
                            error("Rebalance list timeout on port " + port);
                        }
                    }).start();
                    sendDstoreMessage(port, Protocol.LIST_TOKEN);
                }

                if (ports.size() < replicationFactor) {
                    log("Not enough Dstores responded to list request, rebalance stopping");
                } else {
                    Map<String, Integer> unbalancedFiles = new ConcurrentHashMap<>();

                    filesStored.forEach((fileName, storedPorts) -> {
                        int count = storedPorts.size();

                        if (count < replicationFactor) {
                            unbalancedFiles.put(fileName, replicationFactor - count);
                        }
                    });

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

                    TriFunction<String, Integer, Integer, Void> sendAndRemove = (fileName, source, destination) -> {
                        if (!sends.get(source).containsKey(fileName))
                            sends.get(source).put(fileName, new ArrayList<>());

                        boolean originator = true;
                        for (var entry : sends.entrySet()) {
                            int sourcePort = entry.getKey();
                            var sendingFiles = entry.getValue();

                            if (sendingFiles.containsKey(fileName) && sendingFiles.get(fileName).contains(source)) {
                                sends.get(sourcePort).get(fileName).add(destination);
                                sends.get(sourcePort).get(fileName).remove(source);
                                originator = false;
                                break;
                            }
                        }

                        if (originator) {
                            sends.get(source).get(fileName).add(destination);

                            if (!removals.get(source).contains(fileName)) {
                                removals.get(source).add(fileName);
                            }
                        }

                        filesStored.get(fileName).remove(source);
                        filesStored.get(fileName).add(destination);

                        currents.get(source).remove(fileName);
                        currents.get(destination).add(fileName);

                        return null;
                    };

                    while ((unbalancedPort = getUnbalancedPort(minimum, maximum, currents)) != null) {
                        log("Port " + unbalancedPort + " is unbalanced");
                        changed.set(true);
                        ports.sort(Comparator.comparingInt(port -> currents.get(port).size()));

                        if (currents.get(unbalancedPort).size() > maximum) {
                            String chosenFile = null;
                            int minimumPort = unbalancedPort;
                            int minimumCount = Integer.MAX_VALUE;
                            for (String fileName : currents.get(unbalancedPort)) {
                                Integer port = findLowestNotContaining(fileName, currents);

                                if (port == null || port.equals(unbalancedPort)) continue;

                                int fileCount = currents.get(port).size();
                                if (fileCount < minimumCount) {
                                    minimumPort = port;
                                    minimumCount = fileCount;
                                    chosenFile = fileName;
                                }
                            }

                            if (minimumPort != unbalancedPort) {
                                sendAndRemove.apply(chosenFile, unbalancedPort, minimumPort);
                            }
                        } else {
                            List<String> fileList = currents.get(unbalancedPort);
                            Integer takingFrom = findHighestNotContainingSome(fileList, currents);

                            if (takingFrom != null) {
                                for (String fileName : currents.get(takingFrom)) {
                                    if (!fileList.contains(fileName)) {
                                        sendAndRemove.apply(fileName, takingFrom, unbalancedPort);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (changed.get()) {
                        CountDownLatch latch = new CountDownLatch(ports.size());

                        for (int port : ports) {
                            StringBuilder request = new StringBuilder(Protocol.REBALANCE_TOKEN).append(" ");

                            Map<String, List<Integer>> sendingTo = sends.get(port);
                            request.append(sendingTo.size());

                            for (var entry : sendingTo.entrySet()) {
                                String fileName = entry.getKey();
                                List<Integer> sendingToPorts = entry.getValue();

                                request.append(" ").append(fileName).append(" ").append(sendingToPorts.size());

                                for (int sendingToPort : sendingToPorts) {
                                    request.append(" ").append(sendingToPort);
                                }
                            }

                            List<String> storeRemovals = removals.get(port);
                            request.append(" ").append(storeRemovals.size());
                            for (var fileName : storeRemovals) {
                                request.append(" ").append(fileName);
                            }

                            sendDstoreMessage(port, request.toString());

                            String message = waitForDstoreMessage(port);

                            if (message != null && message.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                                latch.countDown();
                                dstorePorts.get(port).setFileCount(currents.get(port).size());
                                log("Rebalance confirmation received from port " + port);
                            } else if (message == null) {
                                error("Rebalance completion timeout on " + port);
                            } else {
                                error("Expected 'REBALANCE_COMPLETE' from port " + port + ", received '" + message + "'");
                            }
                        }

                        if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                            log("ALL DSTORES COMPLETED REBALANCE");
                            for (var entry : filesStored.entrySet()) {
                                String fileName = entry.getKey();
                                List<Integer> storedPorts = entry.getValue();

                                fileIndex.get(fileName).setDstorePorts(storedPorts);
                            }

                            Set<String> fullRemovals = getRemovals();

                            for (var fileName : fullRemovals) {
                                fileIndex.remove(fileName);
                            }
                        } else {
                            error("NOT ALL DSTORES COMPLETED REBALANCE");
                        }
                    } else {
                        log("No rebalance needed");
                    }
                }
            }
        } catch (Exception e) {
            error(e);
        } finally {
            log("Rebalance completed");
            isRebalancing = false;
            restartSockets();
        }
    }

    private Set<String> getRemovals() {
        Set<String> removals = new HashSet<>();

        for (var entry : fileIndex.entrySet()) {
            String fileName = entry.getKey();
            FileInfo info = entry.getValue();

            if (info.getStatus().equals(FileInfo.Status.REMOVE_IN_PROGRESS)) {
                removals.add(fileName);
            }
        }

        return removals;
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
                if (!fileNames.contains(fileName)) {
                    portsNotContaining.add(port);
                    break;
                }
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
                while(!isRebalancing && (line = in.readLine()) != null) {
                    log("TCP message received: " + line);

                    String[] cmd = line.split(" ");

                    if (cmd[0].equals(Protocol.JOIN_TOKEN)) {
                        sockets.remove(socket);
                        join(Integer.parseInt(cmd[1]), socket, in);
                        break;
                    }

                    if (!cmd[0].equals(Protocol.RELOAD_TOKEN)) previousPorts.clear();

                    switch (cmd[0]) {
                        case Protocol.STORE_TOKEN:
                            if (notEnoughDstores(out)) break;
                            store(out, cmd[1], Integer.parseInt(cmd[2]));
                            break;

                        case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN:
                            if (notEnoughDstores(out)) break;
                            load(out, cmd[1]);
                            break;

                        case Protocol.REMOVE_TOKEN:
                            if (notEnoughDstores(out)) break;
                            remove(out, cmd[1]);
                            break;

                        case Protocol.LIST_TOKEN:
                            if (notEnoughDstores(out)) break;
                            list(out);
                            break;
                    }
                }
            } catch(Exception e) {
                error(e.toString());
            }
        }
    }

    @FunctionalInterface
    interface TriFunction<T, U, V, R> {
        R apply(T t, U u, V v);
    }
}