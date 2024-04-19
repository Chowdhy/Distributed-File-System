import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Controller {
    private ServerSocket serverSocket;
    private final Map<String, FileInfo> fileIndex;
    private final int replicationFactor;
    private final int timeout;
    private final List<Integer> previousPorts;
    private final Map<Integer, DstoreInfo> dstorePorts;
    private boolean isRebalancing = false;
    private final List<Socket> sockets;
    
    private void log(String message) {
        System.out.println("[CONTROLLER] " + message);
    }

    private void log(Object o) {
        if (o == null) {
            log("null");
            return;
        }

        log(o.toString());
    }
    
    private void error(String message) {
        System.err.println("[CONTROLLER] Error: " + message);
    }

    private void error(Exception e) {
        error(e.toString());
    }

    public Controller(int cport, int replicationFactor, int timeout, int rebalancePeriod) {
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;

        fileIndex = new HashMap<>();
        previousPorts = new ArrayList<>();
        dstorePorts = new HashMap<>();
        sockets = new ArrayList<>();

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
                new Thread(new ServiceThread(socket)).start();
                sockets.add(socket);
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

    private String waitForDstoreMessage(int port, String commandWord) throws TimeoutException {
        DstoreInfo info = dstorePorts.get(port);
        String message = info.waitForMessage(commandWord, timeout);
        log("Message received from port " + port + ": " + message);
        return message;
    }
    
    private String listenToDstoreMessage(Socket socket) throws SocketTimeoutException {
        int port = socket.getPort();

        try {
            socket.setSoTimeout(timeout);

            String message = dstorePorts.get(port).getReader().readLine();
            if (message == null) throw new SocketTimeoutException();

            log("Dstore (" + port + ") TCP message received: " + message);
            return message;
        } catch (IOException e) {
            throw new SocketTimeoutException();
        }
    }

    private void join(int port) {
        try {
            InetAddress address = InetAddress.getLocalHost();
            Socket socket = new Socket(address, port);
            log("Connection established on port " + port);

            DstoreInfo info = new DstoreInfo(port, socket);
            System.out.println("GOT HERE");
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
                    String message = waitForDstoreMessage(port, Protocol.STORE_ACK_TOKEN);

                    if (message.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
                        info.setFileCount(info.getFileCount() + 1);
                        inc.getAndIncrement();
                    }

                    if (inc.get() == replicationFactor) {
                        fileIndex.get(fileName).setStatus(FileInfo.Status.STORE_COMPLETE);
                        System.out.println("ALL ACKS RECEIVED");
                        System.out.println(fileIndex.get(fileName).getStatus());
                        sendClientMessage(out, Protocol.STORE_COMPLETE_TOKEN);
                    }
                } catch (TimeoutException e) {
                    log("Request timed out");
                }
            }).start();
        }

        sendClientMessage(out, Protocol.STORE_TO_TOKEN + usedPortString);

        new Thread(() -> {
            try {
                Thread.sleep(timeout);
                if (storeInProgress(fileName)) {
                    fileIndex.remove(fileName);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
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
            new Thread(() -> {
                try {
                    String message = waitForDstoreMessage(port, Protocol.REMOVE_ACK_TOKEN);

                    if (message.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName) || message.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName)) {
                        inc.getAndIncrement();

                        if (inc.get() == ports.size()) {
                            fileIndex.remove(fileName);
                            sendClientMessage(out, Protocol.REMOVE_COMPLETE_TOKEN);
                        }
                    }
                } catch (TimeoutException e) {
                    log("Message timeout");
                }
            }).start();

            sendDstoreMessage(port, Protocol.REMOVE_TOKEN + " " + fileName);
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

            System.out.println("count: " + fileCount);
            System.out.println("min: " + minimum);
            System.out.println("max: " + maximum);

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
            for (boolean processed = false; !processed;) {
                processed = true;

                for (String fileName : fileIndex.keySet()) {
                    if (storeInProgress(fileName) || removeInProgress(fileName)) {
                        processed = false;
                        break;
                    }
                }
            }

            checkAliveStores();

            if (dstorePorts.size() < replicationFactor) {
                log("Not enough Dstores to rebalance");
            } else {
                Map<String, List<Integer>> filesStored = new HashMap<>();

                List<Integer> ports = new ArrayList<>();

                int dstoreNumber = dstorePorts.keySet().size();
                int fileNumber = fileIndex.size();

                double threshold = (double) (replicationFactor * fileNumber) / dstoreNumber;
                int minimum = (int) Math.floor(threshold);
                int maximum = (int) Math.ceil(threshold);

                Map<Integer, List<String>> currents = new HashMap<>();

                Map<Integer, Map<String, List<Integer>>> sends = new HashMap<>();
                Map<Integer, List<String>> removals = new HashMap<>();

                dstorePorts.forEach((port, info) -> {
                    ports.add(port);

                    new Thread(() -> {
                        try {
                            String message = waitForDstoreMessage(port, Protocol.LIST_TOKEN);

                            String[] args = message.split(" ");
                            if (args[0].equals(Protocol.LIST_TOKEN)) {
                                sends.put(port, new HashMap<>());
                                removals.put(port, new ArrayList<>());

                                List<String> fileNames = new ArrayList<>(List.of(Arrays.copyOfRange(args, 1, args.length)));
                                currents.put(port, fileNames);
                                for (var fileName : fileNames) {
                                    if (!filesStored.containsKey(fileName))
                                        filesStored.put(fileName, new ArrayList<>());
                                    filesStored.get(fileName).add(port);
                                }
                            }
                        } catch (TimeoutException e) {
                            log("Message timeout");
                        }
                    }).start();

                    sendDstoreMessage(port, Protocol.LIST_TOKEN);
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
                log(getUnbalancedPort(minimum, maximum, currents));

                TriFunction<String, Integer, Integer, Void> sendAndRemove = (fileName, source, destination) -> {
                    if (!sends.get(source).containsKey(fileName)) sends.get(source).put(fileName, new ArrayList<>());

                    sends.get(source).get(fileName).add(destination);

                    removals.get(source).add(fileName);

                    filesStored.get(fileName).remove(source);
                    filesStored.get(fileName).add(destination);

                    currents.get(source).remove(fileName);
                    currents.get(destination).add(fileName);

                    return null;
                };

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
                    for (int port : ports) {
                        StringBuilder request = new StringBuilder(Protocol.REBALANCE_TOKEN).append(" ");

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
                            String message = waitForDstoreMessage(port, Protocol.REBALANCE_COMPLETE_TOKEN);

                            if (message.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                                dstorePorts.get(port).setFileCount(currents.get(port).size());
                            }
                        } catch (TimeoutException e) {
                            log("Message timeout");
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
            }
        } catch (Exception e) {
            error(e);
        } finally {
            log("Rebalance completed");
            isRebalancing = false;
            restartSockets();
        }
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

                    if (!cmd[0].equals(Protocol.RELOAD_TOKEN)) previousPorts.clear();

                    switch (cmd[0]) {
                        case Protocol.JOIN_TOKEN:
                            join(Integer.parseInt(cmd[1]));
                            break;

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