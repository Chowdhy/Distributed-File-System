import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Dstore {
    final int port;
    final int cport;
    int timeout;
    File fileFolder;
    ServerSocket serverSocket;
    PrintWriter controllerWriter;

    private void clearFolder(File folder) {
        File[] files = folder.listFiles();

        if (files == null) return;

        for (var file : files) {
            if (file.isDirectory()) {
                clearFolder(file);
            }
            file.delete();
        }
    }

    private void sendControllerMessage(String message) {
        controllerWriter.println(message);
        log("Sent controller message: " + message);
    }

    private void log(String message) {
        System.out.println("[DSTORE " + port + "] " + message);
    }

    private void error(String message) {
        System.err.println("[DSTORE " + port + "] Error: " + message);
    }

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = new File(fileFolder);

        if (!this.fileFolder.mkdir()) {
            clearFolder(this.fileFolder);
        }

        Socket controllerSocket;

        try {
            InetAddress address = InetAddress.getLocalHost();
            controllerSocket = new Socket(address, cport);
            controllerWriter = new PrintWriter(controllerSocket.getOutputStream(), true);

            controllerWriter.println(Protocol.JOIN_TOKEN + " " + port);
            log("Sent join request to " + cport);

            new Thread(new ServiceThread(controllerSocket)).start();

            serverSocket = new ServerSocket(port);
            log("Dstore server started on port " + port);

            while (true) {
                try {
                    Socket newSocket = serverSocket.accept();
                    log("Connection request accepted");

                    new Thread(new ServiceThread(newSocket)).start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    private void saveFile(String fileName, byte[] fileBytes) {
        try {
            File file = new File(fileFolder.getAbsolutePath() + File.separator + fileName);

            file.createNewFile();

            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(fileBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void store(String fileName, byte[] fileBytes) {
        saveFile(fileName, fileBytes);
        sendControllerMessage(Protocol.STORE_ACK_TOKEN + " " + fileName);
    }

    private void deleteFile(String fileName) {
        File file = new File(fileFolder.getPath() + File.separator + fileName);
        file.delete();
    }

    private void remove(String fileName) {
        File file = new File(fileFolder.getPath() + File.separator + fileName);

        try {
            if (file.createNewFile()) {
                sendControllerMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                file.delete();
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        file.delete();
        sendControllerMessage(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
    }

    private void list() {
        StringBuilder fileNameList = new StringBuilder("LIST");

        File[] files = fileFolder.listFiles();

        if (files != null) {
            for (File file : files) {
                fileNameList.append(" ").append(file.getName());
            }
        }

        sendControllerMessage(fileNameList.toString());
    }

    private void sendFile(String fileName, int port, int fileSize, byte[] content) throws SocketTimeoutException {
        try {
            InetAddress address = InetAddress.getLocalHost();
            try (Socket socket = new Socket(address, port)) {
                PrintWriter textWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader textReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream dataWriter = socket.getOutputStream();

                String message = Protocol.REBALANCE_STORE_TOKEN + " " + fileName + " " + fileSize;

                textWriter.println(message);
                log("Dstore message sent: " + message);

                socket.setSoTimeout(timeout);

                try {
                    String ack = textReader.readLine();
                    if (ack.equals(Protocol.ACK_TOKEN)) {
                        dataWriter.write(content);
                    }
                } catch (IOException e) {
                    throw new SocketTimeoutException();
                }
            }
        } catch (SocketTimeoutException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readFile(String fileName) throws FileNotFoundException {
        File file = new File(fileFolder.getPath() + File.separator + fileName);

        if (!file.exists()) {
            throw new FileNotFoundException();
        }

        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            return fileInputStream.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void rebalance(String[] args) {
        boolean success = true;

        int currentIndex = 0;

        int transferCount = Integer.parseInt(args[currentIndex]);
        for (int i = 0; i < transferCount; i++) {
            int fileNameIndex = ++currentIndex;
            int portCountIndex = ++currentIndex;

            String fileName = args[fileNameIndex];
            try {
                byte[] fileContent = readFile(fileName);
                int fileSize = fileContent.length;

                int portCount = Integer.parseInt(args[portCountIndex]);

                CountDownLatch latch = new CountDownLatch(portCount);

                for (int i_ = 0; i_ < portCount; i_++) {
                    int portIndex = ++currentIndex;
                    int port = Integer.parseInt(args[portIndex]);

                    new Thread(() -> {
                        try {
                            sendFile(fileName, port, fileSize, fileContent);
                            latch.countDown();
                        } catch (SocketTimeoutException e) {
                            error("Rebalance ack not received from port " + port);
                        }
                    });
                }

                try {
                    if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                        log("ALL REBALANCE ACKS RECEIVED");
                    } else {
                        error("NOT ALL REBALANCE ACKS RECEIVED");
                        success = false;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (FileNotFoundException e) {
                error("Requested file '" + fileName + "' does not exist");
                success = false;
            }
        }

        int removeCountIndex = ++currentIndex;
        int removeCount = Integer.parseInt(args[removeCountIndex]);
        for (int i = 0; i < removeCount; i++) {
            int removeIndex = ++currentIndex;
            String fileToRemove = args[removeIndex];
            deleteFile(fileToRemove);
        }

        if (success) sendControllerMessage(Protocol.REBALANCE_COMPLETE_TOKEN);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        Dstore store = new Dstore(port, cport, timeout, fileFolder);
    }

    class ServiceThread implements Runnable {
        Socket socket;
        ServiceThread(Socket s) {
            socket = s;
        }
        public void run() {
            try {
                BufferedReader textReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter textWriter = new PrintWriter(socket.getOutputStream(), true);

                InputStream dataReader = socket.getInputStream();
                OutputStream dataWriter = socket.getOutputStream();

                String line;
                while ((line = textReader.readLine()) != null) {
                    log("TCP message received: " + line);
                    String[] cmd = line.split(" ");

                    switch (cmd[0]) {
                        case Protocol.STORE_TOKEN:
                            textWriter.println(Protocol.ACK_TOKEN);
                            log("Client message sent: " + Protocol.ACK_TOKEN);
                            store(cmd[1], dataReader.readNBytes(Integer.parseInt(cmd[2])));
                            break;

                        case Protocol.LOAD_DATA_TOKEN:
                            try {
                                byte[] fileBytes = readFile(cmd[1]);
                                dataWriter.write(fileBytes);
                            } catch (FileNotFoundException e) {
                                socket.close();
                            }
                            break;

                        case Protocol.REMOVE_TOKEN:
                            remove(cmd[1]);
                            break;

                        case Protocol.LIST_TOKEN:
                            list();
                            break;

                        case Protocol.REBALANCE_TOKEN:
                            rebalance(Arrays.copyOfRange(cmd, 1, cmd.length));
                            break;

                        case Protocol.REBALANCE_STORE_TOKEN:
                            textWriter.println(Protocol.ACK_TOKEN);
                            log("Dstore message sent: " + Protocol.ACK_TOKEN);
                            saveFile(cmd[1], dataReader.readNBytes(Integer.parseInt(cmd[2])));
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
