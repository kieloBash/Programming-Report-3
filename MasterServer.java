import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class MasterServer {
    private static final ExecutorService CLIENT_EXECUTOR = Executors.newCachedThreadPool();
    private static final List<SlaveConnection> SLAVE_CONNECTIONS = new CopyOnWriteArrayList<>();

    private static final int CLIENT_LISTENING_PORT = 4999;
    private static final int SLAVE_LISTENING_PORT = 5001;
    private static int slavePrimes = 0;

    public static void main(String[] args) {
        // Separate thread for listening to slave server connections
        Thread slaveListenerThread = new Thread(MasterServer::listenForSlaves);
        slaveListenerThread.start();

        // Listen for client connections
        try (ServerSocket serverSocket = new ServerSocket(CLIENT_LISTENING_PORT)) {
            System.out.println("Listening for CLIENT on port: " + CLIENT_LISTENING_PORT);

            // Handle each client connection in a separate thread
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected...");
                System.out.println("Waiting for request to process");
                CLIENT_EXECUTOR.submit(() -> handleClient(clientSocket));
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            CLIENT_EXECUTOR.shutdown();
        }
    }

    private static void listenForSlaves() {
        try (ServerSocket slaveListener = new ServerSocket(SLAVE_LISTENING_PORT)) {
            System.out.println("Listening for SLAVES on port: " + SLAVE_LISTENING_PORT);
            while (true) {
                Socket slaveSocket = slaveListener.accept();
                BufferedReader input = new BufferedReader(new InputStreamReader(slaveSocket.getInputStream()));
                String slaveAddress = slaveSocket.getInetAddress().getHostAddress();
                int slavePort = Integer.parseInt(input.readLine());

                // Create and add a new slave connection to the list
                SlaveConnection newSlave = new SlaveConnection(slaveAddress, slavePort);
                if (!SLAVE_CONNECTIONS.contains(newSlave)) {
                    System.out.println("New SLAVE - (Address, Port) (" + newSlave.getAddress() + ", " + newSlave.getPort() + ")");
                    SLAVE_CONNECTIONS.add(newSlave);
                }
            }
        } catch (IOException ex) {
            System.err.println("Error in slave listener: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try {
            DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());

            // Read input from client
            int start = dis.readInt();
            int end = dis.readInt();
            int numThreads = dis.readInt();

            System.out.println("Received task from client: start="+ start + " end=" + end + ", Threads=" + numThreads);

            // Distribute tasks to slaves and master
            List<Integer> primes = new ArrayList<>();
            int segmentSize = end / (SLAVE_CONNECTIONS.size() + 1); // +1 for the master
            int start1 = start;
            int end1 = segmentSize;

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            for (int i = 0; i < SLAVE_CONNECTIONS.size() + 1; i++) {
                if (i == SLAVE_CONNECTIONS.size()) {
                    // Master handles the last segment
                    //end1 = end;
                }

                if (i == 0) {
                    // Master
                    System.out.println("Master handling segment: " + start + " to " + end1);
                    executor.submit(new PrimeTask(start, end1, primes));
                } else {
                    // Slaves
                    SlaveConnection slave = SLAVE_CONNECTIONS.get(i - 1);
                    System.out.println("Sending task to slave " + i + ": " + start + " to " + end1);
                    sendTaskToSlave(slave.getAddress(), slave.getPort(), numThreads, start, end1);
                }

                start = end1 + 1;
                end1 += segmentSize;
            }

            awaitTaskCompletion(executor);
            System.out.println("All tasks completed.");

            // Send result back to client
            //System.out.println("primes ni master "+ primes);
            dos.writeUTF("Total primes found: " + (primes.size() + slavePrimes));

        } catch (IOException ex) {
            System.err.println("Error handling client request: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendTaskToSlave(String address, int port, int numThreads, int start, int end) {
        try (Socket socket = new Socket(address, port)) {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeInt(numThreads);
            dos.writeInt(start);
            dos.writeInt(end);

            // Receive prime count from slave
            slavePrimes = (dis.readInt());
        } catch (IOException ex) {
            System.err.println("Error sending task to slave: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void awaitTaskCompletion(ExecutorService executor) {
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait for all tasks to finish
        }
    }

    static class SlaveConnection {
        private String address;
        private int port;

        public SlaveConnection(String address, int port) {
            this.address = address;
            this.port = port;
        }

        public String getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            SlaveConnection that = (SlaveConnection) obj;
            return port == that.port && address.equals(that.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, port);
        }
    }

    static class PrimeTask implements Runnable {
        private int start;
        private int end;
        private List<Integer> primes;

        public PrimeTask(int start, int end, List<Integer> primes) {
            this.start = start;
            this.end = end;
            this.primes = primes;
        }

        @Override
        public void run() {
            for (int i = start; i <= end; i++) {
               // System.out.println("master processing:  "+ i);

                if (isPrime(i)) {
                    synchronized (primes) {
                        primes.add(i);
                    }
                }
            }
        }

        private boolean isPrime(int n) {
            if (n <= 1) return false;
            if (n <= 3) return true;
            if (n % 2 == 0 || n % 3 == 0) return false;
            for (int i = 5; i * i <= n; i += 6) {
                if (n % i == 0 || n % (i + 2) == 0) return false;
            }
            return true;
        }
    }
}
