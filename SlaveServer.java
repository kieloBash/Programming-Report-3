import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SlaveServer {
    private static final String DEFAULT_MASTER_ADDRESS = "localhost";
    private static final int MASTER_PORT = 5001;
    private static final int DEFAULT_SLAVE_SERVICE_PORT = 5003;

    public static void main(String[] args) {
        String defaultMasterAddress = DEFAULT_MASTER_ADDRESS;
        int defaultSlaveServicePort = DEFAULT_SLAVE_SERVICE_PORT;

        // Process command-line arguments
        ServerConfiguration config = processArguments(args, defaultSlaveServicePort);
        defaultMasterAddress = config.masterAddress;
        defaultSlaveServicePort = config.slaveServicePort;

        System.out.println("Master Address: " + defaultMasterAddress);
        System.out.println("Slave Service Port: " + defaultSlaveServicePort);

        if (!connectToMaster(defaultMasterAddress, defaultSlaveServicePort)) {
            System.err.println("Failed to connect to the MasterServer. Exiting...");
            System.exit(1);
        }

        startTaskListener(defaultSlaveServicePort);
    }

    private static void startTaskListener(int slaveServicePort) {
        try (ServerSocket serverSocket = new ServerSocket(slaveServicePort)) {
            System.out.println("Listening for tasks from MasterServer on port " + slaveServicePort);

            while (true) {
                Socket masterSocket = serverSocket.accept();
                System.out.println("Connected to MasterServer for a task.");
                handleTask(masterSocket);
            }
        } catch (BindException e) {
            System.err.println("Port " + slaveServicePort + " is already in use. Please choose a different port.");
            e.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Failed to listen on port " + slaveServicePort + ": " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void handleTask(Socket masterSocket) {
        try (DataInputStream dis = new DataInputStream(masterSocket.getInputStream());
             DataOutputStream dos = new DataOutputStream(masterSocket.getOutputStream())) {

            int numThreads = dis.readInt();
            if (numThreads <= 0) {
                throw new IllegalArgumentException("Number of threads must be greater than 0.");
            }

            List<Integer> primes = new ArrayList<>();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            int num = dis.readInt();
            int start = dis.readInt();
            executor.submit(new PrimeTask(num, primes, start));
            awaitTaskCompletion(executor);

            int primeCount = primes.size();
            //System.out.println("primes ni slave= "+primes);
            dos.writeInt(primeCount);
        } catch (EOFException e) {
            System.err.println("Connection terminated unexpectedly by the MasterServer.");
            e.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Failed to handle task from master: An error occurred with the MasterServer connection.");
            ex.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid input: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean connectToMaster(String masterAddress, int listeningPort) {
        System.out.println("Attempting to connect to MasterServer...");
        try (Socket socket = new Socket(masterAddress, MASTER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(listeningPort);
            System.out.println("Successfully connected to MasterServer.");
            return true;

        } catch (IOException e) {
            System.err.println("Failed to connect to MasterServer (" + masterAddress + ":" + MASTER_PORT + "). Exiting...");
            e.printStackTrace();
        }
        return false;
    }

    private static ServerConfiguration processArguments(String[] args, int defaultSlaveServicePort) {
        String defaultMasterAddress = DEFAULT_MASTER_ADDRESS;
        int slaveServicePort = defaultSlaveServicePort;

        if (args.length == 1) {
            if (args[0].matches("\\d+")) {
                slaveServicePort = Integer.parseInt(args[0]);
            } else {
                defaultMasterAddress = args[0];
            }
        } else if (args.length == 2) {
            defaultMasterAddress = args[0];
            try {
                slaveServicePort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid port number provided. Using default port: " + defaultSlaveServicePort);
            }
        } else if (args.length > 2) {
            System.exit(1);
        }

        return new ServerConfiguration(defaultMasterAddress, slaveServicePort);
    }

    private static void awaitTaskCompletion(ExecutorService executor) {
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait for all tasks to finish
        }
    }

    // Configuration object for server settings
    private static class ServerConfiguration {
        private final String masterAddress;
        private final int slaveServicePort;

        ServerConfiguration(String masterAddress, int slaveServicePort) {
            this.masterAddress = masterAddress;
            this.slaveServicePort = slaveServicePort;
        }
    }

    // Runnable task to determine if a number is prime
    static class PrimeTask implements Runnable {
        private final List<Integer> primes;
        private final int start;
        private final int end;

        PrimeTask(int start, List<Integer> primes, int end) {
            this.start = start;
            this.primes = primes;
            this.end = end;
        }

        @Override
        public void run() {
            for (int i = start; i <= end; i++) {
                //System.out.println("slave processing:  "+ i);
                if (isPrime(i)) {
                    synchronized (primes) {
                        primes.add(i);
                    }
                }
            }
        }

        // Check if a number is prime
        private boolean isPrime(int n) {
            //System.out.println("n = "+ n);
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
