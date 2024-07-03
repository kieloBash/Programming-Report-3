import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SlaveServer {
    // Constants
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

        System.out.println("masterAddress: " + defaultMasterAddress);
        System.out.println("slaveServicePort: " + defaultSlaveServicePort);

        if (!masterConnection(defaultMasterAddress, defaultSlaveServicePort)) {
            System.err.println("Failed to connect to the MasterServer. Exiting...");
            System.exit(1);
        }

        taskListener(defaultSlaveServicePort);
    }

    // Process command-line arguments for custom parameters
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

    // Connect to the MasterServer
    private static boolean masterConnection(String masterAddress, int listeningPort) {
        System.out.println("Attempting to connect to MasterServer...");
        Socket socket = null;
        PrintWriter out = null;

        try {

            socket = new Socket(masterAddress, MASTER_PORT);
            out = new PrintWriter(socket.getOutputStream(), true);

            out.println(listeningPort);
            System.out.println("Successfully connected to MasterServer.");
            return true;
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + masterAddress);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("I/O error while connecting to MasterServer (" + masterAddress + ":" + MASTER_PORT + "). Exiting...");
            e.printStackTrace();
        } finally {
            try {
                if (out != null) out.close();
                if (socket != null) socket.close();
            } catch (IOException e) {
                System.err.println("Failed to close socket or output stream: " + e.getMessage());
                e.printStackTrace();
            }
        }
        return false;
    }

    // Listen for tasks from the MasterServer
    private static void taskListener(int slaveServicePort) {
        try (ServerSocket serverSocket = new ServerSocket(slaveServicePort)) {
            System.out.println("Listening for tasks from MasterServer on port " + slaveServicePort);

            while (true) {
                Socket masterSocket = serverSocket.accept();
                System.out.println("Connected to MasterServer for a task.");
                taskHandler(masterSocket);
            }
        } catch (BindException e) {
            System.err.println("Port " + slaveServicePort + " is already in use. Please choose a different port.");
            e.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Failed to listen on port " + slaveServicePort + ": " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    // Handle task received from MasterServer
    private static void taskHandler(Socket masterSocket) {
        try (DataInputStream dis = new DataInputStream(masterSocket.getInputStream());
             DataOutputStream dos = new DataOutputStream(masterSocket.getOutputStream())) {

            int nThreads = dis.readInt();
            if (nThreads <= 0) {
                throw new IllegalArgumentException("Number of threads must be greater than 0.");
            }

            List<Integer> primes = new ArrayList<>();
            Lock primesLock = new ReentrantLock();
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);

            int num;
            while ((num = dis.readInt()) != -1) {
                if (num <= 0) {
                    System.err.println("Invalid number: " + num);
                    continue;
                }
                executor.submit(new PrimeTask(num, primes, primesLock));
            }

            awaitTaskCompletion(executor);

            int primeCount = primes.size();
            System.out.println("Successfully calculated " + primeCount + " primes. Sending result to MasterServer.");
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

    // Await task completion by shutting down the executor and waiting for termination
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
        private final Lock lock;
        private final int num;

        PrimeTask(int num, List<Integer> primes, Lock lock) {
            this.num = num;
            this.primes = primes;
            this.lock = lock;
        }

        @Override
        public void run() {
            if (isPrime(num)) {
                lock.lock();
                try {
                    primes.add(num);
                } finally {
                    lock.unlock();
                }
            }
        }

        // Check if a number is prime
        private boolean isPrime(int n) {
            if (n <= 1) return false;
            for (int i = 2; i * i <= n; i++) {
                if (n % i == 0) return false;
            }
            return true;
        }
    }
}
