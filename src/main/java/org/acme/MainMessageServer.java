package org.acme;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.security.SecureRandom;

@QuarkusMain
public class MainMessageServer {

    private static final Logger log = LoggerFactory.getLogger(MainMessageServer.class);

    public static void main(String... args) {
        Quarkus.run(MyApp.class, args);
    }

    public static class MyApp implements QuarkusApplication {

        private static class Worker extends Thread {
            private ZContext context;

            private Worker(ZContext context) {
                this.context = context;
            }

            @Override
            public void run() {
                ZMQ.Socket socket = context.createSocket(SocketType.REP);
                socket.connect("inproc://workers");
                SecureRandom sleep = new SecureRandom();

                while (true) {
                    String request = socket.recvStr(0);
                    log.info(Thread.currentThread().getName() + " Received request: [" + request + "]");
                    try {
                        // Do some 'work'
                        Thread.sleep(sleep.nextInt(200));
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                    String response = Thread.currentThread().getName() + " world ";
                    socket.send(response.getBytes(ZMQ.CHARSET), 0);
                }
            }
        }

        public Thread foo(ZContext context) {
            return new Worker(context);
        }

        @Override
        public int run(String... args) throws Exception {
            log.info("MainMessageServer::run");
            try (ZContext context = new ZContext()) {
                ZMQ.Socket clients = context.createSocket(SocketType.ROUTER);
                clients.bind("tcp://*:5555");

                ZMQ.Socket workers = context.createSocket(SocketType.DEALER);
                workers.bind("inproc://workers");

                int threadPoolSize = ConfigProvider.getConfig().getValue("quarkus.vertx.worker-pool-size", int.class);
                log.info("thread pool size: " + threadPoolSize);

                for (int thread_nbr = 0; thread_nbr < threadPoolSize; thread_nbr++) {
                    Uni<Thread> uni = Uni.createFrom().item(foo(context)).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
                    uni.await().indefinitely().start();
                }

                //  Connect work threads to client threads via a queue
                ZMQ.proxy(clients, workers, null);
            }
            Quarkus.waitForExit();
            return 0;
        }
    }
}
