//DEPS org.zeromq:jeromq:0.5.3
package org.acme;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class HelloWorldClient {

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            //  Socket to talk to server
            System.out.println("Connecting to hello world server");

            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5555");

            for (int requestNbr = 0; requestNbr != 100; requestNbr++) {
                String request = "Hello " + requestNbr + " from " + ProcessHandle.current().pid();
                System.out.println("Sending Hello " + requestNbr);
                socket.send(request.getBytes(ZMQ.CHARSET), 0);

                byte[] reply = socket.recv(0);
                System.out.println(
                        "Received " + new String(reply, ZMQ.CHARSET) + requestNbr
                );
            }
        }
    }
}
