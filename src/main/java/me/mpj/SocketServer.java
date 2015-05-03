package me.mpj;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {

    private ServerSocket _server;

    /**
     * listen on a port for socket connections. Whenever one is connected,
     * start a worker on a new thread and pass the socket connection into it.
     */
    public void listen() {
        try {
            _server = new ServerSocket(4444);
        } catch (IOException e) {
            System.out.println("SockerServer Error: Could not listen on port.");
            System.exit(-1);
        }
        while(true) {
            try {
                System.out.println("Waiting for connection ...");
                final Socket socket = _server.accept();

                SocketWorker worker = new SocketWorker(socket);
                Thread thread = new Thread(worker);
                thread.start();

            } catch (IOException e) {
                System.out.println("SocketServer Error: Socket accept failed.");
            }
        }
    }

    public static void main(String[] args) {
        new SocketServer().listen();
    }
}
