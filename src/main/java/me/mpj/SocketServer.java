package me.mpj;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {

    private ServerSocket server;

    public void listen() {
        try {
            server = new ServerSocket(4444);
        } catch (IOException e) {
            System.out.println("SockerServer Error: Could not listen on port.");
            System.exit(-1);
        }
        while(true){

            try {
                System.out.println("Waiting for connection ...");
                final Socket socket = server.accept();

                SocketWorker worker = new SocketWorker(socket);
                Thread thread = new Thread(worker);
                thread.start();

            } catch (IOException e) {
                System.out.println("SocketServer Error: Socket accept failed.");
                System.exit(-1);
            }
        }
    }

    /* creating new server and call it's listen method */
    public static void main(String[] args) {
        new SocketServer().listen();
    }
}
