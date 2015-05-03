package me.mpj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketWorker implements Runnable {
    private Socket _socket;
    public SocketWorker(Socket socket) {
        _socket = socket;
    }

    @Override
    public void run() {
        final InputStream inputStream;
        try {
            inputStream = _socket.getInputStream();
            final InputStreamReader streamReader = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(streamReader);
            // readLine blocks until line arrives or socket closes, upon which it returns null
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            _socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
