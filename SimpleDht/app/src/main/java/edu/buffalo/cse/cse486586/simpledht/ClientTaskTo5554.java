package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientTaskTo5554  extends AsyncTask<String, Void, Void> {
    static String TAG = SimpleDhtProvider.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {

        String x = "11108";
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x));

            String[] msgToSend = {"inform_emu_id",msgs[0]};

            ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
            clientOutputStream.writeObject(msgToSend);

            Log.e(TAG,msgToSend+"sent to 5554");
            socket.close();
        }
        catch (UnknownHostException e) {
            Log.e(TAG, "ClientTaskTo5554() UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTaskTo5554() socket IOException ");
            e.printStackTrace();
        }

        return null;
    }
}