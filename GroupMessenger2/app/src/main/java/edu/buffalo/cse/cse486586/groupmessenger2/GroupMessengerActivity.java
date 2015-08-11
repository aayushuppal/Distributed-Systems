package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Message;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static int countI=0;
    static final int SERVER_PORT = 10000;
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static String myPort;
    static int msgIdnum = 100;
    static HashMap<String,MessageC> IdToMsgMap = new HashMap<>();           // contains intial msg object put in PQ
    static HashMap<String,MessageC> FullIdToMsgMap = new HashMap<>();       // contains all info
    static int ProcessProposedNum = 0;
    static PriorityQueue<MessageC> PQ = new PriorityQueue<>();
    static HashMap<String,HashSet<Double>> IdToHashSetMap = new HashMap<>(); // contains id to all it's proposal number mapping
    static HashMap<String,HashSet<String>> IdToRecvFromSetMap = new HashMap<>(); // contains id to all it's proposal recv from port mapping
    static HashMap<String,Boolean> IdToPropSentBoolMap = new HashMap<>();
    static ArrayList<String> remotePort = new ArrayList<>();
    static Hashtable<String,TimerTrigger_initial> IdToTimer = new Hashtable<>();
    static HashSet<String> PQMsgIdSet = new HashSet<>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));


        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button SendBtn =(Button) findViewById(R.id.button4);

        SendBtn.setOnClickListener(new Button.OnClickListener() {


            public void onClick(View v)
            {
                String msg = editText.getText().toString() + "\n";
                MessageC msgobj = new MessageC(msg,msgIdnum,myPort);
                editText.setText(""); // This is one way to reset the input box.
                HashSet<Double> hh = new HashSet<Double>();
                IdToHashSetMap.put(msgobj.id,hh);
                HashSet<String> ss = new HashSet<String>();
                IdToRecvFromSetMap.put(msgobj.id,ss);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgobj);
                msgIdnum++;
            }
        });


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while(true) {
                try {
                    Socket server = serverSocket.accept();
                    ObjectInputStream clientInputStream = new  ObjectInputStream(server.getInputStream());
                    // use buffer input reader to avoid packet loss
                    try {
                        MessageC msgobj = (MessageC) clientInputStream.readObject();
                        Log.e(TAG,"Server Process"+myPort);

                        // Priority queue builder ------------------------------------------------------------------------------------//
                        if(msgobj.AgreeFlag && !PQMsgIdSet.contains(msgobj.id)){
                            synchronized (this){
                                String[] Aruityr = {"QWERTY","PUSHING IN PQ:"+ msgobj.data};
                                publishProgress(Aruityr);

                                PQ.remove(IdToMsgMap.get(msgobj.id));
                                MessageC zzz = IdToMsgMap.get(msgobj.id);
                                zzz.AgreedNum = msgobj.AgreedNum;
                                zzz.AgreeFlag = msgobj.AgreeFlag;
                                zzz.ProposedNum = msgobj.ProposedNum;
                                PQ.offer(zzz);


                                if (msgobj.delProcId > 0){
                                    //remove redundant from PQ
                                    PQ = RemovedelProc(PQ,msgobj.delProcId);
                                }

                                while (!PQ.isEmpty() && PQ.peek().AgreeFlag){
                                    String[] Arltyr = {"QWERTY","PUBLISHING:"+ PQ.peek().data};
                                    publishProgress(Arltyr);
                                    Log.e(TAG,"publishing on "+myPort+" "+PQ.peek().data+" with "+PQ.peek().AgreedNum);
                                    MessageC tempr = PQ.poll();
                                    String[] TempArr = {tempr.data};
                                    PQMsgIdSet.add(tempr.id);
                                    publishProgress(TempArr);
                                }
                            }
                        }


                        // Priority queue builder ------------------------------------------------------------------------------------//

                        // Publish from priority queue ------------------------------------------------------------------------------------//

                        // Publish from priority queue ------------------------------------------------------------------------------------//



                        // proposal collector and as soon as collected then multicastor of agreement -------------------------------------------//
                        if (msgobj.ProcessId == Integer.parseInt(myPort) && msgobj.PropSentFrom !=null && !IdToRecvFromSetMap.get(msgobj.id).contains(msgobj.PropSentFrom)){

                            synchronized (this){

                                Log.e(TAG,"Does not have prop from"+msgobj.PropSentFrom+" so adding it now");

                                IdToRecvFromSetMap.get(msgobj.id).add(msgobj.PropSentFrom);
                                msgobj.PropRecvFrom.clear();
                                for (String xcv:IdToRecvFromSetMap.get(msgobj.id)){
                                    msgobj.PropRecvFrom.add(xcv);
                                }

                                for (Double x: msgobj.ProposedNum){
                                    IdToHashSetMap.get(msgobj.id).add(x);
                                }
                                msgobj.ProposedNum.clear();
                                for (Double x: IdToHashSetMap.get(msgobj.id)){
                                    msgobj.ProposedNum.add(x);
                                }
                                Log.e(TAG,"Now has"+msgobj.ProposedNum);
                                Log.e(TAG,"Now has"+"----------------");

                                FullIdToMsgMap.put(msgobj.id,msgobj); // has prop nums all, has recv ports all

                                if (FullIdToMsgMap.get(msgobj.id).ProposedNum.size() == 5){
                                    IdToTimer.get(msgobj.id).timer.cancel();
                                    IdToTimer.get(msgobj.id).timer.purge();

                                    Log.e(TAG, "TimerTrigger_propFromitself executed and msg here has prop from" + FullIdToMsgMap.get(msgobj.id).PropRecvFrom);

                                    msgobj = FullIdToMsgMap.get(msgobj.id);
                                    Collections.sort(msgobj.ProposedNum);
                                    msgobj.AgreedNum = msgobj.ProposedNum.get(msgobj.ProposedNum.size()-1); // for 5 > set 4
                                    msgobj.AgreeFlag = true;
                                    Log.e(TAG,"Agreement multicasted"+msgobj.AgreedNum+" "+msgobj.AgreeFlag);
                                    new NoTimerClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgobj);

                                }
                            }


                        }
                        // proposal collector and as soon as collected then multicastor of agreement -------------------------------------------//
                        // msgobj.ProcessId == Integer.parseInt(myPort) && whatToExpect == 4 && FullIdToMsgMap.get(msgobj.id).ProposedNum.size()==4



                        // Proposal sender --------------------------------------------------------------------------------------------------//
                        if(!IdToMsgMap.containsKey(msgobj.id)){

                            synchronized (this){

                                String[] Arr = {"QWERTY","FSTRCVD:"+ msgobj.data+" PRP SNTO:"+" "+msgobj.ProcessId/2};
                                publishProgress(Arr);
                                Log.e(TAG,"At "+myPort+"for "+msgobj.id+" prop sent: true");
                                IdToPropSentBoolMap.put(msgobj.id, true);
                                msgobj.PropSentFrom = myPort;
                                Log.e(TAG,"HERE in other process to call specific client task");
                                ProcessProposedNum++;
                                msgobj.ProposedNum.add(ProcessProposedNum + (double) Integer.parseInt(myPort)/100000);

                                PQ.offer(msgobj);
                                Log.e(TAG,"adding this msg"+msgobj.data +" to PQ on this"+myPort);
                                Log.e(TAG,"IdToMsgMap contains the updated msgobj that is being referred by PQ here");
                                IdToMsgMap.put(msgobj.id,msgobj);
                                FullIdToMsgMap.put(msgobj.id,msgobj);
                                new SpecificClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgobj);
                            }

                        }
                        // Proposal sender --------------------------------------------------------------------------------------------------//


                    }
                    catch (ClassNotFoundException ex) {
                        System.err.println("A ClassNotFoundException was caught: " + ex.getMessage());
                        ex.printStackTrace();
                    }
                    server.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ServerTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ServerTask socket IOException");
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String...msgobj) {
            String strReceived = msgobj[0].trim();

            if(!strReceived.equals("QWERTY")){
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append("\n"+strReceived+" "+countI + "\t\n");

                final Uri mUri;
                final ContentValues mContentValues;

                mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
                mContentValues = new ContentValues();
                mContentValues.put("key",Integer.toString(countI));
                mContentValues.put("value", strReceived);
                countI++;

                try {
                    getContentResolver().insert(mUri, mContentValues);
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }

            }
            else {
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append("\n"+msgobj[1] + "\t\n");

            }

            return;
        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

    }



    private class ClientTask extends AsyncTask<MessageC, Void, Void> {


        @Override
        protected Void doInBackground(MessageC... msgobj) {

            remotePort = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
            for (String x: remotePort){
                try {
                    Log.e(TAG,"Sending to "+x+" "+msgobj[0].AgreedNum+msgobj[0].AgreeFlag);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x));
                    ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    clientOutputStream.writeObject(msgobj[0]);
                    socket.close();
                }
                catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask() UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask() socket IOException ");
                    //e.printStackTrace();
                }
            }

            TimerTrigger_initial Tobj = new TimerTrigger_initial(10,msgobj[0]);
            IdToTimer.put(msgobj[0].id,Tobj);

            return null;
        }
    }


    private class NoTimerClientTask extends AsyncTask<MessageC, Void, Void> {


        @Override
        protected Void doInBackground(MessageC... msgobj) {

            remotePort = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
            for (String x: remotePort){
                try {
                    Log.e(TAG,"Sending to "+x+" "+msgobj[0].AgreedNum+msgobj[0].AgreeFlag);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x));
                    ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    clientOutputStream.writeObject(msgobj[0]);
                    socket.close();
                }
                catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask() UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask() socket IOException ");
                    //e.printStackTrace();
                }
            }

            return null;
        }
    }


    private class SpecificClientTask extends AsyncTask<MessageC, Void, Void> {

        @Override
        protected Void doInBackground(MessageC... msgobj) {
            try {
                // Log.e(TAG,"Sending "+msgobj[0].ProposedNum+"to specificclient task");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),msgobj[0].ProcessId);
                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgobj[0]);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }

    public static class MessageC implements Serializable, Comparable<MessageC>{
        private static final long serialVersionUID = -7060210544600464481L;
        String data;
        String id;
        int ProcessId;
        ArrayList<Double> ProposedNum = new ArrayList<Double>();
        double AgreedNum;
        boolean AgreeFlag;
        String PropSentFrom;
        ArrayList<String> PropRecvFrom = new ArrayList<>();
        int delProcId;

        MessageC(String data, int idnum, String port){
            this.data = data;
            this.id = idnum+"."+port;
            this.ProcessId = Integer.parseInt(port);
            this.AgreedNum = 0;
            this.AgreeFlag = false;
            this.delProcId= 0;
        }

        MessageC(){

        }

        @Override
        public int compareTo(MessageC m) {
            Collections.sort(this.ProposedNum);
            Collections.sort(m.ProposedNum);

            if (this.ProposedNum.get(this.ProposedNum.size()-1) > m.ProposedNum.get(m.ProposedNum.size()-1)){
                return 1;
            }

            if (this.ProposedNum.get(this.ProposedNum.size()-1) < m.ProposedNum.get(m.ProposedNum.size()-1)){
                return -1;
            }
            return 0;

        }

    }

    public class TimerTrigger_initial {
        Timer timer;

        public TimerTrigger_initial(int seconds, MessageC msgobj) {
            timer = new Timer();
            timer.schedule(new ActualFunction(msgobj), seconds * 1000);
        }

        class ActualFunction extends TimerTask {
            MessageC msgobj;
            public ActualFunction(MessageC msgobj){
                this.msgobj = msgobj;
            }
            public void run() {
                ArrayList<String> PortArray = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
                Log.e(TAG, "TimerTrigger_propFromitself executed and msg here has prop from" + FullIdToMsgMap.get(msgobj.id).PropRecvFrom);

                    msgobj = FullIdToMsgMap.get(msgobj.id);

                    for(String hju:PortArray){
                        if(!msgobj.PropRecvFrom.contains(hju)){
                            msgobj.delProcId = Integer.parseInt(hju);
                        }
                    }

                    Collections.sort(msgobj.ProposedNum);
                    msgobj.AgreedNum = msgobj.ProposedNum.get(msgobj.ProposedNum.size()-1); // for 5 > set 4
                    msgobj.AgreeFlag = true;
                    Log.e(TAG,"Agreement multicasted"+msgobj.AgreedNum+" "+msgobj.AgreeFlag);
                    new NoTimerClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgobj);

                timer.cancel(); // Terminate the timer thread
            }
        }

    }


    public static PriorityQueue<MessageC> RemovedelProc(PriorityQueue<MessageC> pqw, int delProcId){
        for(MessageC xcv : pqw){
            if(xcv.ProcessId == delProcId){
                pqw.remove(xcv);
                PQMsgIdSet.add(xcv.id);
            }
        }
        return pqw;
    }




}