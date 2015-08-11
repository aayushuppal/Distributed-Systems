package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {


    static String TAG = ServerTask.class.getSimpleName();
    static String[] strArray;
    static String temp_lowest;
    static String temp_highest;
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];

        while(true) {
            try {

                Socket server = serverSocket.accept();
                ObjectInputStream clientInputStream = new ObjectInputStream(server.getInputStream());

                try{
                    strArray = (String[])clientInputStream.readObject();
                }
                catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask ClassNotFoundException");
                e.printStackTrace();
                } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
                e.printStackTrace();
                } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException");
                e.printStackTrace();
                }


                // Message type: set predecessor and successor //
                if (strArray[0].equals("set_pred_succ")){
                    SimpleDhtProvider.myPred_Str = strArray[1];
                    SimpleDhtProvider.mySucc_Str = strArray[2];
                    SimpleDhtProvider.LowestEmu = strArray[3];
                    SimpleDhtProvider.HighestEmu = strArray[4];
                    SimpleDhtProvider.AllEmus[0] = strArray[5];
                    SimpleDhtProvider.AllEmus[1] = strArray[6];
                    SimpleDhtProvider.AllEmus[2] = strArray[7];
                    SimpleDhtProvider.AllEmus[3] = strArray[8];
                    SimpleDhtProvider.AllEmus[4] = strArray[9];

                    Log.e(TAG,"Set values: "+SimpleDhtProvider.myEmuId_Str+" "+SimpleDhtProvider.myPred_Str+" "+SimpleDhtProvider.mySucc_Str+" "+SimpleDhtProvider.LowestEmu+" "+SimpleDhtProvider.HighestEmu+" "+SimpleDhtProvider.AllEmus[0]+" "+SimpleDhtProvider.AllEmus[1]+" "+SimpleDhtProvider.AllEmus[2]+" "+SimpleDhtProvider.AllEmus[3]+" "+SimpleDhtProvider.AllEmus[4]);
                }
                // ---------------------------------------- //


                // Message type: inform emulator id //
                if (strArray[0].equals("inform_emu_id")){

                    String temp = strArray[1];

                    Log.e(TAG, "server listens at emulator"+SimpleDhtProvider.myEmuId_Str+" "+temp);

                    if (!SimpleDhtProvider.allEmuId_Str_HMap.containsKey(temp)){
                        SimpleDhtProvider.allEmuId_Str_HMap.put(temp,SimpleDhtProvider.returnHash(temp));
                    }

                    SetPredSucc(SimpleDhtProvider.allEmuId_Str_HMap);

                    for (String x:SimpleDhtProvider.EmuId_To_PdSc_HMap.keySet()){
                        new ClientTaskToSpecific().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, x,SimpleDhtProvider.EmuId_To_PdSc_HMap.get(x).get(0),SimpleDhtProvider.EmuId_To_PdSc_HMap.get(x).get(1),temp_lowest,temp_highest,SimpleDhtProvider.AllEmus[0],SimpleDhtProvider.AllEmus[1],SimpleDhtProvider.AllEmus[2],SimpleDhtProvider.AllEmus[3],SimpleDhtProvider.AllEmus[4] );
                    }
                }
                // ------------------------------------ //

                // Message type: key value pair //
                if (strArray[0].equals("K_V")){
                        Log.e(TAG,"key value received "+strArray);
                        publishProgress(strArray);
                }
                // ------------------------------------ //

                // Message type: InsertLowest_K_V //
                if (strArray[0].equals("InsertLowest_K_V")){
                    Log.e(TAG,"insert lowest key value received "+strArray);
                    SimpleDhtProvider obj1 = new SimpleDhtProvider();
                    obj1.InsertHere(strArray[1],strArray[2]);
                }
                // ------------------------------------ //      FinalQuery_Lowest


                // Message type: QueryIn@Destination //
                if (strArray[0].equals("QueryIn@Destination")){
                    Log.e(TAG,"finally process query here"+strArray);
                    SimpleDhtProvider obj1 = new SimpleDhtProvider();

                    String keyString = obj1.StringRespondToQuery(strArray[1])[0];
                    String valString = obj1.StringRespondToQuery(strArray[1])[1];
                    Log.e(TAG,"cursor string: "+keyString+" "+valString);
                    new ClientTask_FinalReturn().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, strArray[2],keyString,valString);
                }
                // ------------------------------------ //


                // Message type: Final_Val_Return //
                if (strArray[0].equals("Final_Val_Return")){
                    Log.e(TAG,"final val here"+strArray);
                    String keyString = strArray[1];
                    String valString = strArray[2];
                    MatrixCursor csr2 = new MatrixCursor(new String[] { "key", "value"});
                    csr2.addRow(new String[] {keyString,valString});
                    SimpleDhtProvider.QueryToCsrMap.put(keyString,csr2);
                }
                // ------------------------------------ //


                // Message type: Process@here //
                if (strArray[0].equals("Process@here")){
                    Log.e(TAG,"@ process query here"+strArray);
                    SimpleDhtProvider obj1 = new SimpleDhtProvider();
                    String[] StOneArr = obj1.StringProcessAtInRing();
                    new ClientTask_ReturnAtToHost().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, strArray[2],StOneArr[0],StOneArr[1]);
                }
                // ------------------------------------ //

                // Message type: AtCollection //
                if (strArray[0].equals("AtCollection")){
                    Log.e(TAG,"@ collection here "+strArray);

                    if(!SimpleDhtProvider.ForStar_Emu_To_KeyMap.containsKey(strArray[3])){
                        SimpleDhtProvider.ForStar_Emu_To_KeyMap.put(strArray[3],strArray[1]);
                        SimpleDhtProvider.ForStar_Emu_To_ValMap.put(strArray[3],strArray[2]);
                    }
                }
                // ------------------------------------ //

                // Message type: del@ //
                if (strArray[0].equals("del@")){
                    Log.e(TAG,"@ del here"+strArray);
                    SimpleDhtProvider obj1 = new SimpleDhtProvider();
                    obj1.DelAt_InRing();
                }
                // ------------------------------------ //

                // Message type: del_this //
                if (strArray[0].equals("del_this")){
                    Log.e(TAG,"single query del here from server"+strArray);
                    SimpleDhtProvider obj1 = new SimpleDhtProvider();
                    obj1.LocalSingle_QueryDelete(strArray[1]);
                }
                // ------------------------------------ //


                server.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException");
                e.printStackTrace();
            }
        }

    }

    protected void onProgressUpdate(String...stArr){



            final Uri mUri;
            final ContentValues mContentValues;

            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            mContentValues = new ContentValues();
            mContentValues.put("key",stArr[1]);
            mContentValues.put("value", stArr[2]);

            try {
                SimpleDhtProvider obj = new SimpleDhtProvider();
                obj.insert(mUri, mContentValues);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }



    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static void SetPredSucc(HashMap<String,String> Hmp){
       HashMap<String,String> InvHmp = new HashMap<>(); // map of hashvalue - emulatorid

       for (String key:Hmp.keySet()){
           InvHmp.put(Hmp.get(key),key);
       }

        ArrayList<String> hashVal_Lst = new ArrayList<>();
        hashVal_Lst.addAll(InvHmp.keySet()) ;
        Collections.sort(hashVal_Lst);
        ArrayList<String> Ordered_EmIds = new ArrayList<>();

        Log.e(TAG,"-----------------");
        int ic =0;
        for (String xc: hashVal_Lst){
            Log.e(TAG,""+xc+": "+InvHmp.get(xc));
            Ordered_EmIds.add(InvHmp.get(xc));
            SimpleDhtProvider.AllEmus[ic]=InvHmp.get(xc);
            ic++;
        }
        Log.e(TAG,"-----------------");
        Log.e(TAG,""+Ordered_EmIds);
        temp_lowest = Ordered_EmIds.get(0);
        temp_highest = Ordered_EmIds.get(Ordered_EmIds.size()-1);

        String tmp_pred ="";
        String tmp_succ ="";
        String tmp_em_id = "";
        ArrayList<String> tmp_lst = new ArrayList<>();

        SimpleDhtProvider.EmuId_To_PdSc_HMap.clear();

        for (int i =0; i<Ordered_EmIds.size(); i++){


            if (i == 0){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(Ordered_EmIds.size()-1);
                tmp_succ = Ordered_EmIds.get(i+1);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDhtProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }

            if (i == Ordered_EmIds.size()-1){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(i-1);
                tmp_succ = Ordered_EmIds.get(0);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDhtProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }

            if (i != 0 && i != Ordered_EmIds.size()-1){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(i-1);
                tmp_succ = Ordered_EmIds.get(i+1);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDhtProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }
        }


    }

}
