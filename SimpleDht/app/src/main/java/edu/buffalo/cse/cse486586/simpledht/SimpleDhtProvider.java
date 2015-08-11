package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static String myEmuId_Str="ll"; // identifies the port of the avd eg: 5554
    static String mySucc_Str; // identifies the successor
    static String myPred_Str; // identifies the predecessor
    static String LowestEmu;
    static String HighestEmu;
    static String[] AllEmus= {"empty","empty","empty","empty","empty"};
    static HashMap<String,String> allEmuId_Str_HMap = new HashMap<>(); //keeps a map of emulatorids to their hashes
    static HashMap<String,ArrayList<String>> EmuId_To_PdSc_HMap = new HashMap<>(); //keeps a map of emulatorids to their predecessors and successors
    static Context staticContext;
    static HashMap<String,MatrixCursor> QueryToCsrMap = new HashMap<>();
    static HashMap<String,String> ForStar_Emu_To_KeyMap = new HashMap<>();
    static HashMap<String,String> ForStar_Emu_To_ValMap = new HashMap<>();

    public void SimpleDhtProvider_FirstTime(){

        // identifies the emulator id of the avd eg: 5554
        TelephonyManager tel = (TelephonyManager) this.staticContext.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String emuId_Str = String.valueOf((Integer.parseInt(portStr)));
        // -------------------------- //

        Log.e(TAG, "Begin SimpleDhtProvider constructor");
        myEmuId_Str = emuId_Str;


        // launch a server listen port //
        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(NewAsync.NEW_THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }
        // ----------------------------------- //


        if (!myEmuId_Str.equals("5554")){
            Log.e(TAG,"calling ClientTaskTo5554 by"+myEmuId_Str);
            new ClientTaskTo5554().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, myEmuId_Str);
        } else {
                allEmuId_Str_HMap.put(myEmuId_Str,returnHash(myEmuId_Str));
                Log.e(TAG,"inserted 5554 in map");
                //myPred_Str = myEmuId_Str;
                //mySucc_Str = myEmuId_Str;
                //LowestEmu  = myEmuId_Str;
                //HighestEmu = myEmuId_Str;
                //Log.e(TAG,"set 5554 pred and succ"+myPred_Str+" "+mySucc_Str);
        }



    }



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("query", selection);

        if (mySucc_Str == null && myPred_Str == null) {         // case when only one emulator
            Del_SingleEmulatorQuery(selection);
        }
        else {
            Del_RingEmulatorQuery(selection);
        }
        return 0;
    }


    public void Del_SingleEmulatorQuery(String selection){

      if(selection.equals("\"*\"") || selection.equals("\"@\"")){

            for (String filename:staticContext.fileList()){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    Log.v("file path check", fin.toString());
                    fin.delete();
                } catch (Exception e) {
                    Log.v(TAG, "file read failed");
                }
            }

        }   else {
            LocalSingle_QueryDelete(selection);
        }
    }

    public void Del_RingEmulatorQuery(String selection){

        if(selection.equals("\"*\"") || selection.equals("\"@\"")){
            if(selection.equals("\"@\"")){
                DelAt_InRing();
            }

            if(selection.equals("\"*\"")){
                for (String g:AllEmus){
                    if(!g.equals("empty")){
                        new ClientTask_AtToOneEmu().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, g,"del@");
                    }
                }
            }

        }   else {
            String Destination_Emulator = WhereToSend(selection);
            Log.e(TAG,"destination of delete for this query is "+Destination_Emulator);

            if (Destination_Emulator.equals(myEmuId_Str)){
                LocalSingle_QueryDelete(selection);
            }
            else{
                new ClientTask_ToDestination().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, Destination_Emulator,selection,"del_this");
            }
        }
    }

    public void LocalSingle_QueryDelete(String quer){
        try {
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + quer);
            Log.v("file path check", fin.toString());
            fin.delete();
        } catch (Exception e) {
            Log.v(TAG, "file read and delete failed");
        }
    }

    public void DelAt_InRing(){
        for (String filename:staticContext.fileList()){
            try{
                File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                Log.v("file path check", fin.toString());
                fin.delete();
            } catch (Exception e) {
                Log.v(TAG, "file read failed");
            }
        }
    }
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
            if (mySucc_Str == null && myPred_Str == null){
                String K = values.get("key").toString();
                String V = values.get("value").toString();
                InsertHere(K,V); // insert at this content provider
            } else {
                Log.e(TAG,"Incoming key: "+values.get("key").toString()+" value: "+values.get("value").toString());
                String K = values.get("key").toString();
                String V = values.get("value").toString();

                Log.e(TAG,"this emulator"+myEmuId_Str+" has "+"pred: "+myPred_Str+"succ: "+mySucc_Str);
                String keyHash = returnHash(K);
                String myEmuIdHash = returnHash(myEmuId_Str);
                String PredHash = returnHash(myPred_Str);
                String SuccHash = returnHash(mySucc_Str);
                String LowestHash = returnHash(LowestEmu);
                String HighestHash = returnHash(HighestEmu);

                if(keyHash.compareTo(LowestHash)<=0 || keyHash.compareTo(HighestHash) > 0){
                    // insert at lowestEmu
                    new ClientTask_InsertLowest().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, LowestEmu,K,V); // send key-value to Lowest
                }
                else {

                    if (keyHash.compareTo(myEmuIdHash) <= 0 && keyHash.compareTo(PredHash) >=0){
                        Log.e(TAG,"condition in range met at: "+myEmuId_Str);
                        InsertHere(K,V); // insert at this content provider
                    }

                    else {
                        Log.e(TAG,"PathCheck: key value sending to "+mySucc_Str);
                        new ClientTaskForKV().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, mySucc_Str,K,V); // send key-value to Successor
                    }


                }

            }

        return uri;
    }

    @Override
    public boolean onCreate() {
        staticContext = getContext();
        SimpleDhtProvider_FirstTime();
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor OC = new MatrixCursor(new String[] { "key", "value"});

        Log.v("query", selection);
        String queryArr[] = {selection, myEmuId_Str};

        if (mySucc_Str == null && myPred_Str == null) {         // case when only one emulator
            OC = (MatrixCursor) Final_ProcessSingleEmulatorQuery(selection);
        }
        else {
            OC = (MatrixCursor) ProcessQueryInRing(queryArr);
        }


        return  OC;
    }

    public Cursor ProcessQueryInRing(String[] queryArr){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});

        if(queryArr[0].equals("\"*\"")){

           // send @ to all present emulators and collect at this one
           for (String g:AllEmus){
               if(!g.equals("empty")){
                   new ClientTask_AtToOneEmu().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, g,"\"@\"",myEmuId_Str);
               }
           }

            int reqCount =0;
            while (reqCount<5 && !AllEmus[reqCount].equals("empty")){
                reqCount++;
            }

            while (ForStar_Emu_To_ValMap.keySet().size() < reqCount){
                try{
                    Log.e(TAG,"thread sleep for 100ms");
                    Thread.sleep(100L);
                }catch (InterruptedException e){
                    Log.e(TAG,"InterruptedException");
                    e.printStackTrace();
                }
            }
            LocalCursor = (MatrixCursor)BuildStarCursor(ForStar_Emu_To_KeyMap,ForStar_Emu_To_ValMap);
            ForStar_Emu_To_KeyMap.clear(); // to enable logic for same key again
            ForStar_Emu_To_ValMap.clear(); // to enable logic for same key again

        }

        if(queryArr[0].equals("\"@\"")){

            LocalCursor = (MatrixCursor)ProcessAtInRing();

        }

        if(!queryArr[0].equals("\"*\"") && !queryArr[0].equals("\"@\"")) {

            String Destination_Emulator = WhereToSend(queryArr[0]);
            Log.e(TAG,"destination of query is "+Destination_Emulator);

            if (Destination_Emulator.equals(myEmuId_Str)){
                LocalCursor = (MatrixCursor)RespondToQuery(queryArr[0]);
            }
            else{
                Log.e(TAG,"PathCheck: query sending to ClientTask_ToDestination");
                new ClientTask_ToDestination().executeOnExecutor(NewAsync.SERIAL_EXECUTOR, Destination_Emulator,queryArr[0],myEmuId_Str);
                while (!QueryToCsrMap.containsKey(queryArr[0])){
                    try{
                        Log.e(TAG,"thread sleep for 100ms");
                        Thread.sleep(100L);
                    }catch (InterruptedException e){
                        Log.e(TAG,"InterruptedException");
                        e.printStackTrace();
                    }
                }
                LocalCursor = QueryToCsrMap.get(queryArr[0]);
                QueryToCsrMap.remove(queryArr[0]); // to enable logic for same key again
            }
        }

        return LocalCursor;
    }

    public Cursor BuildStarCursor(HashMap<String,String> kmap,HashMap<String,String> vmap){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});
        String NetKey="";
        String NetVal="";
        ArrayList<String> KeyList = new ArrayList<>();
        ArrayList<String> ValList = new ArrayList<>();
        for(String emu:kmap.keySet()){
            NetKey = NetKey+kmap.get(emu);
            NetVal = NetVal+vmap.get(emu);
        }
        for (String retval: NetKey.split("~")){
            KeyList.add(retval);
        }
        for (String retval: NetVal.split("~")){
            ValList.add(retval);
        }
        for(int i=0;i<KeyList.size();i++){
         LocalCursor.addRow(new String[] {KeyList.get(i),ValList.get(i)});
        }
        return LocalCursor;
    }

    public String WhereToSend(String quer){
        String querhash = returnHash(quer);
        String Lowhash = returnHash(LowestEmu);
        String Hihash = returnHash(HighestEmu);
        String returnval="";
        if (querhash.compareTo(Hihash)>0 || querhash.compareTo(Lowhash)<=0){
            returnval = LowestEmu;
        }
        else{
            for (int i =1; i<=4;i++){
                if(!AllEmus[i].equals("empty") && querhash.compareTo(returnHash(AllEmus[i]))<=0){
                    returnval = AllEmus[i];
                    break;
                }
            }
        }
        return returnval;
    }

    public Cursor ProcessAtInRing(){
        MatrixCursor LCursor = new MatrixCursor(new String[] { "key", "value"});
        for (String filename:staticContext.fileList()){
            try{
                File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                Log.v("file path check", fin.toString());

                FileInputStream fis = new FileInputStream(fin);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line = br.readLine();
                Log.v("check line read", line);
                br.close();
                LCursor.addRow(new String[] {filename,line});

            } catch (Exception e) {
                Log.v(TAG, "file read failed");
            }

        }
        return LCursor;
    }

    public String[] StringProcessAtInRing(){
        String[] STArr = {"",""};

        for (String filename:staticContext.fileList()){
            try{
                File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                Log.v("file path check", fin.toString());

                FileInputStream fis = new FileInputStream(fin);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line = br.readLine();
                Log.v("check line read", line);
                br.close();
                STArr[0] = STArr[0]+"~"+filename;
                STArr[1] = STArr[1]+"~"+line;

            } catch (Exception e) {
                Log.v(TAG, "file read failed");
            }

        }
        return STArr;
    }

    public Cursor Final_ProcessSingleEmulatorQuery(String selection){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});

        if(selection.equals("\"*\"") || selection.equals("\"@\"")){

            for (String filename:staticContext.fileList()){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    Log.v("file path check", fin.toString());

                    FileInputStream fis = new FileInputStream(fin);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    String line = br.readLine();
                    Log.v("check line read", line);
                    br.close();
                    LocalCursor.addRow(new String[] {filename,line});

                } catch (Exception e) {
                    Log.v(TAG, "file read failed");
                }

            }

        }   else {

            try{
                File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + selection);
                Log.v("file path check", fin.toString());

                FileInputStream fis = new FileInputStream(fin);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line = br.readLine();
                Log.v("check line read", line);
                br.close();
                LocalCursor.addRow(new String[] {selection,line});
            } catch (Exception e) {
                Log.v(TAG, "file read failed");
            }

        }

        return LocalCursor;
    }


    public  Cursor RespondToQuery(String query){
        try{
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + query);
            Log.v("file path check", fin.toString());

            FileInputStream fis = new FileInputStream(fin);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = br.readLine();
            Log.v("check line read", line);
            br.close();
            MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
            mc.addRow(new String[] {query,line});

            return mc;
        } catch (Exception e) {
            Log.v(TAG, "file read failed");
        }
        return null;
    }

    public  String[] StringRespondToQuery(String query){
        String[] arr = new String[2];
        try{
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + query);
            Log.v("file path check", fin.toString());

            FileInputStream fis = new FileInputStream(fin);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = br.readLine();
            Log.v("check line read", line);
            br.close();

            arr[0] = query;
            arr[1] = line;


        } catch (Exception e) {
            Log.v(TAG, "file read failed");
        }
        return  arr;
    }


        @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    public static String returnHash(String x){
        String hashedVal = "";

        try {
            hashedVal = genHash(x);
        }catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException @ SimpleDhtActivity");
            e.printStackTrace();
        }

        return hashedVal;
    }

    public void InsertHere(String K, String V){
        try {
            FileOutputStream outputStream;
            outputStream = staticContext.openFileOutput(K, Context.MODE_PRIVATE);
            outputStream.write(V.getBytes());
            Log.e(TAG,"File write done for "+K+" - "+V+" @ "+myEmuId_Str);
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }
    }
}
