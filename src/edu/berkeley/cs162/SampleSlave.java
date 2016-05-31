package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Random;

import static edu.berkeley.cs162.StaticConstants.*;

public class SampleSlave {

    static String logPath1;
    static TPCLog log1;
    static KVServer keyServer1;
    static SocketServer server1;
    static long slaveID1;
    
    
    static String logPath2;
    static TPCLog log2;
    static KVServer keyServer2;
    static SocketServer server2;
    static long slaveID2;
    
    static String masterHostname;

    static int masterPort = 8080;
    static int registrationPort = 9090;

    public static void main(String[] args) throws IOException, KVException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Need master IP address");
        }

        Random rand = new Random();
        slaveID1 = Math.abs(rand.nextLong());

        masterHostname = args[0];
        if (masterHostname.charAt(0) == '$') {
            masterHostname = InetAddress.getLocalHost().getHostAddress();
        }
        System.out.println("Looking for master at " + masterHostname);    
        
        keyServer1 = new KVServer(100, 10);
        server1 = new SocketServer(InetAddress.getLocalHost().getHostAddress(),3030);
        logPath1 = "/Users/ejmnorl/Desktop/log1/" + slaveID1 + "@" + server1.hostname;
        System.out.println("logPath1 = " + logPath1);
        log1 = new TPCLog(logPath1, keyServer1);
       
        TPCMasterHandler handler = new TPCMasterHandler(slaveID1, keyServer1, log1);
        server1.addHandler(handler);
        
        handler.registerWithMaster(masterHostname, server1);
        
        System.out.println("BEFORE CONNECT...");       
        server1.connect();
        System.out.println("AFTER CONNECT....");
        
   //     handler.registerWithMaster(masterHostname, server1);
       

        System.out.println("Starting SlaveServer " + slaveID1 + " at " +
            server1.hostname + ":" + server1.port);  
        
        
        server1.run();  
      
        
        //-----------------------------------
        
  
  
        /*
        Random rand2 = new Random();
        long slaveID2 = rand.nextLong();

       
        keyServer2 = new KVServer(100, 10);
        
        server2 = new SocketServer(InetAddress.getLocalHost().getHostAddress(),4040);
        logPath2 = "/Users/ejmnorl/Desktop/log2/" + slaveID2 + "@" + server2.hostname;
        System.out.println("logPath2 = " + logPath2);
        log2 = new TPCLog(logPath2, keyServer2);

        
        TPCMasterHandler handler2 = new TPCMasterHandler(slaveID2, keyServer2, log2);
        server2.addHandler(handler2);
        server2.connect();



        handler.registerWithMaster(masterHostname, server2);

        System.out.println("Starting SlaveServer " + slaveID2 + " at " +
            server2.hostname + ":" + server2.port);
        
        
        server2.run();
        */
        
    }
    
   

}
