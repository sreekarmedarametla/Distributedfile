/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.app;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;
	private String leaderHost="";
	private int leaderPort=0;
	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}
	
	 private ArrayList<ByteString> divideFileChunks(File file) throws IOException {
	        ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
	        int sizeOfFiles = 1024 * 1024; // equivalent to 1 Megabyte
	        byte[] buffer = new byte[sizeOfFiles];

	        try {
	            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
	            int tmp = 0;
	            while ((tmp = bis.read(buffer)) > 0) {
	                ByteString byteString = ByteString.copyFrom(buffer, 0, tmp);
	                chunkedFile.add(byteString);
	            }
	            return chunkedFile;
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null;
	        }
	    }

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		System.out.println("---> Host : " + msg.getLeaderroute().getHost());
		System.out.println("---> Port : " + msg.getLeaderroute().getPort());
		leaderHost=msg.getLeaderroute().getHost();
		leaderPort=msg.getLeaderroute().getPort();
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "localhost";
		int port = 4168;

		try {
			MessageClient mc = new MessageClient(host, port);			
			DemoApp da = new DemoApp(mc);
			//mc.askForLeader();
			
			int choice = 0;
			Scanner s=new Scanner(System.in);
            while (true) {
                System.out.println("Enter your option \n1. WRITE a file. \n2. READ a file. \n3. Update a File. \n4. Delete a File\n 5 Ping(Global)\n 6 Exit");
                choice = s.nextInt();
                switch (choice) {
                    case 1: 
                        System.out.println("Enter the full pathname of the file to be written ");
                        String currFileName = s.next();
                        File file = new File(currFileName);
                        if (file.exists()) {
                            ArrayList<ByteString> chunkedFileList = da.divideFileChunks(file);
                            String name = file.getName();
                            int i = 0;                            
                            for (ByteString string : chunkedFileList) {
                                mc.writeFile(name, string, chunkedFileList.size(), i++);
                            }
                        } else {
                            throw new FileNotFoundException("File does not exist in this path ");
                        } 
                    break;
                    default:
                    	System.out.println("Invalid option");                   
                }
            }           
		} catch (Exception e) {
			e.printStackTrace();
		} /*finally {
			CommConnection.getInstance().release();
		}*/
	}
}
