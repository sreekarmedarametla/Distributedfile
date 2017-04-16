package raft;


import java.util.concurrent.LinkedBlockingDeque;

import gash.router.server.PrintUtil;
import gash.router.server.edges.EdgeInfo;
import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.Request;
import pipe.common.Common.WriteBody;
import pipe.election.Election.LeaderStatus;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import routing.Pipe.CommandMessage;


public class LeaderState implements RaftState {
 private RaftManager Manager;
 LinkedBlockingDeque<WorkMessage> chunkMessageQueue=new LinkedBlockingDeque<WorkMessage>();
 LinkedBlockingDeque<WorkMessage> TemporaryMessageQueue=new LinkedBlockingDeque<WorkMessage>();
 double logCount=0;
 double clusterSize=0;
	
 @Override
 	public synchronized void process() { 
		System.out.println("In leaders process method"); 
	 	try{
	 		  if(chunkMessageQueue.isEmpty())
	 		  {	  
			    for(EdgeInfo ei:Manager.getEdgeMonitor().getOutBoundEdges().map.values())
			    {
				  if(ei.isActive()&&ei.getChannel()!=null)
				   {							
					Manager.getEdgeMonitor().sendMessage(createHB());
					System.out.println("sent hb to"+ei.getRef());
					clusterSize++;
				    }				
			    
			    }
	 		  } 
			     if(!chunkMessageQueue.isEmpty())
			     {
			    	 System.out.println("before taking message "+ chunkMessageQueue.size());
			    	 for(EdgeInfo ei:Manager.getEdgeMonitor().getOutBoundEdges().map.values())
					 {
						if(ei.isActive()&&ei.getChannel()!=null)
						{							
							Manager.getEdgeMonitor().sendMessage(createAppendandHeartbeat());
							System.out.println("after taking message queue size "+ chunkMessageQueue.size());
							
		                  
							
						}				
					 }
			    	 
			     } 
			    
	 		 
	 		 
	 			
	 			 
	 		 
			
		 }
		 catch(Exception e){
			 e.printStackTrace();
		 }
		 
	 }
 	
	//CREATE HEARTBEAT
	public WorkMessage createHB() {
	WorkState.Builder sb = WorkState.newBuilder();
	sb.setEnqueued(-1);
	sb.setProcessed(-1);
	
	Heartbeat.Builder bb = Heartbeat.newBuilder();
	bb.setState(sb);
	
	Header.Builder hb = Header.newBuilder();
	hb.setNodeId(Manager.getNodeId());
	hb.setDestination(-1);
	hb.setTime(System.currentTimeMillis());
	
	LeaderStatus.Builder lb=LeaderStatus.newBuilder();
	lb.setLeaderId(Manager.getNodeId());
	lb.setLeaderTerm(Manager.getTerm());
	
	WorkMessage.Builder wb = WorkMessage.newBuilder();		
	wb.setHeader(hb);		
	wb.setBeat(bb);
	wb.setLeader(lb);
	wb.setSecret(10);				
	return wb.build();	
	}
	
	//creating heartbeat and appendentry
public synchronized WorkMessage	createAppendandHeartbeat()
{
  if(!chunkMessageQueue.isEmpty())
  {	
	WorkMessage msg=null;
	try {
		msg = chunkMessageQueue.take();
		addToTemporaryQueue(msg);
		
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}  
	
	WorkState.Builder sb = WorkState.newBuilder();
	sb.setEnqueued(-1);
	sb.setProcessed(-1);
	
	Heartbeat.Builder bb = Heartbeat.newBuilder();
	bb.setState(sb);
	
	Header.Builder hb = Header.newBuilder();
	hb.setNodeId(Manager.getNodeId());
	hb.setDestination(-1);
	hb.setTime(System.currentTimeMillis());
	
	LeaderStatus.Builder lb=LeaderStatus.newBuilder();
	lb.setLeaderId(Manager.getNodeId());
	lb.setLeaderTerm(Manager.getTerm());
	Chunk.Builder chb=Chunk.newBuilder();
	chb.setChunkId(msg.getRequest().getRwb().getChunk().getChunkId());
	chb.setChunkData(msg.getRequest().getRwb().getChunk().getChunkData());
	chb.setChunkSize(msg.getRequest().getRwb().getChunk().getChunkSize());
	
	WriteBody.Builder wb=WriteBody.newBuilder();
	
	wb.setFilename(msg.getRequest().getRwb().getFilename());
	wb.setChunk(chb);
	wb.setNumOfChunks(msg.getRequest().getRwb().getNumOfChunks());
	
	Request.Builder rb = Request.newBuilder();
	//request type, read,write,etc				
	rb.setRwb(wb);	
	rb.setRequestType(Request.RequestType.WRITEFILE);

	
	
	
	WorkMessage.Builder wbs = WorkMessage.newBuilder();		
	wbs.setHeader(hb);		
	wbs.setBeat(bb);
	wbs.setLeader(lb);
	wbs.setSecret(10);	
	wbs.setRequest(rb);
	System.out.println("i am working fine from appendentry method");
	PrintUtil.printWork(wbs.build());
	
	return wbs.build();	
  }
  else
  {
	  return createHB();
  }
	
}

 // adding to temporary queue
public void addToTemporaryQueue(WorkMessage msg)
{
	try {
		TemporaryMessageQueue.put(msg);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  	

}

	 
 //received hearbeat no need to implement here
 public synchronized void receivedHeartBeat(WorkMessage msg)
 {
	    Manager.randomizeElectionTimeout();		
		System.out.println("received hearbeat from the Leader: "+msg.getLeader().getLeaderId());
		PrintUtil.printWork(msg);		
		Manager.setCurrentState(Manager.Follower);
		Manager.setLastKnownBeat(System.currentTimeMillis());
 }
 
   @Override
	public synchronized void setManager(RaftManager Mgr) {
		this.Manager = Mgr;
	}

	@Override
	public synchronized RaftManager getManager() {
		return Manager;
	}

	 @Override
	 public void onRequestVoteReceived(WorkMessage msg) {
	 	// TODO Auto-generated method stub
	 	
	 }	
	 @Override
	 public void receivedVoteReply(WorkMessage msg) {
	 	// TODO Auto-generated method stub
		 return;
	 	
	 }	
	  public void receivedLogToWrite(CommandMessage msg)
		{
			System.out.println("reached leader now ");
			System.out.println("building work message");
			
			WorkMessage wm=buildWorkMessage(msg); 
			
			try {
				chunkMessageQueue.put(wm);
			System.out.println("size of queue is "+chunkMessageQueue.size());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("queue size is "+chunkMessageQueue.size());
			//PrintUtil.printWork(wm);
//			for(EdgeInfo ei:Manager.getEdgeMonitor().getOutBoundEdges().map.values())
//			{
//				if(ei.getChannel()!=null&	&ei.isActive())
//				{
//					Manager.getEdgeMonitor().sendMessage(wm);
//					
//				}
//			}
			
		}
	  
	  public WorkMessage buildWorkMessage(CommandMessage msg)
	  {
		System.out.println("wmsg fun");
		   
		    Header.Builder hb = Header.newBuilder();
			hb.setNodeId(Manager.getLeaderId());
			hb.setTime(System.currentTimeMillis());
			hb.setDestination(-1);
			
			Chunk.Builder chb=Chunk.newBuilder();
			chb.setChunkId(msg.getRequest().getRwb().getChunk().getChunkId());
			chb.setChunkData(msg.getRequest().getRwb().getChunk().getChunkData());
			chb.setChunkSize(msg.getRequest().getRwb().getChunk().getChunkSize());
			
			WriteBody.Builder wb=WriteBody.newBuilder();
			
			wb.setFilename(msg.getRequest().getRwb().getFilename());
			wb.setChunk(chb);
			wb.setNumOfChunks(msg.getRequest().getRwb().getNumOfChunks());
			
			Request.Builder rb = Request.newBuilder();
			//request type, read,write,etc				
			rb.setRwb(wb);	
			rb.setRequestType(Request.RequestType.WRITEFILE);
			WorkMessage.Builder wbs = WorkMessage.newBuilder();
			// Prepare the CommandMessage structure
		
			wbs.setHeader(hb);
			wbs.setRequest(rb);
			wbs.setSecret(10);
			System.out.println("retunr fun");
			return wbs.build();
	  }
	  
	  
	  public void chunkReceived(WorkMessage msg)
	  {
		  return;
	  }
	  
	  public void responseToChuckSent(WorkMessage msg)
	  {
		  logCount++;
		  System.out.println("log count is"+ logCount);
		  System.out.println("cluster size is "+clusterSize);
		  System.out.println("inside the response to chunk sent method");
		  System.out.println("reached step1");
		  if(logCount>=(clusterSize/2))
			{
				
				clusterSize=0;
				logCount=0;
							
			}		  
	  }
	 
}









