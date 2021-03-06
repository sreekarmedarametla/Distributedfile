package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.work.Work.WorkMessage;
import raft.RaftManager;
import routing.Pipe.CommandMessage;
import routing.Pipe.LeaderRoute;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	private RaftManager Manager;
	ServerState state;
	public CommandHandler(ServerState state,RoutingConf conf) {
		this.state=state;
		if (conf != null) {
			this.conf = conf;
		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		try {
			// TODO How can you implement this without if-else statements?
			if (msg.hasPing()) {
				
				logger.info("ping from " + msg.getHeader().getNodeId()+" to "+msg.getHeader().getDestination());
				PrintUtil.printCommand(msg);
				
			}			
			else if (msg.getRequest().hasRwb()){
				System.out.println("has write request");				
//				System.out.println("File id "+msg.getRequest().getRwb().getFileId());
//				System.out.println("File name "+msg.getRequest().getRwb().getFilename());
//				System.out.println("Chunk id "+msg.getRequest().getRwb().getChunk().getChunkId());
//				System.out.println("Chunk size "+msg.getRequest().getRwb().getChunk().getChunkSize());
//				System.out.println("Chunk data "+msg.getRequest().getRwb().getChunk().getChunkData());
//				System.out.println("Numbr of ChunkS "+msg.getRequest().getRwb().getNumOfChunks());
		
				if(state.getManager().getLeaderId()==state.getManager().getNodeId())
				{		
				    state.getManager().getCurrentState().receivedLogToWrite(msg);
				}
				
			}else 
			if (msg.hasWhoisleader()) {				
					System.out.println("asked for who is leader");	
					/*LeaderRoute.Builder lrb=LeaderRoute.newBuilder();
					lrb.setHost(Manager.getLeaderHost());
					lrb.setPort(Manager.getLeaderPort());
					CommandMessage.Builder cmb=CommandMessage.newBuilder();
					cmb.setLeaderroute(lrb);				 
					channel.writeAndFlush(cmb.build());*/
					
					
			}						
			else {
			}
			

		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}