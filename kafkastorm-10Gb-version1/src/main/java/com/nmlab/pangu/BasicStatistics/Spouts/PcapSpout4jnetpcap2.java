
package main.java.com.nmlab.pangu.BasicStatistics.Spouts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.com.nmlab.pangu.BasicStatistics.Helpers.CreateValue;

import org.jnetpcap.Pcap;
import org.jnetpcap.PcapHeader;
import org.jnetpcap.PcapIf;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.network.Ip6;
import org.jnetpcap.protocol.tcpip.Http;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;
import org.jnetpcap.nio.JBuffer;
import org.jnetpcap.packet.format.FormatUtils;
import org.jnetpcap.packet.JRegistry;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import main.java.com.nmlab.pangu.BasicStatistics.Helpers.KafkaProperties;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class PcapSpout4jnetpcap2 implements IRichSpout {

	private SpoutOutputCollector outputCollector;
	PcapIf device;
	Pcap pcap; 
	StringBuilder errbuf = new StringBuilder(); // For any error msgs
	//vars
	//private PcapPacket packet = null;  
	//private Tcp tcp = null;
	//private Udp udp = null;
	//private Http http = null;
	private CreateValue create;
	
	
	//start capture
	private String deviceName = null;
	private int count = -1;
	private String filter = null;
	private String srcFilename =null ;
	private String dstFilename = null;
	private int sampLen = 64*1024;
	public int countPacket = 0;
	private int flags = Pcap.MODE_PROMISCUOUS; // capture all packets
	private int timeout = 10 ; // 10 seconds in millis
	
	
	private ConsumerConnector consumer;
	
    private String topic=null;
    private Map<String, Integer> topicCountMap;
    private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap ;
    private KafkaStream<byte[], byte[]> stream ;
    private ConsumerIterator<byte[], byte[]> it;
    
    
    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", "group2");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
	
		
	//test
	Ip4 ip4;
	Ip6 ip6;
	int id;
	long time = 0;
	 long slots = 0;
	 long throughput = 0;
	 //PcapPacket packet;
	// long countPacket = 0;
	 //PcapHeader hdr;  
     	//JBuffer buf; 
	PcapPacket packet;
    public PcapSpout4jnetpcap2(){};
   
    
    public PcapSpout4jnetpcap2(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen,String topic){
    	this.deviceName = deviceName;
    	this.count = count; //閺堫亙濞囬悽顭掔礉閺冪姵鏅�
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 64*1024;
    	this.sampLen = sampLen;
    	
    	this.topic=topic;

    }
    public PcapSpout4jnetpcap2(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen,String topic){
    	this.deviceName = deviceName;
    	this.count = Integer.parseInt(count); //閺堫亙濞囬悽顭掔礉閺冪姵鏅�
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	int slen=0;
    	if(Integer.parseInt(sampLen)<0)
            slen = 64*1024;// Capture all packets, no trucation
    	this.sampLen = slen;
    	
    	this.topic=topic;

    }

    PcapIf getDevice(){
    	List<PcapIf> alldevs = new ArrayList<PcapIf>(); // Will be filled with NICs
    	int r = Pcap.findAllDevs(alldevs, errbuf);
		if (r == Pcap.NOT_OK || alldevs.isEmpty()) {
			System.err.printf("Can't read list of devices, error is %s", errbuf.toString());
			return null;
		}
		int i = 0,chooseid=0;
		for (PcapIf device : alldevs) {
			String description =(device.getDescription() != null) ? device.getDescription(): "No description available";
			if(deviceName!=null && deviceName!="" && device.getName().equals(deviceName))
				chooseid=i;
			System.out.printf("#%d: %s [%s]\n", i++, device.getName(), description);
		}
		return alldevs.get(chooseid);
    }

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = spoutOutputCollector;		
        try { 
        	
        	this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                    createConsumerConfig());
        	this.topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, new Integer(1));
            this.consumerMap = consumer.createMessageStreams(topicCountMap);
            this.stream = consumerMap.get(topic).get(0);
            this.it = stream.iterator();
        	
        	//open device
        	if(srcFilename!=null){
        		System.out.println("before open");
        		pcap=Pcap.openOffline(srcFilename, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening srcfile  for capture: "+ errbuf.toString());
        			return;
        		}
			id = JRegistry.mapDLTToId(pcap.datalink());
			 //hdr = new PcapHeader(JMemory.POINTER);  
			 //buf = new JBuffer(JMemory.POINTER);
			 packet=new PcapPacket(JMemory.POINTER);
			 create= new CreateValue(); 
				System.out.println("after open");
        	}
        	else
        	{
        		this.sampLen=64*1024;
        		device = getDevice();
        		System.out.println(device);
        		System.out.println(this.sampLen);
        		
        		pcap =Pcap.openLive(device.getName(), this.sampLen, this.flags, this.timeout, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening device for capture: "+ errbuf.toString());
        			return;
        		}
			id = JRegistry.mapDLTToId(pcap.datalink());
			 //hdr = new PcapHeader(JMemory.POINTER);  
			 //buf = new JBuffer(JMemory.POINTER);
			 packet=new PcapPacket(JMemory.POINTER);
			create= new CreateValue();
        	}
        	ip4=new Ip4();	
        	ip6=new Ip6();
        	//apply space
        	//hdr = new PcapHeader();  
           // buf = new JBuffer(JMemory.POINTER);
            //this.packet =new PcapPacket(hdr,buf); 
        	
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void nextTuple() {
		//int flag=0;
		// TODO Auto-generated method stub
		//System.out.println("This is before nextTuple try");
		try{
			if(this.it.hasNext())
			{
				//countPacket++;
				//System.out.println("countPacket:"+countPacket);
				//System.out.println("if(this.it.hasNext())");
				byte[] dstpacket=it.next().message();
				//System.out.println("dstpacket.toString():"+new String(dstpacket));
				//System.out.println("byte[] dstpacket=it.next().message();");
				//System.out.println("dstpacket.length:"+dstpacket.length);
				PcapPacket p1 = new PcapPacket(dstpacket);
				
				//System.out.println("p1.toString():"+p1.toString());
				//System.out.println("PcapPacket p1 = new PcapPacket(dstpacket);");
				//System.out.println("p1.getCaptureHeader().seconds():"+p1.getCaptureHeader().seconds());
				//System.out.println("p1.getCaptureHeader().caplen():"+p1.getCaptureHeader().caplen());
				//System.out.println("p1.getTotalSize():"+p1.getTotalSize());
				this.outputCollector.emit("basic",new Values(p1.getCaptureHeader().seconds(), null , null,p1.getCaptureHeader().wirelen()));
				p1=null;
				//System.out.println("this.outputCollector.emit(basic,new Values(p1.getCaptureHeader().seconds(), null , null,p1.getCaptureHeader().caplen()));");
			}
		} catch(Exception e) {
			    	System.out.println("fail to deal with packet");
			    }
			    //this.outputCollector.emit("basic",create.createValues(packet));
			

		//System.out.println("This is after nextTuple try");
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		//outputFieldsDeclarer.declare(new Pcap().createFields());
		outputFieldsDeclarer.declareStream("basic",new Fields("sec","src","dst","len"));//后面bolt都依据这个方法来定义字段
		outputFieldsDeclarer.declareStream("udp",new Fields("sec","src","dst","len","src_port","dst_port"));
		outputFieldsDeclarer.declareStream("tcp",new Fields("sec","src","dst","len","src_port","dst_port"));
		//outputFieldsDeclarer.declareStream("ip",new Fields("sec","src","dst","len"));
		outputFieldsDeclarer.declareStream("http",new Fields());
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
    	public void ack(Object arg0) {
    		// TODO Auto-generated method stub
    		
    	}

    	public void activate() {
    		// TODO Auto-generated method stub
    		
    	}

    	public void close() {
    		// TODO Auto-generated method stub
    		pcap.close();
    	}

    	public void deactivate() {
    		// TODO Auto-generated method stub
    		
    	}

    	public void fail(Object arg0) {
    		// TODO Auto-generated method stub
    		
    	}

}
