package main.java.com.nmlab.pangu.BasicStatistics.Helpers;

import java.io.UnsupportedEncodingException;

import org.jnetpcap.PcapHeader;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.format.FormatUtils;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.network.Ip6;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;

import backtype.storm.tuple.Values;

public class CreateValue {
	public CreateValue(){}
	
	public Values createValues(PcapPacket pkt,Ip6 ip6,Ip4 ip4) throws UnsupportedEncodingException {
		
		//final Ip6 ip6 = new Ip6();
		//final Ip4 ip4 = new Ip4();
		//System.out.println("in createValues");
		String src = null;
		String dst = null;
		long sec = pkt.getCaptureHeader().seconds();
		int len = pkt.getCaptureHeader().caplen();
		//System.out.println("sec:"+sec);
		//System.out.println("len:"+len);
		//System.out.println("before pkt.hasHeader(Ip4.ID)");
		if(pkt.hasHeader(Ip4.ID))
		{
			//System.out.println("in pkt.hasHeader(Ip4.ID)");
			pkt.getHeader(ip4);
			src = FormatUtils.ip(ip4.source());
			dst = FormatUtils.ip(ip4.destination());
		}
		if(pkt.hasHeader(Ip6.ID))
		{
			//System.out.println("in pkt.hasHeader(Ip6.ID)");
			pkt.getHeader(ip6);
			src = FormatUtils.asStringIp6(ip6.source(),false);
			dst = FormatUtils.asStringIp6(ip6.destination(),false);
		}
		//System.out.println("src:"+src);
		//System.out.println("dst:"+dst);
        return new Values(
                sec,
                src,
                dst,
                len
        );
	}
	
	public Values createValues(PcapPacket pkt, Tcp tcp,Ip6 ip6,Ip4 ip4) throws UnsupportedEncodingException {
		//final Ip6 ip6 = new Ip6();
		//final Ip4 ip4 = new Ip4();
		String src = null;
		String dst = null;
		long sec = pkt.getCaptureHeader().timestampInMillis();
		int len = pkt.getCaptureHeader().caplen();
		if(pkt.hasHeader(Ip4.ID))
		{
			pkt.getHeader(ip4);
			src = FormatUtils.ip(ip4.source());
			dst = FormatUtils.ip(ip4.destination());
		}
		if(pkt.hasHeader(Ip6.ID))
		{
			pkt.getHeader(ip6);
			src = FormatUtils.asStringIp6(ip6.source(),false);
			dst = FormatUtils.asStringIp6(ip6.destination(),false);
		}
		int src_port = tcp.source();
		int dst_port = tcp.destination();
        return new Values(
                sec,
                src,
                dst,
                len,
                src_port,
                dst_port
        );
	}
	
	public Values createValues(PcapPacket pkt, Udp udp,Ip6 ip6,Ip4 ip4) throws UnsupportedEncodingException {
		//final Ip6 ip6 = new Ip6();
		//final Ip4 ip4 = new Ip4();
		String src = null;
		String dst = null;
		long sec = pkt.getCaptureHeader().timestampInMillis();
		int len = pkt.getCaptureHeader().caplen();
		if(pkt.hasHeader(Ip4.ID))
		{
			pkt.getHeader(ip4);
			src = FormatUtils.ip(ip4.source());
			dst = FormatUtils.ip(ip4.destination());
		}
		if(pkt.hasHeader(Ip6.ID))
		{
			pkt.getHeader(ip6);
			src = FormatUtils.asStringIp6(ip6.source(),false);
			dst = FormatUtils.asStringIp6(ip6.destination(),false);
		}
		int src_port = udp.source();
		int dst_port = udp.destination();
		
        return new Values(
                sec,
                src,
                dst,
                len,
                src_port,
                dst_port
        );
	}
	
	public Values createValues(PcapPacket packet, Http http) {
	
		
        return new Values(
        );
	}
}
