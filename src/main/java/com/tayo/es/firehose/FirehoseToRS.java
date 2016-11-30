package com.tayo.es.firehose;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class FirehoseToRS 
{
	public static final String DELIVERY_STREAM_NAME = "RS-Test";
	public static void main (String [] args)
	{
		AWSCredentials credentials = new  ProfileCredentialsProvider("default").getCredentials();
		AmazonKinesisFirehoseClient fire = new AmazonKinesisFirehoseClient(credentials);
		DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
		describeDeliveryStreamRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);
		
		try
		{
			PutRecordRequest putRecReq = new PutRecordRequest();
			putRecReq.setDeliveryStreamName(DELIVERY_STREAM_NAME);
			//String obj = "18 | gbemi | olajide | Senator";
			String obj = "11|vuga00|love|Governor|xxxx|2016-10-12 08:15:22|2016-10-11 08:24:32|2";
			ByteBuffer buffer = ByteBuffer.wrap(obj.toString().getBytes("UTF-8"));
			Record record = new Record(); 
			record.setData(buffer);				
			putRecReq.setRecord(record);				
			PutRecordResult putRecResult = new PutRecordResult();
			putRecResult = fire.putRecord(putRecReq);
			System.out.println("Result of putting obj " + obj.toString() + "is record Id " + putRecResult.getRecordId());
		}
		catch(UnsupportedEncodingException use)
		{
			System.out.println(use.toString());
		}
		
	}

}
