package com.tayo.es.firehose;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class FirehoseToES 
{
	public static final String DELIVERY_STREAM_NAME = "tayo-es-firehose";
	private static final String filePath = "C:\\Users\\Administrator\\Downloads\\moviedata\\moviedata.json";
	
	
	public static void main (String [] args)
	{
		AWSCredentials credentials = new  ProfileCredentialsProvider("default").getCredentials();
		AmazonKinesisFirehoseClient fire = new AmazonKinesisFirehoseClient(credentials);
		DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
		describeDeliveryStreamRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);
		DescribeDeliveryStreamResult result = fire.describeDeliveryStream(describeDeliveryStreamRequest);
		
		List<JSONObject> jsonObjectList= new ArrayList<JSONObject>();
		if(result.getDeliveryStreamDescription() != null)
		{
			PutRecordRequest putRecReq = new PutRecordRequest();
			putRecReq.setDeliveryStreamName(DELIVERY_STREAM_NAME);
			jsonObjectList = getDataObjects();
			if (!jsonObjectList.isEmpty())
			{
				for(JSONObject obj : jsonObjectList)
				{
					ByteBuffer buffer = null;
					try 
					{
						buffer = ByteBuffer.wrap(obj.toString().getBytes("UTF-8"));
					} 
					catch (UnsupportedEncodingException e) 
					{

						e.printStackTrace();
					}				
					Record record = new Record(); 
					record.setData(buffer);				
					putRecReq.setRecord(record);				
					PutRecordResult putRecResult = new PutRecordResult();				
					putRecResult = fire.putRecord(putRecReq);
					System.out.println("Result of putting obj " + obj.toString() + "is record Id " + putRecResult.getRecordId());
				}
				
			}
		}
		
	}
	
	public static List<JSONObject> getDataObjects()
	{
		List<JSONObject> jsonObjectList= new ArrayList<JSONObject>();
		try {
			// read the json file
			FileReader reader = new FileReader(filePath);
			JSONParser jsonParser = new JSONParser();
			JSONArray jsonArray = (JSONArray)jsonParser.parse(reader);

			for(Object obj : jsonArray)
			{
				JSONObject jsonObject = (JSONObject) obj;
				///get a String from the JSON object
				Long year = (Long) jsonObject.get("year");
				String title = (String) jsonObject.get("title");
				JSONObject info =  (JSONObject) jsonObject.get("info");
				//JSONArray directors =  new JSONArray();
				JSONArray directors = (JSONArray) info.get("directors");
				String releaseDate = (String) info.get("release-date");
				
				Object rating = info.get("rating");
				if(rating != null && rating.toString().length()> 1)
				{
					rating = Double.valueOf(rating.toString());
				}
				else if (rating != null && rating.toString().length() == 1)
				{
					rating = Long.valueOf(rating.toString());
				}
			
				JSONArray genres =  (JSONArray)info.get("genres");
				String imageUrl = (String) info.get("image_url");
				String plot = (String) info.get("plot");
				Long rank = (Long) info.get("rank");
				Long runningTime = (Long) info.get("running_time_secs");
				JSONArray actors = (JSONArray) info.get("actors");				
				//System.out.println("{\"index\":{\"_index\":\"movies\", \"_type\":\"movie\"}}");														
				///System.out.println(jsonObject.toString());
				jsonObjectList.add(jsonObject);
			}	
						
		} 
		catch (FileNotFoundException ex) 
		{
			ex.printStackTrace();
		} catch (IOException ex) 
		{
			ex.printStackTrace();
		} catch (ParseException ex) 
		{
			ex.printStackTrace();
		} catch (NullPointerException ex) 
		{
			ex.printStackTrace();
		}
		
		return jsonObjectList;
	}

}
