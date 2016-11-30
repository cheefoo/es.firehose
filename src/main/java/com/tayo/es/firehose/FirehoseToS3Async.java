package com.tayo.es.firehose;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class FirehoseToS3Async 
{
	public static final String DELIVERY_STREAM_NAME = "AsyncClientTester";
	private static final String filePath = "C:\\Users\\Administrator\\Downloads\\moviedata\\moviedata.json";
	
	
	public static void main (String [] args) throws InterruptedException, ExecutionException
	{
		AWSCredentials credentials = new  ProfileCredentialsProvider("default").getCredentials();
		AmazonKinesisFirehoseAsyncClient fire = new AmazonKinesisFirehoseAsyncClient(credentials);
		DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
		describeDeliveryStreamRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);
		Future<DescribeDeliveryStreamResult> result = fire.describeDeliveryStreamAsync(describeDeliveryStreamRequest);
		
		List<JSONObject> jsonObjectList= new ArrayList<JSONObject>();
		if(result.get() != null)
		{
			PutRecordBatchRequest putRecBatchReq = new PutRecordBatchRequest();
			putRecBatchReq.setDeliveryStreamName(DELIVERY_STREAM_NAME);
			jsonObjectList = getDataObjects();
			List<Record> records = new ArrayList<Record>();
			Future<PutRecordBatchResult> results = null;
			int counter = 1;
			if (!jsonObjectList.isEmpty())
			{
				for(JSONObject obj : jsonObjectList)
				{
				/*for(int i = 1; i < 400; i++)
				{*/
					ByteBuffer buffer = null;
					try 
					{
						buffer = ByteBuffer.wrap(obj.toString().getBytes("UTF-8"));
						Record recordAsync = new Record();
						recordAsync.setData(buffer);
						records.add(recordAsync);
						if(records.size() % 499 == 0 )
						{
							PutRecordBatchResult prbr = new PutRecordBatchResult();
							System.out.println(counter  + " objects added");
							putRecBatchReq.setRecords(records);
						    results = fire.putRecordBatchAsync(putRecBatchReq, 
						    		new FhCallbackHandler());
						    System.out.println("Completed Object put batch request");
						    /*do
							{
								wait(results);
							}while(!results.isDone());
							
							
							//Assuming results should be available after 10 second sleep
							for(int j=0; j < results.get().getRequestResponses().size(); j++)
							{
								System.out.println("Printing :" + results.get().getRequestResponses().get(j));
								
							}*/	
							records = new ArrayList<Record>();
							counter++;
						}
					} 
					catch (UnsupportedEncodingException e) 
					{

						e.printStackTrace();
					}
								
				}
				
			}
			
		}
		System.out.println("All done");
	}

	private static void wait(Future<PutRecordBatchResult> results) throws InterruptedException 
	{
		
			System.out.println("Begin Sleep :" + System.currentTimeMillis());
			Thread.sleep(10000);
			System.out.println("End Sleep :" + System.currentTimeMillis());
		
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
