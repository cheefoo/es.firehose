package com.tayo.es.firehose;

import java.util.List;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;

public class FhCallbackHandler implements AsyncHandler<PutRecordBatchRequest, PutRecordBatchResult> 
{

	public void onError(Exception arg0) 
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onSuccess(PutRecordBatchRequest request, PutRecordBatchResult result) 
	{
		List<PutRecordBatchResponseEntry> requestResponses = result.getRequestResponses();
        int failedPutCount = result.getFailedPutCount();
        System.out.println("Failed Count is :" + failedPutCount);
        
        
        for (int i = 0; i < requestResponses.size(); i++) 
        {
            PutRecordBatchResponseEntry responseEntry = requestResponses.get(i);
            String errorCode = responseEntry.getErrorCode();
            if(errorCode != null || errorCode.length() != 0)
            {
            	System.out.println("Failed Record is:" + responseEntry.toString());
            }
            
        }
		
	}

}
