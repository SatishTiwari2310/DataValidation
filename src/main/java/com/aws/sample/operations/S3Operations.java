package com.aws.sample.operations;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.aws.sample.util.AWSUtil;
import com.aws.sample.util.Util;

public class S3Operations {
	
	private static S3Operations s3Operations;
	private static AmazonS3 s3Client;
	
	private S3Operations() {
		
	}
	
	public static S3Operations getinstance() {
		if (s3Operations==null) {
			s3Operations = new S3Operations();
			if (s3Client==null) {
				s3Client = AWSUtil.getAmazonS3Client();
			}
		}
		return s3Operations;
	}
	
	public void getObject(String bucketName, String key) {
		try {
            System.out.println("Downloading an object");
            S3Object s3object = s3Client.getObject(new GetObjectRequest(
            		bucketName, key));
            System.out.println("Content-Type: "  + 
            		s3object.getObjectMetadata().getContentType());
            Util.writeFile(s3object.getObjectContent(), key);
            
            System.out.println("Downloading has done");
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
            		" means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
            		" the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

	}
	
	public void uploadObject(String bucketName, String key, String uploadFileName) {
		 try {
	            System.out.println("Uploading a new object to S3 from a file\n");
	            if (uploadFileName==null || uploadFileName.isEmpty())
					uploadFileName=key;
	            
	            File file = new File(uploadFileName);
	           
	            s3Client.putObject(new PutObjectRequest(
	            		                 bucketName, key, file));

	         } catch (AmazonServiceException ase) {
	            System.out.println("Caught an AmazonServiceException, which " +
	            		"means your request made it " +
	                    "to Amazon S3, but was rejected with an error response" +
	                    " for some reason.");
	            System.out.println("Error Message:    " + ase.getMessage());
	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	            System.out.println("Error Type:       " + ase.getErrorType());
	            System.out.println("Request ID:       " + ase.getRequestId());
	        } catch (AmazonClientException ace) {
	            System.out.println("Caught an AmazonClientException, which " +
	            		"means the client encountered " +
	                    "an internal error while trying to " +
	                    "communicate with S3, " +
	                    "such as not being able to access the network.");
	            System.out.println("Error Message: " + ace.getMessage());
	        }
		
	}
	public void uploadObjectAndEncrypt(String bucketName, String key, String uploadFileName) {
		try {
			System.out.println("Uploading a new object to S3 from a file\n");
			if (uploadFileName==null || uploadFileName.isEmpty())
				uploadFileName=key;
			
			File file = new File(uploadFileName);
			
			PutObjectRequest putRequest = new PutObjectRequest(
                    bucketName, key, file);
			
			// Request server-side encryption.
			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);     
			putRequest.setMetadata(objectMetadata);

			PutObjectResult response = s3Client.putObject(putRequest);
			System.out.println("Uploaded object encryption status is " + 
			                  response.getSSEAlgorithm());
			
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " +
					"means your request made it " +
					"to Amazon S3, but was rejected with an error response" +
					" for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " +
					"means the client encountered " +
					"an internal error while trying to " +
					"communicate with S3, " +
					"such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		
	}
	
	public Bucket getBucket(String bucketName) {
		Bucket bucket =null;
		System.out.println("Listing buckets");
        for (Bucket bket : s3Client.listBuckets()) {
            if(bket.getName().equalsIgnoreCase(bucketName)) {
            	bucket = bket;
            	break;
            }
        }
        return bucket;
	}
	

}
