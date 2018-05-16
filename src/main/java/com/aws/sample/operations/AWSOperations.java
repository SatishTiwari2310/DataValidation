package com.aws.sample.operations;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.aws.sample.pojo.S3MetaPojo;
import com.aws.sample.util.Util;

public class AWSOperations {
	private static String Access_Key_ID = "";
	private static String Secret_Access_key = "";
	private static String bucketName = "";
	static {
		Properties prop = Util.getProperty();
		Access_Key_ID = prop.getProperty("Access_Key_ID");
		Secret_Access_key = prop.getProperty("Secret_Access_key");
		bucketName = prop.getProperty("bucketName");
	}

	public static void createS3Bucket() {

		BasicAWSCredentials creds = new BasicAWSCredentials(Access_Key_ID, Secret_Access_key);

		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
				.withRegion("ap-southeast-1").build();

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon S3");
		System.out.println("===========================================\n");

		System.out.println("Creating bucket " + bucketName + "\n");
		s3.createBucket(bucketName);

		// getting list of all Buckets
		System.out.println("Listing buckets");
		for (Bucket bucket : s3.listBuckets()) {
			System.out.println(" - " + bucket.getName());
		}
		System.out.println();

	}

	public static void downloadS3File() throws IOException, ParseException {

		BasicAWSCredentials creds = new BasicAWSCredentials(Access_Key_ID, Secret_Access_key);

		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
				.withRegion("ap-southeast-1").build();

		System.out.println("Downloading an object");
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, "depain.json"));

		InputStream objectData = object.getObjectContent();

		String json = IOUtils.toString(objectData);

		objectData.close();

		JSONParser parse = new JSONParser();
		JSONObject obj = (JSONObject) parse.parse(json);

		String name = obj.get("name") + "";
		String dept = obj.get("dept") + "";
		System.out.println(name);
		System.out.println(dept);
	}

	public static void uploadS3File() {

		BasicAWSCredentials creds = new BasicAWSCredentials(Access_Key_ID, Secret_Access_key);

		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
				.withRegion("ap-southeast-1").build();

		File file = new File("C:\\Users\\DB00449989.TECHMAHINDRA\\Desktop\\depain.json");

		String key = file.getName();

		System.out.println("===========================================");
		System.out.println("Uplading File on S3");
		System.out.println("===========================================\n");
		PutObjectRequest out = new PutObjectRequest(bucketName, key, file);
		s3.putObject(out);

	}

	public static ArrayList<S3MetaPojo> getS3MetaData() {
		BasicAWSCredentials creds = new BasicAWSCredentials(Access_Key_ID, Secret_Access_key);

		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
				.withRegion("ap-southeast-1").build();

		System.out.println("Listing objects");
		ObjectListing objectListing = s3
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(""));

		ArrayList<S3MetaPojo> list = new ArrayList<S3MetaPojo>();
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			S3MetaPojo metaData = new S3MetaPojo();

			metaData.setFileName(objectSummary.getKey());
			metaData.setLastModified(objectSummary.getLastModified());
			metaData.setSize(objectSummary.getSize());

			list.add(metaData);

		}
		return list;
	}

}
