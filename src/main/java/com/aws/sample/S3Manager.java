package com.aws.sample;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.aws.sample.operations.S3Operations;
import com.aws.sample.util.AWSUtil;
import com.aws.sample.util.Util;

public class S3Manager {

	public static void main(String[] args) {
		S3Operations s3Ops = S3Operations.getinstance();

		// 1.  Get profile credentials

		// 2. Get S3 client

		// 3. Upload Object
		String bucketName = "antarnaad";
		String keyName = "Report-20180119140951.csv";
		String filePath = "./uploads/";
		String uploadFileName = filePath+keyName;

		//s3Ops.uploadObject(s3Client, bucketName, keyName, uploadFileName);
		s3Ops.uploadObjectAndEncrypt(bucketName, keyName, uploadFileName);

	}

}
