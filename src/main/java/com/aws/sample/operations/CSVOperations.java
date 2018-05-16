package com.aws.sample.operations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.aws.sample.pojo.S3MetaPojo;
import com.aws.sample.util.Util;


public class CSVOperations {
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	private static String bucketName="";
	static {
		Properties prop=Util.getProperty();
		bucketName=prop.getProperty("bucketName");
	}

	public static void writeCSV(ArrayList<S3MetaPojo> list) {
		FileWriter write=null;
		try {
			File file=new File("Output/"+bucketName+".csv");
			System.out.println(file.getAbsolutePath());
			write=new FileWriter(file);
			write.append("File Name");
			write.append(COMMA_DELIMITER);
			write.append("Size");
			write.append(COMMA_DELIMITER);
			write.append("Last Modified Date");
			write.append(NEW_LINE_SEPARATOR);
			for(S3MetaPojo meta : list) {
				write.append(meta.getFileName());
				write.append(COMMA_DELIMITER);
				write.append(String.valueOf(meta.getSize()));
				write.append(COMMA_DELIMITER);
				write.append(String.valueOf(meta.getLastModified()));
				write.append(NEW_LINE_SEPARATOR);
			}
			System.out.println("CSV file was created successfully !!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				write.flush();
				write.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
