package com.aws.sample.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;


@Service
public class DataMigratorUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataMigratorUtil.class);

	@Value("${application.temp-local-directory}")
	String tempLocalDirectory;
	
	@Autowired
	private ResourceLoader resourceLoader;
	
	public void decompress() {
		System.out.println("Bye");
		String outputFileName = "wrs1-faultlogfact--01-2018";
		String resourcePath = "/avroschema/faultlogfact/DW.FAULTLOGFACT.avsc";
		try {
			
			//******************************** decompress code, can be deleted ***************************************
			InputStream schemaFile1 = resourceLoader.getResource("classpath:"+resourcePath)
					.getInputStream();
			
			decompresCSVFile(schemaFile1, tempLocalDirectory + outputFileName);
			//*********************************************************************************************************
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void decompresCSVFile(InputStream schemaFile, String fileName) {
		DatumReader<GenericRecord> datumReader = null;
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			
			Schema schema = new Parser().parse(schemaFile);
		
			File file = new File(fileName+".avro");
			LOGGER.info("File : "+file);
			

			FileOutputStream fileOutputStream = new FileOutputStream(fileName+".csv");

			writeHeadersToCsvFile(file, fileOutputStream);
			
			datumReader = new GenericDatumReader<GenericRecord>(schema);
			dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
			GenericRecord tblRecord = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
	
				tblRecord = dataFileReader.next();
				List<Field> fields = tblRecord.getSchema().getFields();
				
				String values = "";
				for (int i = 0; i < fields.size() - 1; i++)
					values = (null == tblRecord.get(fields.get(i).name())) ? values + "null,"
							: values + tblRecord.get(fields.get(i).name()).toString() + ",";
				values = (null == tblRecord.get(fields.get(fields.size() - 1).name())) ? values + "null\n"
						: values + tblRecord.get(fields.get(fields.size() - 1).name()).toString() + "\n";

				fileOutputStream.write(values.getBytes());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(dataFileReader != null)
					dataFileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void writeHeadersToCsvFile(File file, FileOutputStream fileOutputStream) throws IOException {
		DataFileReader<GenericRecord> dataFileReader = null;
		try {
			DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<GenericRecord>();

			dataFileReader = new DataFileReader<GenericRecord>(file, userDatumReader);
			GenericRecord record = null;
			if (dataFileReader.hasNext()) {
				record = dataFileReader.next();
				List<Field> fields = record.getSchema().getFields();
				StringBuilder header = new StringBuilder();
				for (int i = 0; i < fields.size() - 1; i++)
					header.append(fields.get(i).name() + ",");
				header.append(fields.get(fields.size() - 1).name() + "\n");
				fileOutputStream.write(header.toString().getBytes());
			}
		} finally {
			if (null != dataFileReader)
				dataFileReader.close();
		}
	}

	/*public void compressAndWriteToCsvFiles(InputStream schemaFile, String outputFileName, ResultSet rs)
			throws SQLException, IOException {
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
	
			Schema schema = new Parser().parse(schemaFile);
			File file = new File(outputFileName);
	
			if (!file.getParentFile().exists()) {
				boolean statusFolder = file.getParentFile().mkdirs();
			}
			try {
				boolean fileStatus = file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			CodecFactory c = CodecFactory.deflateCodec(9);
			dataFileWriter.setCodec(c);
			dataFileWriter.create(schema, file);
			LOGGER.info("Processing compression");
			
			while (rs.next()) {
				GenericRecord record = new GenericData.Record(schema);
				
				for (int i = 1; i < columnCount+1; i++) {
					String value = rs.getString(i);
					
					String columnName = metaData.getColumnName(i);
					int columnType = metaData.getColumnType(i);
	
					Schema recSchema = schema.getField(columnName).schema();
					Type fieldType = recSchema.getType();
					
					if (Type.UNION.equals(fieldType)) {
						
						fieldType = recSchema.getTypes().get(0).getType();
					} 
					
					//LOGGER.info("Coumn Name : "+columnName+" @@ column Type : "+columnType+"  @@ Field Type : "+fieldType);
					
					LOGGER.info("Column : "+metaData.getColumnLabel(i)+" ### Column Type Name : "+
							metaData.getColumnTypeName(i)+" ### Column Type : "+metaData.getColumnType(i)+
							" ### Value : "+value
							);
					
					if (Type.STRING.equals(fieldType)) {
						
						record.put(columnName, (value == null || "null".equals(value)) ? null : value);
						
					} else if (Type.LONG.equals(fieldType)) {
						switch (columnType) {
							case Types.DATE:
							case Types.TIMESTAMP:
							case Types.TIME:
								value = (value == null || "null".equals(value)) ? null : tsToEpoch(value);
								break;
						}
						record.put(columnName, (value == null || "null".equals(value)) ? null : Long.parseLong(value));
						
					} else if (Type.INT.equals(fieldType)) {
						
						record.put(columnName, (value == null || "null".equals(value)) ? null : Integer.parseInt(value));
						
					} else if (Type.FLOAT.equals(fieldType)) {
						
						record.put(columnName, (value == null || "null".equals(value)) ? null : Float.parseFloat(value));
						
					} else if (Type.DOUBLE.equals(fieldType)) {
						
						record.put(columnName, (value == null || "null".equals(value)) ? null : Double.parseDouble(value));
						
					}
	
					//System.out.print("-");
					//System.out.print(" ### "+record.get(columnName));
				}
				dataFileWriter.append(record);
			}
			dataFileWriter.close();
			LOGGER.info("Completed compressing, uploading file to S3...");
		} catch (Exception e) {
			
			LOGGER.error("Schema File "+outputFileName +" Exception "+e.toString());
			//e.printStackTrace();
		}
	}

	

	
	
	public static String tsToEpoch(String timestamp) {
		//LOGGER.info("Epoch Time Stamp : "+timestamp);
		if(timestamp == null) return null;
		try {
			//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date dt = GenericUtil.parse(timestamp);//sdf.parse(timestamp);
			long epoch = dt.getTime();
			String sEpoch = epoch+"";//(epoch/1000)+"";
			//LOGGER.info("Time Stamp : "+sEpoch);
			return sEpoch;
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public ObjectMetadata exportCsvFileToS3(String bucketName, String key, String fileToUpload) {
		try {

			File file = new File(fileToUpload);
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key.toLowerCase(), file);

			SSEAwsKeyManagementParams sseAwsKeyManagementParams = null;
			if (null != awsKmsKeyId && !awsKmsKeyId.isEmpty()) {
				sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(awsKmsKeyId);
			} else {
				sseAwsKeyManagementParams = new SSEAwsKeyManagementParams();
			}

			putObjectRequest.putCustomRequestHeader("x-amz-server-side-encryption", "aws:kms");
			PutObjectResult putObjResult = s3Client.putObject(putObjectRequest.withSSEAwsKeyManagementParams(sseAwsKeyManagementParams));
			System.out.println("%%%%%%% version details :: "+fileToUpload+" :: metadata :"+putObjResult.getMetadata().getRawMetadata()+" :: "+putObjResult.getVersionId());

			file.delete();
			return putObjResult.getMetadata();
		} catch (AmazonServiceException ase) {
			LOGGER.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			LOGGER.error("Error Message:    " + ase.getMessage());
			LOGGER.error("HTTP Status Code: " + ase.getStatusCode());
			LOGGER.error("AWS Error Code:   " + ase.getErrorCode());
			LOGGER.error("Error Type:       " + ase.getErrorType());
			LOGGER.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			LOGGER.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			LOGGER.error("Error Message: " + ace.getMessage());
		}
		return null;
	}
	
	public Boolean deleteFileFromS3(String tableName, String reportType, String stDate, String edDate) { //String bucketName, String key) {
		try {
			ArrayList<MetaData> lsMetaData = fetchS3FileDetails(tableName, reportType, stDate, edDate);
			
			lsMetaData.forEach(metaData -> {
				
				DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(metaData.getBucketName(), metaData.getBucketFileKey());

				s3Client.deleteObject(deleteObjectRequest);
				Boolean bool = deleteS3FileDetails(tableName, reportType, stDate, edDate);

				System.out.println("S3 File deleted successfully...deleted key "+metaData.getBucketName()+"/"+metaData.getBucketFileKey());
			});

			return true;
		} catch (AmazonServiceException ase) {

			LOGGER.error("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			LOGGER.error("Error Message:    " + ase.getMessage());
			LOGGER.error("HTTP Status Code: " + ase.getStatusCode());
			LOGGER.error("AWS Error Code:   " + ase.getErrorCode());
			LOGGER.error("Error Type:       " + ase.getErrorType());
			LOGGER.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			
			LOGGER.error("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			LOGGER.error("Error Message: " + ace.getMessage());
			return false;
		} catch (DataAccessException dae) {
		
			LOGGER.error("Caught an while trying to delete S3 file");
			LOGGER.error("Error Message: " + dae.getMessage());
			return false;
		} catch (IOException ioe) {
			
			LOGGER.error("Caught an while trying to delete S3 file");
			LOGGER.error("Error Message: " + ioe.getMessage());
			return false;
		}
	}
	
	public String generateOutputFileName(String delimiter, String extension, String... fileNameInputs) {
		
		String outputFileName = null;
		int fileNameInputsLength = fileNameInputs.length;
		if (fileNameInputsLength > 0) {
			outputFileName = "";
			for (int i = 0; i < fileNameInputsLength - 1; i++) {
				outputFileName = outputFileName + fileNameInputs[i] + delimiter;
			}
			outputFileName = outputFileName + fileNameInputs[fileNameInputsLength - 1] + extension;
		}
		return outputFileName;
	}

	public String generateKey(String delimiter, String... folderNames) {
		
		String key = null;
		int folderNamesLength = folderNames.length;
		if (folderNamesLength > 0) {
			key = "";
			for (int i = 0; i < folderNamesLength - 1; i++) {
				key = key + folderNames[i] + delimiter;
			}
			key = key + folderNames[folderNamesLength - 1];
		}
		return key;
	}
	
	
	public ArrayList<String> computeSEDates(String startDate, String endDate) {
		
		ArrayList<String> alSEDates = new ArrayList<String>();
		
		try {
		
			DateFormat formaterDate = new SimpleDateFormat("yy-MMM-dd HH:mm:ss");
			Date sd = formaterDate.parse(startDate);
		    LOGGER.info("Date :"+sd);

		    Date ed = null;
		    if((endDate != null && "".equals(endDate.trim())) || endDate == null)
		    	ed = new Date();
		    else 
		    	ed = formaterDate.parse(endDate);
		    
		    LOGGER.info("Date : "+ed);
		    
		    Calendar stCal = Calendar.getInstance();
		    Calendar edCal = Calendar.getInstance();
		    
		    stCal.setTime(sd);
		    edCal.setTime(ed);    
		    
		    LOGGER.info(formaterDate.format(stCal.getTime()).toUpperCase());
		    LOGGER.info(formaterDate.format(edCal.getTime()).toUpperCase());
		    
		    alSEDates.add(formaterDate.format(stCal.getTime()).toUpperCase());
		    alSEDates.add(formaterDate.format(edCal.getTime()).toUpperCase());
		    
		    return alSEDates;
		} catch (ParseException e) {
		    e.printStackTrace();
		} 
		return null;
	}
	
	public ArrayList<String> computeDateRange(String startDate, String endDate) {

		ArrayList<String> dateRange = new ArrayList<String>();
        try {

			DateFormat formaterDate = new SimpleDateFormat("yy-MMM-dd");
			Date sd = formaterDate.parse(startDate);
		    LOGGER.info("Date :"+sd);
		    
		    Date ed = null;
		    if((endDate != null && "".equals(endDate.trim())) || endDate == null)
		    	ed = new Date();
		    else 
		    	ed = formaterDate.parse(endDate);
		    
		    LOGGER.info("Date : "+ed);
		    
	        DateFormat formater = new SimpleDateFormat("MM-yyyy");

	        Calendar beginCalendar = Calendar.getInstance();
	        Calendar finishCalendar = Calendar.getInstance();

            beginCalendar.setTime(sd);
            finishCalendar.setTime(ed);
            //finishCalendar.setTime(formater.parse((new Date()).toString()));
	
	        while (beginCalendar.before(finishCalendar) || beginCalendar.equals(finishCalendar)) {
	            String date = formater.format(beginCalendar.getTime()).toUpperCase();
	            dateRange.add(date);
	            LOGGER.info(date);
	            beginCalendar.add(Calendar.MONTH, 1);
	        }
	        return dateRange;
        } catch (ParseException e) {
            e.printStackTrace();
        } 
        return null;
	}

	public ArrayList<ArrayList<String>> splitDateAsReqiured(String splitType, String startDate, String endDate) {

		ArrayList<ArrayList<String>> dateRange = new ArrayList<ArrayList<String>>();
        try {

        	System.out.println("Start Date : "+startDate+" :: End Date : "+endDate);
			//DateFormat formaterDate = new SimpleDateFormat("yy-MMM-dd HH:mm:ss");
			Date sd = parseDate(startDate);//  formaterDate.parse(startDate);
			
			if("monthly".equalsIgnoreCase(splitType))
				sd = getStartOfMonth(sd);
			else if("daily".equalsIgnoreCase(splitType))
				sd = getStartOfDay(sd);
			
		    System.out.println("Date :"+sd);
		    
		    Date ed = null;
		    if((endDate != null && "".equals(endDate.trim())) || endDate == null)
		    	ed = new Date();
		    else 
		    	ed = parseDate(endDate);//formaterDate.parse(endDate);
		    
			if("monthly".equalsIgnoreCase(splitType))
				ed = getEndOfMonth(ed);
			else if("daily".equalsIgnoreCase(splitType))
				ed = getEndOfDay(ed);

		    System.out.println("Date : "+ed);
		    
	        DateFormat frmDayMonthYear = new SimpleDateFormat("yyyy-MM-dd");
	        DateFormat frmDayMonthYearHour = new SimpleDateFormat("yy-MM-dd HH:mm:ss");

	        Calendar beginCalendar = Calendar.getInstance();
	        Calendar finishCalendar = Calendar.getInstance();

            beginCalendar.setTime(sd);
            finishCalendar.setTime(ed);
            //finishCalendar.setTime(formater.parse((new Date()).toString()));
	
	        while (beginCalendar.before(finishCalendar) || beginCalendar.equals(finishCalendar)) {
	        	
	        	ArrayList<String> alDtYr = new ArrayList<String>();
	        	
	        	String stDate = frmDayMonthYearHour.format(beginCalendar.getTime()).toUpperCase();
	        	String dtDMY = frmDayMonthYear.format(beginCalendar.getTime()).toUpperCase();
	        	
	        	if("monthly".equalsIgnoreCase(splitType)) 
		            beginCalendar.add(Calendar.MONTH, 1);
	        	else if ("daily".equalsIgnoreCase(splitType)) 
	        		beginCalendar.add(Calendar.DATE, 1);
	        	else if ("hourly".equalsIgnoreCase(splitType))
	        		beginCalendar.add(Calendar.HOUR, 1);
	        	
	        	Calendar tempCalendar = Calendar.getInstance();
	        	tempCalendar.setTime(beginCalendar.getTime());
	        	tempCalendar.add(Calendar.SECOND, -1);
	        	
	        	String edDate = frmDayMonthYearHour.format(tempCalendar.getTime()).toUpperCase();
	        	System.out.println(stDate +" :: "+ dtDMY+" :: "+edDate);
	        	alDtYr.add(stDate);	            	            
	        	alDtYr.add(edDate);
	        	alDtYr.add(dtDMY);
	        	
	        	dateRange.add(alDtYr);
	        }
	        return dateRange;
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return null;
	}
	
	private Date getStartOfMonth(Date date) {
		
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTime(date);
	    calendar.set(Calendar.HOUR_OF_DAY, 0);
	    calendar.set(calendar.DAY_OF_MONTH, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
		
	    return calendar.getTime();
	}
	
	private Date getEndOfMonth(Date date) {
		
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTime(date);
	    calendar.set(Calendar.HOUR_OF_DAY, 23);
	    calendar.set(Calendar.MINUTE, 59);
	    calendar.set(Calendar.SECOND, 59);
	    calendar.set(calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		
	    return calendar.getTime();
	}
	
	private Date getStartOfDay(Date date) {
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DATE);
		calendar.set(year, month, day, 0, 0, 0);
		return calendar.getTime();
	}

	private Date getEndOfDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);		    
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DATE);
		calendar.set(year, month, day, 23, 59, 59);
		return calendar.getTime();
	}
	
	private String getYesterday() {
		
        DateFormat frmDayMonthYear = new SimpleDateFormat("yyyy-MM-dd");

	    final Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.DATE, -1);
	    return frmDayMonthYear.format(cal.getTime());
	}
	
	private static final String[] formats = { 
        "yyyy-MM-dd'T'HH:mm:ss'Z'",   "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd'T'HH:mm:ss",      "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd HH:mm:ss", 
        "MM/dd/yyyy HH:mm:ss",        "MM/dd/yyyy'T'HH:mm:ss.SSS'Z'", 
        "MM/dd/yyyy'T'HH:mm:ss.SSSZ", "MM/dd/yyyy'T'HH:mm:ss.SSS", 
        "MM/dd/yyyy'T'HH:mm:ssZ",     "MM/dd/yyyy'T'HH:mm:ss", 
        "yyyy:MM:dd HH:mm:ss",        "yyyyMMdd", 
        //"yy-MMM-dd HH:mm:ss", "yy-MMM-dd HH", "yy-MMM-dd",
        "yyyy-MMM-dd HH:mm:ss", //"yyyy-MMM-dd HH", 
        "yyyy-MMM-dd",
        //"yy-MM-dd HH:mm:ss", "yy-MM-dd HH", "yy-MM-dd",
        "yyyy-MM-dd HH:mm:ss", //"yyyy-MM-dd HH", 
        "yyyy-MM-dd",
        //"yy-MMM", "yy-MM", 
        "yyyy-MMM", 
        "yyyy-MM"
	};

	public static Date parseDate(String d) {
	
		if (d != null) {
			for (String parse : formats) {
				SimpleDateFormat sdf = new SimpleDateFormat(parse);
	            try {

	            	return sdf.parse(d);
	            } catch (ParseException e) {}
            }
        }
		return null;
    }
	
	private static Boolean isValidDate(String d) {
		
		Date pDate = null;
		if (d == null)
			return false;
		
		for (String parse : formats) {
			
			SimpleDateFormat sdf = new SimpleDateFormat(parse);
            try {
            	
            	pDate = sdf.parse(d);
            } catch (Exception e) {}
        }
		
		if(pDate == null)
			return false;
		else
			return true;
	}

	public Boolean checIfFileExists(String tableName, String reportType, String plantCode, String year,
			String startDateMonthFormatted, Integer plantId, String stDate, String edDate) throws DataAccessException, IOException {
		
		return metaJdbcTemplate.query(new PreparedStatementCreator() {
			
			@Override
			public PreparedStatement createPreparedStatement(Connection c) throws SQLException {
				
				PreparedStatement ps;
				
				String strRep = null;
				if("daily".equalsIgnoreCase(reportType))
					strRep = "D";
				else if("monthly".equalsIgnoreCase(reportType))
					strRep = "M";
				else if("hourly".equalsIgnoreCase(reportType))
					strRep = "H";
				
				String query = "SELECT COUNT(*) AS count FROM S3_FACT_META_DATA WHERE DATA_TYPE='"+tableName+"' AND "
						+ "PLANT_CODE='"+plantCode+"' AND DATA_FORMAT='"+strRep+"' AND "
						+ "TO_DATE(DATA_MIN_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(stDate) + "', 'YYYY-MM-DD') AND "
						+ "TO_DATE(DATA_MAX_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(edDate) + "', 'YYYY-MM-DD')";
				
				LOGGER.info("count query : "+query);
				
				ps = c.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				//ps.setInt(1, plantCode);
				return ps;
			}
		}, new ResultSetExtractor<Boolean>() {

			@Override
			public Boolean extractData(ResultSet rs) throws SQLException, DataAccessException {
				
				rs.next();
				if(rs.getInt("count") > 0)
					return true;
				else 
					return false;
			}
		});
	}
	
	public ArrayList<MetaData> fetchS3FileDetails(String tableName, String reportType, String stDate, String edDate) throws DataAccessException, IOException {
		
		return metaJdbcTemplate.query(new PreparedStatementCreator() {
			
			@Override
			public PreparedStatement createPreparedStatement(Connection c) throws SQLException {
				
				PreparedStatement ps;
				
				String strRep = null;
				if("daily".equalsIgnoreCase(reportType))
					strRep = "D";
				else if("monthly".equalsIgnoreCase(reportType))
					strRep = "M";
				else if("hourly".equalsIgnoreCase(reportType))
					strRep = "H";
				
				String query = "SELECT * FROM S3_FACT_META_DATA WHERE DATA_TYPE='"+tableName+"' AND "
						+ "DATA_FORMAT='"+strRep+"' AND "
						+ "TO_DATE(DATA_MIN_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(stDate) + "', 'YYYY-MM-DD') AND "
						+ "TO_DATE(DATA_MAX_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(edDate) + "', 'YYYY-MM-DD')";
				
				LOGGER.info("count query : "+query);
				
				ps = c.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				//ps.setInt(1, plantCode);
				return ps;
			}
		}, new ResultSetExtractor<ArrayList<MetaData>>() {

			@Override
			public ArrayList<MetaData> extractData(ResultSet rs) throws SQLException, DataAccessException {
				
				ArrayList<MetaData> alMetaData = new ArrayList<MetaData>();
				while(rs.next()) {
					MetaData md = new MetaData();
					md.setBucketName(rs.getString("BUCKET_NAME"));
					md.setBucketFileKey(rs.getString("BUCKET_FILE_KEY"));
					alMetaData.add(md);
				}
				return alMetaData;
			}
		});
	}
	
	public Boolean deleteS3FileDetails(String tableName, String reportType, String stDate, String edDate) {
		try {
			String strRep = null;
			if("daily".equalsIgnoreCase(reportType))
				strRep = "D";
			else if("monthly".equalsIgnoreCase(reportType))
				strRep = "M";
			else if("hourly".equalsIgnoreCase(reportType))
				strRep = "H";
			
			String query = "DELETE FROM S3_FACT_META_DATA WHERE DATA_TYPE='"+tableName+"' AND "
					+ "DATA_FORMAT='"+strRep+"' AND "
					+ "TO_DATE(DATA_MIN_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(stDate) + "', 'YYYY-MM-DD') AND "
					+ "TO_DATE(DATA_MAX_TIMESTAMP, 'YYYY-MM-DD')=TO_DATE('"+ GenericUtil.formatDateToDMY(edDate) + "', 'YYYY-MM-DD')";
			
			LOGGER.info("count query : "+query);
			
			metaJdbcTemplate.execute(query);
			return true;
		} catch(DataAccessException exc) {
			LOGGER.error("Exception while deleting data from metadata - "+exc.toString());
			return false;
		}
	}
	
	public List<String> validateInput(Object obj) {
		
		List<String> alRes = new ArrayList<String>();
		int bugIdentified = 0;
		
		if(obj == null) {
		
			alRes.add("Request to migrate data failed as required input not found, please passed in the required json");
			return alRes;
		}
		
		if(obj instanceof FactData) {
			
			FactData fdObj = (FactData) obj;
			String repType = fdObj.getReportType();
			ArrayList<Site> sites = fdObj.getSites();
			
			String expectedFormat = Arrays.toString(formats);

			
			if((repType != null && repType.trim().equals("")) || repType==null || repType.equalsIgnoreCase("null")) {
				
				alRes.add("'Report Type' missing in the request body");
			}

			if(sites==null) {
				
				alRes.add("Site details are missing in the request body");
			} else {
	
				System.out.println("Validating sites...");
				for (Site site : sites) {
					
					if(bugIdentified == 0) {
						
						String plantCode = site.getPlantCode();
						String stDate = site.getStartDate();
						String edDate = site.getEndDate();
						System.out.println("Plant Code : "+plantCode+" stDate : "+stDate+" edDate : "+edDate);
						if((plantCode != null && plantCode.trim().equals("")) || plantCode==null || plantCode.equalsIgnoreCase("null")) {
							
							alRes.add("One or more 'Plant Code' missing in the request body");
							bugIdentified = 1;
						}
						if ("daily".equalsIgnoreCase(repType) && 
							((stDate != null && stDate.trim().equals("")) || stDate==null || stDate.equalsIgnoreCase("null")) && 
							((edDate != null && edDate.trim().equals("")) || edDate==null || edDate.equalsIgnoreCase("null"))) {
							
							site.setStartDate(getYesterday());
							site.setEndDate(getYesterday());
						} else if ("monthly".equalsIgnoreCase(repType) && 
							((stDate != null && stDate.trim().equals("")) || stDate==null || stDate.equalsIgnoreCase("null")) && 
							((edDate != null && edDate.trim().equals("")) || edDate==null || edDate.equalsIgnoreCase("null"))) {
							
							site.setStartDate(getPreviousMonthStart());
							site.setEndDate(getPreviousMonthEnd());
						} else {
							if((stDate != null && stDate.trim().equals("")) || stDate==null || stDate.equalsIgnoreCase("null")) {
								
								alRes.add("One of the start date is missing. Expected formats are - "+expectedFormat);
								bugIdentified = 1;
							}
							else if(!isValidDate(stDate)) {
								
								alRes.add("One of the start date is not matching the expected format. "
										+ "Expected formats are - "+expectedFormat);
								bugIdentified = 1;
							}
							
							if((edDate != null && edDate.trim().equals("")) || edDate==null || edDate.equalsIgnoreCase("null")) {
								
								//alRes.add("One of the end date is missing. Expected formats are - "+expectedFormat);
								//bugIdentified = 1;
								site.setEndDate(getYesterday());
							}
							else if(!isValidDate(edDate)) {
								
								alRes.add("One of the end date is not matching the expected format. "
										+ "Expected formats are - "+expectedFormat);
								bugIdentified = 1;
							}
						}
					} else {
						return alRes;
					}
				}
			}
		} else {
			
			alRes.add("Request failed, please contact admin");
			return alRes;
		}
		return alRes; 
	}
	
	private String getPreviousMonthStart() {
		
        DateFormat frmDayMonthYear = new SimpleDateFormat("yyyy-MM-dd");

	    final Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.MONTH, -1);
	    
	    return frmDayMonthYear.format(getStartOfMonth(cal.getTime()));
	    
	}
	
	private String getPreviousMonthEnd() {
		
        DateFormat frmDayMonthYear = new SimpleDateFormat("yyyy-MM-dd");

	    final Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.MONTH, -1);
	    
	    return frmDayMonthYear.format(getEndOfMonth(cal.getTime()));
	    
	}
	
	public FactData constructPreviousMonthFactData(FactData factData) {

		FactData fdData = new FactData();
		fdData.setReportType("monthly");
		ArrayList<Site> alSites = new ArrayList<Site>();
		
		factData.getSites().forEach(fdSite -> {
			Site newSite = new Site();
			newSite.setPlantName(fdSite.getPlantName());
			newSite.setPlantCode(fdSite.getPlantCode());
			newSite.setStartDate(getPreviousMonthStart());
			newSite.setEndDate(getPreviousMonthEnd());
			alSites.add(newSite);
		  }
		);
		fdData.setSites(alSites);
		return fdData;
	}*/
}
