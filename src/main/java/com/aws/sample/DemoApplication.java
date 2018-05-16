package com.aws.sample;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ResourceLoader;

import com.aws.sample.util.DataMigratorUtil;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner{
	@Autowired
	DataMigratorUtil dataMigratorUtil;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Hello");
		
		dataMigratorUtil.decompress();
		System.out.println("done");
		
	}
}
