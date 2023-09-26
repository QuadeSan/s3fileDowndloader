package com.my;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3VersionSummary;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class S3FileDownloader {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: java -jar S3FileDownloader.jar <FILE_KEY> <TARGET_DATE>");
            System.exit(1);
        }

        Regions clientRegion = Regions.EU_CENTRAL_1;
        String awsProfileName = "FullAccessS3User";
        String bucketName = "vladisaks3";
        String fileKey = args[0];
        String targetDateStr = args[1];

        try {
            Date targetDate = new SimpleDateFormat("yyyy-MM-dd").parse(targetDateStr);

            ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile();
            ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(profilesConfigFile, awsProfileName);

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new AWSStaticCredentialsProvider(credentialsProvider.getCredentials()))
                    .build();

            List<S3VersionSummary> versionSummaries = s3Client.listVersions(bucketName, fileKey).getVersionSummaries();
            S3Object latestVersion = null;

            for (S3VersionSummary versionSummary : versionSummaries) {
                Date lastModified = versionSummary.getLastModified();
                if (lastModified.before(targetDate)) {
                    if (latestVersion == null || lastModified.after(latestVersion.getObjectMetadata().getLastModified())) {
                        latestVersion = s3Client.getObject(bucketName, versionSummary.getKey());
                    }
                }
            }


            if (latestVersion != null) {
                int dotIndex = fileKey.lastIndexOf('.');
                String filename = fileKey.substring(0, dotIndex);
                String downloadFilePath = "Q:\\Java study\\Xstack\\s3\\" + filename + "-latest-version.txt";

                S3ObjectInputStream inputStream = latestVersion.getObjectContent();

                Files.copy(inputStream, Paths.get(downloadFilePath), StandardCopyOption.REPLACE_EXISTING);

                System.out.println("Downloaded the latest version to: " + downloadFilePath);
            } else {
                System.out.println("No valid version found before the target date: " + targetDateStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
