package edu.harvard.iq.dataverse.filesystem;

/*
   Copyright (C) 2005-2016, by the President and Fellows of Harvard College.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Dataverse Network - A web application to share, preserve and analyze research data.
   Developed at the Institute for Quantitative Social Science, Harvard University.
*/

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;
import edu.harvard.iq.dataverse.api.UtilIT;
import edu.harvard.iq.dataverse.batch.entities.JobExecutionEntity;
import edu.harvard.iq.dataverse.batch.entities.StepExecutionEntity;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.batch.runtime.BatchStatus;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.jayway.restassured.RestAssured.given;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

/**
 * Batch Job Integration Tests
 */
public class FileRecordJobIT {

    private static final String SEP = File.separator;
    private static final String API_TOKEN_HTTP_HEADER = "X-Dataverse-key";
    private static final String AUTHORITY = "10.5072/FK2";
    private static final String PROTOCOL = "doi";
    private static final String DOI_SEP = "/";
    private static final String DATA_DIR = "/usr/local/glassfish4/glassfish/domains/domain1/files";
    private static final String FILESYSTEM_API = "/api/import/dataset/files";
    private static final String JOB_STATUS_API = "/api/batch/job/";
    private static final int POLLING_RETRIES = 10;
    private static final long POLLING_WAIT = 1000;

    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public static void setUpClass() {
        RestAssured.baseURI = UtilIT.getRestAssuredBaseUri();
    }

    @AfterClass
    public static void tearDownClass() {
        // no-op
    }

    /**
     * Test 1
     * Import the same file in different directories, in the same dataset.
     * This is not permitted via HTTP file upload since identical checksums are not allowed in the same dataset.
     * Ignores failed checksum manifest import.
     */
    @Test
    public void testSameFileInDifferentDirectories() {

        try {
            String username;
            String apiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse1Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse1Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse1Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse1Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse1Response);

            // create dataset
            Response createDataset1Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset1Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset1Response);
            JsonPath createdDataset = JsonPath.from(createDataset1Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            if (identifier == null) {
                System.out.println("Unable to parse dataset identifier from json response: " +
                        createDataset1Response.body().asString());
                fail();
            }
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create a test file and put it in two places
            File file1 = createTestFile(datasetDir, "testfile.txt", 0.5);
            boolean isDirCreated = new File(datasetDir + SEP + "subdir").mkdirs();
            if (!isDirCreated) {
                System.out.println("Unable to create directory: " + datasetDir + SEP + "subdir");
                fail();
            }
            File fileCopy = new File(datasetDir + SEP + "subdir" + SEP + "testfile.txt");
            if (file1 != null) {
                FileUtils.copyFile(file1, fileCopy);
            } else {
                System.out.println("Unable to copy file: " + datasetDir + SEP + "subdir" + SEP + "testfile.txt");
                fail();
            }

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(200, jobResponse.getStatusCode());

            // get job id and wait for completion
            String jobId = JsonPath.from(jobResponse.body().asString()).getString("data.executionId");
            String jobResult = pollJobStatus(jobId, apiToken, POLLING_RETRIES, POLLING_WAIT);

            JobExecutionEntity job;
            job = mapper.readValue(jobResult, JobExecutionEntity.class);
            assertEquals(job.getSteps().size(), 2);
            StepExecutionEntity step1 = job.getSteps().get(0);
            Map<String, Long> metrics = step1.getMetrics();
            // check job status
            assertEquals(job.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(job.getStatus(), BatchStatus.COMPLETED);
            // check step 1 status and name
            assertEquals(step1.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step1.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step1.getName(), "import-files");
            // verify step 1 metrics
            assertEquals((long) metrics.get("write_skip_count"), 0);
            assertEquals((long) metrics.get("commit_count"), 1);
            assertEquals((long) metrics.get("process_skip_count"), 0);
            assertEquals((long) metrics.get("read_skip_count"), 0);
            assertEquals((long) metrics.get("write_count"), 2);
            assertEquals((long) metrics.get("rollback_count"), 0);
            assertEquals((long) metrics.get("filter_count"), 0);
            assertEquals((long) metrics.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step1.getPersistentUserData(), null);

            // confirm data files were imported
            List<String> filenames = new ArrayList<>();
            Response updatedDatasetResponse = UtilIT.getDatasetResponse(pid, apiToken);
            //updatedDatasetResponse.prettyPrint();
            JsonPath dsJson = JsonPath.from(updatedDatasetResponse.body().asString());
            String fileOne = dsJson.getString("data.latestVersion.files[0].datafile.filename");
            String fileTwo = dsJson.getString("data.latestVersion.files[1].datafile.filename");
            filenames.add(fileOne);
            filenames.add(fileTwo);
            assert(filenames.contains("testfile.txt"));
            assert(filenames.contains("subdir/testfile.txt"));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testIdenticalFilesInDifferentDirectories: " + e.getMessage());
            fail();
        }
    }

    @Test
    public void testFilesWithChecksumManifest() {

        try {
            String username;
            String apiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse2Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse2Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse2Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse2Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse2Response);

            // create dataset
            Response createDataset2Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset2Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset2Response);
            JsonPath createdDataset = JsonPath.from(createDataset2Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create test files and checksum manifest
            File file1 = createTestFile(datasetDir, "testfile1.txt", 0.5);
            File file2 = createTestFile(datasetDir, "testfile2.txt", 0.5);
            String checksum1 = "";
            String checksum2 = "";
            if (file1 != null && file2 != null) {
                PrintWriter pw = new PrintWriter(new FileWriter(datasetDir + "/files.sha"));
                checksum1 = getFileChecksum(file1.getAbsolutePath(), "SHA1");
                checksum2 = getFileChecksum(file2.getAbsolutePath(), "SHA1");
                pw.write(checksum1 + " " + file1.getName());
                pw.write("\n");
                pw.write(checksum2 + " " + file2.getName());
                pw.write("\n");
                pw.close();
            } else {
                fail();
            }

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(200, jobResponse.getStatusCode());

            // get job id and wait for completion
            String jobId = JsonPath.from(jobResponse.body().asString()).getString("data.executionId");
            String jobResult = pollJobStatus(jobId, apiToken, POLLING_RETRIES, POLLING_WAIT);

            JobExecutionEntity job;
            job = mapper.readValue(jobResult, JobExecutionEntity.class);
            assertEquals(job.getSteps().size(), 2);
            StepExecutionEntity step1 = job.getSteps().get(0);
            StepExecutionEntity step2 = job.getSteps().get(1);
            Map<String, Long> metrics1 = step1.getMetrics();
            Map<String, Long> metrics2 = step2.getMetrics();
            // check job status
            assertEquals(job.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(job.getStatus(), BatchStatus.COMPLETED);
            // check step 1 status and name
            assertEquals(step1.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step1.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step1.getName(), "import-files");
            // verify step 1 metrics
            assertEquals((long) metrics1.get("write_skip_count"), 0);
            assertEquals((long) metrics1.get("commit_count"), 1);
            assertEquals((long) metrics1.get("process_skip_count"), 0);
            assertEquals((long) metrics1.get("read_skip_count"), 0);
            assertEquals((long) metrics1.get("write_count"), 2);
            assertEquals((long) metrics1.get("rollback_count"), 0);
            assertEquals((long) metrics1.get("filter_count"), 0);
            assertEquals((long) metrics1.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step1.getPersistentUserData(), null);
            // check step 2 status and name
            assertEquals(step2.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step2.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step2.getName(), "import-checksums");
            // verify step 2 metrics
            assertEquals((long) metrics2.get("write_skip_count"), 0);
            assertEquals((long) metrics2.get("commit_count"), 1);
            assertEquals((long) metrics2.get("process_skip_count"), 0);
            assertEquals((long) metrics2.get("read_skip_count"), 0);
            assertEquals((long) metrics2.get("write_count"), 2);
            assertEquals((long) metrics2.get("rollback_count"), 0);
            assertEquals((long) metrics2.get("filter_count"), 0);
            assertEquals((long) metrics2.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step2.getPersistentUserData(), null);

            // confirm files were imported
            List<String> filenames = new ArrayList<>();
            Response datasetResponse = UtilIT.getDatasetResponse(pid, apiToken);
            JsonPath dsJson = JsonPath.from(datasetResponse.body().asString());
            String fileOne = dsJson.getString("data.latestVersion.files[0].datafile.filename");
            String fileTwo = dsJson.getString("data.latestVersion.files[1].datafile.filename");
            filenames.add(fileOne);
            filenames.add(fileTwo);
            assert(filenames.contains("testfile1.txt"));
            assert(filenames.contains("testfile2.txt"));

            // confirm checksums were imported
            List<String> checksums = new ArrayList<>();
            String checksumOne = dsJson.getString("data.latestVersion.files[0].datafile.md5");
            String checksumTwo = dsJson.getString("data.latestVersion.files[1].datafile.md5");
            checksums.add(checksumOne);
            checksums.add(checksumTwo);
            assert(checksums.contains(checksum1));
            assert(checksums.contains(checksum2));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testChecksumImport: " + e.getMessage());
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFilesWithoutChecksumManifest() {

        try {
            String username;
            String apiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse2Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse2Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse2Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse2Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse2Response);

            // create dataset
            Response createDataset2Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset2Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset2Response);
            JsonPath createdDataset = JsonPath.from(createDataset2Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create test files and NO checksum manifest
            createTestFile(datasetDir, "testfile1.txt", 0.25);
            createTestFile(datasetDir, "testfile2.txt", 0.25);

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(200, jobResponse.getStatusCode());

            // get job id and wait for completion
            String jobId = JsonPath.from(jobResponse.body().asString()).getString("data.executionId");
            String jobResult = pollJobStatus(jobId, apiToken, POLLING_RETRIES, POLLING_WAIT);

            JobExecutionEntity job;
            job = mapper.readValue(jobResult, JobExecutionEntity.class);
            assertEquals(job.getSteps().size(), 2);
            StepExecutionEntity step1 = job.getSteps().get(0);
            StepExecutionEntity step2 = job.getSteps().get(1);
            Map<String, Long> metrics1 = step1.getMetrics();
            Map<String, Long> metrics2 = step2.getMetrics();
            // check job status
            assertEquals(job.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(job.getStatus(), BatchStatus.COMPLETED);
            // check step 1 status and name
            assertEquals(step1.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step1.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step1.getName(), "import-files");
            // verify step 1 metrics
            assertEquals((long) metrics1.get("write_skip_count"), 0);
            assertEquals((long) metrics1.get("commit_count"), 1);
            assertEquals((long) metrics1.get("process_skip_count"), 0);
            assertEquals((long) metrics1.get("read_skip_count"), 0);
            assertEquals((long) metrics1.get("write_count"), 2);
            assertEquals((long) metrics1.get("rollback_count"), 0);
            assertEquals((long) metrics1.get("filter_count"), 0);
            assertEquals((long) metrics1.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step1.getPersistentUserData(), null);
            // check step 2 status and name
            assertEquals(step2.getExitStatus(), BatchStatus.FAILED.name());
            assertEquals(step2.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step2.getName(), "import-checksums");
            // verify step 2 metrics
            assertEquals((long) metrics2.get("write_skip_count"), 0);
            assertEquals((long) metrics2.get("commit_count"), 1);
            assertEquals((long) metrics2.get("process_skip_count"), 0);
            assertEquals((long) metrics2.get("read_skip_count"), 0);
            assertEquals((long) metrics2.get("write_count"), 0);
            assertEquals((long) metrics2.get("rollback_count"), 0);
            assertEquals((long) metrics2.get("filter_count"), 0);
            assertEquals((long) metrics2.get("read_count"), 0);
            // should include detailed error message
            assert(step2.getPersistentUserData().contains("FAILED: missing checksums"));

            // confirm files were imported
            List<String> filenames = new ArrayList<>();
            Response datasetResponse = UtilIT.getDatasetResponse(pid, apiToken);
            //datasetResponse.prettyPrint();
            JsonPath dsJson = JsonPath.from(datasetResponse.body().asString());
            String fileOne = dsJson.getString("data.latestVersion.files[0].datafile.filename");
            String fileTwo = dsJson.getString("data.latestVersion.files[1].datafile.filename");
            filenames.add(fileOne);
            filenames.add(fileTwo);
            assert(filenames.contains("testfile1.txt"));
            assert(filenames.contains("testfile2.txt"));

            // confirm checksums are unknown
            String checksumOne = dsJson.getString("data.latestVersion.files[0].datafile.md5");
            String checksumTwo = dsJson.getString("data.latestVersion.files[1].datafile.md5");
            assert(checksumOne.equalsIgnoreCase("unknown"));
            assert(checksumTwo.equalsIgnoreCase("unknown"));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testChecksumImportMissingManifest: " + e.getMessage());
            fail();
        }

    }

    @Test
    public void testFileMissingInChecksumManifest() {

        try {
            String username;
            String apiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse2Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse2Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse2Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse2Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse2Response);

            // create dataset
            Response createDataset2Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset2Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset2Response);
            JsonPath createdDataset = JsonPath.from(createDataset2Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create test files and checksum manifest with just one of the files
            File file1 = createTestFile(datasetDir, "testfile1.txt", 0.25);
            File file2 = createTestFile(datasetDir, "testfile2.txt", 0.25);
            String checksum1 = "";
            if (file1 != null && file2 != null) {
                PrintWriter pw = new PrintWriter(new FileWriter(datasetDir + "/files.sha"));
                checksum1 = getFileChecksum(file1.getAbsolutePath(), "SHA1");
                pw.write(checksum1 + " " + file1.getName());
                pw.write("\n");
                pw.close();
            } else {
                fail();
            }

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(200, jobResponse.getStatusCode());

            // get job id and wait for completion
            String jobId = JsonPath.from(jobResponse.body().asString()).getString("data.executionId");
            String jobResult = pollJobStatus(jobId, apiToken, POLLING_RETRIES, POLLING_WAIT);

            JobExecutionEntity job;
            job = mapper.readValue(jobResult, JobExecutionEntity.class);
            assertEquals(job.getSteps().size(), 2);
            StepExecutionEntity step1 = job.getSteps().get(0);
            StepExecutionEntity step2 = job.getSteps().get(1);
            Map<String, Long> metrics1 = step1.getMetrics();
            Map<String, Long> metrics2 = step2.getMetrics();
            // check job status
            assertEquals(job.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(job.getStatus(), BatchStatus.COMPLETED);
            // check step 1 status and name
            assertEquals(step1.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step1.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step1.getName(), "import-files");
            // verify step 1 metrics
            assertEquals((long) metrics1.get("write_skip_count"), 0);
            assertEquals((long) metrics1.get("commit_count"), 1);
            assertEquals((long) metrics1.get("process_skip_count"), 0);
            assertEquals((long) metrics1.get("read_skip_count"), 0);
            assertEquals((long) metrics1.get("write_count"), 2);
            assertEquals((long) metrics1.get("rollback_count"), 0);
            assertEquals((long) metrics1.get("filter_count"), 0);
            assertEquals((long) metrics1.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step1.getPersistentUserData(), null);
            // check step 2 status and name
            assertEquals(step2.getExitStatus(), BatchStatus.FAILED.name());
            assertEquals(step2.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step2.getName(), "import-checksums");
            // verify step 2 metrics
            assertEquals((long) metrics2.get("write_skip_count"), 0);
            assertEquals((long) metrics2.get("commit_count"), 1);
            assertEquals((long) metrics2.get("process_skip_count"), 0);
            assertEquals((long) metrics2.get("read_skip_count"), 0);
            assertEquals((long) metrics2.get("write_count"), 1);
            assertEquals((long) metrics2.get("rollback_count"), 0);
            assertEquals((long) metrics2.get("filter_count"), 0);
            assertEquals((long) metrics2.get("read_count"), 1);
            // should include detailed error message
            assert(step2.getPersistentUserData().contains("FAILED: missing checksums [testfile2.txt]"));

            // confirm files were imported
            List<String> filenames = new ArrayList<>();
            Response datasetResponse = UtilIT.getDatasetResponse(pid, apiToken);
            JsonPath dsJson = JsonPath.from(datasetResponse.body().asString());
            String fileOne = dsJson.getString("data.latestVersion.files[0].datafile.filename");
            String fileTwo = dsJson.getString("data.latestVersion.files[1].datafile.filename");
            filenames.add(fileOne);
            filenames.add(fileTwo);
            assert(filenames.contains("testfile1.txt"));
            assert(filenames.contains("testfile2.txt"));

            // confirm checksums were imported
            List<String> checksums = new ArrayList<>();
            String checksumOne = dsJson.getString("data.latestVersion.files[0].datafile.md5");
            String checksumTwo = dsJson.getString("data.latestVersion.files[1].datafile.md5");
            checksums.add(checksumOne);
            checksums.add(checksumTwo);
            assert(checksums.contains(checksum1));
            assert(checksums.contains("Unknown"));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testChecksumImport: " + e.getMessage());
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFileInChecksumManifestDoesntExist() {

        try {
            String username;
            String apiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse2Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse2Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse2Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse2Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse2Response);

            // create dataset
            Response createDataset2Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset2Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset2Response);
            JsonPath createdDataset = JsonPath.from(createDataset2Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create test files and checksum manifest with record that doesn't exist
            File file1 = createTestFile(datasetDir, "testfile1.txt", 0.25);
            File file2 = createTestFile(datasetDir, "testfile2.txt", 0.25);
            String checksum1 = "";
            String checksum2 = "";
            if (file1 != null && file2 != null) {
                PrintWriter pw = new PrintWriter(new FileWriter(datasetDir + "/files.sha"));
                checksum1 = getFileChecksum(file1.getAbsolutePath(), "SHA1");
                checksum2 = getFileChecksum(file2.getAbsolutePath(), "SHA1");
                pw.write(checksum1 + " " + file1.getName());
                pw.write("\n");
                pw.write(checksum2 + " " + file2.getName());
                pw.write("\n");
                pw.write("asdfae34034asfaf9r3  fileThatDoesntExist.txt");
                pw.write("\n");
                pw.close();
            } else {
                fail();
            }

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(200, jobResponse.getStatusCode());

            // get job id and wait for completion
            String jobId = JsonPath.from(jobResponse.body().asString()).getString("data.executionId");
            String jobResult = pollJobStatus(jobId, apiToken, POLLING_RETRIES, POLLING_WAIT);

            JobExecutionEntity job;
            job = mapper.readValue(jobResult, JobExecutionEntity.class);
            assertEquals(job.getSteps().size(), 2);
            StepExecutionEntity step1 = job.getSteps().get(0);
            StepExecutionEntity step2 = job.getSteps().get(1);
            Map<String, Long> metrics1 = step1.getMetrics();
            Map<String, Long> metrics2 = step2.getMetrics();
            // check job status
            assertEquals(job.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(job.getStatus(), BatchStatus.COMPLETED);
            // check step 1 status and name
            assertEquals(step1.getExitStatus(), BatchStatus.COMPLETED.name());
            assertEquals(step1.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step1.getName(), "import-files");
            // verify step 1 metrics
            assertEquals((long) metrics1.get("write_skip_count"), 0);
            assertEquals((long) metrics1.get("commit_count"), 1);
            assertEquals((long) metrics1.get("process_skip_count"), 0);
            assertEquals((long) metrics1.get("read_skip_count"), 0);
            assertEquals((long) metrics1.get("write_count"), 2);
            assertEquals((long) metrics1.get("rollback_count"), 0);
            assertEquals((long) metrics1.get("filter_count"), 0);
            assertEquals((long) metrics1.get("read_count"), 2);
            // should be no user data (error messages)
            assertEquals(step1.getPersistentUserData(), null);
            // check step 2 status and name
            assertEquals(step2.getExitStatus(), BatchStatus.FAILED.name());
            assertEquals(step2.getStatus(), BatchStatus.COMPLETED);
            assertEquals(step2.getName(), "import-checksums");
            // verify step 2 metrics
            assertEquals((long) metrics2.get("write_skip_count"), 0);
            assertEquals((long) metrics2.get("commit_count"), 1);
            assertEquals((long) metrics2.get("process_skip_count"), 0);
            assertEquals((long) metrics2.get("read_skip_count"), 0);
            assertEquals((long) metrics2.get("write_count"), 2);
            assertEquals((long) metrics2.get("rollback_count"), 0);
            assertEquals((long) metrics2.get("filter_count"), 1);
            assertEquals((long) metrics2.get("read_count"), 3);
            // should report missing data file
            assert(step2.getPersistentUserData().contains("FAILED: missing data files [fileThatDoesntExist.txt]"));

            // confirm files were imported
            List<String> filenames = new ArrayList<>();
            Response datasetResponse = UtilIT.getDatasetResponse(pid, apiToken);
            JsonPath dsJson = JsonPath.from(datasetResponse.body().asString());
            String fileOne = dsJson.getString("data.latestVersion.files[0].datafile.filename");
            String fileTwo = dsJson.getString("data.latestVersion.files[1].datafile.filename");
            filenames.add(fileOne);
            filenames.add(fileTwo);
            assert(filenames.contains("testfile1.txt"));
            assert(filenames.contains("testfile2.txt"));

            // confirm checksums were imported
            List<String> checksums = new ArrayList<>();
            String checksumOne = dsJson.getString("data.latestVersion.files[0].datafile.md5");
            String checksumTwo = dsJson.getString("data.latestVersion.files[1].datafile.md5");
            checksums.add(checksumOne);
            checksums.add(checksumTwo);
            assert(checksums.contains(checksum1));
            assert(checksums.contains(checksum2));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testChecksumImport: " + e.getMessage());
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testNoDatasetFound() {

        try {
            String username;
            String apiToken;

            // create test user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // run batch job
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, apiToken)
                    .queryParam("datasetId", "NO_SUCH_DATASET")
                    .queryParam("key", apiToken)
                    .get(FILESYSTEM_API);
            assertEquals(400, jobResponse.getStatusCode());

            //jobResponse.prettyPrint();
            JsonPath json = JsonPath.from(jobResponse.body().asString());
            assertEquals("ERROR", json.getString("status"));
            assertEquals("Can't find dataset with ID: NO_SUCH_DATASET", json.getString("message"));

            // tear down: delete dataverse, dataset and files
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testNoDatasetFound: " + e.getMessage());
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testUnauthorizedUser() {

        try {
            String username;
            String apiToken;
            String unauthorizedUsername;
            String unauthorizedApiToken;
            String dataverseAlias;
            String datasetDir;
            int datasetId;

            // create authorized user
            Response createUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUserResponse.getStatusCode());
            apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
            username = UtilIT.getUsernameFromResponse(createUserResponse);

            // create dataverse
            Response createDataverse2Response = UtilIT.createRandomDataverse(apiToken);
            if (createDataverse2Response.getStatusCode() != 201) {
                System.out.println("A test dataverse couldn't be created in the root dataverse:");
                System.out.println(createDataverse2Response.body().asString());
                System.out.println("Make sure users can created dataverses in the root for this test to run.");
            }
            assertEquals(201, createDataverse2Response.getStatusCode());
            dataverseAlias = UtilIT.getAliasFromResponse(createDataverse2Response);

            // create dataset
            Response createDataset2Response = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
            assertEquals(201, createDataset2Response.getStatusCode());
            datasetId = UtilIT.getDatasetIdFromResponse(createDataset2Response);
            JsonPath createdDataset = JsonPath.from(createDataset2Response.body().asString());
            String identifier = createdDataset.getString("data.identifier");
            String pid = PROTOCOL + ":" + AUTHORITY + DOI_SEP + identifier;
            datasetDir = DATA_DIR + SEP + AUTHORITY + SEP + identifier;

            // create unauthorized user
            Response createUnauthorizedUserResponse = UtilIT.createRandomUser();
            assertEquals(200, createUnauthorizedUserResponse.getStatusCode());
            unauthorizedApiToken = UtilIT.getApiTokenFromResponse(createUnauthorizedUserResponse);
            unauthorizedUsername = UtilIT.getUsernameFromResponse(createUnauthorizedUserResponse);

            // run batch job as unauthorized user
            Response jobResponse = given()
                    .header(API_TOKEN_HTTP_HEADER, unauthorizedApiToken)
                    .queryParam("datasetId", pid)
                    .queryParam("key", unauthorizedApiToken)
                    .get(FILESYSTEM_API);
            assertEquals(403, jobResponse.getStatusCode());

            //jobResponse.prettyPrint();
            JsonPath json = JsonPath.from(jobResponse.body().asString());
            assertEquals("ERROR", json.getString("status"));
            assertEquals("User is not authorized.", json.getString("message"));

            // tear down: delete dataverse, dataset and files
            FileUtils.deleteDirectory(new File(datasetDir));
            Response deleteDataset1Response = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
            assertEquals(200, deleteDataset1Response.getStatusCode());
            Response deleteDataverse1Response = UtilIT.deleteDataverse(dataverseAlias, apiToken);
            assertEquals(200, deleteDataverse1Response.getStatusCode());
            Response deleteUser1Response = UtilIT.deleteUser(username);
            assertEquals(200, deleteUser1Response.getStatusCode());
            Response deleteUser2Response = UtilIT.deleteUser(unauthorizedUsername);
            assertEquals(200, deleteUser2Response.getStatusCode());

        } catch (Exception e) {
            System.out.println("Error testChecksumImport: " + e.getMessage());
            e.printStackTrace();
            fail();
        }
    }


    // UTILS

    /***
     * Create a test file with a size in GB
     *
     * @param dir where to save the file (directory will be created if it doesn't exist)
     * @param name the test file name
     * @param size the desired size in GB
     * @return the file
     */
    static File createTestFile(String dir, String name, double size) {

        try {
            File myDir = new File(dir);
            Random random = new Random();
            boolean isDirCreated = myDir.exists();
            if (!isDirCreated) {
                isDirCreated = myDir.mkdirs();
            }
            if (isDirCreated) {
                File file = new File(dir + SEP + name);
                //long start = System.currentTimeMillis();
                PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(file), "UTF-8")), false);
                int counter = 0;
                while (true) {
                    String sep = "";
                    for (int i = 0; i < 100; i++) {
                        int number = random.nextInt(1000) + 1;
                        writer.print(sep);
                        writer.print(number / 1e3);
                        sep = " ";
                    }
                    writer.println();
                    if (++counter == 20000) {
                        //System.out.printf("Size: %.3f GB%n", file.length() / 1e9);
                        if (file.length() >= size * 1e9) {
                            writer.close();
                            break;
                        } else {
                            counter = 0;
                        }
                    }
                }
                //long time = System.currentTimeMillis() - start;
                //System.out.printf("Took %.1f seconds to create a file of %.3f GB", time / 1e3, file.length() / 1e9);
                return file;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return null;
    }

    /**
     * Get the SHA1 checksum for a file.
     *
     * @param file absolute path to file
     * @param format the checksum format (e.g., SHA1, MD5)
     * @return the checksum for the file as a hex string
     */
    static String getFileChecksum(String file, String format) {

        try {
            MessageDigest md = MessageDigest.getInstance(format);
            FileInputStream fis = new FileInputStream(file);
            byte[] dataBytes = new byte[1024];

            int nread;
            while ((nread = fis.read(dataBytes)) != -1) {
                md.update(dataBytes, 0, nread);
            }

            byte[] mdbytes = md.digest();

            //convert the byte to hex format
            StringBuilder sb = new StringBuilder("");
            for (byte b : mdbytes) {
                sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();

        } catch (Exception e) {
            System.out.println("Error getting " + format + " checksum for " + file + ": " + e.getMessage());
            return null;
        }

    }

    public static String pollJobStatus(String jobId, String apiToken, int retry, long sleep) {

        int maxTries = 0;
        String json = "";
        String status = BatchStatus.STARTED.name();
        try {
            while (!status.equalsIgnoreCase(BatchStatus.COMPLETED.name())) {
                if (maxTries < retry) {
                    maxTries++;
                    Thread.sleep(sleep);
                    Response jobResponse = given()
                            .header(API_TOKEN_HTTP_HEADER, apiToken)
                            .get(JOB_STATUS_API + jobId);
                    json = jobResponse.body().asString();
                    status = JsonPath.from(json).getString("status");
                } else {
                    break;
                }
            }
        }catch (InterruptedException ie) {
           System.out.println(ie.getMessage());
        }
        return json;
    }

}
