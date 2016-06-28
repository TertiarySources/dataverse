package edu.harvard.iq.dataverse.batch.jobs.importer.filesystem;

import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.context.JobContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Named
@Dependent
public class FileRecordReader extends AbstractItemReader {

    private static final Logger logger = Logger.getLogger(FileRecordReader.class.getName());

    @Inject
    JobContext jobContext;

    @Inject
    @BatchProperty
    String dataDir;

    @Inject
    @BatchProperty
    String excludes;

    File directory;

    List<File> files;

    Iterator<File> iterator;

    long currentRecordNumber;

    long totalRecordNumber;

    @Override
    public void open(Serializable checkpoint) throws Exception {

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Properties jobParams = jobOperator.getParameters(jobContext.getExecutionId());
        String doiDir = ((String) jobParams.get("datasetId")).replace("doi:","");
        directory = new File(dataDir + doiDir);

        checkDirectory();
        files = getFiles(directory);
        iterator = files.listIterator();
        currentRecordNumber = 0;
        totalRecordNumber = (long) files.size();
    }

    @Override
    public File readItem() {
        if (iterator.hasNext()) {
            currentRecordNumber++;
            logger.log(Level.INFO,
                    "Reading file " + Long.toString(currentRecordNumber) + " of " + Long.toString(totalRecordNumber));

            // skip if it's in the ignore list


            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        // no-op
    }

    private List<File> getFiles(final File directory) {

        // create filter from excludes property
        FileFilter excludeFilter = new NotFileFilter(
                new WildcardFileFilter(Arrays.asList(excludes.split("\\s*,\\s*"))));

        List<File> files = new ArrayList<>();
        File[] filesList = directory.listFiles(excludeFilter);
        if (filesList != null) {
            for (File file : filesList) {
                if (file.isFile()) {
                    files.add(file);
                } else {
                    files.addAll(getFiles(file));
                }
            }
        }
        return files;
    }

    private void checkDirectory() {
        String path = directory.getAbsolutePath();
        if (!directory.exists())
            logger.log(Level.SEVERE, "Directory " + path + "does not exist.");
        if (!directory.isDirectory())
            logger.log(Level.SEVERE, path + " is not a directory.");
        if (!directory.canRead())
            logger.log(Level.SEVERE, "Unable to read files from directory " + path + ". Permission denied.");
    }

}
