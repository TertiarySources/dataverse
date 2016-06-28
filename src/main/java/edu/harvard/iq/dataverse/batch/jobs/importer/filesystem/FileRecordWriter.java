package edu.harvard.iq.dataverse.batch.jobs.importer.filesystem;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetServiceBean;

import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.context.JobContext;
import javax.ejb.EJB;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;


@Named
@Dependent
public class FileRecordWriter extends AbstractItemWriter {

    @Inject
    private JobContext jobContext;

    @EJB
    DatasetServiceBean datasetService;

    private Dataset dataset;

    @Override
    public void open(Serializable checkpoint) throws Exception {
        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Properties jobParams = jobOperator.getParameters(jobContext.getExecutionId());
        dataset = datasetService.findByGlobalId((String)jobParams.get("datasetId"));
    }

    @Override
    public void writeItems(List list) {
        List<DataFile> datafiles = dataset.getFiles();
        for (Object dataFile : list) {
            datafiles.add((DataFile)dataFile);
            dataset.getLatestVersion().getDataset().setFiles(datafiles);
        }
    }
}