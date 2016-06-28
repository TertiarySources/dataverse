package edu.harvard.iq.dataverse.batch.api;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetServiceBean;
import edu.harvard.iq.dataverse.Dataverse;
import edu.harvard.iq.dataverse.PermissionServiceBean;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;

import javax.batch.operations.JobOperator;
import javax.batch.operations.JobSecurityException;
import javax.batch.operations.JobStartException;
import javax.batch.runtime.BatchRuntime;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder.jsonObjectBuilder;

@Stateless
@Path("import")
@Produces(MediaType.APPLICATION_JSON)
public class FileRecordJobResource extends AbstractApiBean {

    private static final Logger logger = Logger.getLogger(FileRecordJobResource.class.getName());

    @EJB
    PermissionServiceBean permissionService;

    @EJB
    DatasetServiceBean datasetService;

    @GET
    @Path("/dataset/files")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFilesystemImport(
            @QueryParam("datasetId") String dsId,
            @QueryParam("key") String key)
    {
        try {
            try {
                Dataset ds = datasetService.findByGlobalId(dsId);
                if (ds != null) {

                    DataverseRequest req = createDataverseRequest(findAuthenticatedUserOrDie());

                    if (isAuthorized(req, ds.getOwner())) {

                        long jid = 0;

                        try {

                            System.out.println("Authenticated User: " + req.getUser().getIdentifier());
                            Properties props = new Properties();
                            props.setProperty("datasetId", dsId);
                            props.setProperty("userId", req.getUser().getIdentifier().replace("@",""));
                            JobOperator jo = BatchRuntime.getJobOperator();
                            jid = jo.start("FileSystemImportJob", props);

                        } catch (JobStartException | JobSecurityException ex) {
                            logger.log(Level.SEVERE, "Job Error: " + ex.getMessage());
                            ex.printStackTrace();
                        }

                        if (jid > 0) {

                            // success json
                            // use 202 Accepted response?
                            JsonObjectBuilder bld = jsonObjectBuilder();
                            return this.okResponse(bld
                                    .add("executionId", jid)
                                    .add("message", "FileSystemImportJob in progress")
                            );

                        } else {
                            return this.errorResponse(Response.Status.BAD_REQUEST,
                                    "Error creating FilesystemImportJob with dataset with ID: " + dsId);
                        }

                    } else {
                        return this.errorResponse(Response.Status.FORBIDDEN, "User is not authorized.");
                    }

                } else {
                    return this.errorResponse(Response.Status.BAD_REQUEST, "Can't find dataset with ID: " + dsId);
                }
            } catch (WrappedResponse wr) {
                return wr.getResponse();
            }
        } catch (Exception e) {
            return this.errorResponse(Response.Status.BAD_REQUEST, "Import Exception - " + e.getMessage());
        }
    }

    /**
     *
     * @param dvReq
     * @param dv
     * @return
     */
    private boolean isAuthorized(DataverseRequest dvReq, Dataverse dv) {
        if (permissionService.requestOn(dvReq, dv).has(Permission.EditDataverse))
            return true;
        else
            return false;
    }

}