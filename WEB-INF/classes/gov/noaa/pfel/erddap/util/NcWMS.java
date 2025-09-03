package gov.noaa.pfel.erddap.util;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import com.cohort.util.MustBe;
import com.cohort.util.String2;

import gov.noaa.pfel.erddap.Erddap;
import gov.noaa.pfel.erddap.dataset.EDDGrid;

/**
 * This class manage connection to ncWMS server to create/refresh/delete layer
 * 
 */
public class NcWMS {

    static final String griddapErddapBaseUrl=EDStatic.preferredErddapUrl+"/griddap/";
    static final String datasetstatus=EDStatic.config.ncwmsUrl+"/admin/datasetStatus?dataset=";
    static final String refreshDataset=EDStatic.config.ncwmsUrl+"/admin/refreshDataset?id=";
    static final String addDataset=EDStatic.config.ncwmsUrl+"/admin/addDataset?";
    static final String removeDataset=EDStatic.config.ncwmsUrl+"/admin/removeDataset?id=";
   
    /**
    *
    * @param erddap the Erddap instance
    * @param tDatasetID must be specified or nothing is done
    */
    public static void createOrRefreshDataset(EDDGrid eddgrid,String tDatasetID) {        
        try{
            String2.log("Info: NcWMS.createOrRefreshDataset: " + tDatasetID);            
            if (String2.isSomething(tDatasetID)) {
                if (isDatasetReady(tDatasetID)){
                    ncWmsPost(refreshDataset+tDatasetID);
                }
                else {
                    ncWmsPost(addDataset+"id="+tDatasetID+"&location=" + griddapErddapBaseUrl + tDatasetID+
                    "&downloadable=true&title=" + URLEncoder.encode(eddgrid.title(EDMessages.DEFAULT_LANGUAGE), "UTF-8")); 
                }              
            }
        }catch (Throwable subT) {
            String content = MustBe.throwableToString(subT);
            String2.log("Error in NcWMS.createOrRefreshDataset:\n" + content);
            EDStatic.email(EDStatic.config.emailEverythingToCsv, "Error in NcWMS.createOrRefreshDataset", content);
        }
    }    
    
    /**
    * 
    * @param erddap the Erddap instance
    * @param tDatasetID must be specified or nothing is done
    */
    public static void removeDataset(String tDatasetID) {
        try {
            if (String2.isSomething(tDatasetID) ) {
                ncWmsPost(removeDataset+tDatasetID);                                             
            }
        } catch (Throwable subT) {
            String content = MustBe.throwableToString(subT);
            String2.log("Error in NcWMS.removeDataset:\n" + content);
            EDStatic.email(EDStatic.config.emailEverythingToCsv, "Error in NcWMS.removeDataset", content);
        }
    }

    /**
    *
    *
    * @param urlString the URL string to POST to
    */
    private static HttpResponse<String> ncWmsPost(String urlString) throws Exception {
        HttpClient client = HttpClient.newHttpClient();        
        String2.log("Info: NcWMS.ncWmsPost: " + urlString);
        HttpRequest postRequest = HttpRequest.newBuilder()
            .uri(URI.create(urlString))
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("Authorization", "Basic " + 
                Base64.getEncoder().encodeToString((EDStatic.config.ncwmsUsername+":"+EDStatic.config.ncwmPassword).getBytes()))
            .build();
        HttpResponse<String> response = client.send(postRequest, HttpResponse.BodyHandlers.ofString());
        String2.log("  Info: HTTP status code=" + response.statusCode() + ", response=" + response.body());  
        return response;
    }

    /**
    *
    *
    * @param urlString the URL string to POST to
    */
    private static HttpResponse<String> ncWmsGet(String urlString) throws Exception {
        HttpClient client = HttpClient.newHttpClient();                
        HttpRequest postRequest = HttpRequest.newBuilder()
            .uri(URI.create(urlString))
            .GET()
            .header("Authorization", "Basic " + 
                Base64.getEncoder().encodeToString((EDStatic.config.ncwmsUsername+":"+EDStatic.config.ncwmPassword).getBytes()))
            .build();
        String2.log(urlString);
        HttpResponse<String> response = client.send(postRequest, HttpResponse.BodyHandlers.ofString());
        String2.log("  Info: HTTP status code=" + response.statusCode() + ", response=" + response.body());  
        return response;
    }

    /**
    *
    *
    * @param tDatasetID must be specified or nothing is done
    */
    private static boolean isDatasetReady(String tDatasetID) throws Exception
    {
        try{            
            HttpResponse<String> response = ncWmsGet(datasetstatus+tDatasetID);
            if (response.statusCode()==200) {
                String responseBody = response.body();
                if (responseBody.startsWith("Dataset: "+ tDatasetID + " (") && responseBody.endsWith("): READY")) {
                    return true;
                }
            }
            return false;
        } catch (Throwable subT) {
            String content = MustBe.throwableToString(subT);
            String2.log("Error in NcWMS.isDatasetReady:\n" + content);
            EDStatic.email(EDStatic.config.emailEverythingToCsv, "Error in NcWMS.isDatasetReady", content);
            return false;
        }
    }


}
