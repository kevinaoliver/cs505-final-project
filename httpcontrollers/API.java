package cs505-final-project.httpcontrollers;

import com.google.gson.Gson;
import cs505-final-project.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    //MF 1 - Get team request
    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            System.out.println("WHAT");
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "Hammer-Isa-Oliver");
            responseMap.put("Team_members_sids", "[10730209,912349421,12257265]");
            responseMap.put("app_status_code","1");

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("lastoutput",Launcher.lastCEPOutput);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Application Management Function 2
    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try {
            int reset_status_code = 0;

            Launcher.graphDBEngine.clearDB();
            
            reset_status_code = 1;

            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("reset_status_code",String.valueOf(reset_status_code));
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
        }

     // Real-time Reporting Function 1 
     @GET
     @Path("/zipalertlist")
     @Produces(MediaType.APPLICATION_JSON)
     public Response zipalertlist() {
         String responseString = "{}";
         try {
             // Look through list of zipcodes to see if any match with zipcodes in the previous output
             for(Map.Entry<String, Integer> set : Launcher.zipcodes.entrySet()) {
                 // The current key value (zipcode) in zipcodes map
                 String current_zip = set.getKey();
                 //System.out.println("Current ZIP: " + current_zip);
                 // See if the current new zipcode is also in the old zipcodes
                 if (Launcher.old_zipcodes.containsKey(current_zip)) {
                     // Check if the count value of the old zipcode is greater than or equal to 
                     // the count value of the new zipcode
                     int old_count = Launcher.old_zipcodes.get(current_zip);
                     int new_count = Launcher.zipcodes.get(current_zip);
                     if (new_count >= (old_count * 2)) {
                         Launcher.alert_zips.add(current_zip);
                         //System.out.println("Added ZIP Code to alert list!");
                     }
                 }
             }
             
             // Set new zipcodes to old_zipcodes
             //System.out.println("Old Old Zipcodes: " + Launcher.old_zipcodes);
             Launcher.old_zipcodes = Launcher.zipcodes;
             //System.out.println("New Old Zipcodes: " + Launcher.old_zipcodes);
 
             Map<String, ArrayList<String>> responseMap = new HashMap<>();
             responseMap.put("ziplist", Launcher.alert_zips);
             responseString = gson.toJson(responseMap);
 
         } catch (Exception ex) {
 
             StringWriter sw = new StringWriter();
             ex.printStackTrace(new PrintWriter(sw));
             String exceptionAsString = sw.toString();
             ex.printStackTrace();
 
             return Response.status(500).entity(exceptionAsString).build();
         }
         return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
     }
 
     // Real-time Reporting Function 2
     @GET
     @Path("/alertlist")
     @Produces(MediaType.APPLICATION_JSON)
     public Response alertlist() {
         String responseString = "{}";
         try {
             int state_status = 0;
 
             /*
             Launcher.alert_zips.add("1");
             Launcher.alert_zips.add("2");
             Launcher.alert_zips.add("3");
             Launcher.alert_zips.add("4");
             Launcher.alert_zips.add("5");
             Launcher.alert_zips.add("6");
             */ 
             
             if (Launcher.alert_zips.size() >= 5) {
                 state_status = 1;
             }
 
             Map<String,String> responseMap = new HashMap<>();
             responseMap.put("state_status", String.valueOf(state_status)); 
             responseString = gson.toJson(responseMap);
 
         } catch (Exception ex) {
 
             StringWriter sw = new StringWriter();
             ex.printStackTrace(new PrintWriter(sw));
             String exceptionAsString = sw.toString();
             ex.printStackTrace();
 
             return Response.status(500).entity(exceptionAsString).build();
         }
         return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
     }

    // Contact Tracing Function 1  **IN-PROGRESS**
    @GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getconfirmedcontacts(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        try{
            System.out.println("Contact Tracing Function 1 Accessed!");
            responseString = "This is what was put in the URL: " + mrn;
            //Launcher.graphDBEngine.getContacts(mrn);
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


    // Contact Tracing Function 2  **IN-PROGRESS**
    @GET
    @Path("/getpossiblecontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getpossiblecontacts(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        try{
            System.out.println("Contact Tracing Function 2 Accessed!");
            responseString = "This is what was put in the URL: " + mrn;
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


}
