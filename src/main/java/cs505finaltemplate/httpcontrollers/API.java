package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
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


    // Application Management Function 1  **COMPLETE**
    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            System.out.println("App Management Function 1 Accessed");
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


    // Application Management Function 2  **COMPLETE**
    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        System.out.println("App Management Function 2 Accessed");
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


    // Real-time Reporting Function 1 **COMPLETE, BUT TESTING NEEDED**
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipalertlist() {
        String responseString = "{}";
        System.out.println("Real-time Reporting Function 1 Accessed");
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


    // Real-time Reporting Function 2  **COMPLETE AND TESTED**
    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertlist() {
        System.out.println("Real-time Reporting Function 2 Accessed");
        String responseString = "{}";
        try {
            // Create variable to store statewide alert status
            int state_status = 0;

            // Put state wide alert on if number of alerted zip codes is 5 or greater
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

            Launcher.graphDBEngine.getContacts(mrn);

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


    @GET
    @Path("/testing")
    @Produces(MediaType.APPLICATION_JSON)
    public Response testing() {
        String responseString = "{}";
        try{

            // Patient 1 //
            // MRN 
            String mrn1 = "1";
            // Contact List
            List<String> contact1 = new ArrayList<String>();
            contact1.add("2");
            contact1.add("3");
            // Event List
            List<String> event1 = new ArrayList<String>();
            event1.add("a");
            event1.add("b");
            // Call enterPatientDataInDB
            Launcher.topicConnector.enterPatientDataInDB(mrn1, contact1, event1);
            System.out.println("Added testing data (patient 1) to database");

            // Patient 2 //
            // MRN 
            String mrn2 = "2";
            // Contact List
            List<String> contact2 = new ArrayList<String>();
            contact2.add("1");
            // Event List
            List<String> event2 = new ArrayList<String>();
            event2.add("b");
            event2.add("c");
            // Call enterPatientDataInDB
            Launcher.topicConnector.enterPatientDataInDB(mrn2, contact2, event2);
            System.out.println("Added testing data (patient 2) to database");

            // Patient 3 //
            // MRN 
            String mrn3 = "3";
            // Contact List
            List<String> contact3 = new ArrayList<String>();
            contact3.add("1");
            contact3.add("2");
            // Event List
            List<String> event3 = new ArrayList<String>();
            event3.add("a");
            // Call enterPatientDataInDB
            Launcher.topicConnector.enterPatientDataInDB(mrn3, contact3, event3);
            System.out.println("Added testing data (patient 3) to database");

            // Patient 4 //
            // MRN 
            String mrn4 = "4";
            // Contact List
            List<String> contact4 = new ArrayList<String>();
            contact4.add("2");
            contact4.add("5");
            // Event List
            List<String> event4 = new ArrayList<String>();
            event4.add("b");
            event4.add("c");
            // Call enterPatientDataInDB
            Launcher.topicConnector.enterPatientDataInDB(mrn4, contact4, event4);
            System.out.println("Added testing data (patient 4) to database");

            // Patient 5 //
            // MRN 
            String mrn5 = "5";
            // Contact List
            List<String> contact5 = new ArrayList<String>();
            contact5.add("2");
            // Event List
            List<String> event5 = new ArrayList<String>();
            event5.add("a");
            event5.add("c");
            // Call enterPatientDataInDB
            Launcher.topicConnector.enterPatientDataInDB(mrn5, contact5, event5);
            System.out.println("Added testing data (patient 5) to database");


            responseString = "Added testing data to database!";
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
