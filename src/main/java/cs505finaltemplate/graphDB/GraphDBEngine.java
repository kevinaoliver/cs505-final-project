package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.*;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.List;
import java.util.*;

public class GraphDBEngine {

    ODatabaseSession db;
    OrientDB orient;

    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        // Create a connection to OrientDB server running on the VM
        orient = new OrientDB("remote:127.0.0.1:2424", "root", "rootpwd", OrientDBConfig.defaultConfig());
        // Create database
        orient.createIfNotExists("test", ODatabaseType.PLOCAL);
        // Open database
        db = orient.open("test", "root", "rootpwd");
        
        System.out.println("Database successfully connected!");

        // Create schema for database (classes for vertices and edges)

        // CLASSES //
        // Create patient and event vertex classes
        // Get the classes
        OClass patient = db.getClass("patient");
        OClass event = db.getClass("event");

        // If the vertex classes have not been created yet
        if (patient == null) {
            patient = db.createVertexClass("patient");
        }
        if (event == null) {
            event = db.createVertexClass("event");
        }
        System.out.println("Created patient and event vertex classes!");

        // EDGES // 
        // Get has_contacted and attended_event edge classes
        OClass has_contacted = db.getClass("has_contacted");
        OClass attended_event = db.getClass("attended_event"); 

        // If edge classes have not been created, create them
        if (has_contacted == null) {
            has_contacted = db.createEdgeClass("has_contacted");
            has_contacted.createIndex("has_contacted_index", OClass.INDEX_TYPE.UNIQUE, "out", "in");
        }
        if (attended_event == null) {
            attended_event = db.createEdgeClass("attended_event");
            attended_event.createIndex("has_contacted_index", OClass.INDEX_TYPE.UNIQUE, "out", "in");
        }
        System.out.println("Created edge classes!");

        // PROPERTIES //
        // Create properties for patient class if they don't exist
        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
        }
        
        if (patient.getProperty("contact_list") == null) {
            patient.createProperty("contact_list", OType.EMBEDDEDLIST, OType.STRING);
        }

        if (patient.getProperty("event_list") == null) {
            patient.createProperty("event_list", OType.EMBEDDEDLIST, OType.STRING);
        }
        System.out.println("Created properties for patient class!");

        // Create properties for event vertex class if it doesn't exist
        if (event.getProperty("event_id") == null) {
            event.createProperty("event_id", OType.STRING);
        }
        System.out.println("Created property for event class!");

        db.close();
    }

    public void clearDB() {
        db = orient.open("test", "root", "rootpwd");
        OResultSet rs = db.command("DELETE VERTEX FROM V");
        rs.close();
        db.close();
    }

    public OVertex createPatient(String mrn) {
        db = orient.open("test", "root", "rootpwd");
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", mrn);
        result.save();
        db.close();

        return result;
    }

    public OVertex createPatient(String patient_mrn, List<String> contact_list, List<String> event_list) {
        db = orient.open("test", "root", "rootpwd");
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.setProperty("contact_list", contact_list);
        result.setProperty("event_list", event_list);
        result.save();
        db.close();
        return result;
    }

    public void createPatientEdge(String fromPatient, String toPatient) {
        db = orient.open("test", "root", "rootpwd");
        OResultSet rs = db.command("CREATE EDGE has_contacted FROM " + fromPatient + " TO " + toPatient);
        rs.close();
        db.close();
    }

    public OVertex createEvent(String event_id) {
        db = orient.open("test", "root", "rootpwd");
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
        db.close();
        return result;
    }

    public void createEventEdge(String patientRID, String eventRID) {
        db = orient.open("test", "root", "rootpwd");
        OResultSet rs = db.command("CREATE EDGE attended_event FROM " + patientRID + " TO " + eventRID);
        rs.close();
        db.close();
    }

    public void getContacts(String patient_mrn) {
        db = orient.open("test", "root", "rootpwd");
        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            //System.out.println("--NEW ITEM--");
            System.out.println("contact: " + item.getProperty("patient_mrn"));
            //System.out.println(item.getClass().getName());
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        db.close();
    }

    public String getRID(String vertexClass, String property, String value ) {
        db = orient.open("test", "root", "rootpwd");

        OResultSet rs = db.query("SELECT @rid FROM " + vertexClass + " WHERE " + property + " = '" + value + "'");
        OResult result = rs.next();
        //System.out.println("RID: " + result.getProperty("@rid"));
        String rid = result.getProperty("@rid").toString();
        
        rs.close();
        db.close();
        
        return rid;
    }
    
    public Boolean ifEventExists(String event_id) {
        db = orient.open("test", "root", "rootpwd");
        
        Boolean exists = false;
        
        // Query the database for event node with specific id
        OResultSet rs = db.query("SELECT FROM event WHERE event_id = '" + event_id + "'");
        
        if(rs.hasNext()) {
            // Get result from result set
            OResult result = rs.next();
            
            // DEBUGGING STUFF
            //System.out.println("ifEventExists function; Is result of query a vertex? Answer: " + result.isVertex());
            //System.out.println("Event_id value of current result: "+ result.getProperty("event_id"));

            
            if (result.getProperty("event_id").equals(event_id)) {
                exists = true;
                //System.out.println("Event node with event_id(" + event_id + ") already exists!");
                //System.out.println(result.getProperty("@rid").getClass().getName());
            }
        }
        // Close the result set
        rs.close();
        db.close();
        return exists;
    }

    public Boolean ifPatientExists(String patient_mrn) {
        db = orient.open("test", "root", "rootpwd");
        
        Boolean exists = false;
        
        // Query the database for patient node with specific mrn
        OResultSet rs = db.query("SELECT FROM patient WHERE patient_mrn = '" + patient_mrn + "'");
        //System.out.println("Number of results returned from query:" + String.valueOf(rs.estimateSize()));

        if(rs.hasNext()) {
            // Get result from result set
            OResult result = rs.next();
            // DEBUGGING STUFF

            //System.out.println("ifPatientExists function; Is result of query a vertex? Answer: " + result.isVertex());
            //System.out.println("MRN value of current result: "+ result.getProperty("patient_mrn"));
            
            if (result.getProperty("patient_mrn").equals(patient_mrn)) {
                exists = true;
                //System.out.println("Patient node with mrn(" + patient_mrn + ") already exists!");
                //System.out.println(result.getProperty("event_list").getClass().getName());
            }
        }
        // Close the result set
        rs.close();
        db.close();
        return exists;
    }

    public void updateContactList(String patient_mrn, List<String> contact_list) {
        db = orient.open("test", "root", "rootpwd");

        String contacts = contact_list.toString();
        // Remove the bracket at the beginning of the string
        contacts = contacts.substring(1);

        // Remove the bracket at the end of the string
        contacts = contacts.substring(0, contacts.length()- 1);

        // Remove all spaces in the string
        contacts = contacts.replaceAll("\\s","");

        //System.out.println(contacts);
        OResultSet rs = db.command("UPDATE patient SET contact_list = '" + contacts + "' WHERE patient_mrn = " + patient_mrn);
        
        db.close();
    }

    public void updateEventList(String patient_mrn, List<String> event_list) {
        db = orient.open("test", "root", "rootpwd");

        String events = event_list.toString();
        // Remove the bracket at the beginning of the string
        events = events.substring(1);

        // Remove the bracket at the end of the string
        events = events.substring(0, events.length()- 1);

        // Remove all spaces in the string
        events = events.replaceAll("\\s","");

        //System.out.println(events);
        OResultSet rs = db.command("UPDATE patient SET event_list = '" + events + "' WHERE patient_mrn = " + patient_mrn);

        rs.close();
        db.close();
    }
}
