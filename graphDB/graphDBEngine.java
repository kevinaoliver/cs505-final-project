package cs505-final-project.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class GraphDBEngine {


    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine() {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard
        //OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        //ODatabaseSession db = orient.open("test", "root", "rootpwd");

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
    }
    if (attended_event == null) {
        attended_event = db.createEdgeClass("attended_event");
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

        /*
        OVertex patient_0 = createPatient(db, "mrn_0");
        OVertex patient_1 = createPatient(db, "mrn_1");
        OVertex patient_2 = createPatient(db, "mrn_2");
        OVertex patient_3 = createPatient(db, "mrn_3");

        //patient 0 in contact with patient 1
        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
        edge1.save();
        //patient 2 in contact with patient 0
        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
        edge3.save();

        getContacts(db, "mrn_0");
        */

        db.close();
        //orient.close();

    }

    public Boolean ifPatientExists(String patient_mrn) {
        db = orient.open("test", "root", "rootpwd");
        
        Boolean exists = true;
        
        // Query the database for patient node with specific mrn
        OResultSet rs = db.query("SELECT FROM patient WHERE patient_mrn = '" + patient_mrn + "'");

        // Check how many records are in the result set, if there are none then set exists to false
        if(rs.elementStream().count() == 0) {
            exists = false;
        }

        // Close the result set
        rs.close();
        db.close();
        return exists;
    }

    public Boolean ifEventExists(String event_id) {
        db = orient.open("test", "root", "rootpwd");
        
        Boolean exists = true;
        
        // Query the database for event node with specific id
        OResultSet rs = db.query("SELECT FROM event WHERE event_id = '" + event_id + "'");
        
        // Check how many records are in the result set, if there are none then set exists to false
        if(rs.elementStream().count() == 0) {
            exists = false;
        }

        // Close the result set
        rs.close();
        db.close();
        return exists;
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

    public OVertex createEvent(String event_id) {
        db = orient.open("test", "root", "rootpwd");
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
        db.close();
        return result;
    }

    public void createEventEdge(OVertex patient, OVertex event) {
        db = orient.open("test", "root", "rootpwd");
        OEdge edge = patient.addEdge(event, "attended_event");
        edge.save();
        db.close();
    }

    public void createPatientEdge(OVertex fromPatient, OVertex toPatient) {
        db = orient.open("test", "root", "rootpwd");
        OEdge edge = fromPatient.addEdge(toPatient, "has_contacted");
        edge.save();
        db.close();
    }

    public OVertex createPatientForEdge(String mrn) {
        db = orient.open("test", "root", "rootpwd");
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", mrn);
        result.save();
        db.close();

        return result;
    }

    public void getContacts(String patient_mrn) {
        db = orient.open("test", "root", "rootpwd");
        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        db.close();
    }

    public void clearDB() {
        db = orient.open("test", "root", "rootpwd");
        OResultSet rs = db.command("DELETE VERTEX FROM V");
        rs.close();
        db.close();
    }

}
