package cs505finaltemplate.Topics;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505finaltemplate.Launcher;
import io.siddhi.query.api.expression.condition.In;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;

    final Type typeOfListMap = new TypeToken<List<Map<String,String>>>(){}.getType();
    final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();

    //private String EXCHANGE_NAME = "patient_data";
    Map<String,String> config;

    public TopicConnector(Map<String,String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            //create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            //create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
            hospitalListChannel(channel);
            vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void patientListChannel(Channel channel) {
        try {

            System.out.println("Creating patient_list channel");

            String topicName = "patient_list";

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Paitent List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");


                List<TestingData> incomingList = gson.fromJson(message, typeListTestingData);
                for (TestingData testingData : incomingList) {

                    //Data to send to CEP
                    Map<String,String> zip_entry = new HashMap<>();
                    zip_entry.put("zip_code",String.valueOf(testingData.patient_zipcode));
                    String testInput = gson.toJson(zip_entry);
                    //uncomment for debug
                    //System.out.println("testInput: " + testInput);

                    //insert into CEP
                    Launcher.cepEngine.input("testInStream",testInput);

                    
                    // Get properties for new vertices in database
                    String patient_mrn = testingData.patient_mrn;
                    List<String> contact_list = testingData.contact_list;
                    List<String> event_list = testingData.event_list;

                    System.out.println("*Java Class*");
                    //System.out.println("\ttesting_id = " + testingData.testing_id);
                    //System.out.println("\tpatient_name = " + testingData.patient_name);
                    System.out.println("\tpatient_mrn = " + testingData.patient_mrn);
                    System.out.println("\tpatient_zipcode = " + testingData.patient_zipcode);
                    System.out.println("\tpatient_status = " + testingData.patient_status);
                    System.out.println("\tcontact_list = " + testingData.contact_list);
                    System.out.println("\tevent_list = " + testingData.event_list);

                    // Put patient data into database; creates patient and event vertices with associated edges
                    enterPatientDataInDB(patient_mrn, contact_list, event_list);

                    /*
                    // Create new patient vertex if a node with that mrn doesn't exist
                    OVertex patient = Launcher.graphDBEngine.createPatient(patient_mrn, contact_list, event_list);
                    //Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);

                    // Create edges between patient and events listed in event_list
                    for (int i = 0; i < event_list.size(); i++) {
                        // Get current event_id 
                        String event_id = event_list.get(i);

                        // Check if event node already exists
                        if (Launcher.graphDBEngine.ifEventExists(event_id) == false) {
                            // Create new event vertex                            
                            OVertex event = Launcher.graphDBEngine.createEvent(event_id);
                            // Create attended_event edge between patient and event
                            Launcher.graphDBEngine.createEventEdge(patient,event);
                        } 
                        
                        else { // Event node already exists
                            // Get existing event node
                            OVertex event = Launcher.graphDBEngine.getVertex("event", "event_id", event_id);
                            // Create attended_event edge between patient and event
                            Launcher.graphDBEngine.createEventEdge(patient, event);
                        }
                        
                    }

                    // Create has_contacted edge between patients 
                    for (int i = 0; i < contact_list.size(); i++) {
                        // Get current patient_mrn
                        String mrn = contact_list.get(i);

                        OVertex contactedPatient = Launcher.graphDBEngine.createPatientForEdge(mrn);

                        Launcher.graphDBEngine.createPatientEdge(patient, contactedPatient);
                    }
                    
                    

                    
                    
                    

                    */
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                //new message
                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> hospitalData : incomingList) {
                    int hospital_id = Integer.parseInt(hospitalData.get("hospital_id"));
                    String patient_name = hospitalData.get("patient_name");
                    String patient_mrn = hospitalData.get("patient_mrn");
                    int patient_status = Integer.parseInt(hospitalData.get("patient_status"));
                    //do something with each each record.
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");


            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");

                //convert string to class
                List<Map<String,String>> incomingList = gson.fromJson(message, typeOfListMap);
                for (Map<String,String> vaxData : incomingList) {
                    int vaccination_id = Integer.parseInt(vaxData.get("vaccination_id"));
                    String patient_name = vaxData.get("patient_name");
                    String patient_mrn = vaxData.get("patient_mrn");
                    //do something with each each record.
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }


    // Put patient data into database
    public void enterPatientDataInDB(String patient_mrn, List<String> contact_list, List<String> event_list) {
        // Check if patient already exists, if not then continue in process
        if (Launcher.graphDBEngine.ifPatientExists(patient_mrn) == false) {
            // Create patient vertex
            OVertex patient = Launcher.graphDBEngine.createPatient(patient_mrn, contact_list, event_list);

            // Add edges from patient to event nodes
            for (int i = 0; i < event_list.size(); i++) {
                // Get current event in list
                String curr_event = event_list.get(i);
                
                // Check if event node exists, if not create event node
                if (Launcher.graphDBEngine.ifEventExists(curr_event) == false) {
                    OVertex event = Launcher.graphDBEngine.createEvent(curr_event);

                    // Add edge from patient to event
                    // Get RIDs for each node
                    String patientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String eventRID = Launcher.graphDBEngine.getRID("event", "event_id", curr_event); 

                    // Create attended_event edge
                    Launcher.graphDBEngine.createEventEdge(patientRID, eventRID); 
                } else {
                    // Event node already exists, get RID and add edge between it and patient
                    // Get RIDs of patient and event
                    String patientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String eventRID = Launcher.graphDBEngine.getRID("event", "event_id", curr_event); 

                    // Add edge between patient and event
                    Launcher.graphDBEngine.createEventEdge(patientRID, eventRID);
                }
            }// End event_list for loop

            // Add edges from patient to patient vertices based on contact_list
            for (int i = 0; i < contact_list.size(); i++) {
                // Get current patient in contact list
                String curr_contact = contact_list.get(i);

                // Check if current patient already has a vertex, if not create partial patient vertex
                if (Launcher.graphDBEngine.ifPatientExists(curr_contact) == false) {
                    // Create partial patient vertex (partial because it does not have event or contact lists)
                    OVertex partialPatient = Launcher.graphDBEngine.createPatient(curr_contact);

                    // Add edge from patient to patient
                    // Get RIDs 
                    String fromPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String toPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", curr_contact);

                    // Do not create edges that start and end at same vertex
                    if (fromPatientRID.equals(toPatientRID) == false) {
                        // Create has_contacted edge between patient vertices
                        Launcher.graphDBEngine.createPatientEdge(fromPatientRID, toPatientRID);
                    }
                } else {
                    // Patient vertex already exists, create edge between the two vertices
                    // Get RIDs
                    String fromPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String toPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", curr_contact);
                    
                    // Do not create edges that start and end at same vertex
                    if (fromPatientRID.equals(toPatientRID) == false) {
                        // Create has_contacted edge between vertices 
                        Launcher.graphDBEngine.createPatientEdge(fromPatientRID, toPatientRID);
                    }
                }
            }// End contact_list for loop

        } else {
            // Patient vertex already exists in the database, assuming that it is a "parital" vertex (only has the patient_mrn and nothing else stored)
            // Set values of event and contact lists
            Launcher.graphDBEngine.updateContactList(patient_mrn, contact_list);
            Launcher.graphDBEngine.updateEventList(patient_mrn, event_list);
            
            // Add edges from patient to event nodes
            for (int i = 0; i < event_list.size(); i++) {
                // Get current event in list
                String curr_event = event_list.get(i);
                
                // Check if event node exists, if not create event node
                if (Launcher.graphDBEngine.ifEventExists(curr_event) == false) {
                    OVertex event = Launcher.graphDBEngine.createEvent(curr_event);

                    // Add edge from patient to event
                    // Get RIDs for each node
                    String patientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String eventRID = Launcher.graphDBEngine.getRID("event", "event_id", curr_event); 

                    // Create attended_event edge
                    Launcher.graphDBEngine.createEventEdge(patientRID, eventRID); 
                } else {
                    // Event node already exists, get RID and add edge between it and patient
                    // Get RIDs of patient and event
                    String patientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String eventRID = Launcher.graphDBEngine.getRID("event", "event_id", curr_event); 

                    // Add edge between patient and event
                    Launcher.graphDBEngine.createEventEdge(patientRID, eventRID);
                }
            }// End event_list for loop

            // Add edges from patient to patient vertices based on contact_list
            for (int i = 0; i < contact_list.size(); i++) {
                // Get current patient in contact list
                String curr_contact = contact_list.get(i);

                // Check if current patient already has a vertex, if not create partial patient vertex
                if (Launcher.graphDBEngine.ifPatientExists(curr_contact) == false) {
                    // Create partial patient vertex (partial because it does not have event or contact lists)
                    OVertex partialPatient = Launcher.graphDBEngine.createPatient(curr_contact);

                    // Add edge from patient to patient
                    // Get RIDs 
                    String fromPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String toPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", curr_contact);

                    // Do not create edges that start and end at same vertex
                    if (fromPatientRID.equals(toPatientRID) == false) {
                        // Create has_contacted edge between patient vertices
                        Launcher.graphDBEngine.createPatientEdge(fromPatientRID, toPatientRID);
                    }
                } else {
                    // Patient vertex already exists, create edge between the two vertices
                    // Get RIDs
                    String fromPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", patient_mrn);
                    String toPatientRID = Launcher.graphDBEngine.getRID("patient", "patient_mrn", curr_contact);
                    
                    // Do not create edges that start and end at same vertex
                    if (fromPatientRID.equals(toPatientRID) == false) {
                        // Create has_contacted edge between vertices 
                        Launcher.graphDBEngine.createPatientEdge(fromPatientRID, toPatientRID);
                    }
                }
            }// End contact_list for loop
        }
    }

}
