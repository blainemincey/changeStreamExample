package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;

public class ResumeChangeStreamExample {

    private ResumeChangeStreamExample() {

    }

    public static void process() {

        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));
        MongoDatabase database = mongoClient.getDatabase("changeStreamDB");
        MongoCollection<Document> collection = database.getCollection("changeStreamCollection");

        // Create the change stream cursor.
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();


        // Insert a test document into the collection.
        //collection.insertOne(Document.parse("{username: 'alice123', name: 'Alice'}"));
        ChangeStreamDocument<Document> next = cursor.next();
        System.out.println(next);

        // Get the resume token from the last document we saw in the previous change stream cursor.
        BsonDocument resumeToken = next.getResumeToken();
        System.out.println(resumeToken);

        // Pass the resume token to the resume after function to continue the change stream cursor.
        cursor = collection.watch().resumeAfter(resumeToken).iterator();

        // Insert a test document.
        //collection.insertOne(Document.parse("{test: 'd'}"));

        // Block until the next result is returned
        next = cursor.next();
        System.out.println(next);
        cursor.close();

    }

    public static void main(String[] args) {
        System.out.println("Starting: " + new java.util.Date());

        ResumeChangeStreamExample.process();

        System.out.println("Ending: " + new java.util.Date());
    }
}
