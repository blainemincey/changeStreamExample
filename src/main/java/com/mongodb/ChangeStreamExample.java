package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;

/**
 *
 */
public class ChangeStreamExample {

    /**
     *
     */
    public ChangeStreamExample() {

        this.runChangeStreamExample();

    }

    /**
     *
     */
    public void runChangeStreamExample() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));
        MongoDatabase database = mongoClient.getDatabase("changeStreamDB");
        MongoCollection<Document> collection = database.getCollection("changeStreamCollection");

        // lambda expression
        Block<ChangeStreamDocument<Document>> printBlock = changeStreamDocument -> myApplyMethod(changeStreamDocument);

        collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach(printBlock);

        /**
        collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        **/

    }

    /**
     *
     * @param changeStreamDocument
     */
    public void myApplyMethod(ChangeStreamDocument changeStreamDocument) {
        System.out.println(changeStreamDocument);
        System.out.println("Namespace: " + changeStreamDocument.getNamespace());
        System.out.println("Op Type: " + changeStreamDocument.getOperationType().getValue());

    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Starting: " + new java.util.Date());

        new ChangeStreamExample();

        System.out.println("Ending: " + new java.util.Date());
    }

}
