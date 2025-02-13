package vttp.batch5.paf.movies.repositories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import jakarta.json.JsonObject;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

 // TODOx: Task 2.3
 // You can add any number of parameters and return any type from the method
 // You can throw any checked exceptions from the method
 // Write the native Mongo query you implement in the method in the comments
    /*
    db.imdb.insertMany([
        {_id: ?, title: ?, directors:?, overview: ?, tagline: ?, genres: ?, imdb_rating: ?, imdb_votes: ?},
        {_id: ?, title: ?, directors:?, overview: ?, tagline: ?, genres: ?, imdb_rating: ?, imdb_votes: ?}, 
        ...
    ])
    */
 //
    public boolean batchInsertMovies(List<JsonObject> movies) {
    
        List<Document> docsToInsert = new ArrayList<>();

        for (JsonObject m : movies) {
            Document d = new Document();
            d.put("_id", m.getString("imdb_id"));
            d.put("title", m.getString("title"));
            d.put("directors", m.getString("director"));
            d.put("overview", m.getString("overview"));
            d.put("tagline", m.getString("tagline"));
            d.put("genres", m.getString("genres"));
            d.put("imdb_rating", m.getInt("imdb_rating"));
            d.put("imdb_votes", m.getInt("imdb_votes"));

            docsToInsert.add(d);
        }

        Collection<Document> newDocs = mongoTemplate.insert(docsToInsert, "imdb");

        // Check if all the docs are added
        return newDocs.size() == 25;
    }

    // TODO: Task 2.4
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    /*
        db.imdb.insert({
            ids: ["", "", ...],
            error: "<Exception error>",
            timestamp: <Date of exception>
        })
    */ 
    //
    public void logError(Document errorDoc) {
        mongoTemplate.insert(errorDoc, "errors");
    }

    // TODO: Task 3
    // Write the native Mongo query you implement in the method in the comments
    //
    //    native MongoDB query here
    //


    }
