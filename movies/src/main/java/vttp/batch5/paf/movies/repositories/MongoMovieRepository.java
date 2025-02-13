package vttp.batch5.paf.movies.repositories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.stereotype.Repository;


import jakarta.json.JsonObject;

import static vttp.batch5.paf.movies.repositories.MongoConstants.*;
import vttp.batch5.paf.movies.models.Director;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    /*
     * db.imdb.find({})
     
    public boolean isMongoEmpty() {
        
    }

    */

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
            docsToInsert.add(jsonObjToDoc(m));
        }
        
        Collection<Document> newDocs = mongoTemplate.insert(docsToInsert, IMDB_COLLECTION);

        // Check if all the docs are added
        return newDocs.size() > 0;
    }

    private Document jsonObjToDoc(JsonObject m) {
        Document d = new Document();

        d.put(I_ID, m.getString("imdb_id"));
        d.put(I_TITLE, m.getString("title"));
        d.put(I_DIRECTORS, m.getString("director"));
        d.put(I_OVERVIEW, m.getString("overview"));
        d.put(I_TAGLINE, m.getString("tagline"));
        d.put(I_GENRES, m.getString("genres"));
        d.put(I_RATING, m.getInt("imdb_rating"));
        d.put(I_VOTES, m.getInt("imdb_votes"));
        
        return d;
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
/*
    db.imdb.aggregate([
        {
            $group: {
                _id: "$directors",
                number_of_movies: { $sum : 1},
                imdb_ids: {$push: "$_id"}
            }
        },
        {
            $sort: {number_of_movies : -1}
        },
        {
            $limit: <n>
        }
    ])
 */
//
    public List<Director> getTopDirectors(int n) {

        GroupOperation groupByDirector = Aggregation.group(I_DIRECTORS)
            .count().as(I_NUMBER_OF_MOVIES)
            .push(I_ID).as(I_IDS);

        SortOperation sortByNumberOfMovies = Aggregation.sort(Direction.DESC, I_NUMBER_OF_MOVIES);

        LimitOperation limitByN = Aggregation.limit(n);

        Aggregation pipeline = Aggregation.newAggregation(groupByDirector, sortByNumberOfMovies, limitByN);

        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, IMDB_COLLECTION, Document.class);

        List<Director> directors = results.getMappedResults().stream()
            .map(doc -> docToDirector(doc))
            .collect(Collectors.toList());

        for (Director d : directors) {
            System.out.println(d);
        }

        return directors;
    }

    private Director docToDirector(Document doc) {

        Director d = new Director();

        // director name becomes the primary name
        d.setName(doc.getString("_id"));
        d.setNumberOfMovies(doc.getInteger(I_NUMBER_OF_MOVIES));
        d.setImdbIds(doc.getList(I_IDS, String.class));

        return d;
    }
}
