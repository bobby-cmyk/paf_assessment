package vttp.batch5.paf.movies.bootstrap;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.services.MovieService;

//TODO: Task 2

@Component
public class Dataloader implements CommandLineRunner{

  @Value("${DATA_PATH}")
  private String dataPath; 

  @Autowired
  private MovieService movieSvc;

  @Override
  public void run(String... args) throws Exception {
    

    System.out.println(">>> Print from DataLoader\n");

    //TODO: -> Complete this
    List<JsonObject> movies = new ArrayList<>();

    try {
      movieSvc.batchInsertMovies(movies);
    }

    catch (RuntimeException e) {
      Document errorDoc = createErrorDoc(movies, e.getMessage());
      movieSvc.logError(errorDoc);
    }

  }

  private Document createErrorDoc(List<JsonObject> movies, String errorMsg) {
    
    Document doc = new Document();
    
    List<String> imdbIds = new ArrayList<>();

    for (JsonObject m : movies) {
      imdbIds.add(m.getString("imdb_id"));
    }

    Date date = new Date();

    doc.put("ids", imdbIds);
    doc.put("error", errorMsg);
    doc.put("timestamp", date);

    return doc;
  }

}
