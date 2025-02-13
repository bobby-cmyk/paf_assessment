package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;
import vttp.batch5.paf.movies.services.MovieService;

//TODO: Task 2

@Component
public class Dataloader{

  @Value("${DATA_PATH}")
  private String dataPath; 

  @Autowired
  private MovieService movieSvc;

  @Autowired
  private MySQLMovieRepository sqlRepo;
  
  public void loadData() throws Exception{

    // Check if contents of moviews post loaded
    if (sqlRepo.isSqlEmpty()) {

      System.out.println("SQL is empty");

      ZipFile zf = new ZipFile("../" + dataPath);

      ZipEntry ze = zf.entries().nextElement();

      BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze), "UTF-8"));

      String line = br.readLine();

      List<JsonObject> movies = new ArrayList<>();

      while (line != null) {
        JsonReader jsonReader = Json.createReader(new StringReader(line));
        JsonObject movieObj = jsonReader.readObject();

        String dateString = movieObj.getString("release_date");

        if (getYear(dateString)>= 2018) {
          //TODO -> impute missing attributes
          movies.add(movieObj);
        }
      }

      zf.close();
      br.close();

      System.out.println(">>> Inserting to sql and mongodb\n");

      batchInsertMovies(movies, 25);

      System.out.println(">>> Finish inserting to sql and mongodb\n");
      
    }
  }

  private int getYear(String dateString) {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
    LocalDate date = LocalDate.parse(dateString, fmt);
    return date.getYear();
  }


  private void batchInsertMovies(List<JsonObject> movies, int partitionSize) {
    int counter = 0;

    for (List<JsonObject> chunk : Lists.partition(movies, partitionSize)) {
      try {
        movieSvc.batchInsertMovies(chunk);
      }
      catch (RuntimeException e) {
        Document errorDoc = createErrorDoc(movies, e.getMessage());
        movieSvc.logError(errorDoc);
      }
      // Keep track of which batch
      counter++;
      System.out.printf(">>> Inserted batch: %d\n", counter);
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
