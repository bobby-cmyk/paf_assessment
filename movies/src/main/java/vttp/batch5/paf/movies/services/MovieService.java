package vttp.batch5.paf.movies.services;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoRepo;

  @Autowired
  private MySQLMovieRepository sqlRepo;

  // TODO: Task 2
  @Transactional
  public void batchInsertMovies(List<JsonObject> movies) {
    if (!sqlRepo.batchInsertMovies(movies)) {
      throw new RuntimeException("Error occured during insertion to MySQL");
    }
    if (!mongoRepo.batchInsertMovies(movies)) {

      throw new RuntimeException("Error occured during insertion to MongoDB");
    }
  }

  public void logError(Document errorDoc) {
    mongoRepo.logError(errorDoc);
  }

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void getProlificDirectors() {
  }

  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {

  }

}
