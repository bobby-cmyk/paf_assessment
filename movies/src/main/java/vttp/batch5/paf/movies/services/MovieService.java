package vttp.batch5.paf.movies.services;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.util.JRLoader;
import net.sf.jasperreports.json.data.JsonDataSource;
import vttp.batch5.paf.movies.models.Director;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoRepo;

  @Autowired
  private MySQLMovieRepository sqlRepo;

  @Value("${MY_NAME}")
  private String myName;

  @Value("${MY_BATCH}")
  private String myBatch;

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
  public List<Director> getProlificDirectors(int n) {
    List<Director> directors = mongoRepo.getTopDirectors(n);

    for (Director d : directors) {
        List<String> imdbIds = d.getImdbIds();

        d.setRevenue(sqlRepo.getDirectorRevenue(imdbIds));
        d.setBudget(sqlRepo.getDirectorBudget(imdbIds));
    }

    return directors;
  }

  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport(int count) throws Exception{

    JsonObjectBuilder reportDsBuilder = Json.createObjectBuilder();

    reportDsBuilder.add("name", myName);
    reportDsBuilder.add("batch", myBatch);
    JsonObject reportDsObj = reportDsBuilder.build();

    JsonDataSource reportDS = new JsonDataSource(new ByteArrayInputStream(reportDsObj.toString().getBytes()));  

    List<Director> directors = getProlificDirectors(count);

    JsonArrayBuilder arrBuilder = Json.createArrayBuilder();
    
    for (Director d : directors) {
      arrBuilder.add(d.toJsonObjectForReport());
    }

    JsonArray directorsDSarr = arrBuilder.build();

    JsonDataSource directorsDS  = new JsonDataSource(new ByteArrayInputStream(directorsDSarr.toString().getBytes()));  

    Map<String, Object> params = new HashMap<>();
    
    params.put("DIRECTOR_TABLE_DATASET", directorsDS);

    JasperReport report = (JasperReport) JRLoader.loadObjectFromFile("../data/director_movies_report.jasper");

    JasperPrint print = JasperFillManager.fillReport(report, params, reportDS);

    JasperExportManager.exportReportToPdfFile(print, "output.pdf");
  }
}
