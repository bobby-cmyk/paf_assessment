package vttp.batch5.paf.movies.controllers;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.servlet.http.HttpSession;
import vttp.batch5.paf.movies.models.Director;
import vttp.batch5.paf.movies.services.MovieService;

@RestController
@RequestMapping("/api")
public class MainController {

  @Autowired
  private MovieService movieSvc;

  // TODO:Task 3
  @GetMapping(path="/summary")
  public ResponseEntity<String> getProlificDirectors(
    @RequestParam(name="count") int count, HttpSession sess)
  {
    //Set count in session
    sess.setAttribute("count", count);
    
    List<Director> directors = movieSvc.getProlificDirectors(count);

    JsonArrayBuilder arrBuilder = Json.createArrayBuilder();
    
    for (Director d : directors) {
      arrBuilder.add(d.toJsonObject());
    }
    return ResponseEntity.ok().body(arrBuilder.build().toString());
  }

  @GetMapping(path="/summary/pdf")
  public ResponseEntity<byte[]> generatePDFReport(
    HttpSession sess
  ) throws Exception {

    //Get count from session
    int count = (Integer) sess.getAttribute("count");

    movieSvc.generatePDFReport(count);

    File file = new File("output.pdf");

    byte[] contents = Files.readAllBytes(file.toPath());

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_PDF);
    
    String filename = "output.pdf";
    headers.setContentDispositionFormData(filename, filename);
    headers.setCacheControl("must-revalidate, post-check=0, pre-check=0");

    ResponseEntity<byte[]> response = new ResponseEntity<>(contents, headers, HttpStatus.OK);
    return response;
  }
}
