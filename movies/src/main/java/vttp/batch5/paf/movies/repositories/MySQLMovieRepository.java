package vttp.batch5.paf.movies.repositories;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

  @Autowired
  private JdbcTemplate sqlTemplate;

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public boolean batchInsertMovies(List<JsonObject> movies) {

    String SQL_INSERT = """
      INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) 
      VALUES(?,?,?,?,?,?,?);
    """;

    List<Object[]> params = movies.stream()
      .map(m -> new Object[]{
        m.getString("imdb_id"),
        m.getInt("vote_average"),
        m.getInt("vote_count"),
        m.getString("release_date"),
        m.getInt("revenue"),
        m.getInt("budget"),
        m.getInt("runtime")})
      .collect(Collectors.toList());

    int added[] = sqlTemplate.batchUpdate(SQL_INSERT, params);

    return added.length == 25;
  }
  
  // TODO: Task 3


}
