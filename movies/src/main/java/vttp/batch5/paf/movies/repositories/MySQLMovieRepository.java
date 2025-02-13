package vttp.batch5.paf.movies.repositories;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import static vttp.batch5.paf.movies.repositories.MySQLQueries.*;

import jakarta.json.JsonObject;

@Repository
public class MySQLMovieRepository {

  @Autowired
  private JdbcTemplate sqlTemplate;

  @Autowired
  private NamedParameterJdbcTemplate sqlNamedTemplate;

  public boolean isSqlEmpty() {
    
    SqlRowSet rs = sqlTemplate.queryForRowSet(COUNT_RECORDS);

    rs.next();

    int count = rs.getInt("count");

    return count == 0;
  }

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public boolean batchInsertMovies(List<JsonObject> movies) {

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

    return added.length > 0;
  }
  
  // DONE TASK 3
  public Double getDirectorRevenue(List<String> imdbIds) {

    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("id", imdbIds);

    SqlRowSet rs = sqlNamedTemplate.queryForRowSet(SQL_GET_REV, params);

    rs.next();

    return rs.getDouble("total_revenue");
  }

  public Double getDirectorBudget(List<String> imdbIds) {

    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("id", imdbIds);

    SqlRowSet rs = sqlNamedTemplate.queryForRowSet(SQL_GET_BUDGET, params);

    rs.next();

    return rs.getDouble("total_budget");
  }

}
