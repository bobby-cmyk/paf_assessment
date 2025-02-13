package vttp.batch5.paf.movies.repositories;

public class MySQLQueries {
    public static String COUNT_RECORDS = """
        SELECT COUNT(*) as count FROM imdb;
    """;

   public static String SQL_INSERT = """
      INSERT INTO imdb (imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) 
      VALUES(?,?,?,?,?,?,?);
    """;

    public static String SQL_GET_REV = """
      SELECT SUM(revenue) AS total_revenue 
      FROM imdb 
      WHERE imdb_id IN (:id)   
    """;

    public static String SQL_GET_BUDGET = """
        SELECT SUM(budget) AS total_budget 
        FROM imdb 
        WHERE imdb_id IN (:id)   
      """;
}
