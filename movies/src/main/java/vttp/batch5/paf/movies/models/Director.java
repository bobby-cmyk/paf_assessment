package vttp.batch5.paf.movies.models;

import java.util.List;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

public class Director {
    private String name;
    private Integer numberOfMovies = 0;
    private List<String> imdbIds;
    private Double revenue=0.0;
    private Double budget=0.0;
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public Integer getNumberOfMovies() {
        return numberOfMovies;
    }
    public void setNumberOfMovies(Integer numberOfMovies) {
        this.numberOfMovies = numberOfMovies;
    }
    public List<String> getImdbIds() {
        return imdbIds;
    }
    public void setImdbIds(List<String> imdbIds) {
        this.imdbIds = imdbIds;
    }
    public Double getRevenue() {
        return revenue;
    }
    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }
    public Double getBudget() {
        return budget;
    }
    public void setBudget(Double budget) {
        this.budget = budget;
    }

    public JsonObject toJsonObject(){
        JsonObjectBuilder objBuilder = Json.createObjectBuilder();

        objBuilder.add("director_name", name);
        objBuilder.add("movies_count", numberOfMovies);
        objBuilder.add("total_revenue", revenue);
        objBuilder.add("total_budget", budget);

        return objBuilder.build();
    }

    public JsonObject toJsonObjectForReport(){
        JsonObjectBuilder objBuilder = Json.createObjectBuilder();

        objBuilder.add("director", name);
        objBuilder.add("count", numberOfMovies);
        objBuilder.add("revenue", revenue);
        objBuilder.add("budget", budget);

        return objBuilder.build();
    }

    @Override
    public String toString() {
        return "Director [name=" + name + ", numberOfMovies=" + numberOfMovies + ", imdbIds=" + imdbIds + ", revenue="
                + revenue + ", budget=" + budget + "]";
    }
}
