package vttp.batch5.paf.movies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import vttp.batch5.paf.movies.bootstrap.Dataloader;

@SpringBootApplication
public class MoviesApplication implements CommandLineRunner {

	@Autowired
	private Dataloader dataLoader;
	public static void main(String[] args) {
		SpringApplication.run(MoviesApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println(">>> APP STARTED");
		dataLoader.loadData();
	}
}
