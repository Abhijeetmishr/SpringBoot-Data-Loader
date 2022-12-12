package io.dataloader.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.format.datetime.DateFormatter;

import io.dataloader.betterreadsdataloader.Repository.AuthorRepository;
import io.dataloader.betterreadsdataloader.Repository.BookRepository;
import io.dataloader.betterreadsdataloader.connection.DataStaxAstraProperties;
import io.dataloader.betterreadsdataloader.entity.Author;
import io.dataloader.betterreadsdataloader.entity.Book;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

	@Autowired
	private AuthorRepository authorRepository;

	@Autowired
	private BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		System.out.println("File Path: " + path);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// Read & parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// Construct Author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace(("/authors/"), ""));
					// Persist using Repository
					System.out.println("Saving author " + author.getName() + "...");
					authorRepository.save(author);
				} catch (JSONException ex) {
					ex.printStackTrace();
				}

			});
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {
			lines.limit(100).forEach(line -> {
				// Read & parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// Construct Author object
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject descriptObj = jsonObject.optJSONObject("description");
					if (descriptObj != null) {
						book.setDescription(descriptObj.optString("value"));
					}
					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if (publishedObj != null) {
						String dateStr = publishedObj.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}
					JSONArray coversJSONArray = jsonObject.optJSONArray("covers");
					if (coversJSONArray != null) {
						List<String> coverIds = new ArrayList<>();
						for (int i = 0; i < coversJSONArray.length(); i++) {
							coverIds.add(coversJSONArray.optString(i));
						}
						book.setCoverIds(coverIds);
					}
					JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
					if (authorsJSONArr != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorsJSONArr.length(); i++) {
							String authorStr = authorsJSONArr.optJSONObject(i).optJSONObject("author").optString("key")
									.replace("/authors/", "");
							authorIds.add(authorStr);
						}
						book.setAuthorId(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(author -> {
									if (!author.isPresent())
										return "Unknown Author";
									return author.get().getName();
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);
						System.out.println("Saving book " + book.getName() + "...");
						bookRepository.save(book);
					}

				} catch (JSONException ex) {
					ex.printStackTrace();
				}

			});
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthors();
		// initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
