package com.example.worldartexport;

import com.example.worldartexport.model.ImdbCsvDto;
import com.example.worldartexport.model.WorldArtCsvDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.github.bonigarcia.wdm.WebDriverManager;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.jetbrains.annotations.NotNull;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class Importer implements ApplicationListener<ApplicationReadyEvent> {

    private static final String IMDB_URI = "https://www.imdb.com/title/";

    WebClient.Builder webClientBuilder;
    CsvMapper csvMapper;
    ApplicationContext applicationContext;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // parseListFromWorldArt(webClient);
        // processDataAgainstImdb();
        // pushToGoogleDrive();
        SpringApplication.exit(applicationContext);
    }

    @SneakyThrows
    private void pushToGoogleDrive() {
        FileSystemResource csvFile = new FileSystemResource("/home/dgray/Downloads/imdb-export.csv");
        Sheets service = createSheetsService();

        List<String> rows = Arrays.stream(new String(csvFile.getInputStream().readAllBytes()).split(StringUtils.LF)).toList();

        int index = 2;

        for (String row : rows) {
            JsonNode node = csvMapper.readTree(row);

            ImdbCsvDto imdbCsvDto = new ImdbCsvDto(
                    node.get(0).asText(), node.get(1).asText(), node.get(2).asText(), node.get(3).asInt()
            );

            String range = String.format("A%1$s:F%1$s", index);
            String urlFormula = String.format("=HYPERLINK(\"%s\", \"IMDb\")", imdbCsvDto.imdbUrl());

            ValueRange valueRange = new ValueRange();
            valueRange.setRange(range);
            valueRange.setValues(List.of(List.of(imdbCsvDto.name(), imdbCsvDto.date(), urlFormula, imdbCsvDto.score())));

            service
                    .spreadsheets()
                    .values()
                    .append("1FXMLtFrUSCtJ451g8KKj1JhXYyAfUYvMbiPXRWapyeo", range, valueRange)
                    .setValueInputOption("USER_ENTERED")
                    .execute();

            sleep();

            index++;
        }
    }

    @SneakyThrows
    private Sheets createSheetsService() {
        NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GsonFactory jacksonFactory = GsonFactory.getDefaultInstance();

        GoogleCredentials credentials = GoogleCredentials.fromStream(
                new FileSystemResource("/home/dgray/Downloads/credentials.json").getInputStream()
        );

        return new Sheets.Builder(httpTransport, jacksonFactory, new HttpCredentialsAdapter(credentials))
                .setApplicationName("sheets-importer")
                .build();
    }

    @NotNull
    private WebClient buildWebClient(String baseUrl) {
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize((int) DataSize.ofGigabytes(NumberUtils.LONG_ONE).toBytes()))
                .build();


        WebClient.Builder builder = webClientBuilder.exchangeStrategies(strategies);

        if (StringUtils.isNotBlank(baseUrl)) {
            builder.baseUrl(baseUrl);
        }

        return builder.build();
    }

    private void parseListFromWorldArt() {
        WebClient webClient = buildWebClient(null);

        WebDriverManager.chromedriver().setup();
        ChromeDriver chromeDriver = new ChromeDriver();
        login(chromeDriver);
        List<WorldArtCsvDto> data = processListPage(chromeDriver, webClient);
        writeToCsvFile(data, WorldArtCsvDto.class, "world-art-export.csv");
        chromeDriver.close();
    }

    @SneakyThrows
    private void processDataAgainstImdb() {
        FileSystemResource csvFile = new FileSystemResource("/home/dgray/Downloads/world-art-export.csv");

        List<String> rows = Arrays.stream(new String(csvFile.getInputStream().readAllBytes()).split(StringUtils.LF)).toList();
        List<ImdbCsvDto> data = new ArrayList<>();

        WebDriverManager.chromedriver().setup();
        ChromeDriver chromeDriver = new ChromeDriver();

        for (String row : rows) {
            JsonNode node = csvMapper.readTree(row);

            WorldArtCsvDto worldArtDto = new WorldArtCsvDto(
                    node.get(0).asText(), node.get(1).asText(), node.get(2).asText(), node.get(3).asInt()
            );

            String englishName = fetchEnglishNameFromImdb(worldArtDto, chromeDriver);
            data.add(new ImdbCsvDto(englishName, worldArtDto.date(), worldArtDto.imdbUrl(), worldArtDto.score()));

            sleep();
        }

        writeToCsvFile(data, ImdbCsvDto.class, "imdb-export.csv");
    }

    @SneakyThrows
    private String fetchEnglishNameFromImdb(WorldArtCsvDto worldArtRow, WebDriver webDriver) {
        String result = worldArtRow.name();

        if (StringUtils.isNotBlank(worldArtRow.imdbUrl())) {
            webDriver.get(worldArtRow.imdbUrl());

            try {
                WebElement elementByXPath = ((ChromeDriver) webDriver)
                        .findElementByXPath("/html/body/div[2]/main/div/section[1]/section/div[3]/section/section/div[1]/div[1]/h1");

                result = elementByXPath.getText();
            } catch (Exception e) {}

            sleep();
        }

        log.debug("Processing film: {}", worldArtRow.imdbUrl());

        return result;
    }

    private void login(RemoteWebDriver webDriver) {
        webDriver.get("http://www.world-art.ru/enter.php");
        By submitButton = By.xpath("/html/body/center/table[6]/tbody/tr/td/table/tbody/tr/td[3]/center/table/tbody/tr/td/form/table/tbody/tr/td/input[3]");
        waitFor(submitButton, webDriver);

        webDriver.findElementByName("login").sendKeys("");  // TODO login here
        webDriver.findElementByName("pass").sendKeys("");   // TODO password here
        webDriver.findElement(submitButton).click();
    }

    private List<WorldArtCsvDto> processListPage(RemoteWebDriver webDriver, WebClient webClient) {
        webDriver.get("http://www.world-art.ru/account/list.php?id=83253&list_id=31373&sector=cinema&sort=10&edit_mode=0");
        List<WebElement> tableRows = webDriver
                .findElementByXPath("/html/body/center/table[6]/tbody/tr/td/table/tbody/tr/td[3]/table[6]/tbody")
                .findElements(By.tagName("tr"));

        return tableRows
                .stream()
                .skip(NumberUtils.LONG_ONE) // 0 is header
                .map(element -> {
                    List<WebElement> columns = element.findElements(By.tagName("td"));

                    String name = columns.get(1).findElement(By.tagName("a")).getText();
                    Integer score = getScore(columns);
                    String date = getDate(columns);
                    String imdbUrl = getImdbUrl(webClient, columns);

                    sleep();

                    return new WorldArtCsvDto(name, date, imdbUrl, score);
                })
                .toList();
    }

    /**
     * In order not to spam server
     */
    @SneakyThrows
    private void sleep() {
        TimeUnit.SECONDS.sleep(1L);
    }

    @SneakyThrows
    private void writeToCsvFile(List<?> data, Class<?> dtoClass, String fileName) {
        FileSystemResource csvFile = new FileSystemResource("/home/dgray/Downloads/" + fileName);
        ObjectWriter writer = csvMapper.writerWithTypedSchemaFor(dtoClass);

        try (var outputStream = new FileOutputStream(csvFile.getFile(), true)) {
            for (Object row : data) {
                outputStream.write(writer.writeValueAsString(row).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private String getImdbUrl(WebClient webClient, List<WebElement> columns) {
        String result = StringUtils.EMPTY;
        String worldArtUrl = columns.get(1).findElement(By.tagName("a")).getAttribute("href");

        String htmlPage = webClient
                .get()
                .uri(worldArtUrl)
                .retrieve()
                .toEntity(String.class)
                .block(Duration.ofSeconds(5L))
                .getBody();

        if (htmlPage.contains(IMDB_URI)) {
            int beginning = htmlPage.indexOf(IMDB_URI);
            int ending = htmlPage.indexOf("'", beginning);
            result = htmlPage.substring(beginning, ending);
        }

        return result;
    }

    private Integer getScore(List<WebElement> columns) {
        String text = columns.get(4).getText();
        return "--".equalsIgnoreCase(text) ? 5 : Integer.parseInt(text);
    }

    private String getDate(List<WebElement> columns) {
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter worldArtPattern = DateTimeFormatter.ofPattern("yyyy.MM.dd");
        String text = columns.get(7).getText();

        return "нет данных".equalsIgnoreCase(text)
                ? pattern.format(LocalDate.now())
                : pattern.format(worldArtPattern.parse(text));
    }

    private void waitFor(By condition, RemoteWebDriver webDriver) {
        Wait<RemoteWebDriver> wait = new FluentWait<>(webDriver)
                .withTimeout(Duration.ofSeconds(30L))
                .pollingEvery(Duration.ofSeconds(3L))
                .ignoring(NoSuchElementException.class);

        wait.until(driver -> driver.findElement(condition));
    }

}
