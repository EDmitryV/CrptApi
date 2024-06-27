import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.net.http.HttpRequest;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class CrptApi {

    public enum DocumentType {
        LP_INTRODUCE_GOODS
    }

    public class Description {
        @JsonProperty("participantInn")
        final String participantInn;

        public Description(String participantInn) {
            this.participantInn = participantInn;
        }
    }

    public class Product {
        @JsonProperty("certificate_document")
        final String certificateDocument;
        @JsonProperty("certificate_document_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        final Date certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        final String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        final String ownerInn;
        @JsonProperty("producer_inn")
        final String producerInn;
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        final Date productionDate;
        @JsonProperty("tnved_code")
        final String tnvedCode;
        @JsonProperty("uit_code")
        final String uitCode;
        @JsonProperty("uitu_code")
        final String uituCode;

        public Product(String certificateDocument,
                       Date certificateDocumentDate,
                       String certificateDocumentNumber,
                       String ownerInn,
                       String producerInn,
                       Date productionDate,
                       String tnvedCode,
                       String uitCode,
                       String uituCode) {
            this.certificateDocument = certificateDocument;
            this.certificateDocumentDate = certificateDocumentDate;
            this.certificateDocumentNumber = certificateDocumentNumber;
            this.ownerInn = ownerInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.tnvedCode = tnvedCode;
            this.uitCode = uitCode;
            this.uituCode = uituCode;
        }
    }

    public class CrptApiDocument {

        @JsonProperty("description")
        final Description description;
        @JsonProperty("doc_id")
        final String docId;
        @JsonProperty("doc_status")
        final String docStatus;
        @JsonProperty("doc_type")
        final DocumentType docType;
        @JsonProperty("importRequest")
        final boolean importRequest;
        @JsonProperty("owner_inn")
        final String ownerInn;
        @JsonProperty("participant_inn")
        final String participantInn;
        @JsonProperty("producer_inn")
        final String producerInn;
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        final Date productionDate;
        @JsonProperty("production_type")
        final String productionType;
        @JsonProperty("products")
        final List<Product> products;
        @JsonProperty("reg_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        final Date regDate;
        @JsonProperty("reg_number")
        final String regNumber;

        public CrptApiDocument(Description description,
                               String docId,
                               String docStatus,
                               DocumentType docType,
                               boolean importRequest,
                               String ownerInn,
                               String participantInn,
                               String producerInn,
                               Date productionDate,
                               String productionType,
                               List<Product> products,
                               Date regDate,
                               String regNumber) {
            this.description = description;
            this.docId = docId;
            this.docStatus = docStatus;
            this.docType = docType;
            this.importRequest = importRequest;
            this.ownerInn = ownerInn;
            this.participantInn = participantInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.productionType = productionType;
            this.products = products;
            this.regDate = regDate;
            this.regNumber = regNumber;
        }
    }

    private class DocumentSignPare {
        final CrptApiDocument doc;
        final String sign;

        private DocumentSignPare(CrptApiDocument document, String sign) {
            this.doc = document;
            this.sign = sign;
        }
    }

    private final TimeUnit timeUnit;
    private final int requestLimit;
    private AtomicBoolean isRequestsLoopActive;

    private final ConcurrentLinkedQueue<Date> requestsDatesQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<DocumentSignPare> documentsSignsToRequestQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<DocumentSignPare> documentsSignsInRequestProcess = new ConcurrentLinkedQueue<>();
    private final HttpClient client = HttpClient.newHttpClient();
    private final String url = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        isRequestsLoopActive.set(false);
    }

    public void createDocument(CrptApiDocument doc, String sign) {
        documentsSignsToRequestQueue.add(new DocumentSignPare(doc, sign));
        if (isRequestsLoopActive.compareAndSet(false, true)) {
            new Thread(() -> {
                while (!documentsSignsToRequestQueue.isEmpty() || !requestsDatesQueue.isEmpty()) {
                    Date currentTime = new Date();
                    while (requestsDatesQueue.size() >= requestLimit) {
                        while (timeUnit.convert(currentTime.getTime() - requestsDatesQueue.peek().getTime(), TimeUnit.MILLISECONDS) > timeUnit.convert(1, timeUnit)) {
                            requestsDatesQueue.poll();
                        }
                        long timeDiffMillis = currentTime.getTime() - requestsDatesQueue.peek().getTime();
                        long limitDiff = timeUnit.convert(1, timeUnit) - timeUnit.convert(timeDiffMillis, TimeUnit.MILLISECONDS);
                        int limitDiffInNanos = (int) (limitDiff % 1000000);
                        long limitDiffInMillis = timeUnit.toMillis(limitDiff / 1000000);
                        try {
                            Thread.sleep(limitDiffInMillis, limitDiffInNanos);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    DocumentSignPare docSignPare = documentsSignsToRequestQueue.peek();
                    String json;
                    try {
                        json = objectMapper.writeValueAsString(docSignPare.doc);
                    } catch (Exception e) {
                        e.printStackTrace();
                        documentsSignsToRequestQueue.poll();
                        continue;
                    }
                    HttpRequest request = HttpRequest.newBuilder()
                            .header("Content-Type", "application/json")
                            .header("charset", "utf-8")
                            //так как не понял для чего подпись - предположил, что это токен
                            .header("Authorization", "Bearer " + docSignPare.sign)
                            .uri(URI.create(url))
                            .POST(HttpRequest.BodyPublishers.ofString(json))
                            .build();

                    documentsSignsInRequestProcess.add(documentsSignsToRequestQueue.poll());
                    requestsDatesQueue.add(new Date());
                    Consumer<HttpResponse<String>> callback = response -> {
                        if (response.statusCode() != 200) {
                            documentsSignsToRequestQueue.add(docSignPare);
                        }
                        documentsSignsInRequestProcess.remove(docSignPare);
                    };
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept(callback);
                }
                isRequestsLoopActive.set(false);
            }).start();
        }
    }
}
