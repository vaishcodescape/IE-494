package pmj.bdp.dynamo.lab10;

import java.net.URI;
import java.time.Duration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Connection to DynamoDB Local, same settings as the setup sheet
 * (endpoint http://localhost:8000, access key 1234 / secret 1234, region ap-south-1).
 */
final class Dynamo {

    static final String ENDPOINT = System.getProperty("ddb.endpoint", "http://localhost:8000");

    private Dynamo() {
    }

    /** Low-level client: talks to DynamoDB Local instead of AWS. SDK defaults (30 s socket timeout, retries). */
    static DynamoDbClient client() {
        return builder(Duration.ofSeconds(30)).build();
    }

    /**
     * DescribeTable on DynamoDB Local counts every row of its SQLite table, which takes minutes
     * on 1.65 million items, so that call gets a 30-minute timeout and no retries.
     */
    static DynamoDbClient longTimeoutClient() {
        Duration limit = Duration.ofMinutes(30);
        return builder(limit)
                .overrideConfiguration(o -> o.retryStrategy(AwsRetryStrategy.doNotRetry())
                        .apiCallTimeout(limit).apiCallAttemptTimeout(limit))
                .build();
    }

    private static DynamoDbClientBuilder builder(Duration socketTimeout) {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("1234", "1234")))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(socketTimeout));
    }

    /** High-level (Enhanced) client built on top of the low-level one. */
    static DynamoDbEnhancedClient enhanced(DynamoDbClient client) {
        return DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
    }
}
