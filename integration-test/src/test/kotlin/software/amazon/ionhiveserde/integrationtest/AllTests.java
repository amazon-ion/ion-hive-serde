package software.amazon.ionhiveserde.integrationtest;

import static software.amazon.ionhiveserde.integrationtest.TestConfigurationKt.getTestSuiteConfiguration;
import static software.amazon.ionhiveserde.integrationtest.docker.DockerUtilKt.waitForHiveServer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    SuccessfulRoundTripTest.class,
    SuccessfulJdbcTest.class
})
public class AllTests {

    @BeforeClass
    public static void beforeAll() {
        Base.setConfiguration(getTestSuiteConfiguration());
        waitForHiveServer();
    }

    @AfterClass
    public static void afterAll() {
        Base.hive().close();
    }
}
