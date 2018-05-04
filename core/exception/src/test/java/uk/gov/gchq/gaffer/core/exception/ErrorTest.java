package uk.gov.gchq.gaffer.core.exception;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.DebugUtil;
import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ErrorTest {
    private static final String DETAILED_MSG = "detailedMessage";
    private static final String SIMPLE_MSG = "simpleMessage";


    @Before
    public void setUp() throws Exception {
        setDebugMode(null);
    }

    @After
    public void after() throws Exception {
        setDebugMode(null);
    }

    @Test
    public void shouldNotBuildDetailedMessage() throws Exception {
        // Given
        setDebugMode("false");

        // When
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        assertNotEquals("Detailed message is present when built and debug is false", DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithMissingPropertyFlag() {
        // Given
        setDebugMode(null);

        // When
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        assertNotEquals("Detailed message is present when build and debug is false", DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithIncorrectPropertyFlad() {
        // Given
        setDebugMode("wrong");

        // When
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        assertNotEquals("Detailed message is present when build and debug is false", DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldBuildDetailedMessage() throws Exception {
        // Given
        setDebugMode("true");

        // When
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        assertEquals("Detailed message is not present when built and debug is true", DETAILED_MSG, error.getDetailMessage());
    }

    private void setDebugMode(final String value) {
        if (value == null) {
            System.clearProperty(DebugUtil.DEBUG);
        } else {
            System.setProperty(DebugUtil.DEBUG, value);
        }
        DebugUtil.updateDebugMode();
    }
}
