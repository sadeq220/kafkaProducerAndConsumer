package ir.sadeqcloud.stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.annotation.*;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Test
@Tag("fast")
/**
 * it's a compose-annotation
 */
public @interface FastUnitTest {
}
