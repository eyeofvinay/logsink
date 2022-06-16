import org.junit.Assert;
import org.junit.Test;

public class ExampleTest {
    @Test
    public void testAdd() {
        Example example = new Example();
        int actual = example.add(1, 2);
        int expected= 3;
        Assert.assertEquals(expected, actual);
    }
}
