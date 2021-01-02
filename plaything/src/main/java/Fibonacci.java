import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 *生成斐波拉契数
 */
public class Fibonacci {
    public static void main(String[] args) {
        Stream<Long> fib = Stream.generate(new FibonacciSupplier());
        fib.limit(50).forEach(System.out::println);
    }

    private static class FibonacciSupplier implements Supplier<Long> {
        long a = 0;
        long b = 1;
        long c = 0;
        public Long get() {
            c = a + b;
            a = b;
            b = c;
            return c;
        }
    }
}
