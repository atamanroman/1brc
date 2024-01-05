package dev.morling.onebrc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAverage_atamanroman {

  private static final String FILE = "./measurements.txt";

  static Map<String, Avg> averages = new ConcurrentHashMap<>();

  public static void main(String[] args) throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(FILE,
        "rw"); FileChannel channel = file.getChannel()) {
      MappedByteBuffer mbb = channel.map(MapMode.READ_ONLY, 0, channel.size());
      parseFile(mbb);
    }
    printSummary();
  }

  private static void parseFile(MappedByteBuffer mbb) {
    var buf = new ByteArrayOutputStream();
    while (mbb.position() < mbb.limit()) {
      byte ch = mbb.get();
      if (ch != '\n') {
        buf.write(ch);
      } else {
        var line = buf.toByteArray();
        var sepPos = findSeparator(line);
        var city = parseCity(line, sepPos);
        addReading(city, parseTemp(line, sepPos));
        buf.reset();
      }
    }
  }

  private static int findSeparator(byte[] line) {
    var split = -1;
    for (int n = 0; n < line.length; n++) {
      if (line[n] == ';') {
        split = n;
      }
    }
    return split;
  }

  private static void addReading(String city, double temp) {
    System.out.println("Read:" + city + " with temp=" + temp);
    averages.compute(city, (_, v) -> {
      if (v == null) {
        return new Avg(city, temp);
      } else {
        v.add(temp);
        return v;
      }
    });
  }

  private static double parseTemp(byte[] line, int split) {
    return Double.parseDouble(new String(Arrays.copyOfRange(line, split + 1, line.length)));
  }

  private static String parseCity(byte[] line, int split) {
    return new String(Arrays.copyOfRange(line, 0, split));
  }

  private static void printSummary() {
    averages.values().stream().sorted(Comparator.comparing(o -> o.city)).forEachOrdered(avg -> {
      System.out.println(STR."\{avg.city}: \{avg.calculate()}");
      System.out.println(STR."(was sum=\{avg.sum}/ count=\{avg.count}");
    });
  }


  static class Avg {

    private final String city;
    private double sum = 0;
    private long count = 0;

    public Avg(String city) {
      this.city = city;
    }

    public Avg(String city, double first) {
      this(city);
      sum = first;
      count = 1;
    }

    void add(double temp) {
      sum += temp;
      count += 1;
    }

    public double calculate() {
      return sum / count;
    }
  }
}
