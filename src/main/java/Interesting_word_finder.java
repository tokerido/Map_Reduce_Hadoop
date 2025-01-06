import java.io.*;
import java.util.*;

public class Interesting_word_finder {
    public static void main(String[] args) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            reader = new BufferedReader(new FileReader("/Users/lizgokhvat/Desktop/Projects/AWS/Map_Reduce_Hadoop/output/output1/part-r-00000 (6)"));
            writer = new BufferedWriter(new FileWriter("output.txt"));

            Map<String, List<Pair>> trigramMap = new HashMap<String, List<Pair>>();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length != 4) continue;

                String key = parts[0] + " " + parts[1];
                String w3 = parts[2];
                double prob = Double.parseDouble(parts[3]);

                if (!trigramMap.containsKey(key)) {
                    trigramMap.put(key, new ArrayList<Pair>());
                }
                trigramMap.get(key).add(new Pair(w3, prob));
            }

            for (Map.Entry<String, List<Pair>> entry : trigramMap.entrySet()) {
                if (entry.getValue().size() > 5) {
                    writer.write(entry.getKey() + "\n");

                    Collections.sort(entry.getValue(), new Comparator<Pair>() {
                        public int compare(Pair a, Pair b) {
                            return Double.compare(b.probability, a.probability);
                        }
                    });

                    for (int i = 0; i < 5; i++) {
                        Pair pair = entry.getValue().get(i);
                        writer.write(pair.w3 + " " + pair.probability + "\n");
                    }
                    writer.write("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) reader.close();
                if (writer != null) writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class Pair {
        String w3;
        double probability;

        Pair(String w3, double probability) {
            this.w3 = w3;
            this.probability = probability;
        }
    }
}