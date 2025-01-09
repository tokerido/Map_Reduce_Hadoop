import java.io.*;
import java.util.*;
import java.nio.file.*;

public class Interesting_word_finder {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java Interesting_word_finder <input_folder> <output_file>");
            return;
        }

        String inputFolder = args[0];
        String outputFile = args[1];
        Map<String, List<Pair>> trigramMap = new HashMap<>();

        try {
            // Get all files in the input folder
            Files.list(Paths.get(inputFolder))
                .filter(Files::isRegularFile)
                .forEach(file -> processFile(file, trigramMap));

            // Write results to output file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
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
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processFile(Path filePath, Map<String, List<Pair>> trigramMap) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length != 4) continue;

                String key = parts[0] + " " + parts[1];
                String w3 = parts[2];
                double prob = Double.parseDouble(parts[3]);

                trigramMap.computeIfAbsent(key, k -> new ArrayList<>())
                         .add(new Pair(w3, prob));
            }
        } catch (IOException e) {
            System.err.println("Error processing file: " + filePath);
            e.printStackTrace();
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