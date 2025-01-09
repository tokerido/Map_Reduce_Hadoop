# Word Prediction Knowledge Base

## Overview

This project constructs a knowledge base for Hebrew word prediction based on the Google 3-Gram Hebrew dataset. The solution is implemented using Hadoop MapReduce and runs on the Amazon Elastic MapReduce (EMR) service. The output is a set of word trigrams (w1, w2, w3) with their conditional probabilities \(P(w3 | w1, w2)\). The results are stored in a sorted order based on the first two words (ascending) and probabilities (descending).

## Steps Overview

This project consists of multiple MapReduce steps to process the n-gram data:

1. **Step 1**: Preprocessing the n-gram data to calculate word and sequence counts.
2. **Step 2**: Calculating conditional probabilities for each trigram.
3. **Step 3**: Sorting the output by keys (w1, w2) and descending probabilities for w3.

## Input

- **Google 3-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data`.
- **Google 2-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data`.
- **Google 1-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data`.
- These datasets consist of word n-grams (1-gram, 2-gram, 3-gram) with their respective frequencies.<br>
where the format of each line is:<br>
`ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE`

note: The datasets are in Version 1.

## Step Descriptions

### Step 1

- **Objective**: Process the raw n-gram data to compute counts for individual words, bigrams, and trigrams.
- **Input**: N-gram datasets (1-gram, 2-gram, 3-gram).
- **Output**: Consolidated counts in the format: <br> `{key: nGram , value: C0:_ C1:_ C2:_ N3:_}` or `{key: nGram , value: C0:_ N1:_ N2:_ N3:_}`.

1. Mapper parses each line from the input data.
2. Stop words and invalid entries are filtered out.
3. Emits counts for 1-gram, 2-gram, and 3-gram entries twice (once for the C1,C2 count and another for the N1,N2 count).
4. Reducer aggregates counts for each unique n-gram.

### Step 2: Probability Calculation

- **Objective**: Calculate conditional probabilities \(P(w3 | w1, w2)\) for trigrams.
- **Input**: Aggregated counts from Step 1 : <br> `{key: nGram , value: C0:_ C1:_ C2:_ N3:_}` or `{key: nGram , value: C0:_ N1:_ N2:_ N3:_}`.
- **Output**: Trigrams with their calculated probabilities. `{key: 3-Gram , value: pobability}`.

#### Formula


$$
P(w_3 | w_1, w_2) = k_3 \cdot \frac{N_3}{C_2} + (1-k_3) \cdot k_2 \cdot \frac{N_2}{C_1} + (1-k_3) \cdot (1-k_2) \cdot \frac{N_1}{C_0}
$$

$$
k2 = \frac{log(N_2 + 1) + 1}{log(N_2 + 1) + 2}  ,  k3 = \frac{log(N_3 + 1) + 1}{log(N_3 + 1) + 2}
$$

Where:

- \(N_1\): Counts of the word, bigram, and trigram occurrences.
- \(N_2\): Counts of the word, bigram, and trigram occurrences.
- \(N_3\): Counts of the word, bigram, and trigram occurrences.
- \(C_0\): Total counts for all words, bigrams, and trigrams.
- \(C_1\): Total counts for all words, bigrams, and trigrams.
- \(C_2\): Total counts for all words, bigrams, and trigrams.
- \(k_2, k_3\): Backoff weights.

1. Mapper reads and parses counts for each trigram.
2. Reducer computes the probability using the formula.
3. Outputs the trigram and its probability.

### Step 3: Sorting

- **Objective**: Sort trigrams by \((w1, w2)\) and their probabilities in descending order.
- **Input**: Trigrams with probabilities from Step 3. `{key: 3-Gram , value: pobability}`
- **Output**: Ordered trigrams with probabilities. `{key: 3-Gram , value: pobability}`

1. Mapper emits trigrams with probabilities as keys.
2. GroupingComparator ensures grouping by \((w1, w2)\).
3. Reducer writes sorted trigrams with their probabilities.

## Running the Project

1. Update the `App.java` file with your AWS credentials and S3 bucket name.
2. Compile and Package the code:
   ```
   mvn clean package
   ```
3. Upload the compiled JARs of the steps to the S3 bucket.
   * ensure that the location of the JARs is correct in the `App.java` file.
   * ensure that there are no output folders under the same name as the output folders that are in the code.
4. Execute the `Location in your files/App.jar` to launch the EMR job:
   ```
   java -jar target/App.jar
   ```

## Reports

## Key-Value Pairs for Different Runs

|                   |           |
|----------------------|------------------------|
| Mapper Input         | Input values for Run 1 |
| Mapper Output        | Output values for Run 1|
| Reducer Input        | Input values for Run 1 |
| Reducer Output       | Output values for Run 1|
| Combiner Input       | Input values for Run 1 |
| Combiner Output      | Output values for Run 1|

### Scalability

- full dataset runtime with default num of mappers: .
- full dataset runtime with _ mappers: .
- small sample of the dataset runtime with default num of mappers: .
- small sample of the dataset runtime with _ mappers: .

### Interesting Results

- For selected word pairs, display the top-5 most likely next words.

