# Word Prediction Knowledge Base

## Overview

This project constructs a knowledge base for Hebrew word prediction based on the Google 3-Gram Hebrew dataset. The solution is implemented using Hadoop MapReduce and runs on the Amazon Elastic MapReduce (EMR) service. The output is a set of word trigrams (w1, w2, w3) with their conditional probabilities \(P(w3 | w1, w2)\). The results are stored in a sorted order based on the first two words (ascending) and probabilities (descending).

## Steps Overview

This project consists of multiple MapReduce steps to process the n-gram data:

1. **Step 1**: Initial processing of n-gram data and count calculation.
2. **Step 2**: Data organization/joining and calculations of intermediate values for the probability.
3. **Step 3**: Calculating conditional probabilities for each trigram.
4. **Step 4**: Sorting the output by keys (w1, w2) and descending probabilities for w3.

## Input

- **Google 3-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data`.
- **Google 2-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data`.
- **Google 1-Gram Hebrew Dataset**: Stored in Amazon S3 at `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data`.
- These datasets consist of word n-grams (1-gram, 2-gram, 3-gram) with their respective frequencies.<br>
  where the format of each line is:<br>
  `ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE`

note: The datasets are in Version 1.

## Step Descriptions

### Step 1: Initial Processing and Count Calculation

- **Objective**: Process the raw n-gram data to compute counts for individual words, bigrams, and trigrams.
- **Input**: N-gram datasets (1-gram, 2-gram, 3-gram).
- **Output**: Consolidated counts in the format: <br> `{key: nGram , value: count}`

1. Mapper parses each line from the input data.
2. Stop words and invalid entries are filtered out.
3. Emits counts for 1-gram, 2-gram, and 3-gram entries.
4. Reducer aggregates counts for each unique n-gram.

### Step 2: Data Organization

- **Objective**: Join the counts for 1-gram, 2-gram, and 3-gram entries to create intermediate values for the probabilities for each trigram.
- **Input**: Aggregated counts from Step 1 : <br> `{key: nGram , value: count}`.
- **Output**: Intermediate values for the probabilities for each trigram. `{key: 3-Gram , value: C0:_ C1:_ C2:_ N3:_}` or `{key: 3-Gram , value: C0:_ N1:_ N2:_ N3:_}`

1. Mapper reads and parses counts for each n-gram. For trigrams it emits twice (once for the C1,C2 count and another for the N1,N2 count).
2. Reducer computes intermediate values for C0, C1, C2, N1, N2, N3.
3. Outputs the trigram and its intermediate probabilities.

### Step 3: Probability Calculation

- **Objective**: Calculate conditional probabilities \(P(w3 | w1, w2)\) for trigrams.
- **Input**: Aggregated counts from Step 1 : <br> `{key: nGram , value: C0:_ C1:_ C2:_ N3:_}` or `{key: nGram , value: C0:_ N1:_ N2:_ N3:_}`.
- **Output**: Trigrams with their calculated probabilities. `{key: 3-Gram , value: probability}`.

#### Formula

$$
P(w_3 | w_1, w_2) = k_3 \cdot \frac{N_3}{C_2} + (1-k_3) \cdot k_2 \cdot \frac{N_2}{C_1} + (1-k_3) \cdot (1-k_2) \cdot \frac{N_1}{C_0}
$$

$$
k2 = \frac{log(N_2 + 1) + 1}{log(N_2 + 1) + 2}  ,  k3 = \frac{log(N_3 + 1) + 1}{log(N_3 + 1) + 2}
$$

Where:

- \(N_1\): is the number of times w3 occurs.
- \(N_2\): is the number of times sequence (w2,w3) occurs.
- \(N_3\): is the number of times sequence (w1,w2,w3) occurs.
- \(C_0\): is the total number of word instances in the corpus.
- \(C_1\): is the number of times w2 occurs.
- \(C_2\): is the number of times sequence (w1,w2) occurs.
- \(k_2, k_3\): Backoff weights.

1. Mapper reads and parses counts for each trigram.
2. Reducer computes the probability using the formula.
3. Outputs the trigram and its probability.

### Step 4: Sorting

- **Objective**: Sort trigrams by \((w1, w2)\) and their probabilities in descending order.
- **Input**: Trigrams with probabilities from Step 3. `{key: 3-Gram , value: probability}`
- **Output**: Ordered trigrams with probabilities. `{key: 3-Gram , value: probability}`

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
   - ensure that the location of the JARs is correct in the `App.java` file.
   - ensure that there are no output folders under the same name as the output folders that are in the code.
4. Execute the `Location in your files/App.jar` to launch the EMR job:
   ```
   java -jar target/App.jar
   ```

## Reports

### Step 1

| Run                         | with local aggregation        | without local aggregation       |
|-----------------------------|-------------------------------|---------------------------------|
| Key-Value Pairs to Reducers | 4,145,215                     | 205,621,932                     |
| Size of Data to Reducers    | 58,258,235 bytes (~55.55 MB)  | 4,211,126,503 bytes (~4.21 GB)  |

### Step 2

| Run                         | with local aggregation | without local aggregation    |
|-----------------------------|------------------------|------------------------------|
| Key-Value Pairs to Reducers | combiner not used      | 4,036,287                    |
| Size of Data to Reducers    | combiner not used      | 84,634,278 bytes (~80.7 MB)  |

### Step 3

| Run                         | with local aggregation | without local aggregation     |
|-----------------------------|------------------------|-------------------------------|
| Key-Value Pairs to Reducers | combiner not used      | 735,524                       |
| Size of Data to Reducers    | combiner not used      | 21,426,995 bytes (~20.42 MB)  |

### Step 4

| Run                         | with local aggregation | without local aggregation   |
|-----------------------------|------------------------|-----------------------------|
| Key-Value Pairs to Reducers | combiner not used      | 367,762                     |
| Size of Data to Reducers    | combiner not used      | 8,312,058 bytes (~7.92 MB)  |



### Scalability

- full dataset runtime with default `71` mappers and 6 instances: `15 minutes, 33 seconds`.
- full dataset runtime with `140` mappers and 6 instances: `17 minutes, 22 seconds`.
- small sample of the dataset runtime with default `3` mappers and 1 instance: `7 minutes, 57 seconds`.
- small sample of the dataset runtime with `6` mappers and 1 instance: `7 minutes, 55 seconds`.
