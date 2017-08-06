/*-
 *
 *  * Copyright 2015 Skymind,Inc.
 *  *
 *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *    you may not use this file except in compliance with the License.
 *  *    You may obtain a copy of the License at
 *  *
 *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *    Unless required by applicable law or agreed to in writing, software
 *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *    See the License for the specific language governing permissions and
 *  *    limitations under the License.
 *
 */

package org.deeplearning4j.spark.text.functions;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import edu.berkeley.nlp.util.Counter;
import edu.berkeley.nlp.util.Pair;
import org.deeplearning4j.models.embeddings.loader.VectorsConfiguration;
import org.deeplearning4j.models.word2vec.Huffman;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.deeplearning4j.models.word2vec.wordstore.inmemory.AbstractCache;
import org.deeplearning4j.spark.text.accumulators.WordFreqAccumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A spark based text pipeline
 * with minimum word frequency and stop words
 *
 * @author Adam Gibson
 */
@SuppressWarnings("unchecked")
public class TextPipeline {
    //params
    private JavaRDD<String> corpusRDD;
    private int numWords;
    private int nGrams;
    private String tokenizer;
    private String tokenizerPreprocessor;
    private List<String> stopWords = new ArrayList<>();
    //Setup
    private JavaSparkContext sc;
    private Accumulator<Counter<String>> wordFreqAcc;
    private Broadcast<List<String>> stopWordBroadCast;
    // Return values
    private JavaRDD<Pair<List<String>, AtomicLong>> sentenceWordsCountRDD;
    private final VocabCache<VocabWord> vocabCache = new AbstractCache<>();
    private Broadcast<VocabCache<VocabWord>> vocabCacheBroadcast;
    private JavaRDD<List<VocabWord>> vocabWordListRDD;
    private JavaRDD<AtomicLong> sentenceCountRDD;
    private long totalWordCount;
    private boolean useUnk;
    private VectorsConfiguration configuration;

    // Empty Constructor
    public TextPipeline() {}

    // Constructor
    public TextPipeline(JavaRDD<String> corpusRDD, Broadcast<Map<String, Object>> broadcasTokenizerVarMap)
                    throws Exception {
        this.setRDDVarMap(corpusRDD, broadcasTokenizerVarMap);
        // Setup all Spark variables
        this.setup();
    }

    public void setRDDVarMap(JavaRDD<String> corpusRDD, Broadcast<Map<String, Object>> broadcasTokenizerVarMap) {
        Map<String, Object> tokenizerVarMap = broadcasTokenizerVarMap.getValue();
        this.corpusRDD = corpusRDD;
        numWords = (int) tokenizerVarMap.get("numWords");
        // TokenizerFunction Settings
        nGrams = (int) tokenizerVarMap.get("nGrams");
        tokenizer = (String) tokenizerVarMap.get("tokenizer");
        tokenizerPreprocessor = (String) tokenizerVarMap.get("tokenPreprocessor");
        useUnk = (boolean) tokenizerVarMap.get("useUnk");
        configuration = (VectorsConfiguration) tokenizerVarMap.get("vectorsConfiguration");
        // Remove Stop words
        // if ((boolean) tokenizerVarMap.get("removeStop")) {
        this.stopWords = (List<String>) tokenizerVarMap.get("stopWords");
        //    }
    }

    private void setup() {
        // Set up accumulators and broadcast stopwords
        sc = new JavaSparkContext(this.corpusRDD.context());
        wordFreqAcc = this.sc.accumulator(new Counter<String>(), new WordFreqAccumulator());
        stopWordBroadCast = this.sc.broadcast(this.stopWords);
    }

    public JavaRDD<List<String>> tokenize() {
        if (this.corpusRDD == null) {
            throw new IllegalStateException("corpusRDD not assigned. Define TextPipeline with corpusRDD assigned.");
        }
        return this.corpusRDD.map(new TokenizerFunction(this.tokenizer, this.tokenizerPreprocessor, this.nGrams));
    }

    public JavaRDD<Pair<List<String>, AtomicLong>> updateAndReturnAccumulatorVal(JavaRDD<List<String>> tokenizedRDD) {
        // Update the 2 accumulators
        UpdateWordFreqAccumulatorFunction accumulatorClassFunction =
                        new UpdateWordFreqAccumulatorFunction(this.stopWordBroadCast, this.wordFreqAcc);
        JavaRDD<Pair<List<String>, AtomicLong>> sentenceWordsCountRDD = tokenizedRDD.map(accumulatorClassFunction);

        // Loop through each element to update accumulator. Count does the same job (verified).
        sentenceWordsCountRDD.count();

        return sentenceWordsCountRDD;
    }

    private String filterMinWord(String stringToken, double tokenCount) {
        return tokenCount < numWords ? this.configuration.getUNK() : stringToken;
    }

    private void addTokenToVocabCache(String stringToken, Float tokenCount) {
        // Making string token into actual token if not already an actual token (vocabWord)
        VocabWord actualToken;
        if (this.vocabCache.hasToken(stringToken)) {
            actualToken = this.vocabCache.tokenFor(stringToken);
            actualToken.increaseElementFrequency(tokenCount.intValue());
        } else {
            actualToken = new VocabWord(tokenCount, stringToken);
        }

        // Set the index of the actual token (vocabWord)
        // Put vocabWord into vocabs in InMemoryVocabCache
        boolean vocabContainsWord = this.vocabCache.containsWord(stringToken);
        if (!vocabContainsWord) {
            int idx = this.vocabCache.numWords();

            this.vocabCache.addToken(actualToken);
            actualToken.setIndex(idx);
            this.vocabCache.putVocabWord(stringToken);
        }
    }

    public void filterMinWordAddVocab(Counter<String> wordFreq) {

        if (wordFreq.isEmpty()) {
            throw new IllegalStateException(
                            "IllegalStateException: wordFreqCounter has nothing. Check accumulator updating");
        }

        for (Map.Entry<String, Double> entry : wordFreq.entrySet()) {
            String stringToken = entry.getKey();
            Float tokenCount = entry.getValue().floatValue();

            // Turn words below min count to UNK
            stringToken = this.filterMinWord(stringToken, tokenCount);
            if (!this.useUnk && stringToken.equals("UNK"));
            // Turn tokens to vocab and add to vocab cache
            else
            this.addTokenToVocabCache(stringToken, tokenCount);
        }
    }

    public void buildVocabCache() {

        // Tokenize
        JavaRDD<List<String>> tokenizedRDD = this.tokenize();

        // Update accumulator values and map to an RDD of sentence counts
        this.sentenceWordsCountRDD = this.updateAndReturnAccumulatorVal(tokenizedRDD).cache();

        // Get value from accumulator
        Counter<String> wordFreqCounter = this.wordFreqAcc.value();

        // Filter out low count words and add to vocab cache object and feed into LookupCache
        this.filterMinWordAddVocab(wordFreqCounter);

        // huffman tree should be built BEFORE vocab broadcast
        Huffman huffman = new Huffman(this.vocabCache.vocabWords());
        huffman.build();
        huffman.applyIndexes(this.vocabCache);

        // At this point the vocab cache is built. Broadcast vocab cache
        this.vocabCacheBroadcast = this.sc.broadcast(this.vocabCache);

    }

    public void buildVocabWordListRDD() {

        if (this.sentenceWordsCountRDD == null)
            throw new IllegalStateException("SentenceWordCountRDD must be defined first. Run buildLookupCache first.");

        this.vocabWordListRDD = this.sentenceWordsCountRDD.map(new WordsListToVocabWordsFunction(this.vocabCacheBroadcast))
                        .setName("vocabWordListRDD").cache();
        this.sentenceCountRDD =
                this.sentenceWordsCountRDD.map(new GetSentenceCountFunction()).setName("sentenceCountRDD").cache();
        // Actions to fill vocabWordListRDD and sentenceCountRDD
        this.vocabWordListRDD.count();
        this.totalWordCount = this.sentenceCountRDD.reduce(new ReduceSentenceCount()).get();

        // Release sentenceWordsCountRDD from cache
        this.sentenceWordsCountRDD.unpersist();
    }

    // Getters
    public Accumulator<Counter<String>> getWordFreqAcc() {
        if (this.wordFreqAcc != null) {
            return this.wordFreqAcc;
        } else {
            throw new IllegalStateException("IllegalStateException: wordFreqAcc not set at TextPipline.");
        }
    }

    public Broadcast<VocabCache<VocabWord>> getBroadCastVocabCache() throws IllegalStateException {
        if (this.vocabCache.numWords() > 0) {
            return this.vocabCacheBroadcast;
        } else {
            throw new IllegalStateException("IllegalStateException: VocabCache not set at TextPipline.");
        }
    }

    public VocabCache<VocabWord> getVocabCache() throws IllegalStateException {
        if (this.vocabCache != null && this.vocabCache.numWords() > 0) {
            return this.vocabCache;
        } else {
            throw new IllegalStateException("IllegalStateException: VocabCache not set at TextPipline.");
        }
    }

    public JavaRDD<Pair<List<String>, AtomicLong>> getSentenceWordsCountRDD() {
        if (this.sentenceWordsCountRDD != null) {
            return this.sentenceWordsCountRDD;
        } else {
            throw new IllegalStateException("IllegalStateException: sentenceWordsCountRDD not set at TextPipline.");
        }
    }

    public JavaRDD<List<VocabWord>> getVocabWordListRDD() throws IllegalStateException {
        if (this.vocabWordListRDD != null) {
            return this.vocabWordListRDD;
        } else {
            throw new IllegalStateException("IllegalStateException: vocabWordListRDD not set at TextPipline.");
        }
    }

    public JavaRDD<AtomicLong> getSentenceCountRDD() throws IllegalStateException {
        if (this.sentenceCountRDD != null) {
            return this.sentenceCountRDD;
        } else {
            throw new IllegalStateException("IllegalStateException: sentenceCountRDD not set at TextPipline.");
        }
    }

    public Long getTotalWordCount() {
        if (this.totalWordCount != 0L) {
            return this.totalWordCount;
        } else {
            throw new IllegalStateException("IllegalStateException: totalWordCount not set at TextPipline.");
        }
    }
}
