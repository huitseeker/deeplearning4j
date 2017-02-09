/*
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

package org.deeplearning4j.datasets.loader;

/**
 * @author Adam Gibson
 */
// public class TwentyNewsGroupsLoader extends BaseDataFetcher {

//     private TextVectorizer textVectorizer;
//     private boolean tfidf;
//     public final static String NEWSGROUP_URL = "http://qwone.com/~jason/20Newsgroups/20news-18828.tar.gz";
//     private File twentyRootDir;
//     private static final Logger log = LoggerFactory.getLogger(TwentyNewsGroupsLoader.class);
//     private DataSet load;


//     public TwentyNewsGroupsLoader(boolean tfidf) throws Exception {
//         getIfNotExists();
//         LabelAwareSentenceIterator iter = new LabelAwareFileSentenceIterator(twentyRootDir);
//         List<String> labels =new ArrayList<>();
//         for(File f : twentyRootDir.listFiles()) {
//             if(f.isDirectory())
//                 labels.add(f.getName());
//         }
//         TokenizerFactory tokenizerFactory = new UimaTokenizerFactory();

//         if(tfidf)
//             this.textVectorizer = new TfidfVectorizer.Builder()
//                     .iterate(iter).labels(labels).tokenize(tokenizerFactory).build();

//         else
//             this.textVectorizer = new BagOfWordsVectorizer.Builder()
//                     .iterate(iter).labels(labels).tokenize(tokenizerFactory).build();

//         load = this.textVectorizer.vectorize();
//     }

//     private void getIfNotExists() throws Exception {
//         String home = System.getProperty("user.home");
//         String rootDir = home + File.separator + "twenty";
//         twentyRootDir = new File(rootDir);
//         if(!twentyRootDir.exists())
//             twentyRootDir.mkdir();
//         else if(twentyRootDir.exists())
//             return;


//         File rootTarFile = new File(twentyRootDir,"20news-18828.tar.gz");
//         if(rootTarFile.exists())
//             rootTarFile.delete();
//         rootTarFile.createNewFile();

//         FileUtils.copyURLToFile(new URL(NEWSGROUP_URL), rootTarFile);
//         ArchiveUtils.unzipFileTo(rootTarFile.getAbsolutePath(), twentyRootDir.getAbsolutePath());
//         rootTarFile.delete();
//         FileUtils.copyDirectory(new File(twentyRootDir,"20news-18828"),twentyRootDir);
//         FileUtils.deleteDirectory(new File(twentyRootDir,"20news-18828"));
//         if(twentyRootDir.listFiles() == null)
//             throw new IllegalStateException("No files found!");

//     }


//     /**
//      * Fetches the next dataset. You need to call this
//      * to getFromOrigin a new dataset, otherwise {@link #next()}
//      * just returns the last data applyTransformToDestination fetch
//      *
//      * @param numExamples the number of examples to fetch
//      */
//     @Override
//     public void fetch(int numExamples) {
//         List<DataSet> newData = new ArrayList<>();
//         for(int grabbed = 0; grabbed < numExamples && cursor < load.numExamples(); cursor++,grabbed++) {
//             newData.add(load.get(cursor));
//         }

//         this.curr = DataSet.merge(newData);

//     }
// }
