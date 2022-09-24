package aliyun.sls.chuzhi;


import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestDeletes {

    private final static String softDeletesField  = "deleted";

    public static void main(String[] args) throws Exception{
        Directory dir = new ByteBuffersDirectory();

        {
            IndexWriter indexWriter = new IndexWriter(dir, getWriterConfig(createKeepAllCommitsPolicy()));
            Document doc;

            // 文档0
            doc = new Document();
            doc.add(new StringField("docName", "document0", Field.Store.YES));
            doc.add(new StringField("value", "v0", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv0")));
            indexWriter.addDocument(doc);

            // 文档1 - 1
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv1")));
            indexWriter.addDocument(doc);

            // 文档1 - 2 （已经软删除）
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1-soft-deleted", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("v1-soft-deleted")));
            doc.add(new NumericDocValuesField(softDeletesField, 1));
            indexWriter.addDocument(doc);

            // 文档2
            doc = new Document();
            doc.add(new StringField("docName", "document2", Field.Store.YES));
            doc.add(new StringField("value", "v2", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv2")));
            indexWriter.addDocument(doc);

            // 文档3 (已经软删除)
            doc = new Document();
            doc.add(new StringField("docName", "document3", Field.Store.YES));
            doc.add(new StringField("value", "v3", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv3")));
            doc.add(new NumericDocValuesField(softDeletesField, 110));
            indexWriter.addDocument(doc);

            // 硬删除
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1-new", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv1-new")));
            indexWriter.updateDocument(new Term("docName", "document1"), doc);

            // 软删除
            doc = new Document();
            doc.add(new StringField("docName", "document2", Field.Store.YES));
            doc.add(new StringField("value", "v2-new", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv2-new")));
            indexWriter.softUpdateDocument(new Term("docName", "document2"), doc, new NumericDocValuesField(softDeletesField, 10));

            // 更新DV
            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new"));

            indexWriter.flush();
            // 生成一个段
            indexWriter.commit();

            {
                DirectoryReader reader = DirectoryReader.open(indexWriter);

                search(reader, "case #1");
                System.out.println("------------------");

                LeafReader leafReader = reader.leaves().get(0).reader();
                BinaryDocValues binaryDVs = leafReader.getBinaryDocValues("sqlCol");
                while (binaryDVs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("docId:" + binaryDVs.docID() + ", value: " + binaryDVs.binaryValue().utf8ToString());
                }
                System.out.println("------------------");

                NumericDocValues numericDVs = leafReader.getNumericDocValues(softDeletesField);
                while (numericDVs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("docId:" + numericDVs.docID() + ", value: " + numericDVs.longValue());
                }
                System.out.println("------------------");

                reader.close();
            }

            indexWriter.close();
        }

        {
            // 这里可以查询到软删除的结果
            DirectoryReader reader = DirectoryReader.open(dir);
            search(reader, "case #1.1");
            System.out.println("------------------");
            reader.close();
        }

        {
            IndexWriter indexWriter = new IndexWriter(dir, getWriterConfig(createKeepAllCommitsPolicy()));

            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new2"));
            indexWriter.flush();

            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new3"));
            indexWriter.flush();

            indexWriter.commit();
            indexWriter.close();
        }

        System.out.println("all files #1: " + Arrays.asList(dir.listAll()));
        System.out.println("------------------");

        {
            IndexWriter indexWriter = new IndexWriter(dir, getWriterConfig(createDeleteAllCommitsPolicy()));

            /**
             * 虽然所有的commits此时已经被删除了，但被打开的commit（即latest commit）对应的
             * {@link SegmentCommitInfo} 还是有效的 （{@link IndexFileDeleter}）会确保
             * 它引用的文件不被删除。
             */
            System.out.println("all files #2.1: " + Arrays.asList(dir.listAll()));

            // 文档2
            Document doc = new Document();
            doc.add(new StringField("docName", "document10", Field.Store.YES));
            doc.add(new StringField("value", "10", Field.Store.YES));
            indexWriter.addDocument(doc);

            {
                // 这里仍然可见之前commit的数据
                DirectoryReader reader = DirectoryReader.open(indexWriter);
                search(reader, "case #2");
                reader.close();
            }

            System.out.println("all files #2.2: " + Arrays.asList(dir.listAll()));

            /**
             * close的时候默认会进行commit操作，但是当采用delete all commits policy时，生成的
             * segments_N文件会被立刻删除（参见{@link IndexFileDeleter#checkpoint}，此时参数
             * isCommit为true)。而当{@link IndexFileDeleter#close}执行时，没有被segments_N
             * 引用的文件都将被清楚。因此，indexWriter.close()结束后，index目录中将为空。
             */
            indexWriter.close();
            System.out.println("------------------");
        }

        System.out.println("all files #3: " + Arrays.asList(dir.listAll()));
        System.out.println("------------------");

        {
            IndexWriter indexWriter = new IndexWriter(dir, getWriterConfig(createDeleteAllCommitsPolicy()));

            System.out.println("all files #4: " + Arrays.asList(dir.listAll()));

            {
                DirectoryReader reader = DirectoryReader.open(indexWriter);
                search(reader, "case #4");
                reader.close();
            }

            indexWriter.close();
            System.out.println("------------------");
        }

        System.out.println("all files #5: " + Arrays.asList(dir.listAll()));
    }

    private static IndexWriterConfig getWriterConfig(IndexDeletionPolicy indexDeletionPolicy) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setUseCompoundFile(false);
        indexWriterConfig.setSoftDeletesField(softDeletesField);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        if (indexDeletionPolicy != null) {
            indexWriterConfig.setIndexDeletionPolicy(indexDeletionPolicy);
        }
        return indexWriterConfig;
    }

    private static IndexDeletionPolicy createKeepAllCommitsPolicy() {
        return new IndexDeletionPolicy() {
            @Override
            public void onInit(List<? extends IndexCommit> commits) throws IOException {}

            @Override
            public void onCommit(List<? extends IndexCommit> commits) throws IOException {}
        };
    }

    private static IndexDeletionPolicy createDeleteAllCommitsPolicy() {
        return new IndexDeletionPolicy() {
            @Override
            public void onInit(List<? extends IndexCommit> commits) throws IOException {
                commits.forEach(IndexCommit::delete);
            }

            @Override
            public void onCommit(List<? extends IndexCommit> commits) throws IOException {
                commits.forEach(IndexCommit::delete);
            }
        };
    }

    private static void search(DirectoryReader reader, String tip) throws IOException {
        System.out.println("start search: " + tip);
        ScoreDoc[] scoreDocs = (new IndexSearcher(reader)).search(new MatchAllDocsQuery(), 100).scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("docId: #" + scoreDoc.doc
                    + ", docName: " + reader.document(scoreDoc.doc).get("docName")
                    + ", value: " + reader.document(scoreDoc.doc).get("value"));
        }
    }
}
