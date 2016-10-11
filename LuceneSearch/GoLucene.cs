using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Analysis.Standard;
using Version = Lucene.Net.Util.Version;
using Model;
using System.Reflection;

namespace LuceneSearch
{
    public class GoLucene
    {
        private static string[] getArtifactPropertiesArray()
        {
            Type type = typeof(Artifact);
            var objArray = type.GetProperties().ToArray();
            var fields = new List<string>();

            foreach (var o in objArray)
            {
                if (!o.PropertyType.ToString().Contains("System.Collections.Generic.List"))
                {
                    fields.Add(o.Name);
                }
            }

            string[] outFields = fields.ToArray();

            return outFields;
        }

        private static PropertyInfo[] getArtifactProperties()
        {
            Type type = typeof(Artifact);
            var objArray = type.GetProperties().ToArray();

            return objArray;
        }

        public static string _luceneDir = Path.Combine(HttpContext.Current.Request.PhysicalApplicationPath, "lucene_index");

        private static FSDirectory _directoryTemp;

        private static FSDirectory _directory {
            get {
                if (_directoryTemp == null)
                {
                    _directoryTemp = FSDirectory.Open(new DirectoryInfo(_luceneDir));
                }

                if (IndexWriter.IsLocked(_directoryTemp))
                {
                    IndexWriter.Unlock(_directoryTemp);
                }

                var lockFilePath = Path.Combine(_luceneDir, "write.lock");

                if (File.Exists(lockFilePath))
                {
                    File.Delete(lockFilePath);
                }

                return _directoryTemp;
            }
        }

        public static IEnumerable<Artifact> getAllIndexRecords()
        {
            if (!System.IO.Directory.EnumerateFiles(_luceneDir).Any())
            {
                return new List<Artifact>();
            }

            //search lucene searcher
            var searcher = new IndexSearcher(_directory, false);
            var reader = IndexReader.Open(_directory, false);
            var docs = new List<Document>();
            var term = reader.TermDocs();

            while (term.Next())
            {
                docs.Add(searcher.Doc(term.Doc));
            }

            reader.Dispose();
            searcher.Dispose();

            return _mapLuceneToDataList(docs);
        }

        public static IEnumerable<Artifact> Search(string input, string fieldName = "")
        {
            if (string.IsNullOrEmpty(input))
            {
                return new List<Artifact>();
            }

            var terms = input.Trim().Replace("-", " ").Split(' ').Where(x => !string.IsNullOrEmpty(x)).Select(x => x.Trim() + "*");
            input = string.Join(" ", terms);

            return _search(input, fieldName);

        }

        private static IEnumerable<Artifact> _search(string searchQuery, string searchField = "")
        {
            if (string.IsNullOrEmpty(searchQuery.Replace("*","").Replace("?","")))
            {
                return new List<Artifact>();
            }

            using (var searcher = new IndexSearcher(_directory,false))
            {
                var hits_limit = 1000;
                var analyzer = new StandardAnalyzer(Version.LUCENE_30);

                if (!string.IsNullOrEmpty(searchField))
                {
                    var parser = new QueryParser(Version.LUCENE_30, searchField, analyzer);
                    var query = parseQuery(searchQuery, parser);
                    var hits = searcher.Search(query, hits_limit).ScoreDocs;
                    var results = _mapLuceneToDataList(hits, searcher);
                    analyzer.Close();
                    searcher.Dispose();
                    return results;
                }

                else {
                    var parser = new MultiFieldQueryParser(Version.LUCENE_30, getArtifactPropertiesArray(), analyzer);
                    var query = parseQuery(searchQuery, parser);
                    var hits = searcher.Search(query, null, hits_limit, Sort.INDEXORDER).ScoreDocs;
                    var results = _mapLuceneToDataList(hits, searcher);
                    analyzer.Close();
                    searcher.Dispose();
                    return results;
                }
            }
        }

        private static Query parseQuery(string searchQuery, QueryParser parser)
        {
            Query query;
            try
            {
                query = parser.Parse(searchQuery.Trim());
            }
            catch (ParseException)
            {
                query = parser.Parse(QueryParser.Escape(searchQuery.Trim()));
            }
            return query;
        }

        private static IEnumerable<Artifact> _mapLuceneToDataList(IEnumerable<Document> hits)
        {
            return hits.Select(_mapLuceneDocumentToData).ToList();
        }

        private static Artifact _mapLuceneDocumentToData(Document doc)
        {
            return new Artifact
            {
                Id = Convert.ToInt32(doc.Get("Id")),
                name = doc.Get("name"),
                category = doc.Get("category"),
                culture = doc.Get("culture"),
                origin = doc.Get("origin"),
                description = doc.Get("description"),
                materials = doc.Get("materials"),
                height = Convert.ToInt32(doc.Get("height")),
                width = Convert.ToInt32(doc.Get("width")),
                circaDate = doc.Get("circaDate"),
                culturalNotes = doc.Get("culturalNotes"),
                seller = doc.Get("seller"),
                sellerCity = doc.Get("sellerCity"),
                sellerCountry = doc.Get("sellerCountry"),
                cost = Convert.ToDecimal(doc.Get("cost")),
                yearCollected = doc.Get("yearCollected"),
                estimatedValue = Convert.ToDecimal("estimatedValue"),
                currentLocation = doc.Get("currentLocation"),
                destination = doc.Get("destination")
            };
        }

        private static IEnumerable<Artifact> _mapLuceneToDataList(IEnumerable<ScoreDoc> hits, IndexSearcher searcher)
        {
            return hits.Select(hit => _mapLuceneDocumentToData(searcher.Doc(hit.Doc))).ToList();
        }

        // add/update/clear search index data 
        public static void AddUpdateLuceneIndex(Artifact artifact)
        {
            AddUpdateLuceneIndex(new List<Artifact> { artifact });
        }
        public static void AddUpdateLuceneIndex(IEnumerable<Artifact> artifact)
        {
            // init lucene
            var analyzer = new StandardAnalyzer(Version.LUCENE_30);
            using (var writer = new IndexWriter(_directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED))
            {
                // add data to lucene search index (replaces older entries if any)
                foreach (var a in artifact)
                {
                    _addToLuceneIndex(a, writer);
                }

                // close handles
                analyzer.Close();
                writer.Dispose();
            }
        }
        public static void ClearLuceneIndexRecord(int record_id)
        {
            // init lucene
            var analyzer = new StandardAnalyzer(Version.LUCENE_30);
            using (var writer = new IndexWriter(_directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED))
            {
                // remove older index entry
                var searchQuery = new TermQuery(new Term("Id", record_id.ToString()));
                writer.DeleteDocuments(searchQuery);

                // close handles
                analyzer.Close();
                writer.Dispose();
            }
        }
        public static bool ClearLuceneIndex()
        {
            try
            {
                var analyzer = new StandardAnalyzer(Version.LUCENE_30);
                using (var writer = new IndexWriter(_directory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
                {
                    // remove older index entries
                    writer.DeleteAll();

                    // close handles
                    analyzer.Close();
                    writer.Dispose();
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }
        public static void Optimize()
        {
            var analyzer = new StandardAnalyzer(Version.LUCENE_30);
            using (var writer = new IndexWriter(_directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED))
            {
                analyzer.Close();
                writer.Optimize();
                writer.Dispose();
            }
        }
        private static void _addToLuceneIndex(Artifact artifact, IndexWriter writer)
        {
            // remove older index entry
            var searchQuery = new TermQuery(new Term("Id", artifact.Id.ToString()));
            writer.DeleteDocuments(searchQuery);

            // add new index entry
            var doc = new Document();

            // add lucene fields mapped to db fields
            doc.Add(new Field("Id", artifact.Id.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));

            var fields = getArtifactProperties();

            foreach (var f in fields)
            {
                doc.Add(new Field(f.Name, f.GetValue(artifact).ToString(), Field.Store.YES, Field.Index.ANALYZED));
            }

            // add entry to index
            writer.AddDocument(doc);
        }
    }
}
