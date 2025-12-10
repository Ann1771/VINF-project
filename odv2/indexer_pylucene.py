import lucene
import pandas as pd
import re

from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, TextField, StringField, Field, IntPoint, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig


lucene.initVM()

index_path = "lucene_index"
directory = FSDirectory.open(Paths.get(index_path))


analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)

writer = IndexWriter(directory, config)

df = pd.read_parquet("/app/output/joined_parquet/part-00000-1a437ee3-9c3c-4602-881b-afa61aa3ad2d-c000.snappy.parquet")

def extract_year(val):
    match = re.search(r"\b(\d{4})\b", str(val))
    if match:
        return match.group(1)
    return ""


def add_doc(row):
    doc = Document()

    text_fields = [
    "title",  "tags",
    "alt_name", "creator", "based_on",
    "writer", "director",
    "starring", "music", 
    "producer", "executive_producer",
    "location", "company", "genre",
    "wiki_title", "network",  "language", "country", "num_episodes"
    ]
    string_fields = [
        "num_seasons", "first_aired", "last_aired"
    ]

    stored_fields = [
        "runtime", "budget", "camera_setup", "url", "opening_theme", "ending_theme","description", "synopsis"
    ]

    for f in text_fields:
        value = str(row.get(f, "") or "")
        doc.add(TextField(f, value, Field.Store.YES))

    for f in stored_fields:
        value = str(row.get(f, "") or "")
        doc.add(StoredField(f, value))

    doc.add(StringField("num_seasons", str(row.get("num_seasons", "")), Field.Store.YES))

 
    year1 = extract_year(row.get("first_aired", ""))
    year2 = extract_year(row.get("last_aired", ""))

    doc.add(StringField("first_aired_year", year1, Field.Store.YES))
    doc.add(StringField("last_aired_year", year2, Field.Store.YES))

    doc.add(StoredField("first_aired_raw", str(row.get("first_aired", ""))))
    doc.add(StoredField("last_aired_raw", str(row.get("last_aired", ""))))


    writer.addDocument(doc)


total = len(df)
print("Indexing {} documents...".format(total))


for idx, row in df.iterrows():
    add_doc(row)
    if idx % 500 == 0:
        print("Processed {}/{}".format(idx, total))

writer.commit()
writer.close()

print("Lucene index successfully created in folder:", index_path)
