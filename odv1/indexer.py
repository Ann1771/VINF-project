from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import os
import math
import csv
import tiktoken
from collections import Counter, defaultdict

CUSTOM_STOPWORDS = {
    "episode", "episodes", "recap", "review", "reviews", "drama", "kdrama",
    "series", "season", "show", "actor", "actress", "cast", "scene", 
    "story", "plot", "recaps", "tv", "korean"
}

STOPWORDS = ENGLISH_STOP_WORDS.union(CUSTOM_STOPWORDS)

input="data-extracted/"
output="index/"
os.makedirs(output,exist_ok=True)

# from site https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken
def num_tokens_from_doc(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def iterate_documents(root):
  postings=defaultdict(list)
  df=Counter()
  doc_meta=[]
  total_tokens=0
  doc_id=0
  tiktokens =0

  for dirpath,_,filenames in os.walk(root):
    site =os.path.relpath(dirpath,root).split(os.sep)[0]
    for filename in filenames:
      if not filename.endswith(".txt"):
        continue
      path = os.path.join(dirpath,filename)

      with open(path,"r",encoding="utf-8") as f:
        text =f.read().lower()
        ntiktokens =num_tokens_from_doc(text,"o200k_base")
        tokens= [ t for t in text.split(' ') if t]
        if not tokens:
          continue
        
        # Compute term frequencies (TF)
        doc_tf = Counter(tokens)
        for term, tfi in doc_tf.items():
          tf_weight = 1 + math.log(tfi)
          postings[term].append((doc_id, tf_weight))  
          df[term] += 1

        # Save document metadata
        length=len(tokens)
        tiktokens+=ntiktokens
        total_tokens+=length
        doc_meta.append((doc_id,site,path,length))
        doc_id+=1
      
    N=len(doc_meta)
    sites_count = len(set(site for _, site, _, _ in doc_meta)) 
  return doc_meta, postings, df, N, total_tokens, sites_count, tiktokens


def compute_idfs(df,N):
  vocab={}
  for term, dfi in df.items():
    if dfi > 0:
      idf_ordinary = math.log(N / dfi) # first formula                
      idf_log = math.log(1 + N / dfi)   # second formula logarithmic                   
    else:
      idf_ordinary = 0.0
      idf_log = 0.0

    vocab[term] = (dfi, idf_ordinary, idf_log)
  return vocab


# saving 
def write_tsv(doc_meta,postings,vocab,N, total_tokens,sites_count, tiktokens):
  with open(os.path.join(output, "docs.tsv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["doc_id", "site", "path", "length"])
    for doc_id, site, path, length in doc_meta:
      w.writerow([doc_id, site, path, length])

  
  with open(os.path.join(output, "vocab.tsv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["term", "df", "idf_ordinary", "idf_log"])
    for term, (dfi, idf_ordinary, idf_log) in sorted(vocab.items()):
      w.writerow([ term, dfi, f"{idf_ordinary:.6f}", f"{idf_log:.6f}" ])

  with open(os.path.join(output, "postings.tsv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["term", "doc_freq", "postings"])
    for term, plist in sorted(postings.items()):
      postings_str = ", ".join(f"({doc}:{tf:.6f})" for doc, tf in plist)
      w.writerow([term, len(plist), postings_str])
  
  vocab_size = len(vocab)
  with open(os.path.join(output, "globals.tsv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["name", "value"])
    w.writerow(["Number of documents", N])
    w.writerow(["total_tokens(words)", total_tokens])
    w.writerow(["tiktokens", tiktokens])
    w.writerow(["vocab_size", vocab_size])
    w.writerow(["sites_count", sites_count])

if __name__=="__main__":
  doc_meta, postings, df, N, total_tokens, sites_count ,tiktokens= iterate_documents(input)
  vocab = compute_idfs(df, N)
  write_tsv(doc_meta, postings, vocab, N, total_tokens, sites_count,tiktokens)
  print(f"{N} docs, {len(vocab)} unique words.")
