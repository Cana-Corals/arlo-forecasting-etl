"""
NLP analysis of Google / TripAdvisor reviews for Arlo Williamsburg.

Reads all CSV files in data/raw/ matching *reviews*.csv and any manual exports.
Outputs:
  outputs/reviews_nlp_summary.json   — machine-readable for the Streamlit app
  outputs/reviews_nlp_report.txt     — human-readable summary

Usage:
    python scripts/15_analyze_reviews_nlp.py

Manual review imports:
  - Google Business Profile: Business Profile Manager → Reviews → Export (CSV)
  - TripAdvisor: Management Center → Reviews → Download CSV
  Drop exported files in data/raw/ with "review" anywhere in the filename.
  Required columns (case-insensitive): text/review_text/comment, rating/stars, date/time
"""

import re
import sys
import json
import string
import collections
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

BASE_DIR    = Path(__file__).resolve().parents[1]
RAW_DIR     = BASE_DIR / "data" / "raw"
OUTPUTS_DIR = BASE_DIR / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# ── NLTK bootstrap ────────────────────────────────────────────────────────────
try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    from nltk.sentiment.vader import SentimentIntensityAnalyzer

    for pkg in ["stopwords", "wordnet", "vader_lexicon", "punkt"]:
        try:
            if pkg == "stopwords":
                nltk.data.find("corpora/stopwords")
            elif pkg == "wordnet":
                nltk.data.find("corpora/wordnet")
            elif pkg == "vader_lexicon":
                nltk.data.find("sentiment/vader_lexicon")
            elif pkg == "punkt":
                nltk.data.find("tokenizers/punkt")
        except LookupError:
            nltk.download(pkg, quiet=True)

    STOP_WORDS = set(stopwords.words("english"))
    lemmatizer = WordNetLemmatizer()
    vader      = SentimentIntensityAnalyzer()
    NLTK_OK    = True
except Exception as e:
    print(f"Warning: NLTK not available ({e}). Falling back to basic stopwords.")
    NLTK_OK = False
    STOP_WORDS = {
        "the","a","an","and","or","but","in","on","at","to","for","of","with",
        "is","was","are","were","be","been","being","have","has","had","do","does",
        "did","will","would","could","should","may","might","shall","can","that",
        "this","these","those","it","its","i","me","my","we","our","you","your",
        "he","she","they","their","there","here","from","by","as","not","so","very",
        "just","get","got","really","also","hotel","room","stay","arlo","williamsburg",
        "brooklyn","nyc","new","york","us","than","more","much","great","good","nice",
    }
    lemmatizer = None
    vader      = None

# Additional hotel-specific stopwords (too generic to be useful)
HOTEL_STOP = {
    "hotel","room","rooms","stay","stayed","place","night","nights","trip","visit",
    "arlo","williamsburg","brooklyn","staff","service","time","day","one","two",
    "get","got","us","would","could","also","really","great","good","nice","well",
    "went","came","made","back","even","though","way","much","many","bit","little",
    "just","know","think","feel","felt","looked","look","like","around","near",
    "still","away","area","far","long","short","went","come","back","want","need",
    "everything","something","nothing","anything","everyone","someone","anyone",
}
STOP_WORDS = STOP_WORDS | HOTEL_STOP

# ── Load reviews ──────────────────────────────────────────────────────────────
def find_review_files() -> list[Path]:
    patterns = ["*review*", "*Review*", "*tripadvisor*", "*TripAdvisor*", "*google*"]
    found = set()
    for pat in patterns:
        found.update(RAW_DIR.glob(f"{pat}.csv"))
        found.update(RAW_DIR.glob(f"{pat}.xlsx"))
    return sorted(found)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map varied export column names to standard: text, rating, date, source."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    renames = {}
    for col in df.columns:
        if col in ("review_text", "comment", "body", "content", "review_body"):
            renames[col] = "text"
        elif col in ("stars", "score", "overall", "overall_rating"):
            renames[col] = "rating"
        elif col in ("review_date", "published_at", "date_of_experience", "date", "publish_time"):
            renames[col] = "date"
        elif col in ("reviewer", "author_name", "user"):
            renames[col] = "author"
        elif col in ("platform", "channel"):
            renames[col] = "source"
    df = df.rename(columns=renames)
    if "text" not in df.columns:
        # Try to find any long-text column
        for col in df.columns:
            if df[col].dtype == object and df[col].str.len().mean() > 40:
                df = df.rename(columns={col: "text"})
                break
    return df


def load_all_reviews() -> pd.DataFrame:
    files = find_review_files()
    if not files:
        print("No review files found in data/raw/.")
        print("Run scripts/14_fetch_reviews.py first, or drop a CSV export there.")
        return pd.DataFrame()

    parts = []
    for f in files:
        try:
            raw = pd.read_csv(f) if f.suffix == ".csv" else pd.read_excel(f)
            raw = normalise_columns(raw)
            if "text" not in raw.columns:
                print(f"  Skipping {f.name}: no recognisable review-text column")
                continue
            if "source" not in raw.columns:
                raw["source"] = "Google" if "google" in f.name.lower() else "TripAdvisor"
            parts.append(raw)
            print(f"  Loaded {len(raw):>4} reviews from {f.name}")
        except Exception as e:
            print(f"  Failed to load {f.name}: {e}")

    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df["text"] = df["text"].fillna("").astype(str).str.strip()
    df = df[df["text"].str.len() > 10].reset_index(drop=True)
    return df


# ── Text preprocessing ────────────────────────────────────────────────────────
def clean_token(tok: str) -> str:
    tok = tok.lower().strip(string.punctuation)
    return tok if tok.isalpha() and len(tok) > 2 else ""


def preprocess(text: str) -> list[str]:
    tokens = re.findall(r"[a-zA-Z']+", text.lower())
    tokens = [clean_token(t) for t in tokens]
    tokens = [t for t in tokens if t and t not in STOP_WORDS]
    if NLTK_OK and lemmatizer:
        tokens = [lemmatizer.lemmatize(t) for t in tokens]
    return tokens


def sentiment_score(text: str) -> float:
    if NLTK_OK and vader:
        return vader.polarity_scores(text)["compound"]
    # Fallback: count positive vs negative words (very rough)
    pos = {"excellent","perfect","amazing","fantastic","wonderful","great","love","loved",
           "beautiful","clean","comfortable","friendly","recommend","best","enjoy","enjoyed"}
    neg = {"dirty","rude","awful","terrible","horrible","worst","disappoint","broken",
           "noisy","noise","smell","smelly","problem","issue","bad","poor","wrong","uncomfortable"}
    words = set(text.lower().split())
    score = (len(words & pos) - len(words & neg)) / max(len(words), 1)
    return float(np.clip(score * 5, -1, 1))


# ── Analysis ──────────────────────────────────────────────────────────────────
def word_frequency(docs: list[list[str]], top_n: int = 30) -> list[dict]:
    counter = collections.Counter(w for doc in docs for w in doc)
    return [{"word": w, "count": c} for w, c in counter.most_common(top_n)]


def tfidf_top_terms(texts: list[str], top_n: int = 20) -> list[dict]:
    if len(texts) < 3:
        return []
    vec   = TfidfVectorizer(max_features=500, ngram_range=(1, 2),
                            stop_words=list(STOP_WORDS), min_df=2)
    tfidf = vec.fit_transform(texts)
    scores = tfidf.mean(axis=0).A1
    terms  = vec.get_feature_names_out()
    ranked = sorted(zip(terms, scores), key=lambda x: -x[1])
    return [{"term": t, "score": round(float(s), 4)} for t, s in ranked[:top_n]]


def lda_topics(texts: list[str], n_topics: int = 5,
               n_words: int = 8) -> list[dict]:
    if len(texts) < n_topics * 2:
        return []
    vec = CountVectorizer(max_features=300, stop_words=list(STOP_WORDS),
                          min_df=2, ngram_range=(1, 2))
    dtm = vec.fit_transform(texts)
    lda = LatentDirichletAllocation(n_components=n_topics, random_state=42,
                                    max_iter=20, learning_method="online")
    lda.fit(dtm)
    terms = vec.get_feature_names_out()
    topics = []
    for i, comp in enumerate(lda.components_):
        top_idx = comp.argsort()[-n_words:][::-1]
        topics.append({
            "topic_id":  i + 1,
            "top_words": [terms[j] for j in top_idx],
            "weight":    round(float(comp[top_idx].sum() / lda.components_.sum()), 4),
        })
    return topics


def issue_phrases(texts: list[str], min_df: int = 2) -> list[dict]:
    """Find negative-context bigrams — likely complaint topics."""
    neg_triggers = re.compile(
        r"\b(no|not|never|without|missing|broken|dirty|bad|poor|terrible|awful|"
        r"disappoint|issue|problem|complaint|wrong|rude|loud|noise|small|tiny|"
        r"uncomfortable|slow|waited|wait|overpriced|expensive|smell|leak|"
        r"couldn'?t|didn'?t|wasn'?t|aren'?t|doesn'?t)\b",
        re.I,
    )
    neg_sentences = []
    for t in texts:
        for sent in re.split(r"[.!?]", t):
            if neg_triggers.search(sent):
                neg_sentences.append(sent.strip())

    if len(neg_sentences) < 2:
        return []

    vec   = CountVectorizer(ngram_range=(2, 3), stop_words=list(STOP_WORDS),
                            min_df=min_df, max_features=200)
    try:
        dtm = vec.fit_transform(neg_sentences)
    except ValueError:
        return []

    counts = dtm.sum(axis=0).A1
    terms  = vec.get_feature_names_out()
    ranked = sorted(zip(terms, counts), key=lambda x: -x[1])
    return [{"phrase": t, "mentions": int(c)} for t, c in ranked[:20]]


def rating_distribution(df: pd.DataFrame) -> dict:
    if "rating" not in df.columns:
        return {}
    ratings = pd.to_numeric(df["rating"], errors="coerce").dropna()
    dist = {str(int(r)): int((ratings == r).sum()) for r in sorted(ratings.unique())}
    return {
        "distribution": dist,
        "mean":  round(float(ratings.mean()), 2),
        "count": int(len(ratings)),
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Arlo Williamsburg — Review NLP Analysis")
    print("=" * 60)

    df = load_all_reviews()
    if df.empty:
        print("\nNo reviews to analyse. Exiting.")
        sys.exit(0)

    print(f"\nTotal reviews loaded: {len(df)}")

    texts  = df["text"].tolist()
    tokens = [preprocess(t) for t in texts]

    # Sentiment
    df["sentiment"] = df["text"].apply(sentiment_score)
    mean_sent  = float(df["sentiment"].mean())
    pos_pct    = float((df["sentiment"] > 0.05).mean() * 100)
    neg_pct    = float((df["sentiment"] < -0.05).mean() * 100)
    neu_pct    = 100 - pos_pct - neg_pct

    # Top words
    freq = word_frequency(tokens, top_n=30)

    # TF-IDF important phrases
    tfidf = tfidf_top_terms(texts, top_n=20)

    # LDA topics
    topics = lda_topics(texts, n_topics=5)

    # Issue / complaint phrases
    issues = issue_phrases(texts)

    # Rating distribution
    rating_dist = rating_distribution(df)

    # ── Print report ──────────────────────────────────────────────────────────
    print(f"\n{'─'*50}")
    print("SENTIMENT OVERVIEW")
    print(f"{'─'*50}")
    print(f"  Mean compound score : {mean_sent:+.3f}  (-1 very negative, +1 very positive)")
    print(f"  Positive reviews    : {pos_pct:.0f}%")
    print(f"  Neutral reviews     : {neu_pct:.0f}%")
    print(f"  Negative reviews    : {neg_pct:.0f}%")

    if rating_dist:
        print(f"\n  Star rating mean    : {rating_dist['mean']} / 5  (n={rating_dist['count']})")

    print(f"\n{'─'*50}")
    print("TOP 20 MOST FREQUENT WORDS")
    print(f"{'─'*50}")
    for i, item in enumerate(freq[:20], 1):
        bar = "█" * min(item["count"], 40)
        print(f"  {i:>2}. {item['word']:<20} {item['count']:>4}  {bar}")

    print(f"\n{'─'*50}")
    print("TOP TF-IDF PHRASES (most distinctive terms)")
    print(f"{'─'*50}")
    for item in tfidf[:15]:
        print(f"  {item['term']:<30} score: {item['score']:.4f}")

    print(f"\n{'─'*50}")
    print("LDA TOPICS (recurring themes)")
    print(f"{'─'*50}")
    for t in topics:
        print(f"  Topic {t['topic_id']}: {', '.join(t['top_words'])}")

    if issues:
        print(f"\n{'─'*50}")
        print("MOST FREQUENT COMPLAINT PHRASES")
        print(f"{'─'*50}")
        for item in issues[:15]:
            print(f"  {item['phrase']:<35} ×{item['mentions']}")

    # ── Save outputs ──────────────────────────────────────────────────────────
    summary = {
        "generated_at":    pd.Timestamp.now().isoformat(),
        "total_reviews":   len(df),
        "sources":         df["source"].value_counts().to_dict() if "source" in df.columns else {},
        "sentiment": {
            "mean":     mean_sent,
            "positive_pct": pos_pct,
            "neutral_pct":  neu_pct,
            "negative_pct": neg_pct,
        },
        "rating_distribution": rating_dist,
        "top_words":    freq,
        "tfidf_phrases": tfidf,
        "lda_topics":   topics,
        "complaint_phrases": issues,
    }

    json_path = OUTPUTS_DIR / "reviews_nlp_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSaved NLP summary → {json_path.relative_to(BASE_DIR)}")

    # Also save enriched reviews CSV (with sentiment) for model feature use
    df_out = df[["source", "rating", "text", "sentiment", "date"]
                if all(c in df.columns for c in ["source","rating","text","sentiment","date"])
                else [c for c in ["source","rating","text","sentiment","date"] if c in df.columns]]
    csv_path = OUTPUTS_DIR / "reviews_with_sentiment.csv"
    df_out.to_csv(csv_path, index=False)
    print(f"Saved enriched reviews → {csv_path.relative_to(BASE_DIR)}")

    print(f"\n{'='*60}")
    print("Done. Next step: run scripts/16_merge_review_features.py")
    print("to add monthly sentiment scores to the model feature set.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
