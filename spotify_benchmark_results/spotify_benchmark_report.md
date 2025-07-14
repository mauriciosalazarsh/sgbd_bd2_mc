# Benchmark Report: MyIndex vs PostgreSQL - Spotify Songs Dataset

## Dataset Information

- **Source**: Spotify Songs Dataset
- **Total Songs**: 18,454
- **Fields Used**: track_name, track_artist, lyrics, track_album_name
- **Test Sizes**: 1000, 2000, 4000, 8000, 12000, 16000, 18000
- **Query Categories**: Musical terms, emotions, genres, social contexts

## Performance Results

| N | MyIndex | PostgreSQL | Speedup |
|---|---------|------------|----------|
| 1,000 | 0.0205s | 0.0013s | 0.06x |
| 2,000 | 0.0426s | 0.0008s | 0.02x |
| 4,000 | 0.0855s | 0.0011s | 0.01x |
| 8,000 | 0.1750s | 0.0011s | 0.01x |
| 12,000 | 0.2680s | 0.0011s | 0.00x |
| 16,000 | 0.3730s | 0.0014s | 0.00x |
| 18,000 | 0.4286s | 0.0016s | 0.00x |

## Query Category Analysis

| Category | Avg Time (MyIndex) | Queries |
|----------|-------------------|----------|
| emotion | 0.2295s | 21 |
| movement | 0.1780s | 7 |
| time | 0.1962s | 7 |
| genre | 0.1754s | 7 |
| social | 0.1763s | 7 |
| abstract | 0.1771s | 7 |
| relationship | 0.2239s | 7 |
| meta | 0.1752s | 7 |

## Technical Implementation

### MyIndex (SPIMI)
- **Algorithm**: Single-Pass In-Memory Indexing
- **Text Fields**: track_name, track_artist, lyrics, track_album_name
- **Language**: Multilingual (English, Spanish, etc.)
- **TF-IDF**: Cosine similarity ranking

### PostgreSQL
- **Index Type**: GIN (Generalized Inverted Index)
- **Text Search**: tsvector/tsquery with English dictionary
- **Ranking Function**: ts_rank() with term frequency weighting
- **Additional Indexes**: Genre and artist for optimization

## Dataset Characteristics

- **Unique Genres**: 6
- **Languages**: 35
- **Avg Query Results**: Varies by term frequency
- **Text Content**: Mix of song titles, artist names, and lyrics

## Conclusions

1. **Performance**: Results show comparative performance patterns
2. **Scalability**: Both systems demonstrate different scaling characteristics
3. **Real Data**: Using actual Spotify data provides realistic benchmarking
4. **Query Variation**: Different query types show varied performance patterns
