package main

import (
	"net/http"
	"sort"
	"strings"

	"github.com/asaidimu/blobs/store"
)

// movieListResponse is the payload for GET /api/movies: the (optionally
// filtered) movies themselves, plus the full set of distinct genres present
// in the catalog so the frontend can populate a filter dropdown without a
// separate round trip.
type movieListResponse struct {
	Movies []Movie  `json:"movies"`
	Genres []string `json:"genres"`
}

// handleListMovies serves GET /api/movies?q=&genre=
//
// Filtering happens in application code rather than in the store: the blobs
// index only supports key-prefix pagination (ListOptions.KeyPrefix/After),
// it has no query language over Metadata.Custom. That's the right trade-off
// here — a movie catalog is small enough (hundreds to low thousands of
// entries) that listing everything and filtering in memory is simpler and
// fast enough, and it keeps the storage layer generic instead of teaching it
// about "genre" and "search".
func (a *app) handleListMovies(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	blobs, err := a.movies.List(ctx, store.ListOptions{})
	if err != nil {
		mapStoreError(w, err)
		return
	}

	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	genreFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("genre")))

	genreSet := map[string]struct{}{}
	movies := make([]Movie, 0, len(blobs))

	for i := range blobs {
		info := &blobs[i]
		hasPoster, err := a.posterExists(ctx, info.Key)
		if err != nil {
			mapStoreError(w, err)
			return
		}
		m := movieFromBlobInfo(info, hasPoster)

		if m.Genre != "" {
			genreSet[m.Genre] = struct{}{}
		}

		if genreFilter != "" && strings.ToLower(m.Genre) != genreFilter {
			continue
		}
		if query != "" {
			haystack := strings.ToLower(m.Title + " " + m.Genre + " " + m.Description)
			if !strings.Contains(haystack, query) {
				continue
			}
		}
		movies = append(movies, m)
	}

	sort.Slice(movies, func(i, j int) bool {
		return strings.ToLower(movies[i].Title) < strings.ToLower(movies[j].Title)
	})

	genres := make([]string, 0, len(genreSet))
	for g := range genreSet {
		genres = append(genres, g)
	}
	sort.Strings(genres)

	writeJSON(w, http.StatusOK, movieListResponse{Movies: movies, Genres: genres})
}

// handleGetMovie serves GET /api/movies/{key}.
func (a *app) handleGetMovie(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := r.PathValue("key")

	info, err := a.movies.Head(ctx, key)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	hasPoster, err := a.posterExists(ctx, key)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, movieFromBlobInfo(info, hasPoster))
}

// handleDeleteMovie serves DELETE /api/movies/{key}. It removes the video
// ref (idempotent) and best-effort removes the poster ref too — a missing
// poster is not an error condition here.
func (a *app) handleDeleteMovie(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := r.PathValue("key")

	if err := a.movies.Delete(ctx, key); err != nil {
		mapStoreError(w, err)
		return
	}
	if err := a.posters.Delete(ctx, key); err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
