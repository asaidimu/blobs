package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// Catalog metadata is stored in each movie blob's Metadata.Custom map
// (store.PutOptions.Custom / staging.BeginOptions.Custom). These are the
// keys this application reads and writes. Keys must not start with "_bs_"
// (reserved by the blobs library itself) — none of these do.
const (
	customTitle       = "title"
	customGenre       = "genre"
	customYear        = "year"
	customDescription = "description"
)

// Movie is the JSON view of a catalog entry sent to the frontend.
type Movie struct {
	Key         string    `json:"key"`
	Title       string    `json:"title"`
	Genre       string    `json:"genre,omitempty"`
	Year        string    `json:"year,omitempty"`
	Description string    `json:"description,omitempty"`
	Size        int64     `json:"size"`
	ContentType string    `json:"contentType"`
	HasPoster   bool      `json:"hasPoster"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// movieFromBlobInfo builds the API view of a movie from its stored record.
// hasPoster is looked up separately since posters live in their own
// namespace (see (a *app) posterExists).
func movieFromBlobInfo(info *object.BlobInfo, hasPoster bool) Movie {
	custom := info.Metadata.Custom
	return Movie{
		Key:         info.Key,
		Title:       firstNonEmpty(custom[customTitle], info.Key),
		Genre:       custom[customGenre],
		Year:        custom[customYear],
		Description: custom[customDescription],
		Size:        info.Metadata.Size,
		ContentType: info.Metadata.ContentType,
		HasPoster:   hasPoster,
		CreatedAt:   info.Metadata.CreatedAt,
		UpdatedAt:   info.Metadata.UpdatedAt,
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// posterExists reports whether a poster blob exists for the given movie key,
// swallowing NotFoundError (that's the expected "no poster yet" case) and
// surfacing any other error to the caller.
func (a *app) posterExists(ctx context.Context, key string) (bool, error) {
	if _, err := a.posters.Head(ctx, key); err != nil {
		if index.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

var (
	slugInvalidRunRe = regexp.MustCompile(`[^a-z0-9]+`)
	slugTrimRe       = regexp.MustCompile(`^-+|-+$`)
)

// slugify turns a movie title into a URL- and filesystem-safe storage key,
// e.g. "The Matrix (1999)" -> "the-matrix-1999".
func slugify(title string) string {
	s := strings.ToLower(strings.TrimSpace(title))
	s = slugInvalidRunRe.ReplaceAllString(s, "-")
	s = slugTrimRe.ReplaceAllString(s, "")
	if s == "" {
		return "movie"
	}
	return s
}

// uniqueMovieKey returns a storage key derived from title that doesn't
// already exist in ns, appending "-2", "-3", etc. if the base slug is taken
// — the same collision-avoidance the blobs staging example applies to
// uploaded filenames, applied here to catalog titles instead.
func uniqueMovieKey(ctx context.Context, ns *store.NamespaceHandle, title string) (string, error) {
	base := slugify(title)
	if _, err := ns.Head(ctx, base); err != nil {
		if index.IsNotFound(err) {
			return base, nil
		}
		return "", err
	}
	for i := 2; i < 10000; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if _, err := ns.Head(ctx, candidate); err != nil {
			if index.IsNotFound(err) {
				return candidate, nil
			}
			return "", err
		}
	}
	return "", fmt.Errorf("could not find a unique movie key for %q", title)
}
