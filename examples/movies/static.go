package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/index.html static/app.js static/style.css
var staticFS embed.FS

// staticHandler serves the embedded frontend directly out of the compiled
// binary — no separate assets directory needs to ship alongside it.
func staticHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err) // unreachable: the embed directive above guarantees "static" exists
	}
	return http.FileServer(http.FS(sub))
}
