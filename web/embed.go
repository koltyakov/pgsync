// Package web provides the embedded frontend assets for the pgsync web UI.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var webFS embed.FS

// GetWebFS returns the embedded web filesystem
func GetWebFS() (fs.FS, error) {
	return fs.Sub(webFS, "dist")
}
