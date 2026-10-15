//go:build ignore

// Generate encoding probes from the pinned decoder's public reference fixtures.
package main

import (
	"bytes"
	"image"
	"image/color"
	"image/gif"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	out := os.Args[1]
	if err := os.MkdirAll(out, 0755); err != nil {
		panic(err)
	}
	for _, source := range []struct{ root, pattern string }{{filepath.Join(runtime.GOROOT(), "src/image/testdata"), "*.jpeg"}, {filepath.Join(runtime.GOROOT(), "src/image/testdata"), "*.png"}, {filepath.Join(runtime.GOROOT(), "src/image/testdata"), "*.gif"}, {os.Args[2], "*.webp"}} {
		paths, err := filepath.Glob(filepath.Join(source.root, source.pattern))
		if err != nil {
			panic(err)
		}
		for _, path := range paths {
			bytes, err := os.ReadFile(path)
			if err != nil {
				panic(err)
			}
			// Force decoding and re-encoding below the maximum pixel count.
			bytes = append(bytes, make([]byte, 4_000_000)...)
			if err := os.WriteFile(filepath.Join(out, filepath.Base(path)), bytes, 0644); err != nil {
				panic(err)
			}
		}
	}
	palette := color.Palette{color.RGBA{40, 80, 120, 255}, color.RGBA{0, 0, 0, 0}}
	frame := image.NewPaletted(image.Rect(31, 2, 1031, 102), palette)
	for i := range frame.Pix {
		frame.Pix[i] = uint8(i % 2)
	}
	var buf bytes.Buffer
	err := gif.EncodeAll(&buf, &gif.GIF{Image: []*image.Paletted{frame}, Delay: []int{0}, Config: image.Config{ColorModel: palette, Width: 2501, Height: 103}})
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(out, "offset-first-frame.gif"), buf.Bytes(), 0644); err != nil {
		panic(err)
	}

}
