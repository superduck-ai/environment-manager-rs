//go:build ignore

// Generate reproducible image inputs for differential testing. Run directly, not as a package.
package main

import (
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math/rand"
	"os"
	"path/filepath"
)

func main() {
	dir := os.Args[1]
	os.MkdirAll(dir, 0755)
	r := rand.New(rand.NewSource(19))
	for _, c := range []struct {
		name         string
		w, h         int
		noise, alpha bool
	}{{"tiny", 17, 11, false, false}, {"alpha", 19, 13, false, true}, {"wide", 2501, 19, false, false}, {"large", 2101, 2203, true, false}, {"noisy", 1801, 1801, true, true}} {
		m := image.NewNRGBA(image.Rect(0, 0, c.w, c.h))
		for y := 0; y < c.h; y++ {
			for x := 0; x < c.w; x++ {
				a := uint8(255)
				if c.alpha {
					a = uint8(x * 17)
				}
				col := color.NRGBA{uint8(x), uint8(y), uint8(x + y), a}
				if c.noise {
					col.R = uint8(r.Intn(256))
					col.G = uint8(r.Intn(256))
					col.B = uint8(r.Intn(256))
				}
				m.SetNRGBA(x, y, col)
			}
		}
		for _, ext := range []string{"png", "jpg", "gif"} {
			if (c.name == "large" || c.name == "noisy") && ext == "gif" {
				continue
			}
			f, _ := os.Create(filepath.Join(dir, c.name+"."+ext))
			switch ext {
			case "png":
				png.Encode(f, m)
			case "jpg":
				jpeg.Encode(f, m, &jpeg.Options{Quality: 95})
			case "gif":
				gif.Encode(f, m, nil)
			}
			f.Close()
		}
	}
}
