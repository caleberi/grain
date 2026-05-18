package main

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"math"
	"math/cmplx"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"grain/nexus"

	"github.com/caleberi/kloudinary"
)

// MandelbrotGeneratorPlugin generates beautiful Mandelbrot fractal images and animations
type MandelbrotGeneratorPlugin struct{}

type MandelbrotArgs struct {
	Filename string  `json:"filename"`
	Format   string  `json:"format"` // "png" or "gif"
	Width    int     `json:"width"`
	Height   int     `json:"height"`
	CenterX  float64 `json:"centerX"`
	CenterY  float64 `json:"centerY"`
	Zoom     float64 `json:"zoom"`
	MaxIter  int     `json:"maxIter"`
	Frames   int     `json:"frames"` // for GIF animation
}

func (p *MandelbrotGeneratorPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "example.MandelbrotGenerator",
		Description: "Generates beautiful Mandelbrot fractal images or animated GIFs",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"width": {"type": "integer", "default": 800},
				"height": {"type": "integer", "default": 600},
				"filename": {"type": "string"},
				"format": {"type": "string", "enum": ["png", "gif"], "default": "png"},
				"centerX": {"type": "number", "default": -0.5},
				"centerY": {"type": "number", "default": 0.0},
				"zoom": {"type": "number", "default": 1.0},
				"maxIter": {"type": "integer", "default": 100},
				"frames": {"type": "integer", "default": 30}
			},
			"required": ["filename"]
		}`),
	}
}

func (p *MandelbrotGeneratorPlugin) Execute(ctx context.Context, args MandelbrotArgs) (string, error) {
	if err := ensureOutputDir(); err != nil {
		return "", err
	}

	if args.Width <= 0 {
		args.Width = 800
	}
	if args.Height <= 0 {
		args.Height = 600
	}
	if args.MaxIter <= 0 {
		args.MaxIter = 100
	}
	if args.Zoom <= 0 {
		args.Zoom = 1.0
	}
	if args.Format == "" {
		args.Format = "png"
	}

	filename := filepath.Join(outputDir, filepath.Base(args.Filename))

	if args.Format == "gif" && args.Frames > 1 {
		return p.generateGIF(filename, args)
	}
	return p.generatePNG(filename, args)
}

func (p *MandelbrotGeneratorPlugin) generatePNG(filename string, args MandelbrotArgs) (string, error) {
	img := generateMandelbrotImage(args.Width, args.Height, args.CenterX, args.CenterY, args.Zoom, args.MaxIter)

	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if err := png.Encode(file, img); err != nil {
		return "", err
	}
	return fmt.Sprintf("mandelbrot image generated: %s", filename), nil
}

func (p *MandelbrotGeneratorPlugin) generateGIF(filename string, args MandelbrotArgs) (string, error) {
	anim := &gif.GIF{}

	for i := 0; i < args.Frames; i++ {
		zoom := args.Zoom * math.Pow(1.1, float64(i))
		img := generateMandelbrotImage(args.Width, args.Height, args.CenterX, args.CenterY, zoom, args.MaxIter)

		palettedImg := image.NewPaletted(img.Bounds(), generatePalette())
		for y := 0; y < args.Height; y++ {
			for x := 0; x < args.Width; x++ {
				palettedImg.Set(x, y, img.At(x, y))
			}
		}

		anim.Image = append(anim.Image, palettedImg)
		anim.Delay = append(anim.Delay, 5)
	}

	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if err := gif.EncodeAll(file, anim); err != nil {
		return "", err
	}
	return fmt.Sprintf("mandelbrot animation generated: %s (%d frames)", filename, args.Frames), nil
}

func generateMandelbrotImage(width, height int, cx, cy, zoom float64, maxIter int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	xmin, xmax := cx-2.0/zoom, cx+2.0/zoom
	ymin, ymax := cy-1.5/zoom, cy+1.5/zoom

	for py := 0; py < height; py++ {
		y := float64(py)/float64(height)*(ymax-ymin) + ymin
		for px := range width {
			x := float64(px)/float64(width)*(xmax-xmin) + xmin
			c := complex(x, y)
			iter := mandelbrotIterations(c, maxIter)
			img.Set(px, py, mandelbrotColor(iter, maxIter))
		}
	}
	return img
}

func mandelbrotIterations(c complex128, maxIter int) int {
	z := complex(0, 0)
	for i := range maxIter {
		z = z*z + c
		if cmplx.Abs(z) > 2 {
			return i
		}
	}
	return maxIter
}

func mandelbrotColor(iter, maxIter int) color.RGBA {
	if iter == maxIter {
		return color.RGBA{0, 0, 0, 255}
	}

	t := float64(iter) / float64(maxIter)
	r := uint8(9 * (1 - t) * t * t * t * 255)
	g := uint8(15 * (1 - t) * (1 - t) * t * t * 255)
	b := uint8(8.5 * (1 - t) * (1 - t) * (1 - t) * t * 255)

	return color.RGBA{r, g, b, 255}
}

func generatePalette() color.Palette {
	palette := make(color.Palette, 256)
	for i := range 256 {
		palette[i] = mandelbrotColor(i, 256)
	}
	return palette
}

func (a *MandelbrotArgs) unimplemented() {}

// FileMapPlugin processes files in parallel (map phase)
type FileMapPlugin struct{}

type FileMapArgs struct {
	Operation string   `json:"operation"` // "wordcount", "linecount", "charcount"
	Filenames []string `json:"filenames"`
}

func (p *FileMapPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "example.FileMapper",
		Description: "Maps over files and extracts statistics in parallel (map phase)",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"filenames": {"type": "array", "items": {"type": "string"}},
				"operation": {"type": "string", "enum": ["wordcount", "linecount", "charcount"]}
			},
			"required": ["filenames", "operation"]
		}`),
	}
}

func (p *FileMapPlugin) Execute(ctx context.Context, args FileMapArgs) (string, error) {
	type result struct {
		err      error
		filename string
		count    int
	}

	results := make(chan result, len(args.Filenames))
	var wg sync.WaitGroup

	for _, filename := range args.Filenames {
		wg.Add(1)
		go func(fn string) {
			defer wg.Done()
			path := filepath.Join(outputDir, filepath.Base(fn))
			data, err := os.ReadFile(path)
			if err != nil {
				results <- result{filename: fn, err: err}
				return
			}

			var count int
			switch args.Operation {
			case "wordcount":
				count = len(strings.Fields(string(data)))
			case "linecount":
				count = strings.Count(string(data), "\n") + 1
			case "charcount":
				count = len(data)
			default:
				results <- result{filename: fn, err: fmt.Errorf("unknown operation: %s", args.Operation)}
				return
			}
			results <- result{filename: fn, count: count}
		}(filename)
	}

	wg.Wait()
	close(results)

	var output strings.Builder
	fmt.Fprintf(&output, "Map results (%s):\n", args.Operation)
	totalCount := 0
	for r := range results {
		if r.err != nil {
			fmt.Fprintf(&output, "  %s: error - %v\n", r.filename, r.err)
		} else {
			fmt.Fprintf(&output, "  %s: %d\n", r.filename, r.count)
			totalCount += r.count
		}
	}
	fmt.Fprintf(&output, "Total: %d\n", totalCount)

	return output.String(), nil
}

func (a *FileMapArgs) unimplemented() {}

// FileReducePlugin aggregates results (reduce phase)
type FileReducePlugin struct{}

type FileReduceArgs struct {
	Operation string   `json:"operation"` // "sum", "avg", "max", "min"
	Filenames []string `json:"filenames"`
}

func (p *FileReducePlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "example.FileReducer",
		Description: "Reduces file statistics to aggregate values (reduce phase)",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"filenames": {"type": "array", "items": {"type": "string"}},
				"operation": {"type": "string", "enum": ["sum", "avg", "max", "min"]}
			},
			"required": ["filenames", "operation"]
		}`),
	}
}

func (p *FileReducePlugin) Execute(ctx context.Context, args FileReduceArgs) (string, error) {
	var sizes []int
	for _, filename := range args.Filenames {
		path := filepath.Join(outputDir, filepath.Base(filename))
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		sizes = append(sizes, len(data))
	}

	if len(sizes) == 0 {
		return "no files processed", nil
	}

	var result int
	switch args.Operation {
	case "sum":
		for _, s := range sizes {
			result += s
		}
	case "avg":
		sum := 0
		for _, s := range sizes {
			sum += s
		}
		result = sum / len(sizes)
	case "max":
		result = sizes[0]
		for _, s := range sizes {
			if s > result {
				result = s
			}
		}
	case "min":
		result = sizes[0]
		for _, s := range sizes {
			if s < result {
				result = s
			}
		}
	default:
		return "", fmt.Errorf("unknown operation: %s", args.Operation)
	}

	return fmt.Sprintf("Reduce result (%s): %d bytes across %d files", args.Operation, result, len(sizes)), nil
}

func (a *FileReduceArgs) unimplemented() {}

// FileUploadPlugin uploads files using kloudinary
type FileUploadPlugin struct{}

type FileUploadArgs struct {
	Filename  string `json:"filename"`
	APIKey    string `json:"apiKey"`
	APISecret string `json:"apiSecret"`
	CloudName string `json:"cloudName"`
}

func (p *FileUploadPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "example.FileUploader",
		Description: "Uploads generated files to Kloudinary cloud storage",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"filename": {"type": "string"},
				"apiKey": {"type": "string"},
				"apiSecret": {"type": "string"},
				"cloudName": {"type": "string"}
			},
			"required": ["filename"]
		}`),
	}
}

func (p *FileUploadPlugin) Execute(ctx context.Context, args FileUploadArgs) (string, error) {
	path := filepath.Join(outputDir, filepath.Base(args.Filename))

	info, err := os.Stat(path)

	if err != nil {
		return "", fmt.Errorf("file not found: %s", path)
	}
	fmt.Printf("Uploading file: %s (%d bytes)\n", path, info.Size())

	if args.CloudName == "" {
		args.CloudName = os.Getenv("CLOUDINARY_CLOUD_NAME")
	}
	if args.APIKey == "" {
		args.APIKey = os.Getenv("CLOUDINARY_API_KEY")
	}
	if args.APISecret == "" {
		args.APISecret = os.Getenv("CLOUDINARY_API_SECRET")
	}

	if args.CloudName == "" || args.APIKey == "" || args.APISecret == "" {
		return "", fmt.Errorf("missing Cloudinary credentials (set CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET or provide args)")
	}

	client, err := kloudinary.NewAssetUploadManager(args.CloudName, args.APIKey, args.APISecret)
	if err != nil {
		return "", fmt.Errorf("failed to create kloudinary client: %w", err)
	}

	uploadCtx := ctx
	if uploadCtx == nil {
		uploadCtx = context.Background()
	}
	var cancel context.CancelFunc
	uploadCtx, cancel = context.WithTimeout(uploadCtx, 30*time.Second)
	defer cancel()

	result, err := client.UploadSingleFile(uploadCtx, path)
	if err != nil {
		return "", fmt.Errorf("upload failed: %w", err)
	}
	if result == nil {
		return "", fmt.Errorf("upload failed: empty response")
	}
	url := result.SecureURL
	if url == "" {
		url = result.URL
	}
	return fmt.Sprintf("file uploaded: %s -> %s", args.Filename, url), nil
}

func (a *FileUploadArgs) unimplemented() {}
