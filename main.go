package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"mm_svyaz/pipeline"
	"os"
	"path/filepath"
	"sync"
)

// FileHash хранит путь к файлу и его MD5-хеш.
type FileHash struct {
	Path string
	Hash string
}

func main() {
	// Флаги командной строки
	dir := flag.String("dir", ".", "директория для сканирования")
	workers := flag.Int("workers", 10, "количество параллельных воркеров")
	flag.Parse()

	// Создание пайплайна
	p := pipeline.NewPipeline()

	// Узел-источник: рекурсивный обход директории
	scanNode := pipeline.NewNode(0, 1, func(ctx context.Context, in []<-chan interface{}, out []chan interface{}) {
		_ = filepath.Walk(*dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			select {
			case out[0] <- path:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})

	// Узел-воркер: параллельный расчёт MD5
	hashNode := pipeline.NewNode(1, 1, func(ctx context.Context, in []<-chan interface{}, out []chan interface{}) {
		var wg sync.WaitGroup
		sem := make(chan struct{}, *workers)

		for item := range in[0] {
			select {
			case <-ctx.Done():
				break
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(v interface{}) {
				defer wg.Done()
				defer func() { <-sem }()

				path := v.(string)
				file, err := os.Open(path)
				if err != nil {
					return
				}
				defer file.Close()

				h := md5.New()
				if _, err := io.Copy(h, file); err != nil {
					return
				}
				hash := hex.EncodeToString(h.Sum(nil))
				select {
				case out[0] <- FileHash{Path: path, Hash: hash}:
				case <-ctx.Done():
				}
			}(item)
		}
		wg.Wait()
	})

	// Узел-приёмник: вывод результатов
	sinkNode := pipeline.NewNode(1, 0, func(ctx context.Context, in []<-chan interface{}, out []chan interface{}) {
		for item := range in[0] {
			fh := item.(FileHash)
			fmt.Printf("%s  %s\n", fh.Hash, fh.Path)
		}
	})

	// Сборка пайплайна
	p.AddNode(scanNode)
	p.AddNode(hashNode)
	p.AddNode(sinkNode)
	p.Connect(scanNode, 0, hashNode, 0)
	p.Connect(hashNode, 0, sinkNode, 0)

	// Запуск и ожидание завершения
	p.Run()
	p.Wait()
}
