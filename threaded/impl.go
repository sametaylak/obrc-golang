package threaded

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
)

const NUM_CHUNKS int64 = 32
const FILE_NAME string = "../1brc.data/measurements-1000000000.txt"

type Measurement struct {
	Min, Max, Sum float64
	Count         int64
}

type Measurements = map[string]*Measurement

func Threaded() {
	f, err := os.Open(FILE_NAME)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}

	fileSizeInBytes := stat.Size()
	chunkSizeInBytes := fileSizeInBytes / NUM_CHUNKS
	localMeasurementsChan := make(chan Measurements)

	var wg sync.WaitGroup
	var wg1 sync.WaitGroup
	var startPos int64 = 0
	for chunkId := range NUM_CHUNKS {
		cursorPos := chunkSizeInBytes * (chunkId + 1)

		buffer := make([]byte, 1024)
		nBytes, err := f.ReadAt(buffer, cursorPos)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		lastNewLineByteIndex := int64(bytes.LastIndexByte(buffer[:nBytes], 10))
		if lastNewLineByteIndex == -1 {
			panic("Could not find last new line index\n")
		}

		absoluteLastNewLineByteIndex := cursorPos + lastNewLineByteIndex

		wg.Add(1)
		go processChunk(startPos, absoluteLastNewLineByteIndex-1, localMeasurementsChan, &wg)

		startPos = absoluteLastNewLineByteIndex + 1
	}

	if startPos < fileSizeInBytes {
		wg.Add(1)
		go processChunk(startPos, fileSizeInBytes, localMeasurementsChan, &wg)
	}

	wg1.Add(1)

	uniqStationNames := []string{}
	globalMeasurements := Measurements{}
	go func() {
		for {
			localMeasurements, ok := <-localMeasurementsChan
			if !ok {
				break
			}

			for station, stationMeasurement := range localMeasurements {
				measurement, ok := globalMeasurements[station]
				if !ok {
					uniqStationNames = append(uniqStationNames, station)

					globalMeasurements[station] = &Measurement{
						Min:   stationMeasurement.Min,
						Max:   stationMeasurement.Max,
						Sum:   stationMeasurement.Sum,
						Count: stationMeasurement.Count,
					}
				} else {
					measurement.Count += stationMeasurement.Count
					measurement.Sum += stationMeasurement.Sum
					measurement.Min = min(measurement.Min, stationMeasurement.Min)
					measurement.Max = max(measurement.Max, stationMeasurement.Max)
				}
			}
		}

		wg1.Done()
	}()

	wg.Wait()
	close(localMeasurementsChan)

	wg1.Wait()

	sort.Strings(uniqStationNames)

	fmt.Printf("{")
	for i, v := range uniqStationNames {
		measurement := globalMeasurements[v]
		mean := measurement.Sum / float64(measurement.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f", v, measurement.Min, mean, measurement.Max)

		if i != len(uniqStationNames)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}")
}

func processChunk(start, end int64, localMeasurementsChan chan Measurements, wg *sync.WaitGroup) {
	f, err := os.Open(FILE_NAME)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Seek(start, 0)
	if err != nil {
		panic(err)
	}

	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)

	remainingBytes := end - start
	measurements := Measurements{}
	measurementDelim := []byte(";")
	for s.Scan() {
		if remainingBytes <= 0 {
			break
		}

		lineBytes := s.Bytes()

		station, temp, ok := bytes.Cut(lineBytes, measurementDelim)
		if !ok {
			panic("Delim not found")
		}

		stationStr := string(station)
		tempStr := string(temp)
		tempF64, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		measurement, ok := measurements[stationStr]
		if !ok {
			measurements[stationStr] = &Measurement{
				Min:   tempF64,
				Max:   tempF64,
				Sum:   tempF64,
				Count: 1,
			}
		} else {
			measurement.Count++
			measurement.Sum += tempF64
			measurement.Min = min(measurement.Min, tempF64)
			measurement.Max = max(measurement.Max, tempF64)
		}

		remainingBytes -= int64(len(lineBytes))
	}

	localMeasurementsChan <- measurements
	wg.Done()
}
