package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type Measurement struct {
	Min, Max, Sum float64
	Count         int64
}

type Measurements = map[string]*Measurement

func main() {
	f, err := os.Open("../1brc.data/measurements-1000000000.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	measurements := Measurements{}
	scanner := bufio.NewScanner(f)
	measurementDelim := []byte(";")
	uniqStationNames := []string{}

	for scanner.Scan() {
		lineBytes := scanner.Bytes()

		station, temp, found := bytes.Cut(lineBytes, measurementDelim)
		if !found {
			panic("Measurement delim not found!")
		}

		stationStr := string(station)
		tempStr := string(temp)
		tempF64, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		measurement, ok := measurements[stationStr]
		if !ok {
			uniqStationNames = append(uniqStationNames, stationStr)

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
	}

	sort.Strings(uniqStationNames)

	fmt.Printf("{")
	for i, v := range uniqStationNames {
		measurement := measurements[v]
		mean := measurement.Sum / float64(measurement.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f", v, measurement.Min, mean, measurement.Max)

		if i != len(uniqStationNames)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Printf("}")
}
