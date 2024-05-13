# One billion row challenge

## Original Challenge
- https://github.com/gunnarmorling/1brc

## Usage
```console
$ go build .
$ ./obrc-golang > a.out
$ diff -w a.out measurements-100000.out
```

You should see nothing from the diff result.

## PC Specs
- CPU: AMD Ryzen 9 7950X
- Ram: 64GB
- SSD: Samsung SSD 990 PRO 2TB

## Results
- 1B row -> ~7.252s
- 100k row -> ~0.005s
