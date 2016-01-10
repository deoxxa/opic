package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"fknsrs.biz/p/opic"
)

var (
	filename   = flag.String("filename", "opic.db", "File to keep state in.")
	interval   = flag.Duration("interval", time.Hour*24, "Interval for OPIC algorithm.")
	initialise = flag.Float64("initialise", 1, "Initialise the OPIC state with this global cash.")
	importFile = flag.String("import", "", "CSV file with URLS to import.")
	stats      = flag.Bool("stats", false, "Show stats about the OPIC state.")
	read       = flag.Bool("read", false, "Read accurate data about the arguments.")
	estimate   = flag.Bool("estimate", false, "Estimate current cash of the arguments.")
	distribute = flag.String("distribute", "", "Distribute cash from the URL to the rest of the arguments.")
	inputTime  = flag.String("time", "", "Time to use for estimate and distribute.")
)

func main() {
	flag.Parse()

	t := time.Now()
	if *inputTime != "" {
		_t, err := time.Parse(time.RFC3339, *inputTime)
		if err != nil {
			panic(err)
		}
		t = _t
	}

	a := opic.NewPersistent(*filename)
	if err := a.Load(&opic.PersistentLoadOptions{IgnoreMissing: true}); err != nil {
		panic(err)
	}

	switch {
	case *read:
		for _, u := range flag.Args() {
			ah, ac, af := a.Get(u)
			fmt.Printf("%s\t%v\t%v\t%v\n", u, ah, ac, af)
		}
	case *estimate:
		for _, u := range flag.Args() {
			f := a.Estimate(u, *interval, t)
			fmt.Printf("%s\t%v\n", u, f)
		}
	case *distribute != "":
		a.Distribute(*distribute, flag.Args(), t)
	case *importFile != "":
		fmt.Printf("# importing from %s\n", *importFile)

		f, err := os.Open(*importFile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		var r *bufio.Reader

		var initialURLs []string

		r = bufio.NewReader(f)
		for {
			l, err := r.ReadString('\n')
			if err != nil && err != io.EOF {
				panic(err)
			}

			b := strings.SplitN(strings.TrimRight(l, " \r\n"), "\t", 2)

			initialURLs = append(initialURLs, b[0])

			if err == io.EOF {
				break
			}
		}

		fmt.Printf("# initialising to %v with %d urls\n", *initialise, len(initialURLs))

		a.Initialise(*initialise, initialURLs)
	}

	if a.Dirty() {
		fmt.Printf("# saving\n")

		if err := a.Save(); err != nil {
			panic(err)
		}
	}
}
