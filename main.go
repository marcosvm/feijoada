package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

func main() {
	var (
		client = http.DefaultClient
	// wg     sync.WaitGroup
	)

	dat, err := ioutil.ReadFile("./tickets.tar")
	if err != nil {
		log.Fatalln(err)
	}

	r := bytes.NewReader(dat)
	tr := tar.NewReader(r)

	for {

		var buf bytes.Buffer

		hdr, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}

		if err != nil {
			log.Fatalln(err)
		}

		log.Printf("Name: %s, Size: %d", hdr.Name, hdr.Size)
		if _, err := io.Copy(&buf, tr); err != nil {
			log.Fatalln(err)
		}

		url := fmt.Sprintf("http://localhost:8098/buckets/objects/keys/%s", hdr.Name)

		json := bytes.NewReader(buf.Bytes())
		req, err := http.NewRequest("PUT", url, json)

		if err != nil {
			log.Printf("failed to create PUT to %s because %v", url, err)
			return
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Content-Length", strconv.FormatInt(hdr.Size, 10))

		log.Printf("[copyTicket] Writing to: %q", url)

		resp, err := client.Do(req)

		if req.Body != nil {
			req.Body.Close()
		}

		if err != nil {
			log.Printf("[copyTicket][error] PUT to %q failed: %v", url, err)
		} else {
			_, _ = ioutil.ReadAll(resp.Body)
			if resp.Body != nil {
				resp.Body.Close()
			}
		}
	}
}
