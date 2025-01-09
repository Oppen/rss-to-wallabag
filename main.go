package main

import (
	"bytes"
	"context"
	_ "crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
)

// Environment
var (
	NumDlJobs    int = 10
	BatchSize    int = 10
	Verbose      bool = false

	LocalConnect bool = false
	BagUrl       string
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
)

// Program controlled globals
var (
	dnsResolverTimeoutMsecs int = 5000
	dnsResolverProto        string = "udp"
	dnsResolverIp           string = "1.1.1.1:53"

	printCh chan string
	errorCh chan error
)

func init() {
	if v, ok := os.LookupEnv("RSS2BAG_NUM_DL_JOBS"); ok {
		jobs, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			panic("invalid RSS2BAG_NUM_DL_JOBS")
		}
		NumDlJobs = int(jobs)
	}
	if v, ok := os.LookupEnv("RSS2BAG_BATCH_SIZE"); ok {
		batch, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			panic("invalid RSS2BAG_BATCH_SIZE")
		}
		BatchSize = int(batch)
	}
	_, LocalConnect = os.LookupEnv("RSS2BAG_LOCAL_CONNECT")

	var ok bool
	if BagUrl, ok = os.LookupEnv("RSS2BAG_BAG_URL"); !ok {
		panic("invalid RSS2BAG_BAG_URL")
	}
	if ClientID, ok = os.LookupEnv("RSS2BAG_CLIENT_ID"); !ok {
		panic("invalid RSS2BAG_CLIENT_ID")
	}
	if ClientSecret, ok = os.LookupEnv("RSS2BAG_CLIENT_SECRET"); !ok {
		panic("invalid RSS2BAG_CLIENT_SECRET")
	}
	if Username, ok = os.LookupEnv("RSS2BAG_USERNAME"); !ok {
		panic("invalid RSS2BAG_USERNAME")
	}
	if Password, ok = os.LookupEnv("RSS2BAG_PASSWORD"); !ok {
		panic("invalid RSS2BAG_PASSWORD")
	}

	printCh = make(chan string, 8192)
	errorCh = make(chan error, 8192)
}

/* TODO:
 * 1. Replace gofeed with stdlib xml, only extract links
 * 2. Maybe use explicit Marshal/Unmarshal implementations to build with tinygo
 *    This may help: https://github.com/json-iterator/tinygo
 *    Or: https://github.com/buger/jsonparser
 *    Or: https://github.com/mailru/easyjson
 *    Or: encoding/json itself, supposedly reflection works now
 * 3. Maybe try to make it work in bounded memory (and in that case, do without GC)
 * 4. Add support for extracting links from public Telegram channels (The Crab News)
 * 5. Make common variables globals, e.g. channels
 * 6. Unify print and orchestrator routines with select
 * 9. Execline script for orchestrating everything
 */

type Url struct {
	URL string `json:"url"`
}

type FeedItem struct {
	URL          string        `json:"url"`
	LatestPost   string        `json:"latest_post"`
	ETag        *string        `json:"etag,omitempty"`
	ETagRemove  *string        `json:"etag_remove,omitempty"`
	Modified    *string        `json:"last_modified,omitempty"`
	StripParams  bool          `json:"strip_params,omitempty"`
	Redirect    *string        `json:"redirect,omitempty"`
	TitleMatch  *string        `json:"title_matching,omitempty"`
	TitleRegex  *regexp.Regexp `json:"-"`
}

type Config struct {
	Feeds     []FeedItem `json:"feeds"`
}

func (cfg *Config) FillDefaults() {
	for i := range cfg.Feeds {
		feed := &cfg.Feeds[i]
		if feed.TitleMatch != nil {
			feed.TitleRegex = regexp.MustCompile(*feed.TitleMatch)
		}
	}
}

func Auth(client *http.Client) (string, error) {
	authURL := BagUrl + "oauth/v2/token"
	params := map[string]string{
		"grant_type": "password",
		"client_id": ClientID,
		"client_secret": ClientSecret,
		"username": Username,
		"password": Password,
	}
	jsonStr, _ := json.Marshal(params)
	req, err := http.NewRequest("POST", authURL, bytes.NewBuffer([]byte(jsonStr)))
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json, */*;q=0.5")

	if LocalConnect {
		req.RemoteAddr = "127.0.0.1:80"
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("response returned status code: %s", resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	auth := map[string]string{}
	json.Unmarshal(b, &auth)

	return auth["access_token"], nil
}

func SendBatch(urls []Url, AccessToken string, client *http.Client) error {
	post := func(Url string, arg string, payload interface{}) error {
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal URL: %v", err)
		}

		req, err := http.NewRequest("POST", Url, nil)
		if err != nil {
			return fmt.Errorf("request creation failed: %v", err)
		}
		if LocalConnect {
			req.RemoteAddr = "127.0.0.1:80"
		}
		values := req.URL.Query()
		values.Add("access_token", AccessToken)
		values.Add(arg, string(b))
		req.URL.RawQuery = values.Encode()

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("upload failed: %v", err)
		}
		defer func() {
			// NOTE: reading all and closing the response allows reusing the connection
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode != 200 {
			return fmt.Errorf("response returned status code: %s", resp.Status)
		}
		return nil
	}

	if err := post(BagUrl + "api/entries/lists", "urls", urls); err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	return nil
}

func ulWorker(accessToken string, client *http.Client, done chan<- struct{}, itemsCh <-chan []Url) {
	defer func() {
		done<- struct{}{}
	}()

	batch := make([]Url, 0, BatchSize)

	flush := func() {
		defer func() {
			batch = batch[:0]
		}()

		err := SendBatch(batch, accessToken, client)
		if err != nil {
			for _, item := range batch {
				errorCh<- fmt.Errorf("💥  %s: upload error: %v", item.URL, err)
			}
			return
		}
		for _, item := range batch {
			printCh<- fmt.Sprintf("🚀  uploaded %s", item.URL)
		}
	}

	for items := range itemsCh {
		for len(items) + len(batch) >= BatchSize {
			toAppend := BatchSize-len(batch)
			batch = append(batch, items[:toAppend]...)
			flush()
			items = items[toAppend:]
		}
		batch = append(batch, items...)
	}

	if len(batch) > 0 {
		flush()
	}
}

func dlWorker(done chan<- struct{}, feedCh <-chan *FeedItem, itemsCh chan<- []Url) {
	defer func() {
		done<- struct{}{}
	}()

	fp := gofeed.NewParser()
	dialer := &net.Dialer{
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer { Timeout: time.Duration(dnsResolverTimeoutMsecs) * time.Millisecond }
				return d.DialContext(ctx, dnsResolverProto, dnsResolverIp)
			},
		},
	}
	dialContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: dialContext}
	client := &http.Client{Transport: transport}
	domainRE := regexp.MustCompile("^http(?:s)?://[^/]+/")

	for element := range feedCh {
		printCh<- fmt.Sprintf("🍺  downloading %s", element.URL)
		req, err := http.NewRequest("GET", element.URL, nil)
		req.Header.Add("Accept", "application/xml, application/atom+xml, application/rss+xml, text/xml")
		req.Header.Add("Connection", "close")
		req.Close = true
		if element.ETag != nil {
			etag := *element.ETag
			if element.ETagRemove != nil {
				etag = strings.Replace(etag, *element.ETagRemove, "", 1)
			}
			req.Header.Add("If-None-Match", etag)
		} else if element.Modified != nil {
			req.Header.Add("If-Modified-Since", *element.Modified)
		}
		start := time.Now()
		res, err := client.Do(req)
		printCh<- fmt.Sprintf("%s: response after %v", element.URL, time.Since(start))
		if err != nil {
			errorCh<- fmt.Errorf("💥  %s: error downloading feed: %v", element.URL, err)
			if res != nil && res.Body != nil {
				io.Copy(ioutil.Discard, res.Body)
				res.Body.Close()
			}
			continue
		}
		if res.StatusCode == 304 {
			printCh<- fmt.Sprintf("😎  %s: already up to date", element.URL)
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
			continue
		} else if element.ETag != nil && *element.ETag == res.Header.Get("ETag") {
			errorCh<- fmt.Errorf("❌ %s: status: [%d] req-etag: [%s] res-etag: [%s]",
			element.URL, res.StatusCode, req.Header.Get("If-None-Match"), res.Header.Get("ETag"))
		}

		feed, err := fp.Parse(res.Body)
		if err != nil {
			errorCh<- fmt.Errorf("💥  %s: error parsing feed: %v", element.URL, err)
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
			continue
		}
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()

		printCh<- fmt.Sprintf("🚀  downloaded %s", element.URL)

		strip := element.StripParams
		latest := element.LatestPost
		filter := element.TitleRegex
		redirect := element.Redirect
		domain := domainRE.FindString(element.URL)

		// TODO: maybe use a pool
		items := make([]Url, 0, len(feed.Items))
		for _, element := range feed.Items {
			if element.Link == latest {
				break
			}
			if filter != nil && !filter.MatchString(element.Title) {
				continue
			}
			item := Url{element.Link}
			if strings.HasPrefix(item.URL, "/") {
				item.URL = domain + item.URL
			}
			if redirect != nil {
				item.URL = domainRE.ReplaceAllLiteralString(item.URL, *redirect)
			}
			if strip {
				stripped, _, _ := strings.Cut(item.URL, "?")
				item.URL = stripped
			}
			items = append(items, item)
		}
		if len(feed.Items) > 0 {
			element.LatestPost = feed.Items[0].Link
		}
		if etag := res.Header.Get("ETag"); etag != "" {
			element.ETag = &etag
		}
		if modified := string(res.Header.Get("Last-Modified")); modified != "" {
			element.Modified = &modified
		}
		itemsCh<- items
	}
}

func printWorker(doneCh chan<- struct{}) {
	defer func() {
		doneCh<- struct{}{}
	}()
	for {
		if printCh == nil && errorCh == nil {
			break
		}

		select {
		case line, ok := <-printCh:
			if !ok {
				printCh = nil
				continue
			}
			if Verbose {
				fmt.Fprintln(os.Stderr, line)
			}
		case line, ok := <-errorCh:
			if !ok {
				errorCh = nil
				continue
			}
			fmt.Fprintln(os.Stderr, line)
		}
	}
}

func main() {
	var cfg Config
	dec := json.NewDecoder(os.Stdin)
	if err := dec.Decode(&cfg); err != nil {
		panic(fmt.Errorf("💥  error reading feeds: %v", err))
	}
	cfg.FillDefaults()

	client := &http.Client{}
	fmt.Fprintln(os.Stderr, "🚀  Authenticating with Walabag instance: ", BagUrl)
	access_token, err := Auth(client)
	if err != nil {
		panic(fmt.Errorf("💥  authentication failed: %v", err))
	}

	printDone := make(chan struct{})

	go printWorker(printDone)

	dlDoneCh := make(chan struct{}, NumDlJobs)
	ulDoneCh := make(chan struct{})
	feedCh := make(chan *FeedItem, NumDlJobs * 2)
	itemsCh := make(chan []Url, NumDlJobs * 2)
	for _ = range(NumDlJobs) {
		go dlWorker(dlDoneCh, feedCh, itemsCh)
	}

	go ulWorker(access_token, client, ulDoneCh, itemsCh)

	for i := range cfg.Feeds {
		element := &cfg.Feeds[i]
		feedCh<- element
	}
	close(feedCh)

	// Wait for dl workers to finish
	for _ = range NumDlJobs {
		_ = <-dlDoneCh
	}
	close(itemsCh)
	_ = <-ulDoneCh

	close(printCh)
	close(errorCh)
	_ = <-printDone

	slices.SortFunc(cfg.Feeds, func(lhs, rhs FeedItem) int {
		return strings.Compare(lhs.URL, rhs.URL)
	})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(&cfg); err != nil {
		panic(fmt.Errorf("💥  error writing feeds: %v", err))
	}
}
