package main

import (
	"context"
	_ "crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/mmcdole/gofeed"
	"golang.org/x/net/html/charset"
)

// Environment
var (
	UserAgent string = "rss2bag/0.1.0"
	NumDlJobs int    = 10
	BatchSize int    = 10
	Verbose   bool   = false

	LocalConnect bool = false
	BagUrl       string
	AccessToken  string
)

// Program controlled globals
var (
	dnsResolverTimeoutMsecs int    = 5000
	dnsResolverProto        string = "udp"
	dnsResolverIp           string = "1.1.1.1:53"

	client *http.Client

	printCh chan string
	errorCh chan error

	feedCh     chan *FeedItem
	articlesCh chan Article
	linksCh    chan []Url

	dlWg    sync.WaitGroup
	ulWg    sync.WaitGroup
	printWg sync.WaitGroup
)

func init() {
	if v, ok := os.LookupEnv("RSS2BAG_USER_AGENT"); ok {
		UserAgent = v
	}
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
	if AccessToken, ok = os.LookupEnv("RSS2BAG_ACCESS_TOKEN"); !ok {
		panic("invalid RSS2BAG_ACCESS_TOKEN")
	}

	dialer := &net.Dialer{
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: time.Duration(dnsResolverTimeoutMsecs) * time.Millisecond}
				return d.DialContext(ctx, dnsResolverProto, dnsResolverIp)
			},
		},
	}
	dialContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: dialContext}
	client = &http.Client{Transport: transport}

	linksCh = make(chan []Url, NumDlJobs*2)
	articlesCh = make(chan Article, NumDlJobs*2)
	feedCh = make(chan *FeedItem, NumDlJobs*2)

	printCh = make(chan string, 8192)
	errorCh = make(chan error, 8192)
}

type Article struct {
	Url     string
	Title   string
	Content string
}
type Url string
type FilterFunc func(*gofeed.Feed) *gofeed.Feed
type FilterConfig struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}
type FeedItem struct {
	URL         string         `json:"url"`
	UserAgent   string         `json:"user_agent,omitempty"`
	LatestPost  string         `json:"latest_post,omitempty"`
	SendContent bool           `json:"send_content,omitempty"`
	ETag        string         `json:"etag,omitempty"`
	ETagRemove  string         `json:"etag_remove,omitempty"`
	Modified    string         `json:"last_modified,omitempty"`
	Filters     []FilterConfig `json:"filters,omitempty"`
	FiltersFns  []FilterFunc   `json:"-"`
}
type Config []FeedItem

func (cfg Config) FillDefaults() {
	for i := range cfg {
		feed := &cfg[i]
		if feed.UserAgent == "" {
			feed.UserAgent = UserAgent
		}
		domain := domainRE.FindString(feed.URL)
		feed.FiltersFns = append(feed.FiltersFns, buildFilter("qualify", json.RawMessage("\""+domain+"\"")))
		for _, filter := range feed.Filters {
			feed.FiltersFns = append(feed.FiltersFns, buildFilter(filter.Name, filter.Args))
		}
		feed.FiltersFns = append(feed.FiltersFns, buildFilter("latest", json.RawMessage("\""+feed.LatestPost+"\"")))
	}
}

var domainRE *regexp.Regexp = regexp.MustCompile("^http(?:s)?://[^/]+/")

func buildFilter(name string, args json.RawMessage) FilterFunc {
	type SkipFilterArgs struct {
		HasPrefix string `json:"has_prefix"`
	}
	type SplitFilterArgs struct {
		LinkSelector string `json:"link_selector"`
	}
	switch name {
	case "skip":
		skipArgs := &SkipFilterArgs{}
		if err := json.Unmarshal(args, skipArgs); err != nil {
			panic(fmt.Errorf("failed to unmarshal skip filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			j := 0
			for i := range feed.Items {
				if !strings.HasPrefix(feed.Items[i].Title, skipArgs.HasPrefix) {
					feed.Items[j] = feed.Items[i]
					j++
				}
			}
			feed.Items = feed.Items[:j]
			return feed
		}
	case "strip_params":
		return func(feed *gofeed.Feed) *gofeed.Feed {
			for i := range feed.Items {
				stripped, _, _ := strings.Cut(feed.Items[i].Link, "?")
				feed.Items[i].Link = stripped
			}
			return feed
		}
	case "remove_element":
		var selectors []string
		if err := json.Unmarshal(args, &selectors); err != nil {
			panic(fmt.Errorf("failed to unmarshal remove_element filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			for i := range feed.Items {
				doc, err := goquery.NewDocumentFromReader(strings.NewReader(feed.Items[i].Content))
				if err != nil {
					panic(fmt.Errorf("failed to parse content: %v", err))
				}
				for _, selector := range selectors {
					doc.Find(selector).Remove()
				}
				feed.Items[i].Content, _ = doc.Html()
			}
			return feed
		}
	case "split":
		splitArgs := &SplitFilterArgs{}
		if err := json.Unmarshal(args, splitArgs); err != nil {
			panic(fmt.Errorf("failed to unmarshal split filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			// TODO: maybe add title for later filters
			items := make([]*gofeed.Item, 0, len(feed.Items))
			for i := range feed.Items {
				doc, err := goquery.NewDocumentFromReader(strings.NewReader(feed.Items[i].Content))
				if err != nil {
					panic(fmt.Errorf("failed to parse content: %v", err))
				}
				doc.Find(splitArgs.LinkSelector).Each(func(_ int, s *goquery.Selection) {
					link, _ := s.Attr("href")
					items = append(items, &gofeed.Item{Link: link})
				})
			}
			feed.Items = items
			return feed
		}
	case "redirect":
		var redirectUrl string
		if err := json.Unmarshal(args, &redirectUrl); err != nil {
			panic(fmt.Errorf("failed to unmarshal redirect filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			for i := range feed.Items {
				feed.Items[i].Link = domainRE.ReplaceAllLiteralString(feed.Items[i].Link, redirectUrl)
			}
			return feed
		}
	case "qualify":
		var dommain string
		if err := json.Unmarshal(args, &dommain); err != nil {
			panic(fmt.Errorf("failed to unmarshal qualify filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			for i := range feed.Items {
				if strings.HasPrefix(feed.Items[i].Link, "/") {
					feed.Items[i].Link = dommain + feed.Items[i].Link
				}
			}
			return feed
		}
	case "latest":
		var latest string
		if err := json.Unmarshal(args, &latest); err != nil {
			panic(fmt.Errorf("failed to unmarshal latest filter args: %v", err))
		}
		return func(feed *gofeed.Feed) *gofeed.Feed {
			for i := range feed.Items {
				if feed.Items[i].Link == latest {
					feed.Items = feed.Items[:i]
					break
				}
			}
			return feed
		}
	}
	return nil
}

func Post(url string, args []string, payloads []any) error {
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("request creation failed: %v", err)
	}
	if LocalConnect {
		req.RemoteAddr = "127.0.0.1:80"
	}
	values := req.URL.Query()
	values.Add("access_token", AccessToken)

	for i := range args {
		b, err := json.Marshal(payloads[i])
		if err != nil {
			return fmt.Errorf("failed to marshal URL: %v", err)
		}

		values.Add(args[i], string(b))
	}
	req.URL.RawQuery = values.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	defer consumeResponse(resp)
	if resp.StatusCode != 200 {
		return fmt.Errorf("response returned status code: %s", resp.Status)
	}
	return nil
}

func articleUlWorker() {
	defer ulWg.Done()
	for items := range articlesCh {
		err := Post(BagUrl+"api/entries", []string{"url", "title", "content"}, []any{items.Url, items.Title, items.Content})
		if err != nil {
			errorCh <- fmt.Errorf("💥  %s: upload error: %v", items.Url, err)
			continue
		}
		printCh <- fmt.Sprintf("🚀  uploaded %s", items.Url)
	}
}

func linkUlWorker() {
	defer ulWg.Done()
	batch := make([]Url, 0, BatchSize)

	flush := func() {
		defer func() {
			batch = batch[:0]
		}()

		err := Post(BagUrl+"api/entries/lists", []string{"urls"}, []any{batch})
		if err != nil {
			for _, item := range batch {
				errorCh <- fmt.Errorf("💥  %s: upload error: %v", item, err)
			}
			return
		}
		for _, item := range batch {
			printCh <- fmt.Sprintf("🚀  uploaded %s", item)
		}
	}

	for items := range linksCh {
		batches := len(linksCh) + 1
		for i := range batches {
			for len(items)+len(batch) >= BatchSize {
				toAppend := BatchSize - len(batch)
				batch = append(batch, items[:toAppend]...)
				flush()
				items = items[toAppend:]
			}
			batch = append(batch, items...)
			if i < batches-1 {
				items = <-linksCh
			}
		}
		flush()
	}
}

func dlWorker() {
	defer dlWg.Done()
	fp := gofeed.NewParser()

	for element := range feedCh {
		printCh <- fmt.Sprintf("🍺  downloading %s", element.URL)
		req, err := http.NewRequest("GET", element.URL, nil)
		req.Header.Set("Connection", "close")
		//"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0"
		req.Header.Set("User-Agent", element.UserAgent)
		req.Close = true
		if element.ETag != "" {
			etag := element.ETag
			if element.ETagRemove != "" {
				etag = strings.Replace(etag, element.ETagRemove, "", 1)
			}
			req.Header.Add("If-None-Match", etag)
		} else if element.Modified != "" {
			req.Header.Add("If-Modified-Since", element.Modified)
		}
		start := time.Now()
		res, err := client.Do(req)
		feed, err := func() (*gofeed.Feed, error) {
			printCh <- fmt.Sprintf("%s: response after %v", element.URL, time.Since(start))
			if err != nil {
				return nil, fmt.Errorf("💥  %s: error downloading feed: %v", element.URL, err)
			}
			defer consumeResponse(res)
			if res.StatusCode == 304 {
				printCh <- fmt.Sprintf("😎  %s: already up to date", element.URL)
				return nil, nil
			} else if element.ETag != "" && element.ETag == res.Header.Get("ETag") {
				return nil, fmt.Errorf("💥  %s: status: [%d] req-etag: [%s] res-etag: [%s]",
					element.URL, res.StatusCode, req.Header.Get("If-None-Match"), res.Header.Get("ETag"))
			}

			if res.StatusCode/100 != 2 {
				return nil, fmt.Errorf("💥  %s: feed download returned: %s", element.URL, res.Status)
			}

			r, err := charset.NewReader(res.Body, res.Header.Get("Content-Type"))
			if err != nil {
				return nil, fmt.Errorf("💥  %s: error detecting feed encoding: %v", element.URL, err)
			}

			feed, err := fp.Parse(r)
			if err != nil {
				return nil, fmt.Errorf("💥  %s: error parsing feed: %v", element.URL, err)
			}
			return feed, nil
		}()

		if err != nil {
			errorCh <- err
			continue
		}
		if feed == nil {
			continue
		}

		printCh <- fmt.Sprintf("🚀  downloaded %s", element.URL)

		for _, filter := range element.FiltersFns {
			feed = filter(feed)
		}

		if element.SendContent {
			articlesCh <- Article{element.URL, feed.Title, feed.Description}
		} else {
			// TODO: maybe use a pool
			items := make([]Url, 0, len(feed.Items))
			for _, element := range feed.Items {
				items = append(items, Url(element.Link))
			}
			linksCh <- items
		}
		if len(feed.Items) > 0 {
			element.LatestPost = feed.Items[0].Link
		}
		if etag := res.Header.Get("ETag"); etag != "" {
			element.ETag = etag
		}
		if modified := string(res.Header.Get("Last-Modified")); modified != "" {
			element.Modified = modified
		}
	}
}

func printWorker() {
	defer printWg.Done()
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

	dlWg.Add(NumDlJobs)
	ulWg.Add(2)
	printWg.Add(1)

	go printWorker()
	for range NumDlJobs {
		go dlWorker()
	}
	go articleUlWorker()
	go linkUlWorker()

	for i := range cfg {
		element := &cfg[i]
		feedCh <- element
	}
	close(feedCh)

	dlWg.Wait()
	close(articlesCh)
	close(linksCh)
	ulWg.Wait()
	close(printCh)
	close(errorCh)
	printWg.Wait()

	slices.SortFunc(cfg, func(lhs, rhs FeedItem) int {
		return strings.Compare(lhs.URL, rhs.URL)
	})
	for i := range cfg {
		if cfg[i].UserAgent == UserAgent {
			cfg[i].UserAgent = ""
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	if err := enc.Encode(&cfg); err != nil {
		panic(fmt.Errorf("💥  error writing feeds: %v", err))
	}
}

func consumeResponse(res *http.Response) {
	if res == nil || res.Body == nil {
		return
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
}
