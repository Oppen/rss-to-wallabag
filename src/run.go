package gobag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"

	"github.com/mmcdole/gofeed"
)

/* TODO:
 * 1. Replace gofeed with stdlib xml, only extract links
 * 2. Make single file
 * 3. Maybe use explicit Marshal/Unmarshal implementations to build with tinygo
 *    This may help: https://github.com/json-iterator/tinygo
 * 4. Maybe try to make it work in bounded memory (and in that case, do without GC)
 * 5. Add support for extracting links from public Telegram channels (The Crab News)
 */

type AtomLink struct {
	Url string `xml:"href,attr"`
}

type Feed struct {
	AtomLinks []AtomLink `xml:"entry>link"`
	RssLinks  []string   `xml:"channel>item>link"`
}

type Url struct {
	URL string `json:"url"`
}

type FeedItem struct {
	URL         string        `json:"url"`
	LatestPost  string        `json:"latest_post"`
	Redirect   *string        `json:"redirect,omitempty"`
	TitleMatch *string        `json:"title_matching,omitempty"`
	TitleRegex *regexp.Regexp `json:"-"`
}

type RunConfig struct {
	NumDlJobs int `json:"num_dl_jobs"`
	BatchSize int `json:"batch_size"`
}

type BagConfig struct {
	BaseUrl      string `json:"base_url"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	UserName     string `json:"username"`
	Password     string `json:"password"`
}

type Config struct {
	BagConfig BagConfig  `json:"server_config"`
	RunConfig RunConfig  `json:"runtime_config"`
	Feeds     []FeedItem `json:"feeds"`
}

func (cfg *Config) FillDefaults() {
	if cfg.RunConfig.NumDlJobs == 0 {
		cfg.RunConfig.NumDlJobs = runtime.NumCPU() * 4
	}
	if cfg.RunConfig.BatchSize == 0 {
		cfg.RunConfig.BatchSize = 10
	}
	for i := range cfg.Feeds {
		feed := &cfg.Feeds[i]
		if feed.TitleMatch != nil {
			feed.TitleRegex = regexp.MustCompile(*feed.TitleMatch)
		}
	}
}

func Auth(bagcfg *BagConfig, client *http.Client) (string, error) {
	type PostRequest struct {
		GrantType    string `json:"grant_type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		UserName     string `json:"username"`
		Password     string `json:"password"`
	}

	baseURL := bagcfg.BaseUrl + "oauth/v2/token"
	jsonStr := &PostRequest{
		"password",
		getCreds(bagcfg.ClientID),
		getCreds(bagcfg.ClientSecret),
		getCreds(bagcfg.UserName),
		getCreds(bagcfg.Password),
	}
	b, err := json.Marshal(jsonStr)

	req, err := http.NewRequest("POST", baseURL, bytes.NewBuffer(b))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json, */*;q=0.5")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("response returned status code: %s", resp.Status)
	}

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	type Response struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	auth := Response{}
	json.Unmarshal(b, &auth)

	return auth.AccessToken, nil
}

func getCreds(key string) (value string) {
	if key[0:2] == "$(" {
		cmd := key[2 : len(key)-1]
		secret, _ := exec.Command("bash", "-c", cmd).Output()
		return string(secret)
	}
	return string(key)
}

func SendBatch(urls []Url, AccessToken string, baseUrl string, client *http.Client) error {
	post := func(Url string, arg string, payload interface{}) error {
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal URL: %v", err)
		}
		bearer := "Bearer " + AccessToken

		req, err := http.NewRequest("POST", Url, nil)
		req.Header.Add("Authorization", bearer)
		values := req.URL.Query()
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

	if err := post(baseUrl + "api/entries/lists", "urls", urls); err != nil {
		return fmt.Errorf("upload failed: %v", err)
	}
	return nil
}

func ulWorker(accessToken string, baseURL string, batchSize int, client *http.Client, done chan<- struct{}, itemsCh <-chan []Url, printCh chan<- string, errorCh chan<- error) {
	defer func() {
		done<- struct{}{}
	}()

	batch := make([]Url, 0, batchSize)

	flush := func() {
		defer func() {
			batch = batch[:0]
		}()

		err := SendBatch(batch, accessToken, baseURL, client)
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
		for len(items) + len(batch) >= batchSize {
			toAppend := batchSize-len(batch)
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

func dlWorker(done chan<- struct{}, feedCh <-chan *FeedItem, itemsCh chan<- []Url, printCh chan<- string, errorCh chan<- error) {
	defer func() {
		done<- struct{}{}
	}()

	// Create a new RSS parser instance
	fp := gofeed.NewParser()

	for element := range feedCh {
		printCh<- fmt.Sprintf("🍺  downloading %s", element.URL)
		feed, err := fp.ParseURL(element.URL)
		if err != nil {
			errorCh<- fmt.Errorf("💥  %s: error parsing feed: %v", element.URL, err)
			continue
		}
		printCh<- fmt.Sprintf("🚀  downloaded %s", element.URL)

		latest := element.LatestPost
		filter := element.TitleRegex
		redirect := element.Redirect
		domainRE := regexp.MustCompile("^http(?:s)://[^/]+/")

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
			if redirect != nil {
				item.URL = domainRE.ReplaceAllLiteralString(item.URL, *redirect)
			}
			items = append(items, item)
		}
		if len(feed.Items) > 0 {
			element.LatestPost = feed.Items[0].Link
		}
		itemsCh<- items
	}
}

func printWorker(doneCh chan<- struct{}, printCh <-chan string, errorCh <-chan error) {
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
			fmt.Println(line)
		case line, ok := <-errorCh:
			if !ok {
				errorCh = nil
				continue
			}
			fmt.Fprintln(os.Stderr, line)
		}
	}
}

func Run() {
	cfg_path, ok := os.LookupEnv("RSS2BAG_CONFIG")
	if !ok {
		cfg_path = os.ExpandEnv("$HOME/.local/share/go-rss-to-wallabag/config.json")
	}

	cfg_bytes, err := os.ReadFile(cfg_path)
	if err != nil {
		panic(fmt.Errorf("💥  error reading config: %v", err))
	}

	var cfg Config
	if err := json.Unmarshal(cfg_bytes, &cfg); err != nil {
		panic(fmt.Errorf("💥  error parsing config: %v", err))
	}
	cfg.FillDefaults()

	client := &http.Client{}
	fmt.Println("🚀  Authenticating with Walabag instance: ", cfg.BagConfig.BaseUrl)
	access_token, err := Auth(&cfg.BagConfig, client)
	if err != nil {
		panic(fmt.Errorf("💥  authentication failed: %v", err))
	}

	printDone := make(chan struct{})
	printCh := make(chan string, 8192)
	errorCh := make(chan error, 8192)

	go printWorker(printDone, printCh, errorCh)

	dlDoneCh := make(chan struct{}, cfg.RunConfig.NumDlJobs)
	ulDoneCh := make(chan struct{})
	feedCh := make(chan *FeedItem, cfg.RunConfig.NumDlJobs * 2)
	itemsCh := make(chan []Url, cfg.RunConfig.NumDlJobs * 2)
	for _ = range(cfg.RunConfig.NumDlJobs) {
		go dlWorker(dlDoneCh, feedCh, itemsCh, printCh, errorCh)
	}

	go ulWorker(access_token, cfg.BagConfig.BaseUrl, cfg.RunConfig.BatchSize, client, ulDoneCh, itemsCh, printCh, errorCh)

	for i := range cfg.Feeds {
		element := &cfg.Feeds[i]
		feedCh<- element
	}
	close(feedCh)

	// Wait for dl workers to finish
	for _ = range cfg.RunConfig.NumDlJobs {
		_ = <-dlDoneCh
	}
	close(itemsCh)
	_ = <-ulDoneCh

	close(printCh)
	close(errorCh)
	_ = <-printDone

	cfg_bytes, err = json.MarshalIndent(&cfg, "", "    ")
	if err != nil {
		panic(fmt.Errorf("💥  error updating config: %v", err))
	}

	// Fsck safety
	if err := os.WriteFile(cfg_path, cfg_bytes, 0640); err != nil {
		panic(fmt.Errorf("💥  error updating config: %v", err))
	}
}
