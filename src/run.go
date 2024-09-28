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
	"runtime"

	"github.com/mmcdole/gofeed"
	"github.com/spf13/viper"
)

type feedItem struct {
	URL        string `mapstructure:"url"`
	Tags       string `mapstructure:"tags"`
	LatestPost string `mapstructure:"latestpost"`
}

type runConfig struct {
	NumDlJobs int `mapstructure:"num_dl_jobs"`
	BatchSize int `mapstructure:"batch_size"`
}

type feeds struct {
	Feeds []feedItem
}

type urlTag struct {
	URL string `json:"url"`
	Tags string `json:"tags"`
}

type BagConfig struct {
	BaseUrl      string `mapstructure:"baseUrl"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
	UserName     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
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

func SendBatch(items []urlTag, AccessToken string, baseUrl string, client *http.Client) error {
	type PostURL struct {
		URL string `json:"url"`
	}
	urls := make([]PostURL, len(items))
	list := items
	for i := range urls {
		urls[i] = PostURL{list[i].URL}
	}

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
	if err := post(baseUrl + "api/entries/tags/lists", "list", list); err != nil {
		return fmt.Errorf("tagging failed: %v", err)
	}
	return nil
}

func ulWorker(accessToken string, baseURL string, batchSize int, client *http.Client, done chan<- struct{}, itemsCh <-chan []urlTag, printCh chan<- string, errorCh chan<- error) {
	defer func() {
		done<- struct{}{}
	}()

	batch := make([]urlTag, 0, batchSize)

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

func dlWorker(done chan<- struct{}, feedCh <-chan *feedItem, itemsCh chan<- []urlTag, printCh chan<- string, errorCh chan<- error) {
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
		tags := element.Tags

		// For each of the items in that feed
		// TODO: maybe use a pool
		items := make([]urlTag, 0, len(feed.Items))
		for _, element := range feed.Items {

			if element.Link == latest {
				break
			} else {
				item := urlTag{element.Link, tags}
				items = append(items, item)
			}
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
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.local/share/go-rss-to-wallabag")

	viper.SetDefault("num_dl_jobs", runtime.NumCPU() * 4)
	viper.SetDefault("batch_size", 10)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("💥  error reading config: %v", err))
	}


	var runcfg runConfig
	err = viper.Unmarshal(&runcfg)
	if err != nil {
		panic(fmt.Errorf("💥  error reading config: %v", err))
	}

	f := feeds{}
	err = viper.Unmarshal(&f)
	if err != nil {
		panic(fmt.Errorf("💥  error reading config: %v", err))
	}

	bagcfg := &BagConfig{}
	err = viper.Unmarshal(bagcfg)
	if err != nil {
		panic(fmt.Errorf("💥  error reading config: %v", err))
	}

	client := &http.Client{}
	fmt.Println("🚀  Authenticating with Walabag instance: ", bagcfg.BaseUrl)
	access_token, err := Auth(bagcfg, client)
	if err != nil {
		panic(fmt.Errorf("💥  authentication failed: %v", err))
	}

	printDone := make(chan struct{})
	printCh := make(chan string, 8192)
	errorCh := make(chan error, 8192)

	go printWorker(printDone, printCh, errorCh)

	dlDoneCh := make(chan struct{}, runcfg.NumDlJobs)
	ulDoneCh := make(chan struct{})
	feedCh := make(chan *feedItem, runcfg.NumDlJobs * 2)
	itemsCh := make(chan []urlTag, runcfg.NumDlJobs * 2)
	for _ = range(runcfg.NumDlJobs) {
		go dlWorker(dlDoneCh, feedCh, itemsCh, printCh, errorCh)
	}

	go ulWorker(access_token, bagcfg.BaseUrl, runcfg.BatchSize, client, ulDoneCh, itemsCh, printCh, errorCh)

	for i := range f.Feeds {
		element := &f.Feeds[i]
		feedCh<- element
	}
	close(feedCh)

	// Wait for dl workers to finish
	for _ = range runcfg.NumDlJobs {
		_ = <-dlDoneCh
	}
	close(itemsCh)
	_ = <-ulDoneCh

	close(printCh)
	close(errorCh)
	_ = <-printDone

	viper.Set("feeds", f.Feeds)
	err = viper.WriteConfig()
	if err != nil {
		panic(fmt.Errorf("💥  error updating config: %v", err))
	}
}
