package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

type CommandLineArgs map[string]interface{}
type MapOfItems map[string]map[string]struct{}

const (
	pathToFileKey      = "pathToFile"
	shardsAmountKey    = "shardsAmount"
	shardNameKey       = "shardName"
	filterKey          = "ext"
	defaultDelimiter   = ";"
	shardsPageTemplate = "http://catalog-master-menu.wbx-ru.svc.k8s.dataline/newshard?name="
)

var (
	defaultDataKeys = []string{
		"subject",
		"brand",
		"ext",
	}
)

func ShardUtil() {

	var (
		args          = parseShardUtilCommandArgs()
		queries, err  = getShardQueriesFromCsv(args[pathToFileKey].(string), args[shardNameKey].(string))
		shard         = args[shardNameKey].(string)
		pagesAmount   = args[shardsAmountKey].(int)
		pagesItems    = make(map[string]map[string]map[string]struct{})
		arrangedItems = make(map[string]map[string]map[string]struct{}) //ext и brand потребуют доп перераспределения
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	for i := 1; i <= pagesAmount; i++ {
		content, err := getPageData(
			fmt.Sprintf("%s%s%d", shardsPageTemplate,
				shard,
				i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
			os.Exit(1)
		}
		shardNameNumeric := fmt.Sprintf("%s%d", shard, i)
		pagesItems[shardNameNumeric] =
			getDataFromQuery(content, defaultDataKeys[0:1], defaultDelimiter)
		arrangedItems[shardNameNumeric] = make(map[string]map[string]struct{})
	}

	for _, query := range queries {
		data := getDataFromQuery(query[8], defaultDataKeys, defaultDelimiter)
		for shardNameNumeric, items := range pagesItems {
			for subject := range data[defaultDataKeys[0]] {
				fmt.Println(subject)
				if _, in := items[defaultDataKeys[0]][subject]; !in {
					if _, ok := arrangedItems[shardNameNumeric][defaultDataKeys[0]]; !ok {
						arrangedItems[shardNameNumeric][defaultDataKeys[0]] = make(map[string]struct{})
					}
					arrangedItems[shardNameNumeric][defaultDataKeys[0]][subject] = struct{}{}
				}
			}
			changeQuery(query, arrangedItems)
		}

	}

	//arrangedData, err = arrangeDataAcrossPages(shard, data, shardPages)
}

//"https://wbxcatalog-ru.wildberries.ru/electronic1/catalog?subject=520;593;790;1407;3152"
func changeQuery(query []string, arrangedItems map[string]map[string]map[string]struct{}) {
	changedQuery := "-one-by-one-join"
	query[5] = "catalog"
	for k, v := range arrangedItems {
		subjects := ""
		for k1, _ := range v[defaultDataKeys[0]] {
			subjects += k1 + ";"
		}
		fmt.Println(subjects)
		subjects = subjects[:len(subjects)-1]
		changedQuery += fmt.Sprintf(" \"https://wbxcatalog-ru.wildberries.ru/%s/catalog?subject=%s\" ", k, subjects)
	}
	query[6] = changedQuery
	fmt.Println(strings.Join(query, "|"))
}

func getPageData(page string) (string, error) {
	client := http.Client{
		Timeout: time.Second * 5,
	}
	resp, err := client.Get(page)
	if err != nil {
		return "", err
	} else if resp.StatusCode != 200 {
		return resp.Status, err
	}
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func parseShardUtilCommandArgs() CommandLineArgs {
	var (
		pathToCsv    = flag.String("p", ".", "path to csv file with queries")
		shardsAmount = flag.Int("n", 6, "amount of shards pages")
		shardName    = flag.String("s", "", "name of shard")
		args         = make(map[string]interface{})
	)
	flag.Parse()
	args[pathToFileKey] = *pathToCsv
	args[shardsAmountKey] = *shardsAmount
	args[shardNameKey] = *shardName
	return args
}

func getShardQueriesFromCsv(pathToFile, shard string) ([][]string, error) {
	var (
		file, err = os.Open(pathToFile)
		queries   = make([][]string, 0)
	)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '|'
	reader.FieldsPerRecord = 10
	reader.LazyQuotes = true

	for {
		record, e := reader.Read()
		if e != nil {
			break
		} else if strings.TrimSpace(record[7]) == shard {
			queries = append(queries, record)
		}
	}
	return queries, nil
}

func getDataFromQuery(query string, dataKeys []string, delimiter string) map[string]map[string]struct{} {

	regexpString := "("
	for i := range dataKeys {
		regexpString += dataKeys[i]
		if i != len(dataKeys)-1 {
			regexpString += "|"
		} else {
			regexpString += ")"
		}
	}

	data := make(map[string]map[string]struct{})
	r := regexp.MustCompile(regexpString)
	indices := r.FindAllStringIndex(query, -1)

	for i := range indices {
		rb := 0
		if i == len(indices)-1 {
			rb = len(query)
		} else {
			rb = indices[i+1][0] - 1
		}
		key := query[indices[i][0]:indices[i][1]]
		objects := strings.Split(query[indices[i][1]+1:rb], delimiter)
		if _, ok := data[key]; !ok {
			data[key] = make(map[string]struct{})
		}
		for _, obj := range objects {
			if obj == "" {
				continue
			}
			data[key][obj] = struct{}{}
		}
	}
	return data
}

func main() {
	ShardUtil()
}
