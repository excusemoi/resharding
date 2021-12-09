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
		"f",
	}
)

func ShardUtil() {

	var (
		args          = parseShardUtilCommandArgs()
		queries, err  = getShardQueriesFromCsv(args[pathToFileKey].(string), args[shardNameKey].(string))
		shard         = args[shardNameKey].(string)
		pagesAmount   = args[shardsAmountKey].(int)
		pagesItems    = make(map[string]map[string]map[string]struct{})
		arrangedItems = make(map[string]map[string]map[string]map[string]struct{}) //ext и brand потребуют доп перераспределения
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	of, err := os.Create("of.txt")
	defer of.Close()

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	for i := 1; i <= pagesAmount; i++ {
		url := fmt.Sprintf("%s%s%d", shardsPageTemplate,
			shard,
			i)
		content, err := getPageData(url)

		if err != nil {
			fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
			os.Exit(1)
		}
		shardNameNumeric := fmt.Sprintf("%s%d", shard, i)
		pagesItems[shardNameNumeric] =
			getDataFromQuery(content, []string{"subjects", "exts"}, ",")

	}

	os.Open("of.txt")

	addedSubjects := make(map[string]struct{})
	queryResult := make([][]string, 0)

	for _, query := range queries {
		queryString := strings.Join(query, "|")
		arrangedItems[queryString] = map[string]map[string]map[string]struct{}{}
		data := getDataFromQuery(query[8], defaultDataKeys, defaultDelimiter)
		for shardNameNumeric, items := range pagesItems {
			for subject := range data[defaultDataKeys[0]] {
				if _, in := items["subjects"][subject]; in {
					if _, added := arrangedItems[queryString][shardNameNumeric]; !added {
						arrangedItems[queryString][shardNameNumeric] = make(map[string]map[string]struct{})
					}
					if _, ok := arrangedItems[queryString][shardNameNumeric][defaultDataKeys[0]]; !ok {
						arrangedItems[queryString][shardNameNumeric][defaultDataKeys[0]] = make(map[string]struct{})
					}
					if _, in := arrangedItems[queryString][shardNameNumeric][defaultDataKeys[0]][subject]; !in {
						if _, added := addedSubjects[subject]; !added {
							arrangedItems[queryString][shardNameNumeric][defaultDataKeys[0]][subject] = struct{}{}
							addedSubjects[subject] = struct{}{}
							delete(data[defaultDataKeys[0]], subject)
						}
					}
				}
			}
		}
		addedSubjects = make(map[string]struct{})
		changedQuery := changeQuery(query, arrangedItems[queryString])

		if len(data[defaultDataKeys[0]]) == 0 && changedQuery != "Must be elastic" {
			queryResult = append(queryResult, []string{queryString, changedQuery})
		}
		of.WriteString(fmt.Sprintf("Query: %s\nResult: %s\n\n", queryString, changeQuery(query, arrangedItems[queryString])))
	}
	err = changeFileStrings(args[pathToFileKey].(string), queryResult)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}
}

func changeFileStrings(f string, s [][]string) error {
	var (
		input, err = ioutil.ReadFile(f)
	)

	if err != nil {
		return err
	}
	lines := strings.Split(string(input), "\n")
	j := 0
	for i, line := range lines {
		if strings.TrimSpace(line) == strings.TrimSpace(s[j][0]) {
			lines[i] = strings.TrimSpace(s[j][1])
			j++
			if j == len(s) {
				break
			}
		}
	}
	fmt.Println("Заменено", j)
	output := strings.Join(lines, "\n")
	err = ioutil.WriteFile("_"+f, []byte(output), 0644)
	if err != nil {
		return err
	}
	return nil
}

func changeQuery(query []string, arrangedItems map[string]map[string]map[string]struct{}) string {
	changedQuery := "--one-by-one-join"
	lastKey := ""
	subjects := ""
	for k, v := range arrangedItems {
		lastKey = k
		for k1 := range v[defaultDataKeys[0]] {
			subjects += k1 + ";"
		}
		if len(subjects) == 0 {
			continue
		} else if subjects[len(subjects)-1] == ';' {
			subjects = subjects[0 : len(subjects)-1]
		}
		changedQuery += fmt.Sprintf(" \"https://wbxcatalog-ru.wildberries.ru/%s/catalog?subject=%s\"", k, subjects)
		subjects = ""
	}
	res := ""
	if len(arrangedItems) == 1 {
		query[7] = lastKey
	} else if len(arrangedItems) == 0 || strings.Contains(query[8], "ext") {
		return "Must be elastic"
	} else {
		query[5] = "catalog" //
		query[6] = changedQuery
		query[7] = "preset/bucketX"
		if len(query[1]) != 0 {
			query[8] = "preset=" + query[1]
		} else {
			query[8] = "preset=x"
		}
		res += "\n"
	}
	res += strings.Join(query, "|")
	return res
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
