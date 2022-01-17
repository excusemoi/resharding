package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
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
	bucketKey          = "bucket"
	shardsPageTemplate = "http://catalog-master-menu.wbx-ru.svc.k8s.dataline/newshard?name="
)

var (
	defaultDataKeys = []string{
		"subject",
		"brand",
		"ext",
		"kind",
		"f",
	}
	fullLink = "http://catalog-master-menu.wbx-ru.svc.k8s.dataline/menu/full"
)

type shardStruct struct {
	Shard string `json:"shard"`
}

func ShardUtil() {

	var (
		args          = parseShardUtilCommandArgs()
		queries, err  = getShardQueriesFromCsv(args[pathToFileKey].(string), args[shardNameKey].(string))
		pagesItems    = make(map[string]map[string]map[string]struct{})
		arrangedItems = make(map[string]map[string]map[string]map[string]struct{})
	)

	log, _ := os.Open(fmt.Sprintf("log%s.txt", args[shardNameKey].(string)))

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	allShards, err := getAllShards()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	//pagesItems, err = getAllFullSubjects(allShards)

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}

	//getting content from shards urls
	for k := range allShards {
		url := fmt.Sprintf("%s%s", shardsPageTemplate, //
			k)
		content, err := getPageData(url)

		if err != nil {
			fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
			os.Exit(1)
		}

		shardNameNumeric := k
		pagesItems[shardNameNumeric] =
			getDataFromQuery(content, []string{"subjects", "exts"}, ",")

	}

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
							//fmt.Println(subject)
							arrangedItems[queryString][shardNameNumeric][defaultDataKeys[0]][subject] = struct{}{}
							addedSubjects[subject] = struct{}{}
							delete(data[defaultDataKeys[0]], subject)
						}
					}
				}
			}
		}
		addedSubjects = make(map[string]struct{})
		changedQuery := changeQuery(query,
			arrangedItems[queryString],
			len(arrangedItems[queryString]) == 0 ||
				strings.Contains(query[8], "ext") ||
				len(data[defaultDataKeys[0]]) != 0)

		queryResult = append(queryResult, []string{queryString, changedQuery})
		log.WriteString(fmt.Sprintf("Query: %s\nResult: %s\n\n", queryString, changedQuery))
	}
	err = changeFileStrings(args[pathToFileKey].(string), queryResult)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}
}

func getAllFullSubjects(shards map[string]struct{}) (map[string]map[string]map[string]struct{}, error) {
	pagesItems := make(map[string]map[string]map[string]struct{})
	for shard := range shards {
		res, err := exec.Command("bash", "hand.sh", shard).Output()
		if err != nil {
			return nil, err
		}
		fs := fullSubjects{}
		err = json.Unmarshal(res, &fs)
		if err != nil {
			if len(res) <= 1 {
				continue
			}
			return nil, err
		}
		pagesItems[shard] = make(map[string]map[string]struct{})
		pagesItems[shard][defaultDataKeys[0]] = make(map[string]struct{})
		for _, s := range fs.FullSubjects {
			pagesItems[shard][defaultDataKeys[0]][fmt.Sprintf("%d", s)] = struct{}{}
		}
	}
	return pagesItems, nil
}

func getNumericShards(shardName string, amount int) map[string]struct{} {
	numericShards := make(map[string]struct{})
	for i := 1; i <= amount; i++ {
		numericShards[fmt.Sprintf("%s%d", shardName, i)] = struct{}{}
	}
	return numericShards
}

func getAllShards() (map[string]struct{}, error) {
	content, err := getPageData(fullLink)
	if err != nil {
		return nil, err
	}
	r := regexp.MustCompile(`"shard":"[^,]+"`)
	res := make(map[string]struct{})
	shards := r.FindAllString(content, -1)
	for _, str := range shards {
		s := shardStruct{}
		str = "{" + str + "}"
		_ = json.Unmarshal([]byte(str), &s)
		res[s.Shard] = struct{}{}
	}
	return res, nil
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
	err = ioutil.WriteFile(f, []byte(output), 0644)
	if err != nil {
		return err
	}
	return nil
}

func changeQuery(query []string, arrangedItems map[string]map[string]map[string]struct{}, elastic bool) string {
	changedQuery := "--one-by-one-join"
	lastKey := ""
	subjects := ""
	data := getDataFromQuery(query[8], defaultDataKeys, defaultDelimiter)
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
	} else {
		if elastic {
			query[5] = "elasticsearch"
			query[6] = "--query="
			req := strings.Replace(query[0], "(", "", -1)
			req = strings.Replace(req, ")", "", -1)
			req = strings.Join(strings.Split(req, " "), " ")
			query[6] += "\"" + req + "\" "
			query[6] += "--max-product=15000 "
			query[6] += `--filter="subjectId:(`
			for s := range data[defaultDataKeys[0]] {
				query[6] += s + " OR "
			}
			query[6] = query[6][:len(query[6])-4]
			query[6] += ")\""
		} else {
			query[5] = "catalog" //
			query[6] = changedQuery
		}
		query[7] = "presets/bucket_17" //bucket захардкожен
		if len(query[1]) != 0 {
			query[8] = "preset=" + query[1]
		} else {
			query[8] = "preset=x" //preset не автоматизирован
		}
		if len(data["kind"]) != 0 {
			for k := range data["kind"] {
				query[8] += k + ";"
			}
			query[8] = query[8][:len(query[8])-1]
		}
		res += "\n"
	}
	query[9] = "()"
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
		pathToCsv = flag.String("p", ".", "path to csv file with queries")
		shardName = flag.String("s", "", "name of shard")
		bucket    = flag.String("b", "17", "bucket number")
		args      = make(map[string]interface{})
	)
	flag.Parse()
	args[pathToFileKey] = *pathToCsv
	args[shardNameKey] = *shardName
	args[bucketKey] = *bucket
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

type fullSubjects struct {
	FullSubjects []int `json:"fullsubjects"`
}

func test() {
	s, _ := getAllShards()
	for k := range s {
		_, err := exec.Command("bash", "hand.sh", k).Output()
		if err != nil {
			fmt.Println(err)
		}

	}
}

func main() {
	ShardUtil()
}
