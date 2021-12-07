package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
)

type CommandLineArgs map[string]interface{}

const (
	pathToFileKey   = "pathToFile"
	shardsAmountKey = "shardsAmount"
	shardNameKey    = "shardName"
)

func parseShardUtilCommandArgs() CommandLineArgs {
	var (
		pathToCsv    = flag.String("p", ".", "path to csv file sith queries")
		shardsAmount = flag.Int("n", 1, "amount of shards pages")
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
	for {
		record, e := reader.Read()
		if e != nil {
			break
		} else if strings.Contains(strings.Join(record, ""), fmt.Sprintf("|%s|", shard)) {
			queries = append(queries, record)
		}
	}
	return queries, nil
}

func ShardUtil() {
	args := parseShardUtilCommandArgs()
	queries, err := getShardQueriesFromCsv(args[pathToFileKey].(string), args[shardNameKey].(string))

	if err != nil {
		fmt.Fprintf(os.Stderr, "ShardUtil: %s ", err.Error())
		os.Exit(1)
	}
	for _, query := range queries {
		fmt.Printf("%s", strings.Join(query, "\t|"))
	}
}

func main() {
	ShardUtil()
}
