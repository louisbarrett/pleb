package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
)

var (
	flagESURL     = flag.String("endpoint", "http://localhost:9200", "ElasticSearch URL")
	flagStdio     = flag.Bool("stdio", false, "Output to stdio instead of sending data to ElasticSearch")
	flagIndexName = flag.String("index", "pleb-app", "ElasticSearch index name")
	flagFirstRun  = flag.Bool("first-run", false, "Initialize the index with a geo point")
	flagRegion    = flag.String("region", "bayarea", "pleb region - bayarea, dc, la, nyc, atl")
	searchArea    string
	createIndex   = esapi.IndicesCreateRequest{
		Index: *flagIndexName,
		Body:  strings.NewReader(""),
	}
	geopointMapping = esapi.IndicesPutMappingRequest{
		Index:          []string{*flagIndexName},
		AllowNoIndices: *&flagFirstRun,
		Body:           strings.NewReader(geoPoint),
	}
)

const (
	geoPoint string = `
	{
		  "properties": {
			"spot": {
			  "type": "geo_point"
			}
		  }
	}
	`
	areaDC  = "insideBoundingBox[0]=38.84380915675703&insideBoundingBox[1]=-77.1075024989351&insideBoundingBox[2]=38.936640513121716&insideBoundingBox[3]=-76.91557750106404&limit=1000"
	areaNY  = "insideBoundingBox[0]=40.03772166571517&insideBoundingBox[1]=-75.42215413703332&insideBoundingBox[2]=41.409212436475684&insideBoundingBox[3]=-72.50984586296659&limit=1000"
	areaLA  = "insideBoundingBox[0]=33.44141720498979&insideBoundingBox[1]=-119.55669421111577&insideBoundingBox[2]=34.69196829035221&insideBoundingBox[3]=-117.12730578888386&limit=1000"
	areaSF  = "insideBoundingBox[0]=36.76389518150444&insideBoundingBox[1]=-123.96480637490123&insideBoundingBox[2]=38.40703624878333&insideBoundingBox[3]=-120.51119362508587&limit=1000"
	areaATL = "insideBoundingBox[0]=33.658841591795195&insideBoundingBox[1]=-84.62313624103547&insideBoundingBox[2]=33.863613481175605&insideBoundingBox[3]=-84.22677325547699&limit=1000"
)

func main() {
	// parse flag values
	flag.Parse()
	cfg := elasticsearch.Config{
		Addresses: []string{
			*flagESURL,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Println("Error creating elasticsearch client", err)
	}
	if *flagFirstRun {
		fmt.Println(createIndex.Do(context.Background(), es))
		fmt.Println(geopointMapping.Do(context.Background(), es))
		return
	}

	switch *flagRegion {
	case "atl":
		searchArea = areaATL
	case "ny":
		searchArea = areaNY
	case "nyc":
		searchArea = areaNY
	case "la":
		searchArea = areaLA
	case "dc":
		searchArea = areaDC
	case "sf":
		searchArea = areaSF
	case "bayarea":
		searchArea = areaSF
	}

	// Query plebeian
	RecentCrime, err := http.Get("https://citizen.com/api/incident/search?" + searchArea)
	CitizenResponse, err := ioutil.ReadAll(RecentCrime.Body)
	ParsedJSON, err := gabs.ParseJSON(CitizenResponse)
	if err != nil {
		log.Fatal(err)
	}
	crimeBlotter, _ := ParsedJSON.Path("hits").Children()

	for i := range crimeBlotter {
		citizenData := crimeBlotter[i]

		// Convert unix ts to datetime
		createdAtInt := int64(citizenData.Path("created_at").Data().(float64) / 1000)
		createdAt := time.Unix(createdAtInt, 0)
		citizenData.SetP(createdAt, "createdAt")
		updatedAtInt := int64(citizenData.Path("updated_at").Data().(float64) / 1000)
		updatedAt := time.Unix(updatedAtInt, 0)
		citizenData.SetP(updatedAt, "updatedAt")

		geoLat, _ := citizenData.Path("_geoloc.lat").Children()
		geoLon, _ := citizenData.Path("_geoloc.lng").Children()

		geoPointString := "POINT (" + geoLon[0].String() + " " + geoLat[0].String() + ")"
		// write geo point data
		citizenData.SetP(geoPointString, "spot")
		if *flagStdio == false {

			// index data into elasticsearch
			req := esapi.IndexRequest{
				Index:      *flagIndexName,
				Body:       strings.NewReader(citizenData.String()),
				DocumentID: citizenData.Path("objectID").String(),
				Refresh:    "true",
			}

			res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()
		} else {
			fmt.Println(citizenData.StringIndent("    ", "    "))
		}
	}

}
