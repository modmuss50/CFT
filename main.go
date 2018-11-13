package main

import (
	"flag"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/modmuss50/CAV2"
	"log"
	"strconv"
	"time"
)

var (
	DB       *string
	url      *string
	username *string
	password *string
)

func main() {

	DB = flag.String("database", "curse_downloadstats", "Database name")
	url = flag.String("url", "http://127.0.0.1:8086", "Database URL")
	username = flag.String("username", "test", "Database username")
	password = flag.String("password", "test", "Database password")

	flag.Parse()

	cav2.SetupDefaultConfig()
	run()

}

func run() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     *url,
		Username: *username,
		Password: *password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  *DB,
		Precision: "s", //Write the data with a precision of seconds, this is prob overkill
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Loading addons")
	addons, err := cav2.GetAllAddons()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Building db query with " + strconv.Itoa(len(addons)) + " addons")

	for _, addon := range addons {
		writeAddon(addon, bp)
	}

	fmt.Println("Writing to db")

	if err := c.Write(bp); err != nil {
		log.Fatal(err)
	}

	fmt.Println("All done")

	c.Close()
}

func writeAddon(addon cav2.Addon, bp client.BatchPoints) {

	tags := map[string]string{
		"projectID": strconv.Itoa(addon.ID),
		"ownerName": addon.Authors[0].Name,
	}

	fields := map[string]interface{}{
		"downloads":        addon.DownloadCount,
		"popularity_score": addon.PopularityScore,
	}

	pt, err := client.NewPoint(
		"project_info",
		tags,
		fields,
		time.Now(),
	)
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)
}
