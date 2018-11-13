package main

import (
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/modmuss50/CAV2"
	"log"
	"strconv"
	"time"
)

const (
	DB       = "project_download_stats_1"
	username = "test"
	password = "test"
)

func main() {
	cav2.SetupDefaultConfig()
	run()

}

func run() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://10.0.0.104:8086",
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  DB,
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
