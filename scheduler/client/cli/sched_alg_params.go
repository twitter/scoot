package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/wisechengyi/scoot/common/client"
)

// lbsSchedAlgParams load based scheduling params.  The setter API is associated with
// this structure.
type lbsSchedAlgParams struct {
	ClassLoadPercents        map[string]int32
	RequestorMap             map[string]string
	RebalanceMinimumDuration int
	RebalanceThreshold       int
}

// getLBSSchedAlgParams structure for getting the load based scheduling params.
type getLBSSchedAlgParams struct {
	printAsJSON bool
	lbsSchedAlgParams
}

func (g *getLBSSchedAlgParams) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "get_scheduling_alg_params",
		Short: "GetSchedAlgParams",
	}
	r.Flags().BoolVar(&g.printAsJSON, "json", false, "Print out scheduling algorithm parameters as JSON")
	return r
}

func (g *getLBSSchedAlgParams) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {
	log.Info("Getting Scheduling Algorithm Parameters", args)

	var err error
	g.ClassLoadPercents, err = cl.ScootClient.GetClassLoadPercents()
	if err != nil {
		return returnError(err)
	}

	g.RequestorMap, err = cl.ScootClient.GetRequestorToClassMap()
	if err != nil {
		return returnError(err)
	}

	var tInt int32
	tInt, err = cl.ScootClient.GetRebalanceMinimumDuration()
	if err != nil {
		return returnError(err)
	}
	g.RebalanceMinimumDuration = int(tInt)

	tInt, err = cl.ScootClient.GetRebalanceThreshold()
	if err != nil {
		return returnError(err)
	}
	g.RebalanceThreshold = int(tInt)

	if g.printAsJSON {
		asJSON, err := json.Marshal(g.lbsSchedAlgParams)
		if err != nil {
			log.Errorf("Error converting status to JSON: %v", err.Error())
			return fmt.Errorf("Error converting status to JSON: %v", err.Error())
		}
		log.Infof("%s\n", string(asJSON))
		fmt.Printf("%s\n", string(asJSON)) // must also go to stdout in case caller looking in stdout for the results
	} else {
		log.Info("Class Load Percents:")
		fmt.Println("Class Load Percents:")
		for class, pct := range g.ClassLoadPercents {
			log.Infof("%s:%d", class, pct)
			fmt.Println(class, ":", pct)
		}
		log.Info("Requestor (reg exp) to class map:")
		fmt.Println("Requestor (reg exp) to class map:")
		for requestorRe, class := range g.RequestorMap {
			log.Infof("%s:%s", requestorRe, class)
			fmt.Println(requestorRe, ":", class)
		}
		log.Infof("Rebalance Duration:%d (minutes)\n", g.RebalanceMinimumDuration)
		fmt.Println("Rebalance Duration:", g.RebalanceMinimumDuration, " (minutes)")
		log.Infof("Rebalance Threshold:%d\n", g.RebalanceThreshold)
		fmt.Println("Rebalance Threshold:", g.RebalanceThreshold)

	}

	return nil
}

type setLbsSchedAlgParams struct {
	lbsSchedAlgParams

	clpFilePath    string
	reqMapFilePath string
}

func (s *setLbsSchedAlgParams) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "set_scheduling_alg_params",
		Short: "SetSchedAlgParams",
	}
	r.Flags().StringVar(&s.clpFilePath, "class_pcts_file", "", "JSON file to read class load percents from.")
	r.Flags().StringVar(&s.reqMapFilePath, "requestor_map_file", "", "JSON file to read requestor to class map from.")
	r.Flags().IntVar(&s.RebalanceMinimumDuration, "rebalance_min", -1, "The number of minutes the tasks must be over rebalance threshold to trigger rebalance. 0 implies no rebalance. (Default of -1 implies no entry)")
	r.Flags().IntVar(&s.RebalanceThreshold, "rebalance_threshold", -1, "The rebalance threshold. 0 implies no rebalance. (Default of -1 implies no entry)")
	return r
}

func (s *setLbsSchedAlgParams) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {
	log.Info("Setting Scheduling Algorithm Parameters", args)

	if s.RebalanceMinimumDuration > -1 {
		err := cl.ScootClient.SetRebalanceMinimumDuration(int32(s.RebalanceMinimumDuration))
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}
	if s.RebalanceThreshold > -1 {
		err := cl.ScootClient.SetRebalanceThreshold(int32(s.RebalanceThreshold))
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}

	if s.clpFilePath != "" {
		content, err := s.readSettingsFile(s.clpFilePath)
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
		err = json.Unmarshal(content, &s.ClassLoadPercents)
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
		err = cl.ScootClient.SetClassLoadPercents(s.ClassLoadPercents)
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}

	if s.reqMapFilePath != "" {
		content, err := s.readSettingsFile(s.reqMapFilePath)
		if err != nil {
			return err
		}
		err = json.Unmarshal(content, &s.RequestorMap)
		if err != nil {
			return err
		}
		cl.ScootClient.SetRequestorToClassMap(s.RequestorMap)
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}
	return nil
}

func (s *setLbsSchedAlgParams) readSettingsFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	asBytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return asBytes, err
}
