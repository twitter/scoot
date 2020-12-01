package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// lbsSchedAlgParams load based scheduling params.  The setter API is associated with
// this structure.
type lbsSchedAlgParams struct {
	ClassLoadPercents        map[string]int32
	RequestorMap             map[string]string
	RebalanceMinimumDuration int
	RebalanceThreshold       int

	clpFilePath    string
	reqMapFilePath string
}

// getLBSSchedAlgParams structure for getting the load based scheduling params.
type getLBSSchedAlgParams struct {
	printAsJSON bool
	params      lbsSchedAlgParams
}

func (g *getLBSSchedAlgParams) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "get_scheduling_alg_params",
		Short: "GetSchedAlgParams",
	}
	r.Flags().BoolVar(&g.printAsJSON, "json", false, "Print out scheduling algorithm parameters as JSON")
	return r
}

func (g *getLBSSchedAlgParams) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	log.Info("Getting Scheduling Algorithm Parameters", args)

	var err error
	g.params.ClassLoadPercents, err = cl.scootClient.GetClassLoadPercents()
	if err != nil {
		return returnError(err)
	}

	g.params.RequestorMap, err = cl.scootClient.GetRequestorToClassMap()
	if err != nil {
		return returnError(err)
	}

	var tInt int32
	tInt, err = cl.scootClient.GetRebalanceMinimumDuration()
	if err != nil {
		return returnError(err)
	}
	g.params.RebalanceMinimumDuration = int(tInt)

	tInt, err = cl.scootClient.GetRebalanceThreshold()
	if err != nil {
		return returnError(err)
	}
	g.params.RebalanceThreshold = int(tInt)

	if g.printAsJSON {
		asJSON, err := json.Marshal(g.params)
		if err != nil {
			log.Errorf("Error converting status to JSON: %v", err.Error())
			return fmt.Errorf("Error converting status to JSON: %v", err.Error())
		}
		log.Infof("%s\n", string(asJSON))
		fmt.Printf("%s\n", string(asJSON)) // must also go to stdout in case caller looking in stdout for the results
	} else {
		log.Info("Class Load Percents:")
		fmt.Println("Class Load Percents:")
		for class, pct := range g.params.ClassLoadPercents {
			log.Infof("%s:%d", class, pct)
			fmt.Println(class, ":", pct)
		}
		log.Info("Requestor (reg exp) to class map:")
		fmt.Println("Requestor (reg exp) to class map:")
		for requestorRe, class := range g.params.RequestorMap {
			log.Infof("%s:%s", requestorRe, class)
			fmt.Println(requestorRe, ":", class)
		}
		log.Infof("Rebalance Duration:%d (minutes)\n", g.params.RebalanceMinimumDuration)
		fmt.Println("Rebalance Duration:", g.params.RebalanceMinimumDuration, " (minutes)")
		log.Infof("Rebalance Threshold:%d\n", g.params.RebalanceThreshold)
		fmt.Println("Rebalance Threshold:", g.params.RebalanceThreshold)

	}

	return nil
}

func (s *lbsSchedAlgParams) registerFlags() *cobra.Command {
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

func (s *lbsSchedAlgParams) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	log.Info("Setting Scheduling Algorithm Parameters", args)

	if s.RebalanceMinimumDuration > -1 {
		err := cl.scootClient.SetRebalanceMinimumDuration(int32(s.RebalanceMinimumDuration))
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}
	if s.RebalanceThreshold > -1 {
		err := cl.scootClient.SetRebalanceThreshold(int32(s.RebalanceThreshold))
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
		err = cl.scootClient.SetClassLoadPercents(s.ClassLoadPercents)
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
		cl.scootClient.SetRequestorToClassMap(s.RequestorMap)
		if err != nil {
			log.Errorf("%s", err)
			return err
		}
	}
	return nil
}

func (s *lbsSchedAlgParams) readSettingsFile(filename string) ([]byte, error) {
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
