package cli

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type lbsSchedAlgParams struct {
	classLoadPcts        map[string]int32
	requestorMap         map[string]string
	rebalanceMinDuration int
	rebalanceThreshold   int

	clpFilePath    string
	reqMapFilePath string
}
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
	g.params.classLoadPcts, err = cl.scootClient.GetClassLoadPcts()
	if err != nil {
		return getReturnError(err)
	}
	g.params.requestorMap, err = cl.scootClient.GetRequestorToClassMap()
	if err != nil {
		return getReturnError(err)
	}
	var tInt int32
	tInt, err = cl.scootClient.GetRebalanceMinDuration()
	if err != nil {
		return getReturnError(err)
	}
	g.params.rebalanceMinDuration = int(tInt)
	tInt, err = cl.scootClient.GetRebalanceThreshold()
	if err != nil {
		return getReturnError(err)
	}
	g.params.rebalanceThreshold = int(tInt)

	if g.printAsJSON {
		asJSON, err := json.Marshal(g.params)
		if err != nil {
			return fmt.Errorf("Error converting status to JSON: %v", err.Error())
		}
		log.Infof("%s\n", asJSON)
		fmt.Printf("%s\n", asJSON) // must also go to stdout in case caller looking in stdout for the results
	} else {
		log.Info("Class Load Percents:")
		fmt.Println("Class Load Percents:")
		for class, pct := range g.params.classLoadPcts {
			log.Infof("%s:%d", class, pct)
			fmt.Println(class, ":", pct)
		}
		log.Info("Requestor (reg exp) to class map:")
		fmt.Println("Requestor (reg exp) to class map:")
		for requestorRe, class := range g.params.requestorMap {
			log.Infof("%s:%s", requestorRe, class)
			fmt.Println(requestorRe, ":", class)
		}
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
	r.Flags().IntVar(&s.rebalanceMinDuration, "rebalance_min", -1, "The number of minutes the tasks must be over rebalance threshold to trigger rebalance. 0 implies no rebalance. (Default of -1 implies no entry)")
	r.Flags().IntVar(&s.rebalanceThreshold, "rebalance_threshold", -1, "The rebalance threshold. 0 implies no rebalance. (Default of -1 implies no entry)")
	return r
}

func (s *lbsSchedAlgParams) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	log.Info("Setting Scheduling Algorithm Parameters", args)

	if s.rebalanceMinDuration > -1 {
		cl.scootClient.SetRebalanceMinDuration(int32(s.rebalanceMinDuration))
	}
	if s.rebalanceThreshold > -1 {
		cl.scootClient.SetRebalanceThreshold(int32(s.rebalanceThreshold))
	}

	content, err := s.readSettingsFile(s.clpFilePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(content, &s.classLoadPcts)
	if err != nil {
		return err
	}

	content, err = s.readSettingsFile(s.reqMapFilePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(content, &s.requestorMap)
	if err != nil {
		return err
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
