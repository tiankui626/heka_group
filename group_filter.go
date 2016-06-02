//a heka extention to influxdb
//@author zzh <zhouzhou@mgtv.com>
//@date 201605
package group

import (
	"errors"
	"fmt"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"strconv"
	"strings"
	"time"
)

type GroupFilter struct {
	msgLoopCount  uint
	data          *map[string][]*Value
	tags          []string
	groups        []string
	values        []string
	logger        string
	serie         string
	onlyProvice   bool //private
	debug         bool
	FlushInterval time.Duration
	batchNum      int
}

type Value struct {
	Name    string
	value   float64
	counter int
}

var (
	Debug        = false
	OnlyProvince = false
)

func getConfString(config interface{}, key string) (string, error) {
	var (
		fieldConf interface{}
		ok        bool
	)
	conf := config.(pipeline.PluginConfig)
	if fieldConf, ok = conf[key]; !ok {
		return "", errors.New(fmt.Sprintf("No '%s' setting", key))
	}
	value, ok := fieldConf.(string)
	if ok {
		return value, nil
	}
	return "", nil
}

func (v *Value) Value() string {
	if len(v.Name) == 0 {
		return fmt.Sprintf("counter=%d", v.counter)
	}
	if v.counter != 0 {
		return fmt.Sprintf("counter_%s=%d,%s=%f", v.Name, v.counter, v.Name, v.value/float64(v.counter))
	}
	return ""
}

func DataValues(values []*Value) string {
	if len(values) == 0 {
		return ""
	}
	var dvs []string
	for _, v := range values {
		s := v.Value()
		if len(s) != 0 {
			dvs = append(dvs, s)
		}
	}
	return strings.Join(dvs, ",")
}

func ReadValue(msg *message.Message, key string) string {
	var value string
	if len(key) == 0 {
		return ""
	}
	if key == "Hostname" {
		return msg.GetHostname()
	}
	fields := msg.GetFields()
	for _, f := range fields {
		if f.GetName() == key {
			vs := f.GetValueString()
			if vs != nil {
				value = vs[0]
			}
			break
		}
	}
	if strings.ToLower(key) == "city" && OnlyProvince && len(value) > 5 {
		bytes_v := []byte(value)
		bytes_v[len(bytes_v)-1] = '0'
		bytes_v[len(bytes_v)-2] = '0'
		bytes_v[len(bytes_v)-3] = '0'
		bytes_v[len(bytes_v)-4] = '0'
		value = string(bytes_v)
	}
	return value
}

func GetKeys(msg *message.Message, keys []string) string {
	var result []string
	for _, key := range keys {
		v := ReadValue(msg, key)
		if len(v) > 0 {
			result = append(result, key+"="+v)
		}
	}
	return strings.Join(result, ",")
}

func (f *GroupFilter) ProcessMessage(msg *message.Message) {
	tags := GetKeys(msg, f.tags)
	groups := GetKeys(msg, f.groups)
	key := tags + " " + groups
	d, ok := (*f.data)[key]
	if !ok {
		if len(f.values) == 0 {
			//values is empty
			d = append(d, &Value{Name: "", value: 0, counter: 0})
		} else {
			for _, v := range f.values {
				d = append(d, &Value{Name: v, value: 0, counter: 0})
			}
		}

		(*f.data)[key] = d
	}
	for _, value := range d {
		if len(value.Name) == 0 {
			//no values in config
			value.counter++
		} else {
			v := ReadValue(msg, value.Name)
			v_float, e := strconv.ParseFloat(v, 64)
			if e == nil {
				//value key exists in msg
				value.value += v_float
				value.counter++
			}
		}
	}
}

// Extract hosts value from config and store it on the plugin instance.
func (f *GroupFilter) Init(config interface{}) error {
	var (
		err error
	)
	tagsConf, _ := getConfString(config, "tags")
	groupsConf, _ := getConfString(config, "groups")
	valuesConf, _ := getConfString(config, "values")
	intervalConf, _ := getConfString(config, "interval")
	loggerConf, _ := getConfString(config, "logger")
	serieNameConf, _ := getConfString(config, "serie_name")
	onlyProvConf, _ := getConfString(config, "only_province")
	debugConf, _ := getConfString(config, "debug")
	batchNumConf, _ := getConfString(config, "batch_num")
	if len(tagsConf) == 0 {
		return errors.New("No 'tags' setting specified.")
	} else {
		f.tags = strings.Split(tagsConf, " ")
	}
	if len(groupsConf) > 0 {
		f.groups = strings.Split(groupsConf, " ")
	}
	if len(intervalConf) == 0 {
		return errors.New("No 'interval' setting specified.")
	} else if f.FlushInterval, err = time.ParseDuration(intervalConf); err != nil {
		return errors.New("No 'interval' parse error.")
	}

	batchNum, err := strconv.Atoi(batchNumConf)
	if err != nil {
		f.batchNum = 100
	} else {
		f.batchNum = batchNum
	}

	f.values = strings.Split(valuesConf, " ")
	f.data = NewData()
	f.logger = loggerConf
	f.serie = serieNameConf
	OnlyProvince = (onlyProvConf == "1")
	Debug = (debugConf == "1")
	if Debug {
		fmt.Printf("config %+v", f)
	}
	return nil
}

func (f *GroupFilter) InjectMessage(fr pipeline.FilterRunner, h pipeline.PluginHelper, payload string) error {
	pack, err := h.PipelinePack(f.msgLoopCount)
	if pack == nil || err != nil {
		fr.LogError(fmt.Errorf("exceeded MaxMsgLoops = %d, %s",
			h.PipelineConfig().Globals.MaxMsgLoops, err))
		return err
	}
	pack.Message.SetUuid(uuid.NewRandom())
	pack.Message.SetPayload(payload)
	pack.Message.SetLogger(f.logger)
	pack.Message.SetType("GroupFilter")
	fr.Inject(pack)
	return nil
}

func (f *GroupFilter) comitter(fr pipeline.FilterRunner, h pipeline.PluginHelper, data *map[string][]*Value) {
	if len(*data) == 0 {
		return
	} else if Debug {
		fmt.Printf("data len:%d\n", len(*data))
	}
	var values []string
	for key, v := range *data {
		dv := DataValues(v)
		if len(dv) == 0 {
			fmt.Printf("data values is empty")
			continue
		}
		if Debug {
			fmt.Printf("values: %s,%s %s\n", f.serie, key, dv)
		}
		values = append(values, fmt.Sprintf("%s,%s %s", f.serie, key, dv))
		if len(values) > f.batchNum {
			f.InjectMessage(fr, h, strings.Join(values, "\n"))
			if Debug {
				fmt.Println(strings.Join(values, "\n"))
			}
			values = values[0:0]
		}
	}
	if len(values) > 0 {
		f.InjectMessage(fr, h, strings.Join(values, "\n"))
	}
}

func NewData() *map[string][]*Value {
	data := make(map[string][]*Value)
	return &data
}

func (f *GroupFilter) receiver(fr pipeline.FilterRunner, h pipeline.PluginHelper) {
	inChan := fr.InChan()
	ticker := time.Tick(f.FlushInterval)
	for {
		select {
		case pack, ok := <-inChan:
			if !ok {
				fmt.Printf("inChan error\n")
				goto end
			}
			f.msgLoopCount = pack.MsgLoopCount
			f.ProcessMessage(pack.Message)
			pack.Recycle(nil)
		case <-ticker:
			go f.comitter(fr, h, f.data)
			f.data = NewData()
		}
	}
end:
	f.comitter(fr, h, f.data)
	return
}

// Fetch correct output and iterate over received messages, checking against
// message hostname and delivering to the output if hostname is in our config.
func (f *GroupFilter) Run(runner pipeline.FilterRunner, helper pipeline.PluginHelper) (
	err error) {
	f.receiver(runner, helper)
	return
}

func init() {
	pipeline.RegisterPlugin("GroupFilter", func() interface{} {
		return new(GroupFilter)
	})
}
