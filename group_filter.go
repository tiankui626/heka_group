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
	data          *map[string]*Value
	tags          []string
	groups        []string
	value         string
	FlushInterval time.Duration
}

type GroupConfig struct {
	Tags     string `toml:"tags"`
	Groups   string `toml:"groups"`
	Interval string `toml:"interval"`
	Value    string `toml:"value"`
}

type Value struct {
	valueName string
	value     float64
	counter   int
}

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
	if len(v.valueName) == 0 || v.value == 0 {
		return fmt.Sprintf("counter=%d", v.counter)
	}
	return fmt.Sprintf("counter=%d,%s=%f", v.counter, v.valueName, v.value)
}

func ReadValue(msg *message.Message, key string) string {
	if len(key) == 0 {
		return ""
	}
	if key == "Hostname" {
		return msg.GetHostname()
	}
	fields := msg.GetFields()
	for _, f := range fields {
		if f.GetName() == key {
			return f.GetValueString()[0]
		}
	}
	return ""
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
		d = &Value{valueName: f.value, value: 0, counter: 0}
		(*f.data)[key] = d
	}
	d.counter++
	v := ReadValue(msg, f.value)
	v_float, e := strconv.ParseFloat(v, 64)
	if e == nil {
		d.value += v_float
	}
}

// Extract hosts value from config and store it on the plugin instance.
func (f *GroupFilter) Init(config interface{}) error {
	var (
		err  error
		conf GroupConfig
	)
	conf.Tags, _ = getConfString(config, "tags")
	conf.Groups, _ = getConfString(config, "groups")
	conf.Value, _ = getConfString(config, "value")
	conf.Interval, _ = getConfString(config, "interval")
	if len(conf.Tags) == 0 {
		return errors.New("No 'tags' setting specified.")
	} else {
		f.tags = strings.Split(conf.Tags, " ")
	}
	if len(conf.Groups) > 0 {
		f.groups = strings.Split(conf.Groups, " ")
	}
	if len(conf.Interval) == 0 {
		return errors.New("No 'interval' setting specified.")
	} else if f.FlushInterval, err = time.ParseDuration(conf.Interval); err != nil {
		return errors.New("No 'interval' parse error.")
	}
	f.value = conf.Value

	f.data = &make(map[string]*Value)
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
	fr.Inject(pack)
	return nil
}

func (f *GroupFilter) comitter(fr pipeline.FilterRunner, h pipeline.PluginHelper) {
	var values []string
	for key, v := range *f.data {
		values = append(values, fmt.Sprintf("%s %s", key, v.Value()))
		if len(values) > 100 {
			f.InjectMessage(fr, h, strings.Join(values, "\n"))
			values = values[0:0]
		}
	}
	if len(values) > 0 {
		f.InjectMessage(fr, h, strings.Join(values, "\n"))
	}
	f.data = &make(map[string]*Value)
}

func (f *GroupFilter) receiver(fr pipeline.FilterRunner, h pipeline.PluginHelper) {
	inChan := fr.InChan()
	ticker := time.Tick(time.Duration(f.FlushInterval) * time.Millisecond)
	for {
		select {
		case pack, ok := <-inChan:
			if !ok {
				//todo
			}
			f.msgLoopCount = pack.MsgLoopCount
			f.ProcessMessage(pack.Message)
			pack.Recycle(nil)
		case <-ticker:
			f.comitter(fr, h)
		}
	}

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
