package sinks

import (
	"bytes"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

func GetString(event *kube.EnhancedEvent, text string) (string, error) {
	tmpl, err := template.New("template").Funcs(sprig.TxtFuncMap()).Parse(text)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	// TODO: Should we send event directly or more events?
	err = tmpl.Execute(buf, event)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func convertLayoutTemplate(layout map[string]interface{}, ev *kube.EnhancedEvent) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for key, value := range layout {
		m, err := convertTemplate(value, ev)
		if err != nil {
			return nil, err
		}
		result[key] = m
	}
	return result, nil
}

func convertTemplate(value interface{}, ev *kube.EnhancedEvent) (interface{}, error) {
	switch v := value.(type) {
	case string:
		rendered, err := GetString(ev, v)
		if err != nil {
			return nil, err
		}

		return rendered, nil
	case map[interface{}]interface{}:
		strKeysMap := make(map[string]interface{})
		for k, v := range v {
			res, err := convertTemplate(v, ev)
			if err != nil {
				return nil, err
			}
			// TODO: It's a bit dangerous
			strKeysMap[k.(string)] = res
		}
		return strKeysMap, nil
	case map[string]interface{}:
		strKeysMap := make(map[string]interface{})
		for k, v := range v {
			res, err := convertTemplate(v, ev)
			if err != nil {
				return nil, err
			}
			strKeysMap[k] = res
		}
		return strKeysMap, nil
	case []interface{}:
		listConf := make([]interface{}, len(v))
		for i := range v {
			t, err := convertTemplate(v[i], ev)
			if err != nil {
				log.Debug().Err(err).Msgf("convertTemplate failed: %s", v)
				return nil, err
			}
			listConf[i] = t
		}
		return listConf, nil
	}
	return nil, nil
}

func serializeEventWithLayout(layout map[string]interface{}, ev *kube.EnhancedEvent) ([]byte, error) {
	var toSend []byte
	if layout != nil {
		res, err := convertLayoutTemplate(layout, ev)
		if err != nil {
			return nil, err
		}

		toSend, err = json.Marshal(res)
		if err != nil {
			return nil, err
		}
	} else {
		toSend = ev.ToJSON()
	}
	return toSend, nil
}

func serializeEventWithStreamLabels(streamLabels map[string]string, ev *kube.EnhancedEvent) (map[string]string, error) {
	var toSend []byte
	if streamLabels != nil {
		res, err := convertStreamLabelsTemplate(streamLabels, ev)
		if err != nil {
			log.Debug().Err(err).Msgf("parse template failed: %s", streamLabels)
			return nil, err
		}

		toSend, err = json.Marshal(res)
		if err != nil {
			return nil, err
		}
	} else {
		toSend = ev.ToJSON()
	}
	var mapData map[string]string
	err := json.Unmarshal(toSend, &mapData)
	if err != nil {
		return nil, err
	}
	return mapData, nil
}

func convertStreamLabelsTemplate(streamLabels map[string]string, ev *kube.EnhancedEvent) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	cpEvent := ev.DeDot()
	log.Debug().Msgf("cpEvent before: %s", ev)
	log.Debug().Msgf("ev before: %s", ev)
	for key, value := range cpEvent.Event.Labels {
		newKey := strings.Replace(key, "-", "_", -1)
		if newKey != key {
			cpEvent.Event.Labels[newKey] = value
			delete(cpEvent.Event.Labels, key)
		}
	}
	log.Debug().Msgf("cpEvent: %s", cpEvent)
	for key, value := range streamLabels {
		value = strings.Replace(key, "-", "_", -1)
		m, err := convertTemplate(value, &cpEvent)
		if err != nil {
			log.Debug().Err(err).Msgf("convertStreamLabelsTemplate failed: %s", value)
			return nil, err
		}
		result[key] = m
	}
	return result, nil
}
