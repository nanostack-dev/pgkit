package workflow

import (
	"encoding/json"
	"sort"
	"strings"
)

// Root step rows always use an empty item key. Foreach child rows reuse the
// same step name with a concrete item key.
const rootStepItemKey = ""

// stepOutputStateKey centralizes the in-memory output key format shared by the
// runtime and StepContext helpers. Root steps use the bare step name, while
// foreach items use `step[item]`.
func stepOutputStateKey(stepName, itemKey string) string {
	if itemKey == rootStepItemKey {
		return stepName
	}
	return stepName + "[" + itemKey + "]"
}

func stepItemOutputsPrefix(stepName string) string {
	return stepName + "["
}

func rememberStepOutput(state map[string]json.RawMessage, stepName, itemKey string, payload []byte) {
	if state == nil {
		return
	}
	state[stepOutputStateKey(stepName, itemKey)] = payload
}

func collectStepItemOutputs(state map[string]json.RawMessage, stepName string) []StepItemOutput {
	prefix := stepItemOutputsPrefix(stepName)
	outputs := make([]StepItemOutput, 0)
	for key, payload := range state {
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "]") {
			continue
		}
		itemKey := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "]")
		if itemKey == rootStepItemKey {
			continue
		}
		outputs = append(outputs, StepItemOutput{ItemKey: itemKey, Payload: append(json.RawMessage(nil), payload...)})
	}
	sort.Slice(outputs, func(i, j int) bool { return outputs[i].ItemKey < outputs[j].ItemKey })
	return outputs
}

func isRootStepRecord(record StepRecord) bool {
	return record.ItemKey == rootStepItemKey
}

func splitRootAndItemRecords(records []StepRecord) (*StepRecord, []StepRecord) {
	var root *StepRecord
	items := make([]StepRecord, 0, len(records))
	for _, record := range records {
		if isRootStepRecord(record) && root == nil {
			copyRecord := record
			root = &copyRecord
			continue
		}
		items = append(items, record)
	}
	return root, items
}

func findRootStepRecord(records []StepRecord) (StepRecord, bool) {
	for _, record := range records {
		if isRootStepRecord(record) {
			return record, true
		}
	}
	return StepRecord{}, false
}

func allStepRecordsSucceeded(records []StepRecord) bool {
	if len(records) == 0 {
		return false
	}
	for _, record := range records {
		if record.Status != StepStatusSucceeded {
			return false
		}
	}
	return true
}
